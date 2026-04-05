use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use axum_server::Handle;
use axum_server::tls_rustls::RustlsConfig;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{DaemonConfig, NodeSection};
use crate::configuration;

const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone)]
struct ApiState {
    config_rx: watch::Receiver<DaemonConfig>,
    config_tx: watch::Sender<DaemonConfig>,
}

#[derive(Debug, Deserialize)]
struct ConfigurationSyncRequest {
    uuid: String,
    panel_version: String,
    name: String,
    fqdn: String,
    daemon_port: u16,
    sftp_port: u16,
    use_ssl: bool,
    location_name: String,
    location_country: String,
    updated_at: String,
}

#[derive(Debug, Serialize)]
struct ApiSuccessResponse {
    ok: bool,
}

#[derive(Debug, Serialize)]
struct ApiErrorResponse {
    message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ListenerSpec {
    address: SocketAddr,
    tls: Option<TlsPaths>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TlsPaths {
    cert_path: String,
    key_path: String,
}

struct RunningServer {
    handle: Handle,
    spec: ListenerSpec,
    task: JoinHandle<Result<()>>,
}

pub struct ApiService {
    config_rx: watch::Receiver<DaemonConfig>,
    config_tx: watch::Sender<DaemonConfig>,
    cancellation: CancellationToken,
}

impl ApiService {
    pub fn new(
        config_rx: watch::Receiver<DaemonConfig>,
        config_tx: watch::Sender<DaemonConfig>,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            config_rx,
            config_tx,
            cancellation,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let state = ApiState {
            config_rx: self.config_rx.clone(),
            config_tx: self.config_tx.clone(),
        };
        let mut running: Option<RunningServer> = None;

        self.reconcile_server(&state, &mut running).await?;

        loop {
            tokio::select! {
                _ = self.cancellation.cancelled() => {
                    self.stop_server(&mut running).await?;
                    break;
                }
                changed = self.config_rx.changed() => {
                    if changed.is_err() {
                        self.stop_server(&mut running).await?;
                        break;
                    }

                    self.reconcile_server(&state, &mut running).await?;
                }
            }
        }

        Ok(())
    }

    async fn reconcile_server(
        &self,
        state: &ApiState,
        running: &mut Option<RunningServer>,
    ) -> Result<()> {
        let desired = listener_spec(&self.config_rx.borrow())?;

        if running.as_ref().map(|server| &server.spec) == desired.as_ref() {
            return Ok(());
        }

        self.stop_server(running).await?;

        if let Some(spec) = desired {
            *running = Some(start_server(spec, state.clone())?);
        } else {
            info!("api server waiting for node configuration");
        }

        Ok(())
    }

    async fn stop_server(&self, running: &mut Option<RunningServer>) -> Result<()> {
        let Some(server) = running.take() else {
            return Ok(());
        };

        server
            .handle
            .graceful_shutdown(Some(Duration::from_secs(5)));
        server
            .task
            .await
            .context("failed to join api server task")??;

        Ok(())
    }
}

fn start_server(spec: ListenerSpec, state: ApiState) -> Result<RunningServer> {
    let handle = Handle::new();
    let app = Router::new()
        .route("/api/daemon/configuration/sync", post(sync_configuration))
        .with_state(state);
    let server_handle = handle.clone();
    let server_spec = spec.clone();

    let task = tokio::spawn(async move {
        let result = match &server_spec.tls {
            Some(tls) => {
                let config = RustlsConfig::from_pem_file(&tls.cert_path, &tls.key_path)
                    .await
                    .context("failed to load tls certificate files")?;

                info!(
                    address = %server_spec.address,
                    cert_path = %tls.cert_path,
                    key_path = %tls.key_path,
                    "api server listening with tls"
                );

                axum_server::bind_rustls(server_spec.address, config)
                    .handle(server_handle)
                    .serve(app.into_make_service())
                    .await
                    .context("tls api server stopped")
            }
            None => {
                info!(address = %server_spec.address, "api server listening without tls");

                axum_server::bind(server_spec.address)
                    .handle(server_handle)
                    .serve(app.into_make_service())
                    .await
                    .context("api server stopped")
            }
        };

        if let Err(error) = &result {
            warn!(error = %error, "api server exited with error");
        }

        result
    });

    Ok(RunningServer { handle, spec, task })
}

fn listener_spec(config: &DaemonConfig) -> Result<Option<ListenerSpec>> {
    let Some(node) = &config.node else {
        return Ok(None);
    };

    let address = SocketAddr::from((Ipv4Addr::UNSPECIFIED, node.daemon_port));
    let tls = if node.use_ssl {
        let cert_path = node
            .tls_cert_path
            .clone()
            .context("tls_cert_path is required when SSL is enabled")?;
        let key_path = node
            .tls_key_path
            .clone()
            .context("tls_key_path is required when SSL is enabled")?;

        Some(TlsPaths {
            cert_path,
            key_path,
        })
    } else {
        None
    };

    Ok(Some(ListenerSpec { address, tls }))
}

async fn sync_configuration(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<ConfigurationSyncRequest>,
) -> std::result::Result<Json<ApiSuccessResponse>, (StatusCode, Json<ApiErrorResponse>)> {
    let mut config = state.config_rx.borrow().clone();

    authorize_request(&config, &headers)?;

    if payload.panel_version != CURRENT_VERSION {
        return Err(error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "This version of skyportd isn't compatible with Skyport panel {}.",
                payload.panel_version
            ),
        ));
    }

    if payload.uuid != config.daemon.uuid {
        return Err(error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "The daemon identity does not match this configuration.".to_string(),
        ));
    }

    let tls_cert_path = config
        .node
        .as_ref()
        .and_then(|node| node.tls_cert_path.clone());
    let tls_key_path = config
        .node
        .as_ref()
        .and_then(|node| node.tls_key_path.clone());

    config.node = Some(NodeSection {
        name: payload.name,
        fqdn: payload.fqdn,
        daemon_port: payload.daemon_port,
        sftp_port: payload.sftp_port,
        use_ssl: payload.use_ssl,
        location_name: payload.location_name,
        location_country: payload.location_country,
        updated_at: payload.updated_at,
        tls_cert_path,
        tls_key_path,
    });

    configuration::ensure_node_runtime_configuration(&mut config)
        .map_err(|error| error_response(StatusCode::UNPROCESSABLE_ENTITY, error.to_string()))?;

    if let Some(node) = &config.node {
        config.persist_node_configuration(node).map_err(|error| {
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to persist node configuration: {error}"),
            )
        })?;
    }

    let _ = state.config_tx.send(config);

    Ok(Json(ApiSuccessResponse { ok: true }))
}

fn authorize_request(
    config: &DaemonConfig,
    headers: &HeaderMap,
) -> std::result::Result<(), (StatusCode, Json<ApiErrorResponse>)> {
    let expected = config
        .panel
        .daemon_callback_token
        .as_deref()
        .filter(|token| !token.is_empty())
        .ok_or_else(|| {
            error_response(
                StatusCode::UNAUTHORIZED,
                "Missing daemon callback token.".to_string(),
            )
        })?;

    let provided = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .ok_or_else(|| {
            error_response(
                StatusCode::UNAUTHORIZED,
                "Missing callback authorization token.".to_string(),
            )
        })?;

    if provided != expected {
        return Err(error_response(
            StatusCode::UNAUTHORIZED,
            "The callback token is invalid.".to_string(),
        ));
    }

    Ok(())
}

fn error_response(status: StatusCode, message: String) -> (StatusCode, Json<ApiErrorResponse>) {
    (status, Json(ApiErrorResponse { message }))
}
