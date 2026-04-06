use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{delete, post};
use axum::{Json, Router};
use axum_server::Handle;
use axum_server::tls_rustls::RustlsConfig;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{DaemonConfig, NodeSection};
use crate::configuration;
use crate::server_registry::{
    ManagedServerCargo, ManagedServerLimits, ManagedServerRecord, ManagedServerUser,
    ManagedServerVariable, ServerRegistry,
};

const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone)]
struct ApiState {
    config_rx: watch::Receiver<DaemonConfig>,
    config_tx: watch::Sender<DaemonConfig>,
    server_registry: ServerRegistry,
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

#[derive(Debug, Deserialize)]
struct ServerSyncRequest {
    uuid: String,
    panel_version: String,
    server: ServerPayload,
}

#[derive(Debug, Deserialize)]
struct ServerDeleteRequest {
    uuid: String,
    panel_version: String,
}

#[derive(Debug, Deserialize)]
struct ServerPayload {
    id: u64,
    node_id: u64,
    name: String,
    status: String,
    volume_path: String,
    created_at: String,
    updated_at: String,
    user: ServerUserPayload,
    limits: ServerLimitsPayload,
    cargo: ServerCargoPayload,
}

#[derive(Debug, Deserialize)]
struct ServerUserPayload {
    id: u64,
    name: String,
    email: String,
}

#[derive(Debug, Deserialize)]
struct ServerLimitsPayload {
    memory_mib: u64,
    cpu_limit: u64,
    disk_mib: u64,
}

#[derive(Debug, Deserialize)]
struct ServerCargoPayload {
    id: u64,
    name: String,
    slug: String,
    source_type: String,
    startup_command: String,
    config_files: String,
    config_startup: String,
    config_logs: String,
    config_stop: String,
    install_script: Option<String>,
    install_container: Option<String>,
    install_entrypoint: Option<String>,
    features: Vec<String>,
    docker_images: BTreeMap<String, String>,
    file_denylist: Vec<String>,
    file_hidden_list: Vec<String>,
    variables: Vec<ManagedServerVariable>,
    definition: Value,
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
    server_registry: ServerRegistry,
    cancellation: CancellationToken,
}

impl ApiService {
    pub fn new(
        config_rx: watch::Receiver<DaemonConfig>,
        config_tx: watch::Sender<DaemonConfig>,
        server_registry: ServerRegistry,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            config_rx,
            config_tx,
            server_registry,
            cancellation,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let state = ApiState {
            config_rx: self.config_rx.clone(),
            config_tx: self.config_tx.clone(),
            server_registry: self.server_registry.clone(),
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
        .route("/api/daemon/servers/sync", post(sync_server))
        .route("/api/daemon/servers/{server_id}", delete(delete_server))
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
    ensure_compatible_request(&config, &payload.uuid, &payload.panel_version)?;

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

async fn sync_server(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<ServerSyncRequest>,
) -> std::result::Result<Json<ApiSuccessResponse>, (StatusCode, Json<ApiErrorResponse>)> {
    let config = state.config_rx.borrow().clone();

    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &payload.uuid, &payload.panel_version)?;

    let configured_node_id = config.panel.node_id.ok_or_else(|| {
        error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "This daemon is not enrolled to a node yet.".to_string(),
        )
    })?;

    if payload.server.node_id != configured_node_id {
        return Err(error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "The server does not belong to this node.".to_string(),
        ));
    }

    let server = ManagedServerRecord {
        id: payload.server.id,
        node_id: payload.server.node_id,
        name: payload.server.name,
        status: payload.server.status,
        volume_path: payload.server.volume_path,
        created_at: payload.server.created_at,
        updated_at: payload.server.updated_at,
        container_id: None,
        last_error: None,
        user: ManagedServerUser {
            id: payload.server.user.id,
            name: payload.server.user.name,
            email: payload.server.user.email,
        },
        limits: ManagedServerLimits {
            memory_mib: payload.server.limits.memory_mib,
            cpu_limit: payload.server.limits.cpu_limit,
            disk_mib: payload.server.limits.disk_mib,
        },
        cargo: ManagedServerCargo {
            id: payload.server.cargo.id,
            name: payload.server.cargo.name,
            slug: payload.server.cargo.slug,
            source_type: payload.server.cargo.source_type,
            startup_command: payload.server.cargo.startup_command,
            config_files: payload.server.cargo.config_files,
            config_startup: payload.server.cargo.config_startup,
            config_logs: payload.server.cargo.config_logs,
            config_stop: payload.server.cargo.config_stop,
            install_script: payload.server.cargo.install_script,
            install_container: payload.server.cargo.install_container,
            install_entrypoint: payload.server.cargo.install_entrypoint,
            features: payload.server.cargo.features,
            docker_images: payload.server.cargo.docker_images,
            file_denylist: payload.server.cargo.file_denylist,
            file_hidden_list: payload.server.cargo.file_hidden_list,
            variables: payload.server.cargo.variables,
            definition: payload.server.cargo.definition,
        },
    };

    state
        .server_registry
        .upsert_server(&server)
        .map_err(|error| {
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to persist server state: {error}"),
            )
        })?;

    info!(server_id = server.id, node_id = server.node_id, name = %server.name, "server state synced");

    Ok(Json(ApiSuccessResponse { ok: true }))
}

async fn delete_server(
    Path(server_id): Path<u64>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<ServerDeleteRequest>,
) -> std::result::Result<Json<ApiSuccessResponse>, (StatusCode, Json<ApiErrorResponse>)> {
    let config = state.config_rx.borrow().clone();

    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &payload.uuid, &payload.panel_version)?;

    let deleted = state
        .server_registry
        .delete_server(server_id)
        .map_err(|error| {
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to delete server state: {error}"),
            )
        })?;

    if deleted {
        info!(server_id, "server state deleted");
    } else {
        warn!(
            server_id,
            "server state delete requested for missing record"
        );
    }

    Ok(Json(ApiSuccessResponse { ok: true }))
}

fn ensure_compatible_request(
    config: &DaemonConfig,
    request_uuid: &str,
    panel_version: &str,
) -> std::result::Result<(), (StatusCode, Json<ApiErrorResponse>)> {
    if panel_version != CURRENT_VERSION {
        return Err(error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "This version of skyportd isn't compatible with Skyport panel {}.",
                panel_version
            ),
        ));
    }

    if request_uuid != config.daemon.uuid {
        return Err(error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "The daemon identity does not match this configuration.".to_string(),
        ));
    }

    Ok(())
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
