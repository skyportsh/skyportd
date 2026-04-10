mod filesystem;
mod backups;
mod websocket;

use filesystem::*;
use backups::*;
use websocket::*;

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path as StdPath, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use axum_server::Handle;
use axum_server::tls_rustls::RustlsConfig;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::process::Command;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{DaemonConfig, NodeSection, managed_server_volume_path, project_root};
use crate::configuration;
use crate::server_registry::{
    ManagedServerAllocation, ManagedServerCargo, ManagedServerFirewallRule,
    ManagedServerInterconnect, ManagedServerLimits, ManagedServerWorkflow, ManagedServerWorkflowStep,
    ManagedServerRecord, ManagedServerUser, ManagedServerVariable, ServerRegistry,
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
struct ServerPowerRequest {
    uuid: String,
    panel_version: String,
    signal: PowerSignal,
}

#[derive(Debug, Deserialize)]
struct WsAuthorizationQuery {
    uuid: String,
    panel_version: String,
}

#[derive(Debug, Deserialize)]
struct ServerFilesystemQuery {
    uuid: String,
    panel_version: String,
    path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ServerFileContentsRequest {
    uuid: String,
    panel_version: String,
    path: String,
    contents: String,
}

#[derive(Debug, Deserialize)]
struct ServerCreateFilesystemEntryRequest {
    uuid: String,
    panel_version: String,
    path: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct ServerDeleteFilesRequest {
    uuid: String,
    panel_version: String,
    paths: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ServerRenameFilesystemEntryRequest {
    uuid: String,
    panel_version: String,
    path: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct ServerTransferFilesRequest {
    uuid: String,
    panel_version: String,
    paths: Vec<String>,
    destination: String,
}

#[derive(Debug, Deserialize)]
struct ServerUpdatePermissionsRequest {
    uuid: String,
    panel_version: String,
    paths: Vec<String>,
    permissions: String,
}

#[derive(Debug, Deserialize)]
struct ServerArchiveFilesRequest {
    uuid: String,
    panel_version: String,
    paths: Vec<String>,
    path: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct ServerExtractArchiveRequest {
    uuid: String,
    panel_version: String,
    path: String,
    destination: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ServerUploadFileQuery {
    uuid: String,
    panel_version: String,
    path: Option<String>,
    name: String,
}

#[derive(Debug, Serialize)]
pub(super) struct FilesystemEntryPayload {
    kind: String,
    last_modified_at: Option<u64>,
    name: String,
    path: String,
    permissions: Option<String>,
    size_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
pub(super) struct DirectoryListingPayload {
    entries: Vec<FilesystemEntryPayload>,
    parent_path: Option<String>,
    path: String,
}

#[derive(Debug, Serialize)]
pub(super) struct FileContentsPayload {
    contents: String,
    last_modified_at: Option<u64>,
    path: String,
    permissions: Option<String>,
    size_bytes: u64,
}

#[derive(Debug, Serialize)]
struct ServerFilesystemMutationResponse {
    message: String,
    ok: bool,
}

#[derive(Debug, Deserialize)]
struct WsClientEvent {
    event: String,
    #[serde(default)]
    args: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct WsTokenPayload {
    daemon_uuid: String,
    exp: u64,
    panel_version: String,
    server_id: u64,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum PowerSignal {
    Start,
    Stop,
    Restart,
    Kill,
    Reinstall,
}

#[derive(Debug, Deserialize)]
struct ServerPayload {
    id: u64,
    node_id: u64,
    name: String,
    status: String,
    #[serde(rename = "volume_path")]
    _volume_path: String,
    created_at: String,
    updated_at: String,
    docker_image: Option<String>,
    allocation: ServerAllocationPayload,
    user: ServerUserPayload,
    limits: ServerLimitsPayload,
    cargo: ServerCargoPayload,
    #[serde(default)]
    firewall_rules: Vec<ServerFirewallRulePayload>,
    #[serde(default)]
    interconnects: Vec<ServerInterconnectPayload>,
    #[serde(default)]
    workflows: Vec<ServerWorkflowPayload>,
}

#[derive(Debug, Deserialize)]
struct ServerWorkflowPayload {
    id: u64,
    name: String,
    #[serde(default)]
    nodes: Vec<ServerWorkflowStepPayload>,
}

#[derive(Debug, Deserialize)]
struct ServerWorkflowStepPayload {
    id: String,
    #[serde(rename = "type")]
    step_type: String,
    kind: String,
    #[serde(default)]
    config: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct ServerInterconnectPayload {
    id: u64,
    name: String,
}

#[derive(Debug, Deserialize)]
struct ServerFirewallRulePayload {
    id: u64,
    direction: String,
    action: String,
    protocol: String,
    source: String,
    port_start: Option<u64>,
    port_end: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ServerAllocationPayload {
    id: u64,
    bind_ip: String,
    port: u64,
    ip_alias: Option<String>,
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
pub(super) struct ApiErrorResponse {
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
        .route(
            "/api/daemon/servers/{server_id}/files",
            get(list_server_files)
                .post(create_server_file)
                .delete(delete_server_files),
        )
        .route(
            "/api/daemon/servers/{server_id}/files/contents",
            get(read_server_file).put(write_server_file),
        )
        .route(
            "/api/daemon/servers/{server_id}/files/directories",
            post(create_server_directory),
        )
        .route(
            "/api/daemon/servers/{server_id}/files/rename",
            patch(rename_server_file),
        )
        .route(
            "/api/daemon/servers/{server_id}/files/move",
            post(move_server_files),
        )
        .route(
            "/api/daemon/servers/{server_id}/files/copy",
            post(copy_server_files),
        )
        .route(
            "/api/daemon/servers/{server_id}/files/permissions",
            patch(update_server_file_permissions),
        )
        .route(
            "/api/daemon/servers/{server_id}/files/archive",
            post(create_server_archive),
        )
        .route(
            "/api/daemon/servers/{server_id}/files/extract",
            post(extract_server_archive),
        )
        .route(
            "/api/daemon/servers/{server_id}/files/upload",
            post(upload_server_file),
        )
        .route("/api/daemon/servers/{server_id}/power", post(power_server))
        .route("/api/daemon/servers/{server_id}/backups", post(handle_backup))
        .route("/api/daemon/servers/{server_id}/transfer", post(handle_transfer))
        .route("/api/daemon/servers/{server_id}/backups/{backup_uuid}/download", get(download_backup))
        .route(
            "/api/daemon/servers/{server_id}/install-log",
            get(download_install_log),
        )
        .route("/api/daemon/servers/{server_id}/ws", get(server_ws))
        .layer(axum::extract::DefaultBodyLimit::max(1024 * 1024 * 1100)) // 1.1 GB (backup uploads + overhead)
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

    let expected_volume_path = format!("volumes/{}", payload.server.id);
    managed_server_volume_path(payload.server.id).map_err(internal_error)?;

    let existing = state
        .server_registry
        .get_server(payload.server.id)
        .map_err(|error| {
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to load existing server state: {error}"),
            )
        })?;

    let server = ManagedServerRecord {
        id: payload.server.id,
        node_id: payload.server.node_id,
        name: payload.server.name,
        status: payload.server.status,
        volume_path: expected_volume_path,
        created_at: payload.server.created_at,
        updated_at: payload.server.updated_at,
        docker_image: payload.server.docker_image,
        allocation: ManagedServerAllocation {
            id: payload.server.allocation.id,
            bind_ip: payload.server.allocation.bind_ip,
            port: payload.server.allocation.port,
            ip_alias: payload.server.allocation.ip_alias,
        },
        firewall_rules: payload
            .server
            .firewall_rules
            .into_iter()
            .map(|rule| ManagedServerFirewallRule {
                id: rule.id,
                direction: rule.direction,
                action: rule.action,
                protocol: rule.protocol,
                source: rule.source,
                port_start: rule.port_start,
                port_end: rule.port_end,
            })
            .collect(),
        interconnects: payload
            .server
            .interconnects
            .into_iter()
            .map(|ic| ManagedServerInterconnect {
                id: ic.id,
                name: ic.name,
            })
            .collect(),
        workflows: payload
            .server
            .workflows
            .into_iter()
            .map(|w| ManagedServerWorkflow {
                id: w.id,
                name: w.name,
                nodes: w
                    .nodes
                    .into_iter()
                    .map(|n| ManagedServerWorkflowStep {
                        id: n.id,
                        step_type: n.step_type,
                        kind: n.kind,
                        config: n.config,
                    })
                    .collect(),
            })
            .collect(),
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

    handle_synced_server_update(&state.server_registry, existing.as_ref(), &server)
        .await
        .map_err(internal_error)?;

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

async fn list_server_files(
    Path(server_id): Path<u64>,
    Query(query): Query<ServerFilesystemQuery>,
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> std::result::Result<Json<DirectoryListingPayload>, (StatusCode, Json<ApiErrorResponse>)> {
    let config = state.config_rx.borrow().clone();

    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &query.uuid, &query.panel_version)?;
    let server = load_managed_server(&state.server_registry, &config, server_id)?;
    let requested_path = query.path.unwrap_or_default();

    let listing = tokio::task::spawn_blocking(move || list_directory(&server, &requested_path))
        .await
        .context("failed to join files listing task")
        .map_err(internal_error)?
        .map_err(filesystem_error)?;

    Ok(Json(listing))
}

async fn read_server_file(
    Path(server_id): Path<u64>,
    Query(query): Query<ServerFilesystemQuery>,
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> std::result::Result<Json<FileContentsPayload>, (StatusCode, Json<ApiErrorResponse>)> {
    let config = state.config_rx.borrow().clone();

    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &query.uuid, &query.panel_version)?;
    let server = load_managed_server(&state.server_registry, &config, server_id)?;
    let requested_path = query.path.unwrap_or_default();

    let file = tokio::task::spawn_blocking(move || read_text_file(&server, &requested_path))
        .await
        .context("failed to join file read task")
        .map_err(internal_error)?
        .map_err(filesystem_error)?;

    Ok(Json(file))
}

async fn write_server_file(
    Path(server_id): Path<u64>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<ServerFileContentsRequest>,
) -> std::result::Result<Json<ServerFilesystemMutationResponse>, (StatusCode, Json<ApiErrorResponse>)>
{
    let config = state.config_rx.borrow().clone();

    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &payload.uuid, &payload.panel_version)?;
    let server = load_managed_server(&state.server_registry, &config, server_id)?;

    tokio::task::spawn_blocking(move || write_text_file(&server, &payload.path, &payload.contents))
        .await
        .context("failed to join file write task")
        .map_err(internal_error)?
        .map_err(filesystem_error)?;

    Ok(Json(ServerFilesystemMutationResponse {
        message: "File saved successfully.".to_string(),
        ok: true,
    }))
}

async fn create_server_file(
    Path(server_id): Path<u64>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<ServerCreateFilesystemEntryRequest>,
) -> std::result::Result<Json<ServerFilesystemMutationResponse>, (StatusCode, Json<ApiErrorResponse>)>
{
    let config = state.config_rx.borrow().clone();

    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &payload.uuid, &payload.panel_version)?;
    let server = load_managed_server(&state.server_registry, &config, server_id)?;

    tokio::task::spawn_blocking(move || create_file(&server, &payload.path, &payload.name))
        .await
        .context("failed to join file creation task")
        .map_err(internal_error)?
        .map_err(filesystem_error)?;

    Ok(Json(ServerFilesystemMutationResponse {
        message: "File created successfully.".to_string(),
        ok: true,
    }))
}

async fn create_server_directory(
    Path(server_id): Path<u64>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<ServerCreateFilesystemEntryRequest>,
) -> std::result::Result<Json<ServerFilesystemMutationResponse>, (StatusCode, Json<ApiErrorResponse>)>
{
    let config = state.config_rx.borrow().clone();

    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &payload.uuid, &payload.panel_version)?;
    let server = load_managed_server(&state.server_registry, &config, server_id)?;

    tokio::task::spawn_blocking(move || create_directory(&server, &payload.path, &payload.name))
        .await
        .context("failed to join directory creation task")
        .map_err(internal_error)?
        .map_err(filesystem_error)?;

    Ok(Json(ServerFilesystemMutationResponse {
        message: "Directory created successfully.".to_string(),
        ok: true,
    }))
}

async fn delete_server_files(
    Path(server_id): Path<u64>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<ServerDeleteFilesRequest>,
) -> std::result::Result<Json<ServerFilesystemMutationResponse>, (StatusCode, Json<ApiErrorResponse>)>
{
    let config = state.config_rx.borrow().clone();

    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &payload.uuid, &payload.panel_version)?;
    let server = load_managed_server(&state.server_registry, &config, server_id)?;
    let deleted_count = payload.paths.len();

    tokio::task::spawn_blocking(move || delete_files(&server, &payload.paths))
        .await
        .context("failed to join file deletion task")
        .map_err(internal_error)?
        .map_err(filesystem_error)?;

    Ok(Json(ServerFilesystemMutationResponse {
        message: if deleted_count == 1 {
            "Deleted 1 item.".to_string()
        } else {
            format!("Deleted {deleted_count} items.")
        },
        ok: true,
    }))
}

async fn rename_server_file(
    Path(server_id): Path<u64>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<ServerRenameFilesystemEntryRequest>,
) -> std::result::Result<Json<ServerFilesystemMutationResponse>, (StatusCode, Json<ApiErrorResponse>)>
{
    let config = state.config_rx.borrow().clone();

    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &payload.uuid, &payload.panel_version)?;
    let server = load_managed_server(&state.server_registry, &config, server_id)?;

    tokio::task::spawn_blocking(move || {
        rename_filesystem_entry(&server, &payload.path, &payload.name)
    })
    .await
    .context("failed to join file rename task")
    .map_err(internal_error)?
    .map_err(filesystem_error)?;

    Ok(Json(ServerFilesystemMutationResponse {
        message: "Item renamed successfully.".to_string(),
        ok: true,
    }))
}

async fn move_server_files(
    Path(server_id): Path<u64>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<ServerTransferFilesRequest>,
) -> std::result::Result<Json<ServerFilesystemMutationResponse>, (StatusCode, Json<ApiErrorResponse>)>
{
    let config = state.config_rx.borrow().clone();

    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &payload.uuid, &payload.panel_version)?;
    let server = load_managed_server(&state.server_registry, &config, server_id)?;
    let moved_count = payload.paths.len();

    tokio::task::spawn_blocking(move || move_files(&server, &payload.paths, &payload.destination))
        .await
        .context("failed to join file move task")
        .map_err(internal_error)?
        .map_err(filesystem_error)?;

    Ok(Json(ServerFilesystemMutationResponse {
        message: if moved_count == 1 {
            "Moved 1 item.".to_string()
        } else {
            format!("Moved {moved_count} items.")
        },
        ok: true,
    }))
}

async fn copy_server_files(
    Path(server_id): Path<u64>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<ServerTransferFilesRequest>,
) -> std::result::Result<Json<ServerFilesystemMutationResponse>, (StatusCode, Json<ApiErrorResponse>)>
{
    let config = state.config_rx.borrow().clone();

    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &payload.uuid, &payload.panel_version)?;
    let server = load_managed_server(&state.server_registry, &config, server_id)?;
    let copied_count = payload.paths.len();

    tokio::task::spawn_blocking(move || copy_files(&server, &payload.paths, &payload.destination))
        .await
        .context("failed to join file copy task")
        .map_err(internal_error)?
        .map_err(filesystem_error)?;

    Ok(Json(ServerFilesystemMutationResponse {
        message: if copied_count == 1 {
            "Copied 1 item.".to_string()
        } else {
            format!("Copied {copied_count} items.")
        },
        ok: true,
    }))
}

async fn update_server_file_permissions(
    Path(server_id): Path<u64>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<ServerUpdatePermissionsRequest>,
) -> std::result::Result<Json<ServerFilesystemMutationResponse>, (StatusCode, Json<ApiErrorResponse>)>
{
    let config = state.config_rx.borrow().clone();

    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &payload.uuid, &payload.panel_version)?;
    let server = load_managed_server(&state.server_registry, &config, server_id)?;

    tokio::task::spawn_blocking(move || {
        update_file_permissions(&server, &payload.paths, &payload.permissions)
    })
    .await
    .context("failed to join permissions update task")
    .map_err(internal_error)?
    .map_err(filesystem_error)?;

    Ok(Json(ServerFilesystemMutationResponse {
        message: "Permissions updated successfully.".to_string(),
        ok: true,
    }))
}

async fn create_server_archive(
    Path(server_id): Path<u64>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<ServerArchiveFilesRequest>,
) -> std::result::Result<Json<ServerFilesystemMutationResponse>, (StatusCode, Json<ApiErrorResponse>)>
{
    let config = state.config_rx.borrow().clone();

    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &payload.uuid, &payload.panel_version)?;
    let server = load_managed_server(&state.server_registry, &config, server_id)?;

    tokio::task::spawn_blocking(move || {
        create_archive(&server, &payload.paths, &payload.path, &payload.name)
    })
    .await
    .context("failed to join archive creation task")
    .map_err(internal_error)?
    .map_err(filesystem_error)?;

    Ok(Json(ServerFilesystemMutationResponse {
        message: "Archive created successfully.".to_string(),
        ok: true,
    }))
}

async fn extract_server_archive(
    Path(server_id): Path<u64>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<ServerExtractArchiveRequest>,
) -> std::result::Result<Json<ServerFilesystemMutationResponse>, (StatusCode, Json<ApiErrorResponse>)>
{
    let config = state.config_rx.borrow().clone();

    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &payload.uuid, &payload.panel_version)?;
    let server = load_managed_server(&state.server_registry, &config, server_id)?;
    let destination = payload.destination.unwrap_or_default();

    tokio::task::spawn_blocking(move || extract_archive(&server, &payload.path, &destination))
        .await
        .context("failed to join archive extraction task")
        .map_err(internal_error)?
        .map_err(filesystem_error)?;

    Ok(Json(ServerFilesystemMutationResponse {
        message: "Archive extracted successfully.".to_string(),
        ok: true,
    }))
}

async fn upload_server_file(
    Path(server_id): Path<u64>,
    Query(query): Query<ServerUploadFileQuery>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> std::result::Result<Json<ServerFilesystemMutationResponse>, (StatusCode, Json<ApiErrorResponse>)>
{
    let config = state.config_rx.borrow().clone();

    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &query.uuid, &query.panel_version)?;
    let server = load_managed_server(&state.server_registry, &config, server_id)?;
    let target_path = query.path.unwrap_or_default();
    let file_name = query.name;
    let bytes = body.to_vec();

    tokio::task::spawn_blocking(move || upload_file(&server, &target_path, &file_name, &bytes))
        .await
        .context("failed to join file upload task")
        .map_err(internal_error)?
        .map_err(filesystem_error)?;

    Ok(Json(ServerFilesystemMutationResponse {
        message: "File uploaded successfully.".to_string(),
        ok: true,
    }))
}

#[derive(Debug, Deserialize)]
struct TransferRequest {
    action: String,
    transfer_id: u64,
    panel_version: String,
    uuid: String,
}

async fn handle_transfer(
    Path(server_id): Path<u64>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<TransferRequest>,
) -> std::result::Result<Json<ApiSuccessResponse>, (StatusCode, Json<ApiErrorResponse>)> {
    let config = state.config_rx.borrow().clone();
    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &payload.uuid, &payload.panel_version)?;

    let _server = state
        .server_registry
        .get_server(server_id)
        .map_err(internal_error)?
        .ok_or_else(|| error_response(StatusCode::NOT_FOUND, "Server not found.".to_string()))?;

    match payload.action.as_str() {
        "archive" => {
            info!(server_id, transfer_id = payload.transfer_id, "starting transfer archive");

            let _ = state.server_registry.append_console_message(
                server_id,
                "system",
                "Transfer started. Archiving server files...",
            );

            let registry = state.server_registry.clone();
            let transfer_id = payload.transfer_id;
            let config = config.clone();
            let volume_path = managed_server_volume_path(server_id).map_err(internal_error)?;
            let backups_dir = project_root().map_err(internal_error)?.join("transfers");

            tokio::spawn(async move {
                let _ = std::fs::create_dir_all(&backups_dir);
                let archive_path = backups_dir.join(format!("transfer-{transfer_id}.tar.gz"));

                // Archive the server volume.
                let output = tokio::process::Command::new("tar")
                    .arg("czf")
                    .arg(&archive_path)
                    .arg("-C")
                    .arg(&volume_path)
                    .arg(".")
                    .output()
                    .await;

                match output {
                    Ok(out) if out.status.success() => {
                        let size = std::fs::metadata(&archive_path).map(|m| m.len()).unwrap_or(0);

                        let _ = registry.append_console_message(
                            server_id,
                            "system",
                            &format!("Archive complete ({} bytes). Ready for transfer.", size),
                        );

                        let _ = notify_transfer_status(
                            &config, server_id, transfer_id, "transferring", 50, size, None,
                        ).await;
                    }
                    _ => {
                        let _ = registry.append_console_message(
                            server_id,
                            "system",
                            "Transfer archive failed.",
                        );

                        let _ = notify_transfer_status(
                            &config, server_id, transfer_id, "failed", 0, 0,
                            Some("Failed to create transfer archive."),
                        ).await;
                    }
                }
            });
        }
        "cancel" => {
            info!(server_id, transfer_id = payload.transfer_id, "transfer cancelled");

            // Clean up the archive if it exists.
            let transfers_dir = project_root().map_err(internal_error)?.join("transfers");
            let archive_path = transfers_dir.join(format!("transfer-{}.tar.gz", payload.transfer_id));
            let _ = std::fs::remove_file(&archive_path);

            let _ = state.server_registry.append_console_message(
                server_id,
                "system",
                "Transfer has been cancelled.",
            );
        }
        _ => {
            return Err(error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("Unknown transfer action: {}", payload.action),
            ));
        }
    }

    Ok(Json(ApiSuccessResponse { ok: true }))
}

async fn notify_transfer_status(
    config: &DaemonConfig,
    server_id: u64,
    transfer_id: u64,
    status: &str,
    progress: u8,
    archive_size: u64,
    error: Option<&str>,
) -> Result<()> {
    let panel_url = config.panel.url.trim_end_matches('/');
    let daemon_secret = config.panel.daemon_secret.as_deref().context("missing daemon secret")?;

    reqwest::Client::new()
        .post(format!("{panel_url}/api/daemon/servers/{server_id}/runtime"))
        .bearer_auth(daemon_secret)
        .json(&serde_json::json!({
            "uuid": config.daemon.uuid,
            "version": env!("CARGO_PKG_VERSION"),
            "status": null,
            "transfer_id": transfer_id,
            "transfer_status": status,
            "transfer_progress": progress,
            "transfer_archive_size": archive_size,
            "transfer_error": error,
        }))
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    Ok(())
}

async fn power_server(
    Path(server_id): Path<u64>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<ServerPowerRequest>,
) -> std::result::Result<Json<ApiSuccessResponse>, (StatusCode, Json<ApiErrorResponse>)> {
    let config = state.config_rx.borrow().clone();

    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &payload.uuid, &payload.panel_version)?;

    let mut server = load_managed_server(&state.server_registry, &config, server_id)?;

    apply_power_signal(&state.server_registry, &server, payload.signal)
        .await
        .map_err(internal_error)?;

    server = load_managed_server(&state.server_registry, &config, server_id)?;
    info!(server_id = server.id, status = %server.status, "power action processed");

    Ok(Json(ApiSuccessResponse { ok: true }))
}


async fn download_install_log(
    Path(server_id): Path<u64>,
    Query(query): Query<WsAuthorizationQuery>,
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> std::result::Result<Response, (StatusCode, Json<ApiErrorResponse>)> {
    let config = state.config_rx.borrow().clone();

    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &query.uuid, &query.panel_version)?;
    let server = load_managed_server(&state.server_registry, &config, server_id)?;
    let path = resolve_volume_path(&server)
        .map_err(internal_error)?
        .join("install.log");

    let contents = std::fs::read(&path).map_err(|error| {
        error_response(
            StatusCode::NOT_FOUND,
            format!("failed to read install log: {error}"),
        )
    })?;

    let mut response = contents.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!(
            "attachment; filename=server-{server_id}-install.log"
        ))
        .map_err(|error| internal_error(error.into()))?,
    );

    Ok(response)
}

async fn handle_synced_server_update(
    registry: &ServerRegistry,
    existing: Option<&ManagedServerRecord>,
    server: &ManagedServerRecord,
) -> Result<()> {
    let Some(existing) = existing else {
        return Ok(());
    };

    if !server_requires_recreate(existing, server) {
        return Ok(());
    }

    if existing.status == "offline" {
        remove_runtime_and_install_containers(server.id).await?;
        registry.set_server_runtime(server.id, "offline", None, existing.last_error.as_deref())?;
        registry.append_console_message(
            server.id,
            "system",
            "Server settings were updated while offline. The container was removed and the new settings will apply on the next start.",
        )?;

        return Ok(());
    }

    registry.append_console_message(
        server.id,
        "system",
        "Server settings were updated and queued. They will apply on the next restart or start.",
    )?;

    Ok(())
}

fn server_requires_recreate(existing: &ManagedServerRecord, server: &ManagedServerRecord) -> bool {
    existing.limits != server.limits
        || existing.allocation != server.allocation
        || existing.docker_image != server.docker_image
}

pub(super) async fn current_resource_snapshot(server: &ManagedServerRecord) -> Result<Value> {
    let name = runtime_container_name(server.id);
    let disk_bytes = current_disk_usage_bytes(server).await.unwrap_or(0);
    let output = docker_command()
        .arg("stats")
        .arg("--no-stream")
        .arg("--format")
        .arg("{{json .}}")
        .arg(&name)
        .output()
        .await
        .context("failed to inspect docker stats")?;

    if !output.status.success() {
        return Ok(json!({
            "disk_bytes": disk_bytes,
            "server": server.id,
            "running": false,
            "state": server.status,
        }));
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if stdout.is_empty() {
        return Ok(json!({
            "disk_bytes": disk_bytes,
            "server": server.id,
            "running": false,
            "state": server.status,
        }));
    }

    let stats: Value =
        serde_json::from_str(&stdout).context("failed to parse docker stats payload")?;

    Ok(json!({
        "disk_bytes": disk_bytes,
        "server": server.id,
        "running": true,
        "state": server.status,
        "stats": stats,
    }))
}

pub(super) async fn apply_power_signal(
    registry: &ServerRegistry,
    server: &ManagedServerRecord,
    signal: PowerSignal,
) -> Result<()> {
    match signal {
        PowerSignal::Start => {
            let is_running = container_is_running(server.id).await?;
            registry.set_server_runtime(
                server.id,
                runtime_status_after_start_request(is_running),
                None,
                None,
            )?;

            if is_running {
                registry.append_console_message(
                    server.id,
                    "system",
                    "Start requested, but the server is already running.",
                )?;
            }
        }
        PowerSignal::Stop => {
            if !container_is_running(server.id).await? {
                registry.set_server_runtime(server.id, "offline", None, None)?;
                registry.append_console_message(
                    server.id,
                    "system",
                    "Stop requested, but the server was already offline.",
                )?;
            } else {
                registry.set_server_runtime(server.id, "stopping", None, None)?;
                registry.append_console_message(
                    server.id,
                    "system",
                    "Stop requested. The server is shutting down.",
                )?;

                let registry = registry.clone();
                let server = server.clone();
                tokio::spawn(async move {
                    if let Err(error) = graceful_stop(&registry, &server, "offline").await {
                        let _ = registry.set_server_runtime(
                            server.id,
                            "running",
                            None,
                            Some(&error.to_string()),
                        );
                        let _ = registry.append_console_message(
                            server.id,
                            "system",
                            &format!("Stop failed: {error}"),
                        );
                    }
                });
            }
        }
        PowerSignal::Restart => {
            if !container_is_running(server.id).await? {
                registry.set_server_runtime(server.id, "starting", None, None)?;
                registry.append_console_message(
                    server.id,
                    "system",
                    "Restart requested while offline. The server will be started by the lifecycle worker.",
                )?;
            } else {
                registry.set_server_runtime(server.id, "restarting", None, None)?;
                registry.append_console_message(
                    server.id,
                    "system",
                    "Restart requested. The server is shutting down before restart.",
                )?;

                let registry = registry.clone();
                let server = server.clone();
                tokio::spawn(async move {
                    if let Err(error) = graceful_stop(&registry, &server, "restarting").await {
                        let _ = registry.set_server_runtime(
                            server.id,
                            "running",
                            None,
                            Some(&error.to_string()),
                        );
                        let _ = registry.append_console_message(
                            server.id,
                            "system",
                            &format!("Restart failed: {error}"),
                        );
                    }
                });
            }
        }
        PowerSignal::Kill => {
            kill_server(registry, server).await?;
        }
        PowerSignal::Reinstall => {
            remove_runtime_and_install_containers(server.id).await?;
            clear_server_volume(server).await?;
            registry.set_server_runtime(server.id, "pending", None, None)?;
            registry.append_console_message(
                server.id,
                "system",
                "Reinstall requested. Existing files were cleared and the lifecycle worker will reinstall the server.",
            )?;
        }
    }

    Ok(())
}

pub async fn send_command_to_server(
    _registry: &ServerRegistry,
    server: &ManagedServerRecord,
    command: &str,
) -> Result<()> {
    if !container_is_running(server.id).await? {
        bail!("The server is not running.");
    }

    // Use `docker exec -i` and pipe the command through stdin so it reaches
    // PID 1's fd/0 reliably. The previous approach used a shell script that
    // wrote to /proc/pid/fd/0 directly, but this breaks after a daemon
    // reboot because the original stdin pipe from `docker run -i` is dead.
    let mut child = docker_command()
        .arg("exec")
        .arg("-i")
        .arg(runtime_container_name(server.id))
        .arg("sh")
        .arg("-c")
        .arg("cat > /proc/1/fd/0")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context("failed to exec into the runtime container")?;

    if let Some(mut stdin) = child.stdin.take() {
        use tokio::io::AsyncWriteExt;
        let _ = stdin.write_all(format!("{command}\n").as_bytes()).await;
        drop(stdin);
    }

    let output = child
        .wait_with_output()
        .await
        .context("failed to forward command to the runtime container")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();

        if stderr.is_empty() {
            bail!("Failed to send the command to the server process.");
        }

        bail!("Failed to send the command to the server process: {stderr}");
    }

    Ok(())
}

async fn graceful_stop(
    registry: &ServerRegistry,
    server: &ManagedServerRecord,
    final_status: &str,
) -> Result<()> {
    if !container_is_running(server.id).await? {
        registry.set_server_runtime(server.id, "offline", None, None)?;
        registry.append_console_message(
            server.id,
            "system",
            "Stop requested, but the server was already offline.",
        )?;
        return Ok(());
    }

    let stop_command = server.cargo.config_stop.trim();
    let mut graceful_command_sent = false;

    if !stop_command.is_empty() {
        match send_command_to_server(registry, server, stop_command).await {
            Ok(()) => {
                graceful_command_sent = true;
            }
            Err(error) => {
                registry.append_console_message(
                    server.id,
                    "system",
                    &format!(
                        "Failed to send graceful stop command. Falling back to docker stop: {error}"
                    ),
                )?;
            }
        }
    }

    if graceful_command_sent {
        for _ in 0..15 {
            if !container_is_running(server.id).await? {
                remove_runtime_and_install_containers(server.id).await?;
                registry.set_server_runtime(server.id, final_status, None, None)?;
                registry.append_console_message(
                    server.id,
                    "system",
                    stop_completion_message(final_status),
                )?;
                return Ok(());
            }

            time::sleep(Duration::from_secs(1)).await;
        }
    }

    let output = docker_command()
        .arg("stop")
        .arg("-t")
        .arg("10")
        .arg(runtime_container_name(server.id))
        .output()
        .await
        .context("failed to issue docker stop")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if !stderr.is_empty() {
            bail!(stderr);
        }
    }

    remove_runtime_and_install_containers(server.id).await?;
    registry.set_server_runtime(server.id, final_status, None, None)?;
    registry.append_console_message(server.id, "system", stop_completion_message(final_status))?;

    Ok(())
}

fn stop_completion_message(final_status: &str) -> &'static str {
    match final_status {
        "restarting" => "Server stopped successfully. Restart pending.",
        _ => "Server stopped successfully.",
    }
}

async fn kill_server(registry: &ServerRegistry, server: &ManagedServerRecord) -> Result<()> {
    if !container_is_running(server.id).await? {
        registry.set_server_runtime(server.id, "offline", None, None)?;
        registry.append_console_message(
            server.id,
            "system",
            "Kill requested, but the server was already offline.",
        )?;
        return Ok(());
    }

    let output = docker_command()
        .arg("kill")
        .arg(runtime_container_name(server.id))
        .output()
        .await
        .context("failed to issue docker kill")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if !stderr.is_empty() {
            bail!(stderr);
        }
    }

    remove_runtime_and_install_containers(server.id).await?;
    registry.set_server_runtime(server.id, "offline", None, None)?;
    registry.append_console_message(server.id, "system", "Server killed successfully.")?;

    Ok(())
}

async fn clear_server_volume(server: &ManagedServerRecord) -> Result<()> {
    let volume_path = resolve_volume_path(server)?;

    if tokio::fs::try_exists(&volume_path)
        .await
        .context("failed to inspect server volume path")?
    {
        tokio::fs::remove_dir_all(&volume_path)
            .await
            .with_context(|| format!("failed to remove {}", volume_path.display()))?;
    }

    tokio::fs::create_dir_all(&volume_path)
        .await
        .with_context(|| format!("failed to recreate {}", volume_path.display()))?;

    Ok(())
}

pub(super) fn load_managed_server(
    registry: &ServerRegistry,
    config: &DaemonConfig,
    server_id: u64,
) -> std::result::Result<ManagedServerRecord, (StatusCode, Json<ApiErrorResponse>)> {
    let configured_node_id = config.panel.node_id.ok_or_else(|| {
        error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "This daemon is not enrolled to a node yet.".to_string(),
        )
    })?;

    let server = registry
        .get_server(server_id)
        .map_err(internal_error)?
        .ok_or_else(|| {
            error_response(
                StatusCode::NOT_FOUND,
                "The requested server could not be found.".to_string(),
            )
        })?;

    if server.node_id != configured_node_id {
        return Err(error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "The server does not belong to this node.".to_string(),
        ));
    }

    Ok(server)
}

pub(super) fn ensure_compatible_request(
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

pub async fn container_is_running(server_id: u64) -> Result<bool> {
    let output = docker_command()
        .arg("inspect")
        .arg("-f")
        .arg("{{.State.Running}}")
        .arg(runtime_container_name(server_id))
        .output()
        .await
        .context("failed to inspect runtime container")?;

    if !output.status.success() {
        return Ok(false);
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim() == "true")
}

async fn remove_runtime_and_install_containers(server_id: u64) -> Result<()> {
    remove_container(&runtime_container_name(server_id)).await?;
    remove_container(&install_container_name(server_id)).await?;

    Ok(())
}

async fn remove_container(name: &str) -> Result<()> {
    let _ = docker_command()
        .arg("rm")
        .arg("-f")
        .arg(name)
        .output()
        .await;

    Ok(())
}

pub fn runtime_container_name(server_id: u64) -> String {
    format!("skyport-server-{server_id}")
}

fn install_container_name(server_id: u64) -> String {
    format!("skyport-install-{server_id}")
}

pub fn docker_command() -> Command {
    let mut command = Command::new("docker");
    command.kill_on_drop(true);
    command
}

pub(super) fn resolve_volume_path(server: &ManagedServerRecord) -> Result<PathBuf> {
    managed_server_volume_path(server.id)
}

async fn current_disk_usage_bytes(server: &ManagedServerRecord) -> Result<u64> {
    let volume_path = resolve_volume_path(server)?;

    tokio::task::spawn_blocking(move || directory_size(&volume_path))
        .await
        .context("failed to join disk usage task")?
}

fn directory_size(path: &StdPath) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }

    let mut total = 0_u64;

    for entry in
        std::fs::read_dir(path).with_context(|| format!("failed to read {}", path.display()))?
    {
        let entry = entry?;
        let metadata = entry.metadata()?;

        if metadata.is_dir() {
            total += directory_size(&entry.path())?;
        } else {
            total += metadata.len();
        }
    }

    Ok(total)
}

fn internal_error(error: anyhow::Error) -> (StatusCode, Json<ApiErrorResponse>) {
    error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

fn filesystem_error(error: anyhow::Error) -> (StatusCode, Json<ApiErrorResponse>) {
    error_response(StatusCode::UNPROCESSABLE_ENTITY, error.to_string())
}

fn error_response(status: StatusCode, message: String) -> (StatusCode, Json<ApiErrorResponse>) {
    (status, Json(ApiErrorResponse { message }))
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use super::websocket::{authenticate_ws_event, console_event_name, hex_encode, parse_power_signal, parse_ws_client_event, runtime_status_after_start_request, ws_event_payload};

    #[test]
    fn install_console_messages_use_install_output_events() {
        let message = ConsoleMessageRecord {
            id: 1,
            server_id: 42,
            source: "install".to_string(),
            message: "Downloading files...".to_string(),
            created_at: "2026-04-06T00:00:00Z".to_string(),
        };

        let payload = ws_event_payload(
            console_event_name(&message.source),
            vec![Value::String(message.message.clone())],
        );

        expect_json(
            &payload,
            json!({
                "event": "install output",
                "args": ["Downloading files..."],
            }),
        );
    }

    #[test]
    fn non_install_console_messages_use_console_output_events() {
        let payload = ws_event_payload(
            console_event_name("stdout"),
            vec![Value::String("Server started".to_string())],
        );

        expect_json(
            &payload,
            json!({
                "event": "console output",
                "args": ["Server started"],
            }),
        );
    }

    #[test]
    fn system_console_messages_use_daemon_message_events() {
        let payload = ws_event_payload(
            console_event_name("system"),
            vec![Value::String("Pulling runtime image...".to_string())],
        );

        expect_json(
            &payload,
            json!({
                "event": "daemon message",
                "args": ["Pulling runtime image..."],
            }),
        );
    }

    #[test]
    fn status_events_use_wings_style_envelope() {
        let payload = ws_event_payload("status", vec![Value::String("running".to_string())]);

        expect_json(
            &payload,
            json!({
                "event": "status",
                "args": ["running"],
            }),
        );
    }

    #[test]
    fn websocket_client_payloads_parse_like_wings_events() {
        let event = parse_ws_client_event(r#"{"event":"set state","args":["restart"]}"#)
            .expect("valid websocket payload");

        assert_eq!(event.event, "set state");
        assert_eq!(event.args, vec!["restart"]);
    }

    #[test]
    fn directory_size_returns_zero_for_missing_paths() {
        let missing =
            std::env::temp_dir().join(format!("skyportd-api-missing-{}", current_unix_timestamp()));

        assert_eq!(directory_size(&missing).expect("directory size"), 0);
    }

    #[test]
    fn resolve_volume_path_uses_server_id_instead_of_payload_path() {
        let mut server = sample_managed_server();
        server.volume_path = "../../etc".to_string();

        let path = resolve_volume_path(&server).expect("volume path should resolve");

        assert!(path.ends_with("volumes/1"));
    }

    #[test]
    fn auth_event_requires_matching_token() {
        let payload_hex = hex_encode(
            serde_json::to_string(&WsTokenPayload {
                daemon_uuid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
                exp: current_unix_timestamp() + 60,
                panel_version: CURRENT_VERSION.to_string(),
                server_id: 42,
            })
            .expect("token payload json")
            .as_bytes(),
        );
        let signature = hex_encode(
            hmac::sign(
                &hmac::Key::new(hmac::HMAC_SHA256, b"callback-token"),
                payload_hex.as_bytes(),
            )
            .as_ref(),
        );
        let event = WsClientEvent {
            event: "auth".to_string(),
            args: vec![format!("{payload_hex}.{signature}")],
        };

        assert!(
            authenticate_ws_event(
                Some("callback-token"),
                &event,
                42,
                "550e8400-e29b-41d4-a716-446655440000",
                CURRENT_VERSION,
            )
            .is_ok()
        );
        assert!(
            authenticate_ws_event(
                Some("other-token"),
                &event,
                42,
                "550e8400-e29b-41d4-a716-446655440000",
                CURRENT_VERSION,
            )
            .is_err()
        );
    }

    #[test]
    fn set_state_accepts_only_supported_power_actions() {
        assert!(matches!(
            parse_power_signal(Some(&"start".to_string())).expect("start accepted"),
            PowerSignal::Start
        ));
        assert!(matches!(
            parse_power_signal(Some(&"kill".to_string())).expect("kill accepted"),
            PowerSignal::Kill
        ));
        assert!(matches!(
            parse_power_signal(Some(&"reinstall".to_string())).expect("reinstall accepted"),
            PowerSignal::Reinstall
        ));
    }

    #[test]
    fn start_requests_keep_running_servers_in_running_state() {
        assert_eq!(runtime_status_after_start_request(true), "running");
        assert_eq!(runtime_status_after_start_request(false), "starting");
    }

    #[test]
    fn synced_server_recreate_detection_tracks_limits_allocations_and_docker_image() {
        let existing = sample_managed_server();
        let mut updated_limits = sample_managed_server();
        updated_limits.limits.memory_mib = 2048;
        let mut updated_allocation = sample_managed_server();
        updated_allocation.allocation.port = 25566;
        let mut updated_docker_image = sample_managed_server();
        updated_docker_image.docker_image = Some("ghcr.io/skyportsh/yolks:java_21".to_string());

        assert!(server_requires_recreate(&existing, &updated_limits));
        assert!(server_requires_recreate(&existing, &updated_allocation));
        assert!(server_requires_recreate(&existing, &updated_docker_image));
        assert!(!server_requires_recreate(
            &existing,
            &sample_managed_server()
        ));
    }

    #[test]
    fn daemon_console_text_omits_start_and_graceful_stop_noise() {
        let source = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/api/mod.rs"));
        let removed_start_message = [
            "Start requested.",
            " The server will be booted by the lifecycle worker.",
        ]
        .concat();
        let removed_stop_message = ["Sending graceful stop", " command..."].concat();

        assert!(!source.contains(&removed_start_message));
        assert!(!source.contains(&removed_stop_message));
    }

    #[cfg(unix)]
    #[test]
    fn existing_filesystem_paths_reject_symlink_escapes() {
        use std::os::unix::fs::symlink;

        let tempdir = tempdir().unwrap();
        let volume = tempdir.path().join("volume");
        let outside = tempdir.path().join("outside");
        std::fs::create_dir_all(&volume).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        symlink(&outside, volume.join("escape")).unwrap();

        let error = resolve_existing_server_path(&volume, "escape")
            .expect_err("symlink escape should be rejected")
            .to_string();

        assert!(error.contains("within the server volume"));
    }

    #[cfg(unix)]
    #[test]
    fn write_paths_reject_symlink_escapes() {
        use std::os::unix::fs::symlink;

        let tempdir = tempdir().unwrap();
        let volume = tempdir.path().join("volume");
        let outside = tempdir.path().join("outside.txt");
        std::fs::create_dir_all(&volume).unwrap();
        std::fs::write(&outside, "secret").unwrap();
        symlink(&outside, volume.join("escape.txt")).unwrap();

        let error = resolve_server_path_for_write(&volume, "escape.txt")
            .expect_err("symlink escape should be rejected")
            .to_string();

        assert!(error.contains("within the server volume"));
    }

    #[test]
    fn transfer_targets_reject_directory_descendants() {
        let tempdir = tempdir().unwrap();
        let volume = tempdir.path().join("volume");
        std::fs::create_dir_all(volume.join("plugins/backups")).unwrap();
        let canonical_volume = std::fs::canonicalize(&volume).unwrap();
        let destination = std::fs::canonicalize(volume.join("plugins/backups")).unwrap();

        let error = transfer_targets(&canonical_volume, &["plugins".to_string()], &destination)
            .expect_err("directory descendants should be rejected")
            .to_string();

        assert!(error.contains("cannot be moved or copied into itself"));
    }

    #[test]
    fn permission_modes_accept_three_digit_octal_values() {
        assert_eq!(normalize_permissions("755").unwrap(), 0o755);
        assert_eq!(normalize_permissions("0644").unwrap(), 0o644);
        assert!(normalize_permissions("999").is_err());
    }

    #[test]
    fn archive_entries_require_safe_relative_paths() {
        let error = extract_top_level_entries(&[
            "plugins/WorldEdit.jar".to_string(),
            "../../etc/passwd".to_string(),
        ])
        .expect_err("unsafe archive entries should be rejected")
        .to_string();

        assert!(error.contains("within the server volume"));
    }

    #[test]
    fn create_archive_creates_zip_from_selected_paths() {
        let server_id = current_unix_timestamp();
        let mut server = sample_managed_server();
        server.id = server_id;

        let volume_path = resolve_volume_path(&server).expect("volume path should resolve");
        let _ = std::fs::remove_dir_all(&volume_path);
        std::fs::create_dir_all(volume_path.join("plugins")).unwrap();
        std::fs::write(volume_path.join("server.properties"), "motd=Hello").unwrap();
        std::fs::write(volume_path.join("plugins/plugin.jar"), "jar").unwrap();

        create_archive(
            &server,
            &["server.properties".to_string(), "plugins".to_string()],
            "",
            "backup.zip",
        )
        .expect("archive should be created");

        let archive_path = volume_path.join("backup.zip");
        assert!(archive_path.exists());

        let entries = list_archive_entries(&archive_path).expect("archive should be readable");
        assert!(entries.iter().any(|entry| entry == "server.properties"));
        assert!(entries.iter().any(|entry| entry == "plugins/plugin.jar"));

        let _ = std::fs::remove_dir_all(&volume_path);
    }

    fn current_unix_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("valid system time")
            .as_secs()
    }

    fn sample_managed_server() -> ManagedServerRecord {
        ManagedServerRecord {
            id: 1,
            node_id: 1,
            name: "Paper".to_string(),
            status: "offline".to_string(),
            volume_path: "volumes/1".to_string(),
            created_at: "2026-04-06T00:00:00Z".to_string(),
            updated_at: "2026-04-06T00:00:00Z".to_string(),
            docker_image: None,
            allocation: ManagedServerAllocation {
                id: 1,
                bind_ip: "0.0.0.0".to_string(),
                port: 25565,
                ip_alias: None,
            },
            firewall_rules: vec![],
            interconnects: vec![],
            workflows: vec![],
            container_id: None,
            last_error: None,
            user: ManagedServerUser {
                id: 1,
                name: "Test".to_string(),
                email: "test@example.com".to_string(),
            },
            limits: ManagedServerLimits {
                memory_mib: 1024,
                cpu_limit: 100,
                disk_mib: 10240,
            },
            cargo: ManagedServerCargo {
                id: 1,
                name: "Paper".to_string(),
                slug: "paper".to_string(),
                source_type: "pterodactyl".to_string(),
                startup_command: "java -jar server.jar".to_string(),
                config_files: "{}".to_string(),
                config_startup: "{}".to_string(),
                config_logs: "{}".to_string(),
                config_stop: "stop".to_string(),
                install_script: None,
                install_container: None,
                install_entrypoint: None,
                features: vec![],
                docker_images: BTreeMap::new(),
                file_denylist: vec![],
                file_hidden_list: vec![],
                variables: vec![],
                definition: Value::Null,
            },
        }
    }

    fn expect_json(actual: &str, expected: Value) {
        let parsed: Value = serde_json::from_str(actual).expect("valid websocket json");

        assert_eq!(parsed, expected);
    }
}
