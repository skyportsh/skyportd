mod filesystem;
mod backups;
mod websocket;

use filesystem::*;
use backups::*;
use websocket::*;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path as StdPath, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use axum_server::Handle;
use axum_server::tls_rustls::RustlsConfig;
#[cfg(test)]
use ring::hmac;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{DaemonConfig, NodeSection, managed_server_volume_path, project_root};
use crate::configuration;
use crate::docker::{DockerOutput, docker_command};
#[cfg(test)]
use crate::server_registry::ConsoleMessageRecord;
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
pub(super) struct WsTokenPayload {
    daemon_uuid: String,
    exp: u64,
    panel_version: String,
    #[serde(default)]
    permissions: Vec<String>,
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
    #[serde(default)]
    requires_privileged: bool,
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
            requires_privileged: payload.server.cargo.requires_privileged,
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

    if let Err(error) = remove_runtime_and_install_containers(server_id).await {
        warn!(
            server_id,
            error = %error,
            "failed to remove containers during server deletion"
        );
    }

    forget_resource_snapshot(server_id);

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

/// Maximum size of a file accepted by the upload endpoint.
const MAX_UPLOAD_BYTES: u64 = 1024 * 1024 * 1024;

async fn upload_server_file(
    Path(server_id): Path<u64>,
    Query(query): Query<ServerUploadFileQuery>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> std::result::Result<Json<ServerFilesystemMutationResponse>, (StatusCode, Json<ApiErrorResponse>)>
{
    let config = state.config_rx.borrow().clone();

    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &query.uuid, &query.panel_version)?;
    let server = load_managed_server(&state.server_registry, &config, server_id)?;
    let target_path = query.path.unwrap_or_default();
    let file_name = query.name;

    let final_path = tokio::task::spawn_blocking(move || {
        prepare_upload_target(&server, &target_path, &file_name)
    })
    .await
    .context("failed to join file upload task")
    .map_err(internal_error)?
    .map_err(filesystem_error)?;

    // Stream into a hidden temporary file so memory stays flat whatever the
    // upload size, and so a failed transfer never leaves a truncated file
    // under its real name.
    let temp_path = final_path.with_file_name(format!(
        ".{}.{}.upload",
        final_path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));

    let result = stream_body_to_file(body, &temp_path, MAX_UPLOAD_BYTES).await;

    let result = match result {
        Ok(()) if final_path.exists() => Err(anyhow::anyhow!("A file with that name already exists.")),
        Ok(()) => tokio::fs::rename(&temp_path, &final_path)
            .await
            .with_context(|| format!("failed to write {}", final_path.display())),
        Err(error) => Err(error),
    };

    if let Err(error) = result {
        let _ = tokio::fs::remove_file(&temp_path).await;

        return Err(filesystem_error(error));
    }

    Ok(Json(ServerFilesystemMutationResponse {
        message: "File uploaded successfully.".to_string(),
        ok: true,
    }))
}

async fn stream_body_to_file(
    body: axum::body::Body,
    path: &StdPath,
    max_bytes: u64,
) -> Result<()> {
    use futures_util::StreamExt;
    use tokio::io::AsyncWriteExt;

    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
        .with_context(|| format!("failed to create {}", path.display()))?;
    let mut stream = body.into_data_stream();
    let mut written = 0_u64;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("failed to read the upload body")?;

        written += chunk.len() as u64;

        if written > max_bytes {
            bail!("Uploaded files may not be larger than 1 GB.");
        }

        file.write_all(&chunk)
            .await
            .with_context(|| format!("failed to write {}", path.display()))?;
    }

    file.flush().await?;

    Ok(())
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

    crate::http::client()
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
    let volume_path = resolve_volume_path(&server).map_err(internal_error)?;
    let file = crate::volume_fs::open(
        &volume_path,
        StdPath::new("install.log"),
        crate::volume_fs::OpenMode::read(),
    )
    .map(tokio::fs::File::from_std)
    .map_err(|error| {
        error_response(
            StatusCode::NOT_FOUND,
            format!("failed to read install log: {error}"),
        )
    })?;

    // Streamed so a large log never has to fit in memory.
    let mut response = axum::body::Body::from_stream(tokio_util::io::ReaderStream::new(file))
        .into_response();
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

/// A stats sample older than this is not trusted.
const STATS_SAMPLE_MAX_AGE: Duration = Duration::from_secs(5);
/// How long a caller waits for the first sample of a fresh collector.
const STATS_FIRST_SAMPLE_WAIT: Duration = Duration::from_secs(4);
/// A collector stops when nobody asked for its numbers for this long.
const STATS_COLLECTOR_IDLE: Duration = Duration::from_secs(20);
/// Minimum delay before restarting a collector that just died, so a stopped
/// container does not spawn a failing `docker stats` on every request.
const STATS_COLLECTOR_RESTART_DELAY: Duration = Duration::from_secs(2);
/// Disk usage is recomputed in the background once it is older than this.
/// Walking a large volume is expensive, so readers never wait for it after
/// the first computation.
const DISK_CACHE_TTL: Duration = Duration::from_secs(180);

/// State shared between a stats collector task and the requests reading it.
struct StatsCollector {
    state: std::sync::Mutex<CollectorState>,
    updated: tokio::sync::Notify,
}

struct CollectorState {
    /// `Some(None)` is a sample saying the container is not running.
    sample: Option<(Instant, Option<Value>)>,
    last_read: Instant,
    alive: bool,
    died_at: Option<Instant>,
}

impl StatsCollector {
    fn lock(&self) -> std::sync::MutexGuard<'_, CollectorState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Default)]
struct DiskCache {
    value: std::sync::Mutex<Option<(Instant, u64)>>,
    refreshing: std::sync::atomic::AtomicBool,
}

#[derive(Default)]
struct ResourceEntry {
    collector: Option<Arc<StatsCollector>>,
    disk: Arc<DiskCache>,
}

static RESOURCE_CACHE: LazyLock<std::sync::Mutex<HashMap<u64, ResourceEntry>>> =
    LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

/// Drops the cached state of a server that no longer exists. Its collector
/// stops by itself once it has been idle.
pub(crate) fn forget_resource_snapshot(server_id: u64) {
    RESOURCE_CACHE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&server_id);
}

/// Returns the running collector of a server, starting one if needed.
fn collector_for(server_id: u64) -> Option<Arc<StatsCollector>> {
    let mut cache = RESOURCE_CACHE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let entry = cache.entry(server_id).or_default();

    if let Some(collector) = &entry.collector {
        let mut state = collector.lock();

        if state.alive {
            state.last_read = Instant::now();
            return Some(Arc::clone(collector));
        }

        if state
            .died_at
            .is_some_and(|at| at.elapsed() < STATS_COLLECTOR_RESTART_DELAY)
        {
            return None;
        }
    }

    let collector = Arc::new(StatsCollector {
        state: std::sync::Mutex::new(CollectorState {
            sample: None,
            last_read: Instant::now(),
            alive: true,
            died_at: None,
        }),
        updated: tokio::sync::Notify::new(),
    });

    entry.collector = Some(Arc::clone(&collector));
    tokio::spawn(run_stats_collector(server_id, Arc::clone(&collector)));

    Some(collector)
}

/// Keeps ONE persistent `docker stats` process per watched server. It prints a
/// sample every second, so any number of clients cost a single process and no
/// process is spawned per sample.
async fn run_stats_collector(server_id: u64, collector: Arc<StatsCollector>) {
    use tokio::io::{AsyncBufReadExt, BufReader};

    let spawned = docker_command()
        .arg("stats")
        .arg("--format")
        .arg("{{json .}}")
        .arg(runtime_container_name(server_id))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn();

    if let Ok(mut child) = spawned {
        if let Some(stdout) = child.stdout.take() {
            let mut lines = BufReader::new(stdout).lines();
            let mut idle_check = time::interval(Duration::from_secs(1));

            loop {
                tokio::select! {
                    line = lines.next_line() => {
                        let Ok(Some(line)) = line else { break };

                        // The client redraws the terminal with escape codes
                        // around the JSON object.
                        let (Some(start), Some(end)) = (line.find('{'), line.rfind('}')) else {
                            continue;
                        };

                        if let Ok(stats) = serde_json::from_str::<Value>(&line[start..=end]) {
                            // A stopped or removed container keeps printing
                            // placeholders ("--") or all zeros.
                            let running = !stats["MemUsage"]
                                .as_str()
                                .is_some_and(|usage| usage.starts_with("--") || usage.starts_with("0B /"));

                            collector.lock().sample =
                                Some((Instant::now(), running.then_some(stats)));
                            collector.updated.notify_waiters();
                        }
                    }
                    _ = idle_check.tick() => {
                        if collector.lock().last_read.elapsed() > STATS_COLLECTOR_IDLE {
                            break;
                        }
                    }
                }
            }
        }

        // `kill_on_drop` ends the process when `child` goes out of scope.
    }

    {
        let mut state = collector.lock();
        state.alive = false;
        state.died_at = Some(Instant::now());
        state.sample = None;
    }
    collector.updated.notify_waiters();
}

/// Latest stats of a server. `None` means the container is not running.
async fn latest_container_stats(server_id: u64) -> Option<Value> {
    let collector = collector_for(server_id)?;
    let deadline = Instant::now() + STATS_FIRST_SAMPLE_WAIT;

    loop {
        // Register for the notification before reading to not miss it.
        let notified = collector.updated.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        {
            let state = collector.lock();

            if let Some((at, stats)) = &state.sample {
                if at.elapsed() < STATS_SAMPLE_MAX_AGE {
                    return stats.clone();
                }
            }

            if !state.alive {
                return None;
            }
        }

        let remaining = deadline.saturating_duration_since(Instant::now());

        if remaining.is_zero() || tokio::time::timeout(remaining, notified).await.is_err() {
            return None;
        }
    }
}

/// Disk usage of a volume. The first call computes it; later calls return the
/// cached value at once and refresh it in the background when it is stale.
async fn cached_disk_usage(server: &ManagedServerRecord) -> u64 {
    use std::sync::atomic::Ordering;

    let cache = {
        let mut cache = RESOURCE_CACHE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        Arc::clone(&cache.entry(server.id).or_default().disk)
    };

    let cached = *cache
        .value
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    match cached {
        None => {
            let bytes = current_disk_usage_bytes(server).await.unwrap_or(0);
            *cache.value.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) =
                Some((Instant::now(), bytes));
            bytes
        }
        Some((at, bytes)) => {
            if at.elapsed() >= DISK_CACHE_TTL && !cache.refreshing.swap(true, Ordering::AcqRel) {
                let server = server.clone();

                tokio::spawn(async move {
                    let bytes = current_disk_usage_bytes(&server).await.unwrap_or(0);
                    *cache.value.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) =
                        Some((Instant::now(), bytes));
                    cache.refreshing.store(false, Ordering::Release);
                });
            }

            bytes
        }
    }
}

pub(super) async fn current_resource_snapshot(server: &ManagedServerRecord) -> Result<Value> {
    let (stats, disk_bytes) =
        tokio::join!(latest_container_stats(server.id), cached_disk_usage(server));

    Ok(match stats {
        Some(stats) => json!({
            "disk_bytes": disk_bytes,
            "server": server.id,
            "running": true,
            "state": server.status,
            "stats": stats,
        }),
        None => json!({
            "disk_bytes": disk_bytes,
            "server": server.id,
            "running": false,
            "state": server.status,
        }),
    })
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
                if server.status != "offline" {
                    registry.set_server_runtime(server.id, "offline", None, None)?;
                    registry.append_console_message(
                        server.id,
                        "system",
                        "Stop requested, but the server was already offline.",
                    )?;
                }
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
        if server.status != "offline" {
            registry.set_server_runtime(server.id, "offline", None, None)?;
            registry.append_console_message(
                server.id,
                "system",
                "Stop requested, but the server was already offline.",
            )?;
        }
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
        .docker_output()
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
        // Only set offline if not already offline to avoid spamming console
        // when the panel sends repeated kill signals.
        if server.status != "offline" {
            registry.set_server_runtime(server.id, "offline", None, None)?;
            registry.append_console_message(
                server.id,
                "system",
                "Kill requested, but the server was already offline.",
            )?;
        }
        return Ok(());
    }

    let output = docker_command()
        .arg("kill")
        .arg(runtime_container_name(server.id))
        .docker_output()
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
        .docker_output()
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
        .docker_output()
        .await;

    Ok(())
}

pub fn runtime_container_name(server_id: u64) -> String {
    format!("skyport-server-{server_id}")
}

fn install_container_name(server_id: u64) -> String {
    format!("skyport-install-{server_id}")
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
    use axum::body::Body;
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use super::websocket::{authenticate_ws_event, console_event_name, hex_encode, parse_power_signal, parse_ws_client_event, runtime_status_after_start_request, ws_event_payload};

    fn ws_test_config(node_id: u64) -> DaemonConfig {
        DaemonConfig {
            daemon: crate::config::DaemonSection {
                name: "skyportd".to_string(),
                uuid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
                tick_interval: Duration::from_secs(30),
                shutdown_timeout: Duration::from_secs(30),
                allow_private_webhooks: false,
            },
            panel: crate::config::PanelSection {
                url: "http://localhost".to_string(),
                configuration_token: None,
                daemon_secret: None,
                daemon_callback_token: Some("ws-secret".to_string()),
                node_id: Some(node_id),
            },
            logging: crate::config::LoggingSection {
                level: "info".to_string(),
                format: crate::config::LogFormat::Pretty,
            },
            runtime: crate::config::RuntimeSection { worker_threads: 1 },
            node: None,
        }
    }

    fn ws_token(config: &DaemonConfig, server_id: u64) -> String {
        let payload = WsTokenPayload {
            daemon_uuid: config.daemon.uuid.clone(),
            exp: current_unix_timestamp() + 600,
            panel_version: CURRENT_VERSION.to_string(),
            permissions: vec!["*".to_string()],
            server_id,
        };
        let payload_hex = hex_encode(serde_json::to_string(&payload).unwrap().as_bytes());
        let signature = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, b"ws-secret"),
            payload_hex.as_bytes(),
        );

        format!("{payload_hex}.{}", hex_encode(signature.as_ref()))
    }

    /// Reads websocket events until `predicate` matches or `wait` elapses.
    async fn next_ws_event<S>(
        stream: &mut S,
        wait: Duration,
    ) -> Option<(String, Vec<Value>)>
    where
        S: futures_util::Stream<
                Item = std::result::Result<
                    tokio_tungstenite::tungstenite::Message,
                    tokio_tungstenite::tungstenite::Error,
                >,
            > + Unpin,
    {
        use futures_util::StreamExt;

        loop {
            let message = tokio::time::timeout(wait, stream.next()).await.ok()??.ok()?;

            if let tokio_tungstenite::tungstenite::Message::Text(text) = message {
                let value: Value = serde_json::from_str(text.as_str()).unwrap();

                return Some((
                    value["event"].as_str().unwrap().to_string(),
                    value["args"].as_array().cloned().unwrap_or_default(),
                ));
            }
        }
    }

    #[tokio::test]
    async fn websocket_streams_live_console_without_replaying_history() {
        use futures_util::SinkExt;
        use tokio_tungstenite::tungstenite::Message;

        let directory = tempdir().unwrap();
        let registry = ServerRegistry::new(directory.path().join("registry.db"));
        registry.initialize().unwrap();
        let server = sample_managed_server();
        registry.upsert_server(&server).unwrap();

        // A long history that used to be replayed on every connection.
        for index in 0..300 {
            registry
                .append_console_message(server.id, "stdout", &format!("old {index}"))
                .unwrap();
        }

        let config = ws_test_config(server.node_id);
        let (config_tx, config_rx) = watch::channel(config.clone());
        let state = ApiState {
            config_rx,
            config_tx,
            server_registry: registry.clone(),
        };
        let router = Router::new()
            .route("/api/daemon/servers/{server_id}/ws", get(server_ws))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        let url = format!(
            "ws://{address}/api/daemon/servers/{}/ws?uuid={}&panel_version={CURRENT_VERSION}",
            server.id, config.daemon.uuid
        );
        let (mut socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();

        socket
            .send(Message::Text(
                json!({"event": "auth", "args": [ws_token(&config, server.id)]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();

        let (event, _) = next_ws_event(&mut socket, Duration::from_secs(2)).await.unwrap();
        assert_eq!(event, "auth success");

        // Only status events and no history: the next console event must be
        // the live line we append now.
        registry
            .append_console_message(server.id, "stdout", "live line")
            .unwrap();

        let mut saw_status = false;
        let begin = Instant::now();
        loop {
            let (event, args) = next_ws_event(&mut socket, Duration::from_secs(2))
                .await
                .expect("timed out waiting for the live console line");

            match event.as_str() {
                "status" => saw_status = true,
                "console output" => {
                    assert_eq!(args[0], "live line", "history was replayed on connect");
                    break;
                }
                other => panic!("unexpected event {other}"),
            }
        }
        assert!(saw_status);
        assert!(begin.elapsed() < Duration::from_millis(600), "live delivery was slow");

        // History is delivered on request, last 50 lines only.
        socket
            .send(Message::Text(json!({"event": "send logs", "args": []}).to_string().into()))
            .await
            .unwrap();

        let mut lines = Vec::new();
        while let Some((event, args)) = next_ws_event(&mut socket, Duration::from_millis(500)).await {
            if event == "console output" {
                lines.push(args[0].as_str().unwrap().to_string());
            }
        }
        assert_eq!(lines.len(), 50);
        assert_eq!(lines.last().unwrap(), "live line");

        // A burst larger than the broadcast buffer while the client is not
        // reading must still end with the newest line delivered.
        for index in 0..3000 {
            registry
                .append_console_message(server.id, "stdout", &format!("burst {index}"))
                .unwrap();
        }

        // Every hole in the stream must be announced by a skip notice.
        let mut last = String::new();
        let mut previous: Option<u64> = None;
        let mut gap_announced = true;
        while let Some((event, args)) = next_ws_event(&mut socket, Duration::from_secs(2)).await {
            match event.as_str() {
                "daemon message" => {
                    assert!(args[0].as_str().unwrap().contains("skipped"));
                    gap_announced = true;
                }
                "console output" => {
                    last = args[0].as_str().unwrap().to_string();

                    if let Some(index) = last.strip_prefix("burst ").and_then(|n| n.parse::<u64>().ok()) {
                        if let Some(previous) = previous {
                            if index != previous + 1 {
                                assert!(gap_announced, "silent gap between burst {previous} and {index}");
                            }
                        }
                        previous = Some(index);
                        gap_announced = false;
                    }
                }
                _ => {}
            }
        }
        assert_eq!(last, "burst 2999");

        // Status changes are pushed without polling.
        registry
            .set_server_runtime(server.id, "running", None, None)
            .unwrap();
        let (event, args) = next_ws_event(&mut socket, Duration::from_millis(500))
            .await
            .expect("status change was not pushed");
        assert_eq!(event, "status");
        assert_eq!(args[0], "running");

        let _ = socket.close(None).await;
    }

    #[tokio::test]
    async fn upload_stream_is_written_to_disk_in_chunks() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("upload.bin");
        let chunks = (0..64).map(|index| Ok::<_, std::io::Error>(vec![index as u8; 4096]));
        let body = Body::from_stream(futures_util::stream::iter(chunks));

        stream_body_to_file(body, &path, 1024 * 1024).await.unwrap();

        let written = std::fs::read(&path).unwrap();
        assert_eq!(written.len(), 64 * 4096);
        assert_eq!(written[4096], 1);
    }

    #[tokio::test]
    async fn upload_stream_rejects_oversized_bodies() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("too-big.bin");
        let chunks = (0..8).map(|_| Ok::<_, std::io::Error>(vec![0u8; 1024]));
        let body = Body::from_stream(futures_util::stream::iter(chunks));

        let error = stream_body_to_file(body, &path, 4096).await.unwrap_err();

        assert!(error.to_string().contains("larger than 1 GB"));
    }

    #[tokio::test]
    async fn upload_stream_refuses_to_overwrite_a_partial_file() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("existing.bin");
        std::fs::write(&path, "x").unwrap();

        let body = Body::from("data");

        assert!(stream_body_to_file(body, &path, 1024).await.is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "x");
    }

    /// Needs a running docker daemon: `cargo test -- --ignored`.
    #[tokio::test]
    #[ignore = "requires docker"]
    async fn stats_use_one_persistent_docker_process_per_server() {
        let mut server = sample_managed_server();
        server.id = 987_010;
        server.status = "running".to_string();
        let name = runtime_container_name(server.id);

        let _ = docker_command().args(["rm", "-f", &name]).output().await;
        let started = docker_command()
            .args(["run", "-d", "--name", &name, "alpine:3.20", "sleep", "300"])
            .output()
            .await
            .unwrap();
        assert!(started.status.success());

        // Twenty clients ask at once and several times in a row.
        for _ in 0..3 {
            let results = futures_util::future::join_all(
                (0..20).map(|_| current_resource_snapshot(&server)),
            )
            .await;

            for result in &results {
                let payload = result.as_ref().unwrap();
                assert_eq!(payload["running"], true);
                assert_eq!(payload["server"], 987_010);
                assert!(payload["stats"]["MemUsage"].is_string());
            }

            tokio::time::sleep(Duration::from_millis(1200)).await;
        }

        // Exactly one `docker stats` process serves all of them.
        let processes = tokio::process::Command::new("ps")
            .args(["-axo", "command"])
            .output()
            .await
            .unwrap();
        let count = String::from_utf8_lossy(&processes.stdout)
            .lines()
            .filter(|line| line.contains("docker stats") && line.contains(&name))
            .count();
        assert_eq!(count, 1, "expected a single persistent docker stats process");

        // Reading is instant once the collector is warm.
        let begin = Instant::now();
        current_resource_snapshot(&server).await.unwrap();
        assert!(begin.elapsed() < Duration::from_millis(100));

        // A stopped container reports not running.
        let _ = docker_command().args(["rm", "-f", &name]).output().await;
        let mut stopped = false;
        for _ in 0..40 {
            let payload = current_resource_snapshot(&server).await.unwrap();

            if payload["running"] == false {
                stopped = true;
                break;
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert!(stopped, "a removed container still reports running");

        forget_resource_snapshot(server.id);
    }

    // ---------------------------------------------------------------------
    // Integration with the depot images (needs docker and the images built
    // with `depot/tests/run.sh --keep <id>`; run with `--ignored`).
    // ---------------------------------------------------------------------

    const SH_PROGRAM: &str = r#"echo "ready-marker port=$1 mem=$SERVER_MEMORY ip=$INTERNAL_IP user=$(id -un) cwd=$(pwd)"
while read l; do
  echo "got:$l"
  if [ "$l" = "stop" ]; then exit 0; fi
done
"#;

    const NODE_PROGRAM: &str = r#"console.log(`ready-marker port=${process.argv[2]} mem=${process.env.SERVER_MEMORY} ip=${process.env.INTERNAL_IP} user=${require('os').userInfo().username} cwd=${process.cwd()}`);
require('readline').createInterface({ input: process.stdin }).on('line', (l) => {
  console.log('got:' + l);
  if (l === 'stop') process.exit(0);
});
"#;

    const PYTHON_PROGRAM: &str = r#"import getpass, os, sys
print(f"ready-marker port={sys.argv[1]} mem={os.environ.get('SERVER_MEMORY')} ip={os.environ.get('INTERNAL_IP')} user={getpass.getuser()} cwd={os.getcwd()}", flush=True)
for line in sys.stdin:
    line = line.rstrip("\n")
    print("got:" + line, flush=True)
    if line == "stop":
        sys.exit(0)
"#;

    const GO_PROGRAM: &str = r#"package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	cwd, _ := os.Getwd()
	fmt.Printf("ready-marker port=%s mem=%s ip=%s user=%s cwd=%s\n", os.Args[1], os.Getenv("SERVER_MEMORY"), os.Getenv("INTERNAL_IP"), os.Getenv("USER"), cwd)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Println("got:" + line)
		if line == "stop" {
			os.Exit(0)
		}
	}
}
"#;

    const JAVA_PROGRAM: &str = r#"import java.util.Scanner;

public class Run {
    public static void main(String[] args) {
        System.out.println("ready-marker port=" + args[0] + " mem=" + System.getenv("SERVER_MEMORY") + " ip=" + System.getenv("INTERNAL_IP") + " user=" + System.getProperty("user.name") + " cwd=" + System.getProperty("user.dir"));
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            System.out.println("got:" + line);
            if (line.equals("stop")) System.exit(0);
        }
    }
}
"#;

    fn depot_image_tag(id: &str) -> String {
        let info = std::process::Command::new("docker")
            .args(["info", "--format", "{{.Architecture}}"])
            .output()
            .expect("docker must be installed");
        let architecture = String::from_utf8_lossy(&info.stdout);
        let suffix = if architecture.contains("aarch64") || architecture.contains("arm64") {
            "linux-arm64"
        } else {
            "linux-amd64"
        };

        format!("depot-test/{id}:{suffix}")
    }

    async fn wait_until<F: Fn() -> bool>(timeout: Duration, condition: F) -> bool {
        let deadline = Instant::now() + timeout;

        while Instant::now() < deadline {
            if condition() {
                return true;
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        condition()
    }

    async fn docker_text(args: &[&str]) -> String {
        let output = docker_command().args(args).output().await.unwrap();

        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    fn console_text(registry: &ServerRegistry, server_id: u64) -> String {
        registry
            .recent_console_messages(server_id, 1000)
            .unwrap()
            .iter()
            .map(|message| message.message.clone())
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Runs one depot image through the real daemon: install-less boot from
    /// `pending`, startup detection, variable substitution, console input,
    /// daemon restart adoption, power restart and graceful stop.
    async fn depot_image_scenario(
        image_id: &str,
        index: u64,
        program_file: &str,
        program: &str,
        startup: &str,
        compile_with: Option<&str>,
    ) {
        use crate::server_lifecycle::ServerLifecycleService;

        let server_id = 987_100 + index;
        let port = 46_100 + index;
        let name = runtime_container_name(server_id);
        let image = depot_image_tag(image_id);

        assert!(
            docker_command()
                .args(["image", "inspect", &image])
                .output()
                .await
                .unwrap()
                .status
                .success(),
            "build the image first: depot/tests/run.sh --keep {image_id} ({image} is missing)"
        );

        let _ = docker_command().args(["rm", "-f", &name]).output().await;
        let volume = managed_server_volume_path(server_id).unwrap();
        let _ = std::fs::remove_dir_all(&volume);
        std::fs::create_dir_all(&volume).unwrap();
        std::fs::write(volume.join(program_file), program).unwrap();
        // The container user is not the host user: make the volume writable.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            std::fs::set_permissions(&volume, std::fs::Permissions::from_mode(0o777)).unwrap();
            std::fs::set_permissions(
                volume.join(program_file),
                std::fs::Permissions::from_mode(0o644),
            )
            .unwrap();
        }

        // A runtime-only image (JRE) cannot compile: build with a JDK image.
        if let Some(compiler_image) = compile_with {
            let compiled = docker_command()
                .args(["run", "--rm", "-v"])
                .arg(format!("{}:/home/container", volume.display()))
                .args(["-w", "/home/container", "--entrypoint", "javac"])
                .arg(depot_image_tag(compiler_image))
                .arg(program_file)
                .output()
                .await
                .unwrap();
            assert!(
                compiled.status.success(),
                "{}",
                String::from_utf8_lossy(&compiled.stderr)
            );
        }

        let directory = tempdir().unwrap();
        let registry = ServerRegistry::new(directory.path().join("registry.db"));
        registry.initialize().unwrap();

        let mut server = sample_managed_server();
        server.id = server_id;
        server.node_id = 5;
        server.status = "pending".to_string();
        server.allocation.bind_ip = "127.0.0.1".to_string();
        server.allocation.port = port;
        server.limits.memory_mib = 512;
        server.limits.disk_mib = 2048;
        server.cargo.install_script = None;
        server.cargo.install_container = None;
        server.cargo.startup_command = startup.to_string();
        server.cargo.config_startup = "ready-marker".to_string();
        server.cargo.config_stop = "stop".to_string();
        server.cargo.docker_images =
            std::collections::BTreeMap::from([("test".to_string(), image.clone())]);
        registry.upsert_server(&server).unwrap();

        let config = {
            let mut config = ws_test_config(5);
            config.panel.node_id = Some(5);
            config
        };
        let (_config_tx, config_rx) = watch::channel(config);

        // 1. Boot from pending.
        let cancellation = CancellationToken::new();
        let service = ServerLifecycleService::new(config_rx.clone(), registry.clone(), cancellation.clone());
        let handle = tokio::spawn(service.run());

        let running = wait_until(Duration::from_secs(240), || {
            registry.get_server_status(server_id).unwrap().as_deref() == Some("running")
        })
        .await;
        assert!(running, "{image_id}: never became running\n{}", console_text(&registry, server_id));

        let console = console_text(&registry, server_id);
        assert!(
            console.contains(&format!("ready-marker port={port} mem=512 ip=")),
            "{image_id}: {{{{VAR}}}} substitution or env is wrong\n{console}"
        );
        assert!(console.contains("user=container"), "{image_id}: not running as the container user\n{console}");
        assert!(console.contains("cwd=/home/container"), "{image_id}: wrong working directory\n{console}");
        let ip = console
            .split("ip=")
            .nth(1)
            .and_then(|rest| rest.split_whitespace().next())
            .unwrap_or_default();
        assert!(!ip.is_empty() && ip != "user=container", "{image_id}: INTERNAL_IP is empty\n{console}");

        // 2. Console input reaches the program.
        let server = registry.get_server(server_id).unwrap().unwrap();
        send_command_to_server(&registry, &server, "ping").await.unwrap();
        assert!(
            wait_until(Duration::from_secs(10), || console_text(&registry, server_id).contains("got:ping")).await,
            "{image_id}: console command was not delivered\n{}",
            console_text(&registry, server_id)
        );

        // 3. Daemon restart: the container is adopted, not recreated.
        let container_id = docker_text(&["inspect", "-f", "{{.Id}}", &name]).await;
        cancellation.cancel();
        let _ = handle.await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        let cancellation = CancellationToken::new();
        let service = ServerLifecycleService::new(config_rx.clone(), registry.clone(), cancellation.clone());
        let handle = tokio::spawn(service.run());
        tokio::time::sleep(Duration::from_secs(8)).await;

        assert_eq!(
            docker_text(&["inspect", "-f", "{{.Id}} {{.State.Running}}", &name]).await,
            format!("{container_id} true"),
            "{image_id}: container was recreated by a daemon restart"
        );
        send_command_to_server(&registry, &server, "after-restart").await.unwrap();
        assert!(
            wait_until(Duration::from_secs(10), || console_text(&registry, server_id).contains("got:after-restart")).await,
            "{image_id}: console is dead after a daemon restart"
        );

        // 4. Power restart through the websocket code path.
        let server = registry.get_server(server_id).unwrap().unwrap();
        apply_power_signal(&registry, &server, PowerSignal::Restart).await.unwrap();
        let restarted = wait_until(Duration::from_secs(240), || {
            registry.get_server_status(server_id).unwrap().as_deref() == Some("running")
                && console_text(&registry, server_id).matches("ready-marker").count() >= 2
        })
        .await;
        assert!(restarted, "{image_id}: restart did not come back\n{}", console_text(&registry, server_id));

        // 5. Graceful stop: the "stop" command ends the program.
        let server = registry.get_server(server_id).unwrap().unwrap();
        apply_power_signal(&registry, &server, PowerSignal::Stop).await.unwrap();
        let stopped = wait_until(Duration::from_secs(60), || {
            registry.get_server_status(server_id).unwrap().as_deref() == Some("offline")
        })
        .await;
        assert!(stopped, "{image_id}: stop did not complete\n{}", console_text(&registry, server_id));
        assert!(console_text(&registry, server_id).contains("got:stop"), "{image_id}: graceful stop command was not delivered");
        assert!(
            docker_text(&["ps", "-aq", "--filter", &format!("name=^{name}$")]).await.is_empty(),
            "{image_id}: container still exists after stop"
        );

        cancellation.cancel();
        let _ = handle.await;
        let _ = docker_command().args(["rm", "-f", &name]).output().await;
        let _ = std::fs::remove_dir_all(&volume);
    }

    /// The real thing: the Paper catalog entry (install script run in the depot
    /// installer image, then the server booted on the depot Java image) driven
    /// by the daemon, with console input and a graceful stop.
    /// Needs internet access, `installer-debian` and `java-25-jre` built with
    /// `depot/tests/run.sh --keep`.
    #[tokio::test]
    #[ignore = "requires docker, internet and depot images"]
    async fn depot_minecraft_paper_installs_boots_and_stops_under_skyportd() {
        use crate::server_lifecycle::ServerLifecycleService;
        use crate::server_registry::ManagedServerVariable;

        let server_id = 987_200;
        let name = runtime_container_name(server_id);
        let install_image = "depot-test/installer-debian:linux-amd64".to_string();
        let runtime_image = depot_image_tag("java-25-jre");

        for image in [&install_image, &runtime_image] {
            assert!(
                docker_command()
                    .args(["image", "inspect", image])
                    .output()
                    .await
                    .unwrap()
                    .status
                    .success(),
                "{image} is missing: build it with depot/tests/run.sh --keep"
            );
        }

        let install_script = std::fs::read_to_string(
            project_root().unwrap().join("../depot/tests/e2e/minecraft-paper.install.sh"),
        )
        .expect("the depot e2e install script");

        let _ = docker_command().args(["rm", "-f", &name]).output().await;
        let volume = managed_server_volume_path(server_id).unwrap();
        let _ = std::fs::remove_dir_all(&volume);

        let directory = tempdir().unwrap();
        let registry = ServerRegistry::new(directory.path().join("registry.db"));
        registry.initialize().unwrap();

        let variable = |env: &str, default: &str| ManagedServerVariable {
            name: env.to_string(),
            description: String::new(),
            env_variable: env.to_string(),
            default_value: default.to_string(),
            user_viewable: true,
            user_editable: true,
            rules: "required".to_string(),
            field_type: "text".to_string(),
        };

        let mut server = sample_managed_server();
        server.id = server_id;
        server.node_id = 5;
        server.status = "pending".to_string();
        server.allocation.bind_ip = "127.0.0.1".to_string();
        server.allocation.port = 46_200;
        server.limits.memory_mib = 1536;
        server.limits.disk_mib = 4096;
        server.cargo.startup_command = "java -Xms128M -Xmx{{SERVER_MEMORY}}M -Dterminal.jline=false -Dterminal.ansi=true -jar {{SERVER_JARFILE}}".to_string();
        server.cargo.config_startup = r#"{"done":")! For help, type"}"#.to_string();
        server.cargo.config_stop = "stop".to_string();
        server.cargo.install_container = Some(install_image);
        server.cargo.install_entrypoint = Some("bash".to_string());
        server.cargo.install_script = Some(install_script);
        server.cargo.docker_images =
            std::collections::BTreeMap::from([("Java 25".to_string(), runtime_image)]);
        server.cargo.variables = vec![
            variable("SERVER_JARFILE", "server.jar"),
            variable("MINECRAFT_VERSION", "latest"),
            variable("BUILD_NUMBER", "latest"),
        ];
        registry.upsert_server(&server).unwrap();

        let config = {
            let mut config = ws_test_config(5);
            config.panel.node_id = Some(5);
            config
        };
        let (_config_tx, config_rx) = watch::channel(config);
        let cancellation = CancellationToken::new();
        let service = ServerLifecycleService::new(config_rx, registry.clone(), cancellation.clone());
        let handle = tokio::spawn(service.run());

        let running = wait_until(Duration::from_secs(900), || {
            matches!(
                registry.get_server_status(server_id).unwrap().as_deref(),
                Some("running") | Some("install_failed")
            )
        })
        .await;
        let status = registry.get_server_status(server_id).unwrap();
        let console = console_text(&registry, server_id);
        assert!(
            running && status.as_deref() == Some("running"),
            "paper did not reach running (status {status:?})\n{}",
            &console[console.len().saturating_sub(4000)..]
        );
        assert!(console.contains("Installation completed successfully."));
        assert!(console.contains("Startup matcher detected"));
        assert!(volume.join("server.jar").exists());

        // Console command round trip against the real server.
        let server = registry.get_server(server_id).unwrap().unwrap();
        send_command_to_server(&registry, &server, "list").await.unwrap();
        assert!(
            wait_until(Duration::from_secs(30), || {
                console_text(&registry, server_id).contains("players online")
                    || console_text(&registry, server_id).contains("of a max")
            })
            .await,
            "the Paper server did not answer `list`\n{}",
            console_text(&registry, server_id)
        );

        // Graceful stop through the configured stop command.
        apply_power_signal(&registry, &server, PowerSignal::Stop).await.unwrap();
        assert!(
            wait_until(Duration::from_secs(120), || {
                registry.get_server_status(server_id).unwrap().as_deref() == Some("offline")
            })
            .await,
            "the Paper server did not stop\n{}",
            console_text(&registry, server_id)
        );

        cancellation.cancel();
        let _ = handle.await;
        let _ = docker_command().args(["rm", "-f", &name]).output().await;
        let _ = std::fs::remove_dir_all(&volume);
    }

    #[tokio::test]
    #[ignore = "requires docker and depot images"]
    async fn depot_os_alpine_runs_under_skyportd() {
        depot_image_scenario("os-alpine", 0, "run.sh", SH_PROGRAM, "sh run.sh {{SERVER_PORT}}", None).await;
    }

    #[tokio::test]
    #[ignore = "requires docker and depot images"]
    async fn depot_os_debian_runs_under_skyportd() {
        depot_image_scenario("os-debian", 1, "run.sh", SH_PROGRAM, "bash run.sh {{SERVER_PORT}}", None).await;
    }

    #[tokio::test]
    #[ignore = "requires docker and depot images"]
    async fn depot_nodejs_runs_under_skyportd() {
        depot_image_scenario("nodejs-22", 2, "run.js", NODE_PROGRAM, "node run.js {{SERVER_PORT}}", None).await;
    }

    #[tokio::test]
    #[ignore = "requires docker and depot images"]
    async fn depot_python_runs_under_skyportd() {
        depot_image_scenario("python-3.12", 3, "run.py", PYTHON_PROGRAM, "python run.py {{SERVER_PORT}}", None).await;
    }

    #[tokio::test]
    #[ignore = "requires docker and depot images"]
    async fn depot_go_runs_under_skyportd() {
        depot_image_scenario("go-1.24", 4, "run.go", GO_PROGRAM, "go run run.go {{SERVER_PORT}}", None).await;
    }

    #[tokio::test]
    #[ignore = "requires docker and depot images"]
    async fn depot_java_runs_under_skyportd() {
        depot_image_scenario(
            "java-21-jre",
            5,
            "Run.java",
            JAVA_PROGRAM,
            "java -Xmx{{SERVER_MEMORY}}M -cp . Run {{SERVER_PORT}}",
            Some("java-21"),
        )
        .await;
    }

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
                permissions: vec!["console".to_string()],
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
    fn ws_permission_check_matches_power_and_wildcard() {
        use super::websocket::has_permission;

        assert!(has_permission(&["*".to_string()], "power"));
        assert!(has_permission(&["power".to_string(), "console".to_string()], "power"));
        assert!(!has_permission(&["console".to_string()], "power"));
        assert!(!has_permission(&[], "power"));
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
                requires_privileged: false,
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
