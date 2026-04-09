use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::net::{Ipv4Addr, SocketAddr};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path as StdPath, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use axum_server::Handle;
use axum_server::tls_rustls::RustlsConfig;
use ring::hmac;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::process::Command;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{DaemonConfig, NodeSection, managed_server_volume_path, safe_join_relative};
use crate::configuration;
use crate::server_registry::{
    ConsoleMessageRecord, ManagedServerAllocation, ManagedServerCargo, ManagedServerLimits,
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
struct FilesystemEntryPayload {
    kind: String,
    last_modified_at: Option<u64>,
    name: String,
    path: String,
    permissions: Option<String>,
    size_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
struct DirectoryListingPayload {
    entries: Vec<FilesystemEntryPayload>,
    parent_path: Option<String>,
    path: String,
}

#[derive(Debug, Serialize)]
struct FileContentsPayload {
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
enum PowerSignal {
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
        .route(
            "/api/daemon/servers/{server_id}/install-log",
            get(download_install_log),
        )
        .route("/api/daemon/servers/{server_id}/ws", get(server_ws))
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

async fn server_ws(
    ws: WebSocketUpgrade,
    Path(server_id): Path<u64>,
    Query(query): Query<WsAuthorizationQuery>,
    State(state): State<ApiState>,
) -> std::result::Result<Response, (StatusCode, Json<ApiErrorResponse>)> {
    let config = state.config_rx.borrow().clone();

    ensure_compatible_request(&config, &query.uuid, &query.panel_version)?;
    load_managed_server(&state.server_registry, &config, server_id)?;

    Ok(ws.on_upgrade(move |socket| {
        handle_server_ws(
            socket,
            state.server_registry,
            server_id,
            config.panel.daemon_callback_token,
            query.uuid,
            query.panel_version,
        )
    }))
}

async fn handle_server_ws(
    mut socket: WebSocket,
    registry: ServerRegistry,
    server_id: u64,
    expected_token: Option<String>,
    request_uuid: String,
    request_panel_version: String,
) {
    let mut authenticated = false;
    let mut stats_subscribed = false;
    let mut last_id = 0_u64;
    let mut last_status: Option<String> = None;

    let mut console_interval = time::interval(Duration::from_millis(100));
    console_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut stats_interval = time::interval(Duration::from_secs(2));
    stats_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut status_interval = time::interval(Duration::from_millis(500));
    status_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = console_interval.tick(), if authenticated => {
                let messages = match registry.console_messages_since(server_id, last_id, 200) {
                    Ok(messages) => messages,
                    Err(_) => break,
                };

                for message in messages {
                    last_id = message.id;
                    if send_console_event(&mut socket, &message).await.is_err() {
                        return;
                    }
                }
            }
            _ = stats_interval.tick(), if authenticated && stats_subscribed => {
                let server = match registry.get_server(server_id) {
                    Ok(Some(server)) => server,
                    _ => return,
                };

                if send_stats_event(&mut socket, &server).await.is_err() {
                    return;
                }
            }
            _ = status_interval.tick(), if authenticated => {
                let server = match registry.get_server(server_id) {
                    Ok(Some(server)) => server,
                    _ => return,
                };

                if last_status.as_deref() != Some(server.status.as_str()) {
                    let previous_status = last_status.replace(server.status.clone());

                    if send_status_event(&mut socket, &server.status).await.is_err() {
                        return;
                    }

                    if send_install_state_event(&mut socket, previous_status.as_deref(), &server.status).await.is_err() {
                        return;
                    }
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | None => return,
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            return;
                        }
                    }
                    Some(Ok(Message::Text(payload))) => {
                        let event = match parse_ws_client_event(payload.as_str()) {
                            Ok(event) => event,
                            Err(error) => {
                                if send_ws_event(&mut socket, "daemon error", vec![Value::String(error.to_string())]).await.is_err() {
                                    return;
                                }
                                continue;
                            }
                        };

                        if event.event == "auth" {
                            match authenticate_ws_event(
                                expected_token.as_deref(),
                                &event,
                                server_id,
                                &request_uuid,
                                &request_panel_version,
                            ) {
                                Ok(()) => {
                                    authenticated = true;

                                    if send_ws_event(&mut socket, "auth success", Vec::new()).await.is_err() {
                                        return;
                                    }

                                    if let Ok(Some(server)) = registry.get_server(server_id) {
                                        last_status = Some(server.status.clone());

                                        if send_status_event(&mut socket, &server.status).await.is_err() {
                                            return;
                                        }
                                    }

                                    if let Ok(backlog) = registry.console_messages_since(server_id, 0, 200) {
                                        for message in backlog {
                                            last_id = last_id.max(message.id);
                                        }
                                    }
                                }
                                Err(error) => {
                                    if send_ws_event(&mut socket, "jwt error", vec![Value::String(error.to_string())]).await.is_err() {
                                        return;
                                    }
                                }
                            }

                            continue;
                        }

                        if !authenticated {
                            if send_ws_event(&mut socket, "jwt error", vec![Value::String("You must authenticate the websocket before sending commands.".to_string())]).await.is_err() {
                                return;
                            }
                            continue;
                        }

                        match handle_ws_client_event(&registry, server_id, &mut socket, event, &mut stats_subscribed, &mut last_id).await {
                            Ok(()) => {}
                            Err(error) => {
                                if send_ws_event(&mut socket, "daemon error", vec![Value::String(error.to_string())]).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                    Some(Err(_)) => return,
                    _ => {}
                }
            }
        }
    }
}

async fn send_ws_event(socket: &mut WebSocket, event: &str, args: Vec<Value>) -> Result<()> {
    socket
        .send(Message::Text(ws_event_payload(event, args).into()))
        .await
        .with_context(|| format!("failed to send {event} websocket event"))
}

async fn send_status_event(socket: &mut WebSocket, status: &str) -> Result<()> {
    send_ws_event(socket, "status", vec![Value::String(status.to_string())]).await
}

async fn send_console_event(socket: &mut WebSocket, message: &ConsoleMessageRecord) -> Result<()> {
    send_ws_event(
        socket,
        console_event_name(&message.source),
        vec![Value::String(message.message.clone())],
    )
    .await
}

async fn send_stats_event(socket: &mut WebSocket, server: &ManagedServerRecord) -> Result<()> {
    let payload = match current_resource_snapshot(server).await {
        Ok(payload) => payload,
        Err(error) => json!({
            "server": server.id,
            "running": false,
            "state": server.status,
            "error": error.to_string(),
        }),
    };

    send_ws_event(socket, "stats", vec![payload]).await
}

async fn send_install_state_event(
    socket: &mut WebSocket,
    previous_status: Option<&str>,
    current_status: &str,
) -> Result<()> {
    if current_status == "installing" && previous_status != Some("installing") {
        return send_ws_event(socket, "install started", Vec::new()).await;
    }

    if previous_status == Some("installing") && current_status != "installing" {
        return send_ws_event(socket, "install completed", Vec::new()).await;
    }

    Ok(())
}

async fn handle_ws_client_event(
    registry: &ServerRegistry,
    server_id: u64,
    socket: &mut WebSocket,
    event: WsClientEvent,
    stats_subscribed: &mut bool,
    last_id: &mut u64,
) -> Result<()> {
    let server = registry
        .get_server(server_id)?
        .context("The requested server could not be found.")?;

    match event.event.as_str() {
        "send logs" => {
            let backlog = registry.recent_console_messages(server_id, 50)?;

            for message in backlog {
                *last_id = (*last_id).max(message.id);
                send_console_event(socket, &message).await?;
            }
        }
        "send stats" => {
            *stats_subscribed = true;
            send_stats_event(socket, &server).await?;
        }
        "set state" => {
            let signal = parse_power_signal(event.args.first())?;
            apply_power_signal(registry, &server, signal).await?;
        }
        "send command" => {
            let command = event
                .args
                .first()
                .map(String::as_str)
                .filter(|value| !value.trim().is_empty())
                .context("The send command event requires a command string.")?;

            send_command_to_server(registry, &server, command).await?;
        }
        _ => {
            bail!("Unsupported websocket event: {}", event.event);
        }
    }

    Ok(())
}

fn parse_ws_client_event(payload: &str) -> Result<WsClientEvent> {
    serde_json::from_str::<WsClientEvent>(payload).context("Invalid websocket payload.")
}

fn authenticate_ws_event(
    expected_token: Option<&str>,
    event: &WsClientEvent,
    server_id: u64,
    request_uuid: &str,
    request_panel_version: &str,
) -> Result<()> {
    let secret = expected_token
        .filter(|token| !token.is_empty())
        .context("Missing daemon callback token.")?;
    let provided = event
        .args
        .first()
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("The auth event requires a token.")?;
    let (payload_hex, signature_hex) = provided
        .split_once('.')
        .context("The websocket token is malformed.")?;

    let expected_signature = hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes()),
        payload_hex.as_bytes(),
    );

    if hex_encode(expected_signature.as_ref()) != signature_hex {
        bail!("The websocket token signature is invalid.");
    }

    let payload_json = String::from_utf8(hex_decode(payload_hex)?)
        .context("The websocket token payload is invalid UTF-8.")?;
    let payload = serde_json::from_str::<WsTokenPayload>(&payload_json)
        .context("The websocket token payload is invalid JSON.")?;

    if payload.server_id != server_id {
        bail!("The websocket token does not belong to this server.");
    }

    if payload.daemon_uuid != request_uuid {
        bail!("The websocket token daemon identity is invalid.");
    }

    if payload.panel_version != request_panel_version {
        bail!("The websocket token panel version is invalid.");
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("The system clock is invalid.")?
        .as_secs();

    if payload.exp <= now {
        bail!("The websocket token has expired.");
    }

    Ok(())
}

fn runtime_status_after_start_request(is_running: bool) -> &'static str {
    if is_running { "running" } else { "starting" }
}

fn parse_power_signal(signal: Option<&String>) -> Result<PowerSignal> {
    match signal.map(String::as_str) {
        Some("start") => Ok(PowerSignal::Start),
        Some("stop") => Ok(PowerSignal::Stop),
        Some("restart") => Ok(PowerSignal::Restart),
        Some("kill") => Ok(PowerSignal::Kill),
        Some("reinstall") => Ok(PowerSignal::Reinstall),
        Some(value) => bail!("Unsupported power state: {value}"),
        None => bail!("The set state event requires a power state."),
    }
}

fn hex_decode(value: &str) -> Result<Vec<u8>> {
    if value.len() % 2 != 0 {
        bail!("The websocket token payload is malformed.");
    }

    let mut bytes = Vec::with_capacity(value.len() / 2);
    let mut index = 0;

    while index < value.len() {
        let byte = u8::from_str_radix(&value[index..index + 2], 16).with_context(|| {
            format!("The websocket token payload contains invalid hex at byte {index}.")
        })?;
        bytes.push(byte);
        index += 2;
    }

    Ok(bytes)
}

fn hex_encode(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn ws_event_payload(event: &str, args: Vec<Value>) -> String {
    json!({
        "event": event,
        "args": args,
    })
    .to_string()
}

fn console_event_name(source: &str) -> &'static str {
    match source {
        "install" => "install output",
        "system" => "daemon message",
        _ => "console output",
    }
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

async fn current_resource_snapshot(server: &ManagedServerRecord) -> Result<Value> {
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

async fn apply_power_signal(
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

async fn send_command_to_server(
    _registry: &ServerRegistry,
    server: &ManagedServerRecord,
    command: &str,
) -> Result<()> {
    if !container_is_running(server.id).await? {
        bail!("The server is not running.");
    }

    let output = docker_command()
        .arg("exec")
        .arg("-e")
        .arg(format!("SKYPORT_SERVER_COMMAND={command}"))
        .arg(runtime_container_name(server.id))
        .arg("sh")
        .arg("-lc")
        .arg(runtime_command_forward_script())
        .output()
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

fn runtime_command_forward_script() -> &'static str {
    r#"pid=1
while :; do
    child=""
    for stat in /proc/[0-9]*/stat; do
        set -- $(cat "$stat" 2>/dev/null) || continue
        if [ "$4" = "$pid" ]; then
            child="$1"
            break
        fi
    done

    [ -n "$child" ] || break
    pid="$child"
done

printf '%s\n' "$SKYPORT_SERVER_COMMAND" > "/proc/$pid/fd/0""#
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

fn load_managed_server(
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

async fn container_is_running(server_id: u64) -> Result<bool> {
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

fn runtime_container_name(server_id: u64) -> String {
    format!("skyport-server-{server_id}")
}

fn install_container_name(server_id: u64) -> String {
    format!("skyport-install-{server_id}")
}

fn docker_command() -> Command {
    let mut command = Command::new("docker");
    command.kill_on_drop(true);
    command
}

fn resolve_volume_path(server: &ManagedServerRecord) -> Result<PathBuf> {
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

fn list_directory(
    server: &ManagedServerRecord,
    requested_path: &str,
) -> Result<DirectoryListingPayload> {
    let volume_path = ensure_server_volume(server)?;
    let normalized_path = normalize_relative_path(requested_path)?;
    let directory_path = resolve_existing_server_path(&volume_path, &normalized_path)?;
    let metadata = fs::metadata(&directory_path)
        .with_context(|| format!("failed to read metadata for {}", directory_path.display()))?;

    if !metadata.is_dir() {
        bail!("The requested path is not a directory.");
    }

    let mut entries = fs::read_dir(&directory_path)
        .with_context(|| format!("failed to read {}", directory_path.display()))?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| filesystem_entry_payload(&volume_path, entry.path()).transpose())
        .collect::<Result<Vec<_>>>()?;

    entries.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .then_with(|| left.name.to_lowercase().cmp(&right.name.to_lowercase()))
    });

    Ok(DirectoryListingPayload {
        entries,
        parent_path: parent_relative_path(&normalized_path),
        path: normalized_path,
    })
}

fn read_text_file(
    server: &ManagedServerRecord,
    requested_path: &str,
) -> Result<FileContentsPayload> {
    let volume_path = ensure_server_volume(server)?;
    let normalized_path = normalize_relative_path(requested_path)?;

    if normalized_path.is_empty() {
        bail!("The requested path is not a file.");
    }

    let file_path = resolve_existing_server_path(&volume_path, &normalized_path)?;
    let metadata = fs::metadata(&file_path)
        .with_context(|| format!("failed to read metadata for {}", file_path.display()))?;

    if !metadata.is_file() {
        bail!("The requested path is not a file.");
    }

    if metadata.len() > 1024 * 1024 {
        bail!("This file is too large to open in the editor.");
    }

    let bytes =
        fs::read(&file_path).with_context(|| format!("failed to read {}", file_path.display()))?;

    if bytes.contains(&0) {
        bail!("This file could not be opened as text.");
    }

    let contents = String::from_utf8(bytes).context("This file could not be opened as text.")?;

    Ok(FileContentsPayload {
        contents,
        last_modified_at: modified_at(&metadata),
        path: normalized_path,
        permissions: Some(format_permissions(metadata.permissions().mode())),
        size_bytes: metadata.len(),
    })
}

fn write_text_file(
    server: &ManagedServerRecord,
    requested_path: &str,
    contents: &str,
) -> Result<()> {
    if contents.len() > 2 * 1024 * 1024 {
        bail!("This file is too large to save through the editor.");
    }

    let volume_path = ensure_server_volume(server)?;
    let normalized_path = normalize_relative_path(requested_path)?;

    if normalized_path.is_empty() {
        bail!("The requested path is not a file.");
    }

    let file_path = resolve_server_path_for_write(&volume_path, &normalized_path)?;

    if file_path.is_dir() {
        bail!("The requested path is not a file.");
    }

    fs::write(&file_path, contents)
        .with_context(|| format!("failed to write {}", file_path.display()))?;

    Ok(())
}

fn create_file(server: &ManagedServerRecord, requested_path: &str, name: &str) -> Result<()> {
    let volume_path = ensure_server_volume(server)?;
    let directory_path =
        resolve_existing_server_path(&volume_path, &normalize_relative_path(requested_path)?)?;

    if !fs::metadata(&directory_path)?.is_dir() {
        bail!("The requested path is not a directory.");
    }

    let file_path = directory_path.join(validate_entry_name(name)?);

    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&file_path)
        .with_context(|| format!("failed to create {}", file_path.display()))?;

    Ok(())
}

fn create_directory(server: &ManagedServerRecord, requested_path: &str, name: &str) -> Result<()> {
    let volume_path = ensure_server_volume(server)?;
    let directory_path =
        resolve_existing_server_path(&volume_path, &normalize_relative_path(requested_path)?)?;

    if !fs::metadata(&directory_path)?.is_dir() {
        bail!("The requested path is not a directory.");
    }

    let next_directory = directory_path.join(validate_entry_name(name)?);

    fs::create_dir(&next_directory)
        .with_context(|| format!("failed to create {}", next_directory.display()))?;

    Ok(())
}

fn delete_files(server: &ManagedServerRecord, paths: &[String]) -> Result<()> {
    let volume_path = ensure_server_volume(server)?;

    if paths.is_empty() {
        bail!("Please select at least one file or directory.");
    }

    for path in paths {
        let normalized_path = normalize_relative_path(path)?;

        if normalized_path.is_empty() {
            bail!("The server root cannot be deleted.");
        }

        let target_path = resolve_existing_server_path(&volume_path, &normalized_path)?;
        let metadata = fs::metadata(&target_path)
            .with_context(|| format!("failed to read metadata for {}", target_path.display()))?;

        if metadata.is_dir() {
            fs::remove_dir_all(&target_path)
                .with_context(|| format!("failed to delete {}", target_path.display()))?;
        } else {
            fs::remove_file(&target_path)
                .with_context(|| format!("failed to delete {}", target_path.display()))?;
        }
    }

    Ok(())
}

fn rename_filesystem_entry(
    server: &ManagedServerRecord,
    requested_path: &str,
    name: &str,
) -> Result<()> {
    let volume_path = ensure_server_volume(server)?;
    let normalized_path = normalize_relative_path(requested_path)?;

    if normalized_path.is_empty() {
        bail!("The server root cannot be renamed.");
    }

    let source_path = resolve_existing_server_path(&volume_path, &normalized_path)?;
    let target_path = source_path
        .parent()
        .context("path must stay within the server volume")?
        .join(validate_entry_name(name)?);

    if target_path.exists() {
        bail!("A file or directory with that name already exists.");
    }

    fs::rename(&source_path, &target_path).with_context(|| {
        format!(
            "failed to rename {} to {}",
            source_path.display(),
            target_path.display()
        )
    })?;

    Ok(())
}

fn move_files(server: &ManagedServerRecord, paths: &[String], destination: &str) -> Result<()> {
    let volume_path = ensure_server_volume(server)?;
    let destination_path = resolve_destination_directory(&volume_path, destination)?;

    for (source_path, target_path) in transfer_targets(&volume_path, paths, &destination_path)? {
        move_path_recursive(&source_path, &target_path)?;
    }

    Ok(())
}

fn copy_files(server: &ManagedServerRecord, paths: &[String], destination: &str) -> Result<()> {
    let volume_path = ensure_server_volume(server)?;
    let destination_path = resolve_destination_directory(&volume_path, destination)?;

    for (source_path, target_path) in transfer_targets(&volume_path, paths, &destination_path)? {
        copy_path_recursive(&source_path, &target_path)?;
    }

    Ok(())
}

fn update_file_permissions(
    server: &ManagedServerRecord,
    paths: &[String],
    permissions: &str,
) -> Result<()> {
    let volume_path = ensure_server_volume(server)?;
    let mode = normalize_permissions(permissions)?;

    if paths.is_empty() {
        bail!("Please select at least one file or directory.");
    }

    for path in paths {
        let normalized_path = normalize_relative_path(path)?;

        if normalized_path.is_empty() {
            bail!("The server root permissions cannot be changed here.");
        }

        let target_path = resolve_existing_server_path(&volume_path, &normalized_path)?;
        let mut target_permissions = fs::metadata(&target_path)
            .with_context(|| format!("failed to read metadata for {}", target_path.display()))?
            .permissions();
        target_permissions.set_mode(mode);
        fs::set_permissions(&target_path, target_permissions).with_context(|| {
            format!("failed to update permissions for {}", target_path.display())
        })?;
    }

    Ok(())
}

fn create_archive(
    server: &ManagedServerRecord,
    paths: &[String],
    requested_path: &str,
    name: &str,
) -> Result<()> {
    let volume_path = ensure_server_volume(server)?;
    let destination_path = resolve_destination_directory(&volume_path, requested_path)?;
    let archive_path = destination_path.join(archive_file_name(name)?);

    if archive_path.exists() {
        bail!("A file with that name already exists.");
    }

    let normalized_paths = archive_source_paths(&volume_path, paths, &archive_path)?;
    let output = std::process::Command::new("zip")
        .current_dir(&volume_path)
        .arg("-rq")
        .arg(&archive_path)
        .args(&normalized_paths)
        .output()
        .context("failed to execute zip")?;

    if !output.status.success() {
        let _ = fs::remove_file(&archive_path);
        bail!(command_output_message(
            &output.stderr,
            "The archive could not be created."
        ));
    }

    Ok(())
}

fn extract_archive(
    server: &ManagedServerRecord,
    requested_path: &str,
    destination: &str,
) -> Result<()> {
    let volume_path = ensure_server_volume(server)?;
    let normalized_archive_path = normalize_relative_path(requested_path)?;

    if normalized_archive_path.is_empty() {
        bail!("Please choose an archive to extract.");
    }

    let archive_path = resolve_existing_server_path(&volume_path, &normalized_archive_path)?;

    if !fs::metadata(&archive_path)
        .with_context(|| format!("failed to read metadata for {}", archive_path.display()))?
        .is_file()
    {
        bail!("The requested path is not a file.");
    }

    let destination_path = resolve_destination_directory(&volume_path, destination)?;
    let archive_entries = list_archive_entries(&archive_path)?;
    let top_level_entries = extract_top_level_entries(&archive_entries)?;
    let temp_path = unique_temp_path(&volume_path, ".skyport-extract")?;
    fs::create_dir(&temp_path)
        .with_context(|| format!("failed to create {}", temp_path.display()))?;

    let extraction_result = (|| -> Result<()> {
        let output = std::process::Command::new("unzip")
            .arg("-oq")
            .arg(&archive_path)
            .arg("-d")
            .arg(&temp_path)
            .output()
            .context("failed to execute unzip")?;

        if !output.status.success() {
            bail!(command_output_message(
                &output.stderr,
                "The archive could not be extracted."
            ));
        }

        ensure_tree_contains_no_symlinks(&temp_path)?;

        for entry in &top_level_entries {
            let source_path = temp_path.join(entry);
            let target_path = destination_path.join(entry);

            if !source_path.exists() {
                continue;
            }

            if target_path.exists() {
                bail!(format!(
                    "{} already exists in the destination.",
                    target_path.display()
                ));
            }

            move_path_recursive(&source_path, &target_path)?;
        }

        Ok(())
    })();

    let cleanup_result = fs::remove_dir_all(&temp_path);

    extraction_result?;
    cleanup_result.with_context(|| format!("failed to clean up {}", temp_path.display()))?;

    Ok(())
}

fn upload_file(
    server: &ManagedServerRecord,
    requested_path: &str,
    name: &str,
    bytes: &[u8],
) -> Result<()> {
    if bytes.len() > 100 * 1024 * 1024 {
        bail!("Uploaded files may not be larger than 100 MB.");
    }

    let volume_path = ensure_server_volume(server)?;
    let directory_path = resolve_destination_directory(&volume_path, requested_path)?;
    let file_path = directory_path.join(validate_entry_name(name)?);

    if file_path.exists() {
        bail!("A file with that name already exists.");
    }

    fs::write(&file_path, bytes)
        .with_context(|| format!("failed to write {}", file_path.display()))?;

    Ok(())
}

fn resolve_destination_directory(volume_path: &PathBuf, destination: &str) -> Result<PathBuf> {
    let destination_path =
        resolve_existing_server_path(volume_path, &normalize_relative_path(destination)?)?;

    if !fs::metadata(&destination_path)
        .with_context(|| format!("failed to read metadata for {}", destination_path.display()))?
        .is_dir()
    {
        bail!("The requested destination is not a directory.");
    }

    Ok(destination_path)
}

fn transfer_targets(
    volume_path: &PathBuf,
    paths: &[String],
    destination_path: &PathBuf,
) -> Result<Vec<(PathBuf, PathBuf)>> {
    if paths.is_empty() {
        bail!("Please select at least one file or directory.");
    }

    let mut seen_targets = BTreeSet::new();
    let mut transfers = Vec::with_capacity(paths.len());

    for path in paths {
        let normalized_path = normalize_relative_path(path)?;

        if normalized_path.is_empty() {
            bail!("The server root cannot be moved or copied.");
        }

        let source_path = resolve_existing_server_path(volume_path, &normalized_path)?;
        let metadata = fs::metadata(&source_path)
            .with_context(|| format!("failed to read metadata for {}", source_path.display()))?;
        let target_name = source_path
            .file_name()
            .context("failed to determine file name")?;
        let target_path = destination_path.join(target_name);

        if target_path.exists() {
            bail!("A file or directory with that name already exists in the destination.");
        }

        if metadata.is_dir() && destination_path.starts_with(&source_path) {
            bail!("A directory cannot be moved or copied into itself.");
        }

        let target_key = target_path.to_string_lossy().to_string();
        if !seen_targets.insert(target_key) {
            bail!("Two selected items would collide in the destination.");
        }

        transfers.push((source_path, target_path));
    }

    Ok(transfers)
}

fn copy_path_recursive(source_path: &PathBuf, target_path: &PathBuf) -> Result<()> {
    let metadata = fs::symlink_metadata(source_path)
        .with_context(|| format!("failed to read metadata for {}", source_path.display()))?;

    if metadata.file_type().is_symlink() {
        bail!("Symlinks are not supported in file transfers.");
    }

    if metadata.is_dir() {
        fs::create_dir(target_path)
            .with_context(|| format!("failed to create {}", target_path.display()))?;

        for entry in fs::read_dir(source_path)
            .with_context(|| format!("failed to read {}", source_path.display()))?
        {
            let entry =
                entry.with_context(|| format!("failed to read {}", source_path.display()))?;
            let child_source = entry.path();
            let child_target = target_path.join(entry.file_name());
            copy_path_recursive(&child_source, &child_target)?;
        }

        fs::set_permissions(target_path, metadata.permissions()).with_context(|| {
            format!("failed to update permissions for {}", target_path.display())
        })?;
    } else {
        fs::copy(source_path, target_path).with_context(|| {
            format!(
                "failed to copy {} to {}",
                source_path.display(),
                target_path.display()
            )
        })?;
        fs::set_permissions(target_path, metadata.permissions()).with_context(|| {
            format!("failed to update permissions for {}", target_path.display())
        })?;
    }

    Ok(())
}

fn move_path_recursive(source_path: &PathBuf, target_path: &PathBuf) -> Result<()> {
    match fs::rename(source_path, target_path) {
        Ok(()) => Ok(()),
        Err(_) => {
            copy_path_recursive(source_path, target_path)?;
            remove_path_recursive(source_path)
        }
    }
}

fn remove_path_recursive(path: &PathBuf) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to read metadata for {}", path.display()))?;

    if metadata.is_dir() {
        fs::remove_dir_all(path).with_context(|| format!("failed to delete {}", path.display()))?;
    } else {
        fs::remove_file(path).with_context(|| format!("failed to delete {}", path.display()))?;
    }

    Ok(())
}

fn normalize_permissions(permissions: &str) -> Result<u32> {
    let trimmed = permissions.trim();
    let normalized = trimmed.trim_start_matches('0');
    let normalized = if normalized.is_empty() {
        "000"
    } else {
        normalized
    };

    if normalized.len() != 3
        || normalized
            .chars()
            .any(|character| !('0'..='7').contains(&character))
    {
        bail!("Please enter a valid permission mode such as 644 or 755.");
    }

    u32::from_str_radix(normalized, 8).context("failed to parse permissions")
}

fn archive_file_name(name: &str) -> Result<String> {
    let name = validate_entry_name(name)?;

    if name.to_ascii_lowercase().ends_with(".zip") {
        Ok(name.to_string())
    } else {
        Ok(format!("{name}.zip"))
    }
}

fn archive_source_paths(
    volume_path: &PathBuf,
    paths: &[String],
    archive_path: &PathBuf,
) -> Result<Vec<String>> {
    if paths.is_empty() {
        bail!("Please select at least one file or directory to archive.");
    }

    let mut normalized_paths = Vec::with_capacity(paths.len());

    for path in paths {
        let normalized_path = normalize_relative_path(path)?;

        if normalized_path.is_empty() {
            bail!("The server root cannot be archived this way.");
        }

        let source_path = resolve_existing_server_path(volume_path, &normalized_path)?;

        if source_path == *archive_path {
            bail!("The archive cannot contain itself.");
        }

        normalized_paths.push(format!("./{normalized_path}"));
    }

    Ok(normalized_paths)
}

fn list_archive_entries(archive_path: &PathBuf) -> Result<Vec<String>> {
    let output = std::process::Command::new("unzip")
        .arg("-Z1")
        .arg(archive_path)
        .output()
        .context("failed to inspect archive contents")?;

    if !output.status.success() {
        bail!(command_output_message(
            &output.stderr,
            "The archive could not be inspected."
        ));
    }

    Ok(String::from_utf8(output.stdout)
        .context("The archive entry list was not valid UTF-8.")?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| line.trim_end_matches('/').to_string())
        .collect())
}

fn extract_top_level_entries(entries: &[String]) -> Result<Vec<String>> {
    let mut top_level = BTreeSet::new();

    for entry in entries {
        let normalized_entry = normalize_relative_path(entry)?;

        if normalized_entry.is_empty() {
            continue;
        }

        let first_segment = normalized_entry
            .split('/')
            .next()
            .context("failed to determine archive entry path")?;
        top_level.insert(first_segment.to_string());
    }

    Ok(top_level.into_iter().collect())
}

fn ensure_tree_contains_no_symlinks(path: &PathBuf) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to read metadata for {}", path.display()))?;

    if metadata.file_type().is_symlink() {
        bail!("Archives containing symlinks are not supported.");
    }

    if metadata.is_dir() {
        for entry in
            fs::read_dir(path).with_context(|| format!("failed to read {}", path.display()))?
        {
            let entry = entry.with_context(|| format!("failed to read {}", path.display()))?;
            ensure_tree_contains_no_symlinks(&entry.path())?;
        }
    }

    Ok(())
}

fn unique_temp_path(volume_path: &PathBuf, prefix: &str) -> Result<PathBuf> {
    for attempt in 0..16 {
        let candidate = volume_path.join(format!(
            "{prefix}-{}-{attempt}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("The system clock is invalid.")?
                .as_nanos()
        ));

        if !candidate.exists() {
            return Ok(candidate);
        }
    }

    bail!("Failed to reserve a temporary directory.")
}

fn command_output_message(stderr: &[u8], fallback: &str) -> String {
    let stderr = String::from_utf8_lossy(stderr).trim().to_string();

    if stderr.is_empty() {
        fallback.to_string()
    } else {
        stderr
    }
}

fn filesystem_entry_payload(
    volume_path: &PathBuf,
    entry_path: PathBuf,
) -> Result<Option<FilesystemEntryPayload>> {
    let symlink_metadata = fs::symlink_metadata(&entry_path)
        .with_context(|| format!("failed to read metadata for {}", entry_path.display()))?;

    if symlink_metadata.file_type().is_symlink() {
        return Ok(None);
    }

    let metadata = fs::metadata(&entry_path)
        .with_context(|| format!("failed to read metadata for {}", entry_path.display()))?;
    let relative_path = entry_path
        .strip_prefix(volume_path)
        .context("failed to determine relative path")?
        .to_string_lossy()
        .replace('\\', "/");
    let name = entry_path
        .file_name()
        .and_then(|name| name.to_str())
        .context("failed to determine file name")?
        .to_string();

    Ok(Some(FilesystemEntryPayload {
        kind: if metadata.is_dir() {
            "directory".to_string()
        } else {
            "file".to_string()
        },
        last_modified_at: modified_at(&metadata),
        name,
        path: relative_path,
        permissions: Some(format_permissions(metadata.permissions().mode())),
        size_bytes: if metadata.is_file() {
            Some(metadata.len())
        } else {
            None
        },
    }))
}

fn ensure_server_volume(server: &ManagedServerRecord) -> Result<PathBuf> {
    let volume_path = resolve_volume_path(server)?;
    fs::create_dir_all(&volume_path)
        .with_context(|| format!("failed to create {}", volume_path.display()))?;
    fs::canonicalize(&volume_path)
        .with_context(|| format!("failed to resolve {}", volume_path.display()))
}

fn normalize_relative_path(path: &str) -> Result<String> {
    if path.trim().is_empty() {
        return Ok(String::new());
    }

    let mut segments = Vec::new();

    for component in StdPath::new(path).components() {
        match component {
            std::path::Component::Normal(segment) => {
                segments.push(segment.to_string_lossy().to_string())
            }
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir
            | std::path::Component::RootDir
            | std::path::Component::Prefix(_) => {
                bail!("path must stay within the server volume");
            }
        }
    }

    Ok(segments.join("/"))
}

fn resolve_existing_server_path(volume_path: &PathBuf, relative_path: &str) -> Result<PathBuf> {
    if relative_path.is_empty() {
        return Ok(volume_path.clone());
    }

    let candidate = safe_join_relative(volume_path, relative_path)?;
    let resolved = fs::canonicalize(&candidate)
        .with_context(|| format!("failed to resolve {}", candidate.display()))?;

    if !resolved.starts_with(volume_path) {
        bail!("path must stay within the server volume");
    }

    Ok(resolved)
}

fn resolve_server_path_for_write(volume_path: &PathBuf, relative_path: &str) -> Result<PathBuf> {
    let candidate = safe_join_relative(volume_path, relative_path)?;

    if candidate.exists() {
        let resolved = fs::canonicalize(&candidate)
            .with_context(|| format!("failed to resolve {}", candidate.display()))?;

        if !resolved.starts_with(volume_path) {
            bail!("path must stay within the server volume");
        }

        return Ok(resolved);
    }

    let parent = candidate
        .parent()
        .context("path must stay within the server volume")?;
    let resolved_parent = fs::canonicalize(parent)
        .with_context(|| format!("failed to resolve {}", parent.display()))?;

    if !resolved_parent.starts_with(volume_path) {
        bail!("path must stay within the server volume");
    }

    let name = candidate
        .file_name()
        .context("path must stay within the server volume")?;

    Ok(resolved_parent.join(name))
}

fn validate_entry_name(name: &str) -> Result<&str> {
    let trimmed = name.trim();

    if trimmed.is_empty() {
        bail!("name cannot be empty");
    }

    let path = StdPath::new(trimmed);
    let mut components = path.components();

    match (components.next(), components.next()) {
        (Some(std::path::Component::Normal(_)), None) => Ok(trimmed),
        _ => bail!("name must be a single path segment"),
    }
}

fn parent_relative_path(path: &str) -> Option<String> {
    if path.is_empty() {
        return None;
    }

    let mut segments = path.split('/').collect::<Vec<_>>();
    segments.pop();

    Some(segments.join("/"))
}

fn modified_at(metadata: &fs::Metadata) -> Option<u64> {
    metadata
        .modified()
        .ok()?
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
}

fn format_permissions(mode: u32) -> String {
    format!("{:03o}", mode & 0o777)
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
    use std::os::unix::fs::symlink;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

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
    fn command_forward_script_targets_the_leaf_process_stdin() {
        let script = runtime_command_forward_script();

        assert!(script.contains("pid=1"));
        assert!(script.contains("for stat in /proc/[0-9]*/stat"));
        assert!(
            script.contains("printf '%s\\n' \"$SKYPORT_SERVER_COMMAND\" > \"/proc/$pid/fd/0\"")
        );
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
        let source = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/api.rs"));
        let removed_start_message = [
            "Start requested.",
            " The server will be booted by the lifecycle worker.",
        ]
        .concat();
        let removed_stop_message = ["Sending graceful stop", " command..."].concat();

        assert!(!source.contains(&removed_start_message));
        assert!(!source.contains(&removed_stop_message));
    }

    #[test]
    fn existing_filesystem_paths_reject_symlink_escapes() {
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

    #[test]
    fn write_paths_reject_symlink_escapes() {
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
