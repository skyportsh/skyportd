use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use axum_server::Handle;
use axum_server::tls_rustls::RustlsConfig;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::process::Command;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{DaemonConfig, NodeSection, project_root};
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
    token: String,
    uuid: String,
    panel_version: String,
}

#[derive(Debug, Deserialize)]
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
    volume_path: String,
    created_at: String,
    updated_at: String,
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
        .route("/api/daemon/servers/{server_id}/power", post(power_server))
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

    let server = ManagedServerRecord {
        id: payload.server.id,
        node_id: payload.server.node_id,
        name: payload.server.name,
        status: payload.server.status,
        volume_path: payload.server.volume_path,
        created_at: payload.server.created_at,
        updated_at: payload.server.updated_at,
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

    match payload.signal {
        PowerSignal::Start => {
            if container_is_running(server.id)
                .await
                .map_err(internal_error)?
            {
                state
                    .server_registry
                    .append_console_message(
                        server.id,
                        "system",
                        "Start requested, but the server is already running.",
                    )
                    .map_err(internal_error)?;
            } else {
                state
                    .server_registry
                    .set_server_runtime(server.id, "offline", None, None)
                    .map_err(internal_error)?;
                state
                    .server_registry
                    .append_console_message(
                        server.id,
                        "system",
                        "Start requested. The server will be booted by the lifecycle worker.",
                    )
                    .map_err(internal_error)?;
            }
        }
        PowerSignal::Stop => {
            graceful_stop(&state.server_registry, &server)
                .await
                .map_err(internal_error)?;
        }
        PowerSignal::Restart => {
            graceful_stop(&state.server_registry, &server)
                .await
                .map_err(internal_error)?;
            state
                .server_registry
                .set_server_runtime(server.id, "offline", None, None)
                .map_err(internal_error)?;
            state
                .server_registry
                .append_console_message(
                    server.id,
                    "system",
                    "Restart requested. The server will be started again by the lifecycle worker.",
                )
                .map_err(internal_error)?;
        }
        PowerSignal::Kill => {
            kill_server(&state.server_registry, &server)
                .await
                .map_err(internal_error)?;
        }
        PowerSignal::Reinstall => {
            remove_runtime_and_install_containers(server.id)
                .await
                .map_err(internal_error)?;
            clear_server_volume(&server).await.map_err(internal_error)?;
            state
                .server_registry
                .set_server_runtime(server.id, "pending", None, None)
                .map_err(internal_error)?;
            state
                .server_registry
                .append_console_message(server.id, "system", "Reinstall requested. Existing files were cleared and the lifecycle worker will reinstall the server.")
                .map_err(internal_error)?;
        }
    }

    server = load_managed_server(&state.server_registry, &config, server_id)?;
    info!(server_id = server.id, status = %server.status, "power action processed");

    Ok(Json(ApiSuccessResponse { ok: true }))
}

async fn server_ws(
    ws: WebSocketUpgrade,
    Path(server_id): Path<u64>,
    Query(query): Query<WsAuthorizationQuery>,
    State(state): State<ApiState>,
) -> std::result::Result<Response, (StatusCode, Json<ApiErrorResponse>)> {
    let config = state.config_rx.borrow().clone();

    authorize_query(&config, &query)?;
    ensure_compatible_request(&config, &query.uuid, &query.panel_version)?;
    load_managed_server(&state.server_registry, &config, server_id)?;

    Ok(ws.on_upgrade(move |socket| handle_server_ws(socket, state.server_registry, server_id)))
}

async fn handle_server_ws(mut socket: WebSocket, registry: ServerRegistry, server_id: u64) {
    if send_ws_event(&mut socket, "auth success", Vec::new())
        .await
        .is_err()
    {
        return;
    }

    let mut last_id = 0_u64;
    let mut last_status: Option<String> = None;

    if let Ok(Some(server)) = registry.get_server(server_id) {
        last_status = Some(server.status.clone());

        if send_status_event(&mut socket, &server.status)
            .await
            .is_err()
        {
            return;
        }

        let payload = match current_resource_snapshot(&server).await {
            Ok(payload) => payload,
            Err(error) => json!({
                "server": server_id,
                "running": false,
                "state": server.status,
                "error": error.to_string(),
            }),
        };

        if send_ws_event(&mut socket, "stats", vec![payload])
            .await
            .is_err()
        {
            return;
        }
    }

    if let Ok(backlog) = registry.console_messages_since(server_id, 0, 200) {
        for message in backlog {
            last_id = message.id;
            if send_console_event(&mut socket, &message).await.is_err() {
                return;
            }
        }
    }

    let mut console_interval = time::interval(Duration::from_millis(500));
    console_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut stats_interval = time::interval(Duration::from_secs(2));
    stats_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut status_interval = time::interval(Duration::from_millis(500));
    status_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = console_interval.tick() => {
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
            _ = stats_interval.tick() => {
                let server = match registry.get_server(server_id) {
                    Ok(Some(server)) => server,
                    _ => return,
                };

                let payload = match current_resource_snapshot(&server).await {
                    Ok(payload) => payload,
                    Err(error) => json!({
                        "server": server_id,
                        "running": false,
                        "state": server.status,
                        "error": error.to_string(),
                    }),
                };

                if send_ws_event(&mut socket, "stats", vec![payload]).await.is_err() {
                    return;
                }
            }
            _ = status_interval.tick() => {
                let server = match registry.get_server(server_id) {
                    Ok(Some(server)) => server,
                    _ => return,
                };

                if last_status.as_deref() != Some(server.status.as_str()) {
                    last_status = Some(server.status.clone());

                    if send_status_event(&mut socket, &server.status).await.is_err() {
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
                    Some(Ok(Message::Text(_))) => {}
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

fn ws_event_payload(event: &str, args: Vec<Value>) -> String {
    json!({
        "event": event,
        "args": args,
    })
    .to_string()
}

fn console_event_name(source: &str) -> &'static str {
    if source == "install" {
        "install output"
    } else {
        "console output"
    }
}

async fn current_resource_snapshot(server: &ManagedServerRecord) -> Result<Value> {
    let name = runtime_container_name(server.id);
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
            "server": server.id,
            "running": false,
            "state": server.status,
        }));
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if stdout.is_empty() {
        return Ok(json!({
            "server": server.id,
            "running": false,
            "state": server.status,
        }));
    }

    let stats: Value =
        serde_json::from_str(&stdout).context("failed to parse docker stats payload")?;

    Ok(json!({
        "server": server.id,
        "running": true,
        "state": server.status,
        "stats": stats,
    }))
}

async fn graceful_stop(registry: &ServerRegistry, server: &ManagedServerRecord) -> Result<()> {
    if !container_is_running(server.id).await? {
        registry.set_server_runtime(server.id, "offline", None, None)?;
        registry.append_console_message(
            server.id,
            "system",
            "Stop requested, but the server was already offline.",
        )?;
        return Ok(());
    }

    registry.append_console_message(server.id, "system", "Sending graceful stop command...")?;

    let stop_command = server.cargo.config_stop.trim();
    if !stop_command.is_empty() {
        let _ = docker_command()
            .arg("exec")
            .arg(runtime_container_name(server.id))
            .arg("sh")
            .arg("-lc")
            .arg(stop_command)
            .output()
            .await;
    }

    for _ in 0..15 {
        if !container_is_running(server.id).await? {
            remove_runtime_and_install_containers(server.id).await?;
            registry.set_server_runtime(server.id, "offline", None, None)?;
            registry.append_console_message(server.id, "system", "Server stopped successfully.")?;
            return Ok(());
        }

        time::sleep(Duration::from_secs(1)).await;
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
    registry.set_server_runtime(server.id, "offline", None, None)?;
    registry.append_console_message(server.id, "system", "Server stopped successfully.")?;

    Ok(())
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

fn authorize_query(
    config: &DaemonConfig,
    query: &WsAuthorizationQuery,
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

    if query.token.trim() != expected {
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
    Ok(project_root()?.join(&server.volume_path))
}

fn internal_error(error: anyhow::Error) -> (StatusCode, Json<ApiErrorResponse>) {
    error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

fn error_response(status: StatusCode, message: String) -> (StatusCode, Json<ApiErrorResponse>) {
    (status, Json(ApiErrorResponse { message }))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

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

    fn expect_json(actual: &str, expected: Value) {
        let parsed: Value = serde_json::from_str(actual).expect("valid websocket json");

        assert_eq!(parsed, expected);
    }
}
