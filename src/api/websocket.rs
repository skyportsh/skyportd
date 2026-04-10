use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use axum::extract::ws::{Message, WebSocket};
use ring::hmac;
use serde_json::{Value, json};
use tokio::select;
use tokio::time::{self, MissedTickBehavior};
use tracing::warn;

use crate::server_registry::{ConsoleMessageRecord, ManagedServerRecord, ServerRegistry};
use super::{
    ApiErrorResponse, ApiState, PowerSignal, WsAuthorizationQuery, WsClientEvent, WsTokenPayload,
    apply_power_signal, container_is_running, current_resource_snapshot, ensure_compatible_request,
    error_response, load_managed_server, send_command_to_server,
};
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::Response;
use axum::extract::ws::WebSocketUpgrade;
use http::StatusCode;
use axum::Json;

pub(super) async fn server_ws(
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

pub(super) async fn handle_server_ws(
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

pub(super) async fn send_ws_event(socket: &mut WebSocket, event: &str, args: Vec<Value>) -> Result<()> {
    socket
        .send(Message::Text(ws_event_payload(event, args).into()))
        .await
        .with_context(|| format!("failed to send {event} websocket event"))
}

pub(super) async fn send_status_event(socket: &mut WebSocket, status: &str) -> Result<()> {
    send_ws_event(socket, "status", vec![Value::String(status.to_string())]).await
}

pub(super) async fn send_console_event(socket: &mut WebSocket, message: &ConsoleMessageRecord) -> Result<()> {
    send_ws_event(
        socket,
        console_event_name(&message.source),
        vec![Value::String(message.message.clone())],
    )
    .await
}

pub(super) async fn send_stats_event(socket: &mut WebSocket, server: &ManagedServerRecord) -> Result<()> {
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

pub(super) async fn send_install_state_event(
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

pub(super) async fn handle_ws_client_event(
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

pub fn parse_ws_client_event(payload: &str) -> Result<WsClientEvent> {
    serde_json::from_str::<WsClientEvent>(payload).context("Invalid websocket payload.")
}

pub fn authenticate_ws_event(
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

pub fn runtime_status_after_start_request(is_running: bool) -> &'static str {
    if is_running { "running" } else { "starting" }
}

pub fn parse_power_signal(signal: Option<&String>) -> Result<PowerSignal> {
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

pub(super) fn hex_decode(value: &str) -> Result<Vec<u8>> {
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

pub fn hex_encode(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn ws_event_payload(event: &str, args: Vec<Value>) -> String {
    json!({
        "event": event,
        "args": args,
    })
    .to_string()
}

pub fn console_event_name(source: &str) -> &'static str {
    match source {
        "install" => "install output",
        "system" => "daemon message",
        _ => "console output",
    }
}

