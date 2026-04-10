use anyhow::{Context, Result};
use axum::extract::{Json, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use serde::Deserialize;
use tracing::{info, warn};

use crate::config::{project_root, managed_server_volume_path, DaemonConfig};
use super::{
    ApiErrorResponse, ApiState, ApiSuccessResponse, WsAuthorizationQuery,
    authorize_request, ensure_compatible_request, error_response, internal_error,
};

#[derive(Debug, Deserialize)]
pub(super) struct BackupRequest {
    action: String,
    backup_id: u64,
    backup_uuid: String,
    backup_name: String,
    panel_version: String,
    uuid: String,
}

pub(super) async fn handle_backup(
    Path(server_id): Path<u64>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<BackupRequest>,
) -> std::result::Result<Json<ApiSuccessResponse>, (StatusCode, Json<ApiErrorResponse>)> {
    let config = state.config_rx.borrow().clone();
    authorize_request(&config, &headers)?;
    ensure_compatible_request(&config, &payload.uuid, &payload.panel_version)?;

    let server = state
        .server_registry
        .get_server(server_id)
        .map_err(internal_error)?
        .ok_or_else(|| {
            error_response(
                StatusCode::NOT_FOUND,
                "Server not found.".to_string(),
            )
        })?;

    let volume_path = managed_server_volume_path(server.id).map_err(internal_error)?;
    let backups_dir = project_root()
        .map_err(internal_error)?
        .join("backups");
    std::fs::create_dir_all(&backups_dir).map_err(|e| {
        internal_error(anyhow::anyhow!("failed to create backups directory: {e}"))
    })?;

    let backup_path = backups_dir.join(format!("{}.tar.gz", payload.backup_uuid));

    match payload.action.as_str() {
        "create" => {
            info!(
                server_id = server.id,
                backup_id = payload.backup_id,
                name = %payload.backup_name,
                "creating backup"
            );

            let _ = state.server_registry.append_console_message(
                server.id,
                "system",
                &format!("Creating backup '{}'...", payload.backup_name),
            );

            // Create a tar.gz of the volume
            let output = tokio::process::Command::new("tar")
                .arg("czf")
                .arg(&backup_path)
                .arg("-C")
                .arg(&volume_path)
                .arg(".")
                .output()
                .await
                .map_err(|e| internal_error(anyhow::anyhow!("failed to create backup: {e}")))?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                warn!(server_id = server.id, error = %stderr, "backup creation failed");

                // Notify panel of failure
                let _ = notify_backup_status(
                    &config,
                    server.id,
                    payload.backup_id,
                    "failed",
                    0,
                    Some(&stderr),
                )
                .await;

                return Err(error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Backup creation failed: {stderr}"),
                ));
            }

            let size = std::fs::metadata(&backup_path)
                .map(|m| m.len())
                .unwrap_or(0);

            let _ = state.server_registry.append_console_message(
                server.id,
                "system",
                &format!(
                    "Backup '{}' completed ({})",
                    payload.backup_name,
                    format_backup_size(size)
                ),
            );

            let _ = notify_backup_status(&config, server.id, payload.backup_id, "completed", size, None).await;

            info!(server_id = server.id, backup_id = payload.backup_id, size, "backup created");
        }
        "restore" => {
            info!(
                server_id = server.id,
                backup_id = payload.backup_id,
                "restoring backup"
            );

            let _ = state.server_registry.append_console_message(
                server.id,
                "system",
                &format!("Restoring backup '{}'...", payload.backup_name),
            );

            if !backup_path.exists() {
                return Err(error_response(
                    StatusCode::NOT_FOUND,
                    "Backup file not found on this node.".to_string(),
                ));
            }

            // Clear the volume directory
            if volume_path.exists() {
                for entry in std::fs::read_dir(&volume_path).map_err(|e| {
                    internal_error(anyhow::anyhow!("failed to read volume: {e}"))
                })? {
                    let entry = entry.map_err(|e| {
                        internal_error(anyhow::anyhow!("failed to read entry: {e}"))
                    })?;
                    let path = entry.path();
                    if path.is_dir() {
                        let _ = std::fs::remove_dir_all(&path);
                    } else {
                        let _ = std::fs::remove_file(&path);
                    }
                }
            }

            // Extract the backup
            let output = tokio::process::Command::new("tar")
                .arg("xzf")
                .arg(&backup_path)
                .arg("-C")
                .arg(&volume_path)
                .output()
                .await
                .map_err(|e| internal_error(anyhow::anyhow!("failed to restore backup: {e}")))?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                let _ = notify_backup_status(
                    &config,
                    server.id,
                    payload.backup_id,
                    "failed",
                    0,
                    Some(&format!("Restore failed: {stderr}")),
                )
                .await;

                return Err(error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Restore failed: {stderr}"),
                ));
            }

            let _ = notify_backup_status(
                &config,
                server.id,
                payload.backup_id,
                "completed",
                0,
                None,
            )
            .await;

            let _ = state.server_registry.append_console_message(
                server.id,
                "system",
                &format!("Backup '{}' restored successfully.", payload.backup_name),
            );

            info!(server_id = server.id, backup_id = payload.backup_id, "backup restored");
        }
        "delete" => {
            if backup_path.exists() {
                let _ = std::fs::remove_file(&backup_path);
            }
            info!(server_id = server.id, backup_id = payload.backup_id, "backup deleted");
        }
        _ => {
            return Err(error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("Unknown backup action: {}", payload.action),
            ));
        }
    }

    Ok(Json(ApiSuccessResponse { ok: true }))
}

pub(super) async fn download_backup(
    Path((_server_id, backup_uuid)): Path<(u64, String)>,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(_query): Query<WsAuthorizationQuery>,
) -> std::result::Result<axum::response::Response, (StatusCode, Json<ApiErrorResponse>)> {
    use axum::body::Body;
    use axum::response::IntoResponse;
    use tokio_util::io::ReaderStream;

    let config = state.config_rx.borrow().clone();
    authorize_request(&config, &headers)?;

    let backups_dir = project_root().map_err(internal_error)?.join("backups");
    let backup_path = backups_dir.join(format!("{backup_uuid}.tar.gz"));

    if !backup_path.exists() {
        return Err(error_response(
            StatusCode::NOT_FOUND,
            "Backup file not found.".to_string(),
        ));
    }

    let file = tokio::fs::File::open(&backup_path)
        .await
        .map_err(|e| internal_error(anyhow::anyhow!("failed to open backup: {e}")))?;

    let _size = file
        .metadata()
        .await
        .map(|m| m.len())
        .unwrap_or(0);

    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    Ok((
        [
            (http::header::CONTENT_TYPE, "application/gzip"),
            (
                http::header::CONTENT_DISPOSITION,
                &format!("attachment; filename=\"{backup_uuid}.tar.gz\""),
            ),
        ],
        body,
    )
        .into_response())
}

pub(super) fn format_backup_size(bytes: u64) -> String {
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    if bytes < 1024 * 1024 {
        return format!("{:.1} KB", bytes as f64 / 1024.0);
    }
    if bytes < 1024 * 1024 * 1024 {
        return format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0));
    }
    format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
}

pub(super) async fn notify_backup_status(
    config: &DaemonConfig,
    server_id: u64,
    backup_id: u64,
    status: &str,
    size_bytes: u64,
    error: Option<&str>,
) -> Result<()> {
    let panel_url = config.panel.url.trim_end_matches('/');
    let daemon_secret = config
        .panel
        .daemon_secret
        .as_deref()
        .context("missing daemon secret")?;

    let daemon_uuid = config
        .daemon
        .uuid
        .as_str();

    reqwest::Client::new()
        .post(format!("{panel_url}/api/daemon/servers/{server_id}/runtime"))
        .bearer_auth(daemon_secret)
        .json(&serde_json::json!({
            "uuid": daemon_uuid,
            "version": env!("CARGO_PKG_VERSION"),
            "status": null,
            "backup_id": backup_id,
            "backup_status": status,
            "backup_size_bytes": size_bytes,
            "backup_error": error,
        }))
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    Ok(())
}
