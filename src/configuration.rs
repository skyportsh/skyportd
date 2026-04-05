use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::config::DaemonConfig;

#[derive(Debug, Serialize)]
struct ConfigurationRequest<'a> {
    token: &'a str,
    uuid: &'a str,
    version: &'static str,
    hostname: String,
    reported_ip: Option<String>,
    os: &'static str,
    arch: &'static str,
    capabilities: ConfigurationCapabilities,
    docker: DockerMetadata,
}

#[derive(Debug, Serialize)]
struct ConfigurationCapabilities {
    docker: bool,
    metrics: bool,
    sftp: bool,
}

#[derive(Debug, Serialize)]
struct DockerMetadata {
    version: &'static str,
}

#[derive(Debug, Deserialize)]
struct ConfigurationResponse {
    daemon_secret: String,
    daemon_uuid: String,
    heartbeat_interval_seconds: u64,
    node_id: u64,
    panel_time: String,
    task_poll_interval_seconds: u64,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    message: String,
}

pub async fn ensure_configured(mut config: DaemonConfig) -> Result<DaemonConfig> {
    if config.panel.daemon_secret.as_deref().is_some_and(|secret| !secret.is_empty()) {
        return Ok(config);
    }

    let configuration_token = config
        .panel
        .configuration_token
        .as_deref()
        .filter(|token| !token.is_empty())
        .context("daemon is not configured and no panel.configuration_token is configured")?;

    let request = ConfigurationRequest {
        token: configuration_token,
        uuid: &config.daemon.uuid,
        version: env!("CARGO_PKG_VERSION"),
        hostname: hostname::get()
            .context("failed to resolve hostname")?
            .to_string_lossy()
            .into_owned(),
        reported_ip: None,
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        capabilities: ConfigurationCapabilities {
            docker: true,
            metrics: true,
            sftp: true,
        },
        docker: DockerMetadata {
            version: "unknown",
        },
    };

    let base_url = config.panel.url.trim_end_matches('/');
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{base_url}/api/daemon/enroll"))
        .json(&request)
        .send()
        .await
        .context("failed to call panel configuration endpoint")?;

    if !response.status().is_success() {
        let status = response.status();
        let message = response
            .json::<ErrorResponse>()
            .await
            .map(|payload| payload.message)
            .unwrap_or_else(|_| "daemon configuration failed".to_string());

        bail!("panel configuration failed with status {status}: {message}");
    }

    let payload = response
        .json::<ConfigurationResponse>()
        .await
        .context("failed to parse panel configuration response")?;

    config.persist_configuration(
        payload.node_id,
        &payload.daemon_secret,
        &payload.daemon_uuid,
    )?;
    config.panel.daemon_secret = Some(payload.daemon_secret);
    config.panel.node_id = Some(payload.node_id);
    config.panel.configuration_token = None;
    config.daemon.uuid = payload.daemon_uuid;

    info!(
        node_id = payload.node_id,
        heartbeat_interval_seconds = payload.heartbeat_interval_seconds,
        task_poll_interval_seconds = payload.task_poll_interval_seconds,
        panel_time = %payload.panel_time,
        "daemon configuration completed"
    );

    Ok(config)
}
