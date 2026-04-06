use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::select;
use tokio::sync::watch;
use tokio::time::{self, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::{DaemonConfig, NodeSection};
use crate::configuration;
use crate::server_registry::ServerRegistry;

#[derive(Serialize)]
struct HeartbeatRequest<'a> {
    uuid: &'a str,
    version: &'static str,
    servers: Vec<HeartbeatServerStatus>,
}

#[derive(Serialize)]
struct HeartbeatServerStatus {
    id: u64,
    status: String,
}

#[derive(Debug, Deserialize)]
struct HeartbeatResponse {
    configuration: Option<NodeSection>,
}

pub struct HeartbeatService {
    config: DaemonConfig,
    config_updates: watch::Sender<DaemonConfig>,
    server_registry: ServerRegistry,
    cancellation: CancellationToken,
}

impl HeartbeatService {
    pub fn new(
        config: DaemonConfig,
        config_updates: watch::Sender<DaemonConfig>,
        server_registry: ServerRegistry,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            config,
            config_updates,
            server_registry,
            cancellation,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let mut interval = time::interval(self.config.daemon.tick_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let client = reqwest::Client::new();

        info!(
            tick_interval_seconds = self.config.daemon.tick_interval.as_secs(),
            "heartbeat service started"
        );

        loop {
            select! {
                _ = self.cancellation.cancelled() => {
                    info!("heartbeat service stopping");
                    break;
                }
                _ = interval.tick() => {
                    debug!(uuid = %self.config.daemon.uuid, "heartbeat tick");

                    match self.send_heartbeat(&client).await {
                        Ok(Some(configuration)) => {
                            if let Err(error) = self.apply_node_configuration(configuration) {
                                warn!(error = %error, "failed to apply node configuration update");
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            warn!(error = %error, "heartbeat request failed");
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn send_heartbeat(&self, client: &reqwest::Client) -> Result<Option<NodeSection>> {
        let daemon_secret = match self.config.panel.daemon_secret.as_deref() {
            Some(secret) if !secret.is_empty() => secret,
            _ => {
                warn!("missing daemon secret, skipping heartbeat");

                return Ok(None);
            }
        };

        let base_url = self.config.panel.url.trim_end_matches('/');

        let response = client
            .post(format!("{base_url}/api/daemon/heartbeat"))
            .bearer_auth(daemon_secret)
            .json(&HeartbeatRequest {
                uuid: &self.config.daemon.uuid,
                version: env!("CARGO_PKG_VERSION"),
                servers: self.server_statuses()?,
            })
            .send()
            .await?
            .error_for_status()?;

        let payload = response
            .json::<HeartbeatResponse>()
            .await
            .context("failed to parse heartbeat response")?;

        Ok(payload.configuration)
    }

    fn server_statuses(&self) -> Result<Vec<HeartbeatServerStatus>> {
        let Some(node_id) = self.config.panel.node_id else {
            return Ok(Vec::new());
        };

        Ok(self
            .server_registry
            .list_servers_for_node(node_id)?
            .into_iter()
            .map(|server| HeartbeatServerStatus {
                id: server.id,
                status: server.status,
            })
            .collect())
    }

    fn apply_node_configuration(&mut self, mut configuration: NodeSection) -> Result<()> {
        if let Some(current) = &self.config.node {
            configuration.tls_cert_path = current.tls_cert_path.clone();
            configuration.tls_key_path = current.tls_key_path.clone();
        }

        if self.config.node.as_ref() == Some(&configuration) {
            return Ok(());
        }

        self.config.node = Some(configuration);
        configuration::ensure_node_runtime_configuration(&mut self.config)?;

        if let Some(node) = &self.config.node {
            self.config.persist_node_configuration(node)?;

            info!(
                name = %node.name,
                fqdn = %node.fqdn,
                daemon_port = node.daemon_port,
                sftp_port = node.sftp_port,
                use_ssl = node.use_ssl,
                "node configuration updated"
            );
        }

        let _ = self.config_updates.send(self.config.clone());

        Ok(())
    }
}
