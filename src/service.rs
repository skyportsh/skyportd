use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::select;
use tokio::sync::watch;
use tokio::time::{self, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::DaemonConfig;
use crate::config::NodeSection;
use crate::configuration;
use crate::server_registry::ServerRegistry;

#[derive(Serialize)]
struct HeartbeatRequest<'a> {
    uuid: &'a str,
    version: &'static str,
}

#[derive(Debug, Deserialize)]
struct HeartbeatResponse {
    configuration: Option<NodeSection>,
    #[serde(default)]
    server_ids: Vec<u64>,
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
                        Ok(response) => {
                            if let Some(configuration) = response.configuration {
                                if let Err(error) = self.apply_node_configuration(configuration) {
                                    warn!(error = %error, "failed to apply node configuration update");
                                }
                            }

                            if !response.server_ids.is_empty() {
                                self.reconcile_server_list(&response.server_ids, &client).await;
                            }
                        }
                        Err(error) => {
                            warn!(error = %error, "heartbeat request failed");
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn send_heartbeat(&self, client: &reqwest::Client) -> Result<HeartbeatResponse> {
        let daemon_secret = match self.config.panel.daemon_secret.as_deref() {
            Some(secret) if !secret.is_empty() => secret,
            _ => {
                warn!("missing daemon secret, skipping heartbeat");

                return Ok(HeartbeatResponse {
                    configuration: None,
                    server_ids: vec![],
                });
            }
        };

        let base_url = self.config.panel.url.trim_end_matches('/');

        let response = client
            .post(format!("{base_url}/api/daemon/heartbeat"))
            .bearer_auth(daemon_secret)
            .json(&HeartbeatRequest {
                uuid: &self.config.daemon.uuid,
                version: env!("CARGO_PKG_VERSION"),
            })
            .send()
            .await?
            .error_for_status()?;

        let payload = response
            .json::<HeartbeatResponse>()
            .await
            .context("failed to parse heartbeat response")?;

        Ok(payload)
    }

    async fn reconcile_server_list(&self, panel_server_ids: &[u64], client: &reqwest::Client) {
        let node_id = match self.config.panel.node_id {
            Some(id) => id,
            None => return,
        };

        let local_servers = match self.server_registry.list_servers_for_node(node_id) {
            Ok(servers) => servers,
            Err(_) => return,
        };

        // Remove servers that exist locally but not on the panel.
        for server in &local_servers {
            if !panel_server_ids.contains(&server.id) {
                info!(
                    server_id = server.id,
                    name = %server.name,
                    "removing orphaned server not found on panel"
                );
                let _ = self.server_registry.delete_server(server.id);
            }
        }

        // Request sync for servers that exist on the panel but not locally.
        let local_ids: std::collections::HashSet<u64> =
            local_servers.iter().map(|s| s.id).collect();
        let daemon_secret = match self.config.panel.daemon_secret.as_deref() {
            Some(secret) if !secret.is_empty() => secret,
            _ => return,
        };
        let base_url = self.config.panel.url.trim_end_matches('/');

        for &server_id in panel_server_ids {
            if local_ids.contains(&server_id) {
                continue;
            }

            info!(server_id, "server exists on panel but not locally, fetching");

            let url = format!(
                "{base_url}/api/daemon/servers/{server_id}/sync"
            );

            let result = client
                .get(&url)
                .bearer_auth(daemon_secret)
                .query(&[
                    ("uuid", self.config.daemon.uuid.as_str()),
                    ("version", env!("CARGO_PKG_VERSION")),
                ])
                .timeout(std::time::Duration::from_secs(10))
                .send()
                .await;

            match result {
                Ok(response) if response.status().is_success() => {
                    info!(server_id, "fetched server from panel, will be synced by next push");
                }
                Ok(response) => {
                    warn!(server_id, status = %response.status(), "failed to fetch server from panel");
                }
                Err(error) => {
                    warn!(server_id, error = %error, "failed to reach panel for server sync");
                }
            }
        }
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
