use anyhow::Result;
use serde::Serialize;
use tokio::select;
use tokio::time::{self, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::DaemonConfig;

#[derive(Serialize)]
struct HeartbeatRequest<'a> {
    uuid: &'a str,
    version: &'static str,
}

pub struct HeartbeatService {
    config: DaemonConfig,
    cancellation: CancellationToken,
}

impl HeartbeatService {
    pub fn new(config: DaemonConfig, cancellation: CancellationToken) -> Self {
        Self {
            config,
            cancellation,
        }
    }

    pub async fn run(self) -> Result<()> {
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

                    if let Err(error) = self.send_heartbeat(&client).await {
                        warn!(error = %error, "heartbeat request failed");
                    }
                }
            }
        }

        Ok(())
    }

    async fn send_heartbeat(&self, client: &reqwest::Client) -> Result<()> {
        let daemon_secret = match self.config.panel.daemon_secret.as_deref() {
            Some(secret) if !secret.is_empty() => secret,
            _ => {
                warn!("missing daemon secret, skipping heartbeat");

                return Ok(());
            }
        };

        let base_url = self.config.panel.url.trim_end_matches('/');

        client
            .post(format!("{base_url}/api/daemon/heartbeat"))
            .bearer_auth(daemon_secret)
            .json(&HeartbeatRequest {
                uuid: &self.config.daemon.uuid,
                version: env!("CARGO_PKG_VERSION"),
            })
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }
}
