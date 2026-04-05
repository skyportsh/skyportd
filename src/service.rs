use anyhow::Result;
use tokio::select;
use tokio::time::{self, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::config::DaemonConfig;

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
                }
            }
        }

        Ok(())
    }
}
