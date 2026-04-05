use anyhow::{Context, Result};
use tokio::select;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

use crate::config::DaemonConfig;
use crate::service::HeartbeatService;
use crate::shutdown;

pub struct DaemonApp {
    config: DaemonConfig,
}

impl DaemonApp {
    pub fn new(config: DaemonConfig) -> Self {
        Self { config }
    }

    pub async fn run(self) -> Result<()> {
        std::panic::set_hook(Box::new(|panic_info| {
            error!(details = %panic_info, "unhandled panic");
        }));

        info!(
            daemon = %self.config.daemon.name,
            uuid = %self.config.daemon.uuid,
            worker_threads = self.config.runtime.worker_threads,
            "daemon starting"
        );

        let cancellation = CancellationToken::new();
        let tracker = TaskTracker::new();

        tracker.spawn({
            let service = HeartbeatService::new(self.config.clone(), cancellation.child_token());

            async move {
                service.run().await.context("heartbeat service stopped")
            }
        });

        tracker.close();

        select! {
            signal = shutdown::wait_for_shutdown() => {
                signal?;
                info!("shutdown signal received");
            }
            _ = tracker.wait() => {
                warn!("all tracked tasks exited before shutdown signal");
            }
        }

        cancellation.cancel();

        let shutdown_timeout = self.config.daemon.shutdown_timeout;
        match timeout(shutdown_timeout, tracker.wait()).await {
            Ok(_) => {
                info!(timeout_seconds = shutdown_timeout.as_secs(), "daemon shutdown completed");
                Ok(())
            }
            Err(_) => {
                warn!(timeout_seconds = shutdown_timeout.as_secs(), "daemon shutdown timed out");
                Ok(())
            }
        }
    }
}
