use anyhow::{Context, Result};
use tokio::select;
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

use crate::api::ApiService;
use crate::config::DaemonConfig;
use crate::server_lifecycle::ServerLifecycleService;
use crate::server_registry::ServerRegistry;
use crate::service::HeartbeatService;
use crate::sftp::SftpService;
use crate::shutdown;

const ORANGE: &str = "\x1b[38;2;240;90;36m";
const GRAY: &str = "\x1b[38;2;120;120;120m";
const WHITE: &str = "\x1b[38;2;255;255;255m";
const RESET: &str = "\x1b[0m";
const ASCII_ART: &str = r#"         __                          __      __
   _____/ /____  ______  ____  _____/ /_____/ /
  / ___/ //_/ / / / __ \/ __ \/ ___/ __/ __  / 
 (__  ) ,< / /_/ / /_/ / /_/ / /  / /_/ /_/ /  
/____/_/|_|\__, / .___/\____/_/   \__/\__,_/   
          /____/_/"#;

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

        print_startup_banner();

        info!(
            daemon = %self.config.daemon.name,
            uuid = %self.config.daemon.uuid,
            worker_threads = self.config.runtime.worker_threads,
            "daemon starting"
        );

        let cancellation = CancellationToken::new();
        let tracker = TaskTracker::new();
        let (config_updates, config_rx) = watch::channel(self.config.clone());
        let server_registry = ServerRegistry::new_default()?;
        server_registry.initialize()?;

        info!(db_path = %server_registry.db_path().display(), "server registry initialized");

        tracker.spawn({
            let service = HeartbeatService::new(
                self.config.clone(),
                config_updates.clone(),
                cancellation.child_token(),
            );

            async move { service.run().await.context("heartbeat service stopped") }
        });

        tracker.spawn({
            let service = ApiService::new(
                config_rx.clone(),
                config_updates,
                server_registry.clone(),
                cancellation.child_token(),
            );

            async move { service.run().await.context("api service stopped") }
        });

        tracker.spawn({
            let service = SftpService::new(
                config_rx.clone(),
                cancellation.child_token(),
            );

            async move { service.run().await.context("sftp service stopped") }
        });

        tracker.spawn({
            let service =
                ServerLifecycleService::new(config_rx, server_registry, cancellation.child_token());

            async move {
                service
                    .run()
                    .await
                    .context("server lifecycle service stopped")
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
                info!(
                    timeout_seconds = shutdown_timeout.as_secs(),
                    "daemon shutdown completed"
                );
                Ok(())
            }
            Err(_) => {
                warn!(
                    timeout_seconds = shutdown_timeout.as_secs(),
                    "daemon shutdown timed out"
                );
                Ok(())
            }
        }
    }
}

fn print_startup_banner() {
    println!();

    for line in ASCII_ART.lines() {
        println!(
            "{white}{line}{reset}",
            white = WHITE,
            line = line,
            reset = RESET
        );
    }

    println!(
        "{gray}              skyportd {orange}{version}{reset}",
        gray = GRAY,
        orange = ORANGE,
        version = env!("CARGO_PKG_VERSION"),
        reset = RESET,
    );
    println!(
        "{gray}              Copyright © 2026 Skyport{reset}",
        gray = GRAY,
        reset = RESET,
    );
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_banner_includes_version_and_copyright() {
        let banner = startup_banner_text();

        assert!(banner.contains("skyportd"));
        assert!(banner.contains(env!("CARGO_PKG_VERSION")));
        assert!(banner.contains("Copyright © 2026 Skyport"));
    }
}

#[cfg(test)]
fn startup_banner_text() -> String {
    format!(
        "{ascii}\nskyportd {version}\nCopyright © 2026 Skyport",
        ascii = ASCII_ART,
        version = env!("CARGO_PKG_VERSION"),
    )
}
