mod api;
mod app;
mod config;
mod configuration;
mod logging;
mod server_registry;
mod service;
mod shutdown;

use anyhow::Result;

use crate::app::DaemonApp;
use crate::config::DaemonConfig;

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("failed to install rustls crypto provider"))?;

    let config = DaemonConfig::load()?;
    logging::init(&config.logging)?;

    let config = configuration::ensure_configured(config).await?;

    let app = DaemonApp::new(config);

    app.run().await
}
