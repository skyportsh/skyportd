mod app;
mod config;
mod logging;
mod service;
mod shutdown;

use anyhow::Result;

use crate::app::DaemonApp;
use crate::config::DaemonConfig;

#[tokio::main]
async fn main() -> Result<()> {
    let config = DaemonConfig::load()?;
    logging::init(&config.logging)?;

    let app = DaemonApp::new(config);

    app.run().await
}
