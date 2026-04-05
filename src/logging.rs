use anyhow::Result;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::UtcTime;

use crate::config::{LogFormat, LoggingSection};

pub fn init(config: &LoggingSection) -> Result<()> {
    let env_filter =
        EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new(config.level.as_str()))?;

    let builder = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_timer(UtcTime::rfc_3339());

    match config.format {
        LogFormat::Pretty => builder.pretty().try_init().map_err(anyhow::Error::msg)?,
        LogFormat::Json => builder.json().try_init().map_err(anyhow::Error::msg)?,
    }

    Ok(())
}
