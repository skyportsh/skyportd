use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use config::{Config, Environment, File};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct DaemonConfig {
    pub daemon: DaemonSection,
    pub logging: LoggingSection,
    pub runtime: RuntimeSection,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DaemonSection {
    pub name: String,
    pub uuid: String,
    #[serde(with = "humantime_serde")]
    pub tick_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub shutdown_timeout: Duration,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingSection {
    pub level: String,
    pub format: LogFormat,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    Pretty,
    Json,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeSection {
    pub worker_threads: usize,
}

impl DaemonConfig {
    pub fn load() -> Result<Self> {
        let root = project_root()?;
        let default_path = root.join("config/default.toml");
        let local_path = root.join("config/local.toml");

        let builder = Config::builder()
            .add_source(File::from(default_path.clone()))
            .add_source(File::from(local_path).required(false))
            .add_source(Environment::with_prefix("SKYPORT_DAEMON").separator("__"));

        let config = builder
            .build()
            .with_context(|| format!("failed to build config from {}", default_path.display()))?;

        config
            .try_deserialize::<Self>()
            .context("failed to deserialize daemon config")
    }
}

fn project_root() -> Result<PathBuf> {
    std::env::current_dir()
        .context("failed to resolve current directory")
        .map(|dir| normalize_root(&dir))
}

fn normalize_root(path: &Path) -> PathBuf {
    if path.ends_with("src") {
        path.parent().unwrap_or(path).to_path_buf()
    } else {
        path.to_path_buf()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_default_config() {
        let raw = include_str!("../config/default.toml");

        let config = Config::builder()
            .add_source(File::from_str(raw, config::FileFormat::Toml))
            .build()
            .expect("default config should build")
            .try_deserialize::<DaemonConfig>()
            .expect("default config should deserialize");

        assert_eq!(config.daemon.name, "skyportd");
        assert_eq!(config.daemon.uuid, "00000000-0000-0000-0000-000000000000");
        assert_eq!(config.daemon.tick_interval, Duration::from_secs(5));
        assert_eq!(config.daemon.shutdown_timeout, Duration::from_secs(30));
        assert_eq!(config.logging.level, "info");
        assert_eq!(config.runtime.worker_threads, 0);
    }
}
