use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use config::{Config, Environment, File};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct DaemonConfig {
    pub daemon: DaemonSection,
    pub panel: PanelSection,
    pub logging: LoggingSection,
    pub runtime: RuntimeSection,
    pub node: Option<NodeSection>,
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
pub struct PanelSection {
    pub url: String,
    pub configuration_token: Option<String>,
    pub daemon_secret: Option<String>,
    pub daemon_callback_token: Option<String>,
    pub node_id: Option<u64>,
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

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct NodeSection {
    pub name: String,
    pub fqdn: String,
    pub daemon_port: u16,
    pub sftp_port: u16,
    pub use_ssl: bool,
    pub location_name: String,
    pub location_country: String,
    pub updated_at: String,
    #[serde(default)]
    pub tls_cert_path: Option<String>,
    #[serde(default)]
    pub tls_key_path: Option<String>,
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

    pub fn persist_bootstrap(&self, panel_url: &str, configuration_token: &str) -> Result<()> {
        let local_path = local_config_path()?;
        let mut local = load_local_table(&local_path)?;

        upsert_bootstrap_configuration(&mut local, panel_url, configuration_token)?;
        write_local_table(&local_path, &local)
    }

    pub fn persist_configuration(
        &self,
        node_id: u64,
        daemon_secret: &str,
        daemon_callback_token: &str,
        daemon_uuid: &str,
    ) -> Result<()> {
        let local_path = local_config_path()?;
        let mut local = load_local_table(&local_path)?;

        upsert_runtime_configuration(
            &mut local,
            node_id,
            daemon_secret,
            daemon_callback_token,
            daemon_uuid,
        )?;
        write_local_table(&local_path, &local)
    }

    pub fn persist_node_configuration(&self, node: &NodeSection) -> Result<()> {
        let local_path = local_config_path()?;
        let mut local = load_local_table(&local_path)?;

        upsert_node_configuration(&mut local, node)?;
        write_local_table(&local_path, &local)
    }

    pub fn persist_node_tls_paths(&self, cert_path: &str, key_path: &str) -> Result<()> {
        let local_path = local_config_path()?;
        let mut local = load_local_table(&local_path)?;

        upsert_node_tls_paths(&mut local, cert_path, key_path)?;
        write_local_table(&local_path, &local)
    }
}

fn local_config_path() -> Result<PathBuf> {
    Ok(project_root()?.join("config/local.toml"))
}

fn load_local_table(local_path: &Path) -> Result<toml::Table> {
    if !local_path.exists() {
        return Ok(toml::Table::new());
    }

    fs::read_to_string(local_path)
        .with_context(|| format!("failed to read {}", local_path.display()))?
        .parse::<toml::Table>()
        .with_context(|| format!("failed to parse {}", local_path.display()))
}

fn write_local_table(local_path: &Path, local: &toml::Table) -> Result<()> {
    if let Some(parent) = local_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let serialized = toml::to_string_pretty(local).context("failed to serialize local config")?;

    fs::write(local_path, serialized)
        .with_context(|| format!("failed to write {}", local_path.display()))
}

fn upsert_bootstrap_configuration(
    local: &mut toml::Table,
    panel_url: &str,
    configuration_token: &str,
) -> Result<()> {
    let panel = local
        .entry("panel")
        .or_insert_with(|| toml::Value::Table(toml::Table::new()));

    let panel_table = panel
        .as_table_mut()
        .context("panel config must be a TOML table")?;

    panel_table.insert(
        "url".to_string(),
        toml::Value::String(panel_url.to_string()),
    );
    panel_table.insert(
        "configuration_token".to_string(),
        toml::Value::String(configuration_token.to_string()),
    );
    panel_table.remove("daemon_secret");
    panel_table.remove("daemon_callback_token");
    panel_table.remove("node_id");

    Ok(())
}

fn upsert_runtime_configuration(
    local: &mut toml::Table,
    node_id: u64,
    daemon_secret: &str,
    daemon_callback_token: &str,
    daemon_uuid: &str,
) -> Result<()> {
    let panel = local
        .entry("panel")
        .or_insert_with(|| toml::Value::Table(toml::Table::new()));

    let panel_table = panel
        .as_table_mut()
        .context("panel config must be a TOML table")?;

    panel_table.remove("configuration_token");
    panel_table.insert(
        "daemon_secret".to_string(),
        toml::Value::String(daemon_secret.to_string()),
    );
    panel_table.insert(
        "daemon_callback_token".to_string(),
        toml::Value::String(daemon_callback_token.to_string()),
    );
    panel_table.insert("node_id".to_string(), toml::Value::Integer(node_id as i64));

    let daemon = local
        .entry("daemon")
        .or_insert_with(|| toml::Value::Table(toml::Table::new()));

    let daemon_table = daemon
        .as_table_mut()
        .context("daemon config must be a TOML table")?;

    daemon_table.insert(
        "uuid".to_string(),
        toml::Value::String(daemon_uuid.to_string()),
    );

    Ok(())
}

fn upsert_node_configuration(local: &mut toml::Table, node: &NodeSection) -> Result<()> {
    let node_entry = local
        .entry("node")
        .or_insert_with(|| toml::Value::Table(toml::Table::new()));

    let node_table = node_entry
        .as_table_mut()
        .context("node config must be a TOML table")?;

    node_table.insert("name".to_string(), toml::Value::String(node.name.clone()));
    node_table.insert("fqdn".to_string(), toml::Value::String(node.fqdn.clone()));
    node_table.insert(
        "daemon_port".to_string(),
        toml::Value::Integer(i64::from(node.daemon_port)),
    );
    node_table.insert(
        "sftp_port".to_string(),
        toml::Value::Integer(i64::from(node.sftp_port)),
    );
    node_table.insert("use_ssl".to_string(), toml::Value::Boolean(node.use_ssl));
    node_table.insert(
        "location_name".to_string(),
        toml::Value::String(node.location_name.clone()),
    );
    node_table.insert(
        "location_country".to_string(),
        toml::Value::String(node.location_country.clone()),
    );
    node_table.insert(
        "updated_at".to_string(),
        toml::Value::String(node.updated_at.clone()),
    );

    if let Some(cert_path) = &node.tls_cert_path {
        node_table.insert(
            "tls_cert_path".to_string(),
            toml::Value::String(cert_path.clone()),
        );
    }

    if let Some(key_path) = &node.tls_key_path {
        node_table.insert(
            "tls_key_path".to_string(),
            toml::Value::String(key_path.clone()),
        );
    }

    Ok(())
}

fn upsert_node_tls_paths(local: &mut toml::Table, cert_path: &str, key_path: &str) -> Result<()> {
    let node_entry = local
        .entry("node")
        .or_insert_with(|| toml::Value::Table(toml::Table::new()));

    let node_table = node_entry
        .as_table_mut()
        .context("node config must be a TOML table")?;

    node_table.insert(
        "tls_cert_path".to_string(),
        toml::Value::String(cert_path.to_string()),
    );
    node_table.insert(
        "tls_key_path".to_string(),
        toml::Value::String(key_path.to_string()),
    );

    Ok(())
}

pub fn project_root() -> Result<PathBuf> {
    let current_dir = std::env::current_dir().context("failed to resolve current directory")?;

    if current_dir.join("config/default.toml").exists() {
        return Ok(current_dir);
    }

    let current_exe = std::env::current_exe().context("failed to resolve current executable")?;

    for ancestor in current_exe.ancestors() {
        if ancestor.join("config/default.toml").exists() {
            return Ok(ancestor.to_path_buf());
        }
    }

    Ok(normalize_root(&current_dir))
}

pub fn managed_server_volume_path(server_id: u64) -> Result<PathBuf> {
    Ok(project_root()?.join("volumes").join(server_id.to_string()))
}

pub fn safe_join_relative(base: &Path, relative: &str) -> Result<PathBuf> {
    let relative_path = Path::new(relative);

    if relative.trim().is_empty() {
        bail!("path cannot be empty");
    }

    let mut resolved = base.to_path_buf();

    for component in relative_path.components() {
        match component {
            Component::Normal(segment) => {
                resolved.push(segment);
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("path must stay within the server volume");
            }
        }
    }

    Ok(resolved)
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
    fn safe_join_relative_rejects_path_traversal_and_absolute_paths() {
        let base = Path::new("/tmp/skyportd-test");

        let traversal = safe_join_relative(base, "../../etc/passwd")
            .expect_err("path traversal should be rejected")
            .to_string();
        let absolute = safe_join_relative(base, "/etc/passwd")
            .expect_err("absolute paths should be rejected")
            .to_string();

        assert!(traversal.contains("within the server volume"));
        assert!(absolute.contains("within the server volume"));
    }

    #[test]
    fn safe_join_relative_allows_nested_relative_paths() {
        let base = Path::new("/tmp/skyportd-test");
        let path = safe_join_relative(base, "configs/server.properties")
            .expect("nested relative path should resolve");

        assert_eq!(path, base.join("configs/server.properties"));
    }

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
        assert_eq!(config.panel.url, "http://127.0.0.1:8000");
        assert_eq!(config.panel.configuration_token, Some(String::new()));
        assert_eq!(config.panel.daemon_secret, None);
        assert_eq!(config.panel.daemon_callback_token, None);
        assert_eq!(config.panel.node_id, None);
        assert_eq!(config.logging.level, "info");
        assert_eq!(config.runtime.worker_threads, 0);
        assert!(config.node.is_none());
    }

    #[test]
    fn bootstrap_persistence_sets_panel_details_and_clears_runtime_values() {
        let mut local = toml::Table::new();
        let mut panel = toml::Table::new();
        panel.insert(
            "daemon_secret".to_string(),
            toml::Value::String("secret".to_string()),
        );
        panel.insert(
            "daemon_callback_token".to_string(),
            toml::Value::String("callback".to_string()),
        );
        panel.insert("node_id".to_string(), toml::Value::Integer(42));
        local.insert("panel".to_string(), toml::Value::Table(panel));

        upsert_bootstrap_configuration(&mut local, "https://panel.example.com", "token-123")
            .expect("bootstrap config should update");

        let panel = local
            .get("panel")
            .and_then(toml::Value::as_table)
            .expect("panel table should exist");

        assert_eq!(
            panel.get("url").and_then(toml::Value::as_str),
            Some("https://panel.example.com")
        );
        assert_eq!(
            panel
                .get("configuration_token")
                .and_then(toml::Value::as_str),
            Some("token-123")
        );
        assert!(!panel.contains_key("daemon_secret"));
        assert!(!panel.contains_key("daemon_callback_token"));
        assert!(!panel.contains_key("node_id"));
    }

    #[test]
    fn configuration_persistence_sets_runtime_values_and_clears_bootstrap_token() {
        let mut local = toml::Table::new();
        let mut panel = toml::Table::new();
        panel.insert(
            "url".to_string(),
            toml::Value::String("https://panel.example.com".to_string()),
        );
        panel.insert(
            "configuration_token".to_string(),
            toml::Value::String("token-123".to_string()),
        );
        local.insert("panel".to_string(), toml::Value::Table(panel));

        upsert_runtime_configuration(
            &mut local,
            99,
            "secret-456",
            "callback-789",
            "11111111-2222-3333-4444-555555555555",
        )
        .expect("runtime config should update");

        let panel = local
            .get("panel")
            .and_then(toml::Value::as_table)
            .expect("panel table should exist");
        let daemon = local
            .get("daemon")
            .and_then(toml::Value::as_table)
            .expect("daemon table should exist");

        assert_eq!(
            panel.get("url").and_then(toml::Value::as_str),
            Some("https://panel.example.com")
        );
        assert!(!panel.contains_key("configuration_token"));
        assert_eq!(
            panel.get("daemon_secret").and_then(toml::Value::as_str),
            Some("secret-456")
        );
        assert_eq!(
            panel
                .get("daemon_callback_token")
                .and_then(toml::Value::as_str),
            Some("callback-789")
        );
        assert_eq!(
            panel.get("node_id").and_then(toml::Value::as_integer),
            Some(99)
        );
        assert_eq!(
            daemon.get("uuid").and_then(toml::Value::as_str),
            Some("11111111-2222-3333-4444-555555555555")
        );
    }

    #[test]
    fn node_configuration_persistence_updates_local_node_settings() {
        let mut local = toml::Table::new();

        upsert_node_configuration(
            &mut local,
            &NodeSection {
                name: "Amsterdam 01".to_string(),
                fqdn: "ams-01.example.com".to_string(),
                daemon_port: 2800,
                sftp_port: 2022,
                use_ssl: true,
                location_name: "Amsterdam".to_string(),
                location_country: "Netherlands".to_string(),
                updated_at: "2026-04-05T20:00:00+00:00".to_string(),
                tls_cert_path: None,
                tls_key_path: None,
            },
        )
        .expect("node config should update");

        let node = local
            .get("node")
            .and_then(toml::Value::as_table)
            .expect("node table should exist");

        assert_eq!(
            node.get("name").and_then(toml::Value::as_str),
            Some("Amsterdam 01")
        );
        assert_eq!(
            node.get("fqdn").and_then(toml::Value::as_str),
            Some("ams-01.example.com")
        );
        assert_eq!(
            node.get("daemon_port").and_then(toml::Value::as_integer),
            Some(2800)
        );
        assert_eq!(
            node.get("sftp_port").and_then(toml::Value::as_integer),
            Some(2022)
        );
        assert_eq!(
            node.get("use_ssl").and_then(toml::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            node.get("location_name").and_then(toml::Value::as_str),
            Some("Amsterdam")
        );
        assert_eq!(
            node.get("location_country").and_then(toml::Value::as_str),
            Some("Netherlands")
        );
        assert_eq!(
            node.get("updated_at").and_then(toml::Value::as_str),
            Some("2026-04-05T20:00:00+00:00")
        );
    }

    #[test]
    fn node_tls_path_persistence_stores_local_tls_settings() {
        let mut local = toml::Table::new();

        upsert_node_tls_paths(
            &mut local,
            "/etc/letsencrypt/live/node/fullchain.pem",
            "/etc/letsencrypt/live/node/privkey.pem",
        )
        .expect("tls paths should update");

        let node = local
            .get("node")
            .and_then(toml::Value::as_table)
            .expect("node table should exist");

        assert_eq!(
            node.get("tls_cert_path").and_then(toml::Value::as_str),
            Some("/etc/letsencrypt/live/node/fullchain.pem")
        );
        assert_eq!(
            node.get("tls_key_path").and_then(toml::Value::as_str),
            Some("/etc/letsencrypt/live/node/privkey.pem")
        );
    }
}
