use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{broadcast, watch};

use crate::config::project_root;

/// Number of console lines kept in memory for each server.
const CONSOLE_BUFFER_LINES: usize = 1000;
/// Capacity of the live console broadcast channel of each server.
const CONSOLE_BROADCAST_CAPACITY: usize = 1024;
/// Maximum size in bytes of a single stored console line.
const CONSOLE_MAX_LINE_BYTES: usize = 8 * 1024;
/// Hard bound on the memory held by the console of one server, whatever the
/// line length. Older lines are evicted first.
const CONSOLE_BUFFER_BYTES: usize = 2 * 1024 * 1024;
/// Sustained console ingestion rate accepted per server (lines per second).
const CONSOLE_RATE_LINES_PER_SECOND: f64 = 5_000.0;
/// Burst of lines accepted before the rate limit kicks in.
const CONSOLE_RATE_BURST_LINES: f64 = 10_000.0;
/// How long a SQLite connection waits for a competing writer.
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
pub struct ServerRegistry {
    db_path: PathBuf,
    console: Arc<ConsoleHub>,
}

/// In-memory console storage shared by every clone of the registry.
///
/// Console output used to be written to SQLite one row per line, which grew
/// without bound and made every log line a synchronous database write. The
/// hub keeps a bounded ring buffer per server and fans live lines out to
/// websocket clients through a broadcast channel.
#[derive(Debug, Default)]
pub struct ConsoleHub {
    servers: Mutex<HashMap<u64, ConsoleBuffer>>,
}

#[derive(Debug)]
struct ConsoleBuffer {
    next_id: u64,
    lines: VecDeque<ConsoleMessageRecord>,
    bytes: usize,
    sender: broadcast::Sender<ConsoleMessageRecord>,
    /// Latest known status of the server, pushed to websocket clients.
    status: watch::Sender<Option<String>>,
    /// Token bucket protecting the daemon from a server flooding its console.
    tokens: f64,
    refilled_at: Instant,
    dropped_lines: u64,
}

impl ConsoleBuffer {
    fn new() -> Self {
        let (sender, _) = broadcast::channel(CONSOLE_BROADCAST_CAPACITY);
        let (status, _) = watch::channel(None);

        Self {
            next_id: 1,
            lines: VecDeque::new(),
            bytes: 0,
            sender,
            status,
            tokens: CONSOLE_RATE_BURST_LINES,
            refilled_at: Instant::now(),
            dropped_lines: 0,
        }
    }

    /// Token bucket: returns whether one more line may be accepted at `now`.
    fn admit(&mut self, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.refilled_at).as_secs_f64();

        self.refilled_at = now;
        self.tokens =
            (self.tokens + elapsed * CONSOLE_RATE_LINES_PER_SECOND).min(CONSOLE_RATE_BURST_LINES);

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn push(&mut self, server_id: u64, source: String, message: String, created_at: String) {
        let record = ConsoleMessageRecord {
            id: self.next_id,
            server_id,
            source,
            message,
            created_at,
        };

        self.next_id += 1;
        self.bytes += record.message.len();
        self.lines.push_back(record.clone());

        // Bound both the number of lines and the memory they use.
        while self.lines.len() > CONSOLE_BUFFER_LINES
            || (self.bytes > CONSOLE_BUFFER_BYTES && self.lines.len() > 1)
        {
            if let Some(evicted) = self.lines.pop_front() {
                self.bytes = self.bytes.saturating_sub(evicted.message.len());
            }
        }

        // A send error only means that nobody is listening right now.
        let _ = self.sender.send(record);
    }
}

impl ConsoleHub {
    fn with_buffer<T>(&self, server_id: u64, action: impl FnOnce(&mut ConsoleBuffer) -> T) -> T {
        let mut servers = self
            .servers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        action(servers.entry(server_id).or_insert_with(ConsoleBuffer::new))
    }

    fn append(&self, server_id: u64, source: &str, message: &str) {
        self.append_at(server_id, source, message, Instant::now());
    }

    fn append_at(&self, server_id: u64, source: &str, message: &str, now: Instant) {
        let message = truncate_console_line(message);
        let source = source.to_string();
        let created_at = format_utc_timestamp(SystemTime::now());

        self.with_buffer(server_id, |buffer| {
            if !buffer.admit(now) {
                buffer.dropped_lines += 1;
                return;
            }

            // Tell readers once that lines were lost to the rate limit.
            if buffer.dropped_lines > 0 {
                let notice = format!(
                    "{} console lines were dropped because the server printed too fast.",
                    buffer.dropped_lines
                );
                buffer.dropped_lines = 0;
                buffer.push(server_id, "system".to_string(), notice, created_at.clone());
            }

            buffer.push(server_id, source, message, created_at);
        });
    }

    fn publish_status(&self, server_id: u64, status: &str) {
        self.with_buffer(server_id, |buffer| {
            buffer.status.send_replace(Some(status.to_string()));
        });
    }

    fn remove(&self, server_id: u64) {
        let mut servers = self
            .servers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        servers.remove(&server_id);
    }
}

fn truncate_console_line(message: &str) -> String {
    if message.len() <= CONSOLE_MAX_LINE_BYTES {
        return message.to_string();
    }

    let mut end = CONSOLE_MAX_LINE_BYTES;

    while !message.is_char_boundary(end) {
        end -= 1;
    }

    format!("{}…", &message[..end])
}

/// Formats a timestamp as `YYYY-MM-DD HH:MM:SS` in UTC.
fn format_utc_timestamp(time: SystemTime) -> String {
    let seconds = time
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default() as i64;
    let days = seconds.div_euclid(86_400);
    let remainder = seconds.rem_euclid(86_400);

    // Civil-from-days (Howard Hinnant's algorithm).
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 { month_index + 3 } else { month_index - 9 };
    let year = year_of_era + era * 400 + i64::from(month <= 2);

    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}",
        remainder / 3_600,
        remainder % 3_600 / 60,
        remainder % 60
    )
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedServerRecord {
    pub id: u64,
    pub node_id: u64,
    pub name: String,
    pub status: String,
    pub volume_path: String,
    pub created_at: String,
    pub updated_at: String,
    pub docker_image: Option<String>,
    pub allocation: ManagedServerAllocation,
    pub interconnects: Vec<ManagedServerInterconnect>,
    pub firewall_rules: Vec<ManagedServerFirewallRule>,
    pub workflows: Vec<ManagedServerWorkflow>,
    pub container_id: Option<String>,
    pub last_error: Option<String>,
    pub user: ManagedServerUser,
    pub limits: ManagedServerLimits,
    pub cargo: ManagedServerCargo,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedServerUser {
    pub id: u64,
    pub name: String,
    pub email: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedServerLimits {
    pub memory_mib: u64,
    pub cpu_limit: u64,
    pub disk_mib: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedServerWorkflow {
    pub id: u64,
    pub name: String,
    pub nodes: Vec<ManagedServerWorkflowStep>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedServerWorkflowStep {
    pub id: String,
    #[serde(rename = "type")]
    pub step_type: String,
    pub kind: String,
    #[serde(default)]
    pub config: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedServerInterconnect {
    pub id: u64,
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedServerFirewallRule {
    pub id: u64,
    pub direction: String,
    pub action: String,
    pub protocol: String,
    pub source: String,
    pub port_start: Option<u64>,
    pub port_end: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedServerAllocation {
    pub id: u64,
    pub bind_ip: String,
    pub port: u64,
    pub ip_alias: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedServerCargo {
    pub id: u64,
    pub name: String,
    pub slug: String,
    pub source_type: String,
    pub startup_command: String,
    pub config_files: String,
    pub config_startup: String,
    pub config_logs: String,
    pub config_stop: String,
    pub install_script: Option<String>,
    pub install_container: Option<String>,
    pub install_entrypoint: Option<String>,
    pub features: Vec<String>,
    pub docker_images: BTreeMap<String, String>,
    pub file_denylist: Vec<String>,
    pub file_hidden_list: Vec<String>,
    pub requires_privileged: bool,
    pub variables: Vec<ManagedServerVariable>,
    pub definition: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedServerVariable {
    pub name: String,
    pub description: String,
    pub env_variable: String,
    pub default_value: String,
    pub user_viewable: bool,
    pub user_editable: bool,
    pub rules: String,
    pub field_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsoleMessageRecord {
    pub id: u64,
    pub server_id: u64,
    pub source: String,
    pub message: String,
    pub created_at: String,
}

impl ServerRegistry {
    pub fn new(db_path: PathBuf) -> Self {
        Self {
            db_path,
            console: Arc::new(ConsoleHub::default()),
        }
    }

    pub fn new_default() -> Result<Self> {
        Ok(Self::new(project_root()?.join("skyportd.db")))
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn initialize(&self) -> Result<()> {
        if let Some(parent) = self.db_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let connection = self.open()?;
        connection.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            CREATE TABLE IF NOT EXISTS servers (
                id INTEGER PRIMARY KEY,
                node_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                volume_path TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                docker_image TEXT,
                user_id INTEGER NOT NULL,
                user_name TEXT NOT NULL,
                user_email TEXT NOT NULL,
                memory_mib INTEGER NOT NULL,
                cpu_limit INTEGER NOT NULL,
                disk_mib INTEGER NOT NULL,
                cargo_id INTEGER NOT NULL,
                cargo_name TEXT NOT NULL,
                cargo_slug TEXT NOT NULL,
                cargo_source_type TEXT NOT NULL,
                startup_command TEXT NOT NULL,
                config_files TEXT NOT NULL,
                config_startup TEXT NOT NULL,
                config_logs TEXT NOT NULL,
                config_stop TEXT NOT NULL,
                install_script TEXT,
                install_container TEXT,
                install_entrypoint TEXT,
                features_json TEXT NOT NULL,
                docker_images_json TEXT NOT NULL,
                file_denylist_json TEXT NOT NULL,
                file_hidden_list_json TEXT NOT NULL,
                requires_privileged INTEGER NOT NULL DEFAULT 0,
                variables_json TEXT NOT NULL,
                definition_json TEXT NOT NULL,
                allocation_json TEXT NOT NULL DEFAULT '{}',
                firewall_rules_json TEXT NOT NULL DEFAULT '[]',
                interconnects_json TEXT NOT NULL DEFAULT '[]',
                workflows_json TEXT NOT NULL DEFAULT '[]',
                container_id TEXT,
                last_error TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_servers_node_id ON servers (node_id);
            CREATE INDEX IF NOT EXISTS idx_servers_status ON servers (status);
            "#,
        )?;

        self.ensure_server_columns(&connection)?;
        self.drop_legacy_console_table(&connection)?;

        Ok(())
    }

    /// Console output now lives in memory. Older databases still carry the
    /// unbounded `server_console_messages` table, which is dropped (and the
    /// file compacted) once.
    fn drop_legacy_console_table(&self, connection: &Connection) -> Result<()> {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'server_console_messages')",
            [],
            |row| row.get(0),
        )?;

        if exists {
            connection.execute_batch("DROP TABLE server_console_messages;")?;

            // Compacting needs temporary disk space up to the size of the
            // database and can take a while; failing to do it only means the
            // file keeps its size, so it must never stop the daemon.
            if let Err(error) = connection.execute_batch("VACUUM;") {
                tracing::warn!(error = %error, "could not compact the registry database");
            }
        }

        Ok(())
    }

    pub fn upsert_server(&self, server: &ManagedServerRecord) -> Result<()> {
        let connection = self.open()?;
        let existing_runtime = self.server_runtime(&connection, server.id)?;

        let status = existing_runtime
            .as_ref()
            .map(|runtime| runtime.status.clone())
            .unwrap_or_else(|| server.status.clone());
        let container_id = existing_runtime
            .as_ref()
            .and_then(|runtime| runtime.container_id.clone());
        let last_error = existing_runtime.and_then(|runtime| runtime.last_error);

        connection.execute(
            r#"
            INSERT INTO servers (
                id, node_id, name, status, volume_path, created_at, updated_at,
                docker_image,
                user_id, user_name, user_email,
                memory_mib, cpu_limit, disk_mib,
                cargo_id, cargo_name, cargo_slug, cargo_source_type,
                startup_command, config_files, config_startup, config_logs, config_stop,
                install_script, install_container, install_entrypoint,
                features_json, docker_images_json, file_denylist_json, file_hidden_list_json,
                requires_privileged,
                variables_json, definition_json, allocation_json, firewall_rules_json, interconnects_json, workflows_json, container_id, last_error
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7,
                ?8,
                ?9, ?10, ?11,
                ?12, ?13, ?14,
                ?15, ?16, ?17, ?18,
                ?19, ?20, ?21, ?22, ?23,
                ?24, ?25, ?26,
                ?27, ?28, ?29, ?30,
                ?31,
                ?32, ?33, ?34, ?35, ?36, ?37, ?38, ?39
            )
            ON CONFLICT(id) DO UPDATE SET
                node_id = excluded.node_id,
                name = excluded.name,
                status = excluded.status,
                volume_path = excluded.volume_path,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                docker_image = excluded.docker_image,
                user_id = excluded.user_id,
                user_name = excluded.user_name,
                user_email = excluded.user_email,
                memory_mib = excluded.memory_mib,
                cpu_limit = excluded.cpu_limit,
                disk_mib = excluded.disk_mib,
                cargo_id = excluded.cargo_id,
                cargo_name = excluded.cargo_name,
                cargo_slug = excluded.cargo_slug,
                cargo_source_type = excluded.cargo_source_type,
                startup_command = excluded.startup_command,
                config_files = excluded.config_files,
                config_startup = excluded.config_startup,
                config_logs = excluded.config_logs,
                config_stop = excluded.config_stop,
                install_script = excluded.install_script,
                install_container = excluded.install_container,
                install_entrypoint = excluded.install_entrypoint,
                features_json = excluded.features_json,
                docker_images_json = excluded.docker_images_json,
                file_denylist_json = excluded.file_denylist_json,
                file_hidden_list_json = excluded.file_hidden_list_json,
                requires_privileged = excluded.requires_privileged,
                variables_json = excluded.variables_json,
                definition_json = excluded.definition_json,
                allocation_json = excluded.allocation_json,
                firewall_rules_json = excluded.firewall_rules_json,
                interconnects_json = excluded.interconnects_json,
                workflows_json = excluded.workflows_json,
                container_id = excluded.container_id,
                last_error = excluded.last_error
            "#,
            params![
                server.id,
                server.node_id,
                server.name,
                status,
                server.volume_path,
                server.created_at,
                server.updated_at,
                server.docker_image,
                server.user.id,
                server.user.name,
                server.user.email,
                server.limits.memory_mib,
                server.limits.cpu_limit,
                server.limits.disk_mib,
                server.cargo.id,
                server.cargo.name,
                server.cargo.slug,
                server.cargo.source_type,
                server.cargo.startup_command,
                server.cargo.config_files,
                server.cargo.config_startup,
                server.cargo.config_logs,
                server.cargo.config_stop,
                server.cargo.install_script,
                server.cargo.install_container,
                server.cargo.install_entrypoint,
                serde_json::to_string(&server.cargo.features)?,
                serde_json::to_string(&server.cargo.docker_images)?,
                serde_json::to_string(&server.cargo.file_denylist)?,
                serde_json::to_string(&server.cargo.file_hidden_list)?,
                server.cargo.requires_privileged,
                serde_json::to_string(&server.cargo.variables)?,
                serde_json::to_string(&server.cargo.definition)?,
                serde_json::to_string(&server.allocation)?,
                serde_json::to_string(&server.firewall_rules)?,
                serde_json::to_string(&server.interconnects)?,
                serde_json::to_string(&server.workflows)?,
                container_id,
                last_error,
            ],
        )?;
        self.console.publish_status(server.id, &status);

        Ok(())
    }

    pub fn get_server(&self, server_id: u64) -> Result<Option<ManagedServerRecord>> {
        let connection = self.open()?;
        let mut statement = connection.prepare(&format!("{SERVER_SELECT_SQL} WHERE id = ?1"))?;

        statement
            .query_row(params![server_id], map_server_row)
            .optional()
            .context("failed to fetch server from registry")
    }

    pub fn list_servers_for_node(&self, node_id: u64) -> Result<Vec<ManagedServerRecord>> {
        let connection = self.open()?;
        let mut statement = connection.prepare(&format!(
            "{SERVER_SELECT_SQL} WHERE node_id = ?1 ORDER BY id ASC"
        ))?;
        let rows = statement.query_map(params![node_id], map_server_row)?;

        let mut servers = Vec::new();

        for row in rows {
            servers.push(row?);
        }

        Ok(servers)
    }

    pub fn delete_server(&self, server_id: u64) -> Result<bool> {
        let connection = self.open()?;
        let deleted =
            connection.execute("DELETE FROM servers WHERE id = ?1", params![server_id])?;
        self.console.remove(server_id);

        Ok(deleted > 0)
    }

    pub fn set_server_runtime(
        &self,
        server_id: u64,
        status: &str,
        container_id: Option<&str>,
        last_error: Option<&str>,
    ) -> Result<()> {
        let connection = self.open()?;
        connection.execute(
            "UPDATE servers SET status = ?1, container_id = ?2, last_error = ?3 WHERE id = ?4",
            params![status, container_id, last_error, server_id],
        )?;
        self.console.publish_status(server_id, status);

        Ok(())
    }

    /// Subscribes to status changes of a server. The channel is seeded from
    /// the database the first time, so a new subscriber always sees a value.
    pub fn subscribe_status(&self, server_id: u64) -> Result<watch::Receiver<Option<String>>> {
        let receiver = self
            .console
            .with_buffer(server_id, |buffer| buffer.status.subscribe());

        if receiver.borrow().is_none() {
            if let Some(status) = self.get_server_status(server_id)? {
                self.console.with_buffer(server_id, |buffer| {
                    // Never overwrite a status published in the meantime.
                    buffer.status.send_if_modified(|current| {
                        if current.is_none() {
                            *current = Some(status);
                            true
                        } else {
                            false
                        }
                    });
                });
            }
        }

        Ok(self
            .console
            .with_buffer(server_id, |buffer| buffer.status.subscribe()))
    }

    /// Appends a line to the in-memory console of a server and notifies any
    /// live subscriber. This never touches the database and cannot fail.
    pub fn append_console_message(
        &self,
        server_id: u64,
        source: &str,
        message: &str,
    ) -> Result<()> {
        self.console.append(server_id, source, message);

        Ok(())
    }

    pub fn console_messages_since(
        &self,
        server_id: u64,
        last_id: u64,
        limit: u64,
    ) -> Result<Vec<ConsoleMessageRecord>> {
        Ok(self.console.with_buffer(server_id, |buffer| {
            buffer
                .lines
                .iter()
                .filter(|message| message.id > last_id)
                .take(limit as usize)
                .cloned()
                .collect()
        }))
    }

    pub fn recent_console_messages(
        &self,
        server_id: u64,
        limit: u64,
    ) -> Result<Vec<ConsoleMessageRecord>> {
        Ok(self.console.with_buffer(server_id, |buffer| {
            let skip = buffer.lines.len().saturating_sub(limit as usize);

            buffer.lines.iter().skip(skip).cloned().collect()
        }))
    }

    /// Identifier of the newest console line of a server, or 0 when empty.
    pub fn latest_console_id(&self, server_id: u64) -> u64 {
        self.console
            .with_buffer(server_id, |buffer| buffer.next_id.saturating_sub(1))
    }

    /// Subscribes to the live console output of a server.
    pub fn subscribe_console(&self, server_id: u64) -> broadcast::Receiver<ConsoleMessageRecord> {
        self.console
            .with_buffer(server_id, |buffer| buffer.sender.subscribe())
    }

    /// Cheap status lookup that avoids deserializing the whole record.
    pub fn get_server_status(&self, server_id: u64) -> Result<Option<String>> {
        let connection = self.open()?;

        connection
            .query_row(
                "SELECT status FROM servers WHERE id = ?1",
                params![server_id],
                |row| row.get(0),
            )
            .optional()
            .context("failed to fetch server status from registry")
    }

    /// Lists `(id, status)` for every server of a node without decoding the
    /// cargo definition of each one.
    pub fn list_server_statuses_for_node(&self, node_id: u64) -> Result<Vec<(u64, String)>> {
        let connection = self.open()?;
        let mut statement =
            connection.prepare("SELECT id, status FROM servers WHERE node_id = ?1 ORDER BY id ASC")?;
        let rows = statement.query_map(params![node_id], |row| Ok((row.get(0)?, row.get(1)?)))?;
        let mut servers = Vec::new();

        for row in rows {
            servers.push(row?);
        }

        Ok(servers)
    }

    fn open(&self) -> Result<Connection> {
        let connection = Connection::open(&self.db_path)
            .with_context(|| format!("failed to open {}", self.db_path.display()))?;

        // Wait for a competing writer instead of failing with SQLITE_BUSY.
        connection.busy_timeout(SQLITE_BUSY_TIMEOUT)?;
        // WAL is crash safe with NORMAL and avoids an fsync on every commit.
        connection.pragma_update(None, "synchronous", "NORMAL")?;

        Ok(connection)
    }

    fn ensure_server_columns(&self, connection: &Connection) -> Result<()> {
        let existing_columns = self.server_columns(connection)?;

        if !existing_columns
            .iter()
            .any(|column| column == "container_id")
        {
            connection.execute("ALTER TABLE servers ADD COLUMN container_id TEXT", [])?;
        }

        if !existing_columns.iter().any(|column| column == "last_error") {
            connection.execute("ALTER TABLE servers ADD COLUMN last_error TEXT", [])?;
        }

        if !existing_columns
            .iter()
            .any(|column| column == "docker_image")
        {
            connection.execute("ALTER TABLE servers ADD COLUMN docker_image TEXT", [])?;
        }

        if !existing_columns
            .iter()
            .any(|column| column == "allocation_json")
        {
            connection.execute(
                "ALTER TABLE servers ADD COLUMN allocation_json TEXT NOT NULL DEFAULT '{}'",
                [],
            )?;
        }

        if !existing_columns
            .iter()
            .any(|column| column == "firewall_rules_json")
        {
            connection.execute(
                "ALTER TABLE servers ADD COLUMN firewall_rules_json TEXT NOT NULL DEFAULT '[]'",
                [],
            )?;
        }

        if !existing_columns
            .iter()
            .any(|column| column == "interconnects_json")
        {
            connection.execute(
                "ALTER TABLE servers ADD COLUMN interconnects_json TEXT NOT NULL DEFAULT '[]'",
                [],
            )?;
        }

        if !existing_columns
            .iter()
            .any(|column| column == "workflows_json")
        {
            connection.execute(
                "ALTER TABLE servers ADD COLUMN workflows_json TEXT NOT NULL DEFAULT '[]'",
                [],
            )?;
        }

        if !existing_columns
            .iter()
            .any(|column| column == "requires_privileged")
        {
            connection.execute(
                "ALTER TABLE servers ADD COLUMN requires_privileged INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }

        Ok(())
    }

    fn server_columns(&self, connection: &Connection) -> Result<Vec<String>> {
        let mut statement = connection.prepare("PRAGMA table_info(servers)")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(1))?;
        let mut columns = Vec::new();

        for row in rows {
            columns.push(row?);
        }

        Ok(columns)
    }

    fn server_runtime(
        &self,
        connection: &Connection,
        server_id: u64,
    ) -> Result<Option<ServerRuntime>> {
        connection
            .query_row(
                "SELECT status, container_id, last_error FROM servers WHERE id = ?1",
                params![server_id],
                |row| {
                    Ok(ServerRuntime {
                        status: row.get(0)?,
                        container_id: row.get(1)?,
                        last_error: row.get(2)?,
                    })
                },
            )
            .optional()
            .context("failed to load server runtime state")
    }
}

#[derive(Debug)]
struct ServerRuntime {
    status: String,
    container_id: Option<String>,
    last_error: Option<String>,
}

const SERVER_SELECT_SQL: &str = r#"
SELECT
    id, node_id, name, status, volume_path, created_at, updated_at,
    docker_image,
    user_id, user_name, user_email,
    memory_mib, cpu_limit, disk_mib,
    cargo_id, cargo_name, cargo_slug, cargo_source_type,
    startup_command, config_files, config_startup, config_logs, config_stop,
    install_script, install_container, install_entrypoint,
    features_json, docker_images_json, file_denylist_json, file_hidden_list_json,
    variables_json, requires_privileged, definition_json, allocation_json, firewall_rules_json, interconnects_json, workflows_json, container_id, last_error
FROM servers
"#;

fn map_server_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ManagedServerRecord> {
    Ok(ManagedServerRecord {
        id: row.get(0)?,
        node_id: row.get(1)?,
        name: row.get(2)?,
        status: row.get(3)?,
        volume_path: row.get(4)?,
        created_at: row.get(5)?,
        updated_at: row.get(6)?,
        docker_image: row.get(7)?,
        allocation: parse_json(row.get::<_, String>(33)?)?,
        firewall_rules: parse_json(row.get::<_, String>(34)?)?,
        interconnects: parse_json(row.get::<_, String>(35)?)?,
        workflows: parse_json(row.get::<_, String>(36)?)?,
        container_id: row.get(37)?,
        last_error: row.get(38)?,
        user: ManagedServerUser {
            id: row.get(8)?,
            name: row.get(9)?,
            email: row.get(10)?,
        },
        limits: ManagedServerLimits {
            memory_mib: row.get(11)?,
            cpu_limit: row.get(12)?,
            disk_mib: row.get(13)?,
        },
        cargo: ManagedServerCargo {
            id: row.get(14)?,
            name: row.get(15)?,
            slug: row.get(16)?,
            source_type: row.get(17)?,
            startup_command: row.get(18)?,
            config_files: row.get(19)?,
            config_startup: row.get(20)?,
            config_logs: row.get(21)?,
            config_stop: row.get(22)?,
            install_script: row.get(23)?,
            install_container: row.get(24)?,
            install_entrypoint: row.get(25)?,
            features: parse_json(row.get::<_, String>(26)?)?,
            docker_images: parse_json(row.get::<_, String>(27)?)?,
            file_denylist: parse_json(row.get::<_, String>(28)?)?,
            file_hidden_list: parse_json(row.get::<_, String>(29)?)?,
            variables: parse_json(row.get::<_, String>(30)?)?,
            requires_privileged: row.get::<_, i32>(31)? != 0,
            definition: parse_json(row.get::<_, String>(32)?)?,
        },
    })
}

fn parse_json<T>(value: String) -> rusqlite::Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            value.len(),
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sample_server_record() -> ManagedServerRecord {
        ManagedServerRecord {
            id: 11,
            node_id: 1,
            name: "Sample".to_string(),
            status: "offline".to_string(),
            volume_path: "volumes/11".to_string(),
            created_at: "2026-04-06T12:00:00Z".to_string(),
            updated_at: "2026-04-06T12:00:00Z".to_string(),
            docker_image: None,
            container_id: None,
            last_error: None,
            user: ManagedServerUser {
                id: 1,
                name: "Jane".to_string(),
                email: "jane@example.com".to_string(),
            },
            limits: ManagedServerLimits {
                memory_mib: 512,
                cpu_limit: 100,
                disk_mib: 1024,
            },
            cargo: ManagedServerCargo {
                id: 1,
                name: "Paper".to_string(),
                slug: "paper".to_string(),
                source_type: "custom".to_string(),
                startup_command: "java".to_string(),
                config_files: "{}".to_string(),
                config_startup: "{}".to_string(),
                config_logs: "{}".to_string(),
                config_stop: "stop".to_string(),
                install_script: None,
                install_container: None,
                install_entrypoint: None,
                features: vec![],
                docker_images: BTreeMap::new(),
                file_denylist: vec![],
                file_hidden_list: vec![],
                requires_privileged: false,
                variables: vec![],
                definition: serde_json::json!({}),
            },
            allocation: ManagedServerAllocation {
                id: 1,
                bind_ip: "0.0.0.0".to_string(),
                port: 25565,
                ip_alias: None,
            },
            firewall_rules: vec![],
            interconnects: vec![],
            workflows: vec![],
        }
    }

    fn test_registry() -> (tempfile::TempDir, ServerRegistry) {
        let directory = tempdir().unwrap();
        let registry = ServerRegistry::new(directory.path().join("registry.db"));
        registry.initialize().unwrap();

        (directory, registry)
    }

    #[test]
    fn console_keeps_only_the_most_recent_lines() {
        let (_directory, registry) = test_registry();

        for index in 0..(CONSOLE_BUFFER_LINES + 50) {
            registry
                .append_console_message(1, "stdout", &format!("line {index}"))
                .unwrap();
        }

        let all = registry.recent_console_messages(1, u64::MAX).unwrap();

        assert_eq!(all.len(), CONSOLE_BUFFER_LINES);
        assert_eq!(all.first().unwrap().message, "line 50");
        assert_eq!(
            all.last().unwrap().message,
            format!("line {}", CONSOLE_BUFFER_LINES + 49)
        );
        assert_eq!(
            registry.latest_console_id(1),
            (CONSOLE_BUFFER_LINES + 50) as u64
        );
    }

    #[test]
    fn console_since_returns_only_newer_lines_in_order() {
        let (_directory, registry) = test_registry();

        for index in 1..=10 {
            registry
                .append_console_message(7, "stdout", &format!("line {index}"))
                .unwrap();
        }

        let since = registry.console_messages_since(7, 6, 100).unwrap();

        assert_eq!(
            since.iter().map(|m| m.message.as_str()).collect::<Vec<_>>(),
            ["line 7", "line 8", "line 9", "line 10"]
        );
        assert_eq!(registry.console_messages_since(7, 6, 2).unwrap().len(), 2);
        assert!(registry.console_messages_since(7, 10, 100).unwrap().is_empty());
        assert!(registry.console_messages_since(999, 0, 100).unwrap().is_empty());
    }

    #[test]
    fn console_is_isolated_between_servers() {
        let (_directory, registry) = test_registry();

        registry.append_console_message(1, "stdout", "one").unwrap();
        registry.append_console_message(2, "stdout", "two").unwrap();

        assert_eq!(registry.recent_console_messages(1, 10).unwrap().len(), 1);
        assert_eq!(registry.recent_console_messages(2, 10).unwrap()[0].message, "two");
    }

    #[test]
    fn console_lines_are_truncated_on_a_char_boundary() {
        let (_directory, registry) = test_registry();
        let long = "é".repeat(CONSOLE_MAX_LINE_BYTES);

        registry.append_console_message(1, "stdout", &long).unwrap();

        let stored = &registry.recent_console_messages(1, 1).unwrap()[0].message;

        assert!(stored.len() <= CONSOLE_MAX_LINE_BYTES + '…'.len_utf8());
        assert!(stored.ends_with('…'));
    }

    #[tokio::test]
    async fn console_subscribers_receive_live_lines() {
        let (_directory, registry) = test_registry();
        let mut receiver = registry.subscribe_console(3);

        registry.append_console_message(3, "stdout", "hello").unwrap();

        let message = receiver.recv().await.unwrap();

        assert_eq!(message.message, "hello");
        assert_eq!(message.id, 1);
    }

    #[test]
    fn console_memory_is_bounded_in_bytes_not_only_in_lines() {
        let (_directory, registry) = test_registry();
        let line = "x".repeat(CONSOLE_MAX_LINE_BYTES);

        // 400 lines of 8 KiB would be 3.2 MiB: the byte cap must evict.
        for _ in 0..400 {
            registry.append_console_message(1, "stdout", &line).unwrap();
        }

        let stored = registry.recent_console_messages(1, u64::MAX).unwrap();
        let bytes: usize = stored.iter().map(|message| message.message.len()).sum();

        assert!(bytes <= CONSOLE_BUFFER_BYTES, "{bytes} bytes kept");
        assert!(stored.len() < 400);
        assert_eq!(registry.latest_console_id(1), 400);
        assert_eq!(stored.last().unwrap().id, 400);
    }

    #[test]
    fn console_ingestion_is_rate_limited_and_reports_dropped_lines() {
        let (_directory, registry) = test_registry();
        let start = Instant::now();

        // A flood at a single instant: only the burst is accepted.
        for index in 0..(CONSOLE_RATE_BURST_LINES as usize + 500) {
            registry
                .console
                .append_at(1, "stdout", &format!("line {index}"), start);
        }

        assert_eq!(
            registry.latest_console_id(1),
            CONSOLE_RATE_BURST_LINES as u64,
            "lines beyond the burst must be dropped"
        );

        // Later the bucket has refilled: one notice, then the line.
        registry
            .console
            .append_at(1, "stdout", "after flood", start + Duration::from_secs(1));

        let recent = registry.recent_console_messages(1, 2).unwrap();

        assert!(recent[0].message.starts_with("500 console lines were dropped"));
        assert_eq!(recent[0].source, "system");
        assert_eq!(recent[1].message, "after flood");
    }

    #[tokio::test]
    async fn status_changes_are_pushed_to_subscribers() {
        let (_directory, registry) = test_registry();
        let mut server = sample_server_record();
        server.status = "offline".to_string();
        registry.upsert_server(&server).unwrap();

        let mut receiver = registry.subscribe_status(server.id).unwrap();
        // Seeded from the database for a fresh subscriber.
        assert_eq!(receiver.borrow_and_update().as_deref(), Some("offline"));

        registry
            .set_server_runtime(server.id, "starting", None, None)
            .unwrap();
        receiver.changed().await.unwrap();
        assert_eq!(receiver.borrow_and_update().as_deref(), Some("starting"));

        // Deleting the server closes the channel.
        registry.delete_server(server.id).unwrap();
        assert!(receiver.changed().await.is_err());
    }

    #[test]
    fn timestamps_are_formatted_in_utc() {
        assert_eq!(format_utc_timestamp(UNIX_EPOCH), "1970-01-01 00:00:00");
        assert_eq!(
            format_utc_timestamp(UNIX_EPOCH + Duration::from_secs(1_709_164_800 + 3_723)),
            "2024-02-29 01:02:03"
        );
    }

    #[test]
    fn legacy_console_table_is_dropped_on_initialize() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("legacy.db");
        let registry = ServerRegistry::new(path.clone());
        registry.initialize().unwrap();

        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE server_console_messages (id INTEGER PRIMARY KEY, server_id INTEGER, message TEXT);
                 INSERT INTO server_console_messages (server_id, message) VALUES (1, 'old');",
            )
            .unwrap();
        drop(connection);

        registry.initialize().unwrap();

        let connection = Connection::open(&path).unwrap();
        let exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'server_console_messages')",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert!(!exists);
    }

    #[test]
    fn concurrent_writers_do_not_fail_with_busy() {
        let (_directory, registry) = test_registry();
        let handles = (0..8)
            .map(|worker| {
                let registry = registry.clone();

                std::thread::spawn(move || {
                    for index in 0..50 {
                        registry
                            .set_server_runtime(worker, "running", None, Some(&index.to_string()))
                            .unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn registry_can_upsert_and_delete_servers() {
        let tempdir = tempdir().unwrap();
        let registry = ServerRegistry::new(tempdir.path().join("skyportd.db"));
        registry.initialize().unwrap();

        let server = ManagedServerRecord {
            id: 42,
            node_id: 7,
            name: "Paper Survival".to_string(),
            status: "pending".to_string(),
            volume_path: "volumes/42".to_string(),
            created_at: "2026-04-06T12:00:00Z".to_string(),
            updated_at: "2026-04-06T12:00:00Z".to_string(),
            docker_image: None,
            container_id: None,
            last_error: None,
            user: ManagedServerUser {
                id: 5,
                name: "Jane Doe".to_string(),
                email: "jane@example.com".to_string(),
            },
            limits: ManagedServerLimits {
                memory_mib: 4096,
                cpu_limit: 200,
                disk_mib: 20480,
            },
            cargo: ManagedServerCargo {
                id: 9,
                name: "Paper".to_string(),
                slug: "paper".to_string(),
                source_type: "native".to_string(),
                startup_command: "java -jar server.jar".to_string(),
                config_files: "{}".to_string(),
                config_startup: "{}".to_string(),
                config_logs: "{}".to_string(),
                config_stop: "stop".to_string(),
                install_script: Some("echo install".to_string()),
                install_container: Some("ghcr.io/skyport/installers:latest".to_string()),
                install_entrypoint: Some("bash".to_string()),
                features: vec!["eula".to_string()],
                docker_images: BTreeMap::from([(
                    "Default".to_string(),
                    "ghcr.io/skyportsh/yolks:latest".to_string(),
                )]),
                file_denylist: vec!["server.properties".to_string()],
                file_hidden_list: vec![".env".to_string()],
                requires_privileged: false,
                variables: vec![ManagedServerVariable {
                    name: "Jar".to_string(),
                    description: "Jar to run".to_string(),
                    env_variable: "SERVER_JARFILE".to_string(),
                    default_value: "server.jar".to_string(),
                    user_viewable: true,
                    user_editable: true,
                    rules: "required|string".to_string(),
                    field_type: "text".to_string(),
                }],
                definition: serde_json::json!({"meta":{"version":"SPDL_v1"}}),
            },
            allocation: ManagedServerAllocation {
                id: 77,
                bind_ip: "0.0.0.0".to_string(),
                port: 25565,
                ip_alias: Some("play.example.com".to_string()),
            },
            firewall_rules: vec![],
            interconnects: vec![],
            workflows: vec![],
        };

        registry.upsert_server(&server).unwrap();
        registry
            .set_server_runtime(server.id, "installing", Some("container-123"), Some("oops"))
            .unwrap();
        registry
            .append_console_message(server.id, "system", "Preparing volume")
            .unwrap();

        let recent_messages = (1..=60)
            .map(|index| format!("Message {index}"))
            .collect::<Vec<_>>();

        for message in &recent_messages {
            registry
                .append_console_message(server.id, "stdout", message)
                .unwrap();
        }

        let recent = registry.recent_console_messages(server.id, 50).unwrap();

        assert_eq!(recent.len(), 50);
        assert_eq!(
            recent.first().map(|message| message.message.as_str()),
            Some("Message 11")
        );
        assert_eq!(
            recent.last().map(|message| message.message.as_str()),
            Some("Message 60")
        );

        let stored = registry.get_server(server.id).unwrap().unwrap();
        assert_eq!(stored.status, "installing");
        assert_eq!(stored.container_id.as_deref(), Some("container-123"));
        assert_eq!(stored.last_error.as_deref(), Some("oops"));

        let updated = ManagedServerRecord {
            name: "Updated Survival".to_string(),
            ..server.clone()
        };

        registry.upsert_server(&updated).unwrap();

        let stored = registry.get_server(server.id).unwrap().unwrap();
        assert_eq!(stored.name, "Updated Survival");
        assert_eq!(stored.status, "installing");
        assert_eq!(stored.container_id.as_deref(), Some("container-123"));

        let listed = registry.list_servers_for_node(server.node_id).unwrap();
        assert_eq!(listed.len(), 1);

        assert!(registry.delete_server(server.id).unwrap());
        assert!(registry.get_server(server.id).unwrap().is_none());
    }
}
