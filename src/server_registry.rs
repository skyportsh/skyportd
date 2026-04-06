use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::project_root;

#[derive(Clone, Debug)]
pub struct ServerRegistry {
    db_path: PathBuf,
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

impl ServerRegistry {
    pub fn new(db_path: PathBuf) -> Self {
        Self { db_path }
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
                variables_json TEXT NOT NULL,
                definition_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_servers_node_id ON servers (node_id);
            CREATE INDEX IF NOT EXISTS idx_servers_status ON servers (status);
            "#,
        )?;

        Ok(())
    }

    pub fn upsert_server(&self, server: &ManagedServerRecord) -> Result<()> {
        let connection = self.open()?;

        connection.execute(
            r#"
            INSERT INTO servers (
                id, node_id, name, status, volume_path, created_at, updated_at,
                user_id, user_name, user_email,
                memory_mib, cpu_limit, disk_mib,
                cargo_id, cargo_name, cargo_slug, cargo_source_type,
                startup_command, config_files, config_startup, config_logs, config_stop,
                install_script, install_container, install_entrypoint,
                features_json, docker_images_json, file_denylist_json, file_hidden_list_json,
                variables_json, definition_json
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7,
                ?8, ?9, ?10,
                ?11, ?12, ?13,
                ?14, ?15, ?16, ?17,
                ?18, ?19, ?20, ?21, ?22,
                ?23, ?24, ?25,
                ?26, ?27, ?28, ?29,
                ?30, ?31
            )
            ON CONFLICT(id) DO UPDATE SET
                node_id = excluded.node_id,
                name = excluded.name,
                status = excluded.status,
                volume_path = excluded.volume_path,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
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
                variables_json = excluded.variables_json,
                definition_json = excluded.definition_json
            "#,
            params![
                server.id,
                server.node_id,
                server.name,
                server.status,
                server.volume_path,
                server.created_at,
                server.updated_at,
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
                serde_json::to_string(&server.cargo.variables)?,
                serde_json::to_string(&server.cargo.definition)?,
            ],
        )?;

        Ok(())
    }

    pub fn delete_server(&self, server_id: u64) -> Result<bool> {
        let connection = self.open()?;
        let deleted =
            connection.execute("DELETE FROM servers WHERE id = ?1", params![server_id])?;

        Ok(deleted > 0)
    }

    pub fn get_server(&self, server_id: u64) -> Result<Option<ManagedServerRecord>> {
        let connection = self.open()?;
        let mut statement = connection.prepare(
            r#"
            SELECT
                id, node_id, name, status, volume_path, created_at, updated_at,
                user_id, user_name, user_email,
                memory_mib, cpu_limit, disk_mib,
                cargo_id, cargo_name, cargo_slug, cargo_source_type,
                startup_command, config_files, config_startup, config_logs, config_stop,
                install_script, install_container, install_entrypoint,
                features_json, docker_images_json, file_denylist_json, file_hidden_list_json,
                variables_json, definition_json
            FROM servers
            WHERE id = ?1
            "#,
        )?;

        statement
            .query_row(params![server_id], map_server_row)
            .optional()
            .context("failed to fetch server from registry")
    }

    fn open(&self) -> Result<Connection> {
        Connection::open(&self.db_path)
            .with_context(|| format!("failed to open {}", self.db_path.display()))
    }
}

fn map_server_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ManagedServerRecord> {
    Ok(ManagedServerRecord {
        id: row.get(0)?,
        node_id: row.get(1)?,
        name: row.get(2)?,
        status: row.get(3)?,
        volume_path: row.get(4)?,
        created_at: row.get(5)?,
        updated_at: row.get(6)?,
        user: ManagedServerUser {
            id: row.get(7)?,
            name: row.get(8)?,
            email: row.get(9)?,
        },
        limits: ManagedServerLimits {
            memory_mib: row.get(10)?,
            cpu_limit: row.get(11)?,
            disk_mib: row.get(12)?,
        },
        cargo: ManagedServerCargo {
            id: row.get(13)?,
            name: row.get(14)?,
            slug: row.get(15)?,
            source_type: row.get(16)?,
            startup_command: row.get(17)?,
            config_files: row.get(18)?,
            config_startup: row.get(19)?,
            config_logs: row.get(20)?,
            config_stop: row.get(21)?,
            install_script: row.get(22)?,
            install_container: row.get(23)?,
            install_entrypoint: row.get(24)?,
            features: parse_json(row.get::<_, String>(25)?)?,
            docker_images: parse_json(row.get::<_, String>(26)?)?,
            file_denylist: parse_json(row.get::<_, String>(27)?)?,
            file_hidden_list: parse_json(row.get::<_, String>(28)?)?,
            variables: parse_json(row.get::<_, String>(29)?)?,
            definition: parse_json(row.get::<_, String>(30)?)?,
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
        };

        registry.upsert_server(&server).unwrap();

        let stored = registry.get_server(server.id).unwrap().unwrap();
        assert_eq!(stored, server);

        let updated = ManagedServerRecord {
            name: "Updated Survival".to_string(),
            ..server.clone()
        };

        registry.upsert_server(&updated).unwrap();

        let stored = registry.get_server(server.id).unwrap().unwrap();
        assert_eq!(stored.name, "Updated Survival");

        assert!(registry.delete_server(server.id).unwrap());
        assert!(registry.get_server(server.id).unwrap().is_none());
    }
}
