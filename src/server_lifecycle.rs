use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::select;
use tokio::sync::{Mutex, watch};
use tokio::time::{self, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{DaemonConfig, project_root};
use crate::server_registry::{ManagedServerCargo, ManagedServerRecord, ServerRegistry};

pub struct ServerLifecycleService {
    config_rx: watch::Receiver<DaemonConfig>,
    server_registry: ServerRegistry,
    cancellation: CancellationToken,
}

impl ServerLifecycleService {
    pub fn new(
        config_rx: watch::Receiver<DaemonConfig>,
        server_registry: ServerRegistry,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            config_rx,
            server_registry,
            cancellation,
        }
    }

    pub async fn run(self) -> Result<()> {
        let mut interval = time::interval(Duration::from_secs(3));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let active = Arc::new(Mutex::new(HashSet::<u64>::new()));

        info!("server lifecycle service started");

        loop {
            select! {
                _ = self.cancellation.cancelled() => {
                    info!("server lifecycle service stopping");
                    break;
                }
                _ = interval.tick() => {
                    let config = self.config_rx.borrow().clone();
                    let Some(node_id) = config.panel.node_id else {
                        continue;
                    };

                    let servers = self.server_registry.list_servers_for_node(node_id)?;

                    for server in servers {
                        if !should_reconcile(&server, &config).await {
                            continue;
                        }

                        let mut guard = active.lock().await;
                        if !guard.insert(server.id) {
                            continue;
                        }
                        drop(guard);

                        let registry = self.server_registry.clone();
                        let active = Arc::clone(&active);
                        let cancellation = self.cancellation.child_token();
                        let config = config.clone();

                        tokio::spawn(async move {
                            let server_id = server.id;
                            if let Err(error) = reconcile_server(registry.clone(), server, config, cancellation).await {
                                warn!(server_id, error = %error, "server reconciliation failed");
                                let _ = registry.set_server_runtime(
                                    server_id,
                                    "install_failed",
                                    None,
                                    Some(&error.to_string()),
                                );
                                let _ = registry.append_console_message(
                                    server_id,
                                    "system",
                                    &format!("Reconciliation failed: {error}"),
                                );
                            }

                            active.lock().await.remove(&server_id);
                        });
                    }
                }
            }
        }

        Ok(())
    }
}

async fn should_reconcile(server: &ManagedServerRecord, config: &DaemonConfig) -> bool {
    if config.panel.node_id != Some(server.node_id) {
        return false;
    }

    match server.status.as_str() {
        "pending" | "offline" | "starting" | "online" => true,
        "installing" => true,
        "install_failed" => false,
        _ => true,
    }
}

async fn reconcile_server(
    registry: ServerRegistry,
    mut server: ManagedServerRecord,
    config: DaemonConfig,
    cancellation: CancellationToken,
) -> Result<()> {
    if cancellation.is_cancelled() {
        return Ok(());
    }

    match server.status.as_str() {
        "pending" | "installing" => {
            provision_and_install(&registry, &mut server, &config, &cancellation).await?;
        }
        "offline" | "starting" | "online" => {
            if !runtime_container_is_running(&server).await? {
                boot_server(&registry, &mut server, &config, &cancellation).await?;
            }
        }
        _ => {
            provision_and_install(&registry, &mut server, &config, &cancellation).await?;
        }
    }

    Ok(())
}

async fn provision_and_install(
    registry: &ServerRegistry,
    server: &mut ManagedServerRecord,
    config: &DaemonConfig,
    cancellation: &CancellationToken,
) -> Result<()> {
    let volume_path = resolve_volume_path(config, server)?;

    tokio::fs::create_dir_all(&volume_path)
        .await
        .with_context(|| format!("failed to create {}", volume_path.display()))?;

    registry.set_server_runtime(server.id, "installing", None, None)?;
    registry.append_console_message(server.id, "system", "Preparing volume directory...")?;

    let install_script = server
        .cargo
        .install_script
        .as_deref()
        .map(str::trim)
        .unwrap_or("");
    let install_image = server
        .cargo
        .install_container
        .as_deref()
        .map(str::trim)
        .unwrap_or("");

    if install_script.is_empty() || install_image.is_empty() {
        registry.append_console_message(
            server.id,
            "system",
            "No install container configured. Skipping installation.",
        )?;
        registry.set_server_runtime(server.id, "offline", None, None)?;
        server.status = "offline".to_string();

        return boot_server(registry, server, config, cancellation).await;
    }

    registry.append_console_message(
        server.id,
        "system",
        &format!("Running install container {install_image}..."),
    )?;
    remove_container(&install_container_name(server.id)).await?;

    let entrypoint = server
        .cargo
        .install_entrypoint
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("sh");

    let mut command = docker_command();
    command
        .arg("run")
        .arg("--rm")
        .arg("--privileged")
        .arg("--name")
        .arg(install_container_name(server.id))
        .arg("-v")
        .arg(format!("{}:/mnt/server", volume_path.display()))
        .arg("-w")
        .arg("/mnt/server")
        .arg("--entrypoint")
        .arg(entrypoint)
        .arg(install_image)
        .arg("-c")
        .arg(install_script);

    let status =
        run_streaming_command(command, registry, server.id, "install", cancellation).await?;

    if !status.success() {
        let code = status.code().unwrap_or_default();
        let message = format!("Installation failed with exit code {code}.");
        registry.append_console_message(server.id, "system", &message)?;
        registry.set_server_runtime(server.id, "install_failed", None, Some(&message))?;
        bail!(message);
    }

    registry.append_console_message(server.id, "system", "Installation completed successfully.")?;
    registry.set_server_runtime(server.id, "offline", None, None)?;
    server.status = "offline".to_string();

    boot_server(registry, server, config, cancellation).await
}

async fn boot_server(
    registry: &ServerRegistry,
    server: &mut ManagedServerRecord,
    config: &DaemonConfig,
    cancellation: &CancellationToken,
) -> Result<()> {
    let volume_path = resolve_volume_path(config, server)?;
    ensure_disk_limit(&volume_path, server.limits.disk_mib)?;

    let image = select_runtime_image(&server.cargo)?;
    registry.append_console_message(
        server.id,
        "system",
        &format!("Pulling runtime image {image}..."),
    )?;
    let mut pull_command = docker_command();
    pull_command.arg("pull").arg(&image);
    run_quiet_command(pull_command, "failed to pull runtime image").await?;

    apply_config_file_actions(registry, server, &volume_path).await?;
    remove_container(&runtime_container_name(server.id)).await?;

    registry.append_console_message(server.id, "system", "Starting server container...")?;

    let mut command = docker_command();
    command
        .arg("run")
        .arg("-d")
        .arg("--name")
        .arg(runtime_container_name(server.id))
        .arg("--label")
        .arg(format!("skyport.server_id={}", server.id))
        .arg("-v")
        .arg(format!("{}:/home/container", volume_path.display()))
        .arg("-w")
        .arg("/home/container")
        .arg("--memory")
        .arg(format!("{}m", server.limits.memory_mib));

    if server.limits.cpu_limit > 0 {
        command
            .arg("--cpus")
            .arg(format!("{:.2}", server.limits.cpu_limit as f64 / 100.0));
    }

    for (key, value) in default_environment(&server.cargo) {
        command.arg("-e").arg(format!("{key}={value}"));
    }

    command
        .arg(&image)
        .arg("sh")
        .arg("-lc")
        .arg(&server.cargo.startup_command);

    let output = command
        .output()
        .await
        .context("failed to start runtime container")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let message = if stderr.is_empty() {
            "Failed to start runtime container.".to_string()
        } else {
            format!("Failed to start runtime container: {stderr}")
        };

        registry.append_console_message(server.id, "system", &message)?;
        registry.set_server_runtime(server.id, "offline", None, Some(&message))?;
        bail!(message);
    }

    let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
    registry.set_server_runtime(server.id, "starting", Some(&container_id), None)?;
    wait_for_startup(registry, server, &container_id, cancellation).await
}

async fn wait_for_startup(
    registry: &ServerRegistry,
    server: &ManagedServerRecord,
    container_id: &str,
    cancellation: &CancellationToken,
) -> Result<()> {
    let matchers = startup_matchers(&server.cargo.config_startup);
    let deadline = Instant::now() + Duration::from_secs(300);
    let mut command = docker_command();
    command
        .arg("logs")
        .arg("-f")
        .arg(runtime_container_name(server.id))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command.spawn().context("failed to follow runtime logs")?;
    let stdout = child.stdout.take().context("missing runtime stdout")?;
    let stderr = child.stderr.take().context("missing runtime stderr")?;
    let mut stdout_lines = BufReader::new(stdout).lines();
    let mut stderr_lines = BufReader::new(stderr).lines();

    loop {
        if cancellation.is_cancelled() {
            let _ = child.kill().await;
            return Ok(());
        }

        if Instant::now() >= deadline {
            let _ = child.kill().await;
            registry.append_console_message(
                server.id,
                "system",
                "Startup matcher timeout reached after 5 minutes. Marking server online.",
            )?;
            registry.set_server_runtime(server.id, "online", Some(container_id), None)?;
            return Ok(());
        }

        select! {
            line = stdout_lines.next_line() => {
                match line.context("failed to read runtime stdout")? {
                    Some(line) => {
                        registry.append_console_message(server.id, "stdout", &line)?;

                        if matches_startup(&line, &matchers) {
                            let _ = child.kill().await;
                            registry.append_console_message(server.id, "system", "Startup matcher detected. Marking server online.")?;
                            registry.set_server_runtime(server.id, "online", Some(container_id), None)?;
                            return Ok(());
                        }
                    }
                    None => {
                        if !runtime_container_is_running(server).await? {
                            let _ = child.wait().await;
                            let message = "Server exited before startup completed.";
                            registry.append_console_message(server.id, "system", message)?;
                            registry.set_server_runtime(server.id, "offline", None, Some(message))?;
                            bail!(message);
                        }
                    }
                }
            }
            line = stderr_lines.next_line() => {
                if let Some(line) = line.context("failed to read runtime stderr")? {
                    registry.append_console_message(server.id, "stderr", &line)?;

                    if matches_startup(&line, &matchers) {
                        let _ = child.kill().await;
                        registry.append_console_message(server.id, "system", "Startup matcher detected. Marking server online.")?;
                        registry.set_server_runtime(server.id, "online", Some(container_id), None)?;
                        return Ok(());
                    }
                }
            }
            _ = time::sleep(Duration::from_millis(250)) => {}
        }
    }
}

async fn apply_config_file_actions(
    registry: &ServerRegistry,
    server: &ManagedServerRecord,
    volume_path: &Path,
) -> Result<()> {
    let raw = server.cargo.config_files.trim();

    if raw.is_empty() || raw == "{}" {
        return Ok(());
    }

    let config: Value =
        serde_json::from_str(raw).context("failed to parse cargo config.files JSON")?;
    let Some(files) = config.as_object() else {
        bail!("cargo config.files must be a JSON object");
    };

    for (relative_path, rule) in files {
        let parser = rule
            .get("parser")
            .and_then(Value::as_str)
            .unwrap_or_default();

        match parser {
            "properties" => {
                let file_path = volume_path.join(relative_path);
                let mut replacements = BTreeMap::new();
                if let Some(find) = rule.get("find").and_then(Value::as_object) {
                    for (key, value) in find {
                        replacements.insert(
                            key.clone(),
                            interpolate_config_value(value.as_str().unwrap_or_default(), server),
                        );
                    }
                }

                apply_properties_replacements(&file_path, &replacements)?;
                registry.append_console_message(
                    server.id,
                    "system",
                    &format!("Applied config file actions to {}.", relative_path),
                )?;
            }
            _ => {
                registry.append_console_message(
                    server.id,
                    "system",
                    &format!(
                        "Skipping unsupported config parser '{}' for {}.",
                        parser, relative_path
                    ),
                )?;
            }
        }
    }

    Ok(())
}

fn apply_properties_replacements(
    path: &Path,
    replacements: &BTreeMap<String, String>,
) -> Result<()> {
    let existing = std::fs::read_to_string(path).unwrap_or_default();
    let mut lines: Vec<String> = if existing.is_empty() {
        Vec::new()
    } else {
        existing.lines().map(str::to_string).collect()
    };

    for (key, value) in replacements {
        let mut updated = false;

        for line in &mut lines {
            let trimmed = line.trim_start();
            if trimmed.starts_with('#') || trimmed.starts_with(';') {
                continue;
            }

            if let Some((current_key, _)) = line.split_once('=') {
                if current_key.trim() == key {
                    *line = format!("{key}={value}");
                    updated = true;
                    break;
                }
            }
        }

        if !updated {
            lines.push(format!("{key}={value}"));
        }
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    std::fs::write(path, lines.join("\n") + "\n")
        .with_context(|| format!("failed to write {}", path.display()))?;

    Ok(())
}

fn interpolate_config_value(value: &str, server: &ManagedServerRecord) -> String {
    value
        .replace(
            "{{server.memory_limit}}",
            &server.limits.memory_mib.to_string(),
        )
        .replace("{{server.disk_limit}}", &server.limits.disk_mib.to_string())
        .replace("{{server.cpu_limit}}", &server.limits.cpu_limit.to_string())
        .replace("{{server.id}}", &server.id.to_string())
}

fn startup_matchers(config_startup: &str) -> Vec<String> {
    let trimmed = config_startup.trim();

    if trimmed.is_empty() || trimmed == "{}" {
        return Vec::new();
    }

    match serde_json::from_str::<Value>(trimmed) {
        Ok(value) => collect_string_values(&value),
        Err(_) => vec![trimmed.to_string()],
    }
}

fn collect_string_values(value: &Value) -> Vec<String> {
    match value {
        Value::String(value) => vec![value.clone()],
        Value::Array(values) => values.iter().flat_map(collect_string_values).collect(),
        Value::Object(values) => values.values().flat_map(collect_string_values).collect(),
        _ => Vec::new(),
    }
}

fn matches_startup(line: &str, matchers: &[String]) -> bool {
    matchers
        .iter()
        .any(|matcher| !matcher.is_empty() && line.contains(matcher))
}

fn default_environment(cargo: &ManagedServerCargo) -> BTreeMap<String, String> {
    cargo
        .variables
        .iter()
        .map(|variable| {
            (
                variable.env_variable.clone(),
                variable.default_value.clone(),
            )
        })
        .collect()
}

fn resolve_volume_path(_config: &DaemonConfig, server: &ManagedServerRecord) -> Result<PathBuf> {
    Ok(project_root()?.join(&server.volume_path))
}

fn ensure_disk_limit(path: &Path, disk_limit_mib: u64) -> Result<()> {
    let used_bytes = directory_size(path)?;
    let used_mib = used_bytes / (1024 * 1024);

    if used_mib > disk_limit_mib {
        bail!(
            "Server volume exceeds disk limit: used {} MiB, limit {} MiB.",
            used_mib,
            disk_limit_mib,
        );
    }

    Ok(())
}

fn directory_size(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }

    let mut total = 0_u64;

    for entry in
        std::fs::read_dir(path).with_context(|| format!("failed to read {}", path.display()))?
    {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total += directory_size(&entry.path())?;
        } else {
            total += metadata.len();
        }
    }

    Ok(total)
}

fn select_runtime_image(cargo: &ManagedServerCargo) -> Result<String> {
    cargo
        .docker_images
        .values()
        .next()
        .cloned()
        .ok_or_else(|| anyhow!("cargo does not define a runtime docker image"))
}

async fn runtime_container_is_running(server: &ManagedServerRecord) -> Result<bool> {
    let output = docker_command()
        .arg("inspect")
        .arg("-f")
        .arg("{{.State.Running}}")
        .arg(runtime_container_name(server.id))
        .output()
        .await
        .context("failed to inspect runtime container")?;

    if !output.status.success() {
        return Ok(false);
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim() == "true")
}

async fn remove_container(name: &str) -> Result<()> {
    let _ = docker_command()
        .arg("rm")
        .arg("-f")
        .arg(name)
        .output()
        .await;

    Ok(())
}

fn runtime_container_name(server_id: u64) -> String {
    format!("skyport-server-{server_id}")
}

fn install_container_name(server_id: u64) -> String {
    format!("skyport-install-{server_id}")
}

fn docker_command() -> Command {
    let mut command = Command::new("docker");
    command.kill_on_drop(true);
    command
}

async fn run_quiet_command(mut command: Command, context: &str) -> Result<()> {
    let output = command
        .output()
        .await
        .with_context(|| context.to_string())?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if stderr.is_empty() {
            bail!(context.to_string());
        }

        bail!("{context}: {stderr}");
    }

    Ok(())
}

async fn run_streaming_command(
    mut command: Command,
    registry: &ServerRegistry,
    server_id: u64,
    source: &str,
    cancellation: &CancellationToken,
) -> Result<std::process::ExitStatus> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().context("failed to spawn docker command")?;
    let stdout = child.stdout.take().context("missing child stdout")?;
    let stderr = child.stderr.take().context("missing child stderr")?;
    let mut stdout_lines = BufReader::new(stdout).lines();
    let mut stderr_lines = BufReader::new(stderr).lines();

    loop {
        select! {
            _ = cancellation.cancelled() => {
                let _ = child.kill().await;
                return child.wait().await.context("failed to stop docker command");
            }
            line = stdout_lines.next_line() => {
                match line.context("failed to read command stdout")? {
                    Some(line) => registry.append_console_message(server_id, source, &line)?,
                    None => break,
                }
            }
            line = stderr_lines.next_line() => {
                if let Some(line) = line.context("failed to read command stderr")? {
                    registry.append_console_message(server_id, source, &line)?;
                }
            }
        }
    }

    child
        .wait()
        .await
        .context("failed to wait for docker command")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn startup_matchers_collect_string_values() {
        let matchers = startup_matchers(r#"{"done":")! For help, type "}"#);

        assert_eq!(matchers, vec![")! For help, type "]);
    }

    #[test]
    fn properties_replacements_update_existing_and_new_keys() {
        let tempdir = tempdir().unwrap();
        let file = tempdir.path().join("server.properties");
        std::fs::write(&file, "server-ip=127.0.0.1\nmotd=Hello\n").unwrap();

        let replacements = BTreeMap::from([
            ("server-ip".to_string(), "0.0.0.0".to_string()),
            ("server-port".to_string(), "25565".to_string()),
        ]);

        apply_properties_replacements(&file, &replacements).unwrap();

        let contents = std::fs::read_to_string(file).unwrap();
        assert!(contents.contains("server-ip=0.0.0.0"));
        assert!(contents.contains("server-port=25565"));
    }

    #[test]
    fn directory_size_returns_zero_for_missing_paths() {
        let tempdir = tempdir().unwrap();
        let missing = tempdir.path().join("missing");

        assert_eq!(directory_size(&missing).unwrap(), 0);
    }
}
