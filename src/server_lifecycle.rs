use std::collections::{BTreeMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::Client;
use serde_json::Value;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::select;
use tokio::sync::{Mutex, watch};
use tokio::time::{self, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{DaemonConfig, managed_server_volume_path, safe_join_relative};
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
                        let failure_status =
                            reconciliation_failure_status(&server.status)
                                .to_string();

                        tokio::spawn(async move {
                            let server_id = server.id;
                            if let Err(error) = reconcile_server(registry.clone(), server, config.clone(), cancellation).await {
                                warn!(server_id, error = %error, "server reconciliation failed");
                                let _ = set_runtime_state(
                                    &registry,
                                    &config,
                                    server_id,
                                    &failure_status,
                                    None,
                                    Some(&error.to_string()),
                                ).await;
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
        "pending" | "starting" | "running" | "stopping" | "restarting" => true,
        "offline" => false,
        "installing" => true,
        "install_failed" => false,
        _ => true,
    }
}

fn reconciliation_failure_status(status: &str) -> &'static str {
    match status {
        "pending" | "installing" => "install_failed",
        _ => "offline",
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
        "starting" | "running" => {
            if !runtime_container_is_running(&server).await? {
                boot_server(&registry, &mut server, &config, &cancellation).await?;
            }
        }
        "stopping" => {
            if !runtime_container_is_running(&server).await? {
                set_runtime_state(&registry, &config, server.id, "offline", None, None).await?;
            }
        }
        "restarting" => {
            if !runtime_container_is_running(&server).await? {
                server.status = "offline".to_string();
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

    let install_log_path = volume_path.join("install.log");
    reset_install_log(&install_log_path)?;

    set_runtime_state(registry, config, server.id, "installing", None, None).await?;
    registry.append_console_message(server.id, "system", "Preparing volume directory...")?;
    append_install_log(&install_log_path, "system", "Preparing volume directory...")?;

    let install_script = normalize_shell_script(
        server
            .cargo
            .install_script
            .as_deref()
            .map(str::trim)
            .unwrap_or(""),
    );
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
        append_install_log(
            &install_log_path,
            "system",
            "No install container configured. Skipping installation.",
        )?;
        set_runtime_state(registry, config, server.id, "offline", None, None).await?;
        server.status = "offline".to_string();

        return boot_server(registry, server, config, cancellation).await;
    }

    registry.append_console_message(
        server.id,
        "system",
        &format!("Running install container {install_image}..."),
    )?;
    append_install_log(
        &install_log_path,
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
        .arg(entrypoint);

    for (key, value) in default_environment(&server.cargo) {
        command.arg("-e").arg(format!("{key}={value}"));
    }

    command.arg(install_image).arg("-c").arg(&install_script);

    let status = run_streaming_command(
        command,
        registry,
        server.id,
        "install",
        cancellation,
        Some(&install_log_path),
    )
    .await?;

    if !status.success() {
        let code = status.code().unwrap_or_default();
        let message = format!("Installation failed with exit code {code}.");
        registry.append_console_message(server.id, "system", &message)?;
        append_install_log(&install_log_path, "system", &message)?;
        set_runtime_state(
            registry,
            config,
            server.id,
            "install_failed",
            None,
            Some(&message),
        )
        .await?;
        bail!(message);
    }

    registry.append_console_message(server.id, "system", "Installation completed successfully.")?;
    append_install_log(
        &install_log_path,
        "system",
        "Installation completed successfully.",
    )?;
    set_runtime_state(registry, config, server.id, "offline", None, None).await?;
    server.status = "offline".to_string();

    match boot_server(registry, server, config, cancellation).await {
        Ok(()) => Ok(()),
        Err(error) => {
            registry.append_console_message(
                server.id,
                "system",
                &format!(
                    "Initial startup failed after installation. The server remains offline: {error}"
                ),
            )?;
            Ok(())
        }
    }
}

async fn boot_server(
    registry: &ServerRegistry,
    server: &mut ManagedServerRecord,
    config: &DaemonConfig,
    cancellation: &CancellationToken,
) -> Result<()> {
    let volume_path = resolve_volume_path(config, server)?;
    ensure_server_boot_prerequisites(server, &volume_path)?;
    registry.append_console_message(server.id, "system", "Checking disk space limits...")?;
    ensure_disk_limit(&volume_path, server.limits.disk_mib)?;

    let image = select_runtime_image(server)?;
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

    registry.append_console_message(
        server.id,
        "system",
        "All systems ready! Starting server...",
    )?;

    let mut command = docker_command();
    command.args(runtime_container_run_args(server, &volume_path, &image));

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
        set_runtime_state(registry, config, server.id, "offline", None, Some(&message)).await?;
        bail!(message);
    }

    let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
    set_runtime_state(
        registry,
        config,
        server.id,
        "starting",
        Some(&container_id),
        None,
    )
    .await?;
    wait_for_startup(registry, server, config, &container_id, cancellation).await
}

async fn wait_for_startup(
    registry: &ServerRegistry,
    server: &ManagedServerRecord,
    config: &DaemonConfig,
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
    let mut stdout_open = true;
    let mut stderr_open = true;

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
                "Startup matcher timeout reached after 5 minutes. Marking server running.",
            )?;
            set_runtime_state(
                registry,
                config,
                server.id,
                "running",
                Some(container_id),
                None,
            )
            .await?;
            spawn_runtime_log_follower(
                registry.clone(),
                server.clone(),
                cancellation.child_token(),
            );
            return Ok(());
        }

        select! {
            line = stdout_lines.next_line(), if stdout_open => {
                match line.context("failed to read runtime stdout")? {
                    Some(line) => {
                        registry.append_console_message(server.id, "stdout", &line)?;

                        if matches_startup(&line, &matchers) {
                            let _ = child.kill().await;
                            registry.append_console_message(server.id, "system", "Startup matcher detected. Marking server running.")?;
                            set_runtime_state(registry, config, server.id, "running", Some(container_id), None).await?;
                            spawn_runtime_log_follower(
                                registry.clone(),
                                server.clone(),
                                cancellation.child_token(),
                            );
                            return Ok(());
                        }
                    }
                    None => {
                        stdout_open = false;

                        if !runtime_container_is_running(server).await? {
                            let _ = child.wait().await;
                            let message = "Server exited before startup completed.";
                            registry.append_console_message(server.id, "system", message)?;
                            set_runtime_state(registry, config, server.id, "offline", None, Some(message)).await?;
                            bail!(message);
                        }
                    }
                }
            }
            line = stderr_lines.next_line(), if stderr_open => {
                match line.context("failed to read runtime stderr")? {
                    Some(line) => {
                        registry.append_console_message(server.id, "stderr", &line)?;

                        if matches_startup(&line, &matchers) {
                            let _ = child.kill().await;
                            registry.append_console_message(server.id, "system", "Startup matcher detected. Marking server running.")?;
                            set_runtime_state(registry, config, server.id, "running", Some(container_id), None).await?;
                            spawn_runtime_log_follower(
                                registry.clone(),
                                server.clone(),
                                cancellation.child_token(),
                            );
                            return Ok(());
                        }
                    }
                    None => {
                        stderr_open = false;
                    }
                }
            }
            _ = time::sleep(Duration::from_millis(250)) => {}
        }

        if !stdout_open && !stderr_open {
            if !runtime_container_is_running(server).await? {
                let _ = child.wait().await;
                let message = "Server exited before startup completed.";
                registry.append_console_message(server.id, "system", message)?;
                set_runtime_state(registry, config, server.id, "offline", None, Some(message))
                    .await?;
                bail!(message);
            }

            bail!("Runtime log streams ended before startup completed.");
        }
    }
}

fn spawn_runtime_log_follower(
    registry: ServerRegistry,
    server: ManagedServerRecord,
    cancellation: CancellationToken,
) {
    tokio::spawn(async move {
        if let Err(error) = follow_runtime_logs(registry, server, cancellation).await {
            warn!(error = %error, "runtime log follower stopped unexpectedly");
        }
    });
}

async fn follow_runtime_logs(
    registry: ServerRegistry,
    server: ManagedServerRecord,
    cancellation: CancellationToken,
) -> Result<()> {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string();

    let mut command = docker_command();
    command
        .arg("logs")
        .arg("--follow")
        .arg("--since")
        .arg(since)
        .arg(runtime_container_name(server.id))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .context("failed to follow runtime logs after startup")?;
    let stdout = child.stdout.take().context("missing runtime log stdout")?;
    let stderr = child.stderr.take().context("missing runtime log stderr")?;
    let mut stdout_lines = BufReader::new(stdout).lines();
    let mut stderr_lines = BufReader::new(stderr).lines();
    let mut stdout_open = true;
    let mut stderr_open = true;

    loop {
        if cancellation.is_cancelled() {
            let _ = child.kill().await;
            return Ok(());
        }

        select! {
            line = stdout_lines.next_line(), if stdout_open => {
                match line.context("failed to read runtime stdout after startup")? {
                    Some(line) => {
                        registry.append_console_message(server.id, "stdout", &line)?;
                    }
                    None => {
                        stdout_open = false;

                        if !runtime_container_is_running(&server).await? {
                            let _ = child.wait().await;
                            return Ok(());
                        }
                    }
                }
            }
            line = stderr_lines.next_line(), if stderr_open => {
                match line.context("failed to read runtime stderr after startup")? {
                    Some(line) => {
                        registry.append_console_message(server.id, "stderr", &line)?;
                    }
                    None => {
                        stderr_open = false;

                        if !runtime_container_is_running(&server).await? {
                            let _ = child.wait().await;
                            return Ok(());
                        }
                    }
                }
            }
            _ = time::sleep(Duration::from_millis(250)) => {}
        }

        if !stdout_open && !stderr_open {
            if !runtime_container_is_running(&server).await? {
                let _ = child.wait().await;
                return Ok(());
            }

            bail!("Runtime log follower lost both streams while the container is still running.");
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
                let file_path = safe_join_relative(volume_path, relative_path)?;
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
    interpolate_runtime_string(value, server)
}

fn interpolate_runtime_string(value: &str, server: &ManagedServerRecord) -> String {
    let interpolated = value
        .replace(
            "{{server.memory_limit}}",
            &server.limits.memory_mib.to_string(),
        )
        .replace("{{server.disk_limit}}", &server.limits.disk_mib.to_string())
        .replace("{{server.cpu_limit}}", &server.limits.cpu_limit.to_string())
        .replace("{{server.id}}", &server.id.to_string())
        .replace(
            "{{server.build.default.port}}",
            &server.allocation.port.to_string(),
        )
        .replace("{{server.build.default.ip}}", &server.allocation.bind_ip)
        .replace(
            "{{server.build.default.host}}",
            server
                .allocation
                .ip_alias
                .as_deref()
                .unwrap_or(server.allocation.bind_ip.as_str()),
        );

    default_environment(&server.cargo)
        .into_iter()
        .fold(interpolated, |result, (key, value)| {
            result.replace(&format!("{{{{{key}}}}}"), &value)
        })
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

fn ensure_server_boot_prerequisites(
    server: &ManagedServerRecord,
    volume_path: &Path,
) -> Result<()> {
    if cargo_has_feature(&server.cargo, "eula") {
        write_eula_file(volume_path)?;
    }

    Ok(())
}

fn cargo_has_feature(cargo: &ManagedServerCargo, feature: &str) -> bool {
    cargo.features.iter().any(|value| value == feature)
}

fn write_eula_file(volume_path: &Path) -> Result<()> {
    std::fs::write(volume_path.join("eula.txt"), "eula=true\n").context("failed to write eula.txt")
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

fn runtime_environment(server: &ManagedServerRecord) -> BTreeMap<String, String> {
    let mut environment = default_environment(&server.cargo);

    environment.insert(
        "STARTUP".to_string(),
        interpolate_runtime_string(&server.cargo.startup_command, server),
    );
    environment.insert(
        "SERVER_MEMORY".to_string(),
        server.limits.memory_mib.to_string(),
    );
    environment.insert(
        "SERVER_CPU".to_string(),
        server.limits.cpu_limit.to_string(),
    );
    environment.insert(
        "SERVER_DISK".to_string(),
        server.limits.disk_mib.to_string(),
    );
    environment.insert("SERVER_IP".to_string(), server.allocation.bind_ip.clone());
    environment.insert(
        "SERVER_PORT".to_string(),
        server.allocation.port.to_string(),
    );

    environment
}

fn runtime_container_run_args(
    server: &ManagedServerRecord,
    volume_path: &Path,
    image: &str,
) -> Vec<String> {
    let mut args = vec![
        "run".to_string(),
        "-d".to_string(),
        "-i".to_string(),
        "--name".to_string(),
        runtime_container_name(server.id),
        "--label".to_string(),
        format!("skyport.server_id={}", server.id),
        "-v".to_string(),
        format!("{}:/home/container", volume_path.display()),
        "-w".to_string(),
        "/home/container".to_string(),
        "-p".to_string(),
        format!(
            "{}:{}:{}",
            server.allocation.bind_ip, server.allocation.port, server.allocation.port
        ),
        "--memory".to_string(),
        format!("{}m", server.limits.memory_mib),
    ];

    if server.limits.cpu_limit > 0 {
        args.push("--cpus".to_string());
        args.push(format!("{:.2}", server.limits.cpu_limit as f64 / 100.0));
    }

    for (key, value) in runtime_environment(server) {
        args.push("-e".to_string());
        args.push(format!("{key}={value}"));
    }

    args.push(image.to_string());

    args
}

fn normalize_shell_script(script: &str) -> String {
    script.replace("\r\n", "\n").replace('\r', "\n")
}

fn reset_install_log(path: &Path) -> Result<()> {
    std::fs::write(path, "").with_context(|| format!("failed to reset {}", path.display()))
}

fn append_install_log(path: &Path, source: &str, message: &str) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;

    writeln!(file, "[{source}] {message}")
        .with_context(|| format!("failed to write {}", path.display()))
}

async fn set_runtime_state(
    registry: &ServerRegistry,
    config: &DaemonConfig,
    server_id: u64,
    status: &str,
    container_id: Option<&str>,
    last_error: Option<&str>,
) -> Result<()> {
    registry.set_server_runtime(server_id, status, container_id, last_error)?;

    if let Err(error) = notify_panel_runtime_update(config, server_id, status, last_error).await {
        warn!(server_id, status, error = %error, "failed to notify panel about runtime update");
    }

    Ok(())
}

async fn notify_panel_runtime_update(
    config: &DaemonConfig,
    server_id: u64,
    status: &str,
    last_error: Option<&str>,
) -> Result<()> {
    let Some(daemon_secret) = config.panel.daemon_secret.as_deref() else {
        return Ok(());
    };

    let response = Client::new()
        .post(format!(
            "{}/api/daemon/servers/{server_id}/runtime",
            config.panel.url.trim_end_matches('/'),
        ))
        .bearer_auth(daemon_secret)
        .json(&json!({
            "uuid": config.daemon.uuid,
            "version": env!("CARGO_PKG_VERSION"),
            "status": status,
            "last_error": last_error,
        }))
        .send()
        .await
        .context("failed to notify panel about runtime update")?
        .error_for_status()
        .context("panel rejected runtime update")?;

    let _ = response;

    Ok(())
}

fn resolve_volume_path(_config: &DaemonConfig, server: &ManagedServerRecord) -> Result<PathBuf> {
    managed_server_volume_path(server.id)
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

fn select_runtime_image(server: &ManagedServerRecord) -> Result<String> {
    if let Some(image) = &server.docker_image {
        if server
            .cargo
            .docker_images
            .values()
            .any(|value| value == image)
        {
            return Ok(image.clone());
        }
    }

    if let Some(image) = highest_java_runtime_image(&server.cargo.docker_images) {
        return Ok(image);
    }

    server
        .cargo
        .docker_images
        .values()
        .next()
        .cloned()
        .ok_or_else(|| anyhow!("cargo does not define a runtime docker image"))
}

fn highest_java_runtime_image(images: &BTreeMap<String, String>) -> Option<String> {
    images
        .iter()
        .filter_map(|(label, image)| {
            parse_java_major_version(label).map(|version| (version, image))
        })
        .max_by_key(|(version, _)| *version)
        .map(|(_, image)| image.clone())
}

fn parse_java_major_version(label: &str) -> Option<u64> {
    label
        .split(|character: char| !character.is_ascii_digit())
        .find(|segment| !segment.is_empty())
        .and_then(|segment| segment.parse::<u64>().ok())
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
    install_log_path: Option<&Path>,
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
                    Some(line) => {
                        registry.append_console_message(server_id, source, &line)?;
                        if let Some(install_log_path) = install_log_path {
                            append_install_log(install_log_path, source, &line)?;
                        }
                    }
                    None => break,
                }
            }
            line = stderr_lines.next_line() => {
                if let Some(line) = line.context("failed to read command stderr")? {
                    registry.append_console_message(server_id, source, &line)?;
                    if let Some(install_log_path) = install_log_path {
                        append_install_log(install_log_path, source, &line)?;
                    }
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
    use crate::server_registry::ServerRegistry;
    use tempfile::tempdir;

    #[test]
    fn startup_matchers_collect_string_values() {
        let matchers = startup_matchers(r#"{"done":")! For help, type "}"#);

        assert_eq!(matchers, vec![")! For help, type "]);
    }

    #[test]
    fn highest_java_runtime_image_uses_newest_java_label() {
        let images = BTreeMap::from([
            ("Java 25".to_string(), "java-25".to_string()),
            ("Java 11".to_string(), "java-11".to_string()),
            ("Java 21".to_string(), "java-21".to_string()),
        ]);

        assert_eq!(
            highest_java_runtime_image(&images),
            Some("java-25".to_string())
        );
    }

    #[test]
    fn select_runtime_image_prefers_the_server_override() {
        let mut server = sample_server();
        server.cargo.docker_images = BTreeMap::from([
            ("Java 21".to_string(), "java-21".to_string()),
            ("Java 17".to_string(), "java-17".to_string()),
        ]);
        server.docker_image = Some("java-17".to_string());

        assert_eq!(select_runtime_image(&server).unwrap(), "java-17");
    }

    #[test]
    fn daemon_console_text_uses_updated_boot_messages() {
        let source = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/server_lifecycle.rs"
        ));
        let removed_start_message = ["Starting server", " container..."].concat();
        let removed_config_action_message = ["Applied config file actions", " to"].concat();

        assert!(source.contains("All systems ready! Starting server..."));
        assert!(!source.contains(&removed_start_message));
        assert!(!source.contains(&removed_config_action_message));
    }

    #[test]
    fn ensure_server_boot_prerequisites_accepts_eula_when_required() {
        let tempdir = tempdir().unwrap();
        let mut server = sample_server();
        server.cargo.features = vec!["eula".to_string()];

        ensure_server_boot_prerequisites(&server, tempdir.path()).unwrap();

        let eula = std::fs::read_to_string(tempdir.path().join("eula.txt")).unwrap();
        assert_eq!(eula, "eula=true\n");
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

    #[tokio::test]
    async fn config_file_actions_reject_paths_outside_the_server_volume() {
        let tempdir = tempdir().unwrap();
        let registry = ServerRegistry::new(tempdir.path().join("skyportd.db"));
        registry.initialize().unwrap();

        let mut server = sample_server();
        server.cargo.config_files =
            r#"{"../../host.properties":{"parser":"properties","find":{"motd":"Hello"}}}"#
                .to_string();

        let error = apply_config_file_actions(&registry, &server, tempdir.path())
            .await
            .expect_err("path traversal should be rejected")
            .to_string();

        assert!(error.contains("within the server volume"));
        assert!(!tempdir.path().join("../../host.properties").exists());
    }

    #[test]
    fn runtime_interpolation_replaces_cargo_variable_placeholders() {
        let server = sample_server();

        assert_eq!(
            interpolate_runtime_string(&server.cargo.startup_command, &server),
            "java -jar server.jar",
        );
    }

    #[test]
    fn runtime_environment_includes_pterodactyl_style_values() {
        let server = sample_server();
        let environment = runtime_environment(&server);

        assert_eq!(
            environment.get("SERVER_JARFILE"),
            Some(&"server.jar".to_string())
        );
        assert_eq!(
            environment.get("STARTUP"),
            Some(&"java -jar server.jar".to_string())
        );
        assert_eq!(environment.get("SERVER_MEMORY"), Some(&"1024".to_string()));
        assert_eq!(environment.get("SERVER_CPU"), Some(&"100".to_string()));
        assert_eq!(environment.get("SERVER_DISK"), Some(&"10240".to_string()));
        assert_eq!(environment.get("SERVER_IP"), Some(&"0.0.0.0".to_string()));
        assert_eq!(environment.get("SERVER_PORT"), Some(&"25565".to_string()));
    }

    #[test]
    fn runtime_container_args_keep_stdin_open_for_console_commands() {
        let server = sample_server();
        let args = runtime_container_run_args(
            &server,
            Path::new("/tmp/server"),
            "ghcr.io/example/runtime:latest",
        );

        assert!(args.iter().any(|arg| arg == "-i"));
        assert_eq!(args.first(), Some(&"run".to_string()));
        assert!(
            args.iter()
                .any(|arg| arg == "ghcr.io/example/runtime:latest")
        );
    }

    fn sample_server() -> ManagedServerRecord {
        ManagedServerRecord {
            id: 1,
            node_id: 1,
            name: "Paper".to_string(),
            status: "offline".to_string(),
            volume_path: "volumes/1".to_string(),
            created_at: "2026-04-06T00:00:00Z".to_string(),
            updated_at: "2026-04-06T00:00:00Z".to_string(),
            docker_image: None,
            allocation: crate::server_registry::ManagedServerAllocation {
                id: 1,
                bind_ip: "0.0.0.0".to_string(),
                port: 25565,
                ip_alias: None,
            },
            container_id: None,
            last_error: None,
            user: crate::server_registry::ManagedServerUser {
                id: 1,
                name: "Test".to_string(),
                email: "test@example.com".to_string(),
            },
            limits: crate::server_registry::ManagedServerLimits {
                memory_mib: 1024,
                cpu_limit: 100,
                disk_mib: 10240,
            },
            cargo: ManagedServerCargo {
                id: 1,
                name: "Paper".to_string(),
                slug: "paper".to_string(),
                source_type: "pterodactyl".to_string(),
                startup_command: "java -jar {{SERVER_JARFILE}}".to_string(),
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
                variables: vec![crate::server_registry::ManagedServerVariable {
                    name: "Server Jar File".to_string(),
                    description: "".to_string(),
                    env_variable: "SERVER_JARFILE".to_string(),
                    default_value: "server.jar".to_string(),
                    user_viewable: true,
                    user_editable: true,
                    rules: "required".to_string(),
                    field_type: "text".to_string(),
                }],
                definition: Value::Null,
            },
        }
    }

    #[tokio::test]
    async fn offline_servers_with_last_error_do_not_reconcile_automatically() {
        let config = DaemonConfig {
            daemon: crate::config::DaemonSection {
                name: "skyportd".to_string(),
                uuid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
                tick_interval: Duration::from_secs(30),
                shutdown_timeout: Duration::from_secs(30),
            },
            panel: crate::config::PanelSection {
                url: "http://localhost".to_string(),
                configuration_token: None,
                daemon_secret: None,
                daemon_callback_token: None,
                node_id: Some(1),
            },
            logging: crate::config::LoggingSection {
                level: "info".to_string(),
                format: crate::config::LogFormat::Pretty,
            },
            runtime: crate::config::RuntimeSection { worker_threads: 1 },
            node: None,
        };
        let mut server = sample_server();
        server.last_error = Some("Server exited before startup completed.".to_string());

        assert!(!should_reconcile(&server, &config).await);
    }

    #[test]
    fn reconciliation_failures_only_mark_install_failed_during_installation() {
        assert_eq!(reconciliation_failure_status("pending"), "install_failed");
        assert_eq!(
            reconciliation_failure_status("installing"),
            "install_failed"
        );
        assert_eq!(reconciliation_failure_status("offline"), "offline");
        assert_eq!(reconciliation_failure_status("starting"), "offline");
        assert_eq!(reconciliation_failure_status("running"), "offline");
    }

    #[test]
    fn directory_size_returns_zero_for_missing_paths() {
        let tempdir = tempdir().unwrap();
        let missing = tempdir.path().join("missing");

        assert_eq!(directory_size(&missing).unwrap(), 0);
    }

    #[test]
    fn normalize_shell_script_removes_windows_line_endings() {
        assert_eq!(
            normalize_shell_script("echo test\r\nif true; then\r\n  echo ok\r\nfi\r\n"),
            "echo test\nif true; then\n  echo ok\nfi\n",
        );
    }

    #[test]
    fn append_install_log_writes_prefixed_lines() {
        let tempdir = tempdir().unwrap();
        let log_path = tempdir.path().join("install.log");

        reset_install_log(&log_path).unwrap();
        append_install_log(&log_path, "system", "Preparing volume directory...").unwrap();
        append_install_log(&log_path, "install", "Downloading files...").unwrap();

        let contents = std::fs::read_to_string(log_path).unwrap();
        assert!(contents.contains("[system] Preparing volume directory..."));
        assert!(contents.contains("[install] Downloading files..."));
    }
}
