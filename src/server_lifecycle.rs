use std::collections::{BTreeMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use serde_json::Value;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::select;
use tokio::sync::{Mutex, Semaphore, watch};
use tokio::time::{self, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{DaemonConfig, managed_server_volume_path, safe_join_relative};
use crate::docker::{DockerOutput, docker_command, running_runtime_containers};
use crate::server_registry::{ManagedServerCargo, ManagedServerRecord, ServerRegistry};

/// `uid:gid` that owns the server volumes and runs the runtime containers.
const CONTAINER_USER: &str = "1000:1000";

/// Maximum number of servers preparing to start (pull, permissions, install)
/// at the same time. Without a limit a node reboot would pull and start every
/// server at once and saturate the disk and the network.
const MAX_CONCURRENT_BOOTS: usize = 4;
static BOOT_SLOTS: Semaphore = Semaphore::const_new(MAX_CONCURRENT_BOOTS);

/// Number of past log lines replayed when the daemon attaches to a container
/// it did not start.
const ADOPTED_LOG_TAIL_LINES: u32 = 200;

/// Servers that currently have a live `docker logs --follow` task.
static FOLLOWED_SERVERS: LazyLock<std::sync::Mutex<HashSet<u64>>> =
    LazyLock::new(|| std::sync::Mutex::new(HashSet::new()));

struct FollowerGuard(u64);

impl Drop for FollowerGuard {
    fn drop(&mut self) {
        FOLLOWED_SERVERS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.0);
    }
}

fn claim_follower(server_id: u64) -> Option<FollowerGuard> {
    let mut followed = FOLLOWED_SERVERS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    followed.insert(server_id).then(|| FollowerGuard(server_id))
}

fn is_followed(server_id: u64) -> bool {
    FOLLOWED_SERVERS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .contains(&server_id)
}

/// Where a log follower starts reading the container output.
#[derive(Clone, Copy, Debug)]
enum LogStart {
    /// Only output produced from now on.
    Now,
    /// The last `n` lines, then everything new.
    Tail(u32),
}

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
        // Servers with a reconciliation currently in flight.
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

                    let statuses = match self.server_registry.list_server_statuses_for_node(node_id) {
                        Ok(statuses) => statuses,
                        Err(error) => {
                            warn!(error = %error, "failed to list servers, retrying next tick");
                            continue;
                        }
                    };

                    // One `docker ps` for the whole node instead of one
                    // `docker inspect` per server. If docker cannot answer we
                    // skip the tick rather than guess that servers are down.
                    let running = match running_runtime_containers().await {
                        Ok(running) => running,
                        Err(error) => {
                            warn!(error = %error, "docker is unavailable, skipping reconciliation tick");
                            continue;
                        }
                    };

                    for (server_id, status) in statuses {
                        if !should_reconcile_status(&status) {
                            continue;
                        }

                        // Fast path: a healthy server we already follow needs nothing.
                        if status == "running"
                            && running.contains(&runtime_container_name(server_id))
                            && is_followed(server_id)
                        {
                            continue;
                        }

                        if !active.lock().await.insert(server_id) {
                            continue;
                        }

                        let registry = self.server_registry.clone();
                        let active = Arc::clone(&active);
                        let cancellation = self.cancellation.child_token();
                        let config = config.clone();

                        tokio::spawn(async move {
                            reconcile_one(registry, server_id, config, cancellation).await;
                            active.lock().await.remove(&server_id);
                        });
                    }
                }
            }
        }

        Ok(())
    }
}

async fn reconcile_one(
    registry: ServerRegistry,
    server_id: u64,
    config: DaemonConfig,
    cancellation: CancellationToken,
) {
    let server = match registry.get_server(server_id) {
        Ok(Some(server)) => server,
        Ok(None) => return,
        Err(error) => {
            warn!(server_id, error = %error, "failed to load server for reconciliation");
            return;
        }
    };

    if !should_reconcile(&server, &config).await {
        return;
    }

    let failure_status = reconciliation_failure_status(&server.status).to_string();

    if let Err(error) = reconcile_server(registry.clone(), server, config.clone(), cancellation).await {
        // A hung or unreachable docker daemon says nothing about the server
        // itself; leave its state alone and retry on the next tick.
        if is_transient_docker_error(&error) {
            warn!(server_id, error = %error, "docker is not responding, will retry reconciliation");
            return;
        }

        warn!(server_id, error = %error, "server reconciliation failed");
        let _ = set_runtime_state(
            &registry,
            &config,
            server_id,
            &failure_status,
            None,
            Some(&error.to_string()),
        )
        .await;
        let _ = registry.append_console_message(
            server_id,
            "system",
            &format!("Reconciliation failed: {error}"),
        );
    }
}

fn is_transient_docker_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::TimedOut)
            || cause
                .to_string()
                .contains("Cannot connect to the Docker daemon")
    })
}

fn should_reconcile_status(status: &str) -> bool {
    match status {
        "pending" | "starting" | "running" | "stopping" | "restarting" => true,
        "offline" => false,
        "installing" => true,
        "install_failed" => false,
        _ => true,
    }
}

async fn should_reconcile(server: &ManagedServerRecord, config: &DaemonConfig) -> bool {
    if config.panel.node_id != Some(server.node_id) {
        return false;
    }

    should_reconcile_status(&server.status)
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
            if runtime_container_is_running(&server).await? {
                if is_followed(server.id) {
                    return Ok(());
                }

                if server.status == "starting" {
                    // The daemon went away while the server was booting:
                    // resume waiting for the startup marker.
                    let container_id = runtime_container_id(server.id)
                        .await?
                        .unwrap_or_default();
                    registry.append_console_message(
                        server.id,
                        "system",
                        "Resuming startup monitoring after a daemon restart.",
                    )?;
                    wait_for_startup(&registry, &server, &config, &container_id, &cancellation)
                        .await?;
                } else {
                    adopt_running_server(&registry, &server, &cancellation);
                }
            } else {
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

/// Attaches to a container that is already running (for example after the
/// daemon was restarted) without touching it. Console input keeps working
/// because commands are written through `docker exec` into the container's
/// own stdin, which does not depend on the daemon process.
fn adopt_running_server(
    registry: &ServerRegistry,
    server: &ManagedServerRecord,
    cancellation: &CancellationToken,
) {
    let start = if registry.latest_console_id(server.id) == 0 {
        let _ = registry.append_console_message(
            server.id,
            "system",
            "Reattached to the running server after a daemon restart.",
        );
        LogStart::Tail(ADOPTED_LOG_TAIL_LINES)
    } else {
        // The follower died but the console history is intact.
        LogStart::Now
    };

    info!(server_id = server.id, "adopting running container");
    spawn_runtime_log_follower(
        registry.clone(),
        server.clone(),
        cancellation.child_token(),
        start,
    );
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
        .arg("--name")
        .arg(install_container_name(server.id));

    // Note: requires_privileged only applies to the install container.
    // Runtime containers (runtime_container_run_args) intentionally always
    // use Docker defaults without --privileged, since game server processes
    // should never require full host access during normal operation.
    if server.cargo.requires_privileged {
        command.arg("--privileged");
    } else {
        command
            .arg("--security-opt")
            .arg("seccomp=unconfined")
            .arg("--cap-drop=ALL")
            .arg("--cap-add=CHOWN")
            .arg("--cap-add=DAC_OVERRIDE")
            .arg("--cap-add=NET_ADMIN")
            .arg("--cap-add=SYS_PTRACE")
            .arg("--cap-add=SETUID")
            .arg("--cap-add=SETGID");
    }

    command
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

    let install_permit = select! {
        _ = cancellation.cancelled() => return Ok(()),
        permit = BOOT_SLOTS.acquire() => permit.context("boot slots closed")?,
    };
    let status = run_streaming_command(
        command,
        registry,
        server.id,
        "install",
        cancellation,
        Some(&install_log_path),
    )
    .await?;
    // `boot_server` below takes its own slot.
    drop(install_permit);

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
    // The heavy preparation (permissions, quota scan, image pull, container
    // creation) is limited to a few servers at a time. The slot is released
    // before waiting for the startup marker, which is cheap.
    let permit = select! {
        _ = cancellation.cancelled() => return Ok(()),
        permit = BOOT_SLOTS.acquire() => permit.context("boot slots closed")?,
    };

    let container_id = start_runtime_container(registry, server, config, cancellation).await?;
    drop(permit);

    let Some(container_id) = container_id else {
        return Ok(());
    };

    wait_for_startup(registry, server, config, &container_id, cancellation).await
}

/// Prepares the volume and creates the runtime container. Returns its id, or
/// `None` when the operation was cancelled.
async fn start_runtime_container(
    registry: &ServerRegistry,
    server: &mut ManagedServerRecord,
    config: &DaemonConfig,
    cancellation: &CancellationToken,
) -> Result<Option<String>> {
    let volume_path = resolve_volume_path(config, server)?;
    tokio::fs::create_dir_all(&volume_path)
        .await
        .with_context(|| format!("failed to create {}", volume_path.display()))?;
    ensure_server_boot_prerequisites(server, &volume_path)?;
    registry.append_console_message(server.id, "system", "Checking disk space limits...")?;
    {
        let volume_path = volume_path.clone();
        let disk_limit_mib = server.limits.disk_mib;

        tokio::task::spawn_blocking(move || ensure_disk_limit(&volume_path, disk_limit_mib))
            .await
            .context("disk limit check was interrupted")??;
    }

    let image = select_runtime_image(server)?;
    registry.append_console_message(
        server.id,
        "system",
        &format!("Pulling runtime image {image}..."),
    )?;
    let mut pull_command = docker_command();
    pull_command.arg("pull").arg(&image);
    let pull_status = run_streaming_command(
        pull_command,
        registry,
        server.id,
        "system",
        cancellation,
        None,
    )
    .await?;

    if !pull_status.success() {
        // A registry outage must not stop a server whose image is already
        // on this machine.
        if runtime_image_exists_locally(&image).await {
            registry.append_console_message(
                server.id,
                "system",
                "Could not pull the runtime image, using the local copy.",
            )?;
        } else {
            bail!("failed to pull runtime image");
        }
    }

    apply_config_file_actions(registry, server, &volume_path).await?;
    // Last write to the volume before the container starts: the files created
    // above belong to the daemon and must be handed over to the container user.
    fix_volume_permissions(&volume_path).await;
    remove_container(&runtime_container_name(server.id)).await?;

    // Release the port if a stale container from a different server is
    // still holding it. This can happen when a previous server was
    // removed from the panel but its container wasn't cleaned up.
    release_port_if_occupied(server.allocation.port).await;

    registry.append_console_message(
        server.id,
        "system",
        "All systems ready! Starting server...",
    )?;

    let mut command = docker_command();
    command.args(runtime_container_run_args(server, &volume_path, &image));

    let output = command
        .docker_output()
        .await
        .context("failed to start runtime container")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();

        // If the port is still occupied after cleanup, try once more.
        if stderr.contains("port is already allocated") || stderr.contains("address already in use") {
            registry.append_console_message(
                server.id,
                "system",
                "Port conflict detected. Cleaning up stale containers and retrying...",
            )?;
            release_port_if_occupied(server.allocation.port).await;
            time::sleep(Duration::from_secs(2)).await;

            let mut retry_command = docker_command();
            retry_command.args(runtime_container_run_args(server, &volume_path, &image));
            let retry_output = retry_command
                .docker_output()
                .await
                .context("failed to start runtime container on retry")?;

            if retry_output.status.success() {
                let container_id = String::from_utf8_lossy(&retry_output.stdout).trim().to_string();
                connect_interconnect_networks(registry, server, &container_id).await;
                set_runtime_state(registry, config, server.id, "starting", Some(&container_id), None).await?;
                return Ok(Some(container_id));
            }
        }

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

    connect_interconnect_networks(registry, server, &container_id).await;

    set_runtime_state(
        registry,
        config,
        server.id,
        "starting",
        Some(&container_id),
        None,
    )
    .await?;

    Ok(Some(container_id))
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
                LogStart::Now,
            );
            return Ok(());
        }

        select! {
            line = stdout_lines.next_line(), if stdout_open => {
                match tolerate_invalid_utf8(line).context("failed to read runtime stdout")? {
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
                LogStart::Now,
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
                match tolerate_invalid_utf8(line).context("failed to read runtime stderr")? {
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
                LogStart::Now,
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

/// Game servers sometimes print binary garbage. A line that is not valid
/// UTF-8 must not stop the log stream, so it is replaced by a marker.
fn tolerate_invalid_utf8(
    line: std::io::Result<Option<String>>,
) -> std::io::Result<Option<String>> {
    match line {
        Err(error) if error.kind() == std::io::ErrorKind::InvalidData => {
            Ok(Some("[binary output skipped]".to_string()))
        }
        other => other,
    }
}

fn spawn_runtime_log_follower(
    registry: ServerRegistry,
    server: ManagedServerRecord,
    cancellation: CancellationToken,
    start: LogStart,
) {
    // Only one follower per server; a second one would duplicate every line.
    let Some(guard) = claim_follower(server.id) else {
        return;
    };

    tokio::spawn(async move {
        // Releasing the claim when the task ends lets the reconciler
        // re-attach if the follower stopped while the container still runs.
        let _guard = guard;

        if let Err(error) = follow_runtime_logs(registry, server, cancellation, start).await {
            warn!(error = %error, "runtime log follower stopped unexpectedly");
        }
    });
}

async fn follow_runtime_logs(
    registry: ServerRegistry,
    server: ManagedServerRecord,
    cancellation: CancellationToken,
    start: LogStart,
) -> Result<()> {
    let mut command = docker_command();
    command.arg("logs").arg("--follow");

    match start {
        LogStart::Now => {
            let since = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .to_string();

            command.arg("--since").arg(since);
        }
        LogStart::Tail(lines) => {
            command.arg("--tail").arg(lines.to_string());
        }
    }

    command
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
                match tolerate_invalid_utf8(line).context("failed to read runtime stdout after startup")? {
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
                match tolerate_invalid_utf8(line).context("failed to read runtime stderr after startup")? {
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

                apply_properties_replacements(&file_path, &config_replacements(rule, server))?;
            }
            "file" => {
                let file_path = safe_join_relative(volume_path, relative_path)?;

                apply_line_replacements(&file_path, &config_replacements(rule, server))?;
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

fn config_replacements(rule: &Value, server: &ManagedServerRecord) -> BTreeMap<String, String> {
    rule.get("find")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .map(|(key, value)| {
            (
                key.clone(),
                interpolate_config_value(value.as_str().unwrap_or_default(), server),
            )
        })
        .collect()
}

/// Replaces every line starting with one of the keys by its value, keeping the
/// indentation and YAML list marker. Used for YAML, TOML and other formats
/// without a dedicated parser. A missing file is left for the server to create,
/// and keys without a matching line are not appended.
fn apply_line_replacements(path: &Path, replacements: &BTreeMap<String, String>) -> Result<()> {
    let existing = match std::fs::read_to_string(path) {
        Ok(existing) => existing,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", path.display()));
        }
    };

    let lines: Vec<String> = existing
        .lines()
        .map(|line| {
            let mut content = line.trim_start();

            while let Some(item) = content.strip_prefix("- ") {
                content = item.trim_start();
            }

            replacements
                .iter()
                .find(|(key, _)| !key.is_empty() && content.starts_with(key.as_str()))
                .map(|(_, value)| format!("{}{value}", &line[..line.len() - content.len()]))
                .unwrap_or_else(|| line.to_string())
        })
        .collect();

    std::fs::write(path, lines.join("\n") + "\n")
        .with_context(|| format!("failed to write {}", path.display()))?;

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
        // Images do not agree on the uid of their user, so the volume owner is
        // enforced. HOME is set for images that rely on their passwd entry.
        "--user".to_string(),
        CONTAINER_USER.to_string(),
        "-e".to_string(),
        "HOME=/home/container".to_string(),
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

    let response = crate::http::client()
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
        .docker_output()
        .await
        .context("failed to inspect runtime container")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);

        // A missing container simply is not running. Any other failure (for
        // example an unreachable docker daemon) is not an answer. The casing
        // of the message differs between docker versions.
        if is_missing_object_error(&stderr) {
            return Ok(false);
        }

        bail!("docker inspect failed: {}", stderr.trim());
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim() == "true")
}

fn is_missing_object_error(stderr: &str) -> bool {
    let stderr = stderr.to_ascii_lowercase();

    stderr.contains("no such object") || stderr.contains("no such container")
}

async fn runtime_container_id(server_id: u64) -> Result<Option<String>> {
    let output = docker_command()
        .arg("inspect")
        .arg("-f")
        .arg("{{.Id}}")
        .arg(runtime_container_name(server_id))
        .docker_output()
        .await
        .context("failed to inspect runtime container")?;

    if !output.status.success() {
        return Ok(None);
    }

    Ok(Some(String::from_utf8_lossy(&output.stdout).trim().to_string()))
}

async fn runtime_image_exists_locally(image: &str) -> bool {
    docker_command()
        .args(["image", "inspect", "--format", "{{.Id}}", image])
        .docker_output()
        .await
        .is_ok_and(|output| output.status.success())
}

async fn remove_container(name: &str) -> Result<()> {
    let _ = docker_command()
        .arg("rm")
        .arg("-f")
        .arg(name)
        .docker_output()
        .await;

    Ok(())
}

/// Find and force-remove any Docker container that has bound the given
/// port. This handles stale containers from deleted servers or crashed
/// daemons that are still holding a port.
async fn release_port_if_occupied(port: u64) {
    // List all containers binding this port (format: container ID).
    let output = docker_command()
        .args(["ps", "-aq", "--filter", &format!("publish={port}")])
        .docker_output()
        .await;

    let Ok(output) = output else {
        return;
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    for container_id in stdout.lines().filter(|line| !line.trim().is_empty()) {
        let _ = docker_command()
            .args(["rm", "-f", container_id.trim()])
            .docker_output()
            .await;
    }
}

async fn fix_volume_permissions(volume_path: &Path) {
    // On Linux, ensure the volume is owned by the user the runtime container
    // runs as, with permissions that let the process read and write files.
    // On macOS this is a no-op since Docker Desktop handles file sharing
    // differently.
    if !cfg!(target_os = "linux") {
        return;
    }

    let path = volume_path.to_string_lossy().to_string();

    let _ = tokio::process::Command::new("chown")
        .args(["-R", CONTAINER_USER, &path])
        .output()
        .await;

    // Ensure the directory itself is accessible
    let _ = tokio::process::Command::new("chmod")
        .args(["755", &path])
        .output()
        .await;
}

fn slugify_server_name(name: &str) -> String {
    let slug: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '-' })
        .collect();

    let trimmed = slug.trim_matches('-').to_string();

    if trimmed.is_empty() {
        "server".to_string()
    } else {
        trimmed
    }
}

fn interconnect_network_name(interconnect_id: u64) -> String {
    format!("skyport-ic-{interconnect_id}")
}

async fn connect_interconnect_networks(
    registry: &ServerRegistry,
    server: &ManagedServerRecord,
    container_id: &str,
) {
    if server.interconnects.is_empty() {
        return;
    }

    let container_name = runtime_container_name(server.id);

    for ic in &server.interconnects {
        let network_name = interconnect_network_name(ic.id);

        // Create the network if it doesn't exist (ignore errors if it already exists).
        let _ = docker_command()
            .args(["network", "create", "--driver", "bridge", &network_name])
            .docker_output()
            .await;

        // Connect the container to the network with a human-friendly alias.
        let alias = slugify_server_name(&server.name);
        let result = docker_command()
            .args(["network", "connect", "--alias", &alias, &network_name, &container_name])
            .docker_output()
            .await;

        match result {
            Ok(output) if output.status.success() => {
                let _ = registry.append_console_message(
                    server.id,
                    "system",
                    &format!(
                        "Connected to private network '{}' (reachable as '{}')",
                        ic.name, alias
                    ),
                );
            }
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                if !stderr.contains("already exists") {
                    warn!(
                        server_id = server.id,
                        network = %ic.name,
                        container = %container_id,
                        error = %stderr,
                        "failed to connect container to interconnect network"
                    );
                }
            }
            Err(error) => {
                warn!(
                    server_id = server.id,
                    network = %ic.name,
                    error = %error,
                    "failed to run docker network connect"
                );
            }
        }
    }
}

fn runtime_container_name(server_id: u64) -> String {
    format!("skyport-server-{server_id}")
}

fn install_container_name(server_id: u64) -> String {
    format!("skyport-install-{server_id}")
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
    #[test]
    fn follower_claim_is_exclusive_and_released_on_drop() {
        let id = u64::MAX - 1;

        let first = super::claim_follower(id).expect("first claim");
        assert!(super::is_followed(id));
        assert!(super::claim_follower(id).is_none());

        drop(first);
        assert!(!super::is_followed(id));
        assert!(super::claim_follower(id).is_some());
    }

    #[test]
    fn invalid_utf8_lines_do_not_stop_the_stream() {
        let invalid = Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bad"));
        let other = Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe"));

        assert_eq!(
            super::tolerate_invalid_utf8(invalid).unwrap().as_deref(),
            Some("[binary output skipped]")
        );
        assert!(super::tolerate_invalid_utf8(other).is_err());
        assert_eq!(
            super::tolerate_invalid_utf8(Ok(Some("ok".into()))).unwrap().as_deref(),
            Some("ok")
        );
    }

    #[test]
    fn timeouts_and_unreachable_docker_are_transient() {
        let timeout = anyhow::Error::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "slow"))
            .context("failed to inspect runtime container");
        let unreachable = anyhow::anyhow!("docker inspect failed: Cannot connect to the Docker daemon");
        let real = anyhow::anyhow!("failed to pull runtime image");

        assert!(super::is_transient_docker_error(&timeout));
        assert!(super::is_transient_docker_error(&unreachable));
        assert!(!super::is_transient_docker_error(&real));
    }

    #[test]
    fn missing_container_errors_are_recognised_whatever_the_casing() {
        assert!(super::is_missing_object_error("Error: No such object: skyport-server-1"));
        assert!(super::is_missing_object_error("error: no such object: skyport-server-1"));
        assert!(super::is_missing_object_error("Error response from daemon: No such container: x"));
        assert!(!super::is_missing_object_error(
            "Cannot connect to the Docker daemon at unix:///var/run/docker.sock"
        ));
    }

    #[test]
    fn offline_statuses_are_not_reconciled() {
        assert!(super::should_reconcile_status("running"));
        assert!(super::should_reconcile_status("starting"));
        assert!(!super::should_reconcile_status("offline"));
        assert!(!super::should_reconcile_status("install_failed"));
    }
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
        let system_source = ["\"system\"", ",\n        cancellation,"].concat();

        assert!(source.contains("All systems ready! Starting server..."));
        assert!(!source.contains(&removed_start_message));
        assert!(!source.contains(&removed_config_action_message));
        assert!(source.contains("Pulling runtime image {image}..."));
        assert!(source.contains("run_streaming_command("));
        assert!(source.contains(&system_source));
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

    #[tokio::test]
    async fn file_parser_rewrites_matching_lines_and_keeps_their_prefix() {
        let tempdir = tempdir().unwrap();
        let registry = ServerRegistry::new(tempdir.path().join("skyportd.db"));
        registry.initialize().unwrap();
        let path = tempdir.path().join("config.yml");
        std::fs::write(
            &path,
            "# host: comment\nlisteners:\n- query_port: 25577\n  motd: hi\n  host: 0.0.0.0:25577\n",
        )
        .unwrap();

        let mut server = sample_server();
        server.cargo.config_files = r#"{"config.yml":{"parser":"file","find":{"host:":"host: 0.0.0.0:{{server.build.default.port}}","query_port:":"query_port: {{server.build.default.port}}"}}}"#.to_string();
        server.allocation.port = 25600;

        apply_config_file_actions(&registry, &server, tempdir.path())
            .await
            .unwrap();

        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "# host: comment\nlisteners:\n- query_port: 25600\n  motd: hi\n  host: 0.0.0.0:25600\n"
        );
    }

    #[tokio::test]
    async fn file_parser_leaves_a_missing_file_to_the_server() {
        let tempdir = tempdir().unwrap();
        let registry = ServerRegistry::new(tempdir.path().join("skyportd.db"));
        registry.initialize().unwrap();

        let mut server = sample_server();
        server.cargo.config_files =
            r#"{"config.yml":{"parser":"file","find":{"host:":"host: x"}}}"#.to_string();

        apply_config_file_actions(&registry, &server, tempdir.path())
            .await
            .unwrap();

        assert!(!tempdir.path().join("config.yml").exists());
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
    fn runtime_container_runs_as_the_volume_owner() {
        let args = runtime_container_run_args(
            &sample_server(),
            Path::new("/tmp/server"),
            "ghcr.io/example/runtime:latest",
        );
        let image = args
            .iter()
            .position(|arg| arg == "ghcr.io/example/runtime:latest")
            .expect("image argument");
        let user = args
            .iter()
            .position(|arg| arg == "--user")
            .expect("user flag");

        // Docker options placed after the image would be passed to the container.
        assert!(user < image);
        assert_eq!(args[user + 1], super::CONTAINER_USER);
        assert!(args.windows(2).any(|pair| pair == ["-e", "HOME=/home/container"]));
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
            firewall_rules: vec![],
            interconnects: vec![],
            workflows: vec![],
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
                requires_privileged: false,
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
                allow_private_webhooks: false,
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

    async fn docker(args: &[&str]) -> std::process::Output {
        tokio::process::Command::new("docker")
            .args(args)
            .output()
            .await
            .expect("docker must be installed for this test")
    }

    async fn wait_for_console(registry: &ServerRegistry, server_id: u64, needle: &str) -> bool {
        for _ in 0..100 {
            let found = registry
                .recent_console_messages(server_id, 1000)
                .unwrap()
                .iter()
                .any(|message| message.message.contains(needle));

            if found {
                return true;
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        false
    }

    /// Needs a running docker daemon: `cargo test -- --ignored`.
    #[tokio::test]
    #[ignore = "requires docker"]
    async fn adopting_a_running_container_keeps_it_alive_and_wires_the_console() {
        let server_id = 987_001;
        let name = super::runtime_container_name(server_id);
        let mut server = sample_server();
        server.id = server_id;
        server.status = "running".to_string();

        docker(&["rm", "-f", &name]).await;
        let started = docker(&[
            "run", "-d", "-i", "--name", &name, "alpine:3.20", "sh", "-c",
            "echo booted; while read l; do echo \"got:$l\"; done",
        ])
        .await;
        assert!(started.status.success(), "{}", String::from_utf8_lossy(&started.stderr));
        let container_id = String::from_utf8_lossy(&started.stdout).trim().to_string();

        let directory = tempfile::tempdir().unwrap();
        let registry = ServerRegistry::new(directory.path().join("registry.db"));
        registry.initialize().unwrap();

        // Discovery uses one `docker ps` for the whole node.
        assert!(
            crate::docker::running_runtime_containers()
                .await
                .unwrap()
                .contains(&name)
        );
        assert!(super::runtime_container_is_running(&server).await.unwrap());
        assert!(!super::is_followed(server_id));

        let cancellation = CancellationToken::new();
        super::adopt_running_server(&registry, &server, &cancellation);

        assert!(wait_for_console(&registry, server_id, "booted").await, "history was not replayed");
        assert!(super::is_followed(server_id));

        // Adopting twice must not create a second follower (duplicate lines).
        super::adopt_running_server(&registry, &server, &cancellation);

        // Console input reaches the container without it being recreated.
        crate::api::send_command_to_server(&registry, &server, "ping")
            .await
            .unwrap();
        assert!(wait_for_console(&registry, server_id, "got:ping").await);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let pings = registry
            .recent_console_messages(server_id, 1000)
            .unwrap()
            .iter()
            .filter(|message| message.message == "got:ping")
            .count();
        assert_eq!(pings, 1, "console line was duplicated");

        // The container was not restarted by adoption.
        let inspect = docker(&["inspect", "-f", "{{.Id}}", &name]).await;
        assert_eq!(String::from_utf8_lossy(&inspect.stdout).trim(), container_id);

        // The follower releases its claim once the container is gone.
        docker(&["rm", "-f", &name]).await;
        let mut released = false;
        for _ in 0..100 {
            if !super::is_followed(server_id) {
                released = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(released, "follower did not stop after the container exited");
        assert!(!super::runtime_container_is_running(&server).await.unwrap());
    }

    /// End to end: a container that survived a daemon restart must be adopted
    /// by the real reconciliation loop without being recreated.
    #[tokio::test]
    #[ignore = "requires docker"]
    async fn daemon_restart_does_not_restart_running_servers() {
        use crate::config::{DaemonSection, LogFormat, LoggingSection, PanelSection, RuntimeSection};

        let server_id = 987_002;
        let name = super::runtime_container_name(server_id);
        let mut server = sample_server();
        server.id = server_id;
        server.node_id = 5;
        server.status = "running".to_string();

        docker(&["rm", "-f", &name]).await;
        let started = docker(&[
            "run", "-d", "-i", "--name", &name, "alpine:3.20", "sh", "-c",
            "echo survivor; while read l; do echo \"got:$l\"; done",
        ])
        .await;
        assert!(started.status.success());
        let container_id = String::from_utf8_lossy(&started.stdout).trim().to_string();

        let directory = tempfile::tempdir().unwrap();
        let registry = ServerRegistry::new(directory.path().join("registry.db"));
        registry.initialize().unwrap();
        registry.upsert_server(&server).unwrap();
        registry
            .set_server_runtime(server_id, "running", Some(&container_id), None)
            .unwrap();

        let config = DaemonConfig {
            daemon: DaemonSection {
                name: "skyportd".to_string(),
                uuid: "00000000-0000-0000-0000-000000000000".to_string(),
                tick_interval: Duration::from_secs(5),
                shutdown_timeout: Duration::from_secs(30),
                allow_private_webhooks: false,
            },
            panel: PanelSection {
                url: "http://127.0.0.1:1".to_string(),
                configuration_token: None,
                daemon_secret: None,
                daemon_callback_token: None,
                node_id: Some(5),
            },
            logging: LoggingSection {
                level: "info".to_string(),
                format: LogFormat::Pretty,
            },
            runtime: RuntimeSection { worker_threads: 0 },
            node: None,
        };
        let (_config_tx, config_rx) = watch::channel(config);
        let cancellation = CancellationToken::new();
        let service = ServerLifecycleService::new(config_rx, registry.clone(), cancellation.clone());
        let handle = tokio::spawn(service.run());

        assert!(wait_for_console(&registry, server_id, "survivor").await);
        // Let several reconciliation ticks pass.
        tokio::time::sleep(Duration::from_secs(8)).await;

        let inspect = docker(&["inspect", "-f", "{{.Id}} {{.State.Running}}", &name]).await;
        assert_eq!(
            String::from_utf8_lossy(&inspect.stdout).trim(),
            format!("{container_id} true"),
            "the running container was recreated or stopped"
        );
        assert_eq!(registry.get_server_status(server_id).unwrap().as_deref(), Some("running"));
        assert!(super::is_followed(server_id));

        let survivors = registry
            .recent_console_messages(server_id, 1000)
            .unwrap()
            .iter()
            .filter(|message| message.message == "survivor")
            .count();
        assert_eq!(survivors, 1, "logs were replayed more than once");

        cancellation.cancel();
        let _ = handle.await;
        docker(&["rm", "-f", &name]).await;
    }

    /// End to end: install-less servers go from `pending` to `running`
    /// through the real boot path, and a crashed container is brought back.
    #[tokio::test]
    #[ignore = "requires docker"]
    async fn pending_servers_boot_and_crashed_containers_are_restarted() {
        use crate::config::{DaemonSection, LogFormat, LoggingSection, PanelSection, RuntimeSection};
        use std::io::Write as _;

        // A local-only image: the pull fails and the local copy is used.
        let mut build = std::process::Command::new("docker")
            .args(["build", "-q", "-t", "skyport-test-boot:local", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        build.stdin.take().unwrap().write_all(
            b"FROM alpine:3.20\nCMD [\"sh\",\"-c\",\"echo ready-marker; while read l; do echo got:$l; done\"]\n",
        ).unwrap();
        assert!(build.wait().unwrap().success());

        let ids = [987_003_u64, 987_004_u64];
        let directory = tempfile::tempdir().unwrap();
        let registry = ServerRegistry::new(directory.path().join("registry.db"));
        registry.initialize().unwrap();

        for (index, id) in ids.iter().enumerate() {
            docker(&["rm", "-f", &super::runtime_container_name(*id)]).await;

            let mut server = sample_server();
            server.id = *id;
            server.node_id = 5;
            server.status = "pending".to_string();
            server.allocation.bind_ip = "127.0.0.1".to_string();
            server.allocation.port = 45_987 + index as u64;
            server.limits.memory_mib = 128;
            server.limits.disk_mib = 100;
            server.cargo.install_script = None;
            server.cargo.install_container = None;
            server.cargo.config_startup = "ready-marker".to_string();
            server.cargo.docker_images =
                BTreeMap::from([("test".to_string(), "skyport-test-boot:local".to_string())]);
            registry.upsert_server(&server).unwrap();
        }

        let config = DaemonConfig {
            daemon: DaemonSection {
                name: "skyportd".to_string(),
                uuid: "00000000-0000-0000-0000-000000000000".to_string(),
                tick_interval: Duration::from_secs(5),
                shutdown_timeout: Duration::from_secs(30),
                allow_private_webhooks: false,
            },
            panel: PanelSection {
                url: "http://127.0.0.1:1".to_string(),
                configuration_token: None,
                daemon_secret: None,
                daemon_callback_token: None,
                node_id: Some(5),
            },
            logging: LoggingSection {
                level: "info".to_string(),
                format: LogFormat::Pretty,
            },
            runtime: RuntimeSection { worker_threads: 0 },
            node: None,
        };
        let (_config_tx, config_rx) = watch::channel(config);
        let cancellation = CancellationToken::new();
        let service = ServerLifecycleService::new(config_rx, registry.clone(), cancellation.clone());
        let handle = tokio::spawn(service.run());

        async fn wait_running(registry: &ServerRegistry, id: u64) -> bool {
            for _ in 0..300 {
                if registry.get_server_status(id).unwrap().as_deref() == Some("running") {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            false
        }

        for id in ids {
            assert!(wait_running(&registry, id).await, "server {id} never became running");
            assert!(wait_for_console(&registry, id, "ready-marker").await);
            assert!(wait_for_console(&registry, id, "using the local copy").await);
        }

        // Crash one container: the reconciler must boot it again.
        let victim = super::runtime_container_name(ids[0]);
        let before = docker(&["inspect", "-f", "{{.Id}}", &victim]).await;
        docker(&["kill", &victim]).await;

        let mut restarted = false;
        for _ in 0..150 {
            let after = docker(&["inspect", "-f", "{{.Id}} {{.State.Running}}", &victim]).await;
            let after = String::from_utf8_lossy(&after.stdout).trim().to_string();
            let before = String::from_utf8_lossy(&before.stdout).trim().to_string();

            if after.ends_with("true") && !after.starts_with(&before) {
                restarted = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert!(restarted, "crashed container was not restarted");
        assert!(wait_running(&registry, ids[0]).await);

        cancellation.cancel();
        let _ = handle.await;
        for id in ids {
            docker(&["rm", "-f", &super::runtime_container_name(id)]).await;
            let _ = std::fs::remove_dir_all(crate::config::managed_server_volume_path(id).unwrap());
        }
    }
}
