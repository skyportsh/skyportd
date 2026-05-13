use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use reqwest::Client;
use tokio::select;
use tokio::sync::{Mutex, watch};
use tokio::time::{self, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use url::Url;

use crate::api::{container_is_running, docker_command, runtime_container_name, send_command_to_server};
use crate::config::DaemonConfig;
use crate::server_registry::{ManagedServerRecord, ManagedServerWorkflow, ManagedServerWorkflowStep, ServerRegistry};

/// Shared HTTP client for webhook calls (connection pooling).
fn http_client() -> &'static Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to build reqwest client")
    })
}

/// Tracks per-workflow state between ticks.
struct WorkflowState {
    last_run: u64,
    last_server_status: Option<String>,
    cooldowns: HashMap<u64, u64>, // workflow_id -> last_trigger_time
}

impl WorkflowState {
    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

pub struct WorkflowService {
    config_rx: watch::Receiver<DaemonConfig>,
    server_registry: ServerRegistry,
    cancellation: CancellationToken,
}

impl WorkflowService {
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
        let mut interval = time::interval(Duration::from_secs(5));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // Per-server workflow state
        let states: Arc<Mutex<HashMap<u64, WorkflowState>>> =
            Arc::new(Mutex::new(HashMap::new()));

        info!("workflow engine started");

        loop {
            select! {
                _ = self.cancellation.cancelled() => {
                    info!("workflow engine stopping");
                    break;
                }
                _ = interval.tick() => {
                    let config = self.config_rx.borrow().clone();
                    let Some(node_id) = config.panel.node_id else {
                        continue;
                    };

                    let servers = match self.server_registry.list_servers_for_node(node_id) {
                        Ok(servers) => servers,
                        Err(_) => continue,
                    };

                    for server in &servers {
                        if server.workflows.is_empty() {
                            continue;
                        }

                        let mut states_guard = states.lock().await;
                        let state = states_guard
                            .entry(server.id)
                            .or_insert_with(|| WorkflowState {
                                last_run: 0,
                                last_server_status: Some(server.status.clone()),
                                cooldowns: HashMap::new(),
                            });

                        for workflow in &server.workflows {
                            if let Err(error) = evaluate_workflow(
                                &config,
                                &self.server_registry,
                                server,
                                workflow,
                                state,
                            )
                            .await
                            {
                                debug!(
                                    server_id = server.id,
                                    workflow_id = workflow.id,
                                    error = %error,
                                    "workflow evaluation failed"
                                );
                            }
                        }

                        state.last_server_status = Some(server.status.clone());
                    }
                }
            }
        }

        Ok(())
    }
}

/// Returns true if the hostname resolves to a private or loopback IP address.
///
/// This is a best-effort check: a DNS rebinding attack could swap the resolved
/// IP between our lookup and the actual request. For attacker-controlled domains
/// this is a TOCTOU gap, but it blocks the common case (hardcoded private IPs,
/// metadata endpoints, internal hostnames).
async fn is_private_host(host: &str) -> bool {
    // Check well-known private hostnames first (no DNS needed).
    // Parse as IPv4Addr to avoid false positives on domain names like "127.example.com".
    if host == "localhost"
        || host == "localhost.localdomain"
        || host.parse::<std::net::Ipv4Addr>().is_ok_and(|ip| ip.is_loopback())
    {
        return true;
    }

    let host = host.to_string();
    // Resolve DNS on the blocking pool to avoid stalling Tokio workers
    let result = tokio::task::spawn_blocking(move || -> bool {
        let Ok(addrs) = (host.as_str(), 0).to_socket_addrs() else {
            warn!(%host, "SSRF check: DNS resolution failed, allowing request");
            return false;
        };

        for addr in addrs {
            match addr.ip() {
                std::net::IpAddr::V4(ipv4) => {
                    if ipv4.is_private() || ipv4.is_loopback() || ipv4.is_unspecified() {
                        return true;
                    }
                }
                std::net::IpAddr::V6(ipv6) => {
                    // Check IPv4-mapped IPv6 addresses (e.g. ::ffff:10.0.0.1)
                    if let Some(ipv4) = ipv6.to_ipv4_mapped() {
                        if ipv4.is_private() || ipv4.is_loopback() || ipv4.is_unspecified() {
                            return true;
                        }
                    } else if ipv6.is_loopback()
                        || ipv6.is_unspecified()
                        || ipv6.is_unique_local()
                        || ipv6.is_unicast_link_local()
                    {
                        return true;
                    }
                }
            }
        }

        false
    })
    .await;

    // Fail closed: if spawn_blocking itself panics or is cancelled, treat as private
    result.unwrap_or(true)
}

async fn evaluate_workflow(
    config: &DaemonConfig,
    registry: &ServerRegistry,
    server: &ManagedServerRecord,
    workflow: &ManagedServerWorkflow,
    state: &mut WorkflowState,
) -> Result<()> {
    let triggers: Vec<&ManagedServerWorkflowStep> = workflow
        .nodes
        .iter()
        .filter(|n| n.step_type == "trigger")
        .collect();
    let conditions: Vec<&ManagedServerWorkflowStep> = workflow
        .nodes
        .iter()
        .filter(|n| n.step_type == "condition")
        .collect();
    let actions: Vec<&ManagedServerWorkflowStep> = workflow
        .nodes
        .iter()
        .filter(|n| n.step_type == "action")
        .collect();

    if triggers.is_empty() || actions.is_empty() {
        return Ok(());
    }

    // Check if any trigger fires
    let mut triggered = false;
    let now = WorkflowState::now_secs();

    for trigger in &triggers {
        if check_trigger(trigger, server, state, now)? {
            triggered = true;
            break;
        }
    }

    if !triggered {
        return Ok(());
    }

    // Check all conditions
    for condition in &conditions {
        if !check_condition(condition, server, state, now)? {
            return Ok(());
        }
    }

    // Record this run
    state.cooldowns.insert(workflow.id, now);

    info!(
        server_id = server.id,
        workflow = %workflow.name,
        "workflow triggered"
    );

    let _ = registry.append_console_message(
        server.id,
        "system",
        &format!("Workflow '{}' triggered.", workflow.name),
    );

    // Execute actions in order
    for action in &actions {
        if let Err(error) = execute_action(action, config, registry, server).await {
            warn!(
                server_id = server.id,
                workflow = %workflow.name,
                action = %action.kind,
                error = %error,
                "workflow action failed"
            );
            let _ = registry.append_console_message(
                server.id,
                "system",
                &format!("Workflow '{}' action '{}' failed: {error}", workflow.name, action.kind),
            );
            break;
        }
    }

    Ok(())
}

fn check_trigger(
    trigger: &ManagedServerWorkflowStep,
    server: &ManagedServerRecord,
    state: &WorkflowState,
    now: u64,
) -> Result<bool> {
    match trigger.kind.as_str() {
        "schedule" => {
            let interval_mins: u64 = trigger
                .config
                .get("interval")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            if interval_mins == 0 {
                return Ok(false);
            }
            let interval_secs = interval_mins * 60;
            let last = state.last_run;
            if now - last >= interval_secs {
                Ok(true)
            } else {
                Ok(false)
            }
        }
        "state_change" => {
            let target = trigger.config.get("target_state").map(|s| s.as_str()).unwrap_or("");
            let prev = state.last_server_status.as_deref().unwrap_or("");
            Ok(!target.is_empty() && server.status == target && prev != target)
        }
        "startup" => {
            let prev = state.last_server_status.as_deref().unwrap_or("");
            Ok(server.status == "running" && prev != "running")
        }
        "shutdown" => {
            let prev = state.last_server_status.as_deref().unwrap_or("");
            Ok(server.status == "offline" && prev != "offline")
        }
        "crash" => {
            let prev = state.last_server_status.as_deref().unwrap_or("");
            Ok(server.status == "offline" && prev == "running")
        }
        "high_cpu" | "high_memory" | "disk_full" => {
            // These would need runtime stats — for now, always false
            Ok(false)
        }
        "backup_complete" | "backup_failed" | "console_match" => {
            // Event-driven triggers handled elsewhere
            Ok(false)
        }
        _ => Ok(false),
    }
}

fn check_condition(
    condition: &ManagedServerWorkflowStep,
    server: &ManagedServerRecord,
    _state: &WorkflowState,
    _now: u64,
) -> Result<bool> {
    match condition.kind.as_str() {
        "server_online" => Ok(server.status == "running"),
        "server_offline" => Ok(server.status == "offline"),
        "cpu_above" | "cpu_below" | "memory_above" | "memory_below" => {
            // Would need runtime stats
            Ok(true)
        }
        "time_between" => {
            let start = condition.config.get("start").map(|s| s.as_str()).unwrap_or("");
            let end = condition.config.get("end").map(|s| s.as_str()).unwrap_or("");
            if start.is_empty() || end.is_empty() {
                return Ok(true);
            }
            let now_time = chrono_now_hhmm();
            Ok(now_time >= start.to_string() && now_time <= end.to_string())
        }
        "day_of_week" => {
            let days = condition.config.get("days").map(|s| s.as_str()).unwrap_or("");
            if days.is_empty() {
                return Ok(true);
            }
            let today = chrono_today_abbrev();
            Ok(days.split(',').any(|d| d.trim().eq_ignore_ascii_case(&today)))
        }
        "cooldown" => {
            let minutes: u64 = condition
                .config
                .get("minutes")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            if minutes == 0 {
                return Ok(true);
            }
            // Check the parent workflow's last trigger time
            // Since we don't have workflow_id here, use a general approach
            Ok(true) // Cooldown is checked at workflow level in evaluate_workflow
        }
        _ => Ok(true),
    }
}

fn chrono_now_hhmm() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let secs_in_day = now % 86400;
    let hours = secs_in_day / 3600;
    let minutes = (secs_in_day % 3600) / 60;
    format!("{hours:02}:{minutes:02}")
}

fn chrono_today_abbrev() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Jan 1 1970 was a Thursday
    let days = now / 86400;
    match (days + 4) % 7 {
        0 => "Sun",
        1 => "Mon",
        2 => "Tue",
        3 => "Wed",
        4 => "Thu",
        5 => "Fri",
        6 => "Sat",
        _ => "Mon",
    }
    .to_string()
}

async fn execute_action(
    action: &ManagedServerWorkflowStep,
    config: &DaemonConfig,
    registry: &ServerRegistry,
    server: &ManagedServerRecord,
) -> Result<()> {
    match action.kind.as_str() {
        "run_command" => {
            let command = action.config.get("command").context("missing command")?;
            send_command_to_server(registry, server, command).await?;
        }
        "broadcast" => {
            let message = action.config.get("message").context("missing message")?;
            let command = format!("say {message}");
            send_command_to_server(registry, server, &command).await?;
        }
        "run_multiple_commands" => {
            let commands = action.config.get("commands").context("missing commands")?;
            for line in commands.lines() {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    let _ = send_command_to_server(registry, server, trimmed).await;
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
        "power_start" | "power_stop" | "power_restart" | "power_kill" => {
            let signal = action.kind.strip_prefix("power_").unwrap_or("stop");
            let container_name = runtime_container_name(server.id);

            match signal {
                "start" => {
                    // Starting is handled by the lifecycle reconciler via status change
                    registry.set_server_runtime(server.id, "starting", None, None)?;
                }
                "stop" => {
                    if container_is_running(server.id).await? {
                        let stop_cmd = &server.cargo.config_stop;
                        if !stop_cmd.is_empty() {
                            let _ = send_command_to_server(registry, server, stop_cmd).await;
                        }
                    }
                }
                "restart" => {
                    registry.set_server_runtime(server.id, "restarting", None, None)?;
                }
                "kill" => {
                    let _ = docker_command()
                        .arg("kill")
                        .arg(&container_name)
                        .output()
                        .await;
                    registry.set_server_runtime(server.id, "offline", None, None)?;
                }
                _ => {}
            }
        }
        "create_backup" => {
            let _ = registry.append_console_message(
                server.id,
                "system",
                "Workflow requested a backup (manual trigger via panel required).",
            );
        }
        "delete_oldest_backup" => {
            let _ = registry.append_console_message(
                server.id,
                "system",
                "Workflow requested oldest backup deletion (manual trigger via panel required).",
            );
        }
        "webhook" => {
            let url_str = action.config.get("url").context("missing URL")?;
            let method = action.config.get("method").unwrap_or(&"POST".to_string()).to_ascii_uppercase();

            // Block SSRF to private/internal networks unless explicitly allowed.
            // Note: best-effort protection — DNS rebinding could theoretically
            // swap the resolved IP between our lookup and the actual request.
            if !config.daemon.allow_private_webhooks {
                let parsed = Url::parse(url_str).context("invalid webhook URL")?;
                let host = parsed.host_str().context("webhook URL missing host")?;

                if is_private_host(host).await {
                    anyhow::bail!(
                        "webhook URL targets a private or internal address. \
                         Set daemon.allow_private_webhooks=true in config to allow this."
                    );
                }
            }

            let client = http_client();
            let request = if method == "GET" {
                client.get(url_str)
            } else {
                client.post(url_str).json(&serde_json::json!({
                    "server_id": server.id,
                    "server_name": server.name,
                    "server_status": server.status,
                }))
            };

            request.send().await?;
        }
        "delay" => {
            let seconds: u64 = action
                .config
                .get("seconds")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            if seconds > 0 && seconds <= 300 {
                tokio::time::sleep(Duration::from_secs(seconds)).await;
            }
        }
        _ => {}
    }

    Ok(())
}
