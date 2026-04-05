use std::io::{self, IsTerminal, Write};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::config::DaemonConfig;

const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");
const ORANGE: &str = "\x1b[38;2;240;90;36m";
const ORANGE_DARK: &str = "\x1b[38;2;217;36;0m";
const GRAY: &str = "\x1b[38;2;120;120;120m";
const GRAY_DARK: &str = "\x1b[38;2;38;38;38m";
const RESET: &str = "\x1b[0m";

#[derive(Debug, Serialize)]
struct ConfigurationRequest<'a> {
    token: &'a str,
    uuid: &'a str,
    version: &'static str,
    hostname: String,
    reported_ip: Option<String>,
    os: &'static str,
    arch: &'static str,
    capabilities: ConfigurationCapabilities,
    docker: DockerMetadata,
}

#[derive(Debug, Serialize)]
struct ConfigurationCapabilities {
    docker: bool,
    metrics: bool,
    sftp: bool,
}

#[derive(Debug, Serialize)]
struct DockerMetadata {
    version: &'static str,
}

#[derive(Debug, Deserialize)]
struct ConfigurationResponse {
    daemon_secret: String,
    daemon_uuid: String,
    heartbeat_interval_seconds: u64,
    node_id: u64,
    panel_time: String,
    panel_version: String,
    task_poll_interval_seconds: u64,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    message: String,
}

enum EnrollmentFailure {
    Retryable(String),
    Fatal(anyhow::Error),
}

pub async fn ensure_configured(mut config: DaemonConfig) -> Result<DaemonConfig> {
    if config
        .panel
        .daemon_secret
        .as_deref()
        .is_some_and(|secret| !secret.is_empty())
    {
        return Ok(config);
    }

    let interactive = terminal_attached();
    let mut should_prompt = !has_bootstrap_values(&config);

    loop {
        if should_prompt {
            prompt_for_bootstrap_config(&mut config)?;
        }

        match enroll_with_panel(&config).await {
            Ok(payload) => {
                ensure_compatible_panel_version(&payload.panel_version)?;

                config.persist_configuration(
                    payload.node_id,
                    &payload.daemon_secret,
                    &payload.daemon_uuid,
                )?;
                config.panel.daemon_secret = Some(payload.daemon_secret);
                config.panel.node_id = Some(payload.node_id);
                config.panel.configuration_token = None;
                config.daemon.uuid = payload.daemon_uuid;

                info!(
                    node_id = payload.node_id,
                    heartbeat_interval_seconds = payload.heartbeat_interval_seconds,
                    task_poll_interval_seconds = payload.task_poll_interval_seconds,
                    panel_time = %payload.panel_time,
                    panel_version = %payload.panel_version,
                    "daemon configuration completed"
                );

                return Ok(config);
            }
            Err(EnrollmentFailure::Retryable(message)) => {
                if !interactive {
                    bail!(message);
                }

                render_setup_error(&message);
                should_prompt = true;
            }
            Err(EnrollmentFailure::Fatal(error)) => {
                return Err(error);
            }
        }
    }
}

async fn enroll_with_panel(
    config: &DaemonConfig,
) -> std::result::Result<ConfigurationResponse, EnrollmentFailure> {
    let panel_url = normalize_panel_url(&config.panel.url);

    if !is_valid_panel_url(&panel_url) {
        return Err(EnrollmentFailure::Retryable(
            "The panel URL is invalid. Enter a full http:// or https:// URL.".to_string(),
        ));
    }

    let configuration_token = match config
        .panel
        .configuration_token
        .as_deref()
        .filter(|token| !token.trim().is_empty())
    {
        Some(token) => token,
        None => {
            return Err(EnrollmentFailure::Retryable(
                "A configuration token is required before the daemon can enroll.".to_string(),
            ));
        }
    };

    let request = ConfigurationRequest {
        token: configuration_token,
        uuid: &config.daemon.uuid,
        version: CURRENT_VERSION,
        hostname: hostname::get()
            .context("failed to resolve hostname")
            .map_err(EnrollmentFailure::Fatal)?
            .to_string_lossy()
            .into_owned(),
        reported_ip: None,
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        capabilities: ConfigurationCapabilities {
            docker: true,
            metrics: true,
            sftp: true,
        },
        docker: DockerMetadata { version: "unknown" },
    };

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{panel_url}/api/daemon/enroll"))
        .json(&request)
        .send()
        .await
        .map_err(|error| {
            EnrollmentFailure::Retryable(format!(
                "Could not reach the panel at {panel_url}: {error}"
            ))
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let message = response
            .json::<ErrorResponse>()
            .await
            .map(|payload| payload.message)
            .unwrap_or_else(|_| {
                format!(
                    "Panel configuration failed with status {status}. Check the panel URL and configuration token, then try again."
                )
            });

        if is_compatibility_error_message(&message) {
            return Err(EnrollmentFailure::Fatal(anyhow!(message)));
        }

        return Err(EnrollmentFailure::Retryable(message));
    }

    let payload = response.json::<ConfigurationResponse>().await.map_err(|_| {
        EnrollmentFailure::Retryable(format!(
            "The panel at {panel_url} returned an invalid configuration response. Double-check the panel URL and try again."
        ))
    })?;

    Ok(payload)
}

fn ensure_compatible_panel_version(panel_version: &str) -> Result<()> {
    if panel_version == CURRENT_VERSION {
        return Ok(());
    }

    bail!(incompatible_panel_message(panel_version));
}

fn incompatible_panel_message(panel_version: &str) -> String {
    format!("This version of skyportd isn't compatible with Skyport panel {panel_version}.")
}

fn is_compatibility_error_message(message: &str) -> bool {
    message.contains("isn't compatible with Skyport panel")
}

fn prompt_for_bootstrap_config(config: &mut DaemonConfig) -> Result<()> {
    ensure_interactive_terminal()?;
    render_setup_banner(&config.panel.url);

    let panel_url = prompt_panel_url(&config.panel.url)?;
    let configuration_token =
        prompt_configuration_token(config.panel.configuration_token.as_deref())?;

    config.persist_bootstrap(&panel_url, &configuration_token)?;
    config.panel.url = panel_url;
    config.panel.configuration_token = Some(configuration_token);

    println!();
    println!(
        "{orange}✓{reset} {gray}Saved setup details to {orange}config/local.toml{reset}",
        orange = ORANGE,
        gray = GRAY,
        reset = RESET,
    );
    println!();

    Ok(())
}

fn has_bootstrap_values(config: &DaemonConfig) -> bool {
    !config.panel.url.trim().is_empty()
        && config
            .panel
            .configuration_token
            .as_deref()
            .is_some_and(|token| !token.trim().is_empty())
}

fn terminal_attached() -> bool {
    io::stdin().is_terminal() && io::stdout().is_terminal()
}

fn ensure_interactive_terminal() -> Result<()> {
    if terminal_attached() {
        return Ok(());
    }

    bail!(
        "daemon requires interactive setup but no terminal is attached; set panel.url and panel.configuration_token in config/local.toml or with SKYPORT_DAEMON__PANEL__* environment variables"
    )
}

fn render_setup_banner(current_url: &str) {
    let normalized_url = normalize_panel_url(current_url);

    println!();
    println!(
        "{gray_dark}╭──────────────────────────────────────────────────────────────╮{reset}",
        gray_dark = GRAY_DARK,
        reset = RESET,
    );
    println!(
        "{gray_dark}│{reset} {orange}Skyport daemon setup{reset}                                         {gray_dark}│{reset}",
        gray_dark = GRAY_DARK,
        orange = ORANGE,
        reset = RESET,
    );
    println!(
        "{gray_dark}│{reset} Enter your panel URL and one-time configuration token   {gray_dark}│{reset}",
        gray_dark = GRAY_DARK,
        reset = RESET,
    );
    println!(
        "{gray_dark}│{reset} to enroll this daemon and generate its local config.   {gray_dark}│{reset}",
        gray_dark = GRAY_DARK,
        reset = RESET,
    );
    println!(
        "{gray_dark}╰──────────────────────────────────────────────────────────────╯{reset}",
        gray_dark = GRAY_DARK,
        reset = RESET,
    );
    println!(
        "{gray}Default panel URL:{reset} {orange_dark}{url}{reset}",
        gray = GRAY,
        orange_dark = ORANGE_DARK,
        url = normalized_url,
        reset = RESET,
    );
    println!();
}

fn render_setup_error(message: &str) {
    println!();
    println!(
        "{orange}•{reset} {gray}Setup needs another try:{reset} {orange_dark}{message}{reset}",
        orange = ORANGE,
        gray = GRAY,
        orange_dark = ORANGE_DARK,
        message = message,
        reset = RESET,
    );
    println!();
}

fn prompt_panel_url(current_url: &str) -> Result<String> {
    loop {
        let default_url = normalize_panel_url(current_url);
        let input = prompt_line("Panel URL", Some(default_url.as_str()))?;
        let normalized = normalize_panel_url(&input);

        if is_valid_panel_url(&normalized) {
            return Ok(normalized);
        }

        println!(
            "{orange}•{reset} {gray}Enter a valid http:// or https:// URL.{reset}",
            orange = ORANGE,
            gray = GRAY,
            reset = RESET,
        );
    }
}

fn prompt_configuration_token(existing_token: Option<&str>) -> Result<String> {
    loop {
        let default = existing_token.filter(|token| !token.trim().is_empty());
        let label = if default.is_some() {
            "Configuration token (press enter to keep current)"
        } else {
            "Configuration token"
        };

        let input = prompt_line(label, None)?;
        let trimmed = input.trim();

        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }

        if let Some(token) = default {
            return Ok(token.trim().to_string());
        }

        println!(
            "{orange}•{reset} {gray}A configuration token is required.{reset}",
            orange = ORANGE,
            gray = GRAY,
            reset = RESET,
        );
    }
}

fn prompt_line(label: &str, default: Option<&str>) -> Result<String> {
    print!(
        "{orange}›{reset} {gray}{label}{reset}",
        orange = ORANGE,
        gray = GRAY,
        label = label,
        reset = RESET,
    );

    if let Some(default) = default.filter(|value| !value.trim().is_empty()) {
        print!(
            " {gray_dark}[{orange_dark}{default}{gray_dark}]{reset}",
            gray_dark = GRAY_DARK,
            orange_dark = ORANGE_DARK,
            default = default,
            reset = RESET,
        );
    }

    print!(": ");
    io::stdout().flush().context("failed to flush stdout")?;

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .context("failed to read setup input")?;

    let trimmed = input.trim();

    if trimmed.is_empty() {
        return Ok(default.unwrap_or_default().to_string());
    }

    Ok(trimmed.to_string())
}

fn normalize_panel_url(url: &str) -> String {
    url.trim().trim_end_matches('/').to_string()
}

fn is_valid_panel_url(url: &str) -> bool {
    reqwest::Url::parse(url)
        .map(|parsed| matches!(parsed.scheme(), "http" | "https") && parsed.host_str().is_some())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DaemonSection, LogFormat, LoggingSection, PanelSection, RuntimeSection};
    use std::time::Duration;

    fn sample_config(url: &str, token: Option<&str>) -> DaemonConfig {
        DaemonConfig {
            daemon: DaemonSection {
                name: "skyportd".to_string(),
                uuid: "00000000-0000-0000-0000-000000000000".to_string(),
                tick_interval: Duration::from_secs(5),
                shutdown_timeout: Duration::from_secs(30),
            },
            panel: PanelSection {
                url: url.to_string(),
                configuration_token: token.map(str::to_string),
                daemon_secret: None,
                node_id: None,
            },
            logging: LoggingSection {
                level: "info".to_string(),
                format: LogFormat::Pretty,
            },
            runtime: RuntimeSection { worker_threads: 0 },
        }
    }

    #[test]
    fn bootstrap_prompt_is_skipped_when_url_and_token_exist() {
        let config = sample_config("https://panel.example.com", Some("token-123"));

        assert!(has_bootstrap_values(&config));
    }

    #[test]
    fn bootstrap_prompt_is_required_when_token_is_missing() {
        let config = sample_config("https://panel.example.com", Some("   "));

        assert!(!has_bootstrap_values(&config));
    }

    #[test]
    fn normalizes_panel_urls() {
        assert_eq!(
            normalize_panel_url(" https://panel.example.com/// "),
            "https://panel.example.com"
        );
    }

    #[test]
    fn validates_http_and_https_panel_urls() {
        assert!(is_valid_panel_url("http://127.0.0.1:8000"));
        assert!(is_valid_panel_url("https://panel.example.com"));
        assert!(!is_valid_panel_url("ftp://panel.example.com"));
        assert!(!is_valid_panel_url("panel.example.com"));
    }

    #[test]
    fn compatibility_check_requires_matching_versions() {
        assert!(ensure_compatible_panel_version(CURRENT_VERSION).is_ok());

        let error = ensure_compatible_panel_version("9.9.9")
            .expect_err("mismatched versions should fail")
            .to_string();

        assert_eq!(
            error,
            "This version of skyportd isn't compatible with Skyport panel 9.9.9."
        );
    }

    #[test]
    fn detects_compatibility_messages() {
        assert!(is_compatibility_error_message(
            "This version of skyportd isn't compatible with Skyport panel 0.1.0."
        ));
        assert!(!is_compatibility_error_message(
            "The enrollment token is invalid."
        ));
    }
}
