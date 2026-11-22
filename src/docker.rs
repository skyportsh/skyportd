//! Helpers around the `docker` command line client.

use std::collections::HashSet;
use std::io;
use std::process::Output;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::process::Command;

/// Upper bound for a short-lived docker command. Without it a hung docker
/// daemon would block the calling task (and whatever lock it holds) forever.
pub const DOCKER_COMMAND_TIMEOUT: Duration = Duration::from_secs(90);

/// Name prefix shared by every runtime container managed by the daemon.
pub const RUNTIME_CONTAINER_PREFIX: &str = "skyport-server-";

pub fn docker_command() -> Command {
    let mut command = Command::new("docker");
    command.kill_on_drop(true);
    command
}

/// Runs a docker command to completion with a timeout. The child process is
/// killed when the timeout elapses because commands are `kill_on_drop`.
pub trait DockerOutput {
    fn docker_output(&mut self) -> impl Future<Output = io::Result<Output>> + Send;
}

impl DockerOutput for Command {
    async fn docker_output(&mut self) -> io::Result<Output> {
        match tokio::time::timeout(DOCKER_COMMAND_TIMEOUT, self.output()).await {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "docker command timed out after {}s",
                    DOCKER_COMMAND_TIMEOUT.as_secs()
                ),
            )),
        }
    }
}

/// Names of every running runtime container, fetched with a single process
/// instead of one `docker inspect` per server.
pub async fn running_runtime_containers() -> Result<HashSet<String>> {
    let output = docker_command()
        .args([
            "ps",
            "--filter",
            &format!("name=^{RUNTIME_CONTAINER_PREFIX}"),
            "--format",
            "{{.Names}}",
        ])
        .docker_output()
        .await
        .context("failed to list running containers")?;

    if !output.status.success() {
        anyhow::bail!(
            "docker ps failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect())
}
