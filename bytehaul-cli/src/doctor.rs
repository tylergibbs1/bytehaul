use std::env;
use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use clap::{Args, Subcommand};
use console::style;
use tokio::process::Command;

use bytehaul_lib::client::Client;
use crate::output::Reporter;

#[derive(Args)]
pub struct DoctorArgs {
    #[command(subcommand)]
    pub command: Option<DoctorCommand>,
}

#[derive(Subcommand)]
pub enum DoctorCommand {
    /// Check a send workflow before transferring
    Send(DoctorSendArgs),
    /// Check a pull workflow before transferring
    Pull(DoctorPullArgs),
    /// Check a direct daemon address
    Daemon(DoctorDaemonArgs),
}

#[derive(Args)]
pub struct DoctorSendArgs {
    /// Local file or directory to send
    pub source: String,

    /// Remote destination (user@host:/path) or remote path with --daemon
    pub destination: String,

    /// Connect to a running daemon instead of SSH bootstrap
    #[arg(long)]
    pub daemon: Option<String>,
}

#[derive(Args)]
pub struct DoctorPullArgs {
    /// Remote source (user@host:/path) or remote path with --daemon
    pub source: String,

    /// Local destination path
    pub destination: String,

    /// Connect to a running daemon instead of SSH bootstrap
    #[arg(long)]
    pub daemon: Option<String>,
}

#[derive(Args)]
pub struct DoctorDaemonArgs {
    /// Daemon address host:port
    pub address: String,
}

#[derive(Debug, Clone, Copy)]
enum CheckStatus {
    Pass,
    Warn,
    Fail,
}

struct Check {
    status: CheckStatus,
    label: String,
    detail: String,
}

impl Check {
    fn pass(label: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            status: CheckStatus::Pass,
            label: label.into(),
            detail: detail.into(),
        }
    }

    fn warn(label: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            status: CheckStatus::Warn,
            label: label.into(),
            detail: detail.into(),
        }
    }

    fn fail(label: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            status: CheckStatus::Fail,
            label: label.into(),
            detail: detail.into(),
        }
    }
}

pub async fn run(args: DoctorArgs, _reporter: &Reporter) -> Result<()> {
    let mut checks = run_local_checks().await;

    match args.command {
        Some(DoctorCommand::Send(args)) => checks.extend(run_send_checks(args).await),
        Some(DoctorCommand::Pull(args)) => checks.extend(run_pull_checks(args).await),
        Some(DoctorCommand::Daemon(args)) => checks.extend(run_daemon_checks(args).await),
        None => checks.push(Check::warn(
            "Workflow",
            "No workflow selected. Run `bytehaul doctor send ...`, `pull ...`, or `daemon ...` for targeted checks.",
        )),
    }

    render_checks(&checks);

    let failures = checks
        .iter()
        .filter(|check| matches!(check.status, CheckStatus::Fail))
        .count();

    if failures > 0 {
        anyhow::bail!("{failures} doctor check(s) failed");
    }

    Ok(())
}

async fn run_local_checks() -> Vec<Check> {
    let mut checks = Vec::new();

    match env::current_exe() {
        Ok(path) => checks.push(Check::pass(
            "Binary",
            format!("Local bytehaul binary available at {}", path.display()),
        )),
        Err(err) => checks.push(Check::fail(
            "Binary",
            format!("Could not determine local bytehaul path: {err}"),
        )),
    }

    for tool in ["ssh", "scp"] {
        match find_in_path(tool) {
            Some(path) => checks.push(Check::pass(
                format!("{tool}"),
                format!("Found at {}", path.display()),
            )),
            None => checks.push(Check::fail(
                format!("{tool}"),
                format!("{tool} is not on PATH"),
            )),
        }
    }

    checks
}

async fn run_send_checks(args: DoctorSendArgs) -> Vec<Check> {
    let mut checks = Vec::new();
    let source = PathBuf::from(&args.source);
    if !source.exists() {
        checks.push(Check::fail(
            "Source",
            format!("Local path does not exist: {}", source.display()),
        ));
    } else {
        checks.push(Check::pass(
            "Source",
            format!("Found local path {}", source.display()),
        ));
    }

    if let Some(daemon) = args.daemon {
        checks.extend(check_daemon_address(&daemon).await);
        if args.destination.is_empty() {
            checks.push(Check::fail(
                "Destination",
                "Destination path is empty".to_string(),
            ));
        } else {
            checks.push(Check::pass(
                "Destination",
                format!("Remote daemon path: {}", args.destination),
            ));
        }
        return checks;
    }

    let (remote, remote_path) = match parse_remote_path(&args.destination) {
        Ok(parts) => parts,
        Err(err) => {
            checks.push(Check::fail("Destination", err.to_string()));
            return checks;
        }
    };

    checks.push(Check::pass(
        "Destination",
        format!("Parsed remote target {} -> {}", remote, remote_path),
    ));
    checks.extend(check_remote_host(&remote).await);
    checks.extend(check_remote_parent_writable(&remote, &remote_path).await);
    checks
}

async fn run_pull_checks(args: DoctorPullArgs) -> Vec<Check> {
    let mut checks = Vec::new();
    checks.extend(check_local_destination_parent(&args.destination));

    if let Some(daemon) = args.daemon {
        checks.extend(check_daemon_address(&daemon).await);
        if args.source.is_empty() {
            checks.push(Check::fail("Source", "Remote path is empty".to_string()));
        } else {
            checks.push(Check::pass(
                "Source",
                format!("Remote daemon path: {}", args.source),
            ));
        }
        return checks;
    }

    let (remote, remote_path) = match parse_remote_path(&args.source) {
        Ok(parts) => parts,
        Err(err) => {
            checks.push(Check::fail("Source", err.to_string()));
            return checks;
        }
    };

    checks.push(Check::pass(
        "Source",
        format!("Parsed remote source {} -> {}", remote, remote_path),
    ));
    checks.extend(check_remote_host(&remote).await);
    checks.extend(check_remote_path_readable(&remote, &remote_path).await);
    checks
}

async fn run_daemon_checks(args: DoctorDaemonArgs) -> Vec<Check> {
    check_daemon_address(&args.address).await
}

async fn check_daemon_address(addr: &str) -> Vec<Check> {
    let mut checks = Vec::new();
    let socket_addr: SocketAddr = match addr.parse() {
        Ok(addr) => {
            checks.push(Check::pass("Daemon address", format!("Parsed {}", addr)));
            addr
        }
        Err(err) => {
            checks.push(Check::fail(
                "Daemon address",
                format!("Invalid host:port: {err}"),
            ));
            return checks;
        }
    };

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect_daemon(&socket_addr.to_string(), None),
    )
    .await;

    match result {
        Ok(Ok(_)) => checks.push(Check::pass(
            "Daemon reachability",
            format!("Connected to {}", socket_addr),
        )),
        Ok(Err(err)) => checks.push(Check::fail(
            "Daemon reachability",
            format!("Could not connect to {}: {}", socket_addr, err),
        )),
        Err(_) => checks.push(Check::fail(
            "Daemon reachability",
            format!("Timed out connecting to {}", socket_addr),
        )),
    }

    checks
}

async fn check_remote_host(remote: &str) -> Vec<Check> {
    let mut checks = Vec::new();
    let mut cmd = Command::new("ssh");
    cmd.arg("-o").arg("BatchMode=yes");
    cmd.arg("-o").arg("ConnectTimeout=5");
    cmd.arg(remote);
    cmd.arg("true");

    match tokio::time::timeout(Duration::from_secs(7), cmd.status()).await {
        Ok(Ok(status)) if status.success() => checks.push(Check::pass(
            "SSH",
            format!("SSH login works for {}", remote),
        )),
        Ok(Ok(status)) => checks.push(Check::fail(
            "SSH",
            format!("SSH exited with status {} for {}", status, remote),
        )),
        Ok(Err(err)) => checks.push(Check::fail(
            "SSH",
            format!("Failed to run ssh for {}: {}", remote, err),
        )),
        Err(_) => checks.push(Check::fail(
            "SSH",
            format!("Timed out connecting to {}", remote),
        )),
    }

    let mut which_cmd = Command::new("ssh");
    which_cmd.arg("-o").arg("BatchMode=yes");
    which_cmd.arg("-o").arg("ConnectTimeout=5");
    which_cmd.arg(remote);
    which_cmd.arg("which").arg("bytehaul");
    match tokio::time::timeout(Duration::from_secs(7), which_cmd.output()).await {
        Ok(Ok(output)) if output.status.success() => {
            let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
            checks.push(Check::pass(
                "Remote binary",
                format!("bytehaul already installed at {}", path),
            ));
        }
        Ok(Ok(_)) => checks.push(Check::warn(
            "Remote binary",
            "bytehaul not found on remote PATH; SSH bootstrap will upload a temporary binary",
        )),
        Ok(Err(err)) => checks.push(Check::warn(
            "Remote binary",
            format!("Could not check remote PATH: {}", err),
        )),
        Err(_) => checks.push(Check::warn(
            "Remote binary",
            "Timed out checking remote PATH",
        )),
    }

    checks
}

async fn check_remote_parent_writable(remote: &str, remote_path: &str) -> Vec<Check> {
    let mut checks = Vec::new();
    let parent = parent_for_remote(remote_path);
    let script = format!(
        "sh -lc 'd={}; if [ -d \"$d\" ]; then test -w \"$d\"; else test -w \"$(dirname \"$d\")\"; fi'",
        shell_quote(&parent)
    );

    let mut cmd = Command::new("ssh");
    cmd.arg("-o").arg("BatchMode=yes");
    cmd.arg("-o").arg("ConnectTimeout=5");
    cmd.arg(remote);
    cmd.arg(script);

    match tokio::time::timeout(Duration::from_secs(7), cmd.status()).await {
        Ok(Ok(status)) if status.success() => checks.push(Check::pass(
            "Remote destination",
            format!("Parent path looks writable: {}", parent),
        )),
        Ok(Ok(_)) => checks.push(Check::fail(
            "Remote destination",
            format!("Parent path is not writable or does not exist: {}", parent),
        )),
        Ok(Err(err)) => checks.push(Check::fail(
            "Remote destination",
            format!("Could not validate remote destination: {}", err),
        )),
        Err(_) => checks.push(Check::fail(
            "Remote destination",
            "Timed out checking remote destination".to_string(),
        )),
    }

    checks
}

async fn check_remote_path_readable(remote: &str, remote_path: &str) -> Vec<Check> {
    let mut checks = Vec::new();
    let script = format!(
        "sh -lc 'test -e {} && test -r {}'",
        shell_quote(remote_path),
        shell_quote(remote_path)
    );
    let mut cmd = Command::new("ssh");
    cmd.arg("-o").arg("BatchMode=yes");
    cmd.arg("-o").arg("ConnectTimeout=5");
    cmd.arg(remote);
    cmd.arg(script);

    match tokio::time::timeout(Duration::from_secs(7), cmd.status()).await {
        Ok(Ok(status)) if status.success() => checks.push(Check::pass(
            "Remote source",
            format!("Remote path exists and is readable: {}", remote_path),
        )),
        Ok(Ok(_)) => checks.push(Check::fail(
            "Remote source",
            format!("Remote path does not exist or is not readable: {}", remote_path),
        )),
        Ok(Err(err)) => checks.push(Check::fail(
            "Remote source",
            format!("Could not check remote path: {}", err),
        )),
        Err(_) => checks.push(Check::fail(
            "Remote source",
            "Timed out checking remote path".to_string(),
        )),
    }

    checks
}

fn check_local_destination_parent(destination: &str) -> Vec<Check> {
    let mut checks = Vec::new();
    let path = PathBuf::from(destination);
    let parent = if path.is_dir() {
        path.clone()
    } else {
        path.parent().unwrap_or_else(|| Path::new(".")).to_path_buf()
    };

    if parent.exists() {
        checks.push(Check::pass(
            "Local destination",
            format!("Parent path exists: {}", parent.display()),
        ));
    } else if parent
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .exists()
    {
        checks.push(Check::warn(
            "Local destination",
            format!(
                "Parent path does not exist yet but may be creatable: {}",
                parent.display()
            ),
        ));
    } else {
        checks.push(Check::fail(
            "Local destination",
            format!("Parent path is missing: {}", parent.display()),
        ));
    }

    checks
}

fn render_checks(checks: &[Check]) {
    println!();
    println!("  {} {}", style("▲").cyan().bold(), style("ByteHaul Doctor").bold());
    for check in checks {
        let (icon, label_style) = match check.status {
            CheckStatus::Pass => (style("✓").green().bold(), style(&check.label).green()),
            CheckStatus::Warn => (style("!").yellow().bold(), style(&check.label).yellow()),
            CheckStatus::Fail => (style("✗").red().bold(), style(&check.label).red()),
        };
        println!("  {} {}: {}", icon, label_style, check.detail);
    }
    println!();
}

fn parse_remote_path(value: &str) -> Result<(String, String)> {
    if let Some(colon_pos) = value.rfind(':') {
        let remote = &value[..colon_pos];
        let path = &value[colon_pos + 1..];
        if remote.is_empty() || path.is_empty() {
            anyhow::bail!("Expected remote path in the form user@host:/path");
        }
        Ok((remote.to_string(), path.to_string()))
    } else {
        anyhow::bail!("Expected remote path in the form user@host:/path");
    }
}

fn parent_for_remote(path: &str) -> String {
    let path = Path::new(path);
    path.parent()
        .unwrap_or_else(|| Path::new("/"))
        .display()
        .to_string()
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn find_in_path(bin: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    env::split_paths(&path).find_map(|dir| {
        let candidate = dir.join(bin);
        if candidate.is_file() {
            Some(candidate)
        } else {
            executable_with_extensions(&dir, bin)
        }
    })
}

fn executable_with_extensions(dir: &Path, bin: &str) -> Option<PathBuf> {
    if cfg!(windows) {
        for ext in [".exe", ".bat", ".cmd"] {
            let candidate = dir.join(format!("{bin}{ext}"));
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

#[allow(dead_code)]
fn _os(value: &str) -> OsString {
    OsString::from(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_path_parser_works() {
        let (remote, path) = parse_remote_path("user@example:/tmp/out").unwrap();
        assert_eq!(remote, "user@example");
        assert_eq!(path, "/tmp/out");
    }

    #[test]
    fn shell_quote_handles_single_quotes() {
        assert_eq!(shell_quote("a'b"), "'a'\"'\"'b'");
    }
}
