use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::Args;
use console::style;

use bytehaul_proto::resume::StateManager;
use crate::output::Reporter;

#[derive(Args)]
pub struct CleanArgs {
    /// Maximum age of state files to keep (e.g., 7d, 24h, 1d)
    #[arg(long, default_value = "7d")]
    pub max_age: String,

    /// State directory (default: ~/.bytehaul/state)
    #[arg(long)]
    pub state_dir: Option<String>,

    /// Show what would be deleted without deleting
    #[arg(long)]
    pub dry_run: bool,
}

pub async fn run(args: CleanArgs, reporter: &Reporter) -> Result<()> {
    let max_age = parse_duration(&args.max_age)?;
    let state_dir = args.state_dir.map(PathBuf::from);

    let mgr = StateManager::new(state_dir)?;

    if args.dry_run {
        let count = mgr.gc_preview(max_age)?;
        if count == 0 {
            reporter.info("  No stale state files found.");
        } else {
            reporter.info(&format!(
                "  Dry run: would delete {} state file(s) older than {}",
                count,
                style(&args.max_age).cyan()
            ));
        }
    } else {
        let count = mgr.gc(max_age)?;
        if count == 0 {
            reporter.info("  No stale state files found.");
        } else {
            reporter.info(&format!(
                "  {} Removed {} state file(s) older than {}",
                style("✓").green(),
                count,
                args.max_age
            ));
        }
    }

    Ok(())
}

fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim().to_lowercase();
    if let Some(n) = s.strip_suffix('d') {
        let days: u64 = n.parse()?;
        Ok(Duration::from_secs(days * 86400))
    } else if let Some(n) = s.strip_suffix('h') {
        let hours: u64 = n.parse()?;
        Ok(Duration::from_secs(hours * 3600))
    } else if let Some(n) = s.strip_suffix('m') {
        let mins: u64 = n.parse()?;
        Ok(Duration::from_secs(mins * 60))
    } else if let Some(n) = s.strip_suffix('s') {
        let secs: u64 = n.parse()?;
        Ok(Duration::from_secs(secs))
    } else {
        // Assume days
        let days: u64 = s.parse()?;
        Ok(Duration::from_secs(days * 86400))
    }
}
