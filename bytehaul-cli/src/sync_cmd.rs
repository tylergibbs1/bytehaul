use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use console::style;

use bytehaul_lib::client::Client;
use bytehaul_lib::config::TransferConfig;
use bytehaul_proto::congestion::CongestionMode;
use bytehaul_proto::filter::FileFilter;
use bytehaul_proto::manifest::TransferManifest;
use bytehaul_proto::sync::{self, ConflictMode, SyncPlan};

use crate::output::Reporter;

#[derive(Args)]
pub struct SyncArgs {
    /// Local directory to sync
    pub local_dir: String,

    /// Remote directory (user@host:/path)
    pub remote: String,

    /// How to handle files that differ on both sides
    #[arg(long, value_enum, default_value = "newer")]
    pub conflict: ConflictArg,

    /// Use aggressive congestion control
    #[arg(long)]
    pub aggressive: bool,

    /// Block size in MB
    #[arg(long, default_value = "16")]
    pub block_size: u32,

    /// Number of parallel streams
    #[arg(long, default_value = "16")]
    pub parallel: usize,

    /// Connect to a running daemon
    #[arg(long)]
    pub daemon: Option<String>,

    /// Show what would be synced without transferring
    #[arg(long)]
    pub dry_run: bool,

    /// Enable delta transfers for changed files
    #[arg(long)]
    pub delta: bool,

    /// Include only files matching these patterns
    #[arg(long, value_delimiter = ',')]
    pub include: Vec<String>,

    /// Exclude files matching these patterns
    #[arg(long, value_delimiter = ',')]
    pub exclude: Vec<String>,

    /// FEC group size: generate 1 parity chunk per N data chunks (0 = disabled)
    #[arg(long)]
    pub fec_group_size: Option<usize>,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum ConflictArg {
    /// Keep the newer file (by mtime)
    Newer,
    /// Local always wins
    Local,
    /// Remote always wins
    Remote,
    /// Skip conflicting files
    Skip,
}

impl From<ConflictArg> for ConflictMode {
    fn from(arg: ConflictArg) -> Self {
        match arg {
            ConflictArg::Newer => ConflictMode::Newer,
            ConflictArg::Local => ConflictMode::SourceWins,
            ConflictArg::Remote => ConflictMode::DestWins,
            ConflictArg::Skip => ConflictMode::Skip,
        }
    }
}

pub async fn run(args: SyncArgs, reporter: &Reporter) -> Result<()> {
    let local_dir = PathBuf::from(&args.local_dir);
    if !local_dir.is_dir() {
        anyhow::bail!("Local path must be a directory: {}", local_dir.display());
    }

    let (remote_host, remote_path) = if let Some(ref daemon) = args.daemon {
        (String::new(), args.remote.clone())
    } else {
        parse_remote(&args.remote)?
    };

    let conflict_mode: ConflictMode = args.conflict.into();

    // 1. Build local manifest
    reporter.info(&format!(
        "  {} Scanning local directory...",
        style("↔").cyan().bold()
    ));

    let filter = if !args.include.is_empty() || !args.exclude.is_empty() {
        FileFilter::new(&args.include, &args.exclude)
            .context("Invalid include/exclude pattern")?
    } else {
        FileFilter::empty()
    };

    let block_size = args.block_size * 1024 * 1024;
    let local_manifest = TransferManifest::from_directory_filtered(
        &local_dir,
        std::path::Path::new(&remote_path),
        block_size,
        &filter,
    )
    .await
    .unwrap_or_else(|_| {
        // Empty local dir — create a minimal manifest for comparison
        reporter.info("  No local files found (empty directory)");
        TransferManifest::empty(block_size)
    });

    reporter.info(&format!(
        "  Local: {} files, {}",
        local_manifest.files.len(),
        humansize::format_size(local_manifest.total_size(), humansize::BINARY)
    ));

    // 2. Connect and get remote manifest
    reporter.info(&format!(
        "  {} Connecting to remote...",
        style("↔").cyan().bold()
    ));

    let client = if let Some(ref daemon_addr) = args.daemon {
        let connect_config = TransferConfig::default();
        Client::connect_daemon_tuned(daemon_addr, &connect_config).await?
    } else {
        Client::connect_ssh(&remote_host).await?
    };

    // For the remote manifest, we do a pull-style connection:
    // send a request, remote builds manifest and sends it back.
    // For now, use a simpler approach: pull the remote directory listing
    // by doing a dry-run style exchange.
    //
    // Since we don't have a dedicated "list remote files" protocol message yet,
    // we compute the sync plan based on what we know:
    // - Files only locally → push
    // - For now, push all local files (full sync = push-only)
    //
    // Full bidirectional sync with remote manifest exchange requires the
    // SyncRequest wire message which is planned but not yet implemented.
    // For v0.2, sync operates as "smart push with delta".

    reporter.info(&format!(
        "  {} Computing sync plan...",
        style("↔").cyan().bold()
    ));

    // Simplified sync: push all local files that need updating.
    // This is equivalent to `send -r --delta` but with sync semantics.
    let plan_summary = format!(
        "{} files to push",
        local_manifest.files.len()
    );

    if args.dry_run {
        reporter.info(&format!("  Dry run: {}", plan_summary));
        for entry in &local_manifest.files {
            let rel = entry.relative_path.as_deref()
                .unwrap_or(entry.dest_path.as_path());
            reporter.info(&format!("    push {}", rel.display()));
        }
        return Ok(());
    }

    reporter.info(&format!("  Plan: {}", plan_summary));

    // 3. Execute push transfers
    let mut config = TransferConfig::builder()
        .resume(true)
        .block_size_mb(args.block_size)
        .max_parallel_streams(args.parallel)
        .delta(args.delta)
        .congestion(if args.aggressive {
            CongestionMode::Aggressive
        } else {
            CongestionMode::Fair
        });
    if let Some(fec) = args.fec_group_size {
        config = config.fec_group_size(fec);
    }
    let config = config.build();

    let client = client.with_config(config);
    let start = Instant::now();

    let source_str = local_dir.to_str().context("Invalid path")?;

    let transfer = if !args.include.is_empty() || !args.exclude.is_empty() {
        let filter = FileFilter::new(&args.include, &args.exclude)?;
        client.send_directory_filtered(source_str, &remote_path, filter).await?
    } else {
        client.send_directory(source_str, &remote_path).await?
    };

    transfer.wait().await?;

    let elapsed = start.elapsed();
    reporter.info(&format!(
        "\n  {} Sync complete. {} in {:.1}s",
        style("✓").green().bold(),
        plan_summary,
        elapsed.as_secs_f64()
    ));

    reporter.transfer_summary(
        local_manifest.files.len() as u64,
        local_manifest.total_size(),
        elapsed.as_secs_f64(),
        false,
    );

    Ok(())
}

fn parse_remote(remote: &str) -> Result<(String, String)> {
    if let Some(colon_pos) = remote.rfind(':') {
        let host = &remote[..colon_pos];
        let path = &remote[colon_pos + 1..];
        if host.is_empty() || path.is_empty() {
            anyhow::bail!("Invalid remote format. Expected: user@host:/path");
        }
        Ok((host.to_string(), path.to_string()))
    } else {
        anyhow::bail!("Invalid remote format. Expected: user@host:/path");
    }
}
