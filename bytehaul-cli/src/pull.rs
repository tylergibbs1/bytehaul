use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Args;
use console::style;
use indicatif::{ProgressBar, ProgressStyle};

use bytehaul_lib::client::Client;
use bytehaul_lib::config::TransferConfig;

use crate::output::{Reporter, PROGRESS_TEMPLATE, PROGRESS_CHARS, SPINNER_TEMPLATE, format_bytes};
use crate::profiles::{apply_transfer_profile, TransferProfile};

#[derive(Args)]
pub struct PullArgs {
    /// Remote source (user@host:/path or host:/path)
    #[arg(help_heading = "Common")]
    pub source: String,

    /// Local destination directory
    #[arg(help_heading = "Common")]
    pub destination: String,

    /// Resume a previous transfer
    #[arg(long, help_heading = "Common")]
    pub resume: bool,

    /// Friendly preset for common transfer patterns
    #[arg(long, value_enum, default_value_t = TransferProfile::Dev, help_heading = "Common")]
    pub profile: TransferProfile,

    /// Connect to a running daemon instead of SSH bootstrap
    #[arg(long, help_heading = "Common")]
    pub daemon: Option<String>,

    /// Pull directory recursively
    #[arg(short = 'r', long, help_heading = "Common")]
    pub recursive: bool,

    /// Block size in MB
    #[arg(long, default_value = "16", help_heading = "Advanced")]
    pub block_size: u32,

    /// Number of parallel streams
    #[arg(long, default_value = "16", help_heading = "Advanced")]
    pub parallel: usize,

    /// Use aggressive congestion control (saturate link)
    #[arg(long, help_heading = "Advanced")]
    pub aggressive: bool,

    /// Maximum transfer rate (e.g., 500mbps, 1gbps)
    #[arg(long, help_heading = "Advanced")]
    pub max_rate: Option<String>,

    /// FEC group size: generate 1 parity chunk per N data chunks (0 = disabled)
    #[arg(long, help_heading = "Advanced")]
    pub fec_group_size: Option<usize>,

    /// Show what would be pulled without actually transferring
    #[arg(long, help_heading = "Common")]
    pub dry_run: bool,
}

pub async fn run(args: PullArgs, reporter: &crate::output::Reporter) -> Result<()> {
    let start = Instant::now();
    let dest = PathBuf::from(&args.destination);

    // Parse source: user@host:/path or host:/path
    let (remote, remote_path) = if args.daemon.is_some() {
        (String::new(), args.source.clone())
    } else {
        parse_source(&args.source)?
    };

    // Build config
    let (mut config, tuning) = apply_transfer_profile(
        TransferConfig::builder().resume(args.resume),
        args.profile,
        args.block_size,
        args.parallel,
        args.aggressive,
    );

    if let Some(ref rate) = args.max_rate {
        let mbps = parse_rate(rate)?;
        config = config.max_bandwidth_mbps(mbps);
    }

    let mut config = config;
    if let Some(fec) = args.fec_group_size {
        config = config.fec_group_size(fec);
    }
    let config = config.build();

    if args.dry_run {
        reporter.info(&format!("  Dry run: would pull from {}", style(&args.source).bold()));
        reporter.info(&format!("  Destination: {}", dest.display()));
        reporter.info(&tuning.summary_line());
        return Ok(());
    }

    // Ensure destination directory exists or can be created.
    if !dest.exists() {
        tokio::fs::create_dir_all(&dest)
            .await
            .with_context(|| format!("Failed to create destination directory: {}", dest.display()))?;
    }

    // Connect
    let client = if let Some(ref daemon_addr) = args.daemon {
        let msg = format!(
            "  Connecting to daemon at {}...",
            style(daemon_addr).cyan()
        );
        reporter.info(&msg);
        Client::connect_daemon_tuned(daemon_addr, &config).await?
    } else {
        let msg = format!(
            "  {} daemon via SSH...",
            style("Bootstrapping").cyan()
        );
        reporter.info(&msg);
        Client::connect_ssh(&remote).await?
    };

    let client = client.with_config(config);

    reporter.info(&tuning.summary_line());
    let msg = format!(
        "  Pulling: {} -> {}",
        style(&args.source).bold(),
        style(dest.display()).bold(),
    );
    reporter.info(&msg);
    reporter.info("  State: connected, requesting manifest, receiving, waiting for verification...");

    let use_progress = !reporter.is_quiet() && !reporter.is_json();

    // Set up progress bar (unknown size until manifest arrives, use spinner first)
    let pb = if use_progress {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::with_template(SPINNER_TEMPLATE)
                .expect("hard-coded progress template is always valid"),
        );
        Some(pb)
    } else {
        None
    };

    let mut transfer = if args.recursive {
        client.pull_directory(&remote_path, &args.destination).await?
    } else {
        client.pull_file(&remote_path, &args.destination).await?
    };

    if let Some(ref pb) = pb {
        let pb_clone = pb.clone();
        let length_set = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let length_set_clone = length_set.clone();
        transfer.on_progress(move |p| {
            // Switch from spinner to bar once we know the total size.
            if !length_set_clone.load(std::sync::atomic::Ordering::Relaxed) && p.total_bytes > 0 {
                length_set_clone.store(true, std::sync::atomic::Ordering::Relaxed);
                pb_clone.set_length(p.total_bytes);
                pb_clone.set_style(
                    ProgressStyle::with_template(PROGRESS_TEMPLATE)
                        .expect("hard-coded progress template is always valid")
                        .progress_chars(PROGRESS_CHARS),
                );
            }
            pb_clone.set_position(p.transferred_bytes);
        });
    } else if let Some(cb) = reporter.progress_callback() {
        transfer.on_progress(move |p| cb(p));
    }

    let result = tokio::select! {
        r = transfer.wait() => r,
        _ = tokio::signal::ctrl_c() => {
            if let Some(ref pb) = pb {
                pb.finish_and_clear();
            }
            eprintln!();
            eprintln!(
                "  {} Transfer interrupted.",
                style("!").yellow().bold()
            );
            eprintln!(
                "  Resume with: {} pull --resume {} {}",
                style("bytehaul").cyan(),
                args.source,
                args.destination,
            );
            eprintln!();
            return Ok(());
        }
    };

    let elapsed_secs = start.elapsed().as_secs_f64();
    let total_bytes = pb.as_ref().map_or(0, |p| p.position());

    if let Some(ref pb) = pb {
        pb.finish_and_clear();
    }

    result?;
    reporter.transfer_summary(0, total_bytes, elapsed_secs, true);

    Ok(())
}

/// Parse "user@host:/path" or "host:/path" into (remote, path).
fn parse_source(src: &str) -> Result<(String, String)> {
    if let Some(colon_pos) = src.rfind(':') {
        let remote = &src[..colon_pos];
        let path = &src[colon_pos + 1..];
        if remote.is_empty() || path.is_empty() {
            anyhow::bail!(
                "Invalid source: \"{src}\"\n\n  Expected format: user@host:/path\n  Example: bytehaul pull gpu-node:/data/model.pt ./local/"
            );
        }
        Ok((remote.to_string(), path.to_string()))
    } else {
        anyhow::bail!(
            "Invalid source: \"{src}\"\n\n  Expected format: user@host:/path\n  Example: bytehaul pull gpu-node:/data/model.pt ./local/"
        );
    }
}

/// Parse a rate string like "500mbps" or "1gbps" into megabits per second.
fn parse_rate(rate: &str) -> Result<u64> {
    let rate = rate.to_lowercase();
    if let Some(n) = rate.strip_suffix("gbps") {
        let n: u64 = n.parse().context("Invalid rate number")?;
        Ok(n * 1000)
    } else if let Some(n) = rate.strip_suffix("mbps") {
        let n: u64 = n.parse().context("Invalid rate number")?;
        Ok(n)
    } else if let Some(n) = rate.strip_suffix("kbps") {
        let n: u64 = n.parse().context("Invalid rate number")?;
        Ok(n / 1000)
    } else {
        let n: u64 = rate.parse().context("Invalid rate. Use: 500mbps, 1gbps")?;
        Ok(n)
    }
}
