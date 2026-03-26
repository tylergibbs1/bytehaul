use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;
use console::style;
use indicatif::{ProgressBar, ProgressStyle};

use bytehaul_lib::client::Client;
use bytehaul_lib::config::TransferConfig;
use bytehaul_proto::congestion::CongestionMode;

#[derive(Args)]
pub struct PullArgs {
    /// Remote source (user@host:/path or host:/path)
    pub source: String,

    /// Local destination directory
    pub destination: String,

    /// Resume a previous transfer
    #[arg(long)]
    pub resume: bool,

    /// Use aggressive congestion control (saturate link)
    #[arg(long)]
    pub aggressive: bool,

    /// Maximum transfer rate (e.g., 500mbps, 1gbps)
    #[arg(long)]
    pub max_rate: Option<String>,

    /// Block size in MB
    #[arg(long, default_value = "16")]
    pub block_size: u32,

    /// Number of parallel streams
    #[arg(long, default_value = "16")]
    pub parallel: usize,

    /// Connect to a running daemon instead of SSH bootstrap
    #[arg(long)]
    pub daemon: Option<String>,

    /// Pull directory recursively
    #[arg(short = 'r', long)]
    pub recursive: bool,
}

pub async fn run(args: PullArgs) -> Result<()> {
    let dest = PathBuf::from(&args.destination);

    // Ensure destination directory exists or can be created.
    if !dest.exists() {
        tokio::fs::create_dir_all(&dest)
            .await
            .with_context(|| format!("Failed to create destination directory: {}", dest.display()))?;
    }

    // Parse source: user@host:/path or host:/path
    let (remote, remote_path) = if args.daemon.is_some() {
        // With --daemon, source is just the remote path
        (String::new(), args.source.clone())
    } else {
        parse_source(&args.source)?
    };

    // Build config
    let mut config = TransferConfig::builder()
        .resume(args.resume)
        .block_size_mb(args.block_size)
        .max_parallel_streams(args.parallel)
        .congestion(if args.aggressive {
            CongestionMode::Aggressive
        } else {
            CongestionMode::Fair
        });

    if let Some(ref rate) = args.max_rate {
        let mbps = parse_rate(rate)?;
        config = config.max_bandwidth_mbps(mbps);
    }

    let config = config.build();

    // Connect
    let client = if let Some(ref daemon_addr) = args.daemon {
        eprintln!(
            "  Connecting to daemon at {}...",
            style(daemon_addr).cyan()
        );
        Client::connect_daemon(daemon_addr, None).await?
    } else {
        eprintln!(
            "  {} daemon via SSH...",
            style("Bootstrapping").cyan()
        );
        Client::connect_ssh(&remote).await?
    };

    let client = client.with_config(config);

    eprintln!(
        "  Pulling: {} -> {}",
        style(&args.source).bold(),
        style(dest.display()).bold(),
    );

    // Set up progress bar (unknown size until manifest arrives, use spinner first)
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template(
            "  {spinner:.cyan} {bytes} received | {bytes_per_sec} | {elapsed_precise}",
        )
        .expect("hard-coded progress template is always valid"),
    );

    let mut transfer = if args.recursive {
        client.pull_directory(&remote_path, &args.destination).await?
    } else {
        client.pull_file(&remote_path, &args.destination).await?
    };

    let pb_clone = pb.clone();
    let length_set = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let length_set_clone = length_set.clone();
    transfer.on_progress(move |p| {
        // Switch from spinner to bar once we know the total size.
        if !length_set_clone.load(std::sync::atomic::Ordering::Relaxed) && p.total_bytes > 0 {
            length_set_clone.store(true, std::sync::atomic::Ordering::Relaxed);
            pb_clone.set_length(p.total_bytes);
            pb_clone.set_style(
                ProgressStyle::with_template(
                    "  {bar:40.cyan/blue} {bytes}/{total_bytes}\n  Elapsed: {elapsed_precise} | {bytes_per_sec} | ETA: {eta}",
                )
                .expect("hard-coded progress template is always valid")
                .progress_chars("█▉▊▋▌▍▎▏ "),
            );
        }
        pb_clone.set_position(p.transferred_bytes);
    });

    transfer.wait().await?;

    pb.finish_and_clear();
    eprintln!(
        "\n  {} Pull complete. BLAKE3 verified.",
        style("✓").green().bold()
    );

    Ok(())
}

/// Parse "user@host:/path" or "host:/path" into (remote, path).
fn parse_source(src: &str) -> Result<(String, String)> {
    if let Some(colon_pos) = src.rfind(':') {
        let remote = &src[..colon_pos];
        let path = &src[colon_pos + 1..];
        if remote.is_empty() || path.is_empty() {
            anyhow::bail!(
                "Invalid source format. Expected: user@host:/path or host:/path"
            );
        }
        Ok((remote.to_string(), path.to_string()))
    } else {
        anyhow::bail!(
            "Invalid source format. Expected: user@host:/path or host:/path"
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
