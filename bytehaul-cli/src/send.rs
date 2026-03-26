use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;
use console::style;
use indicatif::{ProgressBar, ProgressStyle};

use bytehaul_lib::client::Client;
use bytehaul_lib::config::TransferConfig;
use bytehaul_proto::congestion::CongestionMode;

#[derive(Args)]
pub struct SendArgs {
    /// Local file to send
    pub source: String,

    /// Remote destination (user@host:/path or host:/path)
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
    #[arg(long, default_value = "4")]
    pub block_size: u32,

    /// Number of parallel streams
    #[arg(long, default_value = "16")]
    pub parallel: usize,

    /// Connect to a running daemon instead of SSH bootstrap
    #[arg(long)]
    pub daemon: Option<String>,

    /// Send directory recursively
    #[arg(short = 'r', long)]
    pub recursive: bool,

    /// Delta transfer: only send changed blocks
    #[arg(long)]
    pub delta: bool,

    /// Show what would be transferred without sending
    #[arg(long)]
    pub dry_run: bool,
}

pub async fn run(args: SendArgs) -> Result<()> {
    let source = PathBuf::from(&args.source);
    if !source.exists() {
        anyhow::bail!("Source file not found: {}", source.display());
    }

    let is_dir = source.is_dir();
    if is_dir && !args.recursive {
        anyhow::bail!("Source is a directory. Use -r/--recursive to send directories.");
    }

    let file_size = if is_dir {
        // Walk dir to get total size for progress bar
        let mut total = 0u64;
        for entry in walkdir::WalkDir::new(&source).into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_file() {
                total += entry.metadata().map(|m| m.len()).unwrap_or(0);
            }
        }
        total
    } else {
        tokio::fs::metadata(&source).await?.len()
    };

    // Dry run: show what would be transferred
    if args.dry_run {
        let file_size_display = humansize::format_size(file_size, humansize::BINARY);
        let block_count = (file_size + (args.block_size as u64 * 1024 * 1024) - 1)
            / (args.block_size as u64 * 1024 * 1024);
        if is_dir {
            let file_count = walkdir::WalkDir::new(&source)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().is_file())
                .count();
            eprintln!("  {} Dry run: would send {} files ({})", style("i").cyan(), file_count, file_size_display);
        } else {
            eprintln!("  {} Dry run: would send 1 file ({})", style("i").cyan(), file_size_display);
        }
        eprintln!("  Source:      {}", source.display());
        eprintln!("  Destination: {}", args.destination);
        eprintln!("  Block size:  {} MB", args.block_size);
        eprintln!("  Blocks:      {}", block_count);
        eprintln!("  Streams:     {}", args.parallel);
        eprintln!("  Congestion:  {}", if args.aggressive { "aggressive" } else { "fair" });
        eprintln!("  Delta:       {}", if args.delta { "enabled" } else { "disabled" });
        eprintln!("  Resume:      {}", if args.resume { "enabled" } else { "disabled" });
        return Ok(());
    }

    // Parse destination: in daemon mode, destination is just a remote path;
    // in SSH mode, it's user@host:/path
    let (remote, remote_path) = if args.daemon.is_some() {
        // With --daemon, the destination is just the remote path
        (String::new(), args.destination.clone())
    } else {
        parse_destination(&args.destination)?
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

    let config = config.delta(args.delta).build();

    // Connect
    let client = if let Some(ref daemon_addr) = args.daemon {
        eprintln!(
            "  Connecting to daemon at {}...",
            style(daemon_addr).cyan()
        );
        Client::connect_daemon(daemon_addr, None).await?
    } else {
        eprintln!(
            "  {} receiver via SSH...",
            style("Bootstrapping").cyan()
        );
        Client::connect_ssh(&remote).await?
    };

    let client = client.with_config(config);

    // Set up progress bar
    let pb = ProgressBar::new(file_size);
    pb.set_style(
        ProgressStyle::with_template(
            "  {bar:40.cyan/blue} {bytes}/{total_bytes}\n  Elapsed: {elapsed_precise} | {bytes_per_sec} | ETA: {eta}",
        )
        .expect("hard-coded progress template is always valid")
        .progress_chars("█▉▊▋▌▍▎▏ "),
    );

    let file_size_display = humansize::format_size(file_size, humansize::BINARY);
    eprintln!(
        "  Sending: {} ({})",
        style(source.display()).bold(),
        file_size_display,
    );

    let source_str = source
        .to_str()
        .context("source path contains invalid UTF-8")?;
    let mut transfer = if is_dir {
        client.send_directory(source_str, &remote_path).await?
    } else {
        client.send_file(source_str, &remote_path).await?
    };

    let pb_clone = pb.clone();
    transfer.on_progress(move |p| {
        pb_clone.set_position(p.transferred_bytes);
    });

    transfer.wait().await?;

    pb.finish_and_clear();
    eprintln!(
        "\n  {} Transfer complete. BLAKE3 verified.",
        style("✓").green().bold()
    );

    Ok(())
}

/// Parse "user@host:/path" or "host:/path" into (remote, path).
fn parse_destination(dest: &str) -> Result<(String, String)> {
    if let Some(colon_pos) = dest.rfind(':') {
        let remote = &dest[..colon_pos];
        let path = &dest[colon_pos + 1..];
        if remote.is_empty() || path.is_empty() {
            anyhow::bail!(
                "Invalid destination format. Expected: user@host:/path or host:/path"
            );
        }
        Ok((remote.to_string(), path.to_string()))
    } else {
        anyhow::bail!(
            "Invalid destination format. Expected: user@host:/path or host:/path"
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
