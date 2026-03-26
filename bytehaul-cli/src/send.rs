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
}

pub async fn run(args: SendArgs) -> Result<()> {
    let source = PathBuf::from(&args.source);
    if !source.exists() {
        anyhow::bail!("Source file not found: {}", source.display());
    }

    let file_size = tokio::fs::metadata(&source).await?.len();

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
    let mut transfer = client.send_file(source_str, &remote_path).await?;

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
