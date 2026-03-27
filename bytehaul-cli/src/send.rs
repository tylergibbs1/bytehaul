use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Args;
use console::style;
use indicatif::{ProgressBar, ProgressStyle};

use bytehaul_lib::client::Client;
use bytehaul_lib::config::TransferConfig;
use bytehaul_proto::congestion::CongestionMode;
use bytehaul_proto::filter::FileFilter;

use crate::output::{JsonEvent, Reporter};
use crate::profiles::{apply_transfer_profile, TransferProfile};

#[derive(Args)]
pub struct SendArgs {
    /// Local file to send
    #[arg(help_heading = "Common")]
    pub source: String,

    /// Remote destination(s). Multiple destinations for fan-out:
    /// bytehaul send ./data host1:/path host2:/path host3:/path
    #[arg(num_args = 1.., help_heading = "Common")]
    pub destinations: Vec<String>,

    /// Resume a previous transfer
    #[arg(long, help_heading = "Common")]
    pub resume: bool,

    /// Friendly preset for common transfer patterns
    #[arg(long, value_enum, default_value_t = TransferProfile::Dev, help_heading = "Common")]
    pub profile: TransferProfile,

    /// Send directory recursively
    #[arg(short = 'r', long, help_heading = "Common")]
    pub recursive: bool,

    /// Connect to a running daemon instead of SSH bootstrap
    #[arg(long, help_heading = "Common")]
    pub daemon: Option<String>,

    /// Show what would be transferred without sending
    #[arg(long, help_heading = "Common")]
    pub dry_run: bool,

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

    /// Include only files matching these glob patterns (comma-separated)
    #[arg(long, value_delimiter = ',', help_heading = "Advanced")]
    pub include: Vec<String>,

    /// Exclude files matching these glob patterns (comma-separated)
    #[arg(long, value_delimiter = ',', help_heading = "Advanced")]
    pub exclude: Vec<String>,

    /// Compress chunks with zstd before sending
    #[arg(long, help_heading = "Advanced")]
    pub compress: bool,

    /// Zstd compression level (1-22, default 3)
    #[arg(long, default_value = "3", help_heading = "Advanced")]
    pub compress_level: i32,

    /// Delta transfer: only send changed blocks
    #[arg(long, help_heading = "Experimental")]
    pub delta: bool,
}

pub async fn run(args: SendArgs, json: bool) -> Result<()> {
    let reporter = Reporter::from_flag(json);

    let source = PathBuf::from(&args.source);
    if !source.exists() {
        let msg = format!("Source file not found: {}", source.display());
        reporter.emit(&JsonEvent::Error {
            message: msg.clone(),
        });
        anyhow::bail!("{msg}");
    }

    let is_dir = source.is_dir();
    if is_dir && !args.recursive {
        let msg =
            "Source is a directory. Use -r/--recursive to send directories.".to_string();
        reporter.emit(&JsonEvent::Error {
            message: msg.clone(),
        });
        anyhow::bail!("{msg}");
    }

    let (file_count, file_size) = if is_dir {
        let mut total = 0u64;
        let mut count = 0u64;
        for entry in walkdir::WalkDir::new(&source)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                total += entry.metadata().map(|m| m.len()).unwrap_or(0);
                count += 1;
            }
        }
        (count, total)
    } else {
        (1u64, tokio::fs::metadata(&source).await?.len())
    };

    // Dry run: show what would be transferred
    if args.dry_run {
        let (_, tuning) = apply_transfer_profile(
            TransferConfig::builder(),
            args.profile,
            args.block_size,
            args.parallel,
            args.aggressive,
        );
        let block_count = (file_size + (tuning.block_size_mb as u64 * 1024 * 1024) - 1)
            / (tuning.block_size_mb as u64 * 1024 * 1024);

        if reporter.is_json() {
            reporter.emit(&JsonEvent::DryRun {
                files: file_count,
                total_bytes: file_size,
                block_size: tuning.block_size_mb,
                blocks: block_count,
            });
            return Ok(());
        }

        let file_size_display = humansize::format_size(file_size, humansize::BINARY);
        if is_dir {
            eprintln!(
                "  {} Dry run: would send {} files ({})",
                style("i").cyan(),
                file_count,
                file_size_display,
            );
        } else {
            eprintln!(
                "  {} Dry run: would send 1 file ({})",
                style("i").cyan(),
                file_size_display,
            );
        }
        eprintln!("  Source:      {}", source.display());
        eprintln!("  Destination: {}", args.destinations[0]);
        eprintln!("  Profile:     {}", args.profile.label());
        eprintln!("  Block size:  {} MB", tuning.block_size_mb);
        eprintln!("  Blocks:      {}", block_count);
        eprintln!("  Streams:     {}", tuning.parallel_streams);
        eprintln!(
            "  Congestion:  {}",
            match tuning.congestion {
                CongestionMode::Aggressive => "aggressive",
                CongestionMode::Fair => "fair",
            }
        );
        eprintln!(
            "  Delta:       {}",
            if args.delta { "enabled" } else { "disabled" }
        );
        eprintln!(
            "  Resume:      {}",
            if args.resume { "enabled" } else { "disabled" }
        );
        eprintln!(
            "  Compress:    {}",
            if args.compress {
                format!("enabled (level {})", args.compress_level)
            } else {
                "disabled".to_string()
            }
        );
        return Ok(());
    }

    // ── Multi-destination fan-out ──
    // If multiple destinations, send to each in parallel.
    if args.destinations.len() > 1 && args.daemon.is_none() {
        reporter.info(&format!(
            "  Fan-out: sending to {} destinations in parallel",
            args.destinations.len()
        ));

        let mut handles = Vec::new();
        for dest in &args.destinations {
            let dest = dest.clone();
            let source = source.clone();
            let is_dir = is_dir;
            let (config, _) = apply_transfer_profile(
                TransferConfig::builder().resume(args.resume),
                args.profile,
                args.block_size,
                args.parallel,
                args.aggressive,
            );
            let config = config
                .delta(args.delta)
                .compress(args.compress)
                .compress_level(args.compress_level)
                .build();
            let include = args.include.clone();
            let exclude = args.exclude.clone();

            handles.push(tokio::spawn(async move {
                let (remote, remote_path) = parse_destination(&dest)?;
                let client = Client::connect_ssh(&remote).await?;
                let client = client.with_config(config);
                let source_str = source.to_str().unwrap_or("");
                let has_filters = !include.is_empty() || !exclude.is_empty();

                let transfer = if is_dir && has_filters {
                    let filter = FileFilter::new(&include, &exclude)?;
                    client.send_directory_filtered(source_str, &remote_path, filter).await?
                } else if is_dir {
                    client.send_directory(source_str, &remote_path).await?
                } else {
                    client.send_file(source_str, &remote_path).await?
                };

                transfer.wait().await?;
                Ok::<String, anyhow::Error>(dest)
            }));
        }

        let mut successes = 0;
        let mut failures = 0;
        for handle in handles {
            match handle.await {
                Ok(Ok(dest)) => {
                    reporter.info(&format!("  {} {}", style("✓").green(), dest));
                    successes += 1;
                }
                Ok(Err(e)) => {
                    reporter.info(&format!("  {} {}", style("✗").red(), e));
                    failures += 1;
                }
                Err(e) => {
                    reporter.info(&format!("  {} task panicked: {}", style("✗").red(), e));
                    failures += 1;
                }
            }
        }

        reporter.info(&format!(
            "\n  Fan-out complete: {} succeeded, {} failed",
            successes, failures
        ));
        if failures > 0 {
            anyhow::bail!("{} of {} destinations failed", failures, successes + failures);
        }
        return Ok(());
    }

    // ── Single destination ──
    // Parse destination: in daemon mode, destination is just a remote path;
    // in SSH mode, it's user@host:/path
    let (remote, remote_path) = if args.daemon.is_some() {
        (String::new(), args.destinations[0].clone())
    } else {
        parse_destination(&args.destinations[0])?
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

    let config = config
        .delta(args.delta)
        .compress(args.compress)
        .compress_level(args.compress_level)
        .build();

    // Connect
    let client = if let Some(ref daemon_addr) = args.daemon {
        reporter.info(&format!(
            "  Connecting to daemon at {}...",
            style(daemon_addr).cyan()
        ));
        Client::connect_daemon_tuned(daemon_addr, &config).await?
    } else {
        reporter.info(&format!(
            "  {} receiver via SSH...",
            style("Bootstrapping").cyan()
        ));
        Client::connect_ssh(&remote).await?
    };

    let client = client.with_config(config);

    let file_size_display = humansize::format_size(file_size, humansize::BINARY);
    reporter.info(&tuning.summary_line());
    reporter.info(&format!(
        "  Sending: {} ({})",
        style(source.display()).bold(),
        file_size_display,
    ));

    // Emit a structured transfer-start event (JSON mode only).
    let transfer_id = format!(
        "{:x}-{:x}",
        fxhash(source.to_string_lossy().as_bytes()),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    );
    reporter.emit(&JsonEvent::TransferStart {
        transfer_id,
        files: file_count,
        total_bytes: file_size,
    });

    let source_str = source
        .to_str()
        .context("source path contains invalid UTF-8")?;

    let has_filters = !args.include.is_empty() || !args.exclude.is_empty();

    let mut transfer = if is_dir && has_filters {
        let filter = FileFilter::new(&args.include, &args.exclude)
            .context("Invalid include/exclude glob pattern")?;
        client
            .send_directory_filtered(source_str, &remote_path, filter)
            .await?
    } else if is_dir {
        client.send_directory(source_str, &remote_path).await?
    } else {
        if has_filters {
            reporter.info(&format!(
                "  {} --include/--exclude flags are ignored for single-file transfers",
                style("warning:").yellow()
            ));
        }
        client.send_file(source_str, &remote_path).await?
    };

    let wall_start = Instant::now();
    reporter.info("  State: connected, planned, transferring, waiting for remote verification...");

    // Wire up progress reporting.
    if let Some(cb) = reporter.progress_callback() {
        // JSON mode: structured progress events via the callback.
        transfer.on_progress(cb);
    } else {
        // Human mode: indicatif progress bar.
        let pb = ProgressBar::new(file_size);
        pb.set_style(
            ProgressStyle::with_template(
                "  {bar:40.cyan/blue} {bytes}/{total_bytes}\n  \
                 Elapsed: {elapsed_precise} | {bytes_per_sec} | ETA: {eta}",
            )
            .expect("hard-coded progress template is always valid")
            .progress_chars("█▉▊▋▌▍▎▏ "),
        );

        let pb_clone = pb.clone();
        transfer.on_progress(move |p| {
            pb_clone.set_position(p.transferred_bytes);
        });

        transfer.wait().await?;
        pb.finish_and_clear();

        let elapsed = wall_start.elapsed().as_secs_f64();
        eprintln!(
            "\n  {} Transfer complete. BLAKE3 verified. ({:.1}s)",
            style("\u{2713}").green().bold(),
            elapsed,
        );
        return Ok(());
    }

    // JSON path: await transfer then emit completion event.
    transfer.wait().await?;

    let elapsed = wall_start.elapsed().as_secs_f64();
    let speed_mbps = if elapsed > 0.0 {
        (file_size as f64 * 8.0) / (elapsed * 1_000_000.0)
    } else {
        0.0
    };

    reporter.emit(&JsonEvent::TransferComplete {
        elapsed_secs: elapsed,
        total_bytes: file_size,
        speed_mbps,
        verified: true,
    });

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

/// Simple FNV-1a hash for generating a short transfer ID. Not
/// cryptographic, just enough to disambiguate concurrent transfers.
fn fxhash(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &b in data {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
