use std::collections::HashSet;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Args;
use console::style;
use notify::RecursiveMode;
use notify_debouncer_mini::new_debouncer;
use tokio::sync::mpsc;

use bytehaul_lib::client::Client;
use bytehaul_lib::config::TransferConfig;
use bytehaul_proto::congestion::CongestionMode;

/// Parse a human-readable duration string like "500ms", "2s", "1m" into a `Duration`.
fn parse_debounce(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if let Some(ms) = s.strip_suffix("ms") {
        let n: u64 = ms.parse().map_err(|_| format!("invalid debounce value: {s}"))?;
        Ok(Duration::from_millis(n))
    } else if let Some(secs) = s.strip_suffix('s') {
        let n: f64 = secs.parse().map_err(|_| format!("invalid debounce value: {s}"))?;
        Ok(Duration::from_secs_f64(n))
    } else if let Some(mins) = s.strip_suffix('m') {
        let n: f64 = mins.parse().map_err(|_| format!("invalid debounce value: {s}"))?;
        Ok(Duration::from_secs_f64(n * 60.0))
    } else {
        // Default to milliseconds if no suffix
        let n: u64 = s.parse().map_err(|_| format!("invalid debounce value: {s}"))?;
        Ok(Duration::from_millis(n))
    }
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

#[derive(Args)]
pub struct WatchArgs {
    /// Local directory to watch for changes
    pub source: String,

    /// Remote destination (user@host:/path or host:/path)
    pub destination: String,

    /// Glob pattern to filter which files trigger a sync (e.g., "step_*")
    #[arg(long)]
    pub pattern: Option<String>,

    /// Debounce duration before syncing (e.g., "500ms", "2s")
    #[arg(long, default_value = "500ms", value_parser = parse_debounce)]
    pub debounce: Duration,

    /// Connect to a running daemon instead of SSH bootstrap
    #[arg(long)]
    pub daemon: Option<String>,

    /// Number of parallel streams
    #[arg(long, default_value = "16")]
    pub parallel: usize,

    /// Block size in MB
    #[arg(long, default_value = "16")]
    pub block_size: u32,

    /// Use aggressive congestion control (saturate link)
    #[arg(long)]
    pub aggressive: bool,

    /// Delta transfer: only send changed blocks
    #[arg(long)]
    pub delta: bool,
}

pub async fn run(args: WatchArgs) -> Result<()> {
    let source = PathBuf::from(&args.source);
    if !source.exists() {
        anyhow::bail!("Source directory not found: {}", source.display());
    }
    if !source.is_dir() {
        anyhow::bail!(
            "Source must be a directory: {}. Use `bytehaul send` for individual files.",
            source.display()
        );
    }

    let source = source
        .canonicalize()
        .context("Failed to resolve source directory path")?;

    // Parse destination
    let (remote, remote_path) = if args.daemon.is_some() {
        (String::new(), args.destination.clone())
    } else {
        parse_destination(&args.destination)?
    };

    // Build transfer config
    let config = TransferConfig::builder()
        .block_size_mb(args.block_size)
        .max_parallel_streams(args.parallel)
        .congestion(if args.aggressive {
            CongestionMode::Aggressive
        } else {
            CongestionMode::Fair
        })
        .delta(args.delta)
        .build();

    // Establish persistent connection
    let display_dest = if args.daemon.is_some() {
        args.daemon.as_deref().unwrap_or("daemon").to_string()
    } else {
        remote.clone()
    };

    eprintln!(
        "  {} Connecting to {}...",
        style("⟳").cyan(),
        style(&display_dest).bold()
    );

    let client = if let Some(ref daemon_addr) = args.daemon {
        Client::connect_daemon(daemon_addr, None).await?
    } else {
        Client::connect_ssh(&remote).await?
    };
    let client = client.with_config(config);

    // Compile glob pattern if provided
    let glob_pattern = match &args.pattern {
        Some(pat) => {
            let compiled = globset::GlobBuilder::new(pat)
                .literal_separator(true)
                .build()
                .with_context(|| format!("Invalid glob pattern: {pat}"))?
                .compile_matcher();
            Some(compiled)
        }
        None => None,
    };

    eprintln!(
        "  {} Watching {} for changes{}",
        style("✓").green().bold(),
        style(source.display()).bold(),
        match &args.pattern {
            Some(p) => format!(" (pattern: {})", style(p).yellow()),
            None => String::new(),
        }
    );
    eprintln!(
        "  {} debounce={:?}, delta={}, parallel={}",
        style("⟶").dim(),
        args.debounce,
        args.delta,
        args.parallel,
    );
    eprintln!(
        "  {} Press {} to stop.\n",
        style("i").cyan(),
        style("Ctrl-C").bold()
    );

    // Set up an mpsc channel to bridge the synchronous notify callback into async.
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<PathBuf>>();

    // Create the debounced file watcher.
    let debounce_duration = args.debounce;
    let mut debouncer = new_debouncer(debounce_duration, move |result: notify_debouncer_mini::DebounceEventResult| {
        match result {
            Ok(events) => {
                let paths: Vec<PathBuf> = events.into_iter().map(|e| e.path).collect();
                if !paths.is_empty() {
                    // If the receiver is dropped, we are shutting down.
                    let _ = tx.send(paths);
                }
            }
            Err(errors) => {
                {
                    let e = errors;
                    eprintln!(
                        "  {} watcher error: {}",
                        style("!").red(),
                        e
                    );
                }
            }
        }
    })
    .context("Failed to create file watcher")?;

    debouncer
        .watcher()
        .watch(source.as_ref(), RecursiveMode::Recursive)
        .context("Failed to start watching directory")?;

    // Main event loop: process batches of changed files until interrupted.
    loop {
        // Wait for the first batch of events.
        let first_batch = tokio::select! {
            batch = rx.recv() => {
                match batch {
                    Some(paths) => paths,
                    None => break, // Channel closed, watcher dropped.
                }
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\n  {} Shutting down watcher.", style("i").cyan());
                break;
            }
        };

        // Drain any additional events that arrived while we were processing.
        let mut all_paths: HashSet<PathBuf> = first_batch.into_iter().collect();
        while let Ok(more) = rx.try_recv() {
            all_paths.extend(more);
        }

        // Filter to only existing files (not directories, not deletions).
        let mut changed_files: Vec<PathBuf> = all_paths
            .into_iter()
            .filter(|p| p.is_file())
            .collect();

        // Apply glob pattern filter if set.
        if let Some(ref pat) = glob_pattern {
            changed_files.retain(|p| {
                p.file_name()
                    .map(|name| pat.is_match(name))
                    .unwrap_or(false)
            });
        }

        if changed_files.is_empty() {
            continue;
        }

        changed_files.sort();

        let file_count = changed_files.len();
        let total_size: u64 = changed_files
            .iter()
            .filter_map(|p| p.metadata().ok())
            .map(|m| m.len())
            .sum();

        let start = Instant::now();

        // Send each changed file.
        let mut errors = 0usize;
        for file_path in &changed_files {
            let rel = file_path
                .strip_prefix(&source)
                .unwrap_or(file_path);

            let dest_file = format!(
                "{}/{}",
                remote_path.trim_end_matches('/'),
                rel.to_string_lossy()
            );

            let local_str = match file_path.to_str() {
                Some(s) => s,
                None => {
                    eprintln!(
                        "  {} Skipping file with non-UTF-8 path: {:?}",
                        style("!").yellow(),
                        file_path
                    );
                    errors += 1;
                    continue;
                }
            };

            match client.send_file(local_str, &dest_file).await {
                Ok(mut transfer) => {
                    if let Err(e) = transfer.wait().await {
                        eprintln!(
                            "  {} Failed to transfer {}: {}",
                            style("✗").red(),
                            rel.display(),
                            e
                        );
                        errors += 1;
                    }
                }
                Err(e) => {
                    eprintln!(
                        "  {} Failed to start transfer for {}: {}",
                        style("✗").red(),
                        rel.display(),
                        e
                    );
                    errors += 1;
                }
            }
        }

        let elapsed = start.elapsed();
        let size_display = humansize::format_size(total_size, humansize::BINARY);
        let timestamp = chrono::Local::now().format("%H:%M:%S");

        let succeeded = file_count - errors;
        if errors == 0 {
            eprintln!(
                "  [{}] {} Synced {} file{} ({}) to {}  ({:.1}s)",
                timestamp,
                style("✓").green().bold(),
                succeeded,
                if succeeded == 1 { "" } else { "s" },
                size_display,
                style(&display_dest).bold(),
                elapsed.as_secs_f64(),
            );
        } else {
            eprintln!(
                "  [{}] {} Synced {} file{} ({}) to {}, {} failed  ({:.1}s)",
                timestamp,
                style("!").yellow().bold(),
                succeeded,
                if succeeded == 1 { "" } else { "s" },
                size_display,
                style(&display_dest).bold(),
                errors,
                elapsed.as_secs_f64(),
            );
        }
    }

    Ok(())
}
