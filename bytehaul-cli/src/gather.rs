use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Args;
use console::style;

use bytehaul_lib::client::Client;
use bytehaul_lib::config::TransferConfig;
use bytehaul_proto::congestion::CongestionMode;

use crate::output::Reporter;

/// Gather files from multiple remote hosts to a local directory.
///
/// Designed for collecting distributed checkpoint shards:
///   bytehaul gather node{01..08}:/checkpoints/shard_*.pt ./merged/
#[derive(Args)]
pub struct GatherArgs {
    /// Remote sources (user@host:/path). Multiple allowed.
    #[arg(num_args = 1..)]
    pub sources: Vec<String>,

    /// Local destination directory
    #[arg(long, short = 'o')]
    pub output: String,

    /// Pull recursively
    #[arg(short = 'r', long)]
    pub recursive: bool,

    /// Use aggressive congestion control
    #[arg(long)]
    pub aggressive: bool,

    /// Block size in MB
    #[arg(long, default_value = "16")]
    pub block_size: u32,

    /// Number of parallel streams per host
    #[arg(long, default_value = "16")]
    pub parallel: usize,

    /// FEC group size: generate 1 parity chunk per N data chunks (0 = disabled)
    #[arg(long)]
    pub fec_group_size: Option<usize>,

    /// Show what would be gathered without actually transferring
    #[arg(long)]
    pub dry_run: bool,
}

pub async fn run(args: GatherArgs, reporter: &Reporter) -> Result<()> {
    let output_dir = PathBuf::from(&args.output);

    if args.dry_run {
        reporter.info(&format!(
            "  Dry run: would gather from {} sources to {}",
            args.sources.len(),
            output_dir.display()
        ));
        for source in &args.sources {
            reporter.info(&format!("    {}", source));
        }
        return Ok(());
    }

    tokio::fs::create_dir_all(&output_dir).await?;

    reporter.info(&format!(
        "  {} Gathering from {} sources to {}",
        style("↓").cyan().bold(),
        args.sources.len(),
        output_dir.display()
    ));

    let start = Instant::now();
    let mut handles = Vec::new();

    for (i, source) in args.sources.iter().enumerate() {
        let source = source.clone();
        let output_dir = output_dir.clone();
        let aggressive = args.aggressive;
        let block_size = args.block_size;
        let parallel = args.parallel;
        let recursive = args.recursive;
        let fec_group_size = args.fec_group_size;

        handles.push(tokio::spawn(async move {
            let (remote, remote_path) = parse_source(&source)?;

            let mut config = TransferConfig::builder()
                .block_size_mb(block_size)
                .max_parallel_streams(parallel)
                .congestion(if aggressive {
                    CongestionMode::Aggressive
                } else {
                    CongestionMode::Fair
                });
            if let Some(fec) = fec_group_size {
                config = config.fec_group_size(fec);
            }
            let config = config.build();

            let client = Client::connect_ssh(&remote).await?;
            let client = client.with_config(config);

            // Create a subdirectory per host to avoid conflicts
            let host_dir = output_dir.join(sanitize_host(&remote));
            tokio::fs::create_dir_all(&host_dir).await?;

            let transfer = if recursive {
                client.pull_directory(&remote_path, host_dir.to_str().unwrap()).await?
            } else {
                client.pull_file(&remote_path, host_dir.to_str().unwrap()).await?
            };

            transfer.wait().await?;
            Ok::<(usize, String), anyhow::Error>((i, source))
        }));
    }

    let mut successes = 0;
    let mut failures = 0;
    for handle in handles {
        match handle.await {
            Ok(Ok((_, source))) => {
                reporter.info(&format!("  {} {}", style("✓").green(), source));
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

    let elapsed = start.elapsed();
    reporter.info(&format!(
        "\n  {} Gather complete: {}/{} succeeded in {:.1}s",
        if failures == 0 {
            style("✓").green().bold()
        } else {
            style("!").yellow().bold()
        },
        successes,
        successes + failures,
        elapsed.as_secs_f64()
    ));

    reporter.transfer_summary(
        successes as u64,
        0,
        elapsed.as_secs_f64(),
        false,
    );

    if failures > 0 {
        anyhow::bail!("{} of {} sources failed", failures, successes + failures);
    }
    Ok(())
}

fn parse_source(source: &str) -> Result<(String, String)> {
    if let Some(colon_pos) = source.rfind(':') {
        let remote = &source[..colon_pos];
        let path = &source[colon_pos + 1..];
        if remote.is_empty() || path.is_empty() {
            anyhow::bail!("Invalid source format. Expected: user@host:/path");
        }
        Ok((remote.to_string(), path.to_string()))
    } else {
        anyhow::bail!("Invalid source format. Expected: user@host:/path");
    }
}

fn sanitize_host(host: &str) -> String {
    host.replace('@', "_at_")
        .replace(':', "_")
        .replace('/', "_")
}
