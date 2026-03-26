use std::path::PathBuf;
use std::time::Instant;

use anyhow::Result;
use clap::Args;
use console::style;

#[derive(Args)]
pub struct BenchArgs {
    /// Test file to transfer
    pub source: String,

    /// Remote destination (user@host:/path)
    pub destination: String,

    /// Tools to compare against (comma-separated: scp,rsync)
    #[arg(long, value_delimiter = ',')]
    pub compare: Vec<String>,

    /// Number of iterations per tool
    #[arg(long, default_value = "3")]
    pub iterations: u32,

    /// Skip ByteHaul benchmark (only run comparison tools)
    #[arg(long)]
    pub skip_self: bool,
}

pub async fn run(args: BenchArgs) -> Result<()> {
    let source = PathBuf::from(&args.source);
    if !source.exists() {
        anyhow::bail!("Source file not found: {}", source.display());
    }

    let file_size = tokio::fs::metadata(&source).await?.len();
    let file_size_display = humansize::format_size(file_size, humansize::BINARY);

    eprintln!(
        "  {} ByteHaul Benchmark",
        style("▶").cyan().bold()
    );
    eprintln!("  File: {} ({})", source.display(), file_size_display);
    eprintln!("  Destination: {}", args.destination);
    eprintln!("  Iterations: {}", args.iterations);
    eprintln!();

    let mut results: Vec<BenchResult> = Vec::new();

    // Benchmark ByteHaul
    if !args.skip_self {
        eprintln!("  Benchmarking: {}", style("bytehaul").bold());
        let mut times = Vec::new();
        for i in 0..args.iterations {
            let start = Instant::now();
            // Run bytehaul send
            let status = tokio::process::Command::new("bytehaul")
                .arg("send")
                .arg(&args.source)
                .arg(&args.destination)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .await?;

            let elapsed = start.elapsed();
            if status.success() {
                times.push(elapsed.as_secs_f64());
                eprintln!(
                    "    Run {}: {:.2}s ({:.1} MB/s)",
                    i + 1,
                    elapsed.as_secs_f64(),
                    (file_size as f64 / elapsed.as_secs_f64()) / (1024.0 * 1024.0)
                );
            } else {
                eprintln!("    Run {}: FAILED", i + 1);
            }
        }
        if !times.is_empty() {
            results.push(BenchResult::from_times("bytehaul", file_size, &times));
        }
    }

    // Benchmark comparison tools
    for tool in &args.compare {
        eprintln!("  Benchmarking: {}", style(tool).bold());
        let mut times = Vec::new();

        for i in 0..args.iterations {
            let start = Instant::now();
            let status = match tool.as_str() {
                "scp" => {
                    tokio::process::Command::new("scp")
                        .arg("-q")
                        .arg(&args.source)
                        .arg(&args.destination)
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .status()
                        .await?
                }
                "rsync" => {
                    tokio::process::Command::new("rsync")
                        .arg("-az")
                        .arg("--progress")
                        .arg(&args.source)
                        .arg(&args.destination)
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .status()
                        .await?
                }
                "rclone" => {
                    // rclone needs different destination format, pass as-is
                    tokio::process::Command::new("rclone")
                        .arg("copy")
                        .arg(&args.source)
                        .arg(&args.destination)
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .status()
                        .await?
                }
                other => {
                    eprintln!("    Unknown tool: {}, skipping", other);
                    continue;
                }
            };

            let elapsed = start.elapsed();
            if status.success() {
                times.push(elapsed.as_secs_f64());
                eprintln!(
                    "    Run {}: {:.2}s ({:.1} MB/s)",
                    i + 1,
                    elapsed.as_secs_f64(),
                    (file_size as f64 / elapsed.as_secs_f64()) / (1024.0 * 1024.0)
                );
            } else {
                eprintln!("    Run {}: FAILED", i + 1);
            }
        }

        if !times.is_empty() {
            results.push(BenchResult::from_times(tool, file_size, &times));
        }
    }

    // Print summary
    eprintln!();
    eprintln!(
        "  {} Results",
        style("━━━").cyan()
    );
    eprintln!(
        "  {:<12} {:>10} {:>10} {:>10} {:>12}",
        "Tool", "Min", "Avg", "Max", "Avg Speed"
    );
    eprintln!("  {}", "─".repeat(58));
    for r in &results {
        eprintln!(
            "  {:<12} {:>9.2}s {:>9.2}s {:>9.2}s {:>10.1} MB/s",
            r.tool, r.min_secs, r.avg_secs, r.max_secs, r.avg_speed_mbps
        );
    }

    // Speedup comparison
    if results.len() >= 2 {
        if let Some(bytehaul) = results.iter().find(|r| r.tool == "bytehaul") {
            eprintln!();
            for r in &results {
                if r.tool != "bytehaul" {
                    let speedup = r.avg_secs / bytehaul.avg_secs;
                    eprintln!(
                        "  ByteHaul vs {}: {:.1}x {}",
                        r.tool,
                        speedup,
                        if speedup > 1.0 {
                            style("faster").green().to_string()
                        } else {
                            style("slower").red().to_string()
                        }
                    );
                }
            }
        }
    }

    Ok(())
}

struct BenchResult {
    tool: String,
    min_secs: f64,
    avg_secs: f64,
    max_secs: f64,
    avg_speed_mbps: f64,
}

impl BenchResult {
    fn from_times(tool: &str, file_size: u64, times: &[f64]) -> Self {
        let min = times.iter().cloned().fold(f64::MAX, f64::min);
        let max = times.iter().cloned().fold(0.0_f64, f64::max);
        let avg = times.iter().sum::<f64>() / times.len() as f64;
        let avg_speed = (file_size as f64 / avg) / (1024.0 * 1024.0);
        Self {
            tool: tool.to_string(),
            min_secs: min,
            avg_secs: avg,
            max_secs: max,
            avg_speed_mbps: avg_speed,
        }
    }
}
