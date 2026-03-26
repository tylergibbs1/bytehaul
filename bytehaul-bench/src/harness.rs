use std::path::Path;
use std::time::Instant;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::scenarios::{self, Scenario};

#[derive(Debug, Serialize, Deserialize)]
pub struct BenchResult {
    pub scenario: String,
    pub tool: String,
    pub iterations: Vec<IterationResult>,
    pub avg_secs: f64,
    pub avg_speed_mbps: f64,
    pub min_secs: f64,
    pub max_secs: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IterationResult {
    pub iteration: u32,
    pub elapsed_secs: f64,
    pub speed_mbps: f64,
    pub success: bool,
}

/// Run a single benchmark scenario.
pub async fn run_scenario(
    scenario: &Scenario,
    destination: &str,
    iterations: u32,
    compare_tools: &[String],
    output_file: Option<&str>,
    interface: Option<&str>,
    dry_run: bool,
) -> Result<()> {
    eprintln!("━━━ Scenario: {} ━━━", scenario.name);
    eprintln!("  {}", scenario.description);
    eprintln!(
        "  Network: {} Mbps, {} ms RTT, {:.1}% loss",
        scenario.bandwidth_mbps, scenario.rtt_ms, scenario.loss_percent
    );
    eprintln!();

    // Set up network simulation if an interface is specified
    if let Some(iface) = interface {
        setup_netem(scenario, iface, dry_run).await?;
    }

    // Create test file if needed
    let test_file = create_test_file(scenario).await?;
    let mut results = Vec::new();

    // Benchmark ByteHaul
    eprintln!("  Running: bytehaul ({} iterations)", iterations);
    let result = run_tool("bytehaul", &test_file, destination, iterations, scenario.file_size_bytes).await?;
    results.push(result);

    // Benchmark comparison tools
    for tool in compare_tools {
        eprintln!("  Running: {} ({} iterations)", tool, iterations);
        let result = run_tool(tool, &test_file, destination, iterations, scenario.file_size_bytes).await?;
        results.push(result);
    }

    // Clean up netem
    if let Some(iface) = interface {
        cleanup_netem(iface, dry_run).await?;
    }

    // Print results
    print_results(&results);

    // Save to file if requested
    if let Some(output) = output_file {
        let json = serde_json::to_string_pretty(&results)?;
        tokio::fs::write(output, json).await?;
        eprintln!("\n  Results saved to: {}", output);
    }

    // Clean up test file
    if test_file.starts_with("/tmp/bytehaul-bench") {
        let _ = tokio::fs::remove_file(&test_file).await;
    }

    Ok(())
}

/// Run all benchmark scenarios.
pub async fn run_all_scenarios(
    destination: &str,
    iterations: u32,
    compare_tools: &[String],
    output_dir: &str,
    interface: Option<&str>,
    dry_run: bool,
) -> Result<()> {
    tokio::fs::create_dir_all(output_dir).await?;

    for scenario in scenarios::all_scenarios() {
        let output_file = format!("{}/{}.json", output_dir, scenario.name);
        run_scenario(&scenario, destination, iterations, compare_tools, Some(&output_file), interface, dry_run).await?;
        eprintln!();
    }

    Ok(())
}

/// Set up tc/netem network simulation for a scenario.
async fn setup_netem(scenario: &Scenario, interface: &str, dry_run: bool) -> Result<()> {
    let cmds = scenario.netem_commands(interface);

    eprintln!("  Setting up network simulation on {}:", interface);
    for cmd in &cmds {
        eprintln!("    $ {}", cmd);
        if !dry_run {
            let status = tokio::process::Command::new("sh")
                .arg("-c")
                .arg(cmd)
                .status()
                .await?;
            if !status.success() {
                eprintln!("    Warning: command exited with status {}", status);
            }
        }
    }
    eprintln!();
    Ok(())
}

/// Clean up tc/netem network simulation.
async fn cleanup_netem(interface: &str, dry_run: bool) -> Result<()> {
    let cmd = Scenario::netem_cleanup(interface);
    eprintln!("  Cleaning up network simulation:");
    eprintln!("    $ {}", cmd);
    if !dry_run {
        let _ = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&cmd)
            .status()
            .await;
    }
    Ok(())
}

async fn run_tool(
    tool: &str,
    source: &str,
    destination: &str,
    iterations: u32,
    file_size: u64,
) -> Result<BenchResult> {
    let mut iter_results = Vec::new();

    for i in 0..iterations {
        let start = Instant::now();
        let success = match tool {
            "bytehaul" => {
                tokio::process::Command::new("bytehaul")
                    .arg("send")
                    .arg(source)
                    .arg(destination)
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status()
                    .await?
                    .success()
            }
            "scp" => {
                tokio::process::Command::new("scp")
                    .arg("-q")
                    .arg(source)
                    .arg(destination)
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status()
                    .await?
                    .success()
            }
            "rsync" => {
                tokio::process::Command::new("rsync")
                    .arg("-a")
                    .arg(source)
                    .arg(destination)
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status()
                    .await?
                    .success()
            }
            _ => {
                eprintln!("    Unknown tool: {}", tool);
                false
            }
        };

        let elapsed = start.elapsed().as_secs_f64();
        let speed = if elapsed > 0.0 {
            (file_size as f64 / elapsed) / (1024.0 * 1024.0)
        } else {
            0.0
        };

        iter_results.push(IterationResult {
            iteration: i + 1,
            elapsed_secs: elapsed,
            speed_mbps: speed,
            success,
        });

        if success {
            eprintln!("    [{}/{}] {:.2}s ({:.1} MB/s)", i + 1, iterations, elapsed, speed);
        } else {
            eprintln!("    [{}/{}] FAILED", i + 1, iterations);
        }
    }

    let successful: Vec<&IterationResult> = iter_results.iter().filter(|r| r.success).collect();
    let avg_secs = if successful.is_empty() {
        0.0
    } else {
        successful.iter().map(|r| r.elapsed_secs).sum::<f64>() / successful.len() as f64
    };
    let avg_speed = if avg_secs > 0.0 {
        (file_size as f64 / avg_secs) / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let min_secs = successful.iter().map(|r| r.elapsed_secs).fold(f64::MAX, f64::min);
    let max_secs = successful.iter().map(|r| r.elapsed_secs).fold(0.0_f64, f64::max);

    Ok(BenchResult {
        scenario: String::new(),
        tool: tool.to_string(),
        iterations: iter_results,
        avg_secs,
        avg_speed_mbps: avg_speed,
        min_secs: if min_secs == f64::MAX { 0.0 } else { min_secs },
        max_secs,
    })
}

fn print_results(results: &[BenchResult]) {
    eprintln!();
    eprintln!(
        "  {:<12} {:>10} {:>10} {:>10} {:>12}",
        "Tool", "Min", "Avg", "Max", "Avg Speed"
    );
    eprintln!("  {}", "─".repeat(58));
    for r in results {
        eprintln!(
            "  {:<12} {:>9.2}s {:>9.2}s {:>9.2}s {:>10.1} MB/s",
            r.tool, r.min_secs, r.avg_secs, r.max_secs, r.avg_speed_mbps
        );
    }
}

/// Create a test file of the appropriate size for the scenario.
async fn create_test_file(scenario: &Scenario) -> Result<String> {
    let path = format!("/tmp/bytehaul-bench-{}.dat", scenario.name);
    if Path::new(&path).exists() {
        let meta = tokio::fs::metadata(&path).await?;
        if meta.len() == scenario.file_size_bytes {
            eprintln!("  Using existing test file: {}", path);
            return Ok(path);
        }
    }

    eprintln!(
        "  Creating test file: {} ({})",
        path,
        humansize::format_size(scenario.file_size_bytes, humansize::BINARY)
    );

    // Create sparse file (fast, doesn't actually allocate disk)
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .await?;
    file.set_len(scenario.file_size_bytes).await?;

    Ok(path)
}
