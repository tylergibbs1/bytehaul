
use anyhow::Result;

use crate::harness::BenchResult;

/// Generate a report from benchmark results.
pub fn generate_report(results_dir: &str, format: &str) -> Result<()> {
    let results = load_results(results_dir)?;

    match format {
        "text" => print_text_report(&results),
        "json" => print_json_report(&results)?,
        "markdown" => print_markdown_report(&results),
        _ => anyhow::bail!("Unknown format: {}. Use: text, json, markdown", format),
    }

    Ok(())
}

fn load_results(dir: &str) -> Result<Vec<(String, Vec<BenchResult>)>> {
    let mut all_results = Vec::new();

    let entries = std::fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map(|e| e == "json").unwrap_or(false) {
            let name = path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            let content = std::fs::read_to_string(&path)?;
            let results: Vec<BenchResult> = serde_json::from_str(&content)?;
            all_results.push((name, results));
        }
    }

    all_results.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(all_results)
}

fn print_text_report(results: &[(String, Vec<BenchResult>)]) {
    println!("ByteHaul Benchmark Report");
    println!("========================\n");

    for (scenario, bench_results) in results {
        println!("Scenario: {}", scenario);
        println!(
            "  {:<12} {:>10} {:>10} {:>10} {:>12}",
            "Tool", "Min", "Avg", "Max", "Avg Speed"
        );
        println!("  {}", "─".repeat(58));
        for r in bench_results {
            println!(
                "  {:<12} {:>9.2}s {:>9.2}s {:>9.2}s {:>10.1} MB/s",
                r.tool, r.min_secs, r.avg_secs, r.max_secs, r.avg_speed_mbps
            );
        }
        println!();
    }
}

fn print_json_report(results: &[(String, Vec<BenchResult>)]) -> Result<()> {
    let json = serde_json::to_string_pretty(results)?;
    println!("{}", json);
    Ok(())
}

fn print_markdown_report(results: &[(String, Vec<BenchResult>)]) {
    println!("# ByteHaul Benchmark Report\n");

    for (scenario, bench_results) in results {
        println!("## {}\n", scenario);
        println!("| Tool | Min | Avg | Max | Avg Speed |");
        println!("|------|-----|-----|-----|-----------|");
        for r in bench_results {
            println!(
                "| {} | {:.2}s | {:.2}s | {:.2}s | {:.1} MB/s |",
                r.tool, r.min_secs, r.avg_secs, r.max_secs, r.avg_speed_mbps
            );
        }
        println!();
    }
}
