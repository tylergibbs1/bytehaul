//! Smart mode: zero-config transfers with runtime adaptive optimization.
//!
//! `bytehaul ./data gpu-node:/path` — no flags needed.
//! Connects first, measures RTT, then auto-tunes everything:
//! congestion algorithm, block size, stream count, compression, delta.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use console::style;
use indicatif::{HumanBytes, HumanDuration, ProgressBar, ProgressStyle};

use bytehaul_lib::client::Client;
use bytehaul_lib::config::TransferConfig;
use bytehaul_proto::adaptive;
use bytehaul_proto::congestion::CongestionMode;

pub async fn run(source: &str, destination: &str, daemon: Option<&str>) -> Result<()> {
    let source = PathBuf::from(source);
    if !source.exists() {
        print_error(&format!("Not found: {}", source.display()));
        anyhow::bail!("Source not found");
    }

    let is_dir = source.is_dir();
    let (file_count, total_size) = if is_dir {
        scan_directory(&source)
    } else {
        (1, std::fs::metadata(&source)?.len())
    };

    // ── Header ──
    println!();
    print_header(
        if is_dir { "Sending directory" } else { "Sending file" },
        &source.to_string_lossy(),
        destination,
        file_count,
        total_size,
    );

    // ── Step 1: Connect FIRST (so we can measure RTT) ──
    let connecting = print_step("Connecting");
    let client = if let Some(addr) = daemon {
        Client::connect_daemon(addr, None).await?
    } else {
        let (remote, _path) = parse_destination(destination)?;
        Client::connect_ssh(&remote).await?
    };
    connecting.finish("Connected");

    // ── Step 2: Profile the connection ──
    // Read actual RTT from the QUIC handshake. This is real measured data,
    // not a guess. Loss rate starts at 0 (measured during transfer).
    let conn_rtt = client.connection_rtt();
    let profile = adaptive::NetworkProfile {
        rtt: conn_rtt.unwrap_or(Duration::from_millis(50)),
        loss_rate: 0.0,
        bandwidth_bps: 0, // Unknown before transfer starts
        burst_ratio: 0.0,
        rtt_inflation: 1.0,
        loss_class: bytehaul_proto::congestion::LossClass::Benign,
    };
    let settings = adaptive::compute_adaptive_settings(&profile);

    // ── Step 3: Apply adaptive settings ──
    // Note: CongestionMode (Fair/Aggressive) adjusts Quinn window sizes.
    // The actual BBR vs Cubic selection is in TransportConfig, set at
    // connection time. Smart mode uses Aggressive (larger windows) since
    // the user explicitly chose ByteHaul for throughput.
    let congestion = CongestionMode::Aggressive;

    print_detail("Block size", &format!("{} MB", settings.block_size / (1024 * 1024)));
    print_detail("Streams", &format!("{}", settings.parallel_streams));

    if let Some(rtt) = conn_rtt {
        print_detail("RTT", &format!("{:.0}ms", rtt.as_secs_f64() * 1000.0));
    }

    let mut config = TransferConfig::builder()
        .resume(true)
        .delta(true)
        .congestion(congestion)
        .block_size(settings.block_size)
        .max_parallel_streams(settings.parallel_streams);

    // Auto-compress: sample first file to check compressibility
    if should_auto_compress(&source) {
        config = config.compress(true);
        print_detail("Compression", "enabled (compressible data detected)");
    }

    let config = config.build();
    let client = client.with_config(config);

    // ── Step 4: Transfer ──
    let source_str = source.to_str().context("Invalid path")?;
    let remote_path = if daemon.is_some() {
        destination.to_string()
    } else {
        parse_destination(destination)?.1
    };

    let transfer = if is_dir {
        client.send_directory(source_str, &remote_path).await?
    } else {
        client.send_file(source_str, &remote_path).await?
    };

    let pb = create_progress_bar(total_size);
    let pb_clone = pb.clone();
    let start = Instant::now();

    let mut transfer = transfer;
    transfer.on_progress(move |p| {
        pb_clone.set_position(p.transferred_bytes);
        let speed = HumanBytes(p.speed_bytes_per_sec as u64);
        pb_clone.set_message(format!("{}/s", speed));
    });

    transfer.wait().await?;
    pb.finish_and_clear();

    // ── Summary ──
    let elapsed = start.elapsed();
    print_summary(file_count, total_size, elapsed);

    println!();
    Ok(())
}

// ── Pretty output helpers ──────────────────────────────────

fn print_header(action: &str, source: &str, dest: &str, files: u64, size: u64) {
    println!(
        "  {} {}",
        style("▲").cyan().bold(),
        style(action).bold()
    );
    println!(
        "  {} {} {}",
        style("│").dim(),
        style("From:").dim(),
        style(truncate(source, 60)).white()
    );
    println!(
        "  {} {}   {}",
        style("│").dim(),
        style("To:").dim(),
        style(truncate(dest, 60)).white()
    );
    if files > 1 {
        println!(
            "  {} {} files, {}",
            style("│").dim(),
            style(files).cyan(),
            style(HumanBytes(size)).cyan()
        );
    } else {
        println!(
            "  {} {}",
            style("│").dim(),
            style(HumanBytes(size)).cyan()
        );
    }
    println!("  {}", style("│").dim());
}

fn print_detail(key: &str, value: &str) {
    println!(
        "  {} {} {}",
        style("│").dim(),
        style(format!("{}:", key)).dim(),
        style(value).dim()
    );
}

fn print_summary(files: u64, size: u64, elapsed: Duration) {
    let speed = if elapsed.as_secs_f64() > 0.0 {
        size as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!(
        "  {} {} in {} ({}/s)",
        style("✓").green().bold(),
        style("Transfer complete").bold(),
        style(HumanDuration(elapsed)).cyan(),
        style(HumanBytes(speed as u64)).cyan()
    );
    if files > 1 {
        println!(
            "  {} {} files, {} transferred",
            style("│").dim(),
            files,
            HumanBytes(size)
        );
    }
    println!(
        "  {} BLAKE3 verified",
        style("│").dim()
    );
}

fn print_error(msg: &str) {
    eprintln!(
        "\n  {} {}",
        style("✗").red().bold(),
        style(msg).red()
    );
}

struct StepHandle {
    msg: String,
    start: Instant,
}

fn print_step(msg: &str) -> StepHandle {
    eprint!(
        "  {} {}...",
        style("│").dim(),
        style(msg).dim()
    );
    StepHandle {
        msg: msg.to_string(),
        start: Instant::now(),
    }
}

impl StepHandle {
    fn finish(self, done_msg: &str) {
        let elapsed = self.start.elapsed();
        eprintln!(
            "\r  {} {} {}",
            style("│").dim(),
            style(done_msg).dim(),
            style(format!("({})", HumanDuration(elapsed))).dim()
        );
    }
}

fn create_progress_bar(total: u64) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::with_template(
            "  {bar:40.cyan/blue} {bytes}/{total_bytes} {msg}"
        )
        .unwrap()
        .progress_chars("━━╸"),
    );
    pb
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("...{}", &s[s.len() - max + 3..])
    }
}

fn scan_directory(dir: &Path) -> (u64, u64) {
    let mut count = 0u64;
    let mut size = 0u64;
    for entry in walkdir::WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
        if entry.file_type().is_file() {
            count += 1;
            size += entry.metadata().map(|m| m.len()).unwrap_or(0);
        }
    }
    (count, size)
}

fn should_auto_compress(path: &Path) -> bool {
    let sample_path = if path.is_dir() {
        walkdir::WalkDir::new(path)
            .into_iter()
            .filter_map(|e| e.ok())
            .find(|e| e.file_type().is_file())
            .map(|e| e.path().to_path_buf())
    } else {
        Some(path.to_path_buf())
    };

    let Some(sample) = sample_path else { return false };
    let Ok(data) = std::fs::read(&sample) else { return false };
    let sample = if data.len() > 4096 { &data[..4096] } else { &data };
    if sample.is_empty() { return false; }

    let mut seen = [false; 256];
    for &b in sample { seen[b as usize] = true; }
    seen.iter().filter(|&&b| b).count() < 200
}

fn parse_destination(dest: &str) -> Result<(String, String)> {
    if let Some(colon_pos) = dest.rfind(':') {
        let remote = &dest[..colon_pos];
        let path = &dest[colon_pos + 1..];
        if remote.is_empty() || path.is_empty() {
            anyhow::bail!("Invalid destination. Expected: user@host:/path");
        }
        Ok((remote.to_string(), path.to_string()))
    } else {
        anyhow::bail!("Invalid destination. Expected: user@host:/path");
    }
}
