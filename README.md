# ByteHaul

**QUIC-based fast file transfer protocol for high bandwidth-delay product networks.**

<!-- Badges: uncomment and update when CI is configured
[![CI](https://github.com/bytehaul/bytehaul/actions/workflows/ci.yml/badge.svg)](https://github.com/bytehaul/bytehaul/actions)
[![Crates.io](https://img.shields.io/crates/v/bytehaul.svg)](https://crates.io/crates/bytehaul)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
-->

---

## The Problem

TCP was designed for reliability, not throughput on long fat pipes. On links with high bandwidth-delay product (BDP) -- cross-region, intercontinental, or even metro links with modest latency -- single-stream TCP transfers leave the majority of available bandwidth unused. Window scaling helps, but head-of-line blocking, slow congestion recovery, and single-stream limitations remain.

Commercial WAN acceleration products (Aspera FASP, Signiant, FileCatalyst) solve this, but license costs range from $10,000 to $100,000+ per year. There is no open-source, developer-friendly alternative that is easy to deploy and integrates into existing workflows.

ByteHaul fills that gap.

## How It Works

ByteHaul uses QUIC (via the [Quinn](https://github.com/quinn-rs/quinn) library) as its transport layer, taking advantage of multiplexed streams, zero-RTT connection establishment, and built-in encryption (TLS 1.3).

**Transfer pipeline:**

1. **SSH bootstrap** -- The sender SSHs into the remote host, starts an ephemeral ByteHaul receiver, and reads back the QUIC address. No manual daemon setup required.
2. **Manifest exchange** -- The sender computes a file manifest with fixed-block chunking (default 4 MB blocks) and sends it to the receiver.
3. **Parallel streaming** -- File blocks are sent across multiple concurrent QUIC streams (default 16), saturating available bandwidth regardless of RTT.
4. **BLAKE3 verification** -- Each block is hashed with BLAKE3. The receiver verifies integrity on arrival. No post-transfer checksum pass needed.
5. **Resumable transfers** -- Transfer state (which blocks have been acknowledged) is persisted. Interrupted transfers pick up where they left off.

## Quick Start

### Prerequisites

- Rust 1.70+ (2021 edition)
- SSH access to the remote host (for SSH-bootstrapped transfers)
- `bytehaul` binary installed on the remote host (for SSH bootstrap mode)

### Build

```bash
cargo build --release
```

The binary is at `target/release/bytehaul`.

### Send a File

```bash
bytehaul send ./data.bin user@remote:/path/
```

ByteHaul will SSH into `user@remote`, start an ephemeral receiver, transfer the file over QUIC, verify integrity, and exit.

### Start a Persistent Daemon

```bash
bytehaul daemon --port 7700
```

Then send to it directly:

```bash
bytehaul send --daemon 10.0.0.5:7700 ./data.bin user@remote:/path/
```

### Resume an Interrupted Transfer

```bash
bytehaul send --resume ./large.bin user@remote:/path/
```

## CLI Reference

### `bytehaul send`

Send a file or directory to a remote host.

```
bytehaul send [OPTIONS] <SOURCE> <DESTINATION>
```

| Flag | Default | Description |
|------|---------|-------------|
| `--resume` | off | Resume a previously interrupted transfer |
| `--aggressive` | off | Use aggressive congestion control (saturate link) |
| `--max-rate <RATE>` | unlimited | Bandwidth cap (e.g., `500mbps`, `1gbps`) |
| `--block-size <MB>` | `4` | Block size in megabytes |
| `--parallel <N>` | `16` | Number of parallel QUIC streams |
| `--daemon <ADDR>` | none | Connect to a running daemon instead of SSH bootstrap |
| `-v, --verbose` | off | Enable debug logging |

Destination format: `user@host:/path` or `host:/path`.

### `bytehaul daemon`

Start a receiver daemon that accepts incoming transfers.

```
bytehaul daemon [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `-p, --port <PORT>` | `7700` | Port to listen on |
| `-b, --bind <ADDR>` | `0.0.0.0` | Bind address |
| `-d, --dest <DIR>` | `.` | Directory to write received files to |
| `-v, --verbose` | off | Enable debug logging |

### `bytehaul bench`

Run transfer benchmarks comparing ByteHaul against other tools.

```
bytehaul bench [OPTIONS] <SOURCE> <DESTINATION>
```

| Flag | Default | Description |
|------|---------|-------------|
| `--compare <TOOLS>` | none | Comma-separated list of tools to compare (`scp`, `rsync`, `rclone`) |
| `--iterations <N>` | `3` | Number of iterations per tool |
| `--skip-self` | off | Only benchmark comparison tools, skip ByteHaul |

## Library API

ByteHaul can be used as a Rust library via the `bytehaul-lib` crate.

```rust
use bytehaul_lib::client::Client;
use bytehaul_lib::config::TransferConfig;
use bytehaul_proto::congestion::CongestionMode;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Build transfer configuration
    let config = TransferConfig::builder()
        .resume(true)
        .max_parallel_streams(32)
        .block_size_mb(8)
        .congestion(CongestionMode::Aggressive)
        .max_bandwidth_mbps(5000)
        .build();

    // Connect via SSH bootstrap
    let client = Client::connect_ssh("user@remote-host")
        .await?
        .with_config(config);

    // Or connect to a running daemon
    // let client = Client::connect_daemon("10.0.0.5:7700", None)
    //     .await?
    //     .with_config(config);

    // Prepare and execute transfer
    let mut transfer = client.send_file(
        "/data/large-dataset.tar",
        "/mnt/storage/large-dataset.tar",
    ).await?;

    // Optional: observe progress
    transfer.on_progress(|p| {
        let pct = (p.transferred_bytes as f64 / p.total_bytes as f64) * 100.0;
        eprintln!("{:.1}% ({} / {})", pct, p.transferred_bytes, p.total_bytes);
    });

    transfer.wait().await?;
    Ok(())
}
```

## Benchmark Methodology

ByteHaul includes a structured benchmark framework for reproducible performance comparisons. The methodology is designed to isolate transfer protocol performance from confounding variables.

### Hardware

- Two dedicated machines (bare metal or VMs with dedicated NICs)
- NVMe storage on both ends to eliminate disk bottleneck
- Minimum 10 Gbps NIC for LAN baseline scenario

### Network Emulation

Network conditions are simulated using Linux `tc` and `netem`:

```bash
# Example: simulate cross-region link (1 Gbps, 50ms RTT)
sudo tc qdisc add dev eth0 root netem delay 25ms
sudo tc qdisc add dev eth0 parent 1:1 handle 10: tbf rate 1000000kbit burst 125000k latency 50ms
```

Each scenario sets bandwidth, RTT, and packet loss independently. Tests are run with `tc` applied symmetrically on the sender interface. Cleanup between scenarios:

```bash
sudo tc qdisc del dev eth0 root
```

### File Profiles

- Large single file: generated with `dd if=/dev/urandom` (incompressible random data)
- Many small files: 10,000 x 1 MB files for per-file overhead testing

### Comparison Tools

| Tool | Command |
|------|---------|
| `scp` | `scp -q <file> <dest>` |
| `rsync` | `rsync -az --progress <file> <dest>` |
| `rclone` | `rclone copy <file> <dest>` |
| ByteHaul | `bytehaul send <file> <dest>` |

### Measurement

- Each tool is run N iterations (default 3) per scenario
- Wall-clock time measured per run
- Throughput calculated as `file_size / elapsed_time`
- Results report min, avg, max time and average throughput (MB/s)
- Caches and buffers cleared between runs

### Standard Scenarios

| Scenario | Bandwidth | RTT | Loss | File | Purpose |
|----------|-----------|-----|------|------|---------|
| LAN baseline | 10 Gbps | 1 ms | 0% | 1 x 10 GB | Ceiling test (CPU/IO bound) |
| Metro link | 1 Gbps | 10 ms | 0% | 1 x 10 GB | Moderate BDP |
| Cross-region | 1 Gbps | 50 ms | 0% | 1 x 10 GB | High BDP -- primary win scenario |
| Intercontinental | 1 Gbps | 150 ms | 0.1% | 1 x 10 GB | High BDP + packet loss |
| Degraded link | 100 Mbps | 200 ms | 2% | 1 x 1 GB | Stress test for loss handling |
| Many small files | 1 Gbps | 50 ms | 0% | 10,000 x 1 MB | Per-file overhead test |

Run all scenarios:

```bash
bytehaul-bench run-all user@remote:/tmp/bench --compare scp,rsync --iterations 5 --output-dir ./bench-results
```

## Results

### Real-world: AWS cross-region (Ohio to Ireland)

Tested on **c5.xlarge** instances (4 vCPU, 8 GB RAM, up to 10 Gbps ENA networking).
Real measured RTT: ~85 ms. No traffic shaping or simulation.

| File size | ByteHaul | scp | rsync | Speedup vs scp |
|-----------|----------|-----|-------|----------------|
| 100 MB | **38.9 MB/s** | 16.2 MB/s | 16.6 MB/s | **2.4x** |
| 1 GB | **42.0 MB/s** | 23.7 MB/s | -- | **1.8x** |

### Simulated: Docker + tc/netem (100 MB file)

Tested on Docker containers with `tc netem` controlling RTT, loss, and bandwidth.
Isolates protocol performance from cloud-specific network optimizations.

| Scenario | RTT | Loss | ByteHaul | scp | Speedup |
|----------|-----|------|----------|-----|---------|
| Baseline | <1 ms | 0% | ~295 MB/s | ~295 MB/s | 1x |
| Cross-region | 50 ms | 0% | **434 MB/s** | 6.3 MB/s | **69x** |
| Intercontinental | 150 ms | 0.1% | **229 MB/s** | 0.7 MB/s | **327x** |
| Degraded link | 200 ms | 2% | **184 MB/s** | <0.5 MB/s | **>300x** |

**Interpretation:** On low-latency links, ByteHaul matches scp (no overhead penalty). As RTT and loss increase, TCP's congestion control collapses while ByteHaul's parallel QUIC streams maintain throughput. The simulated numbers are more dramatic than the AWS numbers because AWS inter-region links have highly optimized TCP stacks with large buffers. On consumer or satellite links, the real-world speedup would be closer to the simulated results.

## Architecture

ByteHaul is organized as a Rust workspace with four crates:

### `bytehaul-proto`

Core protocol implementation. Contains all transport-level logic with no CLI or high-level API dependencies.

| Module | Responsibility |
|--------|---------------|
| `transport` | QUIC connection setup via Quinn, TLS configuration |
| `engine` | Transfer orchestration -- chunking, streaming, progress tracking |
| `chunking` | Fixed-block file chunking strategy |
| `verify` | BLAKE3 block-level integrity verification |
| `manifest` | File metadata and block map serialization |
| `resume` | Transfer state persistence and recovery |
| `congestion` | Congestion control mode selection (Fair / Aggressive) |
| `wire` | On-the-wire message encoding (bincode) |

### `bytehaul-lib`

High-level Rust API for embedding ByteHaul in applications.

| Module | Responsibility |
|--------|---------------|
| `client` | `Client` struct -- SSH bootstrap, daemon connection, file sending |
| `server` | `Server` struct -- daemon binding, transfer receiving |
| `config` | `TransferConfig` and its builder |

### `bytehaul-cli`

Command-line interface built with Clap. Provides `send`, `daemon`, and `bench` subcommands with progress bars (indicatif) and colored output (console).

### `bytehaul-bench`

Standalone benchmark harness for structured, reproducible performance testing.

| Module | Responsibility |
|--------|---------------|
| `scenarios` | Standard scenario definitions and `tc`/`netem` command generation |
| `harness` | Test execution, timing, and multi-tool orchestration |
| `report` | Result aggregation and output (text, JSON, Markdown) |

## Roadmap

### v0.1 (current)

- Single-file transfers over QUIC with parallel streams
- SSH-bootstrapped receiver
- BLAKE3 block verification
- Resumable transfers
- CLI with progress display
- Benchmark framework with 6 standard scenarios

### v0.2

- Directory and multi-file transfers
- Delta transfers (only send changed blocks)
- Encryption at rest for transfer state
- Configuration file support
- Package distribution (Homebrew, cargo install, deb/rpm)

### v0.3

- Adaptive stream count based on measured BDP
- Bandwidth scheduling and fairness policies
- Web dashboard for daemon monitoring
- S3/GCS source and destination support

## License

ByteHaul is licensed under the [Apache License 2.0](LICENSE).

## Contributing

Contributions are welcome. Please open an issue to discuss non-trivial changes before submitting a pull request.

To run the test suite:

```bash
cargo test --workspace
```

To run with debug logging:

```bash
RUST_LOG=debug cargo run -- send ./test.bin user@host:/tmp/
```
