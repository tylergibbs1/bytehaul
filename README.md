# ByteHaul

**Fast file transfer over QUIC. 3-4x faster than scp/rsync on cross-region links.**

---

## Quick Start

```bash
# Zero-config smart mode — just source and destination
bytehaul ./checkpoint.pt gpu-node:/data/
bytehaul ./dataset/ gpu-node:/data/

# Or use explicit commands for more control
bytehaul send --profile wan ./checkpoint.pt gpu-node:/data/
bytehaul pull gpu-node:/results/ ./local/
```

## Performance (Real AWS, Ohio to Ireland, ~75ms RTT)

Tested on c5.xlarge instances. All numbers are wall-clock time including BLAKE3 verification.

### Cross-Region Throughput

| Scenario | ByteHaul | scp | rsync | Speedup |
|---|---|---|---|---|
| **500MB checkpoint** | **107 MB/s** | 22 MB/s | 23 MB/s | **4.7x** |
| **100MB model** | **51 MB/s** | 22 MB/s | 23 MB/s | **2.2x** |
| **600 dataset shards** | **50 MB/s** | -- | -- | fixed (was broken) |

Throughput varies ~97-116 MB/s across runs due to AWS burstable networking (median 107, peak 124 MB/s over 35 runs).

## Install

```bash
cargo install bytehaul
# or build from source
cargo build --release
```

## Commands

```bash
# Smart mode — auto-detects direction, profile, compression
bytehaul ./data gpu-node:/path/

# Send with advanced options
bytehaul send -r --include "*.pt" --exclude "*.tmp" ./run/ gpu:/data/
bytehaul send --compress --delta ./checkpoint.pt remote:/models/
bytehaul send --fec-group-size 7 ./data.bin remote:/data/   # FEC for lossy links
bytehaul send ./data host1:/p host2:/p host3:/p              # parallel fan-out

# Pull from remote
bytehaul pull gpu-node:/results/run_42/ ./local/
bytehaul pull -r --profile wan gpu-node:/checkpoints/ ./local/

# Bidirectional sync
bytehaul sync ./checkpoints/ gpu-node:/mnt/checkpoints/
bytehaul sync --delta --conflict newer ./data/ remote:/data/

# Watch and auto-send on changes
bytehaul watch ./checkpoints/ --pattern "step_*" backup:/ckpts/

# Gather from multiple hosts
bytehaul gather node01:/ckpts/shard.pt node02:/ckpts/shard.pt -o ./merged/

# Daemon mode
bytehaul daemon --port 7700 --dest /data/incoming

# Utilities
bytehaul status        # Show active transfers
bytehaul clean         # GC stale state files
bytehaul init          # Create config file
bytehaul completions zsh  # Shell completions
```

## CLI Features

Every command supports `--help` with real-world examples.

| Flag | Description |
|---|---|
| `--profile <dev\|wan\|bulk\|safe>` | Preset tuning (wan = 8MB blocks, 32 streams, aggressive) |
| `--quiet / -q` | Suppress all non-error output (for scripts) |
| `--json` | Machine-readable JSON events on stdout |
| `--dry-run` | Preview what would be transferred |
| `--resume` | Resume an interrupted transfer |
| `--compress` | zstd compression for compressible data |
| `--delta` | Only send changed blocks (rsync-style) |
| `--fec-group-size N` | Forward error correction (7 = ~12.5% overhead) |

Interrupted transfers print resume instructions:

```
  ! Transfer interrupted.
  Resume with: bytehaul send --resume ./data user@host:/path
```

Transfer summaries show detailed stats:

```
  ✓ Transfer complete
    Transferred: 42 files, 1.2 GiB
    Speed:       120 MiB/s (1007 Mbps)
    Elapsed:     10.2s
    Verified:    BLAKE3 ✓
```

## How It Works

1. **QUIC transport** via Quinn -- parallel streams eliminate head-of-line blocking
2. **Tuned transport on both sides** -- BBR with 4MB initial window + 256MB flow control windows applied to both client and server (this alone accounts for a 3x throughput improvement on high-RTT links)
3. **Adaptive transport tuning** -- classifies loss, adjusts stream parallelism, and can enable parity on lossy paths
4. **8MB blocks for WAN** -- the `wan` profile uses 8MB blocks with 32 streams for optimal parallelism granularity
5. **BLAKE3 verification** -- per-chunk and whole-file integrity
6. **Resumable** -- transfer state persisted atomically, crash-safe
7. **Delta transfers** -- only send changed blocks
8. **FEC parity** -- XOR-based forward erasure correction with `--fec-group-size N` (streaming recovery during transfer)
9. **Encrypted resume state** -- `encrypt_state = true` in config encrypts state files at rest with ChaCha20-Poly1305
10. **S3/GCS storage** -- `S3Storage` backend via `s3://` URLs (feature-gated behind `s3` cargo feature)
11. **zstd compression** -- optional per-chunk compression
12. **SSH bootstrap** -- no daemon pre-install needed, auto-uploads binary

## Library API (Rust)

```rust
use bytehaul::{Client, TransferConfig};

let client = Client::connect_ssh("user@gpu-node").await?;
let config = TransferConfig::builder()
    .delta(true)
    .compress(true)
    .adaptive(true)
    .build();

let mut transfer = client
    .with_config(config)
    .send_file("./model.pt", "/data/model.pt")
    .await?;

transfer.on_progress(|p| {
    println!("{}/{} bytes, {} MB/s", p.transferred_bytes, p.total_bytes, p.speed_mbps());
});

transfer.wait().await?;
```

## Python SDK

```bash
pip install bytehaul
```

```python
from bytehaul import Client

client = Client.connect_ssh("gpu-node")
client.send("./checkpoint.pt", "/data/checkpoint.pt", delta=True, compress=True,
            fec_group_size=7, encrypt_state=True)
```

## Configuration

```bash
bytehaul init  # Creates ~/.bytehaul/config.toml
```

```toml
[transfer]
block_size_mb = 16
parallel_streams = 16
congestion = "aggressive"
resume = true
delta = false
compress = false
adaptive = true
encrypt_state = false   # ChaCha20-Poly1305 encryption of resume state files
fec_group_size = 0      # 0 = disabled, 7 = ~12.5% overhead, 15 = ~6%

[daemon]
port = 7700
bind = "0.0.0.0"
```

## Architecture

| Crate | Purpose |
|---|---|
| `bytehaul-proto` | Core protocol: QUIC transport, chunking, verification, resume, delta, adaptive FEC, compression |
| `bytehaul-lib` | Public Rust API: Client, Server, TransferConfig |
| `bytehaul-cli` | CLI: send, pull, sync, watch, gather, daemon, status, clean, init |
| `bytehaul-bench` | Benchmark infrastructure with tc/netem scenarios |
| `bytehaul-python` | Python SDK via PyO3 |

## Deployment

### Kubernetes

```yaml
initContainers:
- name: stage-data
  image: bytehaul/bytehaul:latest
  env:
  - name: BYTEHAUL_SOURCE
    value: "data-node:/datasets/imagenet/"
  - name: BYTEHAUL_DEST
    value: "/data/"
```

### Slurm

```bash
#SBATCH --prolog=/path/to/bytehaul-slurm-stage
export BYTEHAUL_STAGE_IN="storage:/datasets/train/ /local/data/"
```

## Benchmark Methodology

All benchmarks use real AWS infrastructure (not simulated). Standard test setup:

- **Hardware**: c5.xlarge (4 vCPU, 8GB RAM, up to 10 Gbps ENA)
- **Regions**: us-east-2 (Ohio) to eu-west-1 (Ireland), ~75ms RTT
- **Measurement**: Wall-clock time from CLI start to verified exit (includes SSH overhead to remote sender)
- **Verification**: BLAKE3 hash match on every transfer
- **Iterations**: 3 runs per configuration, median reported

AWS burstable networking introduces ~15% run-to-run variance. Optimizations are validated over 35+ runs to distinguish signal from noise.

### Autoresearch findings (Mar 2026)

29 experiments, 50 benchmark runs. Key finding: the QUIC client (sender) was using default Quinn transport settings while only the server had tuned BBR/flow-control. Applying the same transport config to both sides yielded a **3.1x median throughput improvement** (34.85 -> 107 MB/s). See `autoresearch/` for full experiment history.

## Protocol Notes

- Profiles in the CLI enable adaptive transfer behavior by default.
- Manifest-based transfers can emit `FecParity` control messages containing XOR parity for a small batch of chunk indices.
- **Streaming FEC recovery**: the receiver attempts to recover missing chunks inline as parity groups arrive during transfer, not just after all data is received.
- Use `--fec-group-size N` on the CLI (or `fec_group_size` in config) to enable. Recommended: 7 (~12.5% overhead) for lossy links, 15 (~6%) for mildly lossy.
- S3/GCS storage backends are available behind the `s3` cargo feature: `cargo build --features s3`. Use `s3://bucket/key` URLs.
- Resume state encryption uses ChaCha20-Poly1305 with a key derived from the transfer ID via BLAKE3 KDF. Enable with `encrypt_state = true`.

## License

Apache 2.0

## Contributing

```bash
cargo test --workspace --exclude bytehaul-python
RUST_LOG=debug bytehaul send ./test remote:/path   # Debug logging
bytehaul send --dry-run ./data remote:/path         # Preview without transferring
bytehaul -v send ./data remote:/path                # Verbose output
```
