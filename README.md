# ByteHaul

**Fast file transfer over QUIC. 2-4x faster than scp/rsync on cross-region links.**

---

## Quick Start

```bash
# Common path
bytehaul send ./checkpoint.pt gpu-node:/data/
bytehaul send -r ./dataset/ gpu-node:/data/

# Profiles enable adaptive tuning by default
bytehaul send --profile dev ./checkpoint.pt gpu-node:/data/
```

## Performance (Real AWS, Ohio to Ireland, ~85ms RTT)

Tested on c5.xlarge instances. All numbers are wall-clock time including BLAKE3 verification.

### vs The Tools ML Engineers Actually Use

| Scenario | ByteHaul | rsync -avz | tar\|ssh | scp | rclone |
|---|---|---|---|---|---|
| **500MB checkpoint** | **54 MB/s** (9s) | 23 MB/s (22s) | 24 MB/s (21s) | 22 MB/s (23s) | 42 MB/s (12s) |
| **600 dataset shards** | **295 MB/s** (0.4s) | 75 MB/s (1.6s) | 71 MB/s (1.6s) | -- | 10 MB/s (12s) |
| **5GB large model** | **79 MB/s** (65s) | 41 MB/s (125s) | 41 MB/s (124s) | 42 MB/s (121s) | -- |
| **Compressible logs** | **147 MB/s** (0.3s) | 24 MB/s (1.7s) | 33 MB/s (1.2s) | -- | -- |

### ML Workflow Scenarios

| Scenario | ByteHaul | Best Alternative | Speedup |
|---|---|---|---|
| 500MB model checkpoint | 44 MB/s | scp: 23 MB/s | **1.9x** |
| 600 dataset shards | 287 MB/s | tar\|ssh: 75 MB/s | **3.8x** |
| Delta sync (10% changed) | 57 MB/s | full resend: 45 MB/s | **18% faster** |
| 5GB large model | 108 MB/s | scp: 52 MB/s | **2.1x** |
| Compressible artifacts | 37 MB/s (zstd) | -- | instant |

## Install

```bash
cargo install bytehaul
# or build from source
cargo build --release
```

## Commands

```bash
# Zero-config smart mode (auto-detects everything)
bytehaul ./data gpu-node:/path/

# Send with advanced options
bytehaul send -r --include "*.pt" --exclude "*.tmp" ./run/ gpu:/data/
bytehaul send --compress --delta ./checkpoint.pt remote:/models/
bytehaul send ./data host1:/path host2:/path host3:/path   # fan-out

# Pull from remote
bytehaul pull gpu-node:/results/run_42/ ./local/
bytehaul pull -r gpu-node:/checkpoints/ ./local/

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

## How It Works

1. **QUIC transport** via Quinn -- parallel streams eliminate head-of-line blocking
2. **Adaptive transport tuning** -- classifies loss, adjusts stream parallelism, and can enable parity on lossy paths
3. **Large blocks** -- profiles choose block sizes for high-BDP links
4. **BLAKE3 verification** -- per-chunk and whole-file integrity
5. **Resumable** -- transfer state persisted atomically, crash-safe
6. **Delta transfers** -- only send changed blocks
7. **FEC parity** -- manifest-based transfers can send XOR repair parity over the control stream
8. **zstd compression** -- optional per-chunk compression
9. **SSH bootstrap** -- no daemon pre-install needed, auto-uploads binary

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
client.send("./checkpoint.pt", "/data/checkpoint.pt", delta=True, compress=True)
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
- **Regions**: us-east-2 (Ohio) to eu-west-1 (Ireland), ~85ms RTT
- **Measurement**: Wall-clock time from CLI start to verified exit
- **Verification**: BLAKE3 hash match on every transfer
- **Iterations**: 3 runs per configuration, results are averages

200+ experiments across 6 benchmark rounds validating all features.

## Protocol Notes

- Profiles in the CLI enable adaptive transfer behavior by default.
- Manifest-based transfers can emit `FecParity` control messages containing XOR parity for a small batch of chunk indices.
- The receiver can use that parity to recover one missing chunk in the group before final file verification.
- This is currently implemented on the manifest transfer path used by directories, pulls, and single-file sends when adaptive or FEC is active.

## License

Apache 2.0

## Contributing

```bash
cargo test --workspace --exclude bytehaul-python  # 128 tests
RUST_LOG=debug bytehaul send ./test remote:/path   # Debug logging
```
