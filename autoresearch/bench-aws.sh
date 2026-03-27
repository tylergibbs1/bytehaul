#!/usr/bin/env bash
set -euo pipefail

# ═══════════════════════════════════════════════════════════════
# ByteHaul Autoresearch AWS Benchmark
# ═══════════════════════════════════════════════════════════════
#
# Runs benchmarks over a real cross-region link:
#   us-east-2 (Ohio) → eu-west-1 (Ireland), ~85ms RTT
#
# Uses existing instances bh-fix-s and bh-fix-r.
#
# Usage: bash autoresearch/bench-aws.sh [--baseline] [--upload]
#   --baseline: Record as experiment 0 in results-aws.tsv
#   --upload:   Build and upload new binary before benchmarking

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
IPA="18.219.75.21"
IPB="34.248.38.233"
KEY="/tmp/bh-fix-20193.pem"
SS="ssh -i $KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10"
SC="scp -i $KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
TSV_FILE="$PROJECT_DIR/autoresearch/results-aws.tsv"
PORT=7700
ITERATIONS=3

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

median() {
    python3 -c "
import sys
vals = sorted([float(x) for x in sys.argv[1:]])
n = len(vals)
if n == 0: print(0)
elif n % 2 == 1: print(vals[n//2])
else: print((vals[n//2-1] + vals[n//2]) / 2)
" "$@"
}

# ─── Upload binary if requested ───

if [[ "${1:-}" == "--upload" ]] || [[ "${2:-}" == "--upload" ]]; then
    log "Cross-compiling for x86_64-linux..."
    cd "$PROJECT_DIR"

    # Build for Linux (requires cross-compilation toolchain or Docker)
    # Try cargo build with target, fall back to building on the instance
    if command -v cross &>/dev/null; then
        cross build --release --target x86_64-unknown-linux-gnu --bin bytehaul -q 2>&1
        BINARY="$PROJECT_DIR/target/x86_64-unknown-linux-gnu/release/bytehaul"
    else
        log "No cross-compilation available, building on remote..."
        tar czf /tmp/bytehaul-autoresearch-src.tar.gz --exclude='target' --exclude='.git' --exclude='bench-docker' --exclude='experiment-results*' --exclude='autoresearch/results*' .

        # Kill daemons first so the binary isn't busy
        $SS ec2-user@$IPA "sudo pkill -f 'bytehaul daemon' 2>/dev/null; true" || true
        $SS ec2-user@$IPB "sudo pkill -f 'bytehaul daemon' 2>/dev/null; true" || true
        sleep 1

        for ip in $IPA $IPB; do
            log "  Uploading source to $ip..."
            $SC /tmp/bytehaul-autoresearch-src.tar.gz ec2-user@$ip:/tmp/bytehaul-autoresearch-src.tar.gz
            $SS ec2-user@$ip "source ~/.cargo/env; mkdir -p /tmp/bytehaul && cd /tmp/bytehaul && tar xzf /tmp/bytehaul-autoresearch-src.tar.gz && cargo build --release --bin bytehaul -q 2>&1 | tail -3 && sudo cp target/release/bytehaul /usr/local/bin/bytehaul && echo BUILD_OK" &
        done
        wait
        log "Build complete on both instances."
        BINARY=""
    fi

    if [ -n "${BINARY:-}" ] && [ -f "${BINARY:-}" ]; then
        for ip in $IPA $IPB; do
            log "  Uploading binary to $ip..."
            $SC "$BINARY" ec2-user@$ip:/tmp/bytehaul-new
            $SS ec2-user@$ip "chmod +x /tmp/bytehaul-new && sudo mv /tmp/bytehaul-new /usr/local/bin/bytehaul"
        done
    fi

    log "Binary deployed."
fi

# ─── Check connectivity ───

log "Checking connectivity..."
$SS ec2-user@$IPA "bytehaul --version" 2>&1 | head -1
RTT=$($SS ec2-user@$IPA "ping -c 3 -q $IPB 2>/dev/null | tail -1 | awk -F/ '{print \$5}'" 2>/dev/null || echo "?")
log "RTT to receiver: ${RTT}ms"

# ─── Create test data (on sender) ───

log "Creating test data on sender..."
$SS ec2-user@$IPA "
mkdir -p /tmp/bh-bench/dir-600
[ -f /tmp/bh-bench/500mb.bin ] || dd if=/dev/urandom of=/tmp/bh-bench/500mb.bin bs=1M count=500 2>/dev/null
[ -f /tmp/bh-bench/100mb.bin ] || dd if=/dev/urandom of=/tmp/bh-bench/100mb.bin bs=1M count=100 2>/dev/null
if [ ! -d /tmp/bh-bench/dir-600/shard_1.tar ]; then
    for i in \$(seq 1 600); do dd if=/dev/urandom of=/tmp/bh-bench/dir-600/shard_\$i.tar bs=200K count=1 2>/dev/null; done
fi
echo 'ready'
"

# ─── Benchmark runner ───

run_bench() {
    local label="$1"
    local source="$2"
    local flags="$3"
    local size_mb="$4"
    local times=()

    for i in $(seq 1 $ITERATIONS); do
        # Clean receiver (files AND resume state)
        $SS ec2-user@$IPB "rm -rf /tmp/bh-recv && mkdir -p /tmp/bh-recv && rm -rf ~/.bytehaul/state/*" 2>/dev/null

        # Kill any lingering daemon, then start fresh
        $SS ec2-user@$IPB "pkill -f 'bytehaul daemon' 2>/dev/null || true" 2>/dev/null
        sleep 0.5
        $SS ec2-user@$IPB "nohup bytehaul daemon --port $PORT --dest /tmp/bh-recv --overwrite overwrite >/dev/null 2>&1 &" 2>/dev/null
        sleep 1

        # Transfer from sender to receiver
        local start end elapsed speed
        start=$(python3 -c 'import time; print(time.time())')
        if $SS ec2-user@$IPA "RUST_LOG=error bytehaul send --daemon $IPB:$PORT $flags $source /data" >/dev/null 2>&1; then
            end=$(python3 -c 'import time; print(time.time())')
            elapsed=$(python3 -c "print($end - $start)")
            speed=$(python3 -c "print(round($size_mb / $elapsed, 2))")
            times+=("$speed")
            log "  $label [$i/$ITERATIONS]: ${elapsed}s (${speed} MB/s)"
        else
            log "  $label [$i/$ITERATIONS]: FAILED"
            times+=("0")
        fi

        # Stop daemon
        $SS ec2-user@$IPB "pkill -f 'bytehaul daemon' 2>/dev/null || true" 2>/dev/null
        sleep 0.5
    done

    median "${times[@]}"
}

# ─── Main ───

log "Starting AWS benchmark (Ohio -> Ireland)..."
echo ""

# Test 1: 500 MB single file
log "═══ Test 1: 500 MB single file ═══"
T_500=$(run_bench "500mb" "/tmp/bh-bench/500mb.bin" "--profile wan" 500)

# Test 2: 100 MB single file
log "═══ Test 2: 100 MB single file ═══"
T_100=$(run_bench "100mb" "/tmp/bh-bench/100mb.bin" "--profile wan" 100)

# Test 3: 600 files directory
log "═══ Test 3: 600 x 200 KB files ═══"
T_DIR=$(run_bench "dir-600" "/tmp/bh-bench/dir-600" "-r --profile wan" 117)

echo ""
log "═══ AWS Results (${RTT}ms RTT) ═══"
log "  500 MB throughput: ${T_500} MB/s"
log "  100 MB throughput: ${T_100} MB/s"
log "  Dir throughput:    ${T_DIR} MB/s"

# Output JSON
cat <<EOF
{
  "throughput_500mb": $T_500,
  "throughput_100mb": $T_100,
  "throughput_dir": $T_DIR,
  "rtt_ms": "$RTT",
  "timestamp": "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
}
EOF

# ─── Baseline recording ───

if [[ "${1:-}" == "--baseline" ]] || [[ "${2:-}" == "--baseline" ]]; then
    if [ ! -f "$TSV_FILE" ]; then
        echo -e "experiment\tname\tdescription\tthroughput_500mb\tthroughput_100mb\tthroughput_dir\trtt_ms\tkept\ttimestamp" > "$TSV_FILE"
    fi
    echo -e "0\tbaseline\tBaseline on AWS (Ohio->Ireland, ${RTT}ms RTT)\t${T_500}\t${T_100}\t${T_DIR}\t${RTT}\tyes\t$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >> "$TSV_FILE"
    log "Baseline recorded in results-aws.tsv"
fi
