#!/usr/bin/env bash
set -euo pipefail

# ═══════════════════════════════════════════════════════════════
# ByteHaul Autoresearch Benchmark Harness
# ═══════════════════════════════════════════════════════════════
#
# Runs a fixed set of benchmarks and outputs JSON results.
# Designed to complete in ~2 minutes for rapid iteration.
#
# Usage: bash autoresearch/bench.sh [--baseline]
#   --baseline: Also record the result as experiment 0 in results.tsv

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BYTEHAUL="$PROJECT_DIR/target/release/bytehaul"
BENCH_DIR="/tmp/bytehaul-autoresearch-data"
RECV_DIR="/tmp/bytehaul-autoresearch-recv"
RESULTS_FILE="$PROJECT_DIR/autoresearch/results.json"
TSV_FILE="$PROJECT_DIR/autoresearch/results.tsv"
PORT=18800
ITERATIONS=3

# ─── Helpers ───

cleanup() {
    kill "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true
}
trap cleanup EXIT
DAEMON_PID=""

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

median() {
    # Read values from args, return median
    python3 -c "
import sys
vals = sorted([float(x) for x in sys.argv[1:]])
n = len(vals)
if n == 0: print(0)
elif n % 2 == 1: print(vals[n//2])
else: print((vals[n//2-1] + vals[n//2]) / 2)
" "$@"
}

# ─── Build check ───

if [ ! -f "$BYTEHAUL" ]; then
    log "ERROR: Release binary not found. Run: cargo build --release"
    exit 1
fi

# ─── Create test data (once) ───

create_test_data() {
    if [ -f "$BENCH_DIR/.ready" ]; then
        return
    fi
    log "Creating test data..."
    rm -rf "$BENCH_DIR"
    mkdir -p "$BENCH_DIR/dir-600"

    # 500 MB file (incompressible, like model weights)
    dd if=/dev/urandom of="$BENCH_DIR/500mb.bin" bs=1048576 count=500 2>/dev/null

    # 100 MB file
    dd if=/dev/urandom of="$BENCH_DIR/100mb.bin" bs=1048576 count=100 2>/dev/null

    # 600 x 200 KB files (like dataset shards)
    for i in $(seq 1 600); do
        dd if=/dev/urandom of="$BENCH_DIR/dir-600/shard_$i.tar" bs=204800 count=1 2>/dev/null
    done

    touch "$BENCH_DIR/.ready"
    log "Test data created."
}

# ─── Benchmark runner ───

run_bench() {
    local label="$1"
    local flags="$2"
    local source="$3"
    local size_mb="$4"
    local times=()

    for i in $(seq 1 $ITERATIONS); do
        # Clean receiver (files AND resume state)
        rm -rf "$RECV_DIR"
        mkdir -p "$RECV_DIR"
        rm -rf ~/.bytehaul/state/* 2>/dev/null || true

        # Start daemon (suppress all output)
        RUST_LOG=error "$BYTEHAUL" daemon --port $PORT --dest "$RECV_DIR" --overwrite overwrite >/dev/null 2>&1 &
        DAEMON_PID=$!
        sleep 0.3

        # Transfer
        local start end elapsed speed
        start=$(python3 -c 'import time; print(time.time())')
        if RUST_LOG=error "$BYTEHAUL" send --daemon "127.0.0.1:$PORT" $flags "$source" /data >/dev/null 2>&1; then
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
        kill "$DAEMON_PID" 2>/dev/null || true
        wait "$DAEMON_PID" 2>/dev/null || true
        DAEMON_PID=""
    done

    # Return median throughput
    median "${times[@]}"
}

# ─── Main ───

create_test_data

log "Starting benchmark..."
echo ""

# Test 1: 500 MB single file (primary metric)
log "═══ Test 1: 500 MB single file ═══"
T_500=$(run_bench "500mb" "" "$BENCH_DIR/500mb.bin" 500)

# Test 2: 100 MB single file
log "═══ Test 2: 100 MB single file ═══"
T_100=$(run_bench "100mb" "" "$BENCH_DIR/100mb.bin" 100)

# Test 3: 600 files directory
log "═══ Test 3: 600 x 200 KB files ═══"
T_DIR=$(run_bench "dir-600" "-r" "$BENCH_DIR/dir-600" 117)

echo ""
log "═══ Results ═══"
log "  500 MB throughput: ${T_500} MB/s"
log "  100 MB throughput: ${T_100} MB/s"
log "  Dir throughput:    ${T_DIR} MB/s"

# Output JSON to stdout for the agent to parse
cat <<EOF
{
  "throughput_500mb": $T_500,
  "throughput_100mb": $T_100,
  "throughput_dir": $T_DIR,
  "timestamp": "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
}
EOF

# ─── Baseline recording ───

if [ "${1:-}" = "--baseline" ]; then
    if [ ! -f "$TSV_FILE" ]; then
        echo -e "experiment\tname\tdescription\tthroughput_500mb\tthroughput_100mb\tthroughput_dir\tbuild_secs\tkept\ttimestamp" > "$TSV_FILE"
    fi
    echo -e "0\tbaseline\tInitial baseline measurement\t${T_500}\t${T_100}\t${T_DIR}\t0\tyes\t$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >> "$TSV_FILE"
    log "Baseline recorded in results.tsv"
fi
