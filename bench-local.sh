#!/usr/bin/env bash
set -euo pipefail

# ByteHaul vs scp/rsync local benchmark
# Transfers files over localhost to measure protocol overhead and throughput

BYTEHAUL="./target/release/bytehaul"
BENCH_DIR="/tmp/bytehaul-local-bench"
RECV_DIR="/tmp/bytehaul-local-recv"
PORT=17700
ITERATIONS=3

# Colors
CYAN='\033[0;36m'
GREEN='\033[0;32m'
BOLD='\033[1m'
NC='\033[0m'

cleanup() {
    # Kill any leftover daemon
    kill "$DAEMON_PID" 2>/dev/null || true
    rm -rf "$BENCH_DIR" "$RECV_DIR"
}
trap cleanup EXIT

echo -e "${BOLD}ByteHaul Local Benchmark${NC}"
echo "========================"
echo ""

# Build release if needed
if [ ! -f "$BYTEHAUL" ]; then
    echo "Building release binary..."
    cargo build --release -q
fi

# Create test files
mkdir -p "$BENCH_DIR" "$RECV_DIR"

echo -e "${CYAN}Creating test files...${NC}"

# 100MB file (realistic medium transfer)
dd if=/dev/urandom of="$BENCH_DIR/100mb.bin" bs=1048576 count=100 2>/dev/null
# 1GB file (large transfer)
dd if=/dev/urandom of="$BENCH_DIR/1gb.bin" bs=1048576 count=1024 2>/dev/null
# Many small files (10,000 x 10KB)
mkdir -p "$BENCH_DIR/small-files"
for i in $(seq 1 1000); do
    dd if=/dev/urandom of="$BENCH_DIR/small-files/file_$i.bin" bs=10240 count=1 2>/dev/null
done

echo ""
echo -e "${BOLD}Test 1: 100 MB single file${NC}"
echo "─────────────────────────"

# --- ByteHaul (daemon mode, localhost) ---
echo -e "\n${CYAN}bytehaul:${NC}"
for i in $(seq 1 $ITERATIONS); do
    rm -rf "$RECV_DIR"/*
    # Start daemon
    $BYTEHAUL daemon --port $PORT --dest "$RECV_DIR" --overwrite overwrite &
    DAEMON_PID=$!
    sleep 0.5

    START=$(python3 -c 'import time; print(time.time())')
    $BYTEHAUL send --daemon "127.0.0.1:$PORT" "$BENCH_DIR/100mb.bin" /100mb.bin 2>/dev/null
    END=$(python3 -c 'import time; print(time.time())')

    kill "$DAEMON_PID" 2>/dev/null; wait "$DAEMON_PID" 2>/dev/null || true
    ELAPSED=$(python3 -c "print(f'{$END - $START:.2f}')")
    SPEED=$(python3 -c "print(f'{100 / ($END - $START):.1f}')")
    echo "  Run $i: ${ELAPSED}s (${SPEED} MB/s)"
done

# --- scp (localhost) ---
echo -e "\n${CYAN}scp (localhost):${NC}"
for i in $(seq 1 $ITERATIONS); do
    rm -rf "$RECV_DIR"/*
    START=$(python3 -c 'import time; print(time.time())')
    scp -q -o StrictHostKeyChecking=no "$BENCH_DIR/100mb.bin" "localhost:$RECV_DIR/100mb.bin" 2>/dev/null
    END=$(python3 -c 'import time; print(time.time())')
    ELAPSED=$(python3 -c "print(f'{$END - $START:.2f}')")
    SPEED=$(python3 -c "print(f'{100 / ($END - $START):.1f}')")
    echo "  Run $i: ${ELAPSED}s (${SPEED} MB/s)"
done

# --- rsync (localhost) ---
echo -e "\n${CYAN}rsync (localhost):${NC}"
for i in $(seq 1 $ITERATIONS); do
    rm -rf "$RECV_DIR"/*
    START=$(python3 -c 'import time; print(time.time())')
    rsync -a "$BENCH_DIR/100mb.bin" "localhost:$RECV_DIR/100mb.bin" 2>/dev/null
    END=$(python3 -c 'import time; print(time.time())')
    ELAPSED=$(python3 -c "print(f'{$END - $START:.2f}')")
    SPEED=$(python3 -c "print(f'{100 / ($END - $START):.1f}')")
    echo "  Run $i: ${ELAPSED}s (${SPEED} MB/s)"
done

echo ""
echo -e "${BOLD}Test 2: 1 GB single file${NC}"
echo "────────────────────────"

# --- ByteHaul ---
echo -e "\n${CYAN}bytehaul:${NC}"
for i in $(seq 1 $ITERATIONS); do
    rm -rf "$RECV_DIR"/*
    $BYTEHAUL daemon --port $PORT --dest "$RECV_DIR" --overwrite overwrite &
    DAEMON_PID=$!
    sleep 0.5

    START=$(python3 -c 'import time; print(time.time())')
    $BYTEHAUL send --daemon "127.0.0.1:$PORT" "$BENCH_DIR/1gb.bin" /1gb.bin 2>/dev/null
    END=$(python3 -c 'import time; print(time.time())')

    kill "$DAEMON_PID" 2>/dev/null; wait "$DAEMON_PID" 2>/dev/null || true
    ELAPSED=$(python3 -c "print(f'{$END - $START:.2f}')")
    SPEED=$(python3 -c "print(f'{1024 / ($END - $START):.1f}')")
    echo "  Run $i: ${ELAPSED}s (${SPEED} MB/s)"
done

# --- scp ---
echo -e "\n${CYAN}scp (localhost):${NC}"
for i in $(seq 1 $ITERATIONS); do
    rm -rf "$RECV_DIR"/*
    START=$(python3 -c 'import time; print(time.time())')
    scp -q -o StrictHostKeyChecking=no "$BENCH_DIR/1gb.bin" "localhost:$RECV_DIR/1gb.bin" 2>/dev/null
    END=$(python3 -c 'import time; print(time.time())')
    ELAPSED=$(python3 -c "print(f'{$END - $START:.2f}')")
    SPEED=$(python3 -c "print(f'{1024 / ($END - $START):.1f}')")
    echo "  Run $i: ${ELAPSED}s (${SPEED} MB/s)"
done

# --- rsync ---
echo -e "\n${CYAN}rsync (localhost):${NC}"
for i in $(seq 1 $ITERATIONS); do
    rm -rf "$RECV_DIR"/*
    START=$(python3 -c 'import time; print(time.time())')
    rsync -a "$BENCH_DIR/1gb.bin" "localhost:$RECV_DIR/1gb.bin" 2>/dev/null
    END=$(python3 -c 'import time; print(time.time())')
    ELAPSED=$(python3 -c "print(f'{$END - $START:.2f}')")
    SPEED=$(python3 -c "print(f'{1024 / ($END - $START):.1f}')")
    echo "  Run $i: ${ELAPSED}s (${SPEED} MB/s)"
done

echo ""
echo -e "${GREEN}${BOLD}Benchmark complete.${NC}"
echo ""
echo "Note: This is a localhost test — all tools share the same loopback interface."
echo "ByteHaul's advantage grows with RTT and packet loss (cross-region, satellite)."
echo "Use 'bytehaul-bench' with --interface and tc/netem for realistic simulations."
