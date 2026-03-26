#!/usr/bin/env bash
set -euo pipefail

# ByteHaul vs scp/rsync benchmark with tc/netem network simulation
# Run from the SENDER container: docker exec bh-sender run-bench.sh

RECEIVER="172.28.0.20"
RECEIVER_SSH="root@${RECEIVER}"
IFACE="eth0"
ITERATIONS=3
RESULTS_DIR="/data/results"

# ANSI
B='\033[1m'
C='\033[0;36m'
G='\033[0;32m'
R='\033[0;31m'
N='\033[0m'

mkdir -p "$RESULTS_DIR"

# ─────────────────────────────────────────────────────────────
# Helper: apply netem on BOTH sender and receiver (symmetric)
# ─────────────────────────────────────────────────────────────
apply_netem() {
    local delay_ms=$1   # one-way delay (RTT = 2x)
    local loss=$2       # loss percentage
    local rate=$3       # bandwidth in mbit

    # Clear existing rules
    tc qdisc del dev $IFACE root 2>/dev/null || true
    ssh $RECEIVER_SSH "tc qdisc del dev $IFACE root 2>/dev/null || true"

    if [ "$delay_ms" -gt 0 ] || [ "$(echo "$loss > 0" | bc)" -eq 1 ]; then
        local netem_args="delay ${delay_ms}ms"
        if [ "$(echo "$loss > 0" | bc)" -eq 1 ]; then
            netem_args="$netem_args loss ${loss}%"
        fi

        # Apply netem for delay/loss
        tc qdisc add dev $IFACE root handle 1: netem $netem_args
        ssh $RECEIVER_SSH "tc qdisc add dev $IFACE root handle 1: netem $netem_args"

        # Add bandwidth limit as child if specified
        if [ "$rate" -gt 0 ]; then
            local burst=$((rate * 1000 / 8))  # burst in bytes
            [ "$burst" -lt 1600 ] && burst=1600
            tc qdisc add dev $IFACE parent 1: handle 10: tbf rate ${rate}mbit burst ${burst} latency 100ms
            ssh $RECEIVER_SSH "tc qdisc add dev $IFACE parent 1: handle 10: tbf rate ${rate}mbit burst ${burst} latency 100ms"
        fi
    elif [ "$rate" -gt 0 ]; then
        local burst=$((rate * 1000 / 8))
        [ "$burst" -lt 1600 ] && burst=1600
        tc qdisc add dev $IFACE root tbf rate ${rate}mbit burst ${burst} latency 100ms
        ssh $RECEIVER_SSH "tc qdisc add dev $IFACE root tbf rate ${rate}mbit burst ${burst} latency 100ms"
    fi
}

clear_netem() {
    tc qdisc del dev $IFACE root 2>/dev/null || true
    ssh $RECEIVER_SSH "tc qdisc del dev $IFACE root 2>/dev/null || true"
}

# ─────────────────────────────────────────────────────────────
# Helper: run a single tool benchmark
# ─────────────────────────────────────────────────────────────
run_tool() {
    local tool=$1
    local file=$2
    local file_size_mb=$3
    local label=$4

    echo -e "  ${C}${label}${N}"

    local total_time=0
    local runs=0

    for i in $(seq 1 $ITERATIONS); do
        # Clean receiver
        ssh $RECEIVER_SSH "rm -f /recv/*" 2>/dev/null || true

        local start end elapsed speed
        start=$(python3 -c 'import time; print(time.time())')

        case "$tool" in
            bytehaul)
                # Start daemon on receiver, transfer, stop daemon
                ssh $RECEIVER_SSH "nohup bytehaul daemon --port 7700 --dest /recv --overwrite overwrite </dev/null >/dev/null 2>&1 &"
                sleep 1
                RUST_LOG=error bytehaul send --daemon "${RECEIVER}:7700" "$file" /bench.bin >/dev/null 2>&1
                ssh $RECEIVER_SSH "pkill -f 'bytehaul daemon' || true" 2>/dev/null
                ;;
            bytehaul-aggressive)
                ssh $RECEIVER_SSH "nohup bytehaul daemon --port 7700 --dest /recv --overwrite overwrite </dev/null >/dev/null 2>&1 &"
                sleep 1
                RUST_LOG=error bytehaul send --daemon "${RECEIVER}:7700" --aggressive "$file" /bench.bin >/dev/null 2>&1
                ssh $RECEIVER_SSH "pkill -f 'bytehaul daemon' || true" 2>/dev/null
                ;;
            scp)
                scp -q "$file" "${RECEIVER_SSH}:/recv/bench.bin" 2>/dev/null
                ;;
            rsync)
                rsync -q "$file" "${RECEIVER_SSH}:/recv/bench.bin" 2>/dev/null
                ;;
        esac

        end=$(python3 -c 'import time; print(time.time())')
        elapsed=$(python3 -c "print(f'{$end - $start:.2f}')")
        speed=$(python3 -c "print(f'{$file_size_mb / ($end - $start):.1f}')")
        echo "    Run $i: ${elapsed}s  (${speed} MB/s)"

        total_time=$(python3 -c "print($total_time + $end - $start)")
        runs=$((runs + 1))
    done

    local avg_speed
    avg_speed=$(python3 -c "print(f'{$file_size_mb / ($total_time / $runs):.1f}')")
    echo -e "    ${G}Avg: ${avg_speed} MB/s${N}"
    echo ""
}

# ─────────────────────────────────────────────────────────────
# Create test files
# ─────────────────────────────────────────────────────────────
echo -e "${B}ByteHaul Network Benchmark${N}"
echo "=========================="
echo ""
echo "Sender:   172.28.0.10"
echo "Receiver: 172.28.0.20"
echo "Iterations per tool: $ITERATIONS"
echo ""

echo "Creating test files..."
dd if=/dev/urandom of=/data/100mb.bin bs=1M count=100 2>/dev/null
dd if=/dev/urandom of=/data/1gb.bin bs=1M count=1024 2>/dev/null
echo ""

# ─────────────────────────────────────────────────────────────
# Scenario 1: No simulation (baseline)
# ─────────────────────────────────────────────────────────────
echo -e "${B}━━━ Scenario: Baseline (no netem) ━━━${N}"
echo "  Network: unrestricted, ~0ms RTT"
echo ""

clear_netem
run_tool bytehaul /data/100mb.bin 100 "bytehaul (100 MB)"
run_tool scp      /data/100mb.bin 100 "scp (100 MB)"
run_tool rsync    /data/100mb.bin 100 "rsync (100 MB)"

# ─────────────────────────────────────────────────────────────
# Scenario 2: Metro link (1 Gbps, 10ms RTT)
# ─────────────────────────────────────────────────────────────
echo -e "${B}━━━ Scenario: Metro Link ━━━${N}"
echo "  Network: 1 Gbps, 10ms RTT, 0% loss"
echo ""

apply_netem 5 0 1000
run_tool bytehaul /data/100mb.bin 100 "bytehaul (100 MB)"
run_tool scp      /data/100mb.bin 100 "scp (100 MB)"
run_tool rsync    /data/100mb.bin 100 "rsync (100 MB)"

# ─────────────────────────────────────────────────────────────
# Scenario 3: Cross-region (1 Gbps, 50ms RTT) — PRIMARY WIN
# ─────────────────────────────────────────────────────────────
echo -e "${B}━━━ Scenario: Cross-Region (primary win scenario) ━━━${N}"
echo "  Network: 1 Gbps, 50ms RTT, 0% loss"
echo ""

apply_netem 25 0 1000
run_tool bytehaul            /data/100mb.bin 100 "bytehaul fair (100 MB)"
run_tool bytehaul-aggressive /data/100mb.bin 100 "bytehaul aggressive (100 MB)"
run_tool scp                 /data/100mb.bin 100 "scp (100 MB)"
run_tool rsync               /data/100mb.bin 100 "rsync (100 MB)"

# 1 GB transfers for this important scenario
echo "  --- 1 GB transfers ---"
run_tool bytehaul            /data/1gb.bin 1024 "bytehaul fair (1 GB)"
run_tool bytehaul-aggressive /data/1gb.bin 1024 "bytehaul aggressive (1 GB)"
run_tool scp                 /data/1gb.bin 1024 "scp (1 GB)"

# ─────────────────────────────────────────────────────────────
# Scenario 4: Intercontinental (1 Gbps, 150ms RTT, 0.1% loss)
# ─────────────────────────────────────────────────────────────
echo -e "${B}━━━ Scenario: Intercontinental ━━━${N}"
echo "  Network: 1 Gbps, 150ms RTT, 0.1% loss"
echo ""

apply_netem 75 0.1 1000
run_tool bytehaul            /data/100mb.bin 100 "bytehaul fair (100 MB)"
run_tool bytehaul-aggressive /data/100mb.bin 100 "bytehaul aggressive (100 MB)"
run_tool scp                 /data/100mb.bin 100 "scp (100 MB)"
run_tool rsync               /data/100mb.bin 100 "rsync (100 MB)"

# ─────────────────────────────────────────────────────────────
# Scenario 5: Degraded link (100 Mbps, 200ms RTT, 2% loss)
# ─────────────────────────────────────────────────────────────
echo -e "${B}━━━ Scenario: Degraded Link ━━━${N}"
echo "  Network: 100 Mbps, 200ms RTT, 2% loss"
echo ""

apply_netem 100 2 100
run_tool bytehaul            /data/100mb.bin 100 "bytehaul fair (100 MB)"
run_tool bytehaul-aggressive /data/100mb.bin 100 "bytehaul aggressive (100 MB)"
run_tool scp                 /data/100mb.bin 100 "scp (100 MB)"
run_tool rsync               /data/100mb.bin 100 "rsync (100 MB)"

# ─────────────────────────────────────────────────────────────
# Cleanup
# ─────────────────────────────────────────────────────────────
clear_netem

echo ""
echo -e "${G}${B}Benchmark complete.${N}"
echo "Results saved to: $RESULTS_DIR"
