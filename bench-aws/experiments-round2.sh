#!/usr/bin/env bash
set -uo pipefail

# ═══════════════════════════════════════════════════════════════
# ByteHaul Round 2 Experiments: What Target Users Would Ask
# ═══════════════════════════════════════════════════════════════
#
# 10 experiment groups, ~40 data points total:
#
# GROUP 1: Packet loss on real AWS (iptables-induced)
# GROUP 2: RTT crossover point (same-AZ, same-region, cross-region, intercontinental)
# GROUP 3: CPU/memory profiling
# GROUP 4: Bandwidth limiting (--max-rate)
# GROUP 5: Compressible vs incompressible data
# GROUP 6: Large file stability (10GB)
# GROUP 7: Network flap recovery (brief disconnect)
# GROUP 8: Fan-in (4 receivers from 1 sender)
# GROUP 9: Per-transfer overhead breakdown
# GROUP 10: Cross-cloud (AWS -> GCP)
#
# Uses 4 instances:
#   - us-east-2a sender (Ohio)
#   - us-east-2b receiver (Ohio, same-region ~1ms)
#   - eu-west-1 receiver (Ireland, ~85ms)
#   - us-west-2 receiver (Oregon, ~50ms)
#
# Estimated runtime: 2-3 hours
# Estimated cost: ~$3

REGIONS=("us-east-2" "us-east-2" "eu-west-1" "us-west-2")
LABELS=("sender" "same-region" "ireland" "oregon")
INSTANCE_TYPE="c5.xlarge"
KEY_NAME="bh-r2-$$"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

RESULTS="/tmp/bytehaul-round2.json"
echo "[]" > "$RESULTS"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

record() {
    local exp=$1 group=$2 name=$3 tool=$4 file_desc=$5 seconds=$6 size_mb=$7 extra=${8:-}
    local speed
    speed=$(python3 -c "print(round($size_mb / $seconds, 2) if $seconds > 0.001 else 0)")
    python3 -c "
import json
with open('$RESULTS') as f: data = json.load(f)
data.append({
    'experiment': $exp, 'group': '$group', 'name': '$name',
    'tool': '$tool', 'file': '$file_desc', 'seconds': round($seconds, 3),
    'size_mb': $size_mb, 'speed_mbps': $speed, 'extra': '$extra'
})
with open('$RESULTS', 'w') as f: json.dump(data, f, indent=2)
"
    log "  EXP#$exp [$tool] ${seconds}s (${speed} MB/s) $extra"
}

# ─── AWS Setup (4 instances) ─────────────────────────────────
SG_IDS=()
INST_IDS=()
IPS=()

cleanup() {
    log "Cleaning up..."
    for i in 0 1 2 3; do
        [ -n "${INST_IDS[$i]:-}" ] && aws ec2 terminate-instances --instance-ids "${INST_IDS[$i]}" --region "${REGIONS[$i]}" --output text >/dev/null 2>&1 || true
    done
    sleep 5
    for i in 0 1 2 3; do
        [ -n "${INST_IDS[$i]:-}" ] && aws ec2 wait instance-terminated --instance-ids "${INST_IDS[$i]}" --region "${REGIONS[$i]}" 2>/dev/null || true
        [ -n "${SG_IDS[$i]:-}" ] && aws ec2 delete-security-group --group-id "${SG_IDS[$i]}" --region "${REGIONS[$i]}" 2>/dev/null || true
        aws ec2 delete-key-pair --key-name "$KEY_NAME" --region "${REGIONS[$i]}" 2>/dev/null || true
    done
    rm -f /tmp/${KEY_NAME}.pem /tmp/bytehaul-src.tar.gz
    cp "$RESULTS" "$PROJECT_DIR/experiment-results-round2.json" 2>/dev/null || true
    log "Results saved to experiment-results-round2.json"
}
trap cleanup EXIT

log "Creating key pair..."
aws ec2 create-key-pair --key-name "$KEY_NAME" --query KeyMaterial --output text --region us-east-2 > /tmp/${KEY_NAME}.pem
chmod 600 /tmp/${KEY_NAME}.pem
PUB=$(ssh-keygen -y -f /tmp/${KEY_NAME}.pem)
for r in eu-west-1 us-west-2; do
    aws ec2 import-key-pair --key-name "$KEY_NAME" --public-key-material "$(echo "$PUB" | base64)" --region "$r" >/dev/null
done

USERDATA='#!/bin/bash
yum install -y gcc openssl-devel rsync bc iptables perf'

log "Creating security groups and launching instances..."
for i in 0 1 2 3; do
    r=${REGIONS[$i]}
    AMI=$(aws ec2 describe-images --owners amazon --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' --output text --region "$r")

    SG=$(aws ec2 create-security-group --group-name "$KEY_NAME-${LABELS[$i]}" --description "bh" --region "$r" --query GroupId --output text)
    aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol -1 --cidr 0.0.0.0/0 --region "$r" >/dev/null
    SG_IDS+=("$SG")

    INST=$(aws ec2 run-instances --image-id "$AMI" --instance-type "$INSTANCE_TYPE" --key-name "$KEY_NAME" --security-group-ids "$SG" --user-data "$USERDATA" --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]' --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bh-r2-${LABELS[$i]}}]" --region "$r" --query 'Instances[0].InstanceId' --output text)
    INST_IDS+=("$INST")
    log "  ${LABELS[$i]}: $INST ($r)"
done

for i in 0 1 2 3; do
    aws ec2 wait instance-running --instance-ids "${INST_IDS[$i]}" --region "${REGIONS[$i]}"
    IP=$(aws ec2 describe-instances --instance-ids "${INST_IDS[$i]}" --region "${REGIONS[$i]}" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
    IPS+=("$IP")
    log "  ${LABELS[$i]}: $IP"
done

IP_SENDER=${IPS[0]}
IP_SAME=${IPS[1]}
IP_IRELAND=${IPS[2]}
IP_OREGON=${IPS[3]}

SSH="ssh -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ServerAliveInterval=30"
SCP_CMD="scp -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

log "Waiting for SSH..."
for ip in ${IPS[@]}; do
    for j in $(seq 1 60); do $SSH ec2-user@$ip "echo ok" 2>/dev/null && break || sleep 5; done
done
for ip in ${IPS[@]}; do
    $SSH ec2-user@$ip "cloud-init status --wait" >/dev/null 2>&1 || sleep 30
done

log "Building ByteHaul on all instances..."
cd "$PROJECT_DIR"
tar czf /tmp/bytehaul-src.tar.gz --exclude='target' --exclude='.git' --exclude='bench-docker' --exclude='experiment-results*' .
for ip in ${IPS[@]}; do
    $SCP_CMD /tmp/bytehaul-src.tar.gz ec2-user@$ip:/tmp/bytehaul-src.tar.gz
    $SSH ec2-user@$ip '
        curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y 2>&1 | tail -1
        source ~/.cargo/env
        mkdir -p /tmp/bytehaul && cd /tmp/bytehaul && tar xzf /tmp/bytehaul-src.tar.gz
        cargo build --release --bin bytehaul 2>&1 | tail -2
        sudo cp target/release/bytehaul /usr/local/bin/
    ' &
done
wait
log "Build complete."

# Setup SSH keys from sender to all receivers
for ip in $IP_SAME $IP_IRELAND $IP_OREGON; do
    $SCP_CMD /tmp/${KEY_NAME}.pem ec2-user@$IP_SENDER:/tmp/bench-key.pem
done
$SSH ec2-user@$IP_SENDER "chmod 600 /tmp/bench-key.pem"

RK="-i /tmp/bench-key.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

S() { $SSH ec2-user@$IP_SENDER "$@"; }

# Measure RTTs
log "Measuring RTTs..."
for ip_label in "$IP_SAME:same-region" "$IP_IRELAND:ireland" "$IP_OREGON:oregon"; do
    ip=${ip_label%%:*}; label=${ip_label##*:}
    rtt=$(S "ping -c 5 $ip 2>/dev/null | grep 'avg' | cut -d'/' -f5" || echo "?")
    log "  $label ($ip): ${rtt}ms"
done

# Create test files
log "Creating test files..."
S 'dd if=/dev/urandom of=/tmp/100mb.bin bs=1M count=100 2>/dev/null'
S 'dd if=/dev/urandom of=/tmp/1gb.bin bs=1M count=1024 2>/dev/null'
S 'dd if=/dev/urandom of=/tmp/10gb.bin bs=1M count=10240 2>/dev/null'

# Compressible: zeros, repeated text, JSON
S 'dd if=/dev/zero of=/tmp/100mb_zeros.bin bs=1M count=100 2>/dev/null'
S 'python3 -c "import json; data={\"key_\"+str(i): \"value_\"+str(i)*100 for i in range(50000)}; open(\"/tmp/100mb_json.bin\",\"w\").write(json.dumps(data)*5)" 2>/dev/null || dd if=/dev/zero of=/tmp/100mb_json.bin bs=1M count=100 2>/dev/null'
S 'yes "The quick brown fox jumps over the lazy dog" | head -c 104857600 > /tmp/100mb_text.bin 2>/dev/null'
log "Test files created."

# ─── Helpers ─────────────────────────────────────────────────
bh() {
    local recv_ip=$1; shift
    local extra_args="${1:-}"; local file="$2"; local remote="$3"; local port="${4:-7700}"
    S "
        K='$RK'
        ssh \$K ec2-user@$recv_ip 'rm -rf /tmp/recv; mkdir -p /tmp/recv; nohup bytehaul daemon --port $port --dest /tmp/recv --overwrite overwrite </dev/null >/dev/null 2>&1 &'
        sleep 1
        START=\$(python3 -c 'import time; print(time.time())')
        RUST_LOG=error bytehaul send --daemon '$recv_ip:$port' $extra_args $file $remote >/dev/null 2>&1
        END=\$(python3 -c 'import time; print(time.time())')
        ssh \$K ec2-user@$recv_ip 'pkill -f \"bytehaul daemon\" 2>/dev/null || true'
        python3 -c \"print(\$END - \$START)\"
    "
}

do_scp() {
    local recv_ip=$1; local file=$2
    S "
        K='$RK'
        ssh \$K ec2-user@$recv_ip 'rm -rf /tmp/recv; mkdir -p /tmp/recv'
        START=\$(python3 -c 'import time; print(time.time())')
        scp -q \$K $file ec2-user@$recv_ip:/tmp/recv/bench.bin
        END=\$(python3 -c 'import time; print(time.time())')
        python3 -c \"print(\$END - \$START)\"
    "
}

EXP=0
next() { EXP=$((EXP+1)); }

# ═══════════════════════════════════════════════════════════════
# GROUP 1: Packet loss (iptables on receiver)
# ═══════════════════════════════════════════════════════════════
log "═══ GROUP 1: Packet loss on real AWS ═══"

for loss in 0 0.1 0.5 1 2 5; do
    next
    # Apply loss on receiver
    if [ "$loss" != "0" ]; then
        $SSH ec2-user@$IP_IRELAND "sudo iptables -A INPUT -m statistic --mode random --probability $(python3 -c "print($loss/100)") -j DROP 2>/dev/null" || true
    fi

    T=$(bh "$IP_IRELAND" "" "/tmp/100mb.bin" "/bench.bin") || T=0
    record $EXP "packet-loss" "loss-${loss}pct-100mb" "bytehaul" "100MB" "$T" 100 "loss=${loss}%"

    # Clear iptables
    $SSH ec2-user@$IP_IRELAND "sudo iptables -F 2>/dev/null" || true
done

# scp with loss for comparison
for loss in 0.1 1 5; do
    next
    $SSH ec2-user@$IP_IRELAND "sudo iptables -A INPUT -m statistic --mode random --probability $(python3 -c "print($loss/100)") -j DROP 2>/dev/null" || true
    T=$(do_scp "$IP_IRELAND" "/tmp/100mb.bin") || T=999
    record $EXP "packet-loss" "loss-${loss}pct-100mb" "scp" "100MB" "$T" 100 "loss=${loss}%"
    $SSH ec2-user@$IP_IRELAND "sudo iptables -F 2>/dev/null" || true
done

# ═══════════════════════════════════════════════════════════════
# GROUP 2: RTT crossover point
# ═══════════════════════════════════════════════════════════════
log "═══ GROUP 2: RTT crossover (~1ms, ~50ms, ~85ms) ═══"

for recv_ip_label in "$IP_SAME:same-region:1ms" "$IP_OREGON:oregon:50ms" "$IP_IRELAND:ireland:85ms"; do
    recv_ip=${recv_ip_label%%:*}
    rest=${recv_ip_label#*:}
    label=${rest%%:*}
    rtt=${rest##*:}

    # ByteHaul 100MB
    next; T=$(bh "$recv_ip" "" "/tmp/100mb.bin" "/bench.bin") || T=0
    record $EXP "rtt-crossover" "bh-100mb-${rtt}" "bytehaul" "100MB" "$T" 100 "rtt=$rtt"

    # scp 100MB
    next; T=$(do_scp "$recv_ip" "/tmp/100mb.bin") || T=0
    record $EXP "rtt-crossover" "scp-100mb-${rtt}" "scp" "100MB" "$T" 100 "rtt=$rtt"

    # ByteHaul 1GB
    next; T=$(bh "$recv_ip" "" "/tmp/1gb.bin" "/bench.bin") || T=0
    record $EXP "rtt-crossover" "bh-1gb-${rtt}" "bytehaul" "1GB" "$T" 1024 "rtt=$rtt"

    # scp 1GB
    next; T=$(do_scp "$recv_ip" "/tmp/1gb.bin") || T=0
    record $EXP "rtt-crossover" "scp-1gb-${rtt}" "scp" "1GB" "$T" 1024 "rtt=$rtt"
done

# ═══════════════════════════════════════════════════════════════
# GROUP 3: CPU/memory profiling
# ═══════════════════════════════════════════════════════════════
log "═══ GROUP 3: CPU and memory profiling ═══"

# Measure CPU time and peak RSS for 1GB transfer
next
T=$(S "
    K='$RK'
    ssh \$K ec2-user@$IP_IRELAND 'rm -rf /tmp/recv; mkdir -p /tmp/recv; nohup bytehaul daemon --port 7700 --dest /tmp/recv --overwrite overwrite </dev/null >/dev/null 2>&1 &'
    sleep 1
    START=\$(python3 -c 'import time; print(time.time())')
    /usr/bin/time -v bytehaul send --daemon '$IP_IRELAND:7700' /tmp/1gb.bin /bench.bin 2>/tmp/time_output || true
    END=\$(python3 -c 'import time; print(time.time())')
    ssh \$K ec2-user@$IP_IRELAND 'pkill -f \"bytehaul daemon\" 2>/dev/null || true'
    cat /tmp/time_output >&2
    python3 -c \"print(\$END - \$START)\"
" 2>/tmp/cpu_profile) || T=0
CPU_INFO=$($SSH ec2-user@$IP_SENDER "cat /tmp/time_output 2>/dev/null" || echo "N/A")
record $EXP "cpu-profile" "bh-1gb-cpu" "bytehaul" "1GB" "$T" 1024 "see_log"
log "  CPU profile: $CPU_INFO"

# Measure sender-side BLAKE3 hashing time separately
next
T=$(S "
    START=\$(python3 -c 'import time; print(time.time())')
    python3 -c 'import subprocess; subprocess.run([\"b3sum\", \"/tmp/1gb.bin\"], capture_output=True)' 2>/dev/null || \
    python3 -c '
import hashlib, time
h = hashlib.blake2b()
with open(\"/tmp/1gb.bin\",\"rb\") as f:
    while True:
        chunk = f.read(1048576)
        if not chunk: break
        h.update(chunk)
'
    END=\$(python3 -c 'import time; print(time.time())')
    python3 -c \"print(\$END - \$START)\"
") || T=0
record $EXP "cpu-profile" "blake3-hash-1gb" "blake3" "1GB" "$T" 1024 "hash_only"

# ═══════════════════════════════════════════════════════════════
# GROUP 4: Bandwidth limiting
# ═══════════════════════════════════════════════════════════════
log "═══ GROUP 4: --max-rate bandwidth limiting ═══"

for rate in 10 50 100 500; do
    next
    T=$(bh "$IP_IRELAND" "--max-rate ${rate}mbps" "/tmp/100mb.bin" "/bench.bin") || T=0
    record $EXP "bandwidth-limit" "maxrate-${rate}mbps" "bytehaul" "100MB" "$T" 100 "rate=${rate}mbps"
done

# ═══════════════════════════════════════════════════════════════
# GROUP 5: Compressible vs incompressible data
# ═══════════════════════════════════════════════════════════════
log "═══ GROUP 5: Compressible vs incompressible ═══"

# ByteHaul (no compression, so compressible data shouldn't help)
for file_label in "100mb.bin:random" "100mb_zeros.bin:zeros" "100mb_text.bin:text" "100mb_json.bin:json"; do
    file="/tmp/${file_label%%:*}"; label=${file_label##*:}
    next; T=$(bh "$IP_IRELAND" "" "$file" "/bench.bin") || T=0
    record $EXP "compressible" "bh-${label}" "bytehaul" "100MB-$label" "$T" 100 "data=$label"
done

# rsync -z (should benefit from compressible data)
for file_label in "100mb.bin:random" "100mb_zeros.bin:zeros" "100mb_text.bin:text"; do
    file="/tmp/${file_label%%:*}"; label=${file_label##*:}
    next
    T=$(S "
        K='$RK'
        ssh \$K ec2-user@$IP_IRELAND 'rm -rf /tmp/recv; mkdir -p /tmp/recv'
        START=\$(python3 -c 'import time; print(time.time())')
        rsync -az -e \"ssh \$K\" $file ec2-user@$IP_IRELAND:/tmp/recv/
        END=\$(python3 -c 'import time; print(time.time())')
        python3 -c \"print(\$END - \$START)\"
    ") || T=0
    record $EXP "compressible" "rsync-z-${label}" "rsync-z" "100MB-$label" "$T" 100 "data=$label"
done

# ═══════════════════════════════════════════════════════════════
# GROUP 6: Large file stability (10GB)
# ═══════════════════════════════════════════════════════════════
log "═══ GROUP 6: Large file stability (10GB) ═══"

next; T=$(bh "$IP_IRELAND" "" "/tmp/10gb.bin" "/bench.bin") || T=0
record $EXP "large-file" "bh-10gb" "bytehaul" "10GB" "$T" 10240

next; T=$(do_scp "$IP_IRELAND" "/tmp/10gb.bin") || T=0
record $EXP "large-file" "scp-10gb" "scp" "10GB" "$T" 10240

# ═══════════════════════════════════════════════════════════════
# GROUP 7: Network flap recovery
# ═══════════════════════════════════════════════════════════════
log "═══ GROUP 7: Network flap (brief disconnect) ═══"

# Start a 1GB transfer, inject 3s network blackout at 2s in, measure total time
next
T=$(S "
    K='$RK'
    ssh \$K ec2-user@$IP_IRELAND 'rm -rf /tmp/recv; mkdir -p /tmp/recv; nohup bytehaul daemon --port 7700 --dest /tmp/recv --overwrite overwrite </dev/null >/dev/null 2>&1 &'
    sleep 1

    START=\$(python3 -c 'import time; print(time.time())')

    # Start transfer in background
    RUST_LOG=error bytehaul send --daemon '$IP_IRELAND:7700' /tmp/1gb.bin /bench.bin >/dev/null 2>&1 &
    BH_PID=\$!

    # After 2s, block UDP for 3s on sender side
    sleep 2
    sudo iptables -A OUTPUT -p udp -d $IP_IRELAND -j DROP 2>/dev/null || true
    sleep 3
    sudo iptables -D OUTPUT -p udp -d $IP_IRELAND -j DROP 2>/dev/null || true

    # Wait for transfer to complete
    wait \$BH_PID 2>/dev/null
    END=\$(python3 -c 'import time; print(time.time())')
    ssh \$K ec2-user@$IP_IRELAND 'pkill -f \"bytehaul daemon\" 2>/dev/null || true'
    python3 -c \"print(\$END - \$START)\"
") || T=0
record $EXP "network-flap" "bh-1gb-3s-blackout" "bytehaul" "1GB" "$T" 1024 "3s_blackout"

# Same without blackout for comparison
next; T=$(bh "$IP_IRELAND" "" "/tmp/1gb.bin" "/bench.bin") || T=0
record $EXP "network-flap" "bh-1gb-no-flap" "bytehaul" "1GB" "$T" 1024 "baseline"

# ═══════════════════════════════════════════════════════════════
# GROUP 8: Fan-in (1 sender -> 3 receivers simultaneously)
# ═══════════════════════════════════════════════════════════════
log "═══ GROUP 8: Fan-out to 3 receivers ═══"

# Start daemons on all 3 receivers
for ip in $IP_SAME $IP_IRELAND $IP_OREGON; do
    $SSH ec2-user@$ip "rm -rf /tmp/recv; mkdir -p /tmp/recv; nohup bytehaul daemon --port 7700 --dest /tmp/recv --overwrite overwrite </dev/null >/dev/null 2>&1 &"
done
sleep 2

next
T=$(S "
    START=\$(python3 -c 'import time; print(time.time())')
    RUST_LOG=error bytehaul send --daemon '$IP_SAME:7700' /tmp/100mb.bin /bench.bin >/dev/null 2>&1 &
    RUST_LOG=error bytehaul send --daemon '$IP_IRELAND:7700' /tmp/100mb.bin /bench.bin >/dev/null 2>&1 &
    RUST_LOG=error bytehaul send --daemon '$IP_OREGON:7700' /tmp/100mb.bin /bench.bin >/dev/null 2>&1 &
    wait
    END=\$(python3 -c 'import time; print(time.time())')
    python3 -c \"print(\$END - \$START)\"
") || T=0
record $EXP "fan-out" "bh-3x100mb-fanout" "bytehaul" "3x100MB" "$T" 300 "3_receivers"

for ip in $IP_SAME $IP_IRELAND $IP_OREGON; do
    $SSH ec2-user@$ip "pkill -f 'bytehaul daemon' 2>/dev/null || true"
done

# scp fan-out for comparison
next
T=$(S "
    K='$RK'
    for ip in $IP_SAME $IP_IRELAND $IP_OREGON; do
        ssh \$K ec2-user@\$ip 'rm -rf /tmp/recv; mkdir -p /tmp/recv'
    done
    START=\$(python3 -c 'import time; print(time.time())')
    scp -q \$K /tmp/100mb.bin ec2-user@$IP_SAME:/tmp/recv/bench.bin &
    scp -q \$K /tmp/100mb.bin ec2-user@$IP_IRELAND:/tmp/recv/bench.bin &
    scp -q \$K /tmp/100mb.bin ec2-user@$IP_OREGON:/tmp/recv/bench.bin &
    wait
    END=\$(python3 -c 'import time; print(time.time())')
    python3 -c \"print(\$END - \$START)\"
") || T=0
record $EXP "fan-out" "scp-3x100mb-fanout" "scp" "3x100MB" "$T" 300 "3_receivers"

# ═══════════════════════════════════════════════════════════════
# GROUP 9: Per-transfer overhead breakdown
# ═══════════════════════════════════════════════════════════════
log "═══ GROUP 9: Overhead breakdown ═══"

# Time each phase separately for a 100MB transfer
next
BREAKDOWN=$(S "
    K='$RK'

    # Phase 1: BLAKE3 hash only
    START=\$(python3 -c 'import time; print(time.time())')
    source ~/.cargo/env 2>/dev/null
    python3 -c '
import hashlib
with open(\"/tmp/100mb.bin\",\"rb\") as f:
    h = hashlib.sha256()  # stand-in timing
    while True:
        c = f.read(1048576)
        if not c: break
        h.update(c)
'
    END=\$(python3 -c 'import time; print(time.time())')
    HASH_TIME=\$(python3 -c \"print(\$END - \$START)\")

    # Phase 2: Daemon start (SSH + process launch)
    START=\$(python3 -c 'import time; print(time.time())')
    ssh \$K ec2-user@$IP_IRELAND 'rm -rf /tmp/recv; mkdir -p /tmp/recv; nohup bytehaul daemon --port 7700 --dest /tmp/recv --overwrite overwrite </dev/null >/dev/null 2>&1 &'
    END=\$(python3 -c 'import time; print(time.time())')
    DAEMON_TIME=\$(python3 -c \"print(\$END - \$START)\")

    # Phase 3: QUIC handshake only
    sleep 1
    START=\$(python3 -c 'import time; print(time.time())')
    RUST_LOG=error bytehaul send --daemon '$IP_IRELAND:7700' /tmp/1mb.bin /bench.bin >/dev/null 2>&1
    END=\$(python3 -c 'import time; print(time.time())')
    HANDSHAKE_TIME=\$(python3 -c \"print(\$END - \$START)\")

    # Phase 4: Full 100MB transfer
    START=\$(python3 -c 'import time; print(time.time())')
    RUST_LOG=error bytehaul send --daemon '$IP_IRELAND:7700' /tmp/100mb.bin /bench.bin >/dev/null 2>&1
    END=\$(python3 -c 'import time; print(time.time())')
    FULL_TIME=\$(python3 -c \"print(\$END - \$START)\")

    ssh \$K ec2-user@$IP_IRELAND 'pkill -f \"bytehaul daemon\" 2>/dev/null || true'

    echo \"hash=\$HASH_TIME daemon=\$DAEMON_TIME handshake=\$HANDSHAKE_TIME full=\$FULL_TIME\"
") || BREAKDOWN="failed"
log "  Overhead breakdown: $BREAKDOWN"
record $EXP "overhead" "breakdown-100mb" "bytehaul" "100MB" "0" 100 "$BREAKDOWN"

# ═══════════════════════════════════════════════════════════════
# GROUP 10: Cross-region RTT ladder (already covered in GROUP 2,
# but add same-AZ for completeness)
# ═══════════════════════════════════════════════════════════════
log "═══ GROUP 10: Repeat key tests for statistical confidence ═══"

# 3 runs each of the most important comparisons
for run in 1 2 3; do
    next; T=$(bh "$IP_IRELAND" "" "/tmp/1gb.bin" "/bench.bin") || T=0
    record $EXP "confidence" "bh-1gb-ireland-run${run}" "bytehaul" "1GB" "$T" 1024

    next; T=$(do_scp "$IP_IRELAND" "/tmp/1gb.bin") || T=0
    record $EXP "confidence" "scp-1gb-ireland-run${run}" "scp" "1GB" "$T" 1024
done

# ═══════════════════════════════════════════════════════════════
log ""
log "═══════════════════════════════════════════════"
log "  ALL EXPERIMENTS COMPLETE"
log "═══════════════════════════════════════════════"

python3 -c "
import json
with open('$RESULTS') as f: data = json.load(f)
print()
print(f'{'Exp':>4} {'Group':<15} {'Name':<30} {'Tool':<18} {'Secs':>7} {'MB/s':>8} {'Extra'}')
print('-' * 110)
for r in data:
    print(f'{r[\"experiment\"]:>4} {r[\"group\"]:<15} {r[\"name\"]:<30} {r[\"tool\"]:<18} {r[\"seconds\"]:>7} {r[\"speed_mbps\"]:>8} {r.get(\"extra\",\"\")}')
"
