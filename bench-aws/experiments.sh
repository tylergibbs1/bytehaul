#!/usr/bin/env bash
set -euo pipefail

# ═══════════════════════════════════════════════════════════════
# ByteHaul 50-Experiment Overnight Benchmark Suite
# ═══════════════════════════════════════════════════════════════
#
# Runs 50 experiments across:
#   - Shutdown overhead fix verification
#   - Parallel stream tuning (2-64 streams)
#   - Block size tuning (256KB-64MB)
#   - File size scaling (1MB-5GB)
#   - Directory patterns (10-10000 files)
#   - Delta transfer scenarios (0%-100% changed)
#   - Aggressive vs fair congestion
#   - Comparison tools (scp, rsync, rsync -z)
#   - Concurrent transfers
#   - Resume after kill
#
# Estimated runtime: 2-3 hours
# Estimated cost: ~$1.50 (two c5.xlarge for ~3 hours)

REGION_A="us-east-2"
REGION_B="eu-west-1"
INSTANCE_TYPE="c5.xlarge"
KEY_NAME="bh-exp-$$"
SG_NAME="bh-exp-$$"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

RESULTS="/tmp/bytehaul-experiments.json"
echo "[]" > "$RESULTS"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ─── Record result as JSON ───────────────────────────────────
record() {
    local exp_num=$1 name=$2 tool=$3 file_desc=$4 seconds=$5 size_mb=$6
    local speed
    speed=$(python3 -c "print(round($size_mb / $seconds, 2) if $seconds > 0 else 0)")
    python3 -c "
import json
with open('$RESULTS') as f: data = json.load(f)
data.append({
    'experiment': $exp_num,
    'name': '$name',
    'tool': '$tool',
    'file': '$file_desc',
    'seconds': round($seconds, 2),
    'size_mb': $size_mb,
    'speed_mbps': $speed
})
with open('$RESULTS', 'w') as f: json.dump(data, f, indent=2)
"
    log "  EXP#$exp_num [$tool] ${seconds}s (${speed} MB/s)"
}

# ─── Helper: time a command ──────────────────────────────────
timed() {
    local start end
    start=$(python3 -c 'import time; print(time.time())')
    eval "$@" >/dev/null 2>&1
    end=$(python3 -c 'import time; print(time.time())')
    python3 -c "print($end - $start)"
}

# ─── AWS Setup ───────────────────────────────────────────────
cleanup() {
    log "Cleaning up AWS resources..."
    [ -n "${INST_A:-}" ] && aws ec2 terminate-instances --instance-ids "$INST_A" --region "$REGION_A" --output text >/dev/null 2>&1 || true
    [ -n "${INST_B:-}" ] && aws ec2 terminate-instances --instance-ids "$INST_B" --region "$REGION_B" --output text >/dev/null 2>&1 || true
    sleep 5
    [ -n "${INST_A:-}" ] && aws ec2 wait instance-terminated --instance-ids "$INST_A" --region "$REGION_A" 2>/dev/null || true
    [ -n "${INST_B:-}" ] && aws ec2 wait instance-terminated --instance-ids "$INST_B" --region "$REGION_B" 2>/dev/null || true
    [ -n "${SG_A:-}" ] && aws ec2 delete-security-group --group-id "$SG_A" --region "$REGION_A" 2>/dev/null || true
    [ -n "${SG_B:-}" ] && aws ec2 delete-security-group --group-id "$SG_B" --region "$REGION_B" 2>/dev/null || true
    aws ec2 delete-key-pair --key-name "$KEY_NAME" --region "$REGION_A" 2>/dev/null || true
    aws ec2 delete-key-pair --key-name "$KEY_NAME" --region "$REGION_B" 2>/dev/null || true
    rm -f /tmp/${KEY_NAME}.pem /tmp/bytehaul-src.tar.gz

    # Copy results to project dir
    cp "$RESULTS" "$PROJECT_DIR/experiment-results.json" 2>/dev/null || true
    log "Results saved to experiment-results.json"
    log "Cleanup complete."
}
trap cleanup EXIT

AMI_A=$(aws ec2 describe-images --owners amazon --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' --output text --region "$REGION_A")
AMI_B=$(aws ec2 describe-images --owners amazon --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' --output text --region "$REGION_B")

log "Creating infrastructure..."
aws ec2 create-key-pair --key-name "$KEY_NAME" --query 'KeyMaterial' --output text --region "$REGION_A" > /tmp/${KEY_NAME}.pem
chmod 600 /tmp/${KEY_NAME}.pem
PUB=$(ssh-keygen -y -f /tmp/${KEY_NAME}.pem)
aws ec2 import-key-pair --key-name "$KEY_NAME" --public-key-material "$(echo "$PUB" | base64)" --region "$REGION_B" >/dev/null

SG_A=$(aws ec2 create-security-group --group-name "$SG_NAME" --description "bh" --region "$REGION_A" --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id "$SG_A" --protocol tcp --port 22 --cidr 0.0.0.0/0 --region "$REGION_A" >/dev/null
aws ec2 authorize-security-group-ingress --group-id "$SG_A" --protocol udp --port 0-65535 --cidr 0.0.0.0/0 --region "$REGION_A" >/dev/null
SG_B=$(aws ec2 create-security-group --group-name "$SG_NAME" --description "bh" --region "$REGION_B" --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id "$SG_B" --protocol tcp --port 22 --cidr 0.0.0.0/0 --region "$REGION_B" >/dev/null
aws ec2 authorize-security-group-ingress --group-id "$SG_B" --protocol udp --port 0-65535 --cidr 0.0.0.0/0 --region "$REGION_B" >/dev/null

USERDATA='#!/bin/bash
yum install -y gcc openssl-devel rsync bc'

log "Launching instances..."
INST_A=$(aws ec2 run-instances --image-id "$AMI_A" --instance-type "$INSTANCE_TYPE" --key-name "$KEY_NAME" --security-group-ids "$SG_A" --user-data "$USERDATA" --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bh-exp-sender}]" --region "$REGION_A" --query 'Instances[0].InstanceId' --output text)
INST_B=$(aws ec2 run-instances --image-id "$AMI_B" --instance-type "$INSTANCE_TYPE" --key-name "$KEY_NAME" --security-group-ids "$SG_B" --user-data "$USERDATA" --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bh-exp-receiver}]" --region "$REGION_B" --query 'Instances[0].InstanceId' --output text)

aws ec2 wait instance-running --instance-ids "$INST_A" --region "$REGION_A"
aws ec2 wait instance-running --instance-ids "$INST_B" --region "$REGION_B"

IP_A=$(aws ec2 describe-instances --instance-ids "$INST_A" --region "$REGION_A" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
IP_B=$(aws ec2 describe-instances --instance-ids "$INST_B" --region "$REGION_B" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
log "Sender: $IP_A ($REGION_A) | Receiver: $IP_B ($REGION_B)"

SSH="ssh -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
SCP_CMD="scp -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

# Wait for SSH
for ip in $IP_A $IP_B; do
    for i in $(seq 1 60); do $SSH ec2-user@$ip "echo ok" 2>/dev/null && break || sleep 5; done
done
$SSH ec2-user@$IP_A "cloud-init status --wait" >/dev/null 2>&1 || sleep 30
$SSH ec2-user@$IP_B "cloud-init status --wait" >/dev/null 2>&1 || sleep 30

# Build
log "Building ByteHaul on both instances..."
cd "$PROJECT_DIR"
tar czf /tmp/bytehaul-src.tar.gz --exclude='target' --exclude='.git' --exclude='bench-docker' .

for ip in $IP_A $IP_B; do
    $SCP_CMD /tmp/bytehaul-src.tar.gz ec2-user@$ip:/tmp/bytehaul-src.tar.gz
    $SSH ec2-user@$ip "
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y 2>&1 | tail -1
        source ~/.cargo/env
        mkdir -p /tmp/bytehaul && cd /tmp/bytehaul && tar xzf /tmp/bytehaul-src.tar.gz
        cargo build --release --bin bytehaul 2>&1 | tail -2
        sudo cp target/release/bytehaul /usr/local/bin/
    " &
done
wait
log "Build complete."

# Setup SSH between instances
$SCP_CMD /tmp/${KEY_NAME}.pem ec2-user@$IP_A:/tmp/bench-key.pem
$SSH ec2-user@$IP_A "chmod 600 /tmp/bench-key.pem"

RK="-i /tmp/bench-key.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

# ─── Measure baseline RTT ────────────────────────────────────
RTT=$($SSH ec2-user@$IP_A "ping -c 10 $IP_B 2>/dev/null | tail -1" || echo "unknown")
log "RTT: $RTT"

# ═══════════════════════════════════════════════════════════════
# EXPERIMENTS
# ═══════════════════════════════════════════════════════════════

run_on_sender() {
    $SSH ec2-user@$IP_A "$@"
}

# Helper: start daemon, run transfer, stop daemon, return elapsed seconds
bh_transfer() {
    local extra_args="${1:-}"
    local file="$2"
    local remote_path="$3"
    local port="${4:-7700}"
    run_on_sender "
        K='$RK'
        ssh \$K ec2-user@$IP_B 'rm -rf /tmp/recv; mkdir -p /tmp/recv; nohup bytehaul daemon --port $port --dest /tmp/recv --overwrite overwrite </dev/null >/dev/null 2>&1 &'
        sleep 1
        START=\$(python3 -c 'import time; print(time.time())')
        RUST_LOG=error bytehaul send --daemon '$IP_B:$port' $extra_args $file $remote_path >/dev/null 2>&1
        END=\$(python3 -c 'import time; print(time.time())')
        ssh \$K ec2-user@$IP_B 'pkill -f \"bytehaul daemon\" 2>/dev/null || true'
        python3 -c \"print(\$END - \$START)\"
    "
}

scp_transfer() {
    local file="$1"
    run_on_sender "
        K='$RK'
        ssh \$K ec2-user@$IP_B 'rm -rf /tmp/recv; mkdir -p /tmp/recv'
        START=\$(python3 -c 'import time; print(time.time())')
        scp -q \$K $file ec2-user@$IP_B:/tmp/recv/bench.bin
        END=\$(python3 -c 'import time; print(time.time())')
        python3 -c \"print(\$END - \$START)\"
    "
}

rsync_transfer() {
    local extra_args="${1:-}"
    local file="$2"
    run_on_sender "
        K='$RK'
        ssh \$K ec2-user@$IP_B 'rm -rf /tmp/recv; mkdir -p /tmp/recv'
        START=\$(python3 -c 'import time; print(time.time())')
        rsync $extra_args -e \"ssh \$K\" $file ec2-user@$IP_B:/tmp/recv/
        END=\$(python3 -c 'import time; print(time.time())')
        python3 -c \"print(\$END - \$START)\"
    "
}

# Create test files on sender
log "Creating test files..."
run_on_sender "
dd if=/dev/urandom of=/tmp/1mb.bin bs=1M count=1 2>/dev/null
dd if=/dev/urandom of=/tmp/10mb.bin bs=1M count=10 2>/dev/null
dd if=/dev/urandom of=/tmp/50mb.bin bs=1M count=50 2>/dev/null
dd if=/dev/urandom of=/tmp/100mb.bin bs=1M count=100 2>/dev/null
dd if=/dev/urandom of=/tmp/500mb.bin bs=1M count=500 2>/dev/null
dd if=/dev/urandom of=/tmp/1gb.bin bs=1M count=1024 2>/dev/null
dd if=/dev/urandom of=/tmp/5gb.bin bs=1M count=5120 2>/dev/null

# Directory test files
mkdir -p /tmp/dir10 /tmp/dir100 /tmp/dir1000 /tmp/dir10000
for i in \$(seq 1 10); do dd if=/dev/urandom of=/tmp/dir10/f\$i.bin bs=1M count=10 2>/dev/null; done
for i in \$(seq 1 100); do dd if=/dev/urandom of=/tmp/dir100/f\$i.bin bs=1M count=1 2>/dev/null; done
for i in \$(seq 1 1000); do dd if=/dev/urandom of=/tmp/dir1000/f\$i.bin bs=102400 count=1 2>/dev/null; done
mkdir -p /tmp/dir10000/sub{1,2,3,4,5,6,7,8,9,10}
for d in /tmp/dir10000/sub*; do for i in \$(seq 1 1000); do dd if=/dev/urandom of=\$d/f\$i.bin bs=10240 count=1 2>/dev/null; done; done

# Deep nesting
DIR=/tmp/deepnest
for i in \$(seq 1 10); do DIR=\$DIR/level\$i; mkdir -p \$DIR; dd if=/dev/urandom of=\$DIR/data.bin bs=1M count=1 2>/dev/null; done

# Mixed realistic dataset
mkdir -p /tmp/mixed/logs /tmp/mixed/models /tmp/mixed/configs
dd if=/dev/urandom of=/tmp/mixed/models/model.bin bs=1M count=50 2>/dev/null
for i in \$(seq 1 100); do dd if=/dev/urandom of=/tmp/mixed/logs/log\$i.txt bs=10240 count=1 2>/dev/null; done
for i in \$(seq 1 20); do dd if=/dev/urandom of=/tmp/mixed/configs/cfg\$i.yaml bs=1024 count=1 2>/dev/null; done
"
log "Test files created."

# ═══ Experiments 1-3: Shutdown fix verification ═══════════════
log "═══ Experiments 1-3: Shutdown overhead verification ═══"

for i in 1 2 3; do
    T=$(bh_transfer "" "/tmp/100mb.bin" "/bench.bin")
    record $i "shutdown-fix-100mb" "bytehaul" "100MB" "$T" 100
done

# ═══ Experiments 4-9: Parallel streams ═══════════════════════
log "═══ Experiments 4-9: Parallel stream tuning ═══"

for streams in 2 4 8 16 32 64; do
    exp=$((3 + $(echo "2 4 8 16 32 64" | tr ' ' '\n' | grep -n "^${streams}$" | cut -d: -f1)))
    T=$(bh_transfer "--parallel $streams" "/tmp/100mb.bin" "/bench.bin")
    record $exp "parallel-${streams}-streams" "bytehaul" "100MB" "$T" 100
done

# ═══ Experiments 10-15: Block size ═══════════════════════════
log "═══ Experiments 10-15: Block size tuning ═══"

exp=10
for bs in 1 2 4 8 16 64; do
    T=$(bh_transfer "--block-size $bs" "/tmp/100mb.bin" "/bench.bin")
    record $exp "blocksize-${bs}mb" "bytehaul" "100MB" "$T" 100
    exp=$((exp+1))
done

# ═══ Experiments 16-23: File size scaling ════════════════════
log "═══ Experiments 16-23: File size scaling ═══"

exp=16
for spec in "1mb.bin:1" "10mb.bin:10" "50mb.bin:50" "100mb.bin:100" "500mb.bin:500" "1gb.bin:1024" "5gb.bin:5120"; do
    file="/tmp/$(echo $spec | cut -d: -f1)"
    mb=$(echo $spec | cut -d: -f2)
    T=$(bh_transfer "" "$file" "/bench.bin")
    record $exp "filesize-${mb}mb" "bytehaul" "${mb}MB" "$T" "$mb"
    exp=$((exp+1))
done

# scp for same sizes (subset for comparison)
for spec in "1mb.bin:1" "100mb.bin:100" "1gb.bin:1024"; do
    file="/tmp/$(echo $spec | cut -d: -f1)"
    mb=$(echo $spec | cut -d: -f2)
    T=$(scp_transfer "$file")
    record $exp "filesize-${mb}mb" "scp" "${mb}MB" "$T" "$mb"
    exp=$((exp+1))
done

# ═══ Experiments 26-31: Directory patterns ═══════════════════
log "═══ Experiments 26-31: Directory patterns ═══"

T=$(bh_transfer "-r" "/tmp/dir10" "/dir10"); record 26 "dir-10x10mb" "bytehaul" "10x10MB" "$T" 100
T=$(bh_transfer "-r" "/tmp/dir100" "/dir100"); record 27 "dir-100x1mb" "bytehaul" "100x1MB" "$T" 100
T=$(bh_transfer "-r" "/tmp/dir1000" "/dir1000"); record 28 "dir-1000x100kb" "bytehaul" "1000x100KB" "$T" 97
T=$(bh_transfer "-r" "/tmp/dir10000" "/dir10000"); record 29 "dir-10000x10kb" "bytehaul" "10000x10KB" "$T" 97

T=$(bh_transfer "-r" "/tmp/deepnest" "/deepnest"); record 30 "dir-deep-10levels" "bytehaul" "10x1MB-nested" "$T" 10
T=$(bh_transfer "-r" "/tmp/mixed" "/mixed"); record 31 "dir-mixed-realistic" "bytehaul" "mixed-51MB" "$T" 51

# scp -r for comparison
log "═══ Directory comparison with scp/rsync ═══"
run_on_sender "K='$RK'; ssh \$K ec2-user@$IP_B 'rm -rf /tmp/recv; mkdir -p /tmp/recv'"
T=$(run_on_sender "K='$RK'; START=\$(python3 -c 'import time; print(time.time())'); scp -q -r \$K /tmp/dir1000 ec2-user@$IP_B:/tmp/recv/; END=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$END-\$START)\"")
record 32 "dir-1000x100kb" "scp" "1000x100KB" "$T" 97

T=$(rsync_transfer "-a -r" "/tmp/dir1000"); record 33 "dir-1000x100kb" "rsync" "1000x100KB" "$T" 97

# ═══ Experiments 34-39: Delta scenarios ══════════════════════
log "═══ Experiments 34-39: Delta transfer ═══"

# First: establish baseline file on receiver
bh_transfer "" "/tmp/100mb.bin" "/delta.bin" >/dev/null

# 0% changed (identical)
T=$(bh_transfer "--delta" "/tmp/100mb.bin" "/delta.bin"); record 34 "delta-0pct-changed" "bytehaul-delta" "100MB" "$T" 100

# 1% changed
run_on_sender "dd if=/dev/urandom of=/tmp/100mb.bin bs=1M count=1 conv=notrunc 2>/dev/null"
bh_transfer "" "/tmp/100mb.bin" "/delta.bin" >/dev/null  # update receiver
run_on_sender "dd if=/dev/urandom of=/tmp/100mb.bin bs=1M count=1 conv=notrunc seek=50 2>/dev/null"
T=$(bh_transfer "--delta" "/tmp/100mb.bin" "/delta.bin"); record 35 "delta-1pct-changed" "bytehaul-delta" "100MB" "$T" 100

# 10% changed
run_on_sender "dd if=/dev/urandom of=/tmp/100mb.bin bs=1M count=10 conv=notrunc 2>/dev/null"
T=$(bh_transfer "--delta" "/tmp/100mb.bin" "/delta.bin"); record 36 "delta-10pct-changed" "bytehaul-delta" "100MB" "$T" 100

# 50% changed
run_on_sender "dd if=/dev/urandom of=/tmp/100mb.bin bs=1M count=50 conv=notrunc 2>/dev/null"
T=$(bh_transfer "--delta" "/tmp/100mb.bin" "/delta.bin"); record 37 "delta-50pct-changed" "bytehaul-delta" "100MB" "$T" 100

# 100% changed (all new)
run_on_sender "dd if=/dev/urandom of=/tmp/100mb_new.bin bs=1M count=100 2>/dev/null"
T=$(bh_transfer "--delta" "/tmp/100mb_new.bin" "/delta.bin"); record 38 "delta-100pct-changed" "bytehaul-delta" "100MB" "$T" 100

# No delta (full send, same file for comparison)
T=$(bh_transfer "" "/tmp/100mb.bin" "/delta.bin"); record 39 "delta-disabled-full" "bytehaul" "100MB" "$T" 100

# ═══ Experiments 40-43: Aggressive vs Fair ═══════════════════
log "═══ Experiments 40-43: Congestion mode ═══"

T=$(bh_transfer "" "/tmp/100mb.bin" "/bench.bin"); record 40 "fair-100mb" "bytehaul-fair" "100MB" "$T" 100
T=$(bh_transfer "--aggressive" "/tmp/100mb.bin" "/bench.bin"); record 41 "aggressive-100mb" "bytehaul-aggressive" "100MB" "$T" 100
T=$(bh_transfer "" "/tmp/1gb.bin" "/bench.bin"); record 42 "fair-1gb" "bytehaul-fair" "1GB" "$T" 1024
T=$(bh_transfer "--aggressive" "/tmp/1gb.bin" "/bench.bin"); record 43 "aggressive-1gb" "bytehaul-aggressive" "1GB" "$T" 1024

# ═══ Experiments 44-46: rsync comparison ═════════════════════
log "═══ Experiments 44-46: rsync comparison ═══"

T=$(rsync_transfer "-a" "/tmp/100mb.bin"); record 44 "rsync-100mb" "rsync" "100MB" "$T" 100
T=$(rsync_transfer "-az" "/tmp/100mb.bin"); record 45 "rsync-z-100mb" "rsync-compressed" "100MB" "$T" 100
T=$(rsync_transfer "-a" "/tmp/1gb.bin"); record 46 "rsync-1gb" "rsync" "1GB" "$T" 1024

# ═══ Experiments 47-48: Concurrent transfers ═════════════════
log "═══ Experiments 47-48: Concurrent transfers ═══"

# 4 concurrent 100MB transfers
run_on_sender "K='$RK'; ssh \$K ec2-user@$IP_B 'rm -rf /tmp/recv; mkdir -p /tmp/recv; for p in 7701 7702 7703 7704; do nohup bytehaul daemon --port \$p --dest /tmp/recv --overwrite overwrite </dev/null >/dev/null 2>&1 & done'"
sleep 2
T=$(run_on_sender "
START=\$(python3 -c 'import time; print(time.time())')
for p in 7701 7702 7703 7704; do
    RUST_LOG=error bytehaul send --daemon '$IP_B':\$p /tmp/100mb.bin /bench_\$p.bin >/dev/null 2>&1 &
done
wait
END=\$(python3 -c 'import time; print(time.time())')
python3 -c \"print(\$END - \$START)\"
")
run_on_sender "K='$RK'; ssh \$K ec2-user@$IP_B 'pkill -f bytehaul 2>/dev/null || true'"
record 47 "concurrent-4x100mb" "bytehaul-4x" "4x100MB" "$T" 400

# 4 concurrent scp
T=$(run_on_sender "
K='$RK'
ssh \$K ec2-user@$IP_B 'rm -rf /tmp/recv; mkdir -p /tmp/recv'
START=\$(python3 -c 'import time; print(time.time())')
for i in 1 2 3 4; do
    scp -q \$K /tmp/100mb.bin ec2-user@$IP_B:/tmp/recv/bench_\$i.bin &
done
wait
END=\$(python3 -c 'import time; print(time.time())')
python3 -c \"print(\$END - \$START)\"
")
record 48 "concurrent-4x100mb" "scp-4x" "4x100MB" "$T" 400

# ═══ Experiment 49: Resume after kill ═════════════════════════
log "═══ Experiment 49: Resume reliability ═══"

# Start a 1GB transfer, kill it after 5s, resume
run_on_sender "K='$RK'; ssh \$K ec2-user@$IP_B 'rm -rf /tmp/recv; mkdir -p /tmp/recv; nohup bytehaul daemon --port 7700 --dest /tmp/recv --overwrite overwrite </dev/null >/dev/null 2>&1 &'"
sleep 1
run_on_sender "
timeout 5 bash -c 'RUST_LOG=error bytehaul send --daemon $IP_B:7700 /tmp/1gb.bin /resume_test.bin' 2>/dev/null || true
" >/dev/null 2>&1
run_on_sender "K='$RK'; ssh \$K ec2-user@$IP_B 'pkill -f bytehaul 2>/dev/null || true'"
sleep 1

# Now resume
T=$(bh_transfer "--resume" "/tmp/1gb.bin" "/resume_test.bin")
record 49 "resume-1gb-after-5s" "bytehaul-resume" "1GB-partial" "$T" 1024

# ═══ Experiment 50: Maximum sustained throughput ═════════════
log "═══ Experiment 50: Maximum throughput (5GB) ═══"

T=$(bh_transfer "" "/tmp/5gb.bin" "/bench.bin")
record 50 "max-throughput-5gb" "bytehaul" "5GB" "$T" 5120

# ═══ Summary ═════════════════════════════════════════════════
log ""
log "═══════════════════════════════════════════════"
log "  ALL 50 EXPERIMENTS COMPLETE"
log "  Results: $RESULTS"
log "═══════════════════════════════════════════════"
log ""

# Print summary table
python3 -c "
import json
with open('$RESULTS') as f: data = json.load(f)
print(f'{'Exp':>4} {'Name':<30} {'Tool':<22} {'File':<15} {'Seconds':>8} {'MB/s':>8}')
print('-' * 95)
for r in data:
    print(f'{r[\"experiment\"]:>4} {r[\"name\"]:<30} {r[\"tool\"]:<22} {r[\"file\"]:<15} {r[\"seconds\"]:>8} {r[\"speed_mbps\"]:>8}')
"
