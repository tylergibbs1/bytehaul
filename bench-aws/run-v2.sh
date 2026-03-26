#!/usr/bin/env bash
set -euo pipefail

# ByteHaul v0.2 Real-World AWS Benchmark
# Tests: single file, directory transfer, delta transfer
# Two c5.xlarge instances: us-east-2 (Ohio) -> eu-west-1 (Ireland)

REGION_A="us-east-2"
REGION_B="eu-west-1"
INSTANCE_TYPE="c5.xlarge"
AMI_A=$(aws ec2 describe-images --owners amazon --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' --output text --region "$REGION_A")
AMI_B=$(aws ec2 describe-images --owners amazon --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' --output text --region "$REGION_B")
KEY_NAME="bh-bench-v2-$$"
SG_NAME="bh-bench-v2-$$"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

B='\033[1m' C='\033[0;36m' G='\033[0;32m' N='\033[0m'

echo -e "${B}ByteHaul v0.2 AWS Benchmark${N}"
echo "============================"
echo "Sender:   $REGION_A | Receiver: $REGION_B"
echo "Instance: $INSTANCE_TYPE"
echo ""

# ─── Cleanup ─────────────────────────────────────────────────
cleanup() {
    echo -e "\n${C}Cleaning up...${N}"
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
    echo -e "${G}Done.${N}"
}
trap cleanup EXIT

SSH="ssh -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
SCP="scp -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

# ─── Setup ───────────────────────────────────────────────────
echo -e "${C}Creating key pair...${N}"
aws ec2 create-key-pair --key-name "$KEY_NAME" --query 'KeyMaterial' --output text --region "$REGION_A" > /tmp/${KEY_NAME}.pem
chmod 600 /tmp/${KEY_NAME}.pem
PUB=$(ssh-keygen -y -f /tmp/${KEY_NAME}.pem)
aws ec2 import-key-pair --key-name "$KEY_NAME" --public-key-material "$(echo "$PUB" | base64)" --region "$REGION_B" >/dev/null

echo -e "${C}Creating security groups...${N}"
SG_A=$(aws ec2 create-security-group --group-name "$SG_NAME" --description "bh" --region "$REGION_A" --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id "$SG_A" --protocol tcp --port 22 --cidr 0.0.0.0/0 --region "$REGION_A" >/dev/null
aws ec2 authorize-security-group-ingress --group-id "$SG_A" --protocol udp --port 7700 --cidr 0.0.0.0/0 --region "$REGION_A" >/dev/null
SG_B=$(aws ec2 create-security-group --group-name "$SG_NAME" --description "bh" --region "$REGION_B" --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id "$SG_B" --protocol tcp --port 22 --cidr 0.0.0.0/0 --region "$REGION_B" >/dev/null
aws ec2 authorize-security-group-ingress --group-id "$SG_B" --protocol udp --port 7700 --cidr 0.0.0.0/0 --region "$REGION_B" >/dev/null

USERDATA='#!/bin/bash
yum install -y gcc openssl-devel rsync'

echo -e "${C}Launching instances...${N}"
INST_A=$(aws ec2 run-instances --image-id "$AMI_A" --instance-type "$INSTANCE_TYPE" --key-name "$KEY_NAME" --security-group-ids "$SG_A" --user-data "$USERDATA" --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bh-sender}]" --region "$REGION_A" --query 'Instances[0].InstanceId' --output text)
INST_B=$(aws ec2 run-instances --image-id "$AMI_B" --instance-type "$INSTANCE_TYPE" --key-name "$KEY_NAME" --security-group-ids "$SG_B" --user-data "$USERDATA" --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bh-receiver}]" --region "$REGION_B" --query 'Instances[0].InstanceId' --output text)
echo "  Sender:   $INST_A | Receiver: $INST_B"

aws ec2 wait instance-running --instance-ids "$INST_A" --region "$REGION_A"
aws ec2 wait instance-running --instance-ids "$INST_B" --region "$REGION_B"

IP_A=$(aws ec2 describe-instances --instance-ids "$INST_A" --region "$REGION_A" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
IP_B=$(aws ec2 describe-instances --instance-ids "$INST_B" --region "$REGION_B" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "  IPs: $IP_A -> $IP_B"

echo -e "${C}Waiting for SSH...${N}"
for ip in $IP_A $IP_B; do
    for i in $(seq 1 60); do
        $SSH ec2-user@$ip "echo ok" 2>/dev/null && break || sleep 5
    done
done

echo -e "${C}Waiting for cloud-init...${N}"
$SSH ec2-user@$IP_A "cloud-init status --wait" >/dev/null 2>&1 || sleep 30
$SSH ec2-user@$IP_B "cloud-init status --wait" >/dev/null 2>&1 || sleep 30

# ─── Build ───────────────────────────────────────────────────
echo -e "${C}Building ByteHaul on both instances...${N}"
cd "$PROJECT_DIR"
tar czf /tmp/bytehaul-src.tar.gz --exclude='target' --exclude='.git' --exclude='bench-docker' .

build() {
    local ip=$1 label=$2
    echo "  Building on $label ($ip)..."
    $SCP /tmp/bytehaul-src.tar.gz ec2-user@$ip:/tmp/bytehaul-src.tar.gz
    $SSH ec2-user@$ip "
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y 2>&1 | tail -1
        source ~/.cargo/env
        mkdir -p /tmp/bytehaul && cd /tmp/bytehaul && tar xzf /tmp/bytehaul-src.tar.gz
        cargo build --release --bin bytehaul 2>&1 | tail -2
        sudo cp target/release/bytehaul /usr/local/bin/
        bytehaul --version
    "
}

build "$IP_A" "sender" &
build "$IP_B" "receiver" &
wait

# ─── SSH between instances ───────────────────────────────────
$SCP /tmp/${KEY_NAME}.pem ec2-user@$IP_A:/tmp/bench-key.pem
$SSH ec2-user@$IP_A "chmod 600 /tmp/bench-key.pem"

RKEY="-i /tmp/bench-key.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

# ─── Benchmark ───────────────────────────────────────────────
echo ""
echo -e "${B}============================================${N}"
echo -e "${B}  REAL-WORLD BENCHMARK: $REGION_A -> $REGION_B${N}"
echo -e "${B}============================================${N}"

$SSH ec2-user@$IP_A "
IP_B='$IP_B'
K='$RKEY'

echo ''
echo '--- RTT ---'
ping -c 5 \$IP_B | tail -1
echo ''

# Helper
run() {
    local label=\$1; shift
    local file_mb=\$1; shift
    echo \"--- \$label ---\"
    for i in 1 2 3; do
        START=\$(python3 -c 'import time; print(time.time())')
        eval \"\$@\" >/dev/null 2>&1
        END=\$(python3 -c 'import time; print(time.time())')
        python3 -c \"e=\$END-\$START; print(f'  Run \$i: {e:.2f}s  ({\$file_mb/e:.1f} MB/s)')\"
    done
    echo ''
}

start_daemon() {
    ssh \$K ec2-user@\$IP_B \"rm -rf /tmp/recv; mkdir -p /tmp/recv; nohup bytehaul daemon --port 7700 --dest /tmp/recv --overwrite overwrite </dev/null >/dev/null 2>&1 &\"
    sleep 2
}
stop_daemon() {
    ssh \$K ec2-user@\$IP_B 'pkill -f \"bytehaul daemon\" 2>/dev/null || true'
}

# ═══ Test 1: Single file 100 MB ═══
echo '═══ Test 1: Single file (100 MB) ═══'
dd if=/dev/urandom of=/tmp/100mb.bin bs=1M count=100 2>/dev/null

run 'bytehaul' 100 'start_daemon; RUST_LOG=error bytehaul send --daemon \"\${IP_B}:7700\" /tmp/100mb.bin /bench.bin; stop_daemon'
run 'scp' 100 'ssh \$K ec2-user@\$IP_B \"rm -f /tmp/recv/*; mkdir -p /tmp/recv\"; scp -q \$K /tmp/100mb.bin ec2-user@\$IP_B:/tmp/recv/bench.bin'

# ═══ Test 2: Directory transfer (1000 files, 10KB each = 10MB) ═══
echo '═══ Test 2: Directory transfer (1000 x 10KB = 10 MB) ═══'
rm -rf /tmp/testdir && mkdir -p /tmp/testdir/sub1/sub2
for i in \$(seq 1 500); do dd if=/dev/urandom of=/tmp/testdir/file_\$i.bin bs=10240 count=1 2>/dev/null; done
for i in \$(seq 1 300); do dd if=/dev/urandom of=/tmp/testdir/sub1/file_\$i.bin bs=10240 count=1 2>/dev/null; done
for i in \$(seq 1 200); do dd if=/dev/urandom of=/tmp/testdir/sub1/sub2/file_\$i.bin bs=10240 count=1 2>/dev/null; done

run 'bytehaul -r' 10 'start_daemon; RUST_LOG=error bytehaul send -r --daemon \"\${IP_B}:7700\" /tmp/testdir /testdir; stop_daemon'
run 'scp -r' 10 'ssh \$K ec2-user@\$IP_B \"rm -rf /tmp/recv; mkdir -p /tmp/recv\"; scp -q -r \$K /tmp/testdir ec2-user@\$IP_B:/tmp/recv/'
run 'rsync -r' 10 'ssh \$K ec2-user@\$IP_B \"rm -rf /tmp/recv; mkdir -p /tmp/recv\"; rsync -a -e \"ssh \$K\" /tmp/testdir ec2-user@\$IP_B:/tmp/recv/'

# ═══ Test 3: Delta transfer (modify 10% of a 100MB file) ═══
echo '═══ Test 3: Delta transfer (100 MB, 10% changed) ═══'

# First: full transfer to establish baseline at destination
start_daemon
RUST_LOG=error bytehaul send --daemon \"\${IP_B}:7700\" /tmp/100mb.bin /delta_test.bin >/dev/null 2>&1
stop_daemon

# Modify 10% of the file (overwrite first 10MB with new random data)
dd if=/dev/urandom of=/tmp/100mb.bin bs=1M count=10 conv=notrunc 2>/dev/null

echo '--- bytehaul --delta (only changed blocks) ---'
for i in 1 2 3; do
    start_daemon
    START=\$(python3 -c 'import time; print(time.time())')
    RUST_LOG=error bytehaul send --delta --daemon \"\${IP_B}:7700\" /tmp/100mb.bin /delta_test.bin >/dev/null 2>&1
    END=\$(python3 -c 'import time; print(time.time())')
    stop_daemon
    python3 -c \"e=\$END-\$START; print(f'  Run \$i: {e:.2f}s  ({100/e:.1f} MB/s effective)')\"
done
echo ''

echo '--- bytehaul (full, no delta) ---'
for i in 1 2 3; do
    start_daemon
    START=\$(python3 -c 'import time; print(time.time())')
    RUST_LOG=error bytehaul send --daemon \"\${IP_B}:7700\" /tmp/100mb.bin /delta_test.bin >/dev/null 2>&1
    END=\$(python3 -c 'import time; print(time.time())')
    stop_daemon
    python3 -c \"e=\$END-\$START; print(f'  Run \$i: {e:.2f}s  ({100/e:.1f} MB/s)')\"
done
echo ''

echo '--- scp (always full) ---'
for i in 1 2 3; do
    ssh \$K ec2-user@\$IP_B 'mkdir -p /tmp/recv'
    START=\$(python3 -c 'import time; print(time.time())')
    scp -q \$K /tmp/100mb.bin ec2-user@\$IP_B:/tmp/recv/delta_test.bin
    END=\$(python3 -c 'import time; print(time.time())')
    python3 -c \"e=\$END-\$START; print(f'  Run \$i: {e:.2f}s  ({100/e:.1f} MB/s)')\"
done
echo ''

# ═══ Test 4: 1 GB single file ═══
echo '═══ Test 4: Single file (1 GB) ═══'
dd if=/dev/urandom of=/tmp/1gb.bin bs=1M count=1024 2>/dev/null

run 'bytehaul' 1024 'start_daemon; RUST_LOG=error bytehaul send --daemon \"\${IP_B}:7700\" /tmp/1gb.bin /bench.bin; stop_daemon'
run 'scp' 1024 'ssh \$K ec2-user@\$IP_B \"rm -f /tmp/recv/*; mkdir -p /tmp/recv\"; scp -q \$K /tmp/1gb.bin ec2-user@\$IP_B:/tmp/recv/bench.bin'

echo '═══ DONE ═══'
" 2>/dev/null
