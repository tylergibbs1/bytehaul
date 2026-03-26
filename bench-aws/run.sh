#!/usr/bin/env bash
set -euo pipefail

# ByteHaul Real-World AWS Benchmark
# Spins up two EC2 instances in different regions, benchmarks, tears down.
# Total cost: ~$0.20 (two t3.medium for ~15 minutes)

REGION_A="us-east-2"       # Ohio
REGION_B="eu-west-1"       # Ireland (~120ms RTT)
INSTANCE_TYPE="c5.xlarge"  # 4 vCPU, 8GB, up to 10 Gbps network
AMI_A="ami-0b0b78dcacbab728f"
AMI_B="ami-0d1b55a6d77a0c326"
KEY_NAME="bytehaul-bench-$$"
SG_NAME="bytehaul-bench-$$"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors
B='\033[1m'
C='\033[0;36m'
G='\033[0;32m'
R='\033[0;31m'
N='\033[0m'

echo -e "${B}ByteHaul Real-World AWS Benchmark${N}"
echo "=================================="
echo ""
echo "Region A (sender):   $REGION_A"
echo "Region B (receiver): $REGION_B"
echo "Instance type:       $INSTANCE_TYPE"
echo ""

# ─── Cleanup function ───────────────────────────────────────
cleanup() {
    echo ""
    echo -e "${C}Cleaning up AWS resources...${N}"

    # Terminate instances
    if [ -n "${INSTANCE_A_ID:-}" ]; then
        echo "  Terminating sender ($INSTANCE_A_ID)..."
        aws ec2 terminate-instances --instance-ids "$INSTANCE_A_ID" --region "$REGION_A" --output text >/dev/null 2>&1 || true
    fi
    if [ -n "${INSTANCE_B_ID:-}" ]; then
        echo "  Terminating receiver ($INSTANCE_B_ID)..."
        aws ec2 terminate-instances --instance-ids "$INSTANCE_B_ID" --region "$REGION_B" --output text >/dev/null 2>&1 || true
    fi

    # Wait for termination then delete security groups and key pairs
    sleep 5

    if [ -n "${SG_A_ID:-}" ]; then
        # Need to wait for instances to fully terminate before deleting SG
        if [ -n "${INSTANCE_A_ID:-}" ]; then
            aws ec2 wait instance-terminated --instance-ids "$INSTANCE_A_ID" --region "$REGION_A" 2>/dev/null || true
        fi
        aws ec2 delete-security-group --group-id "$SG_A_ID" --region "$REGION_A" 2>/dev/null || true
    fi
    if [ -n "${SG_B_ID:-}" ]; then
        if [ -n "${INSTANCE_B_ID:-}" ]; then
            aws ec2 wait instance-terminated --instance-ids "$INSTANCE_B_ID" --region "$REGION_B" 2>/dev/null || true
        fi
        aws ec2 delete-security-group --group-id "$SG_B_ID" --region "$REGION_B" 2>/dev/null || true
    fi

    aws ec2 delete-key-pair --key-name "$KEY_NAME" --region "$REGION_A" 2>/dev/null || true
    aws ec2 delete-key-pair --key-name "$KEY_NAME" --region "$REGION_B" 2>/dev/null || true
    rm -f /tmp/${KEY_NAME}.pem

    echo -e "${G}Cleanup complete.${N}"
}
trap cleanup EXIT

# ─── Create SSH key pair ─────────────────────────────────────
echo -e "${C}Creating SSH key pair...${N}"
aws ec2 create-key-pair --key-name "$KEY_NAME" --query 'KeyMaterial' --output text --region "$REGION_A" > /tmp/${KEY_NAME}.pem
chmod 600 /tmp/${KEY_NAME}.pem

# Import the same key to region B
PUBLIC_KEY=$(ssh-keygen -y -f /tmp/${KEY_NAME}.pem)
aws ec2 import-key-pair --key-name "$KEY_NAME" --public-key-material "$(echo "$PUBLIC_KEY" | base64)" --region "$REGION_B" >/dev/null

# ─── Create security groups ─────────────────────────────────
echo -e "${C}Creating security groups...${N}"

# Region A
SG_A_ID=$(aws ec2 create-security-group --group-name "$SG_NAME" --description "ByteHaul benchmark" --region "$REGION_A" --query 'GroupId' --output text)
aws ec2 authorize-security-group-ingress --group-id "$SG_A_ID" --protocol tcp --port 22 --cidr 0.0.0.0/0 --region "$REGION_A" >/dev/null
aws ec2 authorize-security-group-ingress --group-id "$SG_A_ID" --protocol udp --port 7700 --cidr 0.0.0.0/0 --region "$REGION_A" >/dev/null

# Region B
SG_B_ID=$(aws ec2 create-security-group --group-name "$SG_NAME" --description "ByteHaul benchmark" --region "$REGION_B" --query 'GroupId' --output text)
aws ec2 authorize-security-group-ingress --group-id "$SG_B_ID" --protocol tcp --port 22 --cidr 0.0.0.0/0 --region "$REGION_B" >/dev/null
aws ec2 authorize-security-group-ingress --group-id "$SG_B_ID" --protocol udp --port 7700 --cidr 0.0.0.0/0 --region "$REGION_B" >/dev/null

# ─── Launch instances ────────────────────────────────────────
echo -e "${C}Launching instances...${N}"

# User data: just install build dependencies (Rust installed later as ec2-user)
USERDATA=$(cat <<'CLOUD_INIT'
#!/bin/bash
yum install -y gcc openssl-devel git openssh-clients rsync
CLOUD_INIT
)

INSTANCE_A_ID=$(aws ec2 run-instances \
    --image-id "$AMI_A" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SG_A_ID" \
    --user-data "$USERDATA" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bytehaul-sender}]" \
    --region "$REGION_A" \
    --query 'Instances[0].InstanceId' --output text)
echo "  Sender:   $INSTANCE_A_ID ($REGION_A)"

INSTANCE_B_ID=$(aws ec2 run-instances \
    --image-id "$AMI_B" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SG_B_ID" \
    --user-data "$USERDATA" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bytehaul-receiver}]" \
    --region "$REGION_B" \
    --query 'Instances[0].InstanceId' --output text)
echo "  Receiver: $INSTANCE_B_ID ($REGION_B)"

# ─── Wait for instances ──────────────────────────────────────
echo -e "${C}Waiting for instances to be running...${N}"
aws ec2 wait instance-running --instance-ids "$INSTANCE_A_ID" --region "$REGION_A"
aws ec2 wait instance-running --instance-ids "$INSTANCE_B_ID" --region "$REGION_B"

IP_A=$(aws ec2 describe-instances --instance-ids "$INSTANCE_A_ID" --region "$REGION_A" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
IP_B=$(aws ec2 describe-instances --instance-ids "$INSTANCE_B_ID" --region "$REGION_B" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
PRIVATE_IP_B=$(aws ec2 describe-instances --instance-ids "$INSTANCE_B_ID" --region "$REGION_B" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

echo "  Sender IP:   $IP_A"
echo "  Receiver IP: $IP_B"

SSH_OPTS="-i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"

# ─── Wait for SSH + cloud-init ───────────────────────────────
echo -e "${C}Waiting for SSH and cloud-init to complete...${N}"

wait_ssh() {
    local ip=$1
    for i in $(seq 1 60); do
        if ssh -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 -o LogLevel=ERROR ec2-user@$ip "echo ready" 2>/dev/null | grep -q ready; then
            return 0
        fi
        sleep 5
    done
    echo "ERROR: Timed out waiting for $ip"
    return 1
}

wait_ssh "$IP_A"
echo "  Sender SSH ready."
wait_ssh "$IP_B"
echo "  Receiver SSH ready."

# Wait for cloud-init to finish installing gcc etc.
echo "  Waiting for cloud-init..."
ssh -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR ec2-user@$IP_A "cloud-init status --wait" >/dev/null 2>&1 || sleep 30
ssh -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR ec2-user@$IP_B "cloud-init status --wait" >/dev/null 2>&1 || sleep 30
echo "  Both instances ready."

# ─── Build ByteHaul on instances ─────────────────────────────
echo -e "${C}Uploading and building ByteHaul on both instances...${N}"

# Create a source tarball (excluding target dir)
TARBALL="/tmp/bytehaul-src.tar.gz"
cd "$PROJECT_DIR"
tar czf "$TARBALL" --exclude='target' --exclude='.git' --exclude='bench-docker' .

build_on_host() {
    local ip=$1
    local label=$2
    echo "  Building on $label ($ip)..."
    scp -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "$TARBALL" ec2-user@$ip:/tmp/bytehaul-src.tar.gz
    ssh -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR ec2-user@$ip "
        # Install Rust as ec2-user
        if ! command -v cargo &>/dev/null; then
            curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y 2>&1 | tail -1
        fi
        source ~/.cargo/env
        mkdir -p /tmp/bytehaul && cd /tmp/bytehaul
        tar xzf /tmp/bytehaul-src.tar.gz
        cargo build --release --bin bytehaul 2>&1 | tail -3
        sudo cp target/release/bytehaul /usr/local/bin/
        bytehaul --version
    "
}

build_on_host "$IP_A" "sender" &
build_on_host "$IP_B" "receiver" &
wait
echo "  Build complete on both instances."

# ─── Measure real RTT ────────────────────────────────────────
echo ""
echo -e "${C}Measuring real network conditions...${N}"
RTT=$(ssh -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR ec2-user@$IP_A "ping -c 5 $IP_B 2>/dev/null | tail -1")
echo "  Ping $REGION_A -> $REGION_B: $RTT"

# ─── Create test file on sender ──────────────────────────────
echo -e "${C}Creating test files on sender...${N}"
ssh -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR ec2-user@$IP_A "
    dd if=/dev/urandom of=/tmp/100mb.bin bs=1M count=100 2>/dev/null
    dd if=/dev/urandom of=/tmp/1gb.bin bs=1M count=1024 2>/dev/null
    echo 'Test files created'
"

# ─── Set up SSH keys between instances ────────────────────────
echo -e "${C}Setting up SSH between instances...${N}"
scp -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR /tmp/${KEY_NAME}.pem ec2-user@$IP_A:/tmp/bench-key.pem
ssh -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR ec2-user@$IP_A "chmod 600 /tmp/bench-key.pem"

# ─── Run benchmark ───────────────────────────────────────────
echo ""
echo -e "${B}=====================================${N}"
echo -e "${B}  REAL-WORLD BENCHMARK RESULTS${N}"
echo -e "${B}  $REGION_A -> $REGION_B${N}"
echo -e "${B}=====================================${N}"
echo ""

REMOTE_SSH_OPTS="-i /tmp/bench-key.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

ssh -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR ec2-user@$IP_A "
IP_B='$IP_B'
SSH_OPTS='$REMOTE_SSH_OPTS'
ITERATIONS=3

echo '--- Measuring RTT ---'
ping -c 5 \$IP_B | tail -1
echo ''

# ── ByteHaul: 100 MB ──
echo '--- bytehaul: 100 MB ---'
for i in \$(seq 1 \$ITERATIONS); do
    ssh \$SSH_OPTS ec2-user@\$IP_B 'rm -f /tmp/recv/*; mkdir -p /tmp/recv; nohup bytehaul daemon --port 7700 --dest /tmp/recv --overwrite overwrite </dev/null >/dev/null 2>&1 &'
    sleep 2
    START=\$(python3 -c 'import time; print(time.time())')
    RUST_LOG=error bytehaul send --daemon \"\${IP_B}:7700\" /tmp/100mb.bin /bench.bin >/dev/null 2>&1
    END=\$(python3 -c 'import time; print(time.time())')
    ssh \$SSH_OPTS ec2-user@\$IP_B 'pkill -f bytehaul 2>/dev/null || true'
    python3 -c \"e=\$END-\$START; print(f'  Run \$i: {e:.2f}s  ({100/e:.1f} MB/s)')\"
done

echo ''

# ── scp: 100 MB ──
echo '--- scp: 100 MB ---'
for i in \$(seq 1 \$ITERATIONS); do
    ssh \$SSH_OPTS ec2-user@\$IP_B 'rm -f /tmp/recv/*; mkdir -p /tmp/recv'
    START=\$(python3 -c 'import time; print(time.time())')
    scp -q \$SSH_OPTS /tmp/100mb.bin ec2-user@\$IP_B:/tmp/recv/bench.bin
    END=\$(python3 -c 'import time; print(time.time())')
    python3 -c \"e=\$END-\$START; print(f'  Run \$i: {e:.2f}s  ({100/e:.1f} MB/s)')\"
done

echo ''

# ── rsync: 100 MB ──
echo '--- rsync: 100 MB ---'
for i in \$(seq 1 \$ITERATIONS); do
    ssh \$SSH_OPTS ec2-user@\$IP_B 'rm -f /tmp/recv/*; mkdir -p /tmp/recv'
    START=\$(python3 -c 'import time; print(time.time())')
    rsync -e \"ssh \$SSH_OPTS\" -q /tmp/100mb.bin ec2-user@\$IP_B:/tmp/recv/bench.bin
    END=\$(python3 -c 'import time; print(time.time())')
    python3 -c \"e=\$END-\$START; print(f'  Run \$i: {e:.2f}s  ({100/e:.1f} MB/s)')\"
done

echo ''

# ── ByteHaul: 1 GB ──
echo '--- bytehaul: 1 GB ---'
for i in \$(seq 1 \$ITERATIONS); do
    ssh \$SSH_OPTS ec2-user@\$IP_B 'rm -f /tmp/recv/*; mkdir -p /tmp/recv; nohup bytehaul daemon --port 7700 --dest /tmp/recv --overwrite overwrite </dev/null >/dev/null 2>&1 &'
    sleep 2
    START=\$(python3 -c 'import time; print(time.time())')
    RUST_LOG=error bytehaul send --daemon \"\${IP_B}:7700\" /tmp/1gb.bin /bench.bin >/dev/null 2>&1
    END=\$(python3 -c 'import time; print(time.time())')
    ssh \$SSH_OPTS ec2-user@\$IP_B 'pkill -f bytehaul 2>/dev/null || true'
    python3 -c \"e=\$END-\$START; print(f'  Run \$i: {e:.2f}s  ({1024/e:.1f} MB/s)')\"
done

echo ''

# ── scp: 1 GB ──
echo '--- scp: 1 GB ---'
for i in \$(seq 1 \$ITERATIONS); do
    ssh \$SSH_OPTS ec2-user@\$IP_B 'rm -f /tmp/recv/*; mkdir -p /tmp/recv'
    START=\$(python3 -c 'import time; print(time.time())')
    scp -q \$SSH_OPTS /tmp/1gb.bin ec2-user@\$IP_B:/tmp/recv/bench.bin
    END=\$(python3 -c 'import time; print(time.time())')
    python3 -c \"e=\$END-\$START; print(f'  Run \$i: {e:.2f}s  ({1024/e:.1f} MB/s)')\"
done

echo ''
echo '=== DONE ==='
"

echo ""
echo -e "${G}${B}Benchmark complete. Tearing down infrastructure...${N}"

rm -f "$TARBALL"
