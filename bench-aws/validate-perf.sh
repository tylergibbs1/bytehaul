#!/usr/bin/env bash
set -uo pipefail

# Quick validation of performance improvements on real AWS
# Compares ByteHaul vs scp at 100MB, 1GB, 10GB + directory
# Ohio -> Ireland (~85ms RTT)

REGION_A="us-east-2"
REGION_B="eu-west-1"
INSTANCE_TYPE="c5.xlarge"
KEY_NAME="bh-val-$$"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

cleanup() {
    log "Cleaning up..."
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
    log "Done."
}
trap cleanup EXIT

AMI_A=$(aws ec2 describe-images --owners amazon --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' --output text --region "$REGION_A")
AMI_B=$(aws ec2 describe-images --owners amazon --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' --output text --region "$REGION_B")

log "Setting up infrastructure..."
aws ec2 create-key-pair --key-name "$KEY_NAME" --query KeyMaterial --output text --region "$REGION_A" > /tmp/${KEY_NAME}.pem
chmod 600 /tmp/${KEY_NAME}.pem
PUB=$(ssh-keygen -y -f /tmp/${KEY_NAME}.pem)
aws ec2 import-key-pair --key-name "$KEY_NAME" --public-key-material "$(echo "$PUB" | base64)" --region "$REGION_B" >/dev/null

SG_A=$(aws ec2 create-security-group --group-name "$KEY_NAME" --description "bh" --region "$REGION_A" --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id "$SG_A" --protocol -1 --cidr 0.0.0.0/0 --region "$REGION_A" >/dev/null
SG_B=$(aws ec2 create-security-group --group-name "$KEY_NAME" --description "bh" --region "$REGION_B" --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id "$SG_B" --protocol -1 --cidr 0.0.0.0/0 --region "$REGION_B" >/dev/null

USERDATA='#!/bin/bash
yum install -y gcc openssl-devel rsync'

INST_A=$(aws ec2 run-instances --image-id "$AMI_A" --instance-type "$INSTANCE_TYPE" --key-name "$KEY_NAME" --security-group-ids "$SG_A" --user-data "$USERDATA" --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]' --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bh-val-send}]" --region "$REGION_A" --query 'Instances[0].InstanceId' --output text)
INST_B=$(aws ec2 run-instances --image-id "$AMI_B" --instance-type "$INSTANCE_TYPE" --key-name "$KEY_NAME" --security-group-ids "$SG_B" --user-data "$USERDATA" --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]' --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bh-val-recv}]" --region "$REGION_B" --query 'Instances[0].InstanceId' --output text)

aws ec2 wait instance-running --instance-ids "$INST_A" --region "$REGION_A"
aws ec2 wait instance-running --instance-ids "$INST_B" --region "$REGION_B"

IP_A=$(aws ec2 describe-instances --instance-ids "$INST_A" --region "$REGION_A" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
IP_B=$(aws ec2 describe-instances --instance-ids "$INST_B" --region "$REGION_B" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
log "Sender: $IP_A | Receiver: $IP_B"

SSH="ssh -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ServerAliveInterval=30"
SCP_CMD="scp -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

for ip in $IP_A $IP_B; do
    for j in $(seq 1 60); do $SSH ec2-user@$ip "echo ok" 2>/dev/null && break || sleep 5; done
done
$SSH ec2-user@$IP_A "cloud-init status --wait" >/dev/null 2>&1 || sleep 30
$SSH ec2-user@$IP_B "cloud-init status --wait" >/dev/null 2>&1 || sleep 30

log "Building ByteHaul..."
cd "$PROJECT_DIR"
tar czf /tmp/bytehaul-src.tar.gz --exclude='target' --exclude='.git' --exclude='bench-docker' --exclude='experiment-results*' .
for ip in $IP_A $IP_B; do
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

$SCP_CMD /tmp/${KEY_NAME}.pem ec2-user@$IP_A:/tmp/bench-key.pem
$SSH ec2-user@$IP_A "chmod 600 /tmp/bench-key.pem"

RK="-i /tmp/bench-key.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
S() { $SSH ec2-user@$IP_A "$@"; }

# Create test files
log "Creating test files..."
S 'dd if=/dev/urandom of=/tmp/100mb.bin bs=1M count=100 2>/dev/null'
S 'dd if=/dev/urandom of=/tmp/1gb.bin bs=1M count=1024 2>/dev/null'
S 'dd if=/dev/urandom of=/tmp/10gb.bin bs=1M count=10240 2>/dev/null'
S 'mkdir -p /tmp/dir1000; for i in $(seq 1 1000); do dd if=/dev/urandom of=/tmp/dir1000/f$i.bin bs=102400 count=1 2>/dev/null; done'

RTT=$(S "ping -c 5 $IP_B 2>/dev/null | grep avg | cut -d/ -f5")
log "RTT: ${RTT}ms"

echo ""
echo "════════════════════════════════════════════════════════"
echo "  PERFORMANCE VALIDATION: $REGION_A -> $REGION_B"
echo "  Instance: $INSTANCE_TYPE | RTT: ${RTT}ms"
echo "════════════════════════════════════════════════════════"
echo ""

S "
K='$RK'
IP_B='$IP_B'

bh() {
    local extra=\"\$1\"; local file=\"\$2\"
    ssh \$K ec2-user@\$IP_B 'rm -rf /tmp/recv; mkdir -p /tmp/recv; nohup bytehaul daemon --port 7700 --dest /tmp/recv --overwrite overwrite </dev/null >/dev/null 2>&1 &'
    sleep 1
    START=\$(python3 -c 'import time; print(time.time())')
    RUST_LOG=error bytehaul send --daemon \"\${IP_B}:7700\" \$extra \$file /bench.bin >/dev/null 2>&1
    END=\$(python3 -c 'import time; print(time.time())')
    ssh \$K ec2-user@\$IP_B 'pkill -f \"bytehaul daemon\" 2>/dev/null || true'
    python3 -c \"e=\$END-\$START; print(f'{e:.2f}s')\"
}

do_scp() {
    local file=\"\$1\"
    ssh \$K ec2-user@\$IP_B 'rm -rf /tmp/recv; mkdir -p /tmp/recv'
    START=\$(python3 -c 'import time; print(time.time())')
    scp -q \$K \$file ec2-user@\$IP_B:/tmp/recv/bench.bin
    END=\$(python3 -c 'import time; print(time.time())')
    python3 -c \"e=\$END-\$START; print(f'{e:.2f}s')\"
}

echo '── 100 MB (3 runs each) ──'
echo 'bytehaul:'
for i in 1 2 3; do printf '  Run \$i: '; bh '' /tmp/100mb.bin; done
echo 'scp:'
for i in 1 2 3; do printf '  Run \$i: '; do_scp /tmp/100mb.bin; done

echo ''
echo '── 1 GB (3 runs each) ──'
echo 'bytehaul:'
for i in 1 2 3; do printf '  Run \$i: '; bh '' /tmp/1gb.bin; done
echo 'scp:'
for i in 1 2 3; do printf '  Run \$i: '; do_scp /tmp/1gb.bin; done

echo ''
echo '── 10 GB ──'
echo 'bytehaul:'
printf '  Run 1: '; bh '' /tmp/10gb.bin
echo 'scp:'
printf '  Run 1: '; do_scp /tmp/10gb.bin

echo ''
echo '── 1000 files (97 MB) ──'
echo 'bytehaul -r:'
for i in 1 2 3; do
    ssh \$K ec2-user@\$IP_B 'rm -rf /tmp/recv; mkdir -p /tmp/recv; nohup bytehaul daemon --port 7700 --dest /tmp/recv --overwrite overwrite </dev/null >/dev/null 2>&1 &'
    sleep 1
    START=\$(python3 -c 'import time; print(time.time())')
    RUST_LOG=error bytehaul send -r --daemon \"\${IP_B}:7700\" /tmp/dir1000 /dir1000 >/dev/null 2>&1
    END=\$(python3 -c 'import time; print(time.time())')
    ssh \$K ec2-user@\$IP_B 'pkill -f \"bytehaul daemon\" 2>/dev/null || true'
    python3 -c \"e=\$END-\$START; print(f'  Run \$i: {e:.2f}s')\"
done

echo ''
echo '── Rate limiting (100 MB at 50 Mbps) ──'
printf 'bytehaul --max-rate 50mbps: '; bh '--max-rate 50mbps' /tmp/100mb.bin

echo ''
echo '════ DONE ════'
" 2>/dev/null
