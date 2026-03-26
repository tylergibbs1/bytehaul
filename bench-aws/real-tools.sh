#!/usr/bin/env bash
set -uo pipefail

# ═══════════════════════════════════════════════════════════════
# ByteHaul vs What ML Engineers Actually Use
# ═══════════════════════════════════════════════════════════════
#
# Tests against the REAL status quo:
#   rsync -avz        — what most teams default to
#   tar | pigz | ssh  — what senior infra engineers use
#   scp -C            — scp with compression
#   rclone sftp       — what multi-cloud teams use
#   rsync --compress  — rsync with delta + compression
#
# Scenarios:
#   1. 500MB model checkpoint (random data = real FP32 weights)
#   2. 600 dataset shards (many small files)
#   3. 5GB large model
#   4. Compressible logs/configs (~10MB text)

REGION_A="us-east-2"
REGION_B="eu-west-1"
INSTANCE_TYPE="c5.xlarge"
KEY_NAME="bh-rt-$$"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

RESULTS="/tmp/bytehaul-realtools.json"
echo "[]" > "$RESULTS"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

record() {
    local exp=$1 name=$2 tool=$3 file_desc=$4 seconds=$5 size_mb=$6
    local speed
    speed=$(python3 -c "print(round($size_mb / $seconds, 2) if $seconds > 0.001 else 0)")
    python3 -c "
import json
with open('$RESULTS') as f: data = json.load(f)
data.append({'experiment': $exp, 'name': '$name', 'tool': '$tool', 'file': '$file_desc', 'seconds': round($seconds, 3), 'size_mb': $size_mb, 'speed_mbps': $speed})
with open('$RESULTS', 'w') as f: json.dump(data, f, indent=2)
"
    log "  #$exp [$tool] ${seconds}s (${speed} MB/s)"
}

cleanup() {
    log "Cleaning up..."
    [ -n "${IA:-}" ] && aws ec2 terminate-instances --instance-ids "$IA" --region "$REGION_A" --output text >/dev/null 2>&1 || true
    [ -n "${IB:-}" ] && aws ec2 terminate-instances --instance-ids "$IB" --region "$REGION_B" --output text >/dev/null 2>&1 || true
    sleep 5
    [ -n "${IA:-}" ] && aws ec2 wait instance-terminated --instance-ids "$IA" --region "$REGION_A" 2>/dev/null || true
    [ -n "${IB:-}" ] && aws ec2 wait instance-terminated --instance-ids "$IB" --region "$REGION_B" 2>/dev/null || true
    [ -n "${SA:-}" ] && aws ec2 delete-security-group --group-id "$SA" --region "$REGION_A" 2>/dev/null || true
    [ -n "${SB:-}" ] && aws ec2 delete-security-group --group-id "$SB" --region "$REGION_B" 2>/dev/null || true
    aws ec2 delete-key-pair --key-name "$KEY_NAME" --region "$REGION_A" 2>/dev/null || true
    aws ec2 delete-key-pair --key-name "$KEY_NAME" --region "$REGION_B" 2>/dev/null || true
    rm -f /tmp/${KEY_NAME}.pem /tmp/bytehaul-src.tar.gz
    cp "$RESULTS" "$PROJECT_DIR/experiment-results-realtools.json" 2>/dev/null || true
    log "Results saved to experiment-results-realtools.json"
}
trap cleanup EXIT

AMIA=$(aws ec2 describe-images --owners amazon --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' --output text --region "$REGION_A")
AMIB=$(aws ec2 describe-images --owners amazon --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' --output text --region "$REGION_B")

log "Setting up..."
aws ec2 create-key-pair --key-name "$KEY_NAME" --query KeyMaterial --output text --region "$REGION_A" > /tmp/${KEY_NAME}.pem
chmod 600 /tmp/${KEY_NAME}.pem
PUB=$(ssh-keygen -y -f /tmp/${KEY_NAME}.pem)
aws ec2 import-key-pair --key-name "$KEY_NAME" --public-key-material "$(echo "$PUB" | base64)" --region "$REGION_B" >/dev/null

SA=$(aws ec2 create-security-group --group-name "$KEY_NAME" --description bh --region "$REGION_A" --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id "$SA" --protocol -1 --cidr 0.0.0.0/0 --region "$REGION_A" >/dev/null
SB=$(aws ec2 create-security-group --group-name "$KEY_NAME" --description bh --region "$REGION_B" --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id "$SB" --protocol -1 --cidr 0.0.0.0/0 --region "$REGION_B" >/dev/null

# Install ALL the tools engineers actually use
UD='#!/bin/bash
yum install -y gcc openssl-devel rsync pigz
# Install rclone
curl -sSL https://rclone.org/install.sh | bash 2>/dev/null || true'

IA=$(aws ec2 run-instances --image-id "$AMIA" --instance-type "$INSTANCE_TYPE" --key-name "$KEY_NAME" --security-group-ids "$SA" --user-data "$UD" --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]' --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bh-rt-s}]" --region "$REGION_A" --query 'Instances[0].InstanceId' --output text)
IB=$(aws ec2 run-instances --image-id "$AMIB" --instance-type "$INSTANCE_TYPE" --key-name "$KEY_NAME" --security-group-ids "$SB" --user-data "$UD" --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]' --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bh-rt-r}]" --region "$REGION_B" --query 'Instances[0].InstanceId' --output text)

aws ec2 wait instance-running --instance-ids "$IA" --region "$REGION_A"
aws ec2 wait instance-running --instance-ids "$IB" --region "$REGION_B"
IPA=$(aws ec2 describe-instances --instance-ids "$IA" --region "$REGION_A" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
IPB=$(aws ec2 describe-instances --instance-ids "$IB" --region "$REGION_B" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
log "IPs: $IPA -> $IPB"

SS="ssh -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ServerAliveInterval=30"
SC="scp -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

for ip in $IPA $IPB; do for j in $(seq 1 60); do $SS ec2-user@$ip "echo ok" 2>/dev/null && break || sleep 5; done; done
$SS ec2-user@$IPA "cloud-init status --wait" >/dev/null 2>&1 || sleep 60
$SS ec2-user@$IPB "cloud-init status --wait" >/dev/null 2>&1 || sleep 60

log "Building ByteHaul..."
cd "$PROJECT_DIR"
tar czf /tmp/bytehaul-src.tar.gz --exclude='target' --exclude='.git' --exclude='bench-docker' --exclude='experiment-results*' .
for ip in $IPA $IPB; do
    $SC /tmp/bytehaul-src.tar.gz ec2-user@$ip:/tmp/bytehaul-src.tar.gz
    $SS ec2-user@$ip 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y 2>&1 | tail -1; source ~/.cargo/env; mkdir -p /tmp/bytehaul && cd /tmp/bytehaul && tar xzf /tmp/bytehaul-src.tar.gz; cargo build --release --bin bytehaul 2>&1 | tail -2; sudo cp target/release/bytehaul /usr/local/bin/' &
done
wait

# Configure rclone SFTP on sender
$SS ec2-user@$IPA "mkdir -p ~/.config/rclone; cat > ~/.config/rclone/rclone.conf << EOF
[gpu]
type = sftp
host = $IPB
user = ec2-user
key_file = /tmp/k.pem
known_hosts_file = /dev/null
EOF"

$SC /tmp/${KEY_NAME}.pem ec2-user@$IPA:/tmp/k.pem
$SS ec2-user@$IPA "chmod 600 /tmp/k.pem"
RK="-i /tmp/k.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

R() { $SS ec2-user@$IPA "$@"; }
RTT=$(R "ping -c 5 $IPB 2>/dev/null | grep avg | cut -d/ -f5")
log "RTT: ${RTT}ms"

# Create test data
log "Creating test data..."
R 'dd if=/dev/urandom of=/tmp/model.pt bs=1M count=500 2>/dev/null'
R 'dd if=/dev/urandom of=/tmp/big_model.pt bs=1M count=5120 2>/dev/null'
R 'mkdir -p /tmp/shards; for i in $(seq 1 600); do dd if=/dev/urandom of=/tmp/shards/s$i.tar bs=204800 count=1 2>/dev/null; done'
R 'mkdir -p /tmp/logs; for i in $(seq 1 200); do yes "epoch=$i loss=0.$(( RANDOM % 1000 )) lr=0.001 tokens/s=50000 gpu_mem=78.5GB batch=32" | head -2000 > /tmp/logs/train_$i.log 2>/dev/null; done'
log "Data ready."

N=0; nx() { N=$((N+1)); }

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  ByteHaul vs THE REAL STATUS QUO"
echo "  $REGION_A -> $REGION_B | RTT: ${RTT}ms"
echo "═══════════════════════════════════════════════════════════"
echo ""

# ═══ SCENARIO 1: 500MB model checkpoint (incompressible) ═══
log "═══ 500MB model checkpoint (FP32 weights = incompressible) ═══"

# ByteHaul (smart mode equivalent)
nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv; nohup bytehaul daemon --port 7700 --dest /tmp/rv --overwrite overwrite </dev/null >/dev/null 2>&1 &'; sleep 1; S=\$(python3 -c 'import time; print(time.time())'); RUST_LOG=error bytehaul send --daemon '$IPB:7700' /tmp/model.pt /b >/dev/null 2>&1; E=\$(python3 -c 'import time; print(time.time())'); ssh \$K ec2-user@$IPB 'pkill -f \"bytehaul daemon\" 2>/dev/null || true'; python3 -c \"print(\$E-\$S)\"") || T=-1
record $N "checkpoint-500mb" "bytehaul" "500MB" "$T" 500

# rsync -avz (THE default for most teams)
nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv'; S=\$(python3 -c 'import time; print(time.time())'); rsync -avz -e \"ssh \$K\" /tmp/model.pt ec2-user@$IPB:/tmp/rv/ >/dev/null 2>&1; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\"") || T=-1
record $N "checkpoint-500mb" "rsync-avz" "500MB" "$T" 500

# scp -C (scp with compression)
nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv'; S=\$(python3 -c 'import time; print(time.time())'); scp -C -q \$K /tmp/model.pt ec2-user@$IPB:/tmp/rv/b; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\"") || T=-1
record $N "checkpoint-500mb" "scp-C" "500MB" "$T" 500

# tar | ssh (no compression, fastest TCP pipe)
nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv'; S=\$(python3 -c 'import time; print(time.time())'); tar cf - -C /tmp model.pt | ssh \$K ec2-user@$IPB 'tar xf - -C /tmp/rv/'; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\"") || T=-1
record $N "checkpoint-500mb" "tar-ssh" "500MB" "$T" 500

# tar | pigz | ssh (parallel-compressed pipe)
nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv'; S=\$(python3 -c 'import time; print(time.time())'); tar cf - -C /tmp model.pt | pigz -1 | ssh \$K ec2-user@$IPB 'pigz -d | tar xf - -C /tmp/rv/'; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\"") || T=-1
record $N "checkpoint-500mb" "tar-pigz-ssh" "500MB" "$T" 500

# rclone over SFTP
nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv'; S=\$(python3 -c 'import time; print(time.time())'); rclone copy /tmp/model.pt gpu:/tmp/rv/ 2>/dev/null; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\"") || T=-1
record $N "checkpoint-500mb" "rclone-sftp" "500MB" "$T" 500

# ═══ SCENARIO 2: 600 dataset shards ═══
log "═══ 600 dataset shards (many small files) ═══"

nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv; nohup bytehaul daemon --port 7700 --dest /tmp/rv --overwrite overwrite </dev/null >/dev/null 2>&1 &'; sleep 1; S=\$(python3 -c 'import time; print(time.time())'); RUST_LOG=error bytehaul send -r --daemon '$IPB:7700' /tmp/shards /b >/dev/null 2>&1; E=\$(python3 -c 'import time; print(time.time())'); ssh \$K ec2-user@$IPB 'pkill -f \"bytehaul daemon\" 2>/dev/null || true'; python3 -c \"print(\$E-\$S)\"") || T=-1
record $N "shards-600" "bytehaul" "600x200KB" "$T" 117

nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv'; S=\$(python3 -c 'import time; print(time.time())'); rsync -avz -e \"ssh \$K\" /tmp/shards ec2-user@$IPB:/tmp/rv/ >/dev/null 2>&1; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\"") || T=-1
record $N "shards-600" "rsync-avz" "600x200KB" "$T" 117

nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv'; S=\$(python3 -c 'import time; print(time.time())'); tar cf - -C /tmp shards | ssh \$K ec2-user@$IPB 'tar xf - -C /tmp/rv/'; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\"") || T=-1
record $N "shards-600" "tar-ssh" "600x200KB" "$T" 117

nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv'; S=\$(python3 -c 'import time; print(time.time())'); rclone copy /tmp/shards gpu:/tmp/rv/shards 2>/dev/null; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\"") || T=-1
record $N "shards-600" "rclone-sftp" "600x200KB" "$T" 117

# ═══ SCENARIO 3: 5GB large model ═══
log "═══ 5GB large model ═══"

nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv; nohup bytehaul daemon --port 7700 --dest /tmp/rv --overwrite overwrite </dev/null >/dev/null 2>&1 &'; sleep 1; S=\$(python3 -c 'import time; print(time.time())'); RUST_LOG=error bytehaul send --daemon '$IPB:7700' /tmp/big_model.pt /b >/dev/null 2>&1; E=\$(python3 -c 'import time; print(time.time())'); ssh \$K ec2-user@$IPB 'pkill -f \"bytehaul daemon\" 2>/dev/null || true'; python3 -c \"print(\$E-\$S)\"") || T=-1
record $N "large-model-5gb" "bytehaul" "5GB" "$T" 5120

nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv'; S=\$(python3 -c 'import time; print(time.time())'); rsync -av -e \"ssh \$K\" /tmp/big_model.pt ec2-user@$IPB:/tmp/rv/ >/dev/null 2>&1; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\"") || T=-1
record $N "large-model-5gb" "rsync-av" "5GB" "$T" 5120

nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv'; S=\$(python3 -c 'import time; print(time.time())'); tar cf - -C /tmp big_model.pt | ssh \$K ec2-user@$IPB 'tar xf - -C /tmp/rv/'; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\"") || T=-1
record $N "large-model-5gb" "tar-ssh" "5GB" "$T" 5120

nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv'; S=\$(python3 -c 'import time; print(time.time())'); scp -q \$K /tmp/big_model.pt ec2-user@$IPB:/tmp/rv/b; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\"") || T=-1
record $N "large-model-5gb" "scp" "5GB" "$T" 5120

# ═══ SCENARIO 4: Compressible training logs ═══
log "═══ Compressible training logs (~40MB text) ═══"

nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv; nohup bytehaul daemon --port 7700 --dest /tmp/rv --overwrite overwrite </dev/null >/dev/null 2>&1 &'; sleep 1; S=\$(python3 -c 'import time; print(time.time())'); RUST_LOG=error bytehaul send -r --compress --daemon '$IPB:7700' /tmp/logs /b >/dev/null 2>&1; E=\$(python3 -c 'import time; print(time.time())'); ssh \$K ec2-user@$IPB 'pkill -f \"bytehaul daemon\" 2>/dev/null || true'; python3 -c \"print(\$E-\$S)\"") || T=-1
record $N "logs-compressed" "bytehaul-zstd" "200x-logs" "$T" 40

nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv'; S=\$(python3 -c 'import time; print(time.time())'); rsync -avz -e \"ssh \$K\" /tmp/logs ec2-user@$IPB:/tmp/rv/ >/dev/null 2>&1; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\"") || T=-1
record $N "logs-compressed" "rsync-avz" "200x-logs" "$T" 40

nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv'; S=\$(python3 -c 'import time; print(time.time())'); tar cf - -C /tmp logs | pigz -1 | ssh \$K ec2-user@$IPB 'pigz -d | tar xf - -C /tmp/rv/'; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\"") || T=-1
record $N "logs-compressed" "tar-pigz-ssh" "200x-logs" "$T" 40

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  ALL EXPERIMENTS COMPLETE"
echo "═══════════════════════════════════════════════════════════"

python3 -c "
import json
with open('$RESULTS') as f: data = json.load(f)
print()
print(f\"{'#':>3} {'Scenario':<28} {'Tool':<20} {'File':<15} {'Secs':>8} {'MB/s':>8}\")
print('─' * 90)
for r in data:
    print(f\"{r['experiment']:>3} {r['name']:<28} {r['tool']:<20} {r['file']:<15} {r['seconds']:>8} {r['speed_mbps']:>8}\")
"
