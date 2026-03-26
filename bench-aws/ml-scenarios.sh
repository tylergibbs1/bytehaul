#!/usr/bin/env bash
set -uo pipefail

# ═══════════════════════════════════════════════════════════════
# ByteHaul ML Engineer Scenarios — Real AWS Validation
# ═══════════════════════════════════════════════════════════════
#
# 15 experiments that simulate actual ML/AI workflows:
# 1-3:  Model checkpoint transfer (PyTorch-like .pt files)
# 4-6:  Dataset staging (many small files + large archives)
# 7-9:  Checkpoint sync with delta (modify and re-send)
# 10-12: Pull results from GPU node
# 13:   Compressible data (JSON configs, text logs)
# 14:   Fan-out to multiple nodes
# 15:   Large sustained transfer (5GB model)

REGION_A="us-east-2"
REGION_B="eu-west-1"
INSTANCE_TYPE="c5.xlarge"
KEY_NAME="bh-ml-$$"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

RESULTS="/tmp/bytehaul-ml.json"
echo "[]" > "$RESULTS"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

record() {
    local exp=$1 name=$2 tool=$3 file_desc=$4 seconds=$5 size_mb=$6 extra=${7:-}
    local speed
    speed=$(python3 -c "print(round($size_mb / $seconds, 2) if $seconds > 0.001 else 0)")
    python3 -c "
import json
with open('$RESULTS') as f: data = json.load(f)
data.append({'experiment': $exp, 'name': '$name', 'tool': '$tool', 'file': '$file_desc', 'seconds': round($seconds, 3), 'size_mb': $size_mb, 'speed_mbps': $speed, 'extra': '$extra'})
with open('$RESULTS', 'w') as f: json.dump(data, f, indent=2)
"
    log "  #$exp [$tool] ${seconds}s (${speed} MB/s) $extra"
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
    cp "$RESULTS" "$PROJECT_DIR/experiment-results-ml.json" 2>/dev/null || true
    log "Results saved to experiment-results-ml.json"
}
trap cleanup EXIT

AMIA=$(aws ec2 describe-images --owners amazon --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' --output text --region "$REGION_A")
AMIB=$(aws ec2 describe-images --owners amazon --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' --output text --region "$REGION_B")

log "Setting up infrastructure..."
aws ec2 create-key-pair --key-name "$KEY_NAME" --query KeyMaterial --output text --region "$REGION_A" > /tmp/${KEY_NAME}.pem
chmod 600 /tmp/${KEY_NAME}.pem
PUB=$(ssh-keygen -y -f /tmp/${KEY_NAME}.pem)
aws ec2 import-key-pair --key-name "$KEY_NAME" --public-key-material "$(echo "$PUB" | base64)" --region "$REGION_B" >/dev/null

SA=$(aws ec2 create-security-group --group-name "$KEY_NAME" --description bh --region "$REGION_A" --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id "$SA" --protocol -1 --cidr 0.0.0.0/0 --region "$REGION_A" >/dev/null
SB=$(aws ec2 create-security-group --group-name "$KEY_NAME" --description bh --region "$REGION_B" --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id "$SB" --protocol -1 --cidr 0.0.0.0/0 --region "$REGION_B" >/dev/null

UD='#!/bin/bash
yum install -y gcc openssl-devel rsync'

IA=$(aws ec2 run-instances --image-id "$AMIA" --instance-type "$INSTANCE_TYPE" --key-name "$KEY_NAME" --security-group-ids "$SA" --user-data "$UD" --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]' --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bh-ml-s}]" --region "$REGION_A" --query 'Instances[0].InstanceId' --output text)
IB=$(aws ec2 run-instances --image-id "$AMIB" --instance-type "$INSTANCE_TYPE" --key-name "$KEY_NAME" --security-group-ids "$SB" --user-data "$UD" --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]' --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bh-ml-r}]" --region "$REGION_B" --query 'Instances[0].InstanceId' --output text)
log "Instances: $IA / $IB"

aws ec2 wait instance-running --instance-ids "$IA" --region "$REGION_A"
aws ec2 wait instance-running --instance-ids "$IB" --region "$REGION_B"
IPA=$(aws ec2 describe-instances --instance-ids "$IA" --region "$REGION_A" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
IPB=$(aws ec2 describe-instances --instance-ids "$IB" --region "$REGION_B" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
log "IPs: $IPA -> $IPB"

SS="ssh -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ServerAliveInterval=30"
SC="scp -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

for ip in $IPA $IPB; do for j in $(seq 1 60); do $SS ec2-user@$ip "echo ok" 2>/dev/null && break || sleep 5; done; done
$SS ec2-user@$IPA "cloud-init status --wait" >/dev/null 2>&1 || sleep 30
$SS ec2-user@$IPB "cloud-init status --wait" >/dev/null 2>&1 || sleep 30

log "Building ByteHaul..."
cd "$PROJECT_DIR"
tar czf /tmp/bytehaul-src.tar.gz --exclude='target' --exclude='.git' --exclude='bench-docker' --exclude='experiment-results*' .
for ip in $IPA $IPB; do
    $SC /tmp/bytehaul-src.tar.gz ec2-user@$ip:/tmp/bytehaul-src.tar.gz
    $SS ec2-user@$ip 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y 2>&1 | tail -1; source ~/.cargo/env; mkdir -p /tmp/bytehaul && cd /tmp/bytehaul && tar xzf /tmp/bytehaul-src.tar.gz; cargo build --release --bin bytehaul 2>&1 | tail -2; sudo cp target/release/bytehaul /usr/local/bin/' &
done
wait
log "Build complete."

$SC /tmp/${KEY_NAME}.pem ec2-user@$IPA:/tmp/k.pem
$SS ec2-user@$IPA "chmod 600 /tmp/k.pem"
RK="-i /tmp/k.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

R() { $SS ec2-user@$IPA "$@"; }

RTT=$(R "ping -c 5 $IPB 2>/dev/null | grep avg | cut -d/ -f5")
log "RTT: ${RTT}ms"

# Create ML-realistic test files
log "Creating ML-realistic test data..."

# Simulated model checkpoints (random data = incompressible like real weights)
R 'dd if=/dev/urandom of=/tmp/checkpoint_step1000.pt bs=1M count=500 2>/dev/null'
R 'dd if=/dev/urandom of=/tmp/checkpoint_step2000.pt bs=1M count=500 2>/dev/null'
R 'dd if=/dev/urandom of=/tmp/large_model.pt bs=1M count=5120 2>/dev/null'

# Simulated training dataset directory (many small files like ImageNet shards)
R 'mkdir -p /tmp/dataset/train /tmp/dataset/val /tmp/dataset/metadata
for i in $(seq 1 500); do dd if=/dev/urandom of=/tmp/dataset/train/shard_$i.tar bs=204800 count=1 2>/dev/null; done
for i in $(seq 1 100); do dd if=/dev/urandom of=/tmp/dataset/val/shard_$i.tar bs=204800 count=1 2>/dev/null; done'

# Compressible ML artifacts (JSON configs, CSV metrics, text logs)
R 'mkdir -p /tmp/ml_artifacts/configs /tmp/ml_artifacts/logs /tmp/ml_artifacts/metrics
for i in $(seq 1 50); do python3 -c "
import json
cfg = {\"lr\": 0.001*$i, \"epochs\": 100, \"model\": \"llama-7b\", \"batch_size\": 32, \"layers\": list(range(32)), \"vocab\": list(range(32000))}
open(\"/tmp/ml_artifacts/configs/config_$i.json\", \"w\").write(json.dumps(cfg, indent=2))
" 2>/dev/null; done
for i in $(seq 1 100); do yes "step=$i loss=0.$(( RANDOM % 1000 )) lr=0.001 grad_norm=1.5 tokens_per_sec=50000" | head -1000 > /tmp/ml_artifacts/logs/train_$i.log 2>/dev/null; done
for i in $(seq 1 20); do python3 -c "
import csv,random
with open(\"/tmp/ml_artifacts/metrics/eval_$i.csv\",\"w\") as f:
    w=csv.writer(f); w.writerow([\"step\",\"loss\",\"accuracy\",\"f1\"])
    for s in range(0,10000,10): w.writerow([s,random.random(),random.random(),random.random()])
" 2>/dev/null; done'

log "Test data created."

bh() {
    local x="$1" f="$2" p="${3:-7700}"
    R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv; nohup bytehaul daemon --port $p --dest /tmp/rv --overwrite overwrite </dev/null >/dev/null 2>&1 &'; sleep 1; S=\$(python3 -c 'import time; print(time.time())'); RUST_LOG=error bytehaul send --daemon '$IPB:$p' $x $f /b >/dev/null 2>&1; E=\$(python3 -c 'import time; print(time.time())'); ssh \$K ec2-user@$IPB 'pkill -f \"bytehaul daemon\" 2>/dev/null || true'; python3 -c \"print(\$E-\$S)\""
}

sc() {
    local f="$1"
    R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv'; S=\$(python3 -c 'import time; print(time.time())'); scp -q \$K $f ec2-user@$IPB:/tmp/rv/b; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\""
}

N=0; nx() { N=$((N+1)); }

echo ""
echo "═══════════════════════════════════════════════════"
echo "  ML ENGINEER SCENARIOS — $REGION_A -> $REGION_B"
echo "  RTT: ${RTT}ms | Instance: $INSTANCE_TYPE"
echo "═══════════════════════════════════════════════════"
echo ""

# ═══ 1-3: Model checkpoint transfer (500MB .pt file) ═══
log "═══ 1-3: Model checkpoint transfer (500MB) ═══"
nx; T=$(bh "" "/tmp/checkpoint_step1000.pt") || T=0;  record $N "checkpoint-500mb" "bytehaul" "500MB-checkpoint" "$T" 500
nx; T=$(sc "/tmp/checkpoint_step1000.pt") || T=0;      record $N "checkpoint-500mb" "scp" "500MB-checkpoint" "$T" 500
nx; T=$(bh "--compress" "/tmp/checkpoint_step1000.pt") || T=0; record $N "checkpoint-500mb-compressed" "bytehaul-zstd" "500MB-checkpoint" "$T" 500

# ═══ 4-6: Dataset staging (600 files, ~120MB) ═══
log "═══ 4-6: Dataset staging (600 shards) ═══"
nx; T=$(bh "-r" "/tmp/dataset") || T=0;                record $N "dataset-600files" "bytehaul" "600x200KB" "$T" 117
nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv'; S=\$(python3 -c 'import time; print(time.time())'); rsync -a -e \"ssh \$K\" /tmp/dataset ec2-user@$IPB:/tmp/rv/; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\"") || T=0
record $N "dataset-600files" "rsync" "600x200KB" "$T" 117
nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv'; S=\$(python3 -c 'import time; print(time.time())'); tar cf - /tmp/dataset 2>/dev/null | ssh \$K ec2-user@$IPB 'tar xf - -C /tmp/rv/' 2>/dev/null; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\"") || T=0
record $N "dataset-600files" "tar-ssh" "600x200KB" "$T" 117

# ═══ 7-9: Checkpoint sync with delta ═══
log "═══ 7-9: Delta sync (update 10% of checkpoint) ═══"
# First: send baseline
bh "" "/tmp/checkpoint_step1000.pt" >/dev/null
# Modify 10% (simulate next training step updating some weights)
R 'dd if=/dev/urandom of=/tmp/checkpoint_step1000.pt bs=1M count=50 conv=notrunc 2>/dev/null'
nx; T=$(bh "--delta" "/tmp/checkpoint_step1000.pt") || T=0; record $N "delta-sync-10pct" "bytehaul-delta" "500MB-10pct-changed" "$T" 500
# Full re-send for comparison
nx; T=$(bh "" "/tmp/checkpoint_step1000.pt") || T=0;          record $N "full-resend" "bytehaul" "500MB-full" "$T" 500
nx; T=$(sc "/tmp/checkpoint_step1000.pt") || T=0;              record $N "full-resend" "scp" "500MB-full" "$T" 500

# ═══ 10-12: Pull results from GPU node ═══
log "═══ 10-12: Pull results from remote ═══"
# Place a file on the receiver to pull back
R "K='$RK'; ssh \$K ec2-user@$IPB 'dd if=/dev/urandom of=/tmp/results.pt bs=1M count=100 2>/dev/null'"
nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv; nohup bytehaul daemon --port 7700 --dest /tmp/rv --overwrite overwrite </dev/null >/dev/null 2>&1 &'; sleep 1; S=\$(python3 -c 'import time; print(time.time())'); RUST_LOG=error bytehaul pull --daemon '$IPB:7700' /tmp/results.pt /tmp/pulled_results.pt >/dev/null 2>&1; E=\$(python3 -c 'import time; print(time.time())'); ssh \$K ec2-user@$IPB 'pkill -f \"bytehaul daemon\" 2>/dev/null || true'; python3 -c \"print(\$E-\$S)\"") || T=0
record $N "pull-100mb" "bytehaul-pull" "100MB-pull" "$T" 100
nx; T=$(R "K='$RK'; S=\$(python3 -c 'import time; print(time.time())'); scp -q \$K ec2-user@$IPB:/tmp/results.pt /tmp/pulled_scp.pt; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\"") || T=0
record $N "pull-100mb" "scp-pull" "100MB-pull" "$T" 100

# Place a directory on receiver
R "K='$RK'; ssh \$K ec2-user@$IPB 'mkdir -p /tmp/run_results; for i in \$(seq 1 50); do dd if=/dev/urandom of=/tmp/run_results/eval_\$i.pt bs=1M count=2 2>/dev/null; done'"
nx; T=$(R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv; nohup bytehaul daemon --port 7700 --dest /tmp/rv --overwrite overwrite </dev/null >/dev/null 2>&1 &'; sleep 1; S=\$(python3 -c 'import time; print(time.time())'); RUST_LOG=error bytehaul pull -r --daemon '$IPB:7700' /tmp/run_results /tmp/pulled_dir >/dev/null 2>&1; E=\$(python3 -c 'import time; print(time.time())'); ssh \$K ec2-user@$IPB 'pkill -f \"bytehaul daemon\" 2>/dev/null || true'; python3 -c \"print(\$E-\$S)\"") || T=0
record $N "pull-dir-50files" "bytehaul-pull-r" "50x2MB-pull" "$T" 100

# ═══ 13: Compressible ML artifacts ═══
log "═══ 13: Compressible artifacts (configs+logs+metrics) ═══"
nx; T=$(bh "-r --compress" "/tmp/ml_artifacts") || T=0; record $N "artifacts-compressed" "bytehaul-zstd" "configs+logs+metrics" "$T" 10
# Without compression for comparison
# (skip, takes same time since these are tiny)

# ═══ 14: Fan-out to simulated cluster ═══
log "═══ 14: Simulated fan-out (same dest, sequential) ═══"
# Can't do true fan-out with 2 instances, but measure the single-dest overhead
nx; T=$(bh "" "/tmp/checkpoint_step1000.pt") || T=0; record $N "fanout-single" "bytehaul" "500MB-single-dest" "$T" 500

# ═══ 15: Large model transfer (5GB) ═══
log "═══ 15: Large model transfer (5GB) ═══"
nx; T=$(bh "" "/tmp/large_model.pt") || T=0; record $N "large-model-5gb" "bytehaul" "5GB-model" "$T" 5120
# scp for comparison (will take a while)
nx; T=$(sc "/tmp/large_model.pt") || T=0;    record $N "large-model-5gb" "scp" "5GB-model" "$T" 5120

echo ""
echo "═══════════════════════════════════════════════════"
echo "  ALL 15 EXPERIMENTS COMPLETE"
echo "═══════════════════════════════════════════════════"

python3 -c "
import json
with open('$RESULTS') as f: data = json.load(f)
print()
print(f\"{'#':>3} {'Name':<28} {'Tool':<20} {'File':<22} {'Secs':>8} {'MB/s':>8}\")
print('─' * 95)
for r in data:
    print(f\"{r['experiment']:>3} {r['name']:<28} {r['tool']:<20} {r['file']:<22} {r['seconds']:>8} {r['speed_mbps']:>8}\")
"
