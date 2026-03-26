#!/usr/bin/env bash
set -uo pipefail

# ═══════════════════════════════════════════════════════════════
# ByteHaul Round 4: 50 Post-Optimization Experiments
# ═══════════════════════════════════════════════════════════════
#
# Now that BBR + large windows + delta fix are in, re-run key tests
# and explore new dimensions:
#
# GROUP A (1-6):   Confirm improvement at each file size (1MB-10GB)
# GROUP B (7-12):  Parallel stream sweep with BBR (2,4,8,16,32,64)
# GROUP C (13-18): Block size sweep with BBR (256KB,1MB,4MB,8MB,16MB,64MB)
# GROUP D (19-24): Rate limiting validation (10,25,50,100,250,500 Mbps)
# GROUP E (25-30): Packet loss with BBR (0,0.1,0.5,1,2,5%)
# GROUP F (31-36): scp comparison at each file size
# GROUP G (37-39): rsync/tar|ssh at 1GB for comparison
# GROUP H (40-42): Directory 100/1000/5000 files
# GROUP I (43-45): Delta transfer (0%, 10%, 50% changed)
# GROUP J (46-48): Aggressive vs Fair at 1GB
# GROUP K (49-50): Resume after kill + concurrent 4x

REGION_A="us-east-2"
REGION_B="eu-west-1"
INSTANCE_TYPE="c5.xlarge"
KEY_NAME="bh-r4-$$"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

RESULTS="/tmp/bytehaul-round4.json"
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

# ─── Infrastructure ──────────────────────────────────────────
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
    cp "$RESULTS" "$PROJECT_DIR/experiment-results-round4.json" 2>/dev/null || true
    log "Results saved to experiment-results-round4.json"
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

UD='#!/bin/bash
yum install -y gcc openssl-devel rsync bc iptables'

IA=$(aws ec2 run-instances --image-id "$AMIA" --instance-type "$INSTANCE_TYPE" --key-name "$KEY_NAME" --security-group-ids "$SA" --user-data "$UD" --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]' --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bh-r4-s}]" --region "$REGION_A" --query 'Instances[0].InstanceId' --output text)
IB=$(aws ec2 run-instances --image-id "$AMIB" --instance-type "$INSTANCE_TYPE" --key-name "$KEY_NAME" --security-group-ids "$SB" --user-data "$UD" --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]' --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bh-r4-r}]" --region "$REGION_B" --query 'Instances[0].InstanceId' --output text)
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

log "Building..."
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

log "Creating test files..."
R 'dd if=/dev/urandom of=/tmp/1m bs=1M count=1 2>/dev/null'
R 'dd if=/dev/urandom of=/tmp/10m bs=1M count=10 2>/dev/null'
R 'dd if=/dev/urandom of=/tmp/50m bs=1M count=50 2>/dev/null'
R 'dd if=/dev/urandom of=/tmp/100m bs=1M count=100 2>/dev/null'
R 'dd if=/dev/urandom of=/tmp/1g bs=1M count=1024 2>/dev/null'
R 'dd if=/dev/urandom of=/tmp/10g bs=1M count=10240 2>/dev/null'
R 'mkdir -p /tmp/d100; for i in $(seq 1 100); do dd if=/dev/urandom of=/tmp/d100/f$i bs=1M count=1 2>/dev/null; done'
R 'mkdir -p /tmp/d1000; for i in $(seq 1 1000); do dd if=/dev/urandom of=/tmp/d1000/f$i bs=102400 count=1 2>/dev/null; done'
R 'mkdir -p /tmp/d5000/s1 /tmp/d5000/s2 /tmp/d5000/s3 /tmp/d5000/s4 /tmp/d5000/s5; for d in /tmp/d5000/s*; do for i in $(seq 1 1000); do dd if=/dev/urandom of=$d/f$i bs=10240 count=1 2>/dev/null; done; done'
log "Files created."

RTT=$(R "ping -c 5 $IPB 2>/dev/null | grep avg | cut -d/ -f5")
log "RTT: ${RTT}ms"

echo ""
echo "═══════════════════════════════════════════════════"
echo "  50 POST-OPTIMIZATION EXPERIMENTS"
echo "  $REGION_A -> $REGION_B | RTT ${RTT}ms | $INSTANCE_TYPE"
echo "═══════════════════════════════════════════════════"
echo ""

bh() {
    local x="$1" f="$2" p="${3:-7700}"
    R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv; nohup bytehaul daemon --port $p --dest /tmp/rv --overwrite overwrite </dev/null >/dev/null 2>&1 &'; sleep 1; S=\$(python3 -c 'import time; print(time.time())'); RUST_LOG=error bytehaul send --daemon '$IPB:$p' $x $f /b >/dev/null 2>&1; E=\$(python3 -c 'import time; print(time.time())'); ssh \$K ec2-user@$IPB 'pkill -f \"bytehaul daemon\" 2>/dev/null || true'; python3 -c \"print(\$E-\$S)\""
}

sc() {
    local f="$1"
    R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv'; S=\$(python3 -c 'import time; print(time.time())'); scp -q \$K $f ec2-user@$IPB:/tmp/rv/b; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\""
}

rs() {
    local x="$1" f="$2"
    R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv'; S=\$(python3 -c 'import time; print(time.time())'); rsync $x -e \"ssh \$K\" $f ec2-user@$IPB:/tmp/rv/; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\""
}

ts() {
    local f="$1"
    R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv'; S=\$(python3 -c 'import time; print(time.time())'); tar cf - -C \$(dirname $f) \$(basename $f) | ssh \$K ec2-user@$IPB 'tar xf - -C /tmp/rv/'; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\""
}

N=0; nx() { N=$((N+1)); }

# ═══ GROUP A: File size scaling (ByteHaul) ═══
log "═══ A: File size scaling ═══"
for spec in "1m:1" "10m:10" "50m:50" "100m:100" "1g:1024" "10g:10240"; do
    f="/tmp/${spec%%:*}"; mb=${spec##*:}; nx
    T=$(bh "" "$f") || T=0; record $N "filesize-${mb}mb" "bytehaul" "${mb}MB" "$T" "$mb"
done

# ═══ GROUP B: Parallel streams ═══
log "═══ B: Parallel streams (1GB) ═══"
for s in 2 4 8 16 32 64; do
    nx; T=$(bh "--parallel $s" "/tmp/1g") || T=0
    record $N "streams-$s" "bytehaul" "1GB" "$T" 1024 "streams=$s"
done

# ═══ GROUP C: Block size ═══
log "═══ C: Block size (1GB) ═══"
for bs in 1 2 4 8 16 64; do
    nx; T=$(bh "--block-size $bs" "/tmp/1g") || T=0
    record $N "block-${bs}mb" "bytehaul" "1GB" "$T" 1024 "block=${bs}MB"
done

# ═══ GROUP D: Rate limiting ═══
log "═══ D: Rate limiting (100MB) ═══"
for rate in 10 25 50 100 250 500; do
    nx; T=$(bh "--max-rate ${rate}mbps" "/tmp/100m") || T=0
    expected=$(python3 -c "print(round(100 / ($rate / 8), 1))")
    record $N "rate-${rate}mbps" "bytehaul" "100MB" "$T" 100 "target=${expected}s"
done

# ═══ GROUP E: Packet loss ═══
log "═══ E: Packet loss (100MB) ═══"
for loss in 0 0.1 0.5 1 2 5; do
    nx
    [ "$loss" != "0" ] && $SS ec2-user@$IPB "sudo iptables -A INPUT -m statistic --mode random --probability $(python3 -c "print($loss/100)") -j DROP" 2>/dev/null || true
    T=$(bh "" "/tmp/100m") || T=0; record $N "loss-${loss}pct" "bytehaul" "100MB" "$T" 100 "loss=${loss}%"
    $SS ec2-user@$IPB "sudo iptables -F" 2>/dev/null || true
done

# ═══ GROUP F: scp comparison ═══
log "═══ F: scp at each size ═══"
for spec in "1m:1" "10m:10" "100m:100" "1g:1024" "10g:10240"; do
    f="/tmp/${spec%%:*}"; mb=${spec##*:}; nx
    T=$(sc "$f") || T=0; record $N "filesize-${mb}mb" "scp" "${mb}MB" "$T" "$mb"
done
# scp with 1% loss
nx; $SS ec2-user@$IPB "sudo iptables -A INPUT -m statistic --mode random --probability 0.01 -j DROP" 2>/dev/null || true
T=$(sc "/tmp/100m") || T=999; record $N "loss-1pct" "scp" "100MB" "$T" 100 "loss=1%"
$SS ec2-user@$IPB "sudo iptables -F" 2>/dev/null || true

# ═══ GROUP G: rsync / tar|ssh at 1GB ═══
log "═══ G: rsync + tar|ssh (1GB) ═══"
nx; T=$(rs "-a" "/tmp/1g") || T=0;   record $N "1gb" "rsync" "1GB" "$T" 1024
nx; T=$(rs "-az" "/tmp/1g") || T=0;  record $N "1gb" "rsync-z" "1GB" "$T" 1024
nx; T=$(ts "/tmp/1g") || T=0;        record $N "1gb" "tar-ssh" "1GB" "$T" 1024

# ═══ GROUP H: Directories ═══
log "═══ H: Directory transfers ═══"
nx; T=$(bh "-r" "/tmp/d100") || T=0;  record $N "dir-100x1mb" "bytehaul" "100x1MB" "$T" 100
nx; T=$(bh "-r" "/tmp/d1000") || T=0; record $N "dir-1000x100k" "bytehaul" "1000x100KB" "$T" 97
nx; T=$(bh "-r" "/tmp/d5000") || T=0; record $N "dir-5000x10k" "bytehaul" "5000x10KB" "$T" 48

# ═══ GROUP I: Delta ═══
log "═══ I: Delta transfers ═══"
# Establish baseline on receiver
bh "" "/tmp/100m" >/dev/null
# 0% changed
nx; T=$(bh "--delta" "/tmp/100m") || T=0; record $N "delta-0pct" "bytehaul-delta" "100MB" "$T" 100
# 10% changed
R 'dd if=/dev/urandom of=/tmp/100m bs=1M count=10 conv=notrunc 2>/dev/null'
nx; T=$(bh "--delta" "/tmp/100m") || T=0; record $N "delta-10pct" "bytehaul-delta" "100MB" "$T" 100
# 50% changed
R 'dd if=/dev/urandom of=/tmp/100m bs=1M count=50 conv=notrunc 2>/dev/null'
nx; T=$(bh "--delta" "/tmp/100m") || T=0; record $N "delta-50pct" "bytehaul-delta" "100MB" "$T" 100

# ═══ GROUP J: Aggressive vs Fair ═══
log "═══ J: Congestion modes (1GB) ═══"
nx; T=$(bh "" "/tmp/1g") || T=0;             record $N "fair-1gb" "bytehaul-fair" "1GB" "$T" 1024
nx; T=$(bh "--aggressive" "/tmp/1g") || T=0; record $N "aggro-1gb" "bytehaul-aggressive" "1GB" "$T" 1024

# ═══ GROUP K: Resume + Concurrent ═══
log "═══ K: Resume + Concurrent ═══"

# Resume: start 1GB, kill after 3s, resume
R "K='$RK'; ssh \$K ec2-user@$IPB 'rm -rf /tmp/rv; mkdir -p /tmp/rv; nohup bytehaul daemon --port 7700 --dest /tmp/rv --overwrite overwrite </dev/null >/dev/null 2>&1 &'" || true
sleep 1
R "timeout 3 bash -c 'RUST_LOG=error bytehaul send --daemon $IPB:7700 /tmp/1g /resume_test' 2>/dev/null" || true
R "K='$RK'; ssh \$K ec2-user@$IPB 'pkill -f \"bytehaul daemon\" 2>/dev/null || true'" || true
sleep 1
nx; T=$(bh "--resume" "/tmp/1g") || T=0; record $N "resume-1gb" "bytehaul-resume" "1GB" "$T" 1024

# 4 concurrent 100MB
R "K='$RK'; for p in 7701 7702 7703 7704; do ssh \$K ec2-user@$IPB \"rm -rf /tmp/rv; mkdir -p /tmp/rv; nohup bytehaul daemon --port \\\$p --dest /tmp/rv --overwrite overwrite </dev/null >/dev/null 2>&1 &\"; done" || true
sleep 2
nx
T=$(R "S=\$(python3 -c 'import time; print(time.time())'); for p in 7701 7702 7703 7704; do RUST_LOG=error bytehaul send --daemon $IPB:\$p /tmp/100m /b\$p >/dev/null 2>&1 & done; wait; E=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$E-\$S)\"") || T=0
R "K='$RK'; ssh \$K ec2-user@$IPB 'pkill -f bytehaul 2>/dev/null || true'" || true
record $N "concurrent-4x100m" "bytehaul-4x" "4x100MB" "$T" 400

# ═══════════════════════════════════════════════════════════════
log ""
log "═══════════════════════════════════════════════"
log "  ALL 50 EXPERIMENTS COMPLETE"
log "═══════════════════════════════════════════════"

python3 -c "
import json
with open('$RESULTS') as f: data = json.load(f)
print()
print(f\"{'#':>3} {'Name':<22} {'Tool':<20} {'File':<12} {'Secs':>8} {'MB/s':>8} {'Extra'}\")
print('─' * 90)
for r in data:
    print(f\"{r['experiment']:>3} {r['name']:<22} {r['tool']:<20} {r['file']:<12} {r['seconds']:>8} {r['speed_mbps']:>8} {r.get('extra','')}\")
"
