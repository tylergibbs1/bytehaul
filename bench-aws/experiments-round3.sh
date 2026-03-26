#!/usr/bin/env bash
set -uo pipefail

# ═══════════════════════════════════════════════════════════════
# ByteHaul Round 3: Real-World Tool Comparison
# ═══════════════════════════════════════════════════════════════
#
# ByteHaul vs rclone vs aws s3 round-trip vs rsync -avz vs tar|ssh vs croc
#
# Tests: 100MB, 1GB, 10GB single file + 1000-file directory
# Link: us-east-2 -> eu-west-1 (~85ms RTT)

REGION_A="us-east-2"
REGION_B="eu-west-1"
INSTANCE_TYPE="c5.xlarge"
KEY_NAME="bh-r3-$$"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
S3_BUCKET="bytehaul-bench-$$"

RESULTS="/tmp/bytehaul-round3.json"
echo "[]" > "$RESULTS"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

record() {
    local exp=$1 name=$2 tool=$3 file_desc=$4 seconds=$5 size_mb=$6 extra=${7:-}
    local speed
    speed=$(python3 -c "print(round($size_mb / $seconds, 2) if $seconds > 0.001 else 0)")
    python3 -c "
import json
with open('$RESULTS') as f: data = json.load(f)
data.append({
    'experiment': $exp, 'name': '$name', 'tool': '$tool',
    'file': '$file_desc', 'seconds': round($seconds, 3),
    'size_mb': $size_mb, 'speed_mbps': $speed, 'extra': '$extra'
})
with open('$RESULTS', 'w') as f: json.dump(data, f, indent=2)
"
    log "  EXP#$exp [$tool] ${seconds}s (${speed} MB/s) $extra"
}

# ─── AWS Setup ───────────────────────────────────────────────
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
    # Clean up S3 bucket
    aws s3 rb "s3://$S3_BUCKET" --force --region "$REGION_A" 2>/dev/null || true
    rm -f /tmp/${KEY_NAME}.pem /tmp/bytehaul-src.tar.gz
    cp "$RESULTS" "$PROJECT_DIR/experiment-results-round3.json" 2>/dev/null || true
    log "Results saved to experiment-results-round3.json"
}
trap cleanup EXIT

AMI_A=$(aws ec2 describe-images --owners amazon --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' --output text --region "$REGION_A")
AMI_B=$(aws ec2 describe-images --owners amazon --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' --output text --region "$REGION_B")

log "Creating infrastructure..."
aws ec2 create-key-pair --key-name "$KEY_NAME" --query KeyMaterial --output text --region "$REGION_A" > /tmp/${KEY_NAME}.pem
chmod 600 /tmp/${KEY_NAME}.pem
PUB=$(ssh-keygen -y -f /tmp/${KEY_NAME}.pem)
aws ec2 import-key-pair --key-name "$KEY_NAME" --public-key-material "$(echo "$PUB" | base64)" --region "$REGION_B" >/dev/null

SG_A=$(aws ec2 create-security-group --group-name "$KEY_NAME" --description "bh" --region "$REGION_A" --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id "$SG_A" --protocol -1 --cidr 0.0.0.0/0 --region "$REGION_A" >/dev/null
SG_B=$(aws ec2 create-security-group --group-name "$KEY_NAME" --description "bh" --region "$REGION_B" --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id "$SG_B" --protocol -1 --cidr 0.0.0.0/0 --region "$REGION_B" >/dev/null

# Create S3 bucket for the S3 round-trip test
aws s3 mb "s3://$S3_BUCKET" --region "$REGION_A" 2>/dev/null || true

# IAM: instances need S3 access. Use instance profile if available, otherwise skip S3 test.
# For simplicity, we'll use the AWS CLI credentials from the instances.

USERDATA='#!/bin/bash
yum install -y gcc openssl-devel rsync unzip

# Install rclone
curl -sSL https://rclone.org/install.sh | bash 2>/dev/null || true

# Install croc
curl -sSL https://getcroc.schollz.com | bash 2>/dev/null || true

# Install AWS CLI v2
curl -sSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
cd /tmp && unzip -q awscliv2.zip && ./aws/install 2>/dev/null || true
'

log "Launching instances..."
INST_A=$(aws ec2 run-instances --image-id "$AMI_A" --instance-type "$INSTANCE_TYPE" --key-name "$KEY_NAME" --security-group-ids "$SG_A" --user-data "$USERDATA" --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]' --iam-instance-profile Name=EC2-S3-Access --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bh-r3-sender}]" --region "$REGION_A" --query 'Instances[0].InstanceId' --output text 2>/dev/null) || \
INST_A=$(aws ec2 run-instances --image-id "$AMI_A" --instance-type "$INSTANCE_TYPE" --key-name "$KEY_NAME" --security-group-ids "$SG_A" --user-data "$USERDATA" --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]' --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bh-r3-sender}]" --region "$REGION_A" --query 'Instances[0].InstanceId' --output text)

INST_B=$(aws ec2 run-instances --image-id "$AMI_B" --instance-type "$INSTANCE_TYPE" --key-name "$KEY_NAME" --security-group-ids "$SG_B" --user-data "$USERDATA" --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]' --iam-instance-profile Name=EC2-S3-Access --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bh-r3-receiver}]" --region "$REGION_B" --query 'Instances[0].InstanceId' --output text 2>/dev/null) || \
INST_B=$(aws ec2 run-instances --image-id "$AMI_B" --instance-type "$INSTANCE_TYPE" --key-name "$KEY_NAME" --security-group-ids "$SG_B" --user-data "$USERDATA" --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]' --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bh-r3-receiver}]" --region "$REGION_B" --query 'Instances[0].InstanceId' --output text)

log "  Sender: $INST_A ($REGION_A) | Receiver: $INST_B ($REGION_B)"

aws ec2 wait instance-running --instance-ids "$INST_A" --region "$REGION_A"
aws ec2 wait instance-running --instance-ids "$INST_B" --region "$REGION_B"

IP_A=$(aws ec2 describe-instances --instance-ids "$INST_A" --region "$REGION_A" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
IP_B=$(aws ec2 describe-instances --instance-ids "$INST_B" --region "$REGION_B" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
log "  IPs: $IP_A -> $IP_B"

SSH="ssh -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ServerAliveInterval=30"
SCP_CMD="scp -i /tmp/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

log "Waiting for SSH + cloud-init..."
for ip in $IP_A $IP_B; do
    for j in $(seq 1 60); do $SSH ec2-user@$ip "echo ok" 2>/dev/null && break || sleep 5; done
done
$SSH ec2-user@$IP_A "cloud-init status --wait" >/dev/null 2>&1 || sleep 60
$SSH ec2-user@$IP_B "cloud-init status --wait" >/dev/null 2>&1 || sleep 60

# Build ByteHaul
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

# SSH key for sender->receiver
$SCP_CMD /tmp/${KEY_NAME}.pem ec2-user@$IP_A:/tmp/bench-key.pem
$SSH ec2-user@$IP_A "chmod 600 /tmp/bench-key.pem"
RK="-i /tmp/bench-key.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

# Check what tools are available
log "Checking available tools..."
$SSH ec2-user@$IP_A "which rclone croc aws 2>&1; rclone version 2>&1 | head -1; croc --version 2>&1 || true"

# Configure rclone SFTP remote on sender
$SSH ec2-user@$IP_A "
mkdir -p ~/.config/rclone
cat > ~/.config/rclone/rclone.conf << RCLONEEOF
[receiver]
type = sftp
host = $IP_B
user = ec2-user
key_file = /tmp/bench-key.pem
known_hosts_file = /dev/null
RCLONEEOF
"

# Create test files
log "Creating test files..."
$SSH ec2-user@$IP_A 'dd if=/dev/urandom of=/tmp/100mb.bin bs=1M count=100 2>/dev/null; echo ok'
$SSH ec2-user@$IP_A 'dd if=/dev/urandom of=/tmp/1gb.bin bs=1M count=1024 2>/dev/null; echo ok'
$SSH ec2-user@$IP_A 'dd if=/dev/urandom of=/tmp/10gb.bin bs=1M count=10240 2>/dev/null; echo ok'
$SSH ec2-user@$IP_A 'mkdir -p /tmp/dir1000; for i in $(seq 1 1000); do dd if=/dev/urandom of=/tmp/dir1000/f$i.bin bs=102400 count=1 2>/dev/null; done; echo ok'
log "Test files created."

# Measure RTT
RTT=$($SSH ec2-user@$IP_A "ping -c 5 $IP_B 2>/dev/null | grep avg | cut -d/ -f5")
log "RTT: ${RTT}ms"

S() { $SSH ec2-user@$IP_A "$@"; }
EXP=0
next() { EXP=$((EXP+1)); }

# ─── Helpers for each tool ───────────────────────────────────

bh() {
    local extra="${1:-}"; local file="$2"; local port="${3:-7700}"
    S "K='$RK'; ssh \$K ec2-user@$IP_B 'rm -rf /tmp/recv; mkdir -p /tmp/recv; nohup bytehaul daemon --port $port --dest /tmp/recv --overwrite overwrite </dev/null >/dev/null 2>&1 &'; sleep 1; START=\$(python3 -c 'import time; print(time.time())'); RUST_LOG=error bytehaul send --daemon '$IP_B:$port' $extra $file /bench.bin >/dev/null 2>&1; END=\$(python3 -c 'import time; print(time.time())'); ssh \$K ec2-user@$IP_B 'pkill -f \"bytehaul daemon\" 2>/dev/null || true'; python3 -c \"print(\$END-\$START)\""
}

do_scp() {
    local file="$1"
    S "K='$RK'; ssh \$K ec2-user@$IP_B 'rm -rf /tmp/recv; mkdir -p /tmp/recv'; START=\$(python3 -c 'import time; print(time.time())'); scp -q \$K $file ec2-user@$IP_B:/tmp/recv/bench.bin; END=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$END-\$START)\""
}

do_rsync() {
    local extra="${1:-}"; local file="$2"
    S "K='$RK'; ssh \$K ec2-user@$IP_B 'rm -rf /tmp/recv; mkdir -p /tmp/recv'; START=\$(python3 -c 'import time; print(time.time())'); rsync $extra -e \"ssh \$K\" $file ec2-user@$IP_B:/tmp/recv/; END=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$END-\$START)\""
}

do_rclone() {
    local file="$1"
    S "K='$RK'; ssh \$K ec2-user@$IP_B 'rm -rf /tmp/recv; mkdir -p /tmp/recv'; START=\$(python3 -c 'import time; print(time.time())'); rclone copy $file receiver:/tmp/recv/ 2>/dev/null; END=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$END-\$START)\""
}

do_rclone_dir() {
    local dir="$1"
    S "K='$RK'; ssh \$K ec2-user@$IP_B 'rm -rf /tmp/recv; mkdir -p /tmp/recv'; START=\$(python3 -c 'import time; print(time.time())'); rclone copy $dir receiver:/tmp/recv/ 2>/dev/null; END=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$END-\$START)\""
}

do_tar_ssh() {
    local file="$1"
    S "K='$RK'; ssh \$K ec2-user@$IP_B 'rm -rf /tmp/recv; mkdir -p /tmp/recv'; START=\$(python3 -c 'import time; print(time.time())'); tar cf - -C \$(dirname $file) \$(basename $file) | ssh \$K ec2-user@$IP_B 'tar xf - -C /tmp/recv/'; END=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$END-\$START)\""
}

do_tar_ssh_dir() {
    local dir="$1"
    S "K='$RK'; ssh \$K ec2-user@$IP_B 'rm -rf /tmp/recv; mkdir -p /tmp/recv'; START=\$(python3 -c 'import time; print(time.time())'); tar cf - -C \$(dirname $dir) \$(basename $dir) | ssh \$K ec2-user@$IP_B 'tar xf - -C /tmp/recv/'; END=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$END-\$START)\""
}

# ═══════════════════════════════════════════════════════════════
log ""
log "═══════════════════════════════════════════════"
log "  ROUND 3: REAL-WORLD TOOL COMPARISON"
log "  $REGION_A -> $REGION_B (~${RTT}ms RTT)"
log "═══════════════════════════════════════════════"
log ""

# ═══ 100 MB single file ═════════════════════════════════════
log "═══ 100 MB single file ═══"

next; T=$(bh "" "/tmp/100mb.bin") || T=0;           record $EXP "100mb" "bytehaul" "100MB" "$T" 100
next; T=$(do_scp "/tmp/100mb.bin") || T=0;           record $EXP "100mb" "scp" "100MB" "$T" 100
next; T=$(do_rsync "-az" "/tmp/100mb.bin") || T=0;   record $EXP "100mb" "rsync-z" "100MB" "$T" 100
next; T=$(do_rsync "-a" "/tmp/100mb.bin") || T=0;    record $EXP "100mb" "rsync" "100MB" "$T" 100
next; T=$(do_rclone "/tmp/100mb.bin") || T=0;        record $EXP "100mb" "rclone-sftp" "100MB" "$T" 100
next; T=$(do_tar_ssh "/tmp/100mb.bin") || T=0;       record $EXP "100mb" "tar-ssh" "100MB" "$T" 100

# ═══ 1 GB single file ═══════════════════════════════════════
log "═══ 1 GB single file ═══"

next; T=$(bh "" "/tmp/1gb.bin") || T=0;              record $EXP "1gb" "bytehaul" "1GB" "$T" 1024
next; T=$(do_scp "/tmp/1gb.bin") || T=0;             record $EXP "1gb" "scp" "1GB" "$T" 1024
next; T=$(do_rsync "-a" "/tmp/1gb.bin") || T=0;      record $EXP "1gb" "rsync" "1GB" "$T" 1024
next; T=$(do_rclone "/tmp/1gb.bin") || T=0;          record $EXP "1gb" "rclone-sftp" "1GB" "$T" 1024
next; T=$(do_tar_ssh "/tmp/1gb.bin") || T=0;         record $EXP "1gb" "tar-ssh" "1GB" "$T" 1024

# ═══ 10 GB single file ══════════════════════════════════════
log "═══ 10 GB single file ═══"

next; T=$(bh "" "/tmp/10gb.bin") || T=0;             record $EXP "10gb" "bytehaul" "10GB" "$T" 10240
next; T=$(do_scp "/tmp/10gb.bin") || T=0;            record $EXP "10gb" "scp" "10GB" "$T" 10240
next; T=$(do_tar_ssh "/tmp/10gb.bin") || T=0;        record $EXP "10gb" "tar-ssh" "10GB" "$T" 10240

# ═══ 1000 files (100KB each = ~97MB) ════════════════════════
log "═══ 1000 files directory (97 MB total) ═══"

next; T=$(bh "-r" "/tmp/dir1000") || T=0;            record $EXP "dir1000" "bytehaul-r" "1000x100KB" "$T" 97
next; T=$(do_rsync "-a" "/tmp/dir1000") || T=0;      record $EXP "dir1000" "rsync" "1000x100KB" "$T" 97
next; T=$(do_rsync "-az" "/tmp/dir1000") || T=0;     record $EXP "dir1000" "rsync-z" "1000x100KB" "$T" 97
next; T=$(do_rclone_dir "/tmp/dir1000") || T=0;      record $EXP "dir1000" "rclone-sftp" "1000x100KB" "$T" 97
next; T=$(do_tar_ssh_dir "/tmp/dir1000") || T=0;     record $EXP "dir1000" "tar-ssh" "1000x100KB" "$T" 97

# scp -r is very slow for many files, give it a timeout
next
T=$(S "K='$RK'; ssh \$K ec2-user@$IP_B 'rm -rf /tmp/recv; mkdir -p /tmp/recv'; START=\$(python3 -c 'import time; print(time.time())'); timeout 300 scp -q -r \$K /tmp/dir1000 ec2-user@$IP_B:/tmp/recv/ 2>/dev/null; END=\$(python3 -c 'import time; print(time.time())'); python3 -c \"print(\$END-\$START)\"") || T=300
record $EXP "dir1000" "scp-r" "1000x100KB" "$T" 97

# ═══ AWS S3 round-trip (if credentials available) ════════════
log "═══ AWS S3 round-trip (upload Ohio + download Ireland) ═══"

# Check if S3 access works
S3_OK=$($SSH ec2-user@$IP_A "aws s3 ls s3://$S3_BUCKET/ 2>&1 && echo OK || echo FAIL") || S3_OK="FAIL"

if echo "$S3_OK" | grep -q "OK"; then
    # 100MB via S3
    next
    T=$(S "
        START=\$(python3 -c 'import time; print(time.time())')
        aws s3 cp /tmp/100mb.bin s3://$S3_BUCKET/100mb.bin --region $REGION_A >/dev/null 2>&1
        END=\$(python3 -c 'import time; print(time.time())')
        python3 -c \"print(\$END-\$START)\"
    ") || T=0
    record $EXP "s3-upload-100mb" "aws-s3-upload" "100MB" "$T" 100 "ohio"

    next
    T=$($SSH ec2-user@$IP_B "
        rm -f /tmp/recv/100mb.bin; mkdir -p /tmp/recv
        START=\$(python3 -c 'import time; print(time.time())')
        aws s3 cp s3://$S3_BUCKET/100mb.bin /tmp/recv/100mb.bin --region $REGION_A >/dev/null 2>&1
        END=\$(python3 -c 'import time; print(time.time())')
        python3 -c \"print(\$END-\$START)\"
    ") || T=0
    record $EXP "s3-download-100mb" "aws-s3-download" "100MB" "$T" 100 "ireland"

    # 1GB via S3
    next
    T=$(S "
        START=\$(python3 -c 'import time; print(time.time())')
        aws s3 cp /tmp/1gb.bin s3://$S3_BUCKET/1gb.bin --region $REGION_A >/dev/null 2>&1
        END=\$(python3 -c 'import time; print(time.time())')
        python3 -c \"print(\$END-\$START)\"
    ") || T=0
    record $EXP "s3-upload-1gb" "aws-s3-upload" "1GB" "$T" 1024 "ohio"

    next
    T=$($SSH ec2-user@$IP_B "
        rm -f /tmp/recv/1gb.bin; mkdir -p /tmp/recv
        START=\$(python3 -c 'import time; print(time.time())')
        aws s3 cp s3://$S3_BUCKET/1gb.bin /tmp/recv/1gb.bin --region $REGION_A >/dev/null 2>&1
        END=\$(python3 -c 'import time; print(time.time())')
        python3 -c \"print(\$END-\$START)\"
    ") || T=0
    record $EXP "s3-download-1gb" "aws-s3-download" "1GB" "$T" 1024 "ireland"
else
    log "  S3 access not available (no IAM role). Skipping S3 tests."
fi

# ═══ Croc (if available) ════════════════════════════════════
log "═══ croc peer-to-peer ═══"

CROC_OK=$($SSH ec2-user@$IP_A "which croc 2>&1 && echo OK || echo FAIL")
if echo "$CROC_OK" | grep -q "OK"; then
    # croc needs the receiver to run `croc --yes <code>`.
    # We generate a code, start receiver in background, then send.
    next
    T=$(S "
        K='$RK'
        CODE=\$(python3 -c 'import random,string; print(\"\".join(random.choices(string.ascii_lowercase, k=6)))')
        ssh \$K ec2-user@$IP_B \"rm -rf /tmp/recv; mkdir -p /tmp/recv; cd /tmp/recv; nohup croc --yes \$CODE </dev/null >/dev/null 2>&1 &\" 2>/dev/null
        sleep 2
        START=\$(python3 -c 'import time; print(time.time())')
        croc send --code \$CODE /tmp/100mb.bin >/dev/null 2>&1 || true
        END=\$(python3 -c 'import time; print(time.time())')
        ssh \$K ec2-user@$IP_B 'pkill croc 2>/dev/null || true'
        python3 -c \"print(\$END-\$START)\"
    ") || T=0
    record $EXP "100mb" "croc" "100MB" "$T" 100
else
    log "  croc not available. Skipping."
fi

# ═══ Repeat key tests for confidence ═════════════════════════
log "═══ Confidence runs (1GB, 3 iterations each) ═══"

for tool_fn in "bh:bytehaul" "do_scp:scp" "do_rsync -a:rsync" "do_tar_ssh:tar-ssh"; do
    fn=${tool_fn%%:*}
    label=${tool_fn##*:}
    for run in 1 2 3; do
        next
        if [ "$fn" = "bh" ]; then
            T=$(bh "" "/tmp/1gb.bin") || T=0
        elif [ "$fn" = "do_scp" ]; then
            T=$(do_scp "/tmp/1gb.bin") || T=0
        elif [ "$fn" = "do_rsync -a" ]; then
            T=$(do_rsync "-a" "/tmp/1gb.bin") || T=0
        elif [ "$fn" = "do_tar_ssh" ]; then
            T=$(do_tar_ssh "/tmp/1gb.bin") || T=0
        fi
        record $EXP "confidence-1gb-run$run" "$label" "1GB" "$T" 1024
    done
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
print(f'{'Exp':>4} {'Name':<25} {'Tool':<18} {'File':<15} {'Secs':>8} {'MB/s':>10}')
print('=' * 90)
for r in data:
    print(f'{r[\"experiment\"]:>4} {r[\"name\"]:<25} {r[\"tool\"]:<18} {r[\"file\"]:<15} {r[\"seconds\"]:>8} {r[\"speed_mbps\"]:>10}')
"
