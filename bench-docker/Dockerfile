FROM rust:latest AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock* ./
COPY bytehaul-proto/ bytehaul-proto/
COPY bytehaul-lib/ bytehaul-lib/
COPY bytehaul-cli/ bytehaul-cli/
COPY bytehaul-bench/ bytehaul-bench/

RUN cargo build --release --bin bytehaul --bin bytehaul-bench

# Runtime image — must match builder's glibc
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-server openssh-client \
    rsync \
    iproute2 \
    iputils-ping \
    python3 \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set up SSH for scp/rsync benchmarks
RUN mkdir -p /run/sshd \
    && ssh-keygen -A \
    && sed -i 's/#PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config \
    && echo 'root:benchpass' | chpasswd \
    && ssh-keygen -t ed25519 -f /root/.ssh/id_ed25519 -N '' \
    && cat /root/.ssh/id_ed25519.pub >> /root/.ssh/authorized_keys \
    && chmod 600 /root/.ssh/authorized_keys \
    && echo 'Host *\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  LogLevel ERROR' > /root/.ssh/config \
    && chmod 600 /root/.ssh/config

COPY --from=builder /build/target/release/bytehaul /usr/local/bin/bytehaul
COPY --from=builder /build/target/release/bytehaul-bench /usr/local/bin/bytehaul-bench
COPY bench-docker/run-bench.sh /usr/local/bin/run-bench.sh
RUN chmod +x /usr/local/bin/run-bench.sh

# Start SSH daemon by default
CMD ["/usr/sbin/sshd", "-D"]
