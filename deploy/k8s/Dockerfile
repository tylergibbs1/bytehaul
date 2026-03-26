# ByteHaul init container / sidecar for Kubernetes
# Stages data into a PV before the training pod starts.
#
# Usage as init container:
#   initContainers:
#   - name: stage-data
#     image: bytehaul/bytehaul:latest
#     env:
#     - name: BYTEHAUL_SOURCE
#       value: "data-node:/datasets/imagenet/"
#     - name: BYTEHAUL_DEST
#       value: "/data/"
#     volumeMounts:
#     - name: data-volume
#       mountPath: /data
#
# Usage as sidecar (continuous sync):
#   containers:
#   - name: bytehaul-sync
#     image: bytehaul/bytehaul:latest
#     command: ["bytehaul", "watch", "/checkpoints/", "--pattern", "step_*", "backup-node:/checkpoints/"]
#     volumeMounts:
#     - name: checkpoint-volume
#       mountPath: /checkpoints

FROM rust:latest AS builder
WORKDIR /build
COPY . .
RUN cargo build --release --bin bytehaul

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-client ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/bytehaul /usr/local/bin/bytehaul
COPY deploy/k8s/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
