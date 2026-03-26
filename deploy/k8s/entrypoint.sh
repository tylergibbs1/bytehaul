#!/bin/sh
# ByteHaul K8s entrypoint
# If BYTEHAUL_SOURCE is set, pull data before starting.
# Otherwise, run whatever command was passed.

set -e

if [ -n "${BYTEHAUL_SOURCE:-}" ] && [ -n "${BYTEHAUL_DEST:-}" ]; then
    echo "[bytehaul] Staging data: ${BYTEHAUL_SOURCE} -> ${BYTEHAUL_DEST}"
    bytehaul pull -r ${BYTEHAUL_OPTS:-} "${BYTEHAUL_SOURCE}" "${BYTEHAUL_DEST}"
    echo "[bytehaul] Stage complete"
fi

# If there are additional arguments, exec them (for sidecar mode)
if [ $# -gt 0 ]; then
    exec "$@"
fi
