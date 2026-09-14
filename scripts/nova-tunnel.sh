#!/usr/bin/env bash
# Keep reverse SSH tunnel to Nova VPS alive.
# Binds Nova VPS Docker bridge IP (172.17.0.1:8100) to local FlowKit (127.0.0.1:8100).
set -euo pipefail

echo "Starting FlowKit reverse SSH tunnel to nova-vps..."
exec /usr/bin/ssh -N \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=15 \
  -o ServerAliveCountMax=3 \
  -R 172.17.0.1:8100:127.0.0.1:8100 \
  -R 172.17.0.1:8089:127.0.0.1:8089 \
  nova-vps
