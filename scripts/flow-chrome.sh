#!/usr/bin/env bash
# Launch one vanilla Chrome profile for a Flow Kit nick.
#
#   scripts/flow-chrome.sh nick-a
#   scripts/flow-chrome.sh nick-a socks5://USER:PASS@HOST:PORT
#
# Each nick needs its own sticky residential proxy (same country as the
# Google account). Do not rotate. Do not use a datacenter/VPS IP.
# localhost must bypass the proxy so the extension can reach :8100 / :9222.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROFILE_ID="${1:-}"
PROXY="${2:-}"

if [[ -z "$PROFILE_ID" ]]; then
  echo "usage: $0 <profile-id> [proxy-url]" >&2
  echo "  e.g. $0 nick-a socks5://127.0.0.1:1080" >&2
  exit 1
fi

CHROME=""
for candidate in google-chrome google-chrome-stable chromium chromium-browser google-chrome-unstable; do
  if command -v "$candidate" >/dev/null 2>&1; then
    CHROME="$candidate"
    break
  fi
done
if [[ -z "$CHROME" ]]; then
  echo "no Chrome/Chromium binary on PATH" >&2
  exit 1
fi

DATA_DIR="${FLOW_CHROME_DIR:-$HOME/.flowkit/chrome}/$PROFILE_ID"
mkdir -p "$DATA_DIR"
EXT_DIR="$ROOT/extension"
if [[ -x "$ROOT/venv/bin/python" ]]; then
  EXT_DIR="$( cd "$ROOT" && venv/bin/python -c "from agent.services.chrome_nicks import seed_unpacked_extension, sync_extension_copy; p=r'''$DATA_DIR'''; c=sync_extension_copy(p, profile_id=r'''$PROFILE_ID'''); seed_unpacked_extension(p, c); print(c)" )"
fi

ARGS=(
  --user-data-dir="$DATA_DIR"
  --no-first-run
  --no-default-browser-check
  --disable-sync
  --force-webrtc-ip-handling-policy=disable_non_proxied_udp
  --enable-unsafe-extension-debugging
  --disable-features=DisableLoadExtensionCommandLineSwitch
  --load-extension="$EXT_DIR"
  "https://flow.google.com/"
)

if [[ -n "$PROXY" ]]; then
  ARGS=(--proxy-server="$PROXY" "${ARGS[@]}")
fi

echo "profile=$PROFILE_ID data=$DATA_DIR proxy=${PROXY:-none}"
exec "$CHROME" "${ARGS[@]}"
