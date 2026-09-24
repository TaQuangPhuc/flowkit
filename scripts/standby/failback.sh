#!/usr/bin/env bash
# failback.sh — hand the fleet back to the MAIN machine after power returns.
#
# Order matters: standby stops first (so both never run together), state
# rsyncs back to main, then main's own services start on the MAIN side.
# The last step is manual — you run it on the main PC.
set -euo pipefail

MAIN_HOST="${MAIN_HOST:-flowkit-main}"
MAIN_HOME="${MAIN_HOME:-/home/pc}"
DBS=(flow_agent flowkit incidents request_ledger)

log() { echo "[$(date '+%H:%M:%S')] $*"; }

log "1/4 stopping standby fleet"
systemctl --user stop flowkit-tunnel.service flowkit-dashboard.service \
    flowkit-tvc.service flowkit.service 2>/dev/null || true
# Let Chromes die before snapshotting profiles.
sleep 5
pkill -f "user-data-dir=$HOME/.flowkit/chrome" 2>/dev/null || true
sleep 3

log "2/4 pushing db snapshots back to main"
for db in "${DBS[@]}"; do
    [ -f "$HOME/flowkit/$db.db" ] || continue
    sqlite3 "$HOME/flowkit/$db.db" ".backup /tmp/fk-return-$db.db"
    rsync -a "/tmp/fk-return-$db.db" "$MAIN_HOST:$MAIN_HOME/flowkit/$db.db"
done

log "3/4 pushing profiles + repo back (newer wins)"
rsync -az --update \
    --exclude='Cache/' --exclude='Code Cache/' --exclude='GPUCache/' \
    --exclude='Crashpad/' --exclude='FlowKitExtension/' \
    --exclude='SingletonLock' --exclude='SingletonCookie' --exclude='SingletonSocket' \
    "$HOME/.flowkit/chrome/" "$MAIN_HOST:$MAIN_HOME/.flowkit/chrome/"
rsync -az --update --exclude='node_modules/' --exclude='*.db' \
    "$HOME/flowkit/" "$MAIN_HOST:$MAIN_HOME/flowkit/"

log "4/4 done."
echo
echo "Now on the MAIN machine: start its normal services"
echo "  systemctl --user start flowkit flowkit-tvc flowkit-dashboard flowkit-tunnel"
echo "  sudo systemctl start surfshark-gateway flowkit-gateway-netns flowkit-gateway-proxy@18888"
echo "Then verify: curl http://127.0.0.1:8100/health"
