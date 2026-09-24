#!/usr/bin/env bash
# failover.sh — bring the standby fleet UP on this (home) machine.
#
# Run ONLY when the main PC is actually down. The reverse tunnel binds
# -R ports on nova-vps; if the main machine is alive its tunnel still holds
# them and this script's tunnel will fail to bind — a natural guard against
# running both fleets at once. Do NOT bypass that.
set -euo pipefail

log() { echo "[$(date '+%H:%M:%S')] $*"; }

log "0/5 keyring secrets (cookie decryption for synced profiles)"
bash "$HOME/flowkit/scripts/standby/import-keyring.sh" 2>/dev/null \
    || echo "WARN: keyring import failed — synced profiles may be signed out"

log "1/5 gateway (Surfshark proxy :18888, no netns on standby)"
systemctl --user start flowkit-gateway.service

log "2/5 api + studio + dashboard"
systemctl --user start flowkit.service flowkit-tvc.service flowkit-dashboard.service

log "3/5 waiting for api :8100"
for i in $(seq 1 30); do
    curl -sf -m 2 http://127.0.0.1:8100/health >/dev/null 2>&1 && break
    sleep 2
    [ "$i" = 30 ] && { echo "api did not come up"; exit 1; }
done

log "4/5 reverse tunnel to nova-vps"
systemctl --user start flowkit-tunnel.service
sleep 3
systemctl --user is-active --quiet flowkit-tunnel.service \
    || echo "WARN: tunnel not active — if main PC is still up this is EXPECTED (port held). Verify main is really down."

log "5/5 timers"
systemctl --user start flowkit-proxy-expire.timer 2>/dev/null || true

log "done. Health:"
curl -s -m 5 http://127.0.0.1:8100/health | python3 -c "
import json,sys
d=json.load(sys.stdin)
ws=d.get('ws',{})
print('  extension_connected:', d.get('extension_connected'))
print('  workers:', len(ws.get('workers',[])))" || echo "  health query failed — check journalctl --user -u flowkit -f"
echo
echo "Nicks auto-launch on api boot. Warm-up gate holds them out of routing"
echo "until each Flow tab reports app_ready — expect ~1-2 min to full capacity."
