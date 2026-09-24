#!/usr/bin/env bash
# backup-sync.sh — pull FlowKit state from the MAIN machine onto this standby.
#
# Run on the HOME machine (WSL2). Designed for a systemd timer or cron every
# 20–60 min. Never run while the standby fleet is live — rsync --delete would
# clobber profiles under running Chromes. Guard below enforces that.
#
# Required env / defaults:
#   MAIN_HOST   ssh alias of the main PC (tailscale name works)  — default: flowkit-main
#   MAIN_HOME   home dir on the main PC                          — default: /home/pc
set -euo pipefail

MAIN_HOST="${MAIN_HOST:-flowkit-main}"
MAIN_HOME="${MAIN_HOME:-/home/pc}"
FLOWKIT_DIR="$HOME/flowkit"
DATA_DIR="$HOME/.flowkit"
DBS=(flow_agent flowkit incidents request_ledger)
STAGE_DB="$HOME/.flowkit-backup-db"

log() { echo "[$(date '+%H:%M:%S')] $*"; }
die() { echo "FATAL: $*" >&2; exit 1; }

# --- Safety: refuse to sync into a live fleet -------------------------------
if systemctl --user is-active --quiet flowkit.service 2>/dev/null; then
    die "flowkit.service is RUNNING on standby — syncing over live profiles corrupts them. Stop the standby fleet first."
fi

ssh -o ConnectTimeout=10 -o BatchMode=yes "$MAIN_HOST" true \
    || die "cannot reach $MAIN_HOST — check tailscale/ssh"

# --- 1. SQLite snapshots on the main side (online-safe), then pull ----------
log "snapshotting sqlite dbs on $MAIN_HOST"
ssh "$MAIN_HOST" "
    set -e; mkdir -p /tmp/fk-db-snap
    for db in ${DBS[*]}; do
        sqlite3 \"$MAIN_HOME/flowkit/\$db.db\" \".backup /tmp/fk-db-snap/\$db.db\"
    done"
mkdir -p "$STAGE_DB"
rsync -a --delete "$MAIN_HOST:/tmp/fk-db-snap/" "$STAGE_DB/"

# --- 2. Repo (code, extension, gateway binary, dashboard, venv) -------------
log "rsync repo"
rsync -az --delete --timeout=120 \
    --exclude='node_modules/' --exclude='__pycache__/' --exclude='.pytest_cache/' \
    --exclude='*.pyc' --exclude='*.log' --exclude='logs/' \
    --exclude='flow_agent.db' --exclude='flowkit.db' \
    --exclude='incidents.db' --exclude='request_ledger.db' \
    "$MAIN_HOST:$MAIN_HOME/flowkit/" "$FLOWKIT_DIR/"

# Restore db snapshots into the repo copy.
for db in "${DBS[@]}"; do
    [ -f "$STAGE_DB/$db.db" ] && cp "$STAGE_DB/$db.db" "$FLOWKIT_DIR/$db.db"
done

# --- 3. Chrome profiles — the crown jewels ----------------------------------
# Excludes: caches (regenerated), the per-nick baked extension copy (carries
# the parent's profileId — resyncing it recreates the ghost-nick bug), crash
# dumps, singleton locks. LevelDB .log/MANIFEST and *-wal are KEPT — recent
# cookies live there.
log "rsync chrome profiles (no caches)"
rsync -az --delete --timeout=300 --partial \
    --exclude='Cache/' --exclude='Code Cache/' --exclude='GPUCache/' \
    --exclude='DawnGraphiteCache/' --exclude='DawnWebGPUCache/' \
    --exclude='ShaderCache/' --exclude='GrShaderCache/' \
    --exclude='Media Cache/' --exclude='Service Worker/CacheStorage/' \
    --exclude='Service Worker/ScriptCache/' --exclude='Crashpad/' \
    --exclude='component_crx_cache/' --exclude='extensions_crx_cache/' \
    --exclude='FlowKitExtension/' --exclude='*.copying/' \
    --exclude='SingletonLock' --exclude='SingletonCookie' --exclude='SingletonSocket' \
    "$MAIN_HOST:$MAIN_HOME/.flowkit/chrome/" "$DATA_DIR/chrome/" \
    || { rc=$?; [ "$rc" -le 24 ] || exit "$rc"; log "rsync profiles: partial (rc=$rc) — continuing"; }

# --- 4. Cốc Cốc browser + gateway config ------------------------------------
log "rsync coccoc-browser + gateway extras"
rsync -az --delete --exclude='chrome/' \
    "$MAIN_HOST:$MAIN_HOME/.flowkit/" "$DATA_DIR/" \
    || { rc=$?; [ "$rc" -le 24 ] || exit "$rc"; }

# --- 5. SSH bits for the nova-vps tunnel (config + keys only) ---------------
mkdir -p "$HOME/.ssh" && chmod 700 "$HOME/.ssh"
rsync -az --include='config' --include='id_*' --include='known_hosts*' \
    --exclude='*' "$MAIN_HOST:$MAIN_HOME/.ssh/" "$HOME/.ssh/" || true
chmod 600 "$HOME/.ssh"/id_* 2>/dev/null || true

# --- 6. systemd user units (flowkit* only — keeps unit edits propagated) ----
mkdir -p "$HOME/.config/systemd/user"
rsync -az --include='flowkit*' --exclude='*' \
    "$MAIN_HOST:$MAIN_HOME/.config/systemd/user/" "$HOME/.config/systemd/user/" \
    || true
systemctl --user daemon-reload 2>/dev/null || true

# --- 7. gnome-keyring os_crypt secrets — WITHOUT these the synced cookies ---
# --- stay encrypted and every nick is signed out on the standby. -----------
KEYRING_JSON="$HOME/.flowkit-keyring.json"
if ssh "$MAIN_HOST" \
    "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/\$UID/bus python3 -" \
    < "$FLOWKIT_DIR/scripts/standby/export-keyring.py" \
    > "$KEYRING_JSON.new" 2>/dev/null \
    && python3 -c "import json,sys; d=json.load(open('$KEYRING_JSON.new')); assert d.get('chrome')" 2>/dev/null; then
    mv "$KEYRING_JSON.new" "$KEYRING_JSON" && chmod 600 "$KEYRING_JSON"
    log "keyring secrets synced ($(python3 -c "import json;print(','.join(json.load(open('$KEYRING_JSON')).keys()))"))"
else
    rm -f "$KEYRING_JSON.new"
    [ -f "$KEYRING_JSON" ] || log "WARN: keyring export failed and no prior copy — standby cookies will NOT decrypt"
fi

log "sync complete."
