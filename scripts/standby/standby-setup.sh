#!/usr/bin/env bash
# standby-setup.sh — one-time setup + verification for the HOME standby (WSL2).
#
# Prereqs on the Windows 11 host:
#   1. wsl --install Ubuntu-24.04   (or existing Ubuntu with WSL2)
#   2. In the distro:  echo -e "[boot]\nsystemd=true" | sudo tee /etc/wsl.conf
#      then `wsl --shutdown` from PowerShell and reopen.
#   3. Create user `pc` with sudo — keeps every path identical to the main PC.
#   4. sudo apt update && sudo apt install -y rsync sqlite3 ssh curl ffmpeg \
#        python3 python3-venv gnome-keyring libsecret-tools dbus-user-session
#   5. Google Chrome for Linux:
#        wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
#        sudo apt install -y ./google-chrome-stable_current_amd64.deb
#   6. Tailscale inside WSL (or on Windows host + `wsl` reachable):
#        curl -fsSL https://tailscale.com/install.sh | sh && sudo tailscale up
#   7. ssh-keygen on standby + add pubkey to main PC's authorized_keys,
#      and make `flowkit-main` resolve (ssh config Host entry or tailscale name).
#
# This script verifies all of the above, installs the standby units, and runs
# the first sync. Re-runnable.
set -uo pipefail

MAIN_HOST="${MAIN_HOST:-flowkit-main}"
FAIL=0
ok()   { echo "  OK   $*"; }
bad()  { echo "  FAIL $*"; FAIL=1; }
warn() { echo "  WARN $*"; }

echo "== 1. environment =="
[ "$(id -un)" = "pc" ] && ok "user is pc (paths match main)" \
    || warn "user is $(id -un) — paths differ from /home/pc, adjust units/scripts"
systemctl --version >/dev/null 2>&1 && ok "systemd present" \
    || bad "no systemd — enable [boot] systemd=true in /etc/wsl.conf"
for c in rsync sqlite3 ssh curl python3 google-chrome ffmpeg \
         gnome-keyring-daemon secret-tool dbus-daemon; do
    command -v "$c" >/dev/null 2>&1 && ok "$c" || bad "$c missing"
done
[ -n "${WAYLAND_DISPLAY:-}" ] && ok "WSLg wayland available (GUI Chrome works)" \
    || warn "no WAYLAND_DISPLAY — Chromes run but you can't see them for manual relogin"

echo "== 2. connectivity to main =="
ssh -o ConnectTimeout=8 -o BatchMode=yes "$MAIN_HOST" true 2>/dev/null \
    && ok "ssh $MAIN_HOST" || bad "cannot ssh $MAIN_HOST — tailscale up? key authorized?"

echo "== 3. install standby units =="
UDIR="$HOME/.config/systemd/user"
mkdir -p "$UDIR"
for u in flowkit-gateway.service backup-sync.service backup-sync.timer; do
    cp "$HOME/flowkit/scripts/standby/$u" "$UDIR/$u" && ok "installed $u"
done
systemctl --user daemon-reload 2>/dev/null
systemctl --user enable backup-sync.timer 2>/dev/null && ok "backup-sync.timer enabled (30min)"
loginctl show-user "$(id -un)" 2>/dev/null | grep -q "Linger=yes" \
    && ok "linger on" \
    || warn "linger off — run: sudo loginctl enable-linger $(id -un) (else services die on logout)"

echo "== 4. keep services COLD on standby =="
for s in flowkit flowkit-tvc flowkit-dashboard flowkit-tunnel flowkit-gateway; do
    systemctl --user disable "$s.service" >/dev/null 2>&1 || true
    systemctl --user stop "$s.service" >/dev/null 2>&1 || true
done
ok "flowkit services stopped+disabled (standby stays cold; sync timer is the only active piece)"

echo "== 5. first sync =="
if [ "$FAIL" = 0 ]; then
    MAIN_HOST="$MAIN_HOST" bash "$HOME/flowkit/scripts/standby/backup-sync.sh" \
        && ok "first sync done" || bad "first sync failed"
    # keyring secrets → this machine's gnome-keyring (cookie decryption)
    bash "$HOME/flowkit/scripts/standby/import-keyring.sh" \
        && ok "keyring imported — synced sessions will decrypt" \
        || warn "keyring import failed — synced cookies stay encrypted (signed out)"
else
    warn "skipping first sync — fix FAILs above"
fi

echo
[ "$FAIL" = 0 ] && echo "SETUP COMPLETE — standby synced, staying cold. Test failover.sh once manually." \
    || echo "SETUP INCOMPLETE — fix FAILs and re-run."
