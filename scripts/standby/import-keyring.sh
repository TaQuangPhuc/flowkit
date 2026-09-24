#!/usr/bin/env bash
# import-keyring.sh — load the synced os_crypt secrets into THIS machine's
# gnome-keyring so Chrome can decrypt the synced profile cookies.
#
# Needs: gnome-keyring, libsecret-tools (secret-tool), dbus user session.
# The login keyring is created with an EMPTY password so it stays unlocked
# headless (standard for a DR box — protect the file, not the daemon).
set -euo pipefail

KEYRING_JSON="$HOME/.flowkit-keyring.json"
[ -f "$KEYRING_JSON" ] || { echo "no $KEYRING_JSON — run backup-sync.sh first"; exit 1; }

export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=/run/user/$(id -u)/bus}"

# Start daemon + unlock (empty pw → creates unencrypted login keyring if absent)
gnome-keyring-daemon --start --components=secrets >/dev/null 2>&1 || true
echo -n "" | gnome-keyring-daemon --unlock >/dev/null 2>&1 || true
sleep 1

store() { # app label secret
    printf '%s' "$3" | secret-tool store --label="$2" \
        application "$1" xdg:schema chrome_libsecret_os_crypt_password_v2
    echo "stored: $2"
}

CHROME=$(python3 -c "import json;print(json.load(open('$KEYRING_JSON'))['chrome']['secret'])")
store chrome "Chrome Safe Storage" "$CHROME"

CHROMIUM=$(python3 -c "import json;print(json.load(open('$KEYRING_JSON')).get('chromium',{}).get('secret',''))")
[ -n "$CHROMIUM" ] && store chromium "Chromium Safe Storage" "$CHROMIUM"

echo "keyring import done — standby Chrome can now decrypt synced cookies"
