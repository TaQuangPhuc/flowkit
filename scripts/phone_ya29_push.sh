#!/data/data/com.termux/files/usr/bin/bash
# ya29 mint-and-push loop — run inside Termux on the Android device that
# owns the nick. The aas_et master token dies with BadAuthentication when
# minted off-device (different IP/TLS stack), so the PHONE mints ya29 over
# its own mobile connection and pushes the bearer token to FlowKit.
#
# Setup (once, via UltraViewer or locally):
#   pkg install curl
#   mkdir -p ~/flowkit && cp aas_et.txt ~/flowkit/aas_et.txt
#   edit NICK / VPS / PUSH_KEY below
#   nohup bash ~/flowkit/phone_ya29_push.sh >> ~/flowkit/push.log 2>&1 &
#
# Re-capture: when the log shows "BadAuthentication", re-extract aas_et
# from the phone (extract_nick.py / GoogleAccountDataStore.pb) and rewrite
# ~/flowkit/aas_et.txt — the loop picks it up on the next cycle.

NICK="android01"
AAS_FILE="$HOME/flowkit/aas_et.txt"
VPS="http://180.93.43.217:8100"
PUSH_KEY="CHANGE_ME"          # = FLOWKIT_PUSH_SECRET in VPS secrets.env

APP="com.google.android.apps.labs.whisk"
CLIENT_SIG="d27c106448af3ab8b6c5497271356f14024718aa"
SCOPE="oauth2:openid https://www.googleapis.com/auth/aisandbox"

while true; do
  AAS_ET="$(tr -d '[:space:]' < "$AAS_FILE" 2>/dev/null)"
  if [ -z "$AAS_ET" ]; then
    echo "$(date '+%F %T') no aas_et in $AAS_FILE — re-extract" >&2
    sleep 600; continue
  fi

  RESP=$(curl -sS -m 30 "https://android.googleapis.com/auth" \
      --data-urlencode "app=$APP" \
      --data-urlencode "client_sig=$CLIENT_SIG" \
      --data-urlencode "service=$SCOPE" \
      --data-urlencode "Token=$AAS_ET" \
      --data-urlencode "ACCESS_TOKEN=1" 2>&1)

  TOK=$(printf '%s' "$RESP" | tr '\n' ' ' | grep -o 'Auth=ya29[^ ]*' \
        | head -1 | cut -d= -f2-)
  EXP=$(printf '%s' "$RESP" | grep -o 'ExpiresInDurationSec=[0-9]*' \
        | head -1 | cut -d= -f2)
  EXP=${EXP:-3600}

  if [ -n "$TOK" ]; then
    OUT=$(curl -sS -m 30 -X POST "$VPS/api/admin/nicks/$NICK/ya29" \
        -H "Content-Type: application/json" \
        -H "X-Push-Key: $PUSH_KEY" \
        -d "{\"token\":\"$TOK\",\"expires_in\":$EXP}" 2>&1)
    echo "$(date '+%F %T') pushed ya29 (exp ${EXP}s) -> $OUT"
    sleep 1800          # ya29 lives ~1h; refresh at ~30min
  else
    echo "$(date '+%F %T') mint failed: $(echo "$RESP" | head -3)" >&2
    # BadAuthentication => aas_et dead, re-extract from the device
    sleep 300
  fi
done
