#!/bin/bash
# Graceful flowkit-relay restart.
#
# The new binary binds every listener with SO_REUSEPORT, so a temporary
# "bridge" unit can share the ports while the old process drains its
# in-flight gRPC streams (SIGTERM -> listener close + h2 GOAWAY + drain).
# Without the bridge (old binary, or bridge failed to start) we fall back to
# a plain restart.
set -u
main=flowkit-relay
bridge=flowkit-relay-bridge

old_pid=$(systemctl show -p MainPID --value "$main")
if ! systemctl start "$bridge" 2>/dev/null; then
    echo "bridge failed to start - plain restart"
    systemctl restart "$main"
    exit $?
fi
sleep 1
if ! systemctl is-active --quiet "$bridge"; then
    echo "bridge not active - plain restart"
    systemctl restart "$main"
    exit $?
fi

echo "bridge up - draining $main (pid $old_pid)"
kill -TERM "$old_pid"
new_pid=""
for _ in $(seq 1 100); do
    sleep 1
    new_pid=$(systemctl show -p MainPID --value "$main")
    if [ -n "$new_pid" ] && [ "$new_pid" != "$old_pid" ] && \
       systemctl is-active --quiet "$main"; then
        sleep 1
        break
    fi
done
systemctl stop "$bridge" 2>/dev/null || true
if [ -z "$new_pid" ] || [ "$new_pid" = "$old_pid" ]; then
    echo "main did not come back - forcing restart"
    systemctl restart "$main"
    exit $?
fi
curl -sf -m 5 http://127.0.0.1:8100/__gw/status >/dev/null || {
    echo "gateway probe failed after swap"; exit 1; }
echo "relay restarted gracefully: $old_pid -> $new_pid"
