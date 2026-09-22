#!/bin/bash
# Persistent uplink only: never delete/recreate it during gateway restarts.
set -euo pipefail
ns=flowkit-gateway
host_if=fk-vpn-host
peer_if=fk-vpn-peer
if ! ip netns list | awk '{print $1}' | grep -qx "$ns"; then
    ip netns add "$ns"
fi
if ! ip link show "$host_if" >/dev/null 2>&1; then
    ip link add "$host_if" type veth peer name "$peer_if"
    ip link set "$peer_if" netns "$ns"
fi
ip address replace 10.203.255.1/30 dev "$host_if"
ip link set "$host_if" up
ip -n "$ns" address replace 10.203.255.2/30 dev "$peer_if"
ip -n "$ns" link set "$peer_if" up
ip -n "$ns" link set lo up
ip -n "$ns" route replace default via 10.203.255.1
sysctl -q -w net.ipv4.ip_forward=1
ip netns exec "$ns" sysctl -q -w net.ipv4.ip_forward=1
iptables -t nat -C POSTROUTING -s 10.203.255.0/30 ! -d 10.203.255.0/30 -j MASQUERADE 2>/dev/null ||
    iptables -t nat -A POSTROUTING -s 10.203.255.0/30 ! -d 10.203.255.0/30 -j MASQUERADE
iptables -C FORWARD -i "$host_if" -j ACCEPT 2>/dev/null || iptables -I FORWARD 1 -i "$host_if" -j ACCEPT
iptables -C FORWARD -o "$host_if" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT 2>/dev/null ||
    iptables -I FORWARD 1 -o "$host_if" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
install -d -m 755 "/etc/netns/$ns"
printf 'nameserver 1.1.1.1\nnameserver 1.0.0.1\n' > "/etc/netns/$ns/resolv.conf"
