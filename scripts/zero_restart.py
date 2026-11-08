#!/usr/bin/env python3
"""Zero-downtime restart of the FlowKit agent behind the tlsrelay gateway.

Topology:  public :8100 (tlsrelay gateway)
                 ├─ backend A :8101
                 └─ backend B :8102

Flow: pick idle port → spawn new agent there → wait /health → write
.scratch/gw_target (gateway flips within ~1.5s) → drain old instance's
in-flight scheduler work → stop the old process.

Windows spawn uses a detached process. Linux VPS uses systemd template
``flowkit-agent@.service`` when that unit file exists.

Usage:  python scripts/zero_restart.py [--drain-timeout 300]
"""
from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PORTS = (8101, 8102)
GW_TARGET = ROOT / ".scratch" / "gw_target"
SYSTEMD_TEMPLATE = Path("/etc/systemd/system/flowkit-agent@.service")


def health(port: int) -> bool:
    try:
        with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/health", timeout=3) as r:
            return json.loads(r.read()).get("status") == "ok"
    except Exception:
        return False


def pid_on(port: int) -> int:
    if os.name == "nt":
        out = subprocess.run(
            ["netstat", "-ano"], capture_output=True, text=True).stdout
        for line in out.splitlines():
            if f":{port}" in line and "LISTENING" in line:
                return int(line.split()[-1])
        return 0
    out = subprocess.run(
        ["ss", "-lntp"], capture_output=True, text=True).stdout
    needle = f":{port} "
    for line in out.splitlines():
        if needle not in line and not line.rstrip().endswith(f":{port}"):
            continue
        m = re.search(r"pid=(\d+)", line)
        if m:
            return int(m.group(1))
    return 0


def inflight(port: int) -> int:
    """Requests the agent still holds — scheduler in-flight + queued."""
    try:
        with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/api/admin/nicks", timeout=5) as r:
            d = json.loads(r.read())
        return sum(n.get("inflight", 0) for n in d.get("nicks", [])) \
            + d.get("waiters", 0)
    except Exception:
        return -1


def spawn(port: int) -> None:
    GW_TARGET.parent.mkdir(parents=True, exist_ok=True)
    if SYSTEMD_TEMPLATE.is_file():
        subprocess.run(
            ["systemctl", "start", f"flowkit-agent@{port}"], check=True)
        return
    env = dict(os.environ, API_HOST="127.0.0.1", API_PORT=str(port))
    log = open(ROOT / ".scratch" / f"agent-{port}.log", "ab")
    kwargs = dict(
        args=[sys.executable, "-m", "agent.main"],
        cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
        close_fds=True,
    )
    if os.name == "nt":
        kwargs["creationflags"] = (
            subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS)
    else:
        kwargs["start_new_session"] = True
    subprocess.Popen(**kwargs)


def stop_agent(port: int, pid: int) -> None:
    if SYSTEMD_TEMPLATE.is_file():
        subprocess.run(
            ["systemctl", "stop", f"flowkit-agent@{port}"],
            capture_output=True)
        if port == 8101:
            subprocess.run(
                ["systemctl", "stop", "flowkit-agent"], capture_output=True)
            subprocess.run(
                ["systemctl", "disable", "flowkit-agent"], capture_output=True)
        print(f"stopped agent on :{port}")
        return
    if not pid:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/F"], capture_output=True)
    else:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    print(f"killed old agent pid={pid} (:{port})")


def rolling_restart(drain_timeout: int = 300) -> int:
    """Both backends serve traffic (gateway round-robins when gw_target is
    absent). Restart each in turn: pin the gateway to the sibling port via
    gw_target, drain in-flight work, restart, wait for health, unpin.
    Public :8100 never loses its last healthy backend."""
    GW_TARGET.parent.mkdir(parents=True, exist_ok=True)
    for port in PORTS:
        other = next(p for p in PORTS if p != port)
        # The sibling may still be booting (first dual-serve deploy starts
        # :8102 for the first time) — give it time before refusing.
        for _ in range(90):
            if health(other):
                break
            time.sleep(1)
        else:
            print(f":{other} unhealthy — refusing to restart :{port} "
                  "(would drop the last backend)")
            return 1
        GW_TARGET.write_text(str(other))
        print(f"pinned gateway -> :{other}; draining :{port}")
        time.sleep(3)  # let the gateway's 1.5s health loop see the pin
        deadline = time.time() + drain_timeout
        while time.time() < deadline:
            n = inflight(port)
            if n == 0:
                break
            if n > 0:
                print(f"  draining :{port} — {n} in-flight/queued")
            time.sleep(10)
        if SYSTEMD_TEMPLATE.is_file():
            subprocess.run(
                ["systemctl", "restart", f"flowkit-agent@{port}"],
                check=True)
        else:
            pid = pid_on(port)
            if pid:
                os.kill(pid, signal.SIGTERM)
                time.sleep(2)
            spawn(port)
        for _ in range(90):
            if health(port):
                break
            time.sleep(1)
        else:
            print(f":{port} never became healthy — keeping pin on :{other}")
            return 1
        print(f":{port} healthy")
    try:
        GW_TARGET.unlink()
    except FileNotFoundError:
        pass
    print("done — both backends serving, gateway load-balancing")
    return 0


def main() -> int:
    drain_timeout = 300
    if "--drain-timeout" in sys.argv:
        drain_timeout = int(
            sys.argv[sys.argv.index("--drain-timeout") + 1])
    if "--rolling" in sys.argv:
        return rolling_restart(drain_timeout)

    live = [p for p in PORTS if health(p)]
    if len(live) == 2:
        print("both backend ports occupied — kill one first")
        return 1
    idle = next(p for p in PORTS if p not in live)
    old = live[0] if live else None
    old_pid = pid_on(old) if old else 0

    print(f"spawning agent on :{idle} (old: :{old or 'none'} pid={old_pid})")
    spawn(idle)
    for _ in range(90):
        if health(idle):
            break
        time.sleep(1)
    else:
        print(f"agent on :{idle} never became healthy — abort, old untouched")
        return 1

    GW_TARGET.write_text(str(idle))
    print(f"gw_target -> {idle}; gateway flips within ~1.5s")

    if old:
        deadline = time.time() + drain_timeout
        while time.time() < deadline:
            n = inflight(old)
            if n == 0:
                break
            if n < 0:
                print(f"  draining :{old} — admin unreachable, still waiting")
            else:
                print(f"  draining :{old} — {n} in-flight/queued")
            time.sleep(10)
        stop_agent(old, old_pid)
    print("done — public :8100 never went down")
    return 0


if __name__ == "__main__":
    sys.exit(main())
