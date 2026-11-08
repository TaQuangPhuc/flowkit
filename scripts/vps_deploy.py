#!/usr/bin/env python3
"""Pack FlowKit and push it to the generation VPS over the deploy SSH key.

Usage (from repo root):
    python scripts/vps_deploy.py
    python scripts/vps_deploy.py --restart-only
    python scripts/vps_deploy.py --health
"""
from __future__ import annotations

import argparse
import os
import sys
import tarfile
import tempfile
from pathlib import Path

import paramiko

HOST = "180.93.43.217"
USER = "root"
KEY = Path.home() / ".ssh" / "flowkit_vps"
REMOTE_ROOT = "/opt/flowkit"
REMOTE_TAR = "/tmp/flowkit-vps.tar.gz"

INCLUDE_DIRS = [
    "agent",
    "nicks",
    "tools/tlsrelay",
    "crawler",
    "assets/bgm",
    "extension",
    "scripts",
]
# flow_agent.db is live state on the VPS (init_db creates it if missing);
# shipping the local copy clobbered it on every deploy.
INCLUDE_FILES = [
    "requirements.txt",
    "secrets.env",
    "fashion_lookbook_studio.py",
    "batch_image_studio.py",
    "batch_scheduler.py",
    "auto_tvc_server.py",
]
SKIP_DIR = {"__pycache__", ".git"}
SKIP_EXT = {".pyc", ".exe", ".log"}
SKIP_FILES = {"nicks/_routes.json", "flow_agent.db"}


def _private(rel: str) -> bool:
    """Tokens, proxy creds, captcha templates: root-only on the VPS."""
    return rel == "secrets.env" or rel.startswith("nicks/")


def _mode(rel: str) -> int:
    if _private(rel):
        return 0o600
    return 0o755 if rel.endswith(".sh") else 0o644


def connect() -> paramiko.SSHClient:
    if not KEY.is_file():
        sys.exit(f"missing deploy key {KEY} — see docs/VPS.md")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        HOST,
        username=USER,
        key_filename=str(KEY),
        timeout=20,
        allow_agent=False,
        look_for_keys=False,
    )
    return client


def run(client: paramiko.SSHClient, cmd: str, timeout: int = 300) -> str:
    stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode("utf-8", "replace")
    err = stderr.read().decode("utf-8", "replace")
    code = stdout.channel.recv_exit_status()
    if code != 0:
        raise SystemExit(f"$ {cmd}\n{out}\n{err}\nexit {code}")
    return out


def pack(root: Path) -> Path:
    fd, name = tempfile.mkstemp(suffix=".tar.gz")
    os.close(fd)
    path = Path(name)

    def keep(rel: str) -> bool:
        rel = rel.replace("\\", "/")
        if rel in SKIP_FILES or Path(rel).name == "_routes.json":
            return False
        parts = set(rel.split("/"))
        if parts & SKIP_DIR:
            return False
        return Path(rel).suffix.lower() not in SKIP_EXT

    def add(tar: tarfile.TarFile, full: Path, rel: str) -> None:
        # Windows has no POSIX modes; tar would record 0666 and root-owned
        # extraction keeps it, leaving refresh tokens world-readable.
        def fix(ti: tarfile.TarInfo) -> tarfile.TarInfo:
            ti.mode = _mode(rel)
            ti.uid = ti.gid = 0
            ti.uname = ti.gname = "root"
            return ti
        tar.add(full, arcname=rel, filter=fix)

    with tarfile.open(path, "w:gz") as tar:
        for d in INCLUDE_DIRS:
            base = root / d
            if not base.exists():
                continue
            for dirpath, dirnames, filenames in os.walk(base):
                dirnames[:] = [x for x in dirnames if x not in SKIP_DIR]
                for fn in filenames:
                    full = Path(dirpath) / fn
                    rel = full.relative_to(root).as_posix()
                    if keep(rel):
                        add(tar, full, rel)
        for f in INCLUDE_FILES:
            full = root / f
            if full.is_file() and keep(f):
                add(tar, full, f)
    return path


def md5(client: paramiko.SSHClient, path: str) -> str:
    out = run(client, f"md5sum {path} 2>/dev/null || true").strip()
    return out.split()[0] if out else ""


def health(client: paramiko.SSHClient) -> None:
    print(run(
        client,
        "systemctl is-active flowkit-agent@8101 flowkit-agent@8102 "
        "flowkit-agent flowkit-relay flowkit-tvc 2>/dev/null || true",
    ).rstrip())
    print("8100", run(client, "curl -sS -m 5 http://127.0.0.1:8100/health").rstrip()[:200])
    print("gw", run(client, "curl -sS -m 5 http://127.0.0.1:8100/__gw/status").rstrip())
    print("8089", run(
        client,
        "curl -sS -m 5 -o /dev/null -w '%{http_code}' "
        "-H \"X-TVC-Key: $(sed -n 's/^TVC_API_KEY=//p' /etc/flowkit/tvc.env)\" "
        "http://127.0.0.1:8089/api/system/concurrency",
    ).rstrip())


def install_units(client: paramiko.SSHClient) -> None:
    for unit in ("flowkit-agent@.service", "flowkit-relay.service",
                 "flowkit-relay-bridge.service"):
        run(client, f"install -m 644 /opt/flowkit/scripts/systemd/{unit} "
            f"/etc/systemd/system/{unit}")
    # The second backend is the lean serving instance — it must not start
    # the WS bridge (:9222 collision), queue worker, or watchdog daemons.
    run(client,
        "install -d -m 755 /etc/systemd/system/flowkit-agent@8102.service.d"
        " && printf '[Service]\\nEnvironment=FLOWKIT_ROLE=api\\n' > "
        "/etc/systemd/system/flowkit-agent@8102.service.d/role.conf")
    run(client, "systemctl daemon-reload")
    # Both backends stay up permanently — the gateway round-robins.
    run(client, "systemctl enable flowkit-agent@8101 flowkit-agent@8102 "
        "2>/dev/null || true")
    run(client, "systemctl start flowkit-agent@8102 || true")


def zero_restart_agent(client: paramiko.SSHClient, drain: int = 90) -> None:
    print(run(
        client,
        f"/opt/flowkit/venv/bin/python /opt/flowkit/scripts/zero_restart.py "
        f"--rolling",
        timeout=drain + 240,
    ))


def restart_hard(client: paramiko.SSHClient) -> None:
    run(client, "systemctl restart flowkit-relay flowkit-tvc")
    run(client, "systemctl restart flowkit-agent@8101 flowkit-agent@8102 "
        "|| systemctl restart flowkit-agent")
    run(client, "sleep 3")
    health(client)


def deploy(root: Path, hard: bool = False, skip_relay: bool = False) -> None:
    archive = pack(root)
    try:
        client = connect()
        before_bin = md5(client, "/opt/flowkit/tlsrelay")
        before_tvc = md5(client, "/opt/flowkit/auto_tvc_server.py")
        sftp = client.open_sftp()
        sftp.put(str(archive), REMOTE_TAR)
        sftp.close()
        run(client, f"mkdir -p {REMOTE_ROOT} && tar -xzf {REMOTE_TAR} -C {REMOTE_ROOT}")
        # Files the tar does not carry (routes, db, wg html) also stay private.
        run(client, f"chmod 600 {REMOTE_ROOT}/secrets.env && "
            f"find {REMOTE_ROOT}/nicks -type f -exec chmod 600 {{}} + && "
            f"chmod 700 {REMOTE_ROOT}/nicks && "
            f"find {REMOTE_ROOT}/nicks -mindepth 1 -type d -exec chmod 700 {{}} + && "
            f"rm -f {REMOTE_TAR}")
        run(client, f"ln -sfn {REMOTE_ROOT} /home/pc/flowkit")
        run(client, "mkdir -p /opt/flowkit/.scratch")
        install_units(client)
        run(
            client,
            "cd /opt/flowkit/tools/tlsrelay && "
            "/usr/local/go/bin/go build -o /tmp/tlsrelay.new .",
            timeout=180,
        )
        run(
            client,
            "install -d -m 700 /etc/flowkit && "
            "grep -E '^(TVC_API_KEY|TVC_RUN_SECRET)=' /opt/flowkit/secrets.env "
            "> /etc/flowkit/tvc.env && chmod 600 /etc/flowkit/tvc.env",
        )
        if hard:
            run(client, "install -m 755 /tmp/tlsrelay.new /opt/flowkit/tlsrelay")
            restart_hard(client)
        else:
            zero_restart_agent(client)
            after_bin = md5(client, "/tmp/tlsrelay.new")
            if after_bin and after_bin != before_bin and skip_relay:
                print("tlsrelay binary changed — NOT restarting (--skip-relay); "
                      "later: install -m 755 /tmp/tlsrelay.new /opt/flowkit/tlsrelay"
                      " && bash /opt/flowkit/scripts/relay_restart.sh")
            elif after_bin and after_bin != before_bin:
                print("tlsrelay binary changed — graceful relay restart")
                run(client, "install -m 755 /tmp/tlsrelay.new /opt/flowkit/tlsrelay")
                run(client, "bash /opt/flowkit/scripts/relay_restart.sh",
                    timeout=180)
            else:
                print("tlsrelay binary unchanged — leave relay running")
            after_tvc = md5(client, "/opt/flowkit/auto_tvc_server.py")
            if after_tvc != before_tvc:
                print("studio files changed — restarting flowkit-tvc on :8089")
                run(client, "systemctl restart flowkit-tvc")
                run(client, "sleep 2")
            else:
                print("studio unchanged — leave tvc running")
            health(client)
        client.close()
    finally:
        archive.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Deploy FlowKit to the generation VPS")
    parser.add_argument("--restart-only", action="store_true")
    parser.add_argument("--hard", action="store_true",
                        help="restart all units (brief downtime)")
    parser.add_argument("--health", action="store_true")
    parser.add_argument("--skip-relay", action="store_true",
                        help="build but do not swap tlsrelay (it hosts :8100)")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    os.chdir(root)
    client = connect()
    if args.health:
        health(client)
        client.close()
        return 0
    if args.restart_only:
        if args.hard:
            restart_hard(client)
        else:
            install_units(client)
            zero_restart_agent(client)
            health(client)
        client.close()
        return 0
    client.close()
    deploy(root, hard=args.hard, skip_relay=args.skip_relay)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
