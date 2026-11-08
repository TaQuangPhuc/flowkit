#!/usr/bin/env python3
"""Install per-nick mitmdump (WireGuard) on the generation VPS.

Reuses local WG key files and the PC mitmproxy CA so phones only change
Endpoint to 180.93.43.217 — no new CA trust, no Google re-login.

Every serving nick gets an instance, including landon (no upstream proxy).

    python scripts/vps_mitm_setup.py
    python scripts/vps_mitm_setup.py --status
"""
from __future__ import annotations

import io
import sys
from pathlib import Path
from urllib.parse import urlparse

import paramiko

HOST = "180.93.43.217"
USER = "root"
KEY = Path.home() / ".ssh" / "flowkit_vps"
ROOT = Path(__file__).resolve().parents[1]
REMOTE_MITM = "/opt/flowkit/mitm"
REMOTE_LOG = "/opt/flowkit/.scratch/mitm"
CA_SRC = Path.home() / ".mitmproxy"

# Same ports as the existing phone tunnels (do not renumber).
NICKS = (
    ("friend", 51820, 28020),
    ("gemma", 51822, 28022),
    ("kailyn", 51823, 28023),
    ("madyson", 51824, 28024),
    ("landon", 51825, 28025),
)


def connect() -> paramiko.SSHClient:
    if not KEY.is_file():
        sys.exit(f"missing deploy key {KEY} — see docs/VPS.md")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        HOST, username=USER, key_filename=str(KEY),
        timeout=20, allow_agent=False, look_for_keys=False,
    )
    return client


def run(client: paramiko.SSHClient, cmd: str, timeout: int = 300) -> str:
    _, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode("utf-8", "replace")
    err = stderr.read().decode("utf-8", "replace")
    code = stdout.channel.recv_exit_status()
    if code != 0:
        raise SystemExit(f"$ {cmd}\n{out}\n{err}\nexit {code}")
    return out


def put_text(sftp: paramiko.SFTPClient, remote: str, text: str, mode: int = 0o644):
    bio = io.BytesIO(text.encode("utf-8"))
    sftp.putfo(bio, remote)
    sftp.chmod(remote, mode)


def put_file(sftp: paramiko.SFTPClient, local: Path, remote: str, mode: int = 0o644):
    sftp.put(str(local), remote)
    sftp.chmod(remote, mode)


def proxy_parts(nick: str) -> tuple[str, str]:
    """Return (MITM_UPSTREAM host-only URL, user:pass) or ("", "")."""
    p = ROOT / "nicks" / nick / "proxy.txt"
    if not p.is_file():
        return "", ""
    raw = p.read_text(encoding="utf-8").strip()
    if not raw:
        return "", ""
    u = urlparse(raw)
    if not u.hostname or not u.port:
        raise SystemExit(f"bad proxy.txt for {nick}")
    upstream = f"{u.scheme or 'http'}://{u.hostname}:{u.port}"
    auth = ""
    if u.username is not None:
        auth = u.username
        if u.password is not None:
            auth += f":{u.password}"
    return upstream, auth


def env_text(nick: str, wg_port: int, http_port: int) -> str:
    upstream, auth = proxy_parts(nick)
    lines = [
        f"MITM_NICK={nick}",
        f"WG_PORT={wg_port}",
        f"HTTP_PORT={http_port}",
        f"MITM_LOG_DIR={REMOTE_LOG}",
    ]
    if upstream:
        lines.append(f"MITM_UPSTREAM={upstream}")
    if auth:
        lines.append(f"UPSTREAM_AUTH={auth}")
    lines.append("")
    return "\n".join(lines)


def status(client: paramiko.SSHClient) -> None:
    names = " ".join(f"flowkit-mitm@{n}" for n, _, _ in NICKS)
    print(run(client, f"systemctl is-active {names} 2>/dev/null || true").rstrip())
    print(run(
        client,
        "ss -ulnp | grep -E '5182[0-5]' || true; "
        "ls -l /opt/flowkit/.scratch/mitm 2>/dev/null | tail -20 || true",
    ).rstrip())


def install(client: paramiko.SSHClient) -> None:
    for nick, _, _ in NICKS:
        kf = ROOT / ".omc" / "research" / "wg" / f"{nick}.key"
        if not kf.is_file():
            raise SystemExit(f"missing WG key {kf}")
    ca_pem = CA_SRC / "mitmproxy-ca.pem"
    if not ca_pem.is_file():
        raise SystemExit(f"missing mitmproxy CA {ca_pem} — phones already trust this cert")

    run(client, f"mkdir -p {REMOTE_MITM}/wg {REMOTE_MITM}/ca {REMOTE_LOG} /etc/flowkit")
    sftp = client.open_sftp()
    try:
        put_file(sftp, ROOT / ".omc" / "research" / "mitm_logger.py",
                 f"{REMOTE_MITM}/mitm_logger.py")
        put_file(sftp, ROOT / ".omc" / "research" / "tls_hello_dump.py",
                 f"{REMOTE_MITM}/tls_hello_dump.py")
        put_file(sftp, ROOT / "mitm" / "run.sh",
                 f"{REMOTE_MITM}/run.sh", 0o755)
        put_file(sftp, ROOT / "scripts" / "systemd" / "flowkit-mitm@.service",
                 "/etc/systemd/system/flowkit-mitm@.service")
        for nick, wg_port, http_port in NICKS:
            put_file(
                sftp,
                ROOT / ".omc" / "research" / "wg" / f"{nick}.key",
                f"{REMOTE_MITM}/wg/{nick}.key",
                0o600,
            )
            put_text(
                sftp,
                f"/etc/flowkit/mitm-{nick}.env",
                env_text(nick, wg_port, http_port),
                0o600,
            )
        for name in (
            "mitmproxy-ca.pem", "mitmproxy-ca-cert.pem",
            "mitmproxy-ca-cert.cer", "mitmproxy-dhparam.pem",
        ):
            src = CA_SRC / name
            if src.is_file():
                put_file(sftp, src, f"{REMOTE_MITM}/ca/{name}", 0o600)
    finally:
        sftp.close()

    print("installing mitmproxy into /opt/flowkit/venv-mitm (once)…")
    run(
        client,
        "test -x /opt/flowkit/venv-mitm/bin/mitmdump || "
        "/opt/python311/bin/python3.11 -m venv /opt/flowkit/venv-mitm",
    )
    run(
        client,
        "/opt/flowkit/venv-mitm/bin/pip install -q -U pip && "
        "/opt/flowkit/venv-mitm/bin/pip install -q 'mitmproxy>=10.3,<11'",
        timeout=300,
    )
    run(client, "systemctl daemon-reload")
    for nick, _, _ in NICKS:
        run(client, f"systemctl enable --now flowkit-mitm@{nick}")
    run(client, "sleep 2")
    status(client)


def main() -> int:
    client = connect()
    try:
        if "--status" in sys.argv:
            status(client)
            return 0
        install(client)
        print("UDP 51820/51822/51823/51824/51825 on", HOST)
        print("Phones: set Endpoint to 180.93.43.217:<port> or scan new QR (same keys).")
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
