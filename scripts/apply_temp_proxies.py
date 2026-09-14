"""Apply 10 temporary proxies for a 24-hour test period with automatic reversion."""
import datetime
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

PROXY_POOL_FILE = BASE_DIR / "agent" / "proxy_pool.json"
PROXY_POOL_BACKUP = BASE_DIR / "agent" / "proxy_pool.backup.json"
ACCOUNTS_FILE = BASE_DIR / "agent" / "accounts.json"
ACCOUNTS_BACKUP = BASE_DIR / "agent" / "accounts.backup.json"
SESSION_FILE = BASE_DIR / "agent" / "temp_proxy_session.json"

RAW_PROXIES = [
    "103.82.194.60:12880:09qt:09qt",
    "103.82.194.60:32852:09qt:09qt",
    "103.82.194.60:14880:09qt:09qt",
    "103.82.194.60:28872:09qt:09qt",
    "103.82.194.60:27188:09qt:09qt",
    "103.166.184.92:28531:09qt:09qt",
    "103.166.184.92:30867:09qt:09qt",
    "103.166.184.92:35413:09qt:09qt",
    "103.166.184.92:11776:09qt:09qt",
    "103.166.184.92:16381:09qt:09qt",
]


def format_proxy_url(raw: str) -> str:
    ip, port, user, pwd = raw.strip().split(":")
    return f"http://{user}:{pwd}@{ip}:{port}"


def main():
    now_ts = int(time.time())
    duration_s = 24 * 3600
    expire_ts = now_ts + duration_s

    now_dt = datetime.datetime.fromtimestamp(now_ts).astimezone()
    expire_dt = datetime.datetime.fromtimestamp(expire_ts).astimezone()

    now_iso = now_dt.isoformat()
    expire_iso = expire_dt.isoformat()
    systemd_expire_str = expire_dt.strftime("%Y-%m-%d %H:%M:%S")

    print(f"=== Activating 10 Temporary Proxies (24h Test) ===")
    print(f"Start Time:  {now_iso} (ts: {now_ts})")
    print(f"Expiry Time: {expire_iso} (ts: {expire_ts})")

    # 1. Ensure Backups exist
    if not PROXY_POOL_BACKUP.exists():
        shutil.copy2(PROXY_POOL_FILE, PROXY_POOL_BACKUP)
        print(f"Created backup: {PROXY_POOL_BACKUP}")
    else:
        print(f"Preserved existing backup: {PROXY_POOL_BACKUP}")

    if not ACCOUNTS_BACKUP.exists():
        shutil.copy2(ACCOUNTS_FILE, ACCOUNTS_BACKUP)
        print(f"Created backup: {ACCOUNTS_BACKUP}")
    else:
        print(f"Preserved existing backup: {ACCOUNTS_BACKUP}")

    # 2. Format 10 Proxies
    proxy_urls = [format_proxy_url(r) for r in RAW_PROXIES]

    # 3. Save Session File
    session_data = {
        "created_at": now_ts,
        "created_at_iso": now_iso,
        "expires_at": expire_ts,
        "expires_at_iso": expire_iso,
        "duration_seconds": duration_s,
        "duration_hours": 24,
        "proxies": proxy_urls,
    }
    SESSION_FILE.write_text(json.dumps(session_data, indent=2) + "\n", encoding="utf-8")
    print(f"Saved session to {SESSION_FILE}")

    # 4. Update agent/proxy_pool.json
    pool_data = {
        "proxies": proxy_urls,
        "current_index": 0,
        "temporary_session": {
            "expires_at": expire_ts,
            "expires_at_iso": expire_iso,
        }
    }
    PROXY_POOL_FILE.write_text(json.dumps(pool_data, indent=2) + "\n", encoding="utf-8")
    print(f"Updated {PROXY_POOL_FILE} with 10 temporary test proxies.")

    # 5. Update agent/accounts.json
    # nick-a -> proxy #1 (103.82.194.60:12880, Viettel)
    # Nick-b -> proxy #6 (103.166.184.92:28531, VNPT)
    accounts_data = json.loads(ACCOUNTS_FILE.read_text(encoding="utf-8"))
    for acc in accounts_data.get("accounts", []):
        if acc.get("id") == "nick-a":
            acc["proxy_url"] = proxy_urls[0]
            acc["note"] = "Chrome A. Temp 24h test (Viettel Residential)."
        elif acc.get("id") == "Nick-b":
            acc["proxy_url"] = proxy_urls[5]
            acc["note"] = "Chrome B. Temp 24h test (VNPT Residential)."
    ACCOUNTS_FILE.write_text(json.dumps(accounts_data, indent=2) + "\n", encoding="utf-8")
    print(f"Updated {ACCOUNTS_FILE} for nick-a and Nick-b.")

    # 6. Configure Systemd timer for exact 24h expiration
    systemd_user_dir = Path.home() / ".config" / "systemd" / "user"
    systemd_user_dir.mkdir(parents=True, exist_ok=True)

    service_file = systemd_user_dir / "flowkit-proxy-expire.service"
    timer_file = systemd_user_dir / "flowkit-proxy-expire.timer"

    service_content = f"""[Unit]
Description=Revert FlowKit Temporary Proxies After 24 Hours
After=network.target

[Service]
Type=oneshot
WorkingDirectory={BASE_DIR}
ExecStart={BASE_DIR}/venv/bin/python {BASE_DIR}/scripts/revert_temp_proxies.py
"""
    service_file.write_text(service_content, encoding="utf-8")

    timer_content = f"""[Unit]
Description=Timer for FlowKit Temporary Proxy Expiration

[Timer]
OnCalendar={systemd_expire_str}
Persistent=true

[Install]
WantedBy=timers.target
"""
    timer_file.write_text(timer_content, encoding="utf-8")
    print(f"Generated systemd user service & timer scheduled at {systemd_expire_str}.")

    # Enable and start timer
    try:
        subprocess.run(["systemctl", "--user", "daemon-reload"], check=True)
        subprocess.run(["systemctl", "--user", "enable", "--now", "flowkit-proxy-expire.timer"], check=True)
        print("Systemd timer enabled and started successfully.")
    except Exception as exc:
        print(f"Warning: Failed to enable systemd timer: {exc}")

    # 7. Restart flowkit service to immediately apply new proxies to Chrome workers
    print("Restarting flowkit.service...")
    try:
        subprocess.run(["systemctl", "--user", "restart", "flowkit"], check=True)
        print("flowkit.service restarted successfully.")
    except Exception as exc:
        print(f"Warning: Failed to restart flowkit.service: {exc}")


if __name__ == "__main__":
    main()
