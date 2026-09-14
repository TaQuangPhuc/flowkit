"""Automatic reversion script for temporary 24-hour test proxies.

Checks if the temporary proxy testing period has expired (default: 24h).
When expired, it cleanly removes the temporary proxies from agent/proxy_pool.json,
reassigns any accounts currently using temporary proxies to clean original proxies,
preserves all user accounts, and reloads/restarts the workers.
"""
import argparse
import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("revert_temp_proxies")

SESSION_FILE = BASE_DIR / "agent" / "temp_proxy_session.json"
PROXY_POOL_FILE = BASE_DIR / "agent" / "proxy_pool.json"
PROXY_POOL_BACKUP = BASE_DIR / "agent" / "proxy_pool.backup.json"
ACCOUNTS_FILE = BASE_DIR / "agent" / "accounts.json"
ACCOUNTS_BACKUP = BASE_DIR / "agent" / "accounts.backup.json"


def is_expired(force: bool = False) -> tuple[bool, dict]:
    """Check if temporary proxies are expired."""
    if not SESSION_FILE.exists():
        return False, {}
    try:
        session = json.loads(SESSION_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("Could not read session file %s: %s", SESSION_FILE, e)
        return False, {}

    expires_at = session.get("expires_at", 0)
    now = time.time()
    if force or now >= expires_at:
        return True, session
    return False, session


def revert(force: bool = False) -> bool:
    """Revert temporary proxies if expired or forced."""
    expired, session = is_expired(force=force)
    if not expired:
        now = time.time()
        rem = session.get("expires_at", 0) - now if session else 0
        if session:
            logger.info("Temporary proxies still active (%.1f hours remaining). No revert needed.", rem / 3600)
        return False

    logger.warning("⏰ Temporary proxy test period expired (or force requested). Reverting temporary proxies...")

    temp_proxies = set(session.get("proxies") or [])
    reverted = False

    # 1. Update proxy_pool.json: remove temporary proxies, keep original proxies
    try:
        if PROXY_POOL_FILE.exists():
            pool_data = json.loads(PROXY_POOL_FILE.read_text(encoding="utf-8"))
            current_proxies = pool_data.get("proxies") or []
            remaining_proxies = [p for p in current_proxies if p not in temp_proxies]
            if not remaining_proxies and PROXY_POOL_BACKUP.exists():
                backup_data = json.loads(PROXY_POOL_BACKUP.read_text(encoding="utf-8"))
                remaining_proxies = backup_data.get("proxies") or []

            pool_data["proxies"] = remaining_proxies
            pool_data.pop("temporary_session", None)
            PROXY_POOL_FILE.write_text(json.dumps(pool_data, indent=2) + "\n", encoding="utf-8")
            logger.info("Removed %d temporary proxies from %s (remaining: %d).", len(temp_proxies), PROXY_POOL_FILE, len(remaining_proxies))
            reverted = True
        elif PROXY_POOL_BACKUP.exists():
            shutil.copy2(PROXY_POOL_BACKUP, PROXY_POOL_FILE)
            logger.info("Restored %s from backup.", PROXY_POOL_FILE)
            reverted = True
    except Exception as e:
        logger.error("Failed to update proxy_pool.json during revert: %s", e)

    # 2. Update accounts.json: reassign accounts using temporary proxies back to original proxies
    try:
        if ACCOUNTS_FILE.exists():
            acc_data = json.loads(ACCOUNTS_FILE.read_text(encoding="utf-8"))
            pool_data = json.loads(PROXY_POOL_FILE.read_text(encoding="utf-8")) if PROXY_POOL_FILE.exists() else {}
            available_proxies = pool_data.get("proxies") or []
            
            p_idx = 0
            for acc in acc_data.get("accounts", []):
                curr_proxy = acc.get("proxy_url") or ""
                if curr_proxy in temp_proxies or not curr_proxy:
                    if available_proxies:
                        new_p = available_proxies[p_idx % len(available_proxies)]
                        p_idx += 1
                        acc["proxy_url"] = new_p
                        logger.info("Reassigned account %s from expired temp proxy to %s", acc.get("id"), new_p.split("@")[-1])
                    else:
                        acc["proxy_url"] = ""
            
            ACCOUNTS_FILE.write_text(json.dumps(acc_data, indent=2) + "\n", encoding="utf-8")
            logger.info("Updated accounts.json with permanent proxies.")
            reverted = True
    except Exception as e:
        logger.error("Failed to update accounts.json during revert: %s", e)

    # 3. Restart flowkit service to re-initialize workers cleanly
    try:
        import subprocess
        res = subprocess.run(["systemctl", "--user", "restart", "flowkit"], capture_output=True, text=True, timeout=15)
        if res.returncode == 0:
            logger.info("Restarted flowkit.service successfully with restored proxies.")
        else:
            logger.warning("systemctl restart flowkit returned code %d: %s", res.returncode, res.stderr)
    except Exception as e:
        logger.warning("Could not restart flowkit via systemctl: %s", e)

    # 4. Remove session file to mark completed
    if SESSION_FILE.exists():
        try:
            SESSION_FILE.unlink()
            logger.info("Removed temporary session marker %s.", SESSION_FILE)
        except Exception as e:
            logger.warning("Failed to remove %s: %s", SESSION_FILE, e)

    logger.info("✅ 24-hour test proxy cleanup complete. System reverted to original proxies.")
    return reverted


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check and revert temporary 24h proxies.")
    parser.add_argument("--force", action="store_true", help="Force revert immediately")
    parser.add_argument("--check-only", action="store_true", help="Check remaining time without reverting")
    args = parser.parse_args()

    if args.check_only:
        expired, session = is_expired(force=False)
        if not session:
            print("No active temporary proxy session found.")
        else:
            rem = session.get("expires_at", 0) - time.time()
            if rem <= 0:
                print(f"Session EXPIRED ({abs(rem):.0f}s ago). Needs revert.")
            else:
                print(f"Session active. Remaining: {rem/3600:.2f} hours ({rem:.0f}s). Expiry: {session.get('expires_at_iso')}")
    else:
        revert(force=args.force)
