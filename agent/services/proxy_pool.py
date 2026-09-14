"""Proxy pool manager with round-robin rotation and live bridge hot-swapping."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from agent.config import BASE_DIR
from agent.services.accounts import get_account, upsert_account
from agent.services.chrome_nicks import get_bridge
from agent.services.proxy_url import parse_proxy_url

logger = logging.getLogger(__name__)

PROXY_POOL_FILE = BASE_DIR / "agent" / "proxy_pool.json"


def _pool_path(path: Path | None = None) -> Path:
    return path or PROXY_POOL_FILE


def load_proxy_pool(path: Path | None = None) -> dict:
    target = _pool_path(path)
    if not target.exists():
        return {"proxies": [], "current_index": 0}
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
        if not isinstance(data.get("proxies"), list):
            data["proxies"] = []
        if not isinstance(data.get("current_index"), int):
            data["current_index"] = 0
        return data
    except Exception as exc:
        logger.warning("Failed to load proxy pool from %s: %s", target, exc)
        return {"proxies": [], "current_index": 0}


def save_proxy_pool(data: dict, path: Path | None = None) -> None:
    target = _pool_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def add_proxies_to_pool(new_proxies: list[str], path: Path | None = None) -> dict:
    pool = load_proxy_pool(path)
    current_set = set(pool["proxies"])
    added = 0
    for p in new_proxies:
        p_clean = p.strip()
        if not p_clean:
            continue
        if not p_clean.startswith("http://") and not p_clean.startswith("https://"):
            p_clean = "http://" + p_clean
        if p_clean not in current_set:
            pool["proxies"].append(p_clean)
            current_set.add(p_clean)
            added += 1
    save_proxy_pool(pool, path)
    logger.info("Added %d proxies to pool (total: %d)", added, len(pool["proxies"]))
    return pool


def get_next_proxy(path: Path | None = None) -> Optional[str]:
    pool = load_proxy_pool(path)
    proxies = pool.get("proxies") or []
    if not proxies:
        return None
    idx = pool.get("current_index", 0) % len(proxies)
    selected = proxies[idx]
    pool["current_index"] = (idx + 1) % len(proxies)
    save_proxy_pool(pool, path)
    return selected


def get_next_proxy_for_nick(
    nick_id: str = "",
    path: Path | None = None,
    accounts_path: Path | None = None,
    exclude_current: bool = True,
) -> Optional[str]:
    """Get the next proxy from pool, prioritizing proxies not currently assigned to other nicks or current nick."""
    pool = load_proxy_pool(path)
    proxies = pool.get("proxies") or []
    if not proxies:
        return None

    used_proxies = set()
    current_proxy = None
    try:
        from agent.services.accounts import load_accounts
        for acc in load_accounts(accounts_path):
            p = (acc.get("proxy_url") or "").strip()
            if not p:
                continue
            if acc.get("id") == nick_id:
                current_proxy = p
            else:
                used_proxies.add(p)
    except Exception:
        pass

    # Exclude both other nicks' proxies and current nick's proxy if multiple proxies exist
    if exclude_current and current_proxy and len(proxies) > 1:
        available = [p for p in proxies if p not in used_proxies and p != current_proxy]
        if not available:
            available = [p for p in proxies if p != current_proxy]
    else:
        available = [p for p in proxies if p not in used_proxies]

    candidates = available if available else proxies

    idx = pool.get("current_index", 0) % len(candidates)
    selected = candidates[idx]

    pool["current_index"] = (pool.get("current_index", 0) + 1) % len(proxies)
    save_proxy_pool(pool, path)
    return selected


async def rotate_nick_proxy(
    nick_id: str = "nick-a",
    path: Path | None = None,
    preflight: bool = True,
    accounts_path: Path | None = None,
) -> dict:
    """Hot-swap the upstream proxy for nick_id to the next verified clean proxy in the pool."""
    pool = load_proxy_pool(path)
    proxies = pool.get("proxies") or []
    if not proxies:
        return {"ok": False, "error": "Proxy pool is empty"}

    next_url = None
    if preflight:
        try:
            from agent.services.proxy_checker import check_single_proxy
            # Search candidates in pool until a healthy one is verified on Google
            for _ in range(len(proxies)):
                candidate = get_next_proxy_for_nick(
                    nick_id, path, accounts_path=accounts_path, exclude_current=True
                ) or get_next_proxy(path)
                if not candidate:
                    break
                check = check_single_proxy(candidate, timeout=4)
                if check.get("status") == "CLEAN":
                    next_url = candidate
                    logger.info("Pre-flight check passed for %s: %s (%d ms)", nick_id, check["masked"], check["latency_ms"])
                    break
                else:
                    logger.warning("Pre-flight check rejected %s for %s: %s (%s)", check["masked"], nick_id, check["status"], check["error"])
        except Exception as e:
            logger.error("Pre-flight checker error: %s", e)

    if not next_url:
        next_url = get_next_proxy_for_nick(
            nick_id, path, accounts_path=accounts_path, exclude_current=True
        ) or get_next_proxy(path)

    if not next_url:
        return {"ok": False, "error": "Proxy pool is empty"}

    account = get_account(nick_id, path=accounts_path)
    if account is None:
        return {"ok": False, "error": f"Unknown account {nick_id}"}

    account["proxy_url"] = next_url
    upsert_account(account, path=accounts_path)

    parsed = parse_proxy_url(next_url)
    bridge = get_bridge(nick_id)
    switched_live = False
    if bridge and bridge.port:
        bridge.switch_upstream(parsed)
        switched_live = True
        logger.info(
            "Hot-swapped proxy for %s on bridge 127.0.0.1:%d -> %s",
            nick_id, bridge.port, parsed.redacted,
        )
    else:
        from agent.services.chrome_nicks import ensure_bridge, running_chrome_proxy_port
        existing_port = running_chrome_proxy_port(nick_id)
        if existing_port:
            try:
                bridge = await ensure_bridge(nick_id, parsed, port=existing_port)
                switched_live = True
                logger.info(
                    "Restored proxy bridge on 127.0.0.1:%d -> %s for %s",
                    existing_port, parsed.redacted, nick_id,
                )
            except Exception as exc:
                logger.error("Failed to restore bridge on port %d: %s", existing_port, exc)


    return {
        "ok": True,
        "nick_id": nick_id,
        "switched_live": switched_live,
        "proxy": parsed.redacted,
        "proxy_url": next_url,
    }
