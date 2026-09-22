"""Proxy pool manager with round-robin rotation and live bridge hot-swapping."""
from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
import uuid
from pathlib import Path
from typing import Optional

from agent.config import BASE_DIR
from agent.services.accounts import get_account, upsert_account
from agent.services.chrome_nicks import get_bridge
from agent.services.proxy_url import parse_proxy_url
from agent.services.surfshark import bind_nick_proxy, is_surfshark_url, probe_egress

logger = logging.getLogger(__name__)

PROXY_POOL_FILE = BASE_DIR / "agent" / "proxy_pool.json"
_ROTATION_LOCKS: dict[str, threading.Lock] = {}
_ROTATION_GUARD = threading.Lock()


def make_surfshark_url(nick_id: str, country: str = "vn", version: int = 1, ttl_min: int = 60) -> str:
    """Generate a sticky session proxy URL routed through Surfshark gateway."""
    clean_id = re.sub(r"[^a-zA-Z0-9_]", "_", nick_id)
    return bind_nick_proxy(f"http://surf__cr.{country};sessid.{clean_id}_v{version};ttl.{ttl_min}:flowkit2026@127.0.0.1:18888", nick_id)


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
    filter_quarantined: bool = True,
) -> Optional[str]:
    """Get the next proxy from pool, prioritizing proxies not currently assigned to other nicks or current nick."""
    pool = load_proxy_pool(path)
    all_proxies = [p for p in pool.get("proxies", []) if not is_surfshark_url(p)]
    if not all_proxies:
        return make_surfshark_url(nick_id) if nick_id else None

    if filter_quarantined:
        try:
            from agent.services.proxy_checker import is_quarantined
            clean_proxies = [p for p in all_proxies if not is_quarantined(p)]
            proxies = clean_proxies if clean_proxies else all_proxies
        except Exception:
            proxies = all_proxies
    else:
        proxies = all_proxies

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

    pool["current_index"] = (pool.get("current_index", 0) + 1) % len(all_proxies)
    save_proxy_pool(pool, path)
    return selected


def get_verified_proxy_for_nick(
    nick_id: str = "",
    path: Path | None = None,
    accounts_path: Path | None = None,
    attempts: int = 5,
    timeout: int = 8,
) -> Optional[str]:
    """Pick a pool proxy for nick_id only after it proves a public egress IP.

    Auto-assign previously trusted pool order, which once handed a literal
    `user:pass@1.2.3.4:8080` placeholder to a new nick — Chrome launched with a
    dead upstream and could not reach Google sign-in. Each candidate is probed
    before assignment; if none verify we still return a fresh Surfshark sticky
    session (the system's primary allocator) rather than persist a known-dead
    or placeholder proxy.
    """
    for _ in range(max(1, attempts)):
        candidate = get_next_proxy_for_nick(
            nick_id, path=path, accounts_path=accounts_path
        )
        if not candidate:
            break
        try:
            ip = probe_egress(candidate, timeout=timeout)
            logger.info("Verified proxy candidate for %s -> egress %s", nick_id, ip)
            return candidate
        except Exception as exc:
            logger.warning(
                "Proxy candidate for %s failed egress probe: %s", nick_id, exc
            )
    if nick_id:
        logger.warning(
            "No pool proxy verified for %s; falling back to fresh Surfshark session",
            nick_id,
        )
        return make_surfshark_url(nick_id)
    return None


async def rotate_nick_proxy(
    nick_id: str = "nick-a",
    path: Path | None = None,
    preflight: bool = True,
    accounts_path: Path | None = None,
    target_proxy: str | None = None,
) -> dict:
    # Watchdog uses its own event loop; browser sockets and bridges belong to
    # the API loop. Move the whole mutation there before taking a nick lock.
    from agent.services.flow_client import get_flow_client
    loop = get_flow_client()._event_loop
    if loop and loop.is_running() and loop is not asyncio.get_running_loop():
        task = asyncio.run_coroutine_threadsafe(
            rotate_nick_proxy(nick_id, path, preflight, accounts_path, target_proxy), loop)
        return await asyncio.wrap_future(task)
    # Watchdog and API run on different threads/event loops. Never use a
    # process-global asyncio.Lock here; a concurrent rotation fails promptly.
    with _ROTATION_GUARD:
        lock = _ROTATION_LOCKS.setdefault(nick_id, threading.Lock())
    if not lock.acquire(blocking=False):
        return {"ok": False, "error": "ROTATION_IN_PROGRESS", "nick_id": nick_id}
    try:
        return await _rotate_nick_proxy(nick_id, path, preflight, accounts_path, target_proxy)
    except RuntimeError as exc:
        return {"ok": False, "error": str(exc), "nick_id": nick_id}
    finally:
        lock.release()


async def _rotate_nick_proxy(
    nick_id: str, path: Path | None, preflight: bool,
    accounts_path: Path | None, target_proxy: str | None,
) -> dict:
    """Hot-swap the upstream proxy for nick_id to the next verified clean proxy in the pool or target_proxy."""
    account = get_account(nick_id, path=accounts_path)
    if account is None:
        return {"ok": False, "error": f"Unknown account {nick_id}"}

    next_url = target_proxy
    current_url = account.get("proxy_url", "")

    egress_ip = None
    if is_surfshark_url(current_url) and target_proxy and not is_surfshark_url(target_proxy):
        return {"ok": False, "error": "EXCLUSIVE_EGRESS_REQUIRED: target must use the nick IP allocator"}
    if is_surfshark_url(next_url or current_url):
        from agent.services.proxy_checker import check_single_proxy
        session = uuid.uuid4().hex
        next_url = bind_nick_proxy(next_url or current_url, nick_id, session=session)
        prepared = bind_nick_proxy(next_url, nick_id, prepare=True)
        try:
            # Preparing reserves a unique candidate without moving the live
            # nick. IP verification is mandatory even if preflight=False.
            candidate_ip = await asyncio.to_thread(probe_egress, prepared)
            check = await asyncio.to_thread(check_single_proxy, prepared, timeout=8)
            if check.get("status") != "CLEAN" or not check.get("labs_accessible"):
                return {"ok": False, "error": "PROXY_PREFLIGHT_FAILED", "status": check.get("status")}
            egress_ip = candidate_ip
        except Exception as exc:
            logger.warning("Unique IP rotation unavailable for %s: %s", nick_id, type(exc).__name__)
            return {"ok": False, "error": "NO_VERIFIED_DISTINCT_EGRESS", "nick_id": nick_id}

    if not next_url:
        pool = load_proxy_pool(path)
        proxies = pool.get("proxies") or []
        if not proxies:
            return {"ok": False, "error": "Proxy pool is empty"}

        if preflight:
            try:
                from agent.services.proxy_checker import check_single_proxy
                tested_candidates: set[str] = set()
                # Search candidates in pool until a healthy one is verified on Google
                for _ in range(len(proxies)):
                    candidate = get_next_proxy_for_nick(
                        nick_id, path, accounts_path=accounts_path, exclude_current=True, filter_quarantined=True
                    ) or get_next_proxy(path)
                    if not candidate:
                        break
                    if candidate in tested_candidates:
                        continue
                    tested_candidates.add(candidate)
                    check = await asyncio.to_thread(check_single_proxy, candidate, timeout=4)
                    if check.get("status") == "CLEAN":
                        next_url = candidate
                        logger.info("Pre-flight check passed for %s: %s (%d ms)", nick_id, check["masked"], check["latency_ms"])
                        break
                    else:
                        logger.warning("Pre-flight check rejected %s for %s: %s (%s)", check["masked"], nick_id, check["status"], check["error"])
            except Exception as e:
                logger.error("Pre-flight checker error: %s", e)

        if not next_url:
            if preflight:
                return {"ok": False, "error": "NO_HEALTHY_PROXY"}
            next_url = get_next_proxy_for_nick(
                nick_id, path, accounts_path=accounts_path, exclude_current=True, filter_quarantined=True
            ) or get_next_proxy_for_nick(
                nick_id, path, accounts_path=accounts_path, exclude_current=True, filter_quarantined=False
            ) or get_next_proxy(path)

        if not next_url:
            return {"ok": False, "error": "Proxy pool is empty"}

    if is_surfshark_url(next_url) and egress_ip is None:
        return await _rotate_nick_proxy(nick_id, path, preflight, accounts_path, next_url)

    from agent.services.extension_maintenance import proxy_maintenance
    async with proxy_maintenance(nick_id) as maintenance:
        if is_surfshark_url(next_url):
            maintenance["changed"] = True
            try:
                committed_ip = await asyncio.to_thread(probe_egress, next_url)
            except Exception:
                return {"ok": False, "error": "EGRESS_COMMIT_UNVERIFIED"}
            if committed_ip != egress_ip:
                return {"ok": False, "error": "EGRESS_CHANGED_DURING_ROTATION"}
        maintenance["changed"] = True
        result = await _apply_nick_proxy(account, nick_id, next_url, accounts_path, egress_ip)
    result["flow_tab_reloaded"] = maintenance["reloaded"]
    return result


async def _apply_nick_proxy(account, nick_id, next_url, accounts_path, egress_ip):
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
        "egress_ip": egress_ip,
    }
