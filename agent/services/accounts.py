"""Gitignored nick + proxy store. profiles.json stays a committed template."""
from __future__ import annotations

import json
import logging
import re
import os
import tempfile
import threading
import time
from functools import wraps
from pathlib import Path
from typing import Any, Optional

from agent.config import ACCOUNTS_FILE, PROFILES_FILE, load_flow_profiles
from agent.services.proxy_url import ProxyURLError, parse_proxy_url, redact_proxy_url
from agent.services.surfshark import bind_nick_proxy

logger = logging.getLogger(__name__)
_ACCOUNTS_LOCK = threading.RLock()


def _locked(fn):
    @wraps(fn)
    def call(*args, **kwargs):
        with _ACCOUNTS_LOCK:
            return fn(*args, **kwargs)
    return call

_ID_RE = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9._@+-]{0,127}$")
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I
)


def _path(path: Path | None = None) -> Path:
    return path or ACCOUNTS_FILE


def _normalize(row: dict, *, require_id: bool = True) -> dict:
    nick_id = str(row.get("id") or "").strip()
    if require_id and not nick_id:
        raise ValueError("account id is required")
    if nick_id and not _ID_RE.match(nick_id):
        raise ValueError(
            f"invalid account id {nick_id!r}; use letters, digits, dot, dash, underscore, '@', '+' (e.g. email or username)"
        )
    project = str(row.get("project_id") or "").strip()
    if project and not _UUID_RE.match(project):
        raise ValueError("project_id must be a Flow project uuid or empty")
    proxy = str(row.get("proxy_url") or "").strip()
    if proxy:
        parse_proxy_url(proxy)
        proxy = bind_nick_proxy(proxy, nick_id)
    normalized = {
        "id": nick_id,
        "label": str(row.get("label") or nick_id).strip() or nick_id,
        "project_id": project,
        "proxy_url": proxy,
        "note": str(row.get("note") or ""),
        "enabled": bool(row.get("enabled", True)),
        # Trusted-minter farm member: still connects and mints captcha tokens
        # for other nicks, but is excluded from real work routing.
        "mint_only": bool(row.get("mint_only", False)),
        # Alternate browser binary for this nick ("coccoc" → ~/.flowkit/coccoc-browser).
        # Empty = default Google Chrome.
        "browser": str(row.get("browser") or "").strip(),
    }
    # Preserve extension fields (clone_of, clone_ts, ...) — a fixed whitelist
    # silently stripped lineage and made the clone depth cap read clone_of
    # as None forever.
    for key, value in row.items():
        normalized.setdefault(key, value)
    return normalized


@_locked
def load_accounts(path: Path | None = None) -> list[dict]:
    target = _path(path)
    if not target.exists():
        return []
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("accounts file unreadable: %s", exc)
        return []
    rows = raw.get("accounts") if isinstance(raw, dict) else raw
    if not isinstance(rows, list):
        return []
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            out.append(_normalize(row))
        except (ValueError, ProxyURLError) as exc:
            logger.warning("skipping account %s: %s", row.get("id"), exc)
    return out


@_locked
def save_accounts(rows: list[dict], path: Path | None = None) -> list[dict]:
    normalized = []
    seen = set()
    for row in rows:
        item = _normalize(row)
        if item["id"] in seen:
            raise ValueError(f"duplicate account id {item['id']}")
        seen.add(item["id"])
        normalized.append(item)
    target = _path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {"accounts": normalized}
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=target.parent, delete=False) as tmp:
            temp_path = tmp.name
            tmp.write(json.dumps(payload, indent=2) + "\n")
            tmp.flush()
            os.fsync(tmp.fileno())
        os.replace(temp_path, target)
    finally:
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)
    return normalized


@_locked
def upsert_account(row: dict, path: Path | None = None, old_id: str | None = None) -> dict:
    rows = load_accounts(path)
    nick_id = str(row.get("id") or "").strip()
    old_nick_id = str(old_id or "").strip()

    # Handle rename if old_id provided and different
    if old_nick_id and old_nick_id != nick_id:
        try:
            from agent.services.chrome_nicks import chrome_running
            if chrome_running(old_nick_id):
                raise ValueError(
                    f"Cannot rename account {old_nick_id!r} while Chrome is running. "
                    "Please stop Chrome for this account before renaming."
                )
        except ImportError:
            pass

        if any(r["id"] == nick_id for r in rows):
            raise ValueError(f"account with id {nick_id!r} already exists")
        if not any(r["id"] == old_nick_id for r in rows):
            # Without this the rename fell through to the add path below and
            # silently created a second account instead of renaming one.
            raise ValueError(f"account {old_nick_id!r} not found (nothing to rename)")
        for i, existing in enumerate(rows):
            if existing["id"] == old_nick_id:
                # Keep every field the caller did not send (proxy, project,
                # note): a rename posted from the edit modal must not blank them.
                merged = {**existing, **{k: v for k, v in row.items() if v not in ("", None)}}
                merged["id"] = nick_id
                item = _normalize(merged)
                rows[i] = item
                save_accounts(rows, path)
                try:
                    from agent.services.chrome_nicks import chrome_data_dir
                    old_dir = chrome_data_dir(old_nick_id)
                    new_dir = chrome_data_dir(nick_id)
                    if old_dir.exists() and not new_dir.exists():
                        old_dir.rename(new_dir)
                        logger.info("Renamed chrome data dir from %s to %s", old_dir, new_dir)
                except Exception as exc:
                    logger.warning("Could not rename chrome data dir: %s", exc)
                return item

    existing_acc = next((r for r in rows if r["id"] == nick_id), None)

    proxy = str(row.get("proxy_url") or "").strip()
    if not proxy:
        if existing_acc and existing_acc.get("proxy_url"):
            row["proxy_url"] = existing_acc["proxy_url"]
        else:
            try:
                from agent.services.proxy_pool import get_verified_proxy_for_nick
                auto_proxy = get_verified_proxy_for_nick(nick_id)
                if auto_proxy:
                    row["proxy_url"] = auto_proxy
                    logger.info("Auto-assigned proxy from pool for nick %s: %s", nick_id, auto_proxy)
            except Exception as exc:
                logger.warning("Could not auto-assign proxy from pool: %s", exc)

    item = _normalize(row)
    for i, existing in enumerate(rows):
        if existing["id"] == item["id"]:
            merged = {**existing, **item}
            rows[i] = _normalize(merged)
            save_accounts(rows, path)
            return rows[i]
    rows.append(item)
    save_accounts(rows, path)
    return item


def rename_account(old_id: str, new_id: str, path: Path | None = None) -> dict:
    rows = load_accounts(path)
    old_acc = next((r for r in rows if r["id"] == old_id), None)
    if not old_acc:
        raise ValueError(f"account {old_id!r} not found")
    new_nick_id = str(new_id or "").strip()
    if not new_nick_id:
        raise ValueError("new account id is required")
    updated = dict(old_acc)
    updated["id"] = new_nick_id
    return upsert_account(updated, path=path, old_id=old_id)


def sync_account_project(nick_id: str, project_id: str | None = None, path: Path | None = None) -> dict:
    rows = load_accounts(path)
    acc = next((r for r in rows if r["id"] == nick_id), None)
    if not acc:
        raise ValueError(f"account {nick_id!r} not found")

    target_project = (project_id or "").strip()
    if not target_project:
        try:
            from agent.services.flow_client import get_flow_client
            workers = list(get_flow_client().workers() or [])
            for w in workers:
                if str(w.get("profile_id", "")).strip().lower() == nick_id.lower():
                    wp = str(w.get("project_id", "")).strip()
                    if wp:
                        target_project = wp
                        break
        except Exception as exc:
            logger.warning("Failed to lookup worker project_id for %s: %s", nick_id, exc)

    if not target_project:
        raise ValueError(f"no project UUID found for nick {nick_id}")

    acc["project_id"] = target_project
    return upsert_account(acc, path=path)


def delete_account(nick_id: str, path: Path | None = None) -> bool:
    rows = load_accounts(path)
    kept = [r for r in rows if r["id"] != nick_id]
    if len(kept) == len(rows):
        return False
    save_accounts(kept, path)
    return True


def get_account(nick_id: str, path: Path | None = None) -> Optional[dict]:
    for row in load_accounts(path):
        if row["id"] == nick_id:
            return row
    return None


_MINT_CACHE = {"ts": 0.0, "nicks": frozenset()}


def mint_only_nicks(path: Path | None = None, ttl: float = 5.0) -> frozenset:
    """Nicks flagged mint_only, cached briefly — called on the dispatch hot
    path where a disk read per RPC would be wasteful."""
    now = time.monotonic()
    if path is None and now - _MINT_CACHE["ts"] < ttl:
        return _MINT_CACHE["nicks"]
    nicks = frozenset(r["id"] for r in load_accounts(path) if r.get("mint_only"))
    if path is None:
        _MINT_CACHE.update(ts=now, nicks=nicks)
    return nicks


def seed_accounts_from_template(path: Path | None = None) -> list[dict]:
    """Create accounts.json from committed profiles.json if it does not exist."""
    target = _path(path)
    existing = load_accounts(target)
    if existing:
        return existing
    seeded = []
    for row in load_flow_profiles(PROFILES_FILE):
        seeded.append({
            "id": row["id"],
            "label": row["id"],
            "project_id": row.get("project_id") or "",
            "proxy_url": "",
            "note": row.get("note") or "",
            "enabled": True,
        })
    if not seeded:
        seeded = [
            {"id": "nick-a", "label": "nick-a", "project_id": "", "proxy_url": "", "note": "", "enabled": True},
            {"id": "nick-b", "label": "nick-b", "project_id": "", "proxy_url": "", "note": "", "enabled": True},
            {"id": "nick-c", "label": "nick-c", "project_id": "", "proxy_url": "", "note": "", "enabled": True},
        ]
    return save_accounts(seeded, target)


def load_nick_pins() -> list[dict]:
    """Pins the router uses: accounts.json if present, else profiles.json."""
    accounts = load_accounts()
    if accounts:
        return [
            {"id": a["id"], "project_id": a.get("project_id") or "", "note": a.get("note") or ""}
            for a in accounts if a.get("enabled", True)
        ]
    return load_flow_profiles()


def public_account(row: dict, *, reveal: bool = False) -> dict:
    proxy = row.get("proxy_url") or ""
    return {
        "id": row["id"],
        "label": row.get("label") or row["id"],
        "project_id": row.get("project_id") or "",
        "proxy_url": proxy if reveal else "",
        "proxy_display": redact_proxy_url(proxy) if proxy else "",
        "has_proxy": bool(proxy),
        "note": row.get("note") or "",
        "enabled": bool(row.get("enabled", True)),
        "mint_only": bool(row.get("mint_only", False)),
        "browser": str(row.get("browser") or ""),
    }


_PORTED = ("image", "upload", "t2v", "i2v")


def _disabled_reason(row: dict) -> str | None:
    """Why an enabled=False nick is off: auth expiry vs a manual toggle."""
    if row.get("enabled", True):
        return None
    try:
        from agent.services.incident_manager import get_incident_manager
        for inc in get_incident_manager().get_incidents(module="worker", status="OPEN"):
            if inc.get("job_id") == row.get("id") and inc.get("error_code") in (
                "ACCOUNT_AUTH_EXPIRED", "ACCOUNT_SESSION_FLAGGED",
            ):
                return "need_relogin"
    except Exception:
        pass
    return "disabled"


def nick_next_action(row: dict) -> str | None:
    """First setup step still owed on this nick, or None when r2v is ready too."""
    blocked = _disabled_reason(row)
    if blocked:
        return blocked
    if not row.get("chrome_running"):
        return "need_chrome"
    if not row.get("has_proxy"):
        return "need_proxy"
    if not str(row.get("project_id") or "").strip():
        return "need_project"
    if not row.get("connected"):
        return "need_extension"
    worker = row.get("worker") or {}
    if not worker.get("chat_session"):
        return "need_ingredients"
    return None


def nick_api_status(row: dict) -> list[dict]:
    """Per-API readiness for the nicks page. status: ok | need | blocked."""
    from agent.config import FLOW_ALLOW_DEGRADED

    blocked = _disabled_reason(row)
    if blocked:
        status = "need" if blocked == "need_relogin" else "blocked"
        return [
            {"id": key, "status": status, "reason": blocked}
            for key in (*_PORTED, "r2v", "upscale", "chain", "omni")
        ]

    chrome = bool(row.get("chrome_running"))
    connected = bool(row.get("connected"))
    project = bool(str(row.get("project_id") or "").strip())
    chat = bool((row.get("worker") or {}).get("chat_session"))

    def ported(*, need_chat: bool = False) -> tuple[str, str]:
        if not chrome:
            return "need", "need_chrome"
        if not project:
            return "need", "need_project"
        if not connected:
            return "need", "need_extension"
        if need_chat and not chat:
            return "need", "need_ingredients"
        return "ok", "ok"

    apis: list[dict] = []
    for key in _PORTED:
        status, reason = ported()
        apis.append({"id": key, "status": status, "reason": reason})
    status, reason = ported(need_chat=True)
    apis.append({"id": "r2v", "status": status, "reason": reason})
    apis.append({"id": "upscale", "status": "blocked", "reason": "blocked_unported"})
    if FLOW_ALLOW_DEGRADED:
        st, rs = ported()
        apis.append({
            "id": "chain",
            "status": st,
            "reason": "degraded_chain" if st == "ok" else rs,
        })
    else:
        apis.append({"id": "chain", "status": "blocked", "reason": "blocked_chain"})
    apis.append({"id": "omni", "status": "blocked", "reason": "blocked_unported"})
    return apis
