"""GoLogin Orbita-backed nick launcher.

A nick with account browser="gologin" runs inside a GoLogin cloud profile
instead of a plain Chrome user-data-dir. The antidetect fingerprint
(canvas/WebGL/fonts/UA consistency) is the point: plain Chrome clones all
share one device fingerprint, which is what lets Google correlate concurrent
instances of the same account and revoke them.

Lifecycle notes:

- Profiles are created once via the API and their id is stored on the
  account row as ``gologin_profile``.
- Orbita materializes the profile under ``GOLOGIN_DIR/gologin_<id>``. We
  launch with local=True once that dir exists, so a hard kill loses nothing —
  the session lives on disk like a Chrome profile. (local=False would
  re-download the cloud zip and wipe uncommitted state on every start.)
- The SDK is never asked to stop(); orbita is killed by pid pattern like
  Chrome, so FlowKit restarts do not orphan or wipe profiles.
- Proxy is set on the profile itself (changeProfileProxy), which Orbita
  enforces natively — including SOCKS5 auth that plain Chrome cannot do —
  so no local bridge is needed for gologin nicks.
- The FlowKit extension is baked per nick into the usual chrome_data_dir and
  passed via --load-extension; Orbita honors the flag (its own extension
  mechanism uses it).

Token comes from GOLOGIN_TOKEN or ~/.flowkit/gologin.json (never committed).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

os.environ.setdefault("DISABLE_TELEMETRY", "true")

GOLOGIN_DIR = Path(os.environ.get("GOLOGIN_DIR", Path.home() / ".flowkit" / "gologin"))
_TOKEN_FILE = Path.home() / ".flowkit" / "gologin.json"

_handles: dict[str, object] = {}


def _token() -> str:
    tok = os.environ.get("GOLOGIN_TOKEN", "").strip()
    if tok:
        return tok
    try:
        return str(json.loads(_TOKEN_FILE.read_text(encoding="utf-8"))["token"]).strip()
    except Exception as exc:
        raise RuntimeError(
            f"GoLogin token missing — set GOLOGIN_TOKEN or {_TOKEN_FILE}"
        ) from exc


def profile_dir(profile_id: str) -> Path:
    return GOLOGIN_DIR / f"gologin_{profile_id}"


def nick_profile_id(account: dict) -> Optional[str]:
    pid = str(account.get("gologin_profile") or "").strip()
    return pid or None


def _client(profile_id: Optional[str], *, local: bool, extra_params: Optional[list] = None):
    from gologin import GoLogin

    GOLOGIN_DIR.mkdir(parents=True, exist_ok=True)
    return GoLogin({
        "token": _token(),
        "profile_id": profile_id,
        "tmpdir": str(GOLOGIN_DIR),
        "local": local,
        "extra_params": extra_params or [],
        "writeCookiesFromServer": False,
        "restore_last_session": True,
    })


def _create_profile(nick_id: str) -> str:
    gl = _client(None, local=True)
    data = gl.createProfileRandomFingerprint({"os": "lin", "name": nick_id})
    pid = str(data.get("id") or data.get("profile_id") or "").strip()
    if not pid:
        raise RuntimeError(f"gologin profile create returned no id: {str(data)[:200]}")
    return pid


def _set_profile_proxy(profile_id: str, proxy_url: str) -> None:
    from agent.services.proxy_url import parse_proxy_url

    parsed = parse_proxy_url(proxy_url)
    raw = parsed.raw.rstrip("/")
    # parsed.raw is scheme://[user:pass@]host:port — split into gologin fields.
    from urllib.parse import urlparse
    u = urlparse(raw if "://" in raw else f"http://{raw}")
    mode = (u.scheme or "http").lower()
    if mode not in ("http", "https", "socks4", "socks5"):
        mode = "http"
    gl = _client(profile_id, local=True)
    code = gl.changeProfileProxy(profile_id, {
        "mode": mode,
        "host": u.hostname or "",
        "port": u.port or (1080 if mode.startswith("socks") else 80),
        "username": u.username or None,
        "password": u.password or None,
    })
    if code not in (200, 204):
        raise RuntimeError(f"gologin set proxy HTTP {code}")


def ensure_profile(nick_id: str, account: dict) -> str:
    """Return the gologin profile id for this nick, creating + proxying once."""
    pid = nick_profile_id(account)
    if pid:
        return pid
    pid = _create_profile(nick_id)
    account["gologin_profile"] = pid
    try:
        from agent.services.accounts import upsert_account
        upsert_account(account)
    except Exception as exc:
        logger.warning("gologin: could not persist profile id for %s: %s", nick_id, exc)
    logger.info("gologin: created profile %s for nick %s", pid, nick_id)
    return pid


async def launch(nick_id: str, account: dict) -> dict:
    """Launch Orbita for a gologin nick; extension connects back over WS."""
    from agent.services.chrome_nicks import chrome_data_dir, sync_extension_copy

    profile_id = ensure_profile(nick_id, account)

    proxy_url = str(account.get("proxy_url") or "").strip()
    if not proxy_url:
        from agent.services.proxy_pool import get_verified_proxy_for_nick
        proxy_url = await asyncio.to_thread(get_verified_proxy_for_nick, nick_id) or ""
        if proxy_url:
            account["proxy_url"] = proxy_url
            from agent.services.accounts import upsert_account
            upsert_account(account)
    if proxy_url:
        await asyncio.to_thread(_set_profile_proxy, profile_id, proxy_url)

    # Baked ext lives in the nick's FlowKit workspace dir; Orbita loads it via
    # --load-extension (its own userChromeExtensions path would re-download).
    data = chrome_data_dir(nick_id)
    data.mkdir(parents=True, exist_ok=True)
    ext_copy = sync_extension_copy(data, profile_id=nick_id)

    extra = [f"--load-extension={ext_copy}"]
    # Orbita ships without a setuid chrome-sandbox helper (same as the
    # unpacked Cốc Cốc build) — it aborts on startup without this.
    extra.append("--no-sandbox")
    if os.path.exists(f"/run/user/{os.getuid()}/wayland-0"):
        extra.append("--ozone-platform=wayland")
    extra.append("https://flow.google.com/")

    # local=True only once the profile dir exists — first launch must run the
    # SDK's createEmptyProfile (local=False path) to materialize it on disk.
    first_run = not profile_dir(profile_id).exists()
    gl = _client(profile_id, local=not first_run, extra_params=extra)

    debug_url = await asyncio.to_thread(gl.start)
    pid = getattr(gl, "pid", None)
    if not pid or not _pid_alive(pid):
        raise RuntimeError("gologin: orbita did not stay up after spawn")
    _handles[nick_id] = gl
    logger.info(
        "gologin: launched nick=%s profile=%s pid=%s proxy=%s",
        nick_id, profile_id, pid, proxy_url or "none",
    )
    return {
        "ok": True,
        "id": nick_id,
        "already_running": False,
        "pid": pid,
        "data_dir": str(profile_dir(profile_id)),
        "gologin_profile": profile_id,
        "debug_url": debug_url,
        "proxy": proxy_url,
    }


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(int(pid), 0)
    except (ProcessLookupError, PermissionError, OSError, ValueError):
        return False
    return True


def gologin_ps_marker(nick_id: str) -> str:
    """Cmdline token present in every orbita process for this nick."""
    return f"--gologin-profile={nick_id}"
