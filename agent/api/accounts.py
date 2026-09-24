"""CRUD + proxy check + Chrome launch for Flow nicks."""
from __future__ import annotations

import asyncio
import logging
import shutil
import time
from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from agent.services.accounts import (
    delete_account,
    get_account,
    load_accounts,
    nick_api_status,
    nick_next_action,
    public_account,
    save_accounts,
    seed_accounts_from_template,
    upsert_account,
)
from agent.services.chrome_nicks import (
    check_proxy,
    chrome_data_dir,
    chrome_running,
    launch_nick,
    launch_status,
    stop_nick,
)
from agent.services.flow_client import get_flow_client
from agent.services.proxy_pool import (
    add_proxies_to_pool,
    load_proxy_pool,
    rotate_nick_proxy,
)
from agent.services.proxy_url import ProxyURLError, parse_proxy_url

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/accounts", tags=["accounts"])


class ProxyPoolAddBody(BaseModel):
    proxies: list[str] = Field(default_factory=list)



class AccountBody(BaseModel):
    id: str
    label: str = ""
    project_id: str = ""
    proxy_url: str = ""
    note: str = ""
    enabled: bool = True
    mint_only: bool = False
    browser: str = ""
    old_id: str | None = None


class RenameBody(BaseModel):
    new_id: str


class AccountsReplace(BaseModel):
    accounts: list[AccountBody] = Field(default_factory=list)


class ProxyCheckBody(BaseModel):
    proxy_url: str = ""


class RotateProxyBody(BaseModel):
    target_proxy: str | None = None


def _reload_router() -> None:
    get_flow_client().reload_configured_profiles()


def _attach_workers(publics: list[dict]) -> None:
    workers = list(get_flow_client().workers() or [])
    used: set[int] = set()

    def claim(row: dict, index: int) -> None:
        row["worker"] = workers[index]
        row["connected"] = True
        used.add(index)

    for row in publics:
        row["worker"] = None
        row["connected"] = False
        for i, worker in enumerate(workers):
            if i in used:
                continue
            wp = worker.get("profile_id")
            if wp and str(wp).strip().lower() == str(row["id"]).strip().lower():
                claim(row, i)
                break

    for row in publics:
        if row.get("worker"):
            continue
        project = str(row.get("project_id") or "").strip()
        if not project:
            continue
        for i, worker in enumerate(workers):
            if i in used:
                continue
            if str(worker.get("project_id") or "").strip() == project:
                claim(row, i)
                break


def _decorate(rows: list[dict], *, reveal: bool = False) -> list[dict]:
    import asyncio
    publics = []
    for row in rows:
        public = public_account(row, reveal=reveal)
        public.update(launch_status(row["id"]))
        publics.append(public)
    _attach_workers(publics)
    from agent.services.nick_metrics import get_nick_metrics_tracker
    tracker = get_nick_metrics_tracker()
    for public in publics:
        public["apis"] = nick_api_status(public)
        public["next"] = nick_next_action(public)
        public["metrics"] = tracker.get_metrics(public["id"])
        worker = public.get("worker") or {}
        if not public.get("project_id") and worker.get("project_id"):
            public["detected_project_id"] = str(worker.get("project_id")).strip()
        # Proactively trigger r2v auto-bind if connected but missing chat session
        if (public.get("enabled", True) and public.get("connected")
                and public.get("project_id") and not worker.get("chat_session")):
            client = get_flow_client()
            if hasattr(client, "bind_chat_session"):
                asyncio.create_task(client.bind_chat_session(public["id"]))
    return publics


def _live_row(row: dict, *, reveal: bool = False) -> dict:
    rows = load_accounts()
    if not any(r["id"] == row["id"] for r in rows):
        rows = [row]
    decorated = _decorate(rows, reveal=reveal)
    for item in decorated:
        if item["id"] == row["id"]:
            return item
    return _decorate([row], reveal=reveal)[0]


@router.get("")
async def list_accounts(reveal: bool = False):
    rows = load_accounts()
    if not rows:
        rows = seed_accounts_from_template()
    # Gen-capable nicks always first, mint-only at the bottom — stable order.
    rows = sorted(rows, key=lambda r: bool(r.get("mint_only")))
    return {"accounts": _decorate(rows, reveal=reveal)}


@router.put("")
async def replace_accounts(body: AccountsReplace):
    if not body.accounts:
        raise HTTPException(400, "refusing to replace accounts with an empty list")
    try:
        saved = save_accounts([a.model_dump() for a in body.accounts])
    except (ValueError, ProxyURLError) as exc:
        raise HTTPException(400, str(exc)) from exc
    _reload_router()
    return {"accounts": _decorate(saved, reveal=True)}


async def _relaunch_quiet(nick_id: str) -> dict:
    """Best-effort relaunch after a rename. Never turns a good save into a 500."""
    try:
        res = await launch_nick(nick_id)
        return {"ok": bool(res.get("ok")), "id": nick_id}
    except Exception as exc:
        logger.warning("relaunch after rename failed for %s: %s", nick_id, exc)
        return {"ok": False, "id": nick_id, "error": str(exc)}


@router.post("")
async def upsert(body: AccountBody):
    old_id = (body.old_id or "").strip()
    renaming = bool(old_id and old_id != body.id.strip())
    # The data dir cannot be moved under a live browser, so stop it here instead
    # of 400-ing the way every rename from the dashboard used to.
    relaunch = renaming and chrome_running(old_id)
    if relaunch:
        await stop_nick(old_id)
    try:
        # upsert_account can spend ~40s probing pool proxies for a new nick.
        # On the event loop that froze the whole dashboard, /health included.
        saved = await asyncio.to_thread(upsert_account, body.model_dump(), None, body.old_id)
    except (ValueError, ProxyURLError) as exc:
        if relaunch:
            await _relaunch_quiet(old_id)
        raise HTTPException(400, str(exc)) from exc
    _reload_router()
    relaunched = await _relaunch_quiet(saved["id"]) if relaunch else None
    # Row is read after the relaunch, or it reports chrome_running: false for a
    # nick whose Chrome is already back up.
    row = _live_row(saved, reveal=True)
    if relaunched is not None:
        row["relaunched"] = relaunched
    return row


@router.get("/proxy-health")
async def get_proxy_health_endpoint(reveal: bool = False):
    from agent.services.proxy_checker import get_proxy_health_report
    return get_proxy_health_report(reveal=reveal)


@router.post("/proxy-health/check-all")
async def check_all_proxies_endpoint(reveal: bool = False):
    import asyncio
    from agent.services.proxy_checker import check_all_proxies_health
    return await asyncio.to_thread(check_all_proxies_health, reveal=reveal)



# Static path must be registered before /{nick_id}/check-proxy, or
# POST /check-proxy is captured as nick_id="check-proxy".
@router.post("/check-proxy")
async def check_any(body: ProxyCheckBody):
    if not body.proxy_url:
        raise HTTPException(400, "proxy_url is required")
    try:
        return await check_proxy(body.proxy_url)
    except ProxyURLError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/proxy-pool")
async def get_proxy_pool(reveal: bool = False):
    pool = load_proxy_pool()
    proxies = pool.get("proxies") or []
    if reveal:
        rendered = proxies
    else:
        rendered = [parse_proxy_url(p).redacted for p in proxies]
    return {
        "ok": True,
        "total": len(proxies),
        "current_index": pool.get("current_index", 0),
        "proxies": rendered,
    }


@router.post("/proxy-pool")
async def add_to_proxy_pool(body: ProxyPoolAddBody):
    if not body.proxies:
        raise HTTPException(400, "proxies list is empty")
    pool = add_proxies_to_pool(body.proxies)
    return {
        "ok": True,
        "total": len(pool.get("proxies", [])),
        "current_index": pool.get("current_index", 0),
    }


@router.get("/unusual-threshold")
async def get_accounts_unusual_threshold():
    """Forensic analysis: how many requests per IP before hitting unusual activity."""
    from agent.services.unusual_audit import get_unusual_audit
    audit_mgr = get_unusual_audit()
    return audit_mgr.compute_threshold_analysis()


@router.get("/metrics")
async def get_accounts_metrics():
    """Real-time concurrency and RPM telemetry for all Flow nicks and cluster aggregates."""
    from agent.services.nick_metrics import get_nick_metrics_tracker
    return get_nick_metrics_tracker().get_all_metrics()


@router.post("/metrics/reset")
async def reset_accounts_metrics(body: dict | None = None):
    """Reset historical peak metrics and request counters (optional worker_id)."""
    from agent.services.nick_metrics import get_nick_metrics_tracker
    worker_id = (body or {}).get("worker_id") if body else None
    get_nick_metrics_tracker().reset(worker_id=worker_id)
    return {"ok": True, "message": f"Metrics reset for {worker_id or 'all workers'}"}


@router.get("/metrics/outcomes")
async def metrics_outcomes(hours: float = 24.0):
    """Persistent outcome ledger: failure breakdown by normalized error class
    and per-nick success rates — the data source for fleet optimization."""
    from agent.services.request_ledger import outcome_summary
    return outcome_summary(hours)


@router.get("/metrics/rpc")
async def metrics_rpc(hours: float = 24.0):
    """Per-RPC transport stats from the extension netlog ledger."""
    from agent.services.request_ledger import rpc_summary
    return rpc_summary(hours)


@router.get("/metrics/events")
async def metrics_events(hours: float = 24.0, nick: str | None = None,
                         kind: str | None = None, limit: int = 200):
    """Unified lifecycle event stream — EXT_CONNECT/DISCONNECT, STRIKE_*,
    HOLDOUT_*, PROXY_*, TAB_DEAD, BIND_*, MINT_*, ROUTE_*."""
    from agent.services.request_ledger import event_summary, recent_events
    kinds = [k.strip() for k in kind.split(",")] if kind else None
    return {
        "summary": event_summary(hours),
        "events": recent_events(hours=hours, nick=nick, kinds=kinds, limit=limit),
    }


@router.get("/{nick_id}/diagnose")
async def diagnose_nick_endpoint(nick_id: str, hours: float = 24.0):
    """'Why' report for one nick: live state + outcomes + events + findings."""
    return await get_flow_client().diagnose_nick(nick_id, hours=hours)


@router.get("/diagnose")
async def diagnose_fleet(hours: float = 24.0):
    """Fleet-level 'why': per-nick findings for every known nick."""
    from agent.services.accounts import load_accounts
    client = get_flow_client()
    nicks = {a.get("id") for a in load_accounts() if a.get("id")}
    nicks |= {s.get("profile_id") for s in client._extensions.values()
              if s.get("profile_id")}
    import asyncio
    reports = [
        r if isinstance(r, dict) else {"error": str(r)}
        for r in await asyncio.gather(
            *(client.diagnose_nick(n, hours=hours) for n in sorted(nicks)),
            return_exceptions=True)
    ]
    # Fleet-wide events not tied to one nick (ROUTE_ALL_PARKED etc.)
    from agent.services.request_ledger import recent_events
    global_events = [e for e in recent_events(hours=hours, limit=300)
                     if not e.get("nick")]
    return {
        "hours": hours,
        "nicks": reports,
        "global_events": global_events,
    }


@router.get("/auth-report")
async def auth_report_endpoint(window_s: int = 3600):
    """Which nicks are 401, judged from raw netlog status codes.

    Static path, registered before /{nick_id} or it is captured as an id.
    """
    from agent.services.nick_auth import auth_report
    return await asyncio.to_thread(auth_report, window_s)


@router.get("/{nick_id}")
async def get_one(nick_id: str, reveal: bool = True):
    row = get_account(nick_id)
    if row is None:
        raise HTTPException(404, f"unknown account {nick_id}")
    return _live_row(row, reveal=reveal)



@router.delete("/{nick_id}")
async def remove(nick_id: str):
    """Delete a nick and take its Chrome down.

    Deleting only the row left the browser up and its WS session registered, so
    the router kept dispatching to a nick the user had already removed — which
    is how a nick without video model access kept failing jobs after deletion.
    """
    if get_account(nick_id) is None:
        raise HTTPException(404, f"unknown account {nick_id}")
    stopped = False
    if chrome_running(nick_id):
        try:
            stopped = await stop_nick(nick_id)
        except Exception as exc:
            logger.warning("could not stop Chrome for deleted nick %s: %s", nick_id, exc)
    if not delete_account(nick_id):
        raise HTTPException(404, f"unknown account {nick_id}")
    try:
        get_flow_client().clear_model_denied(nick_id)
    except Exception:
        pass
    # The worker sweep only walks nicks still in accounts.json, so anything left
    # OPEN here would sit on the dashboard as a live fault on a nick that no
    # longer exists until the 24h stale TTL.
    closed = 0
    try:
        from agent.services.incident_manager import get_incident_manager
        closed = get_incident_manager().resolve_by_nick(
            module="worker", nick_id=nick_id, action_taken="NICK_DELETED",
        )
    except Exception as exc:
        logger.warning("could not close incidents for deleted nick %s: %s", nick_id, exc)
    _reload_router()
    return {"ok": True, "id": nick_id, "chrome_stopped": stopped, "incidents_closed": closed}


@router.post("/{nick_id}/check-proxy")
async def check_one(nick_id: str, body: ProxyCheckBody | None = None):
    row = get_account(nick_id)
    if row is None:
        raise HTTPException(404, f"unknown account {nick_id}")
    url = (body.proxy_url if body else "") or row.get("proxy_url") or ""
    if not url:
        raise HTTPException(400, "no proxy_url on this nick")
    try:
        return await check_proxy(url)
    except ProxyURLError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/{nick_id}/launch")
async def launch(nick_id: str):
    try:
        return await launch_nick(nick_id)
    except KeyError:
        raise HTTPException(404, f"unknown account {nick_id}") from None
    except ProxyURLError as exc:
        raise HTTPException(400, str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(500, str(exc)) from exc


@router.post("/{nick_id}/stop")
async def stop(nick_id: str):
    stopped = await stop_nick(nick_id)
    return {"ok": True, "stopped": stopped, "id": nick_id}


_SNAPSHOT_EXCLUDES = (
    "Cache", "Code Cache", "GPUCache", "Service Worker/CacheStorage",
    "Crashpad", "BrowserMetrics*", "*.log", "locks", "SingletonLock",
    "SingletonSocket", "SingletonCookie", "DevToolsActivePort",
)


def _snapshot_dir(nick_id: str) -> Path:
    return Path.home() / ".flowkit" / "profile_snapshots" / nick_id


@router.post("/{nick_id}/profile-snapshot")
async def profile_snapshot(nick_id: str):
    """Snapshot a nick's Chrome profile while it is clean and logged in.

    UNUSUAL_ACTIVITY flags live in the profile's persistent site data — a
    snapshot taken now is a 'known-good restore point': future flags get
    fixed by profile-restore instead of a manual re-login.
    """
    if get_account(nick_id) is None:
        raise HTTPException(404, f"unknown account {nick_id}")
    if chrome_running(nick_id):
        raise HTTPException(409, "stop the nick's Chrome first — a live profile cannot be snapshotted safely")
    src = chrome_data_dir(nick_id)
    if not src.exists():
        raise HTTPException(404, f"no profile dir at {src}")
    dest = _snapshot_dir(nick_id)
    dest.parent.mkdir(parents=True, exist_ok=True)

    def _copy():
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(
            src, dest,
            ignore=shutil.ignore_patterns(*_SNAPSHOT_EXCLUDES),
            symlinks=True,
        )
    try:
        await asyncio.to_thread(_copy)
    except Exception as exc:
        raise HTTPException(500, f"snapshot failed: {exc}") from exc
    return {"ok": True, "id": nick_id, "snapshot": str(dest)}


@router.post("/{nick_id}/profile-restore")
async def profile_restore(nick_id: str):
    """Restore a nick's Chrome profile from the clean snapshot.

    Stops Chrome, swaps the live profile dir for the snapshot, and relaunches.
    The nick must have been snapshotted while clean (profile-snapshot).
    """
    if get_account(nick_id) is None:
        raise HTTPException(404, f"unknown account {nick_id}")
    snap = _snapshot_dir(nick_id)
    if not snap.exists():
        raise HTTPException(404, "no snapshot — run profile-snapshot while the nick was clean")
    live = chrome_data_dir(nick_id)

    stopped = await stop_nick(nick_id)

    def _swap():
        backup = live.with_name(live.name + f".burned-{int(time.time())}")
        if live.exists():
            live.rename(backup)
        shutil.copytree(snap, live, symlinks=True)
        return backup
    try:
        backup = await asyncio.to_thread(_swap)
    except Exception as exc:
        raise HTTPException(500, f"restore failed: {exc}") from exc

    try:
        launch_res = await launch_nick(nick_id)
    except Exception as exc:
        return {"ok": True, "id": nick_id, "restored_from": str(snap),
                "backup": str(backup), "launch_error": str(exc)}
    # Drop the hold-out state too — the restored profile is clean, and the
    # strike counters would otherwise keep it parked on stale evidence.
    client = get_flow_client()
    cleared = client.clear_auth_strikes(nick_id) if hasattr(client, "clear_auth_strikes") else {}
    _reload_router()
    return {"ok": True, "id": nick_id, "restored_from": str(snap),
            "backup": str(backup), "launch": launch_res, "cleared": cleared}


@router.post("/{nick_id}/rotate-proxy")
async def rotate_proxy(nick_id: str, body: RotateProxyBody | None = None):
    target = body.target_proxy if body else None
    res = await rotate_nick_proxy(nick_id, target_proxy=target)
    if not res.get("ok"):
        raise HTTPException(400, res.get("error", "Failed to rotate proxy"))
    _reload_router()
    return res


@router.post("/{nick_id}/reload-tab")
async def reload_nick_tab(nick_id: str):
    """Gracefully reload the Google Flow tab in this nick's Chrome browser."""
    client = get_flow_client()
    res = await client.profile_control(nick_id, "reload_flow_tab", {})
    if not res.get("ok"):
        raise HTTPException(400, res.get("error", "Failed to reload Flow tab"))
    return {"ok": True, "id": nick_id, "result": res}


@router.post("/reload-all-tabs")
async def reload_all_tabs():
    """Gracefully reload Google Flow tabs across all connected nick profiles."""
    client = get_flow_client()
    results = {}
    for worker in client.workers():
        pid = worker.get("profile_id")
        if pid:
            results[pid] = await client.profile_control(pid, "reload_flow_tab", {})
    return {"ok": True, "results": results}


@router.post("/{nick_id}/bind-r2v")
async def bind_r2v(nick_id: str):
    client = get_flow_client()
    if not hasattr(client, "bind_chat_session"):
        raise HTTPException(501, "bind_chat_session not supported by current client")
    res = await client.bind_chat_session(nick_id)
    if not res.get("ok"):
        raise HTTPException(400, res.get("error", "Failed to bind r2v session"))
    return res


@router.post("/{nick_id}/sync-project")
async def sync_nick_project(nick_id: str, body: dict | None = None):
    from agent.services.accounts import sync_account_project
    pid = (body or {}).get("project_id") if body else None
    try:
        saved = sync_account_project(nick_id, project_id=pid)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    _reload_router()
    return _live_row(saved, reveal=True)


@router.post("/{nick_id}/rename")
async def rename_nick(nick_id: str, body: RenameBody):
    from agent.services.accounts import rename_account
    relaunch = chrome_running(nick_id)
    if relaunch:
        await stop_nick(nick_id)
    try:
        saved = await asyncio.to_thread(rename_account, nick_id, body.new_id)
    except (ValueError, ProxyURLError) as exc:
        if relaunch:
            await _relaunch_quiet(nick_id)
        raise HTTPException(400, str(exc)) from exc
    _reload_router()
    relaunched = await _relaunch_quiet(saved["id"]) if relaunch else None
    # Row is read after the relaunch, or it reports chrome_running: false for a
    # nick whose Chrome is already back up.
    row = _live_row(saved, reveal=True)
    if relaunched is not None:
        row["relaunched"] = relaunched
    return row


@router.get("/{nick_id}/metrics")
async def get_nick_metrics(nick_id: str):
    """Get real-time concurrency, peak concurrency, current RPM, and peak RPM for one nick."""
    from agent.services.nick_metrics import get_nick_metrics_tracker
    return get_nick_metrics_tracker().get_metrics(nick_id)


@router.get("/{nick_id}/auth")
async def nick_auth_endpoint(nick_id: str, window_s: int = 3600):
    """Netlog verdict for one nick, with the per-rpc status breakdown."""
    if get_account(nick_id) is None:
        raise HTTPException(404, f"unknown account {nick_id}")
    from agent.services.nick_auth import evidence_for
    return await asyncio.to_thread(evidence_for, nick_id, window_s)


@router.post("/{nick_id}/focus")
async def focus_nick(nick_id: str, launch: bool = True):
    """Raise this nick's Chrome window with the Flow tab in front.

    Wayland has no wmctrl/xdotool here, so the only way to point at one window
    out of nine is to ask that nick's own extension to activate its tab.
    """
    if get_account(nick_id) is None:
        raise HTTPException(404, f"unknown account {nick_id}")
    launched = None
    if not chrome_running(nick_id):
        if not launch:
            raise HTTPException(409, f"Chrome is not running for {nick_id}")
        try:
            launched = await launch_nick(nick_id)
        except (KeyError, ProxyURLError, RuntimeError) as exc:
            raise HTTPException(400, f"could not launch Chrome: {exc}") from exc
        # The extension needs to connect before it can be told anything.
        for _ in range(20):
            await asyncio.sleep(0.5)
            if any(w.get("profile_id") == nick_id for w in get_flow_client().workers()):
                break
    # Short timeout: an extension copy predating focus_flow_tab answers
    # UNKNOWN_METHOD, and an even older one does not answer at all.
    res = await get_flow_client().profile_control(nick_id, "focus_flow_tab", {}, timeout=15)
    if not res.get("ok"):
        err = str(res.get("error") or "could not focus the Flow tab")
        if "UNKNOWN_METHOD" in err or "TIMEOUT" in err.upper():
            err = (
                f"{nick_id}'s Chrome is running an extension copy without focus support. "
                "Stop and relaunch this nick to update it."
            )
        raise HTTPException(400, err)
    return {"ok": True, "id": nick_id, "launched": launched, "result": res}


@router.post("/{nick_id}/enable")
async def enable_nick(nick_id: str, enabled: bool = True):
    """Put an auto-disabled nick back in rotation (or take one out by hand).

    Enabling also clears the soft-auth strikes and un-parks the worker, so the
    nick is usable on the next dispatch instead of after the 30m park expires,
    and resolves its open ACCOUNT_AUTH_EXPIRED incident.
    """
    row = get_account(nick_id)
    if row is None:
        raise HTTPException(404, f"unknown account {nick_id}")
    updated = {**row, "enabled": enabled}
    try:
        saved = await asyncio.to_thread(upsert_account, updated)
    except (ValueError, ProxyURLError) as exc:
        raise HTTPException(400, str(exc)) from exc
    cleared = {}
    resolved = 0
    if enabled:
        client = get_flow_client()
        if hasattr(client, "clear_auth_strikes"):
            cleared = client.clear_auth_strikes(nick_id)
        if hasattr(client, "clear_model_denied"):
            cleared["model_denied_cleared"] = client.clear_model_denied(nick_id)
        try:
            from agent.services.incident_manager import get_incident_manager
            mgr = get_incident_manager()
            # Those incidents carry the nick in job_id, so resolve_by_sub() alone
            # matched nothing and they stayed OPEN after a manual re-enable.
            resolved = mgr.resolve_by_nick(
                "worker", nick_id, error_code="ACCOUNT_AUTH_EXPIRED", action_taken="MANUAL_REENABLE"
            )
            resolved += mgr.resolve_by_nick(
                "worker", nick_id, error_code="MODEL_ACCESS_DENIED", action_taken="MANUAL_REENABLE"
            )
        except Exception as exc:
            logger.warning("could not resolve auth incidents for %s: %s", nick_id, exc)
    _reload_router()
    out = _live_row(saved, reveal=True)
    out["auth_reset"] = {**cleared, "incidents_resolved": resolved}
    return out
