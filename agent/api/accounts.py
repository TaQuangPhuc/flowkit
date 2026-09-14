"""CRUD + proxy check + Chrome launch for Flow nicks."""
from __future__ import annotations

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
    old_id: str | None = None


class RenameBody(BaseModel):
    new_id: str


class AccountsReplace(BaseModel):
    accounts: list[AccountBody] = Field(default_factory=list)


class ProxyCheckBody(BaseModel):
    proxy_url: str = ""


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
    for public in publics:
        public["apis"] = nick_api_status(public)
        public["next"] = nick_next_action(public)
        worker = public.get("worker") or {}
        if not public.get("project_id") and worker.get("project_id"):
            public["detected_project_id"] = str(worker.get("project_id")).strip()
        # Proactively trigger r2v auto-bind if connected but missing chat session
        if public.get("connected") and public.get("project_id") and not worker.get("chat_session"):
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
    return {"accounts": _decorate(rows, reveal=reveal)}


@router.put("")
async def replace_accounts(body: AccountsReplace):
    try:
        saved = save_accounts([a.model_dump() for a in body.accounts])
    except (ValueError, ProxyURLError) as exc:
        raise HTTPException(400, str(exc)) from exc
    _reload_router()
    return {"accounts": _decorate(saved, reveal=True)}


@router.post("")
async def upsert(body: AccountBody):
    try:
        saved = upsert_account(body.model_dump(), old_id=body.old_id)
    except (ValueError, ProxyURLError) as exc:
        raise HTTPException(400, str(exc)) from exc
    _reload_router()
    return _live_row(saved, reveal=True)


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


@router.get("/{nick_id}")
async def get_one(nick_id: str, reveal: bool = True):
    row = get_account(nick_id)
    if row is None:
        raise HTTPException(404, f"unknown account {nick_id}")
    return _live_row(row, reveal=reveal)



@router.delete("/{nick_id}")
async def remove(nick_id: str):
    if not delete_account(nick_id):
        raise HTTPException(404, f"unknown account {nick_id}")
    _reload_router()
    return {"ok": True, "id": nick_id}


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


@router.post("/{nick_id}/rotate-proxy")
async def rotate_proxy(nick_id: str):
    res = await rotate_nick_proxy(nick_id)
    if not res.get("ok"):
        raise HTTPException(400, res.get("error", "Failed to rotate proxy"))
    _reload_router()
    return res


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
    try:
        saved = rename_account(nick_id, body.new_id)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    _reload_router()
    return _live_row(saved, reveal=True)



