"""Flow Kit — FastAPI + WebSocket server entry point."""
import asyncio
import json
import logging
import re
import signal
from contextlib import asynccontextmanager

import websockets
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from agent.config import (
    API_HOST, API_PORT, BASE_DIR, WS_HOST, WS_PORT,
    PER_WORKER_VIDEO_COOLDOWN, PER_WORKER_VIDEO_COOLDOWN_MIN, PER_WORKER_VIDEO_COOLDOWN_MAX,
    VIDEO_POLL_TIMEOUT, SMART_REPLAY_ENABLED, SMART_REPLAY_TIMEOUT,
)
from agent.db.schema import init_db, close_db
from agent.api.characters import router as characters_router
from agent.api.projects import router as projects_router
from agent.api.videos import router as videos_router
from agent.api.scenes import router as scenes_router
from agent.api.requests import router as requests_router
from agent.api.flow import router as flow_router
from agent.api.reviews import router as reviews_router
from agent.api.tts import router as tts_router
from agent.api.materials import router as materials_router
from agent.api.music import router as music_router
from agent.api.models import router as models_router
from agent.api.providers import router as providers_router
from agent.api.active_project import router as active_project_router
from agent.api.accounts import router as accounts_router
from agent.api.system import router as system_router
from agent.api.fashion_lookbook import router as fashion_lookbook_router
from agent.api.incidents import router as incidents_router
from agent.services.central_watchdog import start_central_watchdog, stop_central_watchdog
from agent.services.flow_trace import FlowTraceMiddleware, emit as trace_emit, identifier as trace_identifier, summary as trace_summary
from agent.services.request_shield import RequestShieldMiddleware, get_request_shield
from agent.worker.processor import get_worker_controller
from agent.services.flow_client import get_flow_client
from agent.services.event_bus import event_bus
from agent.sdk import init_sdk

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)


# ─── WebSocket Server for Extension ─────────────────────────

async def ws_handler(websocket):
    """Handle a Chrome extension WebSocket connection."""
    client = get_flow_client()
    client.set_extension(websocket)
    logger.info("Extension connected from %s", websocket.remote_address)

    # Send callback secret so extension can authenticate HTTP callbacks
    await websocket.send(json.dumps({"type": "callback_secret", "secret": _CALLBACK_SECRET}))

    try:
        async for raw in websocket:
            try:
                data = json.loads(raw)
                await client.handle_message(data, websocket)
            except json.JSONDecodeError:
                logger.warning("Invalid JSON from extension")
            except Exception as e:
                logger.exception("Error handling extension message: %s", e)
    except websockets.ConnectionClosed:
        pass
    finally:
        client.clear_extension(websocket)
        logger.info("Extension disconnected")


async def run_ws_server():
    """Run WebSocket server for extension connections."""
    async with websockets.serve(ws_handler, WS_HOST, WS_PORT):
        logger.info("WebSocket server listening on ws://%s:%d", WS_HOST, WS_PORT)
        await asyncio.Future()  # run forever


# ─── FastAPI App ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()

    # Load custom materials from DB into in-memory registry
    from agent.db.crud import list_materials as db_list_materials
    from agent.materials import register_material, _BUILTIN_IDS
    try:
        custom_materials = await db_list_materials()
        for m in custom_materials:
            if m["id"] not in _BUILTIN_IDS:
                register_material(m)
                logger.info("Loaded custom material from DB: %s", m["id"])
    except Exception as e:
        logger.warning("Failed to load custom materials: %s", e)

    ops = init_sdk(get_flow_client())
    logger.info("SDK initialized (OperationService ready)")
    logger.info("Flow Kit starting on %s:%d", API_HOST, API_PORT)

    controller = get_worker_controller()

    # Start background tasks
    ws_task = asyncio.create_task(run_ws_server())
    worker_task = asyncio.create_task(controller.start())
    from agent.services.video_reconciliation import VideoReconciler
    from agent.config import VIDEO_POLL_TIMEOUT
    reconciler = VideoReconciler(get_flow_client(), BASE_DIR / '.scratch' / 'video-reconciliation.json',
                                render_timeout=VIDEO_POLL_TIMEOUT)
    reconcile_task = asyncio.create_task(reconciler.run())
    logger.info("WS server + worker started")

    # Auto-restore bridges and launch enabled nicks
    try:
        from agent.services.accounts import load_accounts
        from agent.services.chrome_nicks import ensure_nick_active, restore_all_bridges
        # 1. ALWAYS restore proxy bridges immediately on startup so running Chromes never fail
        restored_bridges = await restore_all_bridges()
        if restored_bridges:
            logger.info("Restored proxy bridges on startup: %s", restored_bridges)
        # 2. Ensure all enabled accounts are active
        for acc in load_accounts():
            if acc.get("enabled", True):
                logger.info("Auto-ensuring Chrome & proxy bridge for enabled nick: %s", acc["id"])
                asyncio.create_task(ensure_nick_active(acc["id"]))
    except Exception as e:
        logger.warning("Auto-restoring nicks & bridges failed: %s", e)

    # Start proxy health & temporary expiry daemon
    try:
        from agent.services.proxy_checker import start_proxy_health_daemon
        start_proxy_health_daemon(interval_seconds=180)
        logger.info("Proxy health & auto-expiry daemon started")
    except Exception as e:
        logger.warning("Failed to start proxy health daemon: %s", e)

    # Start Central Watchdog & Self-Healing Daemon
    try:
        start_central_watchdog()
        logger.info("Central Watchdog Daemon started")
    except Exception as e:
        logger.warning("Failed to start central watchdog: %s", e)

    yield

    reconcile_task.cancel()
    await asyncio.gather(reconcile_task, return_exceptions=True)

    # Graceful shutdown: Never drop in-flight client requests
    shield = get_request_shield()
    shield.set_draining(True)
    logger.info("Graceful shutdown: Waiting for %d active client HTTP requests to complete...", shield.active_count)
    await shield.wait_until_idle(timeout=45.0)

    logger.info("Graceful shutdown: Draining background worker tasks...")
    controller.request_shutdown()
    await controller.drain(timeout=120.0)

    try:
        stop_central_watchdog()
    except Exception:
        pass

    try:
        from agent.services.proxy_checker import stop_proxy_health_daemon
        stop_proxy_health_daemon()
    except Exception:
        pass

    ws_task.cancel()
    worker_task.cancel()
    await close_db()
    logger.info("Flow Kit stopped cleanly without dropping client requests")


app = FastAPI(title="Flow Kit", version="1.1.0", lifespan=lifespan)

app.add_middleware(RequestShieldMiddleware)
app.add_middleware(FlowTraceMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(characters_router, prefix="/api")
app.include_router(projects_router, prefix="/api")
app.include_router(videos_router, prefix="/api")
app.include_router(scenes_router, prefix="/api")
app.include_router(requests_router, prefix="/api")
app.include_router(flow_router, prefix="/api")
app.include_router(reviews_router, prefix="/api")
app.include_router(tts_router, prefix="/api")
app.include_router(materials_router, prefix="/api")
app.include_router(music_router, prefix="/api")
app.include_router(models_router)
app.include_router(providers_router)
app.include_router(active_project_router)
app.include_router(accounts_router, prefix="/api")
app.include_router(system_router, prefix="/api")
app.include_router(fashion_lookbook_router, prefix="/api")
app.include_router(incidents_router)


import secrets as _secrets
_CALLBACK_SECRET = _secrets.token_urlsafe(32)


@app.post("/api/ext/callback")
async def ext_callback(request: Request):
    """HTTP callback for extension to deliver API responses.

    Replaces ws.send() for response delivery — immune to WS disconnect.
    Extension POSTs {id, status, data, error} here instead of sending via WS.
    Requires X-Callback-Secret header matching the secret sent to extension on WS connect.
    """
    data = await request.json()
    client = get_flow_client()
    req_id = data.get("id")
    logger.info("ext/callback: id=%s pending=%d match=%s",
                str(req_id)[:8] if req_id else "none",
                len(client._pending),
                "yes" if req_id and req_id in client._pending else "no")
    trace_emit("extension.callback", extension_request_id=trace_identifier(req_id),
               matched=bool(req_id and req_id in client._pending), result=trace_summary(data))
    if req_id and req_id in client._pending:
        future = client._pending[req_id]
        try:
            future.set_result(data)
        except asyncio.InvalidStateError:
            pass
        return {"ok": True}
    return {"ok": False, "reason": "no matching pending request"}


_NETLOG_PATH = BASE_DIR / ".scratch" / "ext-netlog.jsonl"


def _append_netlog(rec: dict) -> None:
    """Append one capture and trim the file when it outgrows LOG_MAX_MB."""
    from agent.config import LOG_KEEP_MB, LOG_MAX_MB
    from agent.services.log_maintenance import trim_log

    _NETLOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _NETLOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(rec, ensure_ascii=False) + "\n")
    trim_log(_NETLOG_PATH, LOG_MAX_MB * 1024 * 1024, LOG_KEEP_MB * 1024 * 1024)
_NETLOG_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I
)


_NETLOG_UUID_SEARCH = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I
)
_CHAT_RPC_RE = re.compile(r"GN0Bre|StreamChat|CreationAgent|FlowCreationAgent", re.I)


def _session_from_netlog(url: str, freq: str | None) -> str | None:
    if not freq:
        return None
    chatty = bool(_CHAT_RPC_RE.search(f"{url or ''} {freq}"))
    try:
        env = json.loads(freq)
    except json.JSONDecodeError:
        env = None
    if isinstance(env, list) and len(env) >= 2 and env[0] is None and isinstance(env[1], str):
        try:
            inner = json.loads(env[1])
        except json.JSONDecodeError:
            inner = None
        if isinstance(inner, list) and inner and isinstance(inner[0], str) and _NETLOG_UUID.match(inner[0]):
            return inner[0]
    try:
        item = env[0][0]
        rpcid = item[0]
        inner = item[1]
        if isinstance(inner, str):
            inner = json.loads(inner)
        if rpcid == "GN0Bre" or "GN0Bre" in (url or ""):
            sid = inner[0] if isinstance(inner, list) and inner else None
            if isinstance(sid, str) and _NETLOG_UUID.match(sid):
                return sid
    except (TypeError, IndexError, KeyError, json.JSONDecodeError):
        pass
    if chatty:
        found = _NETLOG_UUID_SEARCH.search(freq)
        if found:
            return found.group(0)
    return None


@app.post("/api/ext/netlog")
async def ext_netlog(request: Request):
    """Append a Flow RPC capture from the extension. Also pins GN0Bre session."""
    data = await request.json()
    if data.get("event") == "network_error":
        from agent.services.network_diagnostics import sanitize_failure, append_failure
        record = sanitize_failure(data)
        if record is None:
            return {"ok": False, "error": "INVALID_NETWORK_DIAGNOSTIC"}
        append_failure(_NETLOG_PATH.with_name("ext-network-errors.jsonl"), record)
        logger.warning("ext/network_error rpcid=%s profile=%s request=%s error=%s elapsed_ms=%s",
                       record["rpcid"], record["profileId"], record["requestId"],
                       record["networkError"], record["elapsedMs"])
        return {"ok": True}
    url = str(data.get("url") or "")[:800]
    freq = data.get("freq")
    if isinstance(freq, str) and len(freq) > 24000:
        freq = freq[:24000]
    session = data.get("session") or _session_from_netlog(url, freq if isinstance(freq, str) else None)
    rec = {
        "ts": data.get("ts"),
        "url": url,
        "statusCode": data.get("statusCode"),
        "rpcid": data.get("rpcid"),
        "session": session,
        "freq": freq,
        "profileId": data.get("profileId") or None,
    }
    # One disk write per Flow RPC. Doing it inline blocked the event loop, and
    # nothing ever trimmed the file (it reached 129MB).
    await asyncio.to_thread(_append_netlog, rec)
    try:
        from agent.services.request_ledger import record_rpc
        await asyncio.to_thread(
            record_rpc,
            rec.get("profileId"), rec.get("rpcid"), rec.get("statusCode"),
            data.get("elapsedMs") or data.get("elapsed_ms"), session, rec.get("ts"),
        )
    except Exception:
        pass
    profile_id = data.get("profileId") or None
    if session:
        get_flow_client().remember_chat_session(session, profile_id=profile_id)
    logger.info(
        "ext/netlog rpcid=%s session=%s profile=%s status=%s",
        rec.get("rpcid"),
        session,
        profile_id,
        rec.get("statusCode"),
    )
    return {"ok": True, "session": session}


@app.get("/api/ext/network-errors")
async def ext_network_errors(limit: int = 40):
    from agent.services.network_diagnostics import tail_failures
    entries = tail_failures(_NETLOG_PATH.with_name("ext-network-errors.jsonl"), limit)
    return {"entries": entries, "count": len(entries)}


@app.get("/api/ext/netlog")
async def ext_netlog_tail(limit: int = 40):
    """Tail the extension RPC capture log."""
    if not _NETLOG_PATH.exists():
        return {"count": 0, "path": str(_NETLOG_PATH), "entries": []}
    cap = max(1, min(int(limit or 40), 200))
    # Never read the whole file: it is append-only and unbounded between trims.
    from agent.services.log_maintenance import read_tail
    lines = read_tail(_NETLOG_PATH, 2 * 1024 * 1024).decode("utf-8", "ignore").splitlines()
    entries = []
    for line in lines[-cap:]:
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return {"count": len(lines), "path": str(_NETLOG_PATH), "entries": entries}


@app.get("/health")
async def health():
    client = get_flow_client()
    return {
        "status": "ok",
        "version": "0.2.0",
        "extension_connected": client.connected,
        "video_cooldown_range_s": [PER_WORKER_VIDEO_COOLDOWN_MIN, PER_WORKER_VIDEO_COOLDOWN_MAX],
        "video_cooldown_s": PER_WORKER_VIDEO_COOLDOWN,
        "video_poll_timeout_s": VIDEO_POLL_TIMEOUT,
        "smart_replay_enabled": SMART_REPLAY_ENABLED,
        "smart_replay_timeout_s": SMART_REPLAY_TIMEOUT,
        "ws": client.ws_stats,
        "workers": client.workers(),
    }


# ─── Dashboard WebSocket ──────────────────────────────────────

@app.websocket("/ws/dashboard")
async def dashboard_ws(websocket: WebSocket):
    """WebSocket endpoint for dashboard clients (Chrome extension side panel)."""
    # Reject cross-origin connections (only allow localhost)
    origin = (websocket.headers.get("origin") or "").lower()
    if origin and not any(origin.startswith(p) for p in (
        "http://127.0.0.1", "http://localhost", "chrome-extension://",
    )):
        await websocket.close(code=4003, reason="Origin not allowed")
        return
    await websocket.accept()

    q = event_bus.subscribe()
    try:
        # Send initial snapshot
        client = get_flow_client()
        controller = get_worker_controller()
        from agent.db import crud
        pending_requests = await crud.list_requests(status="PENDING")
        processing_requests = await crud.list_requests(status="PROCESSING")
        snapshot = {
            "type": "snapshot",
            "health": {
                "status": "ok",
                "extension_connected": client.connected,
            },
            "requests": pending_requests + processing_requests,
            "worker": {
                "active": controller.active_count,
                "slots": max(0, 5 - controller.active_count),
            },
        }
        await websocket.send_text(json.dumps(snapshot))

        # Forward events from event_bus to this client
        while True:
            try:
                msg = await asyncio.wait_for(q.get(), timeout=30.0)
                await websocket.send_text(msg)
            except asyncio.TimeoutError:
                # Send keepalive ping
                await websocket.send_text(json.dumps({"type": "ping"}))
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.debug("Dashboard WS client disconnected: %s", e)
    finally:
        event_bus.unsubscribe(q)


if __name__ == "__main__":
    import os
    import uvicorn
    reload_enabled = os.environ.get("GLA_RELOAD", "0") == "1"
    uvicorn.run(
        "agent.main:app",
        host=API_HOST,
        port=API_PORT,
        reload=reload_enabled,
        reload_excludes=["*.db", "*.db-wal", "*.db-shm", "output/*"],
        timeout_graceful_shutdown=60,
    )
