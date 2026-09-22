"""
Flow Client — communicates with Google Flow via the Chrome extension bridge.

Agent runs a WS server. Extension connects as client. Agent sends requests,
extension executes them in browser context (residential IP, cookies, reCAPTCHA).

Two transports live here. The current one is Flow's ``batchexecute`` endpoint on
flow.google.com, whose calls only a signed-in page can sign — the agent builds
the envelope, the extension runs it in the tab (see :mod:`agent.services.flow_batch`).
The old REST path against ``aisandbox-pa.googleapis.com`` is kept behind
``USE_BATCH_RPC=0``; it needs a ``Bearer ya29.…`` that Flow stopped minting in
the September 2026 migration, so it is a post-mortem tool, not a fallback.

Both shape their answers the same way, so everything downstream — the worker's
parsers, the operation poller, the scene/character updaters — is transport-blind.
"""
import asyncio
import base64
import contextvars
import copy
import json
import logging
import os
import random
import time
import urllib.request
import uuid
from typing import Awaitable, Callable, Optional

from agent.config import (
    BASE_DIR,
    GOOGLE_FLOW_API, GOOGLE_API_KEY, ENDPOINTS,
    VIDEO_MODELS, UPSCALE_MODELS, IMAGE_MODELS, VIDEO_POLL_TIMEOUT,
    USE_BATCH_RPC, FLOW_PROJECT_ID, FLOW_ALLOW_DEGRADED,
    DEFAULT_PAYGATE_TIER, PROFILE_MAX_CONCURRENT,
    SMART_REPLAY_ENABLED, SMART_REPLAY_TIMEOUT,
    AUTH_STRIKES_BEFORE_DISABLE,
    AUTH_STRIKE_TTL_S,
)
from agent import config as _config
from agent.services import flow_batch as fb
from agent.services.headers import random_headers

from agent.services.video_evidence import transition as video_evidence
from agent.services.flow_trace import emit as trace_emit, traced, summary as trace_summary, identifier as trace_identifier, fingerprint

logger = logging.getLogger(__name__)

_R2V_OPS_PATH = BASE_DIR / ".scratch" / "r2v_ops.json"
_MEDIA_PROFILES_PATH = BASE_DIR / ".scratch" / "media_profiles.json"
_GETSESSION_DUMP = BASE_DIR / ".scratch" / "getsession-last.txt"
_LISTING_DUMP = BASE_DIR / ".scratch" / "listing-last.txt"
_R2V_AS29S_CAP = 12

# Which Chrome nick the current RPC is running on. `_send` reads this so a
# high-level call can pin (or fail over) without every envelope builder
# knowing about WebSockets.
_current_route: contextvars.ContextVar[Optional[dict]] = contextvars.ContextVar(
    "flow_route", default=None,
)


class FlowClient:
    """Sends commands to Chrome extension via WebSocket."""

    def __init__(self):
        self._event_loop = None
        self._extension_ws = None  # Active authenticated extension connection
        self._extensions: dict[object, dict] = {}
        self._pending: dict[str, asyncio.Future] = {}
        self._pending_ws: dict[str, object] = {}
        self._flow_key: Optional[str] = None
        # Per-operation poll state. `_operation_projects` says which project
        # listing to look a finished media up in; `_operation_media` caches the
        # id once the listing has it, so later rounds skip the listing entirely;
        # `_operation_polls` counts rounds, to keep the listing off most of them.
        self._operation_projects: dict[str, str] = {}
        self._operation_media: dict[str, str] = {}
        self._operation_polls: dict[str, int] = {}
        self._operation_start_time: dict[str, float] = {}
        self._operation_results: dict[str, dict] = {}
        self._operation_poll_tasks: dict[str, asyncio.Task] = {}
        # Which nick created an operation / media id. Poll, get_media, and
        # i2v-after-upload must stay on that Chrome — Flow projects do not
        # exist on another Google account.
        self._operation_profiles: dict[str, str] = {}
        self._media_profiles: dict[str, str] = {}
        # r2v StreamChat: the submit uuid is often a chat-message id, not the
        # listing key. Poll then asks GetSession (GN0Bre) on this conversation.
        self._operation_chat_sessions: dict[str, str] = {}
        self._operation_ref_ids: dict[str, tuple[str, ...]] = {}
        # WS stats
        self._ws_connect_count = 0
        self._ws_disconnect_count = 0
        self._ws_connected_at: Optional[float] = None
        self._ws_last_disconnect_at: Optional[float] = None
        # Creation-agent conversation id from GN0Bre. StreamChat must reuse it;
        # a minted uuid comes back PUBLIC_ERROR_UNUSUAL_ACTIVITY. Global is the
        # single-nick fallback; live r2v reads the per-WS copy.
        self._chat_session_id: Optional[str] = None
        self._profile_chat_sessions: dict[str, str] = {}
        self._chat_bind_inflight: set[str] = set()
        from agent.services.accounts import load_nick_pins
        self._configured_profiles: list[dict] = load_nick_pins()
        self._profile_semaphores: dict[str, asyncio.Semaphore] = {}
        self._retrying_profiles: set[str] = set()
        self._last_route: Optional[dict] = None
        self._operation_complaints: dict[str, str] = {}
        self._profile_dispatched_counts: dict[str, int] = {}
        self._profile_in_flight: dict[str, int] = {}
        self._worker_dispatch_locks: dict[str, asyncio.Lock] = {}
        self._worker_last_video_dispatch: dict[str, float] = {}
        self._replay_in_progress: set[str] = set()
        self._unusual_strikes: dict[str, int] = {}
        self._auth_strikes: dict[str, int] = {}
        self._auth_strike_ts: dict[str, float] = {}
        self._replay_watchdogs: set[str] = set()
        self._load_r2v_ops()
        self._load_media_profiles()

    def _worker_dispatch_lock(self, prof_id: str) -> asyncio.Lock:
        if prof_id not in self._worker_dispatch_locks:
            self._worker_dispatch_locks[prof_id] = asyncio.Lock()
        return self._worker_dispatch_locks[prof_id]

    def reload_configured_profiles(self) -> None:
        from agent.services.accounts import load_nick_pins
        self._configured_profiles = load_nick_pins()
        for ws, session in list(self._extensions.items()):
            if not session.get("chat_session_id"):
                pid = self._session_project(session)
                if pid and self._UUID_RE.match(str(pid)):
                    session["project_id"] = pid
                    asyncio.create_task(self._ensure_chat_session(ws))

    async def bind_chat_session(self, profile_id: str) -> dict:
        """Trigger chat session binding for a connected profile."""
        for ws, session in list(self._extensions.items()):
            sid = session.get("profile_id")
            if sid and str(sid).strip().lower() == str(profile_id).strip().lower():
                pid = self._session_project(session)
                if pid and self._UUID_RE.match(str(pid)):
                    session["project_id"] = pid
                    await self._ensure_chat_session(ws)
                    chat_id = session.get("chat_session_id")
                    return {"ok": bool(chat_id), "session": chat_id, "profile_id": sid}
                return {"ok": False, "error": f"no Flow project UUID for profile {sid}"}
        return {"ok": False, "error": f"profile {profile_id} is not connected"}

    def set_extension(self, ws):
        """Called when extension connects via WS."""
        try:
            self._event_loop = asyncio.get_running_loop()
        except RuntimeError:
            pass
        self._extensions[ws] = {
            "connected_at": time.time(),
            "flow_key": None,
            "token_captured_at": None,
            "unavailable_until": 0,
            "profile_id": None,
            "project_id": None,
            "chat_session_id": None,
            "in_flight": 0,
            "dispatched_count": 0,
        }
        # A new unauthenticated profile must not displace an already
        # authenticated extension. It becomes active after token_captured.
        if self._extension_ws is None:
            self._extension_ws = ws
        self._ws_connect_count += 1
        self._ws_connected_at = time.time()
        logger.info(
            "Extension connected #%d (%d active connection(s)); "
            "waiting for extension_ready/token_captured to sync",
            self._ws_connect_count,
            len(self._extensions),
        )

    def clear_extension(self, ws=None):
        """Called when extension disconnects."""
        disconnected_ws = ws or self._extension_ws
        if disconnected_ws is None:
            return

        self._extensions.pop(disconnected_ws, None)
        self._ws_disconnect_count += 1
        self._ws_last_disconnect_at = time.time()

        # Only cancel requests that were sent through the disconnected socket.
        # Requests owned by other Chrome profiles are still valid.
        disconnected_pending = [
            (req_id, self._pending.get(req_id))
            for req_id, pending_ws in list(self._pending_ws.items())
            if pending_ws is disconnected_ws
        ]
        for req_id, future in disconnected_pending:
            if future is not None and not future.done():
                future.set_exception(ConnectionError("Extension disconnected"))
            self._pending_ws.pop(req_id, None)

        if self._extension_ws is disconnected_ws:
            self._extension_ws = self._select_extension(require_token=True)
            if self._extension_ws is None:
                self._extension_ws = self._select_extension(require_token=False)

        active_session = self._extensions.get(self._extension_ws, {})
        self._flow_key = active_session.get("flow_key")
        logger.warning(
            "Extension disconnected, cancelled %d owned request(s); "
            "%d extension connection(s) remain",
            len(disconnected_pending),
            len(self._extensions),
        )

    def remember_chat_session(
        self,
        session_id: str,
        profile_id: str | None = None,
        ws=None,
    ) -> None:
        """Pin the Flow creation-agent conversation StreamChat has to reuse."""
        sid = (session_id or "").strip()
        if not sid:
            return
        if sid != self._chat_session_id:
            self._chat_session_id = sid
            logger.info("Flow chat session %s", sid)
        if ws is not None and ws in self._extensions:
            self._extensions[ws]["chat_session_id"] = sid
        if profile_id:
            if self._profile_chat_sessions.get(profile_id) != sid:
                self._profile_chat_sessions[profile_id] = sid
                logger.info("Flow chat session %s on profile %s", sid, profile_id)
            for session in self._extensions.values():
                if session.get("profile_id") == profile_id:
                    session["chat_session_id"] = sid
            return
        if len(self._extensions) == 1:
            next(iter(self._extensions.values()))["chat_session_id"] = sid

    def _extension_candidates(self, require_token: bool):
        """Return usable extensions, least-busy first."""
        now = time.time()
        candidates = []
        for ws, session in self._extensions.items():
            if require_token and not session.get("flow_key"):
                continue
            recency = (
                session.get("token_captured_at")
                if require_token
                else session.get("connected_at")
            )
            candidates.append({
                "ws": ws,
                "available": session.get("unavailable_until", 0) <= now,
                "in_flight": int(session.get("in_flight") or 0),
                "recency": recency or 0,
            })

        # Available nicks first, then the fewest in-flight RPCs. Temporarily
        # unavailable sessions stay last-resort so a single-profile setup can
        # still recover. Do not prefer "the active socket" — that serialized
        # every job onto one Chrome.
        candidates.sort(
            key=lambda item: (
                not item["available"],
                item["in_flight"],
                -item["recency"],
            ),
        )
        return [item["ws"] for item in candidates]

    def _select_extension(self, require_token: bool):
        """Choose the preferred authenticated or connected extension."""
        candidates = self._extension_candidates(require_token)
        return candidates[0] if candidates else None

    def _profile_sema(self, key: str) -> asyncio.Semaphore:
        cap = max(1, int(PROFILE_MAX_CONCURRENT or 1))
        if key not in self._profile_semaphores:
            self._profile_semaphores[key] = asyncio.Semaphore(cap)
        return self._profile_semaphores[key]

    def _configured_project(self, profile_id: str | None) -> str | None:
        if not profile_id:
            return None
        for row in self._configured_profiles:
            if row.get("id") == profile_id:
                pid = (row.get("project_id") or "").strip()
                return pid or None
        return None

    def _session_project(self, session: dict) -> str | None:
        return session.get("project_id") or self._configured_project(session.get("profile_id"))

    def _learn_project(self, profile_id: str | None, project_id: str | None) -> None:
        if not profile_id or not project_id or not self._UUID_RE.match(str(project_id)):
            return
        for row in self._configured_profiles:
            if row.get("id") == profile_id and not row.get("project_id"):
                row["project_id"] = project_id
                return

    def _apply_profile_message(self, ws, data: dict) -> None:
        if ws is None or ws not in self._extensions:
            return
        session = self._extensions[ws]
        profile_id = (data.get("profileId") or "").strip() or None
        flow_project = (data.get("flowProjectId") or "").strip() or None
        if profile_id:
            session["profile_id"] = profile_id
            cached = self._profile_chat_sessions.get(profile_id)
            if cached and not session.get("chat_session_id"):
                session["chat_session_id"] = cached
            configured = self._configured_project(profile_id)
            if configured and not session.get("project_id"):
                session["project_id"] = configured
        if flow_project and self._UUID_RE.match(flow_project):
            session["project_id"] = flow_project
            self._learn_project(session.get("profile_id"), flow_project)

    def _resolve_pin(
        self,
        project_id: str | None = None,
        profile_id: str | None = None,
        media_ids: list[str] | None = None,
        operation_id: str | None = None,
    ) -> str | None:
        """Which nick this call is stuck to, if any."""
        if profile_id:
            return profile_id
        if operation_id:
            pinned = self._operation_profiles.get(operation_id)
            if pinned:
                return pinned
            try:
                from agent.services.flow_failover import get_operation_failover, get_operation_replay
                fo = get_operation_failover(operation_id)
                if fo and fo.get("failover_worker_id"):
                    self._operation_profiles[operation_id] = fo["failover_worker_id"]
                    return fo["failover_worker_id"]
                rep = get_operation_replay(operation_id)
                if rep and rep.get("worker_id"):
                    self._operation_profiles[operation_id] = rep["worker_id"]
                    return rep["worker_id"]
            except Exception:
                pass
        for mid in media_ids or []:
            pinned = self._media_profiles.get(mid)
            if pinned:
                return pinned
        req = str(project_id or "").strip()
        if req and self._UUID_RE.match(req):
            for session in self._extensions.values():
                if self._session_project(session) == req:
                    return session.get("profile_id")
            for row in self._configured_profiles:
                if row.get("project_id") == req:
                    return row.get("id")
        return None

    def _profile_candidates(
        self,
        *,
        project_id: str | None = None,
        profile_id: str | None = None,
        media_ids: list[str] | None = None,
        operation_id: str | None = None,
        require_token: bool | None = None,
    ) -> tuple[str | None, list[dict]]:
        """Ordered nick routes. Pin is (profile_id or None, routes)."""
        if require_token is None:
            require_token = not USE_BATCH_RPC
        pin = self._resolve_pin(
            project_id=project_id,
            profile_id=profile_id,
            media_ids=media_ids,
            operation_id=operation_id,
        )
        now = time.time()
        routes = []
        for ws, session in self._extensions.items():
            if require_token and not session.get("flow_key"):
                continue
            sid = session.get("profile_id")
            if pin and ws is not pin:
                if not sid or str(sid).strip().lower() != str(pin).strip().lower():
                    continue
            recency = session.get("token_captured_at") or session.get("connected_at") or 0

            # Check if this worker profile's proxy is currently quarantined or account is disabled
            is_quar = False
            if sid:
                try:
                    from agent.services.accounts import get_account
                    from agent.services.proxy_checker import is_quarantined
                    acc = get_account(sid)
                    if acc and not acc.get("enabled", True):
                        continue
                    if acc and acc.get("proxy_url"):
                        is_quar = is_quarantined(acc["proxy_url"])
                except Exception:
                    pass

            is_avail = (session.get("unavailable_until", 0) <= now) and not is_quar
            cur_dispatched = max(
                int(session.get("dispatched_count") or 0),
                self._profile_dispatched_counts.get(sid, 0) if sid else 0,
            )
            cur_in_flight = max(
                int(session.get("in_flight") or 0),
                self._profile_in_flight.get(sid, 0) if sid else 0,
            )

            now_mono = time.monotonic()
            last_v = self._worker_last_video_dispatch.get(sid, 0.0) if sid else 0.0
            cooldown_rem = max(0.0, _config.PER_WORKER_VIDEO_COOLDOWN_MIN - (now_mono - last_v)) if sid else 0.0

            routes.append({
                "ws": ws,
                "profile_id": sid,
                "project_id": self._session_project(session),
                "pinned": bool(pin),
                "available": is_avail,
                "in_flight": cur_in_flight,
                "dispatched_count": cur_dispatched,
                "cooldown_remaining": cooldown_rem,
                "recency": recency,
            })
        routes.sort(
            key=lambda item: (
                not item["available"],
                not bool(item["profile_id"]),
                not bool(item["project_id"]),
                item["in_flight"],
                round(item.get("cooldown_remaining", 0.0), 1),
                item["dispatched_count"],
                -item["recency"],
            ),
        )
        return pin, routes

    def _select_profile(
        self,
        *,
        project_id: str | None = None,
        profile_id: str | None = None,
        media_ids: list[str] | None = None,
        operation_id: str | None = None,
    ) -> dict | None:
        _pin, routes = self._profile_candidates(
            project_id=project_id,
            profile_id=profile_id,
            media_ids=media_ids,
            operation_id=operation_id,
        )
        return routes[0] if routes else None

    def _route_project_id(self, requested: str, route: dict) -> str:
        nick_pid = route.get("project_id") or ""
        if route.get("pinned"):
            return nick_pid or self._batch_project_id(requested or "")
        if nick_pid:
            return nick_pid
        return self._batch_project_id(requested or "")

    def _chat_session_for_route(self) -> str:
        route = _current_route.get()
        if route:
            ws = route.get("ws")
            if ws is not None:
                sid = (self._extensions.get(ws) or {}).get("chat_session_id")
                if sid:
                    return sid
            pid = route.get("profile_id")
            if pid and self._profile_chat_sessions.get(pid):
                return self._profile_chat_sessions[pid]
        return self._chat_session_id or fb.CHAT_SESSION_SLOT

    async def _mint_chat_session(self, project_id: str) -> str:
        """CreateSession for this r2v. Do not reuse a poisoned conversation.

        A session that already ran ``ask_for_permission`` and was ignored
        stays stuck on Omni Flash even after video_defaults is pinned.
        """
        client_id = fb._client_uuid()
        created = await self._batch_payload(
            fb.RPC_CREATE_SESSION,
            fb.create_session_request(project_id, client_id),
            timeout=45,
        )
        ids = fb.read_session_ids(created, exclude={project_id, client_id})
        sid = ids[0] if ids else None
        if not sid:
            raise fb.FlowBatchError("CreateSession returned no conversation id")
        route = _current_route.get() or {}
        self.remember_chat_session(sid, profile_id=route.get("profile_id"))
        return sid

    def workers(self) -> list[dict]:
        now = time.time()
        now_mono = time.monotonic()
        out = []
        for session in self._extensions.values():
            pid = session.get("profile_id")
            last_v = self._worker_last_video_dispatch.get(pid, 0.0) if pid else 0.0
            cooldown_rem = max(0.0, _config.PER_WORKER_VIDEO_COOLDOWN_MIN - (now_mono - last_v)) if pid else 0.0
            out.append({
                "profile_id": pid,
                "project_id": self._session_project(session),
                "available": session.get("unavailable_until", 0) <= now,
                "video_cooldown_remaining_s": round(cooldown_rem, 1),
                "in_flight": int(session.get("in_flight") or 0),
                "chat_session": bool(session.get("chat_session_id")),
                "flow_key_present": bool(session.get("flow_key")),
                "flow_guard_version": session.get("flow_guard_version"),
            })
        return out

    @traced("worker.route")
    async def _run_on_profile(
        self,
        builder: Callable[[str], Awaitable[dict]],
        requested_project: str = "",
        *,
        profile_id: str | None = None,
        media_ids: list[str] | None = None,
        operation_id: str | None = None,
        allow_failover: bool = True,
        video_submission: bool = False,
    ) -> dict:
        """Run ``builder(flow_project_id)`` on one nick.

        Unpinned work picks the least-busy Chrome and rewrites the RPC onto
        that nick's Flow project. Pinned work (matching project, existing
        operation, or media created on a nick) stays there — another account
        cannot see that project. Failover across nicks is unpinned-only and
        rebuilds the envelope with the next nick's project id.
        """
        pin, candidates = self._profile_candidates(
            project_id=requested_project,
            profile_id=profile_id,
            media_ids=media_ids,
            operation_id=operation_id,
        )
        owners = {self._media_profiles[mid].casefold() for mid in media_ids or []
                  if self._media_profiles.get(mid)}
        if len(owners) > 1 or (owners and pin and str(pin).casefold() not in owners):
            return {"status": 400, "error_code": "media_profile_mismatch", "retryable": False,
                    "error": "MEDIA_PROFILE_MISMATCH: reference images must belong to the selected nick; upload references to one profile"}
        self._last_route = None
        if not candidates:
            if pin:
                return {"error": f"NO_FLOW_TAB: profile {pin} is not connected"}
            if self._extensions:
                return {"error": "Extension not connected"}
            # Unit tests mock batch_rpc with no sockets. get_media / poll do
            # not always have a project; generate calls still need one.
            if requested_project or FLOW_PROJECT_ID:
                pid = self._batch_project_id(requested_project or "")
            else:
                pid = ""
            return await builder(pid)

        # Every unpinned candidate parked means each nick just failed auth or
        # sits on a quarantined proxy. Dispatching anyway burns the job on a
        # known-dead session (and hides the real cause behind a Flow error), so
        # answer 503 and let the worker re-queue until a nick comes back.
        if not pin and not any(route.get("available") for route in candidates):
            parked = ", ".join(str(route.get("profile_id") or "?") for route in candidates)
            # Tell the client how long the park actually lasts. Without it the
            # studios retried for ~10s against a 30m park and failed the item.
            waits = [
                float((self._extensions.get(route.get("ws")) or {}).get("unavailable_until") or 0)
                for route in candidates
            ]
            soonest = min((w for w in waits if w > 0), default=0.0)
            retry_after = int(max(30, min(soonest - time.time(), 900))) if soonest else 60
            return {
                "status": 503,
                "retryable": True,
                "error_code": "all_workers_parked",
                "retry_after_s": retry_after,
                "error": (
                    "Extension not connected: every nick is parked "
                    f"({parked}) — auth expired or proxy quarantined, re-login needed"
                ),
            }

        last: dict = {"error": "Extension not connected"}
        for index, route in enumerate(candidates):
            pid = self._route_project_id(requested_project, route)
            token = _current_route.set(route)
            self._last_route = {
                "profile_id": route.get("profile_id"),
                "project_id": pid,
                "pinned": bool(route.get("pinned")),
            }
            session = self._extensions.get(route["ws"])
            sema_key = str(route.get("profile_id") or id(route["ws"]))
            prof_id = route.get("profile_id") or "nick-a"

            # Telemetry & burst tracker
            from agent.services.unusual_audit import get_unusual_audit
            audit_mgr = get_unusual_audit()
            burst_metrics = {}
            from agent.services.accounts import get_account
            audit_account = get_account(prof_id)
            audit_proxy_url = audit_account.get("proxy_url", "") if audit_account else ""

            from agent.services.nick_metrics import get_nick_metrics_tracker
            nick_metrics = get_nick_metrics_tracker()
            t_start = time.time()

            # Reserve worker immediately to prevent load imbalance across concurrent requests
            if session is not None:
                session["dispatched_count"] = int(session.get("dispatched_count") or 0) + 1
            if prof_id:
                self._profile_dispatched_counts[prof_id] = self._profile_dispatched_counts.get(prof_id, 0) + 1
                self._profile_in_flight[prof_id] = self._profile_in_flight.get(prof_id, 0) + 1

            try:
                trace_emit("worker.wait", profile_hash=fingerprint(prof_id), attempt=index+1,
                           candidates=len(candidates), pinned=bool(pin), operation_id=trace_identifier(operation_id))
                async with self._profile_sema(sema_key):
                    trace_emit("worker.acquired", profile_hash=fingerprint(prof_id),
                               queue_ms=round((time.time()-t_start)*1000))
                    if session is not None:
                        session["in_flight"] = int(session.get("in_flight") or 0) + 1
                        nick_metrics.record_in_flight(prof_id, session["in_flight"])
                    try:
                        if video_submission:
                            async with self._worker_dispatch_lock(prof_id):
                                now_m = time.monotonic()
                                last_disp = self._worker_last_video_dispatch.get(prof_id, 0.0)
                                # Emulate human pacing: randomized jitter between min and max
                                cooldown_target = random.uniform(
                                    _config.PER_WORKER_VIDEO_COOLDOWN_MIN,
                                    _config.PER_WORKER_VIDEO_COOLDOWN_MAX,
                                )
                                wait_s = cooldown_target - (now_m - last_disp)
                                if wait_s > 0:
                                    logger.info(
                                        "Worker %s human jitter video pacing: waiting %.2fs (target: %.2fs in [%.1fs, %.1fs])",
                                        prof_id, wait_s, cooldown_target,
                                        _config.PER_WORKER_VIDEO_COOLDOWN_MIN, _config.PER_WORKER_VIDEO_COOLDOWN_MAX,
                                    )
                                    await asyncio.sleep(wait_s)
                                self._worker_last_video_dispatch[prof_id] = time.monotonic()
                                burst_metrics = audit_mgr.record_request_dispatched(prof_id)
                                burst_metrics["rpc_in_flight"] = session.get("in_flight", 0) if session else 0
                                nick_metrics.record_dispatch(prof_id)
                                try:
                                    last = await builder(pid)
                                except Exception as e:
                                    last = _batch_error(e)
                        else:
                            burst_metrics = audit_mgr.record_request_dispatched(prof_id)
                            burst_metrics["rpc_in_flight"] = session.get("in_flight", 0) if session else 0
                            nick_metrics.record_dispatch(prof_id)
                            try:
                                last = await builder(pid)
                            except Exception as e:
                                last = _batch_error(e)
                    finally:
                        if session is not None:
                            session["in_flight"] = max(
                                0, int(session.get("in_flight") or 1) - 1,
                            )
                            nick_metrics.record_in_flight(prof_id, session["in_flight"])
            finally:
                if prof_id:
                    self._profile_in_flight[prof_id] = max(0, self._profile_in_flight.get(prof_id, 1) - 1)
                _current_route.reset(token)

            duration_ms = int((time.time() - t_start) * 1000)

            if isinstance(last, dict):
                raw_err = str(last.get("error") or last.get("data") or "")
                # Only the extension emits SUBMISSION_OUTCOME_UNKNOWN, and always
                # in the error channel. raw_err falls back to the whole data
                # payload, so matching it there would fail healthy responses —
                # the bug class that disabled four signed-in nicks on 22 Sep.
                if last.get("error") and "SUBMISSION_OUTCOME_UNKNOWN" in str(last["error"]):
                    nick_metrics.record_completion(prof_id, success=False, latency_ms=duration_ms, error=raw_err[:200])
                    return {**last, "retryable": False, "error_code": "upstream_submission_unknown"}
                if video_submission and last.get("error"):
                    # A dropped response may hide an accepted render. Only
                    # explicit pre-submit rejection can safely move/retry it.
                    safe_rejection = any(marker in raw_err.lower() for marker in (
                        "no_at_token", "no_flow_tab", "no_flow_key", "no_flow_project",
                        "extension not connected", "public_error_", "low_priority_only",
                        "ask_for_permission", "unsupported_on_batch_api",
                        "_not_submitted", "status=401", "status 401", "status: 401",
                        "http 401", "401 unauthorized", "unauthorized",
                        "envelope in response", "flow_page_not_ready",
                    ))
                    if not safe_rejection:
                        nick_metrics.record_completion(prof_id, success=False,
                                                       latency_ms=duration_ms, error=raw_err[:200])
                        return {**last, "retryable": False, "error_code": "upstream_submission_unknown"}

                # Classify auth off the error channel and the real status code.
                # raw_err falls back to the whole data payload, so a bare "401"
                # substring test matched healthy responses whose media and
                # operation uuids happen to contain those digits — that is what
                # disabled four signed-in nicks on 22 Sep.
                err_text = str(last.get("error") or "")
                status_code = last.get("status") if isinstance(last.get("status"), int) else None
                low_err = err_text.lower()
                hard_401 = status_code == 401 or any(m in low_err for m in (
                    "status=401", "status 401", "status: 401", "http 401",
                    "401 unauthorized", "unauthorized",
                ))
                is_auth_error = bool(err_text) and (hard_401 or any(m in low_err for m in (
                    "no_at_token", "envelope in response",
                )))
                if is_auth_error and session is not None:
                    # A real 401 means the Google session is dead — parking in
                    # 30m loops just spams retries. Disable the account until
                    # the user re-logs in and re-enables it. Tab-side issues
                    # (no_at_token, destroyed context) stay a 30m park.
                    if hard_401:
                        try:
                            from agent.services.accounts import upsert_account
                            acc = get_account(prof_id)
                            if acc and acc.get("enabled", True):
                                acc["enabled"] = False
                                upsert_account(acc)
                                logger.warning(
                                    "Account %s disabled after 401 — needs Google re-login",
                                    prof_id,
                                )
                                try:
                                    from agent.services.incident_manager import get_incident_manager
                                    get_incident_manager().record_incident(
                                        module="worker",
                                        job_id=prof_id,
                                        severity="CRITICAL",
                                        error_code="ACCOUNT_AUTH_EXPIRED",
                                        message=(
                                            f"{prof_id} returned 401 — Google session expired; "
                                            "account disabled until re-login"
                                        ),
                                        root_cause="Google auth session rejected by upstream (401)",
                                        action_taken="ACCOUNT_DISABLED",
                                    )
                                except Exception:
                                    pass
                        except Exception as dis_exc:
                            logger.warning("Could not disable %s after 401: %s", prof_id, dis_exc)
                    else:
                        session["unavailable_until"] = max(session.get("unavailable_until", 0), time.time() + 1800)
                        # A soft auth failure hides its status code: the batch
                        # envelope is simply absent because the page answered a
                        # sign-in redirect. One is transient; a run of them on
                        # the same nick is the same dead session a hard 401
                        # reports, so escalate instead of re-parking forever.
                        now_s = time.time()
                        if now_s - self._auth_strike_ts.get(prof_id, 0.0) > AUTH_STRIKE_TTL_S:
                            strikes = 1  # previous run aged out
                        else:
                            strikes = self._auth_strikes.get(prof_id, 0) + 1
                        self._auth_strikes[prof_id] = strikes
                        self._auth_strike_ts[prof_id] = now_s
                        logger.warning(
                            "Auth / 401 failure on %s: %s; marked unavailable for 30m (strike %d)",
                            prof_id, raw_err[:120], strikes,
                        )
                        if strikes >= AUTH_STRIKES_BEFORE_DISABLE:
                            try:
                                from agent.services.accounts import upsert_account
                                acc = get_account(prof_id)
                                if acc and acc.get("enabled", True):
                                    acc["enabled"] = False
                                    upsert_account(acc)
                                    logger.warning(
                                        "Account %s disabled after %d auth failures — needs Google re-login",
                                        prof_id, strikes,
                                    )
                                    try:
                                        from agent.services.incident_manager import get_incident_manager
                                        get_incident_manager().record_incident(
                                            module="worker",
                                            job_id=prof_id,
                                            severity="CRITICAL",
                                            error_code="ACCOUNT_AUTH_EXPIRED",
                                            message=(
                                                f"{prof_id} failed auth {strikes}x in a row "
                                                "(no rpc envelope — signed-out page); "
                                                "account disabled until re-login"
                                            ),
                                            root_cause=(
                                                "Flow tab answers a sign-in redirect instead of the "
                                                "batch envelope; the Google session is gone"
                                            ),
                                            action_taken="ACCOUNT_DISABLED",
                                        )
                                    except Exception:
                                        pass
                            except Exception as dis_exc:
                                logger.warning("Could not disable %s after auth failures: %s", prof_id, dis_exc)
                
                # Check for successful RPC
                if not last.get("error") and (
                    not isinstance(last.get("status"), int) or last["status"] < 400
                ):
                    nick_metrics.record_completion(prof_id, success=True, latency_ms=duration_ms)
                    self._unusual_strikes.pop(prof_id, None)
                    self._auth_strikes.pop(prof_id, None)
                    self._auth_strike_ts.pop(prof_id, None)
                    from agent.services.accounts import get_account
                    acc = get_account(prof_id)
                    p_url = acc.get("proxy_url", "") if acc else ""
                    audit_mgr.record_request_success(prof_id, p_url)

                elif "PUBLIC_ERROR_UNUSUAL_ACTIVITY" in raw_err:
                    # Identify RPC ID
                    rpc_id = "unknown"
                    for known_rpc in ["agJzFb", "ogiZ0b", "eb1hJf", "YhhmEf", "k42Yye", "o30O0e", "pDU0ue"]:
                        if known_rpc in raw_err:
                            rpc_id = known_rpc
                            break

                    logger.warning(
                        "UNUSUAL_ACTIVITY detected on %s (RPC %s)! Synchronously rotating proxy...",
                        prof_id, rpc_id,
                    )

                    rotation_info = {
                        "rotation_triggered": False,
                        "retry_attempted": False,
                        "retry_success": False,
                    }

                    try:
                        try:
                            from agent.services.proxy_checker import quarantine_proxy
                            from agent.services.accounts import get_account
                            acc = get_account(prof_id)
                            if acc and acc.get("proxy_url"):
                                quarantine_proxy(acc["proxy_url"], reason="PUBLIC_ERROR_UNUSUAL_ACTIVITY", cooldown_seconds=900)
                        except Exception as q_err:
                            logger.warning("Could not quarantine proxy for %s: %s", prof_id, q_err)

                        from agent.services.proxy_pool import rotate_nick_proxy
                        rot_res = await rotate_nick_proxy(prof_id, preflight=True)
                        rotation_info["rotation_triggered"] = True
                        if not rot_res.get("ok"):
                            last["proxy_rotated"] = False
                            last["retryable"] = True
                            last["message"] = "UNUSUAL_ACTIVITY: chưa đổi được proxy; giữ đường hiện tại."
                            raise RuntimeError(rot_res.get("error") or "PROXY_ROTATION_FAILED")
                        rotation_info["new_proxy"] = rot_res.get("proxy")
                        rotation_info["new_proxy_ip"] = rot_res.get("egress_ip")
                        rotation_info["flow_tab_reloaded"] = bool(rot_res.get("flow_tab_reloaded"))

                        last["proxy_rotated"] = True
                        last["new_proxy"] = rot_res.get("proxy")
                        last["retryable"] = True
                        last["message"] = (
                            f"UNUSUAL_ACTIVITY: Proxy đã được đổi thành công sang IP mới ({rot_res.get('proxy')}). "
                            "Vui lòng retry lại ngay."
                        )

                        # Attempt one immediate retry with the new proxy
                        retrying = getattr(self, "_retrying_profiles", None)
                        if retrying is None:
                            self._retrying_profiles = set()
                            retrying = self._retrying_profiles

                        if prof_id not in retrying and not video_submission:
                            retrying.add(prof_id)
                            try:
                                logger.info(
                                    "Auto-retrying request for %s on new proxy %s...",
                                    prof_id, rot_res.get("proxy"),
                                )
                                # Proxy rotation owns the drain/reload/resume transaction.
                                # An additional reload here could disrupt newly resumed work.
                                rotation_info["flow_tab_reloaded"] = bool(rot_res.get("flow_tab_reloaded"))

                                token_retry = _current_route.set(route)
                                try:
                                    retry_last = None
                                    for retry_try in range(2):
                                        try:
                                            retry_last = await builder(pid)
                                        except Exception as e:
                                            retry_last = _batch_error(e)

                                        retry_err_str = str(retry_last.get("error") or "")
                                        if ("Frame with ID 0" in retry_err_str or "Execution context was destroyed" in retry_err_str) and retry_try == 0:
                                            logger.info("Frame still initializing on %s; waiting 3s before retry attempt 2...", prof_id)
                                            await asyncio.sleep(3.0)
                                            continue
                                        break

                                    rotation_info["retry_attempted"] = True
                                    if not retry_last.get("error") and (
                                        not isinstance(retry_last.get("status"), int)
                                        or retry_last["status"] < 400
                                    ):
                                        logger.info("Auto-retry on new proxy SUCCEEDED for %s!", prof_id)
                                        nick_metrics.record_completion(prof_id, success=True, latency_ms=duration_ms)
                                        rotation_info["retry_success"] = True
                                        self._unusual_strikes.pop(prof_id, None)
                                        audit_mgr.record_unusual_event(
                                            worker_id=prof_id,
                                            rpc_id=rpc_id,
                                            raw_error=raw_err,
                                            burst_metrics=burst_metrics,
                                            call_duration_ms=duration_ms,
                                            payload_summary={"project_id": pid},
                                            rotation_info=rotation_info,
                                            proxy_url=audit_proxy_url,
                                        )
                                        audit_mgr.record_request_success(prof_id, rot_res.get("proxy_url", ""))
                                        return retry_last

                                    rotation_info["retry_success"] = False
                                    rotation_info["retry_error"] = str(retry_last.get("error") or "")[:200]
                                    nick_metrics.record_completion(prof_id, success=False, latency_ms=duration_ms, error="UNUSUAL_ACTIVITY_RETRY_FAILED")
                                    last = retry_last
                                    last["proxy_rotated"] = True
                                    last["new_proxy"] = rot_res.get("proxy")
                                    last["retryable"] = True
                                    last["message"] = (
                                        f"UNUSUAL_ACTIVITY: Proxy đã được đổi thành công sang IP mới ({rot_res.get('proxy')}). "
                                        "Vui lòng retry lại ngay."
                                    )
                                finally:
                                    _current_route.reset(token_retry)
                            finally:
                                retrying.discard(prof_id)
                    except Exception as rot_exc:
                        logger.error("Auto-rotation failed for %s: %s", prof_id, rot_exc)
                        rotation_info["rotation_error"] = str(rot_exc)
                        nick_metrics.record_completion(prof_id, success=False, latency_ms=duration_ms, error=str(rot_exc))

                    # Persist full audit record
                    audit_mgr.record_unusual_event(
                        worker_id=prof_id,
                        rpc_id=rpc_id,
                        raw_error=raw_err,
                        burst_metrics=burst_metrics,
                        call_duration_ms=duration_ms,
                        payload_summary={"project_id": pid},
                        rotation_info=rotation_info,
                        proxy_url=audit_proxy_url,
                    )

                    # Worker-level circuit breaker: every flag already rotates
                    # the proxy, so a repeat flag means the fresh IP failed too
                    # — that is an account/session trust problem, not an IP
                    # problem. Park the worker so unpinned work routes to other
                    # nicks while the session recovers; escalate to a re-login
                    # incident instead of burning more proxy sessions.
                    strikes = self._unusual_strikes.get(prof_id, 0) + 1
                    self._unusual_strikes[prof_id] = strikes
                    cooldown_s = 300 if strikes < 2 else 1800
                    if session is not None:
                        session["unavailable_until"] = max(
                            session.get("unavailable_until", 0),
                            time.time() + cooldown_s,
                        )
                    if strikes >= 2:
                        try:
                            from agent.services.incident_manager import get_incident_manager
                            get_incident_manager().record_incident(
                                module="worker",
                                job_id=prof_id,
                                severity="CRITICAL",
                                error_code="ACCOUNT_SESSION_FLAGGED",
                                message=(
                                    f"{prof_id} flagged UNUSUAL_ACTIVITY {strikes}x in a row "
                                    "across rotated proxies — Google account session needs re-login"
                                ),
                                root_cause=(
                                    "reCAPTCHA trust failure persists on fresh verified IPs; "
                                    "the flag follows the Google session, not the proxy"
                                ),
                                action_taken=f"WORKER_PAUSED_{cooldown_s}S",
                            )
                        except Exception:
                            pass

                elif ("failed: [13]" in raw_err or "ogiz0b failed: [13]" in raw_err.lower()) and not getattr(self, "_in_retry_13", False):
                    prof_id = route.get("profile_id") or "worker"
                    self._in_retry_13 = True
                    try:
                        logger.warning(
                            "Transient Google RPC [13] on %s! Backing off 2.5s and auto-retrying...",
                            prof_id,
                        )
                        await asyncio.sleep(2.5)
                        token_retry = _current_route.set(route)
                        try:
                            try:
                                retry_last = await builder(pid)
                            except Exception as e:
                                retry_last = _batch_error(e)
                            if not retry_last.get("error") and (
                                not isinstance(retry_last.get("status"), int)
                                or retry_last["status"] < 400
                            ):
                                logger.info("Auto-retry on RPC [13] SUCCEEDED for %s!", prof_id)
                                nick_metrics.record_completion(prof_id, success=True, latency_ms=duration_ms)
                                return retry_last
                            last = retry_last
                            nick_metrics.record_completion(prof_id, success=False, latency_ms=duration_ms, error="RPC_13_RETRY_FAILED")
                        finally:
                            _current_route.reset(token_retry)
                    except Exception as r13_exc:
                        logger.error("Auto-retry on RPC [13] failed for %s: %s", prof_id, r13_exc)
                        nick_metrics.record_completion(prof_id, success=False, latency_ms=duration_ms, error=str(r13_exc))
                    finally:
                        self._in_retry_13 = False
                elif raw_err:
                    nick_metrics.record_completion(prof_id, success=False, latency_ms=duration_ms, error=raw_err[:200])


            has_alternative = index + 1 < len(candidates)
            if (
                isinstance(last, dict)
                and self._should_failover(last)
                and allow_failover
                and not route.get("pinned")
                and has_alternative
            ):
                if route["ws"] in self._extensions:
                    self._extensions[route["ws"]]["unavailable_until"] = (
                        time.time() + 60
                    )
                logger.warning(
                    "Profile %s unavailable; retrying through another nick",
                    route.get("profile_id") or "anonymous",
                )
                continue
            return last
        return last

    @staticmethod
    def _should_failover(result: dict) -> bool:
        """Return true for profile-local failures that another tab can solve."""
        message = str(result.get("error") or result.get("data") or "").lower()
        return any(marker in message for marker in (
            "no_flow_key",
            "no_flow_tab",
            "no_flow_project",
            # Batch path: this profile's Flow tab cannot sign a request — it is
            # signed out, still booting, or Chrome discarded it. Another
            # profile's tab may be perfectly able to.
            "no_at_token",
            "flow_tab_discarded",
            "no current window",
            "frame with id 0",
            "execution context was destroyed",
            "target closed",
            "session closed",
            "extension not connected",
            "extension disconnected",
            "extension_switched",
            "public_error_per_model_daily_quota_reached",
            "public_error_user_quota_reached",
            "public_error_unusual_activity",
            "failed: [13]",
            "failed: [14]",
            "status=401",
            "status 401",
            "status: 401",
            "401",
            "unauthorized",
            "envelope in response",
            "flow_page_not_ready",
        ))

    def set_flow_key(self, key: str):
        self._flow_key = key
        if self._extension_ws in self._extensions:
            self._extensions[self._extension_ws]["flow_key"] = key
            self._extensions[self._extension_ws]["token_captured_at"] = time.time()

    @property
    def connected(self) -> bool:
        return bool(self._extensions)

    @property
    def ws_stats(self) -> dict:
        uptime = None
        if self._ws_connected_at and self.connected:
            uptime = int(time.time() - self._ws_connected_at)
        return {
            "connected": self.connected,
            "active_connections": len(self._extensions),
            "authenticated_connections": sum(
                1 for session in self._extensions.values()
                if session.get("flow_key")
            ),
            "connects": self._ws_connect_count,
            "disconnects": self._ws_disconnect_count,
            "uptime_s": uptime,
            "workers": self.workers(),
        }

    async def handle_message(self, data: dict, websocket=None):
        """Handle incoming message from extension."""
        if data.get("type") == "token_captured":
            key = data.get("flowKey")
            source_ws = websocket or self._extension_ws
            if source_ws is not None and source_ws in self._extensions:
                self._extensions[source_ws]["flow_key"] = key
                self._extensions[source_ws]["token_captured_at"] = time.time()
                self._extension_ws = source_ws
                self._apply_profile_message(source_ws, data)
            self._flow_key = key
            logger.info("Flow key captured from extension")
            asyncio.create_task(self._sync_tier())
            return

        if data.get("type") in ("extension_ready", "profile_update"):
            source_ws = websocket or self._extension_ws
            self._apply_profile_message(source_ws, data)
            session = self._extensions.get(source_ws) or {}
            logger.info(
                "Extension %s profile=%s project=%s flowKey=%s",
                data.get("type"),
                session.get("profile_id"),
                session.get("project_id"),
                "yes" if data.get("flowKeyPresent") or session.get("flow_key") else "no",
            )
            if data.get("type") == "extension_ready":
                session["flow_guard_version"] = data.get("flowGuardVersion")
                asyncio.create_task(self._sync_tier())
            if not session.get("chat_session_id"):
                pid = self._session_project(session)
                if pid and self._UUID_RE.match(str(pid)):
                    self._chat_bind_task = asyncio.create_task(
                        self._ensure_chat_session(source_ws)
                    )
            return

        if data.get("type") == "chat_session":
            source_ws = websocket or self._extension_ws
            sid = (data.get("session") or "").strip()
            self._apply_profile_message(source_ws, data)
            session = self._extensions.get(source_ws) or {}
            profile_id = session.get("profile_id") or (data.get("profileId") or "").strip() or None
            self.remember_chat_session(sid, profile_id=profile_id, ws=source_ws)
            logger.info(
                "chat_session %s profile=%s",
                sid, profile_id,
            )
            return

        if data.get("type") == "media_urls_refresh":
            asyncio.create_task(self._refresh_media_urls(data.get("urls", [])))
            return

        if data.get("type") == "pong":
            return

        if data.get("type") == "ping":
            # Respond to keepalive
            target_ws = websocket or self._extension_ws
            if target_ws:
                await target_ws.send(json.dumps({"type": "pong"}))
            return

        # Response to a pending request
        req_id = data.get("id")
        if req_id and req_id in self._pending:
            if not self._pending[req_id].done():
                self._pending[req_id].set_result(data)
            return

    _CHAT_BIND_RETRY = ("NO_INJECTION_RESULT", "NO_FLOW_TAB", "FLOW_TAB_DISCARDED", "Timeout")

    async def _ensure_chat_session(self, ws) -> None:
        """Mint or reuse a creation-agent conversation on this nick.

        Opening Ingredients in the Flow UI only lists sessions (``mrlkwd``);
        the conversation StreamChat needs is created by ``csbIsb`` and lives
        in that *response*. Bind it here so /nicks r2v goes green without a
        human clicking Ingredients or relaying a uuid.
        """
        if not USE_BATCH_RPC:
            return
        session = self._extensions.get(ws) or {}
        if session.get("chat_session_id"):
            return
        profile_id = session.get("profile_id")
        project_id = self._session_project(session) or FLOW_PROJECT_ID
        if not project_id or not self._UUID_RE.match(str(project_id)):
            logger.info("chat session auto-bind deferred: no valid project_id yet for profile=%s", profile_id)
            return
        session["project_id"] = str(project_id)
        guard = str(profile_id or id(ws))
        if guard in self._chat_bind_inflight:
            return
        self._chat_bind_inflight.add(guard)

        async def run(pid: str):
            listed = await self._batch_payload(
                fb.RPC_LIST_SESSIONS, fb.list_sessions_request(pid), timeout=45)
            logger.info("ListSessions profile=%s payload=%r", profile_id, listed)
            ids = fb.read_session_ids(listed, exclude={pid})
            sid = ids[0] if ids else None
            if not sid:
                client_id = fb._client_uuid()
                created = await self._batch_payload(
                    fb.RPC_CREATE_SESSION,
                    fb.create_session_request(pid, client_id),
                    timeout=45,
                )
                logger.info("CreateSession profile=%s payload=%r", profile_id, created)
                ids = fb.read_session_ids(created, exclude={pid, client_id})
                sid = ids[0] if ids else None
            if not sid:
                raise fb.FlowBatchError(
                    "no chat session uuid in ListSessions/CreateSession"
                )
            self.remember_chat_session(sid, profile_id=profile_id, ws=ws)
            return {"ok": True, "session": sid}

        last_error = None
        try:
            for attempt, wait in enumerate((0, 1.5, 3.0, 5.0, 8.0)):
                if wait:
                    await asyncio.sleep(wait)
                if ws not in self._extensions:
                    return
                if (self._extensions.get(ws) or {}).get("chat_session_id"):
                    return
                try:
                    result = await self._run_on_profile(
                        run, str(project_id),
                        profile_id=profile_id, allow_failover=False,
                    )
                except Exception as exc:
                    result = {"error": str(exc)}
                if isinstance(result, dict) and result.get("ok"):
                    return
                last_error = (result or {}).get("error") if isinstance(result, dict) else result
                transient = any(
                    token in str(last_error or "") for token in self._CHAT_BIND_RETRY
                )
                logger.warning(
                    "chat session auto-bind attempt %s profile=%s: %s",
                    attempt + 1, profile_id, last_error,
                )
                if not transient:
                    break
        except Exception:
            logger.exception("chat session auto-bind failed profile=%s", profile_id)
        finally:
            self._chat_bind_inflight.discard(guard)
        if last_error:
            logger.warning(
                "chat session auto-bind failed profile=%s: %s",
                profile_id, last_error,
            )

    async def _sync_tier(self):
        """Detect current tier from credits API and update all active projects."""
        if getattr(self, '_sync_in_progress', False):
            return
        self._sync_in_progress = True
        try:
            result = await self.get_credits()
            data = result.get("data", result)
            tier = data.get("userPaygateTier", "PAYGATE_TIER_ONE")
            logger.info("Syncing tier: %s", tier)

            from agent.db import crud
            projects = await crud.list_projects(status="ACTIVE")
            for p in projects:
                if p.get("user_paygate_tier") != tier:
                    await crud.update_project(p["id"], user_paygate_tier=tier)
                    logger.info("Updated project %s tier: %s -> %s",
                                p["id"][:12], p.get("user_paygate_tier"), tier)
        except Exception as e:
            logger.warning("Failed to sync tier: %s", e)
        finally:
            self._sync_in_progress = False

    _UUID_RE = __import__("re").compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
    # flow-content.google is where the rewritten frontend serves media from;
    # the other two are the pre-migration hosts, still seen on older media.
    _SAFE_URL_RE = __import__("re").compile(
        r'^https://(storage\.googleapis\.com|lh3\.googleusercontent\.com|flow-content\.google)/')

    async def _refresh_media_urls(self, urls: list[dict]):
        """Update scene/character URLs in DB from fresh TRPC-captured signed URLs.

        Each entry: {mediaId: str, mediaType: 'image'|'video', url: str}
        """
        from agent.db import crud
        from agent.services.event_bus import event_bus

        updated = 0
        for entry in urls:
            media_id = entry.get("mediaId", "")
            media_type = entry.get("mediaType", "")
            url = entry.get("url", "")
            if not media_id or not url:
                continue
            # Validate media_id is UUID and url is from trusted domains
            if not self._UUID_RE.match(media_id):
                logger.warning("Rejected invalid media_id: %s", media_id[:20])
                continue
            if not self._SAFE_URL_RE.match(url):
                logger.warning("Rejected untrusted URL domain for media %s", media_id[:12])
                continue
            if media_type not in ("image", "video"):
                continue

            # Try matching against scenes (check both orientations)
            scenes = await crud.list_scenes_by_media_id(media_id)
            for scene in scenes:
                updates = {}
                if media_type == "image":
                    # Update whichever orientation matches
                    if scene.get("vertical_image_media_id") == media_id:
                        updates["vertical_image_url"] = url
                    if scene.get("horizontal_image_media_id") == media_id:
                        updates["horizontal_image_url"] = url
                elif media_type == "video":
                    if scene.get("vertical_video_media_id") == media_id:
                        updates["vertical_video_url"] = url
                    if scene.get("horizontal_video_media_id") == media_id:
                        updates["horizontal_video_url"] = url
                    if scene.get("vertical_upscale_media_id") == media_id:
                        updates["vertical_upscale_url"] = url
                    if scene.get("horizontal_upscale_media_id") == media_id:
                        updates["horizontal_upscale_url"] = url
                if updates:
                    await crud.update_scene(scene["id"], **updates)
                    updated += 1

            # Try matching against characters
            chars = await crud.list_characters_by_media_id(media_id)
            for char in chars:
                if media_type == "image" and char.get("media_id") == media_id:
                    await crud.update_character(char["id"], reference_image_url=url)
                    updated += 1

        if updated:
            logger.info("Refreshed %d media URLs from TRPC intercept", updated)
            await event_bus.emit("urls_refreshed", {"count": updated})

    async def refresh_project_urls(self, project_id: str) -> dict:
        """Re-sign every stored media url for a project.

        The batch path can do this properly: the media rpc answers a media id
        with a freshly signed url, so we walk the project's scenes and entities
        and refresh each id we hold. The legacy path could not — its media
        endpoint returned base64 content rather than a url — so it still asks
        the user to open the project in Chrome and let the intercept catch them.
        """
        if not USE_BATCH_RPC:
            logger.info("URL refresh requested for project %s — legacy path has no "
                        "url-serving media endpoint", project_id[:12])
            return {"refreshed": 0, "found": 0, "note": "Legacy REST path: no URL refresh. "
                    "Open the project in Google Flow in Chrome and let the extension "
                    "intercept fresh URLs, or set USE_BATCH_RPC=1."}

        from agent.db import crud

        # (media_id, kind) -> the scene/character fields it should land in
        targets: dict[tuple[str, str], list[tuple[str, str, str]]] = {}

        def want(media_id, kind, table, row_id, field):
            if media_id and self._UUID_RE.match(media_id):
                targets.setdefault((media_id, kind), []).append((table, row_id, field))

        scenes = []
        for video in await crud.list_videos(project_id):
            scenes.extend(await crud.list_scenes(video["id"]))

        for scene in scenes:
            for prefix in ("vertical", "horizontal"):
                want(scene.get(f"{prefix}_image_media_id"), "image",
                     "scene", scene["id"], f"{prefix}_image_url")
                want(scene.get(f"{prefix}_video_media_id"), "video",
                     "scene", scene["id"], f"{prefix}_video_url")
                want(scene.get(f"{prefix}_upscale_media_id"), "video",
                     "scene", scene["id"], f"{prefix}_upscale_url")
        for char in await crud.get_project_characters(project_id):
            want(char.get("media_id"), "image", "character", char["id"], "reference_image_url")

        refreshed = 0
        for (media_id, kind), fields in targets.items():
            try:
                urls = await self._batch_media_urls(media_id)
            except Exception as e:
                logger.warning("Refresh failed for media %s: %s", media_id[:12], e)
                continue
            url = urls.video if kind == "video" else urls.image
            if not url:
                continue
            for table, row_id, field in fields:
                if table == "scene":
                    await crud.update_scene(row_id, **{field: url})
                else:
                    await crud.update_character(row_id, **{field: url})
                refreshed += 1

        logger.info("Refreshed %d/%d media urls for project %s",
                    refreshed, len(targets), project_id[:12])
        return {"refreshed": refreshed, "found": len(targets)}

    async def profile_control(self, profile_id: str, method: str, params: dict, timeout: float = 75) -> dict:
        """Control exactly one extension, never fail over to another nick."""
        for ws, session in list(self._extensions.items()):
            if session.get("profile_id") != profile_id:
                continue
            if method in {"prepare_proxy_rotation", "finish_proxy_rotation"} and not session.get("flow_guard_version"):
                return {"ok": False, "error": "EXTENSION_UPDATE_REQUIRED"}
            token = _current_route.set({"ws": ws, "profile_id": profile_id})
            try:
                response = await self._send(method, params, timeout=timeout)
                return response.get("result") or {"ok": False, "error": response.get("error", "EXTENSION_CONTROL_FAILED")}
            finally:
                _current_route.reset(token)
        return {"ok": False, "error": "EXTENSION_NOT_CONNECTED"}

    @traced("extension.request")
    async def _send(self, method: str, params: dict, timeout: float = 300) -> dict:
        """Send request to extension and wait for response.

        Always returns a dict. On error, returns {"error": "<reason>"} — callers
        must check result.get("error") or use _is_ws_error() before reading data.
        Never raises; exceptions are caught and returned as error dicts.
        """
        if method == "api_request" and isinstance(params.get("body"), dict):
            for request in params["body"].get("requests", []):
                model = request.get("videoModelKey")
                if model and model not in {fb.VIDEO_MODEL, fb.VIDEO_T2V_MODEL, fb.VIDEO_R2V_MODEL}:
                    return {"status": 400, "error": "LOW_PRIORITY_ONLY: paid video models are disabled"}
        if not self.connected:
            return {"error": "Extension not connected"}

        # A bearer token is only worth routing on when something is going to
        # send one. The batchexecute path authenticates in the page with the
        # session cookie, so demanding a flow key there would reject every
        # profile — no profile on that path ever captures one.
        needs_token = not USE_BATCH_RPC
        routed = _current_route.get()
        if routed and routed.get("ws"):
            extension_candidates = [routed["ws"]]
        else:
            extension_candidates = self._extension_candidates(require_token=needs_token)
        if not extension_candidates and needs_token:
            return {"error": "NO_FLOW_KEY"}
        if not extension_candidates:
            return {"error": "Extension not connected"}

        last_result = {"error": "Extension not connected"}
        for index, extension_ws in enumerate(extension_candidates):
            if extension_ws not in self._extensions:
                continue

            self._extension_ws = extension_ws
            self._flow_key = self._extensions[extension_ws].get("flow_key")
            req_id = str(uuid.uuid4())
            trace_started = time.monotonic()
            trace_emit("extension.dispatch", extension_request_id=req_id, method=trace_identifier(method),
                       rpc_id=trace_identifier(params.get("rpcid")), timeout_s=timeout, attempt=index+1,
                       profile_hash=fingerprint(self._extensions[extension_ws].get("profile_id")),
                       pending_count=len(self._pending))
            future = asyncio.get_running_loop().create_future()
            self._pending[req_id] = future
            self._pending_ws[req_id] = extension_ws

            try:
                await extension_ws.send(json.dumps({
                    "id": req_id,
                    "method": method,
                    "params": params,
                    "expiresAt": int((time.time() + timeout - 1) * 1000),
                }))
                last_result = await asyncio.wait_for(future, timeout=timeout)
            except asyncio.TimeoutError:
                last_result = {"error": f"Timeout ({timeout}s) waiting for {method}"}
            except Exception as e:
                last_result = {"error": str(e)}
            finally:
                trace_emit("extension.result", extension_request_id=req_id,
                           elapsed_ms=round((time.monotonic()-trace_started)*1000), result=trace_summary(last_result))
                self._pending.pop(req_id, None)
                self._pending_ws.pop(req_id, None)

            has_alternative = index + 1 < len(extension_candidates)
            if self._should_failover(last_result) and has_alternative:
                if extension_ws in self._extensions:
                    self._extensions[extension_ws]["unavailable_until"] = (
                        time.time() + 60
                    )
                logger.warning(
                    "Extension profile unavailable for %s; retrying through "
                    "another authenticated profile",
                    method,
                )
                continue

            return last_result

        return last_result

    def _build_url(self, endpoint_key: str, **kwargs) -> str:
        """Build full API URL."""
        path = ENDPOINTS[endpoint_key].format(**kwargs)
        sep = "&" if "?" in path else "?"
        return f"{GOOGLE_FLOW_API}{path}{sep}key={GOOGLE_API_KEY}"

    def _client_context(self, project_id: str, user_paygate_tier: str = "PAYGATE_TIER_TWO") -> dict:
        """Build clientContext with recaptcha placeholder."""
        return {
            "projectId": str(project_id),
            "recaptchaContext": {
                "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB",
                "token": "",  # Extension injects real token
            },
            "sessionId": f";{int(time.time() * 1000)}",
            "tool": "PINHOLE",
            "userPaygateTier": user_paygate_tier,
        }

    # ─── batchexecute transport ──────────────────────────────
    #
    # Flow's rewritten frontend signs every call with the session cookie plus a
    # per-page `at` token, and a generate also carries a single-use reCAPTCHA.
    # None of that can be replayed from here, so the agent builds the envelope
    # and the extension runs it inside a signed-in flow.google.com tab.

    async def batch_rpc(self, rpcid: str, freq: str,
                        captcha_action: str | None = None,
                        match: str | None = None,
                        timeout: float = 300,
                        path: str | None = None) -> dict:
        """Run one batchexecute RPC in the Flow page. Returns the raw body.

        ``match`` asks the extension to cut the response down to an 800-byte
        window around that string before handing it back. The project listing
        is tens of megabytes for the one entry we want, and the cheapest place
        to throw the rest away is inside the tab.

        ``path`` overrides the default ``/data/batchexecute?rpcids=…`` URL —
        StreamChat lives on a service path, not a rpcid.
        """
        params: dict = {"rpcid": rpcid, "freq": freq}
        if captcha_action:
            params["captchaAction"] = captcha_action
        if match:
            params["match"] = match
        if path:
            params["path"] = path
        return await self._send("batch_rpc", params, timeout=timeout)

    @traced("rpc.parse")
    async def _batch_payload(self, rpcid: str | None, freq: str,
                             captcha_action: str | None = None,
                             timeout: float = 300,
                             path: str | None = None):
        """One RPC, unwrapped to its inner payload. Raises on anything else."""
        label = rpcid or "StreamChat"
        result = await self.batch_rpc(label, freq, captcha_action,
                                      timeout=timeout, path=path)
        if result.get("error"):
            raise fb.FlowBatchError(f"{label}: {result['error']}")
        raw = result.get("data") or ""
        if rpcid:
            return fb.first_payload(raw, rpcid)
        return fb.first_payload(raw)

    def _batch_project_id(self, project_id: str) -> str:
        """The Flow project an RPC is scoped to.

        Flow Kit stores the Flow project uuid as the local project id, but a
        few call sites pass "0" or "" for project-less work; those fall back to
        the pinned FLOW_PROJECT_ID.
        """
        if project_id and self._UUID_RE.match(str(project_id)):
            return str(project_id)
        if FLOW_PROJECT_ID:
            return FLOW_PROJECT_ID
        raise fb.FlowBatchError(
            "NO_FLOW_PROJECT: every batchexecute call is scoped to a Flow project. "
            "Create one in the Flow UI and pin its uuid as FLOW_PROJECT_ID."
        )

    def _batch_image_model(self, override: str | None = None) -> str:
        # Read through the module: PATCH /api/models hot-reloads both of these.
        nickname = _config.DEFAULT_IMAGE_MODEL
        return fb.resolve_image_model(
            override or _config.IMAGE_MODELS.get(nickname) or nickname
        )

    def _batch_video_model(self, tier: str, gen_type: str, aspect_ratio: str) -> str:
        legacy = VIDEO_MODELS.get(tier, {}).get(gen_type, {}).get(aspect_ratio)
        return fb.resolve_video_model(legacy)

    def _remember_operation(self, operation_id: str, project_id: str):
        """Which project an operation belongs to — the listing lookup needs it.

        A poll record usually carries the project id, but old operations decay
        to a bare id, so keep our own note. Bounded: this is a cache, and the
        pinned project is always a workable fallback.
        """
        if not operation_id:
            return
        clean_id = operation_id.removeprefix("operations/") if isinstance(operation_id, str) else operation_id
        if len(self._operation_projects) > 2048:
            # Evict old finished operations, never erase pins for active renders.
            for old_id in list(self._operation_results)[:128]:
                for key in (old_id, f"operations/{old_id}"):
                    for cache in (self._operation_projects, self._operation_media,
                                  self._operation_polls, self._operation_start_time,
                                  self._operation_profiles, self._operation_chat_sessions,
                                  self._operation_ref_ids, self._operation_results):
                        cache.pop(key, None)
        self._operation_start_time.setdefault(clean_id, time.time())
        op_prefixed = f"operations/{clean_id}"
        self._operation_projects[clean_id] = project_id
        self._operation_projects[op_prefixed] = project_id
        self._operation_projects[operation_id] = project_id
        route = _current_route.get()
        if route and route.get("profile_id"):
            self._operation_profiles[clean_id] = route["profile_id"]
            self._operation_profiles[op_prefixed] = route["profile_id"]
            self._operation_profiles[operation_id] = route["profile_id"]
        trace_emit("operation.bound", operation_id=trace_identifier(clean_id),
                   project_id=trace_identifier(project_id), profile_hash=fingerprint(self._operation_profiles.get(clean_id)))
        self._save_r2v_ops()
        self._schedule_replay_watchdog(clean_id)

    def _schedule_replay_watchdog(self, clean_id: str) -> None:
        """Fire smart replay on a timer — poll-driven triggers starve when the
        client polls sparsely, which is exactly the stalled-render case."""
        if not SMART_REPLAY_ENABLED or not clean_id or clean_id in self._replay_watchdogs:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        self._replay_watchdogs.add(clean_id)
        asyncio.create_task(self._replay_watchdog(clean_id))

    async def _replay_watchdog(self, clean_id: str) -> None:
        try:
            await asyncio.sleep(SMART_REPLAY_TIMEOUT)
            from agent.services.flow_failover import get_failover_target
            if get_failover_target(clean_id) or clean_id in self._replay_in_progress:
                return
            done = self._operation_results.get(clean_id) or {}
            if str(done.get("status", "")).endswith(("SUCCESSFUL", "FAILED")):
                return
            self._replay_in_progress.add(clean_id)
            await self._maybe_trigger_smart_replay(clean_id)
        finally:
            self._replay_watchdogs.discard(clean_id)

    def _remember_media(self, media_id: str, profile_id: str | None = None) -> None:
        """Which nick uploaded or generated this media id."""
        if not media_id:
            return
        if len(self._media_profiles) > 2048:
            for k in list(self._media_profiles.keys())[:512]:
                self._media_profiles.pop(k, None)
        pid = profile_id
        if not pid:
            route = _current_route.get()
            if route:
                pid = route.get("profile_id")
        if pid:
            self._media_profiles[media_id] = pid
            self._save_media_profiles()

    def _load_media_profiles(self) -> None:
        """Restore media_id -> profile_id mappings so an agent restart maintains pinning."""
        if os.environ.get("PYTEST_CURRENT_TEST"):
            return
        try:
            raw = json.loads(_MEDIA_PROFILES_PATH.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                self._media_profiles.update({str(k): str(v) for k, v in raw.items()})
        except (OSError, json.JSONDecodeError):
            pass

    def _save_media_profiles(self) -> None:
        if os.environ.get("PYTEST_CURRENT_TEST"):
            return
        try:
            _MEDIA_PROFILES_PATH.parent.mkdir(parents=True, exist_ok=True)
            _MEDIA_PROFILES_PATH.write_text(
                json.dumps(self._media_profiles, indent=2), encoding="utf-8"
            )
        except OSError as exc:
            logger.debug("media profiles map not saved: %s", exc)

    def _load_r2v_ops(self) -> None:
        """Restore r2v poll handles so an agent restart can still GetSession."""
        if os.environ.get("PYTEST_CURRENT_TEST"):
            return
        try:
            raw = json.loads(_R2V_OPS_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        if not isinstance(raw, dict):
            return
        for op_id, row in raw.items():
            if not isinstance(op_id, str) or not isinstance(row, dict):
                continue
            session = row.get("session")
            project = row.get("project")
            if session:
                self._operation_chat_sessions[op_id] = session
            if project:
                self._operation_projects[op_id] = project
            refs = row.get("refs")
            if isinstance(refs, list):
                self._operation_ref_ids[op_id] = tuple(
                    item for item in refs if isinstance(item, str)
                )
            profile = row.get("profile")
            if profile:
                self._operation_profiles[op_id] = profile
            media = row.get("media")
            if media:
                self._operation_media[op_id] = media
            if row.get("started_at"):
                self._operation_start_time[op_id] = float(row["started_at"])
            if isinstance(row.get("result"), dict):
                self._operation_results[op_id] = row["result"]

    def _ops_snapshot(self) -> dict:
        """operation_id → project/session/media so a restart can still poll."""
        rows = {}
        for op_id in set(self._operation_projects) | set(self._operation_chat_sessions):
            rows[op_id] = {
                "session": self._operation_chat_sessions.get(op_id) or "",
                "project": self._operation_projects.get(op_id) or "",
                "refs": list(self._operation_ref_ids.get(op_id) or ()),
                "profile": self._operation_profiles.get(op_id) or "",
                "media": self._operation_media.get(op_id) or "",
                "started_at": self._operation_start_time.get(op_id),
                "result": self._operation_results.get(op_id),
            }
        return rows

    def _save_r2v_ops(self) -> None:
        if os.environ.get("PYTEST_CURRENT_TEST"):
            return
        try:
            _R2V_OPS_PATH.parent.mkdir(parents=True, exist_ok=True)
            _R2V_OPS_PATH.write_text(
                json.dumps(self._ops_snapshot(), indent=2), encoding="utf-8")
        except OSError as exc:
            logger.debug("r2v op map not saved: %s", exc)

    # ─── High-level API Methods ──────────────────────────────

    def flow_project_id(self, requested: str | None = None) -> str | None:
        """The Flow project to attach a new Flow Kit project to, if any.

        Project creation went with the labs.google tRPC endpoint the migration
        unauthenticated, so on the batch path a project is made once in the
        Flow UI and its uuid supplied here or pinned as FLOW_PROJECT_ID.
        """
        if requested and self._UUID_RE.match(requested):
            return requested
        return FLOW_PROJECT_ID or None

    async def create_project(self, project_title: str, tool_name: str = "PINHOLE") -> dict:
        if not USE_BATCH_RPC:
            return await self._legacy_create_project(project_title, tool_name)
        pid = self.flow_project_id()
        if not pid:
            return {"error": _UNSUPPORTED_CREATE_PROJECT}
        logger.info("Reusing pinned Flow project %s for '%s'", pid[:12], project_title)
        return {"status": 200, "data": {"projectId": pid}}

    async def generate_images(self, prompt: str, project_id: str,
                               aspect_ratio: str = "IMAGE_ASPECT_RATIO_PORTRAIT",
                               user_paygate_tier: str = "PAYGATE_TIER_TWO",
                               character_media_ids: list[str] = None,
                               image_model: str = None) -> dict:
        """Generate image(s).

        ``character_media_ids`` are attached as reference images, which is what
        keeps an entity the same across scenes. Response is shaped like the
        old REST one so the parsers downstream do not have to care which
        transport produced it.
        """
        if not USE_BATCH_RPC:
            return await self._legacy_generate_images(
                prompt, project_id, aspect_ratio, user_paygate_tier, character_media_ids)

        refs = list(character_media_ids or [])

        async def run(pid: str):
            freq = fb.image_request(
                prompt, pid, count=1, aspect=aspect_ratio,
                model=self._batch_image_model(image_model),
                ref_media_ids=refs or None,
            )
            payload = await self._batch_payload(fb.RPC_GEN_IMAGE, freq, fb.CAPTCHA_IMAGE)
            images = fb.read_images(payload)
            if not images:
                return {"status": 502, "error": "Image generation returned no media url"}
            for image in images:
                self._remember_media(image.media_id)
            return {"status": 200, "data": {"media": [_as_media_record(i) for i in images]}}

        try:
            return await self._run_on_profile(
                run, project_id, media_ids=refs, allow_failover=True)
        except Exception as e:
            return _batch_error(e)

    async def edit_image(self, prompt: str, source_media_id: str,
                          project_id: str,
                          aspect_ratio: str = "IMAGE_ASPECT_RATIO_PORTRAIT",
                          user_paygate_tier: str = "PAYGATE_TIER_ONE",
                          character_media_ids: list[str] = None) -> dict:
        """Regenerate from an existing image plus any entity references.

        The REST path had a dedicated base-image input type; the new payload's
        reference slot was captured but a base-image variant of it was not, so
        here the source rides in as the first reference. In practice that
        conditions the result on the source rather than editing it in place —
        good enough for continuation scenes, not identical to the old edit.
        Capturing the real slot is the fix; see docs/CAPTURE.md.
        """
        if not USE_BATCH_RPC:
            return await self._legacy_edit_image(
                prompt, source_media_id, project_id, aspect_ratio,
                user_paygate_tier, character_media_ids)

        refs = [source_media_id] + [
            mid for mid in (character_media_ids or []) if mid != source_media_id
        ]
        return await self.generate_images(
            prompt=prompt, project_id=project_id, aspect_ratio=aspect_ratio,
            user_paygate_tier=user_paygate_tier, character_media_ids=refs,
        )

    async def generate_video(self, start_image_media_id: str | None = None, prompt: str = "",
                              project_id: str = "", scene_id: str = "",
                              aspect_ratio: str = "VIDEO_ASPECT_RATIO_PORTRAIT",
                              end_image_media_id: str = None,
                              user_paygate_tier: str = "PAYGATE_TIER_TWO",
                              *,
                              profile_id: str | None = None) -> dict:
        """Submit i2v (start frame) or t2v (no start frame). Returns operations."""
        start = start_image_media_id or None
        if not USE_BATCH_RPC:
            if not start:
                return {"error": _unsupported(
                    "text-to-video",
                    "the legacy REST path has no t2v capture",
                )}
            return await self._legacy_generate_video(
                start, prompt, project_id, scene_id,
                aspect_ratio, end_image_media_id, user_paygate_tier)

        if not start:
            if end_image_media_id:
                return {"error": "t2v has no start frame; omit end_image_media_id"}

            async def run_t2v(pid: str):
                freq = fb.t2v_request(
                    prompt, pid, aspect=aspect_ratio, model=fb.VIDEO_T2V_MODEL, count=1)
                payload = await self._batch_payload(
                    fb.RPC_GEN_T2V, freq, fb.CAPTCHA_VIDEO, timeout=120)
                operations = fb.read_operations(payload)
                for operation in operations:
                    self._remember_operation(operation.operation_id, pid)
                    submitted_media = fb.submitted_video_media(payload, operation.operation_id, pid)
                    if submitted_media:
                        self._operation_media[operation.operation_id] = submitted_media
                        self._remember_media(submitted_media)
                        self._save_r2v_ops()
                    video_evidence('submit', operation.operation_id, 'handle_received', payload=payload,
                                   project_id=pid, reference_media_ids=[],
                                   profile_hash=fingerprint((_current_route.get() or {}).get('profile_id')))
                return {"status": 200, "data": {
                    "operations": [_as_pending_operation(op.operation_id) for op in operations]
                }}

            try:
                return await self._run_on_profile(
                    run_t2v, project_id, profile_id=profile_id, allow_failover=True,
                    video_submission=True)
            except Exception as e:
                return _batch_error(e)

        if end_image_media_id:
            if not FLOW_ALLOW_DEGRADED:
                return {"error": _unsupported(
                    "start+end frame chaining",
                    "the new payload's end-image slot was never captured",
                )}
            logger.warning(
                "Scene %s: dropping end frame %s — chaining is not on the batch path, "
                "running plain i2v because FLOW_ALLOW_DEGRADED=1",
                str(scene_id)[:12], end_image_media_id[:12])

        gen_type = "start_end_frame_2_video" if end_image_media_id else "frame_2_video"

        async def run_i2v(pid: str):
            freq = fb.video_request(
                prompt, pid, start, aspect=aspect_ratio,
                model=self._batch_video_model(user_paygate_tier, gen_type, aspect_ratio),
            )
            payload = await self._batch_payload(
                fb.RPC_GEN_VIDEO, freq, fb.CAPTCHA_VIDEO, timeout=120)
            operation = fb.read_operation(payload)
            self._remember_operation(operation.operation_id, pid)
            submitted_media = fb.submitted_video_media(payload, operation.operation_id, pid)
            if submitted_media:
                self._operation_media[operation.operation_id] = submitted_media
                self._remember_media(submitted_media)
                self._save_r2v_ops()
            video_evidence('submit', operation.operation_id, 'handle_received', payload=payload,
                           project_id=pid, reference_media_id=start,
                           profile_hash=fingerprint((_current_route.get() or {}).get('profile_id')))
            return {"status": 200, "data": {
                "operations": [_as_pending_operation(operation.operation_id)]
            }}

        try:
            return await self._run_on_profile(
                run_i2v, project_id, profile_id=profile_id, media_ids=[start], allow_failover=True,
                video_submission=True)
        except Exception as e:
            return _batch_error(e)

    async def generate_video_from_references(self, reference_media_ids: list[str],
                                              prompt: str, project_id: str, scene_id: str,
                                              aspect_ratio: str = "VIDEO_ASPECT_RATIO_PORTRAIT",
                                              user_paygate_tier: str = "PAYGATE_TIER_TWO",
                                              *,
                                              profile_id: str | None = None) -> dict:
        """Generate video from multiple reference images (r2v)."""
        if not USE_BATCH_RPC:
            return await self._legacy_generate_video_from_references(
                reference_media_ids, prompt, project_id, scene_id,
                aspect_ratio, user_paygate_tier)

        if not reference_media_ids:
            return {"error": "No reference media_ids for r2v"}

        refs = list(reference_media_ids)

        async def run(pid: str):
            # StreamChat has no model slot. Without this setting the
            # Ingredients agent picks Omni Flash (~15 credits) and stalls on
            # ask_for_permission. Captured Kcr7Ub pins lite_low_priority (free).
            await self._batch_payload(
                fb.RPC_PROJECT_SETTINGS,
                fb.set_video_defaults_request(pid, aspect=aspect_ratio),
                timeout=45,
            )
            # Fresh conversation — a session that already ignored permission
            # keeps picking Omni Flash even after the model pin.
            session = await self._mint_chat_session(pid)
            logger.info("r2v StreamChat session=%s model=%s",
                        session, fb.VIDEO_R2V_MODEL)
            freq = fb.stream_chat_request(
                prompt, pid, refs, session_id=session)
            result = await self.batch_rpc(
                fb.RPC_STREAM_CHAT, freq, fb.CAPTCHA_CHAT,
                timeout=120, path=fb.STREAM_CHAT_PATH,
            )
            if result.get("error"):
                raise fb.FlowBatchError(f"StreamChat: {result['error']}")
            raw = result.get("data") or ""
            if "ask_for_permission" in raw:
                return {"status": 400, "error": "LOW_PRIORITY_ONLY: ask_for_permission; paid generation was not approved"}
            chunks = []
            try:
                chunks = fb.payloads(raw)
                operation = fb.read_stream_chat_operation(
                    chunks if len(chunks) != 1 else chunks[0])
            except fb.FlowBatchError as exc:
                logger.warning("r2v StreamChat parse: %s raw=%s", exc, raw[:1500])
                operation = None
            except Exception:
                logger.error("[DEBUG] r2v StreamChat raw (%d chars): %s",
                             len(raw), raw[:2000])
                raise
            op_id = operation.operation_id if operation else None
            if not fb._valid_uuid(op_id):
                # Ack had no media/chat uuid. Poll GetSession on this
                # conversation — that is how the Flow UI finds the clip.
                if not session or session == fb.CHAT_SESSION_SLOT:
                    raise fb.FlowBatchError("StreamChat response carried no operation")
                logger.warning("r2v StreamChat had no uuid; polling via session %s", session)
                op_id = session
            self._remember_operation(op_id, pid)
            video_evidence('submit', op_id, 'handle_received', payload=chunks,
                           project_id=pid, reference_media_ids=refs, session_id=trace_identifier(session),
                           profile_hash=fingerprint((_current_route.get() or {}).get('profile_id')))
            if session and session != fb.CHAT_SESSION_SLOT:
                self._operation_chat_sessions[op_id] = session
            self._operation_ref_ids[op_id] = tuple(refs)
            if operation and operation.done and operation.operation_id == op_id:
                self._operation_media[op_id] = op_id
            self._save_r2v_ops()
            return {"status": 200, "data": {
                "operations": [_as_pending_operation(op_id)]
            }}

        try:
            return await self._run_on_profile(
                run, project_id, profile_id=profile_id, media_ids=refs, allow_failover=True,
                video_submission=True)
        except Exception as e:
            logger.error("[DEBUG] r2v StreamChat failed: %s", e)
            return _batch_error(e)

    async def upscale_video(self, media_id: str, scene_id: str,
                             aspect_ratio: str = "VIDEO_ASPECT_RATIO_PORTRAIT",
                             resolution: str = "VIDEO_RESOLUTION_4K") -> dict:
        """Upscale a video."""
        if not USE_BATCH_RPC:
            return await self._legacy_upscale_video(media_id, scene_id, aspect_ratio, resolution)
        return {"error": _unsupported(
            "video upscale",
            "no upsampler rpc appears in the new frontend's captures",
        )}

    async def check_video_status(self, operations: list[dict]) -> dict:
        """One poll round for each submitted operation.

        Three signals have to agree before a clip can be downloaded, and they
        arrive out of order:

        * the operation poll says how the job is going — but it can sit at no
          status at all on a job that finished, and a "Media not found."
          complaint on it is survivable rather than fatal;
        * the project listing is what actually gains a media id;
        * the media record serves the poster image first and grows the
          ``/video/`` url in later.

        So an operation only reports SUCCESSFUL once there is a video url.
        Everything short of that is PENDING, and the caller's own poll loop
        owns the timeout.
        """
        if not USE_BATCH_RPC:
            return await self._legacy_check_video_status(operations)

        out = []
        for entry in operations or []:
            op_id = (entry.get("operation") or {}).get("name") or entry.get("name") or ""
            if not op_id:
                out.append({"operation": {}, "status": "MEDIA_GENERATION_STATUS_FAILED",
                            "error": "operation carried no name"})
                continue
            try:
                out.append(await self._poll_batch_operation(op_id))
            except Exception as e:
                # A hiccup on one poll round costs a round, not the job.
                logger.warning("Operation %s poll failed: %s", op_id[:20], e)
                out.append(_as_pending_operation(op_id, error=str(e)))
        return {"status": 200, "data": {"operations": out}}

    async def _poll_batch_operation(self, operation_id: str) -> dict:
        """Coalesce concurrent polls; a browser disconnect must not cancel the poll."""
        clean_id = operation_id.removeprefix("operations/")
        task = self._operation_poll_tasks.get(clean_id)
        if task is None:
            task = asyncio.create_task(self._poll_batch_operation_once(clean_id))
            self._operation_poll_tasks[clean_id] = task
            def finished(done):
                if self._operation_poll_tasks.get(clean_id) is done:
                    self._operation_poll_tasks.pop(clean_id, None)
            task.add_done_callback(finished)
        result = copy.deepcopy(await asyncio.shield(task))
        result.setdefault("operation", {})["name"] = operation_id
        return result

    @traced("video.poll")
    async def _poll_batch_operation_once(self, clean_id: str) -> dict:
        from agent.services.flow_failover import (
            get_failover_target, get_operation_replay,
            update_operation_failover, update_operation_replay,
        )
        if clean_id in self._operation_results:
            return self._operation_results[clean_id]
        # Keep following failovers created before the low-priority fix.
        target = get_failover_target(clean_id)
        active_id = target or clean_id
        if clean_id not in self._operation_start_time:
            replay = get_operation_replay(clean_id) or {}
            self._operation_start_time[clean_id] = float(replay.get("created_at") or time.time())
            if clean_id not in self._operation_projects:
                p_payload = replay.get("payload") or {}
                if isinstance(p_payload, str):
                    try:
                        p_payload = json.loads(p_payload)
                    except Exception:
                        p_payload = {}
                proj = p_payload.get("project_id")
                if proj:
                    self._remember_operation(clean_id, proj)
            if clean_id not in self._operation_profiles and replay.get("worker_id"):
                self._operation_profiles[clean_id] = replay["worker_id"]
        started = self._operation_start_time[clean_id]
        timeout_budget = min(VIDEO_POLL_TIMEOUT + 180, 480) if target else VIDEO_POLL_TIMEOUT
        remaining = timeout_budget - (time.time() - started)
        trace_emit("video.poll.state", operation_id=trace_identifier(clean_id),
                   media_id=trace_identifier(self._operation_media.get(active_id)),
                   profile_hash=fingerprint(self._operation_profiles.get(active_id)),
                   elapsed_s=round(time.time()-started, 3), remaining_s=round(remaining, 3),
                   round=self._operation_polls.get(active_id, 0))
        # Even at the deadline allow one final bounded lookup: the clip may
        # already exist. Transient RPC/worker errors cannot extend the budget.
        async def run(_pid: str):
            return await self._poll_batch_operation_inner(active_id)
        try:
            result = await asyncio.wait_for(self._run_on_profile(
                run, self._operation_projects.get(active_id) or self._operation_projects.get(clean_id) or "",
                operation_id=active_id, allow_failover=False,
            ), timeout=max(15, min(45, remaining)))
        except Exception as exc:
            result = _as_pending_operation(active_id, error=str(exc))
        if "operation" not in result:
            result = _as_pending_operation(active_id, error=result.get("error"))
        result["operation"]["name"] = clean_id
        status = result.get("status", "")

        # If a failover replay is active but still pending, check if the original finished in parallel
        if target and status != "MEDIA_GENERATION_STATUS_SUCCESSFUL":
            try:
                orig_res = await self._poll_batch_operation_inner(clean_id)
                if orig_res.get("status") == "MEDIA_GENERATION_STATUS_SUCCESSFUL":
                    result = orig_res
                    result["operation"]["name"] = clean_id
                    status = "MEDIA_GENERATION_STATUS_SUCCESSFUL"
            except Exception:
                pass

        if status not in ("MEDIA_GENERATION_STATUS_SUCCESSFUL", "MEDIA_GENERATION_STATUS_FAILED"):
            elapsed = time.time() - started
            # Smart late-replay: if an operation has been rendering for >= SMART_REPLAY_TIMEOUT
            # and has not yet been replayed, trigger a secondary dispatch to rescue the stalled job
            if (
                SMART_REPLAY_ENABLED
                and elapsed >= SMART_REPLAY_TIMEOUT
                and not target
                and clean_id not in self._replay_in_progress
            ):
                self._replay_in_progress.add(clean_id)
                asyncio.create_task(self._maybe_trigger_smart_replay(clean_id))

            if elapsed >= timeout_budget:
                result = self._video_failure(clean_id, "upstream_timeout",
                    f"upstream_timeout: low-priority video has no result after {int(elapsed)}s; "
                    "no paid model or duplicate generation was submitted")
                logger.warning("Operation %s low-priority render timeout after %.1fs", clean_id[:20], elapsed)
        status = result.get("status", "")
        if status in ("MEDIA_GENERATION_STATUS_SUCCESSFUL", "MEDIA_GENERATION_STATUS_FAILED"):
            self._operation_results[clean_id] = result
            terminal = "COMPLETED" if status.endswith("SUCCESSFUL") else "FAILED"
            update_operation_replay(clean_id, status=terminal)
            if target:
                update_operation_failover(clean_id, status=terminal)
            self._save_r2v_ops()
        return result

    def _pick_failover_worker(self, exclude: str = "") -> str | None:
        """Best available connected worker that is not ``exclude``."""
        try:
            _pin, routes = self._profile_candidates()
        except Exception:
            return None
        ex = (exclude or "").casefold()
        for route in routes:
            pid = route.get("profile_id") or ""
            if pid and pid.casefold() != ex and route.get("available") and route.get("project_id"):
                return pid
        return None

    async def _reupload_media_to_worker(self, media_ids: list[str], target_worker: str) -> list[str] | None:
        """Move still-valid source images onto another account.

        Flow media handles are account-scoped, so a cross-account replay must
        move the pixels, not the id. Returns new media ids, or None when any
        source is no longer fetchable.
        """
        moved: list[str] = []
        for mid in media_ids:
            try:
                res = await self.get_media(mid)
                fife = ((res.get("data") or {}).get("image") or {}).get("fifeUrl")
                if not fife:
                    return None
                raw, mime = await asyncio.to_thread(_fetch_url_bytes, fife)
                up = await self.upload_image(
                    base64.b64encode(raw).decode("ascii"), mime,
                    profile_id=target_worker,
                )
                new_id = ((up.get("data") or {}).get("media") or {}).get("name") or up.get("_mediaId")
                if not new_id:
                    return None
                moved.append(new_id)
            except Exception as exc:
                logger.warning("Failover re-upload of %s to %s failed: %s", mid[:12], target_worker, exc)
                return None
        return moved

    async def _maybe_trigger_smart_replay(self, clean_id: str) -> None:
        """Re-dispatch a stalled operation, preferring a different worker/account.

        The original op stays pinned to its worker; the rescue attempt is a
        parallel submit. Media handles are account-scoped, so cross-account
        i2v/r2v re-uploads still-valid source images to the target nick first.
        Falls back to same-worker dispatch when no other worker is usable or
        the source media can no longer be fetched.
        """
        from agent.services.flow_failover import (
            get_operation_replay, record_operation_failover, get_failover_target
        )
        if get_failover_target(clean_id):
            return
        replay = get_operation_replay(clean_id)
        if not replay or replay.get("status") not in ("PENDING", None):
            return
        payload = replay.get("payload") or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                return
        req_type = replay.get("request_type") or "generate_video"
        orig_worker = replay.get("worker_id") or ""
        prompt = payload.get("prompt")
        if not prompt:
            return

        target_worker = self._pick_failover_worker(exclude=orig_worker)
        aspect = payload.get("aspect_ratio") or "VIDEO_ASPECT_RATIO_LANDSCAPE"
        tier = payload.get("user_paygate_tier") or "PAYGATE_TIER_TWO"
        scene = payload.get("scene_id") or ""
        logger.info(
            "Triggering smart late-replay for stalled operation %s (worker: %s -> %s) after %ds",
            clean_id[:12], orig_worker, target_worker or "same", SMART_REPLAY_TIMEOUT
        )

        try:
            if req_type == "generate_video_refs":
                refs = payload.get("reference_media_ids") or []
                if not refs:
                    return
                new_refs = (
                    await self._reupload_media_to_worker(refs[:3], target_worker)
                    if target_worker else None
                )
                if new_refs:
                    res = await self.generate_video_from_references(
                        new_refs, prompt, "", scene,
                        aspect_ratio=aspect, user_paygate_tier=tier,
                        profile_id=target_worker,
                    )
                else:
                    res = await self.generate_video_from_references(
                        refs, prompt, payload.get("project_id") or "", scene,
                        aspect_ratio=aspect, user_paygate_tier=tier,
                    )
            else:
                start = payload.get("start_image_media_id")
                end = payload.get("end_image_media_id")
                new_start = None
                if target_worker and start:
                    moved = await self._reupload_media_to_worker([start], target_worker)
                    new_start = moved[0] if moved else None
                if target_worker and (new_start or not start):
                    # Cross-account: t2v submits directly; i2v uses the
                    # re-uploaded start frame on the target worker's project.
                    # End-frame chaining cannot cross accounts (media-scoped).
                    res = await self.generate_video(
                        new_start, prompt, "", scene,
                        aspect_ratio=aspect, user_paygate_tier=tier,
                        profile_id=target_worker,
                    )
                else:
                    res = await self.generate_video(
                        start, prompt, payload.get("project_id") or "", scene,
                        aspect_ratio=aspect, end_image_media_id=end,
                        user_paygate_tier=tier,
                    )
        except Exception as exc:
            logger.warning("Smart replay failed to dispatch for %s: %s", clean_id[:12], exc)
            return

        # Extract new operation id from res
        new_ops = (res.get("data") or {}).get("operations") if isinstance(res.get("data"), dict) else res.get("operations")
        if isinstance(new_ops, list) and new_ops:
            first = new_ops[0]
            new_op_id = ""
            if isinstance(first, dict):
                new_op_id = (first.get("operation") or {}).get("name") or first.get("name") or ""
            elif isinstance(first, str):
                new_op_id = first
            if new_op_id:
                clean_new = new_op_id.removeprefix("operations/")
                profiles = getattr(self, "_operation_profiles", {})
                new_worker = profiles.get(clean_new) or profiles.get(new_op_id) or ""
                record_operation_failover(
                    original_op_id=clean_id,
                    new_op_id=clean_new,
                    original_worker_id=orig_worker,
                    failover_worker_id=new_worker,
                    status="IN_PROGRESS",
                )
                logger.info(
                    "Smart late-replay mapped successfully: %s -> %s (worker: %s -> %s)",
                    clean_id[:12], clean_new[:12], orig_worker, new_worker
                )

    @staticmethod
    def _video_failure(operation_id: str, code: str, message: str) -> dict:
        return {"operation": {"name": operation_id},
                "status": "MEDIA_GENERATION_STATUS_FAILED",
                "error_code": code, "error": message}

    def _video_complaint_failure(self, operation_id: str, complaint: str | None):
        if not complaint:
            return None
        if "ask_for_permission" in complaint or "LOW_PRIORITY_ONLY" in complaint:
            return self._video_failure(operation_id, "low_priority_only", complaint)
        if "UNSAFE_GENERATION" in complaint:
            return self._video_failure(operation_id, "content_policy_violation", complaint)
        # A missing media row is normal while low-priority renders are queued.
        # Explicit upstream rejection is terminal; network hiccups are not.
        if "PUBLIC_ERROR" in complaint:
            return self._video_failure(operation_id, "upstream_rejected", complaint)
        return None

    async def _poll_batch_operation_inner(
        self, operation_id: str, original_op_id: str | None = None,
        has_explicit_start: bool | None = None,
    ) -> dict:
        operation_id = operation_id.removeprefix("operations/")
        rounds = self._operation_polls.get(operation_id, 0) + 1
        self._operation_polls[operation_id] = rounds
        media_id = self._operation_media.get(operation_id)
        complaint = self._operation_complaints.get(operation_id)
        direct = None
        cached_media_id = media_id
        # A confirmed media handle can finish even when the large listing fails.
        # Check it before the slower discovery path can consume the poll budget.
        if media_id:
            try:
                direct = await self._batch_media_urls(media_id)
                if direct.video:
                    return {"operation": {"name": operation_id,
                            "metadata": {"video": {"mediaId": media_id, "fifeUrl": direct.video}}},
                            "status": "MEDIA_GENERATION_STATUS_SUCCESSFUL"}
            except Exception:
                pass
        if not media_id or rounds % 3 == 0:
            refreshed, complaint = await self._find_operation_media(operation_id)
            failure = self._video_complaint_failure(operation_id, complaint)
            if failure:
                return failure
            if refreshed:
                media_id = refreshed
                self._operation_media[operation_id] = media_id
                self._remember_media(media_id)
                self._save_r2v_ops()
        if not media_id:
            return _as_pending_operation(operation_id, error=complaint)
        urls = direct if cached_media_id == media_id else None
        if urls is None:
            try:
                urls = await self._batch_media_urls(media_id)
            except Exception as exc:
                complaint = str(exc)
        if (not urls or not urls.video) and operation_id in self._operation_chat_sessions:
            refreshed = await self._media_id_from_chat_session(
                operation_id, self._operation_projects.get(operation_id))
            failure = self._video_complaint_failure(
                operation_id, self._operation_complaints.get(operation_id))
            if failure:
                return failure
            if refreshed and refreshed != media_id:
                media_id = refreshed
                self._operation_media[operation_id] = media_id
                self._remember_media(media_id)
                self._save_r2v_ops()
                try:
                    urls = await self._batch_media_urls(media_id)
                except Exception as exc:
                    complaint = str(exc)
        if not urls or not urls.video:
            return _as_pending_operation(operation_id, error=complaint, media_id=media_id)
        return {
            "operation": {"name": operation_id,
                          "metadata": {"video": {"mediaId": media_id, "fifeUrl": urls.video}}},
            "status": "MEDIA_GENERATION_STATUS_SUCCESSFUL",
        }

    async def _find_operation_media(self, operation_id: str) -> tuple[str | None, str | None]:
        """Ask the operation how it is going, then the listing where its media is.

        The listing is the authority — the poll has been seen to never report a
        finished job the listing already knows about — but it is also the
        expensive call, so it is only consulted when the poll says something
        happened, when the poll is unreadable, or every third round regardless.
        """
        clean_op_id = operation_id.removeprefix("operations/") if isinstance(operation_id, str) else operation_id
        rounds = self._operation_polls.get(clean_op_id, 0)
        project_id = (
            self._operation_projects.get(clean_op_id)
            or self._operation_projects.get(operation_id)
            or FLOW_PROJECT_ID
        )
        complaint = None
        worth_looking = rounds % 3 == 0
        try:
            operation_payload = await self._batch_payload(
                fb.RPC_OPERATION, fb.operation_request(operation_id), timeout=60)
            operation = fb.read_operation(operation_payload)
            video_evidence('operation', operation_id,
                           f'{operation.status}:{fingerprint(operation.error or "")}',
                           payload=operation_payload, project_id=trace_identifier(project_id),
                           reported_project_id=trace_identifier(operation.project_id),
                           project_matches=not operation.project_id or operation.project_id == project_id,
                           profile_hash=fingerprint((_current_route.get() or {}).get('profile_id')),
                           diagnostic=trace_summary(operation.error))
            complaint = operation.error
            project_id = operation.project_id or project_id
            if project_id:
                self._remember_operation(operation_id, project_id)
            worth_looking = worth_looking or operation.done or operation.complained
        except Exception as e:
            # An operation that has decayed to a bare id still shows up in the
            # listing, so a failed poll is a reason to look there, not to stop.
            logger.debug("Operation %s poll unreadable (%s), trying the listing",
                         operation_id[:20], e)
            worth_looking = True

        if operation_id in self._operation_chat_sessions:
            # Chat-message uuids are not operation rows. GetSession is how
            # the clip media_id shows up, including while it is still queued.
            worth_looking = True
        if not worth_looking:
            return None, complaint
        if not project_id:
            return None, "no project id for the listing lookup"
        # r2v: the submit uuid is a chat-message id. GetSession names the
        # clip as generate_video_with_references.media_id (often queued,
        # before a /video/ url or CAE listing row exists).
        if operation_id in self._operation_chat_sessions:
            media_id = await self._media_id_from_chat_session(operation_id, project_id)
            if media_id:
                return media_id, complaint
            if getattr(self, "_operation_complaints", {}).get(operation_id):
                return None, self._operation_complaints[operation_id]
            media_id = await self._media_id_from_r2v_listing(operation_id, project_id)
            if media_id:
                return media_id, complaint
        else:
            media_id = await self._media_id_for(operation_id, project_id)
            if media_id:
                return media_id, complaint
        # StreamChat r2v keys the listing row by the media id. i2v
        # slot-matching misses that shape; as29s on the id still serves it.
        try:
            urls = await self._batch_media_urls(operation_id)
            if urls.video or urls.image:
                return operation_id, complaint
        except Exception:
            pass
        return None, complaint

    async def _media_id_from_chat_session(
        self, operation_id: str, project_id: str | None,
    ) -> str | None:
        """Resolve r2v media from GetSession (GN0Bre) on the bound conversation.

        Only used for operations StreamChat submitted — t2v/i2v listing rows
        are keyed by the operation id, and GetSession would otherwise attach
        an unrelated Ingredients clip.

        The transcript names the clip as ``media_id`` while status is still
        ``queued``. The listing row is keyed by workflow_id, not the
        chat-message uuid, so waiting for a CAE match or a ``/video/`` url
        here never binds it. Return that id; the poller as29s until the
        CDN path appears.
        """
        session = self._operation_chat_sessions.get(operation_id)
        if not session or session == fb.CHAT_SESSION_SLOT:
            return None
        try:
            result = await self.batch_rpc(
                fb.RPC_CHAT_SESSION, fb.get_session_request(session), timeout=60,
            )
            if result.get("error"):
                raise fb.FlowBatchError(str(result["error"]))
            raw = result.get("data") or ""
            if not os.environ.get("PYTEST_CURRENT_TEST"):
                try:
                    _GETSESSION_DUMP.parent.mkdir(parents=True, exist_ok=True)
                    _GETSESSION_DUMP.write_text(raw[:200000], encoding="utf-8")
                except OSError:
                    pass
            try:
                blobs: list = fb.payloads(raw)
            except fb.FlowBatchError as exc:
                logger.warning("GetSession parse failed: %s raw_len=%d", exc, len(raw))
                blobs = []
        except Exception as exc:
            logger.warning("GetSession %s failed: %s", session[:12], exc)
            return None

        if "ask_for_permission" in raw:
            logger.warning("GetSession %s blocked paid-model confirmation for op %s",
                           session[:12], operation_id[:12])
            self._operation_complaints[operation_id] = (
                "LOW_PRIORITY_ONLY: ask_for_permission; paid generation was not approved"
            )
            return None

        exclude = {
            operation_id, session, project_id or "",
            *self._operation_ref_ids.get(operation_id, ()),
        }
        skip = {item.lower() for item in exclude if item}
        video_in_raw = "/video/" in raw.replace("\\/", "/")
        video_ids = fb.find_video_media_ids(blobs, exclude=exclude)
        if not video_ids:
            video_ids = [
                mid for mid in fb.video_media_ids_in_text(raw)
                if mid.lower() not in skip
            ]
        if video_ids:
            chosen = video_ids[-1]
            logger.info("r2v GetSession video %s for op %s", chosen[:12], operation_id[:12])
            self._operation_media[operation_id] = chosen
            self._save_r2v_ops()
            return chosen
        session_media = fb.find_r2v_session_media_ids(blobs, exclude=exclude)
        if not session_media:
            session_media = fb.find_r2v_session_media_ids(raw, exclude=exclude)
        if session_media:
            chosen = session_media[-1]
            logger.info("r2v GetSession media_id %s for op %s",
                        chosen[:12], operation_id[:12])
            self._operation_media[operation_id] = chosen
            self._save_r2v_ops()
            return chosen
        leftover = fb.collect_uuids([blobs, raw], exclude=exclude)
        candidates = fb.find_stream_chat_media_ids(blobs, exclude=exclude)[:_R2V_AS29S_CAP]
        for media_id in candidates:
            try:
                urls = await self._batch_media_urls(media_id)
            except Exception:
                continue
            if urls.video:
                logger.info("r2v GetSession media %s for op %s", media_id[:12], operation_id[:12])
                self._operation_media[operation_id] = media_id
                self._save_r2v_ops()
                return media_id
        logger.warning(
            "GetSession %s no r2v media for %s raw_len=%d video_in_raw=%s "
            "uuids=%d candidates=%d",
            session[:12], operation_id[:12], len(raw), video_in_raw,
            len(leftover), len(candidates),
        )
        return None

    async def _media_id_from_r2v_listing(
        self, operation_id: str, project_id: str,
    ) -> str | None:
        """Find the StreamChat clip in the project listing.

        Do not window around the chat-message uuid — that string is not in
        the row. The row is ``[mediaId, projectId, sceneId, "CAE"]``. t2v/i2v
        listing entries use a different shape, so CAE rows here are r2v.
        """
        result = await self.batch_rpc(
            fb.RPC_PROJECT_MEDIA, fb.project_media_request(project_id),
            timeout=120,
        )
        if result.get("error"):
            logger.warning("r2v listing: %s", result["error"])
            return None
        raw = result.get("data") or ""
        if not os.environ.get("PYTEST_CURRENT_TEST"):
            try:
                _LISTING_DUMP.parent.mkdir(parents=True, exist_ok=True)
                _LISTING_DUMP.write_text(
                    f"len={len(raw)}\n" + raw[:4000] + "\n---tail---\n" + raw[-4000:],
                    encoding="utf-8",
                )
            except OSError:
                pass
        skip = {
            operation_id, project_id,
            self._operation_chat_sessions.get(operation_id) or "",
            *self._operation_ref_ids.get(operation_id, ()),
            *self._operation_media.values(),
        }
        candidates = fb.find_r2v_media_ids_in_text(
            raw, project_id, operation_id=operation_id, exclude=skip)
        if not candidates:
            try:
                blobs = fb.payloads(raw)
            except fb.FlowBatchError:
                blobs = []
            # Parsed CAE rows still have to name this chat/operation in slot 2.
            for media_id in fb.find_stream_chat_media_ids(blobs, exclude=skip):
                if media_id.lower() == operation_id.lower():
                    candidates.append(media_id)
        logger.info(
            "r2v listing op=%s raw_len=%d cae=%d",
            operation_id[:12], len(raw), len(candidates),
        )
        for media_id in reversed(candidates[-_R2V_AS29S_CAP:]):
            try:
                urls = await self._batch_media_urls(media_id)
            except Exception:
                continue
            if urls.video:
                logger.info(
                    "r2v listing media %s for op %s", media_id[:12], operation_id[:12])
                self._operation_media[operation_id] = media_id
                self._save_r2v_ops()
                return media_id
        return None

    async def _media_id_for(self, operation_id: str, project_id: str) -> str | None:
        """Find an operation's media id in the project listing.

        Asks the extension for an 800-byte window around the operation id
        rather than the whole listing — that payload is past 17 MB and grows
        with every generation, so anything that ships it whole gets truncated
        and loses roughly half of all lookups.
        """
        result = await self.batch_rpc(
            fb.RPC_PROJECT_MEDIA, fb.project_media_request(project_id),
            match=operation_id, timeout=120,
        )
        if result.get("error"):
            raise fb.FlowBatchError(f"{fb.RPC_PROJECT_MEDIA}: {result['error']}")
        raw = result.get("data") or ""
        media_id = fb.find_media_id_in_text(raw, operation_id)
        if not media_id and raw.lstrip().startswith(")]}"):
            # an extension that cannot filter hands back the whole envelope
            try:
                media_id = fb.find_media_id(
                    fb.first_payload(raw, fb.RPC_PROJECT_MEDIA), operation_id)
            except (fb.FlowBatchError, fb.RpcError, json.JSONDecodeError):
                media_id = None
        video_evidence('listing_binding', operation_id, media_id or 'missing',
                       project_id=trace_identifier(project_id), media_id=trace_identifier(media_id),
                       operation_present=operation_id in raw,
                       profile_hash=fingerprint((_current_route.get() or {}).get('profile_id')))
        return media_id

    async def _batch_media_urls(self, media_id: str) -> "fb.MediaUrls":
        try:
            payload = await self._batch_payload(
                fb.RPC_MEDIA, fb.media_request(media_id), timeout=60)
            urls = fb.read_media_urls(payload, media_id)
            video_evidence('media', media_id, 'video' if urls.video else 'image_only' if urls.image else 'no_urls',
                           payload=payload, profile_hash=fingerprint((_current_route.get() or {}).get('profile_id')))
            return urls
        except fb.RpcError as e:
            codes = [n for n in e.detail if type(n) is int and 0 <= n <= 16] if isinstance(e.detail, list) else []
            video_evidence('media', media_id, 'rpc_error:' + ','.join(map(str,codes)),
                           payload=e.detail, rpc_id=e.rpcid, rpc_codes=codes,
                           profile_hash=fingerprint((_current_route.get() or {}).get('profile_id')))
            if "[5]" in str(e):
                return fb.MediaUrls(media_id=media_id, video=None, image=None)
            raise

    async def get_credits(self) -> dict:
        """Get user credits and tier.

        The new frontend has no captured credits rpc, and the tier no longer
        selects a model — aspect is its own slot and the model names are
        fixed — so on the batch path this answers with the configured default
        rather than pretending to know.
        """
        if not USE_BATCH_RPC:
            return await self._legacy_get_credits()
        return {"status": 200, "data": {
            "userPaygateTier": DEFAULT_PAYGATE_TIER,
            "note": "batchexecute path: tier is configured (DEFAULT_PAYGATE_TIER), not fetched",
        }}

    async def validate_media_id(self, media_id: str) -> bool:
        """Check if a mediaId is still valid."""
        result = await self.get_media(media_id)
        status = result.get("status", 500)
        return isinstance(status, int) and status == 200

    async def get_media(self, media_id: str) -> dict:
        """Fetch a media record, which is where a fresh signed url lives."""
        if not USE_BATCH_RPC:
            return await self._legacy_get_media(media_id)
        async def run(_pid: str):
            urls = await self._batch_media_urls(media_id)
            if not urls.video and not urls.image:
                return {"status": 404, "error": f"No urls for media {media_id}"}
            data: dict = {}
            if urls.video:
                data["video"] = {"fifeUrl": urls.video}
            if urls.image:
                data["image"] = {"fifeUrl": urls.image}
            return {"status": 200, "data": data}

        try:
            return await self._run_on_profile(
                run, "", media_ids=[media_id], allow_failover=False)
        except Exception as e:
            return _batch_error(e)

    async def upload_image(self, image_base64: str, mime_type: str = "image/jpeg",
                            project_id: str = "", file_name: str = "image.jpg",
                            *, profile_id: str | None = None,
                            reference_media_id: str | None = None) -> dict:
        """Upload an image into the project so it can be used as a reference."""
        if reference_media_id:
            owner = self._media_profiles.get(reference_media_id)
            if not owner or not self._UUID_RE.fullmatch(reference_media_id):
                return {"status": 400, "error": "REFERENCE_BINDING_UNKNOWN: upload the first reference again",
                        "retryable": False}
            if profile_id and profile_id.casefold() != owner.casefold():
                return {"status": 400, "error": "MEDIA_PROFILE_MISMATCH", "retryable": False}
            profile_id = owner
        if not USE_BATCH_RPC:
            return await self._legacy_upload_image(image_base64, mime_type, project_id, file_name)
        async def run(pid: str):
            payload = await self._batch_payload(
                fb.RPC_UPLOAD_IMAGE,
                fb.upload_request(image_base64, pid, mime_type, file_name),
                fb.CAPTCHA_IMAGE, timeout=120,
            )
            media_id = fb.read_uploaded_media_id(payload)
            self._remember_media(media_id, profile_id=profile_id)
            return {"status": 200, "data": {"media": {"name": media_id}}, "_mediaId": media_id}

        try:
            return await self._run_on_profile(
                run, project_id, profile_id=profile_id, allow_failover=not bool(reference_media_id))
        except Exception as e:
            return _batch_error(e)

    async def vision_analyze(
        self,
        prompt: str,
        images: list[tuple[str, str]] | list[str] | str | None = None,
        system_instruction: str = "",
        session_uuid: str | None = None,
        project_id: str = "",
        timeout: float = 120.0,
    ) -> dict:
        """Call Gemini 3 Flash Vision via Google Flow's internal RPC agJzFb.

        Zero-cost multimodal analysis and scriptwriting using Flow's signed-in session.
        """
        async def run(pid: str):
            chosen_session = session_uuid or self._chat_session_for_route()
            if not chosen_session:
                chosen_session = fb.CHAT_SESSION_SLOT
            freq = fb.vision_analyze_request(
                prompt=prompt,
                images=images,
                system_instruction=system_instruction,
                session_uuid=chosen_session,
            )
            try:
                from pathlib import Path
                Path("/home/pc/flowkit/.scratch").mkdir(parents=True, exist_ok=True)
                Path("/home/pc/flowkit/.scratch/agjzfb_freq.json").write_text(freq, encoding="utf-8")
            except Exception:
                pass
            path = f"/_/AiSandboxAngularFrontend/data/batchexecute?rpcids={fb.RPC_VISION_ANALYZE}"
            if pid:
                path += f"&source-path=%2Fproject%2F{pid}"
            payload = await self._batch_payload(
                fb.RPC_VISION_ANALYZE, freq,
                captcha_action=fb.CAPTCHA_CHAT,
                timeout=timeout,
                path=path,
            )
            analysis = fb.read_vision_analysis(payload)
            return {"status": 200, "data": analysis}

        try:
            return await self._run_on_profile(
                run, project_id, allow_failover=True,
            )
        except Exception as e:
            return _batch_error(e)

    # ─── Legacy REST methods (aisandbox-pa, pre-migration) ───

    async def _legacy_create_project(self, project_title: str, tool_name: str = "PINHOLE") -> dict:
        """Create a project on Google Flow via tRPC endpoint.

        Returns the full response including projectId.
        """
        url = "https://labs.google/fx/api/trpc/project.createProject"
        body = {"json": {"projectTitle": project_title, "toolName": tool_name}}

        return await self._send("trpc_request", {
            "url": url,
            "method": "POST",
            "headers": {
                "content-type": "application/json",
                "accept": "*/*",
            },
            "body": body,
        }, timeout=30)

    async def _legacy_generate_images(self, prompt: str, project_id: str,
                               aspect_ratio: str = "IMAGE_ASPECT_RATIO_PORTRAIT",
                               user_paygate_tier: str = "PAYGATE_TIER_TWO",
                               character_media_ids: list[str] = None) -> dict:
        """Generate image(s).

        If character_media_ids is provided, uses edit_image flow (batchGenerateImages
        with imageInputs) — same endpoint, but includes character references.
        Without characters, uses plain generate_images.

        Response structure:
            data.media[].name = mediaId (used for video gen)
        """
        ts = int(time.time() * 1000)
        ctx = self._client_context(project_id, user_paygate_tier)

        request_item = {
            "clientContext": {**ctx, "sessionId": f";{ts}"},
            "seed": ts % 1000000,
            "structuredPrompt": {"parts": [{"text": prompt}]},
            "imageAspectRatio": aspect_ratio,
            "imageModelName": IMAGE_MODELS["NANO_BANANA_PRO"],
        }

        # Add character references if provided (edit_image flow)
        if character_media_ids:
            request_item["imageInputs"] = [
                {"name": mid, "imageInputType": "IMAGE_INPUT_TYPE_REFERENCE"}
                for mid in character_media_ids
            ]

        batch_id = f"{uuid.uuid4()}" if character_media_ids else None
        body = {
            "clientContext": ctx,
            "requests": [request_item],
        }
        if batch_id:
            body["mediaGenerationContext"] = {"batchId": batch_id}
            body["useNewMedia"] = True

        url = self._build_url("generate_images", project_id=project_id)
        return await self._send("api_request", {
            "url": url,
            "method": "POST",
            "headers": random_headers(),
            "body": body,
            "captchaAction": "IMAGE_GENERATION",
        })

    async def _legacy_edit_image(self, prompt: str, source_media_id: str,
                          project_id: str,
                          aspect_ratio: str = "IMAGE_ASPECT_RATIO_PORTRAIT",
                          user_paygate_tier: str = "PAYGATE_TIER_ONE",
                          character_media_ids: list[str] = None) -> dict:
        """Edit an existing image using IMAGE_INPUT_TYPE_BASE_IMAGE.

        If character_media_ids is provided, appends them as IMAGE_INPUT_TYPE_REFERENCE
        after the base image. Order: [base_image, char_A, char_B, ...].
        This helps Google Flow detect characters for consistent edits.
        """
        ts = int(time.time() * 1000)
        ctx = self._client_context(project_id, user_paygate_tier)

        image_inputs = [
            {"name": source_media_id, "imageInputType": "IMAGE_INPUT_TYPE_BASE_IMAGE"}
        ]
        if character_media_ids:
            for mid in character_media_ids:
                image_inputs.append({"name": mid, "imageInputType": "IMAGE_INPUT_TYPE_REFERENCE"})

        request_item = {
            "clientContext": {**ctx, "sessionId": f";{ts}"},
            "seed": ts % 1000000,
            "structuredPrompt": {"parts": [{"text": prompt}]},
            "imageAspectRatio": aspect_ratio,
            "imageModelName": IMAGE_MODELS["NANO_BANANA_PRO"],
            "imageInputs": image_inputs,
        }

        body = {
            "clientContext": ctx,
            "mediaGenerationContext": {"batchId": f"{uuid.uuid4()}"},
            "useNewMedia": True,
            "requests": [request_item],
        }

        url = self._build_url("generate_images", project_id=project_id)
        return await self._send("api_request", {
            "url": url,
            "method": "POST",
            "headers": random_headers(),
            "body": body,
            "captchaAction": "IMAGE_GENERATION",
        })

    async def _legacy_generate_video(self, start_image_media_id: str, prompt: str,
                              project_id: str, scene_id: str,
                              aspect_ratio: str = "VIDEO_ASPECT_RATIO_PORTRAIT",
                              end_image_media_id: str = None,
                              user_paygate_tier: str = "PAYGATE_TIER_TWO") -> dict:
        """Generate video from start image (i2v).

        Two sub-types:
        - frame_2_video (i2v): startImage only
        - start_end_frame_2_video (i2v_fl): startImage + endImage (for scene chaining)
        """
        gen_type = "start_end_frame_2_video" if end_image_media_id else "frame_2_video"
        model_key = VIDEO_MODELS.get(user_paygate_tier, {}).get(gen_type, {}).get(aspect_ratio)

        if not model_key:
            return {"error": f"No model for tier={user_paygate_tier} type={gen_type} ratio={aspect_ratio}"}

        request = {
            "aspectRatio": aspect_ratio,
            "seed": int(time.time()) % 10000,
            "textInput": {"structuredPrompt": {"parts": [{"text": prompt}]}},
            "videoModelKey": model_key,
            "startImage": {"mediaId": start_image_media_id},
            "metadata": {"sceneId": scene_id},
        }

        if end_image_media_id:
            request["endImage"] = {"mediaId": end_image_media_id}

        endpoint_key = "generate_video_start_end" if end_image_media_id else "generate_video"
        body = {
            "mediaGenerationContext": {"batchId": f"{uuid.uuid4()}"},
            "clientContext": self._client_context(project_id, user_paygate_tier),
            "requests": [request],
            "useV2ModelConfig": True,
        }

        url = self._build_url(endpoint_key)
        return await self._send("api_request", {
            "url": url,
            "method": "POST",
            "headers": random_headers(),
            "body": body,
            "captchaAction": "VIDEO_GENERATION",
        }, timeout=60)  # Submit only — polling is separate

    async def _legacy_generate_video_from_references(self, reference_media_ids: list[str],
                                              prompt: str, project_id: str, scene_id: str,
                                              aspect_ratio: str = "VIDEO_ASPECT_RATIO_PORTRAIT",
                                              user_paygate_tier: str = "PAYGATE_TIER_TWO") -> dict:
        """Generate video from multiple reference images (r2v).

        Uses referenceImages instead of startImage — the model composes
        a video from all provided reference character images.

        Args:
            reference_media_ids: List of character media_ids (from uploadImage)
        """
        gen_type = "reference_frame_2_video"
        model_key = VIDEO_MODELS.get(user_paygate_tier, {}).get(gen_type, {}).get(aspect_ratio)

        if not model_key:
            return {"error": f"No model for tier={user_paygate_tier} type={gen_type} ratio={aspect_ratio}"}

        request = {
            "aspectRatio": aspect_ratio,
            "seed": int(time.time()) % 10000,
            "textInput": {"structuredPrompt": {"parts": [{"text": prompt}]}},
            "videoModelKey": model_key,
            "referenceImages": [
                {"mediaId": mid, "imageUsageType": "IMAGE_USAGE_TYPE_ASSET"}
                for mid in reference_media_ids
            ],
            "metadata": {},
        }

        body = {
            "mediaGenerationContext": {"batchId": f"{uuid.uuid4()}"},
            "clientContext": self._client_context(project_id, user_paygate_tier),
            "requests": [request],
            "useV2ModelConfig": True,
        }

        url = self._build_url("generate_video_references")
        return await self._send("api_request", {
            "url": url,
            "method": "POST",
            "headers": random_headers(),
            "body": body,
            "captchaAction": "VIDEO_GENERATION",
        }, timeout=60)

    async def _legacy_upscale_video(self, media_id: str, scene_id: str,
                             aspect_ratio: str = "VIDEO_ASPECT_RATIO_PORTRAIT",
                             resolution: str = "VIDEO_RESOLUTION_4K") -> dict:
        """Upscale a video."""
        model_key = UPSCALE_MODELS.get(resolution, "veo_3_1_upsampler_4k")

        body = {
            "clientContext": {
                "sessionId": f";{int(time.time() * 1000)}",
                "recaptchaContext": {
                    "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB",
                    "token": "",
                },
            },
            "requests": [{
                "aspectRatio": aspect_ratio,
                "resolution": resolution,
                "seed": int(time.time()) % 100000,
                "metadata": {"sceneId": scene_id},
                "videoInput": {"mediaId": media_id},
                "videoModelKey": model_key,
            }],
        }

        url = self._build_url("upscale_video")
        return await self._send("api_request", {
            "url": url,
            "method": "POST",
            "headers": random_headers(),
            "body": body,
            "captchaAction": "VIDEO_GENERATION",
        }, timeout=60)

    async def _legacy_check_video_status(self, operations: list[dict]) -> dict:
        """Check status of video generation operations."""
        body = {"operations": operations}
        url = self._build_url("check_video_status")
        return await self._send("api_request", {
            "url": url,
            "method": "POST",
            "headers": random_headers(),
            "body": body,
        }, timeout=30)  # No captcha needed

    async def _legacy_get_credits(self) -> dict:
        """Get user credits and tier."""
        url = self._build_url("get_credits")
        return await self._send("api_request", {
            "url": url,
            "method": "GET",
            "headers": random_headers(),
        }, timeout=15)

    async def _legacy_get_media(self, media_id: str) -> dict:
        """Fetch media metadata from Google Flow.

        Returns the raw API response which contains a fresh signed URL
        in data.fifeUrl or data.servingUri.
        """
        url = f"{GOOGLE_FLOW_API}/v1/media/{media_id}?key={GOOGLE_API_KEY}&clientContext.tool=PINHOLE"
        return await self._send("api_request", {
            "url": url,
            "method": "GET",
            "headers": random_headers(),
        }, timeout=15)

    async def _legacy_upload_image(self, image_base64: str, mime_type: str = "image/jpeg",
                            project_id: str = "", file_name: str = "image.jpg") -> dict:
        """Upload an image for use as start/end frame.

        Uses /v1/flow/uploadImage endpoint.
        Response: {media: {name: "uuid", ...}, workflow: {...}}
        We store media.name as the mediaId for video generation.
        """
        body = {
            "clientContext": {
                "projectId": project_id,
                "tool": "PINHOLE",
            },
            "fileName": file_name,
            "imageBytes": image_base64,
            "isHidden": False,
            "isUserUploaded": True,
            "mimeType": mime_type,
        }

        url = self._build_url("upload_image")
        result = await self._send("api_request", {
            "url": url,
            "method": "POST",
            "headers": random_headers(),
            "body": body,
        }, timeout=60)

        # Extract media.name for convenience (used as mediaId in video gen)
        if not _is_ws_error(result):
            data = result.get("data", {})
            if isinstance(data, dict):
                media = data.get("media", {})
                if isinstance(media, dict) and media.get("name"):
                    result["_mediaId"] = media["name"]

        return result

    async def test_recaptcha_token(self, worker_id: str = "nick-a", action: str = "FLOW_PROBE") -> dict:
        """Test minting 1 live reCAPTCHA Enterprise token through Chrome extension on worker_id."""
        pin, candidates = self._profile_candidates(profile_id=worker_id)
        if not candidates:
            return {"ok": False, "error": f"Worker {worker_id} not connected"}
        route = candidates[0]
        token_route = _current_route.set(route)
        try:
            res = await self._send("solve_captcha", {"captchaAction": action}, timeout=20)
            data = res.get("result") or res
            tok = data.get("token")
            if tok:
                return {
                    "ok": True,
                    "worker_id": worker_id,
                    "action": action,
                    "token_preview": f"{tok[:25]}...{tok[-15:]}",
                    "token_length": len(tok)
                }
            return {
                "ok": False,
                "worker_id": worker_id,
                "action": action,
                "error": data.get("error", "No token returned")
            }
        finally:
            _current_route.reset(token_route)

# ─── Response shaping ────────────────────────────────────────
#
# The batch path answers in Flow's positional arrays; everything downstream
# reads the old REST shapes. These put one back on the other so the parsers,
# the poller and the DB writers never learn which transport ran.

_CAPTURE_HINT = "see docs/CAPTURE.md to record its payload off the new UI"

_UNSUPPORTED_CREATE_PROJECT = (
    "NO_FLOW_PROJECT: Flow's project.createProject endpoint went with the September 2026 "
    "migration, so Flow Kit cannot create one. Make a project in the Flow UI, then either "
    "pass its uuid as flow_project_id or pin it as FLOW_PROJECT_ID."
)


def _unsupported(feature: str, why: str) -> str:
    return f"UNSUPPORTED_ON_BATCH_API: {feature} — {why}; {_CAPTURE_HINT}."


def _batch_error(exc: Exception) -> dict:
    """An exception from the batch path, in the error shape callers expect."""
    logger.error("Batch RPC error: %s: %s", type(exc).__name__, exc, exc_info=True)
    return {"status": 502, "error": f"{type(exc).__name__}: {exc}"}


def _as_media_record(image: "fb.GeneratedImage") -> dict:
    """One generated image, in the REST response's `media[]` shape."""
    return {
        "name": image.media_id,
        "image": {"generatedImage": {"mediaId": image.media_id, "fifeUrl": image.url}},
    }


def _as_pending_operation(operation_id: str, error: str | None = None,
                          media_id: str | None = None) -> dict:
    """An operation that has not produced a fetchable clip yet.

    ``error`` is carried, not acted on: a poll complaint is a diagnostic that
    finished jobs also report, so it exists to make a timeout message useful.
    """
    entry: dict = {
        "operation": {"name": operation_id},
        "status": "MEDIA_GENERATION_STATUS_PENDING",
    }
    if media_id:
        entry["operation"]["metadata"] = {"video": {"mediaId": media_id}}
    if error:
        entry["complaint"] = error
    return entry



def _is_ws_error(result: dict) -> bool:
    return bool(result.get("error")) or (isinstance(result.get("status"), int) and result["status"] >= 400)


def _fetch_url_bytes(url: str, timeout: int = 30) -> tuple[bytes, str]:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read(), (resp.headers.get_content_type() or "image/jpeg")


# Singleton
_client: Optional[FlowClient] = None


def get_flow_client() -> FlowClient:
    global _client
    if _client is None:
        _client = FlowClient()
    return _client
