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
import re
import shutil
import time
import urllib.request
import uuid
from collections import deque
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
from agent.services import request_ledger as _ledger
from agent.services.headers import random_headers

from agent.services.video_evidence import transition as video_evidence
from agent.services.flow_trace import emit as trace_emit, traced, summary as trace_summary, identifier as trace_identifier, fingerprint

logger = logging.getLogger(__name__)

_R2V_OPS_PATH = BASE_DIR / ".scratch" / "r2v_ops.json"
_MEDIA_PROFILES_PATH = BASE_DIR / ".scratch" / "media_profiles.json"
_GETSESSION_DUMP = BASE_DIR / ".scratch" / "getsession-last.txt"
_LISTING_DUMP = BASE_DIR / ".scratch" / "listing-last.txt"
_R2V_AS29S_CAP = 12
# Measured from unusual_activity_audit + netlog (2026-09-23): gen submits that
# arrive <10min after the nick's last strike re-flag 57-87% of the time; at
# 10-20min quiet that drops to ~21%, and beyond 30min gains are marginal.
# 12min captures the decay knee without parking capacity for an hour.
_UNUSUAL_FLAG_DECAY_S = 720
# Forensic threshold (check_unusual_threshold): clean residential exits take a
# median 139 requests before Google flags them; rotate at 110 to stay under.
_PROACTIVE_ROTATE_AFTER_REQUESTS = 110

# Which Chrome nick the current RPC is running on. `_send` reads this so a
# high-level call can pin (or fail over) without every envelope builder
# knowing about WebSockets.
_current_route: contextvars.ContextVar[Optional[dict]] = contextvars.ContextVar(
    "flow_route", default=None,
)


class _SessionFlagged(Exception):
    """Sentinel: skip rotate+retry on a session-flagged nick so the request
    falls through to cross-nick failover instead of burning ~40s and a fresh
    proxy IP on a rotation that cannot help."""


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
        # Negative cache: media ids that already returned "No urls" — expired
        # or never existed. Re-polling burns an RPC per attempt and callers
        # were seen re-polling the same dead id 35+ times; answer from cache
        # for DEAD_MEDIA_TTL_S instead.
        self._dead_media: dict[str, float] = {}
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
        self._solver_retry_pending: set[str] = set()
        self._foreign_mint_pending: set[str] = set()
        # Trusted-minter bookkeeping: minter nick -> unix ts until which its
        # tokens are distrusted, and target nick -> minter that supplied the
        # in-flight token (for pass/fail attribution).
        self._minter_fail_until: dict[str, float] = {}
        self._minter_token_fails: dict[str, int] = {}
        self._active_minter: dict[str, str] = {}
        # Successful mints per minter — tiebreaks _minter_route so load
        # round-robins instead of pinning the first session in dict order.
        self._minter_use_count: dict[str, int] = {}
        # Mint telemetry: per-minter counters + recent-mint timestamps for
        # rate measurement, mirrored to logs/captcha_mint_audit.jsonl.
        self._minter_stats: dict[str, dict] = {}
        self._minter_mint_ts: dict[str, deque] = {}
        self._minter_page_dead: dict[str, float] = {}
        self._minter_reload_at: dict[str, float] = {}
        self._minter_probe_task: asyncio.Task | None = None
        # Standby token pool: action -> deque of {token, minter, minted_at}.
        # Strike retries pop a warm token instead of minting mid-retry.
        self._token_pool: dict[str, deque] = {}
        self._token_pool_lock = asyncio.Lock()
        self._token_pool_task: asyncio.Task | None = None
        self._mint_audit_path = BASE_DIR / "logs" / "captcha_mint_audit.jsonl"
        self._last_route: Optional[dict] = None
        self._operation_complaints: dict[str, str] = {}
        self._profile_dispatched_counts: dict[str, int] = {}
        self._profile_in_flight: dict[str, int] = {}
        self._worker_dispatch_locks: dict[str, asyncio.Lock] = {}
        self._worker_last_video_dispatch: dict[str, float] = {}
        self._worker_last_dispatch: dict[str, float] = {}
        self._replay_in_progress: set[str] = set()
        self._unusual_strikes: dict[str, int] = {}
        self._unusual_strike_ts: dict[str, float] = {}
        # Terminal flag: strikes kept landing after hold-out releases and
        # proxy rotations — the flag follows the Google session, so rotating
        # again only burns clean IPs. Terminal nicks leave routing entirely
        # until an operator repair (clear_auth_strikes).
        self._session_terminal: dict[str, float] = {}
        # Auto-clone dedupe: one clone attempt per terminal nick per boot.
        self._clone_pending: set[str] = set()
        # nick → monotonic time a warm probe last saw app_ready. Distinguishes
        # a session Google revoked mid-watch from a clone that was born dead.
        self._ready_seen: dict[str, float] = {}
        # nick → last time a stale (no page_state) extension was auto-
        # relaunched to pick up current code. 20min cooldown prevents churn.
        self._ext_refresh: dict[str, float] = {}
        # Canary probe: when a hold-out window elapses we fire one cheap RPC
        # before re-admitting real traffic, so a still-flagged session eats a
        # synthetic request instead of a paying one.
        self._canary_pending: set[str] = set()
        # Flag-depth escalation: a nick that re-strikes right after its
        # hold-out release gets a doubled decay each time (12m→24m→48m→96m→192m).
        # Deep flags (hundreds of strikes) don't clear in one 12min window.
        self._flag_depth: dict[str, int] = {}
        self._flag_released: set[str] = set()
        # Strike counts live in memory only, so after a restart nothing knows a
        # nick still carries an open ACCOUNT_SESSION_FLAGGED row. Seeded once
        # from the ledger, lazily, so a success can still close it.
        self._flagged_nicks: set[str] | None = None
        # nick -> epoch until which that account is known to have no access to
        # the Veo models (PUBLIC_ERROR_MODEL_ACCESS_DENIED). Video-only: such a
        # nick can still upload, poll and generate images.
        self._model_denied: dict[str, float] = {}
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
        if self._minter_probe_task is None or self._minter_probe_task.done():
            self._minter_probe_task = asyncio.create_task(self._minter_probe_loop())
        if self._token_pool_task is None or self._token_pool_task.done():
            self._token_pool_task = asyncio.create_task(self._token_pool_loop())
        self._ws_connect_count += 1
        self._ws_connected_at = time.time()
        logger.info(
            "Extension connected #%d (%d active connection(s)); "
            "waiting for extension_ready/token_captured to sync",
            self._ws_connect_count,
            len(self._extensions),
        )
        _ledger.record_event("EXT_CONNECT", detail={
            "active": len(self._extensions),
            "connect_count": self._ws_connect_count,
        })

    def clear_extension(self, ws=None):
        """Called when extension disconnects."""
        disconnected_ws = ws or self._extension_ws
        if disconnected_ws is None:
            return

        session = self._extensions.pop(disconnected_ws, None)
        self._ws_disconnect_count += 1
        self._ws_last_disconnect_at = time.time()
        _ledger.record_event("EXT_DISCONNECT",
                             nick=(session or {}).get("profile_id"),
                             detail={
                                 "pending_cancelled": sum(
                                     1 for _, pws in list(self._pending_ws.items())
                                     if pws is disconnected_ws),
                                 "active": len(self._extensions),
                             })

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
        mint_only = self._minter_nicks()
        for ws, session in self._extensions.items():
            if require_token and not session.get("flow_key"):
                continue
            # Mint-only farm nicks never serve real work — unrouted calls land
            # here, so without this a farm nick could pick up gen traffic.
            if session.get("profile_id") in mint_only:
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
            # A relaunched Chrome brings a fresh ws while the old socket can
            # linger half-open — same profile_id on two sessions means the
            # router may dispatch into a dead socket. Resolve by liveness:
            # a socket that answers stays, the other is the zombie. Blind
            # "newest wins" ping-pongs because the evicted extension just
            # reconnects and re-evicts the survivor.
            for other_ws, other in list(self._extensions.items()):
                if other_ws is not ws and other.get("profile_id") == profile_id:
                    logger.warning(
                        "Duplicate WS session for %s — resolving by liveness",
                        profile_id,
                    )
                    try:
                        asyncio.get_running_loop().create_task(
                            self._resolve_dup_ws(other_ws, ws, profile_id))
                    except RuntimeError:
                        # No loop (shouldn't happen) — keep the new conn.
                        self.clear_extension(other_ws)
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
        video_submission: bool = False,
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
        # Ghost pin: pinned nick has no account row (renamed/deleted since the
        # pin was recorded — e.g. media/operation/caller still points at an old
        # nick id). Dropping it lets unpinned routing pick a live nick instead
        # of hard-failing with NO_FLOW_TAB; media rebind upstream fixes refs.
        if pin:
            try:
                from agent.services.accounts import get_account
                if get_account(pin) is None:
                    logger.warning(
                        "Pin %s resolves to no configured nick — dropping pin, routing unpinned",
                        pin,
                    )
                    pin = None
            except Exception:
                pass
        now = time.time()
        mint_only = self._minter_nicks()
        routes = []
        for ws, session in self._extensions.items():
            if require_token and not session.get("flow_key"):
                continue
            sid = session.get("profile_id")
            if sid and sid in mint_only:
                continue
            if pin and ws is not pin:
                if not sid or str(sid).strip().lower() != str(pin).strip().lower():
                    continue
            recency = session.get("token_captured_at") or session.get("connected_at") or 0

            # Check if this worker profile's proxy is currently quarantined or account is disabled
            is_quar = False
            if sid:
                try:
                    from agent.services.accounts import get_account, load_accounts
                    from agent.services.proxy_checker import is_quarantined
                    acc = get_account(sid)
                    # A deleted nick keeps its Chrome and its WS session, so
                    # without this it stays routable and keeps failing work.
                    if acc is None and load_accounts():
                        continue
                    if acc and not acc.get("enabled", True):
                        continue
                    if acc and acc.get("proxy_url"):
                        is_quar = is_quarantined(acc["proxy_url"])
                except Exception:
                    pass

            # This account has no access to the video models, so a submit here
            # is a guaranteed PUBLIC_ERROR_MODEL_ACCESS_DENIED.
            if video_submission and sid and self._model_denied.get(sid, 0.0) > now:
                continue

            # Session-flagged nicks (recent UNUSUAL_ACTIVITY strikes) cannot
            # produce: Google rejects their submits at the account level
            # regardless of exit IP. Dispatching to them anyway wastes ~10s,
            # records another strike, and deepens the Google-side flag, so
            # hold them out of rotation until the 30min strike window decays
            # — matching the decay semantics at the strike site below. The
            # ledger call is cached, so this only hits sqlite once per boot.
            # Terminal sessions never re-enter rotation: the flag followed
            # the Google session through multiple IP rotations, so more
            # rotating only burns clean proxy IPs. Cleared by operator
            # repair (clear_auth_strikes) — i.e. after a fresh login.
            if sid and sid in self._session_terminal:
                continue

            self._flagged_from_ledger()
            if sid and self._unusual_strikes.get(sid, 0) >= 2:
                last_ts = self._unusual_strike_ts.get(sid, 0)
                decay = _UNUSUAL_FLAG_DECAY_S << min(self._flag_depth.get(sid, 0), 4)
                if now - last_ts <= decay:
                    continue
                # Hold-out elapsed — canary first: fire one cheap ListSessions
                # and only re-admit the nick if Google answers clean. A still
                # -flagged session burns the synthetic probe, not a real job.
                if sid in self._canary_pending:
                    continue
                self._canary_pending.add(sid)
                try:
                    asyncio.get_running_loop().create_task(self._canary_probe(sid))
                except RuntimeError:
                    # No loop (sync caller) — fall back to immediate release.
                    self._canary_pending.discard(sid)
                    self._unusual_strikes[sid] = 0
                    self._flag_released.add(sid)
                continue

            is_avail = (
                session.get("unavailable_until", 0) <= now
                and session.get("warming_until", 0) <= now
                and not is_quar
            )
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
                "video_denied_for_s": max(0, int(self._model_denied.get(pid, 0.0) - now)) if pid else 0,
                "video_cooldown_remaining_s": round(cooldown_rem, 1),
                "in_flight": int(session.get("in_flight") or 0),
                "chat_session": bool(session.get("chat_session_id")),
                "flow_key_present": bool(session.get("flow_key")),
                "flow_guard_version": session.get("flow_guard_version"),
            })
        return out

    def auth_strike_report(self) -> dict[str, dict]:
        """Soft-auth strike counters per nick, for the dashboard's 401 panel.

        These were only ever read inside the failure path, so a nick one strike
        from being disabled looked identical to a healthy one from outside.
        """
        now = time.time()
        parked: dict[str, float] = {}
        for session in self._extensions.values():
            pid = session.get("profile_id")
            if not pid:
                continue
            until = float(session.get("unavailable_until") or 0)
            parked[pid] = max(parked.get(pid, 0.0), max(0.0, until - now))
        out: dict[str, dict] = {}
        for pid in set(self._auth_strikes) | set(parked):
            last = self._auth_strike_ts.get(pid, 0.0)
            # Expired strikes are not strikes; report what the next failure sees.
            strikes = self._auth_strikes.get(pid, 0)
            if last and now - last > _config.AUTH_STRIKE_TTL_S:
                strikes = 0
            out[pid] = {
                "strikes": strikes,
                "last_strike_at": last or None,
                "parked_for_s": round(parked.get(pid, 0.0), 1),
            }
        return out

    def model_denied_nicks(self) -> list[str]:
        """Connected nicks currently parked for PUBLIC_ERROR_MODEL_ACCESS_DENIED."""
        now = time.time()
        return sorted(nick for nick, until in self._model_denied.items() if until > now)

    async def diagnose_nick(self, nick: str, hours: float = 24.0) -> dict:
        """Correlated 'why' report for one nick: live state + persistent
        outcomes + lifecycle events, reduced to human-readable findings."""
        now = time.time()
        from agent.services.accounts import get_account
        acc = get_account(nick) or {}
        session = next(
            (s for s in self._extensions.values()
             if s.get("profile_id") == nick),
            None,
        )

        # ---- live state -------------------------------------------------
        state: dict = {
            "account_row": bool(acc),
            "enabled": bool(acc.get("enabled", True)),
            "mint_only": bool(acc.get("mint_only")),
            "connected": session is not None,
            "chat_session": bool((session or {}).get("chat_session_id")),
            "in_flight": int((session or {}).get("in_flight") or 0),
            "unusual_strikes": self._unusual_strikes.get(nick, 0),
            "flag_depth": self._flag_depth.get(nick, 0),
            "session_terminal": nick in self._session_terminal,
            "canary_pending": nick in self._canary_pending,
            "auth_strikes": self._auth_strikes.get(nick, 0),
            "proxy_url": acc.get("proxy_url"),
            "egress_ip": acc.get("last_egress_ip"),
            "page_state": (session or {}).get("page_state"),
            "page_title": (session or {}).get("page_title"),
        }
        parked_until = float((session or {}).get("unavailable_until") or 0)
        state["parked_for_s"] = round(max(0.0, parked_until - now), 1)

        # hold-out check mirrors _profile_candidates
        decay = _UNUSUAL_FLAG_DECAY_S << min(self._flag_depth.get(nick, 0), 4)
        last_strike = self._unusual_strike_ts.get(nick, 0)
        state["holdout"] = bool(
            state["unusual_strikes"] >= 2 and last_strike
            and now - last_strike <= decay
        )
        if state["holdout"]:
            state["holdout_lifts_in_s"] = round(decay - (now - last_strike), 1)

        quar = None
        if acc.get("proxy_url"):
            try:
                from agent.services.proxy_checker import is_quarantined
                quar = bool(is_quarantined(acc["proxy_url"]))
            except Exception:
                quar = None
        state["proxy_quarantined"] = quar

        # Live page introspection — what the Flow tab is actually showing
        # right now (unusual_wall / signed_out / app_ready / ...). Turns
        # NO_AT_TOKEN from a symptom into a named cause.
        if session is not None:
            try:
                route = {"ws": ws, "profile_id": nick, "pinned": True}
                tok_probe = _current_route.set(route)
                try:
                    health = await self._send("flow_tab_health", {}, timeout=10)
                finally:
                    _current_route.reset(tok_probe)
                hr = health.get("result") or {}
                if hr.get("page_state"):
                    state["page_state"] = hr["page_state"]
                    session["page_state"] = hr["page_state"]
                if hr.get("title"):
                    state["page_title"] = hr["title"]
                    session["page_title"] = hr["title"]
                state["tab_alive"] = hr.get("alive")
                if hr.get("url"):
                    state["tab_url"] = hr["url"][:160]
            except Exception:
                pass

        # ---- ledger pull -------------------------------------------------
        outcomes = _ledger.recent_outcomes(nick=nick, hours=hours, limit=500)
        events = _ledger.recent_events(hours=hours, nick=nick, limit=100)
        fails: dict[str, int] = {}
        queue_samples = []
        for o in outcomes:
            if not o["ok"]:
                fails[o["error_class"]] = fails.get(o["error_class"], 0) + 1
            if o.get("queue_ms"):
                queue_samples.append(o["queue_ms"])
        ok_n = sum(1 for o in outcomes if o["ok"])
        avg_queue = round(sum(queue_samples) / len(queue_samples)) if queue_samples else 0
        max_queue = max(queue_samples) if queue_samples else 0

        # ---- findings ----------------------------------------------------
        findings: list[str] = []
        if not acc:
            findings.append("Không có account row — pin/request vào nick này sẽ được drop (ghost pin)")
        elif not state["enabled"]:
            findings.append("Account disabled — không nhận việc; cần re-login/enable lại")
        if state["mint_only"]:
            findings.append("mint_only — chỉ dùng mint captcha, không route gen")
        if not state["connected"]:
            findings.append("Extension chưa connect — Chrome không chạy hoặc WS rớt")
        elif not state["chat_session"] and not state["mint_only"]:
            findings.append("Chưa có chat session — r2v chưa sẵn sàng")
        if state["session_terminal"]:
            findings.append(
                "SESSION_TERMINAL — flag theo Google session, đã rút hẳn khỏi routing; "
                "cần re-login rồi clear strikes"
            )
        elif state["canary_pending"]:
            findings.append("Đang canary-probe sau hold-out — chờ kết quả")
        elif state["holdout"]:
            findings.append(
                f"Đang HOLD-OUT: {state['unusual_strikes']} strike unusual, "
                f"mở lại sau ~{int(state['holdout_lifts_in_s'])}s"
            )
        elif state["unusual_strikes"]:
            findings.append(f"{state['unusual_strikes']} strike unusual còn trong window (chưa đủ 2 để hold-out)")
        pstate = state.get("page_state")
        if pstate == "unusual_wall":
            findings.append(
                "Flow page đang dính UNUSUAL-ACTIVITY WALL (captcha/verify) — "
                "session×IP này bị Google chặn ngay khi load; rotate IP hoặc clone sang instance khác"
            )
        elif pstate == "signed_out":
            findings.append(
                "Flow page đang SIGNED-OUT — cookies không còn hiệu lực với Google; cần re-login"
            )
        elif pstate == "error_page":
            findings.append("Flow page đang ở trang lỗi — reload tab")
        elif pstate == "loading":
            findings.append("Flow page vẫn đang load — NO_AT_TOKEN chỉ là chưa boot xong")
        if state["parked_for_s"] > 0:
            findings.append(f"Đang park auth {int(state['parked_for_s'])}s — session Google có vấn đề")
        if quar:
            findings.append("Proxy đang quarantine — request bị chặn cho tới khi hết cooldown")
        tab_dead = sum(1 for e in events if e["kind"] == "TAB_DEAD")
        if tab_dead:
            findings.append(f"Tab Flow chết {tab_dead} lần trong {hours:g}h — crash/xám, không phải Google flag")
        bind_fail = sum(1 for e in events if e["kind"] == "BIND_FAIL")
        if bind_fail:
            findings.append(f"Auto-bind r2v fail {bind_fail} lần — xem BIND_FAIL events")
        rotates = sum(1 for e in events if e["kind"] == "PROXY_ROTATE")
        if rotates:
            findings.append(f"Proxy rotate {rotates} lần — IP không ổn định")
        if avg_queue > 3000:
            findings.append(
                f"Queue chờ sema trung bình {avg_queue}ms (đỉnh {max_queue}ms) — "
                "PROFILE_MAX_CONCURRENT đang thắt, cân nhắc nâng"
            )
        if fails:
            top = max(fails.items(), key=lambda kv: kv[1])
            cls_notes = {
                "MEDIA_NOT_FOUND": "fail chủ yếu do media hết hạn/sai — lỗi data, không phải fleet",
                "FLOW_BUSY": "gate tab nghẽn — submit serialize quá tải",
                "TAB_DEAD": "tab/page chết — đã tách khỏi strike Google",
                "UNUSUAL_ACTIVITY": "Google flag thật — session hoặc IP bị mark",
                "AUTH": "session Google hết hạn — cần re-login",
                "NO_AT_TOKEN": "trang Flow chưa sẵn sàng khi gọi RPC",
            }
            findings.append(
                f"Lỗi nhiều nhất: {top[0]} x{top[1]} — {cls_notes.get(top[0], 'xem outcome_log')}"
            )
        if not findings:
            findings.append("Không phát hiện vấn đề — nick đang khoẻ")

        return {
            "nick": nick,
            "hours": hours,
            "state": state,
            "outcomes": {
                "total": len(outcomes), "ok": ok_n,
                "failed": len(outcomes) - ok_n,
                "success_rate": round(100.0 * ok_n / len(outcomes), 1) if outcomes else None,
                "by_class": fails,
                "avg_queue_ms": avg_queue, "max_queue_ms": max_queue,
            },
            "events": events,
            "findings": findings,
        }

    def model_denied_report(self) -> dict:
        now = time.time()
        return {
            nick: {"parked_for_s": int(until - now), "until": until}
            for nick, until in sorted(self._model_denied.items())
            if until > now
        }

    def clear_model_denied(self, profile_id: str) -> bool:
        """Put a nick back in the video rotation (after it was granted access)."""
        return self._model_denied.pop(profile_id, None) is not None

    def _flagged_from_ledger(self) -> set[str]:
        """Nicks carrying an open ACCOUNT_SESSION_FLAGGED row, read once.

        Without this the resolve below is gated on a strike counter that a
        restart wipes, so an incident opened before the restart would stay OPEN
        until the 24h stale TTL even though the nick was submitting fine.
        """
        if self._flagged_nicks is None:
            self._flagged_nicks = set()
            try:
                from agent.services.incident_manager import get_incident_manager
                for inc in get_incident_manager().get_incidents(
                    module="worker", status="OPEN", limit=200
                ):
                    if inc.get("error_code") == "ACCOUNT_SESSION_FLAGGED":
                        nick = inc.get("job_id") or inc.get("sub_id")
                        if nick:
                            self._flagged_nicks.add(nick)
                            # Seed the in-memory strike counters as well — a
                            # restart wipes them, and without this a flagged
                            # nick re-enters rotation and re-burns fresh
                            # dispatches before re-earning its hold-out.
                            self._unusual_strikes[nick] = max(
                                self._unusual_strikes.get(nick, 0), 2
                            )
                            inc_ts = float(inc.get("created_at") or time.time())
                            self._unusual_strike_ts[nick] = max(
                                self._unusual_strike_ts.get(nick, 0), inc_ts
                            )
            except Exception:
                pass
            # Same restart gap, different source: today's strikes are written
            # to the unusual-activity audit jsonl, not the incident ledger.
            # Reseed per-nick strike counts from the last 30min of that log so
            # a boot doesn't hand flagged nicks two free dispatches each.
            try:
                from agent.services.unusual_audit import AUDIT_LOG_FILE
                import datetime as _dt
                now = time.time()
                if AUDIT_LOG_FILE.exists():
                    for line in AUDIT_LOG_FILE.read_text(
                        encoding="utf-8"
                    ).splitlines():
                        try:
                            ev = json.loads(line)
                            ts = _dt.datetime.fromisoformat(
                                ev["timestamp_utc"].replace("Z", "+00:00")
                            ).timestamp()
                        except Exception:
                            continue
                        if now - ts > _UNUSUAL_FLAG_DECAY_S:
                            continue
                        wid = ev.get("worker_id")
                        if not wid:
                            continue
                        # One recent strike is enough to hold the nick out:
                        # with only count=1 it would still be routable and
                        # immediately re-earn its second strike on the first
                        # post-boot dispatch. A real success clears this.
                        self._unusual_strikes[wid] = max(
                            2, self._unusual_strikes.get(wid, 0) + 1
                        )
                        self._unusual_strike_ts[wid] = max(
                            self._unusual_strike_ts.get(wid, 0), ts
                        )
            except Exception:
                pass
            # Terminal state must survive restarts too — otherwise a boot
            # hands a dead session back into rotation until it re-earns
            # terminal through fresh strikes. Latest lifecycle event wins:
            # SESSION_TERMINAL sticks unless a later CANARY_OK /
            # TERMINAL_CLEARED / repair cleared it.
            try:
                seen_nicks: set[str] = set()
                for ev in _ledger.recent_events(
                    hours=72, limit=500,
                    kinds=["SESSION_TERMINAL", "CANARY_OK", "TERMINAL_CLEARED"],
                ):
                    nick = ev.get("nick")
                    if not nick or nick in seen_nicks:
                        continue
                    seen_nicks.add(nick)
                    if ev["kind"] == "SESSION_TERMINAL":
                        self._session_terminal.setdefault(nick, ev["ts"])
                    # newest event per nick wins: a later CANARY_OK /
                    # TERMINAL_CLEARED leaves the nick out of terminal.
            except Exception:
                pass
        return self._flagged_nicks

    async def _maybe_proactive_rotate(self, prof_id: str) -> None:
        """Swap the exit IP before it crosses Google's observed burn line.

        Reactive rotation happens only AFTER an UNUSUAL_ACTIVITY strike — by
        then the IP is already flagged. Forensics (check_unusual_threshold)
        put the median clean-exit lifetime at 139 requests, so rotating at 110
        keeps each egress under the line Google starts scoring against.
        A failed rotate is non-fatal: the submit proceeds on the current IP.
        """
        try:
            from agent.services.accounts import get_account
            from agent.services.unusual_audit import get_unusual_audit
            acc = get_account(prof_id)
            p_url = (acc or {}).get("proxy_url") or ""
            if not p_url:
                return
            total = get_unusual_audit().proxy_request_count(p_url)
            if total < _PROACTIVE_ROTATE_AFTER_REQUESTS:
                return
            from agent.services.proxy_pool import rotate_nick_proxy
            rot = await rotate_nick_proxy(prof_id, preflight=True)
            if rot.get("ok"):
                logger.info(
                    "PROACTIVE_ROTATE %s: %d reqs on exit — new egress %s",
                    prof_id, total, rot.get("egress_ip"),
                )
            else:
                logger.warning(
                    "PROACTIVE_ROTATE %s failed (%s) — continuing on current IP",
                    prof_id, rot.get("error"),
                )
        except Exception as exc:
            logger.debug("proactive rotate skipped for %s: %s", prof_id, exc)

    def clear_auth_strikes(self, profile_id: str) -> dict:
        """Forget a nick's soft-auth history and un-park it (manual re-enable)."""
        had = self._auth_strikes.pop(profile_id, 0)
        self._auth_strike_ts.pop(profile_id, None)
        # Also drop the UNUSUAL_ACTIVITY hold-out state: a manual re-enable is
        # the operator saying "I repaired this session" (browsed, solved the
        # captcha, re-logged in) — keeping it parked would waste that work.
        had_unusual = self._unusual_strikes.pop(profile_id, 0)
        self._unusual_strike_ts.pop(profile_id, None)
        was_terminal = self._session_terminal.pop(profile_id, None) is not None
        if was_terminal:
            _ledger.record_event("TERMINAL_CLEARED", nick=profile_id,
                                 detail={"via": "operator_repair"})
        had_depth = self._flag_depth.pop(profile_id, 0)
        self._flag_released.discard(profile_id)
        unparked = 0
        for session in self._extensions.values():
            if session.get("profile_id") == profile_id and session.get("unavailable_until", 0) > time.time():
                session["unavailable_until"] = 0
                unparked += 1
        return {"profile_id": profile_id, "cleared_strikes": had,
                "cleared_unusual_strikes": had_unusual,
                "cleared_flag_depth": had_depth, "unparked": unparked}

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
            video_submission=video_submission,
        )
        owners = {self._media_profiles[mid].casefold() for mid in media_ids or []
                  if self._media_profiles.get(mid)}
        if len(owners) > 1 or (owners and pin and str(pin).casefold() not in owners):
            return {"status": 400, "error_code": "media_profile_mismatch", "retryable": False,
                    "error": "MEDIA_PROFILE_MISMATCH: reference images must belong to the selected nick; upload references to one profile"}
        self._last_route = None
        if not candidates:
            denied = self.model_denied_nicks()
            if video_submission and denied and not pin:
                return {
                    "status": 503,
                    "retryable": True,
                    "error_code": "model_access_denied",
                    "retry_after_s": 300,
                    "denied_nicks": denied,
                    "error": (
                        "PUBLIC_ERROR_MODEL_ACCESS_DENIED: no connected nick has access to "
                        f"the video models ({', '.join(denied)}) — add a nick whose Google "
                        "account can render in the Flow UI"
                    ),
                }
            if pin:
                _ledger.record_event("ROUTE_EMPTY", nick=pin, detail={
                    "reason": "pinned_profile_not_connected",
                    "project_id": requested_project,
                    "video": video_submission,
                    "connected": [
                        s.get("profile_id") for s in self._extensions.values()
                        if s.get("profile_id")
                    ],
                })
                return {"error": f"NO_FLOW_TAB: profile {pin} is not connected"}
            if self._extensions:
                _ledger.record_event("ROUTE_EMPTY", detail={
                    "reason": "no_eligible_candidate",
                    "project_id": requested_project,
                    "video": video_submission,
                    "connected": [
                        s.get("profile_id") for s in self._extensions.values()
                        if s.get("profile_id")
                    ],
                })
                return {"error": "Extension not connected"}
            # Unit tests mock batch_rpc with no sockets. get_media / poll do
            # not always have a project; generate calls still need one.
            if requested_project or FLOW_PROJECT_ID:
                pid = self._batch_project_id(requested_project or "")
            else:
                pid = ""
            return await builder(pid)

        # A pinned call has exactly one legal target — and dispatching into a
        # warming/parked/walled tab only spends a poll and logs another
        # NO_AT_TOKEN (yousef-dual dripped 1-2/min for 4h this way). Walled
        # states fail fast with a retryable 503; transient gates get the same
        # in-band hold as the unpinned parked path.
        if pin and candidates and not candidates[0].get("available"):
            sess = self._extensions.get(candidates[0]["ws"]) or {}
            state = sess.get("page_state")
            if (state in ("signed_out", "unusual_wall")
                    or pin in self._session_terminal or sess.get("signed_out")):
                _ledger.record_event("PINNED_WALLED", nick=pin, detail={
                    "page_state": state,
                    "media": bool(media_ids),
                    "operation": bool(operation_id),
                })
                return {
                    "status": 503, "retryable": True,
                    "error_code": "pinned_worker_walled",
                    "retry_after_s": 300,
                    "error": (
                        f"PINNED_WORKER_WALLED: profile {pin} is "
                        f"{state or 'terminal'} — pinned work waits for repair"
                    ),
                }
            lift = max(float(sess.get("warming_until") or 0),
                       float(sess.get("unavailable_until") or 0))
            wait_s = lift - time.time()
            if wait_s > 0:
                deadline = time.time() + min(wait_s, _config.PARKED_WAIT_S) + 1.5
                while time.time() < deadline:
                    await asyncio.sleep(min(2.0, deadline - time.time()))
                    _, candidates = self._profile_candidates(
                        project_id=requested_project,
                        profile_id=profile_id,
                        media_ids=media_ids,
                        operation_id=operation_id,
                        video_submission=video_submission,
                    )
                    if candidates and candidates[0].get("available"):
                        break
            if not (candidates and candidates[0].get("available")):
                _ledger.record_event("PINNED_PARKED", nick=pin, detail={
                    "waited_s": round(time.time() - (lift - wait_s)),
                    "page_state": state,
                })
                return {
                    "status": 503, "retryable": True,
                    "error_code": "pinned_worker_unavailable",
                    "retry_after_s": 60,
                    "error": (
                        f"PINNED_WORKER_UNAVAILABLE: profile {pin} still gated "
                        "after the wait window — retry shortly"
                    ),
                }

        # Every unpinned candidate parked means each nick just failed auth or
        # sits on a quarantined proxy. If the soonest park lifts within
        # PARKED_WAIT_S, hold the request in-band and re-check — a slow
        # success beats an instant 503 the caller must retry around.
        if not pin and not any(route.get("available") for route in candidates):
            waits = [
                float((self._extensions.get(route.get("ws")) or {}).get("unavailable_until") or 0)
                for route in candidates
            ]
            soonest = min((w for w in waits if w > 0), default=0.0)
            wait_s = soonest - time.time() if soonest else 0.0
            if 0 < wait_s <= _config.PARKED_WAIT_S:
                deadline = soonest + 1.5
                logger.info(
                    "All nicks parked; holding request %.0fs until %s bench lifts",
                    wait_s, soonest and time.strftime("%H:%M:%S", time.localtime(soonest)),
                )
                while time.time() < deadline:
                    await asyncio.sleep(min(2.0, max(0.1, deadline - time.time())))
                    _, candidates = self._profile_candidates(
                        project_id=requested_project,
                        profile_id=profile_id,
                        media_ids=media_ids,
                        operation_id=operation_id,
                        video_submission=video_submission,
                    )
                    if any(r.get("available") for r in candidates):
                        break
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
            _ledger.record_event("ROUTE_ALL_PARKED", detail={
                "parked": [str(r.get("profile_id") or "?") for r in candidates],
                "retry_after_s": retry_after,
                "video": video_submission,
            })
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
                    queue_ms = round((time.time()-t_start)*1000)
                    trace_emit("worker.acquired", profile_hash=fingerprint(prof_id),
                               queue_ms=queue_ms)
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
                                # Also respect the all-RPC min gap vs the last
                                # dispatch of any kind on this worker.
                                wait_s = max(
                                    wait_s,
                                    _config.PER_WORKER_RPC_MIN_GAP_S
                                    - (now_m - self._worker_last_dispatch.get(prof_id, 0.0)),
                                )
                                if wait_s > 0:
                                    logger.info(
                                        "Worker %s human jitter video pacing: waiting %.2fs (target: %.2fs in [%.1fs, %.1fs])",
                                        prof_id, wait_s, cooldown_target,
                                        _config.PER_WORKER_VIDEO_COOLDOWN_MIN, _config.PER_WORKER_VIDEO_COOLDOWN_MAX,
                                    )
                                    await asyncio.sleep(wait_s)
                                self._worker_last_video_dispatch[prof_id] = time.monotonic()
                                self._worker_last_dispatch[prof_id] = time.monotonic()
                                await self._maybe_proactive_rotate(prof_id)
                                burst_metrics = audit_mgr.record_request_dispatched(prof_id)
                                burst_metrics["rpc_in_flight"] = session.get("in_flight", 0) if session else 0
                                nick_metrics.record_dispatch(prof_id)
                                try:
                                    last = await builder(pid)
                                except Exception as e:
                                    last = _batch_error(e)
                        else:
                            # Min spacing between any two dispatches on the same
                            # worker — the UNUSUAL_ACTIVITY rate-burst signature
                            # is gap<1.2s or >=3 req/10s. Only the dispatch-start
                            # times are serialized; the RPC itself still runs
                            # concurrently so polls/uploads are not queued.
                            async with self._worker_dispatch_lock(prof_id):
                                now_m = time.monotonic()
                                wait_s = _config.PER_WORKER_RPC_MIN_GAP_S - (
                                    now_m - self._worker_last_dispatch.get(prof_id, 0.0)
                                )
                                if wait_s > 0:
                                    await asyncio.sleep(wait_s)
                                await self._maybe_proactive_rotate(prof_id)
                                self._worker_last_dispatch[prof_id] = time.monotonic()
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
                    nick_metrics.record_completion(prof_id, success=False, latency_ms=duration_ms, queue_ms=queue_ms, error=raw_err[:200])
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
                                                       latency_ms=duration_ms, queue_ms=queue_ms, error=raw_err[:200])
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
                    "envelope in response",
                )))
                no_at_token = bool(err_text) and "no_at_token" in low_err
                if no_at_token and session is not None:
                    # Page hasn't captured the AT token yet — tab boot or a
                    # re-navigation, not an auth failure. Park briefly so the
                    # router picks another nick while this one warms up; never
                    # counts toward auth strikes or auto-disable (that's what
                    # killed the dual nicks right after every restart).
                    session["unavailable_until"] = max(
                        session.get("unavailable_until", 0), time.time() + 45)
                    # Kernel visibility: capture WHAT the page is showing so
                    # NO_AT_TOKEN stops being a black box — unusual_wall,
                    # signed_out, error_page, loading, or app_ready.
                    detail = {"error": raw_err[:200], "park_s": 45}
                    try:
                        tok_probe = _current_route.set(route)
                        try:
                            health = await self._send("flow_tab_health", {}, timeout=10)
                        finally:
                            _current_route.reset(tok_probe)
                        hr = health.get("result") or {}
                        if hr.get("page_state"):
                            detail["page_state"] = hr["page_state"]
                            session["page_state"] = hr["page_state"]
                        if hr.get("title"):
                            detail["title"] = hr["title"]
                            session["page_title"] = hr["title"]
                    except Exception:
                        pass
                    _ledger.record_event("NO_AT_TOKEN", nick=prof_id, detail=detail)
                elif is_auth_error and session is not None:
                    # A real 401 means the Google session is dead — parking in
                    # 30m loops just spams retries. Disable the account until
                    # the user re-logs in and re-enables it. Soft failures
                    # (envelope-less sign-in redirect) stay a 30m park.
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
                        # A missing envelope only proves a sign-in redirect when
                        # the tab itself is alive. A dead/crashed tab — or a WS
                        # flap — produces the same empty response, and counting
                        # those as auth failures disabled healthy nicks (burst
                        # of 3 in-flight RPCs → auto-disable in ~1s). Probe the
                        # tab first: dead → reload + TAB_UNRESPONSIVE, no
                        # strike, no 30m park.
                        tab_alive = None
                        health: dict = {}
                        try:
                            tok_probe = _current_route.set(route)
                            try:
                                health = await self._send("flow_tab_health", {}, timeout=10)
                            finally:
                                _current_route.reset(tok_probe)
                            if not health.get("error"):
                                tab_alive = (health.get("result") or {}).get("alive")
                        except Exception:
                            tab_alive = None
                        if tab_alive is not True:
                            reason = "probe_failed"
                            if isinstance(health, dict):
                                reason = str((health.get("result") or {}).get("reason") or health.get("error") or "probe_failed")
                            _ledger.record_event("TAB_DEAD", nick=prof_id, detail={
                                "reason": reason,
                                "masked_as": "auth_no_envelope",
                            })
                            try:
                                tok_rel = _current_route.set(route)
                                try:
                                    await self._send("reload_flow_tab", {}, timeout=30)
                                finally:
                                    _current_route.reset(tok_rel)
                            except Exception:
                                pass
                            session["unavailable_until"] = max(
                                session.get("unavailable_until", 0), time.time() + 45)
                            last["error"] = "TAB_UNRESPONSIVE"
                            last["retryable"] = True
                            last["message"] = (
                                "Tab Flow không phản hồi (chết/mất kết nối) — đã reload. "
                                "Không tính auth strike; retry sau vài giây."
                            )
                            nick_metrics.record_completion(
                                prof_id, success=False, latency_ms=duration_ms,
                                queue_ms=queue_ms, error="TAB_UNRESPONSIVE")
                            return last
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
                        _ledger.record_event("STRIKE_AUTH", nick=prof_id, detail={
                            "strikes": strikes,
                            "error": raw_err[:200],
                            "parked_30m": True,
                        })
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
                    nick_metrics.record_completion(prof_id, success=True, latency_ms=duration_ms, queue_ms=queue_ms)
                    # Foreign-mint attribution: which farm nick supplied the
                    # token that just passed.
                    minter = self._active_minter.pop(prof_id, None)
                    if minter:
                        self._minter_token_fails.pop(minter, None)
                        self._mint_event("token_pass", minter=minter,
                                         target=prof_id)
                        logger.info("Foreign-minted captcha from %s passed for %s", minter, prof_id)
                    # Gated on the pop: only a nick that actually carried strikes
                    # has an ACCOUNT_SESSION_FLAGGED incident to close, so a plain
                    # success does not write to the ledger.
                    had_strikes = self._unusual_strikes.pop(prof_id, None)
                    # Ledger lookup first: `or` would short-circuit past it and
                    # leave the lazy set unseeded.
                    flagged = self._flagged_from_ledger()
                    if had_strikes or prof_id in flagged:
                        flagged.discard(prof_id)
                        logger.info("UNUSUAL_ACTIVITY cleared on %s — session healthy again", prof_id)
                        try:
                            from agent.services.incident_manager import get_incident_manager
                            get_incident_manager().resolve_by_nick(
                                "worker", prof_id, error_code="ACCOUNT_SESSION_FLAGGED",
                                action_taken="SESSION_RECOVERED",
                            )
                        except Exception:
                            pass
                    # Only a video submission proves video-model access — an
                    # image/upload success used to un-park the nick and the
                    # next video job landed on it again (deny↔heal loop).
                    if video_submission and self._model_denied.pop(prof_id, None):
                        logger.info("Model access restored on %s — un-parked for video", prof_id)
                        try:
                            from agent.services.incident_manager import get_incident_manager
                            get_incident_manager().resolve_by_nick(
                                "worker", prof_id, error_code="MODEL_ACCESS_DENIED",
                                action_taken="MODEL_ACCESS_RESTORED",
                            )
                        except Exception:
                            pass
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

                    # Tab-health gate before blaming Google: a crashed/error
                    # page (grey screen) can't produce a real rejection — it
                    # surfaces stale or mangled payloads instead. Probe the
                    # tab; if dead, reload it and fail this call as
                    # TAB_UNRESPONSIVE — no strike, no proxy rotation.
                    try:
                        tok_probe = _current_route.set(route)
                        try:
                            health = await self._send("flow_tab_health", {}, timeout=15)
                        finally:
                            _current_route.reset(tok_probe)
                        health_result = health.get("result") or {}
                        if not health.get("error") and health_result.get("alive") is False:
                            logger.warning(
                                "UNUSUAL_ACTIVITY payload on %s but Flow tab is dead (%s) — "
                                "reloading tab instead of counting a strike",
                                prof_id, health_result.get("reason"),
                            )
                            _ledger.record_event("TAB_DEAD", nick=prof_id, detail={
                                "reason": health_result.get("reason"),
                                "masked_as": "PUBLIC_ERROR_UNUSUAL_ACTIVITY",
                            })
                            tok_rel = _current_route.set(route)
                            try:
                                await self._send("reload_flow_tab", {}, timeout=30)
                            finally:
                                _current_route.reset(tok_rel)
                            last["error"] = "TAB_UNRESPONSIVE"
                            last["retryable"] = True
                            last["message"] = (
                                "Tab Flow của nick đã chết (trang xám/crash) — đã reload. "
                                "Không tính strike; retry sau vài giây."
                            )
                            nick_metrics.record_completion(
                                prof_id, success=False, latency_ms=duration_ms, queue_ms=queue_ms,
                                error="TAB_UNRESPONSIVE",
                            )
                            return last
                    except Exception as probe_exc:
                        logger.debug("tab health probe failed for %s: %s", prof_id, probe_exc)

                    logger.warning(
                        "UNUSUAL_ACTIVITY detected on %s (RPC %s)! Synchronously rotating proxy...",
                        prof_id, rpc_id,
                    )

                    # Session-flag heuristic: strikes older than 30min decay.
                    # Two+ recent strikes mean the flag follows the Google
                    # session, not the IP — another rotate+retry would just
                    # burn ~40s of caller latency and a fresh proxy IP.
                    now = time.time()
                    if now - self._unusual_strike_ts.get(prof_id, 0) > _UNUSUAL_FLAG_DECAY_S:
                        self._unusual_strikes[prof_id] = 0
                    session_flagged = self._unusual_strikes.get(prof_id, 0) >= 2
                    self._unusual_strike_ts[prof_id] = now

                    # Top up the standby pool for this call's action while the
                    # proxy rotates — the mint overlaps the swap instead of
                    # serializing inside the retry.
                    prefetch_action = {
                        fb.RPC_GEN_IMAGE: fb.CAPTCHA_IMAGE,
                        fb.RPC_GEN_VIDEO: fb.CAPTCHA_VIDEO,
                        fb.RPC_GEN_T2V: fb.CAPTCHA_VIDEO,
                        fb.RPC_UPLOAD_IMAGE: fb.CAPTCHA_IMAGE,
                        fb.RPC_STREAM_CHAT: fb.CAPTCHA_CHAT,
                    }.get(rpc_id)
                    if prefetch_action:
                        asyncio.create_task(self._pool_prefetch(prefetch_action))

                    rotation_info = {
                        "rotation_triggered": False,
                        "retry_attempted": False,
                        "retry_success": False,
                    }

                    try:
                        # Terminal graduation: the flag survived rotations and
                        # hold-out releases — it lives on the Google session,
                        # so another rotate only burns a clean IP and deepens
                        # the flag. Stop rotating; failover + leave routing.
                        strikes_next = self._unusual_strikes.get(prof_id, 0) + 1
                        terminal = (
                            prof_id in self._session_terminal
                            or self._flag_depth.get(prof_id, 0) >= 2
                            or strikes_next >= 5
                        )
                        if terminal:
                            if prof_id not in self._session_terminal:
                                self._session_terminal[prof_id] = now
                                _ledger.record_event("SESSION_TERMINAL", nick=prof_id, detail={
                                    "via": "strike", "strikes": strikes_next,
                                    "flag_depth": self._flag_depth.get(prof_id, 0),
                                })
                                self._schedule_terminal_clone(prof_id)
                            rotation_info["skipped"] = "session_terminal"
                            last["retryable"] = True
                            last["message"] = (
                                "UNUSUAL_ACTIVITY: session flagged vĩnh viễn — đã rút khỏi "
                                "routing, cần re-login. Failover sang nick khác."
                            )
                            logger.warning(
                                "UNUSUAL_ACTIVITY on %s → SESSION_TERMINAL (strike #%d, "
                                "depth %d) — no more rotations, failing over",
                                prof_id, strikes_next, self._flag_depth.get(prof_id, 0),
                            )
                            raise _SessionFlagged()
                        if session_flagged:
                            rotation_info["skipped"] = "session_flagged"
                            last["retryable"] = True
                            last["message"] = (
                                "UNUSUAL_ACTIVITY: flag theo session, failover sang nick khác."
                            )
                            logger.warning(
                                "UNUSUAL_ACTIVITY on %s is session-flagged (strike #%d) — "
                                "skipping rotate, failing over",
                                prof_id, self._unusual_strikes.get(prof_id, 0) + 1,
                            )
                            raise _SessionFlagged()
                        try:
                            from agent.services.proxy_checker import quarantine_proxy, mark_ip_burned
                            from agent.services.accounts import get_account
                            acc = get_account(prof_id)
                            if acc and acc.get("proxy_url"):
                                quarantine_proxy(acc["proxy_url"], reason="PUBLIC_ERROR_UNUSUAL_ACTIVITY", cooldown_seconds=900)
                                _ledger.record_event("PROXY_QUARANTINE", nick=prof_id, detail={
                                    "proxy": acc["proxy_url"][:80],
                                    "egress_ip": acc.get("last_egress_ip"),
                                })
                                # Blame the egress IP only on the first strike —
                                # repeat flags on freshly rotated IPs mean the
                                # flag follows the Google session, not the IP.
                                if self._unusual_strikes.get(prof_id, 0) == 0:
                                    burned_ip = acc.get("last_egress_ip")
                                    if not burned_ip:
                                        from agent.services.surfshark import is_surfshark_url, probe_egress
                                        if is_surfshark_url(acc["proxy_url"]):
                                            burned_ip = await asyncio.to_thread(probe_egress, acc["proxy_url"], 5)
                                    if burned_ip:
                                        mark_ip_burned(burned_ip, prof_id)
                                        _ledger.record_event("PROXY_BURNED", nick=prof_id,
                                                             detail={"ip": burned_ip})
                        except Exception as q_err:
                            logger.warning("Could not quarantine proxy for %s: %s", prof_id, q_err)

                        from agent.services.proxy_pool import rotate_nick_proxy
                        # wait_s: if another rotation is mid-flight on this
                        # nick, piggyback on its fresh IP instead of failing
                        # the request with ROTATION_IN_PROGRESS.
                        rot_res = await rotate_nick_proxy(prof_id, preflight=True, wait_s=20)
                        rotation_info["rotation_triggered"] = True
                        if not rot_res.get("ok"):
                            last["proxy_rotated"] = False
                            last["retryable"] = True
                            last["message"] = "UNUSUAL_ACTIVITY: chưa đổi được proxy; giữ đường hiện tại."
                            raise RuntimeError(rot_res.get("error") or "PROXY_ROTATION_FAILED")
                        rotation_info["new_proxy"] = rot_res.get("proxy")
                        rotation_info["new_proxy_ip"] = rot_res.get("egress_ip")
                        rotation_info["flow_tab_reloaded"] = bool(rot_res.get("flow_tab_reloaded"))
                        _ledger.record_event("PROXY_ROTATE", nick=prof_id, detail={
                            "new_proxy": rot_res.get("proxy"),
                            "new_egress_ip": rot_res.get("egress_ip"),
                            "reason": "UNUSUAL_ACTIVITY",
                        })

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
                            # Fallback mode: the retry's captcha comes from the
                            # external solver, not the in-page mint.
                            if _config.CAPTCHA_SOLVER_MODE == "fallback":
                                self._solver_retry_pending.add(prof_id)
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
                                        minter = self._active_minter.pop(prof_id, None)
                                        if minter:
                                            self._minter_token_fails.pop(minter, None)
                                            self._mint_event("token_pass", minter=minter,
                                                             target=prof_id, rpc_id=rpc_id)
                                            logger.info("Foreign-minted captcha from %s passed for %s", minter, prof_id)
                                        nick_metrics.record_completion(prof_id, success=True, latency_ms=duration_ms, queue_ms=queue_ms)
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
                                    minter = self._active_minter.pop(prof_id, None)
                                    if minter:
                                        # A foreign token that still fails is
                                        # usually the TARGET's new IP still
                                        # being flagged, not the minter's token
                                        # being bad. Escalating cooldown: first
                                        # miss only benches the minter briefly;
                                        # consecutive misses converge on a
                                        # genuinely decayed minter at the full
                                        # cooldown. A token_pass resets it.
                                        n = self._minter_token_fails.get(minter, 0) + 1
                                        self._minter_token_fails[minter] = n
                                        cool_s = min(
                                            _config.CAPTCHA_MINTER_TOKEN_FAIL_COOL_S * n,
                                            _config.CAPTCHA_MINTER_FAIL_COOLDOWN_S)
                                        self._minter_fail_until[minter] = time.time() + cool_s
                                        self._mint_event("token_fail", minter=minter,
                                                         target=prof_id, rpc_id=rpc_id,
                                                         reason=str(retry_last.get("error") or "retry_failed")[:120])
                                        self._mint_event("cooldown", minter=minter,
                                                         target=prof_id,
                                                         cooldown_s=cool_s,
                                                         cause="token_fail")
                                        logger.warning(
                                            "Foreign-minted captcha from %s failed for %s; minter cooled %.0fs (streak %d)",
                                            minter, prof_id, cool_s, n,
                                        )
                                    rotation_info["retry_error"] = str(retry_last.get("error") or "")[:200]
                                    nick_metrics.record_completion(prof_id, success=False, latency_ms=duration_ms, queue_ms=queue_ms, error="UNUSUAL_ACTIVITY_RETRY_FAILED")
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
                    except _SessionFlagged:
                        pass
                    except Exception as rot_exc:
                        logger.error("Auto-rotation failed for %s: %s", prof_id, rot_exc)
                        rotation_info["rotation_error"] = str(rot_exc)
                        nick_metrics.record_completion(prof_id, success=False, latency_ms=duration_ms, queue_ms=queue_ms, error=str(rot_exc))

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

                    # Strike counter only — first-strike IP blame above plus
                    # resolving stale SESSION_FLAGGED rows on recovery. No
                    # park: the nick stays routable and each strike rotates the
                    # proxy until a request lands.
                    if prof_id in self._flag_released:
                        # It struck again right after hold-out release — the
                        # flag is deeper than one decay window. Escalate.
                        self._flag_released.discard(prof_id)
                        self._flag_depth[prof_id] = min(
                            self._flag_depth.get(prof_id, 0) + 1, 4
                        )
                        logger.warning(
                            "UNUSUAL_ACTIVITY on %s right after release — flag depth %d, "
                            "next hold-out %ds",
                            prof_id, self._flag_depth[prof_id],
                            _UNUSUAL_FLAG_DECAY_S << self._flag_depth[prof_id],
                        )
                    self._unusual_strikes[prof_id] = self._unusual_strikes.get(prof_id, 0) + 1
                    _ledger.record_event("STRIKE_UNUSUAL", nick=prof_id, detail={
                        "strikes": self._unusual_strikes[prof_id],
                        "rpc_id": rpc_id,
                        "rotated": rotation_info.get("rotation_triggered"),
                        "new_egress_ip": rotation_info.get("new_proxy_ip"),
                        "session_flagged": bool(rotation_info.get("skipped")),
                    })
                    if self._unusual_strikes[prof_id] == 2:
                        _ledger.record_event("HOLDOUT_ENTER", nick=prof_id, detail={
                            "reason": "unusual_strikes>=2",
                            "flag_depth": self._flag_depth.get(prof_id, 0),
                        })
                    # Trusted-minter fallback: this nick's next captcha call —
                    # whether the auto-retry above or a fresh dispatch — mints
                    # on a farm/sibling nick instead of the flagged session.
                    # Set at strike time so video submissions (which skip the
                    # in-band retry) still get a foreign token next attempt.
                    if _config.CAPTCHA_FOREIGN_MINT == "fallback":
                        self._foreign_mint_pending.add(prof_id)

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
                                nick_metrics.record_completion(prof_id, success=True, latency_ms=duration_ms, queue_ms=queue_ms)
                                return retry_last
                            last = retry_last
                            nick_metrics.record_completion(prof_id, success=False, latency_ms=duration_ms, queue_ms=queue_ms, error="RPC_13_RETRY_FAILED")
                        finally:
                            _current_route.reset(token_retry)
                    except Exception as r13_exc:
                        logger.error("Auto-retry on RPC [13] failed for %s: %s", prof_id, r13_exc)
                        nick_metrics.record_completion(prof_id, success=False, latency_ms=duration_ms, queue_ms=queue_ms, error=str(r13_exc))
                    finally:
                        self._in_retry_13 = False
                elif "PUBLIC_ERROR_MODEL_ACCESS_DENIED" in raw_err:
                    # The Google account was never granted the Veo models, so
                    # nothing was submitted and neither the session nor the IP
                    # is at fault: no auth strike, no unusual strike, no proxy
                    # quarantine, no rotation. Park the nick for video only —
                    # it can still upload, poll and generate images — and let
                    # _should_failover move this job to a nick that has access.
                    until = time.time() + _config.MODEL_DENIED_PARK_S
                    first = prof_id not in self._model_denied
                    self._model_denied[prof_id] = until
                    nick_metrics.record_completion(
                        prof_id, success=False, latency_ms=duration_ms, queue_ms=queue_ms,
                        error="MODEL_ACCESS_DENIED",
                    )
                    logger.warning(
                        "MODEL_ACCESS_DENIED on %s (%s): account has no access to the video "
                        "models; parked for video %ds, routing to another nick",
                        prof_id, raw_err[:80], _config.MODEL_DENIED_PARK_S,
                    )
                    if first:
                        try:
                            from agent.services.incident_manager import get_incident_manager
                            get_incident_manager().record_incident(
                                module="worker",
                                job_id=prof_id,
                                sub_id=prof_id,
                                severity="WARNING",
                                error_code="MODEL_ACCESS_DENIED",
                                message=(
                                    f"{prof_id} bị Flow từ chối model video "
                                    "(PUBLIC_ERROR_MODEL_ACCESS_DENIED) — account này chưa được "
                                    "cấp quyền render video"
                                ),
                                root_cause=(
                                    "Google account has no entitlement for the low-priority Veo "
                                    "models; the session and the proxy are both healthy"
                                ),
                                action_taken=f"VIDEO_PARKED_{_config.MODEL_DENIED_PARK_S}S",
                            )
                        except Exception:
                            pass

                elif raw_err:
                    nick_metrics.record_completion(prof_id, success=False, latency_ms=duration_ms, queue_ms=queue_ms, error=raw_err[:200])


            has_alternative = index + 1 < len(candidates)
            if (
                isinstance(last, dict)
                and self._should_failover(last)
                and allow_failover
                and not route.get("pinned")
                and has_alternative
            ):
                if route["ws"] in self._extensions:
                    # A nick carrying UNUSUAL strikes is likely session-flagged
                    # — bench it progressively longer (60s→30min cap) instead of
                    # letting it rejoin after a minute and eat the next job.
                    strikes_n = self._unusual_strikes.get(route.get("profile_id") or "", 0)
                    cool_s = min(60 * (2 ** (strikes_n - 1)), 1800) if strikes_n else 60
                    self._extensions[route["ws"]]["unavailable_until"] = (
                        time.time() + cool_s
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
            # This account cannot render video at all; another nick can.
            "public_error_model_access_denied",
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
                # Warm-up gate: a freshly launched/reloaded Flow tab cannot
                # serve jobs until it reports app_ready. Without this every
                # restart produced a NO_AT_TOKEN storm because the router
                # dispatched into cold tabs.
                sid_w = session.get("profile_id")
                if sid_w and sid_w not in self._minter_nicks():
                    session["warming_until"] = time.time() + 120
                    warm_task = session.get("warm_task")
                    if warm_task is None or warm_task.done():
                        session["warm_task"] = asyncio.create_task(
                            self._warm_probe(sid_w, source_ws)
                        )
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
        # Mint-only nicks never serve r2v — a chat session on them just burns
        # their RPC budget on pointless ListSessions/CreateSession retries.
        if profile_id and profile_id in self._minter_nicks():
            return
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
                    _ledger.record_event("BIND_OK", nick=profile_id)
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
            _ledger.record_event("BIND_FAIL", nick=profile_id, detail={
                "error": str(last_error)[:300],
            })

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
            token = await self._foreign_captcha_token(captcha_action)
            if token:
                params["captchaToken"] = token
        if match:
            params["match"] = match
        if path:
            params["path"] = path
        return await self._send("batch_rpc", params, timeout=timeout)

    def _find_session_route(self, profile_id: str) -> dict | None:
        """Route dict for a connected nick — including mint-only farm nicks,
        which _profile_candidates filters out."""
        now = time.time()
        for ws, session in self._extensions.items():
            if session.get("profile_id") != profile_id:
                continue
            return {
                "ws": ws,
                "profile_id": profile_id,
                "project_id": self._session_project(session),
                "pinned": True,
                "available": session.get("unavailable_until", 0) <= now,
                "in_flight": int(session.get("in_flight") or 0),
                "dispatched_count": int(session.get("dispatched_count") or 0),
                "cooldown_remaining": 0.0,
                "recency": session.get("connected_at") or 0,
            }
        return None

    def _minter_nicks(self) -> set[str]:
        """Mint-only nick set: env var union the per-account mint_only flag."""
        from agent.services.accounts import mint_only_nicks
        return set(_config.CAPTCHA_MINTER_NICKS) | set(mint_only_nicks())

    def _mint_event(self, event: str, minter: str | None = None,
                    target: str | None = None, **extra) -> None:
        """One mint-lifecycle event: update per-minter stats + JSONL audit."""
        now = time.time()
        if minter:
            st = self._minter_stats.setdefault(minter, {
                "mints_ok": 0, "mints_fail": 0, "token_pass": 0,
                "token_fail": 0, "cooldowns": 0, "skipped_cooled": 0,
                "skipped_rate": 0, "last_error": None,
                "last_latency_ms": None, "last_mint_at": None,
                "last_pass_at": None,
            })
            if event == "mint_ok":
                st["mints_ok"] += 1
                st["last_mint_at"] = now
                st["last_latency_ms"] = extra.get("latency_ms")
            elif event == "mint_fail":
                st["mints_fail"] += 1
                st["last_error"] = extra.get("reason")
            elif event == "token_pass":
                st["token_pass"] += 1
                st["last_pass_at"] = now
            elif event == "token_fail":
                st["token_fail"] += 1
                st["last_error"] = extra.get("reason")
            elif event == "cooldown":
                st["cooldowns"] += 1
            elif event == "skipped_cooled":
                st["skipped_cooled"] += 1
            elif event == "skipped_rate":
                st["skipped_rate"] += 1
        try:
            rec = {"ts": now, "event": event, "minter": minter,
                   "target": target, **extra}
            self._mint_audit_path.parent.mkdir(parents=True, exist_ok=True)
            with self._mint_audit_path.open("a") as fh:
                fh.write(json.dumps(rec, default=str) + "\n")
        except Exception:
            logger.debug("mint audit write failed", exc_info=True)
        _ledger.record_event("MINT_" + event.upper(), nick=minter,
                             detail={"target": target, **extra})

    def _minter_rate(self, sid: str) -> int:
        """Successful mints by this minter in the trailing 60 seconds."""
        dq = self._minter_mint_ts.get(sid)
        if not dq:
            return 0
        cutoff = time.time() - 60
        while dq and dq[0] < cutoff:
            dq.popleft()
        return len(dq)

    def minter_health(self) -> dict:
        """Live minter view for GET /api/flow/minters."""
        now = time.time()
        out = {}
        for sid in sorted(self._minter_nicks()):
            connected = any(
                s.get("profile_id") == sid for s in self._extensions.values())
            cooling = self._minter_fail_until.get(sid, 0)
            out[sid] = {
                "connected": connected,
                "page_dead": sid in self._minter_page_dead,
                "page_dead_since": self._minter_page_dead.get(sid),
                "cooling_until": cooling if cooling > now else None,
                "mints_last_60s": self._minter_rate(sid),
                **self._minter_stats.get(sid, {}),
            }
        return {"minters": out, "max_per_min": _config.CAPTCHA_MINTER_MAX_PER_MIN}

    def _minter_route(self, exclude: str | None) -> tuple | None:
        """Pick (route, skip_reasons) — the nick to mint a captcha token for
        another nick's request.

        With a minter pool set (env or mint_only accounts), only those nicks
        mint; otherwise any connected sibling may. Least-flagged, least-busy,
        then least-used first; minters on fail-cooldown or over the per-minute
        cap are skipped (and counted).
        """
        now = time.time()
        prefer = self._minter_nicks()
        best: tuple | None = None
        skipped: list[str] = []
        cap = _config.CAPTCHA_MINTER_MAX_PER_MIN
        for ws, session in self._extensions.items():
            sid = session.get("profile_id")
            if not sid or sid == exclude:
                continue
            if prefer and sid not in prefer:
                continue
            if sid in self._minter_page_dead:
                skipped.append("page_dead")
                self._mint_event("skipped_cooled", minter=sid, target=exclude)
                continue
            if self._minter_fail_until.get(sid, 0) > now:
                skipped.append("cooled")
                self._mint_event("skipped_cooled", minter=sid, target=exclude)
                continue
            if cap and self._minter_rate(sid) >= cap:
                skipped.append("rate_limited")
                self._mint_event("skipped_rate", minter=sid, target=exclude)
                continue
            route = {
                "ws": ws,
                "profile_id": sid,
                "project_id": self._session_project(session),
                "pinned": True,
                "available": session.get("unavailable_until", 0) <= now,
                "in_flight": int(session.get("in_flight") or 0),
                "dispatched_count": int(session.get("dispatched_count") or 0),
                "cooldown_remaining": 0.0,
                "recency": session.get("connected_at") or 0,
            }
            key = (self._unusual_strikes.get(sid, 0), route["in_flight"],
                   self._minter_use_count.get(sid, 0))
            if best is None or key < best[0]:
                best = (key, route)
        return (best[1], skipped) if best else (None, skipped)

    def _clone_depth(self, sid: str) -> int:
        """Generations away from the original account. Manual clones carry no
        clone_of but still count as one generation via the -dual suffix."""
        from agent.services.accounts import get_account
        depth, cur, seen = 0, sid, set()
        while cur and cur not in seen:
            seen.add(cur)
            row = get_account(cur) or {}
            parent = row.get("clone_of")
            if parent:
                depth += 1
                cur = parent
            else:
                if re.search(r"-dual", cur):
                    depth += 1
                break
        return depth

    def _schedule_terminal_clone(self, sid: str) -> None:
        """Fire-and-forget clone of a terminal nick onto a fresh IP."""
        if not _config.AUTO_CLONE_ON_TERMINAL or sid in self._clone_pending:
            return
        if self._clone_depth(sid) >= _config.AUTO_CLONE_MAX_DEPTH:
            # Lineage already replaced itself enough times — a deeper clone
            # would just burn another IP on an account-level flag.
            _ledger.record_event("CLONE_FAIL", nick=sid,
                                 detail={"reason": "depth_cap"})
            logger.warning("Not cloning %s: lineage depth cap reached", sid)
            return
        # A signed-out session clones into another signed-out session — the
        # dead cookies move with the profile. Only clone when the page isn't
        # provably signed out (unusual_wall / dead tab still benefit).
        ws = next((w for w, s in self._extensions.items()
                   if s.get("profile_id") == sid), None)
        if ws is not None and self._extensions[ws].get("page_state") == "signed_out":
            _ledger.record_event("CLONE_FAIL", nick=sid,
                                 detail={"reason": "signed_out_session"})
            logger.warning(
                "Not cloning %s: page is signed_out — cookies are dead "
                "server-side, a clone inherits the corpse. Needs re-login.", sid)
            return
        self._clone_pending.add(sid)
        try:
            asyncio.get_running_loop().create_task(self._clone_terminal_nick(sid))
        except RuntimeError:
            self._clone_pending.discard(sid)

    async def _clone_terminal_nick(self, sid: str) -> None:
        """Terminal clone wrapper — the source retires (disabled + pins
        remapped onto the clone)."""
        try:
            await self._clone_nick(sid, source_retires=True)
        finally:
            self._clone_pending.discard(sid)

    async def clone_nick(self, sid: str) -> dict:
        """Operator-triggered capacity clone. The source keeps its pins and
        comes back up after the profile copy — both instances serve the same
        Google account on different IPs."""
        if sid in self._clone_pending:
            return {"ok": False, "error": "clone already in progress"}
        if self._clone_depth(sid) >= _config.AUTO_CLONE_MAX_DEPTH:
            return {"ok": False, "error": "lineage depth cap reached"}
        ws = next((w for w, s in self._extensions.items()
                   if s.get("profile_id") == sid), None)
        if ws is not None and self._extensions[ws].get("page_state") == "signed_out":
            return {"ok": False,
                    "error": "signed_out session — a clone inherits dead cookies"}
        self._clone_pending.add(sid)
        try:
            clone_id = await self._clone_nick(sid, source_retires=False)
        finally:
            self._clone_pending.discard(sid)
        if not clone_id:
            return {"ok": False, "error": "clone failed — see ledger CLONE_FAIL"}
        return {"ok": True, "clone_id": clone_id}

    async def _clone_nick(self, sid: str, *, source_retires: bool) -> str | None:
        """Clone sid's profile onto a fresh proxy IP.

        The source Chrome is always stopped first — a live profile cannot be
        copied consistently. source_retires=True (terminal path): the source
        account is disabled and its media/operation pins move to the clone.
        False (capacity path): pins stay and the source is relaunched.
        Returns the clone id, or None on failure.
        """
        try:
            from agent.services.accounts import (
                get_account, upsert_account, load_accounts)
            from agent.services.chrome_nicks import (
                stop_nick, launch_nick, chrome_data_dir)
            from agent.services.proxy_pool import make_surfshark_url

            acc = get_account(sid)
            if not acc:
                return None

            # Parent's live page state at spawn time — if the clone is born
            # dead later, this tells whether we copied a corpse.
            parent_state = next(
                (s.get("page_state") for s in self._extensions.values()
                 if s.get("profile_id") == sid), None)

            # Clone id: base-<dual|dual-N>, first free.
            base = re.sub(r"-dual(?:-\d+)?$", "", sid)
            existing = {a.get("id") for a in load_accounts()}
            n = 1
            while True:
                clone_id = f"{base}-dual" if n == 1 else f"{base}-dual-{n}"
                if clone_id not in existing and not chrome_data_dir(clone_id).exists():
                    break
                n += 1
                if n > 20:
                    logger.error("clone_terminal %s: no free clone id", sid)
                    return None

            # Stop the original first so the profile copies cleanly and the
            # flagged session stops touching Google.
            try:
                await stop_nick(sid)
            except Exception as exc:
                logger.warning("clone_terminal %s: stop failed (%s) — continuing", sid, exc)

            src, dst = chrome_data_dir(sid), chrome_data_dir(clone_id)
            if not src.exists():
                _ledger.record_event("CLONE_FAIL", nick=sid,
                                     detail={"stage": "copy", "error": "profile dir missing"})
                return None
            tmp = dst.parent / (clone_id + ".copying")
            try:
                shutil.rmtree(tmp, ignore_errors=True)  # stale partial copy
                # Exclude the baked extension copy — it carries the parent's
                # profileId, so a cloned profile would connect AS the parent
                # and create a duplicate WS session. launch_nick regenerates
                # it with the clone's own id via sync_extension_copy.
                await asyncio.to_thread(
                    shutil.copytree, src, tmp,
                    ignore=shutil.ignore_patterns("FlowKitExtension"))
                # Sanity: a real profile has Preferences — a partial copy must
                # never be renamed into place and launched.
                if not (tmp / "Default" / "Preferences").exists() and not (tmp / "Preferences").exists():
                    raise RuntimeError("copied profile lacks Preferences")
                os.replace(tmp, dst)
            except Exception as exc:
                shutil.rmtree(tmp, ignore_errors=True)
                _ledger.record_event("CLONE_FAIL", nick=sid,
                                     detail={"stage": "copy", "error": str(exc)[:200]})
                logger.warning("clone_terminal %s: profile copy failed: %s", sid, exc)
                return None

            if source_retires:
                acc["enabled"] = False
                acc["note"] = (acc.get("note") or "") + " [auto] session terminal — cloned to " + clone_id
                upsert_account(acc)

            country = random.choice(_config.AUTO_CLONE_COUNTRIES or ["us"])
            row = {
                "id": clone_id,
                "label": f"{clone_id} ({'auto-' if source_retires else ''}clone of {sid})",
                "project_id": acc.get("project_id"),
                "proxy_url": make_surfshark_url(clone_id, country),
                "note": "", "enabled": True,
                "mint_only": bool(acc.get("mint_only")),
                "browser": acc.get("browser") or "",
                "clone_of": sid,
                "clone_ts": int(time.time()),
            }
            upsert_account(row)

            # Same Google account + same project → media/operation pins made
            # on the dead session resolve on the clone. A capacity clone keeps
            # the pins on the still-live source instead.
            if source_retires:
                for m, owner in list(self._media_profiles.items()):
                    if owner == sid:
                        self._media_profiles[m] = clone_id
                for o, owner in list(self._operation_profiles.items()):
                    if owner == sid:
                        self._operation_profiles[o] = clone_id
            # _operation_chat_sessions maps op→chat_session_id (a server-side
            # StreamChat conversation on the same Google account) — leave it:
            # the clone can keep the conversation. Remapping it to a nick id
            # would corrupt the lookup.
            try:
                self._save_media_profiles()
            except Exception:
                pass

            _ledger.record_event("CLONE_SPAWNED", nick=clone_id, detail={
                "from": sid, "country": country,
                "parent_state": parent_state,
                "mode": "terminal" if source_retires else "capacity",
                "proxy": row["proxy_url"][:80],
            })
            logger.warning(
                "Terminal nick %s → auto-cloned to %s on cr.%s; launching",
                sid, clone_id, country)

            res = await launch_nick(clone_id)
            if res.get("ok"):
                _ledger.record_event("CLONE_LAUNCHED", nick=clone_id)
            else:
                _ledger.record_event("CLONE_FAIL", nick=clone_id,
                                     detail={"error": res.get("error")})
                logger.warning("Clone %s launch failed: %s", clone_id, res.get("error"))
            if not source_retires:
                # Capacity clone — bring the source back up so it keeps working.
                try:
                    await launch_nick(sid)
                except Exception as exc:
                    logger.warning("relaunch of source %s failed: %s", sid, exc)
            return clone_id
        except Exception:
            logger.exception("auto-clone of %s failed", sid)
            _ledger.record_event("CLONE_FAIL", nick=sid, detail={"stage": "exception"})
            return None

    async def _canary_probe(self, sid: str) -> None:
        """One cheap RPC on a nick whose hold-out just elapsed.

        Clean answer → clear strikes and re-admit. UNUSUAL_ACTIVITY → re-arm
        the hold-out (the synthetic probe took the hit, not a paying request).
        Deep flags graduate straight to terminal.
        """
        try:
            ws = next((w for w, s in self._extensions.items()
                       if s.get("profile_id") == sid), None)
            session = self._extensions.get(ws) if ws else None
            if ws is None or session is None:
                # Offline nick — nothing to probe; release flags anyway since
                # routing filters disconnected nicks regardless.
                self._unusual_strikes[sid] = 0
                self._flag_released.add(sid)
                return
            pid = self._session_project(session) or FLOW_PROJECT_ID
            route = {"ws": ws, "profile_id": sid,
                     "project_id": pid, "pinned": True}
            flagged = False
            try:
                tok = _current_route.set(route)
                try:
                    await self._batch_payload(
                        fb.RPC_LIST_SESSIONS, fb.list_sessions_request(str(pid)),
                        timeout=30)
                finally:
                    _current_route.reset(tok)
            except Exception as exc:
                flagged = "UNUSUAL_ACTIVITY" in str(exc).upper()
            if flagged:
                # Re-arm the hold-out at the current escalation depth.
                self._unusual_strikes[sid] = 2
                self._unusual_strike_ts[sid] = time.time()
                if self._flag_depth.get(sid, 0) >= 2 and sid not in self._session_terminal:
                    self._session_terminal[sid] = time.time()
                    _ledger.record_event("SESSION_TERMINAL", nick=sid, detail={
                        "via": "canary", "flag_depth": self._flag_depth[sid],
                    })
                    self._schedule_terminal_clone(sid)
                    logger.warning(
                        "Canary on %s still flagged at flag_depth=%d — session terminal, "
                        "leaving rotation until re-login", sid, self._flag_depth[sid])
                else:
                    _ledger.record_event("CANARY_FAIL", nick=sid, detail={
                        "flag_depth": self._flag_depth.get(sid, 0),
                    })
                    logger.warning(
                        "Canary on %s still flagged — hold-out re-armed", sid)
            else:
                self._unusual_strikes[sid] = 0
                self._flag_released.add(sid)
                if self._session_terminal.pop(sid, None) is not None:
                    _ledger.record_event("TERMINAL_CLEARED", nick=sid,
                                         detail={"via": "canary_ok"})
                _ledger.record_event("CANARY_OK", nick=sid)
                _ledger.record_event("HOLDOUT_EXIT", nick=sid)
                logger.info("Canary on %s clean — nick re-admitted", sid)
        except Exception:
            logger.debug("canary probe failed for %s", sid, exc_info=True)
            # Inconclusive probe — release; the real strike path re-catches
            # genuine flags on the next request.
            self._unusual_strikes[sid] = 0
            self._flag_released.add(sid)
        finally:
            self._canary_pending.discard(sid)

    async def _warm_probe(self, sid: str, ws) -> None:
        """Poll a freshly connected Flow tab until it reports app_ready.

        The router keeps the nick unavailable (warming_until) while this
        runs, so real jobs never land on a cold tab. Runs until app_ready
        or the socket dies — a nick that never readies stays gated instead
        of aging back into routing and eating NO_AT_TOKEN failures.
        """
        try:
            while True:
                session = self._extensions.get(ws)
                if session is None or session.get("profile_id") != sid:
                    return
                try:
                    tok = _current_route.set(
                        {"ws": ws, "profile_id": sid, "pinned": True})
                    try:
                        health = await self._send(
                            "flow_tab_health", {}, timeout=8)
                    finally:
                        _current_route.reset(tok)
                except Exception:
                    health = {}
                res = (health or {}).get("result") or {}
                state = res.get("page_state")
                if state == "app_ready" or res.get("app_ready") is True:
                    session["warming_until"] = 0
                    session.pop("signed_out", None)
                    session.pop("tab_reload_tries", None)
                    self._ready_seen[sid] = time.time()
                    _ledger.record_event("EXT_WARMED", nick=sid,
                                         detail={"page_state": state})
                    return
                if state == "signed_out" and not session.get("signed_out"):
                    session["signed_out"] = True
                    _ledger.record_event("SIGNED_OUT", nick=sid, detail={
                        "via": "warm_probe",
                        "cause": self._signed_out_cause(sid),
                    })
                if (state is None and res.get("alive")
                        and "page_state" not in res):
                    # Legacy extension — "alive" only proves the content
                    # bridge answered; a grey Google error page answers too.
                    # Relaunch once per 20min so sync_extension_copy drops a
                    # current ext that actually reports page_state.
                    try:
                        from agent.services.accounts import get_account
                        disabled = not (get_account(sid) or {}).get("enabled", True)
                    except Exception:
                        disabled = False
                    if (not disabled
                            and time.time() - self._ext_refresh.get(sid, 0) > 1200):
                        self._ext_refresh[sid] = time.time()
                        _ledger.record_event("EXT_STALE_REFRESH", nick=sid, detail={
                            "guard": session.get("flow_guard_version")})
                        asyncio.create_task(self._relaunch_nick(sid))
                else:
                    # error_page / unknown / dead tab — reload clears
                    # transient Google 5xx and grey loads. Bounded: 3 tries,
                    # ≥60s apart; after that the gate just stays closed.
                    err_like = state in ("error_page", "unknown") \
                        or not res.get("alive")
                    tries = int(session.get("tab_reload_tries") or 0)
                    last = float(session.get("tab_reload_at") or 0)
                    # Grace: a tab still booting after connect reads
                    # alive:false — don't reload a page mid-load.
                    booting = time.time() - float(
                        session.get("connected_at") or 0) < 45
                    if (err_like and not booting and tries < 3
                            and time.time() - last > 60):
                        session["tab_reload_tries"] = tries + 1
                        session["tab_reload_at"] = time.time()
                        _ledger.record_event("TAB_RELOAD", nick=sid, detail={
                            "state": state, "try": tries + 1})
                        try:
                            tok = _current_route.set(
                                {"ws": ws, "profile_id": sid, "pinned": True})
                            try:
                                await self._send(
                                    "reload_flow_tab", {}, timeout=15)
                            finally:
                                _current_route.reset(tok)
                        except Exception:
                            pass
                # Anything that isn't app_ready — signed_out, unusual_wall,
                # loading, dead tab — keeps the gate closed while we probe.
                # A tab that can't serve work shouldn't take requests just
                # because the initial warm window elapsed.
                session["warming_until"] = time.time() + 45
                await asyncio.sleep(15)
        finally:
            session = self._extensions.get(ws)
            if session is not None:
                session.pop("warm_task", None)

    async def _relaunch_nick(self, sid: str) -> None:
        """Stop+start a nick's Chrome so sync_extension_copy refreshes its
        baked extension to the current code (page_state reporting)."""
        try:
            from agent.services.chrome_nicks import stop_nick, launch_nick
            await stop_nick(sid)
            res = await launch_nick(sid)
            if not res.get("ok"):
                _ledger.record_event("EXT_REFRESH_FAIL", nick=sid,
                                     detail={"error": res.get("error")})
        except Exception as exc:
            logger.warning("stale-ext relaunch of %s failed: %s", sid, exc)
            _ledger.record_event("EXT_REFRESH_FAIL", nick=sid,
                                 detail={"error": str(exc)[:200]})

    def _signed_out_cause(self, sid: str) -> dict:
        """Classify WHY a nick is signed_out, from evidence the kernel has.

        revoked_while_watching — warm probe saw app_ready on this boot, then
            the page flipped signed_out: Google killed the live session
            (concurrent-instance invalidation or forced expiry).
        revoked_after_idle — never ready on this boot, but the ledger shows
            successful work: session died while the nick was parked/offline.
        born_dead — never ready, never worked: the profile was cloned from
            (or restored into) an already-dead session.
        """
        ready_at = self._ready_seen.get(sid)
        last_ok = None
        try:
            oks = [o["ts"] for o in _ledger.recent_outcomes(
                nick=sid, hours=72, limit=300) if o["ok"]]
            last_ok = max(oks) if oks else None
        except Exception:
            pass
        base = re.sub(r"-dual(?:-\d+)?$", "", sid)
        siblings = sorted({
            s.get("profile_id") for s in self._extensions.values()
            if s.get("profile_id") and s["profile_id"] != sid
            and re.sub(r"-dual(?:-\d+)?$", "", s["profile_id"]) == base
        } - {None})
        if ready_at:
            kind = "revoked_while_watching"
        elif last_ok:
            kind = "revoked_after_idle"
        else:
            kind = "born_dead"
        now = time.time()
        return {
            "kind": kind,
            "ready_ago_s": int(now - ready_at) if ready_at else None,
            "last_ok_ago_s": int(now - last_ok) if last_ok else None,
            "siblings_alive": siblings,
        }

    async def _resolve_dup_ws(self, incumbent_ws, new_ws, profile_id: str) -> None:
        """Two sockets claim one profile — keep whichever actually answers.

        A live incumbent means the newcomer is a zombie reconnecting after
        a prior eviction (the extension reschedules a reconnect on every
        close), so the newcomer gets closed with 4001 to break the loop.
        A dead incumbent is the half-open socket a relaunched Chrome left
        behind — evict it and the fresh conn keeps the profile.
        """
        try:
            tok = _current_route.set(
                {"ws": incumbent_ws, "profile_id": profile_id, "pinned": True})
            try:
                probe = await self._send("flow_tab_health", {}, timeout=5)
            finally:
                _current_route.reset(tok)
        except Exception:
            probe = {}
        err = (probe or {}).get("error")
        # Any answered response — even UNKNOWN_METHOD from an old extension —
        # proves the incumbent socket is alive.
        incumbent_alive = not err or str(err).startswith("UNKNOWN_METHOD")
        loser, kept = (new_ws, "incumbent") if incumbent_alive \
            else (incumbent_ws, "new")
        if loser in self._extensions:
            self.clear_extension(loser)
            try:
                await loser.close(code=4001)
            except Exception:
                pass
        _ledger.record_event("WS_DUP_RESOLVED", nick=profile_id,
                             detail={"kept": kept})
        logger.warning("Duplicate WS for %s resolved — kept %s",
                       profile_id, kept)

    async def _minter_probe_loop(self) -> None:
        """Periodic Flow-page liveness check for every connected minter.

        The WS staying open does not prove the Flow tab is alive — a grey,
        discarded, or 401'd page still mints "tokens" that are all rejected.
        Probe via the extension's flow_tab_health; on dead, mark the minter
        out of routing and ask its own extension to reload the tab.
        """
        await asyncio.sleep(30)
        while True:
            try:
                await self._probe_minters()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.debug("minter probe sweep failed", exc_info=True)
            await asyncio.sleep(_config.CAPTCHA_MINTER_PROBE_SECS)

    async def _probe_minters(self) -> None:
        prefer = self._minter_nicks()
        if not prefer:
            return
        now = time.time()
        for ws, session in list(self._extensions.items()):
            sid = session.get("profile_id")
            if not sid or sid not in prefer:
                continue
            route = {"ws": ws, "profile_id": sid,
                     "project_id": self._session_project(session),
                     "pinned": True}
            tok = _current_route.set(route)
            res = await self._send("flow_tab_health", {}, timeout=15)
            _current_route.reset(tok)
            if res.get("error"):
                # Extension predates flow_tab_health — can't probe, but that's
                # not proof the page is dead; skip rather than eject it.
                if "UNKNOWN_METHOD" in str(res["error"]):
                    continue
                res = {"alive": False, "reason": "PROBE_FAILED",
                       "error": res["error"]}
            else:
                res = res.get("result") or {}
            alive = bool(res.get("alive"))
            if alive:
                if sid in self._minter_page_dead:
                    self._minter_page_dead.pop(sid, None)
                    self._mint_event("page_recovered", minter=sid)
                    logger.info("Minter %s Flow page recovered", sid)
                continue
            reason = res.get("reason") or "unknown"
            if sid not in self._minter_page_dead:
                self._minter_page_dead[sid] = now
                self._mint_event("page_dead", minter=sid, reason=reason,
                                 error=res.get("error"))
                logger.warning("Minter %s Flow page dead (%s) — excluding from "
                               "mint routing, reloading tab", sid, reason)
            # Reload debounce: at most one reload per RELOAD_SECS per minter.
            if now - self._minter_reload_at.get(sid, 0) < _config.CAPTCHA_MINTER_RELOAD_SECS:
                continue
            self._minter_reload_at[sid] = now
            tok = _current_route.set(route)
            rres = await self._send("reload_flow_tab", {}, timeout=30)
            _current_route.reset(tok)
            if rres.get("error") or rres.get("result", {}).get("ok") is False:
                logger.warning("Minter %s tab reload failed: %s",
                               sid, rres.get("error") or rres)
            else:
                logger.info("Minter %s tab reload dispatched", sid)

    async def _mint_raw(self, action: str, exclude: str | None) -> tuple[str | None, str | None]:
        """Mint one Enterprise token on a healthy sibling. (token, minter)."""
        route, skipped = self._minter_route(exclude=exclude)
        if not route:
            reason = ("no_minter_connected" if not skipped
                      else "all_minters_" + "_".join(sorted(set(skipped))))
            self._mint_event("no_minter", target=exclude, reason=reason)
            logger.warning("No minter available for %s: %s", exclude, reason)
            return None, None
        minter = route["profile_id"]
        tok = _current_route.set(route)
        started = time.time()
        try:
            res = await self._send("solve_captcha",
                                   {"captchaAction": action}, timeout=30)
        except Exception as exc:
            # _send can raise (ws dropped mid-mint) — count it and let the
            # caller fall back to in-page minting instead of failing the job.
            self._mint_event("mint_fail", minter=minter, target=exclude,
                             reason=f"exception:{type(exc).__name__}",
                             action=action)
            self._minter_fail_until[minter] = time.time() + 300
            logger.warning("Foreign captcha mint raised on %s for %s: %s",
                           minter, exclude, exc)
            return None, None
        finally:
            _current_route.reset(tok)
        latency_ms = (time.time() - started) * 1000
        token = (res.get("result") or res).get("token")
        if token:
            self._minter_mint_ts.setdefault(minter, deque()).append(time.time())
            self._minter_use_count[minter] = self._minter_use_count.get(minter, 0) + 1
            self._mint_event("mint_ok", minter=minter, target=exclude,
                             action=action, latency_ms=round(latency_ms, 1),
                             token_len=len(token))
            return token, minter
        # Mint itself failing = page/env problem on the minter side.
        reason = str((res.get("result") or res).get("error") or "no_token")
        self._minter_fail_until[minter] = time.time() + 300
        self._mint_event("mint_fail", minter=minter, target=exclude,
                         reason=reason, action=action,
                         latency_ms=round(latency_ms, 1))
        self._mint_event("cooldown", minter=minter, target=exclude,
                         cooldown_s=300, cause="mint_fail")
        logger.warning("Foreign captcha mint failed on %s for %s: %s",
                       minter, exclude, reason)
        return None, None

    async def _mint_on_sibling(self, action: str, target_prof: str | None) -> str | None:
        """Mint an Enterprise token inside another nick's signed-in Flow tab."""
        token, minter = await self._mint_raw(action, exclude=target_prof)
        if token and target_prof:
            self._active_minter[target_prof] = minter
            logger.info("Foreign captcha minted on %s for %s action=%s",
                        minter, target_prof, action)
        return token

    async def _token_pool_loop(self) -> None:
        """Keep a few fresh foreign tokens warm per action.

        A strike retry used to mint serially inside the retry path — the pool
        holds ready tokens (per action, since reCAPTCHA validates it) so the
        retry consumes one instantly. Entries expire at TOKEN_TTL_S.
        """
        await asyncio.sleep(20)
        while True:
            try:
                if _config.CAPTCHA_FOREIGN_MINT != "off" and self._minter_nicks():
                    await self._pool_tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.debug("token pool tick failed", exc_info=True)
            await asyncio.sleep(_config.CAPTCHA_TOKEN_POOL_TICK_S)

    async def _pool_tick(self) -> None:
        now = time.time()
        for action in (fb.CAPTCHA_IMAGE, fb.CAPTCHA_VIDEO, fb.CAPTCHA_CHAT):
            dq = self._token_pool.setdefault(action, deque())
            while dq and now - dq[0]["minted_at"] > _config.CAPTCHA_TOKEN_TTL_S:
                dq.popleft()
            while len(dq) < _config.CAPTCHA_TOKEN_POOL_SIZE:
                if not await self._pool_mint(action, dq):
                    break

    async def _pool_mint(self, action: str, dq: deque) -> bool:
        """Mint one token into the pool. Serialized so a tick burst and a
        strike prefetch cannot pile onto the same minter."""
        async with self._token_pool_lock:
            if len(dq) >= _config.CAPTCHA_TOKEN_POOL_SIZE:
                return True
            token, minter = await self._mint_raw(action, exclude=None)
            if not token:
                return False
            dq.append({"token": token, "minter": minter,
                       "minted_at": time.time()})
            return True

    async def _pool_prefetch(self, action: str) -> None:
        """Strike-time top-up: mint while the proxy rotates so the retry
        finds a warm token instead of minting after the swap."""
        try:
            dq = self._token_pool.setdefault(action, deque())
            if len(dq) >= _config.CAPTCHA_TOKEN_POOL_SIZE:
                return
            await self._pool_mint(action, dq)
        except Exception:
            logger.debug("pool prefetch failed for %s", action, exc_info=True)

    def _pool_take(self, action: str, prof_id: str | None) -> str | None:
        """Pop the freshest live pooled token for ``action``, or None."""
        dq = self._token_pool.get(action)
        if not dq:
            return None
        now = time.time()
        while dq:
            entry = dq.pop()
            if now - entry["minted_at"] <= _config.CAPTCHA_TOKEN_TTL_S:
                if prof_id:
                    self._active_minter[prof_id] = entry["minter"]
                self._mint_event("pool_hit", minter=entry["minter"],
                                 target=prof_id, action=action,
                                 age_s=round(now - entry["minted_at"], 1))
                return entry["token"]
        self._mint_event("pool_miss", target=prof_id, action=action)
        return None

    async def _rebind_media(self, media_id: str, owner: str,
                            target: str, project_id: str) -> str | None:
        """Move a ref image onto ``target``'s account: fetch the bytes through
        the owner's still-connected tab, re-upload into the target's project.

        A media id only exists inside the owning Google account's project, so
        a ref created on a now-mint-only (or otherwise unroutable) nick is
        invisible to the gen fleet — re-upload is the only way to rebind it.
        """
        src = self._find_session_route(owner)
        dst = self._find_session_route(target)
        if not src or not dst:
            return None
        tok = _current_route.set(src)
        try:
            urls = await self._batch_media_urls(media_id)
        except Exception as exc:
            logger.warning("rebind %s: url fetch on %s failed: %s",
                           media_id[:8], owner, exc)
            return None
        finally:
            _current_route.reset(tok)
        if not urls.image:
            return None
        try:
            blob, mime = await asyncio.to_thread(_fetch_url_bytes, urls.image, 60)
        except Exception as exc:
            logger.warning("rebind %s: download failed: %s", media_id[:8], exc)
            return None
        pid = dst.get("project_id") or self._batch_project_id(project_id)
        b64 = base64.b64encode(blob).decode()
        tok = _current_route.set(dst)
        try:
            payload = await self._batch_payload(
                fb.RPC_UPLOAD_IMAGE,
                fb.upload_request(b64, pid, mime, f"rebind-{media_id[:8]}"),
                fb.CAPTCHA_IMAGE, timeout=120)
        except Exception as exc:
            logger.warning("rebind %s: upload on %s failed: %s",
                           media_id[:8], target, exc)
            return None
        finally:
            _current_route.reset(tok)
        new_mid = fb.read_uploaded_media_id(payload)
        if new_mid:
            self._remember_media(new_mid, profile_id=target)
            return new_mid
        return None

    async def _consolidate_media_ids(self, media_ids: list[str] | None,
                                     project_id: str = "") -> list[str]:
        """Rebind refs so every id belongs to the nick that will run the call.

        Two poison shapes: refs spread across multiple owner accounts, and refs
        owned by a nick that cannot serve work (mint_only farm, disabled, or
        disconnected). Both strand the call with MEDIA_PROFILE_MISMATCH /
        NO_FLOW_TAB — the media physically live in another account's project.
        Fetch via the owner's tab and re-upload onto the target nick instead.
        """
        mids = [m for m in (media_ids or []) if m]
        if not mids:
            return list(media_ids or [])
        owners = {m: self._media_profiles.get(m) for m in mids}
        known = {o for o in owners.values() if o}
        if not known:
            return list(media_ids or [])

        _, cands = self._profile_candidates(project_id=project_id)
        routable = {str(c.get("profile_id")) for c in cands if c.get("profile_id")}

        # Target: the pin when it is routable, else the majority routable
        # owner (fewest re-uploads), else the first available candidate.
        from collections import Counter
        pin = self._resolve_pin(project_id=project_id)
        target = None
        if pin and pin in routable:
            target = pin
        else:
            common = Counter(o for o in owners.values() if o in routable)
            if common:
                target = common.most_common(1)[0][0]
            else:
                for c in cands:
                    if c.get("available") and c.get("profile_id"):
                        target = c["profile_id"]
                        break
                if not target and cands:
                    target = cands[0].get("profile_id")
        if not target:
            return list(media_ids or [])

        out, rebound = [], 0
        for m in mids:
            owner = owners[m]
            keep = (owner is None or owner == target
                    or (owner in routable and len(known) == 1))
            if keep:
                out.append(m)
                continue
            new_mid = await self._rebind_media(m, owner, target, project_id)
            out.append(new_mid or m)
            if new_mid:
                rebound += 1
        if rebound:
            logger.info(
                "Rebound %d ref media onto %s (owners were: %s)",
                rebound, target, sorted(known))
        return out

    async def _foreign_captcha_token(self, action: str) -> str | None:
        """Token minted outside the requesting nick's own page.

        Trusted-minter pool (CAPTCHA_FOREIGN_MINT): a token minted in a healthy
        sibling's logged-in Flow tab — measured to rescue flagged nicks.
        External solver (CAPTCHA_SOLVER_MODE) stays as a dormant second source.
        ``fallback`` limits both to the auto-retry after UNUSUAL_ACTIVITY.
        Any failure falls back to the in-page mint so nothing can stop
        generation entirely.
        """
        prof_id = (_current_route.get() or {}).get("profile_id")
        mode_f = _config.CAPTCHA_FOREIGN_MINT
        mode_s = _config.CAPTCHA_SOLVER_MODE
        allowed_f = mode_f == "always" or (
            mode_f == "fallback" and prof_id in self._foreign_mint_pending)
        allowed_s = bool(_config.CAPTCHA_SOLVER_API_KEY) and (
            mode_s == "always" or
            (mode_s == "fallback" and prof_id in self._solver_retry_pending))
        self._foreign_mint_pending.discard(prof_id)
        self._solver_retry_pending.discard(prof_id)
        if allowed_f:
            token = self._pool_take(action, prof_id)
            if not token:
                token = await self._mint_on_sibling(action, prof_id)
            if token:
                return token
        if allowed_s:
            try:
                from agent.services.captcha_solver import solve_recaptcha_v3
                token = await solve_recaptcha_v3(action)
                logger.info("Solver captcha token minted for %s action=%s", prof_id, action)
                return token
            except Exception as exc:
                logger.warning("Solver token failed for %s (%s); in-page mint fallback", prof_id, exc)
        return None

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
                               image_model: str = None,
                               profile_id: str | None = None) -> dict:
        """Generate image(s).

        ``character_media_ids`` are attached as reference images, which is what
        keeps an entity the same across scenes. Response is shaped like the
        old REST one so the parsers downstream do not have to care which
        transport produced it.
        """
        if not USE_BATCH_RPC:
            return await self._legacy_generate_images(
                prompt, project_id, aspect_ratio, user_paygate_tier, character_media_ids)

        refs = await self._consolidate_media_ids(
            list(character_media_ids or []), project_id)

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
                run, project_id, media_ids=refs, allow_failover=True,
                profile_id=profile_id)
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

        start = (await self._consolidate_media_ids([start], project_id))[0]

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

        refs = await self._consolidate_media_ids(list(reference_media_ids), project_id)

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
        dead_until = self._dead_media.get(media_id, 0)
        if dead_until > time.time():
            return {"status": 404, "error": f"No urls for media {media_id} (dead-cached)"}
        async def run(_pid: str):
            urls = await self._batch_media_urls(media_id)
            if not urls.video and not urls.image:
                if media_id not in self._dead_media:
                    _ledger.record_event("MEDIA_DEAD", detail={"media_id": media_id})
                self._dead_media[media_id] = time.time() + 3600
                if len(self._dead_media) > 4096:
                    now = time.time()
                    for k in [k for k, v in self._dead_media.items() if v <= now][:2048]:
                        self._dead_media.pop(k, None)
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
            # A reference owned by an unroutable nick (mint_only/disabled) can
            # no longer bind this upload — rebind the reference itself first.
            reference_media_id = (
                await self._consolidate_media_ids([reference_media_id], project_id))[0]
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

    async def test_cross_captcha(self, mint_worker: str, use_worker: str,
                                 foreign_token: str | None = None,
                                 kind: str = "upload") -> dict:
        """Mint a captcha token on ``mint_worker``, spend it on ``use_worker``.

        Uses the upload RPC (maseQ): carries a captcha but costs no gen quota.
        Pass mint_worker == use_worker for the domestic control. The answer
        decides whether external minting can ever help: if a foreign token
        fails where a domestic one passes, Enterprise tokens are
        environment-bound and no solver/minter architecture can work.
        """
        from agent.services import flow_batch as fb
        from agent.services.accounts import get_account

        # 1x1 PNG — the upload carries a captcha but costs no generation quota.
        TINY_PNG = (
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDw"
            "AEhQGAhKmMIQAAAABJRU5ErkJggg=="
        )
        acc = get_account(use_worker)
        project_id = (acc or {}).get("project_id") or ""
        if not project_id:
            return {"ok": False, "stage": "setup",
                    "error": f"{use_worker} has no project_id"}

        mint_route = self._find_session_route(mint_worker)
        _, use_cands = self._profile_candidates(profile_id=use_worker)
        if not mint_route or not use_cands:
            return {"ok": False, "stage": "setup",
                    "error": "worker not connected"}

        if foreign_token:
            token = foreign_token
        else:
            tok = _current_route.set(mint_route)
            try:
                res = await self._send("solve_captcha",
                                       {"captchaAction": fb.CAPTCHA_IMAGE}, timeout=30)
            finally:
                _current_route.reset(tok)
            token = (res.get("result") or res).get("token")
            if not token:
                return {"ok": False, "stage": "mint", "mint_on": mint_worker,
                        "error": (res.get("result") or res).get("error")}

        if kind == "gen":
            # ogiZ0b — real image generation, costs one image gen. This is the
            # RPC that actually evaluates the captcha: upload (maseQ) accepts
            # even garbage tokens, so only gen proves token validity.
            freq = fb.image_request(
                "a small grey square", project_id, count=1,
                model=self._batch_image_model(None))
            rpcid = fb.RPC_GEN_IMAGE
        else:
            freq = fb.upload_request(TINY_PNG, project_id, "image/png",
                                     "cross-captcha-probe.png")
            rpcid = fb.RPC_UPLOAD_IMAGE
        tok = _current_route.set(use_cands[0])
        try:
            out = await self._send("batch_rpc", {
                "rpcid": rpcid, "freq": freq,
                "captchaAction": fb.CAPTCHA_IMAGE, "captchaToken": token,
            }, timeout=180)
        finally:
            _current_route.reset(tok)
        return {"ok": not out.get("error"), "mint_on": mint_worker,
                "use_on": use_worker, "kind": kind, "result": out}

    async def test_recaptcha_token(self, worker_id: str = "nick-a", action: str = "FLOW_PROBE") -> dict:
        """Test minting 1 live reCAPTCHA Enterprise token through Chrome extension on worker_id."""
        # _find_session_route reaches mint-only farm nicks too, which
        # _profile_candidates filters out of work routing.
        route = self._find_session_route(worker_id)
        if not route:
            return {"ok": False, "error": f"Worker {worker_id} not connected"}
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
