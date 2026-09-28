"""Per-nick (phone account) observability for the direct gRPC transport.

Reads the bounded jsonl diagnostics emitted by flow_trace (no prompts, no
tokens, no URLs) and exposes:

  GET /api/admin/nicks  — live scheduler state + per-nick counters
  GET /api/admin/trace  — recent trace events, filterable by nick/errors
  GET /admin            — single-file monitoring page (agent/static/admin.html)
"""
import json
import logging
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse

from agent.config import BASE_DIR

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin", tags=["admin"])

TRACE_FILE = Path(BASE_DIR) / ".scratch" / "flow-trace-api.jsonl"
_ADMIN_HTML = Path(__file__).resolve().parents[1] / "static" / "admin.html"

_ERROR_EVENTS = {"grpc.facade.error", "grpc.rpc.error", "grpc.error",
                 "http.exception"}
_OK_EVENTS = {"grpc.facade.end", "grpc.rpc.end"}


def _tail_events(max_lines: int = 5000) -> list[dict]:
    """Tail the rotating jsonl trace; returns events oldest -> newest."""
    events = deque(maxlen=max_lines)
    try:
        with TRACE_FILE.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except ValueError:
                    continue
    except FileNotFoundError:
        pass
    return list(events)


def _is_error(ev: dict) -> bool:
    if ev.get("event") in _ERROR_EVENTS:
        return True
    if ev.get("event") == "http.end":
        st = ev.get("status")
        if isinstance(st, int) and st >= 400:
            return True
        res = ev.get("result") or {}
        if res.get("has_error"):
            return True
    return False


@router.get("/nicks")
def admin_nicks():
    """Live scheduler state + aggregated stats per nick (from trace tail)."""
    try:
        from agent.services.flow_grpc import get_flow_grpc
        t = get_flow_grpc()
        t.reload()
    except Exception as e:
        t = None
        logger.warning("admin_nicks: transport unavailable: %s", e)

    rows: dict[str, dict] = {}
    if t:
        now = time.time()
        for name in t.nicks():
            nick = t._nicks.get(name)
            rows[name] = {
                "nick": name,
                "project": getattr(nick, "project_uuid", ""),
                "proxy": bool(getattr(nick, "proxy", None)),
                "inflight": t._inflight.get(name, 0),
                "cooldown_s": max(
                    0, int(t._throttle_until.get(name, 0) - now)),
                "app_version": (getattr(nick, "meta", {}) or {}).get(
                    "app_version", "") if nick else "",
            }

    # VN local day bucket — user quota intuition runs on local days.
    vn = timezone(timedelta(hours=7))
    today = datetime.now(vn).strftime("%Y-%m-%d")

    for ev in _tail_events():
        nick = ev.get("nick")
        if not nick:
            continue
        s = rows.setdefault(nick, {"nick": nick, "live": False})
        s.setdefault("ok", 0)
        s.setdefault("errors", 0)
        s["last_ts"] = ev.get("ts")
        # daily gen counters
        ts = ev.get("ts") or ""
        try:
            day = datetime.fromisoformat(
                ts.replace("Z", "+00:00")).astimezone(vn).strftime("%Y-%m-%d")
        except Exception:
            day = ""
        if day == today:
            if ev["event"] == "grpc.facade.end":
                op = ev.get("op", "")
                if op == "generate_images":
                    s["today_images"] = s.get("today_images", 0) + len(
                        ev.get("media") or [1])
                elif op in ("t2v", "i2v", "r2v"):
                    s["today_videos"] = s.get("today_videos", 0) + len(
                        ev.get("ops") or [1])
            elif ev["event"] == "grpc.facade.error" and "THROTTLED" in str(
                    ev.get("error", "")).upper():
                s.setdefault("first_throttled_ts", ev.get("ts"))
                s["throttled_today"] = s.get("throttled_today", 0) + 1
        if ev["event"] in _OK_EVENTS:
            s["ok"] = s.get("ok", 0) + 1
            s["last_op"] = ev.get("op") or ev.get("rpc")
        elif ev["event"] in _ERROR_EVENTS:
            s["errors"] = s.get("errors", 0) + 1
            s["last_error"] = str(ev.get("error") or ev.get("detail")
                                  or ev.get("exception_type") or "")[:300]
            s["last_error_ts"] = ev.get("ts")
            s["last_error_op"] = ev.get("op") or ev.get("rpc")

    return {
        "nicks": sorted(rows.values(), key=lambda r: r["nick"]),
        "waiters": getattr(t, "_waiters", 0) if t else 0,
        "max_per_nick": 12,
        "today": today,
        "trace_file": str(TRACE_FILE),
    }


@router.get("/trace")
def admin_trace(nick: str = "", errors: bool = False,
                limit: int = Query(200, le=2000)):
    """Recent trace events, newest first.

    ``nick`` also pulls the http.* events of that nick's requests by
    resolving trace_id -> nick from grpc.* events.
    """
    events = _tail_events()
    if nick:
        tids = {e.get("trace_id") for e in events
                if e.get("nick") == nick and e.get("trace_id")}
        events = [e for e in events
                  if e.get("nick") == nick or e.get("trace_id") in tids]
    if errors:
        events = [e for e in events if _is_error(e)]
    return {"events": list(reversed(events))[:limit]}


@router.get("", response_class=HTMLResponse, include_in_schema=False)
def admin_page():
    if _ADMIN_HTML.exists():
        return HTMLResponse(_ADMIN_HTML.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>admin.html missing</h1>", status_code=500)
