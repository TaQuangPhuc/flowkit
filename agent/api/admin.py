"""Per-nick (phone account) observability for the direct gRPC transport.

Reads the bounded jsonl diagnostics emitted by flow_trace (no prompts, no
tokens, no URLs) and exposes:

  GET /api/admin/nicks  — live scheduler state + per-nick counters
  GET /api/admin/trace  — recent trace events, filterable by nick/errors
  GET /admin            — single-file monitoring page (agent/static/admin.html)
"""
import json
import logging
import secrets
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Header, Query, Request
from fastapi.responses import HTMLResponse

from agent.config import BASE_DIR
from agent.services import flow_stats

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin", tags=["admin"])

TRACE_GLOB = "flow-trace-api*.jsonl"
EXCHANGE_GLOB = "flow-exchange-api*.jsonl"


def _log_files(pattern: str) -> list[Path]:
    """Dual-serve agents each write flow-*-<port>.jsonl; glob merges both
    (plus the unsuffixed local-dev file)."""
    d = Path(BASE_DIR) / ".scratch"
    return sorted(p for p in d.glob(pattern) if p.is_file())
_ADMIN_HTML = Path(__file__).resolve().parents[1] / "static" / "admin.html"

_ERROR_EVENTS = {"grpc.facade.error", "grpc.rpc.error", "grpc.error",
                 "http.exception", "grpc.poll.rejected"}
_OK_EVENTS = {"grpc.facade.end", "grpc.rpc.end"}


def _read_tail_lines(path: Path, max_lines: int) -> list[str]:
    """Read only the last `max_lines` lines of a jsonl file by walking
    backwards in 1MB blocks. The trace file grows to tens of MB under
    load — a full scan per admin poll is what made /admin laggy."""
    try:
        size = path.stat().st_size
    except OSError:
        return []
    buf = b""
    with path.open("rb") as f:
        pos = size
        while pos > 0 and buf.count(b"\n") <= max_lines:
            step = min(1 << 20, pos)
            pos -= step
            f.seek(pos)
            buf = f.read(step) + buf
    lines = buf.split(b"\n")
    if pos > 0:
        lines = lines[1:]  # first line is a partial cut
    return [ln.decode("utf-8", errors="replace") for ln in lines[-max_lines:]]


def _tail_events(max_lines: int = 5000) -> list[dict]:
    """Tail the rotating jsonl trace(s); returns events oldest -> newest."""
    events = []
    for path in _log_files(TRACE_GLOB):
        for line in _read_tail_lines(path, max_lines):
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except ValueError:
                continue
    events.sort(key=lambda e: e.get("ts", ""))
    return events[-max_lines:]


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
                "platform": (getattr(nick, "meta", {}) or {}).get(
                    "platform", "iphone") if nick else "iphone",
                "inflight": t._inflight.get(name, 0),
                "cooldown_s": max(
                    0, int(t._throttle_until.get(name, 0) - now)),
                "cooldown_off": name in getattr(t, "_no_cooldown", set()),
                "throttle_streak": t._throttle_streak.get(name, 0),
                "app_version": (getattr(nick, "meta", {}) or {}).get(
                    "app_version", "") if nick else "",
            }
    try:
        from agent.services import vids_client
        if vids_client.NICKS_DIR.exists():
            for vd in sorted(vids_client.NICKS_DIR.iterdir()):
                if not vd.is_dir() or vd.name.startswith("_"):
                    continue
                q = vids_client.load_vids_quota(vd.name)
                if vd.name in rows:
                    if q:
                        rows[vd.name]["credits_left"] = q.get("credits_left")
                        rows[vd.name]["credits_total"] = q.get("credits_total")
                elif (vd / "vids_cookies.json").exists() and (vd / "vids_doc.txt").exists():
                    rows[vd.name] = {
                        "nick": vd.name,
                        "project": (vd / "vids_doc.txt").read_text().strip()[:16],
                        "proxy": (vd / "proxy.txt").exists(),
                        "platform": "iphone",
                        "vids_only": True,
                        "inflight": 0,
                        "cooldown_s": 0,
                        "cooldown_off": True,
                        "throttle_streak": 0,
                        "session_dead": vids_client.is_nick_dead(vd.name),
                        "credits_left": q.get("credits_left"),
                        "credits_total": q.get("credits_total"),
                    }
    except Exception:
        pass

    # VN local day bucket — user quota intuition runs on local days.
    # Daily counts come from the persistent counter file; the trace tail
    # only covers the last ~30 min under load.
    today = flow_stats.today()
    for nick, c in flow_stats.day_counts(today).items():
        if t and nick not in rows:
            continue
        s = rows.setdefault(nick, {"nick": nick, "live": False})
        if c.get("images"):
            s["today_images"] = c["images"]
        if c.get("videos"):
            s["today_videos"] = c["videos"]
        if c.get("rejected"):
            s["rejected_today"] = c["rejected"]
        if c.get("policy"):
            s["policy_today"] = c["policy"]
        if c.get("throttled"):
            s["throttled_today"] = c["throttled"]
            s["first_throttled_ts"] = c.get("first_throttled_ts")

    for ev in _tail_events():
        nick = ev.get("nick")
        if not nick or (t and nick not in rows):
            continue
        s = rows.setdefault(nick, {"nick": nick, "live": False})
        s.setdefault("ok", 0)
        s.setdefault("errors", 0)
        s["last_ts"] = ev.get("ts")
        if ev["event"] in _OK_EVENTS:
            s["ok"] = s.get("ok", 0) + 1
            s["last_op"] = ev.get("op") or ev.get("rpc")
        elif ev["event"] in _ERROR_EVENTS:
            err_str = str(ev.get("error") or ev.get("detail")
                          or ev.get("exception_type") or "")[:300]
            from agent.services.flow_grpc import _POLICY_RE
            if _POLICY_RE.search(err_str):
                continue
            s["errors"] = s.get("errors", 0) + 1
            s["last_error"] = err_str
            s["last_error_ts"] = ev.get("ts")
            s["last_error_op"] = ev.get("op") or ev.get("rpc")

    return {
        "nicks": sorted(rows.values(), key=lambda r: r["nick"]),
        "waiters": getattr(t, "_waiters", 0) if t else 0,
        "queued_images": (getattr(t, "_waiters_ops", {})
                          .get("image", 0)) if t else 0,
        "queued_videos": (getattr(t, "_waiters_ops", {})
                          .get("video", 0)) if t else 0,
        "max_per_nick": 12,
        "today": today,
        "trace_file": [str(p) for p in _log_files(TRACE_GLOB)],
    }


@router.post("/nicks/{name}/park")
def admin_park_nick(name: str, seconds: int = Query(1800, ge=0, le=86400)):
    """Park a nick for SUBMITS: sets its throttle cooldown without a real
    THROTTLED event. Unpinned work avoids it; pinned i2v refs rebase onto
    healthy nicks (anchor prefers non-cooling owners). Polls/downloads and
    profile_id-pinned probes still run — parking is a scheduling hint, not
    a hard block. `seconds=0` unparks immediately.
    Streak is raised while parked so a stray THROTTLED probe re-parks the
    nick (geometric backoff) instead of resetting the park early."""
    try:
        from agent.services.flow_grpc import get_flow_grpc
        t = get_flow_grpc()
        t.reload()
    except Exception as e:
        return {"status": 503, "error": f"transport unavailable: {e}"}
    if name not in t._nicks:
        return {"status": 404, "error": f"unknown nick {name}"}
    if seconds == 0:
        t.set_park(name, 0)
        return {"nick": name, "parked": False}
    until = time.time() + seconds
    t.set_park(name, until)  # persisted to nicks/_parked.json
    t._throttle_streak[name] = max(t._throttle_streak.get(name, 0), 6)
    return {"nick": name, "parked": True, "cooldown_s": seconds,
            "until": until}


_PUSH_SECRET_CACHE = [0.0, ""]


def _push_secret() -> str:
    """FLOWKIT_PUSH_SECRET lives in secrets.env (root-only, not exported to
    the process env) — re-read at most once a minute."""
    if time.time() - _PUSH_SECRET_CACHE[0] < 60:
        return _PUSH_SECRET_CACHE[1]
    val = ""
    try:
        for ln in (BASE_DIR / "secrets.env").read_text().splitlines():
            if ln.startswith("FLOWKIT_PUSH_SECRET="):
                val = ln.split("=", 1)[1].strip().strip('"').strip("'")
    except Exception:
        pass
    _PUSH_SECRET_CACHE[0] = time.time()
    _PUSH_SECRET_CACHE[1] = val
    return val


@router.post("/nicks/{name}/ya29")
async def admin_push_ya29(name: str, request: Request,
                          x_push_key: str = Header("")):
    """Receive a ya29 minted ON THE PHONE and install it on the nick.
    aas_et minted off-device gets BadAuthentication within ~a day, so the
    owning Android pushes fresh ya29s every ~35min. The token is written to
    nicks/_ya29.json so the sibling agent adopts it via _refresh_flags and
    it survives restarts. Auth: X-Push-Key must equal FLOWKIT_PUSH_SECRET
    in secrets.env — unset means disabled (fail closed)."""
    secret = _push_secret()
    if not secret:
        return {"status": 503, "error": "push key not configured"}
    if not secrets.compare_digest(x_push_key, secret):
        return {"status": 401, "error": "unauthorized"}
    try:
        body = await request.json()
    except Exception:
        return {"status": 400, "error": "bad json"}
    token = str(body.get("token") or "")
    if not token.startswith("ya29."):
        return {"status": 400, "error": "token must be a ya29.* access token"}
    try:
        exp_in = float(body.get("expires_in") or 3600)
    except (TypeError, ValueError):
        exp_in = 3600
    try:
        from agent.services.flow_grpc import get_flow_grpc
        t = get_flow_grpc()
        t.reload()
    except Exception as e:
        return {"status": 503, "error": f"transport unavailable: {e}"}
    nick = t._nicks.get(name)
    if nick is None:
        return {"status": 404, "error": f"unknown nick {name}"}
    exp = time.time() + min(max(exp_in, 60), 3900) - 120
    nick._ya29 = token
    nick._ya29_exp = exp
    t._save_pushed_token(name, token, exp)
    t.set_park(name, 0)  # fresh cred — drop any auth-dead park
    try:
        from agent.services.flow_trace import emit as _trace_emit
        _trace_emit("grpc.nick.ya29_push", nick=name,
                    exp_in_s=int(exp - time.time()))
    except Exception:
        pass
    return {"status": 200, "nick": name, "ya29_exp": exp}


@router.post("/nicks/{name}/cooldown")
def admin_cooldown_toggle(name: str, off: bool = Query(True)):
    """Per-nick THROTTLED cooldown switch. `off=true` (default) keeps the
    nick schedulable even when upstream throttles it — the failover loop
    still moves the failed request elsewhere, but the nick itself never
    sits out. Persisted to nicks/_no_cooldown.json across restarts."""
    try:
        from agent.services.flow_grpc import get_flow_grpc
        t = get_flow_grpc()
        t.reload()
    except Exception as e:
        return {"status": 503, "error": f"transport unavailable: {e}"}
    if name not in t._nicks:
        return {"status": 404, "error": f"unknown nick {name}"}
    t.set_no_cooldown(name, off)
    return {"nick": name, "cooldown_off": off}


@router.get("/quota")
def admin_quota(nick: str = ""):
    """Google-side quota ledger per nick, minted live from the nick's
    OAuthLogin RT -> web cookies -> DrWK4 activity ledger.

    Returns Flow credits, reset_ts, and rolling accept counts
    (1h/4h/24h/today, gen-only vs upscale split). `nick=<name>` limits
    to one nick; empty polls all (slow — one session mint each).
    """
    from agent.services import nick_quota
    try:
        if nick:
            return nick_quota.get_nick_quota(nick)
        return {"nicks": nick_quota.get_all_quota()}
    except Exception as e:
        return {"status": 502, "error": str(e)[:300]}


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


ERR_DIR = Path(BASE_DIR) / "logs" / "errors"


def _line_ts(ln: str) -> float | None:
    """Timestamp of a jsonl event line without a full parse when possible."""
    try:
        ev = json.loads(ln)
    except ValueError:
        return None
    try:
        return datetime.fromisoformat(
            str(ev.get("ts", "")).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _tail_lines_until(path: Path, cutoff: float,
                      max_lines: int) -> list[str]:
    """Tail-read a jsonl file backwards until its events precede `cutoff`.
    Same block walk as _read_tail_lines, but time-bounded instead of
    line-bounded — error days can run to tens of MB."""
    try:
        size = path.stat().st_size
    except OSError:
        return []
    buf = b""
    pos = size
    with path.open("rb") as f:
        while pos > 0:
            step = min(1 << 20, pos)
            pos -= step
            f.seek(pos)
            buf = f.read(step) + buf
            lines = buf.split(b"\n")
            if len(lines) > max_lines:
                break
            first = lines[0]
            ts = _line_ts(first.decode("utf-8", errors="replace"))
            if ts is not None and ts < cutoff:
                break
    lines = buf.split(b"\n")
    if pos > 0:
        lines = lines[1:]
    return [ln.decode("utf-8", errors="replace") for ln in lines]


@router.get("/errors")
def admin_errors(nick: str = "", hours: float = Query(24, ge=0.25, le=720),
                 q: str = "", limit: int = Query(300, le=2000)):
    """Durable error log — every error-class event ever emitted, kept in
    logs/errors/errors-YYYYMMDD.jsonl (per-UTC-day, no rotation loss).
    Unlike /api/admin/trace this survives heavy load and deploys.
    `q` is a raw substring filter (e.g. 'THROTTLED', 'nova_req_…')."""
    cutoff = time.time() - hours * 3600
    files = sorted(ERR_DIR.glob("errors-*.jsonl"), reverse=True) \
        if ERR_DIR.exists() else []
    out: list[dict] = []
    scanned = []
    for path in files:
        if len(out) >= limit:
            break
        lines = _tail_lines_until(path, cutoff, 20000)
        if not lines:
            continue
        scanned.append(path.name)
        hit = 0
        for ln in reversed(lines):
            if not ln.strip():
                continue
            if nick and f'"nick": "{nick}"' not in ln:
                continue
            if q and q not in ln:
                continue
            try:
                ev = json.loads(ln)
            except ValueError:
                continue
            try:
                ts = datetime.fromisoformat(str(ev.get("ts", ""))
                                            .replace("Z", "+00:00")
                                            ).timestamp()
            except ValueError:
                continue
            if ts < cutoff:
                hit = -1  # reached the window edge — stop scanning
                break
            out.append(ev)
            hit += 1
            if len(out) >= limit:
                break
        if hit < 0 and out:
            break  # already inside cutoff and moved past it
    return {"events": out[:limit], "files_scanned": scanned,
            "dir": str(ERR_DIR)}


def _tail_jsonl(pattern: str, max_lines: int) -> list[dict]:
    rows = []
    for path in _log_files(pattern):
        for line in _read_tail_lines(path, max_lines):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except ValueError:
                continue
    rows.sort(key=lambda e: e.get("ts", ""))
    return rows[-max_lines:]


@router.get("/exchange")
def admin_exchange(trace_id: str = "", nick: str = "",
                   limit: int = Query(100, le=500)):
    """Redacted Nova/Google request-response pairs, newest first.

    Filter with the Nova ``nova_req_…`` id or a nick name.
    """
    rows = _tail_jsonl(EXCHANGE_GLOB, 4000)
    if trace_id:
        rows = [r for r in rows if r.get("trace_id") == trace_id]
    if nick:
        rows = [r for r in rows if r.get("nick") == nick or nick in str(r.get("body"))]
    return {"events": list(reversed(rows))[:limit]}
def admin_page():
    if _ADMIN_HTML.exists():
        return HTMLResponse(_ADMIN_HTML.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>admin.html missing</h1>", status_code=500)
