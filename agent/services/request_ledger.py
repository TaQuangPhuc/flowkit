"""Persistent request outcome ledger — the data source for fleet optimization.

Two tables in request_ledger.db:

- ``rpc_log``: every Flow RPC observed by the extension netlog (transport
  truth: rpcid, profile, http status, elapsed).
- ``outcome_log``: every logical request the dispatcher completes
  (nick, latency, normalized error class, egress IP) — written from
  ``nick_metrics.record_completion`` so coverage matches the success-rate
  metrics exactly.

Error classes let dashboards separate fleet problems (UNUSUAL_ACTIVITY,
FLOW_BUSY, AUTH, PROXY) from data problems (media expired, model denied)
instead of lumping everything into one success rate.
"""
from __future__ import annotations

import json
import logging
import re
import sqlite3
import threading
import time
from pathlib import Path

from agent.config import BASE_DIR

logger = logging.getLogger(__name__)

DB_PATH = BASE_DIR / "request_ledger.db"
_LOCK = threading.Lock()
_LOCAL = threading.local()

_SCHEMA = """
CREATE TABLE IF NOT EXISTS rpc_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         REAL NOT NULL,
    profile    TEXT,
    rpcid      TEXT,
    status     INTEGER,
    elapsed_ms INTEGER,
    session    TEXT
);
CREATE INDEX IF NOT EXISTS ix_rpc_log_ts ON rpc_log(ts);
CREATE INDEX IF NOT EXISTS ix_rpc_log_profile_ts ON rpc_log(profile, ts);

CREATE TABLE IF NOT EXISTS outcome_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL NOT NULL,
    nick        TEXT,
    ok          INTEGER NOT NULL,
    latency_ms  INTEGER,
    error_class TEXT,
    error       TEXT,
    egress_ip   TEXT,
    queue_ms    INTEGER
);
CREATE INDEX IF NOT EXISTS ix_outcome_ts ON outcome_log(ts);
CREATE INDEX IF NOT EXISTS ix_outcome_nick_ts ON outcome_log(nick, ts);

CREATE TABLE IF NOT EXISTS event_log (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    ts     REAL NOT NULL,
    nick   TEXT,
    kind   TEXT NOT NULL,
    detail TEXT
);
CREATE INDEX IF NOT EXISTS ix_event_ts ON event_log(ts);
CREATE INDEX IF NOT EXISTS ix_event_nick_ts ON event_log(nick, ts);
CREATE INDEX IF NOT EXISTS ix_event_kind_ts ON event_log(kind, ts);
"""

# Order matters: first matching pattern wins.
_ERROR_CLASSES = [
    ("TAB_DEAD", r"TAB_UNRESPONSIVE|ERROR_PAGE_NAVIGATED|Receiving end does not exist|Could not establish connection|FLOW_TAB_DISCARDED"),
    ("UNUSUAL_ACTIVITY", r"unusual.activity|PUBLIC_ERROR_UNUSUAL"),
    ("FLOW_BUSY", r"FLOW_BUSY|FLOW_DEADLINE|NOT_SUBMITTED"),
    ("MEDIA_NOT_FOUND", r"No urls for media|media not found|MEDIA_NOT_FOUND"),
    ("AUTH", r"HTTP 401|unauthorized|UNAUTHENTICATED|401"),
    ("CAPTCHA", r"CAPTCHA|recaptcha"),
    ("PROXY", r"BURNED_PROXY|DEAD_PROXY|PROXY_PREFLIGHT|proxy"),
    ("MODEL_DENIED", r"MODEL_ACCESS_DENIED|low_priority_only"),
    ("NO_FLOW_TAB", r"NO_FLOW_TAB"),
    ("NO_AT_TOKEN", r"NO_AT_TOKEN|FLOW_PAGE_NOT_READY"),
    ("TIMEOUT", r"timeout|TimedOut|timed out|upstream_timeout"),
    ("XHR", r"XHR_FAILED|ERR_ABORTED|network_error"),
    ("UNSAFE", r"UNSAFE_GENERATION|safety"),
]


def classify_error(error: str | None) -> str:
    if not error:
        return "UNKNOWN"
    text = str(error)
    for cls, pattern in _ERROR_CLASSES:
        if re.search(pattern, text, re.IGNORECASE):
            return cls
    return "OTHER"


def _conn() -> sqlite3.Connection:
    conn = getattr(_LOCAL, "conn", None)
    if conn is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(_SCHEMA)
        # Migrations for DBs created before a column existed.
        cols = {r[1] for r in conn.execute("PRAGMA table_info(outcome_log)")}
        if "queue_ms" not in cols:
            conn.execute("ALTER TABLE outcome_log ADD COLUMN queue_ms INTEGER")
            conn.commit()
        _LOCAL.conn = conn
    return conn


def _egress_for(nick: str | None) -> str | None:
    if not nick:
        return None
    try:
        from agent.services.accounts import get_account
        acc = get_account(nick) or {}
        return acc.get("last_egress_ip")
    except Exception:
        return None


def record_rpc(profile: str | None, rpcid: str | None, status,
               elapsed_ms=None, session: str | None = None, ts: float | None = None) -> None:
    try:
        with _LOCK:
            _conn().execute(
                "INSERT INTO rpc_log(ts, profile, rpcid, status, elapsed_ms, session)"
                " VALUES (?,?,?,?,?,?)",
                (ts or time.time(), profile, rpcid, status, elapsed_ms, session),
            )
            _conn().commit()
    except Exception as exc:
        logger.debug("rpc_log write failed: %s", exc)


def record_outcome(nick: str | None, ok: bool, latency_ms: int = 0,
                   error: str = "", ts: float | None = None,
                   queue_ms: int | None = None) -> None:
    cls = "SUCCESS" if ok else classify_error(error)
    try:
        with _LOCK:
            _conn().execute(
                "INSERT INTO outcome_log(ts, nick, ok, latency_ms, error_class, error, egress_ip, queue_ms)"
                " VALUES (?,?,?,?,?,?,?,?)",
                (ts or time.time(), nick, int(ok), int(latency_ms or 0),
                 cls, str(error)[:300] if error else None, _egress_for(nick),
                 queue_ms),
            )
            _conn().commit()
    except Exception as exc:
        logger.debug("outcome_log write failed: %s", exc)


# Noisy kinds fire on retry loops (auto-bind, pinned-op retries) — hundreds of
# identical rows per hour drown the signal. Cap them at one row per
# kind+nick per 60s; rare kinds always write.
_NOISY_KINDS = {"ROUTE_EMPTY", "ROUTE_ALL_PARKED", "BIND_FAIL", "NO_AT_TOKEN",
                "WS_DUP_RESOLVED", "EXT_CONNECT", "EXT_DISCONNECT"}
_NOISY_WINDOW_S = 60.0
_last_noisy: dict[tuple, float] = {}
_NOISY_MAX_KEYS = 5000


def record_event(kind: str, nick: str | None = None,
                 detail: dict | None = None, ts: float | None = None) -> None:
    """Append one structured lifecycle event. Kind is a stable token
    (EXT_CONNECT, STRIKE_UNUSUAL, PROXY_ROTATE, TAB_DEAD, ...); detail is
    free-form JSON context. Fire-and-forget — never raises."""
    now = ts or time.time()
    if kind in _NOISY_KINDS:
        key = (kind, nick)
        last = _last_noisy.get(key, 0.0)
        if now - last < _NOISY_WINDOW_S:
            return
        if len(_last_noisy) >= _NOISY_MAX_KEYS:
            cutoff = now - _NOISY_WINDOW_S
            for k, v in [(k, v) for k, v in _last_noisy.items() if v < cutoff]:
                _last_noisy.pop(k, None)
        _last_noisy[key] = now
    try:
        with _LOCK:
            _conn().execute(
                "INSERT INTO event_log(ts, nick, kind, detail) VALUES (?,?,?,?)",
                (now, nick, kind,
                 json.dumps(detail, ensure_ascii=False, default=str)[:2000]
                 if detail else None),
            )
            _conn().commit()
    except Exception as exc:
        logger.debug("event_log write failed: %s", exc)


def event_summary(hours: float = 24.0) -> dict:
    since = time.time() - hours * 3600
    conn = _conn()
    by_kind = conn.execute(
        "SELECT kind, COUNT(*) FROM event_log WHERE ts>=? GROUP BY kind"
        " ORDER BY COUNT(*) DESC", (since,)
    ).fetchall()
    by_nick = conn.execute(
        "SELECT nick, kind, COUNT(*) FROM event_log WHERE ts>=? AND nick IS NOT NULL"
        " GROUP BY nick, kind ORDER BY nick, COUNT(*) DESC", (since,)
    ).fetchall()
    return {
        "hours": hours,
        "by_kind": [{"kind": k, "count": n} for k, n in by_kind],
        "by_nick": [{"nick": n, "kind": k, "count": c} for n, k, c in by_nick],
    }


def recent_events(hours: float = 24.0, nick: str | None = None,
                  kinds: list[str] | None = None, limit: int = 200) -> list[dict]:
    since = time.time() - hours * 3600
    sql = "SELECT ts, nick, kind, detail FROM event_log WHERE ts>=?"
    args: list = [since]
    if nick:
        sql += " AND nick=?"
        args.append(nick)
    if kinds:
        sql += " AND kind IN (%s)" % ",".join("?" * len(kinds))
        args.extend(kinds)
    sql += " ORDER BY ts DESC LIMIT ?"
    args.append(limit)
    rows = _conn().execute(sql, args).fetchall()
    out = []
    for ts, n, k, d in rows:
        try:
            d = json.loads(d) if d else None
        except Exception:
            pass
        out.append({"ts": ts, "nick": n, "kind": k, "detail": d})
    return out


def recent_outcomes(nick: str | None = None, hours: float = 24.0,
                    limit: int = 500) -> list[dict]:
    since = time.time() - hours * 3600
    sql = ("SELECT ts, nick, ok, latency_ms, error_class, error, egress_ip, queue_ms"
           " FROM outcome_log WHERE ts>=?")
    args: list = [since]
    if nick:
        sql += " AND nick=?"
        args.append(nick)
    sql += " ORDER BY ts DESC LIMIT ?"
    args.append(limit)
    rows = _conn().execute(sql, args).fetchall()
    return [
        {"ts": ts, "nick": n, "ok": bool(o), "latency_ms": lat,
         "error_class": cls, "error": err, "egress_ip": ip, "queue_ms": q}
        for ts, n, o, lat, cls, err, ip, q in rows
    ]


def outcome_summary(hours: float = 24.0) -> dict:
    """Aggregate outcomes: totals, per-class breakdown, per-nick breakdown."""
    since = time.time() - hours * 3600
    conn = _conn()
    total, fails = conn.execute(
        "SELECT COUNT(*), SUM(ok=0) FROM outcome_log WHERE ts>=?", (since,)
    ).fetchone()
    by_class = conn.execute(
        "SELECT error_class, COUNT(*) c FROM outcome_log WHERE ts>=? AND ok=0"
        " GROUP BY error_class ORDER BY c DESC", (since,)
    ).fetchall()
    by_nick = conn.execute(
        "SELECT nick, COUNT(*), SUM(ok), SUM(ok=0), AVG(latency_ms),"
        " AVG(queue_ms), MAX(queue_ms) FROM outcome_log WHERE ts>=?"
        " GROUP BY nick ORDER BY COUNT(*) DESC", (since,)
    ).fetchall()
    hourly = conn.execute(
        "SELECT CAST((ts-?)/3600 AS INT) h, COUNT(*), SUM(ok) FROM outcome_log"
        " WHERE ts>=? GROUP BY h ORDER BY h", (since, since)
    ).fetchall()
    return {
        "hours": hours,
        "total": total or 0,
        "failed": fails or 0,
        "success_rate": round(100.0 * (total - (fails or 0)) / total, 1) if total else None,
        "failures_by_class": [{"class": c, "count": n} for c, n in by_class],
        "by_nick": [
            {"nick": n, "total": t, "ok": o or 0, "failed": f or 0,
             "success_rate": round(100.0 * (o or 0) / t, 1) if t else None,
             "avg_latency_ms": round(al or 0),
             "avg_queue_ms": round(aq or 0), "max_queue_ms": mq or 0}
            for n, t, o, f, al, aq, mq in by_nick
        ],
        "hourly": [{"hour_offset": h, "total": t, "ok": o or 0} for h, t, o in hourly],
    }


def rpc_summary(hours: float = 24.0) -> dict:
    since = time.time() - hours * 3600
    conn = _conn()
    by_rpc = conn.execute(
        "SELECT rpcid, COUNT(*), SUM(status=200), AVG(elapsed_ms) FROM rpc_log"
        " WHERE ts>=? GROUP BY rpcid ORDER BY COUNT(*) DESC", (since,)
    ).fetchall()
    by_status = conn.execute(
        "SELECT status, COUNT(*) FROM rpc_log WHERE ts>=? GROUP BY status"
        " ORDER BY COUNT(*) DESC", (since,)
    ).fetchall()
    return {
        "hours": hours,
        "by_rpc": [{"rpcid": r, "total": t, "ok": o or 0,
                    "avg_ms": round(a or 0)} for r, t, o, a in by_rpc],
        "by_status": [{"status": s, "count": n} for s, n in by_status],
    }
