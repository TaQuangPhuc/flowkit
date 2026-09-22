"""Per-nick auth verdicts read off raw netlog evidence.

The fleet already knew which nick was 401 — it just never said so anywhere a
person could look. The evidence is in .scratch/ext-netlog.jsonl, one record per
RPC with profileId + rpcid + statusCode, and that is the only source here that
cannot lie: a verdict derived from the agent's own classification would repeat
whatever mistake the classifier made (22 Sep: four nicks disabled because a
media uuid contained the digits "401").

Rule that matters: 401 on generation rpcs while chat/list rpcs are 200 on the
same profile is an account-level block, not a dead session, so re-login does
nothing. Signed out = everything 401.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from agent.config import AUTH_STRIKE_TTL_S, BASE_DIR
from agent.services.log_maintenance import read_tail

logger = logging.getLogger(__name__)

NETLOG_PATH = BASE_DIR / ".scratch" / "ext-netlog.jsonl"

# Reading the whole 135MB netlog per dashboard poll is not an option; the tail
# covers hours of traffic at the rate the fleet actually generates.
TAIL_BYTES = 4 * 1024 * 1024
WINDOW_S = 3600

# RPCs that spend credits. A block hits these first and often only these.
GENERATION_RPCS = {
    "maseQ",    # upload image
    "ogiZ0b",   # generate image
    "eb1hJf",   # i2v
    "YhhmEf",   # t2v
}

VERDICT_SIGNED_OUT = "SIGNED_OUT"
VERDICT_BLOCKED = "ACCOUNT_BLOCKED"
VERDICT_RECOVERED = "RECOVERED"
VERDICT_OK = "OK"
VERDICT_NO_EVIDENCE = "NO_EVIDENCE"

# What a person should actually do about each verdict.
ADVICE = {
    VERDICT_SIGNED_OUT: "Open this nick's Chrome and sign in to flow.google.com again.",
    VERDICT_BLOCKED: "Session is alive but generation is refused — check this account in the Flow UI. Re-login will not fix it.",
    VERDICT_RECOVERED: "Was 401, now succeeding again. No action needed.",
    VERDICT_OK: "No 401 in the window.",
    VERDICT_NO_EVIDENCE: "No traffic in the window — nothing to judge. Launch Chrome or send a job.",
}


def _epoch(ts: Any) -> float:
    """Netlog ts is an ISO-8601 string, not epoch ms."""
    if isinstance(ts, (int, float)):
        return float(ts) / 1000.0 if ts > 1e11 else float(ts)
    if not isinstance(ts, str) or not ts:
        return 0.0
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def _iso(epoch: float) -> Optional[str]:
    if not epoch:
        return None
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _blank(nick_id: str, window_s: int) -> Dict[str, Any]:
    return {
        "nick_id": nick_id,
        "window_s": window_s,
        "samples": 0,
        "ok": 0,
        "unauthorized": 0,
        "other": 0,
        "per_rpc": {},
        "gen_ok": 0,
        "gen_unauthorized": 0,
        "nongen_ok": 0,
        "nongen_unauthorized": 0,
        "last_ok_at": None,
        "last_unauthorized_at": None,
        "verdict": VERDICT_NO_EVIDENCE,
        "advice": ADVICE[VERDICT_NO_EVIDENCE],
    }


def scan_netlog(
    window_s: int = WINDOW_S,
    path: Path | None = None,
    tail_bytes: int = TAIL_BYTES,
    now: float | None = None,
) -> Dict[str, Dict[str, Any]]:
    """Bucket recent netlog records by profileId → rpcid → status class."""
    target = path or NETLOG_PATH
    now = now if now is not None else time.time()
    cutoff = now - window_s
    out: Dict[str, Dict[str, Any]] = {}
    try:
        if not target.exists():
            return out
        blob = read_tail(target, tail_bytes)
    except OSError as exc:
        logger.warning("nick_auth: cannot read netlog %s: %s", target, exc)
        return out

    for line in blob.splitlines():
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if not isinstance(rec, dict):
            continue
        nick = str(rec.get("profileId") or "").strip()
        if not nick:
            continue
        ts = _epoch(rec.get("ts"))
        if ts and ts < cutoff:
            continue
        try:
            status = int(rec.get("statusCode") or 0)
        except (TypeError, ValueError):
            continue
        rpcid = str(rec.get("rpcid") or "?")

        ev = out.setdefault(nick, _blank(nick, window_s))
        ev["samples"] += 1
        bucket = ev["per_rpc"].setdefault(rpcid, {"ok": 0, "unauthorized": 0, "other": 0})
        gen = rpcid in GENERATION_RPCS
        if status in (401, 403):
            bucket["unauthorized"] += 1
            ev["unauthorized"] += 1
            ev["gen_unauthorized" if gen else "nongen_unauthorized"] += 1
            if ts > (ev["_last_unauth"] if "_last_unauth" in ev else 0):
                ev["_last_unauth"] = ts
        elif 200 <= status < 400:
            bucket["ok"] += 1
            ev["ok"] += 1
            ev["gen_ok" if gen else "nongen_ok"] += 1
            if ts > (ev["_last_ok"] if "_last_ok" in ev else 0):
                ev["_last_ok"] = ts
        else:
            bucket["other"] += 1
            ev["other"] += 1

    for ev in out.values():
        ev["last_ok_at"] = _iso(ev.pop("_last_ok", 0.0))
        ev["last_unauthorized_at"] = _iso(ev.pop("_last_unauth", 0.0))
        ev["verdict"] = _verdict(ev)
        ev["advice"] = ADVICE[ev["verdict"]]
    return out


def _verdict(ev: Dict[str, Any]) -> str:
    if not ev["samples"]:
        return VERDICT_NO_EVIDENCE
    if not ev["unauthorized"]:
        return VERDICT_OK
    if not ev["ok"]:
        return VERDICT_SIGNED_OUT
    # Both present. Generation refused while the rest of the page still answers
    # is the account-level block; a 401 that stopped is just history.
    if ev["gen_unauthorized"] and not ev["gen_ok"] and ev["nongen_ok"]:
        return VERDICT_BLOCKED
    last_ok = ev["last_ok_at"] or ""
    last_bad = ev["last_unauthorized_at"] or ""
    if last_ok > last_bad:
        return VERDICT_RECOVERED
    return VERDICT_SIGNED_OUT


def evidence_for(nick_id: str, window_s: int = WINDOW_S, **kw) -> Dict[str, Any]:
    """Netlog verdict for one nick."""
    return scan_netlog(window_s=window_s, **kw).get(nick_id) or _blank(nick_id, window_s)


# Order the dashboard sorts by: worst first.
SEVERITY = {
    VERDICT_SIGNED_OUT: 0,
    VERDICT_BLOCKED: 1,
    VERDICT_RECOVERED: 2,
    VERDICT_NO_EVIDENCE: 3,
    VERDICT_OK: 4,
}


def auth_report(window_s: int = WINDOW_S, netlog_path: Path | None = None) -> Dict[str, Any]:
    """Netlog evidence merged with what the agent itself believes per nick.

    Three independent signals, kept separate on purpose so a wrong one is
    visible rather than averaged in: raw netlog status codes, the soft-auth
    strike counters inside FlowClient, and the incident ledger.
    """
    from agent.services.accounts import load_accounts
    from agent.services.flow_client import get_flow_client
    from agent.services.incident_manager import get_incident_manager

    evidence = scan_netlog(window_s=window_s, path=netlog_path)

    try:
        strikes = get_flow_client().auth_strike_report()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("nick_auth: strike report failed: %s", exc)
        strikes = {}

    incidents: Dict[str, List[Dict[str, Any]]] = {}
    try:
        for inc in get_incident_manager().get_incidents(status="OPEN", limit=200):
            if inc.get("error_code") != "ACCOUNT_AUTH_EXPIRED":
                continue
            key = str(inc.get("sub_id") or inc.get("job_id") or "")
            if key:
                incidents.setdefault(key, []).append(inc)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("nick_auth: incident lookup failed: %s", exc)

    rows: List[Dict[str, Any]] = []
    for acct in load_accounts():
        nick_id = acct.get("id")
        if not nick_id:
            continue
        ev = evidence.get(nick_id) or _blank(nick_id, window_s)
        st = strikes.get(nick_id) or {}
        open_incidents = incidents.get(nick_id, [])
        enabled = bool(acct.get("enabled", True))
        rows.append({
            "nick_id": nick_id,
            "enabled": enabled,
            "label": acct.get("label") or acct.get("note") or "",
            "verdict": ev["verdict"],
            "advice": ev["advice"],
            "samples": ev["samples"],
            "ok": ev["ok"],
            "unauthorized": ev["unauthorized"],
            "gen_ok": ev["gen_ok"],
            "gen_unauthorized": ev["gen_unauthorized"],
            "nongen_ok": ev["nongen_ok"],
            "last_ok_at": ev["last_ok_at"],
            "last_unauthorized_at": ev["last_unauthorized_at"],
            "per_rpc": ev["per_rpc"],
            "auth_strikes": st.get("strikes", 0),
            "last_strike_at": st.get("last_strike_at"),
            "strike_ttl_s": AUTH_STRIKE_TTL_S,
            "parked_for_s": st.get("parked_for_s", 0),
            "open_incidents": [
                {
                    "id": i.get("id"),
                    "message": i.get("message"),
                    "created_at": i.get("created_at"),
                    "root_cause": i.get("root_cause"),
                }
                for i in open_incidents
            ],
            # One boolean for the UI badge: needs a human, for any of the reasons.
            "needs_attention": (
                not enabled
                or bool(open_incidents)
                or ev["verdict"] in (VERDICT_SIGNED_OUT, VERDICT_BLOCKED)
            ),
        })

    rows.sort(key=lambda r: (not r["needs_attention"], SEVERITY.get(r["verdict"], 9), r["nick_id"]))
    return {
        "window_s": window_s,
        "generated_at": _iso(time.time()),
        "netlog": str(netlog_path or NETLOG_PATH),
        "netlog_available": (netlog_path or NETLOG_PATH).exists(),
        "counts": {
            "total": len(rows),
            "needs_attention": sum(1 for r in rows if r["needs_attention"]),
            "signed_out": sum(1 for r in rows if r["verdict"] == VERDICT_SIGNED_OUT),
            "blocked": sum(1 for r in rows if r["verdict"] == VERDICT_BLOCKED),
            "disabled": sum(1 for r in rows if not r["enabled"]),
        },
        "nicks": rows,
    }
