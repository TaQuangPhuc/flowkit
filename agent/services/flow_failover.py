"""SQLite operation history and compatibility with existing failover mappings.

Accepted low-priority jobs are no longer replayed based on queue duration.
Retain mappings created by older versions so clients can still poll them.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from contextlib import contextmanager
from typing import Any, Dict, Optional

from agent.config import DB_PATH

logger = logging.getLogger(__name__)


def _clean_op_id(op_id: Any) -> str:
    if not op_id:
        return ""
    if isinstance(op_id, dict):
        op_id = (op_id.get("operation") or {}).get("name") or op_id.get("name") or ""
    s = str(op_id).strip()
    return s.removeprefix("operations/")


@contextmanager
def _get_conn():
    conn = sqlite3.connect(str(DB_PATH), timeout=15.0)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        with conn:
            yield conn
    finally:
        # sqlite3.Connection.__exit__ commits/rolls back; it does NOT close.
        conn.close()


def save_operation_replay(
    operation_id: str,
    request_type: str,
    payload: dict | str,
    worker_id: str = "",
) -> None:
    """Persist original request payload for an operation to allow replay on failover."""
    cid = _clean_op_id(operation_id)
    if not cid:
        return

    payload_str = json.dumps(payload) if isinstance(payload, dict) else str(payload)
    now = time.time()

    try:
        with _get_conn() as conn:
            conn.execute(
                """
                INSERT INTO flow_operation_replay (
                    operation_id, request_type, payload, worker_id, retry_count, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 0, 'PENDING', ?, ?)
                ON CONFLICT(operation_id) DO UPDATE SET
                    payload = excluded.payload,
                    worker_id = COALESCE(NULLIF(excluded.worker_id, ''), flow_operation_replay.worker_id),
                    updated_at = excluded.updated_at
                """,
                (cid, request_type, payload_str, worker_id or "", now, now),
            )
            conn.commit()
        logger.info("Saved flow operation replay for %s (type=%s, worker=%s)", cid[:12], request_type, worker_id)
    except Exception as exc:
        logger.warning("Failed to save flow operation replay for %s: %s", cid[:12], exc)


def get_operation_replay(operation_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve saved replay record for an operation."""
    cid = _clean_op_id(operation_id)
    if not cid:
        return None

    try:
        with _get_conn() as conn:
            cur = conn.execute(
                """
                SELECT operation_id, request_type, payload, worker_id, retry_count, status, created_at, updated_at
                FROM flow_operation_replay
                WHERE operation_id = ?
                """,
                (cid,),
            )
            row = cur.fetchone()
            if not row:
                return None
            res = dict(row)
            try:
                res["payload"] = json.loads(res["payload"])
            except Exception:
                pass
            return res
    except Exception as exc:
        logger.warning("Failed to get flow operation replay for %s: %s", cid[:12], exc)
        return None


def update_operation_replay(operation_id: str, **kwargs) -> None:
    """Update replay fields (e.g. retry_count, status, worker_id)."""
    cid = _clean_op_id(operation_id)
    if not cid or not kwargs:
        return

    kwargs["updated_at"] = time.time()
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    vals = list(kwargs.values()) + [cid]

    try:
        with _get_conn() as conn:
            conn.execute(f"UPDATE flow_operation_replay SET {sets} WHERE operation_id = ?", vals)
            conn.commit()
    except Exception as exc:
        logger.warning("Failed to update flow operation replay for %s: %s", cid[:12], exc)


def record_operation_failover(
    original_op_id: str,
    new_op_id: str,
    original_worker_id: str = "",
    failover_worker_id: str = "",
    status: str = "IN_PROGRESS",
) -> None:
    """Record transparent failover mapping from original_op_id to new_op_id."""
    c_orig = _clean_op_id(original_op_id)
    c_new = _clean_op_id(new_op_id)
    if not c_orig or not c_new:
        return

    now = time.time()
    try:
        with _get_conn() as conn:
            conn.execute(
                """
                INSERT INTO flow_operation_failover (
                    original_op_id, new_op_id, original_worker_id, failover_worker_id, failover_at, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(original_op_id) DO UPDATE SET
                    new_op_id = excluded.new_op_id,
                    original_worker_id = excluded.original_worker_id,
                    failover_worker_id = excluded.failover_worker_id,
                    failover_at = excluded.failover_at,
                    status = excluded.status
                """,
                (c_orig, c_new, original_worker_id, failover_worker_id, now, status, now),
            )
            # Mark replay as failed over
            conn.execute(
                """
                UPDATE flow_operation_replay
                SET retry_count = retry_count + 1, status = 'FAILED_OVER', updated_at = ?
                WHERE operation_id = ?
                """,
                (now, c_orig),
            )
            conn.commit()
        logger.info(
            "Recorded transparent failover: %s -> %s (worker: %s -> %s)",
            c_orig[:12], c_new[:12], original_worker_id, failover_worker_id,
        )
    except Exception as exc:
        logger.warning("Failed to record flow operation failover: %s", exc)


def get_operation_failover(original_op_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve failover mapping for an original operation."""
    c_orig = _clean_op_id(original_op_id)
    if not c_orig:
        return None

    try:
        with _get_conn() as conn:
            cur = conn.execute(
                """
                SELECT original_op_id, new_op_id, original_worker_id, failover_worker_id, failover_at, status, created_at
                FROM flow_operation_failover
                WHERE original_op_id = ?
                """,
                (c_orig,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception as exc:
        logger.warning("Failed to get flow operation failover for %s: %s", c_orig[:12], exc)
        return None


def update_operation_failover(original_op_id: str, **kwargs) -> None:
    """Update failover mapping fields (e.g. status='COMPLETED')."""
    c_orig = _clean_op_id(original_op_id)
    if not c_orig or not kwargs:
        return

    sets = ", ".join(f"{k} = ?" for k in kwargs)
    vals = list(kwargs.values()) + [c_orig]

    try:
        with _get_conn() as conn:
            conn.execute(f"UPDATE flow_operation_failover SET {sets} WHERE original_op_id = ?", vals)
            conn.commit()
    except Exception as exc:
        logger.warning("Failed to update flow operation failover for %s: %s", c_orig[:12], exc)


def get_failover_target(operation_id: str) -> Optional[str]:
    """Resolve the latest target operation id for an original operation if failover occurred."""
    c_orig = _clean_op_id(operation_id)
    if not c_orig:
        return None

    visited = set()
    current = c_orig
    while current and current not in visited:
        visited.add(current)
        mapping = get_operation_failover(current)
        if mapping and mapping.get("new_op_id"):
            current = _clean_op_id(mapping["new_op_id"])
        else:
            break

    return current if current != c_orig else None


def prune_replays(completed_ttl_s: float, pending_ttl_s: float) -> Dict[str, int]:
    """Drop replay rows that can no longer be replayed.

    Nothing pruned this table before: it reached 1417 rows with 175 PENDING
    entries up to two days old, which also made every stale-PENDING scan
    slower and kept test-fixture operation ids around forever.
    """
    now = time.time()
    res = {"expired": 0, "deleted": 0}
    try:
        with _get_conn() as conn:
            cur = conn.execute(
                """
                UPDATE flow_operation_replay
                SET status = 'EXPIRED', updated_at = ?
                WHERE status = 'PENDING' AND updated_at < ?
                """,
                (now, now - pending_ttl_s),
            )
            res["expired"] = cur.rowcount
            cur = conn.execute(
                """
                DELETE FROM flow_operation_replay
                WHERE status IN ('COMPLETED', 'FAILED', 'FAILED_OVER', 'EXPIRED')
                  AND updated_at < ?
                """,
                (now - completed_ttl_s,),
            )
            res["deleted"] = cur.rowcount
            conn.commit()
    except Exception as exc:
        logger.warning("Failed to prune flow operation replays: %s", exc)
        return res
    if res["expired"] or res["deleted"]:
        logger.info("Pruned flow replays: %d expired, %d deleted", res["expired"], res["deleted"])
    return res


def reconcile_failovers(stuck_ttl_s: float, purge_ttl_s: float) -> Dict[str, int]:
    """Close out failovers nobody will ever update, then purge ancient mappings.

    A failover row is only moved to COMPLETED by the poll that sees the new
    operation finish. When that poll never happens (server restart, client
    gone) the row stays IN_PROGRESS forever — 13 of them, up to 44h old.
    Mappings are purged much later than they are closed because
    get_failover_target() still has to resolve a late poll.
    """
    now = time.time()
    res = {"abandoned": 0, "purged": 0}
    try:
        with _get_conn() as conn:
            cur = conn.execute(
                """
                UPDATE flow_operation_failover
                SET status = 'ABANDONED'
                WHERE status = 'IN_PROGRESS' AND failover_at < ?
                """,
                (now - stuck_ttl_s,),
            )
            res["abandoned"] = cur.rowcount
            cur = conn.execute(
                "DELETE FROM flow_operation_failover WHERE created_at < ?",
                (now - purge_ttl_s,),
            )
            res["purged"] = cur.rowcount
            conn.commit()
    except Exception as exc:
        logger.warning("Failed to reconcile flow operation failovers: %s", exc)
        return res
    if res["abandoned"] or res["purged"]:
        logger.info("Reconciled flow failovers: %d abandoned, %d purged",
                    res["abandoned"], res["purged"])
    return res
