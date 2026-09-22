"""Incident & Self-Healing Telemetry Manager — Centralized error and auto-recovery tracking."""

import asyncio
import json
import logging
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

from agent.config import DB_PATH

logger = logging.getLogger(__name__)

# Incidents that must stay open until a human acts, never auto-closed on age.
STALE_KEEP_OPEN_CODES = frozenset({"ACCOUNT_AUTH_EXPIRED"})


class IncidentManager:
    """Thread-safe and async-compatible Incident Ledger & Event Bus."""

    _instance: Optional["IncidentManager"] = None
    _lock = threading.Lock()

    def __init__(self, db_path=DB_PATH):
        self.db_path = str(db_path)
        self._listeners: List[asyncio.Queue] = []
        self._ensure_table()

    @classmethod
    def get_instance(cls) -> "IncidentManager":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @contextmanager
    def _get_connection(self):
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            with conn:
                yield conn
        finally:
            conn.close()

    def _ensure_table(self):
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS incident (
                    id            TEXT PRIMARY KEY,
                    module        TEXT NOT NULL,
                    job_id        TEXT,
                    sub_id        TEXT,
                    severity      TEXT NOT NULL CHECK(severity IN ('INFO','WARNING','CRITICAL','HEALED')),
                    error_code    TEXT NOT NULL,
                    message       TEXT NOT NULL,
                    root_cause    TEXT,
                    action_taken  TEXT,
                    status        TEXT NOT NULL CHECK(status IN ('OPEN','AUTO_HEALING','RESOLVED','FAILED')),
                    retry_count   INTEGER NOT NULL DEFAULT 0,
                    created_at    REAL NOT NULL,
                    resolved_at   REAL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_incident_status ON incident(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_incident_created ON incident(created_at DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_incident_module ON incident(module)")
            conn.commit()

    def record_incident(
        self,
        module: str,
        message: str,
        job_id: Optional[str] = None,
        sub_id: Optional[str] = None,
        severity: str = "WARNING",
        error_code: str = "GENERIC_ERROR",
        root_cause: str = "",
        action_taken: str = "",
        status: str = "OPEN"
    ) -> Dict[str, Any]:
        """Record a new incident or update an existing open incident."""
        now = time.time()
        with self._lock:
            with self._get_connection() as conn:
                # Check for existing open incident for same module + job + error_code to prevent flapping
                existing = conn.execute(
                    """
                    SELECT id, retry_count FROM incident
                    WHERE module = ? AND (job_id = ? OR (job_id IS NULL AND ? IS NULL))
                      AND (sub_id = ? OR (sub_id IS NULL AND ? IS NULL))
                      AND error_code = ? AND status IN ('OPEN', 'AUTO_HEALING')
                    LIMIT 1
                    """,
                    (module, job_id, job_id, sub_id, sub_id, error_code)
                ).fetchone()

                if existing:
                    inc_id = existing["id"]
                    new_retries = int(existing["retry_count"]) + (1 if action_taken else 0)
                    conn.execute(
                        """
                        UPDATE incident
                        SET severity = ?, message = ?, root_cause = ?, action_taken = ?,
                            status = ?, retry_count = ?, resolved_at = NULL
                        WHERE id = ?
                        """,
                        (severity, message, root_cause, action_taken, status, new_retries, inc_id)
                    )
                    conn.commit()
                else:
                    inc_id = f"inc_{uuid.uuid4().hex[:12]}"
                    conn.execute(
                        """
                        INSERT INTO incident (
                            id, module, job_id, sub_id, severity, error_code,
                            message, root_cause, action_taken, status, retry_count,
                            created_at, resolved_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                        """,
                        (
                            inc_id, module, job_id, sub_id, severity, error_code,
                            message, root_cause, action_taken, status,
                            1 if action_taken else 0, now
                        )
                    )
                    conn.commit()

        inc_data = {
            "id": inc_id,
            "module": module,
            "job_id": job_id,
            "sub_id": sub_id,
            "severity": severity,
            "error_code": error_code,
            "message": message,
            "root_cause": root_cause,
            "action_taken": action_taken,
            "status": status,
            "created_at": now
        }

        logger.warning(
            "[INCIDENT][%s][%s] %s (job: %s, action: %s)",
            severity, module, message, job_id or "-", action_taken or "none"
        )
        self._broadcast_event("incident_recorded", inc_data)
        return inc_data

    def resolve_incident(
        self,
        incident_id: str,
        action_taken: str = "",
        severity: str = "HEALED"
    ) -> bool:
        """Mark an incident as resolved/healed."""
        now = time.time()
        with self._lock:
            with self._get_connection() as conn:
                cur = conn.execute(
                    """
                    UPDATE incident
                    SET status = 'RESOLVED', severity = ?,
                        action_taken = CASE WHEN ? != '' THEN ? ELSE action_taken END,
                        resolved_at = ?
                    WHERE id = ?
                    """,
                    (severity, action_taken, action_taken, now, incident_id)
                )
                conn.commit()
                updated = cur.rowcount > 0

        if updated:
            logger.info("[INCIDENT][RESOLVED] %s -> %s (%s)", incident_id, severity, action_taken)
            self._broadcast_event("incident_resolved", {
                "id": incident_id,
                "severity": severity,
                "action_taken": action_taken,
                "status": "RESOLVED",
                "resolved_at": now
            })
        return updated

    def resolve_by_job(self, module: str, job_id: str, action_taken: str = "AUTO_HEALED") -> int:
        """Resolve all open incidents for a specific job."""
        now = time.time()
        with self._lock:
            with self._get_connection() as conn:
                cur = conn.execute(
                    """
                    UPDATE incident
                    SET status = 'RESOLVED', severity = 'HEALED',
                        action_taken = ?, resolved_at = ?
                    WHERE module = ? AND job_id = ? AND status IN ('OPEN', 'AUTO_HEALING')
                    """,
                    (action_taken, now, module, job_id)
                )
                conn.commit()
                count = cur.rowcount
        if count > 0:
            logger.info("[INCIDENT][AUTO_HEALED] Resolved %d open incidents for job %s (%s)", count, job_id, module)
        return count

    def resolve_by_nick(
        self,
        module: str,
        nick_id: str,
        error_code: Optional[str] = None,
        action_taken: str = "AUTO_HEALED",
    ) -> int:
        """Resolve open incidents about one nick, keyed on job_id OR sub_id.

        Worker incidents are recorded with the nick in `job_id` and proxy/auth
        sweeps put their subject in `sub_id`, so a caller that only knows the
        nick has to match either column — resolve_by_sub() alone silently
        resolved nothing for the nick incidents raised by flow_client.
        """
        now = time.time()
        query = """
            UPDATE incident
            SET status = 'RESOLVED', severity = 'HEALED', action_taken = ?, resolved_at = ?
            WHERE module = ? AND (job_id = ? OR sub_id = ?) AND status IN ('OPEN', 'AUTO_HEALING')
        """
        params: List[Any] = [action_taken, now, module, nick_id, nick_id]
        if error_code:
            query += " AND error_code = ?"
            params.append(error_code)
        with self._lock:
            with self._get_connection() as conn:
                cur = conn.execute(query, params)
                conn.commit()
                count = cur.rowcount
        if count > 0:
            logger.info(
                "[INCIDENT][AUTO_HEALED] Resolved %d open incidents for nick %s (%s/%s)",
                count, nick_id, module, error_code or "*",
            )
        return count

    def resolve_by_sub(
        self,
        module: str,
        sub_id: str,
        error_code: Optional[str] = None,
        action_taken: str = "AUTO_HEALED",
        severity: str = "HEALED",
    ) -> int:
        """Resolve open incidents for a subject (nick id, proxy ip, ...).

        Incident ids are generated (`inc_<hex>`), so a caller that only knows
        the subject cannot use resolve_incident() — it matches on the primary
        key and silently returns False, which is why watchdog worker/proxy
        incidents stayed OPEN for days.
        """
        now = time.time()
        query = """
            UPDATE incident
            SET status = 'RESOLVED', severity = ?, action_taken = ?, resolved_at = ?
            WHERE module = ? AND sub_id = ? AND status IN ('OPEN', 'AUTO_HEALING')
        """
        params: List[Any] = [severity, action_taken, now, module, sub_id]
        if error_code:
            query += " AND error_code = ?"
            params.append(error_code)

        with self._lock:
            with self._get_connection() as conn:
                cur = conn.execute(query, params)
                conn.commit()
                count = cur.rowcount

        if count > 0:
            logger.info(
                "[INCIDENT][AUTO_HEALED] Resolved %d open incidents for %s/%s (%s)",
                count, module, sub_id, action_taken,
            )
            self._broadcast_event("incident_resolved", {
                "module": module,
                "sub_id": sub_id,
                "error_code": error_code,
                "severity": severity,
                "action_taken": action_taken,
                "status": "RESOLVED",
                "resolved_at": now,
                "count": count,
            })
        return count

    def close_stale(
        self,
        max_age_s: float,
        action_taken: str = "AUTO_CLOSED_STALE",
        keep_codes: Optional[frozenset] = None,
    ) -> int:
        """Close unresolved incidents older than max_age_s.

        Some incidents have no resolver at all — the code that opened 20
        GHOST_QUEUE_FAILOVER rows was deleted while they sat AUTO_HEALING for
        two days. Severity drops to INFO, not HEALED, so auto-closing never
        inflates the healed count, and a still-live problem is re-recorded by
        the next sweep.
        """
        cutoff = time.time() - max_age_s
        now = time.time()
        codes = STALE_KEEP_OPEN_CODES if keep_codes is None else keep_codes
        placeholders = ",".join("?" for _ in codes) or "''"
        with self._lock:
            with self._get_connection() as conn:
                cur = conn.execute(
                    f"""
                    UPDATE incident
                    SET status = 'RESOLVED', severity = 'INFO',
                        action_taken = ?, resolved_at = ?
                    WHERE status IN ('OPEN', 'AUTO_HEALING')
                      AND created_at < ?
                      AND error_code NOT IN ({placeholders})
                    """,
                    [action_taken, now, cutoff, *codes],
                )
                conn.commit()
                count = cur.rowcount
        if count > 0:
            logger.info("[INCIDENT][STALE] Auto-closed %d incidents older than %.1fh",
                        count, max_age_s / 3600.0)
        return count

    def get_incidents(
        self,
        module: Optional[str] = None,
        status: Optional[str] = None,
        severity: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Retrieve paginated incidents with optional filters."""
        query = "SELECT * FROM incident WHERE 1=1"
        params: List[Any] = []
        if module:
            query += " AND module = ?"
            params.append(module)
        if status:
            query += " AND status = ?"
            params.append(status)
        if severity:
            query += " AND severity = ?"
            params.append(severity)

        query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self._get_connection() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]

    def get_summary(self) -> Dict[str, Any]:
        """Aggregate incident metrics and health overview across modules."""
        now = time.time()
        day_ago = now - 86400

        with self._get_connection() as conn:
            unresolved = conn.execute(
                "SELECT COUNT(*) FROM incident WHERE status IN ('OPEN', 'AUTO_HEALING')"
            ).fetchone()[0]

            critical = conn.execute(
                "SELECT COUNT(*) FROM incident WHERE status IN ('OPEN', 'AUTO_HEALING') AND severity = 'CRITICAL'"
            ).fetchone()[0]

            healed_24h = conn.execute(
                "SELECT COUNT(*) FROM incident WHERE severity = 'HEALED' AND (resolved_at >= ? OR created_at >= ?)",
                (day_ago, day_ago)
            ).fetchone()[0]

            # Module breakdowns
            mod_rows = conn.execute(
                """
                SELECT module, status, COUNT(*) as cnt
                FROM incident
                WHERE status IN ('OPEN', 'AUTO_HEALING')
                GROUP BY module, status
                """
            ).fetchall()

            module_status = {}
            for r in mod_rows:
                m = r["module"]
                if m not in module_status:
                    module_status[m] = {"open": 0, "auto_healing": 0}
                if r["status"] == "OPEN":
                    module_status[m]["open"] += r["cnt"]
                elif r["status"] == "AUTO_HEALING":
                    module_status[m]["auto_healing"] += r["cnt"]

        # 5 Monitored modules default health
        all_modules = ["lookbook", "tvc", "batch", "worker", "proxy"]
        modules_health = {}
        for m in all_modules:
            open_cnt = module_status.get(m, {}).get("open", 0)
            modules_health[m] = {
                "status": "CRITICAL" if open_cnt > 3 else ("WARNING" if open_cnt > 0 else "HEALTHY"),
                "open_incidents": open_cnt
            }

        return {
            "ok": True,
            "system_health": "CRITICAL" if critical > 0 else ("WARNING" if unresolved > 0 else "HEALTHY"),
            "unresolved_count": unresolved,
            "critical_count": critical,
            "healed_24h_count": healed_24h,
            "modules": modules_health,
            "timestamp": now
        }

    # ─── Realtime SSE Stream Subscription ────────────────────────────

    def register_listener(self) -> asyncio.Queue:
        """Register an async queue for SSE broadcast."""
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._listeners.append(q)
        return q

    def unregister_listener(self, q: asyncio.Queue):
        """Remove listener queue on disconnect."""
        if q in self._listeners:
            self._listeners.remove(q)

    def _broadcast_event(self, event_type: str, data: Dict[str, Any]):
        """Push incident events to all active SSE queues."""
        if not self._listeners:
            return

        payload = {"event": event_type, "data": data, "timestamp": time.time()}
        stale_queues = []
        for q in self._listeners:
            try:
                q.put_nowait(payload)
            except (asyncio.QueueFull, Exception):
                stale_queues.append(q)

        for sq in stale_queues:
            if sq in self._listeners:
                self._listeners.remove(sq)


def get_incident_manager() -> IncidentManager:
    return IncidentManager.get_instance()
