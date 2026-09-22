"""Pruning, log trimming and the client-side parked-fleet backoff.

Everything here covers code that did not exist while the tables it maintains
grew unbounded: 1417 replay rows, 13 failovers stuck IN_PROGRESS for up to 44h,
a 618MB server.log, and studios that failed an item after ~10s of 503s against
a 30-minute park.
"""
import json
import sqlite3
import time
from unittest.mock import patch

import pytest

from agent.api.flow import _respond_flow_result
from agent.services import flow_failover
from agent.services.flow_failover import prune_replays, reconcile_failovers
from agent.services.log_maintenance import read_tail, trim_log, trim_logs
from agent.services.parked_retry import (
    ParkedBackoff,
    is_parked,
    retry_after_seconds,
)

H = 3600.0


def _conn():
    # Must be resolved per call: the isolate_database fixture rebinds the
    # module attribute, so a DB_PATH imported at import time is the live DB.
    return sqlite3.connect(str(flow_failover.DB_PATH))


def _replay(op_id, status, age_s):
    ts = time.time() - age_s
    with _conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO flow_operation_replay"
            " (operation_id, request_type, payload, worker_id, retry_count, status, created_at, updated_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (op_id, "video", json.dumps({}), "nick-a", 0, status, ts, ts),
        )
        c.commit()


def _replay_row(op_id):
    with _conn() as c:
        return c.execute(
            "SELECT status FROM flow_operation_replay WHERE operation_id = ?", (op_id,)
        ).fetchone()


def _failover(op_id, status, failover_age_s, created_age_s=None):
    now = time.time()
    with _conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO flow_operation_failover"
            " (original_op_id, new_op_id, original_worker_id, failover_worker_id,"
            "  failover_at, status, created_at) VALUES (?,?,?,?,?,?,?)",
            (op_id, f"new-{op_id}", "nick-a", "Nick-b",
             now - failover_age_s, status,
             now - (created_age_s if created_age_s is not None else failover_age_s)),
        )
        c.commit()


def _failover_row(op_id):
    with _conn() as c:
        return c.execute(
            "SELECT status FROM flow_operation_failover WHERE original_op_id = ?", (op_id,)
        ).fetchone()


# ─── 1. REPLAY PRUNING ──────────────────────────────────────────────


class TestPruneReplays:
    def test_stale_pending_becomes_expired(self):
        _replay("prune-pending-old", "PENDING", 10 * H)
        res = prune_replays(completed_ttl_s=24 * H, pending_ttl_s=6 * H)
        assert res["expired"] >= 1
        assert _replay_row("prune-pending-old")[0] == "EXPIRED"

    def test_fresh_pending_is_left_alone(self):
        _replay("prune-pending-fresh", "PENDING", 60)
        prune_replays(completed_ttl_s=24 * H, pending_ttl_s=6 * H)
        assert _replay_row("prune-pending-fresh")[0] == "PENDING"

    def test_old_terminal_rows_are_deleted(self):
        for status in ("COMPLETED", "FAILED", "FAILED_OVER", "EXPIRED"):
            _replay(f"prune-term-{status}", status, 48 * H)
        res = prune_replays(completed_ttl_s=24 * H, pending_ttl_s=6 * H)
        assert res["deleted"] >= 4
        for status in ("COMPLETED", "FAILED", "FAILED_OVER", "EXPIRED"):
            assert _replay_row(f"prune-term-{status}") is None

    def test_recent_completed_row_survives(self):
        _replay("prune-term-fresh", "COMPLETED", 60)
        prune_replays(completed_ttl_s=24 * H, pending_ttl_s=6 * H)
        assert _replay_row("prune-term-fresh")[0] == "COMPLETED"

    def test_expired_in_one_sweep_is_not_deleted_in_the_same_sweep(self):
        """Expiry rewrites updated_at, so the row gets a full TTL to be read."""
        _replay("prune-two-phase", "PENDING", 48 * H)
        prune_replays(completed_ttl_s=24 * H, pending_ttl_s=6 * H)
        assert _replay_row("prune-two-phase")[0] == "EXPIRED"

    def test_db_failure_is_swallowed(self):
        with patch("agent.services.flow_failover._get_conn", side_effect=sqlite3.Error("boom")):
            assert prune_replays(24 * H, 6 * H) == {"expired": 0, "deleted": 0}


# ─── 2. FAILOVER RECONCILIATION ─────────────────────────────────────


class TestReconcileFailovers:
    def test_stuck_in_progress_is_abandoned(self):
        _failover("fo-stuck", "IN_PROGRESS", 44 * H)
        res = reconcile_failovers(stuck_ttl_s=6 * H, purge_ttl_s=7 * 24 * H)
        assert res["abandoned"] >= 1
        assert _failover_row("fo-stuck")[0] == "ABANDONED"

    def test_recent_in_progress_is_left_alone(self):
        _failover("fo-live", "IN_PROGRESS", 60)
        reconcile_failovers(stuck_ttl_s=6 * H, purge_ttl_s=7 * 24 * H)
        assert _failover_row("fo-live")[0] == "IN_PROGRESS"

    def test_completed_rows_are_not_abandoned(self):
        _failover("fo-done", "COMPLETED", 44 * H)
        reconcile_failovers(stuck_ttl_s=6 * H, purge_ttl_s=7 * 24 * H)
        assert _failover_row("fo-done")[0] == "COMPLETED"

    def test_ancient_mapping_is_purged(self):
        _failover("fo-ancient", "COMPLETED", 8 * 24 * H)
        res = reconcile_failovers(stuck_ttl_s=6 * H, purge_ttl_s=7 * 24 * H)
        assert res["purged"] >= 1
        assert _failover_row("fo-ancient") is None

    def test_abandoned_mapping_outlives_its_closure(self):
        """get_failover_target() must still resolve a late poll for days."""
        _failover("fo-fresh-abandon", "IN_PROGRESS", 10 * H, created_age_s=10 * H)
        reconcile_failovers(stuck_ttl_s=6 * H, purge_ttl_s=7 * 24 * H)
        assert _failover_row("fo-fresh-abandon")[0] == "ABANDONED"

    def test_db_failure_is_swallowed(self):
        with patch("agent.services.flow_failover._get_conn", side_effect=sqlite3.Error("boom")):
            assert reconcile_failovers(6 * H, 7 * 24 * H) == {"abandoned": 0, "purged": 0}


# ─── 3. LOG TRIMMING ────────────────────────────────────────────────


class TestReadTail:
    def test_small_file_is_returned_whole(self, tmp_path):
        p = tmp_path / "small.log"
        p.write_bytes(b"alpha\nbeta\n")
        assert read_tail(p, 1024) == b"alpha\nbeta\n"

    def test_tail_starts_at_a_line_boundary(self, tmp_path):
        p = tmp_path / "lines.log"
        p.write_bytes(b"".join(f"line-{i:04d}\n".encode() for i in range(500)))
        tail = read_tail(p, 100)
        assert not tail.startswith(b"ine")
        assert tail.startswith(b"line-")
        assert tail.endswith(b"line-0499\n")

    def test_tail_without_any_newline_is_returned_as_is(self, tmp_path):
        p = tmp_path / "blob.log"
        p.write_bytes(b"x" * 500)
        assert read_tail(p, 100) == b"x" * 100


class TestTrimLog:
    def test_file_under_the_cap_is_untouched(self, tmp_path):
        p = tmp_path / "s.log"
        p.write_bytes(b"a\n" * 10)
        res = trim_log(p, max_bytes=1024, keep_bytes=256)
        assert res["trimmed"] is False
        assert p.read_bytes() == b"a\n" * 10

    def test_oversize_file_keeps_its_tail(self, tmp_path):
        p = tmp_path / "big.log"
        p.write_bytes(b"".join(f"row-{i:05d}\n".encode() for i in range(5000)))
        before = p.stat().st_size
        res = trim_log(p, max_bytes=4096, keep_bytes=2048)
        assert res["trimmed"] is True
        assert res["before"] == before
        assert res["after"] < before
        body = p.read_bytes()
        assert body.startswith(b"--- log trimmed ")
        assert b"row-04999\n" in body
        assert b"row-00000\n" not in body

    def test_appending_after_a_trim_lands_at_the_new_end(self, tmp_path):
        """systemd holds server.log with O_APPEND, so truncation must be safe."""
        p = tmp_path / "append.log"
        p.write_bytes(b"".join(f"row-{i:05d}\n".encode() for i in range(5000)))
        trim_log(p, max_bytes=4096, keep_bytes=2048)
        after_trim = p.stat().st_size
        with p.open("ab") as fh:
            fh.write(b"post-trim\n")
        assert p.stat().st_size == after_trim + len(b"post-trim\n")
        assert p.read_bytes().endswith(b"post-trim\n")

    def test_missing_file_is_not_an_error(self, tmp_path):
        res = trim_log(tmp_path / "nope.log", 1024, 256)
        assert res == {"path": str(tmp_path / "nope.log"), "trimmed": False, "before": 0, "after": 0}

    def test_unreadable_file_reports_the_error(self, tmp_path):
        p = tmp_path / "bad.log"
        p.write_bytes(b"x" * 4096)
        with patch("agent.services.log_maintenance.read_tail", side_effect=OSError("eio")):
            res = trim_log(p, max_bytes=1024, keep_bytes=256)
        assert res["trimmed"] is False and "eio" in res["error"]

    def test_trim_logs_reports_one_row_per_path(self, tmp_path):
        a = tmp_path / "a.log"
        a.write_bytes(b"x" * 10)
        rows = trim_logs([a, tmp_path / "missing.log"], 1024, 256)
        assert [r["trimmed"] for r in rows] == [False, False]
        assert len(rows) == 2


# ─── 4. CLIENT-SIDE PARKED BACKOFF ──────────────────────────────────


class _Err:
    def __init__(self, code=503, retry_after=None):
        self.code = code
        self.headers = {"Retry-After": str(retry_after)} if retry_after is not None else {}


PARKED_BODY = '{"error_code": "all_workers_parked", "retryable": true}'


class TestRetryAfterSeconds:
    def test_header_is_used(self):
        assert retry_after_seconds(_Err(retry_after=45), 2.0) == 45.0

    def test_missing_header_falls_back(self):
        assert retry_after_seconds(_Err(), 2.0) == 2.0

    def test_garbage_header_falls_back(self):
        assert retry_after_seconds(_Err(retry_after="soon"), 2.0) == 2.0

    def test_header_is_clamped(self):
        assert retry_after_seconds(_Err(retry_after=99999), 2.0) == 300.0
        assert retry_after_seconds(_Err(retry_after=0), 2.0) == 1.0

    def test_object_without_headers_falls_back(self):
        assert retry_after_seconds(object(), 3.0) == 3.0


class TestIsParked:
    def test_parked_503(self):
        assert is_parked(_Err(), PARKED_BODY) is True

    def test_other_503_is_not_parked(self):
        assert is_parked(_Err(), '{"error": "UNUSUAL"}') is False

    def test_marker_on_a_non_503_is_not_parked(self):
        assert is_parked(_Err(code=500), PARKED_BODY) is False

    def test_empty_body(self):
        assert is_parked(_Err(), "") is False


class TestParkedBackoff:
    def test_non_parked_error_does_not_wait(self):
        b = ParkedBackoff()
        with patch("agent.services.parked_retry.time.sleep") as slept:
            assert b.wait(_Err(code=429), "{}") is False
        slept.assert_not_called()

    def test_parked_error_sleeps_the_retry_after(self):
        b = ParkedBackoff()
        with patch("agent.services.parked_retry.time.sleep") as slept:
            assert b.wait(_Err(retry_after=120), PARKED_BODY) is True
        assert slept.call_args[0][0] == pytest.approx(120, abs=1)
        assert b.waits == 1

    def test_default_delay_when_no_header(self):
        b = ParkedBackoff()
        with patch("agent.services.parked_retry.time.sleep") as slept:
            b.wait(_Err(), PARKED_BODY)
        assert slept.call_args[0][0] == pytest.approx(60, abs=1)

    def test_budget_bounds_the_total_wait(self):
        """Without the deadline the studios would retry a park forever."""
        b = ParkedBackoff(budget_s=100)
        with patch("agent.services.parked_retry.time.sleep") as slept:
            assert b.wait(_Err(retry_after=300), PARKED_BODY) is True
        assert slept.call_args[0][0] == pytest.approx(100, abs=1)

    def test_giving_up_once_the_budget_is_spent(self):
        b = ParkedBackoff(budget_s=10)
        b.deadline = time.time() - 1
        with patch("agent.services.parked_retry.time.sleep") as slept:
            assert b.wait(_Err(), PARKED_BODY) is False
        slept.assert_not_called()

    def test_deadline_is_set_once_not_extended_per_wait(self):
        b = ParkedBackoff(budget_s=600)
        with patch("agent.services.parked_retry.time.sleep"):
            b.wait(_Err(retry_after=5), PARKED_BODY)
            first = b.deadline
            b.wait(_Err(retry_after=5), PARKED_BODY)
        assert b.deadline == first and b.waits == 2


# ─── 5. THE 503 THE STUDIOS BACK OFF FROM ───────────────────────────


class TestParkedResponse:
    def test_parked_result_becomes_a_503_with_retry_after(self):
        res = _respond_flow_result({
            "status": 503, "error": "all nicks parked: nick-a, Nick-b",
            "error_code": "all_workers_parked", "retryable": True, "retry_after_s": 420,
        })
        assert res.status_code == 503
        assert res.headers["Retry-After"] == "420"
        body = json.loads(res.body)
        assert body["error"] == "ALL_WORKERS_PARKED"
        assert body["error_code"] == "all_workers_parked"
        assert body["retryable"] is True
        assert body["retry_after_s"] == 420

    def test_missing_retry_after_s_defaults_to_a_minute(self):
        res = _respond_flow_result({
            "status": 503, "error": "parked", "error_code": "all_workers_parked",
        })
        assert res.headers["Retry-After"] == "60"
        assert json.loads(res.body)["retry_after_s"] == 60

    def test_the_body_is_what_is_parked_makes_the_studios_back_off(self):
        """ParkedBackoff matches on the body, so the two must agree."""
        res = _respond_flow_result({
            "status": 503, "error": "parked", "error_code": "all_workers_parked",
            "retry_after_s": 90,
        })
        err = _Err(retry_after=int(res.headers["Retry-After"]))
        assert is_parked(err, res.body.decode()) is True

    def test_other_errors_still_raise(self):
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc:
            _respond_flow_result({"status": 500, "error": "kaboom"})
        assert exc.value.status_code == 500
