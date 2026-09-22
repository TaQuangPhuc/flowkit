"""Incident auto-resolution — the watchdog must be able to close what it opens.

_sweep_workers/_sweep_proxies used to call resolve_incident(incident_id="worker_<nick>"),
but ids are generated (inc_<hex>) and that UPDATE matches on the primary key, so every
healing call silently returned False and incidents stayed OPEN for days.
"""

import asyncio

import pytest

from agent.services.central_watchdog import CentralWatchdog, _run_coro_blocking
from agent.services.incident_manager import get_incident_manager


def _open_worker_incident(nick_id, error_code="CHROME_WORKER_OFFLINE"):
    return get_incident_manager().record_incident(
        module="worker",
        sub_id=nick_id,
        severity="WARNING",
        error_code=error_code,
        message=f"{nick_id} offline",
        status="OPEN",
    )


def _status_of(inc_id):
    mgr = get_incident_manager()
    with mgr._get_connection() as conn:
        row = conn.execute("SELECT status FROM incident WHERE id = ?", (inc_id,)).fetchone()
    return row["status"] if row else None


class TestResolveBySub:
    def test_synthetic_id_never_matched(self):
        """Documents the original defect so it cannot come back."""
        mgr = get_incident_manager()
        _open_worker_incident("nick-synth")
        assert mgr.resolve_incident("worker_nick-synth", action_taken="PROBE") is False

    def test_resolves_by_subject(self):
        mgr = get_incident_manager()
        inc = _open_worker_incident("nick-heal")
        assert mgr.resolve_by_sub(
            module="worker", sub_id="nick-heal",
            error_code="CHROME_WORKER_OFFLINE", action_taken="WORKER_ACTIVE",
        ) == 1
        assert _status_of(inc["id"]) == "RESOLVED"

    def test_respects_error_code_filter(self):
        mgr = get_incident_manager()
        offline = _open_worker_incident("nick-multi", "CHROME_WORKER_OFFLINE")
        other = _open_worker_incident("nick-multi", "ACCOUNT_AUTH_EXPIRED")
        assert mgr.resolve_by_sub(
            module="worker", sub_id="nick-multi", error_code="CHROME_WORKER_OFFLINE",
        ) == 1
        assert _status_of(offline["id"]) == "RESOLVED"
        assert _status_of(other["id"]) == "OPEN"

    def test_second_call_is_a_noop(self):
        mgr = get_incident_manager()
        _open_worker_incident("nick-once")
        assert mgr.resolve_by_sub(module="worker", sub_id="nick-once") == 1
        assert mgr.resolve_by_sub(module="worker", sub_id="nick-once") == 0

    def test_wrong_module_does_not_match(self):
        mgr = get_incident_manager()
        inc = _open_worker_incident("nick-scope")
        assert mgr.resolve_by_sub(module="proxy", sub_id="nick-scope") == 0
        assert _status_of(inc["id"]) == "OPEN"


class TestSweepWorkersHealing:
    def test_sweep_resolves_incident_when_chrome_is_back(self, monkeypatch):
        inc = _open_worker_incident("nick-back")
        monkeypatch.setattr("agent.services.accounts.load_accounts",
                            lambda *a, **k: [{"id": "nick-back", "enabled": True}])
        monkeypatch.setattr("agent.services.chrome_nicks.chrome_running", lambda nid: True)
        monkeypatch.setattr("agent.services.chrome_nicks.find_running_chrome_pid", lambda nid: 4242)

        res = CentralWatchdog()._sweep_workers()
        assert res["healed"] == 1
        assert _status_of(inc["id"]) == "RESOLVED"

    def test_sweep_keeps_incident_open_while_offline(self, monkeypatch):
        monkeypatch.setattr("agent.services.accounts.load_accounts",
                            lambda *a, **k: [{"id": "nick-down", "enabled": True}])
        monkeypatch.setattr("agent.services.chrome_nicks.chrome_running", lambda nid: False)
        monkeypatch.setattr("agent.services.chrome_nicks.find_running_chrome_pid", lambda nid: None)

        res = CentralWatchdog()._sweep_workers()
        assert res["failed"] == 1
        open_ids = [i["id"] for i in get_incident_manager().get_incidents(module="worker", status="OPEN")]
        assert open_ids, "offline worker must leave an open incident"


class TestRunCoroBlocking:
    def test_outside_a_loop(self):
        async def coro():
            return {"ok": True, "where": "no-loop"}
        assert _run_coro_blocking(coro())["where"] == "no-loop"

    async def test_inside_a_running_loop(self):
        """asyncio.run() raises here — this is the /sweep endpoint's path."""
        async def coro():
            await asyncio.sleep(0)
            return {"ok": True, "where": "in-loop"}
        assert _run_coro_blocking(coro())["where"] == "in-loop"

    async def test_propagates_exceptions(self):
        async def boom():
            raise ValueError("rotation failed")
        with pytest.raises(ValueError, match="rotation failed"):
            _run_coro_blocking(boom())


class TestSweepEndpoint:
    def test_sweep_runs_off_the_event_loop(self, monkeypatch):
        """run_sweep() does blocking network probes; on the loop it stalls the API."""
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        import agent.api.incidents as incidents_api

        seen = {}

        def fake_sweep(self):
            try:
                asyncio.get_running_loop()
                seen["on_loop"] = True
            except RuntimeError:
                seen["on_loop"] = False
            return {"timestamp": 0}

        monkeypatch.setattr(incidents_api.CentralWatchdog, "run_sweep", fake_sweep)
        app = FastAPI()
        app.include_router(incidents_api.router)
        with TestClient(app) as cli:
            res = cli.post("/api/system/incidents/sweep")

        assert res.status_code == 200
        assert seen["on_loop"] is False


class TestCloseStale:
    def test_closes_old_unresolved_incidents(self):
        mgr = get_incident_manager()
        inc = _open_worker_incident("nick-old")
        with mgr._get_connection() as conn:
            conn.execute("UPDATE incident SET created_at = created_at - 172800 WHERE id = ?", (inc["id"],))
            conn.commit()
        assert mgr.close_stale(86400) >= 1
        assert _status_of(inc["id"]) == "RESOLVED"

    def test_keeps_recent_incidents_open(self):
        mgr = get_incident_manager()
        inc = _open_worker_incident("nick-fresh")
        mgr.close_stale(86400)
        assert _status_of(inc["id"]) == "OPEN"

    def test_keeps_auth_expired_open_for_a_human(self):
        mgr = get_incident_manager()
        inc = _open_worker_incident("nick-auth", error_code="ACCOUNT_AUTH_EXPIRED")
        with mgr._get_connection() as conn:
            conn.execute("UPDATE incident SET created_at = created_at - 172800 WHERE id = ?", (inc["id"],))
            conn.commit()
        mgr.close_stale(86400)
        assert _status_of(inc["id"]) == "OPEN"

    def test_closing_does_not_inflate_healed_count(self):
        mgr = get_incident_manager()
        inc = _open_worker_incident("nick-noheal")
        with mgr._get_connection() as conn:
            conn.execute("UPDATE incident SET created_at = created_at - 172800 WHERE id = ?", (inc["id"],))
            conn.commit()
        mgr.close_stale(86400)
        with mgr._get_connection() as conn:
            row = conn.execute("SELECT severity FROM incident WHERE id = ?", (inc["id"],)).fetchone()
        assert row["severity"] == "INFO"

    def test_auto_healing_status_is_also_closed(self):
        """20 GHOST_QUEUE_FAILOVER rows sat in AUTO_HEALING, not OPEN."""
        mgr = get_incident_manager()
        inc = mgr.record_incident(module="flow", job_id="op-orphan",
                                  error_code="GHOST_QUEUE_FAILOVER",
                                  message="orphaned", status="AUTO_HEALING")
        with mgr._get_connection() as conn:
            conn.execute("UPDATE incident SET created_at = created_at - 172800 WHERE id = ?", (inc["id"],))
            conn.commit()
        assert mgr.close_stale(86400) >= 1
        assert _status_of(inc["id"]) == "RESOLVED"

    def test_sweep_reports_stale_domain(self, monkeypatch):
        monkeypatch.setattr("agent.services.accounts.load_accounts", lambda *a, **k: [])
        res = CentralWatchdog().run_sweep()
        assert "stale" in res and "closed" in res["stale"]
