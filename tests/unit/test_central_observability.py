"""Unit tests for Central Watchdog & Incident Manager Observability System."""

import pytest
from fastapi.testclient import TestClient

from agent.main import app
from agent.services.incident_manager import get_incident_manager
from agent.services.central_watchdog import CentralWatchdog

client = TestClient(app)


def test_incident_manager_lifecycle():
    """Verify recording, retrieving, and resolving incidents in SQLite."""
    mgr = get_incident_manager()

    # 1. Record incident
    inc = mgr.record_incident(
        module="lookbook",
        job_id="test_job_123",
        sub_id="clip_1",
        severity="WARNING",
        error_code="TEST_TIMEOUT",
        message="Test incident message",
        root_cause="Mock timeout",
        action_taken="MOCK_RETRY"
    )
    assert inc["id"].startswith("inc_")
    assert inc["severity"] == "WARNING"

    # 2. Query incidents list
    incidents = mgr.get_incidents(module="lookbook", status="OPEN")
    matched = [i for i in incidents if i["id"] == inc["id"]]
    assert len(matched) == 1
    assert matched[0]["error_code"] == "TEST_TIMEOUT"

    # 3. Resolve incident
    success = mgr.resolve_incident(inc["id"], action_taken="MOCK_RESOLVED", severity="HEALED")
    assert success is True

    # 4. Verify resolved in summary
    summary = mgr.get_summary()
    assert summary["ok"] is True
    assert "system_health" in summary
    assert "modules" in summary


def test_incidents_api_endpoints():
    """Verify REST endpoints for incidents."""
    # GET summary
    resp = client.get("/api/system/incidents/summary")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert "system_health" in data
    assert "unresolved_count" in data
    assert "modules" in data

    # GET list
    resp = client.get("/api/system/incidents?limit=10")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert "incidents" in data
    assert isinstance(data["incidents"], list)

    # POST sweep
    resp = client.post("/api/system/incidents/sweep")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert "sweep_results" in data
    assert "summary" in data


def test_central_watchdog_sweep_domains():
    """Verify CentralWatchdog sweeps all 5 domains without raising exceptions."""
    watchdog = CentralWatchdog()
    res = watchdog.run_sweep()
    assert "lookbook" in res
    assert "tvc" in res
    assert "batch" in res
    assert "workers" in res
    assert "proxies" in res
    assert res["lookbook"]["checked"] >= 0
    assert res["tvc"]["checked"] >= 0
