"""Unit tests for Unusual Activity Audit Logging & Forensic Diagnostics."""
import json
import time
from pathlib import Path
from agent.services.unusual_audit import UnusualAuditManager, ProxyHealthRecord

def test_proxy_health_record():
    proxy_url = "http://testuser:testpass@163.61.71.7:29419"
    rec = ProxyHealthRecord(proxy_url)
    assert rec.ip == "163.61.71.7"
    assert rec.port == 29419
    assert rec.total_requests == 0
    assert rec.consecutive_errors == 0

    rec.record_success()
    assert rec.total_requests == 1
    assert rec.successful_requests == 1
    assert rec.consecutive_successes == 1

    rec.record_error("PUBLIC_ERROR_UNUSUAL_ACTIVITY")
    assert rec.total_requests == 2
    assert rec.failed_requests == 1
    assert rec.consecutive_errors == 1
    assert rec.consecutive_successes == 0


def test_audit_manager_burst_tracking(tmp_path):
    mgr = UnusualAuditManager()
    worker_id = "test-worker-1"

    m1 = mgr.record_request_dispatched(worker_id)
    assert m1["burst_10s"] == 1
    assert m1["gap_seconds"] == 999.0

    # Dispatch second request immediately
    m2 = mgr.record_request_dispatched(worker_id)
    assert m2["burst_10s"] == 2
    assert m2["gap_seconds"] < 1.0


def test_audit_manager_heuristic_diagnosis():
    mgr = UnusualAuditManager()

    # Case 1: Rate Burst
    burst = {"gap_seconds": 0.5, "burst_10s": 4}
    diag = mgr.diagnose_root_cause("ogiZ0b", burst, None, "PUBLIC_ERROR_UNUSUAL_ACTIVITY")
    assert "RATE_BURST" in diag["primary_cause"]

    # Case 2: Burned Datacenter IP
    burst_normal = {"gap_seconds": 15.0, "burst_10s": 1}
    rec = ProxyHealthRecord("http://u:p@103.20.10.5:8080")
    diag2 = mgr.diagnose_root_cause("ogiZ0b", burst_normal, rec, "PUBLIC_ERROR_UNUSUAL_ACTIVITY")
    assert "BURNED_PROXY_IP" in diag2["primary_cause"]

    # Case 3: Combined recovery succeeds without isolating the root cause.
    rot_info = {"retry_attempted": True, "retry_success": True, "new_proxy_ip": "161.248.213.53"}
    diag3 = mgr.diagnose_root_cause("ogiZ0b", burst_normal, None, "PUBLIC_ERROR_UNUSUAL_ACTIVITY", rotation_result=rot_info)
    assert any("RECOVERY_SUCCEEDED_CAUSE_UNCONFIRMED" in r for r in diag3["reasons"])
    assert not any("100%" in r or "CONFIRMED_IP_REPUTATION" in r for r in diag3["reasons"])


def test_audit_manager_record_and_summary(tmp_path):
    mgr = UnusualAuditManager()
    mgr.clear_in_memory_records()

    event = mgr.record_unusual_event(
        worker_id="nick-a",
        rpc_id="agJzFb",
        raw_error="RpcError: agJzFb failed: [7, None, [['type.googleapis.com/google.rpc.ErrorInfo', ['PUBLIC_ERROR_UNUSUAL_ACTIVITY']]]]",
        burst_metrics={"gap_seconds": 2.1, "burst_10s": 1, "burst_30s": 1, "burst_60s": 1, "global_burst_10s": 1},
        call_duration_ms=850,
        payload_summary={"project_id": "test-pid-123"},
        rotation_info={"rotation_triggered": True, "new_proxy": "http://***:***@161.248.212.33:29702", "new_proxy_ip": "161.248.212.33"},
    )

    assert event["worker_id"] == "nick-a"
    assert event["rpc_id"] == "agJzFb"
    assert "Gemini" in event["action_name"]
    assert "diagnosis" in event

    recent = mgr.get_recent_events(limit=10)
    assert len(recent) >= 1
    assert recent[0]["id"] == event["id"]

    summary = mgr.get_audit_summary()
    assert summary["total_incidents_recorded"] >= 1
    assert summary["incidents_last_24h"] >= 1
    assert "agJzFb" in summary["breakdown_by_rpc_24h"]
