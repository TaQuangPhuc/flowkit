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


class TestProxyErrorDecay:
    """Error streaks are lifetime counters kept on disk.

    Without a decay window the watchdog re-quarantined the same proxies on every
    sweep forever — on 22 Sep that took the live surfshark gateway down with a
    retired pool, parking the nicks behind it for 15 minutes at a time.
    """

    def _errored(self, age_s: float) -> ProxyHealthRecord:
        rec = ProxyHealthRecord("http://u:p@10.0.0.9:8000")
        for _ in range(3):
            rec.record_error("PUBLIC_ERROR_UNUSUAL_ACTIVITY")
        rec.last_error_at = time.time() - age_s
        return rec

    def test_a_fresh_streak_is_kept(self):
        rec = self._errored(60)
        assert rec.decay_stale_errors(1800) is False
        assert rec.consecutive_errors == 3

    def test_a_stale_streak_is_forgotten(self):
        rec = self._errored(3600)
        assert rec.decay_stale_errors(1800) is True
        assert rec.consecutive_errors == 0
        assert rec.last_error_reason is None

    def test_lifetime_totals_survive_the_decay(self):
        rec = self._errored(3600)
        rec.decay_stale_errors(1800)
        assert rec.failed_requests == 3 and rec.total_requests == 3

    def test_a_clean_record_is_untouched(self):
        rec = ProxyHealthRecord("http://u:p@10.0.0.9:8000")
        rec.record_success()
        assert rec.decay_stale_errors(1800) is False

    def test_a_state_file_written_before_the_decay_existed(self):
        """No last_error_at: stop punishing it rather than punish it forever."""
        rec = self._errored(0)
        rec.last_error_at = None
        rec.last_used_at = None
        assert rec.decay_stale_errors(1800) is True
        assert rec.consecutive_errors == 0

    def test_record_error_stamps_the_time(self):
        rec = ProxyHealthRecord("http://u:p@10.0.0.9:8000")
        rec.record_error("X")
        assert rec.last_error_at is not None
        assert abs(rec.last_error_at - rec.last_used_at) < 1.0
        rec.record_success()
        assert rec.last_error_at is None

    def test_the_timestamp_round_trips_through_disk(self, tmp_path, monkeypatch):
        import agent.services.unusual_audit as mod
        monkeypatch.setattr(mod, "FATIGUE_STATE_FILE", tmp_path / "fatigue.json")
        mgr = UnusualAuditManager()
        rec = mgr._get_or_create_proxy_record("http://u:p@10.0.0.9:8000")
        rec.record_error("PUBLIC_ERROR_UNUSUAL_ACTIVITY")
        mgr._save_proxy_state_to_disk()

        reloaded = UnusualAuditManager()
        reloaded._proxy_records.clear()
        reloaded._load_proxy_state_from_disk()
        back = [r for r in reloaded._proxy_records.values() if r.ip == "10.0.0.9"]
        assert back and back[0].last_error_at == rec.last_error_at
