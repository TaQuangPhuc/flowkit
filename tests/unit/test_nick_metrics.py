"""Unit tests for NickMetricsTracker real-time telemetry."""
import time
import pytest
from agent.services.nick_metrics import NickMetricsTracker, get_nick_metrics_tracker


@pytest.fixture(autouse=True)
def reset_tracker():
    tracker = get_nick_metrics_tracker()
    tracker.clear()
    yield tracker
    tracker.clear()


def test_concurrency_tracking(reset_tracker):
    tracker = reset_tracker
    worker_id = "test-worker-1"

    # Initially 0
    m0 = tracker.get_metrics(worker_id)
    assert m0["current_concurrency"] == 0
    assert m0["peak_concurrency"] == 0
    assert m0["max_concurrency_limit"] == 20

    # Surge to 12 in-flight
    tracker.record_in_flight(worker_id, 12)
    m1 = tracker.get_metrics(worker_id)
    assert m1["current_concurrency"] == 12
    assert m1["peak_concurrency"] == 12

    # Spike to 19 in-flight
    tracker.record_in_flight(worker_id, 19)
    m2 = tracker.get_metrics(worker_id)
    assert m2["current_concurrency"] == 19
    assert m2["peak_concurrency"] == 19

    # Drop back down to 3
    tracker.record_in_flight(worker_id, 3)
    m3 = tracker.get_metrics(worker_id)
    assert m3["current_concurrency"] == 3
    # Peak concurrency must remain at 19!
    assert m3["peak_concurrency"] == 19


def test_rpm_sliding_window_and_peaks(reset_tracker):
    tracker = reset_tracker
    worker_id = "test-worker-2"
    now = time.time()

    # Simulate 5 dispatches within last 10 seconds
    for offset in [50, 40, 30, 20, 10]:
        tracker._get_worker(worker_id).record_dispatch(now - offset)

    snapshot = tracker.get_metrics(worker_id)
    assert snapshot["current_rpm"] == 5
    assert snapshot["peak_rpm"] == 5

    # Simulate 3 older requests from 90 seconds ago (outside 60s window)
    for offset in [120, 100, 90]:
        tracker._get_worker(worker_id).dispatch_timestamps.appendleft(now - offset)

    # 60s RPM should still be 5
    snapshot2 = tracker.get_metrics(worker_id)
    assert snapshot2["current_rpm"] == 5
    # 5m average includes the older ones (8 / 5.0 = 1.6)
    assert snapshot2["rpm_5m_avg"] == 1.6


def test_completion_and_latencies(reset_tracker):
    tracker = reset_tracker
    worker_id = "test-worker-3"

    tracker.record_dispatch(worker_id)
    tracker.record_completion(worker_id, success=True, latency_ms=300)

    tracker.record_dispatch(worker_id)
    tracker.record_completion(worker_id, success=True, latency_ms=500)

    tracker.record_dispatch(worker_id)
    tracker.record_completion(worker_id, success=False, latency_ms=200, error="QuotaExceeded")

    m = tracker.get_metrics(worker_id)
    assert m["total_requests"] == 3
    assert m["successful_requests"] == 2
    assert m["failed_requests"] == 1
    assert m["success_rate_percent"] == 66.7
    assert m["avg_latency_ms"] == 333.3
    assert m["last_latency_ms"] == 200
    assert m["last_error"] == "QuotaExceeded"


def test_cluster_aggregates(reset_tracker):
    tracker = reset_tracker

    tracker.record_in_flight("nick-1", 10)
    tracker.record_in_flight("nick-2", 8)

    tracker.record_dispatch("nick-1")
    tracker.record_dispatch("nick-2")

    all_m = tracker.get_all_metrics()
    cluster = all_m["cluster"]

    assert cluster["current_concurrency"] == 18
    assert cluster["peak_concurrency"] == 18
    assert cluster["current_rpm"] == 2
    assert cluster["total_requests"] == 2
    assert cluster["active_nicks_count"] == 2


def test_reset_metrics(reset_tracker):
    tracker = reset_tracker
    tracker.record_in_flight("nick-1", 15)
    tracker.record_dispatch("nick-1")
    tracker.record_completion("nick-1", success=True, latency_ms=400)

    assert tracker.get_metrics("nick-1")["peak_concurrency"] == 15
    assert tracker.get_metrics("nick-1")["total_requests"] == 1

    # Reset single worker
    tracker.reset("nick-1")
    m = tracker.get_metrics("nick-1")
    assert m["total_requests"] == 0
    assert m["peak_concurrency"] == 15  # reset keeps current as peak


def test_api_metrics_endpoints(reset_tracker):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from agent.api import accounts as accounts_api

    tracker = reset_tracker
    tracker.record_in_flight("nick-a", 7)
    tracker.record_dispatch("nick-a")
    tracker.record_completion("nick-a", success=True, latency_ms=250)

    app = FastAPI()
    app.include_router(accounts_api.router, prefix="/api")
    client = TestClient(app)

    # Test GET /api/accounts/metrics
    resp = client.get("/api/accounts/metrics")
    assert resp.status_code == 200
    data = resp.json()
    assert "cluster" in data
    assert data["cluster"]["current_concurrency"] == 7
    assert data["cluster"]["peak_concurrency"] == 7
    assert "nick-a" in data["nicks"]
    assert data["nicks"]["nick-a"]["current_concurrency"] == 7
    assert data["nicks"]["nick-a"]["peak_concurrency"] == 7
    assert data["nicks"]["nick-a"]["total_requests"] == 1

    # Test GET /api/accounts/{nick_id}/metrics
    resp_nick = client.get("/api/accounts/nick-a/metrics")
    assert resp_nick.status_code == 200
    nick_data = resp_nick.json()
    assert nick_data["worker_id"] == "nick-a"
    assert nick_data["current_concurrency"] == 7
    assert nick_data["max_concurrency_limit"] == 20

    # Test POST /api/accounts/metrics/reset
    resp_reset = client.post("/api/accounts/metrics/reset", json={"worker_id": "nick-a"})
    assert resp_reset.status_code == 200
    assert resp_reset.json()["ok"] is True

    # After reset, total_requests is 0
    resp_after = client.get("/api/accounts/nick-a/metrics")
    assert resp_after.json()["total_requests"] == 0

