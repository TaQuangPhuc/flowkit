"""Real-time Concurrency and RPM Telemetry System for Google Flow Nicks.

Tracks and exposes:
- Current active in-flight requests per nick
- Peak (all-time high) concurrency seen per nick
- Current rolling 60-second RPM (Requests Per Minute) per nick
- Peak rolling 60-second RPM recorded per nick
- 5-minute rolling average RPM
- Success / failure rates, latencies, and cluster-wide aggregates
"""
from __future__ import annotations

import collections
import logging
import threading
import time
from typing import Any, Dict, List, Optional

from agent.config import PROFILE_MAX_CONCURRENT

logger = logging.getLogger(__name__)


class NickMetricRecord:
    """Telemetry counters and sliding-window stats for a single nick/worker."""

    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.started_at = time.time()
        self.current_concurrency = 0
        self.peak_concurrency = 0
        self.max_concurrency_limit = PROFILE_MAX_CONCURRENT
        self.peak_rpm = 0
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.last_latency_ms = 0
        self.last_request_at: Optional[float] = None
        self.last_error: Optional[str] = None
        self.last_error_at: Optional[float] = None

        # Sliding window of dispatch timestamps (up to 2000 events)
        self.dispatch_timestamps: collections.deque[float] = collections.deque(maxlen=2000)
        # Recent latencies for rolling average
        self.recent_latencies: collections.deque[int] = collections.deque(maxlen=200)

    def record_in_flight(self, count: int) -> None:
        self.current_concurrency = max(0, count)
        if self.current_concurrency > self.peak_concurrency:
            self.peak_concurrency = self.current_concurrency

    def record_dispatch(self, timestamp: Optional[float] = None) -> int:
        now = timestamp or time.time()
        self.dispatch_timestamps.append(now)
        self.total_requests += 1
        self.last_request_at = now

        # Compute current 60s RPM
        rpm = sum(1 for t in self.dispatch_timestamps if now - t <= 60.0)
        if rpm > self.peak_rpm:
            self.peak_rpm = rpm
        return rpm

    def record_completion(
        self,
        success: bool,
        latency_ms: int = 0,
        error: str = "",
        timestamp: Optional[float] = None,
    ) -> None:
        now = timestamp or time.time()
        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
            if error:
                self.last_error = str(error)[:200]
                self.last_error_at = now

        if latency_ms > 0:
            self.last_latency_ms = latency_ms
            self.recent_latencies.append(latency_ms)

    def get_snapshot(self, now: Optional[float] = None) -> dict[str, Any]:
        ts = now or time.time()
        # Clean / count in 60s window
        rpm_60s = sum(1 for t in self.dispatch_timestamps if ts - t <= 60.0)
        rpm_5m_count = sum(1 for t in self.dispatch_timestamps if ts - t <= 300.0)
        rpm_5m_avg = round(rpm_5m_count / 5.0, 1)

        if rpm_60s > self.peak_rpm:
            self.peak_rpm = rpm_60s

        success_rate = (
            round((self.successful_requests / self.total_requests) * 100.0, 1)
            if self.total_requests > 0
            else 100.0
        )
        avg_latency = (
            round(sum(self.recent_latencies) / len(self.recent_latencies), 1)
            if self.recent_latencies
            else 0.0
        )

        return {
            "worker_id": self.worker_id,
            "current_concurrency": self.current_concurrency,
            "peak_concurrency": self.peak_concurrency,
            "max_concurrency_limit": self.max_concurrency_limit,
            "current_rpm": rpm_60s,
            "peak_rpm": self.peak_rpm,
            "rpm_5m_avg": rpm_5m_avg,
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "success_rate_percent": success_rate,
            "avg_latency_ms": avg_latency,
            "last_latency_ms": self.last_latency_ms,
            "last_request_at": self.last_request_at,
            "last_error": self.last_error,
            "last_error_at": self.last_error_at,
            "uptime_seconds": int(ts - self.started_at),
        }

    def reset(self) -> None:
        """Reset historical peak metrics and request counters."""
        self.started_at = time.time()
        self.peak_concurrency = self.current_concurrency
        now = time.time()
        rpm_60s = sum(1 for t in self.dispatch_timestamps if now - t <= 60.0)
        self.peak_rpm = rpm_60s
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.recent_latencies.clear()
        self.last_error = None
        self.last_error_at = None


class NickMetricsTracker:
    """Thread-safe singleton tracking concurrency and RPM across all Flow workers."""

    _instance: Optional[NickMetricsTracker] = None
    _init_lock = threading.Lock()

    def __init__(self):
        self._lock = threading.Lock()
        self._workers: dict[str, NickMetricRecord] = {}
        self._global_dispatches: collections.deque[float] = collections.deque(maxlen=10000)
        self._global_peak_rpm = 0
        self._global_peak_concurrency = 0

    @classmethod
    def get_instance(cls) -> NickMetricsTracker:
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def _get_worker(self, worker_id: str) -> NickMetricRecord:
        key = str(worker_id or "default").strip()
        if key not in self._workers:
            self._workers[key] = NickMetricRecord(key)
        return self._workers[key]

    def record_in_flight(self, worker_id: str, count: int) -> None:
        """Update current in-flight concurrent requests for a worker."""
        with self._lock:
            w = self._get_worker(worker_id)
            w.record_in_flight(count)

            # Update global peak concurrency
            total_current = sum(record.current_concurrency for record in self._workers.values())
            if total_current > self._global_peak_concurrency:
                self._global_peak_concurrency = total_current

    def record_dispatch(self, worker_id: str) -> dict[str, int]:
        """Record an RPC dispatch event for a worker."""
        now = time.time()
        with self._lock:
            w = self._get_worker(worker_id)
            rpm = w.record_dispatch(now)
            self._global_dispatches.append(now)

            global_rpm = sum(1 for t in self._global_dispatches if now - t <= 60.0)
            if global_rpm > self._global_peak_rpm:
                self._global_peak_rpm = global_rpm

            return {"worker_rpm": rpm, "global_rpm": global_rpm}

    def record_completion(
        self,
        worker_id: str,
        success: bool,
        latency_ms: int = 0,
        error: str = "",
        queue_ms: int | None = None,
    ) -> None:
        """Record the completion of an RPC request."""
        with self._lock:
            w = self._get_worker(worker_id)
            w.record_completion(success=success, latency_ms=latency_ms, error=error)
        try:
            from agent.services.request_ledger import record_outcome
            record_outcome(worker_id, success, latency_ms, error, queue_ms=queue_ms)
        except Exception:
            pass

    def get_metrics(self, worker_id: str) -> dict[str, Any]:
        """Get snapshot metrics for a single worker."""
        now = time.time()
        with self._lock:
            w = self._get_worker(worker_id)
            return w.get_snapshot(now)

    def get_all_metrics(self) -> dict[str, Any]:
        """Get snapshot metrics for all workers plus cluster-wide aggregates."""
        now = time.time()
        with self._lock:
            worker_metrics = {k: v.get_snapshot(now) for k, v in self._workers.items()}

            total_concurrency = sum(m["current_concurrency"] for m in worker_metrics.values())
            global_rpm = sum(1 for t in self._global_dispatches if now - t <= 60.0)
            if global_rpm > self._global_peak_rpm:
                self._global_peak_rpm = global_rpm

            total_requests = sum(m["total_requests"] for m in worker_metrics.values())
            total_success = sum(m["successful_requests"] for m in worker_metrics.values())
            total_failed = sum(m["failed_requests"] for m in worker_metrics.values())

            cluster_success_rate = (
                round((total_success / total_requests) * 100.0, 1)
                if total_requests > 0
                else 100.0
            )

            return {
                "cluster": {
                    "current_concurrency": total_concurrency,
                    "peak_concurrency": self._global_peak_concurrency,
                    "current_rpm": global_rpm,
                    "peak_rpm": self._global_peak_rpm,
                    "total_requests": total_requests,
                    "successful_requests": total_success,
                    "failed_requests": total_failed,
                    "success_rate_percent": cluster_success_rate,
                    "active_nicks_count": len(worker_metrics),
                },
                "nicks": worker_metrics,
            }

    def clear(self) -> None:
        """Completely clear all tracked workers and global state."""
        with self._lock:
            self._workers.clear()
            self._global_dispatches.clear()
            self._global_peak_rpm = 0
            self._global_peak_concurrency = 0

    def reset(self, worker_id: Optional[str] = None) -> None:
        """Reset historical peak metrics."""
        with self._lock:
            if worker_id:
                key = str(worker_id).strip()
                if key in self._workers:
                    self._workers[key].reset()
            else:
                for w in self._workers.values():
                    w.reset()
                self._global_dispatches.clear()
                self._global_peak_rpm = 0
                self._global_peak_concurrency = sum(
                    w.current_concurrency for w in self._workers.values()
                )


def get_nick_metrics_tracker() -> NickMetricsTracker:
    return NickMetricsTracker.get_instance()
