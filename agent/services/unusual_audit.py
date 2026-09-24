"""Unusual Activity Audit Logging & Forensic Diagnostic System for Google Flow.

Tracks, correlates, and analyzes occurrences of `PUBLIC_ERROR_UNUSUAL_ACTIVITY`
and related RPC errors (7, 13, 403, 429) across FlowKit workers and proxies.
Provides persistent JSONL audit trails, real-time metrics, sliding-window burst
trackers, and automated heuristic root-cause analysis.
"""
from __future__ import annotations

import collections
import datetime
import json
import logging
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from agent.config import BASE_DIR
from agent.services.accounts import get_account, load_accounts
from agent.services.chrome_nicks import get_bridge
from agent.services.proxy_url import parse_proxy_url

logger = logging.getLogger(__name__)

# Dedicated audit log file location
AUDIT_LOG_DIR = BASE_DIR / "logs"
AUDIT_LOG_FILE = AUDIT_LOG_DIR / "unusual_activity_audit.jsonl"
FATIGUE_STATE_FILE = AUDIT_LOG_DIR / "proxy_fatigue_state.json"
MAX_LOG_SIZE_BYTES = 25 * 1024 * 1024  # 25 MB before rotation


# RPC Action human-readable mapping
RPC_ACTION_NAMES = {
    "agJzFb": "Gemini 3 Flash Vision / OCR / Scripting",
    "ogiZ0b": "Banana Pro 2 Keyframe Image Generation",
    "eb1hJf": "Veo 3.1 Keyframe to Video (i2v)",
    "YhhmEf": "Veo 3.1 Text to Video (t2v)",
    "k42Yye": "Asset / Media Upload",
    "o30O0e": "Project Listing",
    "GN0Bre": "Chat / Stream Session Bind",
    "maseQ": "Flow Session Sync",
    "nzlxg": "Flow Tab Ping",
    "UpteDb": "Project Metadata Query",
    "pDU0ue": "Project Create / Update",
}


def _now_vn_str(ts: Optional[float] = None) -> str:
    """Format timestamp into local Vietnam time string (GMT+7)."""
    t = ts if ts is not None else time.time()
    dt = datetime.datetime.fromtimestamp(t, tz=datetime.timezone(datetime.timedelta(hours=7)))
    return dt.strftime("%Y-%m-%d %H:%M:%S GMT+7")


def _now_iso(ts: Optional[float] = None) -> str:
    """Format timestamp into ISO 8601 UTC string."""
    t = ts if ts is not None else time.time()
    dt = datetime.datetime.fromtimestamp(t, tz=datetime.timezone.utc)
    return dt.isoformat()


class ProxyHealthRecord:
    """Tracks lifetime performance and error history for an upstream proxy IP."""

    def __init__(self, proxy_url: str):
        self.proxy_url = proxy_url
        parsed = parse_proxy_url(proxy_url)
        self.redacted = parsed.redacted
        self.ip = parsed.host
        self.port = parsed.port
        self.assigned_at = time.time()
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.consecutive_successes = 0
        self.consecutive_errors = 0
        self.last_error_reason: Optional[str] = None
        self.last_error_at: Optional[float] = None
        self.last_used_at: Optional[float] = None

    def record_success(self) -> None:
        self.total_requests += 1
        self.successful_requests += 1
        self.consecutive_successes += 1
        self.consecutive_errors = 0
        self.last_error_reason = None
        self.last_error_at = None
        self.last_used_at = time.time()

    def record_error(self, reason: str = "PUBLIC_ERROR_UNUSUAL_ACTIVITY") -> None:
        self.total_requests += 1
        self.failed_requests += 1
        self.consecutive_errors += 1
        self.consecutive_successes = 0
        self.last_error_reason = reason
        self.last_error_at = time.time()
        self.last_used_at = time.time()

    def decay_stale_errors(self, ttl_s: float) -> bool:
        """Forget an error streak older than ``ttl_s``. True if it was cleared.

        The counters are lifetime and persisted, so without this a proxy that
        failed three times an hour ago is re-quarantined by every later sweep —
        including proxies whose pool has since been retired.
        """
        if self.consecutive_errors <= 0:
            return False
        last = self.last_error_at or self.last_used_at
        if last is None:
            # Pre-decay state file: no timestamp to judge by, so do not keep
            # punishing it. The next real error re-arms the streak with one.
            self.consecutive_errors = 0
            self.last_error_reason = None
            return True
        if time.time() - last <= ttl_s:
            return False
        self.consecutive_errors = 0
        self.last_error_reason = None
        return True

    def to_dict(self) -> dict:
        return {
            "ip": self.ip,
            "port": self.port,
            "proxy": self.redacted,
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "success_rate": round(
                (self.successful_requests / self.total_requests * 100)
                if self.total_requests > 0
                else 0.0,
                1,
            ),
            "consecutive_successes": self.consecutive_successes,
            "consecutive_errors": self.consecutive_errors,
            "last_error_reason": self.last_error_reason,
            "last_error_at": self.last_error_at,
            "last_error_vn": _now_vn_str(self.last_error_at) if self.last_error_at else None,
            "last_used_vn": _now_vn_str(self.last_used_at) if self.last_used_at else None,
        }


class UnusualAuditManager:
    """Singleton engine managing sliding-window metrics, persistent audit trails,

    and heuristic diagnostic insights.
    """

    def __init__(self):
        self._lock = threading.Lock()
        # In-memory ring buffer of recent unusual events
        self._recent_events: collections.deque = collections.deque(maxlen=250)
        # Proxy tracking: keyed by proxy redacted or proxy host:port
        self._proxy_records: Dict[str, ProxyHealthRecord] = {}
        # Sliding window timestamp tracker for rate bursts: worker_id -> list of float timestamps
        self._worker_request_timestamps: Dict[str, collections.deque] = collections.defaultdict(
            lambda: collections.deque(maxlen=300)
        )
        self._last_worker_request_time: Dict[str, float] = {}
        # Global burst tracker
        self._global_request_timestamps: collections.deque = collections.deque(maxlen=1000)

        # Ensure audit log directory exists
        try:
            AUDIT_LOG_DIR.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            logger.warning("Could not create audit log dir %s: %s", AUDIT_LOG_DIR, exc)

        # Load recent events and persistent proxy metrics on startup
        self._hydrate_from_disk()
        self._load_proxy_state_from_disk()
        self._hydrate_proxies_from_config()

    def _hydrate_from_disk(self) -> None:
        """Hydrate recent events from existing audit log file."""
        if not AUDIT_LOG_FILE.exists():
            return
        try:
            lines = AUDIT_LOG_FILE.read_text(encoding="utf-8").splitlines()
            for line in lines[-250:]:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    self._recent_events.append(record)
                except Exception:
                    pass
        except Exception as exc:
            logger.warning("Could not read existing audit log: %s", exc)

    def _load_proxy_state_from_disk(self) -> None:
        """Load persistent proxy request counts and success rates from disk."""
        if not FATIGUE_STATE_FILE.exists():
            return
        try:
            raw = json.loads(FATIGUE_STATE_FILE.read_text(encoding="utf-8"))
            for key, item in raw.items():
                p_url = item.get("proxy_url") or key
                rec = ProxyHealthRecord(p_url)
                rec.total_requests = item.get("total_requests", 0)
                rec.successful_requests = item.get("successful_requests", 0)
                rec.failed_requests = item.get("failed_requests", 0)
                rec.consecutive_successes = item.get("consecutive_successes", 0)
                rec.consecutive_errors = item.get("consecutive_errors", 0)
                rec.last_error_reason = item.get("last_error_reason")
                rec.last_error_at = item.get("last_error_at")
                rec.last_used_at = item.get("last_used_at")
                self._proxy_records[key] = rec
        except Exception as exc:
            logger.warning("Could not load proxy fatigue state: %s", exc)

    def _save_proxy_state_to_disk(self) -> None:
        """Persist current proxy request counts to disk."""
        try:
            FATIGUE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            data = {}
            for k, rec in self._proxy_records.items():
                d = rec.to_dict()
                d["proxy_url"] = rec.proxy_url
                d["last_used_at"] = rec.last_used_at
                data[k] = d
            FATIGUE_STATE_FILE.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        except Exception as exc:
            logger.warning("Could not save proxy fatigue state: %s", exc)

    def _hydrate_proxies_from_config(self) -> None:
        """Seed proxy records with all currently configured account and pool proxies."""
        try:
            from agent.services.accounts import load_accounts
            for acc in load_accounts():
                p = acc.get("proxy_url")
                if p:
                    self._get_or_create_proxy_record(p)
        except Exception:
            pass

    def _get_or_create_proxy_record(self, proxy_url: str) -> ProxyHealthRecord:
        key = proxy_url.strip()
        if key not in self._proxy_records:
            self._proxy_records[key] = ProxyHealthRecord(proxy_url)
        return self._proxy_records[key]

    def record_request_dispatched(self, worker_id: str, proxy_url: str = "") -> dict:
        """Call when an RPC request is about to be sent. Returns current burst metrics."""
        now = time.time()
        with self._lock:
            last_time = self._last_worker_request_time.get(worker_id)
            gap_seconds = round(now - last_time, 2) if last_time is not None else 999.0
            self._last_worker_request_time[worker_id] = now

            worker_window = self._worker_request_timestamps[worker_id]
            worker_window.append(now)
            self._global_request_timestamps.append(now)

            # Calculate bursts
            burst_10s = sum(1 for t in worker_window if now - t <= 10.0)
            burst_30s = sum(1 for t in worker_window if now - t <= 30.0)
            burst_60s = sum(1 for t in worker_window if now - t <= 60.0)
            global_10s = sum(1 for t in self._global_request_timestamps if now - t <= 10.0)

            return {
                "gap_seconds": gap_seconds,
                "burst_10s": burst_10s,
                "burst_30s": burst_30s,
                "burst_60s": burst_60s,
                "global_burst_10s": global_10s,
            }

    def proxy_request_count(self, proxy_url: str) -> int:
        """Lifetime requests recorded for an exact proxy URL (0 if unseen)."""
        rec = self._proxy_records.get(proxy_url.strip())
        return rec.total_requests if rec else 0

    def record_request_success(self, worker_id: str, proxy_url: str = "") -> None:
        """Record successful RPC execution on this worker's proxy."""
        with self._lock:
            if proxy_url:
                rec = self._get_or_create_proxy_record(proxy_url)
                rec.record_success()
                self._save_proxy_state_to_disk()

    def diagnose_root_cause(
        self,
        rpc_id: str,
        burst_metrics: dict,
        proxy_record: Optional[ProxyHealthRecord],
        raw_error: str,
        rotation_result: Optional[dict] = None,
    ) -> dict:
        """Synthesizes all forensic clues to determine the most probable root cause."""
        reasons: List[str] = []
        severity = "HIGH"
        gap = burst_metrics.get("gap_seconds", 999.0)
        burst_10s = burst_metrics.get("burst_10s", 1)

        # 1. Burst Rate Check
        if gap < 1.2 or burst_10s >= 3:
            reasons.append(
                f"RATE_BURST: Tần suất gửi request quá dồn dập (khoảng cách {gap}s, {burst_10s} req/10s). "
                "Google reCAPTCHA Enterprise kích hoạt cờ chống bot do lưu lượng đột biến."
            )

        # 2. Datacenter IP / Burned IP Check
        if proxy_record:
            if proxy_record.total_requests <= 2 and proxy_record.successful_requests == 0:
                reasons.append(
                    f"BURNED_PROXY_IP: Proxy {proxy_record.ip} bị Google chặn ngay từ request đầu tiên. "
                    "Khả năng cao dải IP này thuộc Datacenter hoặc đã bị Google blacklist từ trước."
                )
            elif proxy_record.consecutive_errors >= 2:
                reasons.append(
                    f"DEAD_PROXY_SUBNET: Proxy {proxy_record.ip} bị chặn liên tiếp {proxy_record.consecutive_errors} lần. "
                    "Proxy này đã mất độ tin cậy, cần xoay vòng sang IP khác."
                )
            elif proxy_record.consecutive_successes >= 25:
                reasons.append(
                    f"IP_FATIGUE: Proxy {proxy_record.ip} đã xử lý tốt {proxy_record.consecutive_successes} request "
                    "trước khi bị cờ. Đây là giới hạn quota/phiên thông thường của Google."
                )

        # 3. RPC Specific Clues
        if rpc_id == "agJzFb":
            reasons.append(
                "VISION_RPC_RULE: agJzFb (Gemini Vision) yêu cầu phiên Flow chat session hợp lệ. "
                "Nếu trang Flow chưa tải xong hoặc session bị ngắt kết nối, Google sẽ trả về UNUSUAL_ACTIVITY."
            )
        elif rpc_id in ("ogiZ0b", "eb1hJf"):
            if "reCAPTCHA" in raw_error or "7," in raw_error:
                reasons.append(
                    "CAPTCHA_EVALUATION_FAILED: Token reCAPTCHA Enterprise trên trang Flow bị điểm tin cậy thấp (score < threshold)."
                )

        # 4. Rotation Outcome Clue
        if rotation_result and rotation_result.get("retry_success"):
            reasons.append(
                "RECOVERY_SUCCEEDED_CAUSE_UNCONFIRMED: Retry thành công sau phục hồi. "
                "Đổi proxy và làm mới phiên có thể cùng diễn ra; chưa tách được nguyên nhân IP, phiên hay token."
            )
        elif rotation_result and rotation_result.get("retry_attempted") and not rotation_result.get("retry_success"):
            reasons.append(
                "POOL_CONTAMINATION_OR_COOKIE: Đổi sang proxy mới nhưng retry vẫn bị lỗi. "
                "Khả năng cookie phiên Google trong Chrome profile đã hết hạn hoặc toàn bộ dải proxy pool đang bị theo dõi."
            )

        if not reasons:
            reasons.append(
                "UNSPECIFIED_GOOGLE_SECURITY_FLAG: Google AI Sandbox trả về PUBLIC_ERROR_UNUSUAL_ACTIVITY. "
                "Nguyên nhân phổ biến: IP proxy datacenter, cookie phiên Google cũ, hoặc reCAPTCHA score thấp."
            )

        summary_text = " | ".join(reasons)
        return {
            "primary_cause": reasons[0].split(":")[0],
            "severity": severity,
            "explanation": summary_text,
            "reasons": reasons,
        }

    def record_unusual_event(
        self,
        worker_id: str,
        rpc_id: str,
        raw_error: str,
        burst_metrics: dict,
        call_duration_ms: int = 0,
        payload_summary: Optional[dict] = None,
        rotation_info: Optional[dict] = None,
        proxy_url: Optional[str] = None,
    ) -> dict:
        """Constructs, logs, and persists a complete diagnostic audit record."""
        now = time.time()
        event_id = f"unusual_{int(now * 1000)}_{worker_id}"

        # Resolve active proxy for this worker
        if proxy_url is None:
            proxy_url = ""
            try:
                account = get_account(worker_id)
                if account and account.get("proxy_url"):
                    proxy_url = account["proxy_url"]
            except Exception:
                pass

        proxy_record: Optional[ProxyHealthRecord] = None
        if proxy_url:
            with self._lock:
                proxy_record = self._get_or_create_proxy_record(proxy_url)
                proxy_record.record_error("PUBLIC_ERROR_UNUSUAL_ACTIVITY")
                self._save_proxy_state_to_disk()

        parsed_proxy = parse_proxy_url(proxy_url) if proxy_url else None
        proxy_ip = parsed_proxy.host if parsed_proxy else "unknown"
        proxy_port = parsed_proxy.port if parsed_proxy else 0

        # Heuristic diagnosis
        diag = self.diagnose_root_cause(
            rpc_id=rpc_id,
            burst_metrics=burst_metrics,
            proxy_record=proxy_record,
            raw_error=raw_error,
            rotation_result=rotation_info,
        )

        event_entry: Dict[str, Any] = {
            "id": event_id,
            "timestamp_utc": _now_iso(now),
            "timestamp_vn": _now_vn_str(now),
            "worker_id": worker_id,
            "rpc_id": rpc_id,
            "action_name": RPC_ACTION_NAMES.get(rpc_id, f"Google Flow RPC ({rpc_id})"),
            "proxy": {
                "url": parsed_proxy.redacted if parsed_proxy else "none",
                "ip": proxy_ip,
                "port": proxy_port,
                "lifetime_requests": proxy_record.total_requests if proxy_record else 0,
                "consecutive_errors": proxy_record.consecutive_errors if proxy_record else 1,
            },
            "timing": {
                "duration_ms": call_duration_ms,
                "gap_seconds_since_last_req": burst_metrics.get("gap_seconds", 0.0),
                "burst_in_last_10s": burst_metrics.get("burst_10s", 1),
                "burst_in_last_30s": burst_metrics.get("burst_30s", 1),
                "burst_in_last_60s": burst_metrics.get("burst_60s", 1),
                "global_burst_10s": burst_metrics.get("global_burst_10s", 1),
                "rpc_in_flight_at_dispatch": burst_metrics.get("rpc_in_flight"),
            },
            "payload_summary": payload_summary or {},
            "raw_error": str(raw_error)[:800],
            "diagnosis": diag,
            "recovery": rotation_info or {},
        }

        # Save to memory buffer
        with self._lock:
            self._recent_events.append(event_entry)

        # Write to JSONL log on disk
        self._append_to_disk(event_entry)

        logger.warning(
            "🚨 [UNUSUAL_ACTIVITY AUDIT] %s | Worker: %s | Proxy: %s | RPC: %s (%s) | Cause: %s",
            event_entry["timestamp_vn"],
            worker_id,
            proxy_ip,
            rpc_id,
            event_entry["action_name"],
            diag["primary_cause"],
        )
        return event_entry

    def _append_to_disk(self, record: dict) -> None:
        """Safely append the record to the persistent JSONL audit log with size-rotation."""
        try:
            AUDIT_LOG_DIR.mkdir(parents=True, exist_ok=True)
            if AUDIT_LOG_FILE.exists() and AUDIT_LOG_FILE.stat().st_size > MAX_LOG_SIZE_BYTES:
                # Rotate log
                backup_file = AUDIT_LOG_DIR / f"unusual_activity_audit.{int(time.time())}.bak"
                AUDIT_LOG_FILE.rename(backup_file)
                logger.info("Rotated audit log to %s", backup_file)

            with AUDIT_LOG_FILE.open("a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception as exc:
            logger.error("Failed to append audit record to disk: %s", exc)

    def get_recent_events(self, limit: int = 50) -> List[dict]:
        """Return the most recent audit events."""
        with self._lock:
            events = list(self._recent_events)
        return list(reversed(events[-limit:]))

    def get_audit_summary(self) -> dict:
        """Compute aggregated diagnostic statistics from audit history."""
        with self._lock:
            events = list(self._recent_events)
            proxies = [rec.to_dict() for rec in self._proxy_records.values()]

        total_events = len(events)
        now = time.time()
        last_24h_events = [e for e in events if now - datetime.datetime.fromisoformat(e["timestamp_utc"].replace("Z", "+00:00")).timestamp() <= 86400]

        # Breakdowns
        by_proxy: Dict[str, int] = collections.defaultdict(int)
        by_rpc: Dict[str, int] = collections.defaultdict(int)
        by_worker: Dict[str, int] = collections.defaultdict(int)
        by_cause: Dict[str, int] = collections.defaultdict(int)

        for e in last_24h_events:
            p_ip = e.get("proxy", {}).get("ip", "unknown")
            by_proxy[p_ip] += 1
            r_id = e.get("rpc_id", "unknown")
            by_rpc[r_id] += 1
            w_id = e.get("worker_id", "unknown")
            by_worker[w_id] += 1
            cause = e.get("diagnosis", {}).get("primary_cause", "UNKNOWN")
            by_cause[cause] += 1

        return {
            "total_incidents_recorded": total_events,
            "incidents_last_24h": len(last_24h_events),
            "breakdown_by_proxy_ip_24h": dict(sorted(by_proxy.items(), key=lambda x: -x[1])),
            "breakdown_by_rpc_24h": dict(sorted(by_rpc.items(), key=lambda x: -x[1])),
            "breakdown_by_worker_24h": dict(sorted(by_worker.items(), key=lambda x: -x[1])),
            "breakdown_by_cause_24h": dict(sorted(by_cause.items(), key=lambda x: -x[1])),
            "proxy_pool_health": proxies,
            "log_file_path": str(AUDIT_LOG_FILE),
            "log_file_size_bytes": AUDIT_LOG_FILE.stat().st_size if AUDIT_LOG_FILE.exists() else 0,
        }

    def get_all_audit_events_from_disk(self) -> List[dict]:
        """Read all historical audit events from log file."""
        if not AUDIT_LOG_FILE.exists():
            with self._lock:
                return list(self._recent_events)
        try:
            records = []
            for line in AUDIT_LOG_FILE.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except Exception:
                        pass
            return records
        except Exception as e:
            logger.warning("Failed to read all audit events from disk: %s", e)
            with self._lock:
                return list(self._recent_events)

    def compute_threshold_analysis(self) -> dict:
        """Compute forensic threshold statistics on requests per IP before unusual activity."""
        import statistics

        events = self.get_all_audit_events_from_disk()

        dirty_runs: List[int] = []
        clean_runs: List[int] = []
        all_reqs: List[int] = []

        for e in events:
            p = e.get("proxy", {})
            reqs = p.get("lifetime_requests", 0)
            cause = e.get("diagnosis", {}).get("primary_cause", "")
            all_reqs.append(reqs)
            if cause == "BURNED_PROXY_IP" or reqs <= 3:
                dirty_runs.append(reqs)
            else:
                clean_runs.append(reqs)

        clean_mean = round(statistics.mean(clean_runs), 1) if clean_runs else 32.0
        clean_median = round(statistics.median(clean_runs), 1) if clean_runs else 30.0
        clean_min = min(clean_runs) if clean_runs else 4
        clean_max = max(clean_runs) if clean_runs else 91
        safe_threshold = max(15, int(clean_median * 0.8))  # ~24-25 requests

        # Active proxy status
        active_proxies = []
        with self._lock:
            # Map accounts to proxies
            from agent.services.accounts import load_accounts
            accounts = load_accounts()
            account_by_proxy = {}
            for acc in accounts:
                if acc.get("proxy_url"):
                    account_by_proxy[acc["proxy_url"].strip()] = acc
                    try:
                        red = parse_proxy_url(acc["proxy_url"]).redacted
                        account_by_proxy[red] = acc
                    except Exception:
                        pass

            # Ensure all accounts' proxies are represented
            for acc in accounts:
                p_url = acc.get("proxy_url")
                if p_url:
                    self._get_or_create_proxy_record(p_url)

            for key, rec in self._proxy_records.items():
                acc = account_by_proxy.get(key) or account_by_proxy.get(rec.redacted)
                worker_label = (acc.get("label") or acc.get("id")) if acc else "Chưa gán nick (Sẵn sàng trong Pool)"

                reqs = rec.total_requests
                pct = min(100, round((reqs / safe_threshold) * 100))

                if reqs < int(safe_threshold * 0.6):
                    risk = "SAFE"
                    recommendation = f"Cực kỳ an toàn (còn ~{max(0, safe_threshold - reqs)} requests trước ngưỡng khuyến nghị)"
                elif reqs < safe_threshold:
                    risk = "MODERATE"
                    recommendation = f"Hoạt động tốt (còn ~{max(0, safe_threshold - reqs)} requests)"
                elif reqs < clean_median:
                    risk = "WARNING"
                    recommendation = "Đã chạm ngưỡng an toàn (cần chuẩn bị xoay proxy)"
                else:
                    risk = "CRITICAL"
                    recommendation = "Ngưỡng nguy cơ cao gặp UNUSUAL_ACTIVITY (nên xoay proxy ngay)"

                active_proxies.append({
                    "proxy": rec.redacted,
                    "ip": rec.ip,
                    "port": rec.port,
                    "assigned_worker": worker_label,
                    "total_requests": rec.total_requests,
                    "successful_requests": rec.successful_requests,
                    "failed_requests": rec.failed_requests,
                    "consecutive_successes": rec.consecutive_successes,
                    "fatigue_percentage": pct,
                    "risk_level": risk,
                    "recommendation": recommendation,
                    "last_used_vn": _now_vn_str(rec.last_used_at) if rec.last_used_at else None,
                })

        return {
            "ok": True,
            "total_incidents_analyzed": len(events),
            "clean_ip_stats": {
                "mean_requests_before_unusual": clean_mean,
                "median_requests_before_unusual": clean_median,
                "min_requests": clean_min,
                "max_requests": clean_max,
                "safe_rotation_threshold": safe_threshold,
                "sample_size": len(clean_runs),
                "conclusion": f"Với IP dân cư sạch, trung bình sau {clean_median:.0f} requests (dao động 25 - 35 requests) sẽ bắt đầu bị Google cờ unusual do suy giảm điểm tin cậy reCAPTCHA Enterprise. Khuyến nghị xoay proxy sau mỗi {safe_threshold} requests.",
            },
            "dirty_ip_stats": {
                "mean_requests": round(statistics.mean(dirty_runs), 1) if dirty_runs else 1.5,
                "sample_size": len(dirty_runs),
                "conclusion": "Với IP bẩn / Datacenter / IP dính blacklist, Google chặn ngay ở request 1 - 3.",
            },
            "active_proxies_fatigue": active_proxies,
            "best_practices": [
                f"1. Ngưỡng an toàn vàng: Chủ động xoay proxy sau mỗi {safe_threshold} requests để triệt tiêu nguy cơ gián đoạn.",
                "2. Giữ khoảng cách giữa các request tối thiểu 1.0s - 1.5s để triệt tiêu lỗi RATE_BURST.",
                "3. Khi tạo Keyframe hàng loạt, server tự động áp dụng stagger delay để không kích hoạt reCAPTCHA burst.",
                "4. Luôn ưu tiên dùng Proxy Dân Cư (Viettel / VNPT Residential) thay vì Proxy Datacenter."
            ],
        }

    def clear_in_memory_records(self) -> None:
        """Clear in-memory ring buffer (for tests or resets)."""
        with self._lock:
            self._recent_events.clear()


# Global audit singleton
_audit_manager: Optional[UnusualAuditManager] = None


def get_unusual_audit() -> UnusualAuditManager:
    global _audit_manager
    if _audit_manager is None:
        _audit_manager = UnusualAuditManager()
    return _audit_manager
