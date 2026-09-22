"""Central Watchdog Daemon & Self-Healing Engine — 24/7 proactive health monitor for FlowKit."""

import asyncio
import json
import logging
import sqlite3
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

from agent.config import (
    BASE_DIR,
    DB_PATH,
    FAILOVER_PURGE_TTL_D,
    FAILOVER_STUCK_TTL_H,
    INCIDENT_STALE_TTL_H,
    LOG_KEEP_MB,
    LOG_MAX_MB,
    REPLAY_COMPLETED_TTL_H,
    REPLAY_PENDING_TTL_H,
)
from agent.services.incident_manager import get_incident_manager

logger = logging.getLogger(__name__)

AUTO_RUNS_DIR = BASE_DIR / "auto_runs"
_watchdog_thread: Optional[threading.Thread] = None
_stop_event = threading.Event()
_SCAN_INTERVAL_S = 60


def _run_coro_blocking(coro, timeout: float = 90.0) -> Dict[str, Any]:
    """Run a coroutine from sync code, even under a running event loop.

    run_sweep() is called both from the daemon thread (no loop) and from the
    /sweep endpoint (loop running). asyncio.run() raises in the second case,
    so proxy rotation never actually ran on the manual path.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    box: Dict[str, Any] = {}

    def _runner():
        try:
            box["value"] = asyncio.run(coro)
        except Exception as exc:  # surfaced to the caller below
            box["error"] = exc

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    t.join(timeout=timeout)
    if "error" in box:
        raise box["error"]
    if "value" not in box:
        return {"ok": False, "error": "coroutine_timeout"}
    return box["value"]


class CentralWatchdog:
    """Centralized proactive scanner and self-healer across 5 FlowKit domains:

    1. Fashion Lookbook Studio (Stage 1 Images, Stage 2 Video Clips, Master Stitch)
    2. Auto TVC Studio (Script, Voice TTS, Lip-sync, Video, Concat)
    3. Core Batch Requests (request table processing queue)
    4. Chrome Workers (Multi-profile Chrome processes & extensions)
    5. Residential Proxy Pool & Forwarding Bridges
    """

    def __init__(self):
        self.incident_mgr = get_incident_manager()

    def run_sweep(self) -> Dict[str, Any]:
        """Execute one complete health check and self-healing pass across all 5 domains."""
        results = {
            "timestamp": time.time(),
            "lookbook": self._sweep_lookbook(),
            "tvc": self._sweep_tvc(),
            "batch": self._sweep_batch_requests(),
            "workers": self._sweep_workers(),
            "proxies": self._sweep_proxies(),
            "stale": self._sweep_stale_incidents(),
            "replays": self._sweep_replays(),
            "logs": self._sweep_logs(),
        }
        return results

    def _sweep_replays(self) -> Dict[str, int]:
        """Prune replay rows and close out abandoned failovers."""
        out: Dict[str, int] = {"expired": 0, "deleted": 0, "abandoned": 0, "purged": 0}
        try:
            from agent.services.flow_failover import prune_replays, reconcile_failovers
            out.update(prune_replays(
                completed_ttl_s=max(1, REPLAY_COMPLETED_TTL_H) * 3600,
                pending_ttl_s=max(1, REPLAY_PENDING_TTL_H) * 3600,
            ))
            out.update(reconcile_failovers(
                stuck_ttl_s=max(1, FAILOVER_STUCK_TTL_H) * 3600,
                purge_ttl_s=max(1, FAILOVER_PURGE_TTL_D) * 86400,
            ))
        except Exception as e:
            logger.warning("[WATCHDOG] Replay sweep error: %s", e)
        return out

    def _sweep_logs(self) -> Dict[str, int]:
        """Trim the append-only logs; nothing else rotates them."""
        try:
            from agent.services.log_maintenance import trim_logs
            targets = [
                BASE_DIR / "server.log",
                BASE_DIR / ".scratch" / "ext-netlog.jsonl",
                BASE_DIR / ".scratch" / "ext-network-errors.jsonl",
            ]
            results = trim_logs(targets, LOG_MAX_MB * 1024 * 1024, LOG_KEEP_MB * 1024 * 1024)
        except Exception as e:
            logger.warning("[WATCHDOG] Log sweep error: %s", e)
            return {"trimmed": 0}
        return {"trimmed": sum(1 for r in results if r.get("trimmed"))}

    def _sweep_stale_incidents(self) -> Dict[str, int]:
        """Close incidents that nothing will ever resolve."""
        try:
            closed = self.incident_mgr.close_stale(max(1, INCIDENT_STALE_TTL_H) * 3600)
        except Exception as e:
            logger.warning("[WATCHDOG] Stale incident sweep error: %s", e)
            return {"closed": 0}
        return {"closed": closed}

    # ─── 1. FASHION LOOKBOOK SWEEP & SELF-HEALING ────────────────────

    def _sweep_lookbook(self) -> Dict[str, int]:
        healed, checked, failed = 0, 0, 0
        if not AUTO_RUNS_DIR.exists():
            return {"checked": 0, "healed": 0, "failed": 0}

        now = time.time()
        for jdir in AUTO_RUNS_DIR.glob("lookbook_*"):
            if not jdir.is_dir():
                continue
            jfile = jdir / "job.json"
            if not jfile.exists():
                continue

            checked += 1
            jid = jdir.name.replace("lookbook_", "")
            try:
                job = json.loads(jfile.read_text(encoding="utf-8"))
            except Exception:
                continue

            st = job.get("status", "")
            if st in ["COMPLETED", "STAGE1_COMPLETED", "STAGE2_COMPLETED"]:
                # Ensure master video exists if all clips exist
                clips = job.get("video_clips", [])
                if clips and all(c.get("status") == "COMPLETED" for c in clips):
                    master = jdir / "final_lookbook.mp4"
                    if not master.exists() or master.stat().st_size < 50000:
                        try:
                            import fashion_lookbook_studio as fls
                            if fls.stitch_lookbook_final(jid):
                                healed += 1
                                self.incident_mgr.record_incident(
                                    module="lookbook",
                                    job_id=jid,
                                    severity="HEALED",
                                    error_code="MISSING_MASTER_STITCH",
                                    message=f"Tự động ghép Master Video cho Lookbook #{jid[:8]}",
                                    action_taken="AUTO_STITCH_MASTER",
                                    status="RESOLVED"
                                )
                        except Exception as e:
                            logger.error("[WATCHDOG] Lookbook stitch error %s: %s", jid, e)
                continue

            # Stage 2 Video Clips Healing
            video_clips = job.get("video_clips", [])
            job_dirty = False

            for clip in video_clips:
                cid = clip.get("clip_id")
                clip_path = jdir / f"clip_{cid}.mp4"
                c_status = clip.get("status", "")

                # 1. Clip is already on disk -> sync status
                if clip_path.exists() and clip_path.stat().st_size > 50000:
                    if c_status != "COMPLETED":
                        clip["status"] = "COMPLETED"
                        clip["status_text"] = "Đã hoàn thành video"
                        clip["video_url"] = f"/api/fashion-lookbook/stage2/clips/{jid}/{cid}"
                        job_dirty = True
                        healed += 1
                        self.incident_mgr.record_incident(
                            module="lookbook",
                            job_id=jid,
                            sub_id=f"clip_{cid}",
                            severity="HEALED",
                            error_code="STALE_STATUS_SYNC",
                            message=f"Đồng bộ trạng thái Clip {cid} đã tải về thành công cho Job #{jid[:8]}",
                            action_taken="SYNC_DISK_FILE",
                            status="RESOLVED"
                        )
                    continue

                # 2. Clip has an active operation on Google Flow -> Poll CDN
                op_name = clip.get("operation_name")
                if op_name and c_status in ["RENDERING_VIDEO", "PENDING"]:
                    try:
                        import fashion_lookbook_studio as fls
                        p_body = {"operations": [{"operation": {"name": op_name}}]}
                        p_res = fls.call_flowkit_api("/api/flow/check-status", p_body, timeout=20, max_retries=1)
                        ret_ops = p_res.get("operations") or (p_res.get("data") or {}).get("operations") or []
                        for op_item in ret_ops:
                            st_op = str(op_item.get("status") or (op_item.get("operation") or {}).get("metadata", {}).get("status") or "")
                            meta = (op_item.get("operation") or {}).get("metadata", {})
                            fife = meta.get("video", {}).get("fifeUrl")

                            if st_op == "MEDIA_GENERATION_STATUS_SUCCESSFUL" or fife:
                                urllib.request.urlretrieve(fife, str(clip_path))
                                if clip_path.exists() and clip_path.stat().st_size > 50000:
                                    clip["status"] = "COMPLETED"
                                    clip["status_text"] = "Đã hoàn thành video"
                                    clip["video_url"] = f"/api/fashion-lookbook/stage2/clips/{jid}/{cid}"
                                    job_dirty = True
                                    healed += 1
                                    self.incident_mgr.record_incident(
                                        module="lookbook",
                                        job_id=jid,
                                        sub_id=f"clip_{cid}",
                                        severity="HEALED",
                                        error_code="CDN_POLL_DOWNLOAD",
                                        message=f"Tự động tải video hoàn tất từ Google Flow CDN cho Clip {cid} (Job #{jid[:8]})",
                                        action_taken="FETCH_FIFE_URL",
                                        status="RESOLVED"
                                    )
                                break
                            elif "FAIL" in st_op.upper():
                                failed += 1
                                self.incident_mgr.record_incident(
                                    module="lookbook",
                                    job_id=jid,
                                    sub_id=f"clip_{cid}",
                                    severity="CRITICAL",
                                    error_code="VEO_OPERATION_FAILED",
                                    message=f"Google Flow báo lỗi render Clip {cid} (Job #{jid[:8]}): {st_op}",
                                    root_cause=f"Operation {op_name} returned failure status: {st_op}",
                                    status="FAILED"
                                )
                                break
                    except Exception as e:
                        logger.warning("[WATCHDOG] Lookbook op poll failed for %s: %s", op_name, e)

                # 3. Clip stalled for > 10m without op or progress
                created_at = clip.get("created_at") or job.get("updated_at") or job.get("created_at") or now
                if c_status in ["RENDERING_VIDEO", "PENDING"] and (now - created_at > 600):
                    failed += 1
                    self.incident_mgr.record_incident(
                        module="lookbook",
                        job_id=jid,
                        sub_id=f"clip_{cid}",
                        severity="WARNING",
                        error_code="STALLED_VIDEO_RENDER",
                        message=f"Clip {cid} của Job #{jid[:8]} đang kết xuất quá 10 phút chưa phản hồi",
                        root_cause=f"Clip created at {created_at}, current time {now}, delta {now - created_at:.0f}s",
                        status="OPEN"
                    )

            if job_dirty:
                try:
                    import fashion_lookbook_studio as fls
                    completed_clips = sum(1 for c in video_clips if c.get("status") == "COMPLETED")
                    if completed_clips == len(video_clips) and len(video_clips) > 0:
                        job["status"] = "STAGE2_COMPLETED"
                        job["progress_percent"] = 100
                        job["message"] = f"Đã hoàn thành toàn bộ {len(video_clips)} video thời trang!"
                        jfile.write_text(json.dumps(job, indent=2, ensure_ascii=False), encoding="utf-8")
                        fls.stitch_lookbook_final(jid)
                    else:
                        jfile.write_text(json.dumps(job, indent=2, ensure_ascii=False), encoding="utf-8")
                except Exception as e:
                    logger.error("[WATCHDOG] Failed to save updated lookbook job %s: %s", jid, e)

        return {"checked": checked, "healed": healed, "failed": failed}

    # ─── 2. AUTO TVC STUDIO SWEEP & SELF-HEALING ─────────────────────

    def _sweep_tvc(self) -> Dict[str, int]:
        healed, checked, failed = 0, 0, 0
        if not AUTO_RUNS_DIR.exists():
            return {"checked": 0, "healed": 0, "failed": 0}

        now = time.time()
        for jdir in AUTO_RUNS_DIR.iterdir():
            if not jdir.is_dir() or jdir.name.startswith("lookbook_") or jdir.name.startswith("batch_"):
                continue

            jfile = jdir / "job.json"
            if not jfile.exists():
                continue

            checked += 1
            jid = jdir.name
            try:
                job = json.loads(jfile.read_text(encoding="utf-8"))
            except Exception:
                continue

            st = job.get("status", "")
            if st in ["COMPLETED", "FAILED"]:
                continue

            # Check if all scenes exist and auto-stitch if master is missing
            scenes = job.get("scenes", [])
            valid_clips = list(jdir.glob("clip_*.mp4")) + list(jdir.glob("scene_*.mp4"))
            master_tvc = jdir / "final_tvc.mp4"

            if scenes and len(valid_clips) >= len(scenes) and not master_tvc.exists():
                try:
                    concat_txt = jdir / "scenes.txt"
                    with open(concat_txt, "w") as f:
                        for c in sorted(valid_clips, key=lambda x: str(x)):
                            f.write(f"file '{c.name}'\n")
                    import subprocess
                    cmd = [
                        "ffmpeg", "-y", "-f", "concat", "-safe", "0",
                        "-i", str(concat_txt), "-c", "copy", str(master_tvc)
                    ]
                    subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                    if master_tvc.exists() and master_tvc.stat().st_size > 50000:
                        job["status"] = "COMPLETED"
                        job["final_video_url"] = f"/job/{jid}/final"
                        jfile.write_text(json.dumps(job, indent=2, ensure_ascii=False), encoding="utf-8")
                        healed += 1
                        self.incident_mgr.record_incident(
                            module="tvc",
                            job_id=jid,
                            severity="HEALED",
                            error_code="MISSING_TVC_MASTER",
                            message=f"Tự động ghép Master Video TVC cho Job #{jid[:8]}",
                            action_taken="AUTO_STITCH_TVC",
                            status="RESOLVED"
                        )
                except Exception as e:
                    logger.warning("[WATCHDOG] TVC stitch auto-heal failed for %s: %s", jid, e)

            # Check for stalled TVC job (> 10 min)
            updated_at = job.get("updated_at") or job.get("created_at") or now
            if st in ["PROCESSING", "RENDERING"] and (now - updated_at > 600):
                failed += 1
                self.incident_mgr.record_incident(
                    module="tvc",
                    job_id=jid,
                    severity="WARNING",
                    error_code="STALLED_TVC_JOB",
                    message=f"Dự án TVC #{jid[:8]} đang xử lý quá 10 phút chưa cập nhật",
                    root_cause=f"Status is {st}, last updated {now - updated_at:.0f}s ago",
                    status="OPEN"
                )

        return {"checked": checked, "healed": healed, "failed": failed}

    # ─── 3. CORE BATCH REQUESTS SWEEP ────────────────────────────────

    def _sweep_batch_requests(self) -> Dict[str, int]:
        checked, healed, failed = 0, 0, 0
        now = time.time()
        stale_threshold = now - 600  # 10m

        conn = None
        try:
            conn = sqlite3.connect(str(DB_PATH), timeout=5.0)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT id, project_id, video_id, scene_id, type, request_id, retry_count, created_at
                FROM request
                WHERE status = 'PROCESSING'
                ORDER BY created_at ASC
                LIMIT 50
                """
            ).fetchall()
            checked = len(rows)

            for r in rows:
                req_id = r["id"]
                created_s = time.time()  # default
                try:
                    # parse sqlite datetime
                    created_str = r["created_at"]
                    # If string format: 2026-09-19T...
                    import datetime
                    dt = datetime.datetime.fromisoformat(created_str.replace("Z", "+00:00"))
                    created_s = dt.timestamp()
                except Exception:
                    pass

                if created_s < stale_threshold:
                    failed += 1
                    self.incident_mgr.record_incident(
                        module="batch",
                        job_id=r["video_id"],
                        sub_id=req_id,
                        severity="WARNING",
                        error_code="STALLED_BATCH_REQUEST",
                        message=f"Request {r['type']} (#{req_id[:8]}) đang kẹt ở trạng thái PROCESSING > 10m",
                        root_cause=f"External request_id: {r['request_id']}, retries: {r['retry_count']}",
                        status="OPEN"
                    )
        except Exception as e:
            logger.warning("[WATCHDOG] Batch request sweep error: %s", e)
        finally:
            if conn is not None:
                conn.close()

        return {"checked": checked, "healed": healed, "failed": failed}

    # ─── 4. CHROME WORKERS SWEEP ─────────────────────────────────────

    def _sweep_workers(self) -> Dict[str, int]:
        checked, healed, failed = 0, 0, 0
        try:
            from agent.services.accounts import load_accounts
            from agent.services.chrome_nicks import chrome_running, find_running_chrome_pid
            accounts = load_accounts()
            checked = len(accounts)

            for acc in accounts:
                if not acc.get("enabled", True):
                    continue
                nick_id = acc["id"]
                is_running = chrome_running(nick_id)
                pid = find_running_chrome_pid(nick_id)

                if not is_running and pid is None:
                    failed += 1
                    self.incident_mgr.record_incident(
                        module="worker",
                        sub_id=nick_id,
                        severity="WARNING",
                        error_code="CHROME_WORKER_OFFLINE",
                        message=f"Tài khoản Chrome Worker '{nick_id}' hiện đang tắt",
                        root_cause="Process ID not found and chrome_running returned false",
                        action_taken="MONITORING",
                        status="OPEN"
                    )
                else:
                    healed += self.incident_mgr.resolve_by_sub(
                        module="worker",
                        sub_id=nick_id,
                        error_code="CHROME_WORKER_OFFLINE",
                        action_taken="WORKER_ACTIVE",
                    )
        except Exception as e:
            logger.warning("[WATCHDOG] Worker sweep error: %s", e)

        return {"checked": checked, "healed": healed, "failed": failed}

    # ─── 5. RESIDENTIAL PROXIES SWEEP ────────────────────────────────

    def _sweep_proxies(self) -> Dict[str, int]:
        checked, healed, failed = 0, 0, 0
        try:
            from agent.services.unusual_audit import get_unusual_audit
            audit = get_unusual_audit()
            records = list(audit._proxy_records.values())
            checked = len(records)

            from agent.services.accounts import load_accounts
            from agent.services.proxy_pool import rotate_nick_proxy
            accounts = load_accounts()
            account_by_proxy = {
                acc.get("proxy_url"): acc["id"]
                for acc in accounts
                if acc.get("proxy_url")
            }

            from agent.services.proxy_checker import quarantine_proxy, run_revival_cycle

            for rec in records:
                if rec.consecutive_errors >= 3:
                    failed += 1
                    # Quarantine proxy with 15m cooldown (900s)
                    try:
                        quarantine_proxy(rec.proxy_url, reason=rec.last_error_reason or "PERSISTENT_ERRORS", cooldown_seconds=900)
                    except Exception as q_err:
                        logger.warning("Watchdog proxy quarantine error: %s", q_err)

                    nick_id = account_by_proxy.get(rec.proxy_url)
                    action_taken = "FLAGGED_FOR_ROTATION"
                    if nick_id:
                        try:
                            rotation = _run_coro_blocking(rotate_nick_proxy(nick_id))
                            if rotation.get("ok"):
                                healed += 1
                                action_taken = f"AUTO_ROTATED_PROXY_{nick_id}"
                            else:
                                logger.warning("Watchdog rotation deferred for %s: %s", nick_id, rotation.get("error"))
                        except Exception as rot_e:
                            logger.warning("Watchdog auto-rotate failed for %s: %s", nick_id, rot_e)

                    self.incident_mgr.record_incident(
                        module="proxy",
                        sub_id=rec.ip or rec.redacted,
                        severity="CRITICAL",
                        error_code="PROXY_CONSECUTIVE_ERRORS",
                        message=f"Proxy {rec.redacted} gặp {rec.consecutive_errors} lỗi liên tiếp ({rec.last_error_reason})",
                        root_cause=f"High failure rate: {rec.failed_requests}/{rec.total_requests} requests failed",
                        action_taken=action_taken,
                        status="RESOLVED" if "AUTO_ROTATED" in action_taken else "OPEN"
                    )
                elif rec.consecutive_successes >= 5:
                    healed += self.incident_mgr.resolve_by_sub(
                        module="proxy",
                        sub_id=rec.ip or rec.redacted,
                        error_code="PROXY_CONSECUTIVE_ERRORS",
                        action_taken="PROXY_HEALTHY_RECOVERED",
                    )

            # Closed-Loop Proxy Recycling: probe quarantined proxies every 5 minutes against Google Labs
            try:
                revival_res = run_revival_cycle(probe_interval=300)
                if revival_res.get("restored"):
                    healed += len(revival_res["restored"])
                    logger.info("[WATCHDOG] Recycled and restored %d proxies to pool", len(revival_res["restored"]))
            except Exception as rev_err:
                logger.warning("[WATCHDOG] Proxy revival cycle error: %s", rev_err)

        except Exception as e:
            logger.warning("[WATCHDOG] Proxy sweep error: %s", e)

        return {"checked": checked, "healed": healed, "failed": failed}


def _daemon_loop():
    logger.info("Central Watchdog Daemon started (sweep interval: %ds)", _SCAN_INTERVAL_S)
    watchdog = CentralWatchdog()
    while not _stop_event.is_set():
        try:
            res = watchdog.run_sweep()
            logger.debug("[WATCHDOG] Sweep completed: %s", res)
        except Exception as e:
            logger.error("[WATCHDOG] Sweep loop uncaught error: %s", e)

        # Sleep with stop event check
        _stop_event.wait(timeout=_SCAN_INTERVAL_S)


def start_central_watchdog():
    """Start Central Watchdog in a dedicated background daemon thread."""
    global _watchdog_thread
    if _watchdog_thread and _watchdog_thread.is_alive():
        return
    _stop_event.clear()
    _watchdog_thread = threading.Thread(target=_daemon_loop, name="CentralWatchdogDaemon", daemon=True)
    _watchdog_thread.start()
    logger.info("Central Watchdog Daemon spawned successfully.")


def stop_central_watchdog():
    """Stop the background watchdog daemon."""
    _stop_event.set()
    if _watchdog_thread:
        _watchdog_thread.join(timeout=5.0)
