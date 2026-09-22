"""Pre-flight Proxy Health Checker for Google and Google Flow endpoints.

Validates proxy connectivity, Google anti-bot status (detects /sorry/ captchas and HTTP 429),
and Google Labs availability BEFORE assigning the proxy to any Chrome worker.
"""
from __future__ import annotations

import json
import logging
import threading
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from agent.services.proxy_url import resolve_known_proxy
from agent.services.surfshark import is_surfshark_url, monitored_proxy_urls

logger = logging.getLogger(__name__)

# Default timeout per test probe (seconds)
PROBE_TIMEOUT = 5


def check_single_proxy(proxy_url: str, timeout: int = PROBE_TIMEOUT) -> dict:
    """Run 3-tier validation on a proxy against Google.
    
    Tier 1: Google Connectivity & TCP/TLS (generate_204)
    Tier 2: Google Bot / Captcha / Blacklist Check (search query, checks /sorry/ or 429)
    Tier 3: Google Labs accessibility (labs.google)
    """
    resolved_proxy = resolve_known_proxy(proxy_url)
    proxy_clean = resolved_proxy.strip()
    masked = proxy_clean.split("@")[-1] if "@" in proxy_clean else proxy_clean
    result = {
        "proxy_url": proxy_clean,
        "masked": masked,
        "alive": False,
        "google_clean": False,
        "labs_accessible": False,
        "recaptcha_clean": False,
        "latency_ms": 0,
        "egress_ip": None,
        "status": "UNKNOWN",
        "error": None,
        "timestamp": time.time(),
    }

    p_handler = urllib.request.ProxyHandler({"http": proxy_clean, "https": proxy_clean})
    opener = urllib.request.build_opener(p_handler)
    t0 = time.time()

    # Tier 1: Zero-content connectivity check
    try:
        req1 = urllib.request.Request(
            "https://www.google.com/generate_204",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        with opener.open(req1, timeout=timeout) as r1:
            if r1.getcode() == 204:
                result["alive"] = True
    except Exception as exc1:
        result["latency_ms"] = round((time.time() - t0) * 1000)
        result["status"] = "DEAD"
        result["error"] = f"Connectivity probe failed: {type(exc1).__name__}"
        return result

    # Fetch egress IP via ipify if alive
    try:
        req_ip = urllib.request.Request(
            "https://api.ipify.org",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        with opener.open(req_ip, timeout=min(timeout, 3)) as r_ip:
            if r_ip.getcode() == 200:
                result["egress_ip"] = r_ip.read().decode("utf-8", errors="ignore").strip()
    except Exception:
        pass

    # Tier 2: Bot / Captcha detection probe
    try:
        req2 = urllib.request.Request(
            "https://www.google.com/search?q=test",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            }
        )
        with opener.open(req2, timeout=timeout) as r2:
            final_url = r2.geturl()
            if "sorry" in final_url or r2.getcode() != 200:
                result["latency_ms"] = round((time.time() - t0) * 1000)
                result["status"] = "CAPTCHA_BLOCKED"
                result["error"] = "Google anti-bot active: redirected to /sorry/ challenge"
                return result
            result["google_clean"] = True
    except urllib.error.HTTPError as he2:
        result["latency_ms"] = round((time.time() - t0) * 1000)
        if he2.code == 429 or "sorry" in str(he2.geturl()):
            result["status"] = "CAPTCHA_BLOCKED"
            result["error"] = "Google HTTP 429 Too Many Requests / Captcha"
        else:
            result["status"] = f"HTTP_{he2.code}"
            result["error"] = f"HTTP error {he2.code} on Google Search"
        return result
    except Exception as exc2:
        result["latency_ms"] = round((time.time() - t0) * 1000)
        result["status"] = "DEAD"
        result["error"] = f"Google probe failed: {type(exc2).__name__}"
        return result

    # Tier 3: Google Labs domain check
    try:
        req3 = urllib.request.Request(
            "https://labs.google/",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        with opener.open(req3, timeout=timeout) as r3:
            if r3.getcode() == 200:
                result["labs_accessible"] = True
    except Exception:
        pass

    # Tier 4: Google reCAPTCHA Enterprise anchor probe (checks if IP is blocked/challenged by reCAPTCHA)
    result["recaptcha_clean"] = False
    try:
        site_key = "6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV"
        req4 = urllib.request.Request(
            f"https://www.google.com/recaptcha/enterprise/anchor?ar=1&k={site_key}&co=aHR0cHM6Ly9sYWJzLmdvb2dsZTo0NDM.&hl=en",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Referer": "https://labs.google/"
            }
        )
        with opener.open(req4, timeout=timeout) as r4:
            if r4.getcode() == 200:
                body4 = r4.read().decode("utf-8", errors="ignore")
                if "sorry" in r4.geturl() or "sorry" in body4.lower():
                    result["latency_ms"] = round((time.time() - t0) * 1000)
                    result["status"] = "RECAPTCHA_BLOCKED"
                    result["error"] = "Google reCAPTCHA Enterprise blocked: redirected to /sorry/ challenge"
                    return result
                result["recaptcha_clean"] = True
            else:
                result["latency_ms"] = round((time.time() - t0) * 1000)
                result["status"] = "RECAPTCHA_BLOCKED"
                result["error"] = f"reCAPTCHA Enterprise probe returned HTTP {r4.getcode()}"
                return result
    except urllib.error.HTTPError as he4:
        result["latency_ms"] = round((time.time() - t0) * 1000)
        result["status"] = "RECAPTCHA_BLOCKED"
        result["error"] = f"reCAPTCHA Enterprise HTTP error {he4.code}"
        return result
    except Exception as exc4:
        result["latency_ms"] = round((time.time() - t0) * 1000)
        result["status"] = "RECAPTCHA_FAILED"
        result["error"] = f"reCAPTCHA Enterprise probe failed: {type(exc4).__name__}"
        return result

    result["latency_ms"] = round((time.time() - t0) * 1000)
    result["status"] = "CLEAN" if (result["google_clean"] and result.get("recaptcha_clean")) else "WARNING"
    return result


def check_proxy_list(proxies: list[str], max_workers: int = 10) -> list[dict]:
    """Concurrently check an entire list of proxy URLs."""
    if not proxies:
        return []
    with ThreadPoolExecutor(max_workers=min(len(proxies), max_workers)) as executor:
        return list(executor.map(check_single_proxy, proxies))


def find_first_healthy_proxy(candidates: list[str], timeout: int = 4) -> Optional[str]:
    """Test candidate proxies one by one and return the first verified CLEAN proxy."""
    for p in candidates:
        res = check_single_proxy(p, timeout=timeout)
        if res.get("status") == "CLEAN":
            logger.info("Pre-flight check passed for proxy: %s (%d ms)", res["masked"], res["latency_ms"])
            return p
        else:
            logger.warning("Pre-flight check rejected proxy %s: %s (%s)", res["masked"], res["status"], res["error"])
    return None

# ─── Periodic Auto-Revival & Quarantine Lifecycle Manager ──────────────────────


QUARANTINE_FILE = Path(__file__).resolve().parent.parent / "proxy_quarantine.json"

# In-memory lifecycle store: proxy_url -> { "status": "QUARANTINED", "quarantined_at": ts, "cooldown_s": 900, "reason": ... }
_PROXY_LIFECYCLE: dict[str, dict] = {}
_LIFECYCLE_LOCK = threading.Lock()
_DAEMON_THREAD: Optional[threading.Thread] = None
_last_quarantine_mtime: float = 0.0
_cached_quarantine_disk: dict = {}


def load_quarantine_state() -> dict:
    global _last_quarantine_mtime, _cached_quarantine_disk
    if not QUARANTINE_FILE.exists():
        return {}
    try:
        mtime = QUARANTINE_FILE.stat().st_mtime
        if mtime == _last_quarantine_mtime and _cached_quarantine_disk:
            return _cached_quarantine_disk
        data = json.loads(QUARANTINE_FILE.read_text(encoding="utf-8"))
        _last_quarantine_mtime = mtime
        _cached_quarantine_disk = data
        return data
    except Exception:
        return _cached_quarantine_disk or {}


def save_quarantine_state(state: dict) -> None:
    global _last_quarantine_mtime, _cached_quarantine_disk
    try:
        QUARANTINE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
        _last_quarantine_mtime = QUARANTINE_FILE.stat().st_mtime
        _cached_quarantine_disk = dict(state)
    except Exception as e:
        logger.warning("Failed to save quarantine state: %s", e)


def quarantine_proxy(proxy_url: str, reason: str = "PUBLIC_ERROR_UNUSUAL_ACTIVITY", cooldown_seconds: int = 900) -> None:
    """Put a proxy into quarantine cooldown for cooldown_seconds (default 15 mins)."""
    clean_p = proxy_url.strip()
    now = time.time()
    with _LIFECYCLE_LOCK:
        _PROXY_LIFECYCLE.update(load_quarantine_state())
        _PROXY_LIFECYCLE[clean_p] = {
            "status": "QUARANTINED",
            "quarantined_at": now,
            "cooldown_s": cooldown_seconds,
            "release_after": now + cooldown_seconds,
            "reason": reason,
            "recheck_attempts": 0,
        }
        save_quarantine_state(_PROXY_LIFECYCLE)
    masked = clean_p.split("@")[-1] if "@" in clean_p else clean_p
    logger.warning("🚨 [QUARANTINE] Proxy %s entered quarantine for %d minutes (Reason: %s)", masked, cooldown_seconds // 60, reason)


def is_quarantined(proxy_url: str) -> bool:
    clean_p = proxy_url.strip()
    now = time.time()
    with _LIFECYCLE_LOCK:
        disk_state = load_quarantine_state()
        if disk_state:
            _PROXY_LIFECYCLE.update(disk_state)
        item = _PROXY_LIFECYCLE.get(clean_p)
        if not item:
            return False
        if item.get("status") == "QUARANTINED":
            if now < item.get("release_after", 0):
                return True
            return False
    return False


def get_lifecycle_summary() -> dict:
    now = time.time()
    with _LIFECYCLE_LOCK:
        disk_state = load_quarantine_state()
        if disk_state:
            _PROXY_LIFECYCLE.update(disk_state)
        quarantined = []
        for p, data in _PROXY_LIFECYCLE.items():
            if data.get("status") == "QUARANTINED":
                rem = max(0, round(data.get("release_after", now) - now))
                masked = p.split("@")[-1] if "@" in p else p
                quarantined.append({
                    "proxy_url": p,
                    "masked": masked,
                    "reason": data.get("reason"),
                    "seconds_remaining": rem,
                    "minutes_remaining": round(rem / 60, 1),
                    "recheck_attempts": data.get("recheck_attempts", 0),
                })
        return {
            "quarantined_count": len(quarantined),
            "quarantined_proxies": quarantined,
        }


def run_revival_cycle(probe_interval: int = 300) -> dict:
    """Check quarantined proxies every probe_interval seconds (default 5m) against Google Labs and restore clean ones."""
    now = time.time()
    restored = []
    still_blocked = []
    from agent.services.accounts import load_accounts
    active = {a.get("proxy_url") for a in load_accounts() if a.get("proxy_url")}
    from agent.services.proxy_pool import load_proxy_pool
    pool_proxies = set(load_proxy_pool().get("proxies") or [])
    relevant = active | pool_proxies

    with _LIFECYCLE_LOCK:
        disk_state = load_quarantine_state()
        if disk_state:
            _PROXY_LIFECYCLE.update(disk_state)
        # Purge dead/stale proxies that are no longer configured anywhere
        for stale in list(_PROXY_LIFECYCLE.keys()):
            if stale not in relevant:
                _PROXY_LIFECYCLE.pop(stale, None)
        save_quarantine_state(_PROXY_LIFECYCLE)

        targets = []
        for p, data in _PROXY_LIFECYCLE.items():
            if data.get("status") == "QUARANTINED":
                last_probe = data.get("last_probe_at", data.get("quarantined_at", 0))
                # Probe every 5m or when cooldown expired
                if (now - last_probe >= probe_interval) or (now >= data.get("release_after", 0)):
                    targets.append((p, dict(data)))

    for p, meta in targets:
        masked = p.split("@")[-1] if "@" in p else p
        logger.info("⏳ [REVIVAL PROBE] Probing quarantined proxy %s against Google Labs...", masked)
        probe = check_single_proxy(p, timeout=5)
        with _LIFECYCLE_LOCK:
            _PROXY_LIFECYCLE[p]["last_probe_at"] = now
            if probe.get("status") == "CLEAN":
                # Google lifted the block! Restore to healthy and return to pool with 0 extra proxy cost
                _PROXY_LIFECYCLE[p]["status"] = "RESTORED"
                _PROXY_LIFECYCLE[p]["restored_at"] = now
                restored.append({"masked": masked, "latency_ms": probe.get("latency_ms")})
                logger.info("🎉 [REVIVAL SUCCESS] Google has unblocked proxy %s! Restoring to pool.", masked)

                # Reset failure counts
                _CONSECUTIVE_FAILURES[p] = 0

                # Return to proxy pool
                try:
                    from agent.services.proxy_pool import add_proxies_to_pool
                    add_proxies_to_pool([p])
                except Exception as pool_err:
                    logger.warning("Could not re-add restored proxy %s to pool: %s", masked, pool_err)

                # Log incident resolution
                try:
                    from agent.services.incident_manager import get_incident_manager
                    get_incident_manager().record_incident(
                        module="proxy",
                        sub_id=masked,
                        severity="HEALED",
                        error_code="PROXY_LEAKY_BUCKET_CLEARED",
                        message=f"Proxy {masked} restored after Google cooldown. Returned to pool with 0 extra proxy cost.",
                        action_taken="RESTORED_TO_POOL",
                        status="RESOLVED",
                    )
                except Exception:
                    pass
            else:
                # Still blocked, extend cooldown by 10 minutes if passed release_after
                if now >= meta.get("release_after", 0):
                    _PROXY_LIFECYCLE[p]["release_after"] = now + 600
                _PROXY_LIFECYCLE[p]["recheck_attempts"] = _PROXY_LIFECYCLE[p].get("recheck_attempts", 0) + 1
                still_blocked.append({"masked": masked, "status": probe.get("status"), "error": probe.get("error")})
                logger.warning("❌ [STILL BLOCKED] Proxy %s is still blocked by Google: %s. Recheck attempts: %d.", masked, probe.get("status"), _PROXY_LIFECYCLE[p]["recheck_attempts"])

        save_quarantine_state(_PROXY_LIFECYCLE)

    return {"restored": restored, "still_blocked": still_blocked}


def check_temporary_proxies_expiration() -> bool:
    """Check if 24-hour temporary proxy test has expired, and revert if so."""
    try:
        from scripts.revert_temp_proxies import revert
        return revert(force=False)
    except Exception as exc:
        logger.warning("Failed to check temporary proxy expiration: %s", exc)
        return False


# ─── Global Health Registry & Monitoring Task ────────────────────────────────

_PROXY_HEALTH_REGISTRY: dict[str, dict] = {}
_HEALTH_LOCK = threading.RLock()
_LAST_CHECK_TIME: float = 0.0
_IS_CHECKING: bool = False
_MONITOR_INTERVAL_S: int = 180
_CONSECUTIVE_FAILURES: dict[str, int] = {}


def check_all_proxies_health(timeout: int = 5, reveal: bool = False) -> dict:
    """Run concurrent health checks on all proxies from proxy_pool.json and accounts.json."""
    global _LAST_CHECK_TIME, _IS_CHECKING
    from agent.services.accounts import load_accounts
    from agent.services.proxy_pool import load_proxy_pool

    with _HEALTH_LOCK:
        if _IS_CHECKING:
            return get_proxy_health_report(reveal=reveal)
        _IS_CHECKING = True

    try:
        pool = load_proxy_pool()
        pool_proxies = pool.get("proxies") or []
        accounts = load_accounts()

        all_proxies = monitored_proxy_urls(pool_proxies, accounts)

        if not all_proxies:
            with _HEALTH_LOCK:
                _LAST_CHECK_TIME = time.time()
                _IS_CHECKING = False
            return get_proxy_health_report(reveal=reveal)

        # Map assigned accounts: proxy_url -> list of account labels/ids
        assigned_map: dict[str, list[str]] = {}
        for acc in accounts:
            p = (acc.get("proxy_url") or "").strip()
            if p:
                assigned_map.setdefault(p, []).append(acc.get("label") or acc.get("id"))

        # Check proxies concurrently
        max_workers = min(10, max(1, len(all_proxies)))
        results: list[dict] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_proxy = {executor.submit(check_single_proxy, p, timeout): p for p in all_proxies}
            for future in as_completed(future_to_proxy):
                p = future_to_proxy[future]
                try:
                    res = future.result()
                except Exception as exc:
                    res = {
                        "proxy_url": p,
                        "masked": p.split("@")[-1] if "@" in p else p,
                        "alive": False,
                        "status": "DEAD",
                        "error": str(exc),
                        "timestamp": time.time(),
                        "latency_ms": 0,
                    }
                results.append(res)

        now = time.time()
        with _HEALTH_LOCK:
            # Purge removed proxies
            for stale_p in list(_PROXY_HEALTH_REGISTRY.keys()):
                if stale_p not in all_proxies:
                    _PROXY_HEALTH_REGISTRY.pop(stale_p, None)
                    _CONSECUTIVE_FAILURES.pop(stale_p, None)

            for res in results:
                p = res["proxy_url"]
                status = res.get("status", "UNKNOWN")
                if status == "CLEAN":
                    _CONSECUTIVE_FAILURES[p] = 0
                else:
                    _CONSECUTIVE_FAILURES[p] = _CONSECUTIVE_FAILURES.get(p, 0) + 1

                # Auto-quarantine on captcha blocks or 3 consecutive failures
                quarantined = is_quarantined(p)
                if not quarantined and (
                    status in ("CAPTCHA_BLOCKED", "RECAPTCHA_BLOCKED")
                    or _CONSECUTIVE_FAILURES.get(p, 0) >= 3
                ):
                    reason = f"Health monitor auto-quarantine: {status} (failures: {_CONSECUTIVE_FAILURES.get(p, 0)})"
                    quarantine_proxy(p, reason=reason)
                    quarantined = True

                record = {
                    **res,
                    "in_pool": p in pool_proxies,
                    "assigned_accounts": assigned_map.get(p, []),
                    "quarantined": quarantined,
                    "consecutive_failures": _CONSECUTIVE_FAILURES.get(p, 0),
                    "last_checked": now,
                }
                _PROXY_HEALTH_REGISTRY[p] = record

            _LAST_CHECK_TIME = now

        # Run temporary proxy expiration and quarantine revival
        check_temporary_proxies_expiration()
        run_revival_cycle()

    except Exception as exc:
        logger.error("Error in check_all_proxies_health: %s", exc)
    finally:
        with _HEALTH_LOCK:
            _IS_CHECKING = False

    return get_proxy_health_report(reveal=reveal)


def get_proxy_health_report(reveal: bool = False) -> dict:
    """Return consolidated health metrics and list of all known proxies."""
    from agent.services.proxy_url import parse_proxy_url
    now = time.time()
    with _HEALTH_LOCK:
        records = list(_PROXY_HEALTH_REGISTRY.values())
        last_check = _LAST_CHECK_TIME
        is_checking = _IS_CHECKING
        interval = _MONITOR_INTERVAL_S

    # If registry is empty, populate placeholder rows from pool and accounts
    if not records:
        from agent.services.accounts import load_accounts
        from agent.services.proxy_pool import load_proxy_pool
        pool_proxies = load_proxy_pool().get("proxies") or []
        accounts = load_accounts()
        known = monitored_proxy_urls(pool_proxies, accounts)
        for p in known:
            clean = p.strip()
            if not clean:
                continue
            masked = clean.split("@")[-1] if "@" in clean else clean
            records.append({
                "proxy_url": clean,
                "masked": masked,
                "alive": False,
                "google_clean": False,
                "labs_accessible": False,
                "recaptcha_clean": False,
                "latency_ms": 0,
                "egress_ip": None,
                "status": "PENDING",
                "error": "Waiting for initial health check",
                "timestamp": now,
                "last_checked": 0,
                "in_pool": clean in pool_proxies,
                "assigned_accounts": [
                    acc.get("label") or acc["id"]
                    for acc in accounts
                    if (acc.get("proxy_url") or "").strip() == clean
                ],
                "quarantined": is_quarantined(clean),
                "consecutive_failures": 0,
            })

    total = len(records)
    healthy = sum(1 for r in records if r.get("status") == "CLEAN")
    warning = sum(1 for r in records if r.get("status") == "WARNING")
    blocked = sum(1 for r in records if r.get("status") in ("CAPTCHA_BLOCKED", "RECAPTCHA_BLOCKED"))
    dead = sum(
        1 for r in records
        if r.get("status") not in ("CLEAN", "WARNING", "CAPTCHA_BLOCKED", "RECAPTCHA_BLOCKED", "PENDING")
    )
    quarantined = sum(1 for r in records if r.get("quarantined"))

    rendered = []
    for r in records:
        item = dict(r)
        raw_url = item.get("proxy_url", "")
        if not reveal:
            try:
                item["proxy_url"] = parse_proxy_url(raw_url).redacted
            except Exception:
                item["proxy_url"] = item.get("masked", raw_url)
        rendered.append(item)

    status_priority = {
        "DEAD": 1,
        "RECAPTCHA_FAILED": 1,
        "CAPTCHA_BLOCKED": 2,
        "RECAPTCHA_BLOCKED": 2,
        "WARNING": 3,
        "PENDING": 4,
        "CLEAN": 5,
    }
    rendered.sort(key=lambda x: (status_priority.get(x.get("status"), 1), -x.get("latency_ms", 0)))

    return {
        "ok": True,
        "summary": {
            "total": total,
            "healthy": healthy,
            "warning": warning,
            "dead": dead,
            "blocked": blocked,
            "quarantined": quarantined,
            "checking": is_checking,
            "last_check_time": last_check,
            "interval_seconds": interval,
            "daemon_running": _DAEMON_THREAD is not None and _DAEMON_THREAD.is_alive(),
        },
        "proxies": rendered,
    }


def set_monitor_interval(seconds: int) -> None:
    global _MONITOR_INTERVAL_S
    _MONITOR_INTERVAL_S = max(30, seconds)


_DAEMON_STOP_EVENT = threading.Event()


def _daemon_loop(interval_seconds: int = 180):
    global _MONITOR_INTERVAL_S
    _MONITOR_INTERVAL_S = interval_seconds
    logger.info("Proxy Health Monitoring Daemon started (interval: %ds)", interval_seconds)
    # Run initial health check shortly after daemon start
    try:
        check_all_proxies_health()
    except Exception as exc:
        logger.error("Error during initial proxy health check: %s", exc)

    while not _DAEMON_STOP_EVENT.is_set():
        if _DAEMON_STOP_EVENT.wait(timeout=_MONITOR_INTERVAL_S):
            break
        try:
            check_all_proxies_health()
        except Exception as e:
            logger.error("Daemon error in proxy health loop: %s", e)


def start_proxy_health_daemon(interval_seconds: int = 180) -> None:
    global _DAEMON_THREAD
    if _DAEMON_THREAD and _DAEMON_THREAD.is_alive():
        return
    _DAEMON_STOP_EVENT.clear()
    _DAEMON_THREAD = threading.Thread(target=_daemon_loop, args=(interval_seconds,), daemon=True)
    _DAEMON_THREAD.start()


def stop_proxy_health_daemon() -> None:
    global _DAEMON_THREAD
    _DAEMON_STOP_EVENT.set()
    if _DAEMON_THREAD and _DAEMON_THREAD.is_alive():
        _DAEMON_THREAD.join(timeout=2)
    _DAEMON_THREAD = None

