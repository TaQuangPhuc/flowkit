"""Client Request Shield — Zero-Disruption In-Flight Request Tracker & Graceful Drain.

Protects ongoing client requests from being interrupted when code or configuration
is modified or when the server is being reloaded/restarted.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Dict, List, Optional
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

logger = logging.getLogger(__name__)

# Paths that do not block graceful drain (health checks, status probes, metrics)
_BYPASS_SHIELD_PATHS = {
    "/health",
    "/api/health",
    "/api/system/shield-status",
    "/api/accounts/proxy-health",
    "/favicon.ico",
}


class ClientRequestShield:
    """Tracks active client HTTP requests and manages graceful drain."""

    def __init__(self):
        self._lock = asyncio.Lock()
        self._active_requests: Dict[str, dict] = {}
        self._is_draining: bool = False
        self._idle_event = asyncio.Event()
        self._idle_event.set()

    @property
    def is_draining(self) -> bool:
        return self._is_draining

    @property
    def active_count(self) -> int:
        return len(self._active_requests)

    def set_draining(self, draining: bool = True) -> None:
        self._is_draining = draining
        if draining:
            logger.info("ClientRequestShield: Draining mode ACTIVE. New heavy requests will be paused/rejected gracefully.")
        else:
            logger.info("ClientRequestShield: Draining mode CANCELLED. Normal operation resumed.")

    async def acquire(self, req_id: str, path: str, method: str, client_ip: str) -> None:
        async with self._lock:
            self._active_requests[req_id] = {
                "id": req_id,
                "path": path,
                "method": method,
                "client_ip": client_ip,
                "start_time": time.time(),
            }
            self._idle_event.clear()

    async def release(self, req_id: str) -> None:
        async with self._lock:
            self._active_requests.pop(req_id, None)
            if not self._active_requests:
                self._idle_event.set()

    async def wait_until_idle(self, timeout: float = 45.0) -> bool:
        """Wait until all active client HTTP requests have completed."""
        if not self._active_requests:
            return True
        logger.info(
            "Waiting up to %.1fs for %d client requests to finish...",
            timeout,
            len(self._active_requests),
        )
        try:
            await asyncio.wait_for(self._idle_event.wait(), timeout=timeout)
            logger.info("All client HTTP requests drained successfully.")
            return True
        except asyncio.TimeoutError:
            logger.warning(
                "Timeout reached while draining requests. %d requests still active: %s",
                len(self._active_requests),
                [r["path"] for r in self._active_requests.values()],
            )
            return False

    def get_status(self) -> dict:
        now = time.time()
        requests_summary = []
        for r in list(self._active_requests.values()):
            requests_summary.append({
                "id": r["id"][:8],
                "path": r["path"],
                "method": r["method"],
                "client_ip": r["client_ip"],
                "elapsed_seconds": round(now - r["start_time"], 2),
            })

        # Check worker controller active count if available
        worker_count = 0
        try:
            from agent.worker.processor import get_worker_controller
            worker_count = get_worker_controller().active_count
        except Exception:
            pass

        return {
            "ok": True,
            "is_draining": self._is_draining,
            "active_http_requests": len(self._active_requests),
            "active_worker_tasks": worker_count,
            "safe_to_reload": (len(self._active_requests) == 0 and worker_count == 0),
            "requests": requests_summary,
        }


_GLOBAL_SHIELD: Optional[ClientRequestShield] = None


def get_request_shield() -> ClientRequestShield:
    global _GLOBAL_SHIELD
    if _GLOBAL_SHIELD is None:
        _GLOBAL_SHIELD = ClientRequestShield()
    return _GLOBAL_SHIELD


class RequestShieldMiddleware(BaseHTTPMiddleware):
    """FastAPI/Starlette middleware that shields in-flight client requests."""

    async def dispatch(self, request: Request, call_next) -> Response:
        shield = get_request_shield()
        path = request.url.path

        # Bypass shield for health checks, static files, and admin status
        if path in _BYPASS_SHIELD_PATHS or path.startswith("/static/") or path.startswith("/assets/"):
            return await call_next(request)

        # If server is draining and client attempts a mutating operation (POST/PUT/DELETE/PATCH),
        # return 503 with Retry-After so client re-polls smoothly instead of failing mid-execution
        if shield.is_draining and request.method in {"POST", "PUT", "DELETE", "PATCH"}:
            if not path.startswith("/api/system/"):
                return JSONResponse(
                    status_code=503,
                    headers={"Retry-After": "3"},
                    content={
                        "detail": "Server is performing a zero-disruption graceful reload. Please retry in 3 seconds.",
                        "draining": True,
                        "retry_after": 3,
                    },
                )

        req_id = uuid.uuid4().hex[:12]
        client_ip = request.client.host if request.client else "unknown"
        await shield.acquire(req_id, path, request.method, client_ip)
        try:
            return await call_next(request)
        finally:
            await shield.release(req_id)
