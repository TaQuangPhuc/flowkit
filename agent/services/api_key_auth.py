"""API-key gate for remote /api/* calls.

The daemon listens on 0.0.0.0:8100 so the Nova gateway can reach it over
Tailscale — but that also exposes every generation endpoint to anyone on the
tailnet/LAN. With FLOWKIT_API_KEY set, remote callers must present the key via
``X-Api-Key``. Loopback callers (the TVC daemon, local scripts, the tray UI)
keep working without a key.
"""

import os
import secrets

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

_API_KEY = os.environ.get("FLOWKIT_API_KEY", "").strip()
_LOOPBACK = {"127.0.0.1", "::1", "localhost"}


class APIKeyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path
        if not path.startswith("/api/"):
            return await call_next(request)
        client = request.client.host if request.client else ""
        if client in _LOOPBACK:
            return await call_next(request)
        if not _API_KEY:
            # Fail closed: an unset key would leave the API public on the
            # tailnet. Loopback callers above are unaffected.
            return JSONResponse({"detail": "api key not configured"}, status_code=503)
        presented = request.headers.get("x-api-key", "")
        if presented and secrets.compare_digest(presented, _API_KEY):
            return await call_next(request)
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
