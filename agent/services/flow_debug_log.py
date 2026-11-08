"""Short-lived request/response log for Nova and Google errors.

One JSON line per HTTP response or gRPC call. Secrets, image bytes, and
signed media URLs are not stored. Set FLOW_DEBUG_EXCHANGE=0 to turn it off.
File: .scratch/flow-exchange-api.jsonl (8 MB × 3).
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

_logger = logging.getLogger("flowkit.exchange")
_logger.propagate = False
_logger.setLevel(logging.INFO)

_ENABLED = os.environ.get("FLOW_DEBUG_EXCHANGE", "1") != "0"
_SECRET = re.compile(
    r"(ya29\.[A-Za-z0-9_\-]+|1//[A-Za-z0-9_\-]+|Bearer\s+\S+"
    r"|0cAF[A-Za-z0-9_\-]{20,})",
    re.I,
)
_BLOB = re.compile(r"[A-Za-z0-9+/=]{80,}")
_UUID = re.compile(
    rb"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_DROP = {
    "prompt", "authorization", "access_token", "refresh_token",
    "image", "bytes", "captcha", "token", "password", "secret",
}


def enabled() -> bool:
    return _ENABLED


def redact(text: str, limit: int = 400) -> str:
    text = _SECRET.sub("[redacted]", text or "")
    text = _BLOB.sub("[blob]", text)
    return text[:limit]


def slim(obj, depth: int = 0):
    """Keep status fields. Drop prompts, tokens, and signed URLs."""
    if depth > 5:
        return "…"
    if isinstance(obj, dict):
        out = {}
        for i, (k, v) in enumerate(obj.items()):
            if i >= 30:
                break
            lk = str(k).lower()
            if lk in _DROP or "token" in lk or "password" in lk or "secret" in lk:
                out[k] = "[redacted]"
                continue
            if lk in ("fifeurl", "fife_url", "url") and isinstance(v, str):
                if "/video/" in v:
                    out[k] = "video"
                elif "/image/" in v:
                    out[k] = "image"
                else:
                    out[k] = "url"
                continue
            out[k] = slim(v, depth + 1)
        return out
    if isinstance(obj, list):
        return [slim(x, depth + 1) for x in obj[:8]]
    if isinstance(obj, str):
        return redact(obj, 240)
    if isinstance(obj, (int, float, bool)) or obj is None:
        return obj
    return redact(str(obj), 120)


def _handler() -> logging.Handler:
    if _logger.handlers:
        return _logger.handlers[0]
    from agent.config import API_PORT, BASE_DIR
    # Per-agent file under the dual-serve gateway — two rotating writers
    # on one path would corrupt each other's rotation.
    suffix = f'-{API_PORT}' if API_PORT != 8100 else ''
    path = BASE_DIR / ".scratch" / f"flow-exchange-api{suffix}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        path, maxBytes=8 * 1024 * 1024, backupCount=3,
        encoding="utf-8", delay=True)
    handler.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(handler)
    return handler


def exchange(**fields) -> None:
    """Append one redacted pair. Never raises."""
    if not _ENABLED:
        return
    try:
        tid = fields.pop("trace_id", None)
        if not tid:
            from agent.services.flow_trace import trace_id
            tid = trace_id.get()
        _handler()
        _logger.info(json.dumps({
            "ts": datetime.now(timezone.utc).isoformat(),
            "trace_id": tid or "",
            **fields,
        }, ensure_ascii=False, default=str))
    except Exception:
        pass


def note_http(method: str, path: str, status, elapsed_ms: int, body: bytes) -> None:
    if not _ENABLED or not path.startswith(("/api/flow", "/api/vids")):
        return
    parsed = None
    if body:
        try:
            parsed = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            parsed = redact(body.decode("utf-8", "replace"), 400)
    exchange(
        kind="http",
        method=method,
        path=path,
        status=status,
        elapsed_ms=elapsed_ms,
        body=slim(parsed) if parsed is not None else None,
    )


def note_grpc(nick: str, rpc: str, elapsed_ms: int, resp: bytes | None = None,
              error: str = "", grpc_code: str = "") -> None:
    if not _ENABLED:
        return
    raw = resp or b""
    texts = []
    if error:
        texts.append(redact(error, 400))
    if raw:
        for match in re.finditer(rb"[\x20-\x7e]{8,180}", raw):
            piece = match.group().decode("ascii", "ignore")
            low = piece.lower()
            if any(s in low for s in (
                "error", "fail", "not found", "unsafe", "denied",
                "throttl", "internal", "invalid",
            )):
                texts.append(redact(piece, 200))
            if len(texts) >= 4:
                break
    has_video = b"/video/" in raw
    has_image = b"/image/" in raw
    if rpc == "BatchGetMedia" and b"googleusercontent.com" in raw:
        try:
            from agent.services.flow_grpc import _read_media_urls
            for rec in _read_media_urls(raw).values():
                if rec.get("video"):
                    has_video = True
                if rec.get("image"):
                    has_image = True
        except Exception:
            pass
    exchange(
        kind="grpc",
        nick=nick,
        rpc=rpc,
        elapsed_ms=elapsed_ms,
        grpc_code=grpc_code or None,
        resp_bytes=len(raw),
        has_video=has_video,
        has_image=has_image,
        uuids=[u.decode() for u in _UUID.findall(raw)[:6]],
        google_text=texts,
    )
