"""Bounded, payload-free diagnostics for HTTP, worker routing and extension RPCs."""
import contextvars
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import re
import time
import uuid
import sys
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path

trace_id = contextvars.ContextVar('flow_trace_id', default='')
_logger = logging.getLogger('flowkit.trace')
_logger.propagate = False
_logger.setLevel(logging.INFO)


def fingerprint(value):
    return hashlib.sha256(str(value).encode()).hexdigest()[:16]


def identifier(value):
    value = str(value or '')
    return value if re.fullmatch(r'[a-zA-Z0-9_/-]{1,128}', value) else ('hash:' + fingerprint(value) if value else '')


def summary(value):
    """Do not log arbitrary messages, payloads, URLs, prompts or auth material."""
    if isinstance(value, dict):
        text = ' '.join(str(value.get(k, ''))[:16384] for k in ('error', 'error_code', 'detail', 'status', 'data'))
    else:
        text = str(value)[:65536]
    lower = text.lower()
    markers = [x for x in (
        'flow_request_not_submitted', 'flow_busy_not_submitted', 'upstream_timeout',
        'submission_outcome_unknown', 'upstream_submission_unknown', 'low_priority_only',
        'ask_for_permission', 'unusual_activity', 'unsafe_generation', 'no_flow_tab',
        'no_flow_key', 'media_profile_mismatch', 'extension not connected',
        'rotation_in_progress', 'failed to fetch', 'timeout', 'media not found',
        'no urls for media', 'execution context was destroyed', 'target closed',
        'net::err_network_changed', 'net::err_aborted', 'net::err_proxy_connection_failed',
        'net::err_tunnel_connection_failed', 'net::err_connection_reset',
    ) if x in lower]
    out = {'payload_bytes': len(text.encode()), 'fingerprint': fingerprint(text), 'markers': markers}
    detail = getattr(value, 'detail', None)
    if isinstance(detail, list):
        out['rpc_codes'] = [x for x in detail if type(x) is int and 0 <= x <= 16]
        out['rpc_id'] = identifier(getattr(value, 'rpcid', ''))
    if isinstance(value, dict):
        for key in ('status', 'error_code', 'retryable', 'retry_after_s', 'done'):
            v = value.get(key)
            if type(v) in (bool, int, float):
                out[key] = v
            elif key == 'status' and v in ('MEDIA_GENERATION_STATUS_PENDING', 'MEDIA_GENERATION_STATUS_FAILED', 'MEDIA_GENERATION_STATUS_SUCCESSFUL'):
                out[key] = v
        op = value.get('operation')
        if isinstance(op, dict) and op.get('name'):
            out['operation_id'] = identifier(op['name'])
        out['has_error'] = bool(value.get('error') or value.get('error_code'))
    return out


def emit(event, **fields):
    # Observability must never fail or retry the actual work.
    try:
        if not _logger.handlers:
            from agent.config import BASE_DIR
            service = 'studio' if any('auto_tvc_server' in arg for arg in sys.argv) else 'api'
            path = Path(BASE_DIR) / '.scratch' / f'flow-trace-{service}.jsonl'
            path.parent.mkdir(parents=True, exist_ok=True)
            handler = RotatingFileHandler(path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding='utf-8', delay=True)
            handler.setFormatter(logging.Formatter('%(message)s'))
            _logger.addHandler(handler)
        _logger.info(json.dumps({'ts': datetime.now(timezone.utc).isoformat(), 'v': 1,
                                'event': event, 'trace_id': trace_id.get(), **fields}, ensure_ascii=False))
    except Exception:
        pass


def traced(stage):
    def decorate(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            token = trace_id.set(trace_id.get() or 'fk_' + uuid.uuid4().hex)
            started = time.monotonic()
            emit(stage + '.start')
            try:
                result = await fn(*args, **kwargs)
                emit(stage + '.end', elapsed_ms=round((time.monotonic()-started)*1000), result=summary(result))
                return result
            except BaseException as exc:
                emit(stage + '.exception', elapsed_ms=round((time.monotonic()-started)*1000),
                     exception_type=type(exc).__name__, result=summary(exc))
                raise
            finally:
                trace_id.reset(token)
        return wrapper
    return decorate


def traced_sync(stage):
    def decorate(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            token = trace_id.set(trace_id.get() or 'fk_' + uuid.uuid4().hex)
            started = time.monotonic()
            emit(stage + '.start', endpoint=identifier(args[0] if args else ''),
                 job_id=identifier(kwargs.get('job_id')))
            try:
                result = fn(*args, **kwargs)
                emit(stage + '.end', elapsed_ms=round((time.monotonic()-started)*1000), result=summary(result))
                return result
            except BaseException as exc:
                emit(stage + '.exception', elapsed_ms=round((time.monotonic()-started)*1000),
                     exception_type=type(exc).__name__, result=summary(exc))
                raise
            finally:
                trace_id.reset(token)
        return wrapper
    return decorate


class FlowTraceMiddleware:
    """Pure ASGI middleware: does not consume request bodies or buffer streams."""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http' or not scope.get('path', '').startswith('/api/'):
            return await self.app(scope, receive, send)
        headers = dict(scope.get('headers', []))
        incoming = headers.get(b'x-request-id', b'').decode('ascii', errors='ignore')
        rid = incoming if re.fullmatch(r'(?:nova_req_|fk_)[a-f0-9]{16,64}', incoming) else 'fk_' + uuid.uuid4().hex
        token = trace_id.set(rid)
        started = time.monotonic()
        status = None
        size = 0
        preview = bytearray()
        is_json = False
        emit('http.start', method=scope['method'], path=identifier(scope.get('path')))

        async def traced_send(message):
            nonlocal status, size, is_json
            if message['type'] == 'http.response.start':
                status = message['status']
                hs = list(message.get('headers', []))
                is_json = any(k.lower() == b'content-type' and b'application/json' in v for k, v in hs)
                hs = [(k,v) for k,v in hs if k.lower() != b'x-flowkit-trace-id']
                message = {**message, 'headers': hs + [(b'x-flowkit-trace-id', rid.encode())]}
            elif message['type'] == 'http.response.body':
                body = message.get('body', b'')
                size += len(body)
                if is_json and len(preview) < 16384:
                    preview.extend(body[:16384-len(preview)])
            await send(message)
        try:
            await self.app(scope, receive, traced_send)
        except BaseException as exc:
            emit('http.exception', exception_type=type(exc).__name__, result=summary(exc))
            raise
        finally:
            result = {}
            if preview:
                try:
                    result = summary(json.loads(preview))
                except (ValueError, UnicodeDecodeError):
                    result = {'truncated_or_non_json': True}
            emit('http.end', status=status, elapsed_ms=round((time.monotonic()-started)*1000),
                 response_bytes=size, result=result)
            trace_id.reset(token)
