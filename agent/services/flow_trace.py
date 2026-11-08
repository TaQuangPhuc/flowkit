"""Bounded, payload-free diagnostics for HTTP, worker routing and extension RPCs."""
import asyncio
import contextvars
import hashlib
import json
import logging
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
import queue as _queue
import re
import threading
import time
import uuid
import sys
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path

trace_id = contextvars.ContextVar('flow_trace_id', default='')
job_id = contextvars.ContextVar('flow_job_id', default='')
# Per-request callable: () -> True once the client disconnected. The gRPC
# submit queue checks it while waiting so an abandoned request never
# creates a Google-side operation nobody will poll.
request_gone = contextvars.ContextVar('flow_request_gone', default=None)
_logger = logging.getLogger('flowkit.trace')
_logger.propagate = False
_logger.setLevel(logging.INFO)

# Durable error sink. The rolling trace file (~10MB x5) drops old events
# within minutes under load, so error-class events are ALSO appended to
# logs/errors/errors-YYYYMMDD.jsonl — one file per UTC day, never rotated
# away. Same redaction rules apply: events carry no prompts/tokens/URLs.
_DURABLE_ERR_EVENTS = {
    'grpc.rpc.error', 'grpc.facade.error', 'grpc.error',
    'grpc.poll.rejected', 'grpc.poll.stuck', 'http.exception',
    'grpc.media.missing', 'grpc.facade.rejected',
}
_err_day = ''
_err_fh = None


def _service() -> str:
    return 'studio' if any('auto_tvc_server' in a for a in sys.argv) else 'api'


def _is_durable_error(event: str, fields: dict) -> bool:
    if event in _DURABLE_ERR_EVENTS or event.endswith('.exception'):
        return True
    if event == 'http.end':
        st = fields.get('status')
        if isinstance(st, int) and st >= 400:
            return True
        res = fields.get('result')
        if isinstance(res, dict) and res.get('has_error'):
            return True
    return False


def _emit_error(line: str) -> None:
    global _err_day, _err_fh
    day = datetime.now(timezone.utc).strftime('%Y%m%d')
    if day != _err_day or _err_fh is None:
        if _err_fh is not None:
            try:
                _err_fh.close()
            except Exception:
                pass
        from agent.config import BASE_DIR
        d = Path(BASE_DIR) / 'logs' / 'errors'
        d.mkdir(parents=True, exist_ok=True)
        _err_fh = open(d / f'errors-{day}.jsonl', 'a',
                       encoding='utf-8', buffering=1)
        _err_day = day
    _err_fh.write(line + '\n')


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
        'invalid argument', 'video_render_timeout', 'entity was not found',
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
        ops = value.get('operations')
        if ops is None and isinstance(value.get('data'), dict):
            ops = value['data'].get('operations')
        if isinstance(ops, list) and ops:
            tallies = {}
            has_video = False
            for item in ops:
                if not isinstance(item, dict):
                    continue
                st = str(item.get('status') or '')
                if st:
                    short = st.rsplit('_', 1)[-1][:16]
                    tallies[short] = tallies.get(short, 0) + 1
                meta = ((item.get('operation') or {}).get('metadata') or {})
                vid = (meta.get('video') or {})
                if vid.get('fifeUrl') or (isinstance(vid.get('fifeUrl'), str) and '/video/' in vid.get('fifeUrl', '')):
                    has_video = True
                elif vid.get('mediaId') and vid.get('fifeUrl'):
                    has_video = True
                if vid.get('fifeUrl'):
                    has_video = True
            out['n_ops'] = len(ops)
            out['op_status'] = tallies
            out['has_video'] = has_video
            name0 = ((ops[0].get('operation') or {}).get('name') if isinstance(ops[0], dict) else '') or ''
            if name0:
                out['operation_id'] = identifier(name0)
        out['has_error'] = bool(value.get('error') or value.get('error_code'))
    return out


_log_queue = _queue.Queue(-1)
_log_listener = None
_log_lock = threading.Lock()


def _ensure_handler() -> None:
    # QueueHandler -> QueueListener writes the rotating file on its own
    # thread, so emit() never does file I/O on the asyncio event loop —
    # under CPU saturation that blocked the loop and flapped the gateway
    # health probe into "no healthy backend" 502s.
    global _log_listener
    if _logger.handlers:
        return
    with _log_lock:
        if _logger.handlers:
            return
        from agent.config import API_PORT, BASE_DIR
        # Both agents write the trace now — suffix by port so the two
        # RotatingFileHandlers never fight over one file's rotation.
        suffix = f'-{API_PORT}' if API_PORT != 8100 else ''
        path = Path(BASE_DIR) / '.scratch' / f'flow-trace-{_service()}{suffix}.jsonl'
        path.parent.mkdir(parents=True, exist_ok=True)
        target = RotatingFileHandler(path, maxBytes=10 * 1024 * 1024,
                                     backupCount=5, encoding='utf-8', delay=True)
        target.setFormatter(logging.Formatter('%(message)s'))
        _logger.addHandler(QueueHandler(_log_queue))
        _log_listener = QueueListener(_log_queue, target,
                                      respect_handler_level=True)
        _log_listener.daemon = True
        _log_listener.start()


def emit(event, **fields):
    # Observability must never fail or retry the actual work.
    try:
        _ensure_handler()
        obj = {'ts': datetime.now(timezone.utc).isoformat(), 'v': 1,
               'event': event, 'trace_id': trace_id.get(), **fields}
        if job_id.get() and 'job' not in obj:
            obj['job'] = job_id.get()
        _logger.info(json.dumps(obj, ensure_ascii=False))
        if _is_durable_error(event, fields):
            obj['svc'] = _service()
            try:
                _emit_error(json.dumps(obj, ensure_ascii=False))
            except Exception:
                pass
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
        path = scope.get('path', '')
        if scope['type'] != 'http' or not path.startswith('/api/') \
                or path.startswith('/api/admin'):
            return await self.app(scope, receive, send)
        headers = dict(scope.get('headers', []))
        incoming = headers.get(b'x-request-id', b'').decode('ascii', errors='ignore')
        rid = incoming if re.fullmatch(r'(?:nova_req_|fk_)[a-f0-9]{16,64}', incoming) else 'fk_' + uuid.uuid4().hex
        jid = headers.get(b'x-nova-job-id', b'').decode('ascii', errors='ignore')
        jid = jid if re.fullmatch(r'[A-Za-z0-9_.:\-]{4,80}', jid) else ''
        jtok = job_id.set(jid)
        token = trace_id.set(rid)
        gone = {'v': False}
        tapped_buf = []
        pending_recv = {'t': None}

        async def tapped_receive():
            if tapped_buf:
                m = tapped_buf.pop(0)
            elif pending_recv['t'] is not None:
                t, pending_recv['t'] = pending_recv['t'], None
                m = await t
            else:
                m = await receive()
            if m['type'] == 'http.disconnect':
                gone['v'] = True
            return m

        async def is_gone():
            # Peek at the ASGI receive channel without consuming: a client
            # disconnect only surfaces as an http.disconnect message, which
            # nobody reads while the handler is off doing gRPC work. One
            # persistent receive task — never cancelled, so nothing is lost;
            # non-disconnect messages are buffered so the handler still sees
            # them in order.
            if gone['v']:
                return True
            if pending_recv['t'] is None:
                pending_recv['t'] = asyncio.ensure_future(receive())
            await asyncio.sleep(0)
            t = pending_recv['t']
            if t is None or not t.done():
                return False
            pending_recv['t'] = None
            try:
                m = t.result()
            except Exception:
                # receive() blew up — the connection is dead either way.
                gone['v'] = True
                return True
            if m['type'] == 'http.disconnect':
                gone['v'] = True
                return True
            tapped_buf.append(m)
            return False
        gtok = request_gone.set(is_gone)
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
                extra = [(b'x-flowkit-trace-id', rid.encode())]
                if jid:
                    extra.append((b'x-nova-job-id', jid.encode()))
                message = {**message, 'headers': hs + extra}
            elif message['type'] == 'http.response.body':
                body = message.get('body', b'')
                size += len(body)
                if is_json and len(preview) < 16384:
                    preview.extend(body[:16384-len(preview)])
            await send(message)
        try:
            await self.app(scope, tapped_receive, traced_send)
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
            elapsed_ms = round((time.monotonic()-started)*1000)
            emit('http.end', status=status, elapsed_ms=elapsed_ms,
                 response_bytes=size, result=result)
            try:
                from agent.services.flow_debug_log import note_http
                note_http(scope.get('method', ''), path, status, elapsed_ms, bytes(preview))
            except Exception:
                pass
            trace_id.reset(token)
            job_id.reset(jtok)
            request_gone.reset(gtok)
            t = pending_recv['t']
            if t is not None and not t.done():
                t.cancel()
            elif t is not None and not t.cancelled():
                # swallow the result so "exception never retrieved" never
                # fires for a receive that died with the connection
                try:
                    t.result()
                except Exception:
                    pass
