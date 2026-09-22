import asyncio
import json
import pytest
from agent.services import flow_trace as ft


def test_summary_never_leaks_payload():
    secret = 'Bearer test-secret user@example.com https://host/?token=secret private prompt'
    out = json.dumps(ft.summary({'error': 'FLOW_BUSY_NOT_SUBMITTED ' + secret, 'data': secret, 'retryable': True}))
    assert secret not in out and 'test-secret' not in out and 'user@example' not in out
    assert 'flow_busy_not_submitted' in out and 'retryable' in out


@pytest.mark.asyncio
async def test_middleware_streams_unchanged_and_correlates(monkeypatch):
    logs = []
    monkeypatch.setattr(ft, 'emit', lambda event, **kw: logs.append((event, ft.trace_id.get(), kw)))
    bodies = [b'{"error":"FLOW_BUSY_', b'NOT_SUBMITTED", "prompt":"secret"}']
    async def app(scope, receive, send):
        assert ft.trace_id.get() == 'nova_req_' + 'a'*20
        await send({'type': 'http.response.start', 'status': 503, 'headers': [(b'content-type', b'application/json')]})
        for i, body in enumerate(bodies):
            await send({'type': 'http.response.body', 'body': body, 'more_body': i == 0})
    sent = []
    async def send(message): sent.append(message)
    await ft.FlowTraceMiddleware(app)({'type':'http', 'path':'/api/flow/generate-video', 'method':'POST', 'headers':[(b'x-request-id', b'nova_req_'+b'a'*20)]}, None, send)
    assert [x['body'] for x in sent[1:]] == bodies
    assert logs[-1][2]['status'] == 503
    assert 'secret' not in json.dumps(logs)
    assert ft.trace_id.get() == ''


@pytest.mark.asyncio
async def test_concurrent_requests_keep_separate_context(monkeypatch):
    monkeypatch.setattr(ft, 'emit', lambda *a, **kw: None)
    seen = []
    async def app(scope, receive, send):
        before = ft.trace_id.get()
        await asyncio.sleep(0)
        seen.append((before, ft.trace_id.get()))
    mw = ft.FlowTraceMiddleware(app)
    await asyncio.gather(*(mw({'type':'http', 'path':'/api/test', 'method':'GET', 'headers':[(b'x-request-id', b'nova_req_'+c*20)]}, None, None) for c in [b'a',b'b']))
    assert all(a == b for a,b in seen) and seen[0][0] != seen[1][0]


@pytest.mark.asyncio
async def test_trace_cancel_preserves_cancellation(monkeypatch):
    events=[]
    monkeypatch.setattr(ft, 'emit', lambda event, **kw: events.append(event))
    @ft.traced('test')
    async def fn(): raise asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError): await fn()
    assert events == ['test.start', 'test.exception']
    assert ft.trace_id.get() == ''


def test_logging_disk_failure_does_not_break_work(monkeypatch):
    def fail(*a, **kw): raise OSError('full')
    monkeypatch.setattr(ft._logger, 'info', fail)
    ft.emit('test')
