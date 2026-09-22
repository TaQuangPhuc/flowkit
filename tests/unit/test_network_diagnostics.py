import json
from agent.services import network_diagnostics as diag


def test_failure_metadata_strips_credentials_query_body_and_unknown_fields():
    result = diag.sanitize_failure({
        'url': 'https://user:password@flow.google.com/_/AiSandboxAngularFrontend/data/batchexecute?at=secret#fragment',
        'requestId': '12.3', 'rpcid': 'ogiZ0b', 'profileId': 'nick-b',
        'networkError': 'net::ERR_CONNECTION_RESET', 'elapsedMs': 123,
        'freq': 'private prompt', 'headers': {'Authorization': 'secret'},
    })
    assert result['profileId'] == 'nick-b'
    assert result['networkError'] == 'net::ERR_CONNECTION_RESET'
    assert result['requestId'] == '12.3' and result['elapsedMs'] == 123
    assert result['url'] == 'https://flow.google.com/_/AiSandboxAngularFrontend/data/batchexecute'
    assert all(word not in json.dumps(result) for word in ['password', 'secret', 'fragment', 'private', 'Authorization'])


def test_failure_rejects_unrelated_origin_and_filters_bad_values():
    assert diag.sanitize_failure({'url': 'https://other.example/'}) is None
    r = diag.sanitize_failure({'url': 'https://labs.google/_/test', 'networkError': 'secret',
                               'requestId': '\nsecret', 'elapsedMs': float('nan')})
    assert r['networkError'] == 'UNKNOWN_NETWORK_ERROR'
    assert r['requestId'] is None and r['elapsedMs'] is None


def test_bounded_file_rotation_and_tail(tmp_path, monkeypatch):
    path = tmp_path / 'errors.jsonl'
    monkeypatch.setattr(diag, 'MAX_BYTES', 20)
    diag.append_failure(path, {'networkError': 'net::ERR_ABORTED'})
    diag.append_failure(path, {'networkError': 'net::ERR_CONNECTION_RESET'})
    assert path.with_suffix('.previous.jsonl').exists()
    assert diag.tail_failures(path, 1) == [{'networkError': 'net::ERR_CONNECTION_RESET'}]


async def test_endpoint_persists_failure_and_profile_without_session_capture(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from agent import main
    monkeypatch.setattr(main, '_NETLOG_PATH', tmp_path / 'netlog.jsonl')
    async def body():
        return {'event': 'network_error', 'url': 'https://flow.google.com/_/test?at=secret',
                'profileId': 'nick-test', 'requestId': '12', 'networkError': 'net::ERR_ABORTED',
                'freq': 'private', 'session': 'private'}
    assert await main.ext_netlog(SimpleNamespace(json=body)) == {'ok': True}
    result = await main.ext_network_errors(10)
    assert result['count'] == 1
    assert result['entries'][0]['profileId'] == 'nick-test'
    assert not (tmp_path / 'netlog.jsonl').exists()
    assert 'private' not in json.dumps(result) and 'secret' not in json.dumps(result)


async def test_draining_accepts_extension_results_but_blocks_new_generation(monkeypatch):
    from types import SimpleNamespace
    from starlette.responses import Response
    from agent.services import request_shield as mod
    shield = mod.ClientRequestShield()
    shield.set_draining(True)
    monkeypatch.setattr(mod, '_GLOBAL_SHIELD', shield)
    middleware = mod.RequestShieldMiddleware(None)
    async def downstream(request):
        return Response(status_code=200)
    for path in ['/api/ext/callback', '/api/ext/netlog']:
        result = await middleware.dispatch(SimpleNamespace(url=SimpleNamespace(path=path), method='POST'), downstream)
        assert result.status_code == 200
    result = await middleware.dispatch(SimpleNamespace(url=SimpleNamespace(path='/api/flow/generate-image'), method='POST'), downstream)
    assert result.status_code == 503
    rejection = json.loads(result.body)
    assert rejection['error'] == 'FLOW_REQUEST_NOT_SUBMITTED'
    assert rejection['retryable'] is True and rejection['retry_after_s'] == 3


async def test_hot_reload_preserves_inflight_shield(monkeypatch):
    import importlib
    from agent.services import request_shield as mod
    shield = mod.ClientRequestShield()
    monkeypatch.setattr(mod, '_GLOBAL_SHIELD', shield)
    await shield.acquire('test', '/api/flow/generate-image', 'POST', '127.0.0.1')
    importlib.reload(mod)
    assert mod.get_request_shield() is shield
    assert shield.get_status()['active_http_requests'] == 1
