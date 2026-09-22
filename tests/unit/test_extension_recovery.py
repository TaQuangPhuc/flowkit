from types import SimpleNamespace
import json
from unittest.mock import AsyncMock, Mock

import pytest

from agent.services.flow_client import FlowClient

PROJECT = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def test_api_does_not_convert_ambiguous_submission_into_retryable_rotation():
    from agent.api.flow import _respond_flow_result
    response = _respond_flow_result({
        "error": "SUBMISSION_OUTCOME_UNKNOWN: Execution context was destroyed",
        "error_code": "upstream_submission_unknown", "retryable": False,
    })
    assert response.status_code == 502
    assert json.loads(response.body)["retryable"] is False


def test_api_distinguishes_queue_pressure_from_unusual_and_failed_rotation():
    from agent.api.flow import _respond_flow_result
    queued = _respond_flow_result({"error": "FLOW_BUSY_NOT_SUBMITTED"})
    assert queued.status_code == 429
    assert json.loads(queued.body)["error"] == "FLOW_REQUEST_NOT_SUBMITTED"
    failed = _respond_flow_result({"error": "PUBLIC_ERROR_UNUSUAL_ACTIVITY", "proxy_rotated": False})
    assert json.loads(failed.body)["proxy_rotated"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("rotation_ok", [False, True])
async def test_recovery_honors_rotation_result_and_never_reloads_twice(monkeypatch, rotation_ok):
    from agent.services import accounts, proxy_pool, proxy_checker, unusual_audit
    client = FlowClient()
    ws = object()
    route = {"ws": ws, "profile_id": "nick-a", "project_id": PROJECT, "pinned": True}
    client._extensions[ws] = {"profile_id": "nick-a", "in_flight": 0}
    client._profile_candidates = Mock(return_value=("nick-a", [route]))
    audit = Mock()
    audit.record_request_dispatched.return_value = {"burst_10s": 1}
    monkeypatch.setattr(unusual_audit, "get_unusual_audit", lambda: audit)
    monkeypatch.setattr(accounts, "get_account", lambda *a, **k: {"proxy_url": "http://old.test:8080"})
    monkeypatch.setattr(proxy_checker, "quarantine_proxy", Mock())
    monkeypatch.setattr(proxy_pool, "rotate_nick_proxy", AsyncMock(return_value={
        "ok": rotation_ok, "error": "NO_DISTINCT_EGRESS", "egress_ip": "8.8.8.8",
        "proxy": "http://proxy.test:8080", "flow_tab_reloaded": True,
    }))
    client._send = AsyncMock()
    builder = AsyncMock(side_effect=[{"error": "ogiZ0b PUBLIC_ERROR_UNUSUAL_ACTIVITY"}, {"data": "ok"}])
    result = await client._run_on_profile(builder, requested_project=PROJECT)
    assert builder.await_count == (2 if rotation_ok else 1)
    client._send.assert_not_called()
    event = audit.record_unusual_event.call_args.kwargs
    assert event["proxy_url"] == "http://old.test:8080"
    assert event["burst_metrics"]["rpc_in_flight"] == 1
    if rotation_ok:
        assert result["data"] == "ok"
        assert event["rotation_info"]["new_proxy_ip"] == "8.8.8.8"
    else:
        assert result["proxy_rotated"] is False
        assert not event["rotation_info"]["retry_attempted"]


@pytest.mark.asyncio
async def test_control_refuses_old_extension_and_never_uses_other_nick():
    client = FlowClient()
    client._extensions[object()] = {"profile_id": "nick-a"}
    client._extensions[object()] = {"profile_id": "nick-b", "flow_guard_version": "new"}
    client._send = AsyncMock()
    result = await client.profile_control("nick-a", "prepare_proxy_rotation", {"leaseId": "one"})
    assert result["error"] == "EXTENSION_UPDATE_REQUIRED"
    client._send.assert_not_called()


@pytest.mark.asyncio
async def test_rotation_drains_before_commit_and_releases_after_bridge_switch(tmp_path, monkeypatch):
    from agent.services import flow_client, proxy_pool, proxy_checker
    from agent.services.accounts import save_accounts
    path = tmp_path / "accounts.json"
    url = "http://surf__cr.vn;sessid.old:test@127.0.0.1:18888"
    save_accounts([{"id": "nick-a", "proxy_url": url}], path)
    events = []

    async def control(nick, method, params, **kwargs):
        events.append(method)
        if method == "finish_proxy_rotation":
            assert params["reload"] is True
        return {"ok": True}

    def probe(url):
        if "prepare.1" in url:
            events.append("probe")
        else:
            assert events[-1] == "prepare_proxy_rotation"
            events.append("commit")
        return "8.8.8.8"

    async def apply(*args):
        events.append("bridge")
        return {"ok": True}

    fake = SimpleNamespace(_event_loop=None, _extensions={object(): {"profile_id": "nick-a"}}, profile_control=control)
    monkeypatch.setattr(flow_client, "get_flow_client", lambda: fake)
    monkeypatch.setattr(proxy_pool, "probe_egress", probe)
    monkeypatch.setattr(proxy_pool, "_apply_nick_proxy", apply)
    monkeypatch.setattr(proxy_checker, "check_single_proxy", lambda *a, **k: {"status": "CLEAN", "labs_accessible": True})
    result = await proxy_pool.rotate_nick_proxy("nick-a", accounts_path=path)
    assert result["flow_tab_reloaded"]
    assert events == ["probe", "prepare_proxy_rotation", "commit", "bridge", "finish_proxy_rotation"]


@pytest.mark.asyncio
async def test_failed_drain_does_not_commit_proxy(tmp_path, monkeypatch):
    from agent.services import flow_client, proxy_pool, proxy_checker
    from agent.services.accounts import save_accounts
    path = tmp_path / "accounts.json"
    save_accounts([{"id": "nick-a", "proxy_url": "http://surf__cr.vn;sessid.old:test@127.0.0.1:18888"}], path)
    before = path.read_bytes()
    fake = SimpleNamespace(_event_loop=None, _extensions={object(): {"profile_id": "nick-a"}},
        profile_control=AsyncMock(return_value={"ok": False, "error": "FLOW_DRAIN_TIMEOUT"}))
    probe = Mock(return_value="8.8.8.8")
    monkeypatch.setattr(flow_client, "get_flow_client", lambda: fake)
    monkeypatch.setattr(proxy_pool, "probe_egress", probe)
    monkeypatch.setattr(proxy_checker, "check_single_proxy", lambda *a, **k: {"status": "CLEAN", "labs_accessible": True})
    result = await proxy_pool.rotate_nick_proxy("nick-a", accounts_path=path)
    assert not result["ok"]
    assert probe.call_count == 1
    assert path.read_bytes() == before
    assert fake.profile_control.call_args_list[-1].args[1] == "finish_proxy_rotation"
