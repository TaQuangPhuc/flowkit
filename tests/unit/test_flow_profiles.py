"""Multi-nick gate: least-busy routing, project pin, per-nick r2v session."""
import json
import time
from unittest.mock import AsyncMock

import pytest

from agent.config import load_flow_profiles
from agent.services import flow_batch as fb
from agent.services.flow_client import FlowClient

PA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
PB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
PC = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
OPERATION = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
MEDIA = "12345678-1234-1234-1234-1234567890ab"


def envelope(rpcid: str, payload) -> str:
    chunk = json.dumps([["wrb.fr", rpcid, json.dumps(payload)]])
    return f")]}}'\n{len(chunk)}\n{chunk}"


def t2v_project(freq: str) -> str:
    inner = json.loads(json.loads(freq)[0][0][1])
    return inner[1][5]


def stream_session(freq: str) -> str:
    return json.loads(json.loads(freq)[1])[0]


def attach(client, profile_id, project_id, *, in_flight=0, recency=None, chat=None):
    ws = object()
    now = recency if recency is not None else time.time()
    client._extensions[ws] = {
        "connected_at": now,
        "flow_key": None,
        "token_captured_at": now,
        "unavailable_until": 0,
        "profile_id": profile_id,
        "project_id": project_id,
        "chat_session_id": chat,
        "in_flight": in_flight,
    }
    return ws


@pytest.fixture
def client(monkeypatch, tmp_path):
    import agent.services.flow_client as module
    monkeypatch.setattr(module, "USE_BATCH_RPC", True)
    monkeypatch.setattr(module, "FLOW_PROJECT_ID", PA)
    monkeypatch.setattr(module, "FLOW_ALLOW_DEGRADED", False)

    # Routing drops nicks whose account row is disabled. These tests are about
    # pin/least-busy order only, so keep them off the real agent/accounts.json.
    acc_path = tmp_path / "fixture-accounts.json"
    acc_path.write_text("[]", encoding="utf-8")
    monkeypatch.setattr("agent.services.accounts.ACCOUNTS_FILE", acc_path)

    c = FlowClient()
    # Handshake tests exercise routing/session binding, not background DB tier sync.
    c._sync_tier = AsyncMock()
    c.responses = {fb.RPC_PROJECT_SETTINGS: {"data": envelope(fb.RPC_PROJECT_SETTINGS, [])}}
    c.calls = []

    async def fake_batch_rpc(rpcid, freq, captcha_action=None, match=None,
                             timeout=300, path=None):
        c.calls.append({"rpcid": rpcid, "freq": freq,
                        "captcha": captcha_action, "match": match, "path": path})
        canned = c.responses.get(rpcid, {"data": ""})
        if callable(canned):
            try:
                return canned(match, freq)
            except TypeError:
                return canned(match)
        return canned

    c.batch_rpc = fake_batch_rpc
    return c


def _t2v_ok():
    return {"data": envelope(fb.RPC_GEN_T2V, [None, 50, [[OPERATION, PA, "scene", None]]])}


class TestSelectProfile:
    def test_least_busy_wins(self, client):
        attach(client, "nick-a", PA, in_flight=2, recency=30)
        attach(client, "nick-b", PB, in_flight=0, recency=10)
        attach(client, "nick-c", PC, in_flight=1, recency=20)
        route = client._select_profile()
        assert route["profile_id"] == "nick-b"
        assert route["pinned"] is False

    def test_matching_project_pins_that_nick(self, client):
        attach(client, "nick-a", PA, in_flight=5)
        attach(client, "nick-b", PB, in_flight=0)
        route = client._select_profile(project_id=PA)
        assert route["profile_id"] == "nick-a"
        assert route["pinned"] is True

    def test_media_pins_the_nick_that_created_it(self, client):
        attach(client, "nick-a", PA, in_flight=0)
        attach(client, "nick-b", PB, in_flight=0, recency=time.time() + 10)
        client._media_profiles[MEDIA] = "nick-a"
        route = client._select_profile(media_ids=[MEDIA])
        assert route["profile_id"] == "nick-a"
        assert route["pinned"] is True

    def test_operation_pins_the_creating_nick(self, client):
        attach(client, "nick-a", PA)
        attach(client, "nick-b", PB, in_flight=0, recency=time.time() + 10)
        client._operation_profiles[OPERATION] = "nick-b"
        route = client._select_profile(operation_id=OPERATION)
        assert route["profile_id"] == "nick-b"

    def test_unknown_project_is_not_a_pin(self, client):
        attach(client, "nick-a", PA)
        attach(client, "nick-b", PB)
        route = client._select_profile(project_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd")
        assert route["pinned"] is False


class TestRunOnProfile:
    @pytest.mark.parametrize("message", ["Extension disconnected", "YhhmEf: Failed to fetch", "eb1hJf failed: [13]"])
    async def test_ambiguous_video_submit_does_not_move_to_another_nick(self, client, message):
        attach(client, "nick-a", PA, recency=20)
        attach(client, "nick-b", PB, recency=10)
        client.responses[fb.RPC_GEN_T2V] = {"error": message}
        result = await client.generate_video(None, "go", "0", "scene-1")
        assert result["retryable"] is False
        assert result["error_code"] == "upstream_submission_unknown"
        assert len(client.calls) == 1

    async def test_mixed_reference_owners_fail_before_google_submission(self, client):
        attach(client, "nick-a", PA)
        attach(client, "nick-b", PB)
        client._media_profiles.update({MEDIA: "nick-a", OPERATION: "nick-b"})
        result = await client.generate_video_from_references([MEDIA, OPERATION], "go", "0", "scene-1")
        assert result["error_code"] == "media_profile_mismatch"
        assert result["retryable"] is False
        assert not client.calls

    async def test_explicit_profile_cannot_use_another_nicks_image(self, client):
        attach(client, "nick-a", PA)
        attach(client, "nick-b", PB)
        client._media_profiles[MEDIA] = "nick-a"
        result = await client.generate_video(MEDIA, "go", "0", "scene-1", profile_id="nick-b")
        assert result["error_code"] == "media_profile_mismatch"
        assert not client.calls

    async def test_unpinned_rewrites_to_the_nicks_project(self, client):
        attach(client, "nick-a", PA, in_flight=2)
        attach(client, "nick-b", PB, in_flight=0)
        client.responses[fb.RPC_GEN_T2V] = _t2v_ok()
        await client.generate_video(None, "go", "0", "scene-1")
        assert client._last_route["profile_id"] == "nick-b"
        assert client._last_route["pinned"] is False
        assert t2v_project(client.calls[0]["freq"]) == PB

    async def test_unpinned_failsover_and_rebuilds_the_envelope(self, client):
        attach(client, "nick-a", PA, recency=20)
        attach(client, "nick-b", PB, recency=10)
        client.responses[fb.RPC_GEN_T2V] = _t2v_ok()

        original = client.batch_rpc

        async def flaky(rpcid, freq, captcha_action=None, match=None,
                        timeout=300, path=None):
            if t2v_project(freq) == PA:
                client.calls.append({"rpcid": rpcid, "freq": freq})
                return {"error": "NO_AT_TOKEN"}
            return await original(rpcid, freq, captcha_action, match, timeout, path)

        client.batch_rpc = flaky
        result = await client.generate_video(None, "go", "0", "scene-1")
        assert not result.get("error")
        assert client._last_route["profile_id"] == "nick-b"
        assert t2v_project(client.calls[-1]["freq"]) == PB

    async def test_unpinned_failsover_on_401_and_no_envelope(self, client):
        attach(client, "nick-a", PA, recency=20)
        attach(client, "nick-b", PB, recency=10)
        client.responses[fb.RPC_GEN_T2V] = _t2v_ok()

        original = client.batch_rpc

        async def fail_401(rpcid, freq, captcha_action=None, match=None,
                           timeout=300, path=None):
            if t2v_project(freq) == PA:
                client.calls.append({"rpcid": rpcid, "freq": freq})
                return {"error": "FlowBatchError: no YhhmEf envelope in response (0 others)", "status": 401}
            return await original(rpcid, freq, captcha_action, match, timeout, path)

        client.batch_rpc = fail_401
        result = await client.generate_video(None, "go", "0", "scene-1")
        assert not result.get("error")
        assert client._last_route["profile_id"] == "nick-b"
        assert t2v_project(client.calls[-1]["freq"]) == PB

    async def test_disabled_account_excluded_from_candidates(self, client, monkeypatch):
        attach(client, "nick-a", PA, recency=20)
        attach(client, "nick-b", PB, recency=10)

        # Mock get_account so nick-a is disabled
        from agent.services import accounts
        orig_get_acc = accounts.get_account
        def mock_get_account(sid):
            if sid == "nick-a":
                return {"id": "nick-a", "enabled": False}
            return {"id": "nick-b", "enabled": True}
        monkeypatch.setattr(accounts, "get_account", mock_get_account)

        _pin, candidates = client._profile_candidates()
        prof_ids = [c["profile_id"] for c in candidates]
        assert "nick-a" not in prof_ids
        assert "nick-b" in prof_ids

    async def test_pinned_project_does_not_failover(self, client):
        attach(client, "nick-a", PA, recency=20)
        attach(client, "nick-b", PB, recency=10)

        async def always_fail(rpcid, freq, captcha_action=None, match=None,
                              timeout=300, path=None):
            client.calls.append({"rpcid": rpcid, "freq": freq})
            return {"error": "NO_AT_TOKEN"}

        client.batch_rpc = always_fail
        result = await client.generate_video(None, "go", PA, "scene-1")
        assert "NO_AT_TOKEN" in result["error"]
        assert client._last_route["profile_id"] == "nick-a"
        assert client._last_route["pinned"] is True
        assert len(client.calls) == 1
        assert t2v_project(client.calls[0]["freq"]) == PA

    async def test_i2v_follows_the_start_image_nick(self, client):
        attach(client, "nick-a", PA, in_flight=0)
        attach(client, "nick-b", PB, in_flight=0, recency=time.time() + 10)
        client._media_profiles[MEDIA] = "nick-a"
        client.responses[fb.RPC_GEN_VIDEO] = {
            "data": envelope(fb.RPC_GEN_VIDEO, [None, 50, [[OPERATION, PA, "s", None]]])
        }
        await client.generate_video(MEDIA, "go", "0", "scene-1")
        assert client._last_route["profile_id"] == "nick-a"
        assert client._operation_profiles[OPERATION] == "nick-a"

    async def test_missing_pinned_nick_errors_instead_of_stealing(self, client):
        attach(client, "nick-b", PB)
        client._media_profiles[MEDIA] = "nick-a"
        result = await client.generate_video(MEDIA, "go", "0", "scene-1")
        assert "nick-a" in result["error"]
        assert not client.calls


class TestR2vSession:
    async def test_each_nick_keeps_its_own_chat_session(self, client):
        attach(client, "nick-a", PA, recency=20)
        attach(client, "nick-b", PB, recency=10)
        client.remember_chat_session("11111111-1111-4111-8111-111111111111", profile_id="nick-a")
        client.remember_chat_session("22222222-2222-4222-8222-222222222222", profile_id="nick-b")
        client.responses[fb.RPC_STREAM_CHAT] = {
            "data": envelope(fb.RPC_STREAM_CHAT, [None, 50, [[OPERATION, PA, "s", None]]])
        }

        def create_for_project(_match, freq):
            inner = json.loads(json.loads(freq)[0][0][1])
            sid = ("11111111-1111-4111-8111-111111111111"
                   if inner[0] == PA else
                   "22222222-2222-4222-8222-222222222222")
            return {"data": envelope(fb.RPC_CREATE_SESSION, [sid])}

        client.responses[fb.RPC_CREATE_SESSION] = create_for_project

        await client.generate_video_from_references(["ref-a"], "go", PA, "s")
        chat = next(c for c in client.calls if c["rpcid"] == fb.RPC_STREAM_CHAT)
        assert stream_session(chat["freq"]) == "11111111-1111-4111-8111-111111111111"
        assert client._last_route["profile_id"] == "nick-a"

        client.calls.clear()
        await client.generate_video_from_references(["ref-b"], "go", PB, "s")
        chat = next(c for c in client.calls if c["rpcid"] == fb.RPC_STREAM_CHAT)
        assert stream_session(chat["freq"]) == "22222222-2222-4222-8222-222222222222"
        assert client._last_route["profile_id"] == "nick-b"


class TestHandshake:
    async def test_extension_ready_stores_profile_and_project(self, client):
        ws = object()
        client.set_extension(ws)
        await client.handle_message({
            "type": "extension_ready",
            "profileId": "nick-a",
            "flowProjectId": PA,
            "flowKeyPresent": False,
        }, websocket=ws)
        await client._chat_bind_task
        session = client._extensions[ws]
        assert session["profile_id"] == "nick-a"
        assert session["project_id"] == PA

    async def test_chat_session_binds_that_ws(self, client):
        ws = object()
        client.set_extension(ws)
        await client.handle_message({
            "type": "chat_session",
            "session": "8c72f80b-41ff-42f6-9dff-5a759553f9f4",
            "profileId": "nick-a",
            "flowProjectId": PA,
        }, websocket=ws)
        session = client._extensions[ws]
        assert session["chat_session_id"] == "8c72f80b-41ff-42f6-9dff-5a759553f9f4"
        assert session["profile_id"] == "nick-a"
        workers = client.workers()
        assert workers[0]["chat_session"] is True

    async def test_extension_ready_reuses_a_listed_session(self, client):
        sid = "8c72f80b-41ff-42f6-9dff-5a759553f9f4"
        client.responses[fb.RPC_LIST_SESSIONS] = {
            "data": envelope(fb.RPC_LIST_SESSIONS, [[sid, "Untitled"]])
        }
        ws = object()
        client.set_extension(ws)
        await client.handle_message({
            "type": "extension_ready",
            "profileId": "nick-a",
            "flowProjectId": PA,
            "flowKeyPresent": False,
        }, websocket=ws)
        await client._chat_bind_task
        assert client._extensions[ws]["chat_session_id"] == sid
        assert [c["rpcid"] for c in client.calls] == [fb.RPC_LIST_SESSIONS]
        assert client.workers()[0]["chat_session"] is True

    async def test_extension_ready_creates_a_session_when_listing_is_empty(self, client):
        sid = "da9d4724-5d1c-4b19-bf13-c24ea499e41f"
        client.responses[fb.RPC_LIST_SESSIONS] = {
            "data": envelope(fb.RPC_LIST_SESSIONS, [])
        }
        client.responses[fb.RPC_CREATE_SESSION] = {
            "data": envelope(fb.RPC_CREATE_SESSION, [sid])
        }
        ws = object()
        client.set_extension(ws)
        await client.handle_message({
            "type": "extension_ready",
            "profileId": "nick-a",
            "flowProjectId": PA,
            "flowKeyPresent": False,
        }, websocket=ws)
        await client._chat_bind_task
        assert client._extensions[ws]["chat_session_id"] == sid
        assert [c["rpcid"] for c in client.calls] == [
            fb.RPC_LIST_SESSIONS, fb.RPC_CREATE_SESSION,
        ]

    async def test_already_bound_session_skips_list_and_create(self, client):
        ws = attach(client, "nick-a", PA, chat="8c72f80b-41ff-42f6-9dff-5a759553f9f4")
        await client._ensure_chat_session(ws)
        assert client.calls == []

    async def test_bind_retries_an_injection_miss(self, client):
        sid = "8c72f80b-41ff-42f6-9dff-5a759553f9f4"
        hits = {"n": 0}

        async def fake_batch_rpc(rpcid, freq, captcha_action=None, match=None,
                                 timeout=300, path=None):
            client.calls.append({"rpcid": rpcid})
            hits["n"] += 1
            if hits["n"] == 1:
                return {"error": "mrlkwd: NO_INJECTION_RESULT"}
            return {"data": envelope(fb.RPC_LIST_SESSIONS, [[sid]])}

        client.batch_rpc = fake_batch_rpc
        ws = attach(client, "nick-a", PA)
        await client._ensure_chat_session(ws)
        assert client._extensions[ws]["chat_session_id"] == sid
        assert hits["n"] == 2

    def test_workers_lists_connected_nicks(self, client):
        attach(client, "nick-a", PA, in_flight=1)
        attach(client, "nick-b", PB, in_flight=0)
        workers = client.workers()
        ids = {w["profile_id"] for w in workers}
        assert ids == {"nick-a", "nick-b"}
        busy = next(w for w in workers if w["profile_id"] == "nick-a")
        assert busy["in_flight"] == 1


class TestLoadProfiles:
    def test_reads_id_and_project(self, tmp_path):
        path = tmp_path / "profiles.json"
        path.write_text(json.dumps({
            "profiles": [
                {"id": "nick-a", "project_id": PA},
                {"id": "", "project_id": PB},
                {"id": "nick-c", "project_id": ""},
            ]
        }))
        rows = load_flow_profiles(path)
        assert [r["id"] for r in rows] == ["nick-a", "nick-c"]
        assert rows[0]["project_id"] == PA
        assert rows[1]["project_id"] == ""

    def test_missing_file_is_empty(self, tmp_path):
        assert load_flow_profiles(tmp_path / "nope.json") == []


class TestFairLoadBalancing:
    def test_least_dispatched_wins_among_idle_workers(self, client):
        ws_a = attach(client, "nick-a", PA, recency=100)
        ws_b = attach(client, "nick-b", PB, recency=50)
        # Manually give nick-a higher dispatched_count
        client._extensions[ws_a]["dispatched_count"] = 10
        client._extensions[ws_b]["dispatched_count"] = 2
        route = client._select_profile()
        assert route["profile_id"] == "nick-b"


class TestOperationPolling:
    @pytest.mark.asyncio
    async def test_poll_normalizes_operation_prefix_and_times_out(self, client):
        op_uuid = "1d82709e-d4a9-4302-97cc-e785bee4439a"
        raw_op_name = f"operations/{op_uuid}"
        client._remember_operation(op_uuid, PA)
        assert client._operation_projects.get(raw_op_name) == PA
        assert client._operation_projects.get(op_uuid) == PA

        # Timeout uses elapsed time, regardless of client polling frequency.
        from agent.config import VIDEO_POLL_TIMEOUT
        client._operation_start_time[op_uuid] = time.time() - VIDEO_POLL_TIMEOUT - 1
        res = await client._poll_batch_operation(raw_op_name)
        assert res["status"] == "MEDIA_GENERATION_STATUS_FAILED"
        assert res["operation"]["name"] == raw_op_name
        assert "upstream_timeout" in res["error"]


async def test_anchored_upload_uses_existing_owner_and_cannot_fail_over(client):
    from unittest.mock import AsyncMock
    client._media_profiles[MEDIA] = "nick-b"
    client._run_on_profile = AsyncMock(return_value={"status": 200})
    await client.upload_image("fixture", reference_media_id=MEDIA)
    kwargs = client._run_on_profile.call_args.kwargs
    assert kwargs["profile_id"] == "nick-b"
    assert kwargs["allow_failover"] is False
    client._run_on_profile.reset_mock()
    result = await client.upload_image("fixture", reference_media_id=MEDIA, profile_id="nick-a")
    assert result["status"] == 400
    client._run_on_profile.assert_not_called()


async def test_unknown_upload_anchor_fails_before_upload(client):
    from unittest.mock import AsyncMock
    client._run_on_profile = AsyncMock()
    result = await client.upload_image("fixture", reference_media_id=MEDIA)
    assert result["status"] == 400
    client._run_on_profile.assert_not_called()


async def test_per_worker_video_pacing_and_cooldown(client, monkeypatch):
    import asyncio
    import time
    from agent import config as _cfg
    monkeypatch.setattr(_cfg, "PER_WORKER_VIDEO_COOLDOWN_MIN", 0.10)
    monkeypatch.setattr(_cfg, "PER_WORKER_VIDEO_COOLDOWN_MAX", 0.15)

    ws1 = object()
    ws2 = object()
    client.set_extension(ws1)
    client.set_extension(ws2)
    client._extensions[ws1]["profile_id"] = "worker-1"
    client._extensions[ws1]["project_id"] = PA
    client._extensions[ws1]["token_captured_at"] = time.time()
    client._extensions[ws2]["profile_id"] = "worker-2"
    client._extensions[ws2]["project_id"] = PB
    client._extensions[ws2]["token_captured_at"] = time.time()

    timestamps = []

    async def fake_builder(_pid):
        timestamps.append(time.monotonic())
        return {"status": 200, "operation": {"name": "op_test"}}

    # Dispatches on the same worker should be spaced by at least 0.10s
    t0 = time.monotonic()
    await client._run_on_profile(fake_builder, PA, profile_id="worker-1", video_submission=True)
    await client._run_on_profile(fake_builder, PA, profile_id="worker-1", video_submission=True)
    t1 = time.monotonic()

    assert len(timestamps) == 2
    gap = timestamps[1] - timestamps[0]
    assert gap >= 0.09, f"Expected gap >= 0.09s, got {gap:.3f}s"

    # Dispatches on different workers should NOT block each other
    timestamps.clear()
    t_start = time.monotonic()
    await asyncio.gather(
        client._run_on_profile(fake_builder, PA, profile_id="worker-1", video_submission=True),
        client._run_on_profile(fake_builder, PB, profile_id="worker-2", video_submission=True),
    )
    t_end = time.monotonic()
    # Since worker-1 and worker-2 are distinct, they should run nearly simultaneously
    assert (t_end - t_start) < 0.25

