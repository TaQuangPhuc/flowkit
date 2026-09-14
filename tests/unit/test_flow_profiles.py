"""Multi-nick gate: least-busy routing, project pin, per-nick r2v session."""
import json
import time

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
def client(monkeypatch):
    import agent.services.flow_client as module
    monkeypatch.setattr(module, "USE_BATCH_RPC", True)
    monkeypatch.setattr(module, "FLOW_PROJECT_ID", PA)
    monkeypatch.setattr(module, "FLOW_ALLOW_DEGRADED", False)

    c = FlowClient()
    c.responses = {}
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
