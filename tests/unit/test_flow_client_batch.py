"""The batch path's answers, in the shapes the rest of the pipeline reads.

Everything downstream of FlowClient — the worker's parsers, the operation
poller, the scene writers — was written against the old REST responses. These
tests hold the adapter to that contract, so a transport swap stays invisible.
"""
import json

import pytest

from agent.services import flow_batch as fb
from agent.services.flow_client import FlowClient
from agent.worker._parsing import _extract_media_id, _extract_output_url, _is_error

PROJECT = "11111111-2222-3333-4444-555555555555"
MEDIA = "12345678-1234-1234-1234-1234567890ab"
OPERATION = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
CHAT_SESSION = "8c72f80b-41ff-42f6-9dff-5a759553f9f4"
IMAGE_URL = f"https://{fb.MEDIA_HOST}/image/{MEDIA}?sig=x"
VIDEO_URL = f"https://{fb.MEDIA_HOST}/video/{MEDIA}?sig=x"


def rpc_call(client, rpcid):
    return next(c for c in client.calls if c["rpcid"] == rpcid)


def stub_create_session(client, session=CHAT_SESSION):
    client.responses[fb.RPC_CREATE_SESSION] = {
        "data": envelope(fb.RPC_CREATE_SESSION, [session])
    }


def envelope(rpcid: str, payload) -> str:
    chunk = json.dumps([["wrb.fr", rpcid, json.dumps(payload)]])
    return f")]}}'\n{len(chunk)}\n{chunk}"


@pytest.fixture
def client(monkeypatch):
    """A FlowClient whose transport replays canned RPC responses.

    `calls` records what each rpc was asked, so a test can assert on the
    envelope as well as on what came back.
    """
    import agent.services.flow_client as module
    monkeypatch.setattr(module, "USE_BATCH_RPC", True)
    monkeypatch.setattr(module, "FLOW_PROJECT_ID", PROJECT)
    monkeypatch.setattr(module, "FLOW_ALLOW_DEGRADED", False)

    c = FlowClient()
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


class TestGenerateImages:
    async def test_answers_in_the_shape_the_media_parser_reads(self, client):
        client.responses[fb.RPC_GEN_IMAGE] = {"data": envelope(fb.RPC_GEN_IMAGE, [[IMAGE_URL]])}
        result = await client.generate_images("a cat", PROJECT)

        assert not _is_error(result)
        assert _extract_media_id(result, "GENERATE_IMAGE") == MEDIA
        assert _extract_output_url(result, "GENERATE_IMAGE") == IMAGE_URL

    async def test_asks_for_a_captcha(self, client):
        client.responses[fb.RPC_GEN_IMAGE] = {"data": envelope(fb.RPC_GEN_IMAGE, [[IMAGE_URL]])}
        await client.generate_images("a cat", PROJECT)
        assert client.calls[0]["captcha"] == fb.CAPTCHA_IMAGE

    async def test_character_refs_ride_in_the_reference_slot(self, client):
        client.responses[fb.RPC_GEN_IMAGE] = {"data": envelope(fb.RPC_GEN_IMAGE, [[IMAGE_URL]])}
        await client.generate_images("a cat", PROJECT, character_media_ids=["ref-a", "ref-b"])

        item = json.loads(json.loads(client.calls[0]["freq"])[0][0][1])[1][0]
        assert item[2] == [["ref-a", None, None, None, fb.REF_TYPE_IMAGE],
                           ["ref-b", None, None, None, fb.REF_TYPE_IMAGE]]

    async def test_a_project_less_call_falls_back_to_the_pinned_project(self, client):
        client.responses[fb.RPC_GEN_IMAGE] = {"data": envelope(fb.RPC_GEN_IMAGE, [[IMAGE_URL]])}
        await client.generate_images("a cat", "0")

        item = json.loads(json.loads(client.calls[0]["freq"])[0][0][1])[1][0]
        assert item[7][5] == PROJECT

    async def test_no_url_back_is_an_error_not_a_silent_success(self, client):
        client.responses[fb.RPC_GEN_IMAGE] = {"data": envelope(fb.RPC_GEN_IMAGE, [[]])}
        assert _is_error(await client.generate_images("a cat", PROJECT))

    async def test_a_transport_error_becomes_an_error_result(self, client):
        client.responses[fb.RPC_GEN_IMAGE] = {"error": "CAPTCHA_FAILED: NO_FLOW_TAB"}
        result = await client.generate_images("a cat", PROJECT)
        assert _is_error(result) and "NO_FLOW_TAB" in result["error"]

    async def test_no_project_anywhere_is_a_named_failure(self, client, monkeypatch):
        import agent.services.flow_client as module
        monkeypatch.setattr(module, "FLOW_PROJECT_ID", "")
        result = await client.generate_images("a cat", "0")
        assert "NO_FLOW_PROJECT" in result["error"]


class TestEditImage:
    async def test_the_source_leads_the_reference_list(self, client):
        client.responses[fb.RPC_GEN_IMAGE] = {"data": envelope(fb.RPC_GEN_IMAGE, [[IMAGE_URL]])}
        await client.edit_image("redraw", "src-1", PROJECT, character_media_ids=["ref-a"])

        item = json.loads(json.loads(client.calls[0]["freq"])[0][0][1])[1][0]
        assert [ref[0] for ref in item[2]] == ["src-1", "ref-a"]

    async def test_the_source_is_not_repeated_when_it_is_also_a_character(self, client):
        client.responses[fb.RPC_GEN_IMAGE] = {"data": envelope(fb.RPC_GEN_IMAGE, [[IMAGE_URL]])}
        await client.edit_image("redraw", "src-1", PROJECT, character_media_ids=["src-1", "ref-a"])

        item = json.loads(json.loads(client.calls[0]["freq"])[0][0][1])[1][0]
        assert [ref[0] for ref in item[2]] == ["src-1", "ref-a"]


class TestGenerateVideo:
    def _submitted(self, client):
        return {"data": envelope(fb.RPC_GEN_VIDEO, [None, 50, [[OPERATION, PROJECT, "scene", None]]])}

    async def test_returns_an_operation_the_poller_can_carry(self, client):
        client.responses[fb.RPC_GEN_VIDEO] = self._submitted(client)
        result = await client.generate_video("mid", "go", PROJECT, "scene-1")

        ops = result["data"]["operations"]
        assert ops[0]["operation"]["name"] == OPERATION
        assert ops[0]["status"] == "MEDIA_GENERATION_STATUS_PENDING"

    async def test_remembers_which_project_to_look_the_media_up_in(self, client):
        client.responses[fb.RPC_GEN_VIDEO] = self._submitted(client)
        await client.generate_video("mid", "go", PROJECT, "scene-1")
        assert client._operation_projects[OPERATION] == PROJECT

    async def test_chaining_fails_loudly_rather_than_dropping_the_end_frame(self, client):
        result = await client.generate_video("mid", "go", PROJECT, "scene-1",
                                             end_image_media_id="end-mid")
        assert "UNSUPPORTED_ON_BATCH_API" in result["error"]
        assert not client.calls, "nothing should have been sent"

    async def test_degraded_mode_runs_i2v_off_the_start_frame(self, client, monkeypatch):
        import agent.services.flow_client as module
        monkeypatch.setattr(module, "FLOW_ALLOW_DEGRADED", True)
        client.responses[fb.RPC_GEN_VIDEO] = self._submitted(client)

        result = await client.generate_video("start-mid", "go", PROJECT, "scene-1",
                                             end_image_media_id="end-mid")
        assert not _is_error(result)
        payload = json.loads(json.loads(client.calls[0]["freq"])[0][0][1])
        assert payload[0][0][4][1] == "start-mid"

    async def test_t2v_uses_yhhmef_when_no_start_image(self, client):
        client.responses[fb.RPC_GEN_T2V] = {
            "data": envelope(fb.RPC_GEN_T2V, [None, 50, [
                [OPERATION, PROJECT, "scene", None],
            ]])
        }
        result = await client.generate_video(None, "go", PROJECT, "scene-1")

        assert not _is_error(result)
        call = client.calls[0]
        assert call["rpcid"] == fb.RPC_GEN_T2V
        assert call["captcha"] == fb.CAPTCHA_VIDEO
        inner = json.loads(json.loads(call["freq"])[0][0][1])
        assert inner[0][0][1] == fb.VIDEO_T2V_MODEL
        assert len(inner[0][0]) == 5
        assert json.dumps(fb.FULL_FRAME_CROP) not in json.dumps(inner)
        assert result["data"]["operations"][0]["operation"]["name"] == OPERATION

    async def test_t2v_empty_start_image_is_not_i2v(self, client):
        client.responses[fb.RPC_GEN_T2V] = {
            "data": envelope(fb.RPC_GEN_T2V, [None, 50, [[OPERATION, PROJECT, "scene", None]]])
        }
        await client.generate_video("", "go", PROJECT, "scene-1")
        assert client.calls[0]["rpcid"] == fb.RPC_GEN_T2V
        assert fb.RPC_GEN_VIDEO not in [c["rpcid"] for c in client.calls]

    async def test_t2v_rejects_an_end_frame_instead_of_dropping_it(self, client):
        result = await client.generate_video(
            None, "go", PROJECT, "scene-1", end_image_media_id="end-mid")
        assert "t2v" in result["error"]
        assert not client.calls

    async def test_t2v_returns_an_operation_per_variant(self, client):
        op_b = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
        client.responses[fb.RPC_GEN_T2V] = {
            "data": envelope(fb.RPC_GEN_T2V, [None, 50, [
                [OPERATION, PROJECT, "scene", None],
                [op_b, PROJECT, "scene", None],
            ]])
        }
        result = await client.generate_video(None, "go", PROJECT, "s")
        names = [o["operation"]["name"] for o in result["data"]["operations"]]
        assert names == [OPERATION, op_b]
        assert client._operation_projects[op_b] == PROJECT

    async def test_r2v_posts_stream_chat_with_the_reference_ids(self, client):
        stub_create_session(client)
        client.responses[fb.RPC_STREAM_CHAT] = self._submitted(client)
        result = await client.generate_video_from_references(
            ["ref-a", "ref-b"], "go", PROJECT, "s")

        assert not _is_error(result)
        assert [c["rpcid"] for c in client.calls][:3] == [
            fb.RPC_PROJECT_SETTINGS, fb.RPC_CREATE_SESSION, fb.RPC_STREAM_CHAT,
        ]
        settings = rpc_call(client, fb.RPC_PROJECT_SETTINGS)
        assert settings["captcha"] is None
        row = json.loads(json.loads(settings["freq"])[0][0][1])
        assert row[1][2][1][0] == fb.VIDEO_R2V_MODEL
        assert row[2] == [["default_generation_settings.video_defaults"]]
        call = rpc_call(client, fb.RPC_STREAM_CHAT)
        assert call["path"] == fb.STREAM_CHAT_PATH
        assert call["captcha"] == fb.CAPTCHA_CHAT
        outer = json.loads(call["freq"])
        inner = json.loads(outer[1])
        assert inner[0] == CHAT_SESSION
        assert inner[1][0][0][0][0] == "go"
        assert inner[1][1] == [["ref-a"], ["ref-b"]]
        assert inner[2][0] == f"projects/{PROJECT}"
        assert inner[2][2][0] == fb.CAPTCHA_SLOT
        assert inner[2][5] == fb.STREAM_CHAT_VIDEO_MODE
        assert result["data"]["operations"][0]["operation"]["name"] == OPERATION

    async def test_r2v_mints_a_fresh_chat_session(self, client):
        stale = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
        fresh = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
        client.remember_chat_session(stale)
        stub_create_session(client, fresh)
        client.responses[fb.RPC_STREAM_CHAT] = self._submitted(client)
        result = await client.generate_video_from_references(
            ["ref-a"], "go", PROJECT, "s")
        assert not _is_error(result)
        chat = rpc_call(client, fb.RPC_STREAM_CHAT)
        inner = json.loads(json.loads(chat["freq"])[1])
        assert inner[0] == fresh
        assert inner[0] != stale
        assert client._operation_chat_sessions[OPERATION] == fresh
        assert client._operation_ref_ids[OPERATION] == ("ref-a",)

    async def test_r2v_without_a_uuid_polls_via_the_chat_session(self, client):
        """An ack with a null operation slot still has to be pollable."""
        stub_create_session(client)
        client.responses[fb.RPC_STREAM_CHAT] = {
            "data": envelope(fb.RPC_STREAM_CHAT, [None, 50, [[None, PROJECT, "s", None]]])
        }
        result = await client.generate_video_from_references(
            ["ref-a"], "go", PROJECT, "s")
        assert result["data"]["operations"][0]["operation"]["name"] == CHAT_SESSION
        assert client._operation_chat_sessions[CHAT_SESSION] == CHAT_SESSION
        assert client._operation_ref_ids[CHAT_SESSION] == ("ref-a",)

    async def test_r2v_prefers_a_later_cae_chunk_over_the_chat_uuid(self, client):
        """StreamChat streams the chat-message first; the listing key is later."""
        chat = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        scene = "99999999-9999-9999-9999-999999999999"
        body = (
            envelope(fb.RPC_STREAM_CHAT, [["chat-event", chat, "hello"]])
            + envelope(fb.RPC_STREAM_CHAT, [[MEDIA, PROJECT, scene, "CAE"]])
        )
        stub_create_session(client)
        client.responses[fb.RPC_STREAM_CHAT] = {"data": body}
        result = await client.generate_video_from_references(
            ["ref-a"], "go", PROJECT, "s")
        assert result["data"]["operations"][0]["operation"]["name"] == MEDIA
        assert client._operation_media[MEDIA] == MEDIA

    async def test_t2v_ops_snapshot_keeps_the_project_pin(self, client):
        client.responses[fb.RPC_GEN_T2V] = {
            "data": envelope(fb.RPC_GEN_T2V, [None, 50, [
                [OPERATION, PROJECT, "scene", None],
            ]])
        }
        await client.generate_video(None, "go", PROJECT, "scene-1")
        row = client._ops_snapshot()[OPERATION]
        assert row["project"] == PROJECT
        assert row["session"] == ""

    async def test_r2v_without_refs_fails_before_a_call(self, client):
        result = await client.generate_video_from_references([], "go", PROJECT, "s")
        assert "No reference media_ids" in result["error"]
        assert not client.calls

    async def test_upscale_is_unported_and_has_no_fallback(self, client, monkeypatch):
        import agent.services.flow_client as module
        monkeypatch.setattr(module, "FLOW_ALLOW_DEGRADED", True)
        result = await client.upscale_video(MEDIA, "scene-1")
        assert "UNSUPPORTED_ON_BATCH_API" in result["error"]


class TestCheckVideoStatus:
    def _poll(self, status=None, complaint=None):
        detail = None
        if complaint:
            detail = [None] * 8 + [[fb.OUTCOME_COMPLAINT, [None, complaint]]]
        record = [OPERATION, PROJECT, "scene", status, None, detail]
        return {"data": envelope(fb.RPC_OPERATION, [None, 50, [record]])}

    def _listing(self, found=True):
        text = (f'["{OPERATION}",null,null,["t",1,2,null,null,"{MEDIA}"]' if found else "")
        return lambda match: {"data": text}

    async def _status(self, client):
        result = await client.check_video_status([{"operation": {"name": OPERATION}}])
        return result["data"]["operations"][0]

    async def test_successful_once_a_video_url_exists(self, client):
        client.responses[fb.RPC_OPERATION] = self._poll(status="CAE")
        client.responses[fb.RPC_PROJECT_MEDIA] = self._listing()
        client.responses[fb.RPC_MEDIA] = {"data": envelope(fb.RPC_MEDIA, [VIDEO_URL])}

        op = await self._status(client)
        assert op["status"] == "MEDIA_GENERATION_STATUS_SUCCESSFUL"
        assert _extract_media_id({"data": {"operations": [op]}}, "GENERATE_VIDEO") == MEDIA
        assert _extract_output_url({"data": {"operations": [op]}}, "GENERATE_VIDEO") == VIDEO_URL

    async def test_a_media_id_with_only_a_poster_is_still_pending(self, client):
        """Downloading on the id alone would save a still picture."""
        client.responses[fb.RPC_OPERATION] = self._poll(status="CAE")
        client.responses[fb.RPC_PROJECT_MEDIA] = self._listing()
        client.responses[fb.RPC_MEDIA] = {"data": envelope(fb.RPC_MEDIA, [IMAGE_URL])}

        assert (await self._status(client))["status"] == "MEDIA_GENERATION_STATUS_PENDING"

    async def test_a_complaint_is_carried_but_does_not_fail_the_job(self, client):
        """Jobs report "Media not found." and still deliver a finished clip."""
        client.responses[fb.RPC_OPERATION] = self._poll(complaint="Media not found.")
        client.responses[fb.RPC_PROJECT_MEDIA] = self._listing(found=False)

        op = await self._status(client)
        assert op["status"] == "MEDIA_GENERATION_STATUS_PENDING"
        assert op["complaint"] == "Media not found."

    async def test_the_listing_decides_even_when_the_poll_never_says_done(self, client):
        """The poll can sit at no status at all on a job that finished, so the
        listing is consulted on a schedule rather than only on the poll's say-so."""
        client.responses[fb.RPC_OPERATION] = self._poll(status=None)
        client.responses[fb.RPC_PROJECT_MEDIA] = self._listing()
        client.responses[fb.RPC_MEDIA] = {"data": envelope(fb.RPC_MEDIA, [VIDEO_URL])}

        await self._status(client)
        await self._status(client)
        assert (await self._status(client))["status"] == "MEDIA_GENERATION_STATUS_SUCCESSFUL"

    async def test_the_listing_is_asked_for_a_window_not_the_whole_thing(self, client):
        client.responses[fb.RPC_OPERATION] = self._poll(status="CAE")
        client.responses[fb.RPC_PROJECT_MEDIA] = self._listing(found=False)

        await self._status(client)
        listing = next(c for c in client.calls if c["rpcid"] == fb.RPC_PROJECT_MEDIA)
        assert listing["match"] == OPERATION

    async def test_an_unreadable_poll_still_consults_the_listing(self, client):
        """Old operations decay to a bare id but stay in the listing."""
        client.responses[fb.RPC_OPERATION] = {"error": "boom"}
        client.responses[fb.RPC_PROJECT_MEDIA] = self._listing()
        client.responses[fb.RPC_MEDIA] = {"data": envelope(fb.RPC_MEDIA, [VIDEO_URL])}

        assert (await self._status(client))["status"] == "MEDIA_GENERATION_STATUS_SUCCESSFUL"

    async def test_a_quiet_poll_does_not_pay_for_the_listing_every_round(self, client):
        """The listing is a 17 MB call; a poll with nothing to report skips it."""
        client.responses[fb.RPC_OPERATION] = self._poll(status=None)
        client.responses[fb.RPC_PROJECT_MEDIA] = self._listing()
        client.responses[fb.RPC_MEDIA] = {"data": envelope(fb.RPC_MEDIA, [VIDEO_URL])}

        assert (await self._status(client))["status"] == "MEDIA_GENERATION_STATUS_PENDING"
        assert not [c for c in client.calls if c["rpcid"] == fb.RPC_PROJECT_MEDIA]

        await self._status(client)
        assert (await self._status(client))["status"] == "MEDIA_GENERATION_STATUS_SUCCESSFUL"

    async def test_a_known_media_id_is_not_looked_up_again(self, client):
        """Once the listing has answered, later rounds go straight to the media."""
        client.responses[fb.RPC_OPERATION] = self._poll(status="CAE")
        client.responses[fb.RPC_PROJECT_MEDIA] = self._listing()
        client.responses[fb.RPC_MEDIA] = {"data": envelope(fb.RPC_MEDIA, [IMAGE_URL])}

        await self._status(client)          # poster only — still pending
        client.calls.clear()
        await self._status(client)
        assert not [c for c in client.calls if c["rpcid"] == fb.RPC_PROJECT_MEDIA]
        assert not [c for c in client.calls if c["rpcid"] == fb.RPC_OPERATION]

    async def test_a_finished_operation_stays_finished_when_re_polled(self, client):
        """A batch re-polls its finished operations alongside its pending ones."""
        client.responses[fb.RPC_OPERATION] = self._poll(status="CAE")
        client.responses[fb.RPC_PROJECT_MEDIA] = self._listing()
        client.responses[fb.RPC_MEDIA] = {"data": envelope(fb.RPC_MEDIA, [VIDEO_URL])}

        assert (await self._status(client))["status"] == "MEDIA_GENERATION_STATUS_SUCCESSFUL"
        assert (await self._status(client))["status"] == "MEDIA_GENERATION_STATUS_SUCCESSFUL"

    async def test_a_nameless_operation_fails_instead_of_polling_forever(self, client):
        result = await client.check_video_status([{"operation": {}}])
        assert result["data"]["operations"][0]["status"] == "MEDIA_GENERATION_STATUS_FAILED"

    async def test_r2v_poll_uses_get_session_when_the_listing_misses(self, client):
        """Chat-message uuid is not the listing key; GN0Bre holds the clip."""
        session = "8c72f80b-41ff-42f6-9dff-5a759553f9f4"
        scene = "99999999-9999-9999-9999-999999999999"
        client._operation_projects[OPERATION] = PROJECT
        client._operation_chat_sessions[OPERATION] = session
        client._operation_ref_ids[OPERATION] = ("ref-a",)
        client.responses[fb.RPC_OPERATION] = self._poll(complaint="Media not found.")
        client.responses[fb.RPC_PROJECT_MEDIA] = self._listing(found=False)
        client.responses[fb.RPC_CHAT_SESSION] = {
            "data": envelope(fb.RPC_CHAT_SESSION, [
                [MEDIA, PROJECT, scene, "CAE"],
                VIDEO_URL,
            ])
        }

        def media_rpc(match, freq=None):
            inner = json.loads(json.loads(freq)[0][0][1])
            if inner[0] == MEDIA:
                return {"data": envelope(fb.RPC_MEDIA, [VIDEO_URL])}
            return {"error": "as29s failed: [5]"}

        client.responses[fb.RPC_MEDIA] = media_rpc

        op = await self._status(client)
        assert op["status"] == "MEDIA_GENERATION_STATUS_SUCCESSFUL"
        assert op["operation"]["metadata"]["video"]["mediaId"] == MEDIA
        assert "/video/" in op["operation"]["metadata"]["video"]["fifeUrl"]
        assert fb.RPC_CHAT_SESSION in [c["rpcid"] for c in client.calls]

    async def test_r2v_poll_binds_queued_get_session_media_id(self, client):
        """GetSession names the clip while queued; as29s grows /video/ later."""
        session = "8c72f80b-41ff-42f6-9dff-5a759553f9f4"
        client._operation_projects[OPERATION] = PROJECT
        client._operation_chat_sessions[OPERATION] = session
        client._operation_ref_ids[OPERATION] = ("ref-a",)
        client.responses[fb.RPC_OPERATION] = self._poll(complaint="Media not found.")
        client.responses[fb.RPC_PROJECT_MEDIA] = self._listing(found=False)
        client.responses[fb.RPC_CHAT_SESSION] = {
            "data": envelope(fb.RPC_CHAT_SESSION, [
                ["media_id", [None, None, MEDIA]],
                ["status", [None, None, "queued"]],
            ])
        }
        client.responses[fb.RPC_MEDIA] = {"data": envelope(fb.RPC_MEDIA, [])}

        op = await self._status(client)
        assert op["status"] == "MEDIA_GENERATION_STATUS_PENDING"
        assert op["operation"]["metadata"]["video"]["mediaId"] == MEDIA
        assert client._operation_media[OPERATION] == MEDIA

        client.responses[fb.RPC_MEDIA] = {"error": "as29s failed: [5]"}
        op = await self._status(client)
        assert op["status"] == "MEDIA_GENERATION_STATUS_PENDING"
        assert op["operation"]["metadata"]["video"]["mediaId"] == MEDIA

        client.responses[fb.RPC_MEDIA] = {"data": envelope(fb.RPC_MEDIA, [VIDEO_URL])}
        op = await self._status(client)
        assert op["status"] == "MEDIA_GENERATION_STATUS_SUCCESSFUL"
        assert op["operation"]["metadata"]["video"]["mediaId"] == MEDIA
        assert "/video/" in op["operation"]["metadata"]["video"]["fifeUrl"]

    async def test_r2v_poll_reads_a_dict_wrapped_get_session_url(self, client):
        """GetSession stuffs the clip url in a JSON object, not a CAE row."""
        session = "8c72f80b-41ff-42f6-9dff-5a759553f9f4"
        client._operation_projects[OPERATION] = PROJECT
        client._operation_chat_sessions[OPERATION] = session
        client.responses[fb.RPC_OPERATION] = self._poll(complaint="Media not found.")
        client.responses[fb.RPC_PROJECT_MEDIA] = self._listing(found=False)
        escaped = f"https://{fb.MEDIA_HOST}\\/video\\/{MEDIA}?sig=x"
        client.responses[fb.RPC_CHAT_SESSION] = {
            "data": envelope(fb.RPC_CHAT_SESSION, {"clip": escaped})
        }
        client.responses[fb.RPC_MEDIA] = {"data": envelope(fb.RPC_MEDIA, [VIDEO_URL])}

        op = await self._status(client)
        assert op["status"] == "MEDIA_GENERATION_STATUS_SUCCESSFUL"
        assert op["operation"]["metadata"]["video"]["mediaId"] == MEDIA

    async def test_r2v_poll_reads_cae_rows_from_the_full_listing(self, client):
        """GetSession is the chat transcript; the listing row is keyed by media id."""
        session = "8c72f80b-41ff-42f6-9dff-5a759553f9f4"
        client._operation_projects[OPERATION] = PROJECT
        client._operation_chat_sessions[OPERATION] = session
        client._operation_ref_ids[OPERATION] = ("ref-a",)
        client.responses[fb.RPC_OPERATION] = self._poll(complaint="Media not found.")
        client.responses[fb.RPC_CHAT_SESSION] = {
            "data": envelope(fb.RPC_CHAT_SESSION, [session, "still cooking"])
        }
        listing = f'[["{MEDIA}","{PROJECT}","{OPERATION}","CAE"]]'
        client.responses[fb.RPC_PROJECT_MEDIA] = {"data": listing}

        def media_rpc(match, freq=None):
            inner = json.loads(json.loads(freq)[0][0][1])
            if inner[0] == MEDIA:
                return {"data": envelope(fb.RPC_MEDIA, [VIDEO_URL])}
            return {"error": "as29s failed: [5]"}

        client.responses[fb.RPC_MEDIA] = media_rpc

        op = await self._status(client)
        assert op["status"] == "MEDIA_GENERATION_STATUS_SUCCESSFUL"
        assert op["operation"]["metadata"]["video"]["mediaId"] == MEDIA
        listing_call = next(c for c in client.calls if c["rpcid"] == fb.RPC_PROJECT_MEDIA)
        assert listing_call["match"] is None

    async def test_r2v_poll_does_not_steal_an_unrelated_cae_clip(self, client):
        """Finished t2v/i2v rows also look like [mediaId, project, op, CAE]."""
        session = "8c72f80b-41ff-42f6-9dff-5a759553f9f4"
        other_op = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        client._operation_projects[OPERATION] = PROJECT
        client._operation_chat_sessions[OPERATION] = session
        client.responses[fb.RPC_OPERATION] = self._poll(complaint="Media not found.")
        client.responses[fb.RPC_CHAT_SESSION] = {
            "data": envelope(fb.RPC_CHAT_SESSION, [session, "still cooking"])
        }
        listing = f'[["{MEDIA}","{PROJECT}","{other_op}","CAE"]]'
        client.responses[fb.RPC_PROJECT_MEDIA] = {"data": listing}

        def media_rpc(match, freq=None):
            inner = json.loads(json.loads(freq)[0][0][1])
            if inner[0] == MEDIA:
                return {"data": envelope(fb.RPC_MEDIA, [VIDEO_URL])}
            return {"error": "as29s failed: [5]"}

        client.responses[fb.RPC_MEDIA] = media_rpc

        op = await self._status(client)
        assert op["status"] == "MEDIA_GENERATION_STATUS_PENDING"

    async def test_t2v_poll_does_not_ask_get_session(self, client):
        """GetSession on a t2v miss would steal an unrelated Ingredients clip."""
        client.responses[fb.RPC_OPERATION] = self._poll(status="CAE")
        client.responses[fb.RPC_PROJECT_MEDIA] = self._listing()
        client.responses[fb.RPC_MEDIA] = {"data": envelope(fb.RPC_MEDIA, [VIDEO_URL])}
        client.responses[fb.RPC_CHAT_SESSION] = {
            "data": envelope(fb.RPC_CHAT_SESSION, [[MEDIA, PROJECT, "s", "CAE"]])
        }

        assert (await self._status(client))["status"] == "MEDIA_GENERATION_STATUS_SUCCESSFUL"
        assert fb.RPC_CHAT_SESSION not in [c["rpcid"] for c in client.calls]


class TestMediaAndUpload:
    async def test_get_media_reports_the_signed_urls(self, client):
        client.responses[fb.RPC_MEDIA] = {"data": envelope(fb.RPC_MEDIA, [VIDEO_URL, IMAGE_URL])}
        result = await client.get_media(MEDIA)
        assert result["status"] == 200
        assert result["data"]["video"]["fifeUrl"] == VIDEO_URL

    async def test_a_media_id_with_no_urls_reads_as_404(self, client):
        client.responses[fb.RPC_MEDIA] = {"data": envelope(fb.RPC_MEDIA, [])}
        assert (await client.get_media(MEDIA))["status"] == 404

    async def test_validate_media_id_follows_the_status(self, client):
        client.responses[fb.RPC_MEDIA] = {"data": envelope(fb.RPC_MEDIA, [VIDEO_URL])}
        assert await client.validate_media_id(MEDIA) is True
        client.responses[fb.RPC_MEDIA] = {"data": envelope(fb.RPC_MEDIA, [])}
        assert await client.validate_media_id(MEDIA) is False

    async def test_upload_returns_the_media_id_the_callers_look_for(self, client):
        client.responses[fb.RPC_UPLOAD_IMAGE] = {
            "data": envelope(fb.RPC_UPLOAD_IMAGE, [[MEDIA, PROJECT, OPERATION, "CAE"]])
        }
        result = await client.upload_image("Ym9keQ==", project_id=PROJECT)
        assert result["_mediaId"] == MEDIA
        assert result["data"]["media"]["name"] == MEDIA

    async def test_upload_carries_a_captcha_like_a_generate(self, client):
        client.responses[fb.RPC_UPLOAD_IMAGE] = {
            "data": envelope(fb.RPC_UPLOAD_IMAGE, [[MEDIA, PROJECT, OPERATION, "CAE"]])
        }
        await client.upload_image("Ym9keQ==", project_id=PROJECT)
        assert client.calls[0]["captcha"] == fb.CAPTCHA_IMAGE


class TestProjectAndCredits:
    async def test_create_project_hands_back_the_pinned_one(self, client):
        result = await client.create_project("My Film")
        assert result["data"]["projectId"] == PROJECT

    async def test_create_project_without_a_pin_explains_itself(self, client, monkeypatch):
        import agent.services.flow_client as module
        monkeypatch.setattr(module, "FLOW_PROJECT_ID", "")
        result = await client.create_project("My Film")
        assert "NO_FLOW_PROJECT" in result["error"]
        assert "FLOW_PROJECT_ID" in result["error"]

    async def test_credits_answers_the_configured_tier_rather_than_guessing(self, client):
        result = await client.get_credits()
        assert result["data"]["userPaygateTier"]
        assert not client.calls, "there is no credits rpc to call"


class TestRefreshProjectUrls:
    """Re-signing every stored media id — what `/fk-refresh-urls` runs."""

    @pytest.fixture
    def db(self, monkeypatch):
        """A stand-in for the crud layer, recording what got written."""
        from agent.db import crud

        state = {
            "videos": [{"id": "vid-1"}],
            "scenes": [{
                "id": "scene-1",
                "vertical_image_media_id": MEDIA,
                "vertical_video_media_id": "22222222-2222-2222-2222-222222222222",
                "horizontal_image_media_id": "CAMSnot-a-uuid",
            }],
            "characters": [{"id": "char-1", "media_id": "33333333-3333-3333-3333-333333333333"}],
            "writes": [],
        }

        async def list_videos(pid): return state["videos"]
        async def list_scenes(vid): return state["scenes"]
        async def get_project_characters(pid): return state["characters"]
        async def update_scene(sid, **kw): state["writes"].append(("scene", sid, kw))
        async def update_character(cid, **kw): state["writes"].append(("character", cid, kw))

        for name, fn in [("list_videos", list_videos), ("list_scenes", list_scenes),
                         ("get_project_characters", get_project_characters),
                         ("update_scene", update_scene), ("update_character", update_character)]:
            monkeypatch.setattr(crud, name, fn)
        return state

    async def test_writes_a_fresh_url_into_each_field_that_holds_the_id(self, client, db):
        def media(match):
            return {"data": envelope(fb.RPC_MEDIA, [VIDEO_URL, IMAGE_URL])}
        client.responses[fb.RPC_MEDIA] = media

        result = await client.refresh_project_urls(PROJECT)

        assert result["found"] == 3, "the CAMS id is not a media id and is skipped"
        assert result["refreshed"] == 3
        written = {(table, tuple(kw)[0]) for table, _, kw in db["writes"]}
        assert written == {
            ("scene", "vertical_image_url"),
            ("scene", "vertical_video_url"),
            ("character", "reference_image_url"),
        }

    async def test_an_image_field_takes_the_image_url_not_the_video_one(self, client, db):
        client.responses[fb.RPC_MEDIA] = lambda m: {
            "data": envelope(fb.RPC_MEDIA, [VIDEO_URL, IMAGE_URL])}

        await client.refresh_project_urls(PROJECT)
        by_field = {tuple(kw)[0]: tuple(kw.values())[0] for _, _, kw in db["writes"]}
        assert by_field["vertical_image_url"] == IMAGE_URL
        assert by_field["vertical_video_url"] == VIDEO_URL

    async def test_one_dead_media_id_does_not_sink_the_rest(self, client, db):
        seen = []

        def media(match):
            seen.append(1)
            if len(seen) == 1:
                return {"error": "NOT_FOUND"}
            return {"data": envelope(fb.RPC_MEDIA, [VIDEO_URL, IMAGE_URL])}
        client.responses[fb.RPC_MEDIA] = media

        result = await client.refresh_project_urls(PROJECT)
        assert result["found"] == 3 and result["refreshed"] == 2


class TestLowPriorityGuard:
    @pytest.mark.parametrize("response", [
        {"data": ""},
        {"data": json.dumps([["wrb.fr", fb.RPC_PROJECT_SETTINGS, None, None, None, [7]]])},
        {"error": "Failed to fetch"},
    ])
    async def test_settings_must_be_acknowledged_before_streamchat(self, client, response):
        client.responses[fb.RPC_PROJECT_SETTINGS] = response
        stub_create_session(client)
        result = await client.generate_video_from_references([MEDIA], "walk", PROJECT, "s")
        assert result.get("error")
        assert not any(call["rpcid"] in (fb.RPC_STREAM_CHAT, fb.RPC_CREATE_SESSION) for call in client.calls)

    async def test_permission_beats_queued_media_id(self, client):
        client._operation_chat_sessions[OPERATION] = CHAT_SESSION
        client._operation_projects[OPERATION] = PROJECT
        client.responses[fb.RPC_CHAT_SESSION] = {"data": envelope(fb.RPC_CHAT_SESSION,
            {"ask_for_permission": {"media_id": MEDIA}, "generate_video_with_references": {"media_id": MEDIA}})}
        assert await client._media_id_from_chat_session(OPERATION, PROJECT) is None
        assert "LOW_PRIORITY_ONLY" in client._operation_complaints[OPERATION]
        assert not any(call["rpcid"] == fb.RPC_MEDIA for call in client.calls)

    async def test_streamchat_permission_never_becomes_pending_render(self, client):
        stub_create_session(client)
        client.responses[fb.RPC_STREAM_CHAT] = {"data": envelope(fb.RPC_STREAM_CHAT, {"ask_for_permission": True})}
        result = await client.generate_video_from_references([MEDIA], "walk", PROJECT, "s")
        assert "LOW_PRIORITY_ONLY" in result["error"]
        assert result["status"] == 400

    async def test_legacy_paid_model_is_blocked_before_transport(self, client):
        result = await client._send("api_request", {"body": {"requests": [{"videoModelKey": "abra_r2v_8s"}]}})
        assert result["status"] == 400
        assert "LOW_PRIORITY_ONLY" in result["error"]
