"""Veo 4s/6s is a model-key suffix. 8s keeps the captured key."""
import asyncio

import pytest
from fastapi import HTTPException

from agent.services.flow_grpc import (
    FlowGrpcError,
    FlowGrpcTransport,
    _leaf_at,
    coerce_veo_duration,
    decode,
    veo_model_for_duration,
)
from agent.services.flow_proto import fld_msg, fld_str
from agent.sdk.services.operations import _scene_veo_duration

OP = "11111111-1111-4111-8111-111111111111"
MEDIA = "22222222-2222-4222-8222-222222222222"
START = "33333333-3333-4333-8333-333333333333"


def _model_at(buf: bytes, tpl: str) -> str:
    from agent.services import flow_grpc as fg
    raw = _leaf_at(decode(buf), fg._VIDEO_MODEL_FIELD[tpl])
    assert isinstance(raw, bytes)
    return raw.decode()


def _fake_submit() -> bytes:
    return fld_msg(3, fld_str(1, OP) + fld_msg(4, fld_str(5, MEDIA)))


class _Nick:
    def __init__(self, tier="low_priority", models=None, directory=None):
        self.name = "landon"
        self.project_uuid = "44444444-4444-4444-8444-444444444444"
        self.meta = {"video_tier": tier}
        if models:
            self.meta["video_models"] = models
        self.dir = directory
        self.calls = []
        self.mints = 0

    def meta_block(self, project_id, fresh_captcha=False):
        self.mints += 1
        return b"meta-block"

    def call(self, method, buf, timeout=180):
        self.calls.append((method, buf))
        return _fake_submit()


def _transport():
    t = FlowGrpcTransport.__new__(FlowGrpcTransport)
    t._op_retry = {}
    t._op_owner = {}
    t._op_project = {}
    t._op_media = {}
    t._media_owner = {}
    t._op_start_image = {}
    t._op_req = {}
    t._op_started = {}
    t._save_routes = lambda *a, **k: None
    return t


def test_eight_keeps_captured_low_priority_key():
    key = "veo_3_1_t2v_lite_low_priority"
    assert veo_model_for_duration(key, 8, "t2v") == key
    assert veo_model_for_duration("veo_3_1_i2v_lite_low_priority", 8, "i2v") == (
        "veo_3_1_i2v_lite_low_priority"
    )


def test_short_clip_inserts_before_low_priority():
    assert veo_model_for_duration(
        "veo_3_1_t2v_lite_low_priority", 4, "t2v"
    ) == "veo_3_1_t2v_lite_4s_low_priority"
    assert veo_model_for_duration(
        "veo_3_1_t2v_lite_low_priority", 6, "t2v"
    ) == "veo_3_1_t2v_lite_6s_low_priority"


def test_free_tier_appends_duration():
    assert veo_model_for_duration("veo_3_1_t2v_lite", 4, "t2v") == (
        "veo_3_1_t2v_lite_4s"
    )


def test_i2v_low_keeps_low_priority_after_s_lite():
    # Phone 2026-10-22: BatchAsyncGenerateVideoStartImage HTTP 200 for
    # veo_3_1_i2v_s_lite_4s_low_priority and _6s_low_priority.
    # veo_3_1_i2v_lite_4s_low_priority (no _s_) was NOT_FOUND.
    assert veo_model_for_duration(
        "veo_3_1_i2v_lite_low_priority", 4, "i2v"
    ) == "veo_3_1_i2v_s_lite_4s_low_priority"
    assert veo_model_for_duration(
        "veo_3_1_i2v_lite_low_priority", 6, "i2v"
    ) == "veo_3_1_i2v_s_lite_6s_low_priority"


def test_i2v_free_tier_omits_low_priority():
    # Measured MP4s: veo_3_1_i2v_s_lite_4s = 4.000s, _6s = 6.000s.
    assert veo_model_for_duration(
        "veo_3_1_i2v_lite", 4, "i2v"
    ) == "veo_3_1_i2v_s_lite_4s"
    assert veo_model_for_duration(
        "veo_3_1_i2v_lite", 6, "i2v"
    ) == "veo_3_1_i2v_s_lite_6s"


def test_durationed_key_is_left_alone():
    key = "veo_3_1_t2v_fast_4s_relaxed"
    assert veo_model_for_duration(key, 6, "t2v") == key


def test_rejects_unsupported_lengths_and_short_r2v():
    with pytest.raises(FlowGrpcError, match="4, 6, or 8"):
        coerce_veo_duration(10, "t2v")
    with pytest.raises(FlowGrpcError, match="4, 6, or 8"):
        coerce_veo_duration(5, "i2v")
    with pytest.raises(FlowGrpcError, match="8 seconds only"):
        coerce_veo_duration(4, "r2v")
    assert coerce_veo_duration(8, "r2v") == 8
    assert veo_model_for_duration(
        "veo_3_1_r2v_lite_low_priority", 8, "r2v"
    ) == "veo_3_1_r2v_lite_low_priority"


def test_scene_duration_coerces_to_veo_lengths():
    assert _scene_veo_duration({}) == 8
    assert _scene_veo_duration({"duration": 4}) == 4
    assert _scene_veo_duration({"duration": 6.2}) == 6
    assert _scene_veo_duration({"duration": 10}) == 8
    assert _scene_veo_duration({"duration": "nope"}) == 8


def test_submit_writes_duration_into_template_model_field():
    t = _transport()
    nick = _Nick()
    for dur, expected in (
        (8, "veo_3_1_t2v_lite_low_priority"),
        (4, "veo_3_1_t2v_lite_4s_low_priority"),
        (6, "veo_3_1_t2v_lite_6s_low_priority"),
    ):
        nick.calls.clear()
        t._gen_video_sync(nick, "t2v", "a paper boat on calm water", None,
                          aspect_ratio="VIDEO_ASPECT_RATIO_PORTRAIT",
                          duration_s=dur)
        assert _model_at(nick.calls[-1][1], "t2v") == expected
        assert b"a paper boat on calm water" in nick.calls[-1][1]
    spec = t._op_retry[OP]
    assert spec["duration_s"] == 6
    assert spec["kind"] == "t2v"


def test_i2v_keeps_start_frame_when_duration_changes():
    t = _transport()
    nick = _Nick()
    t._gen_video_sync(
        nick, "i2v", "the boat drifts", None,
        aspect_ratio="VIDEO_ASPECT_RATIO_PORTRAIT",
        duration_s=4, start_uuid=START)
    buf = nick.calls[-1][1]
    assert _model_at(buf, "i2v") == "veo_3_1_i2v_s_lite_4s_low_priority"
    assert START.encode() in buf
    assert t._op_retry[OP]["duration_s"] == 4
    assert t._op_retry[OP]["start_uuid"] == START


def test_short_r2v_never_mints_or_submits():
    t = _transport()
    nick = _Nick()
    with pytest.raises(FlowGrpcError, match="8 seconds only"):
        t._gen_video_sync(nick, "r2v", "ingredients", None, duration_s=4,
                          ref_uuids=[START])
    assert nick.mints == 0
    assert nick.calls == []


def test_short_clip_permission_does_not_flip_tier(tmp_path):
    import grpc
    t = _transport()
    nick = _Nick(directory=tmp_path)
    nick.meta_path_written = False

    def deny(method, buf, timeout=180):
        nick.calls.append((method, buf))
        raise FlowGrpcError("permission denied",
                            code=grpc.StatusCode.PERMISSION_DENIED)

    nick.call = deny
    with pytest.raises(FlowGrpcError):
        t._gen_video_sync(nick, "t2v", "boat", None, duration_s=4)
    assert len(nick.calls) == 1
    assert nick.meta["video_tier"] == "low_priority"
    assert not (tmp_path / "meta.json").exists()


def test_eight_second_permission_still_tries_other_tier(tmp_path):
    import grpc
    t = _transport()
    nick = _Nick(directory=tmp_path)
    seen = []

    def call(method, buf, timeout=180):
        seen.append(_model_at(buf, "t2v"))
        if len(seen) == 1:
            raise FlowGrpcError("permission denied",
                                code=grpc.StatusCode.PERMISSION_DENIED)
        return _fake_submit()

    nick.call = call
    out = t._gen_video_sync(nick, "t2v", "boat", None, duration_s=8)
    assert out["status"] == 200
    assert seen == [
        "veo_3_1_t2v_lite_low_priority",
        "veo_3_1_t2v_lite",
    ]
    assert nick.meta["video_tier"] == "free"


def test_facade_rejects_bad_duration_before_acquire():
    t = _transport()
    bad = asyncio.run(t.generate_video(prompt="boat", duration_s=10))
    assert bad["status"] == 400
    assert bad["retryable"] is False
    r2v = asyncio.run(t.generate_video_from_references(
        [START], "boat", duration_s=6))
    assert r2v["status"] == 400
    assert "8 seconds only" in r2v["error"]


def test_api_forwards_short_duration_and_rejects_the_rest(monkeypatch):
    from agent.api.flow import (
        GenerateVideoRefsRequest,
        GenerateVideoRequest,
        generate_video,
        generate_video_refs,
    )

    seen = []

    class Client:
        connected = True

        async def generate_video(self, **kw):
            seen.append(("video", kw.get("duration_s"), kw.get("start_image_media_id")))
            return {"status": 200, "data": {"operations": []}}

        async def generate_video_from_references(self, **kw):
            seen.append(("refs", kw.get("duration_s")))
            return {"status": 200, "data": {"operations": []}}

    monkeypatch.setattr("agent.api.flow.get_flow_client", lambda: Client())

    asyncio.run(generate_video(GenerateVideoRequest(prompt="boat", duration_s=4)))
    assert seen[-1] == ("video", 4, None)

    with pytest.raises(HTTPException) as ei:
        asyncio.run(generate_video(GenerateVideoRequest(prompt="boat", duration_s=10)))
    assert ei.value.status_code == 400

    asyncio.run(generate_video_refs(GenerateVideoRefsRequest(
        reference_media_ids=[START],
        prompt="boat <IMAGE_1>",
        project_id="p",
        scene_id="s",
        duration_s=6,
    )))
    assert seen[-1][0] == "video"
    assert seen[-1][1] == 6
    assert seen[-1][2] == START

    with pytest.raises(HTTPException) as er:
        asyncio.run(generate_video_refs(GenerateVideoRefsRequest(
            reference_media_ids=[START, OP],
            prompt="two refs",
            project_id="p",
            scene_id="s",
            duration_s=4,
        )))
    assert er.value.status_code == 400


def test_grpc_client_threads_duration_and_batch_refuses_short(monkeypatch):
    from agent.services.flow_client import FlowClient, _batch_veo_duration_error

    assert _batch_veo_duration_error(8) is None
    assert _batch_veo_duration_error(4)["status"] == 400

    captured = {}

    class Grpc:
        async def generate_video(self, **kw):
            captured.update(kw)
            return {"status": 200, "data": {}}

    monkeypatch.setattr("agent.services.flow_client._grpc_active", lambda: True)
    monkeypatch.setattr("agent.services.flow_client._grpc", lambda: Grpc())
    client = FlowClient.__new__(FlowClient)
    asyncio.run(client.generate_video(prompt="boat", duration_s=6))
    assert captured["duration_s"] == 6

    monkeypatch.setattr("agent.services.flow_client._grpc_active", lambda: False)
    refused = asyncio.run(client.generate_video(prompt="boat", duration_s=4))
    assert refused["status"] == 400
    assert refused["error_code"] == "invalid_argument"
