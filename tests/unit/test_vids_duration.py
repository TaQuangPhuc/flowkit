"""Unit tests for Google Vids / Omni 10s default duration and request normalization."""
import json

from agent.api.vids import VidsGenerateReq
from agent.services import vids_client


def test_vids_generate_req_duration_aliases():
    assert VidsGenerateReq(nick="vids01", prompt="cat").seconds is None
    assert VidsGenerateReq(nick="vids01", prompt="cat", seconds=10).seconds == 10
    assert VidsGenerateReq.model_validate(
        {"nick": "vids01", "prompt": "cat", "duration": 10}
    ).seconds == 10
    assert VidsGenerateReq.model_validate(
        {"nick": "vids01", "prompt": "cat", "duration": "10s"}
    ).seconds == 10
    assert VidsGenerateReq.model_validate(
        {"nick": "vids01", "prompt": "cat", "duration_s": 10}
    ).seconds == 10
    assert VidsGenerateReq.model_validate(
        {"nick": "vids01", "prompt": "cat", "durationSeconds": "10"}
    ).seconds == 10
    assert VidsGenerateReq.model_validate(
        {"nick": "vids01", "prompt": "cat", "duration_seconds": 10}
    ).seconds == 10


def test_vids_generate_req_prompt_10s_override():
    # JSON storyboard prompt specifying duration_seconds: 10 while outer wrapper sent seconds: 8
    storyboard_prompt = json.dumps({
        "scene_id": "Scene_01",
        "model": "Gemini Omni Flash",
        "duration_seconds": 10,
        "aspect_ratio": "9:16",
    })
    req = VidsGenerateReq.model_validate({
        "nick": "vids01",
        "prompt": storyboard_prompt,
        "seconds": 8,
        "mode": "r2v",
    })
    assert req.seconds == 10

    # Natural language 10s prompt without seconds field
    req2 = VidsGenerateReq.model_validate({
        "nick": "vids01",
        "prompt": "tạo con chó 10s",
        "mode": "t2v",
    })
    assert req2.seconds == 10


def test_build_body_default_duration_is_10s():
    raw = vids_client.build_body("t2v", "doc123", "a cat walking", duration=0)
    body = json.loads(raw)
    assert body[4][15][8] == 10


def test_generate_flow_abra_defaults_to_10s(monkeypatch):
    from agent.services import flow_grpc as fg
    from agent.services.flow_proto import fld_msg, fld_str

    op_id = "11111111-1111-4111-8111-111111111111"
    media_id = "22222222-2222-4222-8222-222222222222"
    seen_models = []

    class FakeNick:
        name = "vids01"
        project_uuid = "44444444-4444-4444-8444-444444444444"

        def meta_block(self, project_id, fresh_captcha=False):
            return b"meta-block"

        def call(self, method, buf, timeout=180):
            if method in (fg.M_T2V, fg.M_I2V, fg.M_R2V):
                tpl = {fg.M_T2V: "t2v", fg.M_I2V: "i2v", fg.M_R2V: "r2v"}[method]
                raw = fg._leaf_at(fg.decode(buf), fg._VIDEO_MODEL_FIELD[tpl])
                seen_models.append(raw.decode())
                return fld_msg(3, fld_str(1, op_id) + fld_msg(4, fld_str(5, media_id)))
            raise AssertionError(f"unexpected method {method}")

    class FakeTransport:
        _nicks = {"vids01": FakeNick()}
        _parked = set()
        _op_owner = {}
        _op_project = {}
        _op_media = {}
        _media_owner = {}
        _op_started = {}

        def reload(self):
            pass

        def _save_routes(self):
            pass

    monkeypatch.setattr(fg, "get_flow_grpc", lambda: FakeTransport())
    monkeypatch.setattr(
        fg,
        "_read_media_urls",
        lambda resp: {media_id: {"video": "https://example.com/out.mp4"}},
    )
    monkeypatch.setattr(fg, "_read_media_missing", lambda resp: set())
    FakeNick.call = lambda self, method, buf, timeout=180: (
        seen_models.append(
            fg._leaf_at(
                fg.decode(buf),
                fg._VIDEO_MODEL_FIELD[{fg.M_T2V: "t2v", fg.M_I2V: "i2v", fg.M_R2V: "r2v"}[method]],
            ).decode()
        ) or fld_msg(3, fld_str(1, op_id) + fld_msg(4, fld_str(5, media_id)))
        if method in (fg.M_T2V, fg.M_I2V, fg.M_R2V)
        else b"media"
    )
    monkeypatch.setattr(
        vids_client,
        "_download_flow_abra_video",
        lambda t, fk_nick, mid, url, resolution=720: f"{mid}.mp4",
    )
    monkeypatch.setattr(vids_client.time, "sleep", lambda s: None)

    out = vids_client.generate_flow_abra("vids01", "tạo con mèo", mode="t2v", duration=0)
    assert out["model"] == "abra_t2v_10s"
    assert seen_models[-1] == "abra_t2v_10s"

    out8 = vids_client.generate_flow_abra("vids01", "tạo con mèo", mode="t2v", duration=8)
    assert out8["model"] == "abra_t2v_8s"
    assert seen_models[-1] == "abra_t2v_8s"

    out3 = vids_client.generate_flow_abra(
        "vids01", "tạo con mèo", mode="t2v", duration=3, model="omni_1_2_flash"
    )
    assert out3["model"] == "macaron_cookie_t2v_3s"
    assert seen_models[-1] == "macaron_cookie_t2v_3s"

    out15 = vids_client.generate_flow_abra(
        "vids01", "tạo con mèo", mode="t2v", duration=15, model="omni_1_2_flash"
    )
    assert out15["model"] == "macaron_cookie_t2v_15s"
    assert seen_models[-1] == "macaron_cookie_t2v_15s"

    out_abra = vids_client.generate_flow_abra("vids01", "tạo con mèo", mode="t2v", duration=8, model="abra")
    assert out_abra["model"] == "abra_t2v_8s"
    assert seen_models[-1] == "abra_t2v_8s"
