"""BatchAsyncGenerateVideoUpsampleVideo port (friend capture, 2026-09-29)."""
import asyncio
from pathlib import Path

import pytest

from agent.services.flow_grpc import (
    FlowGrpcTransport, M_UPSAMPLE, M_GET_MEDIA, _build_upscale,
    _read_media_missing, _read_media_urls, _read_operation,
)
from agent.services.flow_proto import decode, fld_msg, fld_str

TPL = Path(__file__).resolve().parents[2] / "nicks" / "_tpl" / "upscale.bin"
RES = (Path(__file__).resolve().parents[2] / ".omc" / "research"
       / "upscale_res.bin")

SRC = "c08f4018-f6ed-436b-9989-32ffcf7be9a2"
OP = "3942b7a9-cd31-4568-aab5-7f6083bdd845"
META = fld_str(1, "meta-block")


def _leaf(fields, path):
    for num in path:
        v = next((v for f, w, v in fields if f == num and w == 2), None)
        assert v is not None, f"missing f{num}"
        fields = decode(v) if path.index(num) != len(path) - 1 else fields
    return v


def test_build_upscale_patches_template():
    buf = _build_upscale(SRC, META)
    top = decode(buf)
    req = next(v for f, w, v in top if f == 1 and w == 2)
    req = decode(req)
    inner = next(v for f, w, v in req if f == 1 and w == 2)
    assert next(v for f, w, v in decode(inner)
                if f == 2 and w == 2) == SRC.encode()
    opf = next(v for f, w, v in req if f == 5 and w == 2)
    op_uuid = next(v for f, w, v in decode(opf)
                   if f == 2 and w == 2).decode()
    assert op_uuid != OP and len(op_uuid) == 36  # fresh uuid per call
    assert next(v for f, w, v in req if f == 32 and w == 2
                ) == b"veo_3_1_upsampler_1080p"
    assert next(v for f, w, v in top if f == 2 and w == 2) == META


def test_build_upscale_model_override():
    buf = _build_upscale(SRC, META, model="veo_3_1_upsampler_4k")
    req = decode(next(v for f, w, v in decode(buf) if f == 1 and w == 2))
    assert next(v for f, w, v in req if f == 32 and w == 2
                ) == b"veo_3_1_upsampler_4k"


def _f3(req):
    return next(v for f, w, v in req if f == 3 and w == 0)


def test_build_upscale_portrait_patches_aspect():
    # Template was captured from a landscape source (f3=2) — a 9:16 request
    # must flip it to the portrait enum or the clip comes back stretched.
    buf = _build_upscale(SRC, META,
                         aspect_ratio="VIDEO_ASPECT_RATIO_PORTRAIT")
    req = decode(next(v for f, w, v in decode(buf) if f == 1 and w == 2))
    assert _f3(req) == 1


def test_build_upscale_landscape_keeps_aspect():
    buf = _build_upscale(SRC, META,
                         aspect_ratio="VIDEO_ASPECT_RATIO_LANDSCAPE")
    req = decode(next(v for f, w, v in decode(buf) if f == 1 and w == 2))
    assert _f3(req) == 2


def test_build_upscale_no_aspect_keeps_template():
    buf = _build_upscale(SRC, META)
    req = decode(next(v for f, w, v in decode(buf) if f == 1 and w == 2))
    assert _f3(req) == 2


@pytest.mark.skipif(not RES.is_file(), reason="capture fixture missing")
def test_read_operation_on_real_response():
    op = _read_operation(RES.read_bytes())
    assert op["op_id"] == OP


def test_read_media_urls_keeps_upsampled_id():
    mid = f"{SRC}_upsampled"
    url = f"https://flow-content.google/video/{mid}"
    resp = fld_msg(1, fld_str(1, mid) + fld_str(11, url))
    urls = _read_media_urls(resp)
    assert urls[mid]["video"] == url


def test_read_media_missing_keeps_upsampled_suffix():
    mid = f"{SRC}_upsampled"
    err = fld_msg(1, fld_msg(2, b"\x08\x05" +
                             f"Media not found: {mid}".encode()
                             .join([b"\x12", b""])))
    # simpler: build error string leaf directly
    from agent.services.flow_proto import fld_str as fs
    resp = fld_msg(1, fld_msg(2, b"\x08\x05" + fs(2, f"Media not found: {mid}")))
    assert _read_media_missing(resp) == {mid}


class _Nick:
    name = "friend"
    project_uuid = "52ca95cb-5562-409a-ac27-756e574d0ae0"

    def meta_block(self, project_id=None, fresh_captcha=False, **kw):
        return META

    def call(self, method, payload, timeout=90):
        assert method == M_UPSAMPLE
        # Captured upsample response shape: f4.f1 output media id,
        # f4.f2 project uuid, f4.f3 operation uuid.
        return fld_msg(4, fld_str(1, f"{SRC}_upsampled")
                       + fld_str(2, self.project_uuid)
                       + fld_str(3, OP))


def test_upscale_sync_routes_upsampled_media():
    t = FlowGrpcTransport.__new__(FlowGrpcTransport)
    t._op_owner = {}
    t._op_project = {}
    t._op_media = {}
    t._media_owner = {}
    t._op_started = {}
    t._op_retry = {}
    t._op_req = {}
    t._save_routes = lambda: None
    r = t._upscale_sync(_Nick(), SRC, None, "veo_3_1_upsampler_1080p")
    ops = r["data"]["operations"]
    assert ops[0]["operation"]["name"] == OP
    assert ops[0]["status"] == "MEDIA_GENERATION_STATUS_PENDING"
    assert t._op_media[OP] == f"{SRC}_upsampled"
    assert t._media_owner[f"{SRC}_upsampled"] == "friend"
