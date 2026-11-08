import asyncio
import base64
from collections import Counter
from types import SimpleNamespace

import agent.services.flow_grpc as fg
from agent.services.flow_grpc import _build_upload, _ensure_jpeg
from agent.services.flow_proto import decode, encode


def test_ensure_jpeg_passthrough():
    jpeg = b"\xff\xd8\xff" + b"x" * 20
    assert _ensure_jpeg(jpeg) is jpeg or _ensure_jpeg(jpeg)[:3] == b"\xff\xd8\xff"


def test_upload_payload_is_meta_plus_raw_jpeg():
    meta = encode([
        (1, 2, b"mobile;prod;abc"),
        (2, 0, 22),
        (6, 2, b"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
        (11, 2, encode([(1, 2, b"0cAFcWeTOKEN"), (2, 0, 3)])),
    ])
    jpeg = b"\xff\xd8\xff" + b"y" * 50
    buf = _build_upload(meta, jpeg)
    fields = decode(buf)
    assert fields[0][0] == 1
    assert fields[1][0] == 2
    assert fields[1][2][:3] == b"\xff\xd8\xff"
    inner = decode(fields[0][2])
    nums = [f for f, _, _ in inner]
    assert nums == [1, 2, 6, 11]


def test_upload_payload_carries_mime_after_image():
    # Google rejects UploadImage with INVALID_ARGUMENT without top-level f3.
    fields = decode(_build_upload(b"", b"\xff\xd8\xff" + b"z" * 10))
    assert [f for f, _, _ in fields] == [1, 2, 3]
    assert fields[2][2] == b"image/jpeg"


def _fleet(monkeypatch, names):
    monkeypatch.setattr(fg, "PACE_MIN_S", 0.0)
    monkeypatch.setattr(fg, "PACE_MAX_S", 0.0)
    monkeypatch.setattr(fg, "_trace_emit", lambda *a, **k: None)
    t = fg.FlowGrpcTransport.__new__(fg.FlowGrpcTransport)
    t._nicks = {n: SimpleNamespace(name=n) for n in names}
    t._order = list(names)
    t._rr_idx = 0
    t._inflight, t._next_slot, t._pace_locks = {}, {}, {}
    t._waiters, t._throttle_until, t._sched_lock = 0, {}, None
    t._quota_dead_until = {}
    t._flag_mtimes = {}
    t._no_cooldown = set()
    t._media_owner = {}
    t._media_seen = {}
    landed = []

    def fake_upload(nick, raw, project_id, captcha_force=False):
        landed.append(nick.name)
        mid = f"00000000-0000-0000-0000-{len(landed):012d}"
        t._media_owner[mid] = nick.name
        return {"status": 200, "_mediaId": mid}

    t._upload_sync = fake_upload
    return t, landed


def test_uploads_spread_across_nicks(monkeypatch):
    names = ["friend", "gemma", "kailyn", "landon", "madyson"]
    t, landed = _fleet(monkeypatch, names)
    img = base64.b64encode(b"\xff\xd8\xff" + b"q" * 8).decode()

    async def go():
        for _ in range(10):
            await t.upload_image(img)

    asyncio.run(go())
    assert Counter(landed) == {n: 2 for n in names}


def test_anchored_upload_stays_on_anchor_owner(monkeypatch):
    t, landed = _fleet(monkeypatch, ["friend", "gemma", "kailyn"])
    t._media_owner["11111111-1111-1111-1111-111111111111"] = "kailyn"
    img = base64.b64encode(b"\xff\xd8\xff" + b"q" * 8).decode()

    async def go():
        for _ in range(3):
            await t.upload_image(
                img,
                reference_media_id="11111111-1111-1111-1111-111111111111")

    asyncio.run(go())
    assert landed == ["kailyn"] * 3
