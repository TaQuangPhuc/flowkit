"""i2v submit ids and poll target match the iOS app (friend, 2026-09-29)."""
from agent.services.flow_grpc import (
    FlowGrpcTransport, M_GET_MEDIA, _read_operation,
)
from agent.services.flow_proto import fld_msg, fld_str


OP = "9b51fb0f-52b3-4f9d-90e2-e18a003b2dd3"
MEDIA = "f2efb500-13a2-4247-84e1-79089952e7c6"
SIDE = "5bd34be6-ddc4-4481-a7f4-2b1c057770e9"
PROJECT = "52ca95cb-5562-409a-ac27-756e574d0ae0"
START = "aa564722-7bd3-4817-8cfd-5c09279290c5"


def _submit_response() -> bytes:
    inner = fld_str(1, "Đổ hũ vào cơm") + fld_str(5, MEDIA) + fld_str(6, SIDE)
    return fld_msg(3, fld_str(1, OP) + fld_msg(4, inner) + fld_str(5, PROJECT))


def _media_response(media_id: str, kind: str) -> bytes:
    url = f"https://flow-content.google/{kind}/{media_id}"
    return fld_msg(1, fld_str(1, media_id) + fld_str(11, url))


class _Nick:
    name = "friend"
    project_uuid = PROJECT

    def __init__(self, videos=()):
        self.videos = set(videos)
        self.calls = []

    def call(self, method, payload, timeout=45):
        self.calls.append(method)
        if method == M_GET_MEDIA:
            # payload is fld_str(1, uuid)
            text = payload.decode("utf-8", "replace")
            for mid in (MEDIA, SIDE, OP, START):
                if mid in text:
                    kind = "video" if mid in self.videos else "image"
                    if mid == SIDE and mid not in self.videos:
                        return _media_response(mid, "image")
                    if mid in self.videos or mid == MEDIA:
                        return _media_response(mid, kind if mid in self.videos else "image")
            return b""
        raise AssertionError(method)


def _transport():
    t = FlowGrpcTransport.__new__(FlowGrpcTransport)
    t._op_media = {}
    t._op_start_image = {}
    t._op_started = {}
    t._op_project = {}
    t._media_owner = {}
    t._poll_cache = {}
    t._op_owner = {}
    t._op_missing_since = {}
    t._op_alias = {}
    t._op_retry = {}
    t._op_req = {}
    t._op_checked = {}
    return t


def test_read_operation_uses_f5_as_media_not_f6():
    got = _read_operation(_submit_response())
    assert got == {"op_id": OP, "media_id": MEDIA, "project_id": PROJECT}


def test_poll_waits_while_output_id_is_poster_only():
    t = _transport()
    t._op_media[OP] = MEDIA
    t._op_start_image[OP] = START
    nick = _Nick()
    entry = t._poll_op_sync(nick, OP)
    assert entry["status"] == "MEDIA_GENERATION_STATUS_PENDING"
    assert nick.calls[0] == M_GET_MEDIA


def test_poll_succeeds_when_same_media_id_gains_video():
    t = _transport()
    t._op_media[OP] = MEDIA
    t._op_start_image[OP] = START
    nick = _Nick(videos={MEDIA})
    entry = t._poll_op_sync(nick, OP)
    assert entry["status"] == "MEDIA_GENERATION_STATUS_SUCCESSFUL"
    assert "/video/" in entry["operation"]["metadata"]["video"]["fifeUrl"]
    assert entry["operation"]["metadata"]["video"]["mediaId"] == MEDIA


def test_inflight_name_is_the_media_id_still_polled():
    """Jobs submitted before the parser fix returned f5 as the op name."""
    t = _transport()
    t._op_media[MEDIA] = SIDE
    t._op_start_image[MEDIA] = START
    nick = _Nick(videos={MEDIA})
    entry = t._poll_op_sync(nick, MEDIA)
    assert entry["status"] == "MEDIA_GENERATION_STATUS_SUCCESSFUL"
    assert all(c == M_GET_MEDIA for c in nick.calls)


def _live_media_block(media_id: str) -> bytes:
    # Shape of a live BatchGetMedia block: f1 media, f2 project, f3 op,
    # then a nested record carrying the poster and clip urls.
    urls = (fld_str(11, f"https://flow-content.google/image/{media_id}")
            + fld_msg(8, fld_str(9, f"https://flow-content.google/video/{media_id}")))
    return fld_msg(1, fld_str(1, media_id) + fld_str(2, PROJECT)
                   + fld_str(3, OP) + fld_msg(6, urls))


def test_media_uuid_that_parses_as_protobuf_keeps_its_key():
    """29726444-… decodes as {f6: '726444-…'}; the url used to be filed
    under the project uuid and the finished clip polled PENDING forever."""
    from agent.services.flow_grpc import _read_media_urls
    for mid in ("29726444-689c-4924-a029-57bd15e971e0",
                "97894fda-b445-4087-a027-6954a72c4715", MEDIA):
        got = _read_media_urls(_live_media_block(mid))
        assert set(got) == {mid}, got
        assert got[mid]["video"].endswith(f"/video/{mid}")


def test_poll_succeeds_for_protobuf_looking_media_uuid():
    mid = "29726444-689c-4924-a029-57bd15e971e0"
    t = _transport()
    t._op_media[OP] = mid
    nick = _Nick()
    nick.call = lambda method, payload, timeout=45: (
        _live_media_block(mid) if mid.encode() in payload else b"")
    entry = t._poll_op_sync(nick, OP)
    assert entry["status"] == "MEDIA_GENERATION_STATUS_SUCCESSFUL"
    assert entry["operation"]["metadata"]["video"]["mediaId"] == mid


def _not_found(mid: str) -> bytes:
    # Live shape: f1{ f2{ f1: 5 (NOT_FOUND), f2: 'Media not found: <id>' } }
    return fld_msg(1, fld_msg(2, bytes([0x08, 5]) + fld_str(2, f"Media not found: {mid}")))


class _GoneNick(_Nick):
    def call(self, method, payload, timeout=45):
        self.calls.append(method)
        return _not_found(payload.decode("utf-8", "replace")[-36:])


def _removed_setup(monkeypatch, owner="friend", age=30):
    import agent.services.flow_grpc as fg
    clock = [1_000_000.0]
    monkeypatch.setattr(fg.time, "time", lambda: clock[0])
    monkeypatch.setattr(fg.flow_stats, "bump", lambda *a, **k: None)
    t = _transport()
    t._op_media[OP] = MEDIA
    t._op_started[OP] = clock[0] - age
    if owner:
        t._op_owner[OP] = owner
    return t, _GoneNick(), clock


def test_read_media_missing_parses_not_found_block():
    from agent.services.flow_grpc import _read_media_missing
    assert _read_media_missing(_not_found(MEDIA)) == {MEDIA}
    assert _read_media_missing(_live_media_block(MEDIA)) == set()


def test_output_removed_by_google_fails_after_confirmation(monkeypatch):
    t, nick, clock = _removed_setup(monkeypatch)
    assert t._poll_op_sync(nick, OP)["status"] == "MEDIA_GENERATION_STATUS_PENDING"
    clock[0] += 8
    assert t._poll_op_sync(nick, OP)["status"] == "MEDIA_GENERATION_STATUS_PENDING"
    clock[0] += 10  # >= 15s since the first not-found
    entry = t._poll_op_sync(nick, OP)
    assert entry["status"] == "MEDIA_GENERATION_STATUS_FAILED"
    assert entry["error_code"] == "upstream_rejected"
    assert "retryable" not in entry
    n = len(nick.calls)
    clock[0] += 60
    assert t._poll_op_sync(nick, OP)["status"] == "MEDIA_GENERATION_STATUS_FAILED"
    assert len(nick.calls) == n  # terminal answer is cached


def test_unknown_owner_never_fails_on_not_found(monkeypatch):
    # Polling the wrong nick also answers not-found; stay PENDING.
    t, nick, clock = _removed_setup(monkeypatch, owner=None)
    for _ in range(5):
        clock[0] += 20
        assert t._poll_op_sync(nick, OP)["status"] == "MEDIA_GENERATION_STATUS_PENDING"


def test_very_young_op_is_not_failed(monkeypatch):
    t, nick, clock = _removed_setup(monkeypatch, age=0)
    t._poll_op_sync(nick, OP)
    clock[0] += 16
    # first not-found at t=0 (< 20s old) does not start the clock
    assert t._poll_op_sync(nick, OP)["status"] == "MEDIA_GENERATION_STATUS_PENDING"


def test_media_reappearing_resets_the_clock(monkeypatch):
    t, nick, clock = _removed_setup(monkeypatch)
    t._poll_op_sync(nick, OP)
    clock[0] += 8
    back = _Nick()  # returns the poster block for MEDIA
    t._poll_op_sync(back, OP)
    assert OP not in t._op_missing_since
    clock[0] += 10
    assert t._poll_op_sync(nick, OP)["status"] == "MEDIA_GENERATION_STATUS_PENDING"


def test_repeat_poll_of_finished_op_is_served_from_cache():
    t = _transport()
    t._op_media[OP] = MEDIA
    nick = _Nick(videos={MEDIA})
    first = t._poll_op_sync(nick, OP)
    n = len(nick.calls)
    for _ in range(5):
        again = t._poll_op_sync(nick, OP)
    assert len(nick.calls) == n
    assert again == first
    again["status"] = "mutated"
    assert t._poll_op_sync(nick, OP)["status"] == first["status"]


def test_stuck_op_is_polled_less_often(monkeypatch):
    import agent.services.flow_grpc as fg
    clock = [1_000_000.0]
    monkeypatch.setattr(fg.time, "time", lambda: clock[0])
    t = _transport()
    t._op_media[OP] = MEDIA
    t._op_started[OP] = clock[0] - fg.VIDEO_STUCK_TIMEOUT - 10
    nick = _Nick()
    t._poll_op_sync(nick, OP)
    n = len(nick.calls)
    clock[0] += 20  # Nova polls every few seconds
    t._poll_op_sync(nick, OP)
    assert len(nick.calls) == n
    clock[0] += 15
    t._poll_op_sync(nick, OP)
    assert len(nick.calls) > n


def test_fresh_op_poll_reaches_google_after_short_ttl(monkeypatch):
    import agent.services.flow_grpc as fg
    clock = [1_000_000.0]
    monkeypatch.setattr(fg.time, "time", lambda: clock[0])
    t = _transport()
    t._op_media[OP] = MEDIA
    t._op_started[OP] = clock[0] - 10
    nick = _Nick()
    t._poll_op_sync(nick, OP)
    n = len(nick.calls)
    clock[0] += 4
    t._poll_op_sync(nick, OP)
    assert len(nick.calls) > n
