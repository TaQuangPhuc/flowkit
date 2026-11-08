"""Submit-time error classification for the gRPC facade and the NOVA API."""
import asyncio
import json
import time
from types import SimpleNamespace

import pytest

import agent.services.flow_grpc as fg
from agent.api.flow import _respond_flow_result


@pytest.fixture
def transport(monkeypatch, tmp_path):
    monkeypatch.setattr(fg, "_trace_emit", lambda *a, **k: None)
    monkeypatch.setattr(fg.flow_stats, "bump", lambda *a, **k: None)
    monkeypatch.setattr(fg, "QUOTA_FILE", tmp_path / "_quota_dead.json")
    monkeypatch.setattr(fg, "PARK_FILE", tmp_path / "_parked.json")
    t = fg.FlowGrpcTransport.__new__(fg.FlowGrpcTransport)
    t._throttle_until = {}
    t._throttle_streak = {}
    t._quota_dead_until = {}
    t._quota_streak = {}
    t._no_cooldown = set()
    t._parked = set()
    return t


NICK = SimpleNamespace(name="friend", meta={})


@pytest.mark.parametrize("detail", [
    "StatusCode.INVALID_ARGUMENT: PUBLIC_ERROR_UNSAFE_GENERATION",
    "StatusCode.INVALID_ARGUMENT: Request violates our content policies",
    "StatusCode.FAILED_PRECONDITION: PUBLIC_ERROR_PROMINENT_PEOPLE_FILTER_FAILED",
    "StatusCode.INVALID_ARGUMENT: PUBLIC_ERROR_MINOR_UPLOAD",
])
def test_policy_rejection_fails_fast(transport, detail):
    r = transport._facade_error("i2v", NICK, fg.FlowGrpcError(detail))
    assert r["status"] == 400
    assert r["error_code"] == "content_policy_violation"
    assert r["retryable"] is False


def test_plain_invalid_argument_still_retryable(transport):
    r = transport._facade_error("t2v", NICK, fg.FlowGrpcError(
        "StatusCode.INVALID_ARGUMENT: Request contains an invalid argument."))
    assert r["status"] == 429 and r["retryable"] is True


def test_throttle_is_not_mistaken_for_policy(transport):
    r = transport._facade_error("t2v", NICK, fg.FlowGrpcError(
        "StatusCode.RESOURCE_EXHAUSTED: PUBLIC_ERROR_USER_REQUESTS_THROTTLED"))
    assert r["error_code"] == "user_requests_throttled"


def test_entity_not_found_is_media_not_found(transport):
    # "Requested entity was not found" = the start image is invisible to
    # this account — failover re-merges routes and tries the true owner.
    r = transport._facade_error("i2v", NICK, fg.FlowGrpcError(
        "StatusCode.NOT_FOUND: Requested entity was not found."))
    assert r["status"] == 502
    assert r["error_code"] == "media_not_found"


def test_api_returns_400_non_retryable_for_policy():
    resp = _respond_flow_result({
        "status": 400, "error_code": "content_policy_violation",
        "retryable": False, "error": "grpc[friend] PUBLIC_ERROR_UNSAFE_GENERATION"})
    assert resp.status_code == 400
    body = json.loads(resp.body)
    assert body["error_code"] == "content_policy_violation"
    assert body["retryable"] is False


def test_upscale_invalid_argument_fails_fast(transport):
    # Upscale args come from a captured template — INVALID_ARGUMENT is a real
    # rejection, not throttling. Mapping it to 429 caused a Nova retry storm.
    r = transport._facade_error("upscale", NICK, fg.FlowGrpcError(
        "StatusCode.INVALID_ARGUMENT: Request contains an invalid argument."))
    assert r["status"] == 400
    assert r["error_code"] == "invalid_argument"
    assert r["retryable"] is False


def test_throttle_cooldown_short_and_bounded(transport):
    err = fg.FlowGrpcError("PUBLIC_ERROR_USER_REQUESTS_THROTTLED")
    transport._facade_error("i2v", NICK, err)
    assert 0 < transport._throttle_until["friend"] - _now() <= 25
    # android + cd-off nicks are never parked (quota is per-IP rolling).
    android = SimpleNamespace(name="android01", meta={"platform": "android"})
    transport._facade_error("i2v", android, err)
    assert transport._throttle_until.get("android01", 0) - _now() < 1


def _now():
    import time
    return time.time()


def test_mint_http_error_is_retryable_and_4xx_parks(transport):
    # mint failures used to escape as requests.HTTPError → bare 500.
    r = transport._facade_error("upload_image", NICK, fg.FlowGrpcError(
        "friend: mint token HTTP 503"))
    assert r["status"] == 429 and r["error_code"] == "auth_mint"
    assert r["retryable"] is True
    assert transport._throttle_until.get("friend", 0) < _now()

    r = transport._facade_error("upload_image", NICK, fg.FlowGrpcError(
        "friend: mint token HTTP 401"))
    assert r["error_code"] == "auth_mint"
    # dead credential parks the nick until someone recaptures it
    assert transport._throttle_until["friend"] - _now() > 1500


def test_upscale_already_exists_returns_pseudo_op(transport, monkeypatch):
    """Google's ALREADY_EXISTS on upsample = op exists but id unknowable.
    We synthesize an op handle on the deterministic <src>_upsampled media
    so polls converge instead of retry-storming."""
    class FakeNick:
        name = "friend"
        project_uuid = "p-uuid"

        def meta_block(self, *a, **k):
            return b"\x0a\x00"

        def call(self, *a, **k):
            raise fg.FlowGrpcError(
                "StatusCode.ALREADY_EXISTS: Requested entity already exists")

    transport._op_owner = {}
    transport._op_media = {}
    transport._media_owner = {}
    transport._op_started = {}
    transport._op_req = {}
    transport._save_routes = lambda *a, **k: None
    r = transport._upscale_sync(FakeNick(), "11111111-2222-3333-4444-555555555555",
                                None, "veo_3_1_upsampler_1080p")
    op_name = r["data"]["operations"][0]["operation"]["name"]
    assert op_name == "11111111-2222-3333-4444-555555555555_upsampled"
    assert r["status"] == 200
    # pseudo-op polls its own deterministic output media
    assert transport._op_media[op_name] == op_name
    assert transport._op_owner[op_name] == "friend"


def test_pushed_ya29_persists_and_adopts(transport, monkeypatch, tmp_path):
    monkeypatch.setattr(fg, "YA29_FILE", tmp_path / "_ya29.json")
    nick = SimpleNamespace(name="android01", _ya29="", _ya29_exp=0.0)
    transport._nicks = {"android01": nick}

    transport._save_pushed_token("android01", "ya29.pushed", _now() + 3000)
    data = json.loads((tmp_path / "_ya29.json").read_text())
    assert data["android01"]["token"] == "ya29.pushed"

    # sibling agent adopts the pushed token
    sibling_nick = SimpleNamespace(name="android01", _ya29="", _ya29_exp=0.0)
    transport._nicks = {"android01": sibling_nick}
    transport._load_pushed_tokens()
    assert sibling_nick._ya29 == "ya29.pushed"

    # a fresher local token is never clobbered by an older push
    sibling_nick._ya29 = "ya29.fresher"
    sibling_nick._ya29_exp = _now() + 3600
    transport._save_pushed_token("android01", "ya29.older", _now() + 100)
    transport._load_pushed_tokens()
    assert sibling_nick._ya29 == "ya29.fresher"

    # expired pushes are skipped and pruned from the file
    sibling_nick._ya29 = ""
    sibling_nick._ya29_exp = 0.0
    transport._save_pushed_token("android01", "ya29.dead", _now() - 5)
    transport._load_pushed_tokens()
    assert sibling_nick._ya29 == ""
    data = json.loads((tmp_path / "_ya29.json").read_text())
    assert data == {}  # expired entry pruned on save


def test_quota_dead_persists_to_file_and_adopts(transport):
    """RESOURCE_EXHAUSTED parks the account for uploads — and the park must
    survive restarts + reach the sibling agent via _quota_dead.json."""
    err = fg.FlowGrpcError(
        "StatusCode.RESOURCE_EXHAUSTED: Resource has been exhausted "
        "(e.g. check quota).")
    nick = SimpleNamespace(name="android01", project_uuid="d6bb966f")
    transport._facade_error("upload_image", nick, err)
    assert transport._quota_dead_until["d6bb966f"] > _now()

    # Fresh transport = process restart — the death must reload.
    other = fg.FlowGrpcTransport.__new__(fg.FlowGrpcTransport)
    other._quota_dead_until, other._quota_streak = {}, {}
    other._load_quota()
    assert other._quota_dead_until["d6bb966f"] > _now()
    assert other._quota_streak["d6bb966f"] >= 1

    # quota_ok clears both and removes the file entry.
    other._quota_dead_until.pop("d6bb966f")
    other._quota_streak.pop("d6bb966f")
    other._save_quota()
    transport._load_quota()
    assert "d6bb966f" not in transport._quota_dead_until


def _saturated_transport(monkeypatch, tmp_path):
    """One nick pinned at MAX_CONCURRENT — _acquire can only queue."""
    t = fg.FlowGrpcTransport.__new__(fg.FlowGrpcTransport)
    t._nicks = {"friend": SimpleNamespace(name="friend", meta={},
                                          project_uuid="p1")}
    t._order = ["friend"]
    t._rr_idx = 0
    t._inflight = {"friend": fg.MAX_CONCURRENT_PER_NICK}
    t._next_slot = {}
    t._waiters = 0
    t._waiters_hi = 0
    t._waiters_ops = {"image": 0, "video": 0}
    t._throttle_until = {}
    t._throttle_streak = {}
    t._quota_dead_until = {}
    t._quota_streak = {}
    t._no_cooldown = set()
    t._parked = set()
    t._sched_lock = None
    t._pace_locks = {}
    t._media_owner = {}
    t._media_seen = {}
    t._flag_mtimes = {}
    monkeypatch.setattr(t, "_refresh_flags", lambda: None)
    monkeypatch.setattr(fg, "_trace_emit", lambda *a, **k: None)
    return t


def test_client_gone_while_queued_aborts_before_submit(monkeypatch,
                                                      tmp_path):
    t = _saturated_transport(monkeypatch, tmp_path)

    async def gone():
        return True

    tok = fg._request_gone.set(gone)
    try:
        with pytest.raises(fg.FlowGrpcError, match="FLOWKIT_CLIENT_GONE"):
            asyncio.run(t._acquire(op="video", deadline=time.time() + 60))
    finally:
        fg._request_gone.reset(tok)
    # Waiter counter was handed back — no phantom queue pressure.
    assert t._waiters == 0
    assert t._waiters_ops.get("video", 0) == 0
    # Nobody acquired a slot.
    assert t._inflight["friend"] == fg.MAX_CONCURRENT_PER_NICK


def test_connected_request_still_times_out_as_busy(monkeypatch, tmp_path):
    t = _saturated_transport(monkeypatch, tmp_path)

    async def alive():
        return False

    tok = fg._request_gone.set(alive)
    try:
        with pytest.raises(fg.FlowGrpcError, match="FLOWKIT_SUBMIT_BUSY"):
            asyncio.run(t._acquire(op="video",
                                   deadline=time.time() + 0.6))
    finally:
        fg._request_gone.reset(tok)
    assert t._waiters == 0


def test_no_request_context_acquires_normally(monkeypatch, tmp_path):
    # Background callers (resubmit, worker) have no HTTP context — the
    # disconnect check must not fire for them.
    t = _saturated_transport(monkeypatch, tmp_path)
    t._inflight = {"friend": 0}
    tok = fg._request_gone.set(None)
    try:
        nick = asyncio.run(t._acquire(op="video",
                                      deadline=time.time() + 10))
    finally:
        fg._request_gone.reset(tok)
    assert nick.name == "friend"
    assert t._inflight["friend"] == 1


def test_api_invalid_argument_is_400_not_retryable():
    resp = _respond_flow_result({
        "status": 400, "error_code": "invalid_argument", "retryable": False,
        "error": "grpc[landon] Request contains an invalid argument."})
    assert resp.status_code == 400
    body = json.loads(resp.body)
    assert body["error"] == "INVALID_ARGUMENT"
    assert body["retryable"] is False


def _two_nick_transport(monkeypatch, tmp_path):
    """a (cooling briefly) + b (parked long) — the main pool is empty and
    the fallback must still skip the parked nick."""
    t = fg.FlowGrpcTransport.__new__(fg.FlowGrpcTransport)
    t._nicks = {n: SimpleNamespace(name=n, meta={}, project_uuid=f"p-{n}")
                for n in ("a", "b")}
    t._order = ["b", "a"]          # b first — round-robin would hit it
    t._rr_idx = 0
    t._inflight = {"a": 0, "b": 0}
    t._next_slot = {}
    t._waiters = 0
    t._waiters_hi = 0
    t._waiters_ops = {}
    t._throttle_until = {"a": _now() + 20, "b": _now() + 1800}
    t._throttle_streak = {}
    t._quota_dead_until = {}
    t._quota_streak = {}
    t._no_cooldown = set()
    t._parked = {"b"}
    t._sched_lock = None
    t._pace_locks = {}
    t._media_owner = {}
    t._media_seen = {}
    t._flag_mtimes = {}
    monkeypatch.setattr(t, "_refresh_flags", lambda: None)
    monkeypatch.setattr(fg, "_trace_emit", lambda *a, **k: None)
    return t


def test_fallback_never_picks_parked_nick(monkeypatch, tmp_path):
    t = _two_nick_transport(monkeypatch, tmp_path)
    nick = asyncio.run(t._acquire(op="video",
                                  deadline=time.time() + 60))
    assert nick.name == "a"


def test_all_nicks_parked_raises(monkeypatch, tmp_path):
    t = _two_nick_transport(monkeypatch, tmp_path)
    t._parked = {"a", "b"}
    with pytest.raises(fg.FlowGrpcError, match="FLOWKIT_ALL_PARKED"):
        asyncio.run(t._acquire(op="video",
                               deadline=time.time() + 60))


def test_upload_dead_accounts_rotate_probes(monkeypatch, tmp_path):
    """All accounts upload-dead → still acquire, probing the longest-dead
    nick first (quota refills gradually; a probe may find it recovered)."""
    t = _two_nick_transport(monkeypatch, tmp_path)
    t._throttle_until = {}          # both healthy for generation
    t._quota_dead_until = {"p-a": _now() + 900, "p-b": _now() + 300}
    nick = asyncio.run(t._acquire(op="upload",
                                  deadline=time.time() + 60))
    assert nick.name == "b"         # p-b dead longest → probed first


def test_image_biases_toward_least_owned_nick(monkeypatch, tmp_path):
    """With both nicks healthy at equal inflight, image gen must pick the
    nick owning fewer media — that nick's future i2v demand depends on it.
    _order puts 'a' first so plain round-robin would pick it instead."""
    t = _two_nick_transport(monkeypatch, tmp_path)
    t._order = ["a", "b"]
    t._throttle_until = {}
    t._parked = set()
    t._media_owner = {"m1": "a", "m2": "a", "m3": "a"}  # a:3, b:0
    t._media_seen = {m: _now() for m in ("m1", "m2", "m3")}
    nick = asyncio.run(t._acquire(op="image",
                                  deadline=time.time() + 60))
    assert nick.name == "b"


def test_unpinned_video_still_uses_least_busy(monkeypatch, tmp_path):
    """Ownership bias must not leak into video picks — those keep the
    least-inflight rule."""
    t = _two_nick_transport(monkeypatch, tmp_path)
    t._order = ["a", "b"]
    t._throttle_until = {}
    t._parked = set()
    t._inflight = {"a": 0, "b": 5}   # a less busy; b less owned
    t._media_owner = {"m1": "a"}      # a:1, b:0
    nick = asyncio.run(t._acquire(op="video",
                                  deadline=time.time() + 60))
    assert nick.name == "a"


def test_model_gate_filters_to_capable_nick(monkeypatch, tmp_path):
    """A video_model request can only land on a nick whose video_models map
    contains the key — other accounts would PERMISSION_DENIED at submit."""
    t = _two_nick_transport(monkeypatch, tmp_path)
    t._order = ["a", "b"]
    t._throttle_until = {}
    t._parked = set()
    t._nicks["b"].meta = {"video_models": {"fast": "x_t2v_fast"}}
    nick = asyncio.run(t._acquire(op="video", model="fast",
                                  deadline=time.time() + 60))
    assert nick.name == "b"


def test_model_gate_no_capable_nick_raises(monkeypatch, tmp_path):
    t = _two_nick_transport(monkeypatch, tmp_path)
    with pytest.raises(fg.FlowGrpcError, match="FLOWKIT_MODEL_UNAVAILABLE"):
        asyncio.run(t._acquire(op="video", model="fast",
                               deadline=time.time() + 60))


def test_pinned_nick_lacking_model_raises(monkeypatch, tmp_path):
    t = _two_nick_transport(monkeypatch, tmp_path)
    t._throttle_until = {}
    t._nicks["a"].meta = {"video_models": {"lite": "x_t2v_lite"}}
    with pytest.raises(fg.FlowGrpcError, match="FLOWKIT_MODEL_UNAVAILABLE"):
        asyncio.run(t._acquire(profile_id="a", op="video", model="fast",
                               deadline=time.time() + 60))


def test_video_model_for_derives_i2v_variant():
    nick = SimpleNamespace(meta={"video_models": {
        "fast": "veo_3_1_t2v_fast_portrait_ultra",
        "omni_8s": "abra_t2v_8s"}})
    fn = fg.GrpcNick.video_model_for
    assert fn(nick, "t2v", "fast") == "veo_3_1_t2v_fast_portrait_ultra"
    assert fn(nick, "i2v", "fast") == "veo_3_1_i2v_fast_portrait_ultra"
    assert fn(nick, "i2v", "omni_8s") == "abra_i2v_8s"
    assert fn(nick, "t2v", "nope") is None


def test_raw_model_string_bypasses_gate(monkeypatch, tmp_path):
    """Legacy callers send raw Google model ids (Nova google/flow-veo:
    veo_3_1_i2v_lite_low_priority). Those are NOT friendly keys and must
    not hit the capability gate — regression made 1000+ jobs fail 400."""
    t = _two_nick_transport(monkeypatch, tmp_path)
    t._throttle_until = {}
    t._parked = set()
    nick = asyncio.run(t._acquire(
        op="video", model="veo_3_1_i2v_lite_low_priority",
        deadline=time.time() + 60))
    assert nick.name in ("a", "b")        # no model_unavailable


def test_raw_model_on_pinned_capabilityless_nick(monkeypatch, tmp_path):
    """A raw model string pinned to a nick without video_models must
    still run — the gate only applies to friendly keys."""
    t = _two_nick_transport(monkeypatch, tmp_path)
    t._throttle_until = {}
    t._parked = set()
    nick = asyncio.run(t._acquire(
        profile_id="a", op="video",
        model="veo_3_1_t2v_fast_6s_relaxed",
        deadline=time.time() + 60))
    assert nick.name == "a"


def test_unpinned_acquire_skips_premium_nick(monkeypatch, tmp_path):
    """Nicks with a video_models map are reserved for friendly-key
    traffic — unpinned ordinary work must not burn premium quota."""
    t = _two_nick_transport(monkeypatch, tmp_path)
    t._throttle_until = {}
    t._parked = set()
    t._nicks["b"].meta = {"video_models": {"fast": "x_t2v_fast"}}
    nick = asyncio.run(t._acquire(op="video",
                                  deadline=time.time() + 60))
    assert nick.name == "a"


def test_ownership_pin_still_reaches_premium_nick(monkeypatch, tmp_path):
    """Media owned by a premium nick keeps its i2v on that account —
    the reservation only filters unpinned scheduling."""
    t = _two_nick_transport(monkeypatch, tmp_path)
    t._throttle_until = {}
    t._parked = set()
    t._nicks["b"].meta = {"video_models": {"fast": "x_t2v_fast"}}
    t._media_owner = {"m1": "b"}
    nick = asyncio.run(t._acquire(op="video", media_ids=["m1"],
                                  deadline=time.time() + 60))
    assert nick.name == "b"


def test_all_premium_fleet_still_usable_unpinned(monkeypatch, tmp_path):
    """Edge case: if every nick is premium-capable, ordinary work must
    still run — reservation degrades gracefully, never deadlocks."""
    t = _two_nick_transport(monkeypatch, tmp_path)
    t._throttle_until = {}
    t._parked = set()
    for n in t._nicks.values():
        n.meta = {"video_models": {"fast": "x_t2v_fast"}}
    nick = asyncio.run(t._acquire(op="video",
                                  deadline=time.time() + 60))
    assert nick.name in ("a", "b")


def test_high_priority_acquires_despite_waiters(monkeypatch, tmp_path):
    """priority='high' ignores the queued-high counter — it IS the
    high-priority traffic."""
    t = _two_nick_transport(monkeypatch, tmp_path)
    t._throttle_until = {}
    t._parked = set()
    t._waiters_hi = 1
    nick = asyncio.run(t._acquire(op="video", priority="high",
                                  deadline=time.time() + 60))
    assert nick.name in ("a", "b")


def test_normal_request_yields_to_queued_high(monkeypatch, tmp_path):
    """While a high-priority waiter is queued, ordinary requests keep
    yielding — they must not grab the slot first."""
    t = _two_nick_transport(monkeypatch, tmp_path)
    t._throttle_until = {}
    t._parked = set()
    t._waiters_hi = 1            # a hi waiter is queued behind us
    with pytest.raises(fg.FlowGrpcError, match="FLOWKIT_SUBMIT_BUSY"):
        asyncio.run(t._acquire(op="video",
                               deadline=time.time() + 1.5))
