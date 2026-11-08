"""Cross-nick character refs are copied onto one nick instead of 400."""
import time

from agent.services.flow_grpc import FlowGrpcTransport


def _transport():
    t = FlowGrpcTransport()
    t._nicks = {"friend": object(), "gemma": object(), "kailyn": object()}
    t._order = ["friend", "gemma", "kailyn"]
    t._media_owner = {}
    t._rebase_cache = {}
    return t


def test_same_nick_is_left_alone():
    t = _transport()
    t._media_owner = {"aaa": "friend", "bbb": "friend"}
    assert t._rebase_media_ids(["aaa", "bbb"]) == ["aaa", "bbb"]


def test_foreign_refs_are_copied_onto_majority_nick():
    t = _transport()
    t._media_owner = {"aaa": "friend", "bbb": "friend", "ccc": "gemma"}
    copied = []

    def copy(mid, dest):
        copied.append((mid, dest))
        return f"{mid}-on-{dest}"

    t._copy_media_onto = copy
    out = t._rebase_media_ids(["aaa", "bbb", "ccc"])
    assert out == ["aaa", "bbb", "ccc-on-friend"]
    assert copied == [("ccc", "friend")]


def test_profile_pin_wins_over_majority():
    t = _transport()
    t._media_owner = {"aaa": "friend", "bbb": "friend", "ccc": "kailyn"}
    copied = []
    t._copy_media_onto = lambda mid, dest: copied.append((mid, dest)) or f"{mid}@{dest}"
    out = t._rebase_media_ids(["aaa", "bbb", "ccc"], profile_id="kailyn")
    assert out == ["aaa@kailyn", "bbb@kailyn", "ccc"]
    assert copied == [("aaa", "kailyn"), ("bbb", "kailyn")]


def test_cooling_majority_owner_loses_anchor():
    t = _transport()
    t._media_owner = {"aaa": "friend", "bbb": "friend", "ccc": "gemma"}
    t._throttle_until = {"friend": time.time() + 60}
    copied = []
    t._copy_media_onto = lambda mid, dest: copied.append((mid, dest)) or f"{mid}-on-{dest}"
    out = t._rebase_media_ids(["aaa", "bbb", "ccc"])
    assert out == ["aaa-on-gemma", "bbb-on-gemma", "ccc"]
    assert copied == [("aaa", "gemma"), ("bbb", "gemma")]


def test_all_owners_cooling_anchor_least_loaded_healthy():
    t = _transport()
    t._media_owner = {"aaa": "friend"}
    t._throttle_until = {"friend": time.time() + 60}
    t._inflight = {"friend": 0, "gemma": 5, "kailyn": 1}
    copied = []
    t._copy_media_onto = lambda mid, dest: copied.append((mid, dest)) or f"{mid}-on-{dest}"
    out = t._rebase_media_ids(["aaa"])
    assert out == ["aaa-on-kailyn"]
    assert copied == [("aaa", "kailyn")]


def test_everyone_cooling_keeps_majority_owner():
    t = _transport()
    t._media_owner = {"aaa": "friend", "bbb": "friend", "ccc": "gemma"}
    future = time.time() + 60
    t._throttle_until = {"friend": future, "gemma": future, "kailyn": future}
    copied = []
    t._copy_media_onto = lambda mid, dest: copied.append((mid, dest)) or f"{mid}-on-{dest}"
    out = t._rebase_media_ids(["aaa", "bbb", "ccc"])
    assert out == ["aaa", "bbb", "ccc-on-friend"]
    assert copied == [("ccc", "friend")]


def test_excluded_owner_without_cooldown_loses_anchor():
    # _no_cooldown nicks never get a _throttle_until stamp, so a throttled
    # owner must be excluded explicitly or i2v stays pinned to it.
    t = _transport()
    t._media_owner = {"aaa": "friend"}
    t._throttle_until = {}
    t._inflight = {"friend": 0, "gemma": 3, "kailyn": 1}
    copied = []
    t._copy_media_onto = lambda mid, dest: copied.append((mid, dest)) or f"{mid}-on-{dest}"
    out = t._rebase_media_ids(["aaa"], None, {"friend"})
    assert out == ["aaa-on-kailyn"]
    assert copied == [("aaa", "kailyn")]


def test_empty_exclude_keeps_owner():
    t = _transport()
    t._media_owner = {"aaa": "friend"}
    t._throttle_until = {}
    assert t._rebase_media_ids(["aaa"], None, set()) == ["aaa"]
