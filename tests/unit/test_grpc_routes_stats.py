"""_routes.json retention/merge and persistent daily counters."""
import json
import time

import agent.services.flow_grpc as fg
import agent.services.flow_stats as fs


def _transport(tmp_path, monkeypatch):
    monkeypatch.setattr(fg, "ROUTES_FILE", tmp_path / "_routes.json")
    monkeypatch.setattr(fg, "ROUTES_LOCK_FILE", tmp_path / "_routes.lock")
    return fg.FlowGrpcTransport()


def test_save_prunes_old_ops_and_media(tmp_path, monkeypatch):
    t = _transport(tmp_path, monkeypatch)
    now = time.time()
    t._op_owner.update(old="friend", new="gemma")
    t._op_media.update(old="m-old", new="m-new")
    t._op_started.update(old=now - fg.ROUTES_OP_TTL_S - 5, new=now)
    t._media_owner.update({"m-old": "friend", "m-new": "gemma"})
    t._media_seen.update({"m-old": now - fg.ROUTES_MEDIA_TTL_S - 5})
    t._save_routes()
    disk = json.loads(fg.ROUTES_FILE.read_text())
    assert disk["ops"] == {"new": "gemma"}
    assert disk["op_media"] == {"new": "m-new"}
    assert disk["media"] == {"m-new": "gemma"}
    assert set(disk["media_seen"]) == {"m-new"}


def test_touch_keeps_reused_reference_media(tmp_path, monkeypatch):
    t = _transport(tmp_path, monkeypatch)
    t._media_owner["ref"] = "kailyn"
    t._media_seen["ref"] = time.time() - fg.ROUTES_MEDIA_TTL_S - 5
    t._touch_media(["ref"])
    t._save_routes()
    assert json.loads(fg.ROUTES_FILE.read_text())["media"] == {"ref": "kailyn"}


def test_save_keeps_entries_written_by_other_process(tmp_path, monkeypatch):
    a = _transport(tmp_path, monkeypatch)
    b = fg.FlowGrpcTransport()
    a._op_owner["op-a"] = "friend"
    a._save_routes()
    b._op_owner["op-b"] = "landon"
    b._save_routes()
    disk = json.loads(fg.ROUTES_FILE.read_text())
    assert disk["ops"] == {"op-a": "friend", "op-b": "landon"}


def test_legacy_routes_without_timestamps_survive_first_save(tmp_path, monkeypatch):
    (tmp_path / "_routes.json").write_text(json.dumps(
        {"media": {"m": "friend"}, "ops": {"o": "friend"}}))
    t = _transport(tmp_path, monkeypatch)
    t._save_routes()
    disk = json.loads(fg.ROUTES_FILE.read_text())
    assert disk["media"] == {"m": "friend"} and disk["ops"] == {"o": "friend"}


def test_concurrent_sibling_saves_keep_every_media_owner(tmp_path, monkeypatch):
    """Two agent processes (simulated by two transports) writing _routes.json
    alternately must never drop the other's media ownership entries."""
    import threading
    a = _transport(tmp_path, monkeypatch)
    b = fg.FlowGrpcTransport()

    def save_many(t, prefix):
        for i in range(25):
            t._media_owner[f"{prefix}-{i}"] = "friend"
            t._save_routes()

    ta = threading.Thread(target=save_many, args=(a, "ma"))
    tb = threading.Thread(target=save_many, args=(b, "mb"))
    ta.start(); tb.start(); ta.join(); tb.join()
    # Saves are debounced — force the pending dirty state to disk.
    a._flush_routes()
    b._flush_routes()
    disk = json.loads(fg.ROUTES_FILE.read_text())
    assert set(disk["media"]) == ({f"ma-{i}" for i in range(25)}
                                | {f"mb-{i}" for i in range(25)})


def test_rebase_adopts_sibling_media_owner_from_disk(tmp_path, monkeypatch):
    """Media uploaded via agent A must be visible to agent B's rebase even
    though B never served the upload."""
    b = fg.FlowGrpcTransport()          # loads routes before they exist
    a = _transport(tmp_path, monkeypatch)
    a._media_owner["m-foreign"] = "kailyn"
    a._save_routes()
    b._nicks = {"kailyn": object(), "friend": object()}
    b._order = ["friend", "kailyn"]
    out = b._rebase_media_ids(["m-foreign"])
    assert b._media_owner.get("m-foreign") == "kailyn"
    assert out == ["m-foreign"]


def test_daily_stats_accumulate_and_keep_first_ts(tmp_path, monkeypatch):
    monkeypatch.setattr(fs, "STATS_FILE", tmp_path / "stats.json")
    fs.bump("friend", videos=1)
    fs.bump("friend", videos=2, images=4)
    fs.bump("gemma", throttled=1, first_throttled_ts="t1")
    fs.bump("gemma", throttled=1, first_throttled_ts="t2")
    got = fs.day_counts()
    assert got["friend"] == {"videos": 3, "images": 4}
    assert got["gemma"] == {"throttled": 2, "first_throttled_ts": "t1"}


def test_daily_stats_drop_days_past_retention(tmp_path, monkeypatch):
    monkeypatch.setattr(fs, "STATS_FILE", tmp_path / "stats.json")
    old = {f"2026-01-{d:02d}": {"friend": {"videos": 1}} for d in range(1, 21)}
    fs.STATS_FILE.write_text(json.dumps(old))
    fs.bump("friend", videos=1)
    data = json.loads(fs.STATS_FILE.read_text())
    assert len(data) == fs.KEEP_DAYS and fs.today() in data
