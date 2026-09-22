"""Auth verdicts must come from raw status codes, not from a classifier."""
from __future__ import annotations

import json
import time

import pytest

from agent.services import nick_auth
from agent.services.nick_auth import (
    VERDICT_BLOCKED,
    VERDICT_NO_EVIDENCE,
    VERDICT_OK,
    VERDICT_RECOVERED,
    VERDICT_SIGNED_OUT,
    evidence_for,
    scan_netlog,
)


def _iso(epoch: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(epoch)) + ".000Z"


def _netlog(tmp_path, records):
    path = tmp_path / "ext-netlog.jsonl"
    with path.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")
    return path


def _rec(nick, rpcid, status, ts=None):
    return {
        "ts": _iso(ts if ts is not None else time.time()),
        "url": "https://flow.google.com/_/…/batchexecute",
        "statusCode": status,
        "rpcid": rpcid,
        "profileId": nick,
    }


class TestVerdicts:
    def test_everything_401_is_signed_out(self, tmp_path):
        path = _netlog(tmp_path, [_rec("n1", "maseQ", 401), _rec("n1", "nzlxg", 401)])
        assert evidence_for("n1", path=path)["verdict"] == VERDICT_SIGNED_OUT

    def test_generation_401_while_chat_is_200_is_an_account_block(self, tmp_path):
        # The 22 Sep pattern: re-login does nothing for this one.
        path = _netlog(tmp_path, [
            _rec("n1", "maseQ", 401),
            _rec("n1", "YhhmEf", 401),
            _rec("n1", "StreamChat", 200),
            _rec("n1", "nzlxg", 200),
        ])
        ev = evidence_for("n1", path=path)
        assert ev["verdict"] == VERDICT_BLOCKED
        assert "Re-login will not fix it" in ev["advice"]

    def test_an_old_401_followed_by_success_is_recovered(self, tmp_path):
        now = time.time()
        path = _netlog(tmp_path, [
            _rec("n1", "maseQ", 401, ts=now - 600),
            _rec("n1", "maseQ", 200, ts=now - 60),
        ])
        assert evidence_for("n1", path=path)["verdict"] == VERDICT_RECOVERED

    def test_all_200_is_ok(self, tmp_path):
        path = _netlog(tmp_path, [_rec("n1", "eb1hJf", 200), _rec("n1", "jwpduf", 200)])
        assert evidence_for("n1", path=path)["verdict"] == VERDICT_OK

    def test_no_traffic_is_not_a_failure(self, tmp_path):
        path = _netlog(tmp_path, [_rec("other", "maseQ", 401)])
        ev = evidence_for("n1", path=path)
        assert ev["verdict"] == VERDICT_NO_EVIDENCE
        assert ev["samples"] == 0

    def test_403_counts_as_unauthorized(self, tmp_path):
        path = _netlog(tmp_path, [_rec("n1", "maseQ", 403)])
        assert evidence_for("n1", path=path)["verdict"] == VERDICT_SIGNED_OUT

    def test_a_500_is_neither_ok_nor_unauthorized(self, tmp_path):
        path = _netlog(tmp_path, [_rec("n1", "maseQ", 500)])
        ev = evidence_for("n1", path=path)
        assert (ev["ok"], ev["unauthorized"], ev["other"]) == (0, 0, 1)
        assert ev["verdict"] == VERDICT_OK


class TestScanning:
    def test_records_outside_the_window_are_ignored(self, tmp_path):
        now = time.time()
        path = _netlog(tmp_path, [
            _rec("n1", "maseQ", 401, ts=now - 7200),
            _rec("n1", "maseQ", 200, ts=now - 10),
        ])
        ev = evidence_for("n1", window_s=3600, path=path)
        assert (ev["samples"], ev["unauthorized"]) == (1, 0)

    def test_per_rpc_breakdown_is_kept(self, tmp_path):
        path = _netlog(tmp_path, [
            _rec("n1", "maseQ", 401),
            _rec("n1", "maseQ", 401),
            _rec("n1", "maseQ", 200),
        ])
        assert evidence_for("n1", path=path)["per_rpc"]["maseQ"] == {"ok": 1, "unauthorized": 2, "other": 0}

    def test_garbage_lines_do_not_break_the_scan(self, tmp_path):
        path = tmp_path / "ext-netlog.jsonl"
        path.write_text(
            "not json\n"
            + json.dumps([1, 2]) + "\n"
            + json.dumps({"statusCode": 200}) + "\n"   # no profileId
            + json.dumps(_rec("n1", "maseQ", 401)) + "\n",
            encoding="utf-8",
        )
        assert scan_netlog(path=path)["n1"]["unauthorized"] == 1

    def test_a_missing_netlog_is_empty_not_an_error(self, tmp_path):
        assert scan_netlog(path=tmp_path / "nope.jsonl") == {}

    def test_epoch_ms_timestamps_still_parse(self, tmp_path):
        rec = _rec("n1", "maseQ", 401)
        rec["ts"] = int(time.time() * 1000)
        path = _netlog(tmp_path, [rec])
        assert evidence_for("n1", path=path)["samples"] == 1


class TestReport:
    def test_report_flags_the_signed_out_nick_and_sorts_it_first(self, tmp_path, monkeypatch):
        path = _netlog(tmp_path, [
            _rec("bad@x.com", "maseQ", 401),
            _rec("good@x.com", "maseQ", 200),
        ])
        monkeypatch.setattr(
            "agent.services.accounts.load_accounts",
            lambda *a, **k: [
                {"id": "good@x.com", "enabled": True},
                {"id": "bad@x.com", "enabled": True},
            ],
        )
        report = nick_auth.auth_report(netlog_path=path)
        assert report["counts"]["signed_out"] == 1
        assert report["nicks"][0]["nick_id"] == "bad@x.com"
        assert report["nicks"][0]["needs_attention"] is True
        assert report["nicks"][1]["needs_attention"] is False

    def test_a_disabled_nick_needs_attention_even_with_no_traffic(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "agent.services.accounts.load_accounts",
            lambda *a, **k: [{"id": "off@x.com", "enabled": False}],
        )
        report = nick_auth.auth_report(netlog_path=tmp_path / "none.jsonl")
        row = report["nicks"][0]
        assert row["verdict"] == VERDICT_NO_EVIDENCE
        assert row["needs_attention"] is True
        assert report["counts"]["disabled"] == 1


class TestStrikeReporting:
    """Strike counters existed but were invisible outside the failure path."""

    def _client(self):
        from agent.services.flow_client import FlowClient
        return FlowClient()

    def test_report_exposes_strikes_and_park_time(self):
        client = self._client()
        client._auth_strikes["n1"] = 2
        client._auth_strike_ts["n1"] = time.time()
        client._extensions["ws1"] = {"profile_id": "n1", "unavailable_until": time.time() + 600}
        row = client.auth_strike_report()["n1"]
        assert row["strikes"] == 2
        assert 500 < row["parked_for_s"] <= 600

    def test_expired_strikes_report_as_zero(self, monkeypatch):
        client = self._client()
        client._auth_strikes["n1"] = 2
        client._auth_strike_ts["n1"] = time.time() - 99999
        assert client.auth_strike_report()["n1"]["strikes"] == 0

    def test_clearing_strikes_also_unparks_the_worker(self):
        client = self._client()
        client._auth_strikes["n1"] = 3
        client._extensions["ws1"] = {"profile_id": "n1", "unavailable_until": time.time() + 1800}
        res = client.clear_auth_strikes("n1")
        assert res == {"profile_id": "n1", "cleared_strikes": 3, "unparked": 1}
        assert client._extensions["ws1"]["unavailable_until"] == 0
        row = client.auth_strike_report()["n1"]
        assert (row["strikes"], row["parked_for_s"]) == (0, 0.0)


class TestChromePsCache:
    def test_one_pgrep_serves_every_nick_in_a_poll(self, monkeypatch):
        from agent.services import chrome_nicks as cn

        calls = []

        def fake_check_output(cmd, text=True):
            calls.append(cmd)
            return ""

        monkeypatch.setattr(cn.subprocess, "check_output", fake_check_output)
        cn.invalidate_chrome_ps()
        for nick in ("a", "b", "c", "d"):
            cn.find_running_chrome_pid(nick)
        assert len(calls) == 1

        # A kill path must not act on a stale snapshot.
        cn.get_all_chrome_pids_for_nick("a")
        assert len(calls) == 2
