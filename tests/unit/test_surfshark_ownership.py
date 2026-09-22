import asyncio
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote, urlsplit
from unittest.mock import patch

import pytest

from agent.services.accounts import load_accounts, save_accounts, upsert_account
from agent.services.surfshark import bind_nick_proxy, is_surfshark_url, monitored_proxy_urls, owner_id

URL = "http://surf__cr.vn;sessid.old;ttl.60:test@127.0.0.1:18888"


def test_owner_follows_nick_not_sanitized_id_or_session():
    assert len({owner_id(n) for n in ["a-b", "a_b", "A-b"]}) == 3
    old = bind_nick_proxy(URL, "a-b")
    new = bind_nick_proxy(old, "a-b", session="new", prepare=True)
    assert f"owner.{owner_id('a-b')}" in unquote(urlsplit(new).username)
    assert "sessttl.60" in new
    assert "prepare.1" in new
    assert "prepare.1" not in bind_nick_proxy(new, "a-b")
    assert not is_surfshark_url("http://surf__cr.vn:p@evil.test:18888")


def test_account_cannot_inherit_another_nicks_owner(tmp_path):
    path = tmp_path / "accounts.json"
    copied = bind_nick_proxy(URL, "a")
    save_accounts([{"id": "a", "proxy_url": copied}, {"id": "b", "proxy_url": copied}], path)
    rows = load_accounts(path)
    assert rows[0]["proxy_url"] != rows[1]["proxy_url"]
    assert f"owner.{owner_id('b')}" in rows[1]["proxy_url"]


def test_monitor_excludes_historical_surfshark_sessions():
    active = bind_nick_proxy(URL, "a", session="active")
    external = "http://user:password@proxy.test:8080"
    assert monitored_proxy_urls([URL, external], [{"proxy_url": active}]) == {active, external}


def test_concurrent_account_updates_do_not_overwrite_other_nicks(tmp_path):
    path = tmp_path / "accounts.json"
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda i: upsert_account({"id": f"nick-{i}", "proxy_url": URL}, path), range(30)))
    assert len(load_accounts(path)) == 30


@pytest.mark.asyncio
async def test_rotation_verifies_before_saving_and_commits_same_candidate(tmp_path):
    from agent.services.proxy_pool import rotate_nick_proxy
    path = tmp_path / "accounts.json"
    save_accounts([{"id": "a", "proxy_url": URL}], path)
    old = load_accounts(path)[0]["proxy_url"]
    calls = []

    def probe(url):
        assert load_accounts(path)[0]["proxy_url"] == old
        calls.append(url)
        return "8.8.8.8"

    with patch("agent.services.proxy_pool.probe_egress", side_effect=probe), \
         patch("agent.services.proxy_checker.check_single_proxy", return_value={"status": "CLEAN", "labs_accessible": True}), \
         patch("agent.services.proxy_pool.get_bridge", return_value=None), \
         patch("agent.services.chrome_nicks.running_chrome_proxy_port", return_value=None):
        result = await rotate_nick_proxy("a", preflight=False, accounts_path=path)
    assert result["ok"] and result["egress_ip"] == "8.8.8.8"
    assert "prepare.1" in calls[0] and "prepare.1" not in calls[1]
    assert load_accounts(path)[0]["proxy_url"] == calls[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["capacity", "google"])
async def test_failed_candidate_keeps_account_and_bridge(tmp_path, failure):
    from agent.services.proxy_pool import rotate_nick_proxy
    path = tmp_path / "accounts.json"
    save_accounts([{"id": "a", "proxy_url": URL}], path)
    before = path.read_bytes()
    with patch("agent.services.proxy_pool.probe_egress", side_effect=RuntimeError() if failure == "capacity" else None, return_value="8.8.8.8") as probe, \
         patch("agent.services.proxy_checker.check_single_proxy", return_value={"status": "CAPTCHA_BLOCKED"}), \
         patch("agent.services.proxy_pool.get_bridge") as bridge:
        result = await rotate_nick_proxy("a", accounts_path=path)
    assert not result["ok"]
    assert path.read_bytes() == before
    bridge.assert_not_called()
    assert probe.call_count == 1


@pytest.mark.asyncio
async def test_simultaneous_same_nick_rotation_is_rejected(tmp_path):
    from agent.services.proxy_pool import rotate_nick_proxy
    path = tmp_path / "accounts.json"
    save_accounts([{"id": "a", "proxy_url": URL}], path)
    started, release = asyncio.Event(), asyncio.Event()

    async def wait_probe(*args, **kwargs):
        started.set()
        await release.wait()
        raise RuntimeError("test capacity exhausted")

    with patch("agent.services.proxy_pool.asyncio.to_thread", side_effect=wait_probe):
        first = asyncio.create_task(rotate_nick_proxy("a", accounts_path=path))
        await started.wait()
        second = await rotate_nick_proxy("a", accounts_path=path)
        assert second["error"] == "ROTATION_IN_PROGRESS"
        release.set()
        assert not (await first)["ok"]


def test_verified_assign_skips_dead_pool_candidate(tmp_path):
    """A dead/placeholder pool entry must not be handed to a new nick."""
    from agent.services import proxy_pool
    pool_path = tmp_path / "pool.json"
    proxy_pool.save_proxy_pool(
        {"proxies": ["http://user:pass@1.2.3.4:8080", "http://good:pw@10.9.9.9:8080"],
         "current_index": 0},
        pool_path,
    )
    seen = []

    def probe(url, timeout=8):
        seen.append(url)
        if "1.2.3.4" in url:
            raise RuntimeError("connect timeout")
        return "8.8.8.8"

    with patch.object(proxy_pool, "probe_egress", side_effect=probe):
        picked = proxy_pool.get_verified_proxy_for_nick(
            "nick-x", path=pool_path, accounts_path=tmp_path / "accounts.json"
        )
    assert picked == "http://good:pw@10.9.9.9:8080"
    assert seen[0].startswith("http://user:pass@1.2.3.4")


def test_verified_assign_falls_back_to_surfshark_when_all_dead(tmp_path):
    from agent.services import proxy_pool
    pool_path = tmp_path / "pool.json"
    proxy_pool.save_proxy_pool(
        {"proxies": ["http://user:pass@1.2.3.4:8080"], "current_index": 0},
        pool_path,
    )
    with patch.object(proxy_pool, "probe_egress", side_effect=RuntimeError("dead")):
        picked = proxy_pool.get_verified_proxy_for_nick(
            "nick-x", path=pool_path, accounts_path=tmp_path / "accounts.json",
            attempts=2,
        )
    assert is_surfshark_url(picked)
    assert f"owner.{owner_id('nick-x')}" in unquote(urlsplit(picked).username)


def test_upsert_assigns_verified_proxy_for_new_nick(tmp_path):
    path = tmp_path / "accounts.json"
    verified = "http://good:pw@10.9.9.9:8080"
    with patch("agent.services.proxy_pool.get_verified_proxy_for_nick", return_value=verified) as pick:
        row = upsert_account({"id": "new-nick"}, path=path)
    assert row["proxy_url"] == verified
    pick.assert_called_once_with("new-nick")


def test_upsert_leaves_proxy_empty_when_nothing_verifies(tmp_path):
    path = tmp_path / "accounts.json"
    with patch("agent.services.proxy_pool.get_verified_proxy_for_nick", return_value=None):
        row = upsert_account({"id": "new-nick"}, path=path)
    assert row["proxy_url"] == ""
