"""Nick + proxy store, URL parse/redact, local auth bridge, accounts API."""
from __future__ import annotations

import asyncio
import base64
import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from agent.services.proxy_url import ProxyURLError, parse_proxy_url, redact_proxy_url
from agent.services.proxy_forward import LocalProxyBridge
from agent.services import accounts as accounts_mod
from agent.api import accounts as accounts_api


PA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


class TestParseProxyUrl:
    def test_userinfo_and_trailing_slash(self):
        parsed = parse_proxy_url("http://alice:s3cret@203.0.113.10:29419/")
        assert parsed.scheme == "http"
        assert parsed.username == "alice"
        assert parsed.password == "s3cret"
        assert parsed.host == "203.0.113.10"
        assert parsed.port == 29419
        assert parsed.has_auth
        assert parsed.chrome_server() == "http://203.0.113.10:29419"
        assert parsed.redacted == "http://alice:***@203.0.113.10:29419"

    def test_redact_helper(self):
        assert redact_proxy_url("http://alice:s3cret@203.0.113.10:29419/") == (
            "http://alice:***@203.0.113.10:29419"
        )
        assert redact_proxy_url("") == ""
        assert redact_proxy_url("not-a-url") == "(invalid proxy url)"

    def test_no_auth(self):
        parsed = parse_proxy_url("http://203.0.113.10:8080")
        assert not parsed.has_auth
        assert parsed.redacted == "http://203.0.113.10:8080"

    def test_socks5_rejected_scheme_ok_but_chrome_server_is_socks(self):
        parsed = parse_proxy_url("socks5://user:pass@203.0.113.10:1080")
        assert parsed.chrome_server() == "socks5://203.0.113.10:1080"

    def test_bad_scheme(self):
        with pytest.raises(ProxyURLError):
            parse_proxy_url("ftp://203.0.113.10:21")

    def test_url_encoded_password(self):
        parsed = parse_proxy_url("http://alice:p%40ss@203.0.113.10:8080")
        assert parsed.password == "p@ss"


class TestAccountsStore:
    def test_save_load_upsert_delete(self, tmp_path):
        path = tmp_path / "accounts.json"
        saved = accounts_mod.save_accounts(
            [{
                "id": "nick-a",
                "label": "A",
                "project_id": PA,
                "proxy_url": "http://alice:s3cret@203.0.113.10:29419/",
                "note": "one",
                "enabled": True,
            }],
            path,
        )
        assert saved[0]["proxy_url"].startswith("http://alice:")
        loaded = accounts_mod.load_accounts(path)
        assert loaded[0]["id"] == "nick-a"
        assert loaded[0]["project_id"] == PA

        public = accounts_mod.public_account(loaded[0], reveal=False)
        assert public["proxy_url"] == ""
        assert "***" in public["proxy_display"]
        assert public["has_proxy"] is True
        revealed = accounts_mod.public_account(loaded[0], reveal=True)
        assert "s3cret" in revealed["proxy_url"]

        accounts_mod.upsert_account(
            {"id": "nick-a", "label": "A2", "project_id": PA, "proxy_url": "", "note": "", "enabled": False},
            path,
        )
        row = accounts_mod.get_account("nick-a", path)
        assert row["label"] == "A2"
        assert row["enabled"] is False
        assert accounts_mod.delete_account("nick-a", path) is True
        assert accounts_mod.get_account("nick-a", path) is None

    def test_invalid_id_and_project(self, tmp_path):
        path = tmp_path / "accounts.json"
        with pytest.raises(ValueError):
            accounts_mod.save_accounts([{"id": "has space"}], path)
        with pytest.raises(ValueError):
            accounts_mod.save_accounts([{"id": "nick-a", "project_id": "CAMSnotauuid"}], path)

    def test_email_id_allowed(self, tmp_path):
        path = tmp_path / "accounts.json"
        saved = accounts_mod.save_accounts([{"id": "s08054903880@gmail.com", "project_id": PA}], path)
        assert saved[0]["id"] == "s08054903880@gmail.com"
        loaded = accounts_mod.load_accounts(path)
        assert loaded[0]["id"] == "s08054903880@gmail.com"

    def test_invalid_proxy_rejected(self, tmp_path):
        path = tmp_path / "accounts.json"
        with pytest.raises(ProxyURLError):
            accounts_mod.save_accounts(
                [{"id": "nick-a", "proxy_url": "not-a-proxy"}],
                path,
            )

    def test_seed_from_template(self, tmp_path, monkeypatch):
        template = tmp_path / "profiles.json"
        template.write_text(json.dumps({
            "profiles": [{"id": "nick-a", "project_id": PA, "note": "from template"}],
        }))
        monkeypatch.setattr(accounts_mod, "PROFILES_FILE", template)
        target = tmp_path / "accounts.json"
        seeded = accounts_mod.seed_accounts_from_template(target)
        assert seeded[0]["id"] == "nick-a"
        assert seeded[0]["project_id"] == PA
        assert seeded[0]["proxy_url"] == ""
        again = accounts_mod.seed_accounts_from_template(target)
        assert again == seeded

    def test_load_nick_pins_prefers_accounts(self, tmp_path, monkeypatch):
        accounts_path = tmp_path / "accounts.json"
        accounts_mod.save_accounts(
            [
                {"id": "nick-a", "project_id": PA, "enabled": True},
                {"id": "nick-off", "project_id": PA, "enabled": False},
            ],
            accounts_path,
        )
        monkeypatch.setattr(accounts_mod, "ACCOUNTS_FILE", accounts_path)
        pins = accounts_mod.load_nick_pins()
        assert [p["id"] for p in pins] == ["nick-a"]


class TestLocalProxyBridge:
    @pytest.mark.asyncio
    async def test_connect_injects_basic_auth(self):
        received = {}

        async def upstream(reader, writer):
            head = b""
            while b"\r\n\r\n" not in head:
                chunk = await reader.read(4096)
                if not chunk:
                    break
                head += chunk
            received["head"] = head
            writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await writer.drain()
            writer.close()

        server = await asyncio.start_server(upstream, "127.0.0.1", 0)
        up_port = server.sockets[0].getsockname()[1]
        parsed = parse_proxy_url(f"http://alice:s3cret@127.0.0.1:{up_port}")
        bridge = LocalProxyBridge(parsed)
        await bridge.start()
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", bridge.port)
            writer.write(b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n")
            await writer.drain()
            reply = await asyncio.wait_for(reader.read(1024), timeout=5)
            writer.close()
            await writer.wait_closed()
        finally:
            await bridge.stop()
            server.close()
            await server.wait_closed()

        assert b"200 Connection Established" in reply
        head = received["head"].decode("iso-8859-1")
        assert "Proxy-Authorization: Basic " in head
        token = head.split("Proxy-Authorization: Basic ", 1)[1].split("\r\n", 1)[0]
        assert base64.b64decode(token) == b"alice:s3cret"

    @pytest.mark.asyncio
    async def test_connect_loopback_goes_direct(self):
        upstream_hits = {"n": 0}

        async def local_http(reader, writer):
            await reader.read(4096)
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\nLOCAL_OK")
            await writer.drain()
            writer.close()

        async def upstream(reader, writer):
            upstream_hits["n"] += 1
            await reader.read(4096)
            writer.write(b"HTTP/1.1 403 Forbidden\r\n\r\n")
            await writer.drain()
            writer.close()

        local = await asyncio.start_server(local_http, "127.0.0.1", 0)
        local_port = local.sockets[0].getsockname()[1]
        up = await asyncio.start_server(upstream, "127.0.0.1", 0)
        up_port = up.sockets[0].getsockname()[1]
        parsed = parse_proxy_url(f"http://alice:s3cret@127.0.0.1:{up_port}")
        bridge = LocalProxyBridge(parsed)
        await bridge.start()
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", bridge.port)
            writer.write(
                f"CONNECT 127.0.0.1:{local_port} HTTP/1.1\r\n"
                f"Host: 127.0.0.1:{local_port}\r\n\r\n".encode()
            )
            await writer.drain()
            head = b""
            while b"\r\n\r\n" not in head:
                chunk = await asyncio.wait_for(reader.read(1024), timeout=5)
                if not chunk:
                    break
                head += chunk
            assert b"200 Connection Established" in head
            writer.write(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
            await writer.drain()
            body = await asyncio.wait_for(reader.read(1024), timeout=5)
            writer.close()
            await writer.wait_closed()
        finally:
            await bridge.stop()
            local.close()
            up.close()
            await local.wait_closed()
            await up.wait_closed()

        assert b"LOCAL_OK" in body
        assert upstream_hits["n"] == 0

    @pytest.mark.asyncio
    async def test_loopback_http_rewrites_absolute_form(self):
        from agent.services.proxy_forward import LocalProxyBridge, _origin_form_head

        rewritten = _origin_form_head(
            b"POST http://127.0.0.1:8100/api/ext/netlog HTTP/1.1\r\n"
            b"Host: 127.0.0.1:8100\r\n\r\n",
            "http://127.0.0.1:8100/api/ext/netlog",
        )
        assert rewritten.startswith(b"POST /api/ext/netlog HTTP/1.1\r\n")

        seen = {}

        async def local_http(reader, writer):
            head = b""
            while b"\r\n\r\n" not in head:
                chunk = await asyncio.wait_for(reader.read(1024), timeout=5)
                if not chunk:
                    break
                head += chunk
            seen["head"] = head
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            await writer.drain()
            writer.close()

        local = await asyncio.start_server(local_http, "127.0.0.1", 0)
        local_port = local.sockets[0].getsockname()[1]
        parsed = parse_proxy_url("http://alice:s3cret@203.0.113.9:29419")
        bridge = LocalProxyBridge(parsed)
        await bridge.start()
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", bridge.port)
            writer.write(
                f"POST http://127.0.0.1:{local_port}/api/ext/netlog HTTP/1.1\r\n"
                f"Host: 127.0.0.1:{local_port}\r\n"
                f"Content-Length: 0\r\n\r\n".encode()
            )
            await writer.drain()
            body = await asyncio.wait_for(reader.read(1024), timeout=5)
            writer.close()
            await writer.wait_closed()
        finally:
            await bridge.stop()
            local.close()
            await local.wait_closed()

        assert b"ok" in body
        assert seen["head"].split(b"\r\n", 1)[0] == b"POST /api/ext/netlog HTTP/1.1"


class FakeFlow:
    def reload_configured_profiles(self):
        pass

    def workers(self):
        return []


@pytest.fixture
def api_client(tmp_path, monkeypatch):
    path = tmp_path / "accounts.json"
    monkeypatch.setattr(accounts_mod, "ACCOUNTS_FILE", path)
    monkeypatch.setattr(accounts_api, "get_flow_client", lambda: FakeFlow())

    def fake_launch_status(nick_id):
        return {
            "chrome_running": False,
            "pid": None,
            "data_dir": str(tmp_path / nick_id),
            "bridge_port": None,
        }

    monkeypatch.setattr(accounts_api, "launch_status", fake_launch_status)

    app = FastAPI()
    app.include_router(accounts_api.router, prefix="/api")
    return TestClient(app), path


class TestChromeLaunchArgs:
    def test_loopback_bypass_only_for_local_bridge(self, tmp_path, monkeypatch):
        from agent.services import chrome_nicks as cn

        monkeypatch.setenv("FLOW_CHROME_DIR", str(tmp_path))
        monkeypatch.setattr(cn, "_chrome_bin", lambda: "/usr/bin/google-chrome")
        remote = cn._launch_args("nick-a", "http://203.0.113.10:8080", local_bridge=False)
        assert any(a.startswith("--proxy-server=") for a in remote)
        assert "--proxy-bypass-list=<-loopback>" not in remote
        assert "--enable-unsafe-extension-debugging" in remote
        assert any(a.startswith("--load-extension=") for a in remote)
        assert not any(a.startswith("--disable-extensions-except=") for a in remote)
        bridged = cn._launch_args("nick-a", "http://127.0.0.1:9999", local_bridge=True)
        assert "--proxy-bypass-list=<-loopback>" not in bridged

    def test_unpacked_id_matches_chrome_path_hash(self):
        from agent.services.chrome_nicks import unpacked_extension_id
        assert unpacked_extension_id("/home/pc/flowkit/extension") == "mogpkadgokfmcimaonbpmlifpbaekccb"

    def test_seed_unpacked_writes_location_4(self, tmp_path):
        from agent.services.chrome_nicks import seed_unpacked_extension, unpacked_extension_id
        ext = tmp_path / "extension"
        ext.mkdir()
        (ext / "manifest.json").write_text(json.dumps({
            "manifest_version": 3,
            "name": "Flow Kit",
            "version": "0.3.0",
            "permissions": ["storage", "tabs"],
            "host_permissions": ["http://127.0.0.1:8100/*"],
            "content_scripts": [{"matches": ["https://flow.google.com/*"], "js": ["content.js"]}],
        }))
        profile = tmp_path / "profile"
        ext_id = seed_unpacked_extension(profile, ext)
        assert ext_id == unpacked_extension_id(ext.resolve())
        prefs = json.loads((profile / "Default" / "Preferences").read_text())
        row = prefs["extensions"]["settings"][ext_id]
        assert row["location"] == 4
        assert row["path"] == str(ext.resolve())
        assert row["from_webstore"] is False
        assert "storage" in row["granted_permissions"]["api"]
        assert "http://127.0.0.1:8100/*" in row["granted_permissions"]["explicit_host"]
        assert prefs["extensions"]["ui"]["developer_mode"] is True
        # second seed keeps first_install_time
        first = row["first_install_time"]
        seed_unpacked_extension(profile, ext)
        again = json.loads((profile / "Default" / "Preferences").read_text())
        assert again["extensions"]["settings"][ext_id]["first_install_time"] == first

    def test_sync_copy_skips_metadata_and_purges_shared_path(self, tmp_path, monkeypatch):
        from agent.services import chrome_nicks as cn
        src = tmp_path / "src-extension"
        src.mkdir()
        (src / "manifest.json").write_text(json.dumps({
            "manifest_version": 3,
            "name": "Flow Kit",
            "version": "0.3.0",
            "permissions": ["storage", "webRequest", "declarativeNetRequest"],
            "declarative_net_request": {
                "rule_resources": [{"id": "referer_rules", "enabled": True, "path": "rules.json"}],
            },
        }))
        (src / "background.js").write_text("const BAKED_PROFILE_ID = null;\nvoid 0;\n")
        (src / "rules.json").write_text("[]\n")
        meta = src / "_metadata" / "generated_indexed_rulesets"
        meta.mkdir(parents=True)
        (meta / "_ruleset1").write_bytes(b"stale")
        monkeypatch.setattr(cn, "extension_dir", lambda: src)
        profile = tmp_path / "nick-a"
        shared_id = cn.unpacked_extension_id(src.resolve())
        cn.seed_unpacked_extension(profile, src.resolve())
        copy = cn.sync_extension_copy(profile, profile_id="nick-a")
        assert (copy / "background.js").is_file()
        assert not (copy / "_metadata").exists()
        assert not (copy / "rules.json").exists()
        assert json.loads((copy / "profile.json").read_text()) == {"profileId": "nick-a"}
        assert 'const BAKED_PROFILE_ID = "nick-a";' in (copy / "background.js").read_text()
        copied_version = json.loads((copy / "manifest.json").read_text())["version"]
        assert copied_version != "0.3.0"
        assert copied_version.startswith("0.3.")
        copied = json.loads((copy / "manifest.json").read_text())
        assert "declarative_net_request" not in copied
        assert "declarativeNetRequest" not in copied["permissions"]
        assert "webRequest" in copied["permissions"]
        new_id = cn.seed_unpacked_extension(profile, copy)
        assert new_id != shared_id
        prefs = json.loads((profile / "Default" / "Preferences").read_text())
        settings = prefs["extensions"]["settings"]
        assert shared_id not in settings
        assert settings[new_id]["path"] == str(copy)
        assert "declarativeNetRequest" not in settings[new_id]["granted_permissions"]["api"]

    def test_drop_stale_lock_removes_zombie_symlink(self, tmp_path):
        from agent.services.chrome_nicks import drop_stale_chrome_lock
        lock = tmp_path / "SingletonLock"
        sock = tmp_path / "SingletonSocket"
        lock.symlink_to("host-99999999")
        sock.symlink_to("/tmp/gone")
        assert drop_stale_chrome_lock(tmp_path) is True
        assert not lock.exists()
        assert not sock.exists()


class TestAccountsAPI:
    def test_upsert_list_redacts_then_reveal(self, api_client):
        client, _path = api_client
        res = client.post("/api/accounts", json={
            "id": "nick-a",
            "label": "A",
            "project_id": PA,
            "proxy_url": "http://alice:s3cret@203.0.113.10:29419/",
            "note": "",
            "enabled": True,
        })
        assert res.status_code == 200, res.text
        body = res.json()
        assert "s3cret" in body["proxy_url"]
        listed = client.get("/api/accounts").json()["accounts"]
        assert listed[0]["proxy_url"] == ""
        assert "***" in listed[0]["proxy_display"]
        one = client.get("/api/accounts/nick-a?reveal=true").json()
        assert "s3cret" in one["proxy_url"]

    def test_check_proxy_static_path_not_captured_as_id(self, api_client, monkeypatch):
        client, _path = api_client

        async def fake_check(url):
            return {"ok": True, "proxy": "http://alice:***@203.0.113.10:29419", "egress_ip": "198.51.100.9"}

        monkeypatch.setattr(accounts_api, "check_proxy", fake_check)
        res = client.post("/api/accounts/check-proxy", json={
            "proxy_url": "http://alice:s3cret@203.0.113.10:29419/",
        })
        assert res.status_code == 200, res.text
        assert res.json()["egress_ip"] == "198.51.100.9"

    def test_bad_proxy_is_400(self, api_client):
        client, _path = api_client
        res = client.post("/api/accounts", json={
            "id": "nick-a",
            "proxy_url": "socks4://203.0.113.10:1080",
        })
        assert res.status_code == 400

    def test_list_attaches_worker_and_api_checklist(self, api_client, monkeypatch):
        class Flow(FakeFlow):
            def workers(self):
                return [{
                    "profile_id": "nick-a",
                    "project_id": PA,
                    "available": True,
                    "in_flight": 0,
                    "chat_session": False,
                    "flow_key_present": False,
                }]

        client, _path = api_client
        monkeypatch.setattr(accounts_api, "get_flow_client", lambda: Flow())
        monkeypatch.setattr(accounts_api, "launch_status", lambda nick_id: {
            "chrome_running": True,
            "pid": 1,
            "data_dir": "/tmp",
            "bridge_port": None,
        })
        res = client.post("/api/accounts", json={
            "id": "nick-a",
            "label": "A",
            "project_id": PA,
            "proxy_url": "http://alice:s3cret@203.0.113.10:29419/",
            "note": "",
            "enabled": True,
        })
        assert res.status_code == 200, res.text
        listed = client.get("/api/accounts").json()["accounts"][0]
        assert listed["connected"] is True
        assert listed["next"] == "need_ingredients"
        by_id = {a["id"]: a for a in listed["apis"]}
        assert by_id["t2v"]["status"] == "ok"
        assert by_id["r2v"]["reason"] == "need_ingredients"
        assert by_id["upscale"]["status"] == "blocked"


class TestNickApiStatus:
    def test_chrome_off(self):
        from agent.services.accounts import nick_api_status, nick_next_action
        row = {
            "chrome_running": False,
            "has_proxy": True,
            "project_id": PA,
            "connected": False,
            "worker": None,
        }
        assert nick_next_action(row) == "need_chrome"
        by_id = {a["id"]: a for a in nick_api_status(row)}
        assert by_id["t2v"]["reason"] == "need_chrome"
        assert by_id["upscale"]["status"] == "blocked"

    def test_r2v_needs_ingredients(self):
        from agent.services.accounts import nick_api_status, nick_next_action
        row = {
            "chrome_running": True,
            "has_proxy": True,
            "project_id": PA,
            "connected": True,
            "worker": {"chat_session": False},
        }
        assert nick_next_action(row) == "need_ingredients"
        by_id = {a["id"]: a for a in nick_api_status(row)}
        assert by_id["t2v"]["status"] == "ok"
        assert by_id["i2v"]["status"] == "ok"
        assert by_id["r2v"]["reason"] == "need_ingredients"

    def test_ready_including_r2v(self):
        from agent.services.accounts import nick_api_status, nick_next_action
        row = {
            "chrome_running": True,
            "has_proxy": True,
            "project_id": PA,
            "connected": True,
            "worker": {"chat_session": True},
        }
        assert nick_next_action(row) is None
        by_id = {a["id"]: a for a in nick_api_status(row)}
        assert by_id["r2v"]["status"] == "ok"

    def test_disabled_manually(self):
        from agent.services.accounts import nick_api_status, nick_next_action
        row = {
            "id": "ghost-nick",
            "enabled": False,
            "chrome_running": True,
            "has_proxy": True,
            "project_id": PA,
            "connected": True,
            "worker": {"chat_session": True},
        }
        assert nick_next_action(row) == "disabled"
        by_id = {a["id"]: a for a in nick_api_status(row)}
        assert by_id["t2v"]["status"] == "blocked"
        assert by_id["t2v"]["reason"] == "disabled"

    def test_disabled_auth_expired_needs_relogin(self, monkeypatch):
        from unittest.mock import MagicMock
        from agent.services.accounts import nick_api_status, nick_next_action
        incidents = MagicMock()
        incidents.get_incidents.return_value = [
            {"job_id": "ghost-nick", "error_code": "ACCOUNT_AUTH_EXPIRED", "status": "OPEN"},
        ]
        monkeypatch.setattr("agent.services.incident_manager.get_incident_manager",
                            lambda: incidents)
        row = {
            "id": "ghost-nick",
            "enabled": False,
            "chrome_running": True,
            "has_proxy": True,
            "project_id": PA,
            "connected": True,
            "worker": {"chat_session": True},
        }
        assert nick_next_action(row) == "need_relogin"
        by_id = {a["id"]: a for a in nick_api_status(row)}
        assert by_id["t2v"]["status"] == "need"
        assert by_id["t2v"]["reason"] == "need_relogin"


class TestNetlogSession:
    def test_streamchat_envelope(self):
        from agent.main import _session_from_netlog
        sid = "da9d4724-5d1c-4b19-bf13-c24ea499e41f"
        inner = json.dumps([sid, [[[["hi"]]]], ["projects/" + PA]])
        freq = json.dumps([None, inner])
        url = (
            "https://flow.google.com/_/AiSandboxAngularFrontend/data/"
            "google.internal.labs.aisandbox.proto.flow.agent.v1."
            "FlowCreationAgentService/StreamChat"
        )
        assert _session_from_netlog(url, freq) == sid


class TestProxyHealthMonitoring:
    def test_get_proxy_health_report(self, monkeypatch, tmp_path):
        from agent.services import proxy_checker as pc
        from agent.services import proxy_pool as pp
        from agent.services import accounts as acc

        # Provide a clean proxy pool file
        pool_file = tmp_path / "proxy_pool.json"
        pool_file.write_text(json.dumps({
            "proxies": ["http://user:pass@103.82.194.60:28872", "http://user:pass@103.166.184.92:28531"],
            "current_index": 0,
        }))
        acc_file = tmp_path / "accounts.json"
        acc_file.write_text(json.dumps({
            "accounts": [{
                "id": "test-nick",
                "label": "Test Nick",
                "project_id": "86c99e42-32ab-4445-9f21-a1557d6ec854",
                "proxy_url": "http://user:pass@103.82.194.60:28872",
                "enabled": True,
            }]
        }))

        monkeypatch.setattr(pp, "PROXY_POOL_FILE", pool_file)
        monkeypatch.setattr(acc, "ACCOUNTS_FILE", acc_file)

        report = pc.get_proxy_health_report(reveal=False)
        assert report["ok"] is True
        assert report["summary"]["total"] >= 2
        # Passwords must be redacted
        for p in report["proxies"]:
            assert "pass" not in p["proxy_url"]

    def test_check_all_proxies_health_mocked(self, monkeypatch, tmp_path):
        from agent.services import proxy_checker as pc
        from agent.services import proxy_pool as pp
        from agent.services import accounts as acc

        pool_file = tmp_path / "proxy_pool.json"
        pool_file.write_text(json.dumps({
            "proxies": ["http://user:pass@103.82.194.60:28872"],
            "current_index": 0,
        }))
        acc_file = tmp_path / "accounts.json"
        acc_file.write_text(json.dumps({"accounts": []}))

        monkeypatch.setattr(pp, "PROXY_POOL_FILE", pool_file)
        monkeypatch.setattr(acc, "ACCOUNTS_FILE", acc_file)

        def mock_check(url, timeout=5):
            return {
                "proxy_url": url,
                "masked": "103.82.194.60:28872",
                "alive": True,
                "google_clean": True,
                "labs_accessible": True,
                "recaptcha_clean": True,
                "latency_ms": 120,
                "egress_ip": "103.82.194.60",
                "status": "CLEAN",
                "error": None,
                "timestamp": 12345.0,
            }

        monkeypatch.setattr(pc, "check_single_proxy", mock_check)
        monkeypatch.setattr(pc, "check_temporary_proxies_expiration", lambda: False)
        monkeypatch.setattr(pc, "run_revival_cycle", lambda: {"restored": [], "still_blocked": []})

        report = pc.check_all_proxies_health()
        assert report["ok"] is True
        assert report["summary"]["healthy"] >= 1
        assert report["proxies"][0]["status"] == "CLEAN"
        assert report["proxies"][0]["egress_ip"] == "103.82.194.60"

    def test_api_proxy_health_endpoints(self, api_client):
        client, path = api_client
        res = client.get("/api/accounts/proxy-health")
        assert res.status_code == 200
        data = res.json()
        assert data["ok"] is True
        assert "summary" in data
        assert "proxies" in data


class TestAccountRenameAndSync:
    def test_rename_account(self, tmp_path):
        from agent.services.accounts import load_accounts, rename_account, save_accounts
        acc_file = tmp_path / "accounts.json"
        save_accounts([{
            "id": "old-nick",
            "label": "Old Nick",
            "project_id": "86c99e42-32ab-4445-9f21-a1557d6ec854",
            "proxy_url": "http://user:pass@103.82.194.60:28872",
            "note": "old",
            "enabled": True,
        }], path=acc_file)

        renamed = rename_account("old-nick", "new-nick", path=acc_file)
        assert renamed["id"] == "new-nick"
        assert renamed["label"] == "Old Nick"

        loaded = load_accounts(acc_file)
        assert len(loaded) == 1
        assert loaded[0]["id"] == "new-nick"

    def test_sync_account_project(self, tmp_path):
        from agent.services.accounts import load_accounts, save_accounts, sync_account_project
        acc_file = tmp_path / "accounts.json"
        save_accounts([{
            "id": "sync-nick",
            "label": "Sync Nick",
            "project_id": "",
            "proxy_url": "",
            "note": "",
            "enabled": True,
        }], path=acc_file)

        updated = sync_account_project("sync-nick", project_id="31e25783-c1d6-4f14-a360-5d1acf612733", path=acc_file)
        assert updated["project_id"] == "31e25783-c1d6-4f14-a360-5d1acf612733"

        loaded = load_accounts(acc_file)
        assert loaded[0]["project_id"] == "31e25783-c1d6-4f14-a360-5d1acf612733"

    def test_api_rename_and_sync_project(self, api_client):
        client, path = api_client
        # Add account
        res = client.post("/api/accounts", json={
            "id": "api-nick",
            "label": "API Nick",
            "project_id": "",
            "proxy_url": "http://user:pass@103.82.194.60:28872",
        })
        assert res.status_code == 200

        # Sync project UUID
        res_sync = client.post("/api/accounts/api-nick/sync-project", json={
            "project_id": "31e25783-c1d6-4f14-a360-5d1acf612733"
        })
        assert res_sync.status_code == 200
        assert res_sync.json()["project_id"] == "31e25783-c1d6-4f14-a360-5d1acf612733"

        # Rename account
        res_rename = client.post("/api/accounts/api-nick/rename", json={
            "new_id": "api-nick-renamed"
        })
        assert res_rename.status_code == 200
        assert res_rename.json()["id"] == "api-nick-renamed"
        assert res_rename.json()["project_id"] == "31e25783-c1d6-4f14-a360-5d1acf612733"


class TestResolveKnownProxy:
    def test_resolve_from_pool(self, monkeypatch, tmp_path):
        from agent.services.proxy_url import resolve_known_proxy
        from agent.services import proxy_pool as pp

        pool_file = tmp_path / "proxy_pool.json"
        pool_file.write_text(json.dumps({
            "proxies": ["http://secretuser:realpass123@103.82.194.60:28872"],
            "current_index": 0,
        }))
        monkeypatch.setattr(pp, "PROXY_POOL_FILE", pool_file)

        redacted = "http://secretuser:***@103.82.194.60:28872"
        resolved = resolve_known_proxy(redacted)
        assert resolved == "http://secretuser:realpass123@103.82.194.60:28872"

    def test_resolve_from_accounts(self, monkeypatch, tmp_path):
        from agent.services.proxy_url import resolve_known_proxy
        from agent.services import accounts as acc

        acc_file = tmp_path / "accounts.json"
        acc_file.write_text(json.dumps({
            "accounts": [{
                "id": "nick-x",
                "proxy_url": "http://myuser:mypassword@103.166.184.92:28531",
            }]
        }))
        monkeypatch.setattr(acc, "ACCOUNTS_FILE", acc_file)

        redacted = "http://myuser:***@103.166.184.92:28531"
        resolved = resolve_known_proxy(redacted)
        assert resolved == "http://myuser:mypassword@103.166.184.92:28531"


class TestRenameChromeRunningGuard:
    def test_rename_rejected_when_chrome_running(self, monkeypatch, tmp_path):
        from agent.services.accounts import rename_account, save_accounts
        from agent.services import chrome_nicks

        acc_file = tmp_path / "accounts.json"
        save_accounts([{
            "id": "running-nick",
            "label": "Running Nick",
            "project_id": "",
            "proxy_url": "",
            "enabled": True,
        }], path=acc_file)

        monkeypatch.setattr(chrome_nicks, "chrome_running", lambda nick_id: nick_id == "running-nick")

        with pytest.raises(ValueError) as excinfo:
            rename_account("running-nick", "renamed-nick", path=acc_file)

        assert "while Chrome is running" in str(excinfo.value)


class TestProxyHealthCountersAndSorting:
    def test_error_status_counted_in_dead_and_sorted_first(self, monkeypatch):
        from agent.services import proxy_checker as pc

        monkeypatch.setattr(pc, "_PROXY_HEALTH_REGISTRY", {
            "p1": {
                "proxy_url": "http://p1:8080",
                "masked": "p1:8080",
                "alive": True,
                "status": "CLEAN",
                "latency_ms": 150,
                "quarantined": False,
            },
            "p2": {
                "proxy_url": "http://p2:8080",
                "masked": "p2:8080",
                "alive": False,
                "status": "RECAPTCHA_FAILED",
                "latency_ms": 500,
                "quarantined": False,
            },
            "p3": {
                "proxy_url": "http://p3:8080",
                "masked": "p3:8080",
                "alive": False,
                "status": "CAPTCHA_BLOCKED",
                "latency_ms": 800,
                "quarantined": True,
            },
        })
        monkeypatch.setattr(pc, "_LAST_CHECK_TIME", 100.0)
        monkeypatch.setattr(pc, "_IS_CHECKING", False)

        report = pc.get_proxy_health_report(reveal=True)
        summary = report["summary"]
        assert summary["total"] == 3
        assert summary["healthy"] == 1
        assert summary["dead"] == 1
        assert summary["blocked"] == 1
        assert summary["quarantined"] == 1

        # Check sorting: errors/blocked must sort before CLEAN
        statuses = [p["status"] for p in report["proxies"]]
        assert statuses.index("RECAPTCHA_FAILED") < statuses.index("CLEAN")
        assert statuses.index("CAPTCHA_BLOCKED") < statuses.index("CLEAN")


class TestProxyRotationAndResolution:
    @pytest.mark.asyncio
    async def test_check_proxy_resolves_redacted(self, monkeypatch, tmp_path):
        from agent.services import chrome_nicks
        from agent.services import proxy_pool as pp
        from unittest.mock import AsyncMock, patch

        pool_file = tmp_path / "proxy_pool.json"
        pool_file.write_text(json.dumps({
            "proxies": ["http://user1:secretpass@1.2.3.4:5678"],
            "current_index": 0,
        }))
        monkeypatch.setattr(pp, "PROXY_POOL_FILE", pool_file)

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.text.return_value = "1.2.3.4"

        # Mock aiohttp session
        class MockSession:
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                pass
            def get(self, url, proxy=None):
                class MockGet:
                    async def __aenter__(self_inner):
                        assert "secretpass" in proxy
                        return mock_resp
                    async def __aexit__(self_inner, *args):
                        pass
                return MockGet()

        with patch("aiohttp.ClientSession", return_value=MockSession()):
            result = await chrome_nicks.check_proxy("http://user1:***@1.2.3.4:5678")
            assert result["ok"] is True
            assert result["egress_ip"] == "1.2.3.4"

    @pytest.mark.asyncio
    async def test_rotate_nick_proxy_preserves_path(self, tmp_path):
        from agent.services.proxy_pool import rotate_nick_proxy, save_proxy_pool
        from agent.services.accounts import load_accounts, save_accounts

        pool_file = tmp_path / "proxy_pool.json"
        acc_file = tmp_path / "accounts.json"

        save_proxy_pool({
            "proxies": ["http://u:p@10.0.0.1:8080", "http://u:p@10.0.0.2:8080"],
            "current_index": 0,
        }, path=pool_file)

        save_accounts([{
            "id": "my-nick",
            "label": "My Nick",
            "project_id": "",
            "proxy_url": "http://u:p@10.0.0.1:8080",
            "enabled": True,
        }], path=acc_file)

        res = await rotate_nick_proxy("my-nick", path=pool_file, preflight=False, accounts_path=acc_file)
        assert res["ok"] is True
        # Verify that acc_file has rotated proxy
        loaded_accs = load_accounts(acc_file)
        assert loaded_accs[0]["proxy_url"] == "http://u:p@10.0.0.2:8080"
        loaded_pool = json.loads(pool_file.read_text())
        assert loaded_pool["current_index"] == 1



