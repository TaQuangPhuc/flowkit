"""Low-priority polling, existing failover mappings and proxy lifecycle tests."""
import asyncio
import json
import sqlite3
import time
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from agent.config import DB_PATH
from agent.db.schema import init_db
from agent.main import app
from agent.services import flow_batch as fb
from agent.services.flow_client import FlowClient, get_flow_client
from agent.services.flow_failover import (
    get_failover_target,
    get_operation_failover,
    get_operation_replay,
    record_operation_failover,
    save_operation_replay,
    update_operation_failover,
    update_operation_replay,
)
from agent.services.incident_manager import get_incident_manager
from agent.services.proxy_checker import (
    _PROXY_LIFECYCLE,
    get_lifecycle_summary,
    is_quarantined,
    quarantine_proxy,
    run_revival_cycle,
)
from agent.services.proxy_pool import add_proxies_to_pool, load_proxy_pool

PA = "86c99e42-32ab-4445-9f21-a1557d6ec854"
PB = "9fd3eefc-cbda-443a-a999-4ff16002a419"
PC = "56245f99-b1b8-4ca5-b8a4-225727e7bff6"


def attach_worker(client, profile_id, project_id, *, in_flight=0, dispatched=0, recency=None):
    ws = object()
    now = recency if recency is not None else time.time()
    client._extensions[ws] = {
        "connected_at": now,
        "flow_key": "mock-token",
        "token_captured_at": now,
        "unavailable_until": 0,
        "profile_id": profile_id,
        "project_id": project_id,
        "chat_session_id": f"chat-{profile_id}",
        "in_flight": in_flight,
        "dispatched_count": dispatched,
    }
    client._profile_dispatched_counts[profile_id] = dispatched
    client._profile_in_flight[profile_id] = in_flight
    return ws


@pytest.fixture(autouse=True)
def setup_db_tables(monkeypatch):
    asyncio.run(init_db())
    test_prefixes = ("test-%", "ghost-%", "orig-%", "one-ref-%", "recovered-%", "fast-poll-%", "op-%")
    with sqlite3.connect(str(DB_PATH)) as conn:
        for pfx in test_prefixes:
            conn.execute("DELETE FROM flow_operation_failover WHERE original_op_id LIKE ? OR new_op_id LIKE ?", (pfx, pfx))
            conn.execute("DELETE FROM flow_operation_replay WHERE operation_id LIKE ?", (pfx,))
        conn.commit()
    import agent.services.proxy_checker as pc
    fake_lifecycle = {}
    monkeypatch.setattr(pc, "_PROXY_LIFECYCLE", fake_lifecycle)
    monkeypatch.setattr(pc, "load_quarantine_state", lambda: dict(fake_lifecycle))
    monkeypatch.setattr(pc, "save_quarantine_state", lambda s: None)
    yield
    with sqlite3.connect(str(DB_PATH)) as conn:
        for pfx in test_prefixes:
            conn.execute("DELETE FROM flow_operation_failover WHERE original_op_id LIKE ? OR new_op_id LIKE ?", (pfx, pfx))
            conn.execute("DELETE FROM flow_operation_replay WHERE operation_id LIKE ?", (pfx,))
        conn.commit()


@pytest.fixture
def mock_flow_client(monkeypatch, tmp_path):
    import agent.services.flow_client as module
    monkeypatch.setattr(module, "USE_BATCH_RPC", True)
    monkeypatch.setattr(module, "FLOW_PROJECT_ID", PA)
    monkeypatch.setattr(module, "FLOW_ALLOW_DEGRADED", False)

    # Routing skips accounts with enabled=False, so never read the real
    # agent/accounts.json — a nick disabled in production would silently
    # empty every candidate list here.
    from agent.services.accounts import save_accounts
    acc_path = tmp_path / "fixture-accounts.json"
    save_accounts(
        [
            {"id": "nick-a", "project_id": PA, "proxy_url": "", "enabled": True},
            {"id": "Nick-b", "project_id": PB, "proxy_url": "", "enabled": True},
            {"id": "Hienhienht98@gmail.com", "project_id": PC, "proxy_url": "", "enabled": True},
        ],
        path=acc_path,
    )
    monkeypatch.setattr("agent.services.accounts.ACCOUNTS_FILE", acc_path)

    c = FlowClient()
    c.responses = {}
    c.calls = []

    async def fake_batch_rpc(rpcid, freq, captcha_action=None, match=None, timeout=300, path=None):
        c.calls.append({"rpcid": rpcid, "freq": freq, "captcha": captcha_action, "match": match, "path": path})
        canned = c.responses.get(rpcid, {"data": ""})
        if callable(canned):
            return canned(match, freq)
        return canned

    c.batch_rpc = fake_batch_rpc
    return c


# ─── 1. SQLITE REPLAY & FAILOVER STORAGE TESTS ──────────────────────


class TestSqliteReplayAndFailoverStorage:
    def test_save_and_get_operation_replay(self):
        op_id = "test-op-001"
        payload = {"prompt": "cinematic cat", "aspect_ratio": "VIDEO_ASPECT_RATIO_PORTRAIT"}
        save_operation_replay(op_id, "generate_video", payload, worker_id="nick-a")

        record = get_operation_replay(op_id)
        assert record is not None
        assert record["operation_id"] == op_id
        assert record["request_type"] == "generate_video"
        assert record["payload"]["prompt"] == "cinematic cat"
        assert record["worker_id"] == "nick-a"
        assert record["retry_count"] == 0
        assert record["status"] == "PENDING"

    def test_operation_failover_mapping(self):
        orig_op = "orig-op-111"
        new_op = "new-op-222"
        record_operation_failover(orig_op, new_op, "nick-a", "Nick-b")

        mapping = get_operation_failover(orig_op)
        assert mapping is not None
        assert mapping["original_op_id"] == orig_op
        assert mapping["new_op_id"] == new_op
        assert mapping["original_worker_id"] == "nick-a"
        assert mapping["failover_worker_id"] == "Nick-b"

        target = get_failover_target(orig_op)
        assert target == new_op

        # Test replay status updated
        rep = get_operation_replay(orig_op)
        if rep:
            assert rep["retry_count"] == 1
            assert rep["status"] == "FAILED_OVER"


# ─── 2. FAIR LOAD BALANCING TESTS ───────────────────────────────────


class TestFairLoadBalancingAcrossThreeWorkers:
    def test_round_robin_least_dispatched_distribution(self, mock_flow_client):
        """Verify requests are distributed evenly across nick-a, Nick-b, Hienhienht98@gmail.com."""
        c = mock_flow_client
        ws_a = attach_worker(c, "nick-a", PA, in_flight=0, dispatched=0)
        ws_b = attach_worker(c, "Nick-b", PB, in_flight=0, dispatched=0)
        ws_c = attach_worker(c, "Hienhienht98@gmail.com", PC, in_flight=0, dispatched=0)

        # 1st select -> one of the idle workers
        r1 = c._select_profile()
        p1 = r1["profile_id"]
        assert p1 in {"nick-a", "Nick-b", "Hienhienht98@gmail.com"}

        # Simulate dispatching to p1
        c._profile_dispatched_counts[p1] += 1
        c._extensions[r1["ws"]]["dispatched_count"] += 1

        # 2nd select -> MUST NOT pick p1 again!
        r2 = c._select_profile()
        p2 = r2["profile_id"]
        assert p2 != p1
        assert p2 in {"nick-a", "Nick-b", "Hienhienht98@gmail.com"}

        # Simulate dispatching to p2
        c._profile_dispatched_counts[p2] += 1
        c._extensions[r2["ws"]]["dispatched_count"] += 1

        # 3rd select -> MUST pick the remaining worker p3!
        r3 = c._select_profile()
        p3 = r3["profile_id"]
        assert p3 != p1 and p3 != p2
        assert {p1, p2, p3} == {"nick-a", "Nick-b", "Hienhienht98@gmail.com"}

    def test_in_flight_prioritization(self, mock_flow_client):
        """Worker with active in_flight requests is not selected over idle worker."""
        c = mock_flow_client
        attach_worker(c, "nick-a", PA, in_flight=2, dispatched=5)
        attach_worker(c, "Nick-b", PB, in_flight=0, dispatched=10)
        attach_worker(c, "Hienhienht98@gmail.com", PC, in_flight=1, dispatched=2)

        route = c._select_profile()
        assert route["profile_id"] == "Nick-b"  # in_flight=0 wins over in_flight=1, 2


# ─── 3. TRANSPARENT AUTO-FAILOVER AT 150s TESTS ─────────────────────


class TestLowPriorityPolling:
    @pytest.mark.parametrize("elapsed", [60, 120, 160])
    async def test_slow_render_never_replays_or_quarantines(self, mock_flow_client, monkeypatch, elapsed):
        c = mock_flow_client
        op = "ghost-slow-low-priority"
        save_operation_replay(op, "generate_video", {"prompt": "walk"}, "nick-a")
        c._operation_start_time[op] = time.time() - elapsed
        c._operation_polls[op] = 1000
        monkeypatch.setattr(c, "_find_operation_media", AsyncMock(return_value=(None, "Media not found.")))
        generate = AsyncMock()
        monkeypatch.setattr(c, "generate_video", generate)
        quarantine = AsyncMock()
        monkeypatch.setattr("agent.services.proxy_checker.quarantine_proxy", quarantine)
        result = await c._poll_batch_operation(op)
        assert result["status"] == "MEDIA_GENERATION_STATUS_PENDING"
        assert get_operation_failover(op) is None
        generate.assert_not_called()
        quarantine.assert_not_called()

    async def test_smart_replay_triggers_at_timeout_and_recovers(self, mock_flow_client, monkeypatch):
        c = mock_flow_client
        op = "ghost-stalled-job"
        target_op = "ghost-recovered-job"
        save_operation_replay(op, "generate_video", {"prompt": "run across field"}, "nick-a")
        c._operation_start_time[op] = time.time() - 200  # > 180s timeout
        monkeypatch.setattr(c, "_find_operation_media", AsyncMock(return_value=(None, "Media not found.")))
        
        generate = AsyncMock(return_value={"status": 200, "data": {"operations": [{"operation": {"name": target_op}}]}})
        monkeypatch.setattr(c, "generate_video", generate)
        
        # Initial poll triggers smart replay
        res = await c._poll_batch_operation(op)
        assert res["status"] == "MEDIA_GENERATION_STATUS_PENDING"
        generate.assert_called_once()
        
        # Verify failover target is recorded
        await asyncio.sleep(0.01)
        assert get_failover_target(op) == target_op
        
        # When target finishes, poll returns SUCCESSFUL mapped back to original op
        async def poll_target(active):
            assert active == target_op
            return {"operation": {"name": active, "metadata": {"video": {"fifeUrl": "http://video.mp4"}}}, "status": "MEDIA_GENERATION_STATUS_SUCCESSFUL"}
        monkeypatch.setattr(c, "_poll_batch_operation_inner", poll_target)
        
        final_res = await c._poll_batch_operation(op)
        assert final_res["status"] == "MEDIA_GENERATION_STATUS_SUCCESSFUL"
        assert final_res["operation"]["name"] == op


    async def test_timeout_is_bounded_even_if_poll_raises(self, mock_flow_client, monkeypatch):
        from agent.config import VIDEO_POLL_TIMEOUT
        c = mock_flow_client
        op = "ghost-timeout-error"
        c._operation_start_time[op] = time.time() - VIDEO_POLL_TIMEOUT - 1
        monkeypatch.setattr(c, "_find_operation_media", AsyncMock(side_effect=OSError("Failed to fetch")))
        result = await c._poll_batch_operation(op)
        assert result["status"] == "MEDIA_GENERATION_STATUS_FAILED"
        assert result["error_code"] == "upstream_timeout"
        assert await c._poll_batch_operation(op) == result

    async def test_final_lookup_can_complete_at_deadline(self, mock_flow_client, monkeypatch):
        from agent.config import VIDEO_POLL_TIMEOUT
        c = mock_flow_client
        op = "ghost-complete-at-deadline"
        c._operation_start_time[op] = time.time() - VIDEO_POLL_TIMEOUT - 1
        successful = {"operation": {"name": op}, "status": "MEDIA_GENERATION_STATUS_SUCCESSFUL"}
        poll = AsyncMock(return_value=successful)
        monkeypatch.setattr(c, "_poll_batch_operation_inner", poll)
        assert (await c._poll_batch_operation(op))["status"].endswith("SUCCESSFUL")
        assert (await c._poll_batch_operation(op))["status"].endswith("SUCCESSFUL")
        assert poll.await_count == 1

    async def test_existing_failover_remains_pollable_after_restart(self, mock_flow_client, monkeypatch):
        c = mock_flow_client
        op, target = "orig-restart", "recovered-restart"
        record_operation_failover(op, target, "nick-a", "Nick-b")
        async def poll(active):
            assert active == target
            return {"operation": {"name": active}, "status": "MEDIA_GENERATION_STATUS_SUCCESSFUL"}
        monkeypatch.setattr(c, "_poll_batch_operation_inner", poll)
        result = await c._poll_batch_operation("operations/" + op)
        assert result["operation"]["name"] == "operations/" + op
        assert get_operation_failover(op)["status"] == "COMPLETED"

    async def test_concurrent_polls_share_one_upstream_call(self, mock_flow_client, monkeypatch):
        c = mock_flow_client
        entered, release = asyncio.Event(), asyncio.Event()
        async def poll(op):
            entered.set()
            await release.wait()
            return {"operation": {"name": op}, "status": "MEDIA_GENERATION_STATUS_PENDING"}
        upstream = AsyncMock(side_effect=poll)
        monkeypatch.setattr(c, "_poll_batch_operation_inner", upstream)
        a = asyncio.create_task(c._poll_batch_operation("op-shared"))
        await entered.wait()
        b = asyncio.create_task(c._poll_batch_operation("operations/op-shared"))
        await asyncio.sleep(0)
        release.set()
        first, second = await asyncio.gather(a, b)
        assert upstream.await_count == 1
        assert first["operation"]["name"] == "op-shared"
        assert second["operation"]["name"] == "operations/op-shared"

    async def test_permission_is_terminal_without_replay(self, mock_flow_client, monkeypatch):
        c = mock_flow_client
        monkeypatch.setattr(c, "_find_operation_media", AsyncMock(return_value=(None, "ask_for_permission")))
        result = await c._poll_batch_operation("op-permission")
        assert result["error_code"] == "low_priority_only"
        assert get_operation_failover("op-permission") is None

    def test_pin_restored_from_existing_mapping(self, mock_flow_client):
        record_operation_failover("orig-pin", "recovered-pin", "nick-a", "Nick-b")
        assert mock_flow_client._resolve_pin(operation_id="orig-pin") == "Nick-b"


# ─── 4. CLOSED-LOOP PROXY RECYCLING TESTS ───────────────────────────


class TestClosedLoopProxyRecycling:
    def test_quarantine_and_revival_cycle(self, monkeypatch):
        proxy = "http://user:pass@1.2.3.4:8080"
        # Revival purges proxies absent from accounts+pool; keep the fixture
        # proxy relevant without writing to the real pool file.
        monkeypatch.setattr("agent.services.proxy_pool.load_proxy_pool",
                            lambda path=None: {"proxies": [proxy]})
        monkeypatch.setattr("agent.services.proxy_pool.add_proxies_to_pool",
                            lambda proxies, path=None: {"proxies": list(proxies)})
        quarantine_proxy(proxy, reason="PUBLIC_ERROR_UNUSUAL_ACTIVITY", cooldown_seconds=900)
        assert is_quarantined(proxy) is True

        summary = get_lifecycle_summary()
        assert summary["quarantined_count"] >= 1

        # Simulate Google Labs unblocking the proxy: check_single_proxy returns CLEAN
        def mock_clean_probe(p, timeout=5):
            return {
                "proxy_url": p,
                "status": "CLEAN",
                "google_clean": True,
                "recaptcha_clean": True,
                "latency_ms": 120,
            }
        monkeypatch.setattr("agent.services.proxy_checker.check_single_proxy", mock_clean_probe)

        # Run revival cycle with probe_interval=0 to test probe trigger
        revival = run_revival_cycle(probe_interval=0)
        assert len(revival["restored"]) >= 1
        assert any(item["masked"] == "1.2.3.4:8080" for item in revival["restored"])

        # Proxy should no longer be quarantined!
        assert is_quarantined(proxy) is False

        # Incident should be logged as HEALED
        incidents = get_incident_manager().get_incidents(module="proxy")
        assert any(inc.get("error_code") == "PROXY_LEAKY_BUCKET_CLEARED" for inc in incidents)


# ─── 5. FASTAPI ENDPOINT PERSISTENCE TESTS ──────────────────────────


class TestFastApiEndpointsReplayPersistence:
    def test_generate_video_and_generate_video_with_references_persist_replay(self, monkeypatch):
        test_client = TestClient(app)

        mock_op = "operations/e5f6g7h8-1111-2222-3333-444455556666"
        async def mock_generate_video(*args, **kwargs):
            return {
                "status": 200,
                "data": {"operations": [{"name": mock_op}]},
            }
        monkeypatch.setattr(get_flow_client(), "generate_video", mock_generate_video)
        monkeypatch.setattr(get_flow_client(), "_extensions", {object(): {"flow_key": "k"}})

        # 1. Test POST /api/flow/generate-video
        payload1 = {
            "prompt": "a cybernetic wolf howling at the neon moon",
            "aspect_ratio": "VIDEO_ASPECT_RATIO_PORTRAIT",
            "model_family": "veo",
            "duration_s": 8,
        }
        res1 = test_client.post("/api/flow/generate-video", json=payload1)
        assert res1.status_code == 200

        clean_id = mock_op.removeprefix("operations/")
        rep1 = get_operation_replay(clean_id)
        assert rep1 is not None
        assert rep1["request_type"] == "generate_video"
        assert rep1["payload"]["prompt"] == payload1["prompt"]

        # 2. Test POST /api/flow/generate-video-with-references
        mock_op_refs = "operations/ref-op-9999-8888-7777-666655554444"
        async def mock_generate_video_refs(*args, **kwargs):
            return {
                "status": 200,
                "data": {"operations": [{"name": mock_op_refs}]},
            }
        monkeypatch.setattr(get_flow_client(), "generate_video_from_references", mock_generate_video_refs)

        payload2 = {
            "reference_media_ids": ["12345678-1234-1234-1234-1234567890ab", "87654321-4321-4321-4321-ba0987654321"],
            "prompt": "two warriors clashing swords",
            "project_id": "test-pid",
            "scene_id": "test-sid",
            "aspect_ratio": "VIDEO_ASPECT_RATIO_PORTRAIT",
            "model_family": "veo",
            "duration_s": 8,
        }
        res2 = test_client.post("/api/flow/generate-video-with-references", json=payload2)
        assert res2.status_code == 200

        clean_ref_id = mock_op_refs.removeprefix("operations/")
        rep2 = get_operation_replay(clean_ref_id)
        assert rep2 is not None
        assert rep2["request_type"] == "generate_video_refs"
        assert rep2["payload"]["prompt"] == payload2["prompt"]


# ─── 6. CROSS-WORKER SMART REPLAY TESTS ─────────────────────────────


class TestCrossWorkerSmartReplay:
    def test_pick_failover_worker_excludes_origin(self, mock_flow_client):
        c = mock_flow_client
        attach_worker(c, "nick-a", PA)
        attach_worker(c, "Nick-b", PB)
        assert c._pick_failover_worker(exclude="nick-a") == "Nick-b"
        assert c._pick_failover_worker(exclude="") in {"nick-a", "Nick-b"}

    def test_pick_failover_worker_skips_unavailable_and_no_project(self, mock_flow_client):
        c = mock_flow_client
        attach_worker(c, "nick-a", PA)
        ws_b = attach_worker(c, "Nick-b", PB)
        c._extensions[ws_b]["unavailable_until"] = time.time() + 600
        attach_worker(c, "no-project-nick", "")
        assert c._pick_failover_worker(exclude="") == "nick-a"

    async def test_t2v_replay_moves_to_other_worker(self, mock_flow_client, monkeypatch):
        c = mock_flow_client
        attach_worker(c, "nick-a", PA)
        attach_worker(c, "Nick-b", PB)
        op = "ghost-t2v-cross"
        save_operation_replay(op, "generate_video", {"prompt": "walk"}, "nick-a")
        c._operation_start_time[op] = time.time() - 200
        monkeypatch.setattr(c, "_find_operation_media",
                            AsyncMock(return_value=(None, "Media not found.")))

        calls = {}
        async def fake_gen(start_image_media_id=None, prompt="", project_id="",
                           scene_id="", aspect_ratio=None, end_image_media_id=None,
                           user_paygate_tier=None, *, profile_id=None):
            calls.update(start=start_image_media_id, profile_id=profile_id,
                         project_id=project_id)
            c._operation_profiles["ghost-t2v-new"] = profile_id or ""
            return {"status": 200, "data": {"operations": [{"operation": {"name": "ghost-t2v-new"}}]}}
        monkeypatch.setattr(c, "generate_video", fake_gen)

        res = await c._poll_batch_operation(op)
        assert res["status"] == "MEDIA_GENERATION_STATUS_PENDING"
        for _ in range(50):
            if calls:
                break
            await asyncio.sleep(0.01)
        assert calls["profile_id"] == "Nick-b"
        assert calls["start"] is None
        assert calls["project_id"] == ""
        assert get_operation_failover(op)["failover_worker_id"] == "Nick-b"

    async def test_i2v_replay_reuploads_media_to_other_worker(self, mock_flow_client, monkeypatch):
        c = mock_flow_client
        attach_worker(c, "nick-a", PA)
        attach_worker(c, "Nick-b", PB)
        op = "ghost-i2v-cross"
        save_operation_replay(op, "generate_video",
                              {"prompt": "dance", "start_image_media_id": "src-media-1"},
                              "nick-a")
        c._operation_start_time[op] = time.time() - 200
        monkeypatch.setattr(c, "_find_operation_media",
                            AsyncMock(return_value=(None, "Media not found.")))
        reupload = AsyncMock(return_value=["moved-media-1"])
        monkeypatch.setattr(c, "_reupload_media_to_worker", reupload)

        calls = {}
        async def fake_gen(start_image_media_id=None, prompt="", project_id="",
                           scene_id="", aspect_ratio=None, end_image_media_id=None,
                           user_paygate_tier=None, *, profile_id=None):
            calls.update(start=start_image_media_id, profile_id=profile_id)
            c._operation_profiles["ghost-i2v-new"] = profile_id or ""
            return {"status": 200, "data": {"operations": [{"operation": {"name": "ghost-i2v-new"}}]}}
        monkeypatch.setattr(c, "generate_video", fake_gen)

        await c._poll_batch_operation(op)
        for _ in range(50):
            if calls:
                break
            await asyncio.sleep(0.01)
        reupload.assert_awaited_once_with(["src-media-1"], "Nick-b")
        assert calls["profile_id"] == "Nick-b"
        assert calls["start"] == "moved-media-1"
        assert get_operation_failover(op)["failover_worker_id"] == "Nick-b"

    async def test_i2v_replay_falls_back_same_worker_when_media_gone(self, mock_flow_client, monkeypatch):
        c = mock_flow_client
        attach_worker(c, "nick-a", PA)
        attach_worker(c, "Nick-b", PB)
        op = "ghost-i2v-expired"
        save_operation_replay(op, "generate_video",
                              {"prompt": "dance", "start_image_media_id": "src-media-1",
                               "project_id": PA},
                              "nick-a")
        c._operation_start_time[op] = time.time() - 200
        monkeypatch.setattr(c, "_find_operation_media",
                            AsyncMock(return_value=(None, "Media not found.")))
        monkeypatch.setattr(c, "_reupload_media_to_worker", AsyncMock(return_value=None))

        calls = {}
        async def fake_gen(start_image_media_id=None, prompt="", project_id="",
                           scene_id="", aspect_ratio=None, end_image_media_id=None,
                           user_paygate_tier=None, *, profile_id=None):
            calls.update(start=start_image_media_id, profile_id=profile_id,
                         project_id=project_id)
            return {"status": 200, "data": {"operations": [{"operation": {"name": "ghost-same-new"}}]}}
        monkeypatch.setattr(c, "generate_video", fake_gen)

        await c._poll_batch_operation(op)
        for _ in range(50):
            if calls:
                break
            await asyncio.sleep(0.01)
        # Media no longer fetchable -> stay on original account so its handle works.
        assert calls["profile_id"] is None
        assert calls["start"] == "src-media-1"
        assert calls["project_id"] == PA

    async def test_watchdog_fires_replay_without_polling(self, mock_flow_client, monkeypatch):
        """Poll starvation: replay must fire on a timer, not on poll arrival."""
        monkeypatch.setattr("agent.services.flow_client.SMART_REPLAY_TIMEOUT", 0.05)
        c = mock_flow_client
        attach_worker(c, "nick-a", PA)
        attach_worker(c, "Nick-b", PB)
        op = "ghost-watchdog"
        save_operation_replay(op, "generate_video", {"prompt": "walk"}, "nick-a")

        calls = {}
        async def fake_gen(start_image_media_id=None, prompt="", project_id="",
                           scene_id="", aspect_ratio=None, end_image_media_id=None,
                           user_paygate_tier=None, *, profile_id=None):
            calls["profile_id"] = profile_id
            c._operation_profiles["ghost-wd-new"] = profile_id or ""
            return {"status": 200, "data": {"operations": [{"operation": {"name": "ghost-wd-new"}}]}}
        monkeypatch.setattr(c, "generate_video", fake_gen)

        # Bind the op the way submit does — no polls at all.
        c._remember_operation(op, PA)
        for _ in range(100):
            if calls:
                break
            await asyncio.sleep(0.02)
        assert calls["profile_id"] == "Nick-b"
        assert get_operation_failover(op)["failover_worker_id"] == "Nick-b"

    async def test_watchdog_skips_finished_or_replayed_ops(self, mock_flow_client, monkeypatch):
        monkeypatch.setattr("agent.services.flow_client.SMART_REPLAY_TIMEOUT", 0.05)
        c = mock_flow_client
        attach_worker(c, "nick-a", PA)
        generate = AsyncMock()
        monkeypatch.setattr(c, "generate_video", generate)

        done_op = "ghost-wd-done"
        save_operation_replay(done_op, "generate_video", {"prompt": "x"}, "nick-a")
        c._operation_results[done_op] = {"status": "MEDIA_GENERATION_STATUS_SUCCESSFUL"}
        c._remember_operation(done_op, PA)

        busy_op = "ghost-wd-busy"
        save_operation_replay(busy_op, "generate_video", {"prompt": "x"}, "nick-a")
        c._replay_in_progress.add(busy_op)
        c._remember_operation(busy_op, PA)

        await asyncio.sleep(0.3)
        generate.assert_not_called()

    async def test_reupload_fetches_fife_and_uploads_to_target(self, mock_flow_client, monkeypatch):
        c = mock_flow_client
        monkeypatch.setattr(c, "get_media", AsyncMock(return_value={
            "status": 200, "data": {"image": {"fifeUrl": "https://x/img"}}}))
        monkeypatch.setattr("agent.services.flow_client._fetch_url_bytes",
                            lambda url, timeout=30: (b"JPEG", "image/jpeg"))
        up = AsyncMock(return_value={"status": 200, "_mediaId": "new-media-9"})
        monkeypatch.setattr(c, "upload_image", up)

        moved = await c._reupload_media_to_worker(["src-1"], "Nick-b")
        assert moved == ["new-media-9"]
        assert up.await_args.kwargs["profile_id"] == "Nick-b"
        assert up.await_args.args[1] == "image/jpeg"

        # Any failure aborts the whole move so the caller falls back.
        monkeypatch.setattr(c, "get_media", AsyncMock(return_value={"status": 404}))
        assert await c._reupload_media_to_worker(["src-1"], "Nick-b") is None


# ─── 7. WORKER-LEVEL UNUSUAL ACTIVITY CIRCUIT BREAKER ───────────────


class TestWorkerUnusualCircuitBreaker:
    async def _flag(self, c, monkeypatch):
        """Run one unpinned request that gets flagged UNUSUAL_ACTIVITY."""
        async def builder(pid):
            return {"error": ("RpcError: eb1hJf failed: [7, None, "
                              "[['type.googleapis.com/google.rpc.ErrorInfo', "
                              "['PUBLIC_ERROR_UNUSUAL_ACTIVITY']]]]")}
        return await c._run_on_profile(builder, PA)

    async def test_flags_count_strikes_without_parking(self, mock_flow_client, monkeypatch):
        """UNUSUAL_ACTIVITY rotates the proxy and counts strikes, but the nick
        stays routable — rotation retries until a request lands."""
        from unittest.mock import MagicMock
        c = mock_flow_client
        ws = attach_worker(c, "nick-a", PA)
        rotate = AsyncMock(return_value={"ok": True, "proxy": "http://x:1",
                                         "egress_ip": "9.9.9.9",
                                         "flow_tab_reloaded": True})
        monkeypatch.setattr("agent.services.proxy_pool.rotate_nick_proxy", rotate)
        audit = MagicMock()
        audit.record_request_dispatched.return_value = {}
        monkeypatch.setattr("agent.services.unusual_audit.get_unusual_audit",
                            lambda: audit)
        incidents = MagicMock()
        monkeypatch.setattr("agent.services.incident_manager.get_incident_manager",
                            lambda: incidents)

        await self._flag(c, monkeypatch)
        assert c._unusual_strikes["nick-a"] == 1
        assert c._extensions[ws].get("unavailable_until", 0) <= time.time()
        incidents.record_incident.assert_not_called()

        await self._flag(c, monkeypatch)
        assert c._unusual_strikes["nick-a"] == 2
        assert c._extensions[ws].get("unavailable_until", 0) <= time.time()
        incidents.record_incident.assert_not_called()
        assert rotate.await_count == 2

    async def test_success_resets_strikes(self, mock_flow_client, monkeypatch):
        from unittest.mock import MagicMock
        c = mock_flow_client
        ws = attach_worker(c, "nick-a", PA)
        c._unusual_strikes["nick-a"] = 2
        c._extensions[ws]["unavailable_until"] = time.time() + 1800
        audit = MagicMock()
        audit.record_request_dispatched.return_value = {}
        monkeypatch.setattr("agent.services.unusual_audit.get_unusual_audit",
                            lambda: audit)

        async def ok_builder(pid):
            return {"status": 200, "data": {"ok": True}}
        res = await c._run_on_profile(ok_builder, PA)
        assert res["status"] == 200
        assert "nick-a" not in c._unusual_strikes


# ─── 8. AUTO-DISABLE ON HARD 401 ────────────────────────────────────


class TestAutoDisableOn401:
    async def test_hard_401_disables_account(self, mock_flow_client, monkeypatch, tmp_path):
        from unittest.mock import MagicMock
        from agent.services.accounts import save_accounts, get_account
        c = mock_flow_client
        attach_worker(c, "nick-a", PA)
        audit = MagicMock()
        audit.record_request_dispatched.return_value = {}
        monkeypatch.setattr("agent.services.unusual_audit.get_unusual_audit",
                            lambda: audit)
        monkeypatch.setattr("agent.services.incident_manager.get_incident_manager",
                            lambda: MagicMock())

        acc_path = tmp_path / "accounts.json"
        save_accounts([{"id": "nick-a", "project_id": PA, "proxy_url": "",
                        "enabled": True}], path=acc_path)
        monkeypatch.setattr("agent.services.accounts.ACCOUNTS_FILE", acc_path)

        async def builder(pid):
            return {"error": "status=401 unauthorized"}
        await c._run_on_profile(builder, PA)

        saved = get_account("nick-a", path=acc_path)
        assert saved["enabled"] is False

    async def test_no_at_token_parks_without_disabling(self, mock_flow_client, monkeypatch, tmp_path):
        from unittest.mock import MagicMock
        from agent.services.accounts import save_accounts, get_account
        c = mock_flow_client
        ws = attach_worker(c, "nick-a", PA)
        audit = MagicMock()
        audit.record_request_dispatched.return_value = {}
        monkeypatch.setattr("agent.services.unusual_audit.get_unusual_audit",
                            lambda: audit)

        acc_path = tmp_path / "accounts.json"
        save_accounts([{"id": "nick-a", "project_id": PA, "proxy_url": "",
                        "enabled": True}], path=acc_path)
        monkeypatch.setattr("agent.services.accounts.ACCOUNTS_FILE", acc_path)

        async def builder(pid):
            return {"error": "NO_AT_TOKEN: tab cannot sign"}
        await c._run_on_profile(builder, PA)

        saved = get_account("nick-a", path=acc_path)
        assert saved["enabled"] is True
        assert c._extensions[ws]["unavailable_until"] > time.time() + 1500

    async def test_soft_auth_failures_disable_after_three_strikes(
        self, mock_flow_client, monkeypatch, tmp_path
    ):
        """A missing rpc envelope carries no status code, so escalate on repeats."""
        from unittest.mock import MagicMock
        from agent.services.accounts import get_account, save_accounts
        c = mock_flow_client
        ws = attach_worker(c, "nick-a", PA)
        audit = MagicMock()
        audit.record_request_dispatched.return_value = {}
        monkeypatch.setattr("agent.services.unusual_audit.get_unusual_audit", lambda: audit)
        incidents = MagicMock()
        monkeypatch.setattr("agent.services.incident_manager.get_incident_manager", lambda: incidents)

        acc_path = tmp_path / "accounts.json"
        save_accounts([{"id": "nick-a", "project_id": PA, "proxy_url": "", "enabled": True}],
                      path=acc_path)
        monkeypatch.setattr("agent.services.accounts.ACCOUNTS_FILE", acc_path)
        monkeypatch.setattr("agent.services.flow_client.AUTH_STRIKES_BEFORE_DISABLE", 3)

        async def builder(pid):
            return {"error": "FlowBatchError: no YhhmEf envelope in response (0 others)"}

        for expected in (1, 2):
            c._extensions[ws]["unavailable_until"] = 0  # park expires between attempts
            await c._run_on_profile(builder, PA)
            assert c._auth_strikes["nick-a"] == expected
            assert c._extensions[ws]["unavailable_until"] > time.time() + 1500
            assert get_account("nick-a", path=acc_path)["enabled"] is True

        c._extensions[ws]["unavailable_until"] = 0
        await c._run_on_profile(builder, PA)
        assert c._auth_strikes["nick-a"] == 3
        assert get_account("nick-a", path=acc_path)["enabled"] is False
        assert incidents.record_incident.call_args.kwargs["error_code"] == "ACCOUNT_AUTH_EXPIRED"

    async def test_success_payload_with_401_digits_does_not_disable(
        self, mock_flow_client, monkeypatch, tmp_path
    ):
        """A uuid containing "401" is not a 401. Four live nicks were disabled by this."""
        from unittest.mock import MagicMock
        from agent.services.accounts import get_account, save_accounts
        c = mock_flow_client
        ws = attach_worker(c, "nick-a", PA)
        audit = MagicMock()
        audit.record_request_dispatched.return_value = {}
        monkeypatch.setattr("agent.services.unusual_audit.get_unusual_audit", lambda: audit)
        monkeypatch.setattr("agent.services.incident_manager.get_incident_manager", lambda: MagicMock())

        acc_path = tmp_path / "accounts.json"
        save_accounts([{"id": "nick-a", "project_id": PA, "proxy_url": "", "enabled": True}],
                      path=acc_path)
        monkeypatch.setattr("agent.services.accounts.ACCOUNTS_FILE", acc_path)

        async def builder(pid):
            return {"status": 200, "data": {"operations": [
                {"operation": {"name": "b19701b7-4010-4c0f-9401-401a1c2d3e4f"}}]}}

        res = await c._run_on_profile(builder, PA)
        assert res["status"] == 200
        assert get_account("nick-a", path=acc_path)["enabled"] is True
        assert c._extensions[ws]["unavailable_until"] == 0
        assert "nick-a" not in c._auth_strikes

    async def test_error_text_with_401_digits_does_not_disable(
        self, mock_flow_client, monkeypatch, tmp_path
    ):
        from unittest.mock import MagicMock
        from agent.services.accounts import get_account, save_accounts
        c = mock_flow_client
        attach_worker(c, "nick-a", PA)
        audit = MagicMock()
        audit.record_request_dispatched.return_value = {}
        monkeypatch.setattr("agent.services.unusual_audit.get_unusual_audit", lambda: audit)
        monkeypatch.setattr("agent.services.incident_manager.get_incident_manager", lambda: MagicMock())

        acc_path = tmp_path / "accounts.json"
        save_accounts([{"id": "nick-a", "project_id": PA, "proxy_url": "", "enabled": True}],
                      path=acc_path)
        monkeypatch.setattr("agent.services.accounts.ACCOUNTS_FILE", acc_path)

        async def builder(pid):
            return {"error": "No urls for media 64fb1e17-4012-4078-8c8d-0384011ab9cd"}

        await c._run_on_profile(builder, PA)
        assert get_account("nick-a", path=acc_path)["enabled"] is True

    async def test_success_resets_auth_strikes(self, mock_flow_client, monkeypatch):
        from unittest.mock import MagicMock
        c = mock_flow_client
        attach_worker(c, "nick-a", PA)
        audit = MagicMock()
        audit.record_request_dispatched.return_value = {}
        monkeypatch.setattr("agent.services.unusual_audit.get_unusual_audit", lambda: audit)
        c._auth_strikes["nick-a"] = 2

        async def ok_builder(pid):
            return {"status": 200, "data": {"ok": True}}
        await c._run_on_profile(ok_builder, PA)
        assert "nick-a" not in c._auth_strikes


    async def test_stale_auth_strike_ages_out(self, mock_flow_client, monkeypatch, tmp_path):
        """Strikes decay: a soft failure a day apart must not disable a live nick."""
        from unittest.mock import MagicMock
        from agent.services.accounts import get_account, save_accounts
        c = mock_flow_client
        ws = attach_worker(c, "nick-a", PA)
        audit = MagicMock()
        audit.record_request_dispatched.return_value = {}
        monkeypatch.setattr("agent.services.unusual_audit.get_unusual_audit", lambda: audit)
        monkeypatch.setattr("agent.services.incident_manager.get_incident_manager", lambda: MagicMock())

        acc_path = tmp_path / "accounts.json"
        save_accounts([{"id": "nick-a", "project_id": PA, "proxy_url": "", "enabled": True}],
                      path=acc_path)
        monkeypatch.setattr("agent.services.accounts.ACCOUNTS_FILE", acc_path)
        monkeypatch.setattr("agent.services.flow_client.AUTH_STRIKES_BEFORE_DISABLE", 3)
        monkeypatch.setattr("agent.services.flow_client.AUTH_STRIKE_TTL_S", 3600)

        # Two strikes from yesterday, then one today.
        c._auth_strikes["nick-a"] = 2
        c._auth_strike_ts["nick-a"] = time.time() - 86400
        c._extensions[ws]["unavailable_until"] = 0

        async def builder(pid):
            return {"error": "FlowBatchError: no YhhmEf envelope in response (0 others)"}

        await c._run_on_profile(builder, PA)
        assert c._auth_strikes["nick-a"] == 1
        assert get_account("nick-a", path=acc_path)["enabled"] is True


# ─── 8b. SUBMISSION_OUTCOME_UNKNOWN CLASSIFICATION ──────────────────


class TestSubmissionUnknownClassification:
    async def test_success_payload_containing_marker_is_not_failed(
        self, mock_flow_client, monkeypatch
    ):
        """The marker only ever arrives in the error channel; raw_err falls back
        to the whole data payload, so matching it there fails healthy jobs."""
        from unittest.mock import MagicMock
        c = mock_flow_client
        attach_worker(c, "nick-a", PA)
        audit = MagicMock()
        audit.record_request_dispatched.return_value = {}
        monkeypatch.setattr("agent.services.unusual_audit.get_unusual_audit", lambda: audit)

        async def builder(pid):
            return {"status": 200, "data": {"operations": [
                {"status": "MEDIA_GENERATION_STATUS_PENDING",
                 "lastOutcome": "SUBMISSION_OUTCOME_UNKNOWN",
                 "operation": {"name": "operations/abc-123"}}]}}

        res = await c._run_on_profile(builder, PA)
        assert res["status"] == 200
        assert res.get("error_code") != "upstream_submission_unknown"
        assert res.get("retryable") is not False

    async def test_error_marker_is_still_non_retryable(self, mock_flow_client, monkeypatch):
        from unittest.mock import MagicMock
        c = mock_flow_client
        attach_worker(c, "nick-a", PA)
        audit = MagicMock()
        audit.record_request_dispatched.return_value = {}
        monkeypatch.setattr("agent.services.unusual_audit.get_unusual_audit", lambda: audit)

        async def builder(pid):
            return {"error": "SUBMISSION_OUTCOME_UNKNOWN: Execution context was destroyed"}

        res = await c._run_on_profile(builder, PA)
        assert res["error_code"] == "upstream_submission_unknown"
        assert res["retryable"] is False


# ─── 9. PARKED-FLEET GATE ───────────────────────────────────────────


class TestAllWorkersParkedGate:
    async def test_unpinned_work_refuses_to_burn_on_parked_nicks(self, mock_flow_client):
        c = mock_flow_client
        for nick, proj in (("nick-a", PA), ("Nick-b", PB)):
            ws = attach_worker(c, nick, proj)
            c._extensions[ws]["unavailable_until"] = time.time() + 1800

        called = []

        async def builder(pid):
            called.append(pid)
            return {"status": 200}

        res = await c._run_on_profile(builder, "")  # unpinned: PA would pin to nick-a
        assert called == []
        assert res["status"] == 503
        assert res["error_code"] == "all_workers_parked"
        assert res["retryable"] is True
        assert "nick-a" in res["error"] and "Nick-b" in res["error"]

    async def test_one_live_nick_still_serves(self, mock_flow_client):
        c = mock_flow_client
        ws_a = attach_worker(c, "nick-a", PA)
        c._extensions[ws_a]["unavailable_until"] = time.time() + 1800
        attach_worker(c, "Nick-b", PB)

        seen = {}

        async def builder(pid):
            seen["profile"] = (_route_profile(c))
            return {"status": 200}

        res = await c._run_on_profile(builder, "")  # unpinned
        assert res["status"] == 200
        assert seen["profile"] == "Nick-b"

    async def test_pinned_work_still_reaches_its_parked_nick(self, mock_flow_client):
        """Polls and media reads must stay on the nick that owns the operation."""
        c = mock_flow_client
        ws = attach_worker(c, "nick-a", PA)
        c._extensions[ws]["unavailable_until"] = time.time() + 1800

        called = []

        async def builder(pid):
            called.append(pid)
            return {"status": 200}

        res = await c._run_on_profile(builder, PA, profile_id="nick-a")
        assert called and res["status"] == 200


def _route_profile(client) -> str:
    return (client._last_route or {}).get("profile_id") or ""
