"""Video recovery and retry regressions; no Google requests or live DB writes."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
import urllib.error

import pytest

from agent.sdk.services import operations as ops
from agent.worker import processor


def operation(name="render-1", status="PENDING", **extra):
    return {"operation": {"name": name}, "status": f"MEDIA_GENERATION_STATUS_{status}", **extra}


def fake_poll_clock(monkeypatch):
    clock = SimpleNamespace(value=0.0)
    monkeypatch.setattr(ops, "time", SimpleNamespace(monotonic=lambda: clock.value))

    async def sleep(seconds):
        clock.value += seconds

    monkeypatch.setattr(ops.asyncio, "sleep", sleep)
    return clock


@pytest.mark.asyncio
@pytest.mark.parametrize("reason,code", [
    ("LOW_PRIORITY_ONLY: ask_for_permission", "low_priority_only"),
    ("PUBLIC_ERROR_UNSAFE_GENERATION", "content_policy_violation"),
    ("low-priority render timed out", "upstream_timeout"),
])
async def test_terminal_poll_keeps_reason(monkeypatch, reason, code):
    fake_poll_clock(monkeypatch)
    client = SimpleNamespace(check_video_status=AsyncMock(return_value={"data": {"operations": [
        operation(status="FAILED", error=reason, error_code=code)]}}))
    result = await ops._poll_operations(client, [operation()])
    assert result == {"error": reason, "error_code": code, "retryable": False}


@pytest.mark.asyncio
async def test_partial_poll_response_does_not_lose_pending_handle(monkeypatch):
    fake_poll_clock(monkeypatch)
    client = SimpleNamespace(check_video_status=AsyncMock(side_effect=[
        {"data": {"operations": [operation("one", "SUCCESSFUL")]}},
        {"data": {"operations": [operation("two", "SUCCESSFUL")]}},
    ]))
    result = await ops._poll_operations(client, [operation("one"), operation("two")])
    assert client.check_video_status.await_count == 2
    assert len(result["data"]["operations"]) == 2


@pytest.mark.asyncio
async def test_poll_rpc_time_counts_toward_timeout(monkeypatch):
    clock = fake_poll_clock(monkeypatch)

    async def slow_poll(_):
        clock.value += 50
        return {"data": {"operations": [operation()]}}

    client = SimpleNamespace(check_video_status=AsyncMock(side_effect=slow_poll))
    result = await ops._poll_operations(client, [operation()], timeout=30)
    assert result["error_code"] == "upstream_timeout"
    assert result["retryable"] is False
    assert client.check_video_status.await_count == 1


@pytest.mark.asyncio
async def test_hung_poll_is_cancelled_at_deadline(monkeypatch):
    monkeypatch.setattr(ops, "VIDEO_POLL_INTERVAL", 0)
    cancelled = asyncio.Event()

    async def hung_poll(_):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    client = SimpleNamespace(check_video_status=hung_poll)
    result = await asyncio.wait_for(ops._poll_operations(client, [operation()], timeout=0.02), timeout=1)
    assert result["error_code"] == "upstream_timeout"
    assert cancelled.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["generate_scene_video", "generate_scene_video_refs", "upscale_scene_video"])
async def test_restart_resumes_all_saved_workflow_handles_without_sources(monkeypatch, method):
    saved = [operation("one", _workflow_mode=True, _primary_media_id="media-one"),
             operation("two", _workflow_mode=True, _primary_media_id="media-two")]
    update = AsyncMock()
    monkeypatch.setattr(ops.crud, "update_request", update)
    await ops._save_video_operations("request-one", saved)
    serialized = update.call_args.kwargs["request_id"]
    monkeypatch.setattr(ops.crud, "get_request", AsyncMock(return_value={"request_id": serialized}))
    poll = AsyncMock(return_value={"data": {"operations": saved}})
    monkeypatch.setattr(ops, "_poll_operations", poll)
    client = AsyncMock()
    service = ops.OperationService(client, Mock())
    await getattr(service, method)({}, "VERTICAL", request_id="request-one")
    assert poll.call_args.args[1] == saved
    client.generate_video.assert_not_called()
    client.generate_video_from_references.assert_not_called()
    client.upscale_video.assert_not_called()


@pytest.mark.asyncio
async def test_old_workflow_without_media_handle_is_not_resubmitted(monkeypatch):
    monkeypatch.setattr(ops, "USE_BATCH_RPC", False)
    monkeypatch.setattr(ops.crud, "get_request", AsyncMock(return_value={
        "request_id": "11111111-2222-4333-8444-555555555555"}))
    client = AsyncMock()
    result = await ops.OperationService(client, Mock()).generate_scene_video({}, "VERTICAL", "request-one")
    assert result["retryable"] is False
    client.generate_video.assert_not_called()


def test_workflow_ack_does_not_require_redundant_media_list():
    result = ops._extract_operations({"data": {"workflows": [
        {"name": "workflow-one", "metadata": {"primaryMediaId": "media-one"}}]}})
    assert result[0]["_primary_media_id"] == "media-one"


def test_request_history_uses_operation_owner_not_last_concurrent_route(monkeypatch):
    from agent.api.flow import _save_video_replay
    from agent.services import flow_failover
    save = Mock()
    monkeypatch.setattr(flow_failover, "save_operation_replay", save)
    client = SimpleNamespace(_last_route={"profile_id": "nick-b"},
                             _operation_profiles={"render-1": "nick-a"})
    _save_video_replay(client, {"data": {"operations": [operation("operations/render-1")]}},
                       "generate_video", {})
    assert save.call_args.kwargs["worker_id"] == "nick-a"


def test_unknown_operation_owner_does_not_borrow_last_route(monkeypatch):
    from agent.api.flow import _save_video_replay
    from agent.services import flow_failover
    save = Mock()
    monkeypatch.setattr(flow_failover, "save_operation_replay", save)
    _save_video_replay(SimpleNamespace(_last_route={"profile_id": "nick-b"}),
                       {"operations": [operation()]}, "generate_video", {})
    assert save.call_args.kwargs["worker_id"] == ""


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [
    {"error": "LOW_PRIORITY_ONLY: ask_for_permission"},
    {"error": "PUBLIC_ERROR_UNSAFE_GENERATION"},
    {"error": "Media not found", "retryable": False},
    {"error": "deadline", "error_code": "upstream_timeout"},
])
async def test_terminal_failure_never_reuploads_or_retries(monkeypatch, error):
    update = AsyncMock()
    recovery = AsyncMock(return_value=True)
    monkeypatch.setattr(processor.crud, "update_request", update)
    monkeypatch.setattr(processor, "_mark_scene_failed", AsyncMock())
    monkeypatch.setattr(processor, "_recover_entity_not_found", recovery)
    await processor._handle_failure("req", {"type": "GENERATE_VIDEO"}, error)
    assert update.call_args.kwargs["status"] == "FAILED"
    recovery.assert_not_called()


@pytest.mark.asyncio
async def test_not_found_recovery_consumes_retry_budget(monkeypatch):
    update = AsyncMock()
    recovery = AsyncMock(return_value=True)
    monkeypatch.setattr(processor.crud, "update_request", update)
    monkeypatch.setattr(processor, "_mark_scene_failed", AsyncMock())
    monkeypatch.setattr(processor, "_recover_entity_not_found", recovery)
    await processor._handle_failure("req", {"type": "GENERATE_VIDEO", "retry_count": 0}, {"error": "Media not found"})
    assert update.call_args.kwargs["retry_count"] == 1
    await processor._handle_failure("req", {"type": "GENERATE_VIDEO", "retry_count": processor.MAX_RETRIES - 1},
                                    {"error": "Media not found"})
    assert update.call_args.kwargs["status"] == "FAILED"
    assert recovery.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("req", [
    {"type": "GENERATE_VIDEO_REFS"},
    {"type": "GENERATE_VIDEO", "request_id": "existing-render"},
])
async def test_refs_and_accepted_jobs_do_not_wait_for_scene_image(monkeypatch, req):
    monkeypatch.setattr(processor.crud, "get_scene", AsyncMock(return_value={"id": "scene-one"}))
    assert await processor._prerequisites_met(req, "VERTICAL")


def studio_modules():
    import auto_tvc_server as tvc
    import batch_image_studio as batch
    import fashion_lookbook_studio as lookbook
    return tvc, batch, lookbook


@pytest.mark.parametrize("module_index", [0, 1, 2])
def test_studio_does_not_resubmit_video_after_network_timeout(monkeypatch, module_index):
    studio = studio_modules()[module_index]
    send = Mock(side_effect=TimeoutError("response lost after submission"))
    monkeypatch.setattr(studio.urllib.request, "urlopen", send)
    monkeypatch.setattr(studio.time, "sleep", lambda _: None)
    with pytest.raises(TimeoutError):
        studio.call_flowkit_api("/api/flow/generate-video", {})
    assert send.call_count == 1


@pytest.mark.parametrize("module_index", [0, 1, 2])
def test_exhausted_rate_limit_raises_instead_of_returning_none(monkeypatch, module_index):
    studio = studio_modules()[module_index]
    send = Mock(side_effect=urllib.error.HTTPError("url", 429, "rate limit", {}, None))
    monkeypatch.setattr(studio.urllib.request, "urlopen", send)
    monkeypatch.setattr(studio.time, "sleep", lambda _: None)
    with pytest.raises(RuntimeError, match="429"):
        studio.call_flowkit_api("/api/flow/upload-image", {}, max_retries=2)
    assert send.call_count == 2


def test_tvc_downloads_success_even_after_old_300_second_cutoff(monkeypatch, tmp_path):
    tvc = studio_modules()[0]
    clock = SimpleNamespace(value=0.0)
    monkeypatch.setattr(tvc, "time", SimpleNamespace(time=lambda: clock.value, sleep=lambda _: None))

    def poll(*args, **kwargs):
        clock.value = 340
        op = operation(status="SUCCESSFUL")
        op["operation"]["metadata"] = {"video": {"fifeUrl": "https://video.example/clip.mp4"}}
        return {"operations": [op]}

    monkeypatch.setattr(tvc, "call_flowkit_api", poll)
    download = Mock()
    monkeypatch.setattr(tvc.urllib.request, "urlretrieve", download)
    monkeypatch.setattr(tvc, "check_video_has_audio", lambda _: True)
    failed = Mock()
    assert tvc.batch_poll_videos_flowkit({1: "render-1"}, "job", tmp_path, on_clip_failed=failed) == {1: tmp_path / "clip_1.mp4"}
    failed.assert_not_called()
    download.assert_called_once()
    assert tvc.FLOWKIT_VIDEO_WAIT_SECONDS >= tvc.VIDEO_POLL_TIMEOUT + 45
