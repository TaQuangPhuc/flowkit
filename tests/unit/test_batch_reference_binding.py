"""Batch regressions: one reference owner, no blind generation replay."""
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pathlib import Path
from threading import Event
from uuid import uuid4
import urllib.error

import pytest
import batch_image_studio as studio


@pytest.fixture
def batch(monkeypatch, tmp_path):
    monkeypatch.setattr(studio, "WORK_DIR", tmp_path)
    monkeypatch.setattr(studio, "BATCH_JOBS", {})
    monkeypatch.setattr(studio, "_BATCH_LOCKS", {})
    monkeypatch.setattr(studio.time, "sleep", lambda _: None)
    class InlineScheduler:
        def register(self, bid):
            self.bid = bid
        def start(self):
            for item in studio.BATCH_JOBS[self.bid]["items"]:
                if item["status"] == "QUEUED":
                    studio.execute_single_item(self.bid, item["item_id"])
    scheduler = InlineScheduler()
    monkeypatch.setattr(studio, "batch_scheduler", lambda: scheduler)
    folder = tmp_path / "batch_test"
    folder.mkdir()
    entities = [{"index": i, "filename": f"outfit{i}.jpg", "media_id": f"legacy-{i}"} for i in range(3)]
    face = {"index": 0, "filename": "face.jpg", "media_id": "legacy-face"}
    for entity in [face] + entities:
        (folder / entity["filename"]).write_bytes(b"fixture")
    value = {"batch_id": "test", "config": {}, "face_models": [face], "outfits": entities,
             "items": [{"item_id": i + 1, "face_index": 0, "outfit_index": i // 4,
                        "status": "PENDING", "prompt": "fixture"} for i in range(12)]}
    studio.BATCH_JOBS["test"] = value
    uploads, generated = [], []
    def upload(path, reference_media_id=""):
        path.read_bytes()
        mid = str(uuid4())
        uploads.append((path.name, reference_media_id, mid))
        return mid
    def generate(endpoint, payload, **kwargs):
        generated.append(payload)
        return {"media": [{"name": str(uuid4()), "image": {"generatedImage": {"fifeUrl": "https://example.invalid/fixture"}}}]}
    monkeypatch.setattr(studio, "upload_image_flowkit", upload)
    monkeypatch.setattr(studio, "call_flowkit_api", generate)
    monkeypatch.setattr(studio.urllib.request, "urlretrieve", lambda url, path: Path(path).write_bytes(b"fixture"))
    return value, uploads, generated


def test_twelve_images_share_one_reference_owner_and_prepare_is_idempotent(batch):
    value, uploads, generated = batch
    studio.start_batch_pipeline("test")
    assert len(uploads) == 4
    anchor = uploads[0][2]
    assert uploads[0][1] == ""
    assert all(row[1] == anchor for row in uploads[1:])
    assert all(row["status"] == "COMPLETED" for row in value["items"])
    assert len(generated) == 12
    allowed = {row[2] for row in uploads}
    assert all(set(row["referenceImageMediaIds"]) <= allowed for row in generated)
    studio.prepare_batch_references("test")
    studio.execute_single_item("test", 1)
    assert len(uploads) == 4 and len(generated) == 12


def test_missing_reference_stops_before_any_generation(batch, monkeypatch):
    value, uploads, generated = batch
    (studio.WORK_DIR / "batch_test" / "outfit2.jpg").unlink()
    studio.start_batch_pipeline("test")
    assert not generated
    assert all(row["status"] == "FAILED" for row in value["items"])


@pytest.mark.parametrize("error", [studio.FlowRequestError("HTTP400 mismatch"), TimeoutError("response lost")])
def test_generation_error_does_not_replay_or_allow_blind_manual_retry(batch, monkeypatch, error):
    value, _, _ = batch
    calls = []
    def fail(*args, **kwargs):
        calls.append(True)
        raise error
    monkeypatch.setattr(studio, "call_flowkit_api", fail)
    studio.execute_single_item("test", 1)
    studio.execute_single_item("test", 1)
    assert len(calls) == 1
    assert value["items"][0]["retry_safe"] is False


def test_download_failure_preserves_media_and_does_not_regenerate(batch, monkeypatch):
    value, _, generated = batch
    def fail(*args):
        raise OSError("download failed")
    monkeypatch.setattr(studio.urllib.request, "urlretrieve", fail)
    studio.execute_single_item("test", 1)
    assert len(generated) == 1
    assert value["items"][0]["output_media_id"]
    assert value["items"][0]["retry_safe"] is False


def test_duplicate_item_calls_generate_once(batch, monkeypatch):
    value, _, generated = batch
    entered, release = Event(), Event()
    original = studio.call_flowkit_api
    def pause(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return original(*args, **kwargs)
    monkeypatch.setattr(studio, "call_flowkit_api", pause)
    with ThreadPoolExecutor(2) as pool:
        first = pool.submit(studio.execute_single_item, "test", 1)
        assert entered.wait(5)
        pool.submit(studio.execute_single_item, "test", 1).result(timeout=5)
        release.set()
        first.result(timeout=5)
    assert len(generated) == 1


def test_http_generation_wrapper_does_not_multiply_retries(monkeypatch):
    calls = []
    def fail(*args, **kwargs):
        calls.append(True)
        raise urllib.error.HTTPError("https://example.invalid", 502, "error", {}, BytesIO(b'{"retryable":false}'))
    monkeypatch.setattr(studio.urllib.request, "urlopen", fail)
    with pytest.raises(studio.FlowRequestError):
        studio.call_flowkit_api("/api/flow/generate-image", {})
    assert len(calls) == 1


def test_bulk_retry_queues_only_failed_items_and_rejects_duplicate(batch, monkeypatch):
    import json
    from types import SimpleNamespace
    import auto_tvc_server as tvc
    value, _, generated = batch
    for item in value["items"]:
        item["status"] = "COMPLETED" if item["item_id"] <= 4 else "FAILED"
        if item["status"] == "FAILED":
            item["error"] = "old reference mismatch"
    launches = []
    class DeferredThread:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
        def start(self):
            launches.append(self.kwargs)
    monkeypatch.setattr(tvc, "threading", SimpleNamespace(Thread=DeferredThread))
    def request():
        codes = []
        body = json.dumps({"batch_id": "test", "all_failed": True}).encode()
        handler = SimpleNamespace(path="/api/batch/retry", headers={"Content-Length": str(len(body))},
            rfile=BytesIO(body), wfile=BytesIO(), send_response=codes.append,
            send_header=lambda *args: None, end_headers=lambda: None)
        tvc.AutoTvcHandler.do_POST(handler)
        return codes[0], json.loads(handler.wfile.getvalue())
    status, result = request()
    assert status == 200 and result["item_ids"] == list(range(5, 13))
    assert request()[0] == 409
    assert len(launches) == 1 and launches[0]["target"] is studio.start_batch_pipeline
    launches[0]["target"](*launches[0]["args"])
    assert len(generated) == 8
    assert all(item["status"] == "COMPLETED" for item in value["items"])
    assert all("error" not in item for item in value["items"])


def test_busy_stays_durable_queued_after_more_than_three_attempts(batch, monkeypatch):
    import json
    value, _, _ = batch
    calls = []
    def busy(*args, **kwargs):
        calls.append(True)
        raise studio.FlowRequestError('busy', retry_safe=True, retry_after_s=45)
    monkeypatch.setattr(studio, 'call_flowkit_api', busy)
    for _ in range(5):
        studio.execute_single_item('test', 1)
    item = value['items'][0]
    saved = json.loads((studio.WORK_DIR / 'batch_test/batch.json').read_text())
    assert len(calls) == 5
    assert item['status'] == 'QUEUED' and item['retry_safe'] is True
    assert item['image_next_attempt_at'] >= studio.time.time() + 44
    assert saved['items'][0]['image_busy_count'] == 5
    assert not saved['stats']['is_done'] and saved['stats']['failed'] == 0


def test_scheduler_fairness_owner_exclusion_and_independent_nicks(batch, tmp_path):
    import json
    from batch_scheduler import BatchScheduler
    value, _, _ = batch
    value['items'] = [{'item_id': 1, 'status': 'QUEUED', 'face_media_id': 'a'},
                      {'item_id': 2, 'status': 'QUEUED', 'face_media_id': 'a'}]
    studio.BATCH_JOBS['second'] = {'items': [{'item_id': 1, 'status': 'QUEUED', 'face_media_id': 'a'}]}
    studio.BATCH_JOBS['third'] = {'items': [{'item_id': 1, 'status': 'QUEUED', 'face_media_id': 'b'}]}
    owners = tmp_path / 'owners.json'
    owners.write_text(json.dumps({'a': 'Nick-A', 'b': 'Nick-B'}))
    scheduler = BatchScheduler(studio, owners)
    for bid in ['test', 'second', 'third']:
        scheduler.register(bid)
    first = scheduler.pick()
    assert first == ('test', 1, 'image', 'nick-a')
    assert scheduler.pick() == ('third', 1, 'image', 'nick-b')
    assert scheduler.pick() is None
    scheduler.active.remove('nick-a')
    value['items'][0]['status'] = 'COMPLETED'
    # The competing batch gets the next nick-A slot, ahead of test's second item.
    assert scheduler.pick() == ('second', 1, 'image', 'nick-a')


def test_restart_preserves_queue_and_polls_handle_without_resubmitting(batch, monkeypatch, tmp_path):
    from batch_scheduler import BatchScheduler
    value, _, generated = batch
    value['queue_version'] = 1
    value['items'] = [
        {'item_id': 1, 'status': 'QUEUED', 'image_next_attempt_at': 9999999999},
        {'item_id': 2, 'status': 'GENERATING'},
        {'item_id': 3, 'status': 'COMPLETED', 'video_status': 'SUBMITTING'},
        {'item_id': 4, 'status': 'COMPLETED', 'video_status': 'RENDERING', 'video_op_name': 'existing-handle'},
        {'item_id': 5, 'status': 'COMPLETED', 'video_status': 'QUEUED'},
    ]
    polled = []
    monkeypatch.setattr(studio, 'start_video_poller', lambda *args: polled.append(args))
    scheduler = BatchScheduler(studio, tmp_path / 'missing')
    monkeypatch.setattr(scheduler, 'start', lambda: None)
    scheduler.recover()
    assert value['items'][0]['status'] == 'QUEUED'
    assert value['items'][1]['retry_safe'] is False
    assert value['items'][2]['video_retry_safe'] is False
    assert polled == [('test', 4)]
    assert scheduler.pick()[1:3] == (5, 'video')
    assert not generated


def test_video_busy_is_queued_then_accepted_without_reupload(batch, monkeypatch):
    value, uploads, calls = batch
    item = value['items'][0]
    item.update(status='COMPLETED', output_media_id=str(uuid4()), video_status='QUEUED')
    seen = []
    def busy(*args, **kwargs):
        raise studio.FlowRequestError('busy', retry_safe=True)
    monkeypatch.setattr(studio, 'call_flowkit_api', busy)
    studio.transfer_item_to_video('test', 1)
    assert item['video_status'] == 'QUEUED' and item['video_retry_safe'] is True
    def accepted(endpoint, payload, **kwargs):
        seen.append(payload)
        return {'operations': [{'name': 'existing-operation'}]}
    monkeypatch.setattr(studio, 'call_flowkit_api', accepted)
    monkeypatch.setattr(studio, 'start_video_poller', lambda *args: None)
    studio.transfer_item_to_video('test', 1)
    studio.transfer_item_to_video('test', 1)
    assert item['video_status'] == 'RENDERING' and item['video_op_name'] == 'existing-operation'
    assert len(seen) == 1 and not uploads
    assert seen[0]['start_image_media_id'] == item['output_media_id']


def test_no_post_when_durable_write_fails(batch, monkeypatch):
    _, _, generated = batch
    studio.prepare_batch_references('test')
    def fail(*args, **kwargs):
        raise OSError('disk full')
    monkeypatch.setattr(studio, 'save_batch_job', fail)
    with pytest.raises(OSError):
        studio.execute_single_item('test', 1)
    assert not generated


def test_scheduler_busy_releases_lane_and_respects_server_delay(batch, monkeypatch, tmp_path):
    from batch_scheduler import BatchScheduler
    value, _, _ = batch
    value['items'] = value['items'][:1]
    value['items'][0]['status'] = 'QUEUED'
    def busy(*args, **kwargs):
        raise studio.FlowRequestError('busy', retry_safe=True, retry_after_s=90)
    monkeypatch.setattr(studio, 'call_flowkit_api', busy)
    scheduler = BatchScheduler(studio, tmp_path / 'missing')
    scheduler.register('test')
    task = scheduler.pick()
    assert task
    scheduler.execute(task)
    assert not scheduler.active and not scheduler.reserved
    assert scheduler.pick(now=studio.time.time() + 30) is None
    assert scheduler.pick(now=studio.time.time() + 100)


def test_video_poll_timeout_preserves_handle_and_no_generation(batch, monkeypatch):
    value, _, generated = batch
    item = value['items'][0]
    item.update(status='COMPLETED', video_status='RENDERING', video_op_name='accepted',
                video_poll_started_at=studio.time.time() - 901)
    studio._poll_batch_video('test', 1)
    assert item['video_status'] == 'FAILED' and item['video_op_name'] == 'accepted'
    assert item['video_retry_safe'] is False and not generated


@pytest.mark.parametrize('create_mode', ['fashion', 'outfit'])
@pytest.mark.parametrize('raw,expected', [('false', False), ('true', True), (False, False), (True, True), ('0', False), ('1', True)])
def test_multipart_video_choice_matches_billed_outputs(batch, monkeypatch, create_mode, raw, expected):
    from types import SimpleNamespace
    monkeypatch.setattr(studio, 'threading', SimpleNamespace(RLock=lambda: __import__('threading').RLock(),
        Thread=lambda **kwargs: SimpleNamespace(start=lambda: None)))
    cfg = {'autoTransferToVideo': raw, 'imageCountPerOutfit': '1'}
    if create_mode == 'fashion':
        bid = studio.create_batch_image_job(b'face', 'face.jpg', [('outfit.jpg', b'outfit')], cfg)
    else:
        bid = studio.create_batch_outfit_job([('face.jpg', b'face')], [('outfit.jpg', b'outfit')], cfg)
    assert studio.BATCH_JOBS[bid]['config']['autoTransferToVideo'] is expected


def test_auto_video_flag_rejects_ambiguous_values():
    with pytest.raises(ValueError):
        studio.parse_auto_transfer('perhaps')
