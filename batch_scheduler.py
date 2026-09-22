"""Single-process, durable Batch Studio dispatch, round-robin by batch per nick.

batch.json is the queue journal. Only QUEUED states may be dispatched after
restart; a lost acknowledgement must never turn into another generation.
"""
import json
import threading
import time
from collections import deque
from pathlib import Path
import uuid
from agent.services.flow_trace import trace_id, emit, identifier, fingerprint, summary


class BatchScheduler:
    def __init__(self, studio, owner_path=None):
        self.studio = studio
        self.owner_path = owner_path or Path(__file__).parent / '.scratch/media_profiles.json'
        self.lock = threading.RLock()
        self.order = deque()
        self.active = set()
        self.reserved = set()
        self.cooldown = {}
        self.thread = None
        self.wake = threading.Event()

    def register(self, batch_id):
        with self.lock:
            if batch_id not in self.order:
                self.order.append(batch_id)
        self.wake.set()

    def start(self):
        with self.lock:
            if self.thread and self.thread.is_alive():
                return
            self.thread = threading.Thread(target=self.run, daemon=True, name='BatchScheduler')
            self.thread.start()

    def pick(self, now=None):
        """Reserve one nick; caller must release it after submission finishes."""
        now = time.time() if now is None else now
        try:
            owners = json.loads(self.owner_path.read_text())
        except (OSError, ValueError):
            owners = {}
        with self.lock:
            if len(self.active) >= 8:
                return None
            for bid in list(self.order):
                with self.studio._batch_lock(bid):
                    batch = self.studio.BATCH_JOBS.get(bid, {})
                    for item in batch.get('items', []):
                        phase = 'image' if item.get('status') == 'QUEUED' else (
                            'video' if item.get('status') == 'COMPLETED' and item.get('video_status') == 'QUEUED' else None)
                        if not phase or item.get(f'{phase}_next_attempt_at', 0) > now:
                            continue
                        key = (bid, item['item_id'], phase)
                        if key in self.reserved:
                            continue
                        mid = item.get('output_media_id') if phase == 'video' else item.get('face_media_id')
                        owner = str(owners.get(mid) or 'unknown-owner').casefold()
                        # Unknown ownership uses a conservative global lane.
                        if owner in self.active or 'unknown-owner' in self.active or (owner == 'unknown-owner' and self.active):
                            continue
                        if self.cooldown.get(owner, 0) > now:
                            continue
                        self.active.add(owner)
                        self.reserved.add(key)
                        self.order.remove(bid)
                        self.order.append(bid)
                        return bid, item['item_id'], phase, owner
        return None

    def execute(self, task):
        bid, iid, phase, owner = task
        trace_token = trace_id.set("fk_" + uuid.uuid4().hex)
        emit("batch.dispatch", batch_id=identifier(bid), item_id=identifier(iid),
             phase=phase, profile_hash=fingerprint(owner))
        try:
            if phase == 'image':
                self.studio.execute_single_item(bid, iid)
            else:
                self.studio.transfer_item_to_video(bid, iid)
        except Exception as exc:
            # Persistence failures must never result in automatic replay.
            with self.studio._batch_lock(bid):
                item = next(i for i in self.studio.BATCH_JOBS[bid]['items'] if i['item_id'] == iid)
                key = 'status' if phase == 'image' else 'video_status'
                item[key] = 'FAILED'
                item['retry_safe' if phase == 'image' else 'video_retry_safe'] = False
                item['error' if phase == 'image' else 'video_error'] = f'Kết quả chưa xác định: {exc}'
                try:
                    self.studio.save_batch_job(bid)
                except OSError:
                    pass
        finally:
            with self.lock:
                item = next(i for i in self.studio.BATCH_JOBS[bid]['items'] if i['item_id'] == iid)
                self.cooldown[owner] = max(time.time() + 1, item.get(f'{phase}_next_attempt_at', 0))
                self.active.discard(owner)
                self.reserved.discard((bid, iid, phase))
            emit("batch.dispatch.end", batch_id=identifier(bid), item_id=identifier(iid),
                 phase=phase, state=identifier(item.get("status")), video_state=identifier(item.get("video_status")),
                 next_attempt_at=item.get(f"{phase}_next_attempt_at"),
                 result=summary({"error": item.get("error") or item.get("video_error")}))
            trace_id.reset(trace_token)
            self.wake.set()

    def run(self):
        while True:
            try:
                task = self.pick()
                if task:
                    threading.Thread(target=self.execute, args=(task,), daemon=True, name='BatchDispatch').start()
                    continue
            except Exception as exc:
                print(f'[BATCH QUEUE] Dispatch paused: {type(exc).__name__}')
            self.wake.wait(1)
            self.wake.clear()

    def recover(self):
        for bid, batch in list(self.studio.BATCH_JOBS.items()):
            # Only new journaled batches are eligible for automatic recovery.
            if batch.get('queue_version') != 1:
                continue
            with self.studio._batch_lock(bid):
                for item in batch.get('items', []):
                    if item.get('status') == 'GENERATING':
                        item.update(status='FAILED', retry_safe=False,
                                    error='SUBMISSION_OUTCOME_UNKNOWN: gián đoạn khi tạo ảnh; cần đối soát.')
                    if item.get('video_status') == 'SUBMITTING':
                        item.update(video_status='FAILED', video_retry_safe=False,
                                    video_error='SUBMISSION_OUTCOME_UNKNOWN: gián đoạn khi gửi video; cần đối soát.')
                    if item.get('video_status') == 'RENDERING' and item.get('video_op_name'):
                        self.studio.start_video_poller(bid, item['item_id'])
                self.studio.save_batch_job(bid)
            self.register(bid)
            if any(i.get('status') == 'PENDING' for i in batch.get('items', [])):
                threading.Thread(target=self.studio.start_batch_pipeline, args=(bid,), daemon=True).start()
        self.start()
