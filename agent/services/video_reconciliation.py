"""Read-only late-result checks. Never generates or changes original job/billing state."""
import asyncio
import json
import os
from pathlib import Path
import time
import uuid
from agent.services.flow_trace import emit, identifier, summary


class VideoReconciler:
    def __init__(self, client, path, render_timeout=540, window=1800, interval=120):
        self.client = client
        self.path = Path(path)
        self.render_timeout = render_timeout
        self.window = window
        self.interval = interval
        try:
            self.rows = json.loads(self.path.read_text())
            if not isinstance(self.rows, dict): self.rows = {}
        except (OSError, ValueError):
            self.rows = {}

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix('.tmp')
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w') as handle:
            json.dump(self.rows, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self.path)

    async def tick(self, now=None):
        now = time.time() if now is None else now
        # Retain discovery evidence for seven days; original operation store is untouched.
        self.rows = {k:v for k,v in self.rows.items() if now-v.get('updated_at', now) < 7*86400}
        for op, result in list(self.client._operation_results.items()):
            if op.startswith('operations/') or result.get('error_code') != 'upstream_timeout':
                continue
            start = self.client._operation_start_time.get(op)
            if not start:
                continue
            cutoff = start + self.render_timeout + self.window
            row = self.rows.get(op, {})
            if row.get('state') in {'late_succeeded', 'upstream_failed', 'expired'}:
                continue
            if now > cutoff:
                # Do not fill the journal with all historical jobs on first startup.
                if row:
                    row.update(state='expired', updated_at=now)
                    self.save()
                    emit('video.reconcile.expired', operation_id=identifier(op))
                continue
            if now < row.get('next_check_at', 0):
                continue
            profile = self.client._operation_profiles.get(op)
            project = self.client._operation_projects.get(op)
            if not profile or not project:
                continue
            workers = self.client.workers()
            owner = next((w for w in workers if w.get('profile_id') == profile), None)
            if not owner or not owner.get('available') or owner.get('in_flight', 0):
                continue
            row = {**row, 'state':'checking', 'updated_at':now,
                   'next_check_at':now+self.interval, 'attempts':row.get('attempts',0)+1}
            self.rows[op] = row
            self.save()  # crash/restart respects persisted backoff
            emit('video.reconcile.start', operation_id=identifier(op), attempt=row['attempts'])
            async def read(_pid):
                return await self.client._poll_batch_operation_inner(op)
            try:
                found = await asyncio.wait_for(self.client._run_on_profile(
                    read, project, profile_id=profile, operation_id=op, allow_failover=False), timeout=30)
                state = found.get('status')
                row['state'] = ('late_succeeded' if state == 'MEDIA_GENERATION_STATUS_SUCCESSFUL'
                                else 'upstream_failed' if state == 'MEDIA_GENERATION_STATUS_FAILED'
                                else 'pending')
                # Preserve full existing-operation result privately for operator review.
                # Never overwrite cached timeout or debit/refund/mark a NOVA job here.
                if row['state'] in {'late_succeeded','upstream_failed'}:
                    row['result'] = found
                row['diagnostic'] = summary(found)
            except Exception as exc:
                row['state'] = 'pending'
                row['diagnostic'] = summary(exc)
            row['updated_at'] = time.time()
            self.save()
            emit('video.reconcile.end', operation_id=identifier(op), state=row['state'], result=row.get('diagnostic'))
            return  # one operation at a time, never stampede the extension

    async def run(self):
        from agent.services.request_shield import get_request_shield
        while True:
            try:
                shield = get_request_shield()
                if not shield.is_draining:
                    request_id = 'reconcile_' + uuid.uuid4().hex
                    await shield.acquire(request_id, '/internal/video-reconciliation', 'GET', 'background')
                    try:
                        await self.tick()
                    finally:
                        await shield.release(request_id)
            except Exception as exc:
                emit('video.reconcile.exception', exception_type=type(exc).__name__)
            await asyncio.sleep(15)
