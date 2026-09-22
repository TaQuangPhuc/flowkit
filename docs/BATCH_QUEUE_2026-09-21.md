# Durable Batch Studio queue — 2026-09-21

Activated in flowkit-tvc.service with user authorization. Flow API and NOVA
unchanged by this release; no paid model fallback or extension guard weakening.

## Behavior

- Batch image and video submissions share one lane per known reference owner.
  Independent nicks can dispatch concurrently (bounded to8 active lanes).
- Batch IDs take round-robin turns. Rejected busy submissions release their lane;
  cooldown respects retry_after_s and exponential3–60s backoff. Only explicit
  FLOW_REQUEST_NOT_SUBMITTED + retryable=true permits automatic requeue.
- QUEUED items remain pending, not FAILED after3 attempts. The UI shows waiting
  counts for images/videos and automatic scheduling instead of5/10 burst choices.
- Images stay pinned to their uploaded reference owner. Video uses the generated
  image UUID directly, avoiding a reupload that silently changed nick.
- Accepted video rendering/polling runs outside submission lanes. Poll expiry
  records a terminal timeout and keeps the operation handle; never regenerates.
- batch.json is the queue journal, atomically replaced after file fsync. Submission
  state is persisted before the generation POST; persistence failure stops dispatch.
- On process restart, queue_version1 QUEUED work resumes; PENDING work prepares
  references; RENDERING videos resume polling existing handles. Interrupted
  GENERATING/SUBMITTING work becomes unknown/unsafe, requiring reconciliation.
  Legacy batches are not automatically replayed.

## Scope and limits

This scheduler coordinates Batch Studio within the single TVC service process.
NOVA, Lookbook and other API producers still share the extension guard; their
load can defer batch work, but explicit not-submitted responses stay queued.
No global cross-service fairness or multi-process coordination is claimed.
Busy work stays pending while capacity is unavailable; real moderation, response
loss and unknown upstream outcomes remain errors requiring investigation.
Queue state survives process restarts; ambiguous acceptance cannot be recovered
by blindly replaying a POST without upstream idempotency support.

## Validation / activation

15 regressions pass in venv: reference ownership, duplicate dispatch, no blind
replay, artifact failures, bulk retry selection, >3 busy responses retained,
server backoff, same-owner exclusion, independent-owner concurrency, fairness,
restart state recovery, video busy/acceptance, timeout handle retention and
persistence failure before POST. Python compile and changed-file diff checks pass.

TVC had only main/watchdog threads before restart. Service restarted successfully;
HTTP8089 returned200. One bulk retry for f72d3487 selected only3,5,7,8; original
2/4/6 outputs backed up by SHA256. Item1 moderation rejection not retried.
Deployment snapshot: scratch/deployments/batch-queue-20260921/.
Live recovery terminal:5/8 images and4 completed videos. New images3/8
completed; video8 completed. Images5/7 returned ambiguous Failed to fetch;
video3 returned UPSTREAM_SUBMISSION_UNKNOWN / Failed to fetch. No replay.
No FLOW_BUSY_NOT_SUBMITTED occurred in this recovery run. Original six
image/video files for2/4/6 match saved SHA256. Item1 moderation unchanged.
This validates queue activation and real dispatch, not resolution of separate
upstream/network failures. Busy deferral and restart safety verified by tests.
