# Tenant 339 — two video failures, 2026-09-21

Read-only investigation; no regeneration, paid fallback, or deployment.
Times below use Asia/Bangkok (UTC+7). UI timestamps are creation times.

## nova_job_b9036ab4e839dca7f609d100

- NOVA created 17:55:18; failed 18:04:41 (about 563 seconds).
- Handle: `flow_ba8d8ab5-78e6-49e5-92f6-df20380d891e`.
- Owner recorded by FlowKit: `nick-a`.
- Media: `e4e47703-f8da-4163-9a9e-8d9509f752cc`.
- NOVA green-container logs show repeated video_get requests, then rendering
  failure. FlowKit persisted `upstream_timeout`, reporting 546 seconds.
- Read-only media recheck still returned `No urls for media`.
- FlowKit server log timeout timestamp is 18:06:04, later than NOVA failure;
  persisted start time also differs from NOVA creation. Clock alignment or
  separate polling timelines remain unresolved; do not assume exact cross-host
  timestamp alignment.
- Confirmed failure stage: accepted handle, no retrievable video by deadline.
  Evidence does not establish why Google did not provide a video.

## nova_job_86f3a8bf435d50210324f6ef

- NOVA created 18:39:10; failed 18:39:59 (about 49 seconds).
- Request: `nova_req_425f6ff0e68cf0ff1ea4`; no hop_job_id stored.
- NOVA green-container log: `flow video create`, `flowkit 503 unavailable`.
- Adapter error mapped to customer `model_unavailable`, then job
  `generation_failed`. This message does not prove Google disabled the model.
- No matching generate-video access entry found in inspected local server-log
  window. Original response body / failed adapter step not retained in these
  error logs. Cause within transport, availability, or submission remains open.
- Missing handle alone does not prove Google never received generation.

## Network evidence and limits

Retained Chrome network-error diagnostics contain no entries in either job
window, and no ERR_NETWORK_CHANGED after gateway isolation in inspected data.
One ERR_ABORTED at 18:41:19 belongs to a later request. This does not rule out
network faults outside Chrome, including NOVA-to-FlowKit transport.

Repeated quarantine alerts for old proxies are not evidence of active worker
rotation. No IP, concurrency, or prompt cause established for these jobs.

Next diagnostic improvement: preserve sanitized adapter error stage, upstream
HTTP status / machine error code, and NOVA request ID end-to-end. Reconcile
cross-host time before correlating sub-second events. Do not blindly replay
either job or switch to a paid model.
