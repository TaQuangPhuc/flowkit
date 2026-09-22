# Video transition evidence and late-result reconciliation

Added to current FlowKit runtime, not Architecture staging.

## Evidence

`video.evidence` records are in `.scratch/flow-trace-api.jsonl` and its bounded
rotations. Submission snapshots cover t2v, i2v and StreamChat reference video.
Operation, listing-binding and media snapshots are emitted at first observation
and state changes. They retain bounded array structure, numeric RPC codes, UUIDs,
known status enums and string lengths. Free text, prompts, auth material and
signed URLs are omitted. Reference UUID/project/profile hash tie a submission
to its owner. Operation reports include expected/reported project and equality.
Media states distinguish image_only, no_urls, video and rpc_error:5.

RPC errors now retain numeric codes directly (e.g. as29s [5]); a transport200
must not be confused with RPC/business success. Snapshot deduplication resets
at restart; persisted rotating events remain available. Structure depth/node
limits mean these are diagnostic snapshots, not full replayable responses.

## Reconciliation

One read-only background check at a time, at most one eligible job per15s.
Only cached upstream_timeout jobs with known owner/project qualify, for30min
after the original render deadline. Each job has at least120s persisted backoff.
Busy/unavailable owners are skipped; owner and project remain pinned; no
cross-account failover or generation method is invoked. Existing read-only
polling uses the same extension lanes as foreground work. Shield tracks each
background check so reload waits for it; checks are bounded to30s.

Private journal: `.scratch/video-reconciliation.json`, atomic replace + fsync,
0600, retained seven days. States: checking, pending, late_succeeded,
upstream_failed, expired. Late result metadata/URL retained privately for operator
review, not automatically downloaded; signed URLs may expire and need refresh.
Original FlowKit cached failure, NOVA job state and financial ledger remain
unchanged. This does not automatically deliver or charge for late results.
Restart resumes backoff and stops rechecking already-discovered late results.
No reconciliation of moderation or other non-timeout failures.

## Verification and UI limitation

68 targeted tests pass, including durable resume, busy/missing owner, time limit,
late success preserving original failure, redaction, transition deduplication,
streaming/context/cancellation, routing and video reliability regressions.
No live generation used to test this change.

Current Chrome processes expose no remote-debugging endpoint and no browser
control connector is available. Google Flow UI outcome was not independently
verified. Read-only operation/listing/media RPC checks are available; do not
claim UI confirmation or infer why Google returned NOT_FOUND.
