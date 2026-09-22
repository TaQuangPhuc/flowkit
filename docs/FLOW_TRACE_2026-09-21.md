# Flow diagnostics — 2026-09-21

Scope: NOVA Flow media adapter; FlowKit API HTTP requests, worker selection and
queue wait, extension dispatch/results/callbacks, RPC parsing, video polling;
Batch Studio dispatch and Batch/TVC calls into FlowKit.

Correlation: NOVA server-generated request ID travels as X-Request-ID. FlowKit
returns X-FlowKit-Trace-ID and records that ID in nested events. Each extension
call logs its complete callback UUID. Batch dispatch logs batch/item IDs and
creates a trace inherited by its synchronous HTTP calls. Async video follow-up
can have a separate trace; operation UUID joins it to submission. Callback HTTP
requests have their own trace; extension_request_id joins them to dispatch.
Chrome network errors remain in ext-network-errors.jsonl, correlated by RPC,
worker and timestamp; they do not yet carry NOVA IDs.

Storage: .scratch/flow-trace-api.jsonl and flow-trace-studio.jsonl, separate
process files, 10 MiB each plus five backups (about 60 MiB per service). NOVA
uses structured container logs with existing deployment log retention. UTC
wall timestamps plus monotonic per-call durations avoid cross-host duration
arithmetic. Fingerprints identify repeated response samples; never reconstruct
content from them.

Captured: HTTP status, attempt, response size/sample hash and known error
markers; transport exception class, timeout/DNS/syscall/errno in NOVA; nick
hash, queue delay, RPC ID, extension request ID, callback match, operation/media
UUID, polling round/elapsed/budget and terminal state. NOVA raw HTTP response
is summarized before existing error classification can collapse it to 503.

No raw prompt, cookie, authorization header, proxy credentials, response body,
signed URL, or email is added to diagnostic logs. Unknown errors retain type,
status and fingerprint; adding a new safe marker may still be necessary.
Response sampling bounded; HTTP streaming is not buffered or consumed. Trace
sink failures cannot trigger generation retries. Retry/model/billing behavior
unchanged. Existing legacy log statements are not a full logging privacy audit.

Checks: Python tracing/redaction, streaming, context isolation, cancellation,
logging failure, existing profile/video/network and batch regression tests;
Go adapter/API tests with race detector and vet; adapter correlation and
redaction tests. No live generation needed for verification.

Architecture v1 backend remains undeployed.
