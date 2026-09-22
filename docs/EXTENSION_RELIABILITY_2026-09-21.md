# Extension request coordination — 21/09/2026

Deployed `flow_guard_version=2026-09-21.1` to all three configured nicks at
approximately 12:43 GMT+7. Existing unrelated workspace edits were preserved.

## Findings

- WebSocket commands ran concurrently without coordinating reloads with active
  page requests. Proxy switching also closed bridge sockets before browser work
  had drained.
- A batch RPC selected its tab independently of captcha selection, allowing a
  token from another tab/document to enter its payload.
- Content-channel errors fell back to a second execution even when the first
  POST might already have reached Google. Error-page recovery could reuse that
  payload in a different tab.
- The two first nicks had older reload handlers than the source copy.
- FlowClient ignored `ok=false` from proxy rotation. The API could also label
  an unknown submission outcome as a retryable session error.
- Audit logs inferred a certain IP cause from a recovery that changed proxy,
  page and token together; they could attribute the original error to the new
  proxy because account configuration was read after rotation.

These are verified code paths and reliability risks, not proof that they
caused every historical `PUBLIC_ERROR_UNUSUAL_ACTIVITY`.

## Changes

`extension/flow_guard.js` coordinates requests per extension/nick. Token minting
and generation submission acknowledgement run in order. Polls may overlap.
Accepted renders do not occupy the submission slot. Queue wait is bounded to
30 seconds and the server deadline; expired work is explicitly not submitted.

Proxy rotation stages and checks its candidate first. Before committing, it
asks the extension to stop admitting work and drain active calls. It then
commits the route, updates the bridge, reloads the Flow page and resumes work.
Drain waits at most 65 seconds; a lost maintenance owner expires after 120
seconds. Failed drain prevents committing a new route. The watchdog delegates
mutations to the API event loop that owns browser sockets and bridges.

Captcha and POST use the same tab and document identifier. Navigation rejects
the stale token before submission. The page deduplicates generation request
IDs for five minutes (bounded cache). A missing content receiver permits
same-tab fallback; losing a response does not. Network waits are bounded and
unknown outcomes remain non-retryable through the API. An arbitrary UUID in
browser storage is no longer used as an unverified chat session fallback.

Reload waits for a new ready document instead of a fixed sleep or stale tab
`complete` status. Rotation failure is reported truthfully; recovery does not
reload twice. Audit records retain the pre-rotation proxy, add the number of
RPCs in flight at dispatch, and remove the unsupported 100% IP-cause claim.
That RPC count still is not a measurement of active video renders.

## Verification

- 468 Python unit tests passed; one existing Starlette deprecation warning.
- 11 Node extension tests passed, plus the MV3 cold-start regression test.
- Tests cover submission serialization, overlapping polls, maintenance drain,
  timeout/lease cleanup, expired queue entries, same-document tokens, ambiguous
  response loss, duplicate request IDs and readiness after navigation.
- API tests cover failed rotation, preventing a second reload, retaining the
  original proxy in audit, exact-nick control and drain-before-commit ordering.
- All three runtime copies of `background.js`, `content.js`, `injected.js` and
  `flow_guard.js` match source after normalizing their baked nick IDs.
- `/health` reports guard version `2026-09-21.1` for all configured nicks.
- Each nick passed a live `FLOW_PROBE` token check. Token minting does not prove
  that Google will accept every generation.
- A live `nick-a` rotation returned `ok=true`, `switched_live=true` and
  `flow_tab_reloaded=true`; all three current gateway leases remained distinct.

No new video was generated for verification. No render concurrency limit or
low-priority-only model policy was changed. Long-duration generation load and
remaining Google/IP reputation factors are not validated by these checks.

Prior per-nick extension copies are backed up under
`scratch/extension-backups/20260921-124215/`. Backend rotation now requires the
guard-capable extension; rolling back extension copies alone intentionally
blocks live rotation with `EXTENSION_UPDATE_REQUIRED`.
