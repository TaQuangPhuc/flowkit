# Video reliability follow-up — 2026-09-21

Follow-up to `LOW_PRIORITY_INCIDENT_2026-09-21.md`. Confirmed bugs below
were reproduced with mocked transport tests. No fresh Google render or
historical job replay was used for validation.

## Confirmed findings and fixes

1. **Premature Auto TVC timeout.** Batch polling discarded even a successful
   result after 300 seconds, and stopped globally at 420 seconds. Single-scene
   polling used 75 sleeps and missed responses wrapped in `data`. Both now
   allow the configured 540-second FlowKit budget plus 60 seconds for final
   lookup/HTTP overhead. The premature per-scene discard was removed.
2. **Ambiguous submissions were retried.** Studio HTTP helpers retried POST
   video submissions after timeouts/5xx, while TVC also re-uploaded the source
   and submitted again after any render exception. Helpers now submit once;
   TVC no longer recreates accepted/possibly accepted work. FlowClient avoids
   retry/failover after lost connections, unreadable responses or RPC `[13]`
   on video submission. Explicit pre-submit rejection can still be retried;
   API rotation retry requires an actual `PUBLIC_ERROR_UNUSUAL_ACTIVITY`.
3. **SDK lost terminal failure reasons and ignored RPC time in its timeout.**
   Polling now preserves error/code, marks terminal results non-retryable,
   uses a monotonic deadline, bounds each network wait, and retains handles
   omitted from a partial poll response. Worker stops immediately for
   permission/policy/configuration failures and exhausted render deadlines.
4. **Legacy workflow retries could create another render.** SDK persisted
   only the first operation name, losing workflow media IDs and other handles.
   It now stores the full operation list as JSON in the existing TEXT
   `request.request_id` field and reads old string handles for compatibility.
   Recovery runs before source validation. An old legacy workflow without a
   recoverable media handle fails explicitly instead of resubmitting.
5. **Incorrect prerequisites/recovery loop.** Reference-to-video no longer
   waits for a scene image it never uses. Accepted video jobs can resume even
   when source fields change. `not found` re-upload recovery consumes a finite
   retry budget and cannot re-upload for an already accepted request.
6. **Concurrent submissions could persist the wrong worker.** Request history
   used shared `_last_route`, which another task could overwrite. It now uses
   only operation-specific ownership; an empty subsequent worker value cannot
   overwrite a known SQLite owner. Conflicting known media/profile owners are
   rejected before Google submission with `MEDIA_PROFILE_MISMATCH`.
7. **Batch Studio failed before sending any request.** Its `FLOWKIT_API`
   constant was missing. Restored configurable URL with local API default.
   Batch/Lookbook exhausted 429 retry loops now raise instead of returning
   `None`, so callers receive the actual failure.

## Validation and deployment

- Full isolated unit suite: **453 passed**, one existing Starlette/AnyIO
  deprecation warning. Includes 32 new regression cases.
- Changed Python modules compile. Existing handshake unit tests now mock
  background tier synchronization; previously they could leave SQLite work
  attached to a closed test event loop. Test database teardown has a deadline.
- Deployment: FlowKit API graceful restart; idle Auto TVC service restart.
  Both services active; API health OK, three configured nicks reconnected,
  Auto TVC HTTP 200. No Nova code change required for this follow-up.
- These fixes do not make Google's queue or content decisions deterministic.
  Mixed-account references must be uploaded to the same nick; the validation
  guard does not automatically copy assets between accounts. Old legacy
  workflow records without media IDs cannot be reconstructed safely.
