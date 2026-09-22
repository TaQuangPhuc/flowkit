# Flow low-priority incident — 2026-09-21

Tenant 288: ten Nova jobs failed. One returned `ask_for_permission`; nine
waited for media without a completed clip. Previous polling code replayed
accepted requests after 150 seconds, quarantined proxies based on elapsed
time alone, and failed after about 330 seconds. Google did not provide proof
that these delays were prompt violations or proxy failures.

The running process also held more than 500 SQLite database descriptors.
`with sqlite3.connect(...)` commits or rolls back but does not close the
connection. Resource exhaustion broke account reads and failover lookups.

## Changes

- Pin t2v/i2v/r2v to low-priority model keys. Reject paid-model configuration,
  Omni submissions and legacy requests with paid `videoModelKey` values.
- Require a parsed successful project-settings RPC acknowledgement before
  creating an Ingredients session. Stop on `ask_for_permission`, even if the
  response also contains a prospective media ID. Never send confirmation.
- Poll the accepted operation for 540 seconds, with a final bounded lookup
  (up to 15 seconds when the deadline has elapsed). No speculative replay or
  proxy quarantine based only on queue duration. Existing failover mappings
  remain readable for operations submitted before this change.
- Coalesce overlapping polls; persist start time and terminal status. Keep
  active worker pins when pruning operation caches.
- Close SQLite connections and proxy sockets in `finally`, including failed
  handshake and cancellation paths.
- Restart through `systemctl --user --no-block` after draining. Blocking on
  restart from inside the service previously deadlocked its own shutdown.
- Nova preserves safe `low_priority_only`, `upstream_timeout` and
  `upstream_rejected` reasons. Pending content returns 409 without failing
  the job. Nova's outer poll budget remains 600 seconds.

## Validation

- Full FlowKit unit suite: 420 passed before the final restart regression;
  subsequent resource/polling regression run: 18 passed, including restart.
- SQLite regression holds 600 connections with garbage collection disabled;
  descriptors close deterministically on success and exceptions.
- Nova provider/API tests and `go vet` passed after integration with remote
  Studio changes.
- Live FlowKit: all three configured nicks reconnected. Database descriptors
  fell to 2 and remained stable during observation. Paid-model PATCH and
  Omni submission returned HTTP 400 before reaching Google.
- No historical jobs were regenerated or financially modified. A new video
  render was not submitted as part of validation; upstream queue availability
  remains outside this fix.

The Nova deployment wrapper now stops on rejected `git push`; previously it
continued with remote code while publishing potentially stale local assets.
