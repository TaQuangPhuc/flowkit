# Chrome network failure diagnostics — 2026-09-21

Extension build2026-09-21.2 captures webRequest.onErrorOccurred for Flow/Labs
RPC POSTs, including failures whose pending record was evicted or not captured.
Records: browser requestId, nick/profileId, RPC ID, timestamp, elapsedMs (when
known), URL origin/path without query/fragment, Chromium net::ERR_* code.
No request body, prompt, cookie, authorization header or captcha token is sent
in the error record. Unknown error strings are replaced with a fixed label.

Backend independently allowlists fields and origins and filters unsafe values.
Network failures use .scratch/ext-network-errors.jsonl (2MiB rotation, one previous
file); existing successful RPC captures remain separate. GET /api/ext/network-errors
returns up to200 most recent current-file entries. Browser requestId identifies a
Chrome network request, not a NOVA job ID; correlate by nick, RPC and timestamps.
Success captures now persist profileId before writing (previously added too late).

Validated:6 Python tests (filtering, bounded rotation, API persistence/privacy),
MV3 bootstrap plus network event regression,11 extension guard tests, JS/Python
syntax and diff checks. Tests contain simulated network errors, no media creation.

This is observability, not automatic retry or a fix for the underlying transport
failure. Never infer Google did not accept generation from a generic fetch error.
Historical failures lacking Chrome error codes cannot be reconstructed.
Activated: all3 configured extensions report2026-09-21.2 and available; API/TVC
services active. GET network-errors already received a real ERR_NETWORK_CHANGED
for maseQ immediately after browser startup (not a synthetic generation test).
This startup event does not establish the cause of earlier failed generations.

During activation found preexisting drain middleware blocked /api/ext/callback
and /api/ext/netlog, with callback503 observed in server.log. Cancelled draining,
added bypasses for response/telemetry callbacks, tested, then hot-reloaded only
request_shield preserving its existing active-request tracker. Re-entered drain;
waited for active image request to exit and all in-flight counters to reach0.
The image request remained pending until its timeout; no replay performed.
Copied new extensions, stopped configured browsers, restarted Flow API; startup
restored the same configured accounts and loaded extension copies.6 tests include
drain allowing callbacks while blocking new generation and hot reload preserving
in-flight tracking. Existing successful RPC payload logging was not expanded.

Backups/snapshot: scratch/deployments/network-diagnostics-20260921/.
