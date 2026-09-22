# Failed-to-fetch investigation — 2026-09-21

Batch f72d3487 recovery: video item3 eb1hJf failed at16:53:37.768;
image items5/7 ogiZ0b failed at16:53:40.949 and16:53:42.744 (Asia/Bangkok).
Source: server.log; batch.json. Item3 image and item8 image/video succeeded.

The exception originates in the Flow page's __flowRunBatch fetch/response path,
propagated through handleBatchRpc as an extension-generated502. No upstream
HTTP status/error code is preserved for these failures. The local502 does not
prove Google returned502. Submission acceptance remains unknown; no replays.

No proxy switch/reload/disconnect appears in the inspected incident log window.
Batch references resolve to Nick-b; its bridge is127.0.0.1:40695. Current assigned
proxy health reports CLEAN, alive, Google/Labs/recaptcha accessible, zero failures,
not quarantined. Read-only curl probes through that bridge returned200 for
https://flow.google.com/ (0.93s) and https://labs.google/ (0.74s).
These probes do not reproduce authenticated POSTs and do not prove historical
network stability. Repeated quarantine entries in server.log cannot be equated
with this active proxy or with a proxy switch.

Observability defect: extension/background.js webRequest.onErrorOccurred deletes
netlogPending without recording d.error. The successful-request recorder only
captures onCompleted. agent/main.py ext_netlog also omits a network-error field
and writes the record before adding profileId, losing durable owner context.
Browser stderr log inspected for Nick-b contains old entries, no useful matching
incident evidence. Existing records cannot identify the exact Chromium net error.

Next diagnostic change: retain bounded sanitized network failures (timestamp,
request ID, nick, RPC ID, URL origin/path without query, Chromium d.error,
elapsed time), no cookies/authorization/request payload. Persist fields in API;
correlate subsequent real failures without generating synthetic paid requests.
Do not label generic Failed to fetch as proven Surfshark, concurrency, moderation,
or CORS failure, and do not retry unknown generation acceptance.

Investigation performed read-only; no proxy mutation, reload or generation replay.

## Root-cause evidence added after diagnostics activation

2026-09-21 batch8f74f896 item5: authenticated image RPC ogiZ0b request430,
Nick-b, Chrome ERR_NETWORK_CHANGED at17:22:08.033+07 (667ms elapsed).
Host journal: veth-8 Link DOWN at17:22:08.026274; gateway logs worker-8
removed for idle timeout at17:22:08.045751. veth-7 created immediately after;
worker-7 replacement ready17:22:10.089787. FlowKit's Failed to fetch17:22:08.074.
Separate event17:17:07 repeats pattern: host veth-4 DOWN17:17:07.678831,
Chrome image error17:17:07.686 server receipt; gateway idle worker removal and
replacement. Multiple nicks/requests affected together.

Active gateway service executes gateway/gateway in host network namespace;
source internal/netns/netns_linux.go creates host-visible veth-N per worker.
worker/manager_linux.go checkWorkers destroys idle workers and triggers grow;
config WORKER_IDLE_TIMEOUT15min, MIN_POOL_SIZE8, WORKER_MAX_LIFETIME60min.
Pool replenishment can therefore create/remove host interfaces even for idle
workers while Chrome submits via other nicks. This host-interface churn is the
strongly supported trigger for these two ERR_NETWORK_CHANGED incidents.
The direct change in Nick-b proxy at17:22:14 was later and is not the trigger.
DNS cache flushes/host DNS updates also occur on worker creation, after the
first Chrome error; do not mislabel that later DNS write as initial trigger.

Recommended remediation: retain minimum warm pool without idle destroy/recreate;
isolate gateway interface lifecycle from Chrome's host namespace (or maintain
stable interface topology) so eventual worker rotation does not notify Chrome
of unrelated network changes. Do not merely increase generation retries.
No gateway/network configuration changed in this investigation.
