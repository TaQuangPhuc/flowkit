# Gateway isolation and batch video billing — 2026-09-21

## Incident and fix

Chrome ERR_NETWORK_CHANGED tracked worker veth deletion on host at17:17:07 and
17:22:08. Surfshark gateway ran directly in host network namespace. Idle cleanup
removed warm workers; pool warmer immediately recreated them. Worker max-lifetime
replacement also changed host topology, so just raising idle timeout was insufficient.

Gateway now runs in flowkit-gateway network namespace behind a persistent host
uplink fk-vpn-host (10.203.255.1/30). Worker veth interfaces/routes live only inside
that namespace. Host ports18888/11080 retain authenticated HTTP/SOCKS endpoints
via systemd-socket-proxyd. Parent uplink is retained across service restarts.
Private mount namespace and namespace-local resolver isolate worker mounts/DNS;
access to host resolver/DBus is blocked. Removed old ExecStopPost that indiscriminately
deleted all host named namespaces. Normal gateway shutdown cleans its workers.

Worker idle cleanup keeps at least MIN_POOL_SIZE ready workers; dead workers do
not count toward that floor. Max-lifetime/failed-worker replacement stays enabled
inside isolation. Existing exclusive public-IP ownership logic preserved.

Reproducible infrastructure: deploy/surfshark/*.service, *.socket, isolation.conf,
setup-netns.sh. Production systemd drop-in: surfshark-gateway.service.d/isolation.conf.
Source gateway patch: scratch/surfshark-gateway/internal/worker/manager_linux.go.

## Billing root cause

NOVA already quotes/charges80 Credits per image and250 per8s video. Read-only
ledger inspection found12-image/0-video purchases as well as properly charged
image+video purchases. No ledger updates performed.

FlowKit multipart create converted string "false" using bool("false"), which
is True in Python. Both fashion-image and outfit-swap factories therefore
started automatic video even when frontend charged images only. parse_auto_transfer
now parses explicit true/false values; both factories persist actual booleans.
Image-only selection creates only images. Image+video selection remains330 Credits
per pair under existing PAYG prices and existing plan entitlements. No historical
charges, refunds, or outstanding batch flags altered; no NOVA price change/deploy
was necessary for this defect.

## Verification and rollout

-34 Python regressions pass (including both factories with real multipart-style
 strings and booleans); compile and diff checks pass.
-Gateway full Go tests, worker/router/proxy race tests, go vet and build pass.
-New pool tests cover retaining idle minimum and excluding dead workers.
-Systemd unit verification and shell syntax pass.
-Before rollout FlowKit drained to zero HTTP/worker/extension requests; TVC idle.
-Drain503 now explicitly marks FLOW_REQUEST_NOT_SUBMITTED, retryable=true,
 retry_after_s3 so the durable queue waits instead of failing during maintenance.
-Saved binary/config/unit backups to scratch/deployments/gateway-isolation-20260921/.
-17:40:13 gateway activated in separate network namespace,8 warm workers by17:40:25.
-Confirmed host no longer contains10.200 worker routes; only fixed10.203 uplink.
-All3 account bridges reach public egress; all3 actual public IPv4s distinct.
 All3 enabled accounts route through local gateway18888.
-Created/raised/deleted a disposable veth inside isolated namespace; no new
 Chrome network error after initial setup. This validates isolation mechanism;
 it is not a long-duration load test.
-TVC restarted with boolean fix; HTTP8089 returns200. API/TVC/gateway and stable
 listeners active; FlowKit drain cancelled. No synthetic generation/paid request.

## Rollback

First drain FlowKit and verify no submissions are active; do not interrupt a
running render submission. Stop stable listener sockets AND their proxy services,
then stop gateway. Restore saved gateway binary; remove isolation drop-in and
reload systemd before starting original host-mode unit. Leave namespace uplink
untouched until maintenance; deleting it also notifies Chrome of network change.
Restore TVC source only from a reviewed known version if needed, preserving other
uncommitted reliability changes. Never restore old financial data or replay
unknown generation requests as part of rollback.
