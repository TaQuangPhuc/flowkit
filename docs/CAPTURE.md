# Capturing a Flow batchexecute payload

`agent/services/flow_batch.py` only knows the RPC shapes that were captured off a
real UI action. Adding one — video upscale, start+end-frame chaining, a
base-image edit — starts by watching the browser do it, because
guessing at Google's positional payloads does not work. Thirty generations were
spent proving that a reference image in the wrong slot is *accepted* and then
silently ignored.

The recorder is deliberately **not** in the shipped extension: it writes live
request bodies to disk, so it goes in for one session and comes straight back out.

## What already exists

| step | rpcid | notes |
|---|---|---|
| generate image | `ogiZ0b` | signed CDN url comes back inline |
| generate video (i2v) | `eb1hJf` | start image + crop; returns an operation id |
| generate video (t2v) | `YhhmEf` | no media slot; model `veo_3_1_t2v_lite_low_priority`; UI sends 2 variants |
| poll operation | `jwpduf` | status `CAE` means finished |
| operation → media id | `Zzl0ze` | `projects/<id>`; the listing is ~17 MB |
| media id → urls | `as29s` | signed `/video/` + poster `/image/` |
| upload an image | `maseQ` | base64 in the payload, captcha like a generate |
| reference-to-video | `StreamChat` | creation-agent path, not batchexecute; captcha action `CHAT_GENERATION`; session from `csbIsb` (CreateSession) **response**. `mrlkwd` lists sessions; `GN0Bre` is GetSession |

Missing, and each blocked behind a capture: **video upscale**,
**start+end-frame chaining**, and the **base-image** variant of the image edit.

## Recording one

1. Add to `extension/background.js`, temporarily:

```js
const NETLOG_HOSTS = ['https://flow.google.com/_/*'];
const pending = new Map();

chrome.webRequest.onBeforeRequest.addListener((d) => {
  const body = d.requestBody?.raw?.length
    ? new TextDecoder().decode(new Uint8Array(d.requestBody.raw[0].bytes))
    : null;
  pending.set(d.requestId, { ts: new Date().toISOString(), url: d.url, body });
}, { urls: NETLOG_HOSTS }, ['requestBody']);

chrome.webRequest.onCompleted.addListener((d) => {
  const rec = pending.get(d.requestId);
  if (!rec) return;
  pending.delete(d.requestId);
  fetch('http://127.0.0.1:8100/api/ext/netlog', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ ...rec, statusCode: d.statusCode }),
  }).catch(() => {});
}, { urls: NETLOG_HOSTS });
```

   Headers are omitted on purpose — the payload shape is what is wanted, and the
   credentials are what caused the mess. Add an `onBeforeSendHeaders` listener
   only if a header itself is the open question, and delete the log afterwards.

2. Add a matching throwaway route to `agent/main.py` that appends the posts to a
   file under the scratch directory.
3. Reload the extension, perform **one** action in Flow, then reload again with
   the listener removed.
4. Read the `f.req` out of the capture: it is
   `[[[rpcid, "<inner payload as JSON string>", null, "generic"]]]`. Diff the
   inner payload against the closest builder in `flow_batch.py` to find which
   slot changed.
5. Delete the capture file. It is evidence, not a fixture — put what you learned
   into a builder and a test instead.

## Two things worth knowing before you diff

**Accepted ≠ used.** A wrong arrangement inside a slot comes back 200 and is
then ignored. Prove a reference image with a prompt that never names its
subject; prove an aspect ratio by reading the JPEG header, not by trusting the
field name.

**Slots do not share encodings.** Image aspect is 1 square / 2 portrait /
3 landscape / 4 is 3:4 / 5 is 4:3. Video aspect is 1 portrait / 2 landscape, in
its own slot. Conflating them renders the wrong shape silently.

---

# Capturing the native iOS app (gRPC)

The iOS Flow app (`com.google.whisk`) does not use batchexecute — it calls
protobuf gRPC on `aisandbox-pa.googleapis.com` (via IP literals, HTTP/2
prior knowledge, and attempted QUIC). The browser recorder above cannot
see any of it. Working recipe (proven 2026-09-27):

1. **WireGuard mode, not HTTP proxy.** `mitmdump --mode wireguard` — the
   app does not respect the system HTTP proxy for gRPC, and QUIC bypasses
   TCP proxies entirely. Phone installs the WireGuard app, scans the QR
   from `mitm.it`, all traffic tunnels through the PC.
2. **HTTP/2 prior-knowledge hook.** The app sends the h2 preface without
   ALPN → mitmproxy's `DisableH2C` kills it as bogus HTTP/1. Fix in the
   logger script (`next_layer` addon): detect `PRI * HTTP/2.0` post-TLS,
   set `client.alpn = b"h2"` so Http2Server/Http2Client is selected.
3. **Force upstream h2.** Upstream ALPN is mirrored from client offers;
   the app offers none → upstream falls back to h1 →
   `http2_handshake_failed`. Set `server_conn.alpn_offers = (b"h2",
   b"http/1.1")` in `tls_start_server`, plus
   `--set connection_strategy=lazy` so the offer applies before the
   upstream handshake, and `--ssl-insecure` to tolerate odd issuers.
4. **Full binary bodies.** Log `application/grpc*` and IP-literal hosts
   with a high body cap, base64-encoded — protobuf is binary and Windows
   consoles choke on raw bytes. Upload bodies (`UploadImage`) exceed the
   200 KB default cap; raise it if upload internals matter.
5. **Egress IP control — via `server_conn.via`, NOT `--mode upstream`.**
   mitmproxy's `upstream:` mode is only an extra proxy listener; it does
   NOT chain traffic from other modes (wireguard flows egress directly —
   verified: phone got the home IP). The working mechanism is per-flow
   `flow.server_conn.via = ("http", (host, port))` set in `tcp_start` /
   `requestheaders`, with `--set connection_strategy=lazy` so `via`
   applies before connect and `--set upstream_auth=user:pass` for
   CONNECT auth. `mitm_logger.py` reads `MITM_UPSTREAM=http://…` for this.
6. **Kill UDP or it leaks the real IP.** QUIC/STUN/NTP cannot traverse an
   HTTP CONNECT proxy — if forwarded they egress via the PC's real IP.
   `--set http3=false` plus killing every `udp_start` flow whose target
   is not the internal DNS resolver (`10.0.0.53:53`) forces TCP fallback.
   Browsers may stall visibly while retrying h3 — expected.

### Multi-phone onboarding

One mitmdump instance per phone: unique WG port + keypair file + upstream
proxy + tagged log. Production capture is on the VPS
(`python scripts/vps_mitm_setup.py`, Endpoint `180.93.43.217`, all five
nicks including landon). `wg_qr.py <nick> <port> --host 180.93.43.217`
reuses existing keys. `start_captures.sh` is the local-PC fallback.

Order on each phone: install mitmproxy CA (`mitm.it` over the tunnel →
profile → Certificate Trust **full trust**) → enable WG → **sign in
Google/Flow only after the tunnel is up** so the account's first-seen IP
is the proxy IP. Gotchas: disable iCloud Private Relay if present
(Safari then reports the relay's IP, not the proxy's); on Windows pass
`pwd -W` paths — mitmdump mis-resolves MSYS `/c/…` paths and silently
generates wrong keypairs (handshake never completes).

Logger reference: `.omc/research/mitm_logger.py`. Everything it captured
is field-mapped in `.omc/research/FLOW_IOS_API.md`, and every gen call is
replayable from PC via the `replay_*.py` scripts there.

### iOS gRPC surface already captured

| flow | method |
|---|---|
| image gen | `FlowService/BatchGenerateImages` |
| video t2v | `VideoFxService/BatchAsyncGenerateVideoText` |
| video i2v | `VideoFxService/BatchAsyncGenerateVideoStartImage` |
| video r2v | `VideoFxService/BatchAsyncGenerateVideoReferenceImages` (f2 repeats per ingredient) |
| upload image | `FlowService/UploadImage` (f1 metadata+captcha, f2 raw bytes) |
| poll | `VideoFxService/BatchCheckAsyncVideoGenerationStatus`, `FlowService/BatchGetMedia` |
| auth | `oauthaccountmanager…/v1/issuetoken` + `oauth2…/token` (1// refresh → ya29) |
| captcha | `recaptcha.net/api3/mrr` (protobuf, reusable token, mints from PC) |

Note the iOS r2v path is plain gRPC — a completely different transport
from web r2v (`StreamChat`/`CHAT_GENERATION`). Prefer the gRPC route
when porting: no session bootstrap, no StreamChat framing.
