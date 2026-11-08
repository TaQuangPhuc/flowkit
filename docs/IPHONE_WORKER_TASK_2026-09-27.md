# Task: iPhone as FlowKit worker node (ios-webkit-debug-proxy transport)

Date: 2026-09-27
Status: **SUPERSEDED 2026-09-27 — see "Direct gRPC transport" below.**

## Update 2026-09-27: Direct gRPC transport (verified, replaces this spec)

MITM capture of the native Flow iOS app (`com.google.whisk`, WireGuard +
mitmproxy with an HTTP/2 prior-knowledge hook) revealed that the app does
**not** use batchexecute — it calls plain gRPC on
`aisandbox-pa.googleapis.com`. Every generation call was replayed
successfully from a plain PC (different IP, different TLS stack):

- `FlowService/BatchGenerateImages` — image gen (12/12 burst @1.5s)
- `VideoFxService/BatchAsyncGenerateVideoText` — t2v
- `VideoFxService/BatchAsyncGenerateVideoReferenceImages` — r2v (1..N
  ingredient media uuids)
- `VideoFxService/BatchAsyncGenerateVideoStartImage` — true i2v
- `FlowService/UploadImage` — external image → media uuid
- `issuetoken` refresh→ya29, `recaptcha/api3/mrr` captcha mint — both
  replayed standalone from PC

Consequences:

- **No iPhone needed at runtime** — the phone's only role is a one-time
  credential bootstrap (capture the `1//` refresh token + blob set once).
- No Safari tab, no USB dock, no debug proxy, no injected.js.
- reCAPTCHA is required on gen calls but the same token replays for ~2h+
  and can be minted fresh from PC by replaying `mrr`.
- Per-nick egress IP works via `https_proxy` on the gRPC channel / HTTP
  calls — verified 10/10 through a residential proxy.
- Full field maps + replay scripts: `.omc/research/FLOW_IOS_API.md`,
  `proto_lib.py`, `replay_*.py`, `mitm_logger.py`.

The Safari/debug-proxy design below is retained for history only.

---

## Why (original spec)

Measured 2026-09-27 on account `friend-test` (heavily UNUSUAL-flagged on web):

| Surface | Result |
|---|---|
| PC Chrome/Cốc Cốc + residential proxy | 0/30+ requests — UNUSUAL on all configs |
| Cloud Android boxphone (app + browser) | fails — virtualized device, datacenter IP |
| **iPhone real device — Safari AND native app** | **12/12 concurrent gens OK** |

UNUSUAL_ACTIVITY on the web surface is per-request probabilistic rejection
bound to session × browser × IP trust stack. A real iPhone passes all of it:
real hardware fingerprint, real Safari TLS/JA3, real sensors, mobile-grade IP.

Goal: plug an iPhone into a FlowKit host and have it serve generate requests
through its own Safari tab — a "nick" whose transport is iOS Safari instead of
a Chrome extension.

## Architecture

```
FlowKit agent  →  ios-webkit-debug-proxy  →  USB  →  iPhone Safari tab
                                                       └→ flow.google.com
                                                          (batchexecute RPC,
                                                           same injected.js)
```

`ios-webkit-debug-proxy` (libimobiledevice suite) talks WebKit Remote Inspector
over usbmuxd — Runtime.evaluate in any Safari tab, no Mac, no jailbreak.
The existing `extension/injected.js` runs in page context, so the whole RPC
layer ports verbatim; only the transport below it changes.

## Phone setup (one time, needs physical access)

1. iPhone: Settings → Safari → Advanced → Web Inspector = ON
2. Plug into PC → Trust computer
3. Open Safari tab → flow.google.com → log in the target Google account
4. Settings → Display → Auto-Lock = Never (iOS suspends background tabs
   aggressively — biggest operational risk; Guided Access also works)
5. Keep the Flow tab foreground while serving

## PC setup

```bash
sudo apt install libimobiledevice-utils libimobiledevice6  # usbmuxd
# ios-webkit-debug-proxy: apt package exists on Ubuntu, else build from
# https://github.com/google/ios-webkit-debug-proxy
idevicepair pair            # verify pairing
ios_webkit_debug_proxy -c null:9222 -d   # expose inspector on :9222
# list tabs: http://127.0.0.1:9222/json
```

Verify first: `GET http://127.0.0.1:9222/json` must list the flow.google.com
tab, and a Runtime.evaluate probe must return WIZ_global_data.

## FlowKit work items

1. [ ] `agent/services/ios_safari_transport.py` (new) — connect to the debug
      proxy's JSON protocol, mimic the extension WS surface that
      `flow_client.py` expects (`flow_batch_rpc`, `flow_tab_health`,
      `reload_flow_tab`). Reuse `extension/injected.js` source verbatim —
      deliver it via Runtime.evaluate.
2. [ ] Register the iPhone as a nick: profile_id like `iphone-1`,
      `transport: "ios_safari"` flag in accounts.json so routing knows the
      WS is a debug-proxy adapter, not a real Chrome extension.
3. [ ] Page-state polling: extension normally pushes `flow_tab_health`;
      over debug-proxy we poll `classifyFlowPage()` on an interval instead.
4. [ ] reCAPTCHA: in-page minting already lives in injected.js GET_CAPTCHA —
      confirm it works inside iOS Safari (grecaptcha.enterprise present).
5. [ ] Media retrieval: signed fifeUrl may work server-side; else fetch the
      bytes inside the page (fetch → base64) and upload back to the agent.
6. [ ] Tab keep-alive: detect suspended tab (evaluate timeout), alert.
      Consider silent-audio keep-alive hack if suspension kills requests.
7. [ ] Dispatch smoke test: pin one gen request to `iphone-1`, measure pass
      rate vs web nicks on the same account.

## Known risks

- iOS 17+ pairing protocol changed — need recent libimobiledevice build.
- Background tab suspension kills long polls; keep tab foreground.
- Debug proxy protocol is not 1:1 with extension WS — adapter must translate
  promise results; batchexecute calls are async (awaitPromise supported).
- One cable per iPhone; a USB hub scales to a few nodes per host.
- Remote-only alternative (if phone can't stay docked): port the Chrome
  extension to a Safari WebExtension iOS app — needs Xcode to build once
  (GitHub Actions macOS runner or cloud Mac), Apple signing ($0 = 7-day
  expiry, $99/yr = TestFlight).

## Related findings this session

- 2captcha tokens (min_score=0.9) fail on flagged sessions: 0/4 on
  friend-test; in-page self-mint also 0/2 — session flag outranks token source.
- flow-fixer measured: burst position collapse (pos0 ~90%, pos6+ ~0%),
  sticky gate persists 6-11+ min; retry storms deepen it.
- Cloud-phone fail on BOTH app+browser suggests virtualization/datacenter-IP
  detection, not just attestation.
- Discriminating tests still open: PC Chrome via iPhone 4G hotspot (is it
  the IP?) vs iPhone on home WiFi (is it the device?).
