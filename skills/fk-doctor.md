Diagnose any FlowKit error and prescribe a fix. Knows the full error taxonomy across Google Flow, the Chrome extension, the FastAPI layer, the worker, and the YouTube upload pipeline.

## When to use this skill

**TRIGGER (auto-invoke) when:**
- Any `/api/requests/*` response has `status=FAILED` or `error_message` is set
- A request has been `PROCESSING` for > 10 minutes with no progress
- `GET /health` returns `extension_connected: false`
- User reports any error string containing: `UNSAFE_GENERATION`, `QUOTA`, `not found`, `CAPTCHA`, `UNUSUAL_ACTIVITY`, `NO_AT_TOKEN`, `NO_FLOW_PROJECT`, `UNSUPPORTED_ON_BATCH_API`, `NO_FLOW_KEY`, `NO_FLOW_TAB`, `FLOW_TAB_DISCARDED`, `extension_switched`, `Failed to fetch`, `MODEL_ACCESS_DENIED`, `PAYGATE_TIER_TWO`, `invalidTags`, `quotaExceeded`, `invalid_grant`
- User asks "why did X fail", "what's wrong with the pipeline", "why is this stuck", "tại sao X lỗi", "lỗi gì vậy"
- An HTTP 4xx/5xx reaches the main agent from any endpoint under `127.0.0.1:8100`
- A YouTube upload returns `HttpError` from `googleapiclient`
- `cryptography` / architecture / import errors surface during setup

**DO NOT use when:**
- The request is still `PENDING` and hasn't been attempted yet
- The user is asking about features, not failures (route to `/fk-status` or the relevant `/fk-*` skill instead)
- The error is in user code unrelated to the FlowKit pipeline

## Usage

- `/fk-doctor` — triage mode: scan recent FAILED requests + extension health, list what's broken and how to fix
- `/fk-doctor <request_id>` — diagnose a single request by ID
- `/fk-doctor "<error message>"` — lookup a specific error string and return the handling playbook

## How to work

You are the on-call doctor for the FlowKit pipeline. Never guess — always consult the taxonomy below and the actual code. When the user reports a symptom:

1. Gather evidence (request row, extension status, processor logs, task output).
2. Classify by **error_message string content**, not HTTP status (Flow lumps many distinct failures under 400).
3. Prescribe the exact handler listed in `agent/worker/processor.py:_handle_failure` (lines 414-481) — don't invent a new one.
4. If auto-recovery should kick in but didn't, explain why (e.g. retry_count maxed, not matched by string).

## Mode 1: Triage (no args)

```bash
# Health
curl -s http://127.0.0.1:8100/health
curl -s http://127.0.0.1:8100/api/flow/status

# Recent failures
curl -s "http://127.0.0.1:8100/api/requests?status=FAILED&limit=20"

# Stuck in PROCESSING > 10 min
curl -s "http://127.0.0.1:8100/api/requests?status=PROCESSING"
```

Bucket the failures by `error_message` prefix, print a table, and for each bucket give the fix from the taxonomy.

## Mode 2: Single request (`/fk-doctor <RID>`)

```bash
curl -s http://127.0.0.1:8100/api/requests/<RID>
```

Read:
- `status` — PENDING / PROCESSING / FAILED / COMPLETED
- `error_message` — primary signal
- `retry_count` — will it retry? (MAX_RETRIES=5)
- `type` — GENERATE_IMAGE / GENERATE_VIDEO / UPSCALE_VIDEO / GENERATE_CHARACTER_IMAGE
- Linked scene_id / character_id for re-upload context

Cross-reference `error_message` against the taxonomy below. Print: **Diagnosis / Cause / Auto-handling / Manual fix**.

## Mode 3: Error string lookup (`/fk-doctor "<error>"`)

Match against taxonomy — even partial matches (`"not found"`, `"captcha"`, `"quota"`).

## Which transport is running

Since Flow moved to `flow.google.com` (September 2026) there are two paths, and
the taxonomy below splits on which one is live:

```bash
python3 -c "from agent.config import USE_BATCH_RPC, FLOW_PROJECT_ID; \
  print('batch' if USE_BATCH_RPC else 'legacy REST', '| project:', FLOW_PROJECT_ID or 'UNPINNED')"
```

- **batch** (default) — the agent builds an `f.req` envelope, the extension runs
  it inside a signed-in `flow.google.com` tab. No bearer token exists on this
  path, so `flow_key_present: false` in `/api/flow/status` is **normal**, not a
  fault. Requires a Flow tab open and `FLOW_PROJECT_ID` pinned.
- **legacy REST** (`USE_BATCH_RPC=0`) — the pre-migration `aisandbox-pa` path.
  It needs a `Bearer ya29.…` Flow no longer mints, so it will 401 on any fresh
  profile. Treat any report of it "suddenly breaking" as the migration, not a
  regression: if `token_age_s` only climbs across tab reloads, the token is not
  stale, it is gone.

## Error Taxonomy

### A. Flow-native structured errors (from `data.error.details[].reason`)

| Reason | Diagnosis | Auto-handling | Manual fix |
|--------|-----------|---------------|------------|
| `PUBLIC_ERROR_UNSAFE_GENERATION` | Safety filter tripped — real people, violence, nudity, brand names | Terminal FAILED | Rewrite prompt: use alias names + physical descriptions (see memory `real-people-bypass`); remove triggers |
| `PUBLIC_ERROR_USER_QUOTA_REACHED` | Daily credits exhausted | Terminal FAILED | Wait for daily reset, or upgrade tier |
| `PUBLIC_ERROR_MODEL_ACCESS_DENIED` | Tier mismatch (TIER_ONE trying Veo 3 / Upscale) | Terminal FAILED | `GET /api/flow/credits` to check tier; `/fk-change-model` to downgrade |
| `Requested entity was not found` | Uploaded `media_id` expired (~1h TTL on uploads) | `_recover_entity_not_found` re-uploads from `image_url`, re-queues PENDING | If auto-recovery fails: manually `POST /api/upload-image`, patch `media_id` |
| `Internal error encountered` | Flow backend transient 500 | Exponential backoff: `2^retry * 10s`, capped 300s | None — wait, or retry manually after a minute |
| `reCAPTCHA failed` / (contains `captcha`) | Extension couldn't solve reCAPTCHA | Retry ≤10× without consuming `retry_count` (processor.py:454-464) | Ensure a Google Flow tab is open and focused; reload extension |
| `PUBLIC_ERROR_UNUSUAL_ACTIVITY` (403, message `reCAPTCHA evaluation failed`) | Google flagged the session as bot-like — usually triggered by rapid bursts of submits (e.g. many GENERATE_VIDEO in <1 minute), shared/VPN IP, or stale auth cookies | NOT auto-handled — Google blocks even fresh requests until the trust signal recovers | (1) **Stop the worker / pipeline** so submits pause. (2) Open Chrome → `chrome://settings/cookies` (or the extension's Chrome profile) → search `google.com` and `labs.google` → **remove all cookies for both**. (3) Reload `https://labs.google/fx/tools/flow` and sign back in (re-solve any reCAPTCHA puzzles manually). (4) Slow down submission cadence (≥1s gap between submits, ≤5 concurrent). If still blocked, switch to a different network or wait 1–6 h |

### B. HTTP status codes

| Status | Origin | When you see it |
|--------|--------|-----------------|
| **400** | Flow API | Invalid payload / UNSAFE / entity-not-found — **route by `details.reason`** |
| **401** | Flow API (legacy path only) | Bearer expired — and on a post-migration profile it is not expired, it was never minted. Switch to the batch path |
| **403** | Extension (`background.js:432`) | `CAPTCHA_FAILED`, `NO_FLOW_TAB`, or `MODEL_ACCESS_DENIED` — read the suffix |
| **404** | Flow API | `media_id` not found — same handler as "entity not found" |
| **429** | Flow API | Rate-limit or quota — backoff; if message mentions QUOTA_REACHED, terminal |
| **500** | Flow backend **or** extension fetch exception (`background.js:504`) | Transient — retry with backoff |
| **502** | FastAPI (`agent/api/flow.py:80,92`) | Extension returned error without explicit status — treat as transient |
| **503** | FastAPI | "Extension not connected" or `NO_FLOW_KEY` — worker re-queues PENDING, waits. Also `ALL_WORKERS_PARKED` when every nick is parked — read `Retry-After`, see §C3 |
| **504** | Agent | 60s WS timeout waiting for extension — transient, re-queue |

Detection lives in `agent/worker/_parsing.py:_is_error`. A result is treated as an error if ANY of these hold:
1. `result.error` is truthy
2. `result.status` is an int and `>= 400`
3. `result.data` is a dict and `data.error` is truthy

### C. Extension / transport error strings

| Error contains | Cause | Fix |
|----------------|-------|-----|
| `Extension not connected` | WS dropped or extension offline | Reload extension at `chrome://extensions`; worker auto-retries |
| `extension reconnected` / `extension disconnected` | WS bounce mid-request | Auto re-queue, `retry_count` NOT incremented |
| `extension_switched` | User switched active Flow tab | Auto re-queue |
| `NO_FLOW_KEY` | No bearer token captured — **legacy path only**; expected and harmless on the batch path | Only meaningful with `USE_BATCH_RPC=0`; otherwise ignore |
| `NO_FLOW_TAB` | No Flow tab for CAPTCHA solve or RPC signing | Open `https://flow.google.com/` and sign in |
| `Failed to fetch` | Network drop inside service worker | Auto-retry with backoff |
| WS 60s timeout | Extension hung | Reload extension; worker re-queues |

### C2. Batch path (`flow.google.com`) errors

| Error contains | Cause | Auto-handling | Fix |
|----------------|-------|---------------|-----|
| `NO_AT_TOKEN` | The Flow tab loaded but `WIZ_global_data.SNlM0e` is absent — the page is signed out, on an interstitial, or still booting | Retried with backoff | Open `https://flow.google.com/`, confirm you are signed in, let the app finish loading |
| `NO_FLOW_TAB` | No Flow tab to sign the request | Extension opens one and retries once | Leave one signed-in Flow tab open; nothing here works headless |
| `FLOW_TAB_DISCARDED` | Chrome discarded the backgrounded tab and the reload did not revive it | Retried with backoff | Pin the Flow tab, or keep its window visible |
| `NO_FLOW_PROJECT` | No Flow project to scope the RPC to | **Terminal — not retried** | Create a project in the Flow UI, pin its uuid as `FLOW_PROJECT_ID` (or pass `flow_project_id` on `POST /api/projects`) |
| `UNSUPPORTED_ON_BATCH_API` | A capability whose payload was never captured off the new UI: **video upscale**, **start+end-frame chaining** | **Terminal — not retried** | For chaining, `FLOW_ALLOW_DEGRADED=1` falls back to plain i2v off the start frame. Upscale has no fallback. r2v is ported (StreamChat). Real fix for the rest: capture the payload — `docs/CAPTURE.md` |
| `UNSUPPORTED_ON_BATCH_API: Omni Flash` | Omni speaks the pre-migration REST + tRPC endpoints; no batchexecute payload captured | **Terminal — not retried** | Use `model_family=veo`, or `USE_BATCH_RPC=0` on a profile that still holds a bearer |
| `PUBLIC_ERROR_UNUSUAL_ACTIVITY` | A reCAPTCHA token was replayed — they are single-use | Retried as a captcha error | Usually self-clears; if it persists the extension is reusing a token, reload it |
| `no ogiZ0b envelope in response` | The RPC answered but not with the payload we came for — usually a signed-out page returning an HTML redirect | Retried with backoff | Re-sign in on the Flow tab |
| `Polling timeout after Ns: Media not found.` | The job never produced media inside the budget | Terminal after `MAX_RETRIES` | The quoted complaint is a **diagnostic, not the cause** — finished jobs report it too. Check the Flow UI: if the clip is there, raise `VIDEO_POLL_TIMEOUT` |

`NO_AT_TOKEN`, `NO_FLOW_TAB` and `FLOW_TAB_DISCARDED` are profile-local, so
with several extension profiles connected the agent fails the request over to
another one before reporting it. A single such error in the log with the job
still succeeding is that failover working, not a fault.

Three behaviours on this path routinely look like bugs and are not:

- **A poll can say "Media not found." and the job still finishes.** The project
  listing is what decides; the poll is a hint. Never treat the complaint as fatal.
- **A media id arrives before the clip is fetchable.** The media record serves
  the poster image first and grows the `/video/` url in later, so a scene sits
  PENDING for a while after its id exists. Downloading on the id alone saves a
  still picture.
- **A retry does not resubmit.** A batch operation id is a bare uuid, which the
  Low Priority workflow path treats as unrecoverable and resubmits. On the batch
  path it is recoverable — the status poll finds it in the project listing — so
  a retried video request re-polls the render already running instead of paying
  for a second one.

### C3. Fleet-level errors (multi-nick router)

These come from the router in `agent/services/flow_client.py`, not from Flow.
They are about *which nick* can take the work, so no single request is at fault.

| Error / code | Cause | Auto-handling | Fix |
|--------------|-------|---------------|-----|
| `ALL_WORKERS_PARKED` / `all_workers_parked` (503) | Every unpinned nick is parked (`unavailable_until` in the future) after an UNUSUAL_ACTIVITY or auth strike. **Nothing was submitted upstream** | Response carries `retryable: true`, `retry_after_s` and a `Retry-After` header; the studios' `ParkedBackoff` waits it out within a 15-minute budget (`PARKED_RETRY_BUDGET_S`) | Nothing, if one nick is due back — the park is minutes, not seconds. If *all* nicks are parked for 30m repeatedly: check `/health` `workers[]`, then §A `PUBLIC_ERROR_UNUSUAL_ACTIVITY` |
| `ACCOUNT_AUTH_EXPIRED` incident + `enabled: false` in `agent/accounts.json` | A nick took `AUTH_STRIKES_BEFORE_DISABLE` (3) soft auth failures inside `AUTH_STRIKE_TTL_S` (1h), or one hard 401 | Nick is removed from rotation and an incident is opened. Strikes decay after the TTL, so an isolated failure a day apart no longer accumulates | **Verify before re-login** — see below. If genuinely signed out, sign in on that nick's Flow tab and flip `enabled` back to `true` |

**`ACCOUNT_AUTH_EXPIRED` has a false-positive history — verify from raw evidence.**
On 22 Sep 2026 four signed-in nicks were auto-disabled because the classifier
matched the bare substring `401` in a *successful* payload (media uuids like
`b19701b7-4010-4c0f-9401-…` contain it). The user's "I refreshed the Flow page
and it looks fine" was correct and the code was wrong. Classification now reads
`last["error"]` text plus the integer `status`, but the lesson stands: never
trust the incident's own label. Check, in this order:

1. `.scratch/ext-netlog.jsonl` — per-rpc status for that nick. A real signout is
   401 on **every** rpcid; an account-level block is 401 on the generation rpcs
   (`maseQ`, `YhhmEf`) while `StreamChat`/`nzlxg` still answer 200 on the same `f.sid`.
2. The Flow tab itself. If it loads signed-in, it is not a session problem.
3. Only then act. An account-level block is not fixed by re-login — take the
   nick out of rotation and check it by hand in the Flow UI.

**Incidents are a ledger, not a diagnosis.** `GET /api/system/incidents` lists
them; `POST /api/system/incidents/sweep` runs `CentralWatchdog.run_sweep()`,
which heals what it can, auto-closes anything unresolved past
`INCIDENT_STALE_TTL_H` (24h, severity `INFO`, never `ACCOUNT_AUTH_EXPIRED`),
prunes `flow_operation_replay` / `flow_operation_failover`, and trims
`server.log` + `.scratch/*.jsonl` past `LOG_MAX_MB`. An OPEN incident with
`age≈0h` is a live problem being re-recorded each sweep; an old one with no
matching symptom is ledger residue, so confirm against `/health` before acting.

**Where to see which nick is 401 (dashboard `/nicks` → "Auth" panel).** The
panel reads raw netlog status codes, not the classifier, so it disagrees with an
incident when the incident is wrong:

| Endpoint | What it gives |
|----------|---------------|
| `GET /api/accounts/auth-report?window_s=3600` | Every nick, worst first: `verdict`, `advice`, last-401/last-200 timestamps, per-rpcid counts, soft-auth `strikes` + `parked_for_s`, open `ACCOUNT_AUTH_EXPIRED` incident, `enabled`. Plus counts `{total, needs_attention, signed_out, blocked, disabled}` |
| `GET /api/accounts/{nick_id}/auth` | The same evidence for one nick |
| `POST /api/accounts/{nick_id}/focus` | Raises that nick's Chrome window and activates its Flow tab (launches Chrome first if it is down). Wayland has no `wmctrl`/`xdotool`, so the extension does it from inside its own browser — this is how you avoid hunting through nine Chrome windows |
| `POST /api/accounts/{nick_id}/enable` | Put the nick back in rotation: sets `enabled`, clears soft-auth strikes, un-parks the worker, resolves the `ACCOUNT_AUTH_EXPIRED` incident |

Verdict vocabulary (`agent/services/nick_auth.py`, from `.scratch/ext-netlog.jsonl`):

| Verdict | Evidence | Action |
|---------|----------|--------|
| `SIGNED_OUT` | 401 on every rpcid, no 200s (or the newest sample is a 401) | Sign in on that nick's Flow tab — `POST /{id}/focus` to find the window — then `POST /{id}/enable` |
| `ACCOUNT_BLOCKED` | 401 only on the generation rpcs (`maseQ`, `ogiZ0b`, `eb1hJf`, `YhhmEf`) while `StreamChat`/`nzlxg` still answer 200 | **Re-login will not fix it.** Account-level block: keep the nick out of rotation and check it by hand in the Flow UI |
| `RECOVERED` | 401s in the window but the newest sample is a 200 | Nothing — it healed. `POST /{id}/enable` if a strike disabled it |
| `OK` | No 401 in the window | Nothing |
| `NO_EVIDENCE` | No netlog samples (a freshly added nick) | Nothing; send it one job |

If the extension answers focus with `UNKNOWN_METHOD:focus_flow_tab`, that
Chrome is running a pre-focus copy of the extension — stop and relaunch the nick
(`scripts/flow-chrome.sh <nick>`) to pick up the current one.

### D. YouTube upload errors (`youtube/upload.py`)

| Error | Cause | Fix |
|-------|-------|-----|
| `invalidTags` (400) | Tags exceed **500 chars with quote overhead** — tags with spaces count `+2` per tag | Trim to fit: `sum(len(t) + (2 if ' ' in t else 0) for t in tags) + (len(tags)-1) <= 500` |
| `invalidCategoryId` (400) | Unknown category_id | Use `"22"` (People & Blogs) or `"24"` (Entertainment) |
| `quotaExceeded` (403) | YT API daily 10K quota exhausted (uploads cost 1600) | Wait 24h — resets at Pacific midnight |
| `uploadLimitExceeded` (400) | Channel daily upload cap hit | Wait 24h or use another channel |
| `invalid_grant` (auth) | OAuth token revoked/expired | `python3 youtube/auth.py <channel>` |
| `scheduledPublishTimeInPast` | `publishAt` <= now | Use `auto_schedule()` or bump to next day |

### E. Setup / environment errors

| Error | Cause | Fix |
|-------|-------|-----|
| `ImportError: incompatible architecture (have 'arm64', need 'x86_64')` | Python 3.13 arch mismatch with `cryptography` | Use `python3.10` — all ML libs need it (per memory `check_skills_first`) |
| `ffprobe` exit 1 on a file still growing | File not finalized | Wait for background encode to complete |
| `curl: (7) Failed to connect to 127.0.0.1:8100` | Agent not running | `python -m agent.main` |

### F. Common symptoms → fix (quick lookup)

When the user describes a symptom in plain language, map it here first.

| Problem | Solution |
|---------|----------|
| Extension shows "Agent disconnected" | Start `python -m agent.main` |
| Extension shows "No token" | Expected on the batch path — there is no bearer any more. Only act on it with `USE_BATCH_RPC=0` |
| `CAPTCHA_FAILED: NO_FLOW_TAB` | Open `https://flow.google.com/` — and check the extension is v0.3.0+, older builds only matched the dead labs.google URL and could not see the tab that was right there |
| 403 `MODEL_ACCESS_DENIED` | Tier mismatch — `GET /api/flow/credits`, downgrade model in `models.json` via `/fk-change-model` |
| 403 `PUBLIC_ERROR_UNUSUAL_ACTIVITY` / `reCAPTCHA evaluation failed` | Google flagged the session as bot-like (rapid bursts, VPN/shared IP, stale cookies). **Pause submits**, then in Chrome: `chrome://settings/cookies` → remove cookies for `google.com` and `labs.google` → reload `labs.google/fx/tools/flow` → sign in & solve any captcha → resubmit with ≥1s gap and ≤5 concurrent. Switch network or wait 1–6 h if still blocked |
| Scene images inconsistent across scenes | Check all refs have UUID `media_id` — run `/fk-fix-uuids` |
| `media_id` starts with `CAMS...` | Run `/fk-fix-uuids` to extract UUID from URL |
| Upscale fails on every scene | On the batch path upscale is unported (`UNSUPPORTED_ON_BATCH_API`) — no upsampler rpc has been captured. On the legacy path it needs `PAYGATE_TIER_TWO` |
| 503 `ALL_WORKERS_PARKED` on every call | Whole fleet is parked. Nothing was submitted, so it is safe to wait — honour `Retry-After`. Check `/health` `workers[]` for how many nicks are `enabled` |
| A nick was auto-disabled / `ACCOUNT_AUTH_EXPIRED` | **Do not re-login on the incident alone** — check `.scratch/ext-netlog.jsonl` per-rpc status and the Flow tab first (§C3). 401 on `maseQ` only, with `StreamChat` at 200, is an account block, not an expired session |
| Request stuck in PROCESSING > 10 min | Check `error_message` history; if extension dropped, reload it at `chrome://extensions` |
| "Requested entity was not found" spam | Image URLs expired — re-upload via `POST /api/upload-image` or wait for `_recover_entity_not_found` |
| Expired signed URLs | Run `/fk-refresh-urls` — on the batch path this re-signs every stored media id through the media rpc |
| YouTube upload `invalidTags` | Tag-char overflow — quote overhead counts (spaces → +2 per tag) |
| Python `cryptography` arch mismatch | Use `python3.10`, not `python3.13` (x86/arm64 binary mismatch) |
| `curl: (7) Failed to connect to 127.0.0.1:8100` | Agent not running — `python -m agent.main` |

## Worker retry policy (`processor.py:_handle_failure`)

Decision order — stop at first match:

0. **`UNSUPPORTED_ON_BATCH_API` / `NO_FLOW_PROJECT`** → FAILED immediately. These are configuration answers, not something a retry can reach.
1. **`"not found"` in message** → `_recover_entity_not_found()` re-uploads media, marks PENDING.
2. **`reconnected` / `disconnected` / `switched`** → PENDING, keep `retry_count`.
3. **`captcha` / `recaptcha`** → PENDING if retry_count < 10; else FAILED.
4. **Default** → increment `retry_count`; if < `MAX_RETRIES` (5), schedule retry at `now + min(2^retry * 10, 300)`s. Else FAILED.

## Output format

Always end with a prescription block:

```
=== DIAGNOSIS ===
Symptom:     <what the user observed>
Root cause:  <what actually went wrong>
Layer:       Flow | Extension | FastAPI | Worker | YouTube | Env
Transport:   batch (flow.google.com) | legacy REST (aisandbox-pa)
Auto-handler: <which branch of _handle_failure fires, or "none — terminal">

=== FIX ===
1. <step 1>
2. <step 2>
...

=== PREVENT ===
<how to avoid this next time, if applicable>
```

## What NOT to do

- Don't write throwaway retry scripts — use `/fk-refresh-urls`, `/fk-fix-uuids`, or direct API patches.
- Don't recommend `--no-verify` or suppress errors.
- Don't guess HTTP status from error message alone — read `data.error.details[].reason` and the actual `status` field.
- Don't mark a request FAILED in the DB if the worker is still retrying — let the policy run.
