# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Flow Kit

> **Handoff Guide**: Đọc tài liệu bàn giao kỹ thuật & kiến trúc tại [docs/PROJECT_HANDOFF.md](docs/PROJECT_HANDOFF.md).

Base URL: `http://127.0.0.1:8100`

## Commands

```bash
./setup.sh                     # One-command setup: Python 3.10+, ffmpeg, Chrome, venv, deps
source venv/bin/activate       # All commands below assume this
python -m agent.main           # Start agent: REST API :8100 + WebSocket :9222 + queue worker

# Tests (pytest, heavy mocking — no server or Chrome needed)
pytest tests/                              # all
pytest tests/unit/test_processor.py        # one file
pytest tests/unit/test_processor.py::TestIsAlreadyCompleted  # one class
node --test tests/extension_flow_guard.test.cjs   # extension MV3 tests (.cjs, node:test)

# Dashboard (React 19 + Vite + Tailwind 4, in dashboard/)
cd dashboard && npm run dev    # dev server
cd dashboard && npm run build  # tsc -b && vite build
cd dashboard && npm run lint   # eslint

# Chrome nick launcher (one Chrome per Google account, sticky residential proxy — never datacenter IP, never rotate)
scripts/flow-chrome.sh nick-a socks5://USER:PASS@HOST:PORT

# Restart agent safely (keeps Chrome + bridges alive)
python3 scripts/safe_restart.py
```

Error taxonomy (Flow reasons, HTTP status, worker retry policy) lives in
`README.md` → **Error Handling** — consult it before guessing at a failure.

## Pre-flight

```bash
curl -s http://127.0.0.1:8100/health
# Must return: {"extension_connected": true}

curl -s http://127.0.0.1:8100/api/flow/status
# Must return: {"transport": "batch", "flow_project_id": "<uuid>", ...}
```

Also needed: **one signed-in `https://flow.google.com/` tab left open** per
Chrome nick. Only the page can sign a Flow request, so nothing works headless.

Three nicks share this same `:8100` URL. The agent picks the least-busy Chrome,
rewrites the RPC onto **that nick's** Flow project, and pins poll/get_media/i2v
to the nick that created the operation. Set `agent/profiles.json` and launch
with `scripts/flow-chrome.sh <nick> [socks5://…]`. `/health` lists `workers`.

## How to work

- Always use `/fk-*` skills — all rules and workflows live inside each skill
- Never write scripts to loop API calls — use `POST /api/requests/batch`
- `media_id` is always UUID format (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`), never `CAMS...` strings
- **On any pipeline error** (request `FAILED`, stuck `PROCESSING`, `extension_connected: false`, HTTP 4xx/5xx from `:8100`, YouTube `HttpError`, error strings like `UNSAFE_GENERATION` / `not found` / `CAPTCHA` / `NO_AT_TOKEN` / `NO_FLOW_PROJECT` / `UNSUPPORTED_ON_BATCH_API`): invoke `/fk-doctor` before guessing a fix
- `flow_key_present: false` is **normal** — the current transport has no bearer token

## Since Flow moved (September 2026)

Flow lives at `flow.google.com` and signs every call in the page. Consequences
that change how you work:

- **Projects are not created by Flow Kit any more.** Make one in the Flow UI and
  pin its uuid as `FLOW_PROJECT_ID`, or pass `flow_project_id` to `POST /api/projects`.
- **Three capabilities are unported** because their payloads were never captured:
  4K upscale, start+end-frame chaining, and Omni Flash. They fail with
  `UNSUPPORTED_ON_BATCH_API` rather than silently producing the wrong thing.
  `FLOW_ALLOW_DEGRADED=1` drops chaining to plain i2v; upscale has no
  fallback. r2v is ported (StreamChat + `CHAT_GENERATION`). t2v is ported
  (`YhhmEf`; omit `start_image_media_id` on `POST /api/flow/generate-video`).
  To restore an
  unported call properly, see `docs/CAPTURE.md`.
- **A poll saying "Media not found." is not a failure.** Finished jobs report it.

## Architecture

Request path for every generation (why nothing works headless):

```
caller → FastAPI (:8100, agent/main.py)
       → FlowClient (agent/services/flow_client.py) — WS bridge to extension (:9222)
       → Chrome extension (extension/background.js) — picks signed-in flow.google.com tab
       → page runs batchexecute RPC (agent/services/flow_batch.py builds f.req envelopes)
       → Flow signs with session cookie + per-page `at` token + single-use reCAPTCHA (minted in-page)
```

Key layers:

- **`agent/main.py`** — FastAPI app + WS server + lifespan; routes in `agent/api/*` (thin, delegate to services).
- **`agent/services/flow_client.py`** (~5.5k lines) — core WS bridge; nick routing (`_run_on_profile`: pick least-busy nick, rewrite project, pin poll/get_media to creator nick; failover across nicks only for unpinned calls).
- **`agent/services/flow_batch.py`** — batchexecute envelope build/parse (`build_envelope`, `RpcError`); rpcids like `eb1hJf` (i2v), `YhhmEf` (t2v).
- **`agent/worker/processor.py`** — queue worker (`_run_loop`); failure routing in `_handle_failure` is by **`error_message` string content, not HTTP status** (Flow lumps distinct failures under HTTP 400). Polling per operation via `_poll_batch_operation_once`; `agent/services/flow_failover.py` records op failover/replay for restart recovery.
- **`agent/db/`** — aiosqlite; `crud.py` with column whitelisting. Pydantic models in `agent/models/`.
- **Multi-nick fleet** — `agent/services/chrome_nicks.py` (launch/lock/sync extension per nick), `agent/services/gologin_service.py` (antidetect Orbita fingerprints), `agent/services/accounts.py` (nick registry, `agent/profiles.json`), proxy stack: `proxy_forward.py` (LocalProxyBridge hot-swap), `proxy_pool.py`, `surfshark.py`, `proxy_checker.py`. Health/fleet state: `GET /health` `workers[]`.
- **`auto_tvc_server.py`** — separate HTTPServer (not FastAPI) orchestrating full TVC jobs over FlowKit API; imports `batch_image_studio.py` / `fashion_lookbook_studio.py` (which use `batch_scheduler.py` — durable fair per-nick scheduler). Job state in `auto_runs/`.
- **`extension/background.js`** — MV3 service worker; finds/reuses Flow tab, injects `injected.js` to run RPCs in-page; reports nick identity + captured project id over WS.
- **Dashboard** — `dashboard/` React SPA backed by same `:8100` API; i18n in `src/i18n`.
- **`youtube/`** — OAuth2 multi-channel upload (`auth.py`, `upload.py`); per-channel config in `youtube/channels/<name>/` (gitignored).
- **`agent/sdk/`** — newer typed layer (models/persistence/services) layered over legacy services; `operations.py` polls/resumes video requests.

Gotchas that span files:

- Nick rename via API must stop Chrome → upsert → relaunch (safety net: "Cannot rename while Chrome is running"); read `_live_row` after relaunch.
- `DELETE /api/accounts/{id}` stops that nick's Chrome; incidents keyed by `job_id` need `Incidents.resolve_by_nick`.
- `scratch/` contains deployment snapshots and extension backups — reference copies, not live code; edit the top-level originals.

## Skills

| Skill | When to use |
|-------|-------------|
| `/fk-create-project` | New project with entities + scenes |
| `/fk-research` | Fact-check before scripting |
| `/fk-gen-refs` | Generate reference images for entities |
| `/fk-gen-images` | Generate scene images |
| `/fk-gen-videos` | Generate scene videos |
| `/fk-gen-chain-videos` | Videos with scene chaining transitions |
| `/fk-review-video` | Review video quality before upscale |
| `/fk-review-board` | Visual scene review board for feedback |
| `/fk-concat` | Download + concat final video |
| `/fk-concat-fit-narrator` | Concat trimmed to narrator duration |
| `/fk-gen-narrator` | Generate narrator text + TTS |
| `/fk-gen-text-overlays` | Generate text overlays from narrator text |
| `/fk-gen-tts-template` | Create voice template for narration |
| `/fk-gen-music` | Generate music via Suno |
| `/fk-creative-mix` | Creative video mixing techniques |
| `/fk-pipeline` | Full pipeline orchestration |
| `/fk-monitor` | Monitor running pipeline |
| `/fk-status` | Project status dashboard |
| `/fk-switch-project` | Switch active project |
| `/fk-fix-uuids` | Fix non-UUID media_ids |
| `/fk-refresh-urls` | Refresh expired signed media URLs |
| `/fk-doctor` | Diagnose errors + prescribe fixes (Flow/extension/worker/YT) |
| `/fk-add-material` | Set image material style |
| `/fk-change-model` | Change video/image model |
| `/fk-change-provider` | View & switch AI CLI provider used for video review (claude/agy/codex) |
| `/fk-insert-scene` | Insert scenes into chain |
| `/fk-upload-image` | Upload local image to get media_id |
| `/fk-thumbnail` | Generate YouTube thumbnails |
| `/fk-brand-logo` | Apply channel logo watermark |
| `/fk-youtube-seo` | Generate YouTube metadata |
| `/fk-youtube-upload` | Upload to YouTube |
| `/fk-camera-guide` | Cinematic camera reference |
| `/fk-thumbnail-guide` | Thumbnail design reference |
| `/fk-import-voice` | Import existing voice template |
| `/fk-dashboard` | Live statusline setup |

<!-- cce-block-version: 4 -->
## Context Engine (CCE)

This project uses Code Context Engine for intelligent code retrieval and
cross-session memory.

### Searching the codebase

**You MUST use `context_search` instead of reading files directly** when
exploring the codebase, answering questions about code, or understanding how
things work. This is a hard requirement, not a suggestion. `context_search`
returns the most relevant code chunks with confidence scores instead of whole
files, and tracks token savings automatically.

When to use `context_search`:
- Answering questions about the codebase ("how does X work?", "where is Y?")
- Exploring structure or architecture
- Finding related code, functions, or patterns
- Any time you would otherwise read a file just to understand it

When to use `Read` instead:
- You need to edit a specific file (read before editing)
- You need the exact, complete content of a known file path

Other search tools:
- `expand_chunk` — get full source for a compressed result
- `related_context` — find what calls/imports a function

### Cross-session memory — use it actively

This project has persistent memory across Claude Code sessions. **You must
use it both ways: recall before answering, record after deciding.** Memory
that is not recorded is lost; memory that is not recalled does nothing.

**Before answering a non-trivial question, call `session_recall`.**
Especially when:
- The question touches architecture, design, or naming choices
- The user asks "what / why / how did we ..."
- You are about to recommend an approach the team may have already chosen
  or already rejected

Pass a topic phrase, not a single word — e.g. `session_recall("auth flow")`,
not `session_recall("auth")`. Recall is vector-similarity-based, so paraphrases
match. If recall returns relevant entries, lead with them ("Per a prior
decision: ...") instead of re-deriving the answer.

**After making a non-obvious decision, call `record_decision`.** Especially:
- Choosing one library / pattern / approach over another
- Resolving an ambiguity in the spec or requirements
- Establishing a convention the project should follow going forward
- Anything you would not want to re-litigate next session

Format: `record_decision(decision="...", reason="...")`. Keep both fields
short and specific — they are surfaced verbatim at the start of future
sessions.

**After meaningful work in a file, call `record_code_area`.** Especially when:
- You added or substantially modified a function/class
- You traced through a non-obvious flow and want future-you to find it fast

Format: `record_code_area(file_path="...", description="...")`.

Skip recording for trivial reads, formatting changes, or one-off lookups —
the goal is durable signal, not an event log.

### Drilling deeper from a recall hit

`session_recall` results are tagged with the source session id, e.g.
`[turn sid:abc123|n:5]`. To drill in:

- `session_timeline(session_id="abc123")` — walk the per-turn summaries of
  that session in order. Use this when the user asks "what was the
  reasoning?" or "how did we get there?".
- `session_event(event_id=N)` — fetch a specific tool event's raw input
  and output (capped at 4 KB at read time). Use this when a turn summary
  references a tool result you actually need to inspect.

Both are read-only and cheap. Prefer them over re-running tool calls or
asking the user to re-paste context.

### Output style

Respond in compressed style. Drop articles (a, an, the) in prose. Use
sentence fragments over full sentences. Use short synonyms (fix not resolve,
check not investigate). Pattern: [thing] [action] [reason]. [next step].
No filler, hedging, pleasantries, trailing summaries, or restating what
the user said. One sentence if one sentence is enough.

When suggesting code changes, show only the changed lines with 3 lines of
context. Never rewrite entire files. Multiple changes in one file: show each
change separately. Never echo back unchanged code the user already has.

Code blocks, file paths, commands, error messages: always written in full.
Security warnings and destructive action confirmations: use full clarity.
<!-- /cce-block -->

## Language Server Intelligence (LSP via cclsp)

Language Server Protocol (LSP) intelligence is enabled via cclsp (supporting Go via gopls, Python via pyright, and TypeScript).

Prioritize LSP tools for precise code navigation and verification:
- `find_definition`: Use when locating the exact definition of a function, method, struct, or type.
- `find_references`: Use before modifying a function or variable to find all usages across the entire workspace.
- `get_hover`: Use to inspect type information, signatures, and docstrings of a symbol.
- `get_diagnostics`: Call immediately after editing a file to verify compiler/type correctness (zero build errors).
- `rename_symbol`: Use for safe, workspace-wide refactoring and symbol renaming.

## Deep Reasoning (sequentialthinking)

Use `sequentialthinking` for complex multi-step reasoning:
- Financial, billing, or quota calculations (wallets, transactions, refunds, clawbacks).
- Architecture and system design tradeoffs before writing code.
- Debugging obscure concurrency, proxy rotation, or rate-limiting bugs.

## Database Access (PostgreSQL MCP)

Use the `postgres` MCP server (`query` tool) to inspect database schema and verify queries directly:
- Read-only SQL queries (`SELECT ...`) to inspect live tables, verify relations, or check column constraints.
- Inspect schemas and foreign keys before writing migrations or ORM queries.
