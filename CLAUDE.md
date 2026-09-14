# Flow Kit

> **Handoff Guide**: Đọc tài liệu bàn giao kỹ thuật & kiến trúc tại [docs/PROJECT_HANDOFF.md](docs/PROJECT_HANDOFF.md).

Base URL: `http://127.0.0.1:8100`

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
