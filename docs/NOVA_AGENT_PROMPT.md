# Prompt dán cho AI trong repo Proxy-Gate-Way (Nova)

Copy toàn bộ khối dưới, dán nguyên vào chat AI **trong `/home/pc/Proxy-Gate-Way`**. Không dán secret nick/proxy/cookie.

---

Repo Nova (nơi viết code): `/home/pc/Proxy-Gate-Way`  
Repo Flow Kit (đối tác, **chỉ đọc**, không copy source vào Nova): `/home/pc/flowkit`

Tích hợp Google Flow vào Nova qua **một URL Flow Kit**. Không clone/copy repo Flow Kit vào Nova. Không gọi Google, Chrome, nick, proxy.

Đọc contract Flow Kit trước (read-only):

- `/home/pc/flowkit/docs/NOVA_INTEGRATION.md` — contract đầy đủ
- `/home/pc/flowkit/docs/NOVA_AGENT_PROMPT.md` — prompt này
- `/home/pc/flowkit/agent/api/flow.py` — request/response thật
- `/home/pc/flowkit/CLAUDE.md` — vận hành gate `:8100`

Không đọc / không commit: `/home/pc/flowkit/agent/accounts.json`, proxy password, Chrome profile, `.env`.

Đọc trong Nova: `AGENTS.md`, `docs/PROJECT_HANDOFF.md`, `internal/api/kling_video.go`, `internal/api/media.go`, `internal/api/media_jobs.go`, `internal/provider/klinggiare/`, `internal/api/public_boundary.go`, `cmd/gateway/main.go`, `config.example.yaml`. Làm theo pattern Kling: provider adapter + nhánh video/image, queue `nova_job_` giữ nguyên. Gate chạy trên máy PC tại `http://127.0.0.1:8100` (code `/home/pc/flowkit`).

## Mục tiêu

Khách Nova không đổi shape:

- `POST /v1/images/generations` `{model, prompt, size?}` → ảnh
- `POST /v1/videos` `{model, prompt, aspect_ratio?, image.url?, reference_images?}` → video
  - prompt only = t2v
  - `image.url` = i2v (khóa frame đầu)
  - `reference_images: [{url}]` = r2v
- Poll `GET /v1/videos/{id}` như Kling/Imagine
- `video` / `video_url` / last-frame / upscale / Omni / start+end → 400, đừng gọi Flow Kit những path đó

Public model gợi ý:

- Image: `google/flow-imagen` (hoặc `flow/imagen`)
- Video: `google/flow-veo` (một model, mode theo input)

Clip Flow luôn ~8s. Ignore `seconds`/`duration` của khách. Aspect `9:16` → portrait, `16:9` → landscape, mặc định portrait.

## Upstream (chỉ các endpoint này)

Base: `FLOWKIT_BASE_URL` (config provider, mặc định `http://127.0.0.1:8100` trên VPS sau reverse tunnel). Timeout HTTP submit 180s, poll 180s. Không API key Google.

`project_id` luôn `""`. `scene_id` = job id Nova. `media_id` chỉ UUID Flow trả về, không bịa.

1. `GET /health`  
   Bắt buộc `"extension_connected": true`. False → 503 `model_unavailable`, đừng enqueue.  
   `"flow_key_present": false` là bình thường.

2. `POST /api/flow/upload-image`  
   Body: `{ "image_base64": "<raw hoặc data:image/jpeg;base64,...>", "mime_type": "image/jpeg", "file_name": "studio.jpg", "project_id": "" }`  
   Trả `{ "media_id": "<uuid>" }`. Không gửi `file_path`. Upload xong generate ngay (TTL ~1h).

3. `POST /api/flow/generate-image`  
   `{ "prompt", "project_id": "", "aspect_ratio": "IMAGE_ASPECT_RATIO_PORTRAIT"|"IMAGE_ASPECT_RATIO_LANDSCAPE" }`  
   Đồng bộ. Lấy `media[0].image.generatedImage.fifeUrl` (`/image/`). Download vào MediaStore, trả URL Nova. CDN Flow hết hạn.

4. `POST /api/flow/generate-video` t2v (không ảnh) hoặc i2v (`start_image_media_id`).  
   `{ "prompt", "project_id": "", "scene_id": "<nova job id>", "aspect_ratio": "VIDEO_ASPECT_RATIO_PORTRAIT"|"VIDEO_ASPECT_RATIO_LANDSCAPE", "start_image_media_id"? }`  
   HTTP 200: `{ "operations": [ { "operation": { "name": "<uuid>" }, "status": "MEDIA_GENERATION_STATUS_PENDING" } ] }`  
   Persist `operation.name`.

5. `POST /api/flow/generate-video-refs` r2v  
   `{ "reference_media_ids": ["<uuid>", ...], "prompt", "project_id": "", "scene_id", "aspect_ratio" }`  
   Cùng shape operations. Đừng gửi `start_image_media_id`.

6. `POST /api/flow/check-status`  
   `{ "operations": [ { "name": "<operation.name>" } ] }`  
   SUCCESS **chỉ khi** `status == MEDIA_GENERATION_STATUS_SUCCESSFUL` **và** `operation.metadata.video.fifeUrl` chứa `/video/`.  
   Không fail khi: `PENDING`; `"complaint":"Media not found."`; `"complaint":"as29s failed: [5]"` kèm `mediaId`; có `mediaId` chưa có `fifeUrl`. Poll 8–10s, timeout 420s.  
   Worker Nova poll, không loop generate.

7. `GET /api/flow/media/{media_id}` — lấy lại CDN khi SUCCESS hoặc URL hết hạn. `{ "video": {"fifeUrl"}, "image": {"fifeUrl"} }`.

Cấm gọi: `/api/accounts`, `/api/ext/*`, `/api/requests/batch`, `/api/flow/upscale-video`, `/api/flow/generate-video-omni`, `model_family=omni_flash`, `end_image_media_id`, dashboard, Chrome.

## Cấm (an toàn)

- Public-bind hoặc Cloudflare `:8100`. Tunnel private (`ssh -R 127.0.0.1:8100:127.0.0.1:8100` từ PC, hoặc WG).
- Log/trả khách: nick, proxy, cookie, `workers`, `profile_id`, `flow_project_id`, `complaint` Google, `provider`/`upstream` (xem `public_boundary.go`).
- Copy `accounts.json`, Chrome profile, Flow Kit source vào Nova.
- Fan-out nhiều generate: Google max 5 + cooldown 10s; mỗi nick ~2. Dùng `media_jobs` queue.
- Retry vô hạn cùng operation khi ghost r2v (submit 200, hết 420s không `/video/` → fail `upstream_timeout`).

Lỗi map:

- HTTP 503 Extension not connected → 503 `model_unavailable`
- `PUBLIC_ERROR_UNUSUAL_ACTIVITY` (HTTP 429 hoặc `proxy_rotated: true`) → Flow Kit đã tự động đổi IP mới thành công. Nova chờ 1s rồi retry submit (tối đa 2 lần).
- `UNSAFE_GENERATION` → `content_policy_violation` + message kiểm duyệt Nova sẵn
- `UNSUPPORTED_ON_BATCH_API` → 400 (đừng gọi path đó)
- timeout poll → `upstream_timeout` + “Video xử lý quá lâu…”
- còn lại 4xx/5xx Flow Kit → 502 `generation_failed` (redact)

## Cách gắn code (bắt chước Kling)

1. Package `internal/provider/flowkit/` — `Adapter` HTTP client: Health, UploadJPEG/dataURL, GenerateImage, GenerateVideo (t2v/i2v), GenerateVideoRefs, CheckStatus, GetMedia. `Name = "flowkit"`. `ListModels` trả imagen + veo. `Do()` chat → 400 video/image-only như Kling.
2. `cmd/gateway/main.go` + `config.example.yaml`: provider `flowkit` `enabled`, `base_url`. Không commit URL public.
3. `internal/api/media.go` (và catalog): nếu public model là Flow → nhánh `serveFlowVideo` / image tương tự `serveKlingVideo` / `klinggiare.IsPublicID`.
4. Video worker/job: create = health + (upload ảnh nếu có) + submit; poll = check-status; content = file MediaStore, **không** redirect CDN Google cho khách.
5. Image generations: generate-image đồng bộ, download `/image/`.
6. Prepaid/catalog/giá: thêm model, **tự đặt giá Nova** (Flow low-priority = 0 credit Google, Nova vẫn charge khách). Đừng copy bảng giá Kling.
7. Tests: httptest fake Flow Kit cho t2v / i2v / r2v / image; PENDING `Media not found.` không fail; SUCCESS chỉ khi `/video/`; 503 khi `extension_connected: false`; public JSON không chứa `flowkit` internals nếu boundary cấm `provider`.
8. Cập nhật `docs/PROJECT_HANDOFF.md` + `.agent/` theo AGENTS.md.

Verify: `go test` các package đụng; nếu Flow Kit tunnel sống thì 1 image + 1 t2v smoke (không bắt buộc nếu `:8100` không reachable từ môi trường test).

Làm xong: tóm tắt file đổi, public model id, config key, cách map t2v/i2v/r2v. Không implement Omni/upscale/chaining.
