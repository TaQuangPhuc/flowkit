# Nova ↔ Flow Kit — hướng dẫn tích hợp

Tài liệu này là contract để **bê Flow vào Nova**. Không copy repo Flow Kit vào Proxy-Gate-Way. Nova chỉ HTTP tới **một URL**. Chrome, nick Google, proxy, extension ở lại máy này.

Đã live-test trên nick-a (`86c99e42-…`): image, upload, t2v, i2v, r2v đều trả CDN `flow-content.google`.

## 1. Kiến trúc

```
Khách → Nova VPS (Studio / /v1/images / /v1/videos)
              │  private tunnel only
              ▼
     PC  127.0.0.1:8100   Flow Kit gate
              │  WS 127.0.0.1:9222
              ▼
     Chrome nick (tab flow.google.com đã login)
              │  sticky proxy / nick
              ▼
           Google Flow
```

Gate chọn nick ít bận, rewrite RPC sang **Flow project của nick đó**. Nova không chọn nick, không gửi proxy, không gửi cookie.

`flow_key_present: false` là bình thường (transport batch không có bearer).

## 2. Cấm

| Cấm | Lý do |
|-----|--------|
| Bind `:8100` ra internet / Cloudflare public | Cookie Google + signed CDN |
| Copy `agent/accounts.json`, proxy password, Chrome profile vào Nova | Secret nick |
| Gọi `/api/accounts`, `/api/ext/*`, dashboard, launch Chrome từ Nova | Nội bộ máy này |
| Trả `profile_id`, proxy, `flow_project_id`, `workers[]` cho khách | Public boundary Nova |
| Tự bịa `media_id` / StreamChat uuid | Flow trả `PUBLIC_ERROR_UNUSUAL_ACTIVITY` |
| Loop `generate-*` trong pipeline Nova | Nova queue 1 job = 1 submit + poll `check-status` |
| `model_family=omni_flash`, `end_image_media_id`, `/upscale-video` | `UNSUPPORTED_ON_BATCH_API` |

`POST /api/requests/batch` là queue **cảnh Flow Kit**, không dùng cho job Studio.

## 3. Mạng (Nova VPS → PC)

Flow Kit listen `127.0.0.1:8100`. Từ **máy PC** (không public-bind):

```bash
# reverse tunnel: Nova gọi http://127.0.0.1:8100 trên VPS
ssh -N -R 127.0.0.1:8100:127.0.0.1:8100 user@NOVA_VPS
```

Hoặc WireGuard / Tailscale, Nova `FLOWKIT_BASE_URL=http://<pc-wg>:8100`.

Nova config (gợi ý, không commit secret):

```yaml
flowkit:
  base_url: "http://127.0.0.1:8100"   # trên VPS, sau reverse tunnel
  poll_interval: 8s
  poll_timeout: 420s
```

Preflight mỗi lúc nhận job (và health worker):

```bash
curl -fsS "$FLOWKIT_BASE_URL/health"
# bắt buộc: "extension_connected": true
# nếu false → 503 model_unavailable, đừng enqueue
```

`GET /api/flow/status` chỉ để debug nội bộ. `transport` phải là `"batch"`.

## 4. Model public trên Nova

Gợi ý catalog (Nova tự đặt giá; low-priority trên Flow = 0 credit Google):

| Public model | Mode khách gửi | Flow Kit |
|--------------|----------------|----------|
| `google/flow-veo` | `prompt` only | t2v |
| cùng model | `image.url` | i2v (khóa frame đầu) |
| cùng model | `reference_images: [{url}]` | r2v |

Clip ~**8 giây**. Ignore `seconds` / `duration` của khách (đừng hứa 4/6/10).  
Aspect: `9:16` → `VIDEO_ASPECT_RATIO_PORTRAIT`, `16:9` → `VIDEO_ASPECT_RATIO_LANDSCAPE`. Mặc định portrait.

Image Studio: `POST /v1/images/generations` → `POST /api/flow/generate-image` (đồng bộ, có URL ngay).

## 5. API Nova được gọi

Base: `http://127.0.0.1:8100`

Mọi `media_id` là UUID `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`. Không dùng chuỗi `CAMS…`.

`project_id` gửi `""`. Gate gán project của nick. Đừng bịa uuid Flow.  
`scene_id` = job id Nova (`nova_job_…`) — chỉ để log.

### 5.1 Upload ảnh khách → `media_id`

Nova đã có JPEG/data URL (giống Kling). Gửi bytes, **không** `file_path`:

```http
POST /api/flow/upload-image
Content-Type: application/json
```

```json
{
  "image_base64": "<raw base64 hoặc data:image/jpeg;base64,...>",
  "mime_type": "image/jpeg",
  "file_name": "studio.jpg",
  "project_id": ""
}
```

```json
{ "media_id": "f85cba5d-1190-4e66-bf14-f6a4ce796055" }
```

TTL upload ~1 giờ nếu chưa dùng. Upload xong generate ngay.

### 5.2 Image (đồng bộ)

```http
POST /api/flow/generate-image
```

```json
{
  "prompt": "A red ceramic cup on a white table, studio lighting, no text",
  "project_id": "",
  "aspect_ratio": "IMAGE_ASPECT_RATIO_PORTRAIT"
}
```

Trả về khi xong (~vài giây):

```json
{
  "media": [
    {
      "name": "<uuid>",
      "image": {
        "generatedImage": {
          "mediaId": "<uuid>",
          "fifeUrl": "https://flow-content.google/image/<uuid>?Expires=...&KeyName=labs-flow-prod-cdn-key&Signature=..."
        }
      }
    }
  ]
}
```

Nova: lấy `media[0].name` + `fifeUrl`, **download vào MediaStore**, trả URL Nova cho khách. CDN Flow hết hạn.

### 5.3 Video — submit (luôn PENDING)

**t2v** — không ảnh:

```http
POST /api/flow/generate-video
```

```json
{
  "prompt": "Slow orbit around a red ceramic cup on a white table, studio lighting, no people, no text",
  "project_id": "",
  "scene_id": "nova_job_abc",
  "aspect_ratio": "VIDEO_ASPECT_RATIO_PORTRAIT"
}
```

**i2v** — một `media_id` frame đầu (sau upload hoặc generate-image):

```json
{
  "prompt": "The cup from the start frame, slow push in, no people, no text",
  "project_id": "",
  "scene_id": "nova_job_abc",
  "start_image_media_id": "f85cba5d-1190-4e66-bf14-f6a4ce796055",
  "aspect_ratio": "VIDEO_ASPECT_RATIO_PORTRAIT"
}
```

**r2v** — 1+ `media_id` (live 1 ảnh; đừng gửi `start_image_media_id`):

```http
POST /api/flow/generate-video-refs
```

```json
{
  "reference_media_ids": ["f85cba5d-1190-4e66-bf14-f6a4ce796055"],
  "prompt": "The red ceramic cup from the reference image sits on a white table. Slow push in, studio lighting, no people, no text",
  "project_id": "",
  "scene_id": "nova_job_abc",
  "aspect_ratio": "VIDEO_ASPECT_RATIO_PORTRAIT"
}
```

Submit HTTP 200:

```json
{
  "operations": [
    {
      "operation": { "name": "25cdc371-55dc-458d-93e3-df12050fc2ee" },
      "status": "MEDIA_GENERATION_STATUS_PENDING"
    }
  ]
}
```

Nova job id nội bộ có thể là `operation.name`. Trả khách pattern sẵn có (`nova_job_…` / poll `/v1/videos/{id}`).

HTTP 503 `Extension not connected` → `model_unavailable`.  
HTTP 4xx/5xx khác → `generation_failed` (redact string Google).

### 5.4 Poll video

```http
POST /api/flow/check-status
```

```json
{
  "operations": [
    { "name": "25cdc371-55dc-458d-93e3-df12050fc2ee" }
  ]
}
```

Chỉ SUCCESS khi có `/video/` trong `fifeUrl`:

```json
{
  "operations": [
    {
      "operation": {
        "name": "25cdc371-55dc-458d-93e3-df12050fc2ee",
        "metadata": {
          "video": {
            "mediaId": "258e92d4-c6ce-4559-997a-2ad3f092ed08",
            "fifeUrl": "https://flow-content.google/video/258e92d4-...?Expires=...&Signature=..."
          }
        }
      },
      "status": "MEDIA_GENERATION_STATUS_SUCCESSFUL"
    }
  ]
}
```

PENDING (tiếp tục poll, **không fail**):

| Tín hiệu | Ý nghĩa |
|----------|---------|
| `MEDIA_GENERATION_STATUS_PENDING` | Chưa có file |
| `"complaint": "Media not found."` | Job xong phía Google vẫn báo câu này — **không phải lỗi** |
| `"complaint": "as29s failed: [5]"` + có `mediaId` | Clip chưa ghi CDN |
| Có `mediaId`, chưa `fifeUrl` | Giữ id, poll tiếp |

Poll 8–10s, timeout **420s**. Live r2v ~35s submit + ~45s tới `/video/` (tổng ~79s). t2v/i2v thường nhanh hơn.

Timeout → `upstream_timeout` / message Nova sẵn (`Video xử lý quá lâu…`).

**Không** tin GetSession `"queued"` hay lưới project trên flow.google.com. Job r2v queue **không** hiện tile trên web.

### 5.5 Lấy lại URL

```http
GET /api/flow/media/{media_id}
```

```json
{
  "video": { "fifeUrl": "https://flow-content.google/video/<uuid>?..." },
  "image": { "fifeUrl": "https://flow-content.google/image/<uuid>?..." }
}
```

Dùng khi poll SUCCESS (download MP4) hoặc refresh URL hết hạn. 404 / `as29s [5]` trên id chưa từng SUCCESS = chưa có file.

## 6. Map Nova Studio → Flow Kit

Giống Kling / Imagine: khách không đổi shape.

```
POST /v1/videos  { model, prompt, aspect_ratio?, image.url?, reference_images? }
```

```
nếu video / video_url / last_frame     → 400 (Nova đã cấm)
nếu health.extension_connected != true → 503
nếu reference_images[]                 → upload từng url → generate-video-refs
else nếu image.url                     → upload 1 ảnh   → generate-video + start_image_media_id
else                                   → generate-video (t2v)
rồi poll check-status đến /video/
download CDN → MediaStore → trả url Nova
```

Adapter nên copy `internal/api/kling_video.go`:

1. `createFlowVideo` — upload + submit, nhớ `operation.name`
2. `pollFlowVideo` — `check-status`, map SUCCESS/PENDING/failed
3. `contentFlowVideo` — file đã lưu store, không stream CDN Google cho khách

Queue `media_jobs` (`nova_job_`) giữ nguyên. Worker Nova gọi Flow Kit, không gọi Google.

Image:

```
POST /v1/images/generations { model, prompt, size? }
  → generate-image → download /image/ → data URL / store
```

Không cần poll.

## 7. Giới hạn vận hành

- Concurrent: Google max **5** + cooldown **10s**. Mỗi nick `PROFILE_MAX_CONCURRENT` mặc định **2**. Nova queue, đừng fan-out 20 generate.
- 3 nick = 3 slot song song, **không** nhân credit (low-priority unlimited).
- Restart agent: poll t2v/i2v/r2v vẫn được nếu op còn trong `.scratch/r2v_ops.json` trên PC. Nova nên persist `operation.name` trên job của mình.
- r2v thỉnh thoảng submit 200 rồi không thành file (ghost). Hết `poll_timeout` thì fail job, đừng retry vô hạn cùng op.

## 8. Lỗi — map sang Nova, đừng leak

| Flow Kit / Google | Nova xử lý / Nova khách |
|-------------------|--------------------------|
| HTTP 503 Extension not connected | 503 `model_unavailable` |
| `UNSUPPORTED_ON_BATCH_API` | 400 — đừng gọi upscale / chain / omni |
| `UNSAFE_GENERATION` / policy | `content_policy_violation` + message kiểm duyệt sẵn |
| `NO_AT_TOKEN` / `NO_FLOW_PROJECT` / `CAPTCHA` | 503 nội bộ; chạy `/fk-doctor` trên PC |
| `PUBLIC_ERROR_UNUSUAL_ACTIVITY` (HTTP 429) | **Proxy đã tự động đổi IP thành công** (`proxy_rotated: true`). Nova sleep 1s rồi retry submit (tối đa 2 lần). |
| HTTP 4xx/5xx từ `:8100` | 502 `generation_failed` |
| Timeout poll | `upstream_timeout` |

### 8.1 Cơ chế Auto Proxy Rotation khi gặp Unusual Activity
Khi một request submit (image, t2v, i2v, r2v, upload) bị Google phát hiện IP bất thường (`PUBLIC_ERROR_UNUSUAL_ACTIVITY`):
1. **Flow Kit tự động hot-swap proxy** ngay lập tức sang IP dân cư sạch kế tiếp trong pool và tự thử retry 1 lần.
2. Nếu trả về client, Flow Kit trả về **HTTP 429 Too Many Requests** kèm thông báo đổi IP thành công:
   ```json
   {
     "ok": false,
     "error": "PUBLIC_ERROR_UNUSUAL_ACTIVITY",
     "message": "UNUSUAL_ACTIVITY: Proxy đã được đổi thành công sang IP mới. Vui lòng retry lại ngay.",
     "detail": "UNUSUAL_ACTIVITY: Proxy đã được đổi thành công sang IP mới. Vui lòng retry lại ngay.",
     "proxy_rotated": true,
     "new_proxy": "http://proxymart...:***@163.223.7.194:29194",
     "retryable": true,
     "retry_after_s": 1
   }
   ```
3. **Phía Nova**: Khi nhận status 429 hoặc JSON có `"proxy_rotated": true` / `"retryable": true`, worker Nova chỉ cần **chờ 1 giây rồi retry submit lại request** (IP mới đã có hiệu lực ngay trên bridge).

Chuỗi Google, `complaint`, `workers`, proxy **không** vào JSON khách.

## 9. Checklist lúc code Nova

1. Provider `flowkit` (song song `klinggiare`): `BaseURL` tunnel, không API key Google.
2. Catalog 1 model video + 1 model image (hoặc chung prefix).
3. Health: `GET /health` → `extension_connected`.
4. Upload `image_base64` trước i2v/r2v.
5. Submit + poll như mục 5. Chỉ SUCCESS khi `fifeUrl` chứa `/video/`.
6. Download CDN vào MediaStore; signed URL không đưa khách giữ lâu.
7. Public boundary: không `provider`, `upstream`, nick, project uuid Flow.
8. **Không** implement trong PR này các endpoint Omni / upscale / start+end.
9. Không public-bind `:8100`.
10. Verify: 1 t2v, 1 i2v, 1 r2v, 1 image từ VPS qua tunnel.

## 10. Curl copy-paste (trên PC, tunnel xong thì trên VPS giống)

```bash
BASE=http://127.0.0.1:8100

curl -fsS $BASE/health

# image
curl -fsS $BASE/api/flow/generate-image -H 'content-type: application/json' -d '{
  "prompt": "A red ceramic cup on a white table, studio lighting, no text",
  "project_id": "",
  "aspect_ratio": "IMAGE_ASPECT_RATIO_PORTRAIT"
}'

# t2v
curl -fsS $BASE/api/flow/generate-video -H 'content-type: application/json' -d '{
  "prompt": "Slow orbit around a red ceramic cup, studio lighting, no people, no text",
  "project_id": "",
  "scene_id": "nova-smoke-t2v",
  "aspect_ratio": "VIDEO_ASPECT_RATIO_PORTRAIT"
}'
# copy operations[0].operation.name → OP

curl -fsS $BASE/api/flow/check-status -H 'content-type: application/json' \
  -d "{\"operations\":[{\"name\":\"$OP\"}]}"
```

i2v/r2v: upload hoặc dùng `media_id` image vừa gen, rồi `start_image_media_id` / `reference_media_ids`.

## 11. Việc còn trên máy PC (không phải Nova)

- Chrome nick login `https://flow.google.com/`, tab để mở.
- Dashboard nick + proxy: `/api/accounts` + `scripts/flow-chrome.sh`.
- Thêm nick 2/3: cùng contract, Nova không đổi.

Xong tunnel + adapter mục 6 là Nova bán được image / t2v / i2v / r2v qua Flow.
