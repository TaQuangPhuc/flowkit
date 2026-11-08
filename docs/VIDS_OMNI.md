# Google Vids / Omni — `/api/vids` integration guide

Direct replay vào `appsgenaiserver-pa.clients6.google.com/v1/genai/generate`
bằng session của nick (SAPISID cookie + `vids_doc.txt`). Không qua Flow gRPC.

Endpoints:

- `POST /api/vids/generate` — generate/edit/extend
- `POST /api/vids/upload` — multipart upload ảnh hoặc video → `AVL_…` token
- `GET /api/vids/file/{name}` — re-host mp4 đã gen
- `GET /api/vids/nicks` — nicks có đủ vids artifacts

## `POST /api/vids/generate` fields

| Field | Type | Ghi chú |
|---|---|---|
| `nick` | string (bắt buộc) | nick có `vids_cookies.json` + `vids_doc.txt` |
| `prompt` | string (bắt buộc) | hỗ trợ tag `{img}`, `{img1}`…`{imgN}` |
| `mode` | `t2v` \| `i2v` \| `r2v` \| `edit` \| `extend` | default `t2v` |
| `images` | string[] | AVL tokens đã upload (ảnh reference) |
| `image_b64` | string[] | bytes base64 → agent tự upload thành AVL |
| `video_avl` | string | AVL token của video upload ngoài — cho `edit`, `extend` |
| `video_b64` | string | bytes base64 → tự upload thành `video_avl` |
| `temp_data_id` | string | video nguồn là clip Omni đã gen — cho `edit`, `extend` |
| `image_name` | string | label mention ảnh (default `Image1`) |
| `aspect` | `landscape` \| `portrait` \| `square` | **phải khớp aspect video nguồn** khi edit/extend, nếu không output bị zoom/crop |

Response: `{model, video_url (re-hosted), raw_video_url, temp_data_id, video_file}`.

## Modes và giới hạn đo được (test live trên watson, Oct 2026)

| Mode | Inputs | Refs tối đa | Model upstream |
|---|---|---|---|
| `t2v` | prompt | — | `omni_t2v_psq` |
| `i2v` | 1 ảnh | 1 | `omni_i2v_psq` (template 375) |
| `r2v` | ≥2 ảnh + prompt tag `{imgN}` | **7 ảnh** (8 → `REQUEST_REFUSED`) | `omni_r2v_psq` |
| `edit` + `temp_data_id` | clip Omni đã gen | 0 | `omni_edit_psq` (template 377) |
| `edit` + `video_avl` | video upload ngoài | 0 | `omni_edit_psq` (template 379) |
| `edit` + `video_avl` + `images` | video + ảnh ref | **5 ảnh** + 1 video (6 → refused) | `omni_edit_psq` (template 380) |
| `extend` + `temp_data_id` | clip Omni đã gen | — | `omni_extend_psq` (template 378) |
| `extend` + `video_avl` | video upload ngoài | — | `omni_extend_psq` (template 378, AVL layout giống 379) |

Image edit/generation qua **Flow gRPC** (nano-banana `BatchGenerateImages`) cap cứng
**3 reference images** — ref tray chỉ 3 slot trong proto, >3 không encode được.

## Prompt tags (`{imgN}`)

- `{img}` ≡ `{img1}`; `{img1}`…`{imgN}` map theo thứ tự `images[]`.
- Mỗi tag render thành mention node UUID-linked (bắt buộc trong protocol —
  ảnh phải được mention để bind vào request).
- Ảnh không được tag tường minh được **auto-append mention ở cuối prompt** để
  không mất binding, nhưng nên đặt tag rõ vị trí/vai trò từng ảnh.
- Ví dụ: `"{img1} cầm {img2} đứng cạnh {img3}, cảnh quay chậm"`.

## AVL token TTL

AVL tokens **hết hạn** — token upload nhiều giờ trước trả `500
INTERNAL_SERVER_ERROR` khi dùng. Upload ngay trước khi generate (dùng
`image_b64`/`video_b64` để agent tự upload mỗi request là an toàn nhất).

## Proxy rotation

- `generate()` rotate sang proxy khác trong fleet ngay khi `ProxyError` /
  `ConnectTimeout` — tối đa `min(pool,6)` lần thử.
- Connect timeout 10s — proxy chết fail nhanh, khách không chờ.
- Cookie/token/doc của nick giữ nguyên; chỉ đổi đường mạng.
- Pool = mọi `nicks/*/proxy.txt`; override bằng `nicks/<nick>/proxy_pool.txt`
  (1 proxy/dòng).
- Upload (`docs.google.com`) giữ nguyên proxy của nick — flap chỉ ảnh hưởng
  endpoint genai.

## Errors

- `400 content_policy_violation` (`REQUEST_REFUSED`, `UNSAFE_GENERATION`) —
  non-retryable, trả về đúng như vậy.
- `502` — upstream/proxy/transient, retry được.
- `404` — nick thiếu `vids_cookies.json`/`vids_doc.txt`.
