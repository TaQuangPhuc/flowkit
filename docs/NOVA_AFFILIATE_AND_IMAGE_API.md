# NOVA ↔ FLOWKIT: TÀI LIỆU ĐẶC TẢ API AFFILIATE TVC, BATCH IMAGE STUDIO & QUẢN LÝ TIẾN TRÌNH

Tài liệu này cung cấp toàn bộ đặc tả kỹ thuật, schema JSON, quy trình xử lý và hướng dẫn giao diện cho đội ngũ phát triển **NOVA (Proxy-Gate-Way)** để tích hợp toàn bộ hệ sinh thái:
1. **API Cấu hình số luồng (Concurrency & Thread Pool)**: Tra cứu và thiết lập số luồng xử lý song song trên giao diện Nova.
2. **API Sửa Prompt Keyframe, Motion và Tái tạo từng cảnh (Interactive Scene Studio)**: Sửa prompt trực tiếp trên giao diện, vẽ lại riêng keyframe hoặc tái tạo lại toàn bộ cảnh.
3. **Cơ chế Hiển thị tiến độ từng cảnh theo thời gian thực (Real-time Scene-by-Scene Status)**: Keyframe xuất xưởng là hiển thị ngay bên dưới, báo trạng thái video đang chạy, cảnh nào hoàn thành, cảnh nào lỗi.
4. **Quy tắc Bảo mật & Sanitize Lỗi**: Tuyệt đối không để lộ thông báo kỹ thuật nội bộ (đổi proxy, unusual activity, IP block) cho khách hàng cuối.
5. **Batch Image Studio & Virtual Try-On**: Tạo ảnh mẫu thời trang & Thử đồ hàng loạt (Dual-Reference Banana Pro 2).
6. **Auto-TVC Affiliate Pipeline**: Tạo video quảng cáo bán hàng TikTok/Reels tự động từ 1 ảnh sản phẩm.

---

## 1. TỔNG QUAN KIẾN TRÚC & BASE URL

```
┌────────────────────────────────────────────────────────────────────────┐
│                        NOVA GATEWAY (Client / UI)                      │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                  HTTP Requests (Multipart / JSON)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│             AUTO-TVC & BATCH STUDIO SERVICE (Port :8089)               │
│  - Điều phối hàng đợi, ThreadPool concurrency (1 - 10)                 │
│  - Cung cấp API quản lý số luồng, chỉnh sửa prompt từng cảnh           │
│  - Quản lý Jobs, Auto-retry 3 lần khi lỗi, Sanitize lỗi nội bộ         │
│  - AI Vision Profiler (Gemini 3.8 Flash) & AI Scriptwriter             │
│  - Bộ ghép FFmpeg (Keyframe, Veo 3.1, Edge-TTS, BGM TikTok, Subtitle)  │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                     Internal RPC Bridge (Port :8100)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                    FLOWKIT GATEWAY ENGINE (Port :8100)                 │
│  - Google Flow Bridge (Nano Banana Pro / Banana 2 & Veo 3.1)           │
│  - Dual-Reference Image Binding (Ref 1: Face + Ref 2: Outfit)          │
│  - Tự động xoay Proxy Dân cư sạch khi gặp Unusual Activity            │
└────────────────────────────────────────────────────────────────────────┘
```

- **Base URL Auto-TVC & Studio**: `http://127.0.0.1:8089` (hoặc qua Tailscale `http://100.122.194.27:8089`)
- **Base URL FlowKit Core Engine**: `http://127.0.0.1:8100`

---

## 2. API CẤU HÌNH SỐ LUỒNG (CONCURRENCY & THREAD POOL)

Hiện tại giao diện web của NOVA chưa có ô cấu hình số luồng. FlowKit cung cấp API để NOVA truy vấn năng lực phần cứng/tài khoản và cho phép người dùng chọn số luồng song song phù hợp.

### 2.1 API Lấy thông tin số luồng & Năng lực hệ thống: `GET /api/system/concurrency`
- **Mục đích**: NOVA gọi endpoint này khi load trang để hiển thị số luồng khả dụng và giá trị khuyến nghị lên UI.
- **Request**: `GET http://127.0.0.1:8089/api/system/concurrency`
- **Response**:
  ```json
  {
    "ok": true,
    "active_workers": 2,
    "max_concurrency": 4,
    "default_threads": 5,
    "min_threads": 1,
    "max_threads": 10,
    "recommended_threads": 5,
    "description": "Số luồng xử lý song song tối ưu cho cụm Google AI."
  }
  ```

### 2.2 Cách NOVA gửi cấu hình số luồng khi tạo Job:
Khi gửi request tạo Job, frontend của NOVA chỉ cần đính kèm tham số luồng:
1. **Tạo TVC Video (`POST /`)**: Truyền trường `num_threads` (từ `1` đến `10`, mặc định `5`).
2. **Tạo Batch Image (`POST /api/batch-image/create`)**: Truyền trường `imageRunMode` (từ `1` đến `10`, mặc định `5`).
3. **Tạo Virtual Try-On (`POST /api/batch-outfit/create`)**: Truyền trường `imageRunMode` (từ `1` đến `10`, mặc định `5`).

> **💡 Gợi ý thiết kế UI cho NOVA**:
> Thêm 1 thanh trượt (Slider) hoặc ô Select box chọn số luồng:
> `⚡ Số luồng xử lý song song: [ 5 luồng (Khuyến nghị) ▼ ]` (Options: 1, 2, 3, 5, 8, 10).

---

## 3. API SỬA PROMPT KEYFRAME, MOTION VÀ TÁI TẠO TỪNG CẢNH (INTERACTIVE SCENE STUDIO)

Khách hàng cần có khả năng xem và sửa prompt vẽ Keyframe, prompt chuyển động video Veo 3.1, và lời thoại ngay trên giao diện web của Nova, sau đó bấm nút để vẽ lại riêng keyframe hoặc tái tạo lại toàn bộ cảnh.

### 3.1 API Lấy chi tiết 1 phân cảnh: `GET /api/scene/{job_id}/{scene_id}`
- **Request**: `GET http://127.0.0.1:8089/api/scene/c88ac2a0/1`
- **Response**:
  ```json
  {
    "ok": true,
    "job_id": "c88ac2a0",
    "scene_id": 1,
    "status": "KEYFRAME_READY",
    "status_text": "Keyframe đã xong. Video đang chạy...",
    "image_prompt": "PRODUCT REFERENCE LOCK — HIGHEST PRIORITY. Copy reference product EXACTLY...",
    "motion_prompt": "PRODUCT LOCK — HIGHEST PRIORITY. Continuous fluid camera movement...",
    "dialogue": "Này các bạn, set gối plush Bemori siêu mềm siêu dễ thương này nè...",
    "keyframe_url": "/job/c88ac2a0/kf/1",
    "video_url": "/job/c88ac2a0/clip/1",
    "tts_url": "/job/c88ac2a0/tts/1"
  }
  ```

### 3.2 API Cập nhật Prompt phân cảnh (không chạy render ngay): `POST /api/scene/update`
- **Content-Type**: `application/json`
- **Body**:
  ```json
  {
    "job_id": "c88ac2a0",
    "scene_id": 1,
    "image_prompt": "Prompt ảnh keyframe mới do khách chỉnh sửa...",
    "motion_prompt": "Prompt chuyển động video mới...",
    "dialogue": "Lời thoại tiếng Việt mới..."
  }
  ```
- **Response**:
  ```json
  {
    "ok": true,
    "message": "Đã cập nhật prompt Cảnh 1 thành công!",
    "scene": { ... }
  }
  ```

### 3.3 API Vẽ lại RIÊNG Keyframe cho 1 Cảnh (~10 giây): `POST /api/scene/regen-keyframe`
- Dành cho trường hợp khách ưng kịch bản nhưng muốn đổi góc chụp ảnh sản phẩm / vóc dáng người mẫu mà **chưa muốn render video tốn tài nguyên**.
- **Content-Type**: `application/json`
- **Body**:
  ```json
  {
    "job_id": "c88ac2a0",
    "scene_id": 1,
    "image_prompt": "Góc máy ba phần tư, người mẫu cầm sản phẩm cười tươi, ánh sáng studio rực rỡ..."
  }
  ```
- **Response**:
  ```json
  {
    "status": "PROCESSING",
    "job_id": "c88ac2a0",
    "scene_id": 1,
    "message": "Đang vẽ lại riêng Keyframe Cảnh 1..."
  }
  ```

### 3.4 API Tái tạo TOÀN BỘ Phân cảnh (Keyframe + Video Veo 3.1 + TTS Audio): `POST /api/scene/regen`
- Chạy lại toàn bộ phân cảnh được chọn và tự động ghép nối lại vào video TVC tổng hợp.
- **Content-Type**: `application/json`
- **Body**:
  ```json
  {
    "job_id": "c88ac2a0",
    "scene_id": 1,
    "image_prompt": "Prompt ảnh mới...",
    "motion_prompt": "Camera zoom chậm vào sản phẩm...",
    "dialogue": "Lời thoại mới...",
    "regen_keyframe": true
  }
  ```
- **Response**:
  ```json
  {
    "status": "PROCESSING",
    "job_id": "c88ac2a0",
    "scene_id": 1,
    "message": "Bắt đầu tái tạo Phân Cảnh 1..."
  }
  ```

---

## 4. CƠ CHẾ HIỂN THỊ TIẾN ĐỘ TỪNG CẢNH THEO THỜI GIAN THỰC (REAL-TIME SCENE CARDS)

Hệ thống cung cấp trạng thái độc lập cho từng phân cảnh trong mảng `scenes` khi gọi `GET /api/status/{job_id}`:

### 4.1 Chu trình trạng thái của từng phân cảnh (`sc.status` & `sc.status_text`):
1. **`PENDING`**: `"Đang chờ trong hàng đợi..."` (Chưa tới lượt xử lý).
2. **`GENERATING_KEYFRAME`**: `"Đang vẽ ảnh Keyframe..."` (Banana Pro 2 đang sinh ảnh bìa).
3. **`KEYFRAME_READY`**: `"Keyframe đã xong. Video đang chạy..."`
   👉 **LẬP TỨC HIỂN THỊ ẢNH**: Trường `keyframe_url` trả về ngay link ảnh `/job/{job_id}/kf/{scene_id}`. Giao diện khách hàng lập tức nhìn thấy ảnh keyframe bên dưới, không cần đợi video xong mới thấy!
4. **`RENDERING_VIDEO`**: `"Keyframe đã xong. Video đang chạy..."` (GPU Veo 3.1 đang render chuyển động 8s).
5. **`COMPLETED`**: `"Đã hoàn thành video"`
   👉 Trường `video_url` trả về link video `/job/{job_id}/clip/{scene_id}`.
6. **`FAILED`**: `"Lỗi khi render video"`
   👉 Cảnh này bị lỗi, các cảnh khác vẫn chạy bình thường. Giao diện hiển thị nút "Tái tạo Phân Cảnh này" để khách bấm thử lại.

### 4.2 Cấu trúc JSON trả về trong `GET /api/status/{job_id}`:
```json
{
  "job_id": "c88ac2a0",
  "status": "RENDERING",
  "message": "Veo 3 đang render đồng loạt các cảnh...",
  "scenes": [
    {
      "scene_id": 1,
      "title": "Hook Mở Đầu",
      "status": "COMPLETED",
      "status_text": "Đã hoàn thành video",
      "keyframe_url": "/job/c88ac2a0/kf/1",
      "video_url": "/job/c88ac2a0/clip/1",
      "tts_url": "/job/c88ac2a0/tts/1",
      "image_generation_prompt": "...",
      "video_motion_prompt": "...",
      "audio_dialogue": "..."
    },
    {
      "scene_id": 2,
      "title": "Công Năng Nổi Bật",
      "status": "RENDERING_VIDEO",
      "status_text": "Keyframe đã xong. Video đang chạy...",
      "keyframe_url": "/job/c88ac2a0/kf/2",
      "video_url": null,
      "tts_url": "/job/c88ac2a0/tts/2",
      "image_generation_prompt": "...",
      "video_motion_prompt": "...",
      "audio_dialogue": "..."
    },
    {
      "scene_id": 3,
      "title": "Kêu Gọi Hành Động (CTA)",
      "status": "FAILED",
      "status_text": "Lỗi khi render video",
      "keyframe_url": "/job/c88ac2a0/kf/3",
      "video_url": null,
      "tts_url": "/job/c88ac2a0/tts/3",
      "image_generation_prompt": "...",
      "video_motion_prompt": "...",
      "audio_dialogue": "..."
    }
  ],
  "final_video_url": null
}
```

---

## 5. QUY TẮC BẢO MẬT & SANITIZE LỖI (KHÔNG LỘ PROXY HAY UNUSUAL CHO KHÁCH)

Khách hàng tuyệt đối không được nhìn thấy các thông báo kỹ thuật nội bộ liên quan đến proxy, IP, hay giới hạn của Google:
1. **Cơ chế Hot-swap Proxy ngầm**: Khi gặp mã `PUBLIC_ERROR_UNUSUAL_ACTIVITY` hoặc HTTP 429, FlowKit tự động đổi sang IP dân cư sạch kế tiếp và tự thử lại (retry) trong nền.
2. **Thông báo thân thiện**: Mọi thông điệp trả về cho người dùng chỉ được sử dụng các câu nhẹ nhàng, tích cực:
   - ✅ `"Hệ thống đang điều phối tài nguyên máy chủ AI..."`
   - ✅ `"Đang kết nối lại máy chủ AI..."`
   - ✅ `"Đang tối ưu dữ liệu phân cảnh..."`
   - ❌ **CẤM**: Không hiển thị các từ: `UNUSUAL_ACTIVITY`, `proxy rotated`, `changing proxy`, `proxy die`, `IP block`, `Google rate limit`, `ogiZ0b`.

---

## 6. MODULE BATCH IMAGE STUDIO (TẠO ẢNH MẪU & VIRTUAL TRY-ON HÀNG LOẠT)

### 6.1 Tạo Job Ảnh Mẫu: `POST /api/batch-image/create`
- **Content-Type**: `multipart/form-data`
- **Form Fields**:
  - `face_image`: 1 ảnh khuôn mặt người mẫu (Face ID).
  - `outfit_files`: N ảnh sản phẩm/quần áo (hỗ trợ tới 50 ảnh).
  - `imageModel`: `Nano Banana Pro` hoặc `Nano Banana 2`.
  - `aspectRatio`: `9:16` hoặc `16:9` hoặc `1:1`.
  - `imageCountPerOutfit`: `1`, `2`, hoặc `4` biến thể góc chụp.
  - `imageRunMode`: `1` đến `10` (số luồng song song, mặc định `5`).
  - `autoTransferToVideo`: `true` hoặc `false` (nếu `true`, mỗi ảnh xong tự động render video Veo 3.1).
  - `preset`: `fashion_studio` | `store_context` | `unboxing` | `custom`.
  - `customPrompt`: Prompt tuỳ biến nếu chọn preset `custom`.

### 6.2 Tạo Job Virtual Try-On: `POST /api/batch-outfit/create`
- **Form Fields**:
  - `model_files`: M ảnh người mẫu.
  - `outfit_files`: N ảnh trang phục.
  - `outfitPairMode`: `all-x-all` (M x N) hoặc `one-to-one` (min(M, N)).
  - `imageRunMode`: `5`.

### 6.3 Theo dõi tiến độ: `GET /batch/{batch_id}/status`
- Trả về tiến độ tổng %, trạng thái từng ảnh (`GENERATING`, `COMPLETED`, `FAILED`), URL ảnh `/batch/{bid}/item/{id}`, URL video `/batch/{bid}/video/{id}`.

### 6.4 Thử lại 1 item ảnh: `POST /api/batch/retry`
```json
{ "batch_id": "a8f9c12d", "item_id": 2 }
```

### 6.5 Chuyển tiếp 1 ảnh sang Veo 3.1: `POST /api/batch/transfer-video`
```json
{
  "batch_id": "a8f9c12d",
  "item_id": 1,
  "motion_prompt": "Cinematic slow push in, model poses elegantly, 8k commercial quality."
}
```

---

## 7. MODULE AUTO-TVC AFFILIATE PIPELINE (TẠO VIDEO BÁN HÀNG TỰ ĐỘNG)

### 7.1 Tạo Video TVC: `POST /`
- **Content-Type**: `multipart/form-data`
- **Form Fields**:
  - `product`: File ảnh sản phẩm (bắt buộc).
  - `model`: File ảnh người mẫu (tuỳ chọn).
  - `background`: File ảnh showroom/bối cảnh (tuỳ chọn).
  - `flow_mode`: `pov` (góc nhìn thứ nhất) | `unboxing` (studio 100% sản phẩm) | `demo` (test tính năng) | `ugc` (reviewer selfie) | `store_review` (tvc showroom).
  - `num_scenes`: Số phân cảnh (`1` đến `5`, chuẩn: `3`).
  - `scene_duration`: Độ dài mỗi cảnh (`4`, `6`, hoặc `8` giây).
  - `voice`: `female_north` | `female_south` | `male_north` | `male_south`.
  - `bgm`: `tiktok_upbeat` | `tiktok_snitch` | `tiktok_vlog` | `acoustic_soft` | `none`.
  - `num_threads`: Số luồng xử lý song song (`1` đến `10`, mặc định `5`).

### 7.2 Tiếp tục Job bị dở dang: `POST /api/job/resume`
```json
{ "job_id": "c88ac2a0" }
```

---

## 8. DANH SÁCH MÃ LỆNH CURL TEST TRỰC TIẾP

### 1. Kiểm tra số luồng khả dụng:
```bash
curl -s http://127.0.0.1:8089/api/system/concurrency
```

### 2. Lấy chi tiết 1 cảnh để hiển thị form sửa prompt:
```bash
curl -s http://127.0.0.1:8089/api/scene/c88ac2a0/1
```

### 3. Cập nhật prompt cảnh:
```bash
curl -s -X POST http://127.0.0.1:8089/api/scene/update \
  -H "Content-Type: application/json" \
  -d '{"job_id": "c88ac2a0", "scene_id": 1, "image_prompt": "Prompt ảnh mới", "dialogue": "Lời thoại mới"}'
```

### 4. Vẽ lại riêng Keyframe cho cảnh 1:
```bash
curl -s -X POST http://127.0.0.1:8089/api/scene/regen-keyframe \
  -H "Content-Type: application/json" \
  -d '{"job_id": "c88ac2a0", "scene_id": 1, "image_prompt": "Góc chụp mới cận cảnh sản phẩm"}'
```

### 5. Tái tạo toàn bộ cảnh 1 (Ảnh + Video + Audio):
```bash
curl -s -X POST http://127.0.0.1:8089/api/scene/regen \
  -H "Content-Type: application/json" \
  -d '{"job_id": "c88ac2a0", "scene_id": 1, "image_prompt": "Prompt mới", "motion_prompt": "Motion mới", "dialogue": "Thoại mới", "regen_keyframe": true}'
```
