# NOVA ↔ FLOWKIT: ĐẶC TẢ API THỜI TRANG AI LOOKBOOK & VIDEO TEMPLATE
**Tài liệu kỹ thuật dành cho đội ngũ phát triển NOVA (Proxy-Gate-Way)**  
**Phiên bản:** 1.0 (Decoupled from TVC Pipeline)  
**Mục tiêu:** Tạo video Lookbook / Catalog thời trang thuần Visual điện ảnh (Zero-Dialogue, Zero-TTS, High-Fashion Runway Motion)

---

## 1. TỔNG QUAN & SỰ KHÁC BIỆT VỚI TVC AFFILIATE

Khác với pipeline **TVC Affiliate** (vốn tập trung vào bán hàng giỏ hàng, người mẫu nói liên tục, báo giá, khuyến mãi, phụ đề), pipeline **Thời Trang AI Lookbook** là một flow độc lập hoàn toàn:

| Đặc tính | TVC Affiliate Pipeline | Thời Trang AI Lookbook Pipeline |
| :--- | :--- | :--- |
| **Mục đích** | Bán hàng trực tiếp (Direct Response / CTA giỏ hàng) | Xây dựng thương hiệu, Lookbook BST, Catalog thời trang |
| **Thoại & Lip-sync** | Bắt buộc (Voice nói liên tục, Lip-sync, Subtitle) | **Hoàn toàn KHÔNG THOẠI (Zero-Dialogue)**. Người mẫu giữ thần thái sang trọng, không mở miệng nói. |
| **Chất lượng Video AI** | GPU phải chia tài nguyên render cơ miệng nói | **100% GPU tập trung vào độ rủ của vải, nếp gấp satin/tơ, bước sải catwalk và chuyển động xoay** |
| **Âm thanh** | Giọng lồng tiếng + Nhạc nền giảm âm | **Track nhạc thời trang Runway/Vogue/Deep House liền mạch từ đầu đến cuối** |
| **Input cần thiết** | Giá, Khuyến mãi, Tên shop, Nỗi đau khách hàng | **Chỉ cần 1 ảnh Trang phục (+ Ảnh mẫu hoặc chọn Mẫu AI có sẵn)** |

---

## 2. DANH SÁCH ENDPOINTS CHÍNH

- **Base URL:** `http://127.0.0.1:8089` (hoặc domain Nova Gateway chỉ định)

| Method | Endpoint | Mục đích |
| :--- | :--- | :--- |
| `GET` | `/api/fashion-lookbook/templates` | Lấy danh sách Template Video Lookbook đóng gói sẵn (Runway, Cyclorama, Paris Street...) |
| `GET` | `/api/fashion-lookbook/models` | Lấy danh sách kho người mẫu AI có sẵn (Á/Âu thanh lịch) |
| `GET` | `/api/fashion-lookbook/music` | Lấy danh sách nhạc nền thời trang bản quyền |
| `POST` | `/api/fashion-lookbook/create` | Tạo mới một Job Video Lookbook hoàn chỉnh |
| `GET` | `/api/fashion-lookbook/status/{job_id}` | Polling tiến độ thời gian thực từng cảnh và nhận video master |
| `POST` | `/api/fashion-lookbook/scene/{job_id}/{scene_id}/regen` | Vẽ lại riêng 1 phân cảnh (đổi góc pose hoặc chuyển động camera) |

---

## 3. CHI TIẾT CÁC ENDPOINT

### 3.1 Lấy danh sách Template Video: `GET /api/fashion-lookbook/templates`
Trả về các gói góc máy và chuyển động điện ảnh đóng gói sẵn để Nova render lên UI cho người dùng chọn nhanh.

#### Response:
```json
{
  "ok": true,
  "templates": [
    {
      "template_id": "runway_catwalk",
      "name": "👠 Runway Catwalk Sải Bước",
      "badge": "Phổ biến nhất",
      "description": "Sải bước catwalk tự tin, zoom cận chi tiết ngực/eo và xoay người 360 độ tung tà váy.",
      "default_num_scenes": 3,
      "default_duration_per_scene": 8,
      "shot_list": [
        {
          "scene_index": 1,
          "shot_type": "Full-body Runway Catwalk",
          "description": "Toàn thân sải bước thanh lịch tiến về phía ống kính, phô diễn trọn vẹn phom dáng trang phục."
        },
        {
          "scene_index": 2,
          "shot_type": "Medium Bodice & Fabric Detail",
          "description": "Cận trung đẩy máy vào chi tiết nơ/cổ/đường may và độ óng ánh của chất liệu vải cao cấp."
        },
        {
          "scene_index": 3,
          "shot_type": "Dynamic 3/4 Turnaround & Exit",
          "description": "Xoay người góc 3/4 mềm mại khoe chuyển động bồng bềnh của tùng váy rồi sải bước quý phái."
        }
      ]
    },
    {
      "template_id": "cyclorama_studio",
      "name": "🏛️ Cyclorama Minimalist Studio",
      "badge": "Chuẩn Lookbook Hãng",
      "description": "Phông vô cực studio trắng/xám tối giản, ánh sáng khuếch tán dịu nhẹ, giữ nguyên 100% phom dáng & đường may.",
      "default_num_scenes": 3,
      "default_duration_per_scene": 8,
      "shot_list": [
        { "scene_index": 1, "shot_type": "Studio Upright Pose", "description": "Tạo dáng đứng thanh lịch thẳng người, ánh mắt tự tin nhìn ống kính." },
        { "scene_index": 2, "shot_type": "Macro Fabric & Craftsmanship", "description": "Góc quay macro cận cảnh chất vải, độ rủ và đường may giấu chỉ tinh tế." },
        { "scene_index": 3, "shot_type": "Side Profile & Silhouette", "description": "Góc nghiêng phô diễn đường cong eo và độ xòe tự nhiên của dáng váy." }
      ]
    },
    {
      "template_id": "paris_streetwalk",
      "name": "☕ Luxury Streetwalk (Paris / Milan)",
      "badge": "Phong cách Ngoại cảnh",
      "description": "Sải bước tự nhiên trên đường phố châu Âu cổ kính, tà áo bay nhẹ trong gió.",
      "default_num_scenes": 3,
      "default_duration_per_scene": 8,
      "shot_list": [
        { "scene_index": 1, "shot_type": "Street Catwalk Push-in", "description": "Sải bước trên phố cổ Paris, tà váy chuyển động tự nhiên theo nhịp chân." },
        { "scene_index": 2, "shot_type": "Medium Waist & Accessories", "description": "Cận cảnh thắt lưng, tay cầm túi hoặc vuốt nhẹ tà áo duyên dáng." },
        { "scene_index": 3, "shot_type": "Slow Motion Turn", "description": "Quay đầu mỉm cười nhẹ trong nắng vàng chiều hoàng hôn." }
      ]
    }
  ]
}
```

---

### 3.2 Lấy danh sách Người Mẫu AI Preset: `GET /api/fashion-lookbook/models`
Dành cho khách hàng không có người mẫu riêng, chỉ cần chọn 1 mẫu AI từ kho có sẵn.

#### Response:
```json
{
  "ok": true,
  "models": [
    {
      "model_id": "asian_elegance_24",
      "name": "Lan Anh (Á Đông Thanh Lịch)",
      "gender": "female",
      "age_approx": 24,
      "height_approx": "1m70",
      "vibe": "Da trắng sứ, thanh tú, vóc dáng mảnh mai, thần thái tiểu thư nhẹ nhàng",
      "thumbnail_url": "/assets/models/asian_elegance_24.jpg"
    },
    {
      "model_id": "korean_minimalist_23",
      "name": "Min-ji (Hàn Quốc Trẻ Trung)",
      "gender": "female",
      "age_approx": 23,
      "height_approx": "1m68",
      "vibe": "Phong cách ulzzang hiện đại, tóc ngắn/búi gọn, phong thái trong trẻo",
      "thumbnail_url": "/assets/models/korean_minimalist_23.jpg"
    },
    {
      "model_id": "caucasian_chic_25",
      "name": "Elena (Âu Mỹ Haute Couture)",
      "gender": "female",
      "age_approx": 25,
      "height_approx": "1m76",
      "vibe": "Gương mặt góc cạnh chuẩn sàn diễn Paris/Milan, thần thái high-fashion sắc sảo",
      "thumbnail_url": "/assets/models/caucasian_chic_25.jpg"
    }
  ]
}
```

---

### 3.3 Tạo Job Video Lookbook: `POST /api/fashion-lookbook/create`

Gửi request dạng `multipart/form-data` hoặc `application/json`.

#### Parameters:
| Tên tham số | Kiểu dữ liệu | Bắt buộc | Mặc định | Mô tả |
| :--- | :--- | :--- | :--- | :--- |
| `outfit_file` | File Binary | Có (nếu ko gửi URL) | - | File ảnh trang phục (váy, đầm, vest, set đồ) |
| `outfit_image_url` | String | Có (nếu ko gửi file) | `""` | Link ảnh trang phục |
| `model_file` | File Binary | Không | `null` | Ảnh chân dung người mẫu riêng của khách hàng |
| `model_image_url` | String | Không | `""` | Link ảnh người mẫu riêng của khách hàng |
| `model_preset_id` | String | Không | `"asian_elegance_24"` | ID mẫu AI nếu không tải ảnh mẫu riêng |
| `template_id` | String | Không | `"runway_catwalk"` | ID template video (`runway_catwalk`, `cyclorama_studio`, `paris_streetwalk`) |
| `aspect_ratio` | String | Không | `"9:16"` | `"9:16"` (Vertical Shorts/Reels) hoặc `"16:9"` (Ngang) |
| `num_scenes` | Integer | Không | `3` | Số phân cảnh (từ `1` đến `5`) |
| `scene_duration` | Integer | Không | `8` | Thời lượng mỗi cảnh: `8` hoặc `10` giây |
| `bgm_id` | String | Không | `"vogue_runway"` | ID nhạc nền (`"vogue_runway"`, `"deep_minimal_house"`, `"paris_chic_jazz"`, `"none"`) |
| `num_threads` | Integer | Không | `5` | Số luồng GPU xử lý song song (1 – 10) |

#### Response (200 OK):
```json
{
  "ok": true,
  "job_id": "fsh_a7b9c1d2",
  "status": "QUEUED",
  "message": "Đang khởi tạo pipeline Lookbook điện ảnh...",
  "template_id": "runway_catwalk",
  "num_scenes": 3,
  "total_duration_seconds": 24,
  "aspect_ratio": "9:16",
  "created_at": 1789709200.15
}
```

---

### 3.4 Polling Tiến Độ Thời Gian Thực: `GET /api/fashion-lookbook/status/{job_id}`

Frontend gọi định kỳ mỗi `3.0s` – `4.0s` để cập nhật tiến độ cho người dùng.

#### Response:
```json
{
  "ok": true,
  "job_id": "fsh_a7b9c1d2",
  "status": "RENDERING_VIDEOS",
  "progress_percent": 65,
  "message": "Cảnh 2/3: Đang hoàn tất video chi tiết chất vải (Veo 3.1)...",
  "template_id": "runway_catwalk",
  "scenes": [
    {
      "scene_id": 1,
      "shot_type": "Full-body Runway Catwalk",
      "status": "COMPLETED",
      "status_text": "Đã hoàn thành video",
      "keyframe_url": "/api/fashion-lookbook/fsh_a7b9c1d2/kf/1",
      "video_url": "/api/fashion-lookbook/fsh_a7b9c1d2/clip/1"
    },
    {
      "scene_id": 2,
      "shot_type": "Medium Bodice & Fabric Detail",
      "status": "RENDERING_VIDEO",
      "status_text": "Keyframe hoàn tất. Đang render video chuyển động...",
      "keyframe_url": "/api/fashion-lookbook/fsh_a7b9c1d2/kf/2",
      "video_url": null
    },
    {
      "scene_id": 3,
      "shot_type": "Dynamic 3/4 Turnaround & Exit",
      "status": "KEYFRAME_READY",
      "status_text": "Keyframe đã sẵn sàng. Chờ nạp vào cụm GPU...",
      "keyframe_url": "/api/fashion-lookbook/fsh_a7b9c1d2/kf/3",
      "video_url": null
    }
  ],
  "final_video_url": null,
  "completed_at": null
}
```

Khi toàn bộ hoàn tất (`"status": "COMPLETED"`):
```json
{
  "ok": true,
  "job_id": "fsh_a7b9c1d2",
  "status": "COMPLETED",
  "progress_percent": 100,
  "message": "Hoàn thành xuất sắc toàn bộ 3 phân cảnh Lookbook điện ảnh (24 giây)!",
  "final_video_url": "/api/fashion-lookbook/fsh_a7b9c1d2/final",
  "completed_at": 1789709420.55
}
```

---

### 3.5 Tái Tạo / Vẽ Lại Riêng 1 Phân Cảnh: `POST /api/fashion-lookbook/scene/{job_id}/{scene_id}/regen`

Cho phép người dùng trên UI bấm "Vẽ lại dáng này" hoặc tinh chỉnh chuyển động camera của phân cảnh bất kỳ mà không phải chạy lại cả job.

#### Body JSON:
```json
{
  "camera_motion_override": "Slow cinematic orbit around model waist, highlighting pleated fabric volume",
  "auto_render": true
}
```

#### Response:
```json
{
  "ok": true,
  "job_id": "fsh_a7b9c1d2",
  "scene_id": 2,
  "status": "RENDERING_VIDEO",
  "message": "Đang tái tạo phân cảnh 2 với chuyển động camera mới..."
}
```

---

## 4. GỢI Ý THIẾT KẾ GIAO DIỆN (UI/UX) CHO ĐỘI NGŨ NOVA

1. **Thanh Tab Chính (Master Navigation):**
   Thêm 1 tab riêng: `👗 VIDEO TEMPLATE: THỜI TRANG AI LOOKBOOK` (đặt cạnh `🎬 STUDIO TVC AFFILIATE`).
2. **Khu Vực Nạp Tài Nguyên (2 Box Kéo Thả Đơn Giản):**
   - **Box 1:** Kéo thả Ảnh Trang Phục (Váy, Đầm, Set đồ).
   - **Box 2:** Chọn Người Mẫu: Có 2 lựa chọn (Tải ảnh mẫu riêng của Shop HOẶC Click chọn 1 trong 3 mẫu AI có sẵn: Á Đông, Hàn Quốc, Âu Mỹ).
3. **Khu Vực Chọn Mẫu Video (Template Carousel / Cards):**
   - Hiển thị 3 Card mẫu có thumbnail động hoặc ảnh gif:
     - Card 1: `👠 Sải Bước Runway Catwalk`
     - Card 2: `🏛️ Studio Phông Vô Cực Cyclorama`
     - Card 3: `☕ Ngoại Cảnh Đường Phố Paris`
4. **Nút Hành Động Duy Nhất:**
   - `[ 🚀 TẠO VIDEO LOOKBOOK ĐIỆN ẢNH (24S) ]`
5. **Timeline / Cụm Hiển Thị Kết Quả:**
   - Ngay khi Cảnh 1 xong Keyframe -> hiển thị ngay ảnh mẫu mặc đồ bên dưới.
   - Khi hoàn thành 3 cảnh -> Tự động ghép Master Video có sẵn nhạc nền thời trang, có nút **Tải Master Video** và các nút **Xem riêng Cảnh 1, 2, 3**.

---

## 5. MASTER FLOW 2 GIAI ĐOẠN (LOOKBOOK GALLERY & I2V DỰNG VIDEO THEO YÊU CẦU)

Đối với người dùng muốn **duyệt Album ảnh tĩnh Lookbook chất lượng cao trước (2K/4K, 15 Dáng Studio), sau đó mới chủ động chọn các ảnh ưng ý để dựng video**, hệ thống cung cấp bộ API 2 giai đoạn chuẩn Master Flow:

### 5.1 Lấy toàn bộ danh mục Presets: `GET /api/fashion-lookbook/presets`
Trả về 15 Dáng POSES, 5 Style Presets, 4 Lighting Presets, 5 Background Presets, 5 Motion Presets, 4 Camera Movements, Models và BGM.

### 5.2 Giai đoạn 1 — Sinh Album Ảnh Lookbook: `POST /api/fashion-lookbook/stage1/generate-images`
- **Hỗ trợ:** `multipart/form-data` hoặc `application/json`.
- **Đầu vào:**
  - `product_files` (hoặc `product_urls`): 1 đến 9 sản phẩm thời trang (áo, quần, chân váy, áo khoác, túi xách...).
  - `model_file` (hoặc `model_url`, `model_preset_id`): Ảnh mẫu riêng hoặc chọn mẫu AI có sẵn.
  - `background_file` (hoặc `background_url`, `background_preset`): Phông nền tùy chọn (mặc định: `pure white studio`).
  - `aspect_ratio`: `3:4` (chuẩn Lookbook/E-commerce), `9:16`, `16:9`.
  - `quality`: `2K`, `4K`.
  - `quantity`: 1, 2, 4, 6, 8 (mặc định: 4).
  - `selected_poses`: Mảng các dáng trong 15 POSES (hoặc để trống hệ thống tự chọn đa dạng góc đẹp nhất).
- **Đầu ra:** `job_id`, `status: "QUEUED"`.

### 5.3 Tải Toàn Bộ Album Dạng ZIP: `GET /api/fashion-lookbook/stage1/zip/{job_id}`
Tải về file ZIP chứa trọn bộ ảnh Lookbook độ nét cao 2K/4K đã sinh.

### 5.4 Giai đoạn 2 — Dựng Video Cho Các Ảnh Đã Chọn: `POST /api/fashion-lookbook/stage2/generate-videos`
- **Body JSON:**
```json
{
  "job_id": "lookbook_job_id",
  "selected_image_ids": [1, 3],
  "motion_preset": "Elegant Turnaround",
  "camera_movement": "Cinematic Push-in",
  "duration": 8,
  "video_model": "Omni Flash",
  "is_lite_mode": false
}
```
- Khóa đúng khung hình gốc `firstFrameImageMediaId = image_mid`, tạo các clip chuyển động mượt mà không biến dạng sản phẩm hay khuôn mặt.

### 5.5 Xem & Tải Từng Video Clip:
- Stream: `GET /api/fashion-lookbook/stage2/clips/{job_id}/{clip_id}`
- Download: `GET /api/fashion-lookbook/stage2/clips/{job_id}/{clip_id}/download`

### 5.6 Ghép Thành Master Video Kèm BGM Runway: `POST /api/fashion-lookbook/stitch-master`
- **Body JSON:** `{"job_id": "...", "bgm_id": "vogue_runway"}`
- Nối tất cả các clip đã hoàn thành thành 1 video duy nhất kèm track nhạc Runway/Vogue: `GET /api/fashion-lookbook/{job_id}/master`.

---

## 6. CẤU HÌNH SỐ LUỒNG XỬ LÝ SONG SONG (CONCURRENCY & THREAD POOL)

Hệ thống hỗ trợ chạy song song nhiều luồng render cả ở **Giai đoạn 1 (sinh ảnh Album)** và **Giai đoạn 2 (kết xuất video I2V)**, giúp tăng tốc độ xử lý lên gấp nhiều lần.

### 6.1 Xem số luồng hiện tại: `GET /api/fashion-lookbook/threads`
- **Response:**
```json
{
  "ok": true,
  "num_threads": 4,
  "default_threads": 4,
  "max_allowed": 10
}
```

### 6.2 Cập nhật số luồng toàn cục: `POST /api/fashion-lookbook/threads`
- **Body JSON:**
```json
{
  "num_threads": 6
}
```
- Số luồng hợp lệ từ `1` đến `10`. Mặc định: `4`.

### 6.3 Chỉ định số luồng riêng cho từng Job:
Cả Stage 1 và Stage 2 đều cho phép truyền tham số `num_threads` riêng biệt theo yêu cầu:
- **Stage 1 (`POST /api/fashion-lookbook/stage1/generate-images`):** Thêm trường `num_threads` vào multipart/form-data hoặc JSON.
- **Stage 2 (`POST /api/fashion-lookbook/stage2/generate-videos`):** Thêm `"num_threads": 4` vào body JSON.

### 6.4 Lấy danh sách các Job gần nhất: `GET /api/fashion-lookbook/jobs?limit=50`
- **Response:**
```json
{
  "ok": true,
  "total": 1,
  "jobs": [
    {
      "job_id": "0047ecc8da1f",
      "stage": 2,
      "status": "COMPLETED",
      "progress_percent": 100,
      "message": "Hoàn thành xuất sắc toàn bộ 3 phân cảnh Lookbook điện ảnh (24 giây)!",
      "aspect_ratio": "3:4",
      "quantity": 4,
      "num_threads": 4,
      "created_at": 1789824028.11,
      "updated_at": 1789825770.09,
      "master_url": "/api/fashion-lookbook/0047ecc8da1f/master",
      "zip_url": "/api/fashion-lookbook/stage1/zip/0047ecc8da1f",
      "num_images": 4,
      "num_clips": 4
    }
  ]
}
```

