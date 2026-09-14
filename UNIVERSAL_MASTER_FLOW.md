# BẢN ĐẶC TẢ UNIVERSAL MASTER FLOW 4.0
### Hệ thống sản xuất Video AI tự động đa phong cách (Zero-Knowledge Engine)

---

## I. SƠ ĐỒ TOÀN CẢNH PIPELINE (5 GIAI ĐOẠN)

Hệ thống hoạt động theo cơ chế **Zero-Knowledge** (Tự động hóa hoàn toàn từ ảnh thô):

```text
                             NGƯỜI DÙNG TẢI LÊN:
         [1 Ảnh Sản Phẩm Thô] (+ [1 Ảnh Mẫu] nếu là Review / UGC)
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 0: ZERO-KNOWLEDGE PROFILER (Quét thị giác & OCR nhãn mác)              │
│ Model: Gemini 3.8 Flash Vision (RPC: agJzFb)                                │
│ • Tự bóc tách: Tên sản phẩm, kiểu dáng, chất liệu, tính năng 2-trong-1...   │
│ • Tự nhận diện: Giới tính, độ tuổi người mẫu & đề xuất chất giọng phù hợp.  │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 1: BRAIN SCRIPTING (Biên kịch chuẩn 4 trường dữ liệu)                  │
│ Model: Gemini 3.8 Flash Thinking (RPC: agJzFb)                              │
│ • Nhận 1 trong 4 chế độ: Review Cửa Hàng | POV | UGC | Unboxing Studio.     │
│ • Khóa nhịp thoại: Đúng 34–36 từ / cảnh 8 giây (Chuẩn nhịp nói Veo).        │
│ • Xuất chuẩn 4 trường: Audio Dialogue, Visual Plan, Image Prompt, Motion.   │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 2: KEYFRAME ANCHORING (Sinh ảnh tĩnh chuẩn xác - SỐNG CÒN)             │
│ Model: Google Banana Pro 2 (`GEM_PIX_2`) (RPC: ogiZ0b)                      │
│ • Nạp Reference: Chỉ Sản phẩm (POV/Unboxing) HOẶC Sản phẩm + Mẫu.           │
│ • Khóa cứng: Mẫu ĐÃ CẦM SẴN sản phẩm trên tay hoặc đặt ngay ngắn trên bàn.  │
│ • Mode Unboxing: 100% Sản phẩm trên bục trưng bày/hộp quà, không có người.   │
│ • Chặn đứng 100% lỗi méo ngón tay, mọc ngón thứ 6, biến dạng nhãn mác.      │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │ (Ảnh tĩnh hoàn chỉnh làm first_frame)
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 3: VEO ANIMATION & NATIVE VOICEOVER (Thổi hồn & Lồng tiếng)            │
│ Model: veo_3_1_i2v_lite_low_priority (Chế độ i2v) (RPC: eb1hJf)              │
│ • Nguyên tắc vàng: `KEEP PRODUCT ALMOST STATIC` (Giữ sản phẩm bất biến).    │
│ • Veo tự động sinh cử động nhẹ, nhép môi và lồng tiếng Việt chuẩn xác.      │
│ • Cơ chế Polling (RPC: jwpduf): 4 giây/lần, timeout 240 vòng (~20 phút).    │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │ (Các clip scene_X.mp4 lẻ)
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 4: HẬU KỲ & XUẤT BẢN (FFmpeg Production Engine)                        │
│ • Nối các clip lẻ thành 1 timeline video 24 giây hoàn chỉnh.                │
│ • Tự động mix nhạc nền BGM (Audio Ducking: hạ volume BGM khi có tiếng nói). │
│ • Xuất file MP4 cuối cùng chuẩn 9:16 (TikTok Shop, Facebook Reels, Shorts). │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## II. BẢNG MA TRẬN 4 CHẾ ĐỘ VIDEO THƯƠNG MẠI

| Tiêu chí kỹ thuật | 1. Review Cửa Hàng | 2. POV (Góc nhìn thứ nhất) | 3. UGC (Người dùng thật) | 4. Unboxing Studio (100% SP) |
|---|---|---|---|---|
| **Ảnh đầu vào** | 1 Ảnh SP + 1 Ảnh Mẫu | CHỈ 1 ẢNH SẢN PHẨM | 1 Ảnh SP + 1 Ảnh Mẫu | **CHỈ 1 ẢNH SẢN PHẨM** |
| **Góc máy Camera** | Góc trung/cận ngang tầm mắt | Góc nhìn từ ngực xuống bàn | Góc diện đối diện cá nhân | Cận cảnh (Macro / Close-up / Slow Orbit) |
| **Nhân vật trong hình** | KOL mặc đồ công sở/thanh lịch | Không có mặt, chỉ có 2 bàn tay | KOL ngồi phòng riêng ấm cúng | **TUYỆT ĐỐI KHÔNG CÓ NGƯỜI, KHÔNG CÓ TAY** |
| **Bối cảnh không gian** | Showroom/cửa hàng nhộn nhịp | Mặt bàn gỗ, drap giường sạch | Bàn làm việc, góc phòng ngủ | Hộp quà sang trọng, bục trưng bày studio |
| **Tâm lý & Chiến thuật** | Chuyên gia, uy tín cửa hàng | Khen chất liệu, cảm xúc thật | Tâm sự, khuyên dùng đời thường | Trầm trồ mở hộp, tôn vinh 100% chi tiết SP |
| **Tỷ lệ rủi ro AI** | Thấp (nếu khóa mặt tốt) | Bằng 0% (An toàn tuyệt đối) | Thấp (nếu khóa mặt tốt) | **Bằng 0% (Sắc nét hoàn hảo, không méo)** |

---

## III. CẤU TRÚC 4 TRƯỜNG DỮ LIỆU BẮT BUỘC CHO MỖI PHÂN CẢNH

Mỗi phân cảnh (Scene) được cấu thành từ đúng 4 trường dữ liệu:

```json
{
  "scene_id": 1,
  "duration_seconds": 8,
  "audio_dialogue": "Lời thoại dài đúng 34-36 từ tiếng Việt, mang khẩu ngữ tự nhiên theo phương ngữ được chỉ định.",
  "visual_plan": "Mô tả bối cảnh và diễn xuất (Tiếng Việt cho người dùng xem và bấm Lưu kịch bản).",
  "image_generation_prompt": "Prompt tiếng Anh cho Banana Pro 2 (Bắt buộc chứa PRODUCT REFERENCE LOCK).",
  "video_motion_prompt": "Prompt tiếng Anh cho Veo 3.1 (Bắt buộc chứa PRODUCT LOCK và lệnh KEEP PRODUCT ALMOST STATIC)."
}
```

---

## IV. BỘ PROMPT CHUẨN THEO TỪNG CHẾ ĐỘ

### 1. QUY TẮC BỐI CẢNH (System Prompt cho Gemini Flash agJzFb):

- **Chế độ Review cửa hàng:**
  > `"LOẠI HÌNH: Review cửa hàng. SỐ CẢNH: 3. THỜI LƯỢNG: 8s/cảnh. QUY TẮC BỐI CẢNH: Cửa hàng thực tế. Phía sau có kệ trưng bày, quầy bán hàng và nhiều sản phẩm giống sản phẩm tham chiếu được bày bán. Có thể thấy khách xem hàng, nhân viên tư vấn ở phía xa tạo không khí nhộn nhịp."`

- **Chế độ POV:**
  > `"LOẠI HÌNH: POV. SỐ CẢNH: 3. THỜI LƯỢNG: 8s/cảnh. QUY TẮC BỐI CẢNH: Góc nhìn thứ nhất (Point of view). Camera nhìn từ tầm mắt người trải nghiệm xuống bàn tay. Tuyệt đối KHÔNG có mặt người. Hai bàn tay tự nhiên tương tác, nắn bóp, trải nghiệm độ mềm mại của sản phẩm."`

- **Chế độ UGC:**
  > `"LOẠI HÌNH: UGC. SỐ CẢNH: 3. THỜI LƯỢNG: 8s/cảnh. QUY TẮC BỐI CẢNH: Người dùng thật chia sẻ (User Generated Content). Nhân vật xuất hiện trực diện trước camera tại phòng riêng, cầm sản phẩm trên tay và review chia sẻ trải nghiệm cá nhân tự nhiên, gần gũi."`

- **Chế độ Unboxing (100% Sản Phẩm - Không người):**
  > `"LOẠI HÌNH: Unboxing Studio. SỐ CẢNH: 3. THỜI LƯỢNG: 8s/cảnh. QUY TẮC BỐI CẢNH: Quá trình mở hộp quà sang trọng, hé lộ sản phẩm đặt trên bục trưng bày studio cao cấp với ánh sáng spotlight dịu nhẹ. TUYỆT ĐỐI KHÔNG CÓ NGƯỜI, KHÔNG CÓ MẶT NGƯỜI, KHÔNG CÓ BÀN TAY TRONG KHUNG HÌNH (NO human, NO face, NO hands in frame). Tập trung hoàn toàn vào vẻ đẹp, đường nét, chất liệu, phụ kiện và sự tinh xảo của sản phẩm."`

---

### 2. BOILERPLATE PROMPT SINH ẢNH KEYFRAME (Google Banana Pro 2 ogiZ0b):

#### A. Cho Review Cửa Hàng & UGC (Có nhân vật):
```text
PRODUCT REFERENCE LOCK — HIGHEST PRIORITY. Copy the reference product EXACTLY. Keep identical shape, size, proportions, color, material, packaging, label, logo, text and visible details. DO NOT redesign, replace, add, remove or modify any product detail. Keep the reference face unchanged. Exactly 2 natural hands, 5 fingers each. Product fully visible, label facing camera, hands must not cover the product. Photorealistic, clean bright lighting, sharp details. NO text overlay, icon, sticker, cart, button, UI, effect or watermark.
LOẠI HÌNH: [Review cửa hàng / UGC]
[Chi tiết hành động từ visual_plan]
Avoid: extra limbs, deformed hands, anatomical nonsense, grid, collage, split screen. Photorealistic 8k vertical 9:16.
```

#### B. Cho POV (Góc nhìn thứ nhất, 2 bàn tay trên bàn, không có mặt người):
```text
PRODUCT REFERENCE LOCK — HIGHEST PRIORITY. Copy the reference product EXACTLY. Keep identical shape, size, proportions, color, material, packaging, label, logo, text and visible details. DO NOT redesign, replace, add, remove or modify any product detail. POV first-person perspective looking down at a wooden tabletop. Exactly 2 natural hands, 5 fingers each, gently holding / interacting with the product. NO human face visible in frame. Photorealistic, clean bright natural ambient lighting, 8k resolution vertical 9:16.
[Chi tiết hành động tương tác từ visual_plan]
```

#### C. Cho Unboxing Studio (100% Sản phẩm, KHÔNG CÓ NGƯỜI, KHÔNG CÓ TAY):
```text
PRODUCT REFERENCE LOCK — HIGHEST PRIORITY. Copy the reference product EXACTLY. Keep identical shape, size, proportions, color, material, packaging, label, logo, text and visible details. DO NOT redesign, replace, add, remove or modify any product detail. Studio unboxing reveal, luxury packaging box opening up to display the product on an elegant aesthetic display pedestal with soft dramatic studio spotlighting. Purely product-focused, completely empty of people, NO human, NO human face, NO hands, NO limbs in frame. Photorealistic, 8k resolution vertical 9:16.
[Chi tiết bối cảnh và góc máy từ visual_plan]
```

---

### 3. BOILERPLATE PROMPT SINH VIDEO (Veo 3.1 Low Priority eb1hJf):

```text
PRODUCT LOCK — HIGHEST PRIORITY. The product must remain EXACTLY identical to the reference image in EVERY FRAME: same shape, size, proportions, color, material, packaging, label, logo and text. NEVER redesign, morph, distort or change the product. KEEP PRODUCT ALMOST STATIC. Use static or very slow smooth camera movement (slow push-in, macro pan, or gentle orbit). SPEECH IS AUDIO ONLY — never visualize spoken words or CTA. NO text, cart icon, button, sticker, UI, overlay, animation, effect or watermark. Use ONE consistent Vietnamese voice across all scenes: same gender, accent, tone, pitch and speed.

[scene.video_motion_prompt]

Ghi chú bắt buộc: PRODUCT LOCK — HIGHEST PRIORITY... KEEP PRODUCT ALMOST STATIC.

Say: "[scene.audio_dialogue]" in [Nữ miền Bắc / Nữ miền Nam / Nam miền Bắc / Nam miền Nam]
```

*Lưu ý riêng cho Unboxing Studio:*
```text
Thêm tiền tố: "Purely product showcase. NO human, NO hands, NO people visible in frame. Smooth slow cinematic camera push-in and subtle spotlight glint over product textures."
```

---

## V. HƯỚNG DẪN TRIỂN KHAI BACKEND GATEWAY (PYTHON)

```python
import asyncio
from typing import Optional, List

async def run_universal_master_pipeline(
    product_image_bytes: bytes,
    model_image_bytes: Optional[bytes] = None,
    mode: str = "UNBOXING",  # "REVIEW_STORE", "POV", "UGC", "UNBOXING"
    voice_accent: str = "Nữ miền Bắc"
) -> str:
    """
    Universal Master Pipeline sản xuất video thương mại tự động 100%.
    """
    # 1. BƯỚC 0: Quét thị giác OCR và phân tích sản phẩm (RPC: agJzFb)
    profile = await flow_service.vision_profile(
        product_image=product_image_bytes,
        model_image=model_image_bytes if mode in ["REVIEW_STORE", "UGC"] else None
    )

    # 2. BƯỚC 1: Lên kịch bản 3 cảnh (RPC: agJzFb)
    script = await flow_service.brain_scripting(
        profile=profile,
        mode=mode,
        accent=voice_accent
    )

    # 3. BƯỚC 2 & 3: Xử lý tuần tự từng Scene
    rendered_clips: List[str] = []
    for scene in script["scenes"]:
        # Bước 2: Keyframe Banana Pro 2 (RPC: ogiZ0b)
        keyframe_media_id = await flow_service.generate_keyframe_banana_pro_2(
            product_image=product_image_bytes,
            model_image=model_image_bytes if mode in ["REVIEW_STORE", "UGC"] else None,
            prompt=scene["image_generation_prompt"]
        )

        # Bước 3: Animate Video Veo Low Priority (RPC: eb1hJf)
        clip_path = await flow_service.generate_veo_clip(
            first_frame_id=keyframe_media_id,
            motion_prompt=scene["video_motion_prompt"],
            dialogue=scene["audio_dialogue"],
            accent=voice_accent,
            duration=8
        )
        rendered_clips.append(clip_path)

    # 4. BƯỚC 4: Hậu kỳ nối video & Auto-Ducking BGM qua FFmpeg
    final_video_path = await ffmpeg_engine.stitch_scenes(
        clips=rendered_clips,
        bgm_path="assets/bgm_acoustic_soft.mp3"
    )

    return final_video_path
```
