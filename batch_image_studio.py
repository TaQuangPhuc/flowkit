"""Master Flow: Module Tạo Ảnh Mẫu & Thử Đồ Hàng Loạt.

Model AI Cốt Lõi: 🍌 Nano Banana Pro / 🍌 Nano Banana 2 (Google Internal: GEM_PIX_2, RPC ogiZ0b)
Cơ Chế Tham Chiếu: Dual-Reference Binding (Ref 1: Khuôn mặt/Dáng mẫu + Ref 2: Sản phẩm/Trang phục)
Module A: ImageCreateView (1 Face ID x N Sản phẩm, 1/2/4 biến thể)
Module B: OutfitCreateView (Ghép đồ All-x-All hoặc One-to-One)
"""

import os
import re
import time
import json
import uuid
import base64
import threading
import subprocess
import urllib.request
import urllib.error
from pathlib import Path
from typing import Optional, Any
from concurrent.futures import ThreadPoolExecutor

WORK_DIR = Path("/home/pc/flowkit/auto_runs")
FLOWKIT_API = "http://127.0.0.1:8100"
NOVA_BASE_URL = "https://novagateway.net/v1"
NOVA_API_KEY = os.environ.get("NOVA_API_KEY", "NOVA_e9eWanAexhxLDKfEeVbLUMb16bxR7_AU")
NOVA_MODEL = os.environ.get("NOVA_MODEL", "google/gemini-3.8-flash")

IMAGE_DIRECTOR = """PRODUCT REFERENCE LOCK — HIGHEST PRIORITY.
Copy the reference product EXACTLY.
Keep identical shape, size, proportions, color, material, packaging, label, logo, text and visible details.
DO NOT redesign, replace, add, remove or modify any product detail.
Keep the reference face unchanged.
Exactly 2 natural hands, 5 fingers each.
Product fully visible, label facing camera, hands must not cover the product.
Photorealistic, clean bright lighting, sharp details.
NO text overlay, icon, sticker, cart, button, UI, effect or watermark."""

PROMPT_TEMPLATES = {
    "fashion_studio": "High-end fashion studio lighting, minimalist background.",
    "store_context": "Professional retail store background, organized shelves with products, soft warm lighting, realistic store atmosphere.",
    "unboxing": "Close up on hands opening a premium package on a clean white desk, cinematic focus."
}

VARIANT_ANGLES = [
    "Front view full-body portrait, standing naturally, product fully highlighted with elegant studio lighting.",
    "Three-quarter dynamic angle, slight head turn, stylish fashion pose, premium aesthetic.",
    "Side profile showcase, focusing on silhouette and product texture with soft rim light.",
    "Commercial hero angle, confident eye contact, clear sharp focus on product design and styling."
]

BATCH_JOBS: dict[str, dict] = {}


def call_flowkit_api(endpoint: str, payload: dict, timeout: int = 180, max_retries: int = 4) -> dict:
    """Send request to FlowKit server with auto-retry and failover support."""
    url = f"{FLOWKIT_API}{endpoint}"
    data = json.dumps(payload).encode("utf-8")
    for attempt in range(1, max_retries + 1):
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "FlowKit-BatchStudio/1.0"
            }
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as err:
            err_body = ""
            try:
                err_body = err.read().decode("utf-8", errors="ignore")
            except Exception:
                pass
            if err.code == 429 or "UNUSUAL" in err_body.upper():
                print(f"[FLOWKIT BATCH] 429/Unusual activity on {endpoint} (Attempt {attempt}/{max_retries}). Delaying 4s...")
                time.sleep(4.0)
                continue
            elif attempt < max_retries and err.code in [500, 502, 503, 504]:
                print(f"[FLOWKIT BATCH] HTTP {err.code} on {endpoint} (Attempt {attempt}/{max_retries}). Retrying in 2.5s...")
                time.sleep(2.5)
                continue
            raise RuntimeError(f"FlowKit error {err.code} on {endpoint}: {err_body or err.reason}")
        except Exception as e:
            if attempt < max_retries:
                print(f"[FLOWKIT BATCH] Network error on {endpoint}: {e}. Retrying in 2.5s...")
                time.sleep(2.5)
                continue
            raise


def upload_image_flowkit(image_path: Path) -> str:
    """Upload image to FlowKit Gateway and return media UUID."""
    b64 = base64.b64encode(image_path.read_bytes()).decode("ascii")
    payload = {
        "image_base64": b64,
        "mime_type": "image/jpeg",
        "file_name": image_path.name
    }
    res = call_flowkit_api("/api/flow/upload-image", payload, timeout=120)
    mid = res.get("media_id") or (res.get("raw", {}).get("media") or {}).get("name") or (res.get("media") or {}).get("name") or res.get("_mediaId")
    if not mid:
        raise RuntimeError(f"Failed to upload image {image_path.name}: {res}")
    return mid


def refine_fashion_prompt(user_prompt: str) -> str:
    """Optimize draft prompt into Pro Fashion English Prompt using AI (Claude CLI / Gemini)."""
    if not user_prompt.strip():
        return PROMPT_TEMPLATES["fashion_studio"]

    prompt_text = (
        f"Hãy tối ưu hóa prompt sau đây thành một pro prompt tiếng Anh cho fashion AI. "
        f"Chỉ trả về prompt kết quả bằng tiếng Anh, không giải thích thêm: {user_prompt.strip()}"
    )

    # 1. Primary: Fast Claude CLI print mode
    try:
        cmd = ["claude", "-p", prompt_text, "--output-format", "text"]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=25)
        if proc.returncode == 0 and proc.stdout.strip():
            lines = [l.strip() for l in proc.stdout.strip().splitlines() if l.strip() and not l.strip().startswith("[claude-code:")]
            result = " ".join(lines).strip()
            if result.startswith('"') and result.endswith('"'):
                result = result[1:-1].strip()
            if len(result) > 15:
                return result
    except Exception as e_cli:
        print(f"[PROMPT REFINE] Claude CLI error: {e_cli}")

    # 2. Secondary: Nova Gateway (Gemini 3.8 Flash)
    try:
        payload = {
            "model": NOVA_MODEL,
            "messages": [
                {"role": "system", "content": "Bạn là chuyên gia prompt cho Fashion AI."},
                {"role": "user", "content": prompt_text}
            ],
            "max_tokens": 1024,
            "thinking_config": {"thinking_budget": 0}
        }
        req = urllib.request.Request(
            f"{NOVA_BASE_URL}/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {NOVA_API_KEY}",
                "Content-Type": "application/json"
            }
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            content = data["choices"][0]["message"]["content"].strip()
            if content.startswith('"') and content.endswith('"'):
                content = content[1:-1].strip()
            return content
    except Exception as e_nova:
        pass

    # 3. Fallback to FlowKit internal agJzFb
    try:
        payload = {
            "prompt": prompt_text,
            "system_instruction": "Bạn là chuyên gia prompt cho Fashion AI.",
            "images": []
        }
        res = call_flowkit_api("/api/flow/vision-analyze", payload, timeout=20, max_retries=1)
        text = res.get("text") or res.get("data", {}).get("text") or str(res)
        return text.strip()
    except Exception:
        pass

    return f"High-end fashion editorial photography, {user_prompt.strip()}, elegant lighting, photorealistic 8k vertical."


def load_all_batch_jobs():
    """Load existing batch jobs from disk into memory."""
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    for p in WORK_DIR.glob("batch_*"):
        if p.is_dir() and (p / "batch.json").exists():
            try:
                b_data = json.loads((p / "batch.json").read_text(encoding="utf-8"))
                bid = b_data.get("batch_id")
                if bid:
                    BATCH_JOBS[bid] = b_data
                    count += 1
            except Exception as e:
                print(f"[BATCH] Error loading {p}: {e}")
    print(f"Loaded {count} batch jobs from {WORK_DIR}")


def save_batch_job(batch_id: str, **kwargs):
    """Update and persist batch job state."""
    b = BATCH_JOBS.setdefault(batch_id, {"batch_id": batch_id})
    b.update(kwargs)
    b["updated_at"] = time.time()

    # Recalculate stats
    items = b.get("items", [])
    total = len(items)
    completed = sum(1 for it in items if it.get("status") == "COMPLETED")
    failed = sum(1 for it in items if it.get("status") == "FAILED")
    processing = sum(1 for it in items if it.get("status") in ["GENERATING", "VIDEO_RENDERING", "SUBMITTING"])
    pending = sum(1 for it in items if it.get("status") == "PENDING")

    pct = round((completed / total * 100), 1) if total > 0 else 0
    b["stats"] = {
        "total": total,
        "completed": completed,
        "failed": failed,
        "processing": processing,
        "pending": pending,
        "progress_percent": pct,
        "is_done": (completed + failed) == total and total > 0
    }

    bdir = WORK_DIR / f"batch_{batch_id}"
    bdir.mkdir(parents=True, exist_ok=True)
    try:
        (bdir / "batch.json").write_text(json.dumps(b, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[BATCH] Error saving batch {batch_id}: {e}")


def execute_single_item(batch_id: str, item_id: int):
    """Execute single image generation task with Dual-Reference Binding and 3-attempt auto-retry."""
    b = BATCH_JOBS.get(batch_id)
    if not b:
        return

    item = next((it for it in b.get("items", []) if it.get("item_id") == item_id), None)
    if not item:
        return

    bdir = WORK_DIR / f"batch_{batch_id}"
    cfg = b.get("config", {})
    image_model = cfg.get("imageModel", "Nano Banana Pro")
    aspect_ratio = cfg.get("aspectRatio", "9:16")
    auto_transfer_video = cfg.get("autoTransferToVideo", False)

    ref_media_ids = [mid for mid in [item.get("face_media_id"), item.get("outfit_media_id")] if mid]
    prompt = item.get("prompt", IMAGE_DIRECTOR)

    max_attempts = 3
    success = False
    last_error = ""

    for attempt in range(1, max_attempts + 1):
        item["status"] = "GENERATING"
        item["retry_count"] = attempt - 1
        item["message"] = f"Đang gọi {image_model} (Lần thử {attempt}/{max_attempts})..."
        save_batch_job(batch_id)

        try:
            payload = {
                "prompt": prompt,
                "modelDisplayName": image_model,
                "referenceImageMediaIds": ref_media_ids,
                "aspectRatio": aspect_ratio
            }
            res = call_flowkit_api("/api/flow/generate-image", payload, timeout=180)
            media_list = res.get("media") or (res.get("data") or {}).get("media") or []
            if not media_list:
                raise RuntimeError(f"FlowKit image generation returned no media: {res}")

            media = media_list[0]
            mid = media.get("name") or media.get("image", {}).get("generatedImage", {}).get("mediaId")
            fife_url = media.get("image", {}).get("generatedImage", {}).get("fifeUrl") or f"{FLOWKIT_API}/api/flow/image/{mid}"

            # Save generated image
            img_out_path = bdir / f"item_{item_id}.jpg"
            urllib.request.urlretrieve(fife_url, str(img_out_path))

            item["status"] = "COMPLETED"
            item["output_media_id"] = mid
            item["image_url"] = f"/batch/{batch_id}/item/{item_id}"
            item["message"] = "Hoàn tất thành công!"
            item["completed_at"] = time.time()
            success = True
            save_batch_job(batch_id)
            print(f"[BATCH {batch_id}] Item {item_id} completed successfully (Media: {mid[:12]}...)!")
            break

        except Exception as e:
            last_error = str(e)
            print(f"[BATCH {batch_id}] Item {item_id} attempt {attempt} failed: {e}")
            if attempt < max_attempts:
                time.sleep(2.0)

    if not success:
        item["status"] = "FAILED"
        item["error"] = last_error
        item["message"] = f"Thất bại sau 3 lần thử: {last_error}"
        save_batch_job(batch_id)
        return

    # Auto-transfer to video Veo 3.1 if enabled
    if auto_transfer_video and item.get("output_media_id"):
        transfer_item_to_video(batch_id, item_id)


def transfer_item_to_video(batch_id: str, item_id: int, motion_prompt: str = ""):
    """Transfer completed image as first frame to Veo 3.1 for an 8s commercial video."""
    b = BATCH_JOBS.get(batch_id)
    if not b:
        return
    item = next((it for it in b.get("items", []) if it.get("item_id") == item_id), None)
    if not item or not item.get("output_media_id"):
        return

    bdir = WORK_DIR / f"batch_{batch_id}"
    v_prompt = motion_prompt or (
        "Cinematic commercial fashion shot, model posing smoothly, subtle breathing movement, "
        "gentle fabric motion, product perfectly visible and stable, photorealistic 8k vertical video."
    )

    item["video_status"] = "SUBMITTING"
    save_batch_job(batch_id)

    try:
        payload = {
            "start_image_media_id": item["output_media_id"],
            "prompt": v_prompt,
            "project_id": "",
            "scene_id": f"batch_{batch_id}_{item_id}",
            "aspect_ratio": "VIDEO_ASPECT_RATIO_PORTRAIT",
            "model_family": "veo",
            "duration_s": 8
        }
        res = call_flowkit_api("/api/flow/generate-video", payload, timeout=120)
        ops = res.get("operations") or (res.get("data") or {}).get("operations") or []
        if not ops:
            raise RuntimeError(f"No operations returned: {res}")
        op_name = (ops[0].get("operation") or {}).get("name") or ops[0].get("name")
        if not op_name:
            raise RuntimeError("Could not extract operation name")

        item["video_op_name"] = op_name
        item["video_status"] = "RENDERING"
        save_batch_job(batch_id)

        # Background poller for this single video
        def _poll_video():
            start_t = time.time()
            time.sleep(25)  # initial settling delay
            while time.time() - start_t < 900:
                time.sleep(8)
                try:
                    p_body = {"operations": [{"operation": {"name": op_name}}]}
                    p_res = call_flowkit_api("/api/flow/check-status", p_body, timeout=40)
                    ret_ops = p_res.get("operations") or (p_res.get("data") or {}).get("operations") or []
                    for op_item in ret_ops:
                        st = op_item.get("status")
                        meta = (op_item.get("operation") or {}).get("metadata", {})
                        fife = meta.get("video", {}).get("fifeUrl")
                        if st == "MEDIA_GENERATION_STATUS_SUCCESSFUL" or fife:
                            vpath = bdir / f"video_{item_id}.mp4"
                            urllib.request.urlretrieve(fife, str(vpath))
                            item["video_status"] = "COMPLETED"
                            item["video_url"] = f"/batch/{batch_id}/video/{item_id}"
                            save_batch_job(batch_id)
                            print(f"[BATCH VIDEO] Item {item_id} video downloaded to {vpath.name}!")
                            return
                        elif "FAIL" in str(st).upper():
                            item["video_status"] = "FAILED"
                            item["video_error"] = str(op_item)
                            save_batch_job(batch_id)
                            return
                except Exception as p_err:
                    print(f"[BATCH VIDEO] Polling err item {item_id}: {p_err}")

        threading.Thread(target=_poll_video, daemon=True).start()

    except Exception as exc:
        item["video_status"] = "FAILED"
        item["video_error"] = str(exc)
        save_batch_job(batch_id)


def start_batch_pipeline(batch_id: str):
    """Manage queue execution with designated concurrency (5 or 10 parallel items)."""
    b = BATCH_JOBS.get(batch_id)
    if not b:
        return

    bdir = WORK_DIR / f"batch_{batch_id}"
    cfg = b.get("config", {})
    concurrency = int(cfg.get("imageRunMode", 5))
    concurrency = max(1, min(10, concurrency))

    # First, ensure all reference media IDs are uploaded to FlowKit
    b["message"] = "Đang nạp ảnh tham chiếu (Dual-Reference) vào cụm Google AI..."
    save_batch_job(batch_id)

    # 1. Upload face models
    for m in b.get("face_models", []):
        if not m.get("media_id"):
            p = bdir / m["filename"]
            if p.exists():
                try:
                    m["media_id"] = upload_image_flowkit(p)
                    print(f"[BATCH {batch_id}] Uploaded model {m['filename']} -> {m['media_id']}")
                except Exception as e:
                    print(f"[BATCH {batch_id}] Error uploading model {m['filename']}: {e}")

    # 2. Upload outfits
    for o in b.get("outfits", []):
        if not o.get("media_id"):
            p = bdir / o["filename"]
            if p.exists():
                try:
                    o["media_id"] = upload_image_flowkit(p)
                    print(f"[BATCH {batch_id}] Uploaded outfit {o['filename']} -> {o['media_id']}")
                except Exception as e:
                    print(f"[BATCH {batch_id}] Error uploading outfit {o['filename']}: {e}")

    # Update items with resolved media IDs
    face_map = {m["index"]: m.get("media_id") for m in b.get("face_models", [])}
    outfit_map = {o["index"]: o.get("media_id") for o in b.get("outfits", [])}

    for item in b.get("items", []):
        if not item.get("face_media_id"):
            item["face_media_id"] = face_map.get(item.get("face_index"))
        if not item.get("outfit_media_id"):
            item["outfit_media_id"] = outfit_map.get(item.get("outfit_index"))

    b["message"] = f"Đang điều phối hàng đợi Banana Pro 2 ({concurrency} ảnh song song)..."
    save_batch_job(batch_id)

    items = b.get("items", [])
    print(f"[BATCH PIPELINE] Starting batch {batch_id} with {len(items)} items (Concurrency: {concurrency})...")

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [
            executor.submit(execute_single_item, batch_id, it["item_id"])
            for it in items
            if it.get("status") in ["PENDING", "FAILED"]
        ]
        for f in futures:
            try:
                f.result()
            except Exception as e:
                print(f"[BATCH PIPELINE] Worker thread exception: {e}")

    b["message"] = "Đã hoàn tất toàn bộ tiến trình hàng đợi!"
    save_batch_job(batch_id)
    print(f"[BATCH PIPELINE] Batch {batch_id} all tasks finished!")


def create_batch_image_job(
    face_image_bytes: bytes,
    face_filename: str,
    outfit_files: list[tuple[str, bytes]],
    config: dict
) -> str:
    """Create Module A Job: 1 Face ID x N Outfits x (1, 2 or 4 variants)."""
    batch_id = str(uuid.uuid4())[:8]
    bdir = WORK_DIR / f"batch_{batch_id}"
    bdir.mkdir(parents=True, exist_ok=True)

    # Save face image
    face_fname = f"face_0.jpg"
    (bdir / face_fname).write_bytes(face_image_bytes)

    face_models = [{
        "index": 0,
        "filename": face_fname,
        "media_id": None,
        "orig_name": face_filename
    }]

    # Save outfits (up to 50)
    outfits = []
    for idx, (fname, data) in enumerate(outfit_files[:50]):
        of_name = f"outfit_{idx}.jpg"
        (bdir / of_name).write_bytes(data)
        outfits.append({
            "index": idx,
            "filename": of_name,
            "media_id": None,
            "orig_name": fname
        })

    # Read config
    image_model = config.get("imageModel", "Nano Banana Pro")
    aspect_ratio = config.get("aspectRatio", "9:16")
    image_res = config.get("imageResolution", "2K")
    variants_per_outfit = max(1, min(4, int(config.get("imageCountPerOutfit", 1))))
    image_run_mode = max(1, min(10, int(config.get("imageRunMode", 5))))
    auto_transfer = bool(config.get("autoTransferToVideo", False))
    preset = config.get("preset", "fashion_studio")
    custom_prompt = config.get("customPrompt", "").strip()

    preset_text = PROMPT_TEMPLATES.get(preset, PROMPT_TEMPLATES["fashion_studio"])
    if preset == "custom" and custom_prompt:
        preset_text = custom_prompt

    # Build items
    items = []
    item_counter = 1
    for o in outfits:
        for v in range(variants_per_outfit):
            angle_desc = VARIANT_ANGLES[v % len(VARIANT_ANGLES)] if variants_per_outfit > 1 else ""
            item_prompt = f"{IMAGE_DIRECTOR} {preset_text} {angle_desc}".strip()

            items.append({
                "item_id": item_counter,
                "title": f"Sản phẩm #{o['index'] + 1} (Góc {v + 1}/{variants_per_outfit})",
                "face_index": 0,
                "outfit_index": o["index"],
                "variant_index": v,
                "face_media_id": None,
                "outfit_media_id": None,
                "prompt": item_prompt,
                "status": "PENDING",
                "retry_count": 0,
                "error": None,
                "output_media_id": None,
                "image_url": None,
                "video_status": "NONE",
                "video_url": None,
                "message": "Đang chờ trong hàng đợi..."
            })
            item_counter += 1

    batch_record = {
        "batch_id": batch_id,
        "module": "image_create",
        "module_title": "Tạo Ảnh Mẫu Hàng Loạt (1 Mẫu x N Sản phẩm)",
        "created_at": time.time(),
        "updated_at": time.time(),
        "config": {
            "imageModel": image_model,
            "aspectRatio": aspect_ratio,
            "imageResolution": image_res,
            "imageCountPerOutfit": variants_per_outfit,
            "imageRunMode": image_run_mode,
            "autoTransferToVideo": auto_transfer,
            "preset": preset,
            "customPrompt": custom_prompt
        },
        "face_models": face_models,
        "outfits": outfits,
        "items": items,
        "stats": {
            "total": len(items),
            "completed": 0,
            "failed": 0,
            "processing": 0,
            "pending": len(items),
            "progress_percent": 0.0,
            "is_done": False
        },
        "message": "Đang chuẩn bị nạp dữ liệu..."
    }

    BATCH_JOBS[batch_id] = batch_record
    save_batch_job(batch_id)

    # Launch pipeline in background thread
    threading.Thread(target=start_batch_pipeline, args=(batch_id,), daemon=True).start()
    return batch_id


def create_batch_outfit_job(
    model_files: list[tuple[str, bytes]],
    outfit_files: list[tuple[str, bytes]],
    config: dict
) -> str:
    """Create Module B Job: Outfit Swap / Virtual Try-On (All-x-All or One-to-One)."""
    batch_id = str(uuid.uuid4())[:8]
    bdir = WORK_DIR / f"batch_{batch_id}"
    bdir.mkdir(parents=True, exist_ok=True)

    # Save models
    face_models = []
    for idx, (fname, data) in enumerate(model_files[:50]):
        mf_name = f"model_{idx}.jpg"
        (bdir / mf_name).write_bytes(data)
        face_models.append({
            "index": idx,
            "filename": mf_name,
            "media_id": None,
            "orig_name": fname
        })

    # Save outfits
    outfits = []
    for idx, (fname, data) in enumerate(outfit_files[:50]):
        of_name = f"outfit_{idx}.jpg"
        (bdir / of_name).write_bytes(data)
        outfits.append({
            "index": idx,
            "filename": of_name,
            "media_id": None,
            "orig_name": fname
        })

    pair_mode = config.get("outfitPairMode", "all-x-all")
    outfit_note = config.get("outfitNote", "").strip()
    image_model = config.get("imageModel", "Nano Banana Pro")
    aspect_ratio = config.get("aspectRatio", "9:16")
    image_res = config.get("imageResolution", "2K")
    image_run_mode = max(1, min(10, int(config.get("imageRunMode", 5))))
    auto_transfer = bool(config.get("autoTransferToVideo", False))

    note_part = f" Styling note: {outfit_note}." if outfit_note else ""
    swap_prompt = (
        "Virtual try-on / outfit swap. Take the person and facial identity from reference image 1, "
        "and accurately wear/replace their clothing with the exact outfit, fabric texture, colors, pattern and details from reference image 2. "
        "Preserve original body posture, face identity, hairstyle, lighting, and photorealistic skin texture. "
        f"Clean high-fashion editorial styling.{note_part}"
    )

    # Build pairs
    pairs = []
    if pair_mode == "one-to-one":
        limit = min(len(face_models), len(outfits))
        for i in range(limit):
            pairs.append((i, i))
    else:  # all-x-all
        for m_idx in range(len(face_models)):
            for o_idx in range(len(outfits)):
                pairs.append((m_idx, o_idx))

    items = []
    for idx, (m_idx, o_idx) in enumerate(pairs, 1):
        items.append({
            "item_id": idx,
            "title": f"Mẫu #{m_idx + 1} ✕ Đồ #{o_idx + 1}",
            "face_index": m_idx,
            "outfit_index": o_idx,
            "variant_index": 0,
            "face_media_id": None,
            "outfit_media_id": None,
            "prompt": swap_prompt,
            "status": "PENDING",
            "retry_count": 0,
            "error": None,
            "output_media_id": None,
            "image_url": None,
            "video_status": "NONE",
            "video_url": None,
            "message": "Đang chờ trong hàng đợi..."
        })

    batch_record = {
        "batch_id": batch_id,
        "module": "outfit_create",
        "module_title": f"Thay Đồ Virtual Try-On ({'Tất Cả x Tất Cả' if pair_mode == 'all-x-all' else 'Ghép Cặp 1-1'})",
        "created_at": time.time(),
        "updated_at": time.time(),
        "config": {
            "imageModel": image_model,
            "aspectRatio": aspect_ratio,
            "imageResolution": image_res,
            "imageRunMode": image_run_mode,
            "outfitPairMode": pair_mode,
            "outfitNote": outfit_note,
            "autoTransferToVideo": auto_transfer
        },
        "face_models": face_models,
        "outfits": outfits,
        "items": items,
        "stats": {
            "total": len(items),
            "completed": 0,
            "failed": 0,
            "processing": 0,
            "pending": len(items),
            "progress_percent": 0.0,
            "is_done": False
        },
        "message": "Đang chuẩn bị nạp dữ liệu..."
    }

    BATCH_JOBS[batch_id] = batch_record
    save_batch_job(batch_id)

    # Launch pipeline in background thread
    threading.Thread(target=start_batch_pipeline, args=(batch_id,), daemon=True).start()
    return batch_id


def parse_multipart_form(body: bytes, boundary: bytes) -> tuple[dict[str, str], dict[str, list[tuple[str, bytes]]]]:
    """Parse multipart/form-data into text fields and file tuples (filename, content)."""
    fields: dict[str, str] = {}
    files: dict[str, list[tuple[str, bytes]]] = {}
    parts = body.split(b"--" + boundary)
    for part in parts:
        if not part or part == b"--\r\n" or part == b"--" or part == b"\r\n":
            continue
        if b"\r\n\r\n" not in part:
            continue
        headers_raw, content = part.split(b"\r\n\r\n", 1)
        content = content.rstrip(b"\r\n")
        header_text = headers_raw.decode("utf-8", errors="ignore")

        cd_match = re.search(r'name="([^"]+)"', header_text)
        if not cd_match:
            continue
        name = cd_match.group(1)

        fn_match = re.search(r'filename="([^"]*)"', header_text)
        if fn_match:
            fname = fn_match.group(1)
            if fname and len(content) > 0:
                files.setdefault(name, []).append((fname, content))
        else:
            fields[name] = content.decode("utf-8", errors="ignore").strip()

    return fields, files
