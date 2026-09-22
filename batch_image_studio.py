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

WORK_DIR = Path("/home/pc/flowkit/auto_runs")
FLOWKIT_API = os.environ.get("FLOWKIT_API", "http://127.0.0.1:8100").rstrip("/")
NOVA_BASE_URL = os.environ.get("NOVA_BASE_URL", "https://api.vilao.ai/v1")
NOVA_API_KEY = os.environ.get("NOVA_API_KEY", "sk-72afd079199f58a7b302e65b6690744ce8cf7b44c0dcd163070052e7fa774535")
NOVA_MODEL = os.environ.get("NOVA_MODEL", "chib/deepseek-v4.1-flash")

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
_BATCH_LOCKS = {}
_SCHEDULER = None
_SCHEDULER_LOCK = threading.Lock()


def batch_scheduler():
    global _SCHEDULER
    with _SCHEDULER_LOCK:
        if _SCHEDULER is None:
            import sys
            from batch_scheduler import BatchScheduler
            _SCHEDULER = BatchScheduler(sys.modules[__name__])
        return _SCHEDULER


def defer_not_submitted(batch_id, item, phase, error):
    count = item.get(f"{phase}_busy_count", 0) + 1
    delay = max(error.retry_after_s, min(60, 3 * 2 ** min(count - 1, 5)))
    item[f"{phase}_busy_count"] = count
    item[f"{phase}_next_attempt_at"] = time.time() + delay
    item["status" if phase == "image" else "video_status"] = "QUEUED"
    item["retry_safe" if phase == "image" else "video_retry_safe"] = True
    item.pop("error" if phase == "image" else "video_error", None)
    item["message"] = "Đang chờ lượt; hệ thống bận, yêu cầu chưa được gửi."
    save_batch_job(batch_id)


def _batch_lock(batch_id):
    return _BATCH_LOCKS.setdefault(batch_id, threading.RLock())


class FlowRequestError(RuntimeError):
    def __init__(self, message, *, retry_safe=False, retry_after_s=3):
        super().__init__(message)
        self.retry_safe = retry_safe
        self.retry_after_s = retry_after_s


from agent.services.flow_trace import traced_sync, trace_id, emit as trace_emit, summary as trace_summary
from agent.services.parked_retry import ParkedBackoff, retry_after_seconds


@traced_sync("batch.api")
def call_flowkit_api(endpoint: str, payload: dict, timeout: int = 180, max_retries: int = 4) -> dict:
    """Send request to FlowKit server with auto-retry and failover support."""
    if endpoint in ("/api/flow/generate-image", "/api/flow/generate-video", "/api/flow/generate-video-refs"):
        max_retries = 1
    url = f"{FLOWKIT_API}{endpoint}"
    data = json.dumps(payload).encode("utf-8")
    parked = ParkedBackoff()
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "FlowKit-BatchStudio/1.0"
            }
        )
        req.add_header("X-Request-ID", trace_id.get())
        trace_emit("batch.api.attempt", attempt=attempt, endpoint=endpoint, timeout_s=timeout)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as err:
            trace_emit("batch.api.http_error", status=err.code, attempt=attempt)
            err_body = ""
            try:
                err_body = err.read().decode("utf-8", errors="ignore")
            except Exception:
                pass
            # A fully parked fleet submitted nothing: wait it out instead of
            # failing the item after ~10s of 503s. Does not consume an attempt.
            if parked.wait(err, err_body):
                attempt -= 1
                continue
            if attempt < max_retries and (err.code == 429 or "UNUSUAL" in err_body.upper()):
                print(f"[FLOWKIT BATCH] 429/Unusual activity on {endpoint} (Attempt {attempt}/{max_retries}). Delaying 4s...")
                time.sleep(retry_after_seconds(err, 4.0))
                continue
            elif attempt < max_retries and err.code in [500, 502, 503, 504]:
                print(f"[FLOWKIT BATCH] HTTP {err.code} on {endpoint} (Attempt {attempt}/{max_retries}). Retrying in 2.5s...")
                time.sleep(2.5)
                continue
            try:
                detail = json.loads(err_body)
            except (ValueError, TypeError):
                detail = {}
            safe = isinstance(detail, dict) and detail.get("error") == "FLOW_REQUEST_NOT_SUBMITTED" and detail.get("retryable") is True
            retry_after = detail.get("retry_after_s", 3) if isinstance(detail, dict) else 3
            try:
                retry_after = max(3, float(retry_after))
            except (ValueError, TypeError):
                retry_after = 3
            raise FlowRequestError(f"FlowKit error {err.code} on {endpoint}: {err_body or err.reason}", retry_safe=safe, retry_after_s=retry_after)
        except Exception as e:
            if attempt < max_retries:
                print(f"[FLOWKIT BATCH] Network error on {endpoint}: {e}. Retrying in 2.5s...")
                time.sleep(2.5)
                continue
            raise


def upload_image_flowkit(image_path: Path, reference_media_id: str = "") -> str:
    """Upload image to FlowKit Gateway and return media UUID."""
    b64 = base64.b64encode(image_path.read_bytes()).decode("ascii")
    payload = {
        "image_base64": b64,
        "mime_type": "image/jpeg",
        "file_name": image_path.name
    }
    if reference_media_id:
        payload["reference_media_id"] = reference_media_id
    res = call_flowkit_api("/api/flow/upload-image", payload, timeout=120)
    mid = res.get("media_id") or (res.get("raw", {}).get("media") or {}).get("name") or (res.get("media") or {}).get("name") or res.get("_mediaId")
    if not mid:
        raise RuntimeError(f"Failed to upload image {image_path.name}: {res}")
    return mid


def prepare_batch_references(batch_id: str):
    """Bind references before generation; reupload legacy mixed-nick inputs."""
    with _batch_lock(batch_id):
        batch = BATCH_JOBS[batch_id]
        entities = batch.get("face_models", []) + batch.get("outfits", [])
        anchor = batch.get("reference_anchor_media_id", "")
        bdir = WORK_DIR / f"batch_{batch_id}"
        for entity in entities:
            if (not anchor or not entity.get("media_id")
                    or entity.get("binding_anchor_media_id") != anchor):
                mid = upload_image_flowkit(bdir / entity["filename"], anchor)
                anchor = anchor or mid
                entity.update(media_id=mid, binding_anchor_media_id=anchor)
                batch["reference_anchor_media_id"] = anchor
                save_batch_job(batch_id)
        faces = {v["index"]: v["media_id"] for v in batch.get("face_models", [])}
        outfits = {v["index"]: v["media_id"] for v in batch.get("outfits", [])}
        for item in batch.get("items", []):
            if item.get("status") == "COMPLETED":
                continue
            face, outfit = faces.get(item.get("face_index")), outfits.get(item.get("outfit_index"))
            if not face or not outfit:
                raise FlowRequestError("REFERENCE_UPLOAD_FAILED: both references are required")
            item.update(face_media_id=face, outfit_media_id=outfit)
        save_batch_job(batch_id)


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

    # 2. Secondary: Nova Gateway (DeepSeek -> Grok fallback)
    candidate_models = [
        os.environ.get("NOVA_MODEL", NOVA_MODEL),
        "spd/grok-4.6",
        "grok-4.6",
        "cnt/grok-4.6",
        "fa/grok-4.6-fast"
    ]
    for model_name in candidate_models:
        try:
            payload = {
                "model": model_name,
                "messages": [
                    {"role": "system", "content": "Bạn là chuyên gia prompt cho Fashion AI."},
                    {"role": "user", "content": prompt_text}
                ],
                "max_tokens": 1024
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
                if "choices" in data and data["choices"]:
                    content = data["choices"][0]["message"].get("content", "").strip()
                    if content.startswith('"') and content.endswith('"'):
                        content = content[1:-1].strip()
                    if content:
                        return content
        except Exception:
            continue

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
    with _batch_lock(batch_id):
        _save_batch_job(batch_id, **kwargs)


def _save_batch_job(batch_id: str, **kwargs):
    b = BATCH_JOBS.setdefault(batch_id, {"batch_id": batch_id})
    b.update(kwargs)
    b["updated_at"] = time.time()

    # Recalculate stats
    items = b.get("items", [])
    total = len(items)
    completed = sum(1 for it in items if it.get("status") == "COMPLETED")
    failed = sum(1 for it in items if it.get("status") == "FAILED")
    processing = sum(1 for it in items if it.get("status") in ["GENERATING", "VIDEO_RENDERING", "SUBMITTING"])
    pending = sum(1 for it in items if it.get("status") in {"PENDING", "QUEUED"})
    video_pending = sum(1 for it in items if it.get("video_status") == "QUEUED")
    video_processing = sum(1 for it in items if it.get("video_status") in {"SUBMITTING", "RENDERING", "GENERATING"})

    pct = round((completed / total * 100), 1) if total > 0 else 0
    is_video_running = any(it.get("video_status") in ["QUEUED", "SUBMITTING", "RENDERING", "GENERATING"] for it in items)
    b["stats"] = {
        "total": total,
        "completed": completed,
        "failed": failed,
        "processing": processing + video_processing,
        "pending": pending,
        "video_pending": video_pending,
        "progress_percent": pct,
        "is_done": (completed + failed) == total and total > 0 and not is_video_running
    }

    if pending or is_video_running:
        b["message"] = "Đang xử lý; các yêu cầu còn lại đang chờ lượt."
    elif (completed + failed) == total:
        b["message"] = f"Đã hoàn thành {completed}/{total} ảnh" + (f" ({failed} ảnh cần kiểm tra)." if failed else ".")

    bdir = WORK_DIR / f"batch_{batch_id}"
    bdir.mkdir(parents=True, exist_ok=True)
    try:
        pending = bdir / "batch.json.pending"
        pending.write_text(json.dumps(b, ensure_ascii=False, indent=2), encoding="utf-8")
        with pending.open("rb") as saved:
            os.fsync(saved.fileno())
        pending.replace(bdir / "batch.json")
    except Exception as e:
        print(f"[BATCH] Error saving batch {batch_id}: {e}")
        raise


def execute_single_item(batch_id: str, item_id: int):
    """Generate once; retry only a proven not-submitted request."""
    b = BATCH_JOBS.get(batch_id)
    if not b:
        return

    item = next((it for it in b.get("items", []) if it.get("item_id") == item_id), None)
    if not item:
        return

    with _batch_lock(batch_id):
        if item.get("status") not in {"PENDING", "QUEUED", "FAILED"} or item.get("retry_safe") is False:
            return
        item["status"] = "GENERATING"
        item.pop("error", None)
        try:
            prepare_batch_references(batch_id)
        except Exception as exc:
            item.update(status="FAILED", error=str(exc), message="Không nạp đủ ảnh tham chiếu; chưa gửi tạo ảnh.")
            save_batch_job(batch_id)
            return

    bdir = WORK_DIR / f"batch_{batch_id}"
    cfg = b.get("config", {})
    image_model = cfg.get("imageModel", "Nano Banana Pro")
    aspect_ratio = cfg.get("aspectRatio", "9:16")
    auto_transfer_video = cfg.get("autoTransferToVideo", False)

    ref_media_ids = [mid for mid in [item.get("face_media_id"), item.get("outfit_media_id")] if mid]
    prompt = item.get("prompt", IMAGE_DIRECTOR)

    max_attempts = 1
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

            item["output_media_id"] = mid
            save_batch_job(batch_id)

            # Save generated image
            img_out_path = bdir / f"item_{item_id}.jpg"
            urllib.request.urlretrieve(fife_url, str(img_out_path))

            item["status"] = "COMPLETED"
            item.pop("error", None)
            item.pop("retry_safe", None)
            item.pop("image_next_attempt_at", None)
            item["output_media_id"] = mid
            item["image_url"] = f"/batch/{batch_id}/item/{item_id}"
            item["message"] = "Hoàn tất thành công!"
            item["completed_at"] = time.time()
            if auto_transfer_video:
                item["video_status"] = "QUEUED"
            success = True
            save_batch_job(batch_id)
            print(f"[BATCH {batch_id}] Item {item_id} completed successfully (Media: {mid[:12]}...)!")
            break

        except Exception as e:
            last_error = str(e)
            print(f"[BATCH {batch_id}] Item {item_id} attempt {attempt} failed: {e}")
            if not isinstance(e, FlowRequestError) or not e.retry_safe:
                item["retry_safe"] = False
                break
            defer_not_submitted(batch_id, item, "image", e)
            return

    if not success:
        item["status"] = "FAILED"
        item["error"] = last_error
        item["message"] = f"Thất bại sau {attempt} lần thử: {last_error}"
        save_batch_job(batch_id)
        return

    # Auto-transfer to video Veo 3.1 if enabled
    # Video submission gets its own fair queue turn; polling never holds a lane.


def queue_batch_video(batch_id, item_id, motion_prompt=""):
    with _batch_lock(batch_id):
        batch = BATCH_JOBS.get(batch_id, {})
        item = next((i for i in batch.get("items", []) if i["item_id"] == item_id), None)
        if (not item or item.get("status") != "COMPLETED"
                or item.get("video_status") in {"QUEUED", "SUBMITTING", "RENDERING", "COMPLETED"}
                or item.get("video_retry_safe") is False):
            return False
        item.update(video_status="QUEUED", queued_motion_prompt=motion_prompt)
        batch["queue_version"] = 1
        save_batch_job(batch_id)
    scheduler = batch_scheduler()
    scheduler.register(batch_id)
    scheduler.start()
    return True


def transfer_item_to_video(batch_id: str, item_id: int, motion_prompt: str = ""):
    """Transfer completed image as first frame to Veo 3.1 for an 8s commercial video."""
    b = BATCH_JOBS.get(batch_id)
    if not b:
        bpath = WORK_DIR / f"batch_{batch_id}" / "batch.json"
        if bpath.exists():
            try:
                b = json.loads(bpath.read_text(encoding="utf-8"))
                BATCH_JOBS[batch_id] = b
            except Exception:
                pass
    if not b:
        return
    item = next((it for it in b.get("items", []) if it.get("item_id") == item_id), None)
    if not item:
        return

    bdir = WORK_DIR / f"batch_{batch_id}"
    item_img = bdir / f"item_{item_id}.jpg"

    v_prompt = motion_prompt or item.get("queued_motion_prompt") or (
        "Cinematic commercial fashion shot, model posing smoothly, subtle breathing movement, "
        "gentle fabric motion, product perfectly visible and stable, photorealistic 8k vertical video."
    )

    with _batch_lock(batch_id):
        if item.get("video_status") in {"SUBMITTING", "RENDERING", "COMPLETED"}:
            return
        if item.get("video_retry_safe") is False:
            return
        item["video_status"] = "SUBMITTING"
        item["video_error"] = None
        save_batch_job(batch_id)

    try:
        # The generated UUID already pins its owner; do not reupload onto another nick.
        start_mid = item.get("output_media_id")
        if not start_mid:
            raise RuntimeError(f"Không tìm thấy ảnh gốc cho item #{item_id}")

        payload = {
            "start_image_media_id": start_mid,
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

        item["video_poll_started_at"] = time.time()
        item.pop("video_next_attempt_at", None)
        save_batch_job(batch_id)
        start_video_poller(batch_id, item_id)

    except Exception as exc:
        if isinstance(exc, FlowRequestError) and exc.retry_safe:
            defer_not_submitted(batch_id, item, "video", exc)
            return
        item["video_status"] = "FAILED"
        item["video_retry_safe"] = False
        item["video_error"] = str(exc)
        save_batch_job(batch_id)


_VIDEO_POLLERS = set()
_VIDEO_POLLERS_LOCK = threading.Lock()


def start_video_poller(batch_id, item_id):
    key = (batch_id, item_id)
    with _VIDEO_POLLERS_LOCK:
        if key in _VIDEO_POLLERS:
            return
        _VIDEO_POLLERS.add(key)
    threading.Thread(target=_poll_batch_video, args=key, daemon=True, name="BatchVideoPoll").start()


def _poll_batch_video(batch_id, item_id):
    item = next(i for i in BATCH_JOBS[batch_id]["items"] if i["item_id"] == item_id)
    op_name = item["video_op_name"]
    deadline = item.get("video_poll_started_at", time.time()) + 900
    try:
        while time.time() < deadline:
            try:
                result = call_flowkit_api("/api/flow/check-status", {"operations": [{"operation": {"name": op_name}}]}, timeout=40)
                ops = result.get("operations") or (result.get("data") or {}).get("operations") or []
                for op in ops:
                    status = str(op.get("status", ""))
                    url = (op.get("operation") or {}).get("metadata", {}).get("video", {}).get("fifeUrl")
                    if url:
                        path = WORK_DIR / f"batch_{batch_id}" / f"video_{item_id}.mp4"
                        urllib.request.urlretrieve(url, str(path))
                        item.update(video_status="COMPLETED", video_url=f"/batch/{batch_id}/video/{item_id}", video_error=None)
                        save_batch_job(batch_id)
                        return
                    if "FAIL" in status.upper():
                        item.update(video_status="FAILED", video_retry_safe=False, video_error=str(op))
                        save_batch_job(batch_id)
                        return
            except Exception as exc:
                print(f"[BATCH VIDEO] Poll failed for {batch_id}/{item_id}: {type(exc).__name__}")
            time.sleep(8)
        item.update(video_status="FAILED", video_retry_safe=False,
                    video_error="UPSTREAM_TIMEOUT: chưa có kết quả; giữ mã thao tác để đối soát, không tạo lại.")
        save_batch_job(batch_id)
    finally:
        with _VIDEO_POLLERS_LOCK:
            _VIDEO_POLLERS.discard((batch_id, item_id))


def start_batch_pipeline(batch_id: str):
    """Prepare references once and persist dispatchable work before scheduling."""
    b = BATCH_JOBS.get(batch_id)
    if not b:
        return
    try:
        prepare_batch_references(batch_id)
    except Exception as exc:
        with _batch_lock(batch_id):
            for item in b.get("items", []):
                if item.get("status") == "PENDING":
                    item.update(status="FAILED", error=str(exc), message="Không nạp đủ ảnh tham chiếu; chưa gửi tạo ảnh.")
            save_batch_job(batch_id)
        return
    with _batch_lock(batch_id):
        b["queue_version"] = 1
        for item in b.get("items", []):
            if item.get("status") == "PENDING":
                item.update(status="QUEUED", message="Đang chờ lượt.")
                item.pop("error", None)
        save_batch_job(batch_id)
    scheduler = batch_scheduler()
    scheduler.register(batch_id)
    scheduler.start()


def parse_auto_transfer(value=False):
    """Multipart fields are strings: bool('false') must never enable paid video."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "on", "yes"}:
            return True
        if normalized in {"false", "0", "off", "no", ""}:
            return False
    raise ValueError("autoTransferToVideo must be true or false")


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
    auto_transfer = parse_auto_transfer(config.get("autoTransferToVideo", False))
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

    batch_record["queue_version"] = 1
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
    auto_transfer = parse_auto_transfer(config.get("autoTransferToVideo", False))

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

    batch_record["queue_version"] = 1
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
