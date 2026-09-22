"""FastAPI router for Fashion Lookbook Studio endpoints."""

import os
import time
from typing import Optional
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel

import fashion_lookbook_studio as fls

router = APIRouter(prefix="/fashion-lookbook", tags=["fashion-lookbook"])


class LookbookCreateJSON(BaseModel):
    outfit_image_url: Optional[str] = ""
    outfit_url: Optional[str] = ""
    model_image_url: Optional[str] = ""
    model_url: Optional[str] = ""
    model_preset_id: Optional[str] = "asian_elegance_24"
    template_id: Optional[str] = "runway_catwalk"
    aspect_ratio: Optional[str] = "9:16"
    num_scenes: Optional[int] = 3
    scene_duration: Optional[int] = 8
    bgm_id: Optional[str] = "vogue_runway"


class LookbookRegenRequest(BaseModel):
    camera_motion_override: Optional[str] = ""
    auto_render: Optional[bool] = True


class Stage2VideoRequest(BaseModel):
    job_id: str
    selected_image_ids: list[int]
    motion_preset: Optional[str] = "Elegant Turnaround"
    camera_movement: Optional[str] = "Cinematic Push-in"
    duration: Optional[int] = 8
    video_model: Optional[str] = "Omni Flash"
    is_lite_mode: Optional[bool] = False
    num_threads: Optional[int] = None


class LookbookThreadsRequest(BaseModel):
    num_threads: int


class StitchMasterRequest(BaseModel):
    job_id: str
    bgm_id: Optional[str] = "vogue_runway"


@router.get("/threads")
async def get_threads():
    """Get current Lookbook thread pool concurrency configuration."""
    return fls.get_lookbook_threads()


@router.post("/threads")
async def set_threads(payload: LookbookThreadsRequest):
    """Set Lookbook thread pool concurrency (1 to 10 parallel threads)."""
    return fls.set_lookbook_threads(payload.num_threads)


@router.get("/jobs")
async def list_jobs(limit: int = 50):
    """List all recent fashion lookbook jobs."""
    jobs_list = []
    for jid, job in list(fls.LOOKBOOK_JOBS.items()):
        item = {
            "job_id": jid,
            "stage": job.get("stage", 1),
            "status": job.get("status"),
            "progress_percent": job.get("progress_percent", 0),
            "message": job.get("message", ""),
            "aspect_ratio": job.get("aspect_ratio", "3:4"),
            "quantity": job.get("quantity", 0),
            "num_threads": job.get("num_threads", fls.LOOKBOOK_CONFIG["num_threads"]),
            "created_at": job.get("created_at"),
            "updated_at": job.get("updated_at"),
            "master_url": f"/api/fashion-lookbook/{jid}/master" if (fls.WORK_DIR / f"lookbook_{jid}" / "final_lookbook.mp4").exists() else None,
            "zip_url": job.get("zip_url"),
            "num_images": len(job.get("images", [])),
            "num_clips": len(job.get("video_clips", []))
        }
        jobs_list.append(item)
    jobs_list.sort(key=lambda x: x.get("updated_at") or 0, reverse=True)
    return {"ok": True, "total": len(jobs_list), "jobs": jobs_list[:limit]}


@router.get("/presets")
async def get_presets():
    """List all Master Flow presets (15 poses, styles, lightings, backgrounds, motions, cameras, models)."""
    return {"ok": True, "presets": fls.get_all_lookbook_presets()}


@router.get("/templates")
async def get_templates():
    """List available fashion lookbook templates."""
    return {"ok": True, "templates": fls.get_public_templates()}


@router.get("/models")
async def get_models():
    """List available AI model presets."""
    return {"ok": True, "models": fls.get_public_models()}


@router.get("/music")
async def get_music():
    """List available fashion runway background music presets."""
    return {"ok": True, "music": fls.get_public_music()}


@router.get("/status/{job_id}")
async def get_status(job_id: str):
    """Poll real-time status of a Lookbook generation job with auto-healing."""
    recovered = fls.recover_lookbook_job(job_id)
    if recovered:
        return recovered
    job = fls.LOOKBOOK_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Lookbook job {job_id} not found")
    return job


@router.post("/create")
async def create_job(request: Request):
    """Create a new Fashion Lookbook generation job (accepts multipart/form-data or JSON)."""
    import batch_image_studio as bis
    outfit_bytes = None
    outfit_url = ""
    model_bytes = None
    model_url = ""
    model_preset_id = "asian_elegance_24"
    template_id = "runway_catwalk"
    aspect_ratio = "9:16"
    num_scenes = 3
    scene_duration = 8
    bgm_id = "vogue_runway"

    content_type = request.headers.get("content-type", "")
    body = await request.body()
    if "boundary=" in content_type:
        boundary = content_type.split("boundary=")[1].strip().encode()
        fields, files = bis.parse_multipart_form(body, boundary)
        outfit_files = files.get("outfit_file") or files.get("outfit_files") or []
        if outfit_files:
            outfit_bytes = outfit_files[0][1]
        model_files = files.get("model_file") or files.get("model_files") or []
        if model_files:
            model_bytes = model_files[0][1]

        outfit_url = fields.get("outfit_image_url") or fields.get("outfit_url") or ""
        model_url = fields.get("model_image_url") or fields.get("model_url") or ""
        model_preset_id = fields.get("model_preset_id") or model_preset_id
        template_id = fields.get("template_id") or template_id
        aspect_ratio = fields.get("aspect_ratio") or aspect_ratio
        try:
            num_scenes = int(fields.get("num_scenes") or num_scenes)
        except Exception:
            pass
        try:
            scene_duration = int(fields.get("scene_duration") or scene_duration)
        except Exception:
            pass
        bgm_id = (
            fields.get("bgm_id") or
            fields.get("music_id") or
            fields.get("music") or
            fields.get("bgm") or
            fields.get("audio_id") or
            fields.get("sound") or
            bgm_id
        )
    else:
        try:
            import json
            req_json = json.loads(body.decode("utf-8")) if body else {}
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")
        outfit_url = req_json.get("outfit_image_url") or req_json.get("outfit_url") or ""
        model_url = req_json.get("model_image_url") or req_json.get("model_url") or ""
        model_preset_id = req_json.get("model_preset_id") or model_preset_id
        template_id = req_json.get("template_id") or template_id
        aspect_ratio = req_json.get("aspect_ratio") or aspect_ratio
        try:
            num_scenes = int(req_json.get("num_scenes") or num_scenes)
        except Exception:
            pass
        try:
            scene_duration = int(req_json.get("scene_duration") or scene_duration)
        except Exception:
            pass
        bgm_id = (
            req_json.get("bgm_id") or
            req_json.get("music_id") or
            req_json.get("music") or
            req_json.get("bgm") or
            req_json.get("audio_id") or
            req_json.get("sound") or
            bgm_id
        )

    if not outfit_bytes and not outfit_url:
        raise HTTPException(status_code=400, detail="Vui lòng tải lên file ảnh trang phục hoặc truyền outfit_image_url!")

    job_id = fls.create_lookbook_job(
        outfit_bytes=outfit_bytes,
        outfit_url=outfit_url,
        model_bytes=model_bytes,
        model_url=model_url,
        model_preset_id=model_preset_id,
        template_id=template_id,
        aspect_ratio=aspect_ratio,
        num_scenes=num_scenes,
        scene_duration=scene_duration,
        bgm_id=bgm_id
    )

    return {
        "ok": True,
        "job_id": job_id,
        "status": "QUEUED",
        "message": "Đang khởi tạo pipeline Lookbook điện ảnh...",
        "template_id": template_id,
        "num_scenes": num_scenes,
        "total_duration_seconds": num_scenes * scene_duration,
        "aspect_ratio": aspect_ratio,
        "created_at": time.time()
    }


@router.post("/scene/{job_id}/{scene_id}/regen")
async def regen_scene(job_id: str, scene_id: int, payload: LookbookRegenRequest):
    """Regenerate a specific scene in a Lookbook job."""
    res = fls.regen_lookbook_scene(job_id, scene_id, payload.camera_motion_override or "")
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error", "Lỗi tái tạo phân cảnh"))
    return res


@router.get("/{job_id}/final")
async def get_final_video(job_id: str):
    """Stream final master lookbook video."""
    fpath = fls.WORK_DIR / f"lookbook_{job_id}" / "final_lookbook.mp4"
    if not fpath.exists():
        raise HTTPException(status_code=404, detail="Final lookbook video not found or still rendering")
    return FileResponse(fpath, media_type="video/mp4")


@router.get("/{job_id}/clip/{scene_id}")
async def get_clip_video(job_id: str, scene_id: int):
    """Stream individual scene video clip."""
    fpath = fls.WORK_DIR / f"lookbook_{job_id}" / f"clip_{scene_id}.mp4"
    if not fpath.exists():
        raise HTTPException(status_code=404, detail=f"Clip for scene {scene_id} not found")
    return FileResponse(fpath, media_type="video/mp4")


@router.get("/{job_id}/kf/{scene_id}")
async def get_keyframe_image(job_id: str, scene_id: int):
    """View scene keyframe image."""
    fpath = fls.WORK_DIR / f"lookbook_{job_id}" / f"keyframe_{scene_id}.jpg"
    if not fpath.exists():
        raise HTTPException(status_code=404, detail=f"Keyframe for scene {scene_id} not found")
    return FileResponse(fpath, media_type="image/jpeg")


# ─── MASTER FLOW: STAGE 1 (MULTI-PRODUCT IMAGE GENERATION) ───────────────────

@router.post("/stage1/generate-images")
async def stage1_generate_images(request: Request):
    """Stage 1: Ingest 1-9 products, model, background and generate Lookbook photo gallery."""
    import batch_image_studio as bis
    content_type = request.headers.get("content-type", "")
    body = await request.body()

    product_files = []
    product_urls = []
    model_file = None
    model_url = ""
    model_preset_id = "asian_elegance_24"
    background_file = None
    background_url = ""
    background_preset = "pure white studio"
    aspect_ratio = "3:4"
    quality = "4K"
    quantity = 4
    style_preset = "Luxury boutique fashion"
    lighting_preset = "soft studio lighting"
    model_ai = "google/nano-banana-pro"
    selected_poses = []

    if "boundary=" in content_type:
        boundary = content_type.split("boundary=")[1].strip().encode()
        fields, files = bis.parse_multipart_form(body, boundary)

        # Product files (up to 9)
        p_files = (
            files.get("product_files") or
            files.get("product_file") or
            files.get("products") or
            files.get("outfit_files") or
            files.get("outfit_file") or []
        )
        product_files = p_files

        # Model file
        m_files = files.get("model_file") or files.get("model_files") or []
        if m_files:
            model_file = m_files[0]

        # Background file
        bg_files = files.get("background_file") or files.get("background_files") or []
        if bg_files:
            background_file = bg_files[0]

        # URLs and text fields
        p_urls_raw = fields.get("product_urls") or fields.get("product_url") or fields.get("outfit_url") or fields.get("outfit_image_url") or ""
        if p_urls_raw:
            try:
                import json
                product_urls = json.loads(p_urls_raw) if p_urls_raw.startswith("[") else [u.strip() for u in p_urls_raw.split(",") if u.strip()]
            except Exception:
                product_urls = [u.strip() for u in p_urls_raw.split(",") if u.strip()]

        model_url = fields.get("model_url") or fields.get("model_image_url") or ""
        model_preset_id = fields.get("model_preset_id") or model_preset_id
        background_url = fields.get("background_url") or fields.get("background_image_url") or ""
        background_preset = fields.get("background_preset") or background_preset
        aspect_ratio = fields.get("aspect_ratio") or aspect_ratio
        quality = fields.get("quality") or quality
        try:
            quantity = int(fields.get("quantity") or quantity)
        except Exception:
            pass
        style_preset = fields.get("style_preset") or fields.get("style") or style_preset
        lighting_preset = fields.get("lighting_preset") or fields.get("lighting") or lighting_preset
        model_ai = fields.get("model_ai") or fields.get("model") or model_ai

        poses_raw = fields.get("selected_poses") or fields.get("poses") or ""
        if poses_raw:
            try:
                import json
                selected_poses = json.loads(poses_raw) if poses_raw.startswith("[") else [p.strip() for p in poses_raw.split(",") if p.strip()]
            except Exception:
                selected_poses = [p.strip() for p in poses_raw.split(",") if p.strip()]

        num_threads = None
        try:
            if fields.get("num_threads"):
                num_threads = int(fields.get("num_threads"))
        except Exception:
            pass
    else:
        try:
            import json
            req_json = json.loads(body.decode("utf-8")) if body else {}
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

        p_urls = req_json.get("product_urls") or []
        if isinstance(p_urls, str):
            product_urls = [u.strip() for u in p_urls.split(",") if u.strip()]
        elif isinstance(p_urls, list):
            product_urls = p_urls
        elif req_json.get("product_url") or req_json.get("outfit_url") or req_json.get("outfit_image_url"):
            product_urls = [req_json.get("product_url") or req_json.get("outfit_url") or req_json.get("outfit_image_url")]

        model_url = req_json.get("model_url") or req_json.get("model_image_url") or ""
        model_preset_id = req_json.get("model_preset_id") or model_preset_id
        background_url = req_json.get("background_url") or req_json.get("background_image_url") or ""
        background_preset = req_json.get("background_preset") or background_preset
        aspect_ratio = req_json.get("aspect_ratio") or aspect_ratio
        quality = req_json.get("quality") or quality
        try:
            quantity = int(req_json.get("quantity") or quantity)
        except Exception:
            pass
        style_preset = req_json.get("style_preset") or req_json.get("style") or style_preset
        lighting_preset = req_json.get("lighting_preset") or req_json.get("lighting") or lighting_preset
        model_ai = req_json.get("model_ai") or req_json.get("model") or model_ai
        selected_poses = req_json.get("selected_poses") or req_json.get("poses") or []
        num_threads = req_json.get("num_threads")
        if num_threads is not None:
            try:
                num_threads = int(num_threads)
            except Exception:
                num_threads = None

    if not product_files and not product_urls:
        raise HTTPException(status_code=400, detail="Vui lòng tải lên ít nhất 1 ảnh sản phẩm thời trang (product_files hoặc product_urls)!")

    try:
        job_id = fls.create_stage1_lookbook_job(
            product_files=product_files,
            product_urls=product_urls,
            model_file=model_file,
            model_url=model_url,
            model_preset_id=model_preset_id,
            background_file=background_file,
            background_url=background_url,
            background_preset=background_preset,
            aspect_ratio=aspect_ratio,
            quality=quality,
            quantity=quantity,
            style_preset=style_preset,
            lighting_preset=lighting_preset,
            model_ai=model_ai,
            selected_poses=selected_poses,
            num_threads=num_threads
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    effective_threads = num_threads or fls.LOOKBOOK_CONFIG["num_threads"]
    return {
        "ok": True,
        "job_id": job_id,
        "stage": 1,
        "status": "QUEUED",
        "message": f"Đang khởi tạo phiên làm việc Lookbook thời trang AI ({effective_threads} luồng)...",
        "num_threads": effective_threads,
        "aspect_ratio": aspect_ratio,
        "quality": quality,
        "quantity": quantity,
        "created_at": time.time()
    }


@router.get("/stage1/images/{job_id}/{image_id}")
async def get_stage1_image(job_id: str, image_id: int):
    """View individual Stage 1 lookbook image."""
    fpath = fls.WORK_DIR / f"lookbook_{job_id}" / f"image_{image_id}.jpg"
    if not fpath.exists():
        raise HTTPException(status_code=404, detail=f"Image {image_id} not found or still generating")
    return FileResponse(fpath, media_type="image/jpeg")


@router.get("/stage1/zip/{job_id}")
async def download_stage1_zip(job_id: str):
    """Download all Stage 1 lookbook gallery images as a ZIP archive."""
    fpath = fls.WORK_DIR / f"lookbook_{job_id}" / "lookbook_images.zip"
    if not fpath.exists():
        fpath = fls.create_stage1_zip(job_id)
    if not fpath or not fpath.exists():
        raise HTTPException(status_code=404, detail="ZIP archive not found or images still generating")
    return FileResponse(fpath, media_type="application/zip", filename=f"lookbook_{job_id}_gallery.zip")


# ─── MASTER FLOW: STAGE 2 (I2V MOTION VIDEO GENERATION) ──────────────────────

@router.post("/stage2/generate-videos")
async def stage2_generate_videos(payload: Stage2VideoRequest):
    """Stage 2: Generate motion videos (I2V) for selected lookbook images."""
    try:
        res = fls.create_stage2_lookbook_job(
            job_id=payload.job_id,
            selected_image_ids=payload.selected_image_ids,
            motion_preset=payload.motion_preset or "Elegant Turnaround",
            camera_movement=payload.camera_movement or "Cinematic Push-in",
            duration=payload.duration or 8,
            video_model=payload.video_model or "Omni Flash",
            is_lite_mode=payload.is_lite_mode or False,
            num_threads=payload.num_threads
        )
        effective_threads = int(payload.num_threads) if payload.num_threads else fls.LOOKBOOK_CONFIG["num_threads"]
        return {
            "ok": True,
            "job_id": payload.job_id,
            "stage": 2,
            "status": "STAGE2_RUNNING",
            "message": f"Bắt đầu kết xuất chuyển động cho {len(payload.selected_image_ids)} ảnh đã chọn ({effective_threads} luồng)...",
            "num_threads": effective_threads,
            "selected_image_ids": payload.selected_image_ids
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/stage2/clips/{job_id}/{clip_id}")
async def get_stage2_clip(job_id: str, clip_id: int):
    """Stream individual Stage 2 motion video clip."""
    fpath = fls.WORK_DIR / f"lookbook_{job_id}" / f"clip_{clip_id}.mp4"
    if not fpath.exists():
        raise HTTPException(status_code=404, detail=f"Clip {clip_id} not found or still rendering")
    return FileResponse(fpath, media_type="video/mp4")


@router.get("/stage2/clips/{job_id}/{clip_id}/download")
async def download_stage2_clip(job_id: str, clip_id: int):
    """Download individual Stage 2 motion video clip."""
    fpath = fls.WORK_DIR / f"lookbook_{job_id}" / f"clip_{clip_id}.mp4"
    if not fpath.exists():
        raise HTTPException(status_code=404, detail=f"Clip {clip_id} not found or still rendering")
    return FileResponse(fpath, media_type="video/mp4", filename=f"lookbook_{job_id}_clip_{clip_id}.mp4")


@router.post("/stitch-master")
async def stitch_master(payload: StitchMasterRequest):
    """Stitch all completed video clips into a master lookbook video with BGM."""
    res = fls.stitch_lookbook_master(payload.job_id, payload.bgm_id or "vogue_runway")
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail="Không thể ghép video master (chưa có clip hoàn thành hoặc lỗi ffmpeg)")
    return res


@router.get("/{job_id}/master")
async def get_master_video(job_id: str):
    """Stream final master lookbook video (alias for /{job_id}/final)."""
    fpath = fls.WORK_DIR / f"lookbook_{job_id}" / "final_lookbook.mp4"
    if not fpath.exists():
        raise HTTPException(status_code=404, detail="Final master lookbook video not found or still rendering")
    return FileResponse(fpath, media_type="video/mp4")

