import asyncio
import logging
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing import Any, Literal, Optional

from agent.config import USE_BATCH_RPC, FLOW_PROJECT_ID, FLOW_ALLOW_DEGRADED
from agent.services.flow_client import get_flow_client
from agent.services.watermark_remover import get_or_clean_image, prefetch_clean_image
from agent.services.omni_flash import (
    check_omni_flash_status,
    generate_omni_flash_first_frame_video,
    generate_omni_flash_first_last_video,
    generate_omni_flash_video,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/flow", tags=["flow"])


def _rewrite_image_media(result: dict, base_url: str):
    """Rewrite fifeUrl in media results to FlowKit's clean image endpoint."""
    data = result.get("data")
    if not isinstance(data, dict):
        return
    media_list = data.get("media")
    if not isinstance(media_list, list):
        return
    for item in media_list:
        if not isinstance(item, dict):
            continue
        mid = item.get("name")
        gen_img = item.get("image", {}).get("generatedImage", {})
        orig_fife = gen_img.get("fifeUrl")
        if mid and orig_fife and ("flow-content.google" in orig_fife or "storage.googleapis" in orig_fife):
            gen_img["rawFifeUrl"] = orig_fife
            gen_img["fifeUrl"] = f"{base_url}/api/flow/image/{mid}"
            asyncio.create_task(prefetch_clean_image(mid, orig_fife))


def _respond_flow_result(result: dict):
    """Return success payload or structured retryable response on UNUSUAL_ACTIVITY."""
    if not result.get("error") and (not isinstance(result.get("status"), int) or result["status"] < 400):
        return result.get("data", result)

    error_str = str(result.get("error") or result.get("data") or "")
    is_transient = (
        result.get("proxy_rotated")
        or "PUBLIC_ERROR_UNUSUAL_ACTIVITY" in error_str
        or "Frame with ID 0" in error_str
        or "Execution context was destroyed" in error_str
        or "Target closed" in error_str
        or "Session closed" in error_str
    )
    if is_transient:
        new_proxy = result.get("new_proxy") or ""
        msg = result.get("message") or (
            f"ROTATION_IN_PROGRESS: Phiên Flow đang làm mới hoặc đổi IP ({new_proxy}). "
            "Vui lòng đợi và thử lại."
        )
        return JSONResponse(
            status_code=429,
            content={
                "ok": False,
                "error": "PUBLIC_ERROR_UNUSUAL_ACTIVITY",
                "message": msg,
                "detail": msg,
                "proxy_rotated": True,
                "new_proxy": new_proxy,
                "retryable": True,
                "retry_after_s": 3,
            },
            headers={"Retry-After": "3"},
        )

    status = result.get("status", 502)
    if not isinstance(status, int) or status < 400:
        status = 502
    raise HTTPException(status, result.get("error", result.get("data")))


class GenerateImageRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    prompt: str
    project_id: Optional[str] = ""
    aspect_ratio: str = Field(default="IMAGE_ASPECT_RATIO_PORTRAIT", alias="aspectRatio")
    user_paygate_tier: str = "PAYGATE_TIER_ONE"
    character_media_ids: Optional[list[str]] = None
    reference_image_media_ids: Optional[list[str]] = Field(default=None, alias="referenceImageMediaIds")
    reference_media_ids: Optional[list[str]] = None
    image_inputs: Optional[list[Any]] = Field(default=None, alias="imageInputs")
    image_model: Optional[str] = Field(default=None, alias="modelDisplayName")

    @model_validator(mode="after")
    def populate_refs(self) -> "GenerateImageRequest":
        extracted = []
        if self.image_inputs and isinstance(self.image_inputs, list):
            for item in self.image_inputs:
                if isinstance(item, str) and item.strip():
                    extracted.append(item.strip())
                elif isinstance(item, dict):
                    mid = item.get("media_id") or item.get("mediaId") or item.get("id") or item.get("name")
                    if mid and isinstance(mid, str):
                        extracted.append(mid.strip())

        if not self.character_media_ids:
            if extracted:
                self.character_media_ids = extracted
            elif self.reference_image_media_ids:
                self.character_media_ids = self.reference_image_media_ids
            elif self.reference_media_ids:
                self.character_media_ids = self.reference_media_ids
        elif extracted:
            # Combine if character_media_ids is already present but image_inputs has extra
            for m in extracted:
                if m not in self.character_media_ids:
                    self.character_media_ids.append(m)
        return self



class GenerateVideoRequest(BaseModel):
    prompt: str
    project_id: Optional[str] = ""
    scene_id: Optional[str] = ""
    # Absent → text-to-video (rpcid YhhmEf). Present → frame-to-video (eb1hJf).
    start_image_media_id: Optional[str] = None
    aspect_ratio: str = "VIDEO_ASPECT_RATIO_PORTRAIT"
    end_image_media_id: Optional[str] = None
    user_paygate_tier: str = "PAYGATE_TIER_ONE"
    # Backward compatible: legacy requests remain Veo unless explicitly set.
    model_family: Literal["veo", "omni_flash"] = "veo"
    duration_s: int = 8


class GenerateVideoRefsRequest(BaseModel):
    reference_media_ids: list[str]
    prompt: str
    project_id: str
    scene_id: str
    aspect_ratio: str = "VIDEO_ASPECT_RATIO_PORTRAIT"
    user_paygate_tier: str = "PAYGATE_TIER_ONE"
    # Backward compatible: existing callers keep the Veo R2V path unless they
    # explicitly opt into Omni Flash.
    model_family: Literal["veo", "omni_flash"] = "veo"
    duration_s: int = 8


class GenerateOmniFlashVideoRequest(BaseModel):
    reference_media_ids: list[str]
    prompt: str
    project_id: str
    scene_id: str = ""
    duration_s: int = 8
    aspect_ratio: str = "VIDEO_ASPECT_RATIO_PORTRAIT"
    user_paygate_tier: str = "PAYGATE_TIER_ONE"


class UpscaleVideoRequest(BaseModel):
    media_id: str
    scene_id: str
    aspect_ratio: str = "VIDEO_ASPECT_RATIO_PORTRAIT"
    resolution: str = "VIDEO_RESOLUTION_4K"


class UploadImageRequest(BaseModel):
    # Local disk on the Flow Kit host (legacy). Nova must send image_base64.
    file_path: str = ""
    image_base64: str = ""
    mime_type: str = ""
    project_id: str = ""
    file_name: str = "image.png"


class CheckStatusRequest(BaseModel):
    operations: list[dict] = []
    # Omni/workflow-mode callers should pass workflow descriptors instead of
    # operation handles. If workflows is set, /check-status automatically uses
    # authenticated Flow project polling.
    workflows: Optional[list[dict]] = None
    project_id: str = ""
    include_encoded_video: bool = False


class CheckOmniStatusRequest(BaseModel):
    workflows: list[dict]
    project_id: str = ""
    include_encoded_video: bool = False


class EditImageRequest(BaseModel):
    prompt: str
    source_media_id: str
    project_id: str
    aspect_ratio: str = "IMAGE_ASPECT_RATIO_PORTRAIT"
    user_paygate_tier: str = "PAYGATE_TIER_ONE"


class VisionAnalyzeRequest(BaseModel):
    prompt: str
    images: Optional[list[str]] = None
    image_base64: Optional[str] = None
    system_instruction: Optional[str] = ""
    session_uuid: Optional[str] = None
    project_id: Optional[str] = ""
    timeout: Optional[float] = 120.0


@router.get("/status")
async def extension_status():
    """Extension health, and which transport it is being asked to speak.

    `flow_key_present` is a legacy-path signal: the batchexecute path has no
    bearer token at all, so false is expected there rather than a fault.
    """
    client = get_flow_client()
    return {
        "connected": client.connected,
        "transport": "batch" if USE_BATCH_RPC else "legacy_rest",
        "flow_project_id": FLOW_PROJECT_ID or None,
        "allow_degraded": FLOW_ALLOW_DEGRADED,
        "flow_key_present": client._flow_key is not None,
        "workers": client.workers(),
    }


@router.get("/credits")
async def get_credits():
    """Get user credits from Google Flow."""
    client = get_flow_client()
    if not client.connected:
        raise HTTPException(503, "Extension not connected")
    result = await client.get_credits()
    if result.get("error"):
        raise HTTPException(502, result["error"])
    return result.get("data", result)


@router.post("/generate-image")
async def generate_image(body: GenerateImageRequest, request: Request):
    """Generate image directly (bypasses queue). Watermark cleaned automatically."""
    client = get_flow_client()
    if not client.connected:
        raise HTTPException(503, "Extension not connected")
    result = await client.generate_images(
        prompt=body.prompt,
        project_id=body.project_id or "",
        aspect_ratio=body.aspect_ratio,
        user_paygate_tier=body.user_paygate_tier,
        character_media_ids=body.character_media_ids,
        image_model=body.image_model,
    )
    if result.get("status") == 200:
        base_url = str(request.base_url).rstrip("/")
        _rewrite_image_media(result, base_url)
    return _respond_flow_result(result)


@router.post("/generate-video")
async def generate_video(body: GenerateVideoRequest):
    """Submit Veo video generation: t2v, i2v, or (unported) start+end.

    Omit ``start_image_media_id`` for text-to-video (``YhhmEf``). Pass it for
    frame-to-video (``eb1hJf``). Existing callers default to Veo. For Omni set
    ``model_family=omni_flash`` and ``duration_s`` to 4/6/8/10 — Omni still
    needs a start image.

    Omni responses include ``flowkitPolling.workflows`` and must use workflow
    media polling rather than legacy operation polling.
    """
    client = get_flow_client()
    if not client.connected:
        raise HTTPException(503, "Extension not connected")

    if body.model_family == "omni_flash":
        try:
            common = dict(
                start_image_media_id=body.start_image_media_id,
                prompt=body.prompt,
                project_id=body.project_id,
                scene_id=body.scene_id,
                duration_s=body.duration_s,
                aspect_ratio=body.aspect_ratio,
                user_paygate_tier=body.user_paygate_tier,
            )
            if body.end_image_media_id:
                result = await generate_omni_flash_first_last_video(
                    end_image_media_id=body.end_image_media_id,
                    **common,
                )
            else:
                result = await generate_omni_flash_first_frame_video(**common)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
    else:
        result = await client.generate_video(
            start_image_media_id=body.start_image_media_id,
            prompt=body.prompt,
            project_id=body.project_id,
            scene_id=body.scene_id,
            aspect_ratio=body.aspect_ratio,
            end_image_media_id=body.end_image_media_id,
            user_paygate_tier=body.user_paygate_tier,
        )

    return _respond_flow_result(result)


@router.post("/generate-video-refs")
async def generate_video_refs(body: GenerateVideoRefsRequest):
    """Submit reference-to-video generation using Veo or Gemini Omni Flash.

    Existing requests default to ``model_family=veo``. Set
    ``model_family=omni_flash`` and ``duration_s`` to 4/6/8/10 to use Omni.
    Omni responses include ``flowkitPolling.workflows``; poll those workflows,
    not the operation-looking handles in the raw Flow response.
    """
    client = get_flow_client()
    if not client.connected:
        raise HTTPException(503, "Extension not connected")

    if body.model_family == "omni_flash":
        try:
            result = await generate_omni_flash_video(
                reference_media_ids=body.reference_media_ids,
                prompt=body.prompt,
                project_id=body.project_id,
                scene_id=body.scene_id,
                duration_s=body.duration_s,
                aspect_ratio=body.aspect_ratio,
                user_paygate_tier=body.user_paygate_tier,
            )
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
    else:
        result = await client.generate_video_from_references(
            **body.model_dump(exclude={"model_family", "duration_s"})
        )

    return _respond_flow_result(result)


@router.post("/generate-video-omni")
async def generate_video_omni(body: GenerateOmniFlashVideoRequest):
    """Submit Gemini Omni Flash reference-to-video generation.

    The response includes ``flowkitPolling.workflows`` for the correct
    workflow/media polling path.
    """
    client = get_flow_client()
    if not client.connected:
        raise HTTPException(503, "Extension not connected")
    try:
        result = await generate_omni_flash_video(**body.model_dump())
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return _respond_flow_result(result)


@router.post("/upscale-video")
async def upscale_video(body: UpscaleVideoRequest):
    """Submit video upscale (returns operations for polling)."""
    client = get_flow_client()
    if not client.connected:
        raise HTTPException(503, "Extension not connected")
    result = await client.upscale_video(**body.model_dump())
    return _respond_flow_result(result)


@router.post("/check-status")
async def check_status(body: CheckStatusRequest):
    """Check Veo operation status or Omni workflow/media status.

    Veo: pass ``operations``.
    Omni Flash: pass ``workflows`` from submit ``flowkitPolling.workflows``.
    """
    client = get_flow_client()
    if not client.connected:
        raise HTTPException(503, "Extension not connected")

    if body.workflows:
        try:
            return await check_omni_flash_status(
                body.workflows,
                include_encoded_video=body.include_encoded_video,
                project_id=body.project_id,
            )
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(502, str(exc)) from exc

    if not body.operations:
        raise HTTPException(400, "Provide operations for Veo or workflows for Omni Flash")

    result = await client.check_video_status(body.operations)
    if result.get("error"):
        raise HTTPException(502, result["error"])
    if isinstance(result.get("status"), int) and result["status"] >= 400:
        raise HTTPException(result["status"], result.get("data", "Flow polling failed"))
    return result.get("data", result)


@router.post("/check-omni-status")
async def check_omni_status(body: CheckOmniStatusRequest):
    """Poll Gemini Omni Flash jobs via workflow primary media IDs."""
    client = get_flow_client()
    if not client.connected:
        raise HTTPException(503, "Extension not connected")
    try:
        return await check_omni_flash_status(
            body.workflows,
            include_encoded_video=body.include_encoded_video,
            project_id=body.project_id,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(502, str(exc)) from exc


@router.post("/vision-analyze")
async def vision_analyze(body: VisionAnalyzeRequest):
    """Analyze images and prompt using Google Flow's internal agJzFb (Gemini 3 Flash)."""
    import base64
    from pathlib import Path

    client = get_flow_client()
    if not client.connected:
        raise HTTPException(503, "Extension not connected")

    raw_images = list(body.images or [])
    if body.image_base64 and body.image_base64 not in raw_images:
        raw_images.append(body.image_base64)

    # Process file paths if any are passed
    processed_images: list[str] = []
    for item in raw_images:
        item_str = str(item).strip()
        p = Path(item_str)
        if (item_str.startswith("/") or item_str.startswith(".")) and p.is_file():
            try:
                mime = "image/jpeg"
                if p.suffix.lower() == ".png":
                    mime = "image/png"
                elif p.suffix.lower() == ".webp":
                    mime = "image/webp"
                encoded = base64.b64encode(p.read_bytes()).decode("ascii")
                processed_images.append(f"data:{mime};base64,{encoded}")
                continue
            except Exception as e:
                logger.warning("Could not read local image %s: %s", item_str, e)
        processed_images.append(item_str)

    result = await client.vision_analyze(
        prompt=body.prompt,
        images=processed_images,
        system_instruction=body.system_instruction or "",
        session_uuid=body.session_uuid,
        project_id=body.project_id or "",
        timeout=body.timeout or 120.0,
    )
    return _respond_flow_result(result)


@router.post("/refresh-urls/{project_id}")
async def refresh_project_urls(project_id: str):
    """Bulk refresh all media URLs for a project via per-media get_media calls."""
    client = get_flow_client()
    if not client.connected:
        raise HTTPException(503, "Extension not connected")
    result = await client.refresh_project_urls(project_id)
    if result.get("error"):
        raise HTTPException(502, result["error"])
    return result


@router.api_route("/image/{media_id}", methods=["GET", "HEAD"])
async def get_image(media_id: str, raw: bool = False):
    """Serve image for media_id, automatically cleaned of Google AI watermark."""
    clean_id = media_id.rsplit(".", 1)[0] if "." in media_id else media_id
    try:
        client = get_flow_client()
        path = await get_or_clean_image(clean_id, raw=raw, flow_client=client)
        return FileResponse(
            path,
            media_type="image/jpeg",
            headers={"Cache-Control": "public, max-age=86400"},
        )
    except Exception as e:
        logger.exception("Failed to retrieve or clean image %s: %s", media_id, e)
        raise HTTPException(502, f"Failed to retrieve or clean image: {e}")


@router.get("/media/{media_id}")
async def get_media(media_id: str, request: Request):
    """Get media metadata + fresh signed URL from Google Flow.

    Returns the raw response which may contain ``video.encodedVideo`` for
    workflow-backed video generations. Images have their fifeUrl pointed to
    the cleaned endpoint, with rawFifeUrl preserving Google's CDN URL.
    """
    client = get_flow_client()
    if not client.connected:
        raise HTTPException(503, "Extension not connected")
    result = await client.get_media(media_id)
    status = result.get("status", 200)
    if status == 404:
        raise HTTPException(404, result.get("error", f"Media {media_id} not found or still generating"))
    if result.get("error"):
        raise HTTPException(502, result["error"])
    if isinstance(status, int) and status >= 400:
        raise HTTPException(status, result.get("data", "Media not found"))
    data = result.get("data", result)
    if isinstance(data, dict) and "image" in data and isinstance(data["image"], dict):
        img_info = data["image"]
        orig_fife = img_info.get("fifeUrl")
        if orig_fife and ("flow-content.google" in orig_fife or "storage.googleapis" in orig_fife):
            base_url = str(request.base_url).rstrip("/")
            img_info["rawFifeUrl"] = orig_fife
            img_info["fifeUrl"] = f"{base_url}/api/flow/image/{media_id}"
            asyncio.create_task(prefetch_clean_image(media_id, orig_fife))
    return data


@router.post("/edit-image")
async def edit_image(body: EditImageRequest, request: Request):
    """Edit an existing image using IMAGE_INPUT_TYPE_BASE_IMAGE (bypasses queue)."""
    client = get_flow_client()
    if not client.connected:
        raise HTTPException(503, "Extension not connected")
    result = await client.edit_image(
        body.prompt, body.source_media_id, body.project_id,
        aspect_ratio=body.aspect_ratio,
        user_paygate_tier=body.user_paygate_tier,
    )
    if result.get("status") == 200:
        base_url = str(request.base_url).rstrip("/")
        _rewrite_image_media(result, base_url)
    return _respond_flow_result(result)


def decode_upload_image(body: UploadImageRequest) -> tuple[str, str, str]:
    """Return ``(base64, mime, file_name)``. Nova sends bytes; local tools may send a path."""
    import base64, mimetypes

    raw = (body.image_base64 or "").strip()
    if raw:
        mime = (body.mime_type or "").strip()
        name = body.file_name or "image.png"
        if raw.startswith("data:") and "," in raw:
            header, raw = raw.split(",", 1)
            raw = "".join(raw.split())
            if not mime and header.startswith("data:") and ";base64" in header:
                mime = header[5:].split(";", 1)[0].strip()
        else:
            raw = "".join(raw.split())
        mime = mime or "image/jpeg"
        if name == "image.png" and mime in ("image/jpeg", "image/jpg"):
            name = "image.jpg"
        if not raw:
            raise ValueError("image_base64 is empty")
        return raw, mime, name

    path = (body.file_path or "").strip()
    if not path:
        raise ValueError("Provide image_base64 or file_path")
    try:
        with open(path, "rb") as f:
            image_bytes = f.read()
    except FileNotFoundError as exc:
        raise FileNotFoundError(path) from exc
    b64 = base64.b64encode(image_bytes).decode()
    mime = mimetypes.guess_type(path)[0] or "image/png"
    return b64, mime, body.file_name or "image.png"


@router.post("/upload-image")
async def upload_image(body: UploadImageRequest):
    """Upload an image to Google Flow and get a media_id UUID.

    Nova (and any remote caller) must send ``image_base64`` — optionally a
    ``data:image/...;base64,...`` URL. ``file_path`` only works on this host.
    """
    client = get_flow_client()
    if not client.connected:
        raise HTTPException(503, "Extension not connected")
    try:
        b64, mime, file_name = decode_upload_image(body)
    except FileNotFoundError as exc:
        raise HTTPException(404, f"File not found: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    result = await client.upload_image(
        b64, mime_type=mime, project_id=body.project_id, file_name=file_name,
    )
    if result.get("error") or (isinstance(result.get("status"), int) and result["status"] >= 400):
        return _respond_flow_result(result)
    media_id = result.get("_mediaId")
    return {"media_id": media_id, "raw": result.get("data", result)}


# ─── Unusual Activity Forensic Diagnostics Endpoints ──────────

@router.get("/diagnostics/unusual-audit")
async def get_unusual_audit_logs(limit: int = 50):
    """Retrieve audit history, frequency metrics, and heuristic root cause diagnoses."""
    from agent.services.unusual_audit import get_unusual_audit
    audit_mgr = get_unusual_audit()
    return {
        "ok": True,
        "summary": audit_mgr.get_audit_summary(),
        "recent_events": audit_mgr.get_recent_events(limit=limit),
    }


@router.post("/diagnostics/unusual-audit/clear")
async def clear_unusual_audit_logs():
    """Clear in-memory audit ring buffer."""
    from agent.services.unusual_audit import get_unusual_audit
    audit_mgr = get_unusual_audit()
    audit_mgr.clear_in_memory_records()
    return {"ok": True, "message": "In-memory audit records cleared"}


@router.get("/diagnostics/proxy-health")
async def get_proxy_health_stats():
    """Get real-time health stats of active and pooled proxies."""
    from agent.services.unusual_audit import get_unusual_audit
    audit_mgr = get_unusual_audit()
    return {
        "ok": True,
        "health": audit_mgr.get_audit_summary().get("proxy_pool_health", []),
    }


@router.get("/diagnostics/test-recaptcha")
async def test_recaptcha_endpoint(worker_id: str = "nick-a", action: str = "FLOW_PROBE"):
    """Test minting 1 live reCAPTCHA Enterprise token through Chrome extension on worker_id."""
    from agent.services.flow_client import get_flow_client
    client = get_flow_client()
    try:
        res = await client.test_recaptcha_token(worker_id=worker_id, action=action)
        return res
    except Exception as e:
        return {"ok": False, "worker_id": worker_id, "action": action, "error": str(e)}


@router.get("/diagnostics/unusual-threshold")
async def get_unusual_threshold():
    """Get forensic analysis of requests per IP before unusual activity occurs."""
    from agent.services.unusual_audit import get_unusual_audit
    audit_mgr = get_unusual_audit()
    return audit_mgr.compute_threshold_analysis()


