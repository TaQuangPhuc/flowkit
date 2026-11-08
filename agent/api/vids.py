"""Google Vids API (docs.google.com/videos genai) — replay trực tiếp,
không qua Flow gRPC. Cần nicks/<nick>/vids_cookies.json + vids_doc.txt
(chromemgr cookie-sync dump)."""
import base64
import logging
import re
from fastapi import APIRouter, HTTPException, Request, UploadFile, File, Form
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, model_validator
from typing import Literal, Optional

from agent.services import vids_client

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/vids", tags=["vids"])

_PROMPT_10S_RE = re.compile(
    r'(?:"duration(?:_sec(?:onds)?)?"\s*:\s*10\b|\b10\s*(?:s|sec|seconds|giây)\b)',
    re.IGNORECASE,
)


def _coerce_seconds(val) -> Optional[int]:
    if val is None or isinstance(val, bool):
        return None
    if isinstance(val, (int, float)):
        n = int(round(val))
        return n if n > 0 else None
    if isinstance(val, str):
        s = val.strip().lower().rstrip("s").strip()
        if s.isdigit():
            n = int(s)
            return n if n > 0 else None
    return None


class VidsGenerateReq(BaseModel):
    nick: str
    prompt: str = ""                       # upscale không cần prompt
    mode: Literal["t2v", "i2v", "r2v", "edit", "extend", "upscale"] = "t2v"
    images: Optional[list[str]] = None       # AVL tokens đã upload
    image_b64: Optional[list[str]] = None    # hoặc bytes base64 → auto upload
    temp_data_id: Optional[str] = None       # video nguồn (clip đã gen) cho edit/extend
    video_avl: Optional[str] = None          # video nguồn upload ngoài (edit)
    video_b64: Optional[str] = None          # hoặc bytes base64 → auto upload
    image_name: Optional[str] = None         # label mention ảnh trong prompt edit
    aspect: Literal["landscape", "portrait", "square"] = "landscape"
    seconds: Optional[int] = None            # output duration; None → auto theo video nguồn
    delogo: bool = True                      # xóa watermark ✦ góc (alpha un-blend)
    resolution: int = 720                    # 1080 → gen 1080p trực tiếp (rẻ hơn upscale sau)
    model: Optional[str] = None              # tùy chọn chọn family (macaron_cookie / abra)

    @model_validator(mode="before")
    @classmethod
    def _normalize_duration_fields(cls, data):
        if not isinstance(data, dict):
            return data
        sec = _coerce_seconds(data.get("seconds"))
        if sec is None:
            for alias in ("duration", "duration_s", "durationSeconds", "duration_seconds"):
                sec = _coerce_seconds(data.get(alias))
                if sec is not None:
                    break
        prompt = data.get("prompt") if isinstance(data.get("prompt"), str) else ""
        if sec in (None, 8) and prompt and _PROMPT_10S_RE.search(prompt):
            sec = 10
        if sec is not None:
            data = dict(data)
            data["seconds"] = sec
        return data


def _probe_seconds(data: bytes) -> tuple[int, int] | None:
    """ffprobe duration video bytes → (seconds_ceil, microseconds)."""
    import math, subprocess, tempfile, os
    fd, path = tempfile.mkstemp(suffix=".mp4")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        r = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_format", path],
            capture_output=True, text=True, timeout=30)
        d = float(__import__("json").loads(r.stdout)["format"]["duration"])
        return math.ceil(d), int(d * 1_000_000)
    except Exception:
        return None
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


@router.post("/generate")
def vids_generate(req: VidsGenerateReq, request: Request):
    try:
        can_switch = not req.images and not req.video_avl
        can_flow_abra = (
            req.mode in ("t2v", "i2v", "r2v")
            and not req.video_b64
            and not req.video_avl
            and not req.temp_data_id
            and not req.images
        )
        nick = req.nick
        if vids_client.is_nick_dead(nick):
            candidates = vids_client.live_nicks(exclude={nick})
            if candidates:
                nick = candidates[0]

        raw_imgs = [base64.b64decode(b64) for b64 in (req.image_b64 or [])]
        vb = base64.b64decode(req.video_b64) if req.video_b64 else None
        seconds, source_us = req.seconds or 0, None
        if vb is not None:
            probe = _probe_seconds(vb)
            if probe:
                probed_s, source_us = probe
                if not seconds:
                    seconds = probed_s

        force_flow = bool(req.model and any(
            k in req.model.lower() for k in ("macaron", "omni_1_2", "omni 1.2", "abra", "360p")
        ))
        if (not vids_client.live_nicks()) or (force_flow and can_flow_abra):
            if can_flow_abra:
                out = vids_client.generate_flow_abra(
                    nick, req.prompt, mode=req.mode, raw_imgs=raw_imgs,
                    aspect=req.aspect, duration=seconds, resolution=req.resolution,
                    model=req.model or "")
            else:
                return JSONResponse(status_code=429, content={
                    "ok": False, "error_code": "user_quota_reached",
                    "error": f"Google Vids 10k quota exhausted on all nicks (mode={req.mode} requires Vids video upload)",
                    "retryable": False})
        else:
            tried = set()
            while True:
                tried.add(nick)
                try:
                    images = list(req.images or [])
                    video_avl = req.video_avl
                    if raw_imgs and vb is not None:
                        from concurrent.futures import ThreadPoolExecutor
                        with ThreadPoolExecutor(max_workers=min(len(raw_imgs) + 1, 4)) as ex:
                            img_futs = [ex.submit(vids_client.upload_bytes, nick, b) for b in raw_imgs]
                            vid_fut = ex.submit(vids_client.upload_bytes, nick, vb)
                            for f in img_futs:
                                images.append(f.result())
                            video_avl = vid_fut.result()
                    else:
                        for img_bytes in raw_imgs:
                            images.append(vids_client.upload_bytes(nick, img_bytes))
                        if vb is not None:
                            video_avl = vids_client.upload_bytes(nick, vb)
                    out = vids_client.generate(
                        nick, req.prompt, mode=req.mode,
                        images=images or None, temp_data_id=req.temp_data_id,
                        aspect=req.aspect, video_avl=video_avl,
                        image_name=req.image_name or "Image1",
                        duration=seconds, source_us=source_us,
                        delogo=req.delogo, resolution=req.resolution)
                    break
                except vids_client.VidsQuotaError as qe:
                    if can_switch:
                        nxt = vids_client.live_nicks(exclude=tried)
                        if nxt:
                            nick = nxt[0]
                            continue
                    if can_flow_abra:
                        logger.info("All Vids nicks quota-exhausted; falling back to Flow Omni for mode=%s", req.mode)
                        out = vids_client.generate_flow_abra(
                            nick, req.prompt, mode=req.mode, raw_imgs=raw_imgs,
                            aspect=req.aspect, duration=seconds, resolution=req.resolution,
                            model=req.model or "")
                        break
                    return JSONResponse(status_code=429, content={
                        "ok": False, "error_code": "user_quota_reached",
                        "error": str(qe)[:300], "retryable": False})

        # video_url google cần auth → re-host file đã tải về; giữ link gốc
        # trong raw_video_url để debug.
        if out.get("video_file"):
            out["raw_video_url"] = out.get("video_url")
            base = str(request.base_url).rstrip("/")
            out["video_url"] = f"{base}/api/vids/file/{out['video_file']}"
        elif not out.get("video_url"):
            raise HTTPException(502, "upstream không trả video_url")
        return out
    except vids_client.VidsPolicyError as e:
        logger.warning("vids_generate policy refused (nick=%s, mode=%s): %s", req.nick, req.mode, e)
        return JSONResponse(status_code=400, content={
            "ok": False, "error_code": "content_policy_violation",
            "error": str(e)[:300], "retryable": False})
    except vids_client.VidsError as e:
        logger.warning("vids_generate error (nick=%s, mode=%s): %s", req.nick, req.mode, e)
        raise HTTPException(502, str(e))
    except vids_client.requests.exceptions.RequestException as e:
        raise HTTPException(502, f"vids upstream network error: {str(e)[:200]}")
    except FileNotFoundError as e:
        raise HTTPException(404, f"{req.nick}: thiếu file nick ({e.filename})")


@router.get("/file/{name}")
def vids_file(name: str):
    try:
        return FileResponse(vids_client.video_path(name), media_type="video/mp4")
    except vids_client.VidsError as e:
        raise HTTPException(404, str(e))


@router.post("/upload")
async def vids_upload(nick: str = Form(...), file: UploadFile = File(...)):
    try:
        if vids_client.is_nick_dead(nick):
            candidates = vids_client.live_nicks(exclude={nick})
            if candidates:
                nick = candidates[0]
        data = await file.read()
        return {"avl": vids_client.upload_bytes(nick, data)}
    except vids_client.VidsError as e:
        raise HTTPException(502, str(e))
    except vids_client.requests.exceptions.RequestException as e:
        raise HTTPException(502, f"vids upload network error: {str(e)[:200]}")
    except FileNotFoundError as e:
        raise HTTPException(404, f"{nick}: thiếu file nick ({e.filename})")


@router.get("/nicks")
def vids_nicks():
    """Nicks nào đã có đủ vids artifacts."""
    out = []
    d = vids_client.NICKS_DIR
    if d.exists():
        for n in sorted(d.iterdir()):
            if not n.is_dir() or n.name.startswith("_"):
                continue
            if (n / "vids_cookies.json").exists() and (n / "vids_doc.txt").exists():
                q = vids_client.load_vids_quota(n.name)
                has_flow_abra = n.name in vids_client.FLOW_ABRA_NICKS
                session_dead = (
                    (n / "_vids_dead").exists()
                    if has_flow_abra
                    else vids_client.is_nick_dead(n.name)
                )
                out.append({"nick": n.name,
                            "doc": (n / "vids_doc.txt").read_text().strip(),
                            "session_dead": session_dead,
                            "vids_quota_dead": vids_client.is_nick_dead(n.name),
                            "flow_abra": has_flow_abra,
                            "credits_total": q.get("credits_total"),
                            "credits_left": q.get("credits_left"),
                            "credits_used": q.get("credits_used"),
                            "quota_updated_at": q.get("updated_at")})
    return {"nicks": out}
