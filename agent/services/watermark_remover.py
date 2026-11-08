"""Service to detect and cleanly remove Google AI watermark (sparkle star)
from Imagen 3 / Gemini / Google Flow images using calibrated reverse alpha
blending and boundary-preserving filtering.
"""

import asyncio
from io import BytesIO
import logging
import os
import re
import time
from pathlib import Path
from typing import Optional, Union
import aiohttp
import numpy as np
from PIL import Image, ImageFilter

logger = logging.getLogger(__name__)

ASSETS_DIR = Path(__file__).resolve().parent.parent / "assets" / "watermark"
MEDIA_CACHE_DIR = Path.home() / ".flowkit" / "media_cache"
MEDIA_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_TEMPLATES = {}
_HP_TEMPLATES = {}
_IN_FLIGHT_FETCHES: dict[str, asyncio.Task] = {}


def _load_templates(size: int):
    if size not in _TEMPLATES:
        path = ASSETS_DIR / f"bg_{size}.png"
        if not path.exists():
            raise FileNotFoundError(f"Watermark mask template not found: {path}")
        im_l = Image.open(path).convert("L")
        arr = np.array(im_l, dtype=np.float32) / 255.0
        norm = arr / arr.max()
        _TEMPLATES[size] = norm

        # High-pass template (radius=8 Gaussian blur subtraction to eliminate background gradient)
        t_blur = im_l.filter(ImageFilter.GaussianBlur(radius=8))
        hp = np.array(im_l, dtype=np.float32) - np.array(t_blur, dtype=np.float32)
        hp_z = hp - hp.mean()
        hp_norm = float(np.linalg.norm(hp_z))
        _HP_TEMPLATES[size] = (hp_z, hp_norm)
    return _TEMPLATES[size], _HP_TEMPLATES[size]


def _load_template(size: int) -> np.ndarray:
    tmpl, _ = _load_templates(size)
    return tmpl


def remove_watermark_from_image(
    im: Image.Image,
    peak_alpha: float = 0.32,
    min_correlation: float = 0.35,
) -> Image.Image:
    """Remove Google's semi-transparent sparkle watermark from a PIL Image.

    Uses high-pass filtered cross-correlation in the bottom-right corner to isolate
    the watermark from complex/high-contrast image backgrounds, then reverses the
    alpha composite:
        recovered = (watermarked - alpha * 255) / (1 - alpha)
    Followed by boundary smoothing to remove JPEG edge ringing artifacts.
    """
    orig_mode = im.mode
    img = im.convert("RGB")
    w, h = img.size

    candidates_config = [(48, range(110, 140))]
    if min(w, h) > 1400:
        candidates_config.append((96, range(220, 280)))

    best_match = None
    best_c = -1.0

    for size, delta_range in candidates_config:
        try:
            norm_template, (hp_tz, hp_tnorm) = _load_templates(size)
        except Exception as e:
            logger.warning("Could not load template for size %d: %s", size, e)
            continue

        corner_pad = delta_range.stop + 30
        corner_w = min(corner_pad, w)
        corner_h = min(corner_pad, h)
        corner_pil = img.crop((w - corner_w, h - corner_h, w, h)).convert("L")
        blurred_corner = corner_pil.filter(ImageFilter.GaussianBlur(radius=8))
        hp_corner = np.array(corner_pil, dtype=np.float32) - np.array(blurred_corner, dtype=np.float32)

        cur_best_c = -1.0
        cur_best_xy = None

        for delta in delta_range:
            for ox in range(-4, 5):
                for oy in range(-4, 5):
                    lx = corner_w - delta + ox
                    ly = corner_h - delta + oy
                    if lx < 0 or ly < 0 or lx + size > corner_w or ly + size > corner_h:
                        continue
                    patch = hp_corner[ly : ly + size, lx : lx + size]
                    pz = patch - patch.mean()
                    pnorm = np.linalg.norm(pz)
                    if pnorm > 1e-2:
                        c = float(np.sum(pz * hp_tz) / (pnorm * hp_tnorm))
                        if c > cur_best_c:
                            cur_best_c = c
                            cur_best_xy = (w - corner_w + lx, h - corner_h + ly)

        if cur_best_xy and cur_best_c > best_c:
            best_c = cur_best_c
            best_match = (size, cur_best_xy, norm_template)

    if best_match is None or best_c < min_correlation:
        logger.debug("No watermark detected in corner (best corr: %.3f)", best_c)
        return im

    logo_size, (x, y), norm_template = best_match
    logger.info(
        "Detected watermark at (%d, %d) with high-pass correlation %.3f (size %dx%d)",
        x, y, best_c, logo_size, logo_size
    )

    img_arr = np.array(img, dtype=np.float32)

    # Reverse alpha blending
    patch = img_arr[y : y + logo_size, x : x + logo_size].copy()
    alpha = np.expand_dims(norm_template * peak_alpha, axis=2)
    one_minus_alpha = np.maximum(1.0 - alpha, 0.01)
    recovered = np.clip((patch - alpha * 255.0) / one_minus_alpha, 0, 255)

    apply_mask = norm_template > 0.01
    patch[apply_mask, :] = recovered[apply_mask, :]
    img_arr[y : y + logo_size, x : x + logo_size] = patch

    # Boundary smoothing for JPEG edge ringing
    pil_clean = Image.fromarray(img_arr.astype(np.uint8))
    pad = 4
    x0, y0 = max(0, x - pad), max(0, y - pad)
    x1, y1 = min(w, x + logo_size + pad), min(h, y + logo_size + pad)
    blurred_patch = pil_clean.crop((x0, y0, x1, y1)).filter(ImageFilter.MedianFilter(size=3))

    m_pil = Image.fromarray((norm_template > 0.05).astype(np.uint8) * 255)
    dilated = np.array(m_pil.filter(ImageFilter.MaxFilter(size=3))) > 128
    eroded = np.array(m_pil.filter(ImageFilter.MinFilter(size=3))) > 128
    boundary = dilated & ~eroded

    b_arr = np.array(blurred_patch, dtype=np.float32)
    sub_b = b_arr[y - y0 : y - y0 + logo_size, x - x0 : x - x0 + logo_size]
    img_arr[y : y + logo_size, x : x + logo_size][boundary, :] = sub_b[boundary, :]

    result = Image.fromarray(img_arr.astype(np.uint8))
    if orig_mode != "RGB":
        result = result.convert(orig_mode)
    return result


def remove_watermark_from_bytes(data: bytes, format: str = "JPEG", quality: int = 95) -> bytes:
    """Remove watermark from raw image bytes and return cleaned image bytes."""
    with Image.open(BytesIO(data)) as im:
        fmt = format or im.format or "JPEG"
        cleaned = remove_watermark_from_image(im)
        buf = BytesIO()
        if fmt.upper() in ("JPG", "JPEG"):
            cleaned.save(buf, format="JPEG", quality=quality)
        else:
            cleaned.save(buf, format=fmt)
        return buf.getvalue()


def remove_watermark_from_file(src: Union[str, Path], dest: Union[str, Path] = None) -> Path:
    """Read an image file, clean the watermark, and save to destination path."""
    src_path = Path(src)
    dest_path = Path(dest) if dest else src_path
    with Image.open(src_path) as im:
        fmt = im.format or "JPEG"
        cleaned = remove_watermark_from_image(im)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        cleaned.save(dest_path, format=fmt, quality=95)
    return dest_path


# A nick relay that accepts the connection and then never writes a body
# used to die at 20s and become HTTP 502 on GET /api/flow/image/{id}.
# One slow full-size fetch is enough to fail a TVC keyframe.
_RELAY_URL = re.compile(r"^http://127\.0\.0\.1:\d+/to/([^/]+)(/.*)$")
_RELAY_DOWNLOAD_TIMEOUT_S = 45
_DIRECT_DOWNLOAD_TIMEOUT_S = 30
_DIRECT_DOWNLOAD_ATTEMPTS = 2


def _unwrap_relay_url(url: str) -> str:
    """Turn http://127.0.0.1:519xx/to/<host><path> back into https://<host><path>."""
    m = _RELAY_URL.match(url or "")
    if not m:
        return ""
    return f"https://{m.group(1)}{m.group(2)}"


def _download_retryable(exc: BaseException) -> bool:
    code = getattr(exc, "code", None)
    if code in (429, 500, 502, 503, 504):
        return True
    name = type(exc).__name__
    if name in {
        "ReadTimeout", "ConnectTimeout", "Timeout",
        "ConnectionError", "ChunkedEncodingError", "ProxyError",
    }:
        return True
    msg = str(exc).lower()
    if "timed out" in msg or "timeout" in msg or "connection reset" in msg or "connection aborted" in msg:
        return True
    if "failed to download image from cdn: http " in msg:
        tail = msg.rsplit(" ", 1)[-1]
        return tail in {"429", "500", "502", "503", "504"}
    return False


def _image_download_order(urls: list[str]) -> list[str]:
    """Relay first, then the signed Google URL, then the relay once more.

    A stuck local relay (read timeout on 127.0.0.1:519xx) must not be the
    only attempt. The signed URL is the same object the relay was fetching.
    """
    relay: list[str] = []
    direct: list[str] = []
    for url in urls:
        if not url:
            continue
        plain = _unwrap_relay_url(url)
        if plain:
            if url not in relay:
                relay.append(url)
            if plain not in direct:
                direct.append(plain)
        elif url not in direct and url not in relay:
            direct.append(url)
    return relay + direct + list(relay)


def _fetch_image_bytes(get, url: str, headers: dict | None, timeout: float, attempts: int) -> bytes:
    last: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            status, body = get(url, headers or {}, timeout)
            if status != 200:
                raise RuntimeError(f"Failed to download image from CDN: HTTP {status}")
            return body
        except Exception as exc:
            last = exc
            if attempt >= attempts or not _download_retryable(exc):
                raise
            time.sleep(min(2.0, 0.5 * attempt))
    if last:
        raise last
    raise RuntimeError("image download failed")


async def _download_raw_image(url: str, headers: dict | None = None) -> bytes:
    """Download raw image bytes from Google CDN.

    Uses requests in a thread rather than aiohttp: aiodns needs the
    SelectorEventLoop on Windows, but the agent runs uvicorn's default
    Proactor loop, which aiohttp DNS cannot use.

    A local relay URL is tried once. The caller then falls through to the
    signed CDN URL instead of burning another full timeout on a stuck port.
    """
    import requests as _requests

    via_relay = bool(_unwrap_relay_url(url))
    timeout = _RELAY_DOWNLOAD_TIMEOUT_S if via_relay else _DIRECT_DOWNLOAD_TIMEOUT_S
    attempts = 1 if via_relay else _DIRECT_DOWNLOAD_ATTEMPTS

    def _get(target: str, hdrs: dict, limit: float) -> tuple[int, bytes]:
        resp = _requests.get(target, timeout=limit, verify=False, headers=hdrs)
        return resp.status_code, resp.content

    return await asyncio.to_thread(
        _fetch_image_bytes, _get, url, headers, timeout, attempts)


async def get_or_clean_image(
    media_id: str,
    raw_url: Optional[str] = None,
    raw: bool = False,
    flow_client=None,
) -> Path:
    """Fetch and return path to clean image file for media_id, using local cache.

    If raw=True, returns the original untouched Google CDN file.
    """
    clean_path = MEDIA_CACHE_DIR / f"{media_id}_clean.jpg"
    raw_path = MEDIA_CACHE_DIR / f"{media_id}_raw.jpg"

    def _cache_ok(p: Path) -> bool:
        if not p.exists():
            return False
        try:
            with Image.open(p) as im:
                return max(im.size) >= 700
        except Exception:
            return True

    if not raw and _cache_ok(clean_path):
        return clean_path
    if raw and _cache_ok(raw_path):
        return raw_path

    # Wait for any in-flight prefetch task for this media_id
    if media_id in _IN_FLIGHT_FETCHES:
        try:
            await _IN_FLIGHT_FETCHES[media_id]
            if not raw and _cache_ok(clean_path):
                return clean_path
            if raw and _cache_ok(raw_path):
                return raw_path
        except Exception as e:
            logger.debug("Prefetch task error for %s: %s", media_id, e)

    # Need URL
    url = raw_url
    if not url:
        if not flow_client:
            raise ValueError(f"No raw_url or flow_client provided to fetch media {media_id}")
        # Freshly generated media may take a beat to become visible to
        # BatchGetMedia — retry briefly before giving up.
        last_err = None
        for attempt in range(4):
            try:
                meta = await flow_client.get_media(media_id)
                img_info = meta.get("data", meta).get("image", {})
                url = img_info.get("fifeUrl") or img_info.get("servingUri")
            except Exception as e:
                last_err = e
            if url:
                break
            if attempt < 3:
                await asyncio.sleep(2.0 + attempt)
        if not url:
            raise RuntimeError(f"Could not resolve CDN URL for media {media_id}")

    auth = {}
    if flow_client and hasattr(flow_client, "media_auth_headers"):
        # mint_access_token() inside is a blocking proxy HTTP call — keep it
        # off the event loop or every image fetch stalls the whole agent.
        try:
            auth = await asyncio.to_thread(
                flow_client.media_auth_headers, media_id) or {}
        except Exception:
            auth = {}
    from agent.services.cdn import bump_google_image_url
    candidates = []
    bumped = bump_google_image_url(url)
    candidates.append(bumped)
    if bumped != url:
        candidates.append(url)
    if flow_client and hasattr(flow_client, "media_url_via_relay"):
        def _wrap_all():
            out = []
            for u in candidates:
                try:
                    out.append(flow_client.media_url_via_relay(u, media_id)
                               or u)
                except Exception:
                    out.append(u)
            return out
        candidates = await asyncio.to_thread(_wrap_all)
    raw_bytes = b""
    last_err = None
    for u in _image_download_order(candidates):
        try:
            raw_bytes = await _download_raw_image(u, headers=auth)
            if raw_bytes.startswith(b"\xff\xd8") or raw_bytes.startswith(b"\x89PNG"):
                break
        except Exception as e:
            last_err = e
            raw_bytes = b""
            logger.warning(
                "image %s download failed via %s: %s",
                media_id,
                "relay" if _unwrap_relay_url(u) else "cdn",
                e,
            )
    if not raw_bytes:
        if last_err:
            raise last_err
        raise RuntimeError(f"empty download for {media_id}")
    if not raw_bytes.startswith(b"\xff\xd8") and not raw_bytes.startswith(b"\x89PNG"):
        raise RuntimeError(
            f"Downloaded {len(raw_bytes)}B for {media_id} is not an image "
            f"(head={raw_bytes[:16]!r}) — refusing to cache")
    raw_path.write_bytes(raw_bytes)

    if raw:
        return raw_path

    # Clean watermark — numpy/PIL work is too heavy for the event loop.
    clean_bytes = await asyncio.to_thread(
        remove_watermark_from_bytes, raw_bytes)
    clean_path.write_bytes(clean_bytes)
    return clean_path


async def prefetch_clean_image(media_id: str, raw_url: str) -> None:
    """Background task to pre-download and pre-clean image as soon as generated."""
    clean_path = MEDIA_CACHE_DIR / f"{media_id}_clean.jpg"
    if clean_path.exists():
        return

    async def _do_prefetch():
        try:
            await get_or_clean_image(media_id, raw_url=raw_url, raw=False)
            logger.info("Prefetched and cleaned image %s in background", media_id)
        except Exception as e:
            logger.warning("Prefetch failed for image %s: %s", media_id, e)
        finally:
            _IN_FLIGHT_FETCHES.pop(media_id, None)

    task = asyncio.create_task(_do_prefetch())
    _IN_FLIGHT_FETCHES[media_id] = task


async def get_or_fetch_video(
    media_id: str,
    raw_url: Optional[str] = None,
    flow_client=None,
) -> Path:
    """Fetch and cache MP4 video for media_id, handling lh3.googleusercontent.com/asb/
    video URLs that require the owning account's Bearer token and =m22/=m37 suffix."""
    import re
    import requests as _requests

    video_path = MEDIA_CACHE_DIR / f"{media_id}.mp4"
    if video_path.exists() and video_path.stat().st_size > 1024:
        return video_path

    url = raw_url
    if not url:
        if not flow_client:
            raise ValueError(f"No raw_url or flow_client provided to fetch video {media_id}")
        for attempt in range(3):
            try:
                meta = await flow_client.get_media(media_id)
                vid_info = meta.get("data", meta).get("video", {})
                url = vid_info.get("rawFifeUrl") or vid_info.get("fifeUrl") or vid_info.get("url")
            except Exception:
                pass
            if url and f"/api/flow/video/{media_id}" not in str(url):
                break
            url = None
            if attempt < 2:
                await asyncio.sleep(1.5 + attempt)
        if not url:
            raise RuntimeError(f"Could not resolve CDN video URL for media {media_id}")

    auth = {}
    if flow_client and hasattr(flow_client, "media_auth_headers"):
        try:
            auth = await asyncio.to_thread(
                flow_client.media_auth_headers, media_id) or {}
        except Exception:
            auth = {}

    if "googleusercontent.com" in url:
        base_u = re.sub(r"=[a-z0-9_-]+$", "", url.split("?", 1)[0], flags=re.I)
        qs = ("?" + url.split("?", 1)[1]) if "?" in url else ""
        # Never include =m18 (360p): Google ASB exposes the URL as soon as =m18
        # is transcoded while =m15 (original master: 1080p/4K for upsampled,
        # 720p for base) / =m22 (720p transcode) takes 2-8s longer to mux.
        # Note: =m37 is 404 on FIFE ASB; =m15 (itag 15) is the true master MP4.
        is_upsampled = media_id.endswith("_upsampled")
        primary_sufs = ("=m15",) if is_upsampled else ("=m15", "=m22")
        fallback_sufs = ("=m22",) if is_upsampled else ()
        primary_candidates = [f"{base_u}{suf}{qs}" for suf in primary_sufs]
        fallback_candidates = [f"{base_u}{suf}{qs}" for suf in fallback_sufs]
    else:
        primary_candidates = [url]
        fallback_candidates = []

    if flow_client and hasattr(flow_client, "media_url_via_relay"):
        def _wrap_list(lst: list[str]) -> list[str]:
            out = []
            for u in lst:
                try:
                    out.append(flow_client.media_url_via_relay(u, media_id) or u)
                except Exception:
                    out.append(u)
            return out
        primary_candidates = await asyncio.to_thread(_wrap_list, primary_candidates)
        if fallback_candidates:
            fallback_candidates = await asyncio.to_thread(_wrap_list, fallback_candidates)

    def _download_one(u: str) -> bytes:
        resp = _requests.get(u, timeout=60, verify=False, headers=auth)
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code}")
        return resp.content

    raw_bytes = b""
    last_err = None
    # Retry primary high-res stream (=m15 master MP4 for 1080p/4K/720p)
    # up to 12 attempts (~24s) while Google CDN finishes muxing it.
    for attempt in range(12):
        for u in primary_candidates:
            try:
                data = await asyncio.to_thread(_download_one, u)
                if len(data) > 1024 and b"ftyp" in data[:32]:
                    raw_bytes = data
                    break
            except Exception as e:
                last_err = e
        if raw_bytes:
            break
        if attempt < 11:
            await asyncio.sleep(2.0)

    if not raw_bytes and fallback_candidates:
        for u in fallback_candidates:
            try:
                data = await asyncio.to_thread(_download_one, u)
                if len(data) > 1024 and b"ftyp" in data[:32]:
                    raw_bytes = data
                    break
            except Exception as e:
                last_err = e

    if not raw_bytes:
        if last_err:
            raise last_err
        raise RuntimeError(f"Could not download valid MP4 for {media_id}")

    tmp_path = MEDIA_CACHE_DIR / f"{media_id}.mp4.tmp"
    tmp_path.write_bytes(raw_bytes)
    tmp_path.replace(video_path)
    return video_path

