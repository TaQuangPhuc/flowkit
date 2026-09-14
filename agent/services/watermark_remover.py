"""Service to detect and cleanly remove Google AI watermark (sparkle star)
from Imagen 3 / Gemini / Google Flow images using calibrated reverse alpha
blending and boundary-preserving filtering.
"""

import asyncio
from io import BytesIO
import logging
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


async def _download_raw_image(url: str) -> bytes:
    """Download raw image bytes from Google CDN."""
    connector = aiohttp.TCPConnector(ssl=False)
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Failed to download image from CDN: HTTP {resp.status}")
            return await resp.read()


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

    if not raw and clean_path.exists():
        return clean_path
    if raw and raw_path.exists():
        return raw_path

    # Wait for any in-flight prefetch task for this media_id
    if media_id in _IN_FLIGHT_FETCHES:
        try:
            await _IN_FLIGHT_FETCHES[media_id]
            if not raw and clean_path.exists():
                return clean_path
            if raw and raw_path.exists():
                return raw_path
        except Exception as e:
            logger.debug("Prefetch task error for %s: %s", media_id, e)

    # Need URL
    url = raw_url
    if not url:
        if not flow_client:
            raise ValueError(f"No raw_url or flow_client provided to fetch media {media_id}")
        meta = await flow_client.get_media(media_id)
        img_info = meta.get("data", meta).get("image", {})
        url = img_info.get("fifeUrl") or img_info.get("servingUri")
        if not url:
            raise RuntimeError(f"Could not resolve CDN URL for media {media_id}")

    raw_bytes = await _download_raw_image(url)
    raw_path.write_bytes(raw_bytes)

    if raw:
        return raw_path

    # Clean watermark
    clean_bytes = remove_watermark_from_bytes(raw_bytes)
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
