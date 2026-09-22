"""Fashion Lookbook Studio — Standalone AI Fashion Video Engine.

Decoupled from TVC Affiliate Pipeline:
- Zero-Dialogue, Zero-TTS (No speech, no lip-sync deformation).
- 100% Visual High-Fashion & Fabric Physics (drape, fold, texture, satin sheen).
- Dual-Reference Lock: Model Lock + Garment Lock.
- Continuous Fashion Runway BGM (Vogue, Deep House, Paris Jazz).
"""

import os
import re
import time
import json
import uuid
import base64
import shutil
import zipfile
import threading
import subprocess
import urllib.request
import urllib.error
from pathlib import Path
from typing import Optional, Any
from concurrent.futures import ThreadPoolExecutor

from agent.services.parked_retry import ParkedBackoff, retry_after_seconds

WORK_DIR = Path("/home/pc/flowkit/auto_runs")
FLOWKIT_API = "http://127.0.0.1:8100"
BGM_DIR = Path("/home/pc/flowkit/assets/bgm")
NOVA_BASE_URL = os.environ.get("NOVA_BASE_URL", "https://api.vilao.ai/v1")
NOVA_API_KEY = os.environ.get("NOVA_API_KEY", "sk-72afd079199f58a7b302e65b6690744ce8cf7b44c0dcd163070052e7fa774535")
NOVA_MODEL = os.environ.get("NOVA_MODEL", "chib/deepseek-v4.1-flash")

LOOKBOOK_JOBS: dict[str, dict] = {}

LOOKBOOK_CONFIG = {
    "num_threads": int(os.environ.get("LOOKBOOK_THREADS", "4")),
    "max_allowed_threads": 10,
    "default_threads": 4
}


def get_lookbook_threads() -> dict:
    return {
        "ok": True,
        "num_threads": LOOKBOOK_CONFIG["num_threads"],
        "default_threads": LOOKBOOK_CONFIG["default_threads"],
        "max_allowed": LOOKBOOK_CONFIG["max_allowed_threads"]
    }


def set_lookbook_threads(threads: int) -> dict:
    val = max(1, min(LOOKBOOK_CONFIG["max_allowed_threads"], int(threads)))
    LOOKBOOK_CONFIG["num_threads"] = val
    return {
        "ok": True,
        "num_threads": val,
        "message": f"Đã cập nhật số luồng xử lý Lookbook thành {val} luồng song song"
    }


# ─── TEMPLATES DEFINITION ────────────────────────────────────────────────────

LOOKBOOK_TEMPLATES = {
    "runway_catwalk": {
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
                "description": "Toàn thân sải bước thanh lịch tiến về phía ống kính, phô diễn trọn vẹn phom dáng trang phục.",
                "pose_desc": "Full-body frontal runway walking pose, model confidently striding down the runway toward the camera, upright posture, elegant pace, complete outfit visible from head to toe, soft luxury studio lighting, photorealistic 8k vertical 9:16.",
                "motion_desc": "Full-body runway catwalk walk. Model strides forward with confident haute couture pace, hips swaying naturally, garment fabric flowing rhythmically with each step. Camera slow gimbal push-in. High-fashion runway pace, smooth motion, 24fps cinematic, photorealistic."
            },
            {
                "scene_index": 2,
                "shot_type": "Medium Bodice & Fabric Detail",
                "description": "Cận trung đẩy máy vào chi tiết nơ/cổ/đường may và độ óng ánh của chất liệu vải cao cấp.",
                "pose_desc": "Medium shot framing from chest to hips, hands resting gently near the waist, highlighting fabric texture, fine stitch lines, neckline design and buttons, luxury diffuse studio lighting, photorealistic 8k vertical 9:16.",
                "motion_desc": "Medium shot. Model subtly shifts weight, gently running fingertips near waistline to showcase the luxury fabric texture and drape. Camera slow elegant push-in, sharp fabric focus, smooth 24fps cinematic."
            },
            {
                "scene_index": 3,
                "shot_type": "Dynamic 3/4 Turnaround & Exit",
                "description": "Xoay người góc 3/4 mềm mại khoe chuyển động bồng bềnh của tùng váy rồi sải bước quý phái.",
                "pose_desc": "Three-quarter turnaround pose showing the side and back silhouette of the outfit, looking back gracefully over the shoulder, hemline flare visible, soft backlighting, photorealistic 8k vertical 9:16.",
                "motion_desc": "Dynamic 3/4 turn. Model executes a smooth graceful turnaround pivot, letting the hemline flare out softly, then pauses looking over shoulder before taking graceful exit steps. Camera slow orbit."
            }
        ]
    },
    "cyclorama_studio": {
        "template_id": "cyclorama_studio",
        "name": "🏛️ Cyclorama Minimalist Studio",
        "badge": "Chuẩn Lookbook Hãng",
        "description": "Phông vô cực studio trắng/xám tối giản, ánh sáng khuếch tán dịu nhẹ, giữ nguyên 100% phom dáng & đường may.",
        "default_num_scenes": 3,
        "default_duration_per_scene": 8,
        "shot_list": [
            {
                "scene_index": 1,
                "shot_type": "Studio Upright Pose",
                "description": "Tạo dáng đứng thanh lịch thẳng người, ánh mắt tự tin nhìn ống kính.",
                "pose_desc": "Full-length editorial standing pose on pure seamless cyclorama studio floor, upright posture, direct confident camera gaze, pristine garment silhouette, soft diffused key lighting, photorealistic 8k vertical 9:16.",
                "motion_desc": "Full body editorial pose. Model stands tall, subtle breathing motion, gently adjusting sleeve or posture with minimal refined gestures. Soft rim lighting, ultra stable."
            },
            {
                "scene_index": 2,
                "shot_type": "Macro Fabric & Craftsmanship",
                "description": "Góc quay macro cận cảnh chất vải, độ rủ và đường may giấu chỉ tinh tế.",
                "pose_desc": "Macro close-up shot focused on fabric weave, buttons, collar and seams, crisp high-definition textile physics, pure minimalist studio, photorealistic 8k vertical 9:16.",
                "motion_desc": "Macro slow pan across the garment bodice and waist, highlighting fine seams, button accents, and premium fabric texture. Gentle lighting shift."
            },
            {
                "scene_index": 3,
                "shot_type": "Side Profile & Silhouette",
                "description": "Góc nghiêng phô diễn đường cong eo và độ xòe tự nhiên của dáng váy.",
                "pose_desc": "Side profile standing pose, arching back gently to accentuate waistline contour and hemline taper against minimalist backdrop, photorealistic 8k vertical 9:16.",
                "motion_desc": "Model slowly turns from profile to three-quarter view, showcasing garment structure from multiple angles with poised grace."
            }
        ]
    },
    "paris_streetwalk": {
        "template_id": "paris_streetwalk",
        "name": "☕ Luxury Streetwalk (Paris / Milan)",
        "badge": "Phong cách Ngoại cảnh",
        "description": "Sải bước tự nhiên trên đường phố châu Âu cổ kính, tà áo bay nhẹ trong gió.",
        "default_num_scenes": 3,
        "default_duration_per_scene": 8,
        "shot_list": [
            {
                "scene_index": 1,
                "shot_type": "Street Catwalk Push-in",
                "description": "Sải bước trên phố cổ Paris, tà váy chuyển động tự nhiên theo nhịp chân.",
                "pose_desc": "Outdoor sidewalk of classic European limestone architecture (Parisian boulevard), model walking forward naturally, breeze moving the dress fabric, natural soft daylight, photorealistic 8k vertical 9:16.",
                "motion_desc": "Model walks gracefully down the Parisian sidewalk, hair and hem flutter gently in the warm breeze. Camera tracks backward at steady eye-level."
            },
            {
                "scene_index": 2,
                "shot_type": "Medium Waist & Accessories",
                "description": "Cận cảnh thắt lưng, tay cầm túi hoặc vuốt nhẹ tà áo duyên dáng.",
                "pose_desc": "Medium shot outdoors, warm golden hour ambient lighting, model holding chic mini handbag, fabric texture glowing softly, photorealistic 8k vertical 9:16.",
                "motion_desc": "Medium outdoor shot. Model pauses, lifts one hand to brush hair behind ear, natural soft smile, fabric shimmering in the golden sunset."
            },
            {
                "scene_index": 3,
                "shot_type": "Slow Motion Turn",
                "description": "Quay đầu mỉm cười nhẹ trong nắng vàng chiều hoàng hôn.",
                "pose_desc": "Three-quarter turn on cobblestone street in evening backlight, dress hem floating in motion, photorealistic 8k vertical 9:16.",
                "motion_desc": "Cinematic slow-motion turn, dress skirt catches the breeze, glowing bokeh background, smooth high-fashion finish."
            }
        ]
    }
}

# ─── MODEL PRESETS ───────────────────────────────────────────────────────────

LOOKBOOK_MODELS = {
    "asian_minimalist_25": {
        "id": "asian_minimalist_25",
        "model_id": "asian_minimalist_25",
        "name": "Cun-Hui (Á Đông Hiện Đại & Tối Giản)",
        "gender": "female",
        "age_approx": 25,
        "height_approx": "1m74",
        "description": "25 tuổi, 1m74, tóc đen suôn dài, nét đẹp Á Đông thuần khiết, phong cách tối giản thanh lịch vượt thời gian.",
        "vibe": "Nét đẹp Á Đông thuần khiết, tóc đen suôn dài, phong cách tối giản thanh lịch",
        "anchor": "An elegant 25-year-old East Asian female fashion model, 174cm height, sleek long black hair, porcelain skin, graceful serene features, minimalist high-fashion poise, effortless quiet luxury vibe.",
        "avatar_url": "/showcase/models/cunhui_yu.jpg",
        "thumbnail_url": "/showcase/models/cunhui_yu.jpg",
        "default_file": Path("/home/pc/flowkit/mau/models/cunhui_yu.jpg")
    },
    "parisian_chic_24": {
        "id": "parisian_chic_24",
        "model_id": "parisian_chic_24",
        "name": "Juliette (Parisian Chic & Tinh Tế)",
        "gender": "female",
        "age_approx": 24,
        "height_approx": "1m75",
        "description": "24 tuổi, 1m75, nét đẹp Pháp tự nhiên thanh tú, mắt nâu sâu thẳm, phong thái lãng mạn nhẹ nhàng.",
        "vibe": "Nét đẹp Pháp tự nhiên, mắt nâu sâu thẳm, phong thái lãng mạn nhẹ nhàng",
        "anchor": "A chic 24-year-old French female fashion model, 175cm height, natural brown hair falling gently over shoulders, refined European features, deep soulful brown eyes, Parisian understated elegance, calm sophisticated gaze.",
        "avatar_url": "/showcase/models/juliette_potier.jpg",
        "thumbnail_url": "/showcase/models/juliette_potier.jpg",
        "default_file": Path("/home/pc/flowkit/mau/models/juliette_potier.jpg")
    },
    "western_gentleman_26": {
        "id": "western_gentleman_26",
        "model_id": "western_gentleman_26",
        "name": "Aiden (Nam Âu Mỹ Lịch Lãm & Suit)",
        "gender": "male",
        "age_approx": 26,
        "height_approx": "1m85",
        "description": "26 tuổi, 1m85, đường nét góc cạnh nam tính, ánh nhìn cuốn hút, chuẩn quý ông thời trang vest & suit.",
        "vibe": "Gương mặt góc cạnh nam tính, ánh nhìn cuốn hút, chuẩn quý ông thời trang cao cấp",
        "anchor": "A handsome 26-year-old Caucasian male fashion model, 185cm height, strong jawline, neatly styled brown hair, warm confident expression, athletic lean build, sophisticated gentleman demeanor.",
        "avatar_url": "/showcase/models/aiden_schmahl.jpg",
        "thumbnail_url": "/showcase/models/aiden_schmahl.jpg",
        "default_file": Path("/home/pc/flowkit/mau/models/aiden_schmahl.jpg")
    },
    "asian_streetwear_23": {
        "id": "asian_streetwear_23",
        "model_id": "asian_streetwear_23",
        "name": "Chang-Yi (Nam Á Đông Streetwear Cá Tính)",
        "gender": "male",
        "age_approx": 23,
        "height_approx": "1m82",
        "description": "23 tuổi, 1m82, phong cách street-style Á Đông sắc nét, vóc dáng chuẩn, phù hợp thời trang unisex và trẻ trung.",
        "vibe": "Street-style Á Đông sắc nét, cá tính, vóc dáng chuẩn",
        "anchor": "A striking 23-year-old East Asian male fashion model, 182cm height, contemporary buzz cut, sharp high-fashion facial contours, intense charismatic gaze, lean athletic physique, edgy modern streetwear aesthetic.",
        "avatar_url": "/showcase/models/changyi_chen.jpg",
        "thumbnail_url": "/showcase/models/changyi_chen.jpg",
        "default_file": Path("/home/pc/flowkit/mau/models/changyi_chen.jpg")
    },
    "southeast_asian_23": {
        "id": "southeast_asian_23",
        "model_id": "southeast_asian_23",
        "name": "Sirimanee (Đông Nam Á Năng Động & Tươi Tắn)",
        "gender": "female",
        "age_approx": 23,
        "height_approx": "1m71",
        "description": "23 tuổi, 1m71, làn da bánh mật khỏe khoắn, nụ cười rạng rỡ, thần thái nhiệt đới tràn đầy sức sống.",
        "vibe": "Làn da bánh mật khỏe khoắn, nụ cười rạng rỡ, năng lượng nhiệt đới tươi mới",
        "anchor": "A radiant 23-year-old Southeast Asian female fashion model, 171cm height, warm honey-toned skin, glowing natural complexion, flowing dark brown hair, bright expressive eyes, youthful energetic high-fashion presence.",
        "avatar_url": "/showcase/models/sirimanee.jpg",
        "thumbnail_url": "/showcase/models/sirimanee.jpg",
        "default_file": Path("/home/pc/flowkit/mau/models/sirimanee.jpg")
    },
    "latin_editorial_25": {
        "id": "latin_editorial_25",
        "model_id": "latin_editorial_25",
        "name": "Tomas (Latin Phóng Khoáng & Khỏe Khoắn)",
        "gender": "male",
        "age_approx": 25,
        "height_approx": "1m84",
        "description": "25 tuổi, 1m84, đường nét Latin nam tính, tóc nâu gợn sóng, phong thái tự do lãng tử và thể thao.",
        "vibe": "Đường nét Latin nam tính, tóc nâu gợn sóng, phong thái tự do lãng tử",
        "anchor": "A charismatic 25-year-old Latino male fashion model, 184cm height, sculpted facial features, textured wavy brown hair, athletic toned build, warm magnetic presence, stylish resort and casual wear aesthetic.",
        "avatar_url": "/showcase/models/tomas_gonzalez.jpg",
        "thumbnail_url": "/showcase/models/tomas_gonzalez.jpg",
        "default_file": Path("/home/pc/flowkit/mau/models/tomas_gonzalez.jpg")
    }
}
# Backward compatibility aliases for removed legacy models
LOOKBOOK_MODELS["asian_elegance_24"] = LOOKBOOK_MODELS["asian_minimalist_25"]
LOOKBOOK_MODELS["korean_chic_22"] = LOOKBOOK_MODELS["asian_minimalist_25"]
LOOKBOOK_MODELS["korean_minimalist_23"] = LOOKBOOK_MODELS["asian_minimalist_25"]
LOOKBOOK_MODELS["caucasian_chic_25"] = LOOKBOOK_MODELS["parisian_chic_24"]
LOOKBOOK_MODELS["elena_runway_25"] = LOOKBOOK_MODELS["parisian_chic_24"]

# ─── MUSIC PRESETS ───────────────────────────────────────────────────────────

LOOKBOOK_BGM = {
    "original": {
        "bgm_id": "original",
        "label": "🎵 Nhạc nền gốc đề xuất (Tự động mix)",
        "file": None
    },
    "vogue_runway": {
        "bgm_id": "vogue_runway",
        "label": "✨ Vogue Runway Beats (120 BPM - Sàn diễn)",
        "file": BGM_DIR / "vogue_runway_120bpm.mp3"
    },
    "french_chic": {
        "bgm_id": "french_chic",
        "label": "🍷 French Chic Lounge (Thanh lịch, thời thượng)",
        "file": BGM_DIR / "french_chic_lounge.mp3"
    },
    "luxury_minimal": {
        "bgm_id": "luxury_minimal",
        "label": "💎 Luxury Minimal Ambient (Sang trọng, êm dịu)",
        "file": BGM_DIR / "luxury_minimal_ambient.mp3"
    },
    "tiktok_viral": {
        "bgm_id": "tiktok_viral",
        "label": "🔥 TikTok Viral Fashion (Bắt tai, xu hướng)",
        "file": BGM_DIR / "tiktok_playful_snitch.mp3"
    },
    "lofi_chill": {
        "bgm_id": "lofi_chill",
        "label": "🍃 Lo-Fi Chill Runway (Êm dịu, tối giản)",
        "file": BGM_DIR / "lofi_chill.mp3"
    },
    "none": {
        "bgm_id": "none",
        "label": "🔇 Giữ âm thanh gốc / Không lồng nhạc ngoài",
        "file": None
    }
}

def get_public_templates() -> list[dict]:
    return list(LOOKBOOK_TEMPLATES.values())

def get_public_models() -> list[dict]:
    res = []
    seen = set()
    for m in LOOKBOOK_MODELS.values():
        mid = m.get("id") or m.get("model_id")
        if mid in seen:
            continue
        seen.add(mid)
        item = dict(m)
        if "default_file" in item:
            item["default_file"] = str(item["default_file"]) if item["default_file"] else None
        res.append(item)
    return res

def get_public_music() -> list[dict]:
    res = []
    for k, v in LOOKBOOK_BGM.items():
        res.append({
            "bgm_id": k,
            "label": v["label"]
        })
    return res


# ─── MASTER FLOW: THỜI TRANG AI (PRESETS & PROMPT DIRECTORS) ─────────────────

POSES = [
    "Toàn thân, nhìn thẳng",
    "Toàn thân, nhìn từ sau",
    "Toàn thân, nhìn nghiêng",
    "Góc ba phần tư",
    "Cận cảnh từ thắt lưng trở lên",
    "Cận cảnh chi tiết chất liệu vải",
    "Cận cảnh chi tiết cổ áo / ngực",
    "Cận cảnh chi tiết tay áo / vai",
    "Cận cảnh chi tiết thắt lưng / độ ôm",
    "Cận cảnh chân váy / gấu áo",
    "Tư thế đang bước đi",
    "Tư thế đang xoay người",
    "Tư thế catalogue chuyên nghiệp",
    "Tư thế chiến dịch thời trang cao cấp",
    "Tư thế phong cách sống tự nhiên"
]

STYLE_PRESETS = [
    "Luxury boutique fashion",
    "Clean ecommerce catalog",
    "High-end fashion lookbook",
    "Street fashion editorial",
    "Outdoor lifestyle fashion"
]

LIGHTING_PRESETS = [
    "soft studio lighting",
    "natural daylight",
    "cinematic boutique lighting",
    "golden hour outdoor light"
]

BACKGROUND_PRESETS = [
    "pure white studio",
    "luxury boutique interior",
    "modern fashion studio",
    "city street",
    "hotel lobby"
]

MOTION_PRESETS = [
    "Elegant Turnaround",
    "Full Outfit Review",
    "Fabric Detail Focus",
    "Boutique Walk",
    "Luxury Campaign Pose"
]

CAMERA_MOVEMENTS = [
    "Cinematic Push-in",
    "Head-to-Toe Scan",
    "Slow Orbit",
    "Gimbal Track"
]

VIDEO_MODELS = [
    "Omni Flash",
    "Veo 3.1 - Fast",
    "Veo 3.1 - Lite",
    "Veo"
]


def build_image_prompt(
    pose: str,
    aspect_ratio: str = "3:4",
    quality: str = "4K",
    style: str = "Luxury boutique fashion",
    lighting: str = "soft studio lighting",
    background_preset: str = "pure white studio",
    has_background_ref: bool = False
) -> str:
    """Master Flow Template #1 & #2: Reverse-engineered fashion director prompt."""
    if has_background_ref:
        background_instruction = "SỬ DỤNG CHÍNH XÁC phông nền từ hình ảnh tham chiếu phông nền đã tải lên. Giữ nguyên màu sắc, ánh sáng, vật liệu và không gian của phông nền đó."
    else:
        background_instruction = f"Sử dụng bối cảnh: {background_preset}."

    return (
        f"Sử dụng hình ảnh người mẫu được cung cấp làm nhân vật chính. Giữ nguyên khuôn mặt, kiểu tóc, màu da và tỷ lệ cơ thể. "
        f"Cho người mẫu mặc các sản phẩm thời trang từ ảnh tham chiếu sản phẩm. Giữ nguyên thiết kế sản phẩm, bao gồm màu sắc, chất liệu, hoa văn, đường may, cổ áo, tay áo và các chi tiết nhìn thấy được. "
        f"Tạo ảnh chụp thời trang chuyên nghiệp tỷ lệ {aspect_ratio}, chất lượng {quality}. "
        f"Phong cách: {style}. "
        f"Ánh sáng: {lighting}. "
        f"{background_instruction} "
        f"Góc chụp: {pose}. "
        f"Tập trung tối đa vào sản phẩm thời trang. Hiệu ứng vải thực tế, nếp gấp tự nhiên, bố cục sạch sẽ, không có watermark, không có văn bản/logo lạ, không biến dạng cơ thể."
    )


def build_video_prompt(
    motion_preset: str = "Elegant Turnaround",
    camera_movement: str = "Cinematic Push-in",
    duration: int = 8
) -> str:
    """Master Flow Template #3: Reverse-engineered fashion video prompt."""
    return (
        f"Sử dụng hình ảnh này làm tham chiếu hình ảnh chính xác. Giữ nguyên danh tính người mẫu, khuôn mặt, trang phục, chi tiết sản phẩm, chất liệu và bối cảnh. "
        f"Tạo video thời trang chất lượng cao dài {duration} giây. "
        f"Người mẫu thực hiện: {motion_preset}. "
        f"Camera thực hiện: {camera_movement}. "
        f"Tập trung vào sản phẩm. Chuyển động mượt mà, vật lý vải thực tế, phong cách quảng cáo cao cấp, không thay đổi khuôn mặt, không biến dạng trang phục, không watermark."
    )


def select_default_poses(quantity: int) -> list[str]:
    """Select the most diverse poses matching the requested lookbook quantity."""
    if quantity <= 1:
        return [POSES[0]]
    if quantity == 2:
        return [POSES[0], POSES[5]]
    if quantity == 3:
        return [POSES[0], POSES[5], POSES[11]]
    if quantity == 4:
        return [POSES[0], POSES[5], POSES[3], POSES[11]]
    if quantity == 6:
        return [POSES[0], POSES[1], POSES[3], POSES[5], POSES[6], POSES[11]]
    if quantity >= 8:
        return [POSES[0], POSES[1], POSES[2], POSES[3], POSES[5], POSES[6], POSES[8], POSES[11]]
    return POSES[:quantity]


def get_all_lookbook_presets() -> dict:
    """Return all catalog presets for UI dropdowns and configurations."""
    return {
        "poses": POSES,
        "styles": STYLE_PRESETS,
        "lightings": LIGHTING_PRESETS,
        "backgrounds": BACKGROUND_PRESETS,
        "motions": MOTION_PRESETS,
        "cameras": CAMERA_MOVEMENTS,
        "video_models": VIDEO_MODELS,
        "models": get_public_models(),
        "templates": get_public_templates(),
        "music": get_public_music(),
        "aspect_ratios": ["3:4", "9:16", "16:9"],
        "qualities": ["2K", "4K"],
        "quantities": [1, 2, 4, 6, 8]
    }


# ─── HELPER FUNCTIONS ────────────────────────────────────────────────────────

def call_flowkit_api(endpoint: str, payload: dict, timeout: int = 180, max_retries: int = 4) -> dict:
    if endpoint in ("/api/flow/generate-video", "/api/flow/generate-video-refs"):
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
                "User-Agent": "FlowKit-LookbookStudio/1.0"
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
            # A fully parked fleet submitted nothing: wait it out instead of
            # failing the item after ~10s of 503s. Does not consume an attempt.
            if parked.wait(err, err_body):
                attempt -= 1
                continue
            if attempt < max_retries and (err.code == 429 or "UNUSUAL" in err_body.upper()):
                time.sleep(retry_after_seconds(err, 4.0))
                continue
            elif attempt < max_retries and err.code in [500, 502, 503, 504]:
                time.sleep(retry_after_seconds(err, 2.5))
                continue
            raise RuntimeError(f"FlowKit error {err.code} on {endpoint}: {err_body or err.reason}")
        except Exception:
            if attempt < max_retries:
                time.sleep(2.5)
                continue
            raise


def upload_image_flowkit(image_path: Path) -> str:
    b64 = base64.b64encode(image_path.read_bytes()).decode("ascii")
    payload = {
        "image_base64": b64,
        "mime_type": "image/jpeg",
        "file_name": image_path.name
    }
    res = call_flowkit_api("/api/flow/upload-image", payload, timeout=120)
    media = res.get("media") or (res.get("data") or {}).get("media") or {}
    if isinstance(media, list):
        media = media[0] if media else {}
    mid = res.get("media_id") or media.get("name") or media.get("id")
    if not mid:
        raise RuntimeError(f"Upload failed: {res}")
    return mid


def grok_vision_analyze(prompt: str, image_paths: list[Path]) -> dict:
    """Analyze images using Nova Gateway Google Gemini 3.8 Flash Vision."""
    images_payload = []
    for p in image_paths:
        if p and p.exists():
            b64 = base64.b64encode(p.read_bytes()).decode("utf-8")
            images_payload.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{b64}"}
            })

    content = [{"type": "text", "text": prompt}] + images_payload
    candidate_models = [
        os.environ.get("NOVA_MODEL", NOVA_MODEL),
        "spd/grok-4.6",
        "grok-4.6",
        "cnt/grok-4.6",
        "fa/grok-4.6-fast"
    ]
    api_key = os.environ.get("NOVA_API_KEY", NOVA_API_KEY)
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    }
    endpoints = [
        f"{NOVA_BASE_URL}/chat/completions",
        "http://127.0.0.1:8080/v1/chat/completions"
    ]
    last_err = None
    for model_name in candidate_models:
        payload = {
            "model": model_name,
            "messages": [{"role": "user", "content": content}],
            "max_tokens": 4096,
            "response_format": {"type": "json_object"}
        }
        for ep in endpoints:
            try:
                req = urllib.request.Request(
                    ep,
                    data=json.dumps(payload).encode("utf-8"),
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=120) as resp:
                    res = json.loads(resp.read().decode("utf-8"))
                raw_text = res["choices"][0]["message"]["content"].strip()
                if "```" in raw_text:
                    raw_text = re.sub(r"^```(?:json)?\s*", "", raw_text.strip())
                    raw_text = re.sub(r"\s*```$", "", raw_text.strip())
                return json.loads(raw_text)
            except Exception as e:
                last_err = e
                continue
    raise last_err or RuntimeError("Failed to analyze outfit vision")


def extract_garment_profile(outfit_path: Path, model_path: Optional[Path] = None) -> dict:
    """Analyze garment image to extract high-fashion visual anchor."""
    prompt = f"""
    Read and analyze the fashion garment image at {outfit_path.name}.
    You are an International Haute Couture Creative Director & Head Stylist.
    Extract the exact garment structure, silhouette, fabric, and color palette.
    Return a strict JSON object with:
    {{
      "product": {{
        "name": "Detailed garment name (e.g. Silk Pleated Evening Gown, Tailored Tweed Blazer Set)",
        "category": "Váy đầm / Set đồ / Áo khoác / Thời trang thiết kế",
        "fabric": "exact fabric texture (lụa tơ tằm, dạ tweed, chiffon, organza, satin cao cấp)",
        "color": "exact color shade and sheen",
        "silhouette": "A-line, fitted waist, oversized, maxi, mermaid, tailored fit",
        "details": "neckline, sleeve cut, buttons, pleats, hemline, waist cinching"
      }},
      "canonical_visual_anchor": "CRITICAL: MUST BE 100% IN CONCISE ENGLISH. Precise visual description of the fashion garment for image generation models. Detail exact silhouette, color palette, fabric texture, natural drape, clean stitching lines, buttons/zipper, and hemline. Emphasize luxurious realistic fabric physics and tailored fit."
    }}
    """
    try:
        data = grok_vision_analyze(prompt, [outfit_path])
        if "canonical_visual_anchor" in data:
            return data
    except Exception as e:
        print(f"[LOOKBOOK VISION] Nova Vision failed ({e}), using default fallback.")

    return {
        "product": {
            "name": "Bộ trang phục thiết kế cao cấp",
            "category": "Thời trang thiết kế",
            "fabric": "Chất liệu cao cấp",
            "color": "Màu sắc thanh lịch",
            "silhouette": "Form dáng chuẩn",
            "details": "Đường may tỉ mỉ"
        },
        "canonical_visual_anchor": "High-end luxury designer garment, tailored fit, pristine fabric texture, natural soft drape and realistic folds, immaculate seam construction, elegant haute couture silhouette."
    }


def extract_model_profile(model_path: Path) -> dict:
    """Analyze uploaded model image with Vision to extract precise identity & facial features."""
    prompt = """
    Analyze the person in this image.
    You are a professional fashion casting director and AI portrait visual consistency specialist.
    Extract the exact physical characteristics to recreate this EXACT SAME PERSON in new fashion poses.
    Return a strict JSON object with key 'model_visual_anchor':
    {
      "model_visual_anchor": "CRITICAL: MUST BE 100% IN CONCISE ENGLISH. Detailed visual description of this exact person's face shape, eye shape, nose bridge, lip fullness, skin tone, and exact hairstyle (length, texture, parting, natural flow) so an image generator can recreate their exact likeness and identity. Do NOT describe any clothing."
    }
    """
    try:
        data = grok_vision_analyze(prompt, [model_path])
        if "model_visual_anchor" in data and data["model_visual_anchor"]:
            return data
    except Exception as e:
        print(f"[LOOKBOOK MODEL VISION] Failed to analyze model ({e})")
    return {}


ACTIVE_LOOKBOOK_WORKERS: set[str] = set()


def save_lookbook_job(*args, **kwargs):
    job_id = args[0] if args else kwargs.pop("job_id", None)
    if not job_id and "job_id" in kwargs:
        job_id = kwargs["job_id"]
    kwargs.pop("job_id", None)
    if not job_id:
        return {}
    job = LOOKBOOK_JOBS.setdefault(job_id, {"job_id": job_id})
    job.update(kwargs)
    job["updated_at"] = time.time()

    jdir = WORK_DIR / f"lookbook_{job_id}"
    jdir.mkdir(parents=True, exist_ok=True)
    try:
        (jdir / "job.json").write_text(json.dumps(job, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[LOOKBOOK] Error saving job {job_id}: {e}")
    return job


def recover_lookbook_job(job_id: str) -> dict:
    """Auto-heal interrupted or stale lookbook jobs across restarts/crashes."""
    jdir = WORK_DIR / f"lookbook_{job_id}"
    jfile = jdir / "job.json"
    if not jfile.exists():
        return {}
    try:
        job = json.loads(jfile.read_text(encoding="utf-8"))
        LOOKBOOK_JOBS[job_id] = job
    except Exception:
        job = LOOKBOOK_JOBS.get(job_id) or {}

    if not job:
        return {}

    # If the main worker thread is actively processing this job, don't interfere
    if job_id in ACTIVE_LOOKBOOK_WORKERS:
        return LOOKBOOK_JOBS.get(job_id, job)

    status = job.get("status")
    created_at = job.get("created_at", 0)
    # If job is in early setup stages and recently created (< 300s), let worker run
    if status in ["QUEUED", "ANALYZING", "PREPARING_REFS", "GENERATING_KEYFRAMES"]:
        if time.time() - created_at < 300:
            return LOOKBOOK_JOBS.get(job_id, job)

    scenes = job.get("scenes", [])
    num_scenes = int(job.get("num_scenes", 3))

    # If already COMPLETED, ensure final_lookbook.mp4 exists
    if status == "COMPLETED":
        final_mp4 = jdir / "final_lookbook.mp4"
        if not final_mp4.exists():
            stitch_lookbook_final(job_id)
        return LOOKBOOK_JOBS.get(job_id, job)

    # ── STAGE 2 RECOVERY ──
    video_clips = job.get("video_clips", [])
    if video_clips or job.get("stage") == 2:
        valid_clips = []
        has_pending = False
        changed = False
        for vc in video_clips:
            cid = vc.get("clip_id") or vc.get("source_image_id")
            cp = jdir / f"clip_{cid}.mp4"
            if cp.exists() and cp.stat().st_size > 50000:
                if vc.get("status") != "COMPLETED":
                    vc["status"] = "COMPLETED"
                    vc["status_text"] = "Đã hoàn thành video"
                    vc["video_url"] = f"/api/fashion-lookbook/stage2/clips/{job_id}/{cid}"
                    changed = True
                valid_clips.append(cp)
                continue

            op_name = vc.get("operation_name")
            if op_name:
                try:
                    p_body = {"operations": [{"operation": {"name": op_name}}]}
                    p_res = call_flowkit_api("/api/flow/check-status", p_body, timeout=20, max_retries=1)
                    ret_ops = p_res.get("operations") or (p_res.get("data") or {}).get("operations") or []
                    for op_item in ret_ops:
                        st = str(op_item.get("status") or "")
                        meta = (op_item.get("operation") or {}).get("metadata", {})
                        fife = meta.get("video", {}).get("fifeUrl")
                        if st == "MEDIA_GENERATION_STATUS_SUCCESSFUL" or fife:
                            urllib.request.urlretrieve(fife, str(cp))
                            vc["status"] = "COMPLETED"
                            vc["status_text"] = "Đã hoàn thành video"
                            vc["video_url"] = f"/api/fashion-lookbook/stage2/clips/{job_id}/{cid}"
                            valid_clips.append(cp)
                            changed = True
                            break
                        elif "PENDING" in st.upper() or not st:
                            has_pending = True
                            vc["status"] = "RENDERING_VIDEO"
                            changed = True
                            break
                except Exception as ex:
                    print(f"[STAGE2 WATCHDOG] Error checking status for {op_name}: {ex}")

            if vc.get("status") in ["PENDING", "RENDERING_VIDEO"]:
                has_pending = True

        if len(valid_clips) == len(video_clips) and len(video_clips) > 0:
            save_lookbook_job(
                job_id,
                stage=2,
                status="STAGE2_COMPLETED",
                progress_percent=100,
                message=f"Đã hoàn thành toàn bộ {len(video_clips)} video thời trang!",
                video_clips=video_clips
            )
            stitch_lookbook_final(job_id)
            return LOOKBOOK_JOBS.get(job_id, job)
        elif has_pending:
            if job_id not in ACTIVE_LOOKBOOK_WORKERS:
                threading.Thread(target=run_stage2_lookbook_worker, args=(job_id,), daemon=True).start()
            save_lookbook_job(
                job_id,
                stage=2,
                status="STAGE2_RUNNING",
                progress_percent=max(10, int(len(valid_clips) / max(1, len(video_clips)) * 90)),
                message=f"Đang tiếp tục kết xuất {len(valid_clips)}/{len(video_clips)} video...",
                video_clips=video_clips
            )
            return LOOKBOOK_JOBS.get(job_id, job)
        elif changed:
            save_lookbook_job(job_id, video_clips=video_clips)
        return LOOKBOOK_JOBS.get(job_id, job)

    if not scenes:
        return LOOKBOOK_JOBS.get(job_id, job)

    # Check if all scene clips exist on disk
    valid_clips = []
    for i in range(1, num_scenes + 1):
        cp = jdir / f"clip_{i}.mp4"
        if cp.exists() and cp.stat().st_size > 50000:
            valid_clips.append(cp)
            # Ensure scene status is COMPLETED
            for sc in scenes:
                if sc.get("scene_id") == i:
                    sc["status"] = "COMPLETED"
                    sc["status_text"] = "Đã hoàn thành video"
                    sc["video_url"] = f"/api/fashion-lookbook/{job_id}/clip/{i}"

    if len(valid_clips) == num_scenes:
        save_lookbook_job(job_id, scenes=scenes)
        stitch_lookbook_final(job_id)
        return LOOKBOOK_JOBS.get(job_id, job)

    # For scenes still marked as RENDERING_VIDEO or recovering with operation_name
    job_changed = False
    for sc in scenes:
        s_idx = sc.get("scene_id")
        sc_status = sc.get("status")
        worker_key = f"{job_id}_{s_idx}"
        cp = jdir / f"clip_{s_idx}.mp4"

        if cp.exists() and cp.stat().st_size > 50000:
            if sc_status != "COMPLETED":
                sc["status"] = "COMPLETED"
                sc["status_text"] = "Đã hoàn thành video"
                sc["video_url"] = f"/api/fashion-lookbook/{job_id}/clip/{s_idx}"
                job_changed = True
            continue

        if (sc_status == "RENDERING_VIDEO" or (sc_status == "FAILED" and sc.get("operation_name"))):
            # Check if there is an active running thread
            if worker_key not in ACTIVE_LOOKBOOK_WORKERS and job_id not in ACTIVE_LOOKBOOK_WORKERS:
                op_name = sc.get("operation_name")
                op_start = sc.get("operation_start_time", 0)
                elapsed = time.time() - op_start if op_start else 9999

                # Try recovering video via operation_name if recent
                recovered = False
                is_pending = False
                if op_name and elapsed < 720:
                    try:
                        p_body = {"operations": [{"operation": {"name": op_name}}]}
                        p_res = call_flowkit_api("/api/flow/check-status", p_body, timeout=20, max_retries=1)
                        ret_ops = p_res.get("operations") or (p_res.get("data") or {}).get("operations") or []
                        for op_item in ret_ops:
                            st = str(op_item.get("status") or "")
                            meta = (op_item.get("operation") or {}).get("metadata", {})
                            fife = meta.get("video", {}).get("fifeUrl")
                            if st == "MEDIA_GENERATION_STATUS_SUCCESSFUL" or fife:
                                urllib.request.urlretrieve(fife, str(cp))
                                sc["status"] = "COMPLETED"
                                sc["status_text"] = "Đã hoàn thành video"
                                sc["video_url"] = f"/api/fashion-lookbook/{job_id}/clip/{s_idx}"
                                recovered = True
                                job_changed = True
                                break
                            elif "PENDING" in st.upper() or not st:
                                is_pending = True
                                break
                    except Exception as e:
                        print(f"[LOOKBOOK WATCHDOG] Poll error for {op_name}: {e}")

                if is_pending:
                    # Keep rendering and spawn poller if not already active
                    sc["status"] = "RENDERING_VIDEO"
                    sc["status_text"] = "Veo 3.1 đang render chuyển động..."
                    def _resume_poll(jid=job_id, sid=s_idx, opn=op_name, cp_path=cp):
                        wkey = f"{jid}_{sid}"
                        if wkey in ACTIVE_LOOKBOOK_WORKERS:
                            return
                        ACTIVE_LOOKBOOK_WORKERS.add(wkey)
                        try:
                            start_t = time.time()
                            while time.time() - start_t < 600:
                                time.sleep(6)
                                p_body = {"operations": [{"operation": {"name": opn}}]}
                                p_res = call_flowkit_api("/api/flow/check-status", p_body, timeout=40, max_retries=2)
                                ret_ops = p_res.get("operations") or (p_res.get("data") or {}).get("operations") or []
                                for op_item in ret_ops:
                                    st = str(op_item.get("status") or "")
                                    meta = (op_item.get("operation") or {}).get("metadata", {})
                                    fife = meta.get("video", {}).get("fifeUrl")
                                    if st == "MEDIA_GENERATION_STATUS_SUCCESSFUL" or fife:
                                        urllib.request.urlretrieve(fife, str(cp_path))
                                        j = LOOKBOOK_JOBS.get(jid) or {}
                                        for s in j.get("scenes", []):
                                            if s.get("scene_id") == sid:
                                                s["status"] = "COMPLETED"
                                                s["status_text"] = "Đã hoàn thành video"
                                                s["video_url"] = f"/api/fashion-lookbook/{jid}/clip/{sid}"
                                        save_lookbook_job(jid, scenes=j.get("scenes", []))
                                        stitch_lookbook_final(jid)
                                        return
                                    elif "FAIL" in st.upper():
                                        j = LOOKBOOK_JOBS.get(jid) or {}
                                        for s in j.get("scenes", []):
                                            if s.get("scene_id") == sid:
                                                s["status"] = "FAILED"
                                                s["status_text"] = "Lỗi khi render video Veo 3.1"
                                        save_lookbook_job(jid, scenes=j.get("scenes", []))
                                        return
                        except Exception as ex:
                            print(f"[LOOKBOOK RESUME] Error for scene {sid}: {ex}")
                        finally:
                            ACTIVE_LOOKBOOK_WORKERS.discard(wkey)

                    threading.Thread(target=_resume_poll, daemon=True).start()

                elif not recovered:
                    rc = sc.get("retry_count", 0)
                    if rc < 2:
                        sc["retry_count"] = rc + 1
                        sc["status"] = "RENDERING_VIDEO"
                        sc["status_text"] = f"Tự động phục hồi và render lại (lần {rc + 1})..."
                        save_lookbook_job(job_id, scenes=scenes)
                        threading.Thread(target=lambda j=job_id, s=s_idx: regen_lookbook_scene(j, s), daemon=True).start()
                    else:
                        # Cleanly mark as failed only after 3 failed attempts
                        sc["status"] = "FAILED"
                        sc["status_text"] = "Tiến trình không phản hồi sau 3 lần tự động thử lại. Vui lòng bấm vẽ lại phân cảnh."
                        job_changed = True
    valid_now = [jdir / f"clip_{i}.mp4" for i in range(1, num_scenes + 1) if (jdir / f"clip_{i}.mp4").exists() and (jdir / f"clip_{i}.mp4").stat().st_size > 50000]
    if len(valid_now) == num_scenes:
        save_lookbook_job(job_id, scenes=scenes)
        stitch_lookbook_final(job_id)
    elif any(s.get("status") in ["RENDERING_VIDEO", "GENERATING_KEYFRAME"] for s in scenes):
        save_lookbook_job(
            job_id,
            status="RENDERING_VIDEOS",
            progress_percent=55,
            message="Đang kết xuất video chuyển động sải bước & nếp vải (Veo 3.1)...",
            scenes=scenes
        )
    elif all(s.get("status") in ["COMPLETED", "FAILED"] for s in scenes):
        save_lookbook_job(
            job_id,
            status="FAILED",
            message=f"Có {num_scenes - len(valid_now)} cảnh chưa hoàn thành. Vui lòng bấm vẽ lại phân cảnh lỗi.",
            scenes=scenes
        )

    return LOOKBOOK_JOBS.get(job_id, job)


def load_all_lookbook_jobs():
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    for p in WORK_DIR.glob("lookbook_*"):
        if p.is_dir() and (p / "job.json").exists():
            try:
                data = json.loads((p / "job.json").read_text(encoding="utf-8"))
                jid = data.get("job_id")
                if jid:
                    LOOKBOOK_JOBS[jid] = data
                    count += 1
                    recover_lookbook_job(jid)
            except Exception as e:
                print(f"[LOOKBOOK] Error loading {p}: {e}")
    print(f"Loaded and auto-healed {count} Lookbook jobs from {WORK_DIR}")


# Start periodic watchdog daemon
def _lookbook_watchdog_daemon():
    while True:
        try:
            time.sleep(30)
            for jid in list(LOOKBOOK_JOBS.keys()):
                job = LOOKBOOK_JOBS.get(jid) or {}
                if job.get("status") in ["RENDERING_VIDEOS", "GENERATING_KEYFRAMES", "STITCHING", "QUEUED"]:
                    recover_lookbook_job(jid)
        except Exception as e:
            print(f"[LOOKBOOK WATCHDOG] Daemon tick error: {e}")

threading.Thread(target=_lookbook_watchdog_daemon, daemon=True, name="LookbookWatchdog").start()


# ─── PIPELINE WORKER ─────────────────────────────────────────────────────────

def run_lookbook_worker(job_id: str):
    ACTIVE_LOOKBOOK_WORKERS.add(job_id)
    try:
        _run_lookbook_worker_impl(job_id)
    finally:
        ACTIVE_LOOKBOOK_WORKERS.discard(job_id)


def _run_lookbook_worker_impl(job_id: str):
    job = LOOKBOOK_JOBS.get(job_id)
    if not job:
        return

    jdir = WORK_DIR / f"lookbook_{job_id}"
    jdir.mkdir(parents=True, exist_ok=True)
    outfit_img = jdir / "outfit.jpg"
    model_img = jdir / "model.jpg"

    template_id = job.get("template_id", "runway_catwalk")
    tpl = LOOKBOOK_TEMPLATES.get(template_id, LOOKBOOK_TEMPLATES["runway_catwalk"])
    num_scenes = int(job.get("num_scenes") or tpl["default_num_scenes"])
    scene_duration = int(job.get("scene_duration") or tpl["default_duration_per_scene"])
    aspect_ratio = job.get("aspect_ratio", "9:16")
    bgm_id = job.get("bgm_id", "vogue_runway")

    # Step 1: Vision Profiler
    save_lookbook_job(job_id, status="ANALYZING", progress_percent=10, message="Đang phân tích phom dáng trang phục & chất vải...")
    profile = extract_garment_profile(outfit_img, model_img if model_img.exists() else None)
    garment_anchor = profile.get("canonical_visual_anchor", "High-end luxury designer outfit")

    # Model Anchor
    model_preset_id = job.get("model_preset_id", "asian_minimalist_25")
    model_preset = LOOKBOOK_MODELS.get(model_preset_id, LOOKBOOK_MODELS["asian_minimalist_25"])
    model_anchor = model_preset["anchor"]
    if model_img.exists():
        save_lookbook_job(job_id, status="ANALYZING", progress_percent=15, message="Đang phân tích diện mạo và nhân dạng người mẫu...")
        m_profile = extract_model_profile(model_img)
        if m_profile.get("model_visual_anchor"):
            model_anchor = m_profile["model_visual_anchor"]

    save_lookbook_job(
        job_id,
        profile=profile,
        garment_anchor=garment_anchor,
        model_anchor=model_anchor
    )

    # Step 2: Upload Reference Images to FlowKit
    save_lookbook_job(job_id, status="PREPARING_REFS", progress_percent=20, message="Đang nạp ảnh mẫu và trang phục vào cụm GPU...")
    outfit_mid = None
    model_mid = None
    try:
        outfit_mid = upload_image_flowkit(outfit_img)
        if model_img.exists():
            model_mid = upload_image_flowkit(model_img)
        elif model_preset.get("default_file") and model_preset["default_file"].exists():
            model_mid = upload_image_flowkit(model_preset["default_file"])
    except Exception as e:
        print(f"[LOOKBOOK REFS] Error uploading refs: {e}")

    ref_mids = [m for m in [model_mid, outfit_mid] if m]
    save_lookbook_job(
        job_id,
        outfit_media_id=outfit_mid,
        model_media_id=model_mid,
        ref_media_ids=ref_mids
    )

    # Step 3: Prepare Scenes Data
    shot_list = tpl["shot_list"]
    scenes_data = []
    for i in range(num_scenes):
        shot_info = shot_list[i % len(shot_list)]
        scene_idx = i + 1
        kf_prompt = (
            f"CREATOR REFERENCE LOCK — HIGHEST PRIORITY: {model_anchor}.. "
            f"PRODUCT REFERENCE LOCK: {garment_anchor}.. "
            f"{shot_info['pose_desc']} "
            f"Zero text, no watermark, no logo overlay, photorealistic 8k vertical {aspect_ratio}."
        )
        motion_prompt = (
            f"PRODUCT LOCK — HIGHEST PRIORITY. KEEP PRODUCT FORM AND FABRIC TEXTURE CONSISTENT. "
            f"{shot_info['motion_desc']} "
            f"Smooth 24fps cinematic motion. Photorealistic 8k."
        )
        scenes_data.append({
            "scene_id": scene_idx,
            "shot_type": shot_info["shot_type"],
            "description": shot_info["description"],
            "status": "PENDING",
            "status_text": "Đang chờ tạo Keyframe",
            "keyframe_prompt": kf_prompt,
            "motion_prompt": motion_prompt,
            "keyframe_media_id": None,
            "keyframe_url": None,
            "video_url": None
        })

    save_lookbook_job(job_id, scenes=scenes_data)

    # Step 4: Generate Keyframes (Scene 1 Golden Anchor -> Scene 2, 3 Consistency)
    save_lookbook_job(job_id, status="GENERATING_KEYFRAMES", progress_percent=30, message="Đang tạo 3 góc chụp điện ảnh (Keyframes)...")
    
    def _gen_keyframe(sc, active_refs=None):
        s_idx = sc["scene_id"]
        kf_path = jdir / f"keyframe_{s_idx}.jpg"
        sc["status"] = "GENERATING_KEYFRAME"
        sc["status_text"] = "Đang vẽ Keyframe..."
        save_lookbook_job(job_id, scenes=scenes_data)

        current_refs = active_refs if active_refs is not None else ref_mids
        payload = {
            "prompt": sc["keyframe_prompt"],
            "character_media_ids": current_refs,
            "reference_image_media_ids": current_refs,
            "imageInputs": [{"media_id": m} for m in current_refs],
            "aspect_ratio": "IMAGE_ASPECT_RATIO_PORTRAIT" if aspect_ratio == "9:16" else "IMAGE_ASPECT_RATIO_LANDSCAPE",
            "model_family": "banana_pro"
        }
        for attempt in range(1, 4):
            try:
                res = call_flowkit_api("/api/flow/generate-image", payload, timeout=180)
                media = res.get("media") or (res.get("data") or {}).get("media") or {}
                if isinstance(media, list):
                    media = media[0] if media else {}
                mid = media.get("name") or media.get("id") or (media.get("image") or {}).get("generatedImage", {}).get("mediaId")
                img_obj = media.get("image", {})
                gen_img = img_obj.get("generatedImage", {}) if isinstance(img_obj, dict) else {}
                fife = gen_img.get("fifeUrl") or gen_img.get("rawFifeUrl") or (f"{FLOWKIT_API}/api/flow/image/{mid}" if mid else None)
                if mid and fife:
                    urllib.request.urlretrieve(fife, str(kf_path))
                    sc["keyframe_media_id"] = mid
                    sc["keyframe_url"] = f"/api/fashion-lookbook/{job_id}/kf/{s_idx}"
                    sc["status"] = "KEYFRAME_READY"
                    sc["status_text"] = "Keyframe đã hoàn thành"
                    save_lookbook_job(job_id, scenes=scenes_data)
                    return True
            except Exception as e:
                print(f"[LOOKBOOK KF] Scene {s_idx} attempt {attempt} error: {e}")
                time.sleep(3.0)
        
        sc["status"] = "FAILED"
        sc["status_text"] = "Lỗi khi tạo Keyframe"
        save_lookbook_job(job_id, scenes=scenes_data)
        return False

    # Generate Scene 1 first as the Golden Keyframe anchor
    if scenes_data:
        _gen_keyframe(scenes_data[0])

    # If Scene 1 succeeded, use its keyframe as primary anchor for Scene 2, 3...
    kf1_mid = scenes_data[0].get("keyframe_media_id")
    if kf1_mid and len(scenes_data) > 1:
        # Subsequent scenes reference Keyframe 1 as primary reference
        subsequent_refs = [kf1_mid] + [m for m in ref_mids if m != kf1_mid]
        for sc in scenes_data[1:]:
            sc["keyframe_prompt"] = (
                f"EXACT SAME MODEL AND OUTFIT AS PRIMARY REFERENCE IMAGE. "
                f"Maintain identical face shape, identical facial bone structure, identical eyes, nose, lip fullness, identical hairstyle and skin tone, and identical garment details from the primary reference image. "
                f"{sc['keyframe_prompt']}"
            )
        with ThreadPoolExecutor(max_workers=min(3, len(scenes_data) - 1)) as executor:
            list(executor.map(lambda s: _gen_keyframe(s, active_refs=subsequent_refs), scenes_data[1:]))
    elif len(scenes_data) > 1:
        with ThreadPoolExecutor(max_workers=min(3, len(scenes_data) - 1)) as executor:
            list(executor.map(lambda s: _gen_keyframe(s), scenes_data[1:]))

    # Step 5: Generate Videos (Veo 3.1, Zero-Dialogue)
    save_lookbook_job(job_id, status="RENDERING_VIDEOS", progress_percent=55, message="Đang kết xuất video chuyển động sải bước & nếp vải (Veo 3.1)...")
    
    clips = [None] * num_scenes

    def _render_video(sc):
        s_idx = sc["scene_id"]
        worker_key = f"{job_id}_{s_idx}"
        ACTIVE_LOOKBOOK_WORKERS.add(worker_key)
        try:
            kf_path = jdir / f"keyframe_{s_idx}.jpg"
            clip_path = jdir / f"clip_{s_idx}.mp4"

            if not kf_path.exists():
                sc["status"] = "FAILED"
                sc["status_text"] = "Thiếu Keyframe"
                save_lookbook_job(job_id, scenes=scenes_data)
                return

            sc["status"] = "RENDERING_VIDEO"
            sc["status_text"] = "Veo 3.1 đang render chuyển động..."
            save_lookbook_job(job_id, scenes=scenes_data)

            # Upload keyframe to ensure fresh media_id
            start_mid = None
            try:
                start_mid = upload_image_flowkit(kf_path)
            except Exception:
                start_mid = sc.get("keyframe_media_id")

            if not start_mid:
                sc["status"] = "FAILED"
                sc["status_text"] = "Không tìm thấy media ID của keyframe"
                save_lookbook_job(job_id, scenes=scenes_data)
                return

            payload = {
                "start_image_media_id": start_mid,
                "prompt": sc["motion_prompt"],
                "project_id": "",
                "scene_id": f"lookbook_{job_id}_{s_idx}",
                "aspect_ratio": "VIDEO_ASPECT_RATIO_PORTRAIT" if aspect_ratio == "9:16" else "VIDEO_ASPECT_RATIO_LANDSCAPE",
                "model_family": "veo",
                "duration_s": scene_duration
            }

            try:
                res = call_flowkit_api("/api/flow/generate-video", payload, timeout=120)
                ops = res.get("operations") or (res.get("data") or {}).get("operations") or []
                if not ops:
                    raise RuntimeError(f"No operations returned: {res}")
                op_name = (ops[0].get("operation") or {}).get("name") or ops[0].get("name")
                sc["operation_name"] = op_name
                sc["operation_start_time"] = time.time()
                save_lookbook_job(job_id, scenes=scenes_data)

                # Poll until done
                start_t = time.time()
                time.sleep(20)
                while time.time() - start_t < 720:
                    time.sleep(6)
                    p_body = {"operations": [{"operation": {"name": op_name}}]}
                    p_res = call_flowkit_api("/api/flow/check-status", p_body, timeout=40)
                    ret_ops = p_res.get("operations") or (p_res.get("data") or {}).get("operations") or []
                    for op_item in ret_ops:
                        st = op_item.get("status")
                        meta = (op_item.get("operation") or {}).get("metadata", {})
                        fife = meta.get("video", {}).get("fifeUrl")
                        if st == "MEDIA_GENERATION_STATUS_SUCCESSFUL" or fife:
                            urllib.request.urlretrieve(fife, str(clip_path))
                            clips[s_idx - 1] = clip_path
                            sc["status"] = "COMPLETED"
                            sc["status_text"] = "Đã hoàn thành video"
                            sc["video_url"] = f"/api/fashion-lookbook/{job_id}/clip/{s_idx}"
                            save_lookbook_job(job_id, scenes=scenes_data)
                            print(f"[LOOKBOOK VIDEO] Scene {s_idx} completed!")
                            return
                        elif "FAIL" in str(st).upper():
                            rc = sc.get("retry_count", 0)
                            if rc < 2:
                                sc["retry_count"] = rc + 1
                                sc["status"] = "RENDERING_VIDEO"
                                sc["status_text"] = f"Tự động render lại (lần {rc + 1})..."
                                save_lookbook_job(job_id, scenes=scenes_data)
                                time.sleep(4)
                                return _render_video(sc)
                            sc["status"] = "FAILED"
                            sc["status_text"] = "Lỗi khi render video Veo 3.1 sau 3 lần thử"
                            save_lookbook_job(job_id, scenes=scenes_data)
                            return
            except Exception as e:
                print(f"[LOOKBOOK VIDEO] Scene {s_idx} error: {e}")
                rc = sc.get("retry_count", 0)
                if rc < 2:
                    sc["retry_count"] = rc + 1
                    sc["status"] = "RENDERING_VIDEO"
                    sc["status_text"] = f"Tự động kết nối và render lại (lần {rc + 1})..."
                    save_lookbook_job(job_id, scenes=scenes_data)
                    time.sleep(4)
                    return _render_video(sc)
                sc["status"] = "FAILED"
                sc["status_text"] = str(e)[:100]
                save_lookbook_job(job_id, scenes=scenes_data)
        finally:
            ACTIVE_LOOKBOOK_WORKERS.discard(worker_key)

    with ThreadPoolExecutor(max_workers=num_scenes) as executor:
        list(executor.map(_render_video, scenes_data))

    # Step 6: Concat and Add Runway BGM
    stitch_lookbook_final(job_id)


def stitch_lookbook_final(job_id: str) -> bool:
    jdir = WORK_DIR / f"lookbook_{job_id}"
    job = LOOKBOOK_JOBS.get(job_id)
    if not job:
        jfile = jdir / "job.json"
        if jfile.exists():
            try:
                job = json.loads(jfile.read_text(encoding="utf-8"))
                LOOKBOOK_JOBS[job_id] = job
            except Exception:
                pass
    if not job or not jdir.exists():
        return False

    num_scenes = int(job.get("num_scenes", 3))
    scene_duration = int(job.get("scene_duration", 8))
    bgm_id = str(job.get("bgm_id", "vogue_runway"))
    scenes_data = job.get("scenes", [])

    video_clips_meta = job.get("video_clips", [])
    valid_clips = []
    if video_clips_meta:
        for vc in video_clips_meta:
            cid = vc.get("clip_id") or vc.get("source_image_id")
            cp = jdir / f"clip_{cid}.mp4"
            if cp.exists() and cp.stat().st_size > 50000:
                valid_clips.append(cp)
        num_scenes = len(valid_clips)

    if not valid_clips:
        for i in range(1, num_scenes + 1):
            cp = jdir / f"clip_{i}.mp4"
            if cp.exists() and cp.stat().st_size > 50000:
                valid_clips.append(cp)

    final_mp4 = jdir / "final_lookbook.mp4"

    if len(valid_clips) > 0:
        save_lookbook_job(job_id, status="STITCHING", progress_percent=90, message="Đang lồng ghép nhạc Runway và xuất bản video master...")
        try:
            concat_txt = jdir / "scenes.txt"
            with open(concat_txt, "w") as f:
                for c in valid_clips:
                    f.write(f"file '{c.name}'\n")

            temp_concat = jdir / "temp_concat.mp4"
            subprocess.run([
                "ffmpeg", "-y", "-f", "concat", "-safe", "0",
                "-i", str(concat_txt),
                "-c", "copy", str(temp_concat)
            ], check=True)

            # Apply Runway BGM or keep original Veo audio
            bgm_clean = str(bgm_id or "").lower().strip()
            bgm_entry = LOOKBOOK_BGM.get(bgm_clean)
            if not bgm_entry:
                bgm_entry = LOOKBOOK_BGM.get(bgm_id, LOOKBOOK_BGM["vogue_runway"])
            bgm_file = bgm_entry.get("file")

            total_dur = num_scenes * scene_duration
            if bgm_clean in ["none", "original", "veo_audio", "no_music", "off", "mute"] or not bgm_file or not bgm_file.exists():
                temp_concat.replace(final_mp4)
            else:
                has_orig_audio = False
                try:
                    chk = subprocess.run(
                        ["ffprobe", "-v", "error", "-select_streams", "a:0", "-show_entries", "stream=codec_name", "-of", "default=noprint_wrappers=1", str(temp_concat)],
                        capture_output=True, text=True
                    )
                    if "codec_name=" in chk.stdout:
                        has_orig_audio = True
                except Exception:
                    pass

                if has_orig_audio:
                    # Blend natural catwalk/crowd audio (30%) with chosen BGM (80%) so atmosphere is preserved
                    filter_str = "[0:a]volume=0.30[orig_a];[1:a]volume=0.80[bgm_a];[orig_a][bgm_a]amix=inputs=2:duration=first:dropout_transition=2[aout]"
                    subprocess.run([
                        "ffmpeg", "-y",
                        "-i", str(temp_concat),
                        "-stream_loop", "-1",
                        "-i", str(bgm_file),
                        "-filter_complex", filter_str,
                        "-map", "0:v",
                        "-map", "[aout]",
                        "-c:v", "copy",
                        "-c:a", "aac",
                        "-b:a", "192k",
                        "-t", str(total_dur),
                        str(final_mp4)
                    ], check=True)
                else:
                    subprocess.run([
                        "ffmpeg", "-y",
                        "-i", str(temp_concat),
                        "-stream_loop", "-1",
                        "-i", str(bgm_file),
                        "-filter_complex", "[1:a]volume=0.85[bgm_v]",
                        "-map", "0:v",
                        "-map", "[bgm_v]",
                        "-c:v", "copy",
                        "-c:a", "aac",
                        "-b:a", "192k",
                        "-t", str(total_dur),
                        str(final_mp4)
                    ], check=True)
                temp_concat.unlink(missing_ok=True)

            save_lookbook_job(
                job_id,
                status="COMPLETED",
                progress_percent=100,
                message=f"Hoàn thành xuất sắc toàn bộ {num_scenes} phân cảnh Lookbook điện ảnh ({total_dur} giây)!",
                final_video_url=f"/api/fashion-lookbook/{job_id}/final",
                completed_at=time.time(),
                scenes=scenes_data
            )
            return True
        except Exception as e:
            print(f"[LOOKBOOK STITCH] Error: {e}")
            save_lookbook_job(job_id, status="FAILED", message=f"Lỗi ghép video: {e}", scenes=scenes_data)
            return False
    else:
        save_lookbook_job(
            job_id,
            status="FAILED",
            message=f"Có {num_scenes - len(valid_clips)} cảnh chưa hoàn thành. Vui lòng bấm vẽ lại phân cảnh lỗi.",
            scenes=scenes_data
        )
        return False


def create_lookbook_job(
    outfit_bytes: Optional[bytes] = None,
    outfit_url: str = "",
    model_bytes: Optional[bytes] = None,
    model_url: str = "",
    model_preset_id: str = "asian_minimalist_25",
    template_id: str = "runway_catwalk",
    aspect_ratio: str = "9:16",
    num_scenes: int = 3,
    scene_duration: int = 8,
    bgm_id: str = "vogue_runway"
) -> str:
    job_id = f"fsh_{uuid.uuid4().hex[:8]}"
    jdir = WORK_DIR / f"lookbook_{job_id}"
    jdir.mkdir(parents=True, exist_ok=True)

    outfit_img = jdir / "outfit.jpg"
    if outfit_bytes:
        outfit_img.write_bytes(outfit_bytes)
    elif outfit_url:
        try:
            urllib.request.urlretrieve(outfit_url, str(outfit_img))
        except Exception as e:
            print(f"[LOOKBOOK] Error fetching outfit url: {e}")

    model_img = jdir / "model.jpg"
    if model_bytes:
        model_img.write_bytes(model_bytes)
    elif model_url:
        try:
            urllib.request.urlretrieve(model_url, str(model_img))
        except Exception as e:
            print(f"[LOOKBOOK] Error fetching model url: {e}")

    save_lookbook_job(
        job_id,
        status="QUEUED",
        progress_percent=0,
        message="Đang xếp hàng khởi tạo pipeline Lookbook...",
        template_id=template_id,
        model_preset_id=model_preset_id,
        aspect_ratio=aspect_ratio,
        num_scenes=num_scenes,
        scene_duration=scene_duration,
        bgm_id=bgm_id,
        created_at=time.time(),
        scenes=[]
    )

    t = threading.Thread(target=run_lookbook_worker, args=(job_id,), daemon=True)
    t.start()
    return job_id


def regen_lookbook_scene(job_id: str, scene_id: int, camera_motion_override: str = ""):
    job = LOOKBOOK_JOBS.get(job_id)
    if not job:
        return {"ok": False, "error": f"Job {job_id} not found"}

    scenes = job.get("scenes", [])
    target_sc = next((sc for sc in scenes if sc.get("scene_id") == scene_id), None)
    if not target_sc:
        return {"ok": False, "error": f"Scene {scene_id} not found"}

    if camera_motion_override:
        target_sc["motion_prompt"] = (
            f"PRODUCT LOCK — HIGHEST PRIORITY. KEEP PRODUCT FORM AND FABRIC TEXTURE CONSISTENT. "
            f"{camera_motion_override.strip()} Smooth 24fps cinematic motion. Photorealistic 8k."
        )

    target_sc["status"] = "RENDERING_VIDEO"
    target_sc["status_text"] = "Đang tái tạo phân cảnh..."
    save_lookbook_job(job_id, scenes=scenes)

    # Launch single worker
    def _do_regen():
        worker_key = f"{job_id}_{scene_id}"
        ACTIVE_LOOKBOOK_WORKERS.add(worker_key)
        try:
            jdir = WORK_DIR / f"lookbook_{job_id}"
            kf_path = jdir / f"keyframe_{scene_id}.jpg"
            clip_path = jdir / f"clip_{scene_id}.mp4"
            if not kf_path.exists():
                return
            try:
                start_mid = upload_image_flowkit(kf_path)
                payload = {
                    "start_image_media_id": start_mid,
                    "prompt": target_sc["motion_prompt"],
                    "project_id": "",
                    "scene_id": f"lookbook_{job_id}_{scene_id}_regen",
                    "aspect_ratio": "VIDEO_ASPECT_RATIO_PORTRAIT",
                    "model_family": "veo",
                    "duration_s": job.get("scene_duration", 8)
                }
                res = call_flowkit_api("/api/flow/generate-video", payload, timeout=120)
                ops = res.get("operations") or (res.get("data") or {}).get("operations") or []
                if not ops:
                    return
                op_name = (ops[0].get("operation") or {}).get("name") or ops[0].get("name")
                target_sc["operation_name"] = op_name
                target_sc["operation_start_time"] = time.time()
                save_lookbook_job(job_id, scenes=scenes)

                start_t = time.time()
                time.sleep(20)
                while time.time() - start_t < 720:
                    time.sleep(6)
                    p_body = {"operations": [{"operation": {"name": op_name}}]}
                    p_res = call_flowkit_api("/api/flow/check-status", p_body, timeout=40)
                    ret_ops = p_res.get("operations") or (p_res.get("data") or {}).get("operations") or []
                    for op_item in ret_ops:
                        st = op_item.get("status")
                        meta = (op_item.get("operation") or {}).get("metadata", {})
                        fife = meta.get("video", {}).get("fifeUrl")
                        if st == "MEDIA_GENERATION_STATUS_SUCCESSFUL" or fife:
                            urllib.request.urlretrieve(fife, str(clip_path))
                            target_sc["status"] = "COMPLETED"
                            target_sc["status_text"] = "Đã hoàn thành video"
                            target_sc["video_url"] = f"/api/fashion-lookbook/{job_id}/clip/{scene_id}"
                            save_lookbook_job(job_id, scenes=scenes)
                            # Re-stitch master if all exist
                            stitch_lookbook_final(job_id)
                            return
                        elif "FAIL" in str(st).upper():
                            target_sc["status"] = "FAILED"
                            target_sc["status_text"] = "Lỗi khi render video Veo 3.1"
                            save_lookbook_job(job_id, scenes=scenes)
                            return
            except Exception as e:
                print(f"[LOOKBOOK REGEN] Error: {e}")
                target_sc["status"] = "FAILED"
                target_sc["status_text"] = str(e)[:100]
                save_lookbook_job(job_id, scenes=scenes)
        finally:
            ACTIVE_LOOKBOOK_WORKERS.discard(worker_key)

    threading.Thread(target=_do_regen, daemon=True).start()
    return {"ok": True, "job_id": job_id, "scene_id": scene_id, "status": "RENDERING_VIDEO"}


# ─── MASTER FLOW: STAGE 1 (MULTI-PRODUCT LOOKBOOK IMAGE GENERATION) ──────────

def create_stage1_zip(job_id: str) -> Optional[Path]:
    """Archive all generated lookbook images for download."""
    jdir = WORK_DIR / f"lookbook_{job_id}"
    zip_path = jdir / "lookbook_images.zip"
    images = list(jdir.glob("image_*.jpg"))
    if not images:
        return None
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for img in sorted(images, key=lambda x: int(re.findall(r"\d+", x.name)[0]) if re.findall(r"\d+", x.name) else 0):
            zf.write(img, arcname=img.name)
    return zip_path


def create_stage1_lookbook_job(
    product_files: list[tuple[str, bytes]] = None,
    product_urls: list[str] = None,
    model_file: Optional[tuple[str, bytes]] = None,
    model_url: str = "",
    model_preset_id: str = "asian_minimalist_25",
    background_file: Optional[tuple[str, bytes]] = None,
    background_url: str = "",
    background_preset: str = "pure white studio",
    aspect_ratio: str = "3:4",
    quality: str = "4K",
    quantity: int = 4,
    style_preset: str = "Luxury boutique fashion",
    lighting_preset: str = "soft studio lighting",
    model_ai: str = "google/nano-banana-pro",
    selected_poses: Optional[list[str]] = None,
    num_threads: Optional[int] = None
) -> str:
    """Create a new Stage 1 Master Flow Lookbook job (1-9 products, 15 poses gallery)."""
    job_id = uuid.uuid4().hex[:12]
    jdir = WORK_DIR / f"lookbook_{job_id}"
    jdir.mkdir(parents=True, exist_ok=True)

    # 1. Save product images (up to 9 products)
    saved_product_paths = []
    prod_idx = 1
    if product_files:
        for fname, fbytes in product_files[:9]:
            if fbytes and len(fbytes) > 100:
                p_path = jdir / f"product_{prod_idx}.jpg"
                p_path.write_bytes(fbytes)
                saved_product_paths.append(p_path)
                prod_idx += 1

    if product_urls:
        for purl in product_urls[:(9 - len(saved_product_paths))]:
            if purl and purl.strip():
                try:
                    p_path = jdir / f"product_{prod_idx}.jpg"
                    urllib.request.urlretrieve(purl.strip(), str(p_path))
                    if p_path.exists() and p_path.stat().st_size > 100:
                        saved_product_paths.append(p_path)
                        prod_idx += 1
                except Exception as e:
                    print(f"[LOOKBOOK STAGE1] Error fetching product url {purl}: {e}")

    # Fallback if no products provided
    if not saved_product_paths:
        raise ValueError("Vui lòng cung cấp ít nhất 1 ảnh sản phẩm thời trang!")

    # 2. Save model image (optional)
    saved_model_path = None
    if model_file and model_file[1] and len(model_file[1]) > 100:
        m_path = jdir / "model.jpg"
        m_path.write_bytes(model_file[1])
        saved_model_path = m_path
    elif model_url and model_url.strip():
        try:
            m_path = jdir / "model.jpg"
            urllib.request.urlretrieve(model_url.strip(), str(m_path))
            if m_path.exists() and m_path.stat().st_size > 100:
                saved_model_path = m_path
        except Exception as e:
            print(f"[LOOKBOOK STAGE1] Error fetching model url: {e}")

    # 3. Save background image (optional)
    saved_bg_path = None
    if background_file and background_file[1] and len(background_file[1]) > 100:
        bg_path = jdir / "background.jpg"
        bg_path.write_bytes(background_file[1])
        saved_bg_path = bg_path
    elif background_url and background_url.strip():
        try:
            bg_path = jdir / "background.jpg"
            urllib.request.urlretrieve(background_url.strip(), str(bg_path))
            if bg_path.exists() and bg_path.stat().st_size > 100:
                saved_bg_path = bg_path
        except Exception as e:
            print(f"[LOOKBOOK STAGE1] Error fetching background url: {e}")

    # 4. Resolve Poses
    valid_poses = [p for p in (selected_poses or []) if p in POSES]
    if not valid_poses:
        valid_poses = select_default_poses(quantity)
    else:
        valid_poses = valid_poses[:quantity]

    # 5. Initialize Job Structure
    images_data = []
    for idx, pose in enumerate(valid_poses):
        images_data.append({
            "image_id": idx + 1,
            "pose": pose,
            "status": "PENDING",
            "status_text": "Đang chờ sinh ảnh...",
            "prompt": "",
            "media_id": None,
            "image_url": None,
            "created_at": time.time()
        })

    effective_threads = int(num_threads) if num_threads else LOOKBOOK_CONFIG["num_threads"]
    job = {
        "job_id": job_id,
        "stage": 1,
        "status": "QUEUED",
        "progress_percent": 5,
        "message": "Đang khởi tạo phiên làm việc Lookbook thời trang AI...",
        "created_at": time.time(),
        "updated_at": time.time(),
        "aspect_ratio": aspect_ratio,
        "quality": quality,
        "quantity": len(valid_poses),
        "num_threads": effective_threads,
        "style_preset": style_preset,
        "lighting_preset": lighting_preset,
        "background_preset": background_preset,
        "has_background_ref": bool(saved_bg_path),
        "model_preset_id": model_preset_id,
        "model_ai": model_ai,
        "poses": valid_poses,
        "images": images_data,
        "num_products": len(saved_product_paths),
        "video_clips": []
    }

    job_payload = dict(job)
    job_payload.pop("job_id", None)
    save_lookbook_job(job_id, **job_payload)

    # Launch background worker
    threading.Thread(target=run_stage1_lookbook_worker, args=(job_id,), daemon=True).start()
    return job_id


def run_stage1_lookbook_worker(job_id: str):
    """Execute Stage 1: Upload references (1-9 products + model + bg) and render Lookbook Gallery with concurrency."""
    ACTIVE_LOOKBOOK_WORKERS.add(job_id)
    try:
        jdir = WORK_DIR / f"lookbook_{job_id}"
        job = LOOKBOOK_JOBS.get(job_id) or {}
        if not job:
            return

        aspect_ratio = job.get("aspect_ratio", "3:4")
        quality = job.get("quality", "4K")
        style_preset = job.get("style_preset", "Luxury boutique fashion")
        lighting_preset = job.get("lighting_preset", "soft studio lighting")
        background_preset = job.get("background_preset", "pure white studio")
        has_bg_ref = bool(job.get("has_background_ref"))
        model_preset_id = job.get("model_preset_id", "asian_minimalist_25")
        images_data = job.get("images", [])
        job_threads = int(job.get("num_threads") or LOOKBOOK_CONFIG["num_threads"])

        # Step 1: Upload references
        save_lookbook_job(job_id, status="PREPARING_REFS", progress_percent=15, message="Đang nạp ảnh sản phẩm và người mẫu vào GPU...")
        
        # Product references
        prod_mids = []
        for p_file in sorted(jdir.glob("product_*.jpg")):
            try:
                mid = upload_image_flowkit(p_file)
                if mid:
                    prod_mids.append(mid)
            except Exception as e:
                print(f"[LOOKBOOK STAGE1] Error uploading {p_file.name}: {e}")

        # Model reference
        model_mid = None
        model_path = jdir / "model.jpg"
        if model_path.exists():
            try:
                model_mid = upload_image_flowkit(model_path)
            except Exception as e:
                print(f"[LOOKBOOK STAGE1] Error uploading model: {e}")
        else:
            model_preset = LOOKBOOK_MODELS.get(model_preset_id, LOOKBOOK_MODELS["asian_minimalist_25"])
            if model_preset.get("default_file") and Path(model_preset["default_file"]).exists():
                try:
                    model_mid = upload_image_flowkit(Path(model_preset["default_file"]))
                except Exception as e:
                    print(f"[LOOKBOOK STAGE1] Error uploading preset model: {e}")

        # Background reference
        bg_mid = None
        bg_path = jdir / "background.jpg"
        if bg_path.exists():
            try:
                bg_mid = upload_image_flowkit(bg_path)
            except Exception as e:
                print(f"[LOOKBOOK STAGE1] Error uploading background: {e}")

        # Compose reference list: [model] + [products] + [background]
        all_refs = []
        if model_mid:
            all_refs.append(model_mid)
        all_refs.extend(prod_mids)
        if bg_mid:
            all_refs.append(bg_mid)

        save_lookbook_job(
            job_id,
            ref_media_ids=all_refs,
            product_media_ids=prod_mids,
            model_media_id=model_mid,
            background_media_id=bg_mid
        )

        # Step 2: Generate Lookbook Images
        save_lookbook_job(job_id, status="GENERATING_IMAGES", progress_percent=30, message="Đang sinh bộ sưu tập ảnh Lookbook chuẩn thời trang...")

        flow_ar = "IMAGE_ASPECT_RATIO_PORTRAIT" if aspect_ratio in ["3:4", "9:16"] else "IMAGE_ASPECT_RATIO_LANDSCAPE"

        def _render_image_item(img_item, current_refs, golden_anchor=None):
            img_id = img_item["image_id"]
            pose = img_item["pose"]
            img_path = jdir / f"image_{img_id}.jpg"

            if img_path.exists() and img_path.stat().st_size > 10000 and img_item.get("status") == "COMPLETED":
                return img_item.get("media_id")

            img_item["status"] = "GENERATING"
            img_item["status_text"] = f"Đang vẽ dáng {pose}..."
            save_lookbook_job(job_id, images=images_data)

            prompt = build_image_prompt(
                pose=pose,
                aspect_ratio=aspect_ratio,
                quality=quality,
                style=style_preset,
                lighting=lighting_preset,
                background_preset=background_preset,
                has_background_ref=bool(bg_mid)
            )

            refs_for_image = list(current_refs)
            if golden_anchor and img_id > 1:
                prompt = (
                    f"EXACT SAME MODEL AND OUTFIT AS PRIMARY REFERENCE IMAGE. "
                    f"Keep face shape, hairstyle, skin tone and exact garment construction consistent. "
                    f"{prompt}"
                )
                refs_for_image = [golden_anchor] + [m for m in current_refs if m != golden_anchor]

            img_item["prompt"] = prompt

            payload = {
                "prompt": prompt,
                "character_media_ids": refs_for_image,
                "reference_image_media_ids": refs_for_image,
                "imageInputs": [{"media_id": m} for m in refs_for_image],
                "aspect_ratio": flow_ar,
                "model_family": "banana_pro"
            }

            for attempt in range(1, 4):
                try:
                    res = call_flowkit_api("/api/flow/generate-image", payload, timeout=180)
                    media = res.get("media") or (res.get("data") or {}).get("media") or {}
                    if isinstance(media, list):
                        media = media[0] if media else {}
                    mid = res.get("media_id") or media.get("name") or media.get("id")
                    img_obj = media.get("image", {})
                    gen_img = img_obj.get("generatedImage", {}) if isinstance(img_obj, dict) else {}
                    fife = gen_img.get("fifeUrl") or gen_img.get("rawFifeUrl") or (f"{FLOWKIT_API}/api/flow/image/{mid}" if mid else None)

                    if mid and fife:
                        urllib.request.urlretrieve(fife, str(img_path))
                        img_item["media_id"] = mid
                        img_item["image_url"] = f"/api/fashion-lookbook/stage1/images/{job_id}/{img_id}"
                        img_item["status"] = "COMPLETED"
                        img_item["status_text"] = "Đã hoàn thành ảnh"
                        save_lookbook_job(job_id, images=images_data)
                        return mid
                except Exception as e:
                    print(f"[LOOKBOOK STAGE1] Attempt {attempt} failed for image {img_id}: {e}")
                    time.sleep(3)

            img_item["status"] = "FAILED"
            img_item["status_text"] = "Lỗi khi sinh ảnh"
            save_lookbook_job(job_id, images=images_data)
            return None

        golden_mid = None
        if images_data:
            # First image generated as Golden Anchor
            golden_mid = _render_image_item(images_data[0], all_refs)

        if len(images_data) > 1:
            max_workers = max(1, min(10, job_threads, len(images_data) - 1))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                list(executor.map(lambda item: _render_image_item(item, all_refs, golden_mid), images_data[1:]))

        # Step 3: Bundle ZIP
        create_stage1_zip(job_id)

        completed_count = sum(1 for img in images_data if img.get("status") == "COMPLETED")
        save_lookbook_job(
            job_id,
            status="STAGE1_COMPLETED",
            stage=1,
            progress_percent=100,
            message=f"Đã hoàn thành {completed_count}/{len(images_data)} ảnh Lookbook thời trang!",
            zip_url=f"/api/fashion-lookbook/stage1/zip/{job_id}",
            images=images_data
        )

    finally:
        ACTIVE_LOOKBOOK_WORKERS.discard(job_id)


# ─── MASTER FLOW: STAGE 2 (I2V FASHION VIDEO GENERATION) ─────────────────────

def create_stage2_lookbook_job(
    job_id: str,
    selected_image_ids: list[int],
    motion_preset: str = "Elegant Turnaround",
    camera_movement: str = "Cinematic Push-in",
    duration: int = 8,
    video_model: str = "Omni Flash",
    is_lite_mode: bool = False,
    num_threads: Optional[int] = None
) -> dict:
    """Create a Stage 2 Video Generation request for selected lookbook images with concurrency."""
    job = LOOKBOOK_JOBS.get(job_id)
    if not job:
        jdir = WORK_DIR / f"lookbook_{job_id}"
        jfile = jdir / "job.json"
        if jfile.exists():
            try:
                job = json.loads(jfile.read_text(encoding="utf-8"))
                LOOKBOOK_JOBS[job_id] = job
            except Exception:
                pass
    if not job:
        raise ValueError(f"Không tìm thấy phiên làm việc {job_id}")

    images_map = {img["image_id"]: img for img in job.get("images", [])}
    valid_ids = [iid for iid in selected_image_ids if iid in images_map]
    if not valid_ids:
        raise ValueError("Vui lòng chọn ít nhất 1 ảnh hợp lệ từ Bộ sưu tập Lookbook để tạo video!")

    video_clips = []
    for iid in valid_ids:
        img_info = images_map[iid]
        video_clips.append({
            "clip_id": iid,
            "source_image_id": iid,
            "pose": img_info.get("pose", ""),
            "motion_preset": motion_preset,
            "camera_movement": camera_movement,
            "duration": duration,
            "video_model": video_model,
            "is_lite_mode": is_lite_mode,
            "status": "PENDING",
            "status_text": "Đang chờ render video...",
            "video_url": None,
            "operation_name": None,
            "created_at": time.time()
        })

    effective_threads = int(num_threads) if num_threads else LOOKBOOK_CONFIG["num_threads"]
    save_lookbook_job(
        job_id,
        stage=2,
        status="STAGE2_RUNNING",
        progress_percent=10,
        num_threads=effective_threads,
        message=f"Bắt đầu dựng {len(video_clips)} video thời trang chuyển động ({effective_threads} luồng)...",
        video_clips=video_clips
    )

    threading.Thread(target=run_stage2_lookbook_worker, args=(job_id,), daemon=True).start()
    return LOOKBOOK_JOBS[job_id]


def run_stage2_lookbook_worker(job_id: str):
    """Execute Stage 2: Render video motion clips concurrently using ThreadPoolExecutor."""
    ACTIVE_LOOKBOOK_WORKERS.add(job_id)
    try:
        jdir = WORK_DIR / f"lookbook_{job_id}"
        job = LOOKBOOK_JOBS.get(job_id) or {}
        if not job:
            return

        video_clips = job.get("video_clips", [])
        aspect_ratio = job.get("aspect_ratio", "3:4")
        video_ar = "VIDEO_ASPECT_RATIO_PORTRAIT" if aspect_ratio in ["3:4", "9:16"] else "VIDEO_ASPECT_RATIO_LANDSCAPE"
        job_threads = int(job.get("num_threads") or LOOKBOOK_CONFIG["num_threads"])

        # 1. Immediate check: Mark existing valid clips as COMPLETED
        for clip in video_clips:
            cid = clip["clip_id"]
            clip_path = jdir / f"clip_{cid}.mp4"
            if clip_path.exists() and clip_path.stat().st_size > 50000:
                clip["status"] = "COMPLETED"
                clip["status_text"] = "Đã hoàn thành video"
                clip["video_url"] = f"/api/fashion-lookbook/stage2/clips/{job_id}/{cid}"

        save_lookbook_job(job_id, video_clips=video_clips)

        # 2. Filter clips that still need rendering
        pending_clips = [c for c in video_clips if c.get("status") != "COMPLETED"]
        if not pending_clips:
            save_lookbook_job(
                job_id,
                status="STAGE2_COMPLETED",
                stage=2,
                progress_percent=100,
                message=f"Đã hoàn thành toàn bộ {len(video_clips)} video thời trang!",
                video_clips=video_clips
            )
            stitch_lookbook_final(job_id)
            return

        max_workers = max(1, min(10, job_threads, len(pending_clips)))

        def _render_single_clip(item_and_stagger):
            idx, clip = item_and_stagger
            stagger_s = idx * 2.0
            if stagger_s > 0:
                time.sleep(stagger_s)

            cid = clip["clip_id"]
            img_path = jdir / f"image_{cid}.jpg"
            clip_path = jdir / f"clip_{cid}.mp4"

            if clip_path.exists() and clip_path.stat().st_size > 50000:
                clip["status"] = "COMPLETED"
                clip["status_text"] = "Đã hoàn thành video"
                clip["video_url"] = f"/api/fashion-lookbook/stage2/clips/{job_id}/{cid}"
                save_lookbook_job(job_id, video_clips=video_clips)
                return

            if not img_path.exists():
                clip["status"] = "FAILED"
                clip["status_text"] = f"Không tìm thấy file ảnh gốc image_{cid}.jpg"
                save_lookbook_job(job_id, video_clips=video_clips)
                return

            clip["status"] = "RENDERING_VIDEO"
            clip["status_text"] = "Đang kết xuất chuyển động video (Veo 3.1 / Omni Flash)..."
            save_lookbook_job(job_id, video_clips=video_clips)

            start_mid = None
            try:
                start_mid = upload_image_flowkit(img_path)
            except Exception:
                start_mid = clip.get("start_media_id")

            if not start_mid:
                clip["status"] = "FAILED"
                clip["status_text"] = "Lỗi nạp ảnh khung hình gốc vào GPU"
                save_lookbook_job(job_id, video_clips=video_clips)
                return

            video_prompt = build_video_prompt(
                motion_preset=clip.get("motion_preset", "Elegant Turnaround"),
                camera_movement=clip.get("camera_movement", "Cinematic Push-in"),
                duration=int(clip.get("duration", 8))
            )

            payload = {
                "start_image_media_id": start_mid,
                "prompt": video_prompt,
                "project_id": "",
                "scene_id": f"lookbook_{job_id}_clip_{cid}",
                "aspect_ratio": video_ar,
                "model_family": "veo",
                "duration_s": int(clip.get("duration", 8))
            }

            try:
                res = call_flowkit_api("/api/flow/generate-video", payload, timeout=120)
                ops = res.get("operations") or (res.get("data") or {}).get("operations") or []
                if not ops:
                    raise RuntimeError(f"No operations returned: {res}")
                op_name = (ops[0].get("operation") or {}).get("name") or ops[0].get("name")
                clip["operation_name"] = op_name
                save_lookbook_job(job_id, video_clips=video_clips)

                # Poll until video is ready
                start_t = time.time()
                time.sleep(15)
                video_saved = False
                while time.time() - start_t < 720:
                    time.sleep(6)
                    p_body = {"operations": [{"operation": {"name": op_name}}]}
                    p_res = call_flowkit_api("/api/flow/check-status", p_body, timeout=40)
                    ret_ops = p_res.get("operations") or (p_res.get("data") or {}).get("operations") or []
                    for op_item in ret_ops:
                        st = op_item.get("status")
                        meta = (op_item.get("operation") or {}).get("metadata", {})
                        fife = meta.get("video", {}).get("fifeUrl")
                        if st == "MEDIA_GENERATION_STATUS_SUCCESSFUL" or fife:
                            urllib.request.urlretrieve(fife, str(clip_path))
                            clip["status"] = "COMPLETED"
                            clip["status_text"] = "Đã hoàn thành video"
                            clip["video_url"] = f"/api/fashion-lookbook/stage2/clips/{job_id}/{cid}"
                            save_lookbook_job(job_id, video_clips=video_clips)
                            video_saved = True
                            break
                        elif "FAIL" in str(st).upper():
                            clip["status"] = "FAILED"
                            clip["status_text"] = "Lỗi khi render video Veo 3.1"
                            save_lookbook_job(job_id, video_clips=video_clips)
                            video_saved = True
                            break
                    if video_saved:
                        break

            except Exception as e:
                print(f"[LOOKBOOK STAGE2] Error rendering clip {cid}: {e}")
                clip["status"] = "FAILED"
                clip["status_text"] = str(e)[:100]
                save_lookbook_job(job_id, video_clips=video_clips)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(executor.map(_render_single_clip, enumerate(pending_clips)))

        completed_clips = sum(1 for c in video_clips if c.get("status") == "COMPLETED")
        if completed_clips == len(video_clips):
            save_lookbook_job(
                job_id,
                status="STAGE2_COMPLETED",
                stage=2,
                progress_percent=100,
                message=f"Đã hoàn thành toàn bộ {len(video_clips)} video thời trang!",
                video_clips=video_clips
            )
            stitch_lookbook_final(job_id)
        else:
            save_lookbook_job(
                job_id,
                status="STAGE2_PARTIAL",
                stage=2,
                progress_percent=int(completed_clips / len(video_clips) * 100),
                message=f"Đã hoàn thành {completed_clips}/{len(video_clips)} video thời trang.",
                video_clips=video_clips
            )

    finally:
        ACTIVE_LOOKBOOK_WORKERS.discard(job_id)


def stitch_lookbook_master(job_id: str, bgm_id: str = "vogue_runway") -> dict:
    """Stitch all completed video clips of a job into a master runway video with BGM."""
    job = LOOKBOOK_JOBS.get(job_id) or {}
    job["bgm_id"] = bgm_id
    save_lookbook_job(job_id, bgm_id=bgm_id)
    success = stitch_lookbook_final(job_id)
    return {
        "ok": success,
        "job_id": job_id,
        "master_url": f"/api/fashion-lookbook/{job_id}/master" if success else None
    }
