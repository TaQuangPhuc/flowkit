import os
import sys
import json
import time
import shutil
import uuid
import re
import base64
import urllib.request
import subprocess
import threading
import concurrent.futures
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
import batch_image_studio as bis
import fashion_lookbook_studio as fls
from agent.config import VIDEO_POLL_TIMEOUT

FLOWKIT_API = "http://127.0.0.1:8100"
# Leave time for FlowKit's bounded final lookup and the HTTP round trip.
FLOWKIT_VIDEO_WAIT_SECONDS = VIDEO_POLL_TIMEOUT + 60
WORK_DIR = Path("/home/pc/flowkit/auto_runs")
WORK_DIR.mkdir(parents=True, exist_ok=True)
BGM_DIR = Path("/home/pc/flowkit/assets/bgm")
BGM_DIR.mkdir(parents=True, exist_ok=True)

# Global in-memory job state: job_id -> dict
JOBS = {}

def is_safe_image_url(url: str) -> bool:
    """Validate image URL against SSRF (reject private IPs, loopback, non-http(s) schemes)."""
    if not url or not isinstance(url, str):
        return False
    try:
        import urllib.parse
        import ipaddress
        import socket

        parsed = urllib.parse.urlparse(url.strip())
        if parsed.scheme.lower() not in ["http", "https"]:
            return False

        hostname = (parsed.hostname or "").lower()
        if not hostname:
            return False

        # Blocked prefixes and hostnames
        blocked_prefixes = [
            "127.", "10.", "192.168.", "0.0.0.0", "localhost"
        ] + [f"172.{i}." for i in range(16, 32)]

        for prefix in blocked_prefixes:
            if hostname == prefix or hostname.startswith(prefix):
                return False

        # Check if direct IP address
        try:
            ip = ipaddress.ip_address(hostname)
            if ip.is_private or ip.is_loopback or ip.is_reserved or ip.is_link_local:
                return False
        except ValueError:
            # Check resolved IP address to prevent DNS rebinding / localhost aliases
            try:
                addr_info = socket.getaddrinfo(hostname, None)
                for item in addr_info:
                    resolved_ip_str = item[4][0]
                    resolved_ip = ipaddress.ip_address(resolved_ip_str)
                    if resolved_ip.is_private or resolved_ip.is_loopback or resolved_ip.is_reserved or resolved_ip.is_link_local:
                        return False
            except Exception:
                return False

        return True
    except Exception:
        return False

def update_job(job_id: str, **kwargs):
    if job_id in JOBS:
        JOBS[job_id].update(kwargs)
        JOBS[job_id]["updated_at"] = time.time()
        try:
            (WORK_DIR / job_id / "job.json").write_text(json.dumps(JOBS[job_id], indent=2, ensure_ascii=False))
        except Exception:
            pass

def extract_json_from_text(stdout: str) -> any:
    """Extract JSON object or array from LLM text output using multiple robust strategies + json_repair."""
    try:
        import json_repair
    except ImportError:
        json_repair = None

    # 1. Try markdown code block
    match = re.search(r"```(?:json)?\s*([\[\{].*?[\]\}])\s*```", stdout, re.DOTALL)
    if match:
        chunk = match.group(1).strip()
        try:
            return json.loads(chunk, strict=False)
        except Exception:
            if json_repair:
                try:
                    return json_repair.loads(chunk)
                except Exception:
                    pass

    # 2. Try slicing outermost brackets/braces
    first_bracket = stdout.find("[")
    first_brace = stdout.find("{")

    candidates = []
    if first_bracket != -1 and (first_brace == -1 or first_bracket < first_brace):
        last_bracket = stdout.rfind("]")
        if last_bracket > first_bracket:
            candidates.append(stdout[first_bracket:last_bracket+1])
    if first_brace != -1:
        last_brace = stdout.rfind("}")
        if last_brace > first_brace:
            candidates.append(stdout[first_brace:last_brace+1])
    if first_bracket != -1 and first_bracket > first_brace:
        last_bracket = stdout.rfind("]")
        if last_bracket > first_bracket:
            candidates.append(stdout[first_bracket:last_bracket+1])

    for cand in candidates:
        try:
            return json.loads(cand, strict=False)
        except Exception:
            if json_repair:
                try:
                    return json_repair.loads(cand)
                except Exception:
                    pass

    # 3. Direct decode with strict=False or json_repair
    try:
        return json.loads(stdout.strip(), strict=False)
    except Exception:
        if json_repair:
            return json_repair.loads(stdout.strip())
        raise

def run_agy_cli_json(prompt: str, timeout: int = 180) -> any:
    """Run Antigravity CLI (agy) in print mode as primary AI fallback."""
    agy_bin = shutil.which("agy") or "/home/pc/.local/bin/agy"
    cmd = [
        agy_bin, "-p", prompt,
        "--output-format", "text"
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    stdout = proc.stdout
    if not stdout and proc.stderr:
        print(f"[AGY FALLBACK STDERR]: {proc.stderr[:300]}")
    try:
        return extract_json_from_text(stdout)
    except Exception as e:
        raise ValueError(f"agy CLI failed to parse JSON ({e}): {stdout[:300]}")

def run_claude_cli_json(prompt: str) -> any:
    """Fallback CLI orchestrator: calls agy first, then claude if agy fails."""
    try:
        print("[LLM FALLBACK] Attempting agy CLI...")
        return run_agy_cli_json(prompt)
    except Exception as e_agy:
        print(f"[LLM FALLBACK] agy CLI failed: {e_agy}. Trying claude CLI...")
        claude_bin = shutil.which("claude") or "/home/pc/.local/bin/claude"
        cmd = [
            claude_bin, "-p", prompt,
            "--output-format", "text"
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        stdout = proc.stdout
        try:
            return extract_json_from_text(stdout)
        except Exception as e:
            raise ValueError(f"All CLI fallbacks (agy & claude) failed ({e}): {stdout[:300]}")

NOVA_BASE_URL = os.environ.get("NOVA_BASE_URL", "https://api.vilao.ai/v1")
NOVA_API_KEY = os.environ.get("NOVA_API_KEY", "sk-72afd079199f58a7b302e65b6690744ce8cf7b44c0dcd163070052e7fa774535")
NOVA_MODEL = os.environ.get("NOVA_MODEL", "chib/deepseek-v4.1-flash")

_DEFAULT_FALLBACKS = ["spd/grok-4.6", "grok-4.6", "cnt/grok-4.6", "fa/grok-4.6-fast"]
_env_fallbacks = os.environ.get("NOVA_FALLBACK_MODELS", "")
if _env_fallbacks:
    NOVA_FALLBACK_MODELS = [m.strip() for m in _env_fallbacks.split(",") if m.strip()]
else:
    NOVA_FALLBACK_MODELS = _DEFAULT_FALLBACKS

def get_candidate_models() -> list[str]:
    primary = os.environ.get("NOVA_MODEL", NOVA_MODEL)
    candidates = [primary]
    for fb in NOVA_FALLBACK_MODELS:
        if fb not in candidates:
            candidates.append(fb)
    return candidates

def grok_chat_completion(messages: list[dict], max_tokens: int = 8192, thinking_budget: int = 0, timeout: int = 150) -> str:
    """Send chat completion to LLM API with automatic multi-tier fallback (DeepSeek -> Grok)."""
    candidate_models = get_candidate_models()
    errors = []
    api_key = os.environ.get("NOVA_API_KEY", NOVA_API_KEY)
    base_url = os.environ.get("NOVA_BASE_URL", NOVA_BASE_URL).rstrip("/")

    for idx, model_name in enumerate(candidate_models):
        payload = {
            "model": model_name,
            "messages": messages,
            "max_tokens": max_tokens,
        }
        if thinking_budget and "gemini" in model_name.lower():
            payload["thinking_config"] = {"thinking_budget": thinking_budget}
        req = urllib.request.Request(
            f"{base_url}/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            }
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                res = json.loads(resp.read().decode("utf-8"))
                if "choices" in res and res["choices"] and "message" in res["choices"][0]:
                    content = res["choices"][0]["message"].get("content", "")
                    if content:
                        if idx > 0:
                            print(f"[LLM FALLBACK SUCCESS] Model '{model_name}' succeeded after fallback!")
                        return content
                    raise ValueError(f"Empty content from model '{model_name}'")
                elif "error" in res:
                    raise ValueError(f"API error: {res['error'].get('message', res['error'])}")
                else:
                    raise ValueError(f"Unexpected response format: {res}")
        except Exception as e:
            err_str = str(e)
            if hasattr(e, "read"):
                try:
                    err_body = e.read().decode("utf-8", errors="ignore")
                    err_str += f" | {err_body}"
                except Exception:
                    pass
            log_msg = f"[LLM FAIL] Model '{model_name}' failed: {err_str}"
            if idx < len(candidate_models) - 1:
                next_model = candidate_models[idx + 1]
                print(f"{log_msg} -> Auto-falling back to '{next_model}'...")
            else:
                print(f"{log_msg} -> All candidate models exhausted.")
            errors.append(f"{model_name}: {err_str}")
            continue

    raise RuntimeError(f"All LLM candidate models failed: {'; '.join(errors)}")

def grok_vision_analyze(prompt: str, image_paths: list[Path]) -> any:
    """Call Google Gemini 3.8 Flash Vision on Nova Gateway with auto AGY Vision fallback."""
    try:
        content = [{"type": "text", "text": prompt}]
        for p in image_paths:
            if p and p.exists():
                b64 = base64.b64encode(p.read_bytes()).decode()
                mime = "image/jpeg"
                if p.suffix.lower() == ".png":
                    mime = "image/png"
                elif p.suffix.lower() == ".webp":
                    mime = "image/webp"
                content.append({
                    "type": "image_url",
                    "image_url": {"url": f"data:{mime};base64,{b64}"}
                })

        messages = [{"role": "user", "content": content}]
        raw_text = grok_chat_completion(messages, max_tokens=8192, thinking_budget=1024)
        return extract_json_from_text(raw_text)
    except Exception as e_grok:
        print(f"[VISION LLM ERROR] Nova Gemini Vision failed: {e_grok}. Auto-switching to AGY CLI fallback...")
        img_paths_str = ", ".join(str(p.resolve()) for p in image_paths if p and p.exists())
        agy_prompt = f"Please inspect the image(s) at {img_paths_str}.\n{prompt}"
        return run_agy_cli_json(agy_prompt)

def grok_script_generate(prompt: str) -> any:
    """Call Google Gemini 3.8 Flash on Nova Gateway for LLM scriptwriting with auto AGY fallback."""
    try:
        messages = [{"role": "user", "content": prompt}]
        raw_text = grok_chat_completion(messages, max_tokens=8192, thinking_budget=2048)
        return extract_json_from_text(raw_text)
    except Exception as e_grok:
        print(f"[SCRIPT LLM ERROR] Nova Gemini Script failed: {e_grok}. Auto-switching to AGY CLI fallback...")
        return run_agy_cli_json(prompt)

VOICE_BIBLE = {
    "female_north": {
        "voice_id": "CHAR_VN_FEMALE_NORTH_01",
        "gender": "female",
        "age": "22-26",
        "accent": "Northern Vietnamese",
        "pitch": "medium, warm mezzo-soprano",
        "timbre": "warm, silky, clear resonant presence",
        "energy": "medium, poised elegant delivery",
        "speed": "medium, steady conversational cadence",
        "delivery": "calm conversational delivery, clear natural pronunciation",
        "vocal_weight": "moderate-light",
        "acoustics": "studio close-mic direct sound, intimate warm presence, zero room echo",
        "negative_constraints": "no pitch drift, no dramatic pitch jumps, no youthful high-pitched screech, no robotic monotone",
        "lock_prompt": (
            "VOICE LOCK — DO NOT reinterpret: "
            "Speaker: Vietnamese female, approx 24 years old. "
            "Voice identity: Warm mezzo-soprano, clear silky texture, moderate vocal weight, stable pitch, calm natural conversational delivery, neutral Northern Vietnamese accent, medium speaking speed, studio close-mic direct sound with zero room echo. "
            "This is the exact same speaker and voice identity used in every previous scene. "
            "Do not change vocal age, gender, accent, pitch range, timbre, vocal texture, or speaking rhythm. "
            "No dramatic pitch changes, no high-pitched screech."
        ),
    },
    "female_south": {
        "voice_id": "CHAR_VN_FEMALE_SOUTH_01",
        "gender": "female",
        "age": "22-25",
        "accent": "Southern Vietnamese",
        "pitch": "medium, melodious warm soprano",
        "timbre": "bright, sweet, friendly resonant texture",
        "energy": "lively, warm welcoming delivery",
        "speed": "medium, expressive fluent cadence",
        "delivery": "engaging conversational reviewer delivery, clear sweet articulation",
        "vocal_weight": "light-moderate",
        "acoustics": "studio close-mic direct sound, intimate warm presence, zero room echo",
        "negative_constraints": "no pitch drift, no dramatic pitch jumps, no cartoonish squeak, no robotic monotone",
        "lock_prompt": (
            "VOICE LOCK — DO NOT reinterpret: "
            "Speaker: Vietnamese female, approx 23 years old. "
            "Voice identity: Sweet melodious soprano, bright warm texture, light-moderate weight, stable pitch, engaging friendly delivery, natural Southern Vietnamese accent, medium speaking speed, studio close-mic direct sound with zero room echo. "
            "This is the exact same speaker and voice identity used in every previous scene. "
            "Do not change vocal age, gender, accent, pitch range, timbre, vocal texture, or speaking rhythm. "
            "No dramatic pitch changes, no cartoonish squeak."
        ),
    },
    "male_north": {
        "voice_id": "CHAR_VN_MALE_NORTH_01",
        "gender": "male",
        "age": "28-32",
        "accent": "Northern Vietnamese",
        "pitch": "low male baritone",
        "timbre": "warm, deep, slightly husky vocal texture",
        "energy": "steady, confident authoritative delivery",
        "speed": "medium, calm conversational speed",
        "delivery": "calm conversational delivery, clear natural pronunciation",
        "vocal_weight": "moderate-heavy",
        "acoustics": "studio close-mic direct sound, intimate warm presence, zero room echo",
        "negative_constraints": "no pitch drift, no dramatic pitch changes, no youthful high-pitched qualities, no robotic monotone",
        "lock_prompt": (
            "VOICE LOCK — DO NOT reinterpret: "
            "Speaker: Vietnamese male, approx 30 years old. "
            "Voice identity: Low male baritone, warm slightly husky vocal texture, moderate vocal weight, stable pitch, calm conversational delivery, neutral Northern Vietnamese accent, medium speaking speed, studio close-mic direct sound with zero room echo. "
            "This is the exact same speaker and voice identity used in every previous scene. "
            "Do not change vocal age, gender, accent, pitch range, timbre, vocal texture, or speaking rhythm. "
            "No dramatic pitch changes, no youthful/high-pitched qualities."
        ),
    },
    "male_south": {
        "voice_id": "CHAR_VN_MALE_SOUTH_01",
        "gender": "male",
        "age": "26-30",
        "accent": "Southern Vietnamese",
        "pitch": "medium-low resonant tenor",
        "timbre": "warm, dynamic, friendly open texture",
        "energy": "dynamic, enthusiastic friendly delivery",
        "speed": "medium, natural fluent reviewer cadence",
        "delivery": "approachable lifestyle reviewer delivery, clear natural pronunciation",
        "vocal_weight": "moderate",
        "acoustics": "studio close-mic direct sound, intimate warm presence, zero room echo",
        "negative_constraints": "no pitch drift, no sudden pitch jumps, no exaggerated screaming, no robotic monotone",
        "lock_prompt": (
            "VOICE LOCK — DO NOT reinterpret: "
            "Speaker: Vietnamese male, approx 28 years old. "
            "Voice identity: Resonant warm tenor, dynamic friendly texture, moderate vocal weight, stable pitch, enthusiastic approachable delivery, natural Southern Vietnamese accent, medium speaking speed, studio close-mic direct sound with zero room echo. "
            "This is the exact same speaker and voice identity used in every previous scene. "
            "Do not change vocal age, gender, accent, pitch range, timbre, vocal texture, or speaking rhythm. "
            "No dramatic pitch changes, no exaggerated screaming."
        ),
    },
}

VOICE_PROFILES = {
    "female_north": {
        "id": "female_north",
        "label": "Nữ Miền Bắc",
        "badge": "👩 Nữ Miền Bắc (Thanh lịch)",
        "tone_desc": "Giọng nữ miền Bắc chuẩn phát thanh, thanh lịch, nhẹ nhàng, tự nhiên (dùng từ: 'nhé', 'ạ', 'mọi người ơi', 'chị em ơi', 'cực kỳ').",
        "veo_prompt": VOICE_BIBLE["female_north"]["lock_prompt"],
        "say_clause": "a warm, clear Northern Vietnamese female voice",
        "voice_bible": VOICE_BIBLE["female_north"],
        "edge_voice": "vi-VN-HoaiMyNeural",
        "rate": "+0%"
    },
    "female_south": {
        "id": "female_south",
        "label": "Nữ Miền Nam",
        "badge": "👩 Nữ Miền Nam (Ngọt ngào)",
        "tone_desc": "Giọng nữ miền Nam ngọt ngào, gần gũi, duyên dáng, thân thiện chuẩn reviewer TikTok (dùng từ: 'nè', 'nghen', 'thiệt sự luôn á', 'cả nhà ơi', 'mọi người ơi').",
        "veo_prompt": VOICE_BIBLE["female_south"]["lock_prompt"],
        "say_clause": "a sweet, melodious Southern Vietnamese female voice (giọng nữ miền Nam)",
        "voice_bible": VOICE_BIBLE["female_south"],
        "edge_voice": "vi-VN-HoaiMyNeural",
        "rate": "+5%"
    },
    "male_north": {
        "id": "male_north",
        "label": "Nam Miền Bắc",
        "badge": "👨 Nam Miền Bắc (Trầm ấm)",
        "tone_desc": "Giọng nam miền Bắc trầm ấm, uy tín, chững chạc, dứt khoát (dùng từ: 'nhé', 'anh em ơi', 'các bác ơi', 'chuẩn xác', 'cực kỳ').",
        "veo_prompt": VOICE_BIBLE["male_north"]["lock_prompt"],
        "say_clause": "a deep, warm Northern Vietnamese male voice",
        "voice_bible": VOICE_BIBLE["male_north"],
        "edge_voice": "vi-VN-NamMinhNeural",
        "rate": "+0%"
    },
    "male_south": {
        "id": "male_south",
        "label": "Nam Miền Nam",
        "badge": "👨 Nam Miền Nam (Hào sảng)",
        "tone_desc": "Giọng nam miền Nam hào sảng, phóng khoáng, thân thiện, năng động chuẩn reviewer (dùng từ: 'nè', 'anh em ơi', 'cả nhà ơi', 'thiệt tình', 'siêu êm').",
        "veo_prompt": VOICE_BIBLE["male_south"]["lock_prompt"],
        "say_clause": "a warm, dynamic Southern Vietnamese male voice (giọng nam miền Nam)",
        "voice_bible": VOICE_BIBLE["male_south"],
        "edge_voice": "vi-VN-NamMinhNeural",
        "rate": "+5%"
    }
}

BGM_PROFILES = {
    "tiktok_upbeat": {
        "label": "🔥 TikTok Upbeat Vui Tươi (Carefree - Đập Hộp/Review Viral)",
        "file": BGM_DIR / "tiktok_upbeat_carefree.mp3"
    },
    "tiktok_snitch": {
        "label": "🕵️ Tò Mò & Bất Ngờ (Sneaky Snitch - Cú Lừa Thị Giác)",
        "file": BGM_DIR / "tiktok_playful_snitch.mp3"
    },
    "tiktok_vlog": {
        "label": "🎸 Acoustic Vlog Thực Tế (Daily Beetle - Review Chân Thật)",
        "file": BGM_DIR / "tiktok_vlog_beetle.mp3"
    },
    "acoustic_soft": {
        "label": "🎵 Acoustic Dịu Nhẹ (Thư giãn, êm ái)",
        "file": BGM_DIR / "acoustic_soft.mp3"
    },
    "none": {
        "label": "🔇 Tắt nhạc nền (Chỉ giữ tiếng thoại AI)",
        "file": None
    }
}

def generate_edge_tts(text: str, output_path: Path, voice: str = "vi-VN-HoaiMyNeural", rate: str = "+0%", max_retries: int = 5) -> bool:
    """Generate TTS audio with edge-tts as clean audio track with retry & backoff."""
    if not text or not text.strip():
        return False
    import asyncio
    import edge_tts
    import time
    
    clean_text = text.strip()
    
    for attempt in range(1, max_retries + 1):
        try:
            if output_path.exists():
                output_path.unlink(missing_ok=True)
                
            async def _synth():
                comm = edge_tts.Communicate(clean_text, voice, rate=rate)
                await comm.save(str(output_path))
                
            asyncio.run(_synth())
            
            if output_path.exists() and output_path.stat().st_size > 1000:
                return True
            else:
                if output_path.exists():
                    output_path.unlink(missing_ok=True)
                print(f"[EDGE-TTS] Warning: Attempt {attempt}/{max_retries} produced empty/invalid file for: {clean_text[:40]}...")
        except Exception as e:
            if output_path.exists():
                output_path.unlink(missing_ok=True)
            print(f"[EDGE-TTS] Attempt {attempt}/{max_retries} error for '{clean_text[:30]}...': {e}")
            
        if attempt < max_retries:
            time.sleep(1.5 * attempt)
            
    print(f"[EDGE-TTS] ❌ Failed to generate TTS after {max_retries} attempts: '{clean_text[:50]}'")
    return False

def get_media_duration(path: Path) -> float:
    """Get exact duration of video or audio file using ffprobe."""
    try:
        cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", str(path)]
        out = subprocess.check_output(cmd, timeout=10).decode().strip()
        return float(out)
    except Exception:
        return 0.0

# ==============================================================================
# CRITICAL ARCHITECTURAL RULE — AUDIO & NATIVE VOICE PRESERVATION:
# 1. Google Veo 3.1 & xAI Grok Video 1.5 have native multimodal audio generation.
#    When prompted with `Say: "..." in natural Vietnamese...`, they generate
#    realistic Vietnamese speech, accurate lip-sync, and object Foley sound effects.
# 2. NEVER overwrite, strip, or replace native video audio with Edge-TTS.
# 3. Always check `check_video_has_audio(clip_path)` first:
#    - If True: PRESERVE native audio 100%. Do NOT mux TTS.
#    - If False: Only then use Edge-TTS as fallback for silent video models.
# Reference Jobs: 3fb7954b (Veo native audio), 1908aee1 (Grok native audio).
# ==============================================================================

def check_video_has_audio(video_path: Path) -> bool:
    """Check if video file already contains a valid audio stream (e.g. native sound from Google Veo or Grok Video)."""
    if not video_path or not video_path.exists():
        return False
    try:
        cmd = ["ffprobe", "-v", "error", "-select_streams", "a", "-show_entries", "stream=codec_name", "-of", "default=noprint_wrappers=1:nokey=1", str(video_path)]
        out = subprocess.check_output(cmd, timeout=10).decode().strip()
        return bool(out)
    except Exception:
        return False

def mux_tts_to_video(video_path: Path, tts_path: Path, output_path: Path = None, target_duration: float = None) -> bool:
    """Mux Edge-TTS audio track with video clip into an MP4 container, replacing any existing audio.
    Synchronizes audio tempo to fit within video duration with a clean buffer (>=0.7s) before scene transition,
    adds natural lead-in delay, and pads audio so video is never truncated."""
    if not video_path.exists() or not tts_path.exists():
        return False
    dest = output_path or video_path
    tmp_out = video_path.parent / f"tmp_mux_{video_path.name}"
    try:
        vid_dur = get_media_duration(video_path)
        if vid_dur <= 0.0:
            vid_dur = float(target_duration or 8.0)
        tts_dur = get_media_duration(tts_path)

        # Natural lead-in delay (120ms) so creator speech does not start abruptly on frame 0 during visual transition
        lead_in_s = 0.12
        lead_in_ms = int(lead_in_s * 1000)

        # Video crossfade in smart_concat_videos starts at (vid_dur - 0.50s).
        # We ensure dialogue finishes comfortably at least 0.20s before crossfade starts (total buffer: 0.70s + lead_in).
        target_speech_dur = max(1.0, vid_dur - 0.70 - lead_in_s)
        if tts_dur > target_speech_dur:
            speed_factor = min(1.45, max(1.0, tts_dur / target_speech_dur))
            afilter = f"adelay={lead_in_ms}|{lead_in_ms},atempo={speed_factor:.4f},apad"
        else:
            afilter = f"adelay={lead_in_ms}|{lead_in_ms},apad"

        # Explicitly map video from 0:v:0 and audio from [aout] (from input 1, stripping any original video audio)
        cmd = [
            "ffmpeg", "-y",
            "-i", str(video_path),
            "-i", str(tts_path),
            "-filter_complex", f"[1:a]{afilter}[aout]",
            "-map", "0:v:0",
            "-map", "[aout]",
            "-c:v", "copy",
            "-c:a", "aac",
            "-b:a", "192k",
            "-t", f"{vid_dur:.3f}",
            str(tmp_out)
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if res.returncode == 0 and tmp_out.exists() and tmp_out.stat().st_size > 1000:
            tmp_out.replace(dest)
            return True
        else:
            print(f"[TVC MUX] Warning muxing TTS: {res.stderr[:250]}")
    except Exception as e:
        print(f"[TVC MUX] Error muxing TTS to video: {e}")
    finally:
        if tmp_out.exists():
            try:
                tmp_out.unlink()
            except Exception:
                pass
    return False

def determine_product_persona(
    product_name: str,
    product_category: str = "",
    highlights: str = "",
    voice_key: str = "female_north",
    model_gender: str = ""
) -> dict:
    """Analyze product details and voice persona to determine target demographic,
    appropriate conversational opening hooks, strict forbidden words, and semantic replacements."""
    combined_text = f"{product_name} {product_category} {highlights}".lower()

    # Detect unisex / dual-gender keywords
    unisex_keywords = [
        "unisex", "nam nữ", "nam va nu", "nam và nữ", "cặp đôi", "cho cả nam",
        "đôi nam nữ", "thích hợp cho cả nam", "phù hợp cho cả nam", "cả nam lẫn nữ"
    ]
    is_unisex_product = any(k in combined_text for k in unisex_keywords)

    # Strictly male grooming tools (even if title adds 'cho nam nữ')
    male_grooming_keywords = [
        "cạo râu", "dao cạo", "bàn cạo râu", "máy cạo râu", "lưỡi cạo", "tông đơ",
        "kem cạo râu", "bọt cạo râu", "dung dịch vệ sinh nam", "bọt vệ sinh nam",
        "quần sịp", "quần lót nam", "pomade"
    ]
    is_strictly_male_groom = any(k in combined_text for k in male_grooming_keywords)

    # General male-targeted keywords
    male_keywords = [
        "sáp vuốt tóc", "wax vuốt tóc", "gel vuốt tóc", "nước hoa nam",
        "áo thun nam", "sơ mi nam", "quần âu nam", "thắt lưng nam", "ví nam", "cà vạt",
        "ví da nam", "giày da nam", "vest nam", "cho nam giới", "dành cho nam", "nam giới", "phái mạnh"
    ]

    # Detect female-targeted product
    female_keywords = [
        "son môi", "son dưỡng", "kem nền", "cushion", "chì kẻ mày", "mascara",
        "nước tẩy trang", "kem chống nắng", "serum dưỡng trắng", "mặt nạ dưỡng da",
        "băng vệ sinh", "dung dịch vệ sinh phụ nữ", "nước hoa nữ", "váy", "đầm", "chân váy",
        "áo ngực", "bra", "quần lót nữ", "túi xách nữ", "guốc", "giày cao gót",
        "dành cho nữ", "phụ nữ", "chị em", "bạn nữ"
    ]

    is_male_product = is_strictly_male_groom or (any(k in combined_text for k in male_keywords) and not is_unisex_product)
    is_female_product = any(k in combined_text for k in female_keywords) and not is_male_product and not is_unisex_product

    # Speaker voice characteristics
    is_male_voice = voice_key.startswith("male_") or (model_gender and model_gender.lower() == "male")
    is_northern = "north" in voice_key
    is_southern = "south" in voice_key

    forbidden_words = []
    recommended_openers = []
    guidance_lines = []
    replacements = {}

    if is_male_voice:
        target_persona = "male_speaker"
        recommended_openers = ["Anh em ơi", "Các bác ơi", "Mọi người ơi", "Cả nhà ơi", "Chào anh em nha"]
        forbidden_words = ["mấy bà ơi", "mấy bà nè", "mấy bà", "chị em ơi", "bà nào", "mê xỉu", "cưng xỉu"]
        replacements = {
            "mấy bà ơi": "Anh em ơi",
            "mấy bà nè": "Anh em nè",
            "mấy bà": "anh em",
            "chị em ơi": "Anh em ơi",
            "chị em": "anh em",
            "bà nào": "bác nào",
            "mê xỉu": "mê ly",
            "cưng xỉu": "quá ưng",
            "xỉu up xỉu down": "cực kỳ ưng ý",
        }
        guidance_lines.append("NGƯỜI NÓI LÀ NAM GIỚI: Phong thái nam tính, chững chạc, uy tín, tự nhiên.")
        guidance_lines.append("TUYỆT ĐỐI KHÔNG DÙNG CÁC TỪ NỮ TÍNH: 'mấy bà ơi', 'chị em ơi', 'mê xỉu', 'cưng xỉu'!")
        guidance_lines.append("BẮT BUỘC MỞ ĐẦU BẰNG: 'Anh em ơi', 'Các bác ơi', 'Mọi người ơi', hoặc 'Cả nhà ơi'.")
    elif is_male_product:
        target_persona = "female_speaking_male_product"
        if is_northern:
            recommended_openers = [
                "Mọi người ơi", "Anh em ơi", "Các bác ơi", "Cả nhà ơi",
                "Chị em nào đang tìm quà cho người yêu hay chồng thì xem ngay nhé"
            ]
            forbidden_words = ["mấy bà ơi", "mấy bà nè", "mấy bà", "bà nào", "mê xỉu", "cưng xỉu", "nghen", "thiệt sự luôn á"]
            replacements = {
                "mấy bà ơi": "Mọi người ơi",
                "mấy bà nè": "Cả nhà nè",
                "mấy bà": "mọi người",
                "bà nào": "ai",
                "mê xỉu": "thích mê",
                "cưng xỉu": "siêu ưng",
                "nghen": "nhé",
                "thiệt sự luôn á": "thực sự luôn nhé",
                "thiệt tình": "thực sự",
            }
            guidance_lines.append("SẢN PHẨM DÀNH CHO NAM GIỚI (hoặc chăm sóc cá nhân cho nam): Người nói là Nữ Miền Bắc.")
            guidance_lines.append("TUYỆT ĐỐI KHÔNG DÙNG 'mấy bà ơi' (đây là sản phẩm nam, xưng hô 'mấy bà ơi' gây sai lệch hoàn toàn đối tượng sử dụng)!")
            guidance_lines.append("TUYỆT ĐỐI KHÔNG DÙNG từ lóng miền Nam: 'mấy bà ơi', 'mê xỉu', 'nghen', 'thiệt sự luôn á'.")
            guidance_lines.append("MỞ ĐẦU PHÙ HỢP: 'Mọi người ơi', 'Anh em ơi', 'Các bác ơi', hoặc góc nhìn quà tặng: 'Chị em nào đang tìm quà cho người yêu hay chồng thì xem ngay nhé'.")
        else: # southern
            recommended_openers = [
                "Mọi người ơi", "Anh em ơi", "Cả nhà ơi",
                "Chị em nào đang tìm quà cho bạn trai hay ông xã thì xem ngay nha"
            ]
            forbidden_words = ["mấy bà ơi", "mấy bà nè", "mấy bà", "bà nào"]
            replacements = {
                "mấy bà ơi": "Mọi người ơi",
                "mấy bà nè": "Cả nhà nè",
                "mấy bà": "mọi người",
                "bà nào": "ai",
                "mê xỉu": "mê lắm nha",
                "cưng xỉu": "cưng lắm nha",
            }
            guidance_lines.append("SẢN PHẨM DÀNH CHO NAM GIỚI: Người nói là Nữ Miền Nam.")
            guidance_lines.append("TUYỆT ĐỐI KHÔNG DÙNG 'mấy bà ơi'!")
            guidance_lines.append("MỞ ĐẦU PHÙ HỢP: 'Mọi người ơi', 'Anh em ơi', 'Cả nhà ơi', hoặc 'Chị em nào đang tìm quà tặng bạn trai hay ông xã thì xem ngay nha'.")
    elif is_female_product:
        target_persona = "female_speaking_female_product"
        if is_northern:
            recommended_openers = ["Chị em ơi", "Mọi người ơi", "Các bác ơi", "Cả nhà ơi"]
            forbidden_words = ["mấy bà ơi", "mấy bà nè", "mấy bà", "nghen", "thiệt sự luôn á"]
            replacements = {
                "mấy bà ơi": "Chị em ơi",
                "mấy bà nè": "Chị em nè",
                "mấy bà": "chị em",
                "nghen": "nhé",
                "thiệt sự luôn á": "thực sự luôn nhé",
            }
            guidance_lines.append("SẢN PHẨM DÀNH CHO NỮ GIỚI: Người nói là Nữ Miền Bắc thanh lịch.")
            guidance_lines.append("TUYỆT ĐỐI KHÔNG DÙNG tiếng lóng miền Nam: 'mấy bà ơi', 'nghen', 'thiệt sự luôn á'.")
            guidance_lines.append("MỞ ĐẦU PHÙ HỢP: 'Chị em ơi', 'Mọi người ơi', 'Các bác ơi', 'Cả nhà ơi'.")
        else:
            recommended_openers = ["Mọi người ơi", "Cả nhà ơi", "Chị em ơi", "Mấy bà ơi"]
            forbidden_words = []
            replacements = {}
            guidance_lines.append("SẢN PHẨM DÀNH CHO NỮ GIỚI: Người nói là Nữ Miền Nam ngọt ngào.")
            guidance_lines.append("MỞ ĐẦU PHÙ HỢP: 'Mọi người ơi', 'Cả nhà ơi', 'Chị em ơi', hoặc 'Mấy bà ơi'.")
    else: # unisex / general
        target_persona = "general_product"
        if is_northern:
            recommended_openers = ["Mọi người ơi", "Cả nhà ơi", "Các bác ơi", "Các bạn ơi"]
            forbidden_words = ["mấy bà ơi", "mấy bà nè", "mấy bà", "nghen", "thiệt sự luôn á"]
            replacements = {
                "mấy bà ơi": "Mọi người ơi",
                "mấy bà nè": "Mọi người nè",
                "mấy bà": "mọi người",
                "bà nào": "ai",
                "nghen": "nhé",
                "thiệt sự luôn á": "thực sự luôn nhé",
            }
            guidance_lines.append("SẢN PHẨM ĐA DỤNG / TIỆN ÍCH CHUNG: Người nói là Nữ Miền Bắc.")
            guidance_lines.append("TUYỆT ĐỐI KHÔNG DÙNG: 'mấy bà ơi', 'nghen', 'thiệt sự luôn á'.")
            guidance_lines.append("MỞ ĐẦU PHÙ HỢP: 'Mọi người ơi', 'Cả nhà ơi', 'Các bác ơi', 'Các bạn ơi'.")
        else:
            recommended_openers = ["Mọi người ơi", "Cả nhà ơi", "Các bạn ơi"]
            forbidden_words = ["mấy bà ơi", "mấy bà nè", "mấy bà"]
            replacements = {
                "mấy bà ơi": "Mọi người ơi",
                "mấy bà nè": "Mọi người nè",
                "mấy bà": "mọi người",
                "bà nào": "ai",
            }
            guidance_lines.append("SẢN PHẨM ĐA DỤNG / TIỆN ÍCH CHUNG: Người nói là Nữ Miền Nam.")
            guidance_lines.append("KHÔNG DÙNG 'mấy bà ơi' để tránh thu hẹp tệp khách hàng đại chúng.")
            guidance_lines.append("MỞ ĐẦU PHÙ HỢP: 'Mọi người ơi', 'Cả nhà ơi', 'Các bạn ơi'.")

    # Dialect orthography: Veo speaks the text literally — the script itself must carry the accent.
    if is_southern:
        guidance_lines.append(
            "PHƯƠNG NGỮ BẮT BUỘC — MIỀN NAM: Viết TOÀN BỘ audio_dialogue bằng văn nói miền Nam tự nhiên. "
            "Dùng 'nha'/'nè'/'nghen' thay 'nhé', 'thiệt' thay 'thật', 'hông' thay 'không' (thân mật), "
            "'dạ/dzạ', 'gòi' thay 'rồi' khi tự nhiên, 'vậy đó', 'trời ơi', 'á', 'luôn á'. "
            "TUYỆT ĐỐI KHÔNG viết câu chuẩn trung tính kiểu Bắc: 'nhé', 'rất là', 'vậy nhé', 'đấy', 'nhỉ'."
        )
    elif is_northern:
        guidance_lines.append(
            "PHƯƠNG NGỮ BẮT BUỘC — MIỀN BẮC: Viết TOÀN BỘ audio_dialogue bằng văn nói miền Bắc tự nhiên: "
            "'nhé', 'ạ', 'vậy', 'đấy', 'rất là', 'thật sự'. TUYỆT ĐỐI KHÔNG dùng văn miền Nam: "
            "'nha', 'nè', 'nghen', 'thiệt', 'hông', 'gòi', 'á'."
        )

    return {
        "target_persona": target_persona,
        "is_male_product": is_male_product,
        "is_female_product": is_female_product,
        "is_unisex_product": is_unisex_product,
        "recommended_openers": recommended_openers,
        "forbidden_words": forbidden_words,
        "replacements": replacements,
        "guidance": "\n".join(guidance_lines)
    }

def apply_persona_replacements(text: str, replacements: dict) -> str:
    """Apply semantic keyword replacements according to persona rules."""
    if not text or not replacements:
        return text
    result = text
    for word, rep in replacements.items():
        if word.lower() in result.lower():
            pattern = re.compile(re.escape(word), re.IGNORECASE)
            result = pattern.sub(rep, result)
    return result

def sync_dialogue_to_motion_prompt(motion_prompt: str, dialogue: str, veo_voice_prompt: str = "") -> str:
    """Ensure the 'Say: ...' clause inside video_motion_prompt precisely matches the spoken audio dialogue,
    locking lip synchronization between AI video rendering and Edge-TTS voiceover."""
    if not motion_prompt:
        return motion_prompt
    clean_dlg = (dialogue or "").strip().replace('"', "'").replace('\\', '')
    if not clean_dlg:
        return motion_prompt

    if veo_voice_prompt:
        voice_clause = f" in {veo_voice_prompt}"
        replacement = f'Say: \\"{clean_dlg}\\"{voice_clause}'
        pattern_escaped_full = re.compile(r'Say:\s*\\\"(.*?)\\\"(?:\s+(?:in|with)\s+[^.]+)?(?:\.|$)', re.DOTALL | re.IGNORECASE)
        pattern_unescaped_full = re.compile(r'Say:\s*\"(.*?)\"(?:\s+(?:in|with)\s+[^.]+)?(?:\.|$)', re.DOTALL | re.IGNORECASE)
        if pattern_escaped_full.search(motion_prompt):
            return pattern_escaped_full.sub(lambda m: replacement.rstrip('.') + '.', motion_prompt, count=1)
        elif pattern_unescaped_full.search(motion_prompt):
            return pattern_unescaped_full.sub(lambda m: replacement.rstrip('.') + '.', motion_prompt, count=1)
        else:
            return f"{motion_prompt.rstrip()} {replacement.rstrip('.')}."
    else:
        replacement = f'Say: \\"{clean_dlg}\\"'
        pattern_escaped = re.compile(r'Say:\s*\\\"(.*?)\\\"', re.DOTALL | re.IGNORECASE)
        pattern_unescaped = re.compile(r'Say:\s*\"(.*?)\"', re.DOTALL | re.IGNORECASE)
        if pattern_escaped.search(motion_prompt):
            return pattern_escaped.sub(lambda m: replacement, motion_prompt, count=1)
        elif pattern_unescaped.search(motion_prompt):
            return pattern_unescaped.sub(lambda m: replacement, motion_prompt, count=1)
        else:
            return f'{motion_prompt.rstrip()} Say: \\"{clean_dlg}\\".'

def rotate_profile_proxy(nick_id: str = "nick-a") -> dict:
    """Call FlowKit API to explicitly rotate proxy for a nick."""
    try:
        req = urllib.request.Request(
            f"{FLOWKIT_API}/api/accounts/{nick_id}/rotate-proxy",
            data=b"{}",
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"Error rotating proxy for {nick_id}: {e}")
        return {"ok": False, "error": str(e)}

from agent.services.flow_trace import traced_sync, trace_id, emit as trace_emit, summary as trace_summary
from agent.services.parked_retry import ParkedBackoff


@traced_sync("tvc.api")
def call_flowkit_api(endpoint: str, payload: dict, timeout: int = 120, max_retries: int = 5, job_id: str = None) -> dict:
    if endpoint in ("/api/flow/generate-video", "/api/flow/generate-video-refs"):
        max_retries = 1  # Lost response does not mean Google rejected the render.
    url = f"{FLOWKIT_API}{endpoint}"
    parked = ParkedBackoff()
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"}
        )
        req.add_header("X-Request-ID", trace_id.get())
        trace_emit("tvc.api.attempt", attempt=attempt, endpoint=endpoint, timeout_s=timeout)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data
        except urllib.error.HTTPError as err:
            trace_emit("tvc.api.http_error", status=err.code, attempt=attempt)
            err_body = ""
            try:
                err_body = err.read().decode("utf-8")
            except Exception:
                pass

            # A fully parked fleet submitted nothing: wait it out instead of
            # failing the item after ~10s of 503s. Does not consume an attempt.
            if parked.wait(err, err_body):
                attempt -= 1
                continue
            is_unusual = "UNUSUAL_ACTIVITY" in err_body or "ogiZ0b failed: [13]" in err_body or err.code == 429
            if is_unusual and attempt < max_retries:
                retry_s = 3.5
                try:
                    err_json = json.loads(err_body)
                    if isinstance(err_json, dict):
                        retry_s = float(err_json.get("retry_after_s") or 3.5)
                except Exception:
                    pass
                print(f"[RETRY] Detected UNUSUAL_ACTIVITY / Rate-limit on {endpoint} (Attempt {attempt}/{max_retries}). FlowKit server is settling/rotating proxy, retrying in {retry_s}s...")
                if job_id:
                    update_job(job_id, message=f"Hệ thống AI đang làm mới phiên (chờ {retry_s}s)...")
                time.sleep(retry_s)
                continue
            elif attempt < max_retries and err.code in [500, 502, 503, 504]:
                print(f"[RETRY] Server error {err.code} on {endpoint} (Attempt {attempt}/{max_retries}). Retrying in 3.5s...")
                time.sleep(3.5)
                continue
            raise RuntimeError(f"FlowKit error {err.code} on {endpoint}: {err_body or err.reason}")
        except Exception as e:
            if attempt < max_retries:
                print(f"[RETRY] Network/Timeout error on {endpoint} ({e}). Retrying in 3.5s...")
                time.sleep(3.5)
                continue
            raise

def flowkit_vision_analyze(prompt: str, image_paths: list[Path], system_instruction: str = "", job_id: str = None) -> dict:
    """Call FlowKit's /api/flow/vision-analyze (agJzFb) with auto-retry on 429/UNUSUAL."""
    payload = {
        "prompt": prompt,
        "images": [str(p.resolve()) for p in image_paths],
        "system_instruction": system_instruction,
    }
    res = call_flowkit_api("/api/flow/vision-analyze", payload, timeout=120, max_retries=5, job_id=job_id)
    if isinstance(res, dict):
        if not res.get("ok", True) and "error" in res:
            raise RuntimeError(res.get("error") or res.get("message"))
        return res.get("data", res)
    return res

def upload_to_flowkit(image_path: Path, job_id: str = None) -> str:
    b64 = base64.b64encode(image_path.read_bytes()).decode()
    payload = {
        "image_base64": b64,
        "mime_type": "image/jpeg",
        "file_name": image_path.name
    }
    res = call_flowkit_api("/api/flow/upload-image", payload, timeout=120, job_id=job_id)
    mid = res.get("media_id") or (res.get("raw", {}).get("media") or {}).get("name") or (res.get("media") or {}).get("name") or res.get("_mediaId")
    if not mid:
        raise RuntimeError(f"Failed to upload {image_path.name}: {res}")
    return mid

def sanitize_safety_content(text: str) -> str:
    """
    Sanitize text prompts, visual anchors, and audio dialogue to strictly avoid content policy violations
    on both Google Veo 3.1 and xAI Grok Imagine Video 1.5.
    
    Replaces:
    1. Revealing / sensual attire & body descriptions (crop tops, halter tops, mini shorts, cleavage, bare midriff, etc.)
       with modest, professional commercial clothing (stylish crewneck top, tailored trousers, elegant modest attire).
    2. Ambiguous physical / tactile verbs (unzip mechanism, pulling out of slit, stroking, rubbing, caressing, etc.)
       with clean commercial presentation verbs (opening mechanism, revealing from pouch, holding gently, presenting).
    3. Age-safety triggers (teenager, teen girl, schoolgirl, etc.) with mature adult titles (young adult woman, female creator).
    4. Vietnamese suggestive or ambiguous phrases (khóa kéo mở bung, kéo khóa khe...) with safe commercial phrases.
    """
    if not text:
        return text

    s = text

    # 1. Adult / NSFW terms only (keep legitimate commercial fashion like crop tops, shorts, dresses 100% intact)
    s = re.sub(r"\b(?:lingerie|underwear|undergarment|bra|panties|thong|nude|naked|topless|bottomless)\b", "outfit", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(?:sexy|sensual|seductive|provocative|erotic)\b", "attractive", s, flags=re.IGNORECASE)

    # 2. Ambiguous Physical & Tactile Verbs (Often flagged by AI safety classifiers as sexual innuendo)
    s = re.sub(r"\bunzip(?:ping)?\s+mechanism\b", "opening mechanism", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(?:delicately\s+|gently\s+)?unzip(?:ping|ped)?\b", "gently opening", s, flags=re.IGNORECASE)
    s = re.sub(r"\bzipper(?:ed)?\s+closure\b", "opening seam", s, flags=re.IGNORECASE)
    s = re.sub(r"\bzippers?\b", "closure seam", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(?:horizontal\s+)?slits?\b", "seam details", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(?:tightly\s+)?hugging\s+.*?against\s+(?:her|his)\s+chest\b", "holding comfortably in front", s, flags=re.IGNORECASE)
    s = re.sub(r"\bpress(?:es|ing)?\s+(?:the\s+)?(?:soft\s+)?fabric\s+against\s+(?:her\s+|his\s+)?chest\b", "holding product gently in front", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(?:at\s+|near\s+|against\s+)?(?:her\s+|his\s+)?chest(?:\s+level)?\b", "in front of creator", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(?:breasts?|cleavage)\b", "front", s, flags=re.IGNORECASE)
    s = re.sub(r"\bpenetrat(?:e|es|ing|ion)\b", "interacting with", s, flags=re.IGNORECASE)
    s = re.sub(r"\bstrok(?:e|es|ing)\b", "holding gently", s, flags=re.IGNORECASE)
    s = re.sub(r"\bcaress(?:es|ing)\b", "holding gently", s, flags=re.IGNORECASE)
    s = re.sub(r"\brub(?:s|bing)\b", "touching gently", s, flags=re.IGNORECASE)
    s = re.sub(r"\bfondl(?:e|es|ing)\b", "handling gently", s, flags=re.IGNORECASE)
    s = re.sub(r"\bgrop(?:e|es|ing)\b", "holding", s, flags=re.IGNORECASE)
    s = re.sub(r"\bundress(?:es|ing)?\b", "uncover", s, flags=re.IGNORECASE)
    s = re.sub(r"\bpeel(?:s|ing)?\s+off\b", "gently reveal", s, flags=re.IGNORECASE)
    s = re.sub(r"\bthrust(?:s|ing)?\b", "move", s, flags=re.IGNORECASE)
    s = re.sub(r"\bgroan(?:s|ing)?\b|\bmoan(?:s|ing)?\b", "speak", s, flags=re.IGNORECASE)

    # 3. Revealing Attire Normalization (Prevent Grok Video false-positive NSFW blocks)
    s = re.sub(r"\b(?:halter\s+top|crop\s+top|halter\s+crop\s+top|halterneck)\b", "sporty crewneck top", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(?:low-rise|booty|mini|short)\s+shorts?\b", "athletic shorts", s, flags=re.IGNORECASE)
    s = re.sub(r"\blow-rise\b", "mid-rise", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(?:bare\s+midriff|exposed\s+midriff|belly\s+button)\b", "covered midriff", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(?:sexy|sensual|seductive|revealing)\b", "stylish", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(?:bikini|lingerie|underwear|undergarment)\b", "casual attire", s, flags=re.IGNORECASE)

    # 4. Age & Identity Normalization (Enforce mature adult context)
    s = re.sub(r"\b(?:teen(?:ager)?|teen\s+girl|schoolgirl|little\s+girl|young\s+girl|underage)\b", "young adult woman", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(?:schoolboy|little\s+boy|young\s+boy)\b", "young adult man", s, flags=re.IGNORECASE)

    # 5. Vietnamese Safety Normalization
    s = re.sub(r"\b(?:siêu\s+hot|quá\s+hot|cực\s+hot|cực\s+cháy)\b", "siêu xịn", s, flags=re.IGNORECASE)
    s = re.sub(r"\bhot\b", "xịn", s, flags=re.IGNORECASE)
    s = re.sub(r"khóa kéo mở bung", "thiết kế mở hé lộ", s, flags=re.IGNORECASE)
    s = re.sub(r"kéo khóa", "mở túi", s, flags=re.IGNORECASE)
    s = re.sub(r"khe hở|khe xẻ", "đường viền", s, flags=re.IGNORECASE)
    s = re.sub(r"ghé má cưng nựng|áp vào má", "cầm nhẹ trước ngực", s, flags=re.IGNORECASE)

    s = re.sub(r"\s+", " ", s).strip()
    return s

def sanitize_kf_prompt(prompt: str, canonical_anchor: str = "", model_anchor: str = "", mode: str = "pov") -> str:
    if not prompt:
        return prompt

    prompt = sanitize_safety_content(prompt)
    canonical_anchor = sanitize_safety_content(canonical_anchor)
    model_anchor = sanitize_safety_content(model_anchor)

    clean_canvas_guard = ", Ignore any promotional stickers, discount badges, shop logos, or colored border frames in the reference photo; generate ONLY the pristine physical product. Clean canvas, NO text overlay, NO typography, NO words, NO subtitles, NO watermark, NO logo overlay, NO graphic badges, NO sale banners."

    if mode == "unboxing":
        if canonical_anchor and "PRODUCT REFERENCE LOCK" not in prompt:
            prompt = f"PRODUCT REFERENCE LOCK — HIGHEST PRIORITY: {canonical_anchor}. {prompt}"
        if "no human" not in prompt.lower() and "no person" not in prompt.lower():
            prompt += ", purely product showcase on luxury display pedestal or aesthetic gift box, completely empty of people, NO human, NO human face, NO hands, NO arms, NO limbs in frame"
        if "upright" not in prompt.lower():
            prompt += ", strictly upright orientation, right-side up"
        if "photorealistic" not in prompt.lower():
            prompt += ", photorealistic 8k vertical 9:16"
        prompt += clean_canvas_guard
        return sanitize_safety_content(prompt)
    elif mode == "pov":
        if canonical_anchor:
            c_low = canonical_anchor.lower()
            if "upright" in c_low and "floppy" in prompt.lower():
                prompt = re.sub(r"\bfloppy\s+ears?\b", "short upright rounded ears", prompt, flags=re.IGNORECASE)
            if "purple" in c_low and "black eyes" in prompt.lower():
                prompt = re.sub(r"\bblack\s+eyes?\b", "circular purple embroidered round eyes", prompt, flags=re.IGNORECASE)
        if "upright" not in prompt.lower():
            prompt += ", strictly upright orientation, heads facing upwards toward top of frame, ears pointing up, characters right-side up, never upside down, never inverted"
        if "hands" in prompt.lower() and "exactly 2" not in prompt.lower():
            prompt += ", exactly 2 natural human hands in frame, 5 fingers each, no extra hands or floating limbs"
        prompt += clean_canvas_guard
        return sanitize_safety_content(prompt)
    elif mode == "demo":
        if canonical_anchor and "PRODUCT REFERENCE LOCK" not in prompt:
            prompt = f"PRODUCT REFERENCE LOCK — HIGHEST PRIORITY: {canonical_anchor}. {prompt}"
        if "hands" in prompt.lower() and "exactly 2" not in prompt.lower():
            prompt += ", exactly 2 natural human hands in frame, 5 fingers each, demonstrating product gently without covering the label, no extra hands or floating limbs"
        if "product label" not in prompt.lower() and "fully visible" not in prompt.lower():
            prompt += ", product fully visible and facing camera, clean bright aesthetic lighting"
        if "photorealistic" not in prompt.lower():
            prompt += ", photorealistic 8k vertical 9:16"
        prompt += clean_canvas_guard
        return sanitize_safety_content(prompt)
    elif mode == "fashion":
        if model_anchor and "CREATOR REFERENCE LOCK" not in prompt and "MODEL REFERENCE LOCK" not in prompt:
            prompt = f"CREATOR REFERENCE LOCK — HIGHEST PRIORITY: {model_anchor}. Same exact person as the attached reference portrait — identical face and hairstyle. {prompt}"
        if canonical_anchor and "PRODUCT REFERENCE LOCK" not in prompt:
            prompt = f"PRODUCT REFERENCE LOCK — HIGHEST PRIORITY: {canonical_anchor}.. {prompt}"
        if "photorealistic" not in prompt.lower():
            prompt += ", luxury fashion boutique lookbook, photorealistic 8k vertical 9:16, elegant drape, realistic fabric textures, clean seams"
        prompt += clean_canvas_guard
        return sanitize_safety_content(prompt)
    else:
        # UGC or Store Review mode
        if model_anchor and "CREATOR REFERENCE LOCK" not in prompt and "KOL REFERENCE LOCK" not in prompt:
            prompt = f"CREATOR REFERENCE LOCK — HIGHEST PRIORITY: {model_anchor}. Same exact person as the attached reference portrait — identical face and hairstyle. {prompt}"
        # Triệt tiêu các lệnh dễ làm biến dạng má, xương hàm và nghiêng vẹo mặt
        prompt = re.sub(r"\bpress(?:es|ing)?\s+(?:the\s+)?(?:soft\s+)?fabric\s+against\s+(?:her\s+|his\s+)?cheek\b", "holding the cute product near chest level while looking straight at camera", prompt, flags=re.IGNORECASE)
        prompt = re.sub(r"\btilt(?:s|ing)?\s+(?:her\s+|his\s+)?head\b", "facing directly forward toward camera with a gentle warm smile", prompt, flags=re.IGNORECASE)
        if "frontal" not in prompt.lower() and "looking directly" not in prompt.lower():
            prompt += ", frontal eye-level portrait looking directly at camera with natural symmetrical facial features matching reference face exactly"
        if "same person" not in prompt.lower():
            prompt += ", same person, identical face, identical hairstyle and identical clothing across all scenes"
        if "hands" in prompt.lower() and "extra" not in prompt.lower():
            prompt += ", natural human anatomy, 5 fingers each hand, no extra floating limbs"
        prompt += clean_canvas_guard
        return sanitize_safety_content(prompt)

def sanitize_video_motion_prompt(prompt: str, has_model: bool = True) -> str:
    """Sanitize motion prompt to eliminate on-screen text/captions, enforce safety, and lock facial identity."""
    if not prompt:
        return prompt

    cleaned = sanitize_safety_content(prompt.strip())

    # 1. Triệt tiêu các cụm từ dễ khiến Veo 3.1 hallucinate vẽ chữ / infographic lên video
    cleaned = re.sub(r"\b2-in-1\b", "transformable dual-design", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b2\s*in\s*1\b", "transformable dual-design", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b3-in-1\b", "multi-functional", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b\d+%\s*off\b", "promotional discount", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bstep\s*\d+\b", "progression", cleaned, flags=re.IGNORECASE)

    # 2. Khóa chuyển động đầu & góc mặt nếu có nhân vật để tránh lệch mặt / biến dạng mặt
    face_lock = ""
    if has_model:
        # Thay thế các cụm từ làm lệch/vẹo mặt như "tilts head", "turns head sideways", "presses cheek against"
        cleaned = re.sub(r"\btilts?\s+(?:her\s+|his\s+)?head\b", "smiles warmly looking directly at camera", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\bturns?\s+(?:her\s+|his\s+)?head(?:\s+sideways)?\b", "keeps face centered looking at camera", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\bpress(?:es|ing)?\s+(?:the\s+)?(?:soft\s+)?fabric\s+against\s+(?:her\s+|his\s+)?cheek\b", "holds product gently in front", cleaned, flags=re.IGNORECASE)
        face_lock = " FACIAL IDENTITY LOCK — HIGHEST PRIORITY: Keep identical facial features, eyes, nose, mouth and facial proportions from the start frame 100% frozen. Direct camera eye contact, natural symmetrical facial expression with subtle lip speech movements only. NO head tilting, NO facial morphing, NO deformation, NO face shape changes."

    # 3. Lệnh cấm triệt để chữ / subtitle / infographic trên màn hình video
    no_text_rule = " STRICTLY CLEAN CINEMATIC FOOTAGE: ZERO on-screen text, NO subtitles, NO captions, NO words, NO typography, NO watermark, NO logo overlay, NO floating labels, NO graphic icons, NO banners."

    # 4. Enforce strict G-rated commercial safety standard for Veo 3.1 & Grok
    safety_rule = " STRICT G-RATED COMMERCIAL STANDARD: Family-friendly broadcast footage, modest professional attire, gentle commercial interaction."

    return sanitize_safety_content(f"{cleaned} {face_lock} {no_text_rule} {safety_rule}".strip())

def refresh_ref_ids_from_disk(job_id: str) -> list[str]:
    job = JOBS.get(job_id, {})
    job_dir = WORK_DIR / job_id
    prod_file = job_dir / "product.jpg"
    model_file = job_dir / "model.jpg"
    user_bg_file = job_dir / "background.jpg"
    flow_mode = job.get("flow_mode", "pov")
    
    new_refs = []
    if flow_mode in ["ugc", "store_review", "fashion"] or (flow_mode == "demo" and model_file.exists()):
        if model_file.exists():
            new_refs.append(upload_to_flowkit(model_file, job_id=job_id))
        if prod_file.exists():
            new_refs.append(upload_to_flowkit(prod_file, job_id=job_id))
        if user_bg_file.exists():
            new_refs.append(upload_to_flowkit(user_bg_file, job_id=job_id))
    else:
        if prod_file.exists():
            new_refs.append(upload_to_flowkit(prod_file, job_id=job_id))
        if user_bg_file.exists():
            new_refs.append(upload_to_flowkit(user_bg_file, job_id=job_id))
            
    update_job(job_id, ref_ids=new_refs)
    return new_refs

def get_or_refresh_ref_ids(job_id: str, force_refresh: bool = False) -> list[str]:
    job = JOBS.get(job_id, {})
    if not force_refresh:
        ref_ids = job.get("ref_ids")
        if ref_ids:
            return ref_ids
    return refresh_ref_ids_from_disk(job_id)

def generate_keyframe_flowkit(prompt: str, ref_media_ids: list[str], job_id: str = None) -> tuple[str, str]:
    payload = {
        "prompt": prompt,
        "modelDisplayName": "Nano Banana Pro",
        "referenceImageMediaIds": ref_media_ids,
        "aspectRatio": "9:16"
    }
    try:
        res = call_flowkit_api("/api/flow/generate-image", payload, timeout=180, job_id=job_id)
    except Exception as e:
        if job_id and ref_media_ids:
            print(f"[REGEN KEYFRAME] Reference image error: {e}. Re-uploading fresh references...")
            fresh_refs = refresh_ref_ids_from_disk(job_id)
            payload["referenceImageMediaIds"] = fresh_refs
            res = call_flowkit_api("/api/flow/generate-image", payload, timeout=180, job_id=job_id)
        else:
            raise e

    media_list = res.get("media") or (res.get("data") or {}).get("media") or []
    if not media_list:
        raise RuntimeError(f"FlowKit image generation returned no media: {res}")
    media = media_list[0]
    mid = media.get("name") or media.get("image", {}).get("generatedImage", {}).get("mediaId")
    fife_url = media.get("image", {}).get("generatedImage", {}).get("fifeUrl") or f"{FLOWKIT_API}/api/flow/image/{mid}"
    return mid, fife_url

def submit_video_flowkit(keyframe_mid: str, motion_prompt: str, scene_idx: int, job_id: str = None, duration_s: int = 8) -> str:
    """Submit video generation request to FlowKit (eb1hJf / Veo 3) and return operation_name immediately.
    
    Zero-wait submission: takes ~2-3s to get operation_name without blocking on rendering.
    """
    has_model = True
    if job_id:
        job = JOBS.get(job_id, {})
        flow_mode = job.get("flow_mode", "pov")
        if flow_mode in ["pov", "unboxing"]:
            has_model = False

    sanitized_motion = sanitize_video_motion_prompt(motion_prompt, has_model=has_model)

    payload = {
        "start_image_media_id": keyframe_mid,
        "prompt": sanitized_motion,
        "project_id": "",
        "scene_id": f"scene_{scene_idx}",
        "aspect_ratio": "VIDEO_ASPECT_RATIO_PORTRAIT",
        "model_family": "veo",
        "duration_s": duration_s
    }
    res = call_flowkit_api("/api/flow/generate-video", payload, timeout=120, job_id=job_id)
    ops = res.get("operations") or (res.get("data") or {}).get("operations") or []
    if not ops:
        raise RuntimeError(f"No operations returned from /api/flow/generate-video for scene {scene_idx}: {res}")
    op_name = (ops[0].get("operation") or {}).get("name") or ops[0].get("name")
    if not op_name:
        raise RuntimeError(f"Could not extract operation name for scene {scene_idx}: {ops}")
    return op_name

def batch_poll_videos_flowkit(
    pending_ops: dict[int, str],
    job_id: str,
    job_dir: Path,
    on_clip_done: callable = None,
    on_clip_failed: callable = None,
    max_duration_s: int = FLOWKIT_VIDEO_WAIT_SECONDS
) -> dict[int, Path]:
    """Centralized Batch Poller Daemon: Polls all pending operations in a single consolidated request.
    
    Eliminates Polling Storms. Uses adaptive delay:
    - First 30s: Initial settling delay (Veo 3 render takes 90s-180s)
    - Then polls every 8-10s with all remaining operations in one batch.
    - As each operation finishes, downloads clip immediately and notifies on_clip_done.
    """
    completed_clips: dict[int, Path] = {}
    active_ops = dict(pending_ops)
    start_time = time.time()
    poll_round = 0
    
    if active_ops:
        print(f"[BATCH POLLER] Job {job_id}: {len(active_ops)} scenes pending ({list(active_ops.keys())}). Initial settling delay 30s...")
        time.sleep(30)
        
    while active_ops and (time.time() - start_time) < max_duration_s:
        poll_round += 1
        poll_body = {
            "operations": [{"operation": {"name": op_name}} for op_name in active_ops.values()]
        }
        try:
            p_data = call_flowkit_api("/api/flow/check-status", poll_body, timeout=40, job_id=job_id)
            returned_ops = p_data.get("operations") or (p_data.get("data") or {}).get("operations") or []
            results_by_name = {}
            for item in returned_ops:
                name = (item.get("operation") or {}).get("name") or item.get("name")
                if name:
                    results_by_name[name] = item
                    
            for scene_idx, op_name in list(active_ops.items()):
                curr = results_by_name.get(op_name)
                if not curr:
                    continue
                status = curr.get("status")
                metadata = (curr.get("operation") or {}).get("metadata", {})
                fife = metadata.get("video", {}).get("fifeUrl")
                
                if status == "MEDIA_GENERATION_STATUS_SUCCESSFUL" or fife:
                    clip_path = job_dir / f"clip_{scene_idx}.mp4"
                    print(f"[BATCH POLLER] 🎉 Scene {scene_idx} completed after {int(time.time() - start_time)}s! Downloading to {clip_path.name}...")
                    urllib.request.urlretrieve(fife, str(clip_path))
                    tts_file = job_dir / f"tts_{scene_idx}.mp3"
                    has_native_audio = check_video_has_audio(clip_path)
                    if has_native_audio:
                        print(f"[BATCH POLLER] 🎙️ Scene {scene_idx} has native AI synchronized audio ({clip_path.name}). Preserving native voice!")
                    else:
                        job_obj = JOBS.get(job_id) or {}
                        if not tts_file.exists() or tts_file.stat().st_size < 1000:
                            sc_list = job_obj.get("scenes", [])
                            matching_sc = next((s for s in sc_list if s.get("scene_id") == scene_idx), None)
                            dlg = (matching_sc.get("audio_dialogue") or "").strip() if matching_sc else ""
                            if dlg:
                                v_key = job_obj.get("voice", "female_north")
                                v_info = VOICE_PROFILES.get(v_key, VOICE_PROFILES["female_north"])
                                generate_edge_tts(dlg, tts_file, voice=v_info["edge_voice"], rate=v_info.get("rate", "+0%"))
                        if tts_file.exists() and tts_file.stat().st_size > 500:
                            job_dur = job_obj.get("scene_duration", 8)
                            mux_tts_to_video(clip_path, tts_file, target_duration=job_dur)
                    completed_clips[scene_idx] = clip_path
                    del active_ops[scene_idx]
                    if on_clip_done:
                        try:
                            on_clip_done(scene_idx, clip_path)
                        except Exception as cb_err:
                            print(f"[BATCH POLLER] Callback error for scene {scene_idx}: {cb_err}")
                elif "FAIL" in str(status).upper():
                    print(f"[BATCH POLLER] ❌ Scene {scene_idx} failed: {curr}")
                    del active_ops[scene_idx]
                    if on_clip_failed:
                        try:
                            on_clip_failed(scene_idx, "Lỗi khi render video từ cụm AI")
                        except Exception as fb_err:
                            print(f"[BATCH POLLER] Failed callback error for scene {scene_idx}: {fb_err}")
                    
            if active_ops:
                if poll_round % 4 == 0:
                    print(f"[BATCH POLLER] Job {job_id}: Round {poll_round}, remaining: {list(active_ops.keys())} ({int(time.time() - start_time)}s elapsed)")
                time.sleep(8)
        except Exception as e:
            print(f"[BATCH POLLER] Warning during poll round {poll_round}: {e}")
            time.sleep(6)
            
    if active_ops:
        print(f"[BATCH POLLER] ⚠️ Job {job_id}: Scenes {list(active_ops.keys())} reached max duration {int(time.time() - start_time)}s.")
        for sc_idx in list(active_ops.keys()):
            if on_clip_failed:
                try:
                    on_clip_failed(sc_idx, f"Quá thời gian render Veo ({int(time.time() - start_time)}s). Bấm 'Render lại Video' để thử lại.")
                except Exception as fb_err:
                    print(f"[BATCH POLLER] Failed callback error for scene {sc_idx}: {fb_err}")
            del active_ops[sc_idx]
        
    return completed_clips

def poll_video_operation_flowkit(op_name: str, scene_idx: int, job_id: str = None) -> str:
    """Poll an already-submitted Veo operation until it yields a fife url."""
    started = time.monotonic()
    poll_idx = 0
    while time.monotonic() - started < FLOWKIT_VIDEO_WAIT_SECONDS:
        poll_idx += 1
        time.sleep(4)
        poll_body = {"operations": [{"operation": {"name": op_name}}]}
        try:
            p_data = call_flowkit_api("/api/flow/check-status", poll_body, timeout=30, job_id=job_id)
            curr = (p_data.get("operations") or (p_data.get("data") or {}).get("operations") or [{}])[0]
            status = curr.get("status")
            metadata = (curr.get("operation") or {}).get("metadata", {})
            fife = metadata.get("video", {}).get("fifeUrl")
            if poll_idx % 15 == 0:
                print(f"[VEO POLL] Scene {scene_idx} ({op_name[:12]}...): loop {poll_idx}, status={status}")
            if status == "MEDIA_GENERATION_STATUS_SUCCESSFUL" or fife:
                print(f"[VEO DONE] Scene {scene_idx} completed after {poll_idx * 4}s!")
                return fife
            elif "FAIL" in str(status).upper():
                raise RuntimeError(f"Veo render failed: {curr}")
        except Exception as e:
            if "Veo render failed" in str(e):
                raise
            continue
    raise TimeoutError(f"Veo video scene {scene_idx} timed out after {FLOWKIT_VIDEO_WAIT_SECONDS}s")


def generate_video_flowkit(keyframe_mid: str, motion_prompt: str, scene_idx: int, job_id: str = None, duration_s: int = 8) -> str:
    """Fallback single-scene video generator (used for single scene regeneration)."""
    op_name = submit_video_flowkit(keyframe_mid, motion_prompt, scene_idx, job_id=job_id, duration_s=duration_s)
    return poll_video_operation_flowkit(op_name, scene_idx, job_id=job_id)

GROK_VOICE_CONFIGS = {
    "female_north": {
        "gender": "female",
        "voice_desc": "Native Vietnamese Northern accent. Young adult female (24-28 years old). Warm, sweet, clear, friendly and trustworthy tone. Medium pitch, medium speaking speed. Clear Vietnamese Northern pronunciation, accurate tones and consonants, natural conversational cadence.",
        "speaking_style": "Conversational, spontaneous and engaging. Sounds like a real Vietnamese creator having an authentic chat. Short natural pauses at commas and sentence boundaries, subtle breathing. Not robotic, not monotone, not a formal announcer voice."
    },
    "female_south": {
        "gender": "female",
        "voice_desc": "Native Vietnamese Southern accent. Young adult female (22-26 years old). Lively, sweet, warm, friendly, cheerful and approachable tone. Medium to slightly bright pitch, energetic speaking speed. Natural Southern Vietnamese intonation, melodious cadence, clear tones.",
        "speaking_style": "Conversational, sweet, enthusiastic and trendy. Sounds like an authentic Vietnamese TikTok reviewer chatting casually. Short natural pauses between phrases, natural breathing. Not robotic, not monotone."
    },
    "male_north": {
        "gender": "male",
        "voice_desc": "Native Vietnamese Northern accent. Young adult male (25-30 years old). Confident, deep, warm, authoritative, trustworthy and articulate tone. Medium-low pitch, steady speaking speed. Clear Vietnamese Northern pronunciation, accurate tones and consonants.",
        "speaking_style": "Conversational, confident, authentic and knowledgeable. Sounds like a genuine expert giving trusted advice. Short natural pauses, decisive delivery, natural breathing. Not robotic, not monotone, not exaggerated."
    },
    "male_south": {
        "gender": "male",
        "voice_desc": "Native Vietnamese Southern accent. Young adult male (24-29 years old). Dynamic, charismatic, friendly, enthusiastic and approachable tone. Medium pitch, lively conversational speed. Natural Southern Vietnamese intonation and warm cadence.",
        "speaking_style": "Conversational, lively, candid and engaging. Sounds like a friendly reviewer sharing real experiences. Short natural pauses, subtle emphasis on key product benefits, natural breathing. Not robotic, not monotone."
    }
}

def build_grok_video_prompt(
    motion_prompt: str,
    dialogue: str = "",
    flow_mode: str = "pov",
    voice_key: str = "female_north",
    has_model: bool = False
) -> str:
    """
    Build structured, token-safe prompt specifically for xAI Grok Imagine Video 1.5.
    Separates into explicit blocks: VISUAL/MOTION, VOICE, SPEAKING STYLE, DIALOGUE, LIP SYNC, ACTING, AUDIO.
    Differentiates between on-screen human creator (UGC/Store Review) vs pure product showcase (Unboxing/POV/Demo).
    Ensures prompt length is ~200-350 tokens, safely below Grok's 4096 token limit.
    Zero effect on Google Veo 3.1 prompts.
    """
    voice_cfg = GROK_VOICE_CONFIGS.get(voice_key, GROK_VOICE_CONFIGS["female_north"])

    # Extract dialogue from motion_prompt if not explicitly provided
    if not dialogue:
        m = re.search(r'Say:\s*\\?"(.*?)\\?"', motion_prompt)
        if not m:
            m = re.search(r'Say:\s*"(.*?)"', motion_prompt)
        if m:
            dialogue = m.group(1).strip()

    # Extract clean visual motion part by stripping 'Say: "..." ...' from motion_prompt
    clean_motion = re.sub(r'Say:\s*\\?".*?\\?"(?:\s*in\s*[^.]+)?\.?', '', motion_prompt, flags=re.DOTALL | re.IGNORECASE).strip()
    clean_motion = re.sub(r'Say:\s*".*?"(?:\s*in\s*[^.]+)?\.?', '', clean_motion, flags=re.DOTALL | re.IGNORECASE).strip()
    # Replace suppression of facial movements with active mouth articulation for speaking character
    clean_motion = re.sub(r'subtle facial movements only\.?', 'natural expressive facial animation with clear mouth articulation matching spoken dialogue.', clean_motion, flags=re.IGNORECASE)
    # Sanitize false-positive trigger keywords that trigger xAI Grok content policy violations
    clean_motion = sanitize_safety_content(clean_motion)
    clean_motion = re.sub(r'\s+', ' ', clean_motion).strip(' .,')

    # Sanitize dialogue if it contains sensitive phrasing
    safe_dialogue = sanitize_safety_content(dialogue)

    # Determine if human / creator is present
    is_human = has_model or flow_mode in ["ugc", "store_review", "fashion"]
    # If clean_motion explicitly mentions "NO human" or "empty of people", force non-human
    if re.search(r'\bno\s+human\b|\bzero\s+human\b|\bempty\s+of\s+people\b', clean_motion, re.IGNORECASE):
        is_human = False

    if is_human:
        prompt_parts = [
            "CHARACTER & VISUAL:",
            f"{clean_motion}.",
            "Maintain natural eye contact with camera. Natural facial animation, realistic human motion.",
            "Consistent face, hair, modest clothing, and natural studio lighting.",
            "",
            "VOICE:",
            f"{voice_cfg['voice_desc']}",
            "Use natural Vietnamese intonation and sentence rhythm. Do not sound like a foreigner speaking Vietnamese. Do not use an English-style rhythm. Clear Vietnamese tones and consonants.",
            "",
            "SPEAKING STYLE:",
            f"{voice_cfg['speaking_style']}",
            "Short 0.2s natural breath before starting speech. Clear, conversational Vietnamese flow.",
            "",
            "DIALOGUE:",
            f'"{safe_dialogue}"' if safe_dialogue else '"Xin chào các bạn, hôm nay mình muốn chia sẻ trải nghiệm tuyệt vời này!"',
            "",
            "LIP SYNC:",
            "Accurate lip synchronization with every spoken Vietnamese word. Clear, natural mouth opening and jaw articulation matching spoken syllables from start to finish. No mouth movement during silence. Maintain character facial identity throughout the shot.",
            "",
            "ACTING:",
            "Maintain natural eye contact with camera. Natural facial expressions, engaging demeanor, gentle hand gestures. Do not overact.",
            "",
            "AUDIO:",
            "Clean studio-quality voice. Voice is crisp and clearly audible. Natural room ambience, no music covering dialogue.",
            "STRICTLY ZERO on-screen text, NO subtitles, NO captions, NO typography, NO watermark, NO logo overlay.",
            "STRICT G-RATED COMMERCIAL BROADCAST STANDARD: 100% family-friendly, fully clothed modest attire, zero sensitive content."
        ]
    else:
        prompt_parts = [
            "VISUAL:",
            "PRODUCT LOCK — HIGHEST PRIORITY. KEEP PRODUCT PRISTINE AND CONSISTENT.",
            f"{clean_motion}.",
            "Purely product showcase. STRICTLY NO humans, NO human face, NO people visible in frame.",
            "Smooth steady camera movement, beautiful dramatic studio lighting, photorealistic 8k vertical 9:16.",
            "",
            "VOICE-OVER / NARRATION:",
            "Native Vietnamese voice-over narration in the background.",
            f"{voice_cfg['voice_desc']}",
            f"{voice_cfg['speaking_style']}",
            "",
            "DIALOGUE:",
            f'"{safe_dialogue}"' if safe_dialogue else '""',
            "",
            "AUDIO:",
            "Clean studio-quality voice-over narration. Voice is crisp and clearly audible in the background.",
            "No music overpowering speech.",
            "STRICTLY ZERO on-screen character or mouth animation on the product.",
            "STRICTLY ZERO on-screen text, NO subtitles, NO captions, NO words, NO typography, NO watermark, NO logo overlay.",
            "STRICT G-RATED COMMERCIAL BROADCAST STANDARD: 100% family-friendly, zero sensitive content."
        ]

    return sanitize_safety_content("\n".join(prompt_parts))

def ai_fix_scene_prompt(
    motion_prompt: str,
    dialogue: str = "",
    error_reason: str = "",
    flow_mode: str = "pov"
) -> dict:
    """Use Gemini 3.8 Flash (with AGY fallback) to sanitize, rephrase and optimize a video motion prompt and dialogue that failed moderation or quality checks."""
    system_instruction = (
        "You are an expert Commercial Film Director & AI Safety Optimization Specialist for xAI Grok Video and Google Veo.\n"
        "A video prompt previously failed or was flagged with content policy / safety issues (e.g. suggestive hand placements, touching near chest/body, revealing angles, or awkward wording).\n"
        "Your task is to sanitize, polish, and rewrite the prompt so it complies 100% with G-Rated Commercial Broadcast Standards while keeping the marketing appeal and product focus.\n\n"
        "CRITICAL RULES:\n"
        "1. VISUAL/MOTION: Rewrite 'fixed_motion_prompt' in concise professional English only. Avoid physical contact near chest, hips, waist, or clothes zippers. Replace with elegant commercial host gestures (e.g. 'standing upright in luxury showroom, neatly presenting product on display stage with warm welcoming smile').\n"
        "2. DIALOGUE: Ensure 'fixed_dialogue' is in natural, polite Vietnamese matching the duration (~20-24 words for 8s, ~10-12 words for 4s). Never use 'mấy bà ơi' for male products or male voices; use 'Anh em ơi', 'Mọi người ơi', 'Các bác ơi'.\n"
        "3. EXPLANATION: Provide a short, friendly 1-sentence explanation in Vietnamese explaining what you sanitized and improved.\n"
        "4. Output STRICT JSON:\n"
        "{\n"
        '  "fixed_motion_prompt": "...",\n'
        '  "fixed_dialogue": "...",\n'
        '  "explanation": "Đã làm sạch cử chỉ cầm sản phẩm trang nhã hơn và tối ưu hóa lời thoại để vượt qua kiểm duyệt."\n'
        "}"
    )

    user_content = f"""Failed Motion Prompt:
{motion_prompt}

Dialogue:
{dialogue}

Error/Flag Reason:
{error_reason or 'content_policy_violation (moderation filter triggered)'}

Mode: {flow_mode}

Please rewrite and return ONLY valid JSON:"""

    messages = [
        {"role": "system", "content": system_instruction},
        {"role": "user", "content": user_content}
    ]

    try:
        raw = grok_chat_completion(messages, max_tokens=2048, thinking_budget=1024, timeout=60)
        res = extract_json_from_text(raw)
        if isinstance(res, dict) and res.get("fixed_motion_prompt"):
            return res
    except Exception as e:
        print(f"[AI FIX PROMPT] Primary LLM failed: {e}. Falling back to rule-based + AGY...")
        try:
            res = run_agy_cli_json(f"{system_instruction}\n\n{user_content}")
            if isinstance(res, dict) and res.get("fixed_motion_prompt"):
                return res
        except Exception as e_agy:
            print(f"[AI FIX PROMPT] AGY CLI fallback failed: {e_agy}")

    # Fallback to rule-based sanitization if LLM unavailable
    clean_motion = sanitize_safety_content(motion_prompt)
    clean_motion = re.sub(r'\b(holding forward with welcoming enthusiasm|chest level|near chest|at chest)\b', 'presenting product gracefully on display counter with warm polite host smile', clean_motion, flags=re.IGNORECASE)
    clean_dlg = sanitize_safety_content(dialogue)
    return {
        "fixed_motion_prompt": clean_motion,
        "fixed_dialogue": clean_dlg,
        "explanation": "Đã tự động loại bỏ các từ khóa cử chỉ nhạy cảm và chuyển sang phong thái tiếp đón thanh lịch."
    }

def generate_video_grok(
    keyframe_path: Path,
    motion_prompt: str,
    scene_idx: int,
    job_id: str = None,
    duration_s: int = 6,
    model: str = "xai/grok-imagine-video-1.5",
    resolution: str = "480p",
    dialogue: str = "",
    flow_mode: str = "",
    voice_key: str = ""
) -> Path:
    """Generate a single video clip from Keyframe using xAI Grok Imagine Video via NOVA Gateway."""
    base_url = os.environ.get("NOVA_BASE_URL", NOVA_BASE_URL).rstrip("/")
    api_key = os.environ.get("NOVA_API_KEY", NOVA_API_KEY)
    
    if not keyframe_path or not keyframe_path.exists():
        raise FileNotFoundError(f"Keyframe file not found for scene {scene_idx}: {keyframe_path}")
        
    with open(keyframe_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")
    data_url = f"data:image/jpeg;base64,{b64}"
    
    # Retrieve job context if available
    job = JOBS.get(job_id, {}) if job_id else {}
    if not flow_mode:
        flow_mode = job.get("flow_mode", "pov")
    if not voice_key:
        voice_key = job.get("voice", "female_north")
    if not dialogue:
        for sc in job.get("scenes", []):
            if sc.get("scene_id") == scene_idx:
                dialogue = sc.get("audio_dialogue", "")
                break
                
    has_model = (flow_mode in ["ugc", "store_review", "fashion"]) or ((keyframe_path.parent / "model.jpg").exists())

    # Build structured prompt for Grok (token-safe, clear speech/lip-sync/voice-over blocks)
    grok_prompt = build_grok_video_prompt(
        motion_prompt,
        dialogue=dialogue,
        flow_mode=flow_mode,
        voice_key=voice_key,
        has_model=has_model
    )
    print(f"[GROK VIDEO] Scene {scene_idx} structured prompt ({len(grok_prompt.split())} words, ~{len(grok_prompt)//4} tokens)")
    
    payload = {
        "model": model,
        "prompt": grok_prompt,
        "aspect_ratio": "9:16",
        "duration": duration_s,
        "resolution": resolution,
        "image": {"url": data_url}
    }
    
    max_render_retries = 2
    for render_round in range(1, max_render_retries + 1):
        init_data = None
        last_init_err = None
        for init_attempt in range(3):
            try:
                req = urllib.request.Request(
                    f"{base_url}/v1/videos",
                    data=json.dumps(payload).encode("utf-8"),
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                        "User-Agent": "Nova-TVC/1.0"
                    },
                    method="POST"
                )
                with urllib.request.urlopen(req, timeout=90) as resp:
                    init_data = json.loads(resp.read().decode("utf-8"))
                if init_data and init_data.get("id"):
                    break
            except Exception as ie:
                last_init_err = ie
                print(f"[GROK VIDEO] Job {job_id} Scene {scene_idx} init attempt {init_attempt+1}/3 error: {ie}")
                if init_attempt < 2:
                    time.sleep(2)
            
        video_id = init_data.get("id") if init_data else None
        if not video_id:
            if render_round < max_render_retries:
                print(f"[GROK VIDEO] Job {job_id} Scene {scene_idx} init failed, retrying round {render_round+1} in 4s...")
                time.sleep(4)
                continue
            raise RuntimeError(f"Grok Video init failed after 3 attempts: {init_data or last_init_err}")
            
        print(f"[GROK VIDEO] Job {job_id} Scene {scene_idx}: video_id={video_id} submitted (round {render_round}). Polling...")
        
        poll_url = f"{base_url}/v1/videos/{video_id}"
        t0 = time.time()
        round_failed_transient = False
        for poll_cycle in range(60): # up to ~5 minutes
            time.sleep(5)
            preq = urllib.request.Request(
                poll_url,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "User-Agent": "Nova-TVC/1.0"
                }
            )
            try:
                with urllib.request.urlopen(preq, timeout=30) as presp:
                    pdata = json.loads(presp.read().decode("utf-8"))
                status = pdata.get("status", "").lower()
                if status in ("completed", "success"):
                    v_url = pdata.get("video_url") or pdata.get("url") or pdata.get("share_url")
                    if not v_url:
                        raise RuntimeError("No video URL returned in completed Grok response")
                    clip_path = keyframe_path.parent / f"clip_{scene_idx}.mp4"
                    
                    # Retry download up to 3 times to guard against network blips
                    downloaded = False
                    for dl_attempt in range(3):
                        try:
                            dl_req = urllib.request.Request(v_url, headers={"User-Agent": "Nova-TVC/1.0", "Authorization": f"Bearer {api_key}"})
                            with urllib.request.urlopen(dl_req, timeout=90) as dresp, open(clip_path, "wb") as out_f:
                                out_f.write(dresp.read())
                            if clip_path.exists() and clip_path.stat().st_size > 10000:
                                downloaded = True
                                break
                        except Exception as dl_err:
                            print(f"[GROK VIDEO] Download attempt {dl_attempt+1}/3 failed for Scene {scene_idx}: {dl_err}")
                            time.sleep(2)
                            
                    if not downloaded:
                        raise RuntimeError(f"Failed to download completed Grok video after 3 attempts: {v_url}")

                    # Auto-align lip-sync lead-in: If speech starts immediately at t=0 (<0.1s) without pause,
                    # add a micro 250ms delay so mouth opening and spoken voice start in perfect synchrony
                    if has_model and clip_path.exists() and clip_path.stat().st_size > 10000:
                        try:
                            p_cmd = ["ffprobe", "-v", "error", "-select_streams", "a", "-show_entries", "stream=codec_name", "-of", "default=noprint_wrappers=1:nokey=1", str(clip_path)]
                            has_audio = bool(subprocess.check_output(p_cmd).decode().strip())
                            if has_audio:
                                s_cmd = ["ffmpeg", "-i", str(clip_path), "-af", "silencedetect=noise=-35dB:d=0.08", "-f", "null", "-"]
                                s_res = subprocess.run(s_cmd, capture_output=True, text=True)
                                has_initial_silence = ("silence_start: 0" in s_res.stderr and "silence_duration: 0.2" in s_res.stderr)
                                if not has_initial_silence:
                                    synced_tmp = clip_path.parent / f"synced_{clip_path.name}"
                                    sync_cmd = ["ffmpeg", "-y", "-i", str(clip_path), "-af", "adelay=250|250", "-c:v", "copy", "-c:a", "aac", "-shortest", str(synced_tmp)]
                                    sync_res = subprocess.run(sync_cmd, capture_output=True)
                                    if sync_res.returncode == 0 and synced_tmp.exists() and synced_tmp.stat().st_size > 10000:
                                        synced_tmp.replace(clip_path)
                                        print(f"[GROK VIDEO] Scene {scene_idx}: Auto-aligned lip-sync lead-in (+250ms)")
                        except Exception as sync_err:
                            print(f"[GROK VIDEO] Scene {scene_idx}: lip-sync alignment check skipped: {sync_err}")

                    print(f"[GROK VIDEO] Job {job_id} Scene {scene_idx}: completed in {int(time.time() - t0)}s! Saved to {clip_path}")
                    return clip_path
                elif status in ("failed", "error"):
                    err_info = pdata.get('error')
                    err_str = str(err_info).lower()
                    is_transient = any(k in err_str for k in ["không khả dụng", "giới hạn tốc độ", "rate_limit", "server_error", "could not store video", "busy", "timeout", "temporarily"])
                    if is_transient and render_round < max_render_retries:
                        print(f"[GROK VIDEO] Job {job_id} Scene {scene_idx}: upstream transient error '{err_info}', retrying in 6s (round {render_round+1}/{max_render_retries})...")
                        time.sleep(6)
                        round_failed_transient = True
                        break
                    raise RuntimeError(f"Grok video render failed: {err_info}")
            except RuntimeError as re:
                if "render failed" in str(re) or "Failed to download" in str(re):
                    raise
                print(f"[GROK VIDEO] Scene {scene_idx} poll error: {re}")
            except urllib.error.HTTPError as he:
                print(f"[GROK VIDEO] Scene {scene_idx} poll HTTP error: {he.code}")
            except Exception as ex:
                print(f"[GROK VIDEO] Scene {scene_idx} poll transient error: {ex}")

        if round_failed_transient:
            continue
            
        raise TimeoutError(f"Grok video scene {scene_idx} timed out after 300s")

def batch_generate_videos_grok(
    scenes_to_run: list,
    job_id: str,
    job_dir: Path,
    scene_duration: int = 6,
    num_threads: int = 3,
    model: str = "xai/grok-imagine-video-1.5",
    resolution: str = "480p",
    flow_mode: str = "",
    voice_key: str = "",
    on_clip_done: callable = None,
    on_clip_failed: callable = None
) -> dict:
    """Generate multiple Grok videos concurrently using thread pool with concurrency throttling and stagger."""
    completed = {}
    max_workers = max(1, min(num_threads, len(scenes_to_run), 2))
    
    def _worker(item_and_delay):
        item, delay_s = item_and_delay
        if delay_s > 0:
            time.sleep(delay_s)
        s_idx, kf_p, m_prompt = item[:3]
        dlg = item[3] if len(item) > 3 else ""
        try:
            c_p = generate_video_grok(
                kf_p, m_prompt, s_idx,
                job_id=job_id,
                duration_s=scene_duration,
                model=model,
                resolution=resolution,
                dialogue=dlg,
                flow_mode=flow_mode,
                voice_key=voice_key
            )
            if on_clip_done:
                on_clip_done(s_idx, c_p)
            return s_idx, c_p, None
        except Exception as e:
            print(f"[GROK BATCH] Scene {s_idx} failed: {e}")
            if on_clip_failed:
                on_clip_failed(s_idx, str(e))
            return s_idx, None, str(e)
            
    items_with_delays = [(item, i * 2.0) for i, item in enumerate(scenes_to_run)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_worker, item_d) for item_d in items_with_delays]
        for fut in concurrent.futures.as_completed(futures):
            s_idx, c_p, err = fut.result()
            if c_p:
                completed[s_idx] = c_p
                
    return completed

def smart_concat_videos(
    clips: list[Path],
    output_path: Path,
    crossfade_s: float = 0.35,
    trim_start_s: float = 0.0,
    trim_end_s: float = 0.15,
    target_duration: float = None,
    bgm_path: Path = None
):
    """Seamlessly concatenate AI video clips: trim freeze frames, enforce target duration, apply crossfades, and optionally apply Auto-Ducking BGM."""
    valid_clips = [c for c in clips if c and c.exists()]
    if not valid_clips:
        raise RuntimeError("No valid clips to concatenate")

    temp_concat = output_path.parent / f"temp_concat_{output_path.name}"

    try:
        durations = []
        for c in valid_clips:
            cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", str(c)]
            dur = float(subprocess.check_output(cmd).decode().strip())
            durations.append(dur)

        if len(valid_clips) == 1:
            c = valid_clips[0]
            if target_duration and durations[0] > float(target_duration):
                subprocess.run(["ffmpeg", "-y", "-i", str(c), "-t", str(target_duration), "-c", "copy", str(temp_concat)], check=True)
            else:
                subprocess.run(["ffmpeg", "-y", "-i", str(c), "-c", "copy", str(temp_concat)], check=True)
        else:
            inputs = []
            for c in valid_clips:
                inputs.extend(["-i", str(c)])

            eff_lens = []
            filter_parts = []
            for i, dur in enumerate(durations):
                start_t = 0.0 if i == 0 else trim_start_s
                clip_limit = float(target_duration) if target_duration else dur
                effective_dur = min(dur, clip_limit)
                end_t = effective_dur if i == len(durations) - 1 else max(1.0, effective_dur - trim_end_s)
                eff_len = max(1.0, end_t - start_t)
                eff_lens.append(eff_len)

                filter_parts.append(f"[{i}:v]trim=start={start_t:.3f}:end={end_t:.3f},setpts=PTS-STARTPTS[v{i}]")
                filter_parts.append(f"[{i}:a]atrim=start={start_t:.3f}:end={end_t:.3f},asetpts=PTS-STARTPTS[a{i}]")

            last_v = "[v0]"
            last_a = "[a0]"
            curr_offset = eff_lens[0] - crossfade_s

            for i in range(1, len(valid_clips)):
                next_v = f"[v{i}]"
                next_a = f"[a{i}]"
                out_v = "[vout]" if i == len(valid_clips) - 1 else f"[v_mix_{i}]"
                out_a = "[aout]" if i == len(valid_clips) - 1 else f"[a_mix_{i}]"

                filter_parts.append(f"{last_v}{next_v}xfade=transition=fade:duration={crossfade_s:.3f}:offset={curr_offset:.3f}{out_v}")
                filter_parts.append(f"{last_a}{next_a}acrossfade=d={crossfade_s:.3f}{out_a}")

                last_v = out_v
                last_a = out_a
                if i < len(valid_clips) - 1:
                    curr_offset += eff_lens[i] - crossfade_s

            fc_str = "; ".join(filter_parts)
            ffmpeg_cmd = [
                "ffmpeg", "-y", *inputs,
                "-filter_complex", fc_str,
                "-map", "[vout]", "-map", "[aout]",
                "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "19", "-preset", "fast",
                "-c:a", "aac", "-b:a", "192k",
                str(temp_concat)
            ]
            res = subprocess.run(ffmpeg_cmd, capture_output=True, text=True)
            if res.returncode != 0 or not temp_concat.exists():
                print(f"Smart concat failed ({res.returncode}), falling back to demuxer...")
                scenes_txt = output_path.parent / "scenes.txt"
                with open(scenes_txt, "w") as f:
                    for clip in valid_clips:
                        f.write(f"file '{clip.name}'\n")
                subprocess.run(["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(scenes_txt), "-c", "copy", str(temp_concat)], check=True)

        # Apply BGM with Auto-Ducking if selected
        if bgm_path and bgm_path.exists():
            print(f"Applying Auto-Ducking BGM from {bgm_path.name}...")
            ducking_cmd = [
                "ffmpeg", "-y",
                "-i", str(temp_concat),
                "-i", str(bgm_path),
                "-filter_complex",
                "[1:a]aloop=loop=-1:size=2e+09[bgm_loop];[bgm_loop]volume=0.10[bgm_vol];[0:a][bgm_vol]amix=inputs=2:duration=first:dropout_transition=2:normalize=0[aout]",
                "-map", "0:v",
                "-map", "[aout]",
                "-c:v", "copy",
                "-c:a", "aac",
                "-b:a", "192k",
                str(output_path)
            ]
            subprocess.run(ducking_cmd, check=True)
            temp_concat.unlink(missing_ok=True)
        else:
            temp_concat.replace(output_path)

    except Exception as e:
        print(f"Smart concat exception: {e}, falling back to direct concat")
        scenes_txt = output_path.parent / "scenes.txt"
        with open(scenes_txt, "w") as f:
            for clip in valid_clips:
                f.write(f"file '{clip.name}'\n")
        subprocess.run(["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(scenes_txt), "-c", "copy", str(output_path)], check=True)

def run_pipeline_worker(job_id: str):
    job = JOBS[job_id]
    job_dir = WORK_DIR / job_id
    prod_file = job_dir / "product.jpg"
    model_file = job_dir / "model.jpg"
    bg_file = job_dir / "background.jpg" if (job_dir / "background.jpg").exists() else None
    
    # 5 Commercial Modes: 'pov', 'unboxing', 'demo', 'ugc', 'store_review'
    raw_mode = job.get("flow_mode", "pov")
    if raw_mode in ["tvc", "store_review"]:
        flow_mode = "store_review"
    elif raw_mode == "ugc":
        flow_mode = "ugc"
    elif raw_mode == "unboxing":
        flow_mode = "unboxing"
    elif raw_mode in ["demo", "demo_product", "product_demo"]:
        flow_mode = "demo"
    elif raw_mode in ["fashion", "fashion_lookbook", "lookbook", "tryon", "virtual_tryon"]:
        flow_mode = "fashion"
    else:
        flow_mode = "pov"

    # Enforce fallback or default model if no model image uploaded for modes requiring model
    if flow_mode in ["ugc", "store_review", "fashion"] and not model_file.exists():
        default_model = Path("/home/pc/flowkit/mau/human.jpg")
        if default_model.exists():
            import shutil
            shutil.copy(default_model, model_file)
            print(f"{flow_mode.upper()} mode: Auto-assigned default realistic model {default_model} -> {model_file}")
        else:
            print("No model image provided, falling back to POV mode.")
            flow_mode = "pov"

    video_engine = job.get("video_engine", "veo")
    if "veo" in str(job.get("video_model", "")).lower():
        video_engine = "veo"
    elif "grok" in str(job.get("video_model", "")).lower():
        if "10" in str(job.get("video_model", "")).lower() or "v1" in str(job.get("video_model", "")).lower():
            video_engine = "grok_10"
        else:
            video_engine = "grok_15"

    default_dur = 6 if "grok" in video_engine.lower() else 8
    scene_duration = int(job.get("scene_duration") or default_dur)
    video_res = str(job.get("resolution") or ("720p" if "veo" in video_engine.lower() else "480p")).lower()
    if video_res not in ["480p", "720p", "1080p"]:
        video_res = "720p" if "veo" in video_engine.lower() else "480p"
    num_scenes = int(job.get("num_scenes") or 3)
    num_threads = int(job.get("num_threads") or 3)
    voice_key = job.get("voice", "female_north")
    voice_info = VOICE_PROFILES.get(voice_key, VOICE_PROFILES["female_north"])
    bgm_key = job.get("bgm", "acoustic_soft")
    bgm_info = BGM_PROFILES.get(bgm_key, BGM_PROFILES["none"])

    update_job(
        job_id,
        flow_mode=flow_mode,
        video_engine=video_engine,
        resolution=video_res,
        voice_label=voice_info["label"],
        scene_duration=scene_duration,
        num_threads=num_threads,
        bgm_label=bgm_info["label"]
    )

    # Word Count Rules according to duration (natural Vietnamese speaking speed is ~2.8-3.0 words/s)
    # Dialogue must comfortably conclude before the scene transition window (at scene_duration - 0.7s)
    if scene_duration <= 4:
        word_count_rule = "EXACTLY between 8 to 10 Vietnamese words (nói vừa vặn trong ~2.8s, TUYỆT ĐỐI KHÔNG vượt quá 10 từ)"
    elif scene_duration <= 6:
        word_count_rule = "EXACTLY between 13 to 16 Vietnamese words (nói vừa vặn trong ~4.8s, TUYỆT ĐỐI KHÔNG vượt quá 16 từ)"
    elif scene_duration <= 8:
        word_count_rule = "EXACTLY between 18 to 22 Vietnamese words (nói tự nhiên vừa vặn trong ~6.5s, TUYỆT ĐỐI KHÔNG vượt quá 22 từ)"
    else: # 10s+
        word_count_rule = f"EXACTLY between 23 to 27 Vietnamese words (nói tự nhiên vừa vặn trong ~{scene_duration - 1.5:.1f}s, TUYỆT ĐỐI KHÔNG vượt quá 27 từ)"

    try:
        # ─── GIAI ĐOẠN 0: ZERO-KNOWLEDGE PROFILER (AI VISION) ────────
        profile_data = job.get("profile")
        if not profile_data:
            if flow_mode == "unboxing":
                update_job(job_id, status="ANALYZING", message="Giai đoạn 0: AI Vision (Google Gemini 3.8 Flash) đang quét sản phẩm và bối cảnh Unboxing Studio 100% Sản Phẩm...", step=1, total_steps=5)
                vision_prompt = f"""
Read and analyze the product image at {prod_file}.
You are an expert E-Commerce Creative Director & Commercial Product Photographer specializing in Luxury Studio Unboxing & Macro Product Showcases.
Extract exact details and return a strict JSON object with:
{{
  "product": {{
    "brand": "brand name or Unknown",
    "name": "detailed product name (read from label/box text if available)",
    "category": "e.g. Gấu bông, Đồ chơi, Mỹ phẩm, Phụ kiện, Gia dụng, Thiết bị",
    "material": "specific materials, texture, finish, tactile feel, reflective properties",
    "features": "key mechanisms, transformable parts, zipper, buttons, compartments, packaging",
    "details": "exact shapes, colors, labels, zippers, logos, accessories, packaging box"
  }},
  "canonical_visual_anchor": "CRITICAL: MUST BE 100% IN CONCISE ENGLISH. Precise visual description of the product items for image generation models (Banana Pro 2). Detail exact shape, color palette, textures, embroidery/embossing, logos/labels, packaging box, accessories. ALWAYS specify that the product is strictly upright, placed on a luxury aesthetic studio pedestal or gift box with soft spotlighting, and STRICTLY NO human, NO human face, and NO hands in frame.",
  "unboxing_hook": "Sensory Unboxing Reveal or Premium Craftsmanship Showcase",
  "suggested_packaging": "luxury matte gift box with magnetic lid or elegant velvet-lined presentation case",
  "suggested_studio_stage": "sleek minimalist marble pedestal or warm wooden display stage with soft dramatic studio spotlighting and subtle rim lighting"
}}
"""
                try:
                    update_job(job_id, message="Giai đoạn 0: Đang gọi AI Vision (Google Gemini 3.8 Flash) quét ảnh sản phẩm Unboxing Studio...")
                    profile_data = grok_vision_analyze(vision_prompt, [prod_file])
                    if not isinstance(profile_data, dict) or "product" not in profile_data:
                        raise ValueError("Gemini 3.8 Flash Vision returned invalid profile structure")
                except Exception as e_grok:
                    print(f"Gemini 3.8 Flash Vision error: {e_grok}, falling back to FlowKit Vision...")
                    try:
                        profile_data = flowkit_vision_analyze(vision_prompt, [prod_file])
                    except Exception as e_fk:
                        print(f"FlowKit vision-analyze fallback to CLI: {e_fk}")
                        profile_data = run_claude_cli_json(vision_prompt)

            elif flow_mode == "pov":
                update_job(job_id, status="ANALYZING", message="Giai đoạn 0: AI Vision (Google Gemini 3.8 Flash) đang bóc tách tự động sản phẩm POV...", step=1, total_steps=5)
                vision_prompt = f"""
    Read and analyze the product image at {prod_file}.
    You are an expert E-Commerce Product Specialist & TikTok Viral Strategist.
    Extract exact details and return a strict JSON object with:
    {{
      "product": {{
        "brand": "brand name or Unknown",
        "name": "detailed product name (read from label/box text if available)",
        "category": "e.g. Gấu bông, Đồ chơi, Mỹ phẩm, Phụ kiện, Gia dụng",
        "material": "specific materials, texture, tactile feel",
        "features": "key mechanisms, transformable parts, zipper, squeeze rebound, packaging",
        "details": "exact shapes, colors, labels, zippers, lids or packaging details"
      }},
      "canonical_visual_anchor": "CRITICAL: MUST BE 100% IN CONCISE ENGLISH. Precise visual description of the product characters/items for image generation models (Imagen/Nano Banana Pro). Detail exact ears (upright rounded vs floppy), eyes (color, embroidered vs bead), facial expressions, mouth, body, colors, pouches/packaging. ALWAYS specify that characters are strictly upright right-side up with heads at the top.",
      "pain_points": "key customer pain points or emotional desires this product satisfies",
      "hook_strategy": "Surprise Reveal Hook (Cú lừa thị giác) or Tactile Squeeze Hook",
      "suggested_tabletop": "clean aesthetic wooden tabletop or minimalist desk with soft warm ambient lighting"
    }}
    """
                try:
                    update_job(job_id, message="Giai đoạn 0: Đang gọi AI Vision (Google Gemini 3.8 Flash) quét ảnh sản phẩm POV...")
                    profile_data = grok_vision_analyze(vision_prompt, [prod_file])
                    if not isinstance(profile_data, dict) or "product" not in profile_data:
                        raise ValueError("Gemini 3.8 Flash Vision returned invalid profile structure")
                except Exception as e_grok:
                    print(f"Gemini 3.8 Flash Vision error: {e_grok}, falling back to FlowKit Vision...")
                    try:
                        profile_data = flowkit_vision_analyze(vision_prompt, [prod_file])
                    except Exception as e_fk:
                        print(f"FlowKit vision-analyze fallback to CLI: {e_fk}")
                        profile_data = run_claude_cli_json(vision_prompt)
    
            elif flow_mode == "ugc":
                update_job(job_id, status="ANALYZING", message="Giai đoạn 0: AI Vision (Google Gemini 3.8 Flash) đang quét sản phẩm & nhận diện chân dung Creator UGC...", step=1, total_steps=5)
                vision_prompt = f"""
    Read and analyze the product image at {prod_file} and creator portrait image at {model_file}.
    IDENTITY SOURCE RULE: the "model" and "canonical_model_anchor" fields MUST describe ONLY the person in the creator portrait image. The product image may contain a DIFFERENT person — never copy that person's hair, face, age or appearance into model fields. If the two people differ, the creator portrait always wins.
    You are an expert E-Commerce Creative Director & TikTok UGC Strategist.
    Extract exact details and return a strict JSON object with:
    {{
      "product": {{
        "brand": "brand name or Unknown",
        "name": "detailed product name",
        "category": "category of product",
        "material": "materials, texture, finish",
        "features": "key features, colors, packaging",
        "details": "exact shapes, textures, labels"
      }},
      "canonical_visual_anchor": "CRITICAL: MUST BE 100% IN CONCISE ENGLISH. Precise visual description of the product characters/items for image generation models (Imagen/Nano Banana Pro). Detail exact ears, eyes, colors, shapes, packaging, logos. CRITICAL SAFETY: Use clean product design terms (e.g. 'zipper closure', 'storage pouch', 'seam pattern'). NEVER use sexually ambiguous terms like 'slit', 'penetrating', or 'flesh'.",
      "model": {{
        "gender": "female/male",
        "age_approx": 23,
        "hair": "hair style and color",
        "clothing": "exact clothing from reference photo (colors, cut, style, straps, logos/patches, fabric)",
        "facial_features": "detailed face description, eyes, skin tone, expression"
      }},
      "canonical_model_anchor": "CRITICAL: MUST BE 100% IN CONCISE ENGLISH. Precise visual identity description of the creator for image generation models (Nano Banana Pro). Include: exact gender, mature adult age (21+), ethnicity, skin tone, eye color/contact lenses, hairstyle/color/parting, exact clothing description (cut, color, straps, text/patterns/flags), distinctive accessories (jewelry, bracelets, earrings). This anchor will be locked across all scenes to ensure zero morphing.",
      "pain_points": "daily relatable struggles (e.g. mỏi cổ vai gáy, da bí bách, phòng bừa bộn, stress cần xả)",
      "suggested_background": "cozy aesthetic personal bedroom or modern wooden work desk setup with soft warm lighting and indoor plant",
      "accent_suggested": "Nữ Miền Nam hoặc Nữ Miền Bắc phù hợp nhất với phong thái của creator"
    }}
    """
                try:
                    update_job(job_id, message="Giai đoạn 0: Đang gọi AI Vision (Google Gemini 3.8 Flash) quét ảnh sản phẩm và Creator UGC...")
                    profile_data = grok_vision_analyze(vision_prompt, [prod_file, model_file])
                    if not isinstance(profile_data, dict) or "product" not in profile_data:
                        raise ValueError("Gemini 3.8 Flash Vision returned invalid profile structure")
                except Exception as e_grok:
                    print(f"Gemini 3.8 Flash Vision error: {e_grok}, falling back to FlowKit Vision...")
                    try:
                        profile_data = flowkit_vision_analyze(vision_prompt, [prod_file, model_file])
                    except Exception as e_fk:
                        profile_data = run_claude_cli_json(vision_prompt)
    
            elif flow_mode == "demo":
                update_job(job_id, status="ANALYZING", message="Giai đoạn 0: AI Vision (Google Gemini 3.8 Flash) đang quét sản phẩm và phân tích công dụng thực tế...", step=1, total_steps=5)
                vision_prompt = f"""
    Read and analyze the product image at {prod_file}.
    You are an expert E-Commerce Product Demonstrator & Commercial TVC Director specializing in Action-Oriented Product Demonstrations and Problem-Solving TikTok Showcases.
    Extract exact details and return a strict JSON object with:
    {{
      "product": {{
        "brand": "brand name or Unknown",
        "name": "detailed product name (read from label/box text if available)",
        "category": "e.g. Mỹ phẩm, Skincare, Đồ gia dụng, Thiết bị tiện ích, Đồ chơi thông minh, Phụ kiện",
        "material": "specific materials, texture, finish, formulation or build quality",
        "features": "key mechanisms, nozzle, dispenser, buttons, texture, application method",
        "details": "exact shapes, colors, labels, bottle/packaging details, volume/capacity"
      }},
      "canonical_visual_anchor": "CRITICAL: MUST BE 100% IN CONCISE ENGLISH. Precise visual description of the product items for image generation models (Banana Pro 2). Detail exact bottle/package shape, primary color, labels/text, cap/dispenser, texture. ALWAYS specify that the product is strictly upright and clearly visible with its label facing the camera. CRITICAL SAFETY: Use clean design terms, NEVER use ambiguous terms like 'slit' or 'penetrating'.",
      "target_problem": "the primary customer pain point or daily frustration this product solves (e.g. da khô bong tróc, mụn thâm, bừa bộn, tốn thời gian)",
      "demonstration_action": "concrete physical action demonstrating the product in use (e.g. pumping a small pea-sized cream, applying gently onto skin, pressing one-touch button, demonstrating instant clean/smooth result)",
      "suggested_demo_environment": "clean aesthetic vanity table with mirror and warm ring lighting, or modern minimalist kitchen/bathroom countertop"
    }}
    """
                try:
                    update_job(job_id, message="Giai đoạn 0: Đang gọi AI Vision (Google Gemini 3.8 Flash) quét ảnh sản phẩm Demo Công Dụng...")
                    profile_data = grok_vision_analyze(vision_prompt, [prod_file])
                    if not isinstance(profile_data, dict) or "product" not in profile_data:
                        raise ValueError("Gemini 3.8 Flash Vision returned invalid profile structure")
                except Exception as e_grok:
                    print(f"Gemini 3.8 Flash Vision error: {e_grok}, falling back to FlowKit Vision...")
                    try:
                        profile_data = flowkit_vision_analyze(vision_prompt, [prod_file])
                    except Exception as e_fk:
                        profile_data = run_claude_cli_json(vision_prompt)

            elif flow_mode == "fashion":
                update_job(job_id, status="ANALYZING", message="Giai đoạn 0: AI Vision (Google Gemini 3.8 Flash) đang bóc tách thiết kế may mặc, chất liệu vải và phom dáng thời trang...", step=1, total_steps=5)
                vision_prompt = f"""
    Read and analyze the fashion garment image at {prod_file} and model portrait image at {model_file}.
    IDENTITY SOURCE RULE: the "model" and "canonical_model_anchor" fields MUST describe ONLY the person in the model portrait image. The garment image may contain a DIFFERENT person — never copy that person's hair, face, age or appearance into model fields. If the two people differ, the model portrait always wins.
    You are a High-Fashion Runway Creative Director & Haute Couture Stylist.
    Extract exact garment structure, silhouette, fabric, and model profile, returning a strict JSON object with:
    {{
      "product": {{
        "brand": "brand name or Boutique",
        "name": "detailed fashion item name (e.g. Đầm lụa xếp ly cúp ngực, Áo blazer may đo oversize, Set dạ tweed thanh lịch)",
        "category": "Thời trang thiết kế / Váy đầm / Áo sơ mi / Quần âu / Áo khoác / Set đồ",
        "material": "exact fabric texture (lụa tơ tằm, dạ tweed cao cấp, cotton organic, voan tơ bay bổng, đũi mát mịn)",
        "features": "collar style, neckline, sleeve cut, waistline fit, button accents, hemline, pleats, silhouette",
        "details": "exact color shade, fabric sheen, pattern/print, sewing lines, drapery, lining"
      }},
      "canonical_visual_anchor": "CRITICAL: MUST BE 100% IN CONCISE ENGLISH. Precise visual description of the fashion garment for image models (Banana Pro 2). Detail exact silhouette, color palette, fabric texture, neckline, sleeve cut, buttons, hemline, natural folds and drape. Strictly highlight realistic fabric physics, neat stitching, and luxury garment construction.",
      "model": {{
        "gender": "female or male",
        "age_approx": 24,
        "hair": "sleek hairstyle fitting luxury lookbook",
        "clothing": "wearing the exact specified fashion garment naturally and gracefully",
        "facial_features": "graceful confident facial expression, flawless skin tone, radiant natural makeup"
      }},
      "canonical_model_anchor": "CRITICAL: MUST BE 100% IN CONCISE ENGLISH. Description of the model: gender, mature adult age (22+), ethnicity, skin tone, hairstyle/color, graceful posture, perfectly fitted into the garment. This anchor will be locked across all scenes to ensure zero morphing.",
      "suggested_background": "Pure white studio background with soft diffuse light, or an elegant luxury minimalist boutique showroom",
      "accent_suggested": "Nữ Miền Bắc thanh lịch hoặc Nữ Miền Nam ngọt ngào"
    }}
    """
                try:
                    update_job(job_id, message="Giai đoạn 0: Đang gọi AI Vision (Google Gemini 3.8 Flash) quét chi tiết thời trang và người mẫu...")
                    profile_data = grok_vision_analyze(vision_prompt, [prod_file, model_file])
                    if not isinstance(profile_data, dict) or "product" not in profile_data:
                        raise ValueError("Gemini 3.8 Flash Vision returned invalid profile structure")
                except Exception as e_grok:
                    print(f"Gemini 3.8 Flash Vision error: {e_grok}, falling back to FlowKit Vision...")
                    try:
                        profile_data = flowkit_vision_analyze(vision_prompt, [prod_file, model_file])
                    except Exception as e_fk:
                        profile_data = run_claude_cli_json(vision_prompt)

            else: # store_review
                update_job(job_id, status="ANALYZING", message="Giai đoạn 0: AI Vision (Google Gemini 3.8 Flash) đang quét sản phẩm và KOL Review Showroom...", step=1, total_steps=5)
                vision_prompt = f"""
    Read and analyze the product image at {prod_file} and model portrait image at {model_file}.
    IDENTITY SOURCE RULE: the "model" and "canonical_model_anchor" fields MUST describe ONLY the person in the model portrait image. The product image may contain a DIFFERENT person — never copy that person's hair, face, age or appearance into model fields. If the two people differ, the model portrait always wins.
    Extract exact details for a high-end Commercial Store Review:
    {{
      "product": {{
        "brand": "brand name or Unknown",
        "name": "detailed product name",
        "category": "category of product",
        "features": "key features, colors, packaging",
        "details": "exact shapes, textures, labels"
      }},
      "canonical_visual_anchor": "CRITICAL: MUST BE 100% IN CONCISE ENGLISH. Precise visual description of the product characters/items for image generation models (Imagen/Nano Banana Pro). Detail exact features, materials, shapes, labels, luxury finish. CRITICAL SAFETY: Use clean product design terms (e.g. 'zipper closure', 'storage pouch', 'seam pattern'). NEVER use sexually ambiguous terms like 'slit', 'penetrating', or 'flesh'.",
      "model": {{
        "gender": "female/male",
        "age_approx": 24,
        "hair": "hair style and color",
        "clothing": "exact elegant showroom outfit from reference photo (colors, style, cut, jewelry)",
        "facial_features": "detailed face description, confident charismatic smile"
      }},
      "canonical_model_anchor": "CRITICAL: MUST BE 100% IN CONCISE ENGLISH. Precise visual identity description of the KOL/presenter for image generation models (Nano Banana Pro). Include: exact gender, mature adult age (22+), ethnicity, skin tone, hairstyle/color, exact outfit/clothing, and jewelry/accessories. This anchor will be locked across all scenes to ensure zero morphing.",
      "suggested_background": "sleek luxury modern showroom with glass display shelves, warm recessed spotlighting and boutique atmosphere",
      "accent_suggested": "Nữ Miền Bắc hoặc Nam Miền Bắc chuẩn mực, uy tín"
    }}
    """
                try:
                    update_job(job_id, message="Giai đoạn 0: Đang gọi AI Vision (Google Gemini 3.8 Flash) quét ảnh sản phẩm và KOL...")
                    profile_data = grok_vision_analyze(vision_prompt, [prod_file, model_file])
                    if not isinstance(profile_data, dict) or "product" not in profile_data:
                        raise ValueError("Gemini 3.8 Flash Vision returned invalid profile structure")
                except Exception as e_grok:
                    print(f"Gemini 3.8 Flash Vision error: {e_grok}, falling back to FlowKit Vision...")
                    try:
                        profile_data = flowkit_vision_analyze(vision_prompt, [prod_file, model_file])
                    except Exception as e_fk:
                        profile_data = run_claude_cli_json(vision_prompt)

            update_job(job_id, profile=profile_data)
        else:
            print(f"Job {job_id}: Reusing existing profile from previous run.")

        # ─── GIAI ĐOẠN 1: BRAIN SCRIPTING (4 TRƯỜNG DỮ LIỆU) ──────────
        mode_titles = {
            "pov": "POV Trên Tay (Góc nhìn thứ nhất)",
            "unboxing": "Unboxing Studio (100% Sản Phẩm - Không người)",
            "demo": "Demo Công Dụng (Thao tác & Hiệu quả thực tế)",
            "ugc": "UGC (Người dùng thật - Phòng riêng)",
            "store_review": "Review Cửa Hàng (Showroom sang trọng)",
            "fashion": "Thời Trang AI (Fashion Lookbook 15 Dáng Studio)"
        }
        update_job(
            job_id,
            status="SCRIPTING",
            message=f"Giai đoạn 1: Đạo diễn Google Gemini 3.8 Flash đang biên kịch chuẩn 4 trường dữ liệu ({mode_titles[flow_mode]}, {num_scenes} cảnh, {scene_duration}s/cảnh, Giọng {voice_info['label']})...",
            step=2
        )

        canonical_anchor = profile_data.get("canonical_visual_anchor") or profile_data.get("product", {}).get("details", "") or profile_data.get("product", {}).get("features", "")
        canonical_model_anchor = profile_data.get("canonical_model_anchor", "")
        if not canonical_model_anchor and flow_mode in ["ugc", "store_review", "fashion"]:
            m = profile_data.get("model", {})
            parts = []
            if m.get("gender"): parts.append(f"{m.get('gender')}")
            if m.get("age_approx"): parts.append(f"around {m.get('age_approx')} years old")
            if m.get("facial_features"): parts.append(m.get("facial_features"))
            if m.get("hair"): parts.append(m.get("hair"))
            if m.get("clothing"): parts.append(m.get("clothing"))
            canonical_model_anchor = ", ".join(parts)

        # Sanitize anchors immediately so clean, modest anchors flow into all downstream prompts
        canonical_anchor = sanitize_safety_content(canonical_anchor)
        canonical_model_anchor = sanitize_safety_content(canonical_model_anchor)
        profile_data["canonical_visual_anchor"] = canonical_anchor
        profile_data["canonical_model_anchor"] = canonical_model_anchor

        # Enrich profile with e-commerce telemetry if provided
        product_name = job.get("product_name")
        product_price = job.get("product_price")
        product_discount = job.get("product_discount")
        product_shop = job.get("product_shop")
        product_highlights = job.get("product_highlights")
        script_style = job.get("script_style", "tiktok_shop")

        if "product" not in profile_data or not isinstance(profile_data["product"], dict):
            profile_data["product"] = {}
        if product_name:
            profile_data["product"]["name"] = product_name
            profile_data["product"]["title"] = product_name
        if product_price:
            profile_data["product"]["price"] = product_price
        if product_discount:
            profile_data["product"]["discount"] = product_discount
        if product_shop:
            profile_data["product"]["shop"] = product_shop
        if product_highlights:
            profile_data["product"]["highlights"] = product_highlights

        profile_data["voice_bible"] = VOICE_BIBLE.get(voice_key, VOICE_BIBLE["female_north"])
        update_job(job_id, profile=profile_data)

        # Build persona rules & audience guidance
        model_data = profile_data.get("model", {}) if isinstance(profile_data.get("model"), dict) else {}
        model_gender = model_data.get("gender", "")
        persona_info = determine_product_persona(
            product_name=product_name or "",
            product_category=profile_data.get("product", {}).get("category", ""),
            highlights=product_highlights or "",
            voice_key=voice_key,
            model_gender=model_gender
        )
        persona_guidance = persona_info["guidance"]
        forbidden_clause = ""
        if persona_info["forbidden_words"]:
            forbidden_list_str = ", ".join(f"'{w}'" for w in persona_info["forbidden_words"])
            forbidden_clause = f"TUYỆT ĐỐI CẤM SỬ DỤNG CÁC TỪ: {forbidden_list_str} trong kịch bản!"
        openers_str = ", ".join(f"'{o}'" for o in persona_info["recommended_openers"])
        example_opener = persona_info["recommended_openers"][0]
        persona_prompt_block = f"""CRITICAL TARGET AUDIENCE & PERSONA RULES:
{persona_guidance}
{f"- {forbidden_clause}" if forbidden_clause else ""}
- CÂU MỞ ĐẦU SCENE 1 BẮT BUỘC CHỌN 1 TRONG CÁC CÁCH XƯNG HÔ PHÙ HỢP: {openers_str}. TUYỆT ĐỐI KHÔNG mở đầu bừa bãi hay mặc định 'Mấy bà ơi'!"""

        # Build exact skeleton with ALL requested scenes
        if flow_mode == "unboxing":
            skeleton_items = [
                {
                    "scene_id": i + 1,
                    "duration_seconds": scene_duration,
                    "audio_dialogue": f"Lời thoại tiếng Việt tự nhiên cho Phân Cảnh {i+1} ({word_count_rule}).",
                    "visual_plan": f"Mô tả bối cảnh đập hộp hé lộ và chi tiết sản phẩm cho Phân Cảnh {i+1}...",
                    "image_generation_prompt": f"PRODUCT REFERENCE LOCK — HIGHEST PRIORITY: {canonical_anchor}. [Action, unboxing reveal, macro camera angle for Scene {i+1}]. Purely product showcase on luxury display pedestal or aesthetic box, completely empty of people, NO human, NO human face, NO hands, NO arms. Photorealistic 8k vertical 9:16.",
                    "video_motion_prompt": f"PRODUCT LOCK — HIGHEST PRIORITY. KEEP PRODUCT ALMOST STATIC. [Smooth cinematic camera push-in, macro pan or gentle orbit for Scene {i+1}]. Purely product showcase, NO human, NO hands, NO people visible in frame. Say: \\\"[exact audio_dialogue for Scene {i+1}]\\\" in {voice_info['say_clause']}. {voice_info['veo_prompt']}"
                } for i in range(num_scenes)
            ]
        elif flow_mode == "demo":
            skeleton_items = [
                {
                    "scene_id": i + 1,
                    "duration_seconds": scene_duration,
                    "audio_dialogue": f"Lời thoại tiếng Việt tự nhiên cho Phân Cảnh {i+1} ({word_count_rule}).",
                    "visual_plan": f"Mô tả bối cảnh và thao tác demo chi tiết cho Phân Cảnh {i+1}...",
                    "image_generation_prompt": f"PRODUCT REFERENCE LOCK — HIGHEST PRIORITY: {canonical_anchor}. [Action demonstrating product use with 2 neat hands on {profile_data.get('suggested_demo_environment', 'clean aesthetic tabletop')} for Scene {i+1}]. Exactly 2 natural human hands, 5 fingers each, interacting gently without covering the product label. Photorealistic 8k vertical 9:16.",
                    "video_motion_prompt": f"PRODUCT LOCK — HIGHEST PRIORITY. KEEP PRODUCT ALMOST STATIC. Hands must stay stable with MINIMAL slow movement. [Clear product demonstration action for Scene {i+1}]. Say: \\\"[exact audio_dialogue for Scene {i+1}]\\\" in {voice_info['say_clause']}. {voice_info['veo_prompt']}"
                } for i in range(num_scenes)
            ]
        elif flow_mode == "pov":
            skeleton_items = [
                {
                    "scene_id": i + 1,
                    "duration_seconds": scene_duration,
                    "audio_dialogue": f"Lời thoại tiếng Việt tự nhiên cho Phân Cảnh {i+1} ({word_count_rule}).",
                    "visual_plan": f"Mô tả bối cảnh và hành động chi tiết cho Phân Cảnh {i+1}...",
                    "image_generation_prompt": f"PRODUCT REFERENCE LOCK — HIGHEST PRIORITY: {canonical_anchor}. [Action and camera angle for Scene {i+1}]. Photorealistic 8k vertical 9:16.",
                    "video_motion_prompt": f"PRODUCT LOCK — HIGHEST PRIORITY: [Subtle movement for Scene {i+1}]. Say: \\\"[exact audio_dialogue for Scene {i+1}]\\\" in {voice_info['say_clause']}. {voice_info['veo_prompt']}"
                } for i in range(num_scenes)
            ]
        elif flow_mode == "fashion":
            fashion_poses_en = [
                "Full body frontal view, graceful runway standing pose, completely showcasing the garment silhouette and natural drape",
                "Medium close-up shot focusing on the luxury fabric texture, sewing seams, collar line, and buttons",
                "Three-quarter angle turnaround pose with natural gentle movement, highlighting waistline fit and graceful movement"
            ]
            skeleton_items = [
                {
                    "scene_id": i + 1,
                    "duration_seconds": scene_duration,
                    "audio_dialogue": f"Lời thoại tiếng Việt tự nhiên cho Phân Cảnh {i+1} ({word_count_rule}).",
                    "visual_plan": f"Mô tả góc chụp thời trang và cử chỉ người mẫu cho Phân Cảnh {i+1}...",
                    "image_generation_prompt": f"CREATOR REFERENCE LOCK — HIGHEST PRIORITY: {canonical_model_anchor}.. PRODUCT REFERENCE LOCK: {canonical_anchor}.. {fashion_poses_en[i % len(fashion_poses_en)]}. Soft luxury studio lighting, pure elegant setting, natural fabric folds, clean composition, no watermark, no text. Photorealistic 8k vertical 9:16.",
                    "video_motion_prompt": f"PRODUCT LOCK — HIGHEST PRIORITY. KEEP PRODUCT ALMOST STATIC. Model maintains elegant poised posture with subtle head movement, gently turning or posing naturally to show off the outfit fit. Direct camera eye contact. Say: \\\"[exact audio_dialogue for Scene {i+1}]\\\" in {voice_info['say_clause']}. {voice_info['veo_prompt']}"
                } for i in range(num_scenes)
            ]
        else:
            skeleton_items = [
                {
                    "scene_id": i + 1,
                    "duration_seconds": scene_duration,
                    "audio_dialogue": f"Lời thoại tiếng Việt tự nhiên cho Phân Cảnh {i+1} ({word_count_rule}).",
                    "visual_plan": f"Mô tả bối cảnh và hành động chi tiết cho Phân Cảnh {i+1}...",
                    "image_generation_prompt": f"CREATOR REFERENCE LOCK — HIGHEST PRIORITY: {canonical_model_anchor}. PRODUCT REFERENCE LOCK: {canonical_anchor}. [Action and angle for Scene {i+1}]. Direct camera eye contact, upright frontal posture, identical face and identical clothing across all scenes. Photorealistic 8k vertical 9:16.",
                    "video_motion_prompt": f"CREATOR LOCK — HIGHEST PRIORITY: [Creator movement and product interaction for Scene {i+1}]. Direct camera eye contact, upright head posture, subtle facial movements only. Say: \\\"[exact audio_dialogue for Scene {i+1}]\\\" in {voice_info['say_clause']}. {voice_info['veo_prompt']}"
                } for i in range(num_scenes)
            ]
        skeleton_json = json.dumps(skeleton_items, indent=2, ensure_ascii=False)

        # Build specific prompt for the chosen mode
        if flow_mode == "unboxing":
            script_prompt = f"""
You are a world-class Commercial Film Director specializing in Luxury Product Unboxing & Cinematic Macro Commercials.
Product Profile: {json.dumps(profile_data['product'], ensure_ascii=False)}
Canonical Visual Lock: {canonical_anchor}
Packaging Style: {profile_data.get('suggested_packaging', 'luxury gift box with magnetic lid')}
Studio Stage: {profile_data.get('suggested_studio_stage', 'minimalist aesthetic display pedestal with studio lighting')}

CRITICAL UNBOXING PRODUCTION RULES:
1. LOẠI HÌNH: Unboxing Studio (Khám phá & Đập hộp 100% Sản Phẩm - HOÀN TOÀN KHÔNG CÓ NGƯỜI).
2. PURE PRODUCT FOCUS - ZERO HUMANS: TUYỆT ĐỐI KHÔNG CÓ NGƯỜI, KHÔNG CÓ MẶT NGƯỜI, KHÔNG CÓ BÀN TAY, KHÔNG CÓ CÁNH TAY TRONG KHUNG HÌNH (NO human, NO human face, NO hands, NO arms, NO limbs in frame). Everything is automated, magical, or purely staged product photography!
3. ZERO-MORPHING & VISUAL IDENTITY LOCK: The product appearance is 100% FROZEN across all scenes! Every scene's "image_generation_prompt" MUST preserve the exact canonical appearance ({canonical_anchor}). NEVER change shapes, colors, or details.
4. PROMPTS MUST BE 100% IN ENGLISH: "image_generation_prompt" and "video_motion_prompt" MUST be written in concise English only!
5. ZERO ON-SCREEN TEXT OR GRAPHICS: TUYỆT ĐỐI KHÔNG CÓ CHỮ TRÊN MÀN HÌNH (NO on-screen text, NO subtitles, NO captions, NO banners, NO watermarks). NEVER use phrases like '2-in-1' or '3-in-1' in prompts; use descriptive words like 'dual-feature' or 'multifunctional' instead.
6. UNBOXING NARRATIVE FLOW:
   - SCENE 1 (THE REVEAL): Premium packaging/gift box elegantly opening or sliding open to reveal the pristine product inside on a luxury pedestal with dramatic spotlight reveal.
   - SCENE 2 (MACRO CRAFTSMANSHIP): Extreme close-up / macro shot highlighting the premium material texture, stitching, fine details, logo, craftsmanship, or modular transformation.
   - SCENE 3 (HERO SHOWCASE & CTA): Hero slow cinematic orbit or low-angle presentation of the complete product with glowing boutique studio backdrop, leaving maximum desire to own.
   - SCENE CONTINUITY LOCK (LIỀN MẠCH — BẮT BUỘC): All {num_scenes} scenes happen in ONE continuous moment on the SAME studio stage ({profile_data.get('suggested_studio_stage', 'minimalist aesthetic display pedestal')}), SAME lighting, SAME packaging arrangement. Every scene's "visual_plan" and "image_generation_prompt" MUST reuse that exact environment verbatim — NEVER invent a new backdrop, room, or lighting in later scenes. Scene N+1 opens from the exact state where Scene N ended (product position, box lid state, camera direction). Only camera framing evolves (push-in → macro → orbit); the physical world stays identical — the viewer must feel ONE unbroken unboxing, not 3 separate shots.
7. CAMERA MOVEMENT: Smooth cinematic camera work — slow push-in, subtle pedestal up, macro slide, slow orbit. Always keep movement gentle and steady (KEEP PRODUCT ALMOST STATIC).
8. TOTAL SCENES: EXACTLY {num_scenes} SCENES.
9. EACH SCENE DURATION: EXACTLY {scene_duration} SECONDS.
10. DIALOGUE WORD LIMIT: Each scene's "audio_dialogue" MUST contain {word_count_rule} to perfectly fit speaking in {scene_duration} seconds!
11. VOICE STYLE & DIALECT: {voice_info['label']} — {voice_info['tone_desc']}
12. TONE & VOCABULARY: Hào hứng đập hộp, thán phục chất lượng hoàn thiện, trầm trồ về độ tỉ mỉ ('Hôm nay cùng mình unbox siêu phẩm...', '{example_opener} xem ngay em này...', 'Từng chi tiết sắc nét đến ngỡ ngàng...', 'Đúng chuẩn hàng cao cấp, nhìn là muốn rinh ngay!').
13. {persona_prompt_block}
14. ZERO CONTENT-POLICY VIOLATIONS (STRICT G-RATED COMMERCIAL STANDARD): TUYỆT ĐỐI KHÔNG VI PHẠM CHÍNH SÁCH KIỂM DUYỆT CỦA GOOGLE VEO & xAI GROK! NEVER use ambiguous tactile or suggestive words in prompts or dialogue (NO 'slit', NO 'unzip slit', NO 'pulling out of slit', NO 'rubbing', NO 'stroking'). Use clean commercial packaging terms: 'opening the presentation box', 'revealing the plush character', 'showcasing fine craftsmanship and soft texture'.
15. OUTPUT EXACTLY {num_scenes} SCENES matching the template below. You MUST complete every scene from 1 to {num_scenes}. DO NOT return fewer than {num_scenes} scenes!

Return ONLY the completed JSON array of EXACTLY {num_scenes} scenes:
{skeleton_json}
"""
        elif flow_mode == "pov":
            script_prompt = f"""
You are an award-winning TikTok Director specializing in POV First-Person Unboxing & Sensory Product Reviews.
Product Profile: {json.dumps(profile_data['product'], ensure_ascii=False)}
Canonical Visual Lock: {canonical_anchor}
Customer Pain Points: {profile_data.get('pain_points', '')}
Hook Strategy: {profile_data.get('hook_strategy', 'Surprise Reveal Hook')}
Tabletop Environment: {profile_data.get('suggested_tabletop', 'clean aesthetic wooden tabletop')}

CRITICAL POV PRODUCTION RULES:
1. LOẠI HÌNH: POV (Point of View - Góc nhìn thứ nhất của người dùng, nhìn từ trên xuống mặt bàn).
2. ZERO-MORPHING & VISUAL IDENTITY LOCK: The character/product appearance is 100% FROZEN across all scenes! Every scene's "image_generation_prompt" MUST preserve the exact canonical appearance ({canonical_anchor}). NEVER invent, alter or contradict features in later scenes (DO NOT change ears from upright to floppy, DO NOT change eye colors from purple to black, DO NOT change mouth/expression).
3. PROMPTS MUST BE 100% IN ENGLISH: "image_generation_prompt" and "video_motion_prompt" MUST be written in concise English only!
4. ZERO ON-SCREEN TEXT OR GRAPHICS: TUYỆT ĐỐI KHÔNG CÓ CHỮ TRÊN MÀN HÌNH (NO on-screen text, NO subtitles, NO captions, NO typography, NO watermark, NO logo overlay). NEVER use hyphenated slogans like '2-in-1' or '3-in-1' in prompts; use 'dual-design' or 'multifunctional' instead.
5. STORYTELLING PACING: For multi-item or transformable products, SCENE 1 MUST focus on unboxing/opening ONLY ONE ITEM with 2 hands (e.g. opening the first pouch to reveal character 1, while the second item rests closed on the side). DO NOT attempt to open 2 separate items at once with 2 hands! SCENE 2 focuses on opening the second item and gentle tactile squeezing. SCENE 3 presents both opened items side by side.
   - SCENE CONTINUITY LOCK (LIỀN MẠCH — BẮT BUỘC): All {num_scenes} scenes happen in ONE continuous moment on the SAME tabletop ({profile_data.get('suggested_tabletop', 'clean aesthetic wooden tabletop')}), SAME top-down POV angle, SAME lighting, SAME hands. Every scene's "visual_plan" and "image_generation_prompt" MUST reuse that exact environment verbatim — NEVER change the table, background, or hand appearance between scenes. Scene N+1 opens from the exact state where Scene N ended (item positions carry over). Only the action progresses; the physical world stays identical — the viewer must feel ONE unbroken POV take, not 3 separate clips.
6. STRICT UPRIGHT POSTURE: All characters and pouches must be strictly upright (heads at the top, ears pointing upwards, face looking forward right-side up). NEVER upside down, never inverted, never tumbling head-first!
7. HAND ANATOMY: Exactly 2 natural human hands in frame, 5 fingers each. NO third hand, NO extra floating limbs.
8. TOTAL SCENES: EXACTLY {num_scenes} SCENES.
9. EACH SCENE DURATION: EXACTLY {scene_duration} SECONDS.
10. DIALOGUE WORD LIMIT: Each scene's "audio_dialogue" MUST contain {word_count_rule} to perfectly fit speaking in {scene_duration} seconds!
11. VOICE STYLE & DIALECT: {voice_info['label']} — {voice_info['tone_desc']}
12. TONE & VOCABULARY: Thân mật, cảm xúc, khen chất liệu, đập hộp bất ngờ ('Bất ngờ chưa mọi người...', 'Nhìn tưởng... nhưng mở ra là...', 'Cầm lên tay là ưng luôn...', 'Chất lượng xuất sắc thực sự').
13. {persona_prompt_block}
14. NO HUMAN FACE: TUYỆT ĐỐI KHÔNG CÓ MẶT NGƯỜI TRONG KHUNG HÌNH (NO human face visible in frame).
15. ZERO CONTENT-POLICY VIOLATIONS (STRICT G-RATED COMMERCIAL STANDARD): TUYỆT ĐỐI KHÔNG VI PHẠM CHÍNH SÁCH KIỂM DUYỆT CỦA GOOGLE VEO & xAI GROK! NEVER use ambiguous tactile or suggestive words in prompts or dialogue (NO 'slit', NO 'unzip slit', NO 'pulling out of slit', NO 'rubbing', NO 'stroking', NO 'penetrate'). For zippered pouches or plush toys: describe as 'gently opening the pouch to reveal the cute character', 'softly pressing the plush toy', 'displaying the adorable character upright'.
16. OUTPUT EXACTLY {num_scenes} SCENES matching the template below. You MUST complete every scene from 1 to {num_scenes}. DO NOT return fewer than {num_scenes} scenes!

Return ONLY the completed JSON array of EXACTLY {num_scenes} scenes:
{skeleton_json}
"""
        elif flow_mode == "demo":
            script_prompt = f"""
You are an award-winning Commercial Director specializing in High-Converting TikTok Product Demos & Problem-Solution TVCs.
Product Profile: {json.dumps(profile_data['product'], ensure_ascii=False)}
Canonical Visual Lock: {canonical_anchor}
Customer Problem / Pain Point: {profile_data.get('target_problem', 'vấn đề thường gặp cần giải quyết')}
Demonstration Action: {profile_data.get('demonstration_action', 'thao tác sử dụng trực quan')}
Demo Setting: {profile_data.get('suggested_demo_environment', 'clean aesthetic countertop')}

CRITICAL PRODUCT DEMO PRODUCTION RULES:
1. LOẠI HÌNH: Demo Công Dụng (Trình diễn tính năng, thao tác sử dụng thực tế và kết quả trước - sau rõ rệt).
2. ZERO-MORPHING & VISUAL IDENTITY LOCK: The product appearance is 100% FROZEN across all scenes! Every scene's "image_generation_prompt" MUST preserve the exact canonical appearance ({canonical_anchor}). Keep identical shape, packaging, label, logo, and color.
3. PROMPTS MUST BE 100% IN ENGLISH: "image_generation_prompt" and "video_motion_prompt" MUST be written in concise English only!
4. ZERO ON-SCREEN TEXT OR GRAPHICS: TUYỆT ĐỐI KHÔNG CÓ CHỮ TRÊN MÀN HÌNH (NO on-screen text, NO subtitles, NO captions, NO typography, NO watermark, NO banners). NEVER write '2-in-1', '3-in-1', or '50% off' in prompts.
5. 3-PHASE NARRATIVE STRUCTURE:
   - SCENE 1 (HOOK & NỖI ĐAU): Nêu bật vấn đề nan giải hoặc thói quen sai lầm khiến người xem khó chịu (ví dụ da khô, đồ đạc bừa bộn, vết bẩn cứng đầu), ngay lập tức đưa ra sản phẩm như vị cứu tinh.
   - SCENE 2 (THAO TÁC SỬ DỤNG TRỰC QUAN): 2 bàn tay hướng dẫn thao tác chi tiết từng bước (thoa đều, ấn nút, xịt dưỡng, xoay nắp, cắt gọt...), cảm nhận chất liệu/kết cấu biến đổi.
   - SCENE 3 (KẾT QUẢ THỰC TẾ & KÊU GỌI CTA): Chứng minh kết quả mỹ mãn (bề mặt căng bóng, sáng bóng, tiện lợi vượt trội) + kêu gọi bấm vào giỏ hàng góc trái màn hình để nhận ưu đãi độc quyền.
   - SCENE CONTINUITY LOCK (LIỀN MẠCH — BẮT BUỘC): All {num_scenes} scenes happen in ONE continuous moment at the SAME setting ({profile_data.get('suggested_demo_environment', 'clean aesthetic countertop')}), SAME lighting, SAME pair of hands, SAME product position. Every scene's "visual_plan" and "image_generation_prompt" MUST reuse that exact environment verbatim — NEVER teleport to a new room, surface, or lighting in later scenes. Scene N+1 opens from the exact state where Scene N ended (product and hands carry over). Only the demonstration action progresses; the physical world stays identical — the viewer must feel ONE unbroken demo, not 3 separate clips.
6. KEEP PRODUCT ALMOST STATIC: In video motion, avoid violent shaking, flipping or tossing. Movement must be steady, slow, and focused on the hands interacting with the product.
7. TOTAL SCENES: EXACTLY {num_scenes} SCENES.
8. EACH SCENE DURATION: EXACTLY {scene_duration} SECONDS.
9. DIALOGUE WORD LIMIT: Each scene's "audio_dialogue" MUST contain {word_count_rule} to perfectly fit speaking in {scene_duration} seconds!
10. VOICE STYLE & DIALECT: {voice_info['label']} — {voice_info['tone_desc']}
11. TONE & VOCABULARY: Chuyên gia hướng dẫn tận tình, thuyết phục bằng hiệu quả thực tế ('Bác nào đang gặp tình trạng... thì xem ngay nhé', 'Chỉ cần một lượng nhỏ thế này thôi...', 'Nhìn hiệu quả sau khi dùng mê thực sự...', 'Bấm ngay vào giỏ hàng bên dưới để trải nghiệm nha').
12. {persona_prompt_block}
13. ZERO CONTENT-POLICY VIOLATIONS (STRICT G-RATED COMMERCIAL STANDARD): TUYỆT ĐỐI KHÔNG VI PHẠM CHÍNH SÁCH KIỂM DUYỆT CỦA GOOGLE VEO & xAI GROK! Keep all product demonstrations strictly professional, clean, and family-friendly. Use clear commercial verbs: 'applying gently', 'pressing one-touch button', 'showcasing the smooth finish'.
14. OUTPUT EXACTLY {num_scenes} SCENES matching the template below. You MUST complete every scene from 1 to {num_scenes}. DO NOT return fewer than {num_scenes} scenes!

Return ONLY the completed JSON array of EXACTLY {num_scenes} scenes:
{skeleton_json}
"""
        elif flow_mode == "ugc":
            script_prompt = f"""
You are a Hollywood TikTok UGC Creative Director specializing in authentic, high-converting Creator Testimonials.
Product Profile: {json.dumps(profile_data['product'], ensure_ascii=False)}
Creator Visual Lock: {canonical_model_anchor}
Product Visual Lock: {canonical_anchor}
Suggested Setting: {profile_data.get('suggested_background', 'cozy personal bedroom with desk setup')}

CRITICAL UGC PRODUCTION RULES:
1. LOẠI HÌNH: UGC (User Generated Content - Người dùng thật chia sẻ kinh nghiệm tại phòng riêng).
2. ZERO-MORPHING & CHARACTER CONTINUITY: The creator's appearance, face, hair, and exact clothing are 100% FROZEN across all scenes! Every scene's "image_generation_prompt" MUST preserve both the CREATOR REFERENCE LOCK ({canonical_model_anchor}) and PRODUCT REFERENCE LOCK ({canonical_anchor}). NEVER change the creator's outfit, face, or hairstyle in later scenes!
3. FACIAL IDENTITY & DIRECT EYE CONTACT LOCK: The creator MUST maintain direct camera eye contact with upright, natural frontal posture looking straight at the lens. NEVER tell the creator to tilt head ('tilts head'), roll head sideways, or press the product against their cheek ('presses against cheek'). Tilting or pressing against face deforms facial geometry and causes face desync/morphing in AI video generation! The creator must hold the product comfortably in front near chest level.
4. ZERO ON-SCREEN TEXT OR GRAPHICS: TUYỆT ĐỐI KHÔNG CÓ CHỮ TRÊN MÀN HÌNH (NO on-screen text, NO subtitles, NO captions, NO typography, NO watermark, NO banners). NEVER write '2-in-1', '3-in-1', '50% off', or numeric slogans in prompts.
5. PROMPTS MUST BE 100% IN ENGLISH: "image_generation_prompt" and "video_motion_prompt" MUST be written in concise English only!
6. TOTAL SCENES: EXACTLY {num_scenes} SCENES.
7. EACH SCENE DURATION: EXACTLY {scene_duration} SECONDS.
8. DIALOGUE WORD LIMIT: Each scene's "audio_dialogue" MUST contain {word_count_rule} to perfectly fit speaking in {scene_duration} seconds!
9. VOICE STYLE & DIALECT: {voice_info['label']} — {voice_info['tone_desc']}
10. TONE & VOCABULARY: Đời thường, gần gũi, khuyên dùng thật lòng, tâm sự chân thật (ví dụ: 'Mình dùng được 2 tuần nay rồi...', '{example_opener} chân ái đây rồi...', 'Nói thật lúc đầu mình cũng đắn đo nhưng cầm lên tay là mê thực sự...', 'Đáng đồng tiền bát gạo luôn nha').
11. {persona_prompt_block}
12. CAMERA & ENVIRONMENT: Frontal camera / selfie close-up angle, creator sitting in personal room/desk, holding product naturally, genuine smiles, natural lighting.
13. SCENE CONTINUITY LOCK (LIỀN MẠCH — BẮT BUỘC): All {num_scenes} scenes happen in ONE continuous take in the SAME room ({profile_data.get('suggested_background', 'cozy personal bedroom with desk setup')}), SAME creator position, SAME outfit, SAME lighting, SAME framing. Every scene's "visual_plan" and "image_generation_prompt" MUST reuse that exact environment verbatim — NEVER move the creator to a new location, change the background, or re-pose them between scenes. Scene N+1 opens from the exact state where Scene N ended (product in hand, posture, camera distance carry over). Only dialogue and small gestures progress; the physical world stays identical — the viewer must feel ONE unbroken talking take, not 3 separate shots.
14. ZERO CONTENT-POLICY VIOLATIONS (STRICT G-RATED COMMERCIAL STANDARD): TUYỆT ĐỐI KHÔNG VI PHẠM CHÍNH SÁCH KIỂM DUYỆT CỦA GOOGLE VEO VÀ xAI GROK!
    - Preserve the creator's exact outfit and appearance ({canonical_model_anchor}) with 100% consistency across all scenes.
    - NEVER use ambiguous tactile actions in prompts or dialogue (NO 'unzipping slit', NO 'smooth zipper', NO 'pulling out of slit', NO 'khóa kéo mở bung', NO 'rubbing', NO 'stroking').
    - For product interaction: keep gestures natural, gentle, and commercial: 'holding product comfortably near chest level', 'pointing gently at feature', 'gently opening the presentation pouch to reveal the cute character'.
15. OUTPUT EXACTLY {num_scenes} SCENES matching the template below. You MUST complete every scene from 1 to {num_scenes}. DO NOT return fewer than {num_scenes} scenes!

Return ONLY the completed JSON array of EXACTLY {num_scenes} scenes:
{skeleton_json}
"""
        elif flow_mode == "fashion":
            script_prompt = f"""
You are an International Haute Couture Creative Director & Commercial Fashion Lookbook Producer specializing in E-Commerce Fashion, Virtual Try-On, and Runway Showcases.
Product (Garment) Profile: {json.dumps(profile_data['product'], ensure_ascii=False)}
Fashion Model Visual Lock: {canonical_model_anchor}
Garment Reference Lock: {canonical_anchor}
Studio Setting: {profile_data.get('suggested_background', 'Pure white studio background or luxury boutique runway with soft professional studio lighting')}

CRITICAL FASHION LOOKBOOK PRODUCTION RULES:
1. LOẠI HÌNH: THỜI TRANG AI — FASHION LOOKBOOK & VIRTUAL TRY-ON (Chuẩn 15 Dáng Studio Quốc Tế).
2. ZERO-MORPHING & OUTFIT CONTINUITY:
   - Model Identity (gương mặt, kiểu tóc, màu da, vóc dáng tỉ lệ cơ thể) KHÓA 100% across all scenes ({canonical_model_anchor}).
   - Garment & Products (áo, quần, váy, đầm, đường may, hoa văn, chất liệu vải, nếp gấp) KHÓA 100% chuẩn xác theo ảnh tham chiếu ({canonical_anchor}).
   - Every scene's "image_generation_prompt" MUST start with:
     `CREATOR REFERENCE LOCK — HIGHEST PRIORITY: {canonical_model_anchor}.. PRODUCT REFERENCE LOCK: {canonical_anchor}..`
3. 15 STUDIO POSES CYCLING:
   - Scene 1: Toàn thân, nhìn thẳng / catwalk tự nhiên (Trình diễn tổng thể outfit, form dáng chuẩn).
   - Scene 2: Cận cảnh chi tiết chất liệu vải / đường may / cổ áo / tay áo (Tôn vinh chất lượng may mặc, độ rủ của vải).
   - Scene 3: Xoay người thanh lịch 360 độ hoặc góc nghiêng ba phần tư (Khoe trọn vẻ đẹp sau lưng và chuyển động bồng bềnh).
   - SCENE CONTINUITY LOCK (LIỀN MẠCH — BẮT BUỘC): All {num_scenes} scenes happen in ONE continuous fashion take in the SAME studio ({profile_data.get('suggested_background', 'pure white studio with soft professional lighting')}), SAME model, SAME outfit, SAME lighting. Every scene's "visual_plan" and "image_generation_prompt" MUST reuse that exact environment verbatim — NEVER change the backdrop, set, or lighting between scenes. Only camera framing and the model's pose progress; the physical world stays identical — the viewer must feel ONE unbroken runway take, not 3 separate shots.
4. ZERO ON-SCREEN TEXT OR GRAPHICS: TUYỆT ĐỐI KHÔNG CÓ CHỮ TRÊN MÀN HÌNH (NO on-screen text, NO subtitles, NO captions, NO typography, NO watermark, NO banners).
5. PROMPTS MUST BE 100% IN ENGLISH: "image_generation_prompt" and "video_motion_prompt" MUST be written in concise English only!
6. TOTAL SCENES: EXACTLY {num_scenes} SCENES.
7. EACH SCENE DURATION: EXACTLY {scene_duration} SECONDS.
8. DIALOGUE WORD LIMIT: Each scene's "audio_dialogue" MUST contain {word_count_rule} to perfectly fit speaking in {scene_duration} seconds!
9. VOICE STYLE & DIALECT: {voice_info['label']} — {voice_info['tone_desc']}
10. TONE & VOCABULARY: Giọng điệu tư vấn thời trang sang trọng, sành điệu, khéo léo khen form dáng, hack chiều cao, chất vải mềm mát, phối đồ dạo phố/đi tiệc/công sở (ví dụ: 'Mẫu đầm thiết kế chuẩn phom tôn dáng cực đỉnh...', 'Chất vải đũi lụa cao cấp mềm mịn, mặc lên nhẹ tênh...', 'Thiết kế chiết eo tinh tế giúp che khuyết điểm hoàn hảo...', 'Bấm ngay vào giỏ hàng góc trái để rinh ngay em này về nhé').
11. {persona_prompt_block}
12. VIDEO MOTION PROMPT FOR VEO 3.1:
    - Must start with: `PRODUCT LOCK — HIGHEST PRIORITY. KEEP PRODUCT ALMOST STATIC.`
    - Model motion: Elegant Turnaround / Boutique Walk / Catwalk poise / gentle fabric rustle.
    - Camera: Cinematic Push-in, Head-to-Toe Scan, Slow Orbit, or Gimbal Track.
    - End with: `Say: \"[exact audio_dialogue]\" in {voice_info['say_clause']}. {voice_info['veo_prompt']}`.
13. OUTPUT EXACTLY {num_scenes} SCENES matching the template below. You MUST complete every scene from 1 to {num_scenes}. DO NOT return fewer than {num_scenes} scenes!

Return ONLY the completed JSON array of EXACTLY {num_scenes} scenes:
{skeleton_json}
"""
        else: # store_review
            script_prompt = f"""
You are a Hollywood Commercial TVC Director specializing in Premium Store & Showroom Product Reviews.
Product Profile: {json.dumps(profile_data['product'], ensure_ascii=False)}
KOL Visual Lock: {canonical_model_anchor}
Product Visual Lock: {canonical_anchor}
Showroom Environment: {profile_data.get('suggested_background', 'sleek modern luxury showroom')}

CRITICAL STORE REVIEW PRODUCTION RULES:
1. LOẠI HÌNH: Review Cửa Hàng / Showroom chuyên nghiệp sang trọng.
2. ZERO-MORPHING & CHARACTER CONTINUITY: The KOL's appearance, face, hair, and exact outfit are 100% FROZEN across all scenes! Every scene's "image_generation_prompt" MUST preserve both the CREATOR REFERENCE LOCK ({canonical_model_anchor}) and PRODUCT REFERENCE LOCK ({canonical_anchor}). NEVER change outfit, face, or hairstyle in later scenes!
3. FACIAL IDENTITY & STABLE HEAD POSTURE: The KOL MUST maintain direct camera eye contact with upright, confident frontal posture looking straight at camera. NEVER instruct the KOL to tilt head or turn away. Keep hands holding product comfortably in front.
4. ZERO ON-SCREEN TEXT OR GRAPHICS: TUYỆT ĐỐI KHÔNG CÓ CHỮ TRÊN MÀN HÌNH (NO on-screen text, NO subtitles, NO captions, NO typography, NO watermark, NO banners). NEVER write '2-in-1', '3-in-1', or numeric slogans in prompts.
5. PROMPTS MUST BE 100% IN ENGLISH: "image_generation_prompt" and "video_motion_prompt" MUST be written in concise English only!
6. TOTAL SCENES: EXACTLY {num_scenes} SCENES.
7. EACH SCENE DURATION: EXACTLY {scene_duration} SECONDS.
8. DIALOGUE WORD LIMIT: Each scene's "audio_dialogue" MUST contain {word_count_rule} to perfectly fit speaking in {scene_duration} seconds!
9. VOICE STYLE & DIALECT: {voice_info['label']} — {voice_info['tone_desc']}
10. TONE & VOCABULARY: Uy tín, chuyên gia, sang trọng, đánh giá phân tích chất lượng cao cấp, phong thái tự tin.
11. {persona_prompt_block}
12. CAMERA & ENVIRONMENT: Eye-level medium / medium close-up, modern commercial showroom with luxury display shelves, professional lighting.
13. SCENE CONTINUITY LOCK (LIỀN MẠCH — BẮT BUỘC): All {num_scenes} scenes happen in ONE continuous take in the SAME showroom ({profile_data.get('suggested_background', 'modern commercial showroom with luxury display shelves')}), SAME KOL position, SAME outfit, SAME lighting, SAME framing. Every scene's "visual_plan" and "image_generation_prompt" MUST reuse that exact environment verbatim — NEVER move to a new location or re-pose between scenes. Scene N+1 opens from the exact state where Scene N ended. Only dialogue and small gestures progress; the physical world stays identical — the viewer must feel ONE unbroken take, not 3 separate shots.
14. ZERO CONTENT-POLICY VIOLATIONS (STRICT G-RATED COMMERCIAL STANDARD): TUYỆT ĐỐI KHÔNG VI PHẠM CHÍNH SÁCH KIỂM DUYỆT CỦA GOOGLE VEO VÀ xAI GROK!
    - Preserve the KOL's exact outfit and appearance ({canonical_model_anchor}) with 100% consistency across all scenes.
    - NEVER use ambiguous tactile actions in prompts or dialogue (NO 'unzipping slit', NO 'smooth zipper', NO 'pulling out of slit', NO 'khóa kéo mở bung', NO 'rubbing', NO 'stroking').
    - Maintain dignified, high-end showroom presentation standard with 100% family-friendly actions and dialogue.
15. OUTPUT EXACTLY {num_scenes} SCENES matching the template below. You MUST complete every scene from 1 to {num_scenes}. DO NOT return fewer than {num_scenes} scenes!

Return ONLY the completed JSON array of EXACTLY {num_scenes} scenes:
{skeleton_json}
"""

        # Inject script style instructions and commercial telemetry
        style_block = ""
        if script_style == "tiktok_shop":
            style_block = f"""
CRITICAL AFFILIATE SCRIPT STYLE: REVIEW BÁN HÀNG TIKTOK SHOP (HIGH CONVERSION)
- Hook cực mạnh ở Phân cảnh 1 gây chú ý lập tức, giải quyết nỗi đau hoặc hé lộ deal hot.
- Lồng ghép tự nhiên thông số: {product_name or 'sản phẩm'} với mức giá khuyến mãi {product_price or ''} {product_discount or ''} của shop {product_shop or ''}.
- Tận dụng các điểm nhấn nổi bật (USPs): {product_highlights or 'chất lượng đỉnh cao, tiện dụng'}.
- Phân cảnh cuối bắt buộc lời kêu gọi hành động (CTA) dứt khoát: 'Bấm ngay vào giỏ hàng màu vàng ở góc trái màn hình để nhận ưu đãi độc quyền nhé!'.
"""
        elif script_style == "pov_ugc":
            style_block = f"""
CRITICAL AFFILIATE SCRIPT STYLE: TRẢI NGHIỆM THỰC TẾ (POV / UGC AUTHENTIC)
- Tông giọng gần gũi, tâm sự, chia sẻ cảm nhận người dùng thật sau khi trải nghiệm sản phẩm.
- Nhấn mạnh chi tiết thực tế: {product_highlights or 'cảm giác sử dụng rất thích'}.
- Khuyên dùng chân thành, không gượng ép bán hàng: 'Nếu bạn cũng đang tìm {product_name or 'sản phẩm'} thì rất đáng thử nha'.
"""
        elif script_style == "luxury":
            style_block = f"""
CRITICAL AFFILIATE SCRIPT STYLE: QUẢNG CÁO SANG TRỌNG (LUXURY BRAND COMMERCIAL)
- Tông giọng tinh tế, đĩnh đạc, sang trọng chuẩn thương hiệu cao cấp.
- Tôn vinh tính thẩm mỹ, đường nét hoàn thiện và đẳng cấp của {product_name or 'sản phẩm'}.
- Luận điểm: {product_highlights or 'hoàn thiện tinh xảo, đẳng cấp vượt thời gian'}.
"""
        if style_block:
            script_prompt = script_prompt.replace(
                "Return ONLY the completed JSON array",
                f"{style_block}\nReturn ONLY the completed JSON array"
            )

        existing_scenes = job.get("scenes")
        if existing_scenes and len(existing_scenes) >= num_scenes:
            print(f"Job {job_id}: Reusing {len(existing_scenes)} existing scenes from previous run.")
            scenes_data = existing_scenes[:num_scenes]
        else:
            scenes_data = None
            try:
                update_job(job_id, message=f"Giai đoạn 1: Đạo diễn Google Gemini 3.8 Flash đang xuất kịch bản chuẩn {num_scenes} phân cảnh ({voice_info['label']})...")
                scenes_data = grok_script_generate(script_prompt)
                if isinstance(scenes_data, dict) and "scenes" in scenes_data:
                    scenes_data = scenes_data["scenes"]
                elif isinstance(scenes_data, dict) and not isinstance(scenes_data, list):
                    scenes_data = [scenes_data]
                if not isinstance(scenes_data, list) or len(scenes_data) == 0:
                    raise ValueError("Gemini 3.8 Flash returned invalid scenes list")
            except Exception as e_gemini_script:
                print(f"Gemini 3.8 Flash Script error: {e_gemini_script}, falling back to AGY CLI...")
                update_job(job_id, message="Giai đoạn 1: Đang biên kịch bằng Antigravity (AGY) engine dự phòng...")
                scenes_data = run_agy_cli_json(script_prompt)
                if isinstance(scenes_data, dict) and "scenes" in scenes_data:
                    scenes_data = scenes_data["scenes"]
                elif isinstance(scenes_data, dict) and not isinstance(scenes_data, list):
                    scenes_data = [scenes_data]

            # Strict scene count enforcement
            if len(scenes_data) < num_scenes:
                print(f"Model returned {len(scenes_data)} scenes, but {num_scenes} requested. Auto-completing missing scenes...")
                missing_count = num_scenes - len(scenes_data)
                follow_up_prompt = f"""
You previously returned only {len(scenes_data)} scenes, but EXACTLY {num_scenes} scenes were requested.
Product: {json.dumps(profile_data['product'], ensure_ascii=False)}
Existing scenes: {json.dumps(scenes_data, ensure_ascii=False)}

Please generate ONLY the remaining {missing_count} missing scene(s) (starting from scene_id: {len(scenes_data)+1} to {num_scenes}).
Each scene must contain: "scene_id", "duration_seconds": {scene_duration}, "audio_dialogue" ({word_count_rule}), "visual_plan", "image_generation_prompt", "video_motion_prompt".
Return ONLY a strict JSON array of the {missing_count} missing scene(s):
"""
                try:
                    missing_scenes = grok_script_generate(follow_up_prompt)
                    if isinstance(missing_scenes, dict) and "scenes" in missing_scenes:
                        missing_scenes = missing_scenes["scenes"]
                    elif isinstance(missing_scenes, dict) and not isinstance(missing_scenes, list):
                        missing_scenes = [missing_scenes]
                    if not isinstance(missing_scenes, list) or len(missing_scenes) == 0:
                        raise ValueError("No missing scenes from primary scriptwriter")
                except Exception as e_miss:
                    print(f"Error getting missing scenes from LLM: {e_miss}, trying AGY CLI...")
                    try:
                        missing_scenes = run_agy_cli_json(follow_up_prompt)
                        if isinstance(missing_scenes, dict) and "scenes" in missing_scenes:
                            missing_scenes = missing_scenes["scenes"]
                        elif isinstance(missing_scenes, dict) and not isinstance(missing_scenes, list):
                            missing_scenes = [missing_scenes]
                    except Exception as e_agy_miss:
                        print(f"AGY missing scenes error: {e_agy_miss}")
                        missing_scenes = []

                if isinstance(missing_scenes, list):
                    for idx, sc in enumerate(missing_scenes, start=len(scenes_data) + 1):
                        sc["scene_id"] = idx
                        sc["duration_seconds"] = scene_duration
                        scenes_data.append(sc)

            # Enforce exact length & sanitize safety for all generated scenes
            scenes_data = scenes_data[:num_scenes]
            for sc in scenes_data:
                if "image_generation_prompt" in sc:
                    sc["image_generation_prompt"] = sanitize_safety_content(sc["image_generation_prompt"])
                if "audio_dialogue" in sc:
                    sc["audio_dialogue"] = sanitize_safety_content(sc["audio_dialogue"])
                    sc["audio_dialogue"] = apply_persona_replacements(sc["audio_dialogue"], persona_info.get("replacements", {}))
                if "video_motion_prompt" in sc:
                    sc["video_motion_prompt"] = sanitize_safety_content(sc["video_motion_prompt"])
                    sc["video_motion_prompt"] = apply_persona_replacements(sc["video_motion_prompt"], persona_info.get("replacements", {}))
                # CRITICAL: Always synchronize motion prompt's Say: clause with dialogue for lip-sync
                if sc.get("audio_dialogue") and sc.get("video_motion_prompt"):
                    sc["video_motion_prompt"] = sync_dialogue_to_motion_prompt(
                        sc["video_motion_prompt"],
                        sc["audio_dialogue"],
                        voice_info.get("say_clause", "")
                    )
            update_job(job_id, scenes=scenes_data)

        # ─── GIAI ĐOẠN 2: KEYFRAME ANCHORING (GOOGLE BANANA PRO 2) ──
        prod_mid = upload_to_flowkit(prod_file, job_id=job_id)

        if flow_mode == "unboxing":
            update_job(job_id, status="KEYFRAMING", message="Giai đoạn 2: Nạp Token Sản Phẩm vào Banana Pro 2 (CHỈ 1 Reference duy nhất, 100% Sản phẩm Studio không người)...", step=3)
            ref_ids = [prod_mid]
            if bg_file and bg_file.exists() and bg_file.name == "background.jpg":
                bg_mid = upload_to_flowkit(bg_file, job_id=job_id)
                ref_ids.append(bg_mid)
        elif flow_mode == "demo":
            update_job(job_id, status="KEYFRAMING", message="Giai đoạn 2: Nạp Token Sản Phẩm vào Banana Pro 2 (Demo Công Dụng - Thao tác & Hiệu quả)...", step=3)
            ref_ids = [prod_mid]
            if model_file and model_file.exists():
                model_mid = upload_to_flowkit(model_file, job_id=job_id)
                ref_ids = [model_mid, prod_mid]
            if bg_file and bg_file.exists() and bg_file.name == "background.jpg":
                bg_mid = upload_to_flowkit(bg_file, job_id=job_id)
                ref_ids.append(bg_mid)
        elif flow_mode == "pov":
            update_job(job_id, status="KEYFRAMING", message="Giai đoạn 2: Nạp Token Sản Phẩm vào Banana Pro 2 (CHỈ 1 Reference duy nhất, góc nhìn POV trên tay)...", step=3)
            ref_ids = [prod_mid]
            if bg_file and bg_file.exists() and bg_file.name == "background.jpg":
                bg_mid = upload_to_flowkit(bg_file, job_id=job_id)
                ref_ids.append(bg_mid)
        elif flow_mode == "ugc":
            update_job(job_id, status="KEYFRAMING", message="Giai đoạn 2: Khóa Chân Dung Creator (Ưu tiên số 1) + Sản Phẩm trên Banana Pro 2...", step=3)
            model_mid = upload_to_flowkit(model_file, job_id=job_id)
            ref_ids = [model_mid, prod_mid]
            if bg_file and bg_file.exists() and bg_file.name == "background.jpg":
                bg_mid = upload_to_flowkit(bg_file, job_id=job_id)
                ref_ids.append(bg_mid)
        elif flow_mode == "fashion":
            update_job(job_id, status="KEYFRAMING", message="Giai đoạn 2: Khóa Nhân Dạng Người Mẫu (Tầng 1) + Chi Tiết Trang Phục Thời Trang (Tầng 2) trên Banana Pro 2...", step=3)
            model_mid = upload_to_flowkit(model_file, job_id=job_id)
            ref_ids = [model_mid, prod_mid]
            if bg_file and bg_file.exists() and bg_file.name == "background.jpg":
                bg_mid = upload_to_flowkit(bg_file, job_id=job_id)
                ref_ids.append(bg_mid)
        else: # store_review
            update_job(job_id, status="KEYFRAMING", message="Giai đoạn 2: Khóa Chân Dung KOL (Ưu tiên số 1) + Sản Phẩm trên Banana Pro 2...", step=3)
            model_mid = upload_to_flowkit(model_file, job_id=job_id)
            ref_ids = [model_mid, prod_mid]
            if bg_file and bg_file.exists() and bg_file.name == "background.jpg":
                bg_mid = upload_to_flowkit(bg_file, job_id=job_id)
                ref_ids.append(bg_mid)

        update_job(job_id, ref_ids=ref_ids)
        # ─── GIAI ĐOẠN 2 & 3: BĂNG CHUYỀN SẢN XUẤT (PIPELINED KEYFRAME & VEO 3) ──────
        update_job(job_id, status="KEYFRAMING", message=f"Khởi động Băng Chuyền Sản Xuất: Chuẩn bị âm thanh TTS và xử lý {num_scenes} phân cảnh liên tục...", step=3)

        # 1. Sinh trước toàn bộ TTS Audio (tuần tự, giãn cách 0.3s chống Edge-TTS websocket throttling)
        for i, sc in enumerate(scenes_data, start=1):
            tts_path = job_dir / f"tts_{i}.mp3"
            dlg = (sc.get("audio_dialogue") or "").strip()
            if dlg and (not tts_path.exists() or tts_path.stat().st_size < 1000):
                try:
                    generate_edge_tts(dlg, tts_path, voice=voice_info["edge_voice"], rate=voice_info.get("rate", "+0%"))
                except Exception as tts_err:
                    print(f"Job {job_id}: Warning generating TTS for scene {i}: {tts_err}")
                time.sleep(0.3)

        keyframes = [None] * num_scenes
        clips = [None] * num_scenes
        pending_ops = {}

        # Khởi tạo trạng thái chi tiết từng phân cảnh để client UI hiển thị ngay lập tức
        for idx, sc in enumerate(scenes_data, start=1):
            sc["scene_id"] = idx
            clip_path = job_dir / f"clip_{idx}.mp4"
            kf_path = job_dir / f"keyframe_{idx}.jpg"
            if clip_path.exists() and clip_path.stat().st_size > 50000:
                sc["status"] = "COMPLETED"
                sc["status_text"] = "Đã hoàn thành video"
                sc["video_url"] = f"/job/{job_id}/clip/{idx}"
                sc["keyframe_url"] = f"/job/{job_id}/kf/{idx}"
            elif kf_path.exists() and kf_path.stat().st_size > 1000:
                sc["status"] = "KEYFRAME_READY"
                sc["status_text"] = "Keyframe đã xong. Video đang chạy..."
                sc["keyframe_url"] = f"/job/{job_id}/kf/{idx}"
                sc["video_url"] = None
            else:
                sc["status"] = "GENERATING_KEYFRAME"
                sc["status_text"] = "Đang vẽ ảnh Keyframe..."
                sc["keyframe_url"] = None
                sc["video_url"] = None
            sc["tts_url"] = f"/job/{job_id}/tts/{idx}"
        update_job(job_id, status="KEYFRAMING", message=f"Giai đoạn 2: Đang sinh đồng thời {num_scenes} ảnh Keyframe bằng Banana Pro 2...", step=3, scenes=scenes_data)

        # 2. Băng chuyền song song: Sinh đồng thời toàn bộ Keyframe qua ThreadPoolExecutor!
        is_grok = video_engine.startswith("grok")
        is_grok_v1 = "10" in video_engine or "v1" in video_engine
        grok_model_name = "xai/grok-imagine-video" if is_grok_v1 else "xai/grok-imagine-video-1.5"
        grok_display = "Grok Video 1.0" if is_grok_v1 else "Grok Video 1.5"

        state_lock = threading.Lock()
        kf_errors = []

        def _process_scene_keyframe(i, sc):
            clip_path = job_dir / f"clip_{i}.mp4"
            kf_path = job_dir / f"keyframe_{i}.jpg"

            try:
                # Đã có clip sẵn từ trước (cache)
                if clip_path.exists() and clip_path.stat().st_size > 50000:
                    print(f"Job {job_id}: Clip {i} already rendered ({clip_path.stat().st_size} bytes), skipping Veo.")
                    with state_lock:
                        clips[i - 1] = clip_path
                        scenes_data[i - 1]["status"] = "COMPLETED"
                        scenes_data[i - 1]["status_text"] = "Đã hoàn thành video"
                        scenes_data[i - 1]["video_url"] = f"/job/{job_id}/clip/{i}"
                        scenes_data[i - 1]["keyframe_url"] = f"/job/{job_id}/kf/{i}"
                        update_job(job_id, clips=[f"/job/{job_id}/clip/{j+1}" for j in range(num_scenes) if clips[j] is not None], scenes=scenes_data)
                    return

                # Sinh Keyframe cho Cảnh i nếu chưa có
                kf_mid = None
                existing_kfs = job.get("keyframes", [])
                for k in existing_kfs:
                    if k.get("scene_id") == i and k.get("media_id") and kf_path.exists() and kf_path.stat().st_size > 1000:
                        kf_mid = k.get("media_id")
                        print(f"Job {job_id}: Keyframe {i} already exists on disk, reusing.")
                        break

                if not kf_mid:
                    # Chống cờ RATE_BURST: Thêm độ trễ so le nhẹ (~0.95s) giữa các luồng
                    # Cảnh 1: 0s, Cảnh 2: ~0.95s, Cảnh 3: ~1.9s, Cảnh 4: ~2.85s...
                    # Giúp các request đến Google giãn cách tự nhiên như người dùng thật, triệt tiêu 100% lỗi RATE_BURST
                    stagger_s = (i - 1) * 0.95
                    if stagger_s > 0:
                        time.sleep(stagger_s)

                    sanitized_prompt = sanitize_kf_prompt(sc["image_generation_prompt"], canonical_anchor, canonical_model_anchor, mode=flow_mode)
                    kf_mid, kf_url = generate_keyframe_flowkit(sanitized_prompt, ref_ids, job_id=job_id)
                    urllib.request.urlretrieve(kf_url, str(kf_path))

                with state_lock:
                    keyframes[i - 1] = {"scene_id": i, "media_id": kf_mid, "url": f"/job/{job_id}/kf/{i}"}
                    done_kfs = [k for k in keyframes if k is not None]

                    # CẬP NHẬT NGAY: Hiển thị ngay ảnh Keyframe ở dưới và chuyển trạng thái sang "Video đang chạy..."
                    scenes_data[i - 1]["status"] = "KEYFRAME_READY"
                    scenes_data[i - 1]["status_text"] = "Keyframe đã xong. Video đang chạy..."
                    scenes_data[i - 1]["keyframe_url"] = f"/job/{job_id}/kf/{i}"
                    scenes_data[i - 1]["image_url"] = f"/job/{job_id}/kf/{i}"
                    update_job(job_id, keyframes=done_kfs, message=f"Băng chuyền: Cảnh {i}/{num_scenes} Keyframe hoàn tất! ({len(done_kfs)}/{num_scenes})", scenes=scenes_data)

                if is_grok:
                    with state_lock:
                        scenes_data[i - 1]["status"] = "RENDERING_VIDEO"
                        scenes_data[i - 1]["status_text"] = f"Keyframe đã xong. {grok_display} đang chạy..."
                        done_count = len([k for k in keyframes if k is not None])
                        update_job(job_id, status="RENDERING", message=f"Đã hoàn tất {done_count}/{num_scenes} Keyframe. Chuyển sang {grok_display} ({scene_duration}s)...", step=4, scenes=scenes_data)
                else:
                    # Pacing 2.0s: Chống RATE_BURST giữa bước tạo Keyframe và nạp Video Veo 3
                    time.sleep(2.0)
                    op_name = submit_video_flowkit(kf_mid, sc["video_motion_prompt"], i, job_id=job_id, duration_s=scene_duration)
                    with state_lock:
                        pending_ops[i] = op_name
                        # Persist the op handle so a later crash/regen can
                        # recover the submitted render instead of paying twice.
                        scenes_data[i - 1]["operation_name"] = op_name
                        scenes_data[i - 1]["operation_prompt"] = sc["video_motion_prompt"]
                        scenes_data[i - 1]["status"] = "RENDERING_VIDEO"
                        scenes_data[i - 1]["status_text"] = "Keyframe đã xong. Video đang chạy..."
                        update_job(job_id, status="RENDERING", message=f"Băng chuyền [Cảnh {i}/{num_scenes}]: Đã nạp Cảnh {i} vào cụm GPU Veo 3 ({scene_duration}s)...", step=4, scenes=scenes_data)
                        print(f"Job {job_id}: Scene {i} submitted to Veo 3 (op: {op_name[:16]}...).")
            except Exception as err:
                print(f"Job {job_id}: Scene {i} keyframe generation failed: {err}")
                with state_lock:
                    scenes_data[i - 1]["status"] = "FAILED"
                    scenes_data[i - 1]["status_text"] = "Lỗi khi vẽ Keyframe"
                    scenes_data[i - 1]["error"] = str(err)
                    update_job(job_id, scenes=scenes_data)
                kf_errors.append(f"Cảnh {i}: {err}")

        max_kf_workers = max(1, min(num_scenes, num_threads, 20))
        print(f"Job {job_id}: Generating {num_scenes} keyframes concurrently with {max_kf_workers} workers...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_kf_workers) as kf_executor:
            futures = [kf_executor.submit(_process_scene_keyframe, i, sc) for i, sc in enumerate(scenes_data, start=1)]
            concurrent.futures.wait(futures)

        if kf_errors and not pending_ops:
            raise RuntimeError(f"Lỗi sinh Keyframe: {'; '.join(kf_errors)}")
        if kf_errors:
            # Some scenes failed keyframing but others already hold live Veo
            # ops — poll those to completion instead of orphaning paid
            # renders. Failed scenes stay FAILED for the rescue path below.
            print(f"Job {job_id}: {len(kf_errors)} keyframe(s) failed ({'; '.join(kf_errors)}), but {len(pending_ops)} video op(s) already submitted — continuing to poll them.")

        # 3. Tất cả các cảnh đã được nạp vào Video Engine -> Bộ Giám Sát Tập Trung
        if video_engine.startswith("grok"):
            is_grok_v1 = "10" in video_engine or "v1" in video_engine
            grok_model_name = "xai/grok-imagine-video" if is_grok_v1 else "xai/grok-imagine-video-1.5"
            grok_display = "Grok Video 1.0" if is_grok_v1 else "Grok Video 1.5"

            def _on_clip_done(scene_idx, c_path):
                clips[scene_idx - 1] = c_path
                done_clips = [c for c in clips if c is not None]
                if scene_idx <= len(scenes_data):
                    scenes_data[scene_idx - 1]["status"] = "COMPLETED"
                    scenes_data[scene_idx - 1]["status_text"] = "Đã hoàn thành video"
                    scenes_data[scene_idx - 1]["video_url"] = f"/job/{job_id}/clip/{scene_idx}"
                update_job(
                    job_id,
                    message=f"{grok_display} đã render xong {len(done_clips)}/{num_scenes} clip...",
                    clips=[f"/job/{job_id}/clip/{j+1}" for j in range(num_scenes) if clips[j] is not None],
                    scenes=scenes_data
                )

            def _on_clip_failed(scene_idx, err_msg):
                if scene_idx <= len(scenes_data):
                    scenes_data[scene_idx - 1]["status"] = "FAILED"
                    err_lower = str(err_msg).lower()
                    if "content_policy_violation" in err_lower or "kiểm duyệt" in err_lower:
                        scenes_data[scene_idx - 1]["status_text"] = "Lỗi kiểm duyệt nội dung (xAI Grok)"
                        scenes_data[scene_idx - 1]["error"] = "xAI Grok từ chối do vi phạm kiểm duyệt (cử chỉ hoặc góc chụp). Bạn có thể bấm 'Sửa Prompt' hoặc 'Vẽ lại Keyframe' kín đáo hơn rồi bấm 'Render lại Video'."
                        scenes_data[scene_idx - 1]["error_code"] = "CONTENT_POLICY_VIOLATION"
                    elif "timed out" in err_lower or "timeout" in err_lower:
                        scenes_data[scene_idx - 1]["status_text"] = "Hết thời gian chờ (Timeout)"
                        scenes_data[scene_idx - 1]["error"] = "Máy chủ AI phản hồi quá lâu hoặc GPU quá tải. Vui lòng bấm 'Render lại Video'."
                        scenes_data[scene_idx - 1]["error_code"] = "TIMEOUT"
                    else:
                        scenes_data[scene_idx - 1]["status_text"] = "Lỗi khi render video"
                        scenes_data[scene_idx - 1]["error"] = f"Lỗi render video: {str(err_msg)[:150]}. Vui lòng bấm 'Render lại Video'."
                        scenes_data[scene_idx - 1]["error_code"] = "RENDER_ERROR"
                update_job(job_id, scenes=scenes_data)

            scenes_to_run = []
            for i, sc in enumerate(scenes_data, start=1):
                c_p = job_dir / f"clip_{i}.mp4"
                if not (c_p.exists() and c_p.stat().st_size > 50000):
                    kf_p = job_dir / f"keyframe_{i}.jpg"
                    scenes_to_run.append((i, kf_p, sc.get("video_motion_prompt", ""), sc.get("audio_dialogue", "")))

            if scenes_to_run:
                update_job(job_id, status="RENDERING", message=f"Giai đoạn 3: {grok_display} đang render song song {len(scenes_to_run)} cảnh...", step=4, scenes=scenes_data)
                batch_generate_videos_grok(
                    scenes_to_run=scenes_to_run,
                    job_id=job_id,
                    job_dir=job_dir,
                    scene_duration=scene_duration,
                    num_threads=num_threads,
                    model=grok_model_name,
                    resolution=video_res,
                    flow_mode=flow_mode,
                    voice_key=voice_key,
                    on_clip_done=_on_clip_done,
                    on_clip_failed=_on_clip_failed
                )
        elif pending_ops:
            def _on_clip_done(scene_idx, c_path):
                clips[scene_idx - 1] = c_path
                done_clips = [c for c in clips if c is not None]
                if scene_idx <= len(scenes_data):
                    scenes_data[scene_idx - 1]["status"] = "COMPLETED"
                    scenes_data[scene_idx - 1]["status_text"] = "Đã hoàn thành video"
                    scenes_data[scene_idx - 1]["video_url"] = f"/job/{job_id}/clip/{scene_idx}"
                update_job(
                    job_id,
                    message=f"Veo 3 đã render xong {len(done_clips)}/{num_scenes} clip...",
                    clips=[f"/job/{job_id}/clip/{j+1}" for j in range(num_scenes) if clips[j] is not None],
                    scenes=scenes_data
                )

            def _on_clip_failed(scene_idx, err_msg):
                if scene_idx <= len(scenes_data):
                    scenes_data[scene_idx - 1]["status"] = "FAILED"
                    err_lower = str(err_msg).lower()
                    if "filter" in err_lower or "safety" in err_lower:
                        scenes_data[scene_idx - 1]["status_text"] = "Lỗi kiểm duyệt (Google Veo)"
                        scenes_data[scene_idx - 1]["error"] = "Google Veo từ chối do chính sách an toàn. Vui lòng bấm 'Sửa Prompt' làm sạch từ khóa rồi bấm 'Render lại Video'."
                        scenes_data[scene_idx - 1]["error_code"] = "CONTENT_POLICY_VIOLATION"
                    elif "timeout" in err_lower or "timed out" in err_lower:
                        scenes_data[scene_idx - 1]["status_text"] = "Hết thời gian chờ (Timeout)"
                        scenes_data[scene_idx - 1]["error"] = "Hết thời gian chờ render Google Veo. Vui lòng bấm 'Render lại Video'."
                        scenes_data[scene_idx - 1]["error_code"] = "TIMEOUT"
                    else:
                        scenes_data[scene_idx - 1]["status_text"] = "Lỗi khi render video"
                        scenes_data[scene_idx - 1]["error"] = f"Lỗi render Veo: {str(err_msg)[:150]}. Bấm 'Render lại Video' để thử lại."
                        scenes_data[scene_idx - 1]["error_code"] = "RENDER_ERROR"
                update_job(job_id, scenes=scenes_data)

            update_job(job_id, status="RENDERING", message=f"Giai đoạn 3: Veo 3 đang đồng loạt render {len(pending_ops)} cảnh...", step=4, scenes=scenes_data)
            batch_poll_videos_flowkit(
                pending_ops=pending_ops,
                job_id=job_id,
                job_dir=job_dir,
                on_clip_done=_on_clip_done,
                on_clip_failed=_on_clip_failed
            )

        # ─── GIAI ĐOẠN 4: HẬU KỲ & XUẤT BẢN (FFMPEG ENGINE) ──────────
        valid_clips = [c for c in clips if c is not None and c.exists() and c.stat().st_size > 50000]
        final_mp4 = job_dir / "final_tvc.mp4"
        has_failures = any(sc.get("status") == "FAILED" for sc in scenes_data) or (len(valid_clips) < num_scenes)

        if not has_failures and len(valid_clips) == num_scenes:
            # If any clip has no audio (e.g. silent video), apply Edge-TTS fallback (except fashion mode which is zero-dialogue)
            if flow_mode != "fashion":
                for idx, c in enumerate(valid_clips, start=1):
                    if not check_video_has_audio(c):
                        t_f = job_dir / f"tts_{idx}.mp3"
                        if not t_f.exists() or t_f.stat().st_size < 1000:
                            sc_data = scenes_data[idx - 1] if idx - 1 < len(scenes_data) else {}
                            dlg = (sc_data.get("audio_dialogue") or "").strip()
                            if dlg:
                                print(f"Job {job_id}: Recovering missing TTS for silent scene {idx} before concatenation...")
                                generate_edge_tts(dlg, t_f, voice=voice_info["edge_voice"], rate=voice_info.get("rate", "+0%"))
                        if t_f.exists() and t_f.stat().st_size > 500:
                            mux_tts_to_video(c, t_f, target_duration=scene_duration)

            smart_concat_videos(
                valid_clips,
                final_mp4,
                target_duration=scene_duration,
                bgm_path=bgm_info.get("file")
            )

            total_dur = len(valid_clips) * scene_duration
            update_job(
                job_id,
                status="COMPLETED",
                error=None,
                message=f"Hoàn thành xuất sắc toàn bộ {len(valid_clips)}/{num_scenes} phân cảnh ({total_dur} giây)!",
                final_video_url=f"/job/{job_id}/final",
                completed_at=time.time(),
                scenes=scenes_data
            )
        elif valid_clips:
            # Thiếu cảnh (có cảnh bị lỗi hoặc chưa render xong): TUYỆT ĐỐI KHÔNG TỰ XUẤT FINAL VIDEO!
            if final_mp4.exists():
                try:
                    final_mp4.unlink()
                except Exception:
                    pass
            fail_count = num_scenes - len(valid_clips)
            update_job(
                job_id,
                status="PARTIAL_SUCCESS",
                message=f"Đã hoàn thành {len(valid_clips)}/{num_scenes} phân cảnh ({fail_count} cảnh chưa xong hoặc bị lỗi). Đang tự động thử lại cảnh thiếu để xuất video final...",
                final_video_url=None,
                completed_at=time.time(),
                scenes=scenes_data
            )
            # Auto-rescue: If exactly 1 scene failed/timed out, automatically attempt 1 background retry
            def _needs_rescue(sc):
                if sc.get("status") == "FAILED":
                    return True
                # Stuck RENDERING_VIDEO with no clip = a submitted op that lost
                # its poller — regen can adopt the live op via operation_name.
                if sc.get("status") == "RENDERING_VIDEO":
                    cp = job_dir / f"clip_{sc.get('scene_id')}.mp4"
                    return not (cp.exists() and cp.stat().st_size > 50000)
                return False
            failed_scene_ids = [sc.get("scene_id") for sc in scenes_data if _needs_rescue(sc)]
            if failed_scene_ids:
                def _rescue_all(fids):
                    for fid in fids:
                        try:
                            run_scene_regeneration_worker(job_id, fid, "", "", "", False)
                        except Exception as rescue_err:
                            print(f"Job {job_id}: auto-rescue scene {fid} failed: {rescue_err}")
                print(f"Job {job_id}: Automatically triggering background rescue for {len(failed_scene_ids)} failed scene(s): {failed_scene_ids}...")
                threading.Thread(target=_rescue_all, args=(failed_scene_ids,), daemon=True).start()
        else:
            if final_mp4.exists():
                try:
                    final_mp4.unlink()
                except Exception:
                    pass
            update_job(job_id, status="FAILED", message="Không có phân cảnh nào hoàn thành, vui lòng thử lại.", final_video_url=None, scenes=scenes_data)

    except Exception as exc:
        err_text = str(exc)
        if "UNUSUAL" in err_text or "proxy" in err_text.lower():
            clean_err = "Hệ thống máy chủ AI đang bận điều phối tài nguyên, vui lòng bấm Tiếp tục hoặc Tái tạo lại."
        else:
            clean_err = err_text
        update_job(job_id, status="FAILED", error=clean_err, message=f"Lỗi: {clean_err}")

def run_keyframe_regeneration_worker(job_id: str, scene_id: int, image_prompt: str = ""):
    try:
        job = JOBS.get(job_id)
        if not job:
            jfile = WORK_DIR / job_id / "job.json"
            if jfile.exists():
                job = json.loads(jfile.read_text())
                JOBS[job_id] = job
        if not job:
            raise ValueError(f"Job {job_id} not found")

        job_dir = WORK_DIR / job_id
        scenes = job.get("scenes", [])
        if scene_id < 1 or scene_id > len(scenes):
            raise ValueError(f"Scene {scene_id} invalid (total scenes: {len(scenes)})")

        sc_idx = scene_id - 1
        sc = scenes[sc_idx]

        if image_prompt:
            sc["image_generation_prompt"] = image_prompt

        update_job(
            job_id,
            scenes=scenes,
            regen_status={
                "scene_id": scene_id,
                "type": "KEYFRAME_ONLY",
                "status": "REGENERATING",
                "step": "KEYFRAME",
                "message": f"Đang gọi Banana Pro 2 vẽ lại riêng Keyframe Cảnh {scene_id}..."
            }
        )

        flow_mode = job.get("flow_mode", "pov")
        canonical_anchor = job.get("profile", {}).get("canonical_visual_anchor", "")
        canonical_model_anchor = job.get("profile", {}).get("canonical_model_anchor", "")
        if not canonical_anchor:
            for s in scenes:
                p_text = s.get("image_generation_prompt", "")
                if "PRODUCT REFERENCE LOCK — HIGHEST PRIORITY:" in p_text:
                    canonical_anchor = p_text.split("PRODUCT REFERENCE LOCK — HIGHEST PRIORITY:", 1)[1].split(". ", 1)[0].strip()
                    break
        if not canonical_model_anchor and flow_mode in ["ugc", "store_review", "fashion"]:
            for s in scenes:
                p_text = s.get("image_generation_prompt", "")
                if "CREATOR REFERENCE LOCK — HIGHEST PRIORITY:" in p_text:
                    canonical_model_anchor = p_text.split("CREATOR REFERENCE LOCK — HIGHEST PRIORITY:", 1)[1].split(". ", 1)[0].strip()
                    break
                elif "KOL REFERENCE LOCK — HIGHEST PRIORITY:" in p_text:
                    canonical_model_anchor = p_text.split("KOL REFERENCE LOCK — HIGHEST PRIORITY:", 1)[1].split(". ", 1)[0].strip()
                    break
                elif "FASHION MODEL REFERENCE LOCK — HIGHEST PRIORITY:" in p_text:
                    canonical_model_anchor = p_text.split("FASHION MODEL REFERENCE LOCK — HIGHEST PRIORITY:", 1)[1].split(". ", 1)[0].strip()
                    break

        canonical_anchor = sanitize_safety_content(canonical_anchor)
        canonical_model_anchor = sanitize_safety_content(canonical_model_anchor)

        kf_path = job_dir / f"keyframe_{scene_id}.jpg"
        ref_ids = get_or_refresh_ref_ids(job_id)
        sanitized_prompt = sanitize_kf_prompt(sc["image_generation_prompt"], canonical_anchor, canonical_model_anchor, mode=flow_mode)
        
        kf_mid, kf_url = generate_keyframe_flowkit(sanitized_prompt, ref_ids, job_id=job_id)
        urllib.request.urlretrieve(kf_url, str(kf_path))

        # Update keyframes list in job
        kfs = job.get("keyframes", [])
        found_kf = False
        for k in kfs:
            if k.get("scene_id") == scene_id:
                k["media_id"] = kf_mid
                k["url"] = f"/job/{job_id}/kf/{scene_id}"
                found_kf = True
                break
        if not found_kf:
            kfs.append({"scene_id": scene_id, "media_id": kf_mid, "url": f"/job/{job_id}/kf/{scene_id}"})
        
        # Also update scene keyframe_url and image_url
        sc["keyframe_url"] = f"/job/{job_id}/kf/{scene_id}"
        sc["image_url"] = f"/job/{job_id}/kf/{scene_id}"

        update_job(
            job_id,
            keyframes=kfs,
            scenes=scenes,
            regen_status={
                "scene_id": scene_id,
                "type": "KEYFRAME_ONLY",
                "status": "COMPLETED",
                "step": "DONE",
                "message": f"Đã vẽ lại xong Keyframe Cảnh {scene_id}! Ảnh mới đã được cập nhật.",
                "completed_at": time.time()
            }
        )
        print(f"Job {job_id} keyframe for scene {scene_id} regenerated successfully in ~10s!")
    except Exception as e:
        print(f"Error regenerating keyframe {scene_id} for job {job_id}: {e}")
        update_job(
            job_id,
            regen_status={
                "scene_id": scene_id,
                "type": "KEYFRAME_ONLY",
                "status": "FAILED",
                "error": str(e),
                "message": f"Vẽ lại Keyframe thất bại: {e}",
                "completed_at": time.time()
            }
        )

def run_scene_regeneration_worker(
    job_id: str,
    scene_id: int,
    image_prompt: str,
    motion_prompt: str,
    dialogue: str,
    regen_keyframe: bool
):
    try:
        job = JOBS.get(job_id)
        if not job:
            jfile = WORK_DIR / job_id / "job.json"
            if jfile.exists():
                job = json.loads(jfile.read_text())
                JOBS[job_id] = job
        if not job:
            raise ValueError(f"Job {job_id} not found")

        job_dir = WORK_DIR / job_id
        scenes = job.get("scenes", [])
        if scene_id < 1 or scene_id > len(scenes):
            raise ValueError(f"Scene {scene_id} invalid (total scenes: {len(scenes)})")

        sc_idx = scene_id - 1
        sc = scenes[sc_idx]

        voice_key = job.get("voice", "female_north")
        voice_info = VOICE_PROFILES.get(voice_key, VOICE_PROFILES["female_north"])

        # Update scene prompts if provided
        if image_prompt:
            sc["image_generation_prompt"] = image_prompt
        if dialogue:
            sc["audio_dialogue"] = dialogue
        if motion_prompt:
            sc["video_motion_prompt"] = motion_prompt

        # Ensure lip-sync match between video motion prompt and audio dialogue
        if sc.get("audio_dialogue") and sc.get("video_motion_prompt"):
            v_prompt = voice_info.get("say_clause", "")
            sc["video_motion_prompt"] = sync_dialogue_to_motion_prompt(
                sc["video_motion_prompt"],
                sc["audio_dialogue"],
                v_prompt
            )

        sc["status"] = "RENDERING_VIDEO" if not regen_keyframe else "GENERATING_KEYFRAME"
        sc["status_text"] = "Đang render lại Video..." if not regen_keyframe else "Đang vẽ lại Keyframe..."
        sc["error"] = None
        sc["error_code"] = None

        update_job(
            job_id,
            scenes=scenes,
            regen_status={
                "scene_id": scene_id,
                "type": "VIDEO_ONLY" if not regen_keyframe else "FULL_SCENE",
                "status": "REGENERATING",
                "step": "START",
                "message": f"Bắt đầu {'render lại Video' if not regen_keyframe else 'tái tạo toàn bộ'} Phân Cảnh {scene_id}..."
            }
        )

        flow_mode = job.get("flow_mode", "pov")
        canonical_anchor = job.get("profile", {}).get("canonical_visual_anchor", "")
        canonical_model_anchor = job.get("profile", {}).get("canonical_model_anchor", "")
        if not canonical_anchor:
            for s in scenes:
                p_text = s.get("image_generation_prompt", "")
                if "PRODUCT REFERENCE LOCK — HIGHEST PRIORITY:" in p_text:
                    canonical_anchor = p_text.split("PRODUCT REFERENCE LOCK — HIGHEST PRIORITY:", 1)[1].split(". ", 1)[0].strip()
                    break
        if not canonical_model_anchor and flow_mode in ["ugc", "store_review", "fashion"]:
            for s in scenes:
                p_text = s.get("image_generation_prompt", "")
                if "CREATOR REFERENCE LOCK — HIGHEST PRIORITY:" in p_text:
                    canonical_model_anchor = p_text.split("CREATOR REFERENCE LOCK — HIGHEST PRIORITY:", 1)[1].split(". ", 1)[0].strip()
                    break
                elif "KOL REFERENCE LOCK — HIGHEST PRIORITY:" in p_text:
                    canonical_model_anchor = p_text.split("KOL REFERENCE LOCK — HIGHEST PRIORITY:", 1)[1].split(". ", 1)[0].strip()
                    break
                elif "FASHION MODEL REFERENCE LOCK — HIGHEST PRIORITY:" in p_text:
                    canonical_model_anchor = p_text.split("FASHION MODEL REFERENCE LOCK — HIGHEST PRIORITY:", 1)[1].split(". ", 1)[0].strip()
                    break

        canonical_anchor = sanitize_safety_content(canonical_anchor)
        canonical_model_anchor = sanitize_safety_content(canonical_model_anchor)

        scene_duration = job.get("scene_duration", 8)

        # 1. Regenerate Keyframe if requested
        kf_path = job_dir / f"keyframe_{scene_id}.jpg"
        kf_mid = None
        if not regen_keyframe:
            # Re-use existing keyframe or re-upload if needed
            kfs = job.get("keyframes", [])
            for k in kfs:
                if k.get("scene_id") == scene_id:
                    kf_mid = k.get("media_id")
                    break
            if not kf_mid and kf_path.exists():
                kf_mid = upload_to_flowkit(kf_path, job_id=job_id)
            if not kf_mid:
                # An i2v submit with start_image_media_id=None silently runs
                # text-to-video — the face and product are no longer anchored.
                # No keyframe exists to reuse, so one must be generated first.
                print(f"Job {job_id}: Scene {scene_id} VIDEO_ONLY regen has no keyframe — upgrading to full regen.")
                regen_keyframe = True
                sc["status"] = "GENERATING_KEYFRAME"
                sc["status_text"] = "Đang vẽ lại Keyframe..."
        if regen_keyframe:
            update_job(
                job_id,
                regen_status={
                    "scene_id": scene_id,
                    "status": "REGENERATING",
                    "step": "KEYFRAME",
                    "message": f"Đang gọi Banana Pro 2 vẽ lại Keyframe Cảnh {scene_id}..."
                }
            )
            ref_ids = get_or_refresh_ref_ids(job_id)
            sanitized_prompt = sanitize_kf_prompt(sc["image_generation_prompt"], canonical_anchor, canonical_model_anchor, mode=flow_mode)
            kf_mid, kf_url = generate_keyframe_flowkit(sanitized_prompt, ref_ids, job_id=job_id)
            urllib.request.urlretrieve(kf_url, str(kf_path))

            # Update keyframes list in job
            kfs = job.get("keyframes", [])
            found_kf = False
            for k in kfs:
                if k.get("scene_id") == scene_id:
                    k["media_id"] = kf_mid
                    k["url"] = f"/job/{job_id}/kf/{scene_id}"
                    found_kf = True
                    break
            if not found_kf:
                kfs.append({"scene_id": scene_id, "media_id": kf_mid, "url": f"/job/{job_id}/kf/{scene_id}"})
            update_job(job_id, keyframes=kfs)

        # 2. Render Video (Grok or Veo)
        video_engine = job.get("video_engine", "veo")
        video_res = str(job.get("resolution") or ("720p" if "veo" in video_engine.lower() else "480p")).lower()
        if video_res not in ["480p", "720p", "1080p"]:
            video_res = "720p" if "veo" in video_engine.lower() else "480p"
        clip_path = job_dir / f"clip_{scene_id}.mp4"
        if "grok" in video_engine.lower():
            is_grok_v1 = "10" in video_engine or "v1" in video_engine
            grok_model_name = "xai/grok-imagine-video" if is_grok_v1 else "xai/grok-imagine-video-1.5"
            grok_display = "Grok Video 1.0" if is_grok_v1 else "Grok Video 1.5"
            update_job(
                job_id,
                regen_status={
                    "scene_id": scene_id,
                    "status": "REGENERATING",
                    "step": "VIDEO",
                    "message": f"{grok_display} đang render lại Video Cảnh {scene_id} ({scene_duration}s)..."
                }
            )
            generate_video_grok(
                kf_path,
                sc["video_motion_prompt"],
                scene_id,
                job_id=job_id,
                duration_s=scene_duration,
                model=grok_model_name,
                resolution=video_res,
                dialogue=sc.get("audio_dialogue", ""),
                flow_mode=flow_mode,
                voice_key=voice_key
            )
        else:
            update_job(
                job_id,
                regen_status={
                    "scene_id": scene_id,
                    "status": "REGENERATING",
                    "step": "VIDEO",
                    "message": f"Veo 3.1 đang render lại Video Cảnh {scene_id} ({scene_duration}s)..."
                }
            )
            # Orphan recovery: if the main pipeline died after submitting this
            # scene's op (same prompt, no clip downloaded), adopt the live op
            # instead of paying for a duplicate render.
            recovered = False
            existing_op = sc.get("operation_name")
            same_prompt = bool(existing_op) and sc.get("operation_prompt") == sc.get("video_motion_prompt")
            if existing_op and same_prompt and not clip_path.exists():
                try:
                    p_data = call_flowkit_api("/api/flow/check-status", {
                        "operations": [{"operation": {"name": existing_op}}]
                    }, timeout=40, job_id=job_id)
                    curr = (p_data.get("operations") or (p_data.get("data") or {}).get("operations") or [{}])[0]
                    st = str(curr.get("status") or "")
                    fife = ((curr.get("operation") or {}).get("metadata") or {}).get("video", {}).get("fifeUrl")
                    if "SUCCESSFUL" in st or fife:
                        urllib.request.urlretrieve(fife, str(clip_path))
                        recovered = True
                        print(f"Job {job_id}: Scene {scene_id} recovered orphaned Veo op {existing_op[:12]} — render already done.")
                    elif "FAIL" in st.upper():
                        sc["operation_name"] = None  # dead op — resubmit below
                    else:
                        video_fife = poll_video_operation_flowkit(existing_op, scene_id, job_id=job_id)
                        urllib.request.urlretrieve(video_fife, str(clip_path))
                        recovered = True
                        print(f"Job {job_id}: Scene {scene_id} adopted still-rendering Veo op {existing_op[:12]}.")
                except Exception as oe:
                    print(f"Job {job_id}: Scene {scene_id} orphan-op poll failed ({oe}); submitting fresh render.")
            if not recovered:
                video_fife = generate_video_flowkit(kf_mid, sc["video_motion_prompt"], scene_id, job_id=job_id, duration_s=scene_duration)
                sc["operation_name"] = None
                urllib.request.urlretrieve(video_fife, str(clip_path))

        # 3. Generate TTS Audio
        update_job(
            job_id,
            regen_status={
                "scene_id": scene_id,
                "status": "REGENERATING",
                "step": "TTS",
                "message": f"Đang tạo giọng nói AI Cảnh {scene_id}..."
            }
        )
        tts_path = job_dir / f"tts_{scene_id}.mp3"
        generate_edge_tts(sc.get("audio_dialogue", ""), tts_path, voice=voice_info["edge_voice"], rate=voice_info.get("rate", "+0%"))
        if not check_video_has_audio(clip_path) and tts_path.exists() and tts_path.stat().st_size > 500:
            mux_tts_to_video(clip_path, tts_path, target_duration=job.get("scene_duration", 8))

        # Update scene in scenes list
        sc["status"] = "COMPLETED"
        sc["status_text"] = "Đã hoàn thành video"
        sc["video_url"] = f"/job/{job_id}/clip/{scene_id}"
        sc["tts_url"] = f"/job/{job_id}/tts/{scene_id}"
        sc["error"] = None
        sc["error_code"] = None

        # 4. Re-Concat Final TVC ONLY IF ALL SCENES ARE VALID AND COMPLETED
        num_scenes = len(scenes)
        all_valid_clips = []
        for j in range(num_scenes):
            c_p = job_dir / f"clip_{j+1}.mp4"
            if c_p.exists() and c_p.stat().st_size > 50000:
                all_valid_clips.append(c_p)

        all_clips = [f"/job/{job_id}/clip/{j+1}" for j in range(num_scenes) if (job_dir / f"clip_{j+1}.mp4").exists() and (job_dir / f"clip_{j+1}.mp4").stat().st_size > 50000]
        final_mp4 = job_dir / "final_tvc.mp4"
        all_scenes_done = (len(all_valid_clips) == num_scenes) and not any(s.get("status") == "FAILED" for s in scenes)

        if all_scenes_done:
            update_job(
                job_id,
                regen_status={
                    "scene_id": scene_id,
                    "status": "REGENERATING",
                    "step": "CONCAT",
                    "message": f"Đang ghép nối toàn bộ {num_scenes} video TVC & Auto-Ducking BGM..."
                }
            )
            bgm_key = job.get("bgm", "tiktok_upbeat")
            bgm_info = BGM_PROFILES.get(bgm_key, BGM_PROFILES["none"])
            smart_concat_videos(
                all_valid_clips,
                final_mp4,
                target_duration=scene_duration,
                bgm_path=bgm_info.get("file")
            )
            update_job(
                job_id,
                status="COMPLETED",
                error=None,
                step=5,
                clips=all_clips,
                final_video_url=f"/job/{job_id}/final",
                message=f"Đã hoàn thành Cảnh {scene_id} và hoàn tất xuất sắc toàn bộ {num_scenes} phân cảnh TVC!",
                scenes=scenes,
                regen_status={
                    "scene_id": scene_id,
                    "status": "COMPLETED",
                    "step": "DONE",
                    "message": f"Đã tái tạo Cảnh {scene_id} và ghép lại TVC thành công!",
                    "completed_at": time.time()
                }
            )
        else:
            # Chưa đủ cảnh: TUYỆT ĐỐI KHÔNG TỰ XUẤT FINAL VIDEO!
            if final_mp4.exists():
                try:
                    final_mp4.unlink()
                except Exception:
                    pass
            missing_count = num_scenes - len(all_valid_clips)
            update_job(
                job_id,
                status="PARTIAL_SUCCESS",
                error=None,
                step=4,
                clips=all_clips,
                final_video_url=None,
                message=f"Đã hoàn thành Cảnh {scene_id}! Hiện có {len(all_valid_clips)}/{num_scenes} cảnh hoàn tất. Còn {missing_count} cảnh cần tái tạo để xuất video final.",
                scenes=scenes,
                regen_status={
                    "scene_id": scene_id,
                    "status": "COMPLETED",
                    "step": "DONE",
                    "message": f"Đã tái tạo Cảnh {scene_id} thành công! (Chờ hoàn tất đủ {num_scenes} cảnh để xuất video final)",
                    "completed_at": time.time()
                }
            )
        print(f"Job {job_id} scene {scene_id} regenerated successfully!")
    except Exception as e:
        print(f"Error regenerating scene {scene_id} for job {job_id}: {e}")
        err_str = str(e)
        err_lower = err_str.lower()
        if "content_policy_violation" in err_lower or "kiểm duyệt" in err_lower:
            err_code = "CONTENT_POLICY_VIOLATION"
            err_clean = "xAI Grok từ chối do vi phạm kiểm duyệt (cử chỉ hoặc góc chụp). Bạn có thể bấm 'Sửa Prompt' hoặc 'Vẽ lại Keyframe' kín đáo hơn rồi bấm 'Render lại Video'."
            err_status = "Lỗi kiểm duyệt nội dung (xAI Grok)"
        elif "timed out" in err_lower or "timeout" in err_lower:
            err_code = "TIMEOUT"
            err_clean = "Máy chủ AI phản hồi quá lâu hoặc GPU quá tải. Vui lòng bấm 'Render lại Video'."
            err_status = "Hết thời gian chờ (Timeout)"
        else:
            err_code = "RENDER_ERROR"
            err_clean = f"Lỗi render video: {err_str[:150]}. Vui lòng thử lại."
            err_status = "Lỗi khi render video"

        if scene_id <= len(scenes):
            scenes[scene_id - 1]["status"] = "FAILED"
            scenes[scene_id - 1]["status_text"] = err_status
            scenes[scene_id - 1]["error"] = err_clean
            scenes[scene_id - 1]["error_code"] = err_code

        update_job(
            job_id,
            scenes=scenes,
            regen_status={
                "scene_id": scene_id,
                "status": "FAILED",
                "error": err_clean,
                "error_code": err_code,
                "message": f"Tái tạo thất bại: {err_clean}",
                "completed_at": time.time()
            }
        )

class AutoTvcHandler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS, HEAD")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, Accept")
        self.end_headers()

    def do_HEAD(self):
        self.do_GET(head_only=True)

    def do_GET(self, head_only: bool = False):
        p = self.path.split("?")[0].strip("/")
        
        # API Diagnostics - Unusual Activity Audit & Proxy Health
        if p == "api/diagnostics/unusual-audit":
            try:
                from agent.services.unusual_audit import get_unusual_audit
                audit_mgr = get_unusual_audit()
                summary = audit_mgr.get_audit_summary()
                recent = audit_mgr.get_recent_events(limit=100)
                data = {
                    "ok": True,
                    "summary": summary,
                    "recent_events": recent,
                }
            except Exception as e:
                data = {"ok": False, "error": str(e), "summary": {}, "recent_events": []}
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            if not head_only:
                self.wfile.write(json.dumps(data).encode("utf-8"))
            return

        # API Rotate Proxies manually
        if p == "api/diagnostics/rotate-proxies":
            results = {}
            try:
                from agent.services.accounts import load_accounts
                accs = load_accounts()
                for acc in accs:
                    if acc.get("enabled", True):
                        aid = acc["id"]
                        results[aid] = rotate_profile_proxy(aid)
            except Exception as e:
                results = {"error": str(e)}
            data = {"ok": True, "rotations": results, "nick-a": results.get("nick-a"), "Nick-b": results.get("Nick-b")}
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            if not head_only:
                self.wfile.write(json.dumps(data).encode("utf-8"))
            return

        # API Pre-flight Proxy Health Check
        if p == "api/diagnostics/preflight-proxies":
            try:
                import agent.services.proxy_checker as pc
                from agent.services.proxy_pool import load_proxy_pool
                pool = load_proxy_pool()
                proxies = pool.get("proxies", [])
                results = pc.check_proxy_list(proxies, max_workers=10)
                data = {"ok": True, "results": results, "total": len(results)}
            except Exception as e:
                data = {"ok": False, "error": str(e), "results": []}
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            if not head_only:
                self.wfile.write(json.dumps(data).encode("utf-8"))
            return

        # API Proxy Lifecycle & Quarantine Status
        if p == "api/diagnostics/lifecycle":
            try:
                import agent.services.proxy_checker as pc
                data = {"ok": True, "lifecycle": pc.get_lifecycle_summary()}
            except Exception as e:
                data = {"ok": False, "error": str(e)}
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            if not head_only:
                self.wfile.write(json.dumps(data).encode("utf-8"))
            return

        # API Trigger Immediate Revival Cycle
        if p == "api/diagnostics/trigger-revival":
            try:
                import agent.services.proxy_checker as pc
                res = pc.run_revival_cycle()
                data = {"ok": True, "result": res}
            except Exception as e:
                data = {"ok": False, "error": str(e)}
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            if not head_only:
                self.wfile.write(json.dumps(data).encode("utf-8"))
            return

        # API Download Raw Audit Log
        if p == "api/diagnostics/download-audit-log":
            from agent.services.unusual_audit import AUDIT_LOG_FILE
            if AUDIT_LOG_FILE.exists():
                raw_bytes = AUDIT_LOG_FILE.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "application/x-ndjson")
                self.send_header("Content-Disposition", 'attachment; filename="unusual_activity_audit.jsonl"')
                self.send_header("Content-Length", str(len(raw_bytes)))
                self.end_headers()
                if not head_only:
                    self.wfile.write(raw_bytes)
                return
            else:
                self.send_response(404)
                self.end_headers()
                return

        # Download Markdown Docs for Nova over Tailscale
        if p in ["FLOWKIT_AFFILIATE_API.md", "api/docs/affiliate-api"]:
            doc_path = Path("/home/pc/flowkit/docs/NOVA_AFFILIATE_AND_IMAGE_API.md")
            if doc_path.exists():
                raw_bytes = doc_path.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/markdown; charset=utf-8")
                self.send_header("Content-Disposition", 'attachment; filename="FLOWKIT_AFFILIATE_API.md"')
                self.send_header("Content-Length", str(len(raw_bytes)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                if not head_only:
                    self.wfile.write(raw_bytes)
                return

        if p.startswith("docs/") or p.startswith("download/"):
            fname = p.split("/")[-1]
            doc_path = Path("/home/pc/flowkit/docs") / fname
            if not doc_path.exists():
                doc_path = Path("/home/pc/flowkit/docs") / f"{fname}.md"
            if doc_path.exists() and doc_path.is_file() and doc_path.suffix.lower() == ".md":
                raw_bytes = doc_path.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/markdown; charset=utf-8")
                self.send_header("Content-Disposition", f'attachment; filename="{doc_path.name}"')
                self.send_header("Content-Length", str(len(raw_bytes)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                if not head_only:
                    self.wfile.write(raw_bytes)
                return

        # API System Concurrency & Capacity for Nova
        if p == "api/system/concurrency":
            workers_count = 2
            try:
                import urllib.request
                with urllib.request.urlopen("http://127.0.0.1:8100/api/accounts", timeout=3) as resp:
                    acc_data = json.loads(resp.read().decode("utf-8"))
                    workers_count = len([w for w in acc_data.get("workers", []) if w.get("connected")])
            except Exception:
                pass
            data = {
                "ok": True,
                "active_workers": max(1, workers_count),
                "max_concurrency": max(20, workers_count * 20),
                "default_threads": 20,
                "min_threads": 1,
                "max_threads": 60,
                "recommended_threads": 25,
                "description": "Số luồng xử lý song song tối ưu cho cụm Google AI."
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            if not head_only:
                self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))
            return

        # API Get Single Scene Detail
        if p.startswith("api/scene/") and len(p.split("/")) == 4:
            parts = p.split("/")
            jid = parts[2]
            try:
                s_id = int(parts[3])
            except:
                s_id = 1
            job = JOBS.get(jid)
            if not job:
                jfile = WORK_DIR / jid / "job.json"
                if jfile.exists():
                    try:
                        job = json.loads(jfile.read_text())
                    except Exception:
                        pass
            if not job:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Job not found"}).encode())
                return
            scenes = job.get("scenes", [])
            if s_id < 1 or s_id > len(scenes):
                self.send_response(404)
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Scene not found"}).encode())
                return
            sc = scenes[s_id - 1]
            data = {
                "ok": True,
                "job_id": jid,
                "scene_id": s_id,
                "status": sc.get("status", "UNKNOWN"),
                "status_text": sc.get("status_text", ""),
                "image_prompt": sc.get("image_generation_prompt", ""),
                "motion_prompt": sc.get("video_motion_prompt", ""),
                "dialogue": sc.get("audio_dialogue", ""),
                "keyframe_url": sc.get("keyframe_url") or f"/job/{jid}/kf/{s_id}",
                "video_url": sc.get("video_url") or f"/job/{jid}/clip/{s_id}",
                "tts_url": f"/job/{jid}/tts/{s_id}"
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            if not head_only:
                self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))
            return

        # API Status
        if p.startswith("api/status/"):
            jid = p.split("/")[-1]
            jfile = WORK_DIR / jid / "job.json"
            if jfile.exists():
                try:
                    JOBS[jid] = json.loads(jfile.read_text())
                except Exception:
                    pass
            job = JOBS.get(jid, {"status": "NOT_FOUND"})
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            if not head_only:
                self.wfile.write(json.dumps(job).encode("utf-8"))
            return

        if p.startswith("api/batch/status/"):
            bid = p.replace("api/batch/status/", "").strip()
            b = bis.BATCH_JOBS.get(bid)
            if not b:
                bfile = WORK_DIR / f"batch_{bid}" / "batch.json"
                if bfile.exists():
                    try:
                        b = json.loads(bfile.read_text(encoding="utf-8"))
                        bis.BATCH_JOBS[bid] = b
                    except Exception:
                        pass
            if not b:
                self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                if not head_only:
                    self.wfile.write(json.dumps({"error": f"Batch {bid} not found"}).encode("utf-8"))
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            if not head_only:
                self.wfile.write(json.dumps(b).encode("utf-8"))
            return

        if p == "api/batch/list":
            batch_list = []
            for bid, b in sorted(bis.BATCH_JOBS.items(), key=lambda x: x[1].get("created_at", 0), reverse=True)[:30]:
                batch_list.append({
                    "batch_id": bid,
                    "module": b.get("module"),
                    "module_title": b.get("module_title"),
                    "created_at": b.get("created_at"),
                    "stats": b.get("stats", {}),
                    "config": b.get("config", {})
                })
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            if not head_only:
                self.wfile.write(json.dumps({"batches": batch_list}).encode("utf-8"))
            return

        # Fashion Lookbook API Endpoints (Decoupled from TVC)
        if p == "api/fashion-lookbook/templates":
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            if not head_only:
                self.wfile.write(json.dumps({"ok": True, "templates": fls.get_public_templates()}, ensure_ascii=False).encode("utf-8"))
            return

        if p == "api/fashion-lookbook/models":
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            if not head_only:
                self.wfile.write(json.dumps({"ok": True, "models": fls.get_public_models()}, ensure_ascii=False).encode("utf-8"))
            return

        if p == "api/fashion-lookbook/music":
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            if not head_only:
                self.wfile.write(json.dumps({"ok": True, "music": fls.get_public_music()}, ensure_ascii=False).encode("utf-8"))
            return

        if p.startswith("api/fashion-lookbook/status/"):
            jid = p.replace("api/fashion-lookbook/status/", "").strip()
            job = fls.recover_lookbook_job(jid) or fls.LOOKBOOK_JOBS.get(jid)
            if not job:
                self.send_response(404)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                if not head_only:
                    self.wfile.write(json.dumps({"ok": False, "error": f"Lookbook Job {jid} not found"}, ensure_ascii=False).encode("utf-8"))
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            if not head_only:
                self.wfile.write(json.dumps(job, ensure_ascii=False).encode("utf-8"))
            return

        if p.startswith("api/fashion-lookbook/"):
            parts = p.split("/")
            if len(parts) >= 4:
                jid = parts[2]
                action = parts[3]
                jdir = fls.WORK_DIR / f"lookbook_{jid}"
                if action == "final":
                    fpath = jdir / "final_lookbook.mp4"
                    if fpath.exists():
                        self.send_file(fpath, "video/mp4", head_only=head_only)
                        return
                elif action == "clip" and len(parts) >= 5:
                    fpath = jdir / f"clip_{parts[4]}.mp4"
                    if fpath.exists():
                        self.send_file(fpath, "video/mp4", head_only=head_only)
                        return
                elif action == "kf" and len(parts) >= 5:
                    fpath = jdir / f"keyframe_{parts[4]}.jpg"
                    if fpath.exists():
                        self.send_file(fpath, "image/jpeg", head_only=head_only)
                        return

        if p in ["api/tvc/list", "api/jobs/list"]:
            # Parse tenant_id from query string or X-Tenant-Id header
            tenant_id = None
            if "?" in self.path:
                try:
                    from urllib.parse import parse_qs, urlparse
                    qs = parse_qs(urlparse(self.path).query)
                    if "tenant_id" in qs and qs["tenant_id"]:
                        tenant_id = str(qs["tenant_id"][0]).strip()
                except Exception:
                    pass
            if not tenant_id:
                tenant_id = str(self.headers.get("X-Tenant-Id") or "").strip()

            # Strict Tenant Isolation: Never leak another tenant's jobs or return jobs to caller without tenant_id
            if not tenant_id:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                if not head_only:
                    self.wfile.write(json.dumps({"ok": True, "jobs": []}, ensure_ascii=False).encode("utf-8"))
                return

            jobs_out = []
            if WORK_DIR.exists():
                for p_dir in WORK_DIR.iterdir():
                    if p_dir.is_dir() and (p_dir / "job.json").exists():
                        jid = p_dir.name
                        if jid not in JOBS:
                            try:
                                JOBS[jid] = json.loads((p_dir / "job.json").read_text(encoding="utf-8"))
                            except Exception:
                                pass

            for jid, j in JOBS.items():
                if not isinstance(j, dict):
                    continue
                # Verify tenant ownership
                job_tenant = str(j.get("tenant_id") or "").strip()
                if job_tenant != tenant_id:
                    continue

                final_file = WORK_DIR / jid / "final_tvc.mp4"
                final_video = j.get("final_video_url")
                if not final_video and final_file.exists():
                    final_video = f"/job/{jid}/final"

                if j.get("status") == "COMPLETED" or final_file.exists() or final_video:
                    prod = j.get("profile", {}).get("product", {}) if isinstance(j.get("profile"), dict) else {}
                    name = prod.get("name") or j.get("product_name") or f"TVC Video #{jid}"
                    features = prod.get("features") or ""
                    material = prod.get("material") or ""
                    prompt = f"{name}. {features}".strip() if (features or material) else name
                    created_at = j.get("created_at") or 0
                    if isinstance(created_at, (int, float)) and created_at < 1e11:
                        created_ms = int(created_at * 1000)
                    elif isinstance(created_at, (int, float)):
                        created_ms = int(created_at)
                    else:
                        created_ms = int(time.time() * 1000)

                    jobs_out.append({
                        "job_id": jid,
                        "tenant_id": job_tenant,
                        "status": j.get("status", "COMPLETED"),
                        "created_at": created_ms,
                        "final_video_url": final_video or f"/job/{jid}/final",
                        "thumbnail_url": f"/job/{jid}/thumb",
                        "product_name": name,
                        "prompt": prompt,
                        "flow_mode": j.get("flow_mode", "ugc"),
                        "num_scenes": j.get("num_scenes", len(j.get("scenes", []))),
                        "scene_duration": j.get("scene_duration", 8),
                        "voice_label": j.get("voice_label") or j.get("voice", ""),
                        "bgm_label": j.get("bgm_label") or j.get("bgm", ""),
                        "scenes": [
                            {
                                "scene_id": sc.get("scene_id"),
                                "keyframe_url": sc.get("keyframe_url") or f"/job/{jid}/kf/{sc.get('scene_id')}",
                                "video_url": sc.get("video_url") or f"/job/{jid}/clip/{sc.get('scene_id')}",
                                "image_prompt": sc.get("image_generation_prompt"),
                                "motion_prompt": sc.get("video_motion_prompt"),
                                "dialogue": sc.get("audio_dialogue") or sc.get("voiceover"),
                            }
                            for sc in j.get("scenes", [])
                        ] if isinstance(j.get("scenes"), list) else []
                    })

            jobs_out.sort(key=lambda x: x.get("created_at", 0), reverse=True)
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            if not head_only:
                self.wfile.write(json.dumps({"ok": True, "jobs": jobs_out[:60]}, ensure_ascii=False).encode("utf-8"))
            return

        # Serve Batch Files
        if p.startswith("batch/"):
            parts = p.split("/")
            if len(parts) >= 4:
                bid = parts[1]
                bdir = WORK_DIR / f"batch_{bid}"
                action = parts[2]
                if action == "item":
                    item_id = parts[3]
                    fpath = bdir / f"item_{item_id}.jpg"
                    if fpath.exists():
                        self.send_file(fpath, "image/jpeg", head_only=head_only)
                        return
                elif action == "video":
                    item_id = parts[3]
                    fpath = bdir / f"video_{item_id}.mp4"
                    if fpath.exists():
                        self.send_file(fpath, "video/mp4", head_only=head_only)
                        return
                elif action == "ref" and len(parts) >= 5:
                    ref_type = parts[3]
                    ref_idx = parts[4]
                    if ref_type == "face":
                        for name in [f"face_{ref_idx}.jpg", f"model_{ref_idx}.jpg"]:
                            fpath = bdir / name
                            if fpath.exists():
                                self.send_file(fpath, "image/jpeg", head_only=head_only)
                                return
                    elif ref_type == "outfit":
                        fpath = bdir / f"outfit_{ref_idx}.jpg"
                        if fpath.exists():
                            self.send_file(fpath, "image/jpeg", head_only=head_only)
                            return

        # Serve Job Files
        if p.startswith("job/"):
            parts = p.split("/")
            if len(parts) >= 3:
                jid = parts[1]
                action = parts[2]
                jdir = WORK_DIR / jid
                if action == "final":
                    fpath = jdir / "final_tvc.mp4"
                    if fpath.exists():
                        self.send_file(fpath, "video/mp4", head_only=head_only)
                        return
                elif action == "clip" and len(parts) >= 4:
                    fpath = jdir / f"clip_{parts[3]}.mp4"
                    if fpath.exists():
                        self.send_file(fpath, "video/mp4", head_only=head_only)
                        return
                elif action == "kf" and len(parts) >= 4:
                    fpath = jdir / f"keyframe_{parts[3]}.jpg"
                    if fpath.exists():
                        self.send_file(fpath, "image/jpeg", head_only=head_only)
                        return
                elif action == "bg":
                    for name in ["generated_bg.jpg", "background.jpg"]:
                        fpath = jdir / name
                        if fpath.exists():
                            self.send_file(fpath, "image/jpeg", head_only=head_only)
                            return
                elif action == "product":
                    fpath = jdir / "product.jpg"
                    if fpath.exists():
                        self.send_file(fpath, "image/jpeg", head_only=head_only)
                        return
                elif action == "model":
                    fpath = jdir / "model.jpg"
                    if fpath.exists():
                        self.send_file(fpath, "image/jpeg", head_only=head_only)
                        return
                elif action == "tts" and len(parts) >= 4:
                    fpath = jdir / f"tts_{parts[3]}.mp3"
                    if fpath.exists():
                        self.send_file(fpath, "audio/mpeg", head_only=head_only)
                        return
                elif action in ["thumb", "poster"]:
                    thumb_path = jdir / "thumb.jpg"
                    if thumb_path.exists():
                        self.send_file(thumb_path, "image/jpeg", head_only=head_only)
                        return
                    kf1 = jdir / "keyframe_1.jpg"
                    if kf1.exists():
                        self.send_file(kf1, "image/jpeg", head_only=head_only)
                        return
                    prod = jdir / "product.jpg"
                    if prod.exists():
                        self.send_file(prod, "image/jpeg", head_only=head_only)
                        return
                    kfs = sorted(list(jdir.glob("keyframe_*.jpg")))
                    if kfs and kfs[0].exists():
                        self.send_file(kfs[0], "image/jpeg", head_only=head_only)
                        return
                    mp4 = jdir / "final_tvc.mp4"
                    if mp4.exists():
                        try:
                            import subprocess
                            subprocess.run([
                                "ffmpeg", "-y", "-ss", "00:00:00.5", "-i", str(mp4),
                                "-vframes", "1", "-q:v", "2", str(thumb_path)
                            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
                            if thumb_path.exists():
                                self.send_file(thumb_path, "image/jpeg", head_only=head_only)
                                return
                        except Exception:
                            pass

        # Serve Main HTML Web App
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        if not head_only:
            self.wfile.write(HTML_UI.encode("utf-8"))

    def send_file(self, path: Path, ctype: str, head_only: bool = False):
        file_size = path.stat().st_size
        range_header = self.headers.get("Range")
        is_download = "download=1" in self.path
        
        if range_header and range_header.startswith("bytes="):
            try:
                ranges = range_header.split("=")[1].split("-")
                start = int(ranges[0]) if ranges[0] else 0
                end = int(ranges[1]) if len(ranges) > 1 and ranges[1] else file_size - 1
                if end >= file_size:
                    end = file_size - 1
                content_length = end - start + 1
                self.send_response(206)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
                self.send_header("Content-Length", str(content_length))
                self.send_header("Accept-Ranges", "bytes")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Cache-Control", "no-cache, must-revalidate")
                if is_download:
                    self.send_header("Content-Disposition", f'attachment; filename="{path.name}"')
                self.end_headers()
                if not head_only:
                    with path.open("rb") as f:
                        f.seek(start)
                        self.wfile.write(f.read(content_length))
                return
            except Exception:
                pass

        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(file_size))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache, must-revalidate")
        if is_download:
            self.send_header("Content-Disposition", f'attachment; filename="{path.name}"')
        self.end_headers()
        if not head_only:
            with path.open("rb") as f:
                self.wfile.write(f.read())

    def do_POST(self):
        p = self.path.split("?")[0].strip("/")
        if p == "api/product/scrape":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length) if length > 0 else b"{}"
            try:
                req_data = json.loads(body.decode("utf-8")) if body else {}
            except Exception as e:
                self.send_response(400)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"success": False, "ok": False, "error": f"Invalid JSON body: {e}"}).encode("utf-8"))
                return

            raw_url = req_data.get("url", "")
            if not isinstance(raw_url, str) or not raw_url.strip():
                self.send_response(400)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"success": False, "ok": False, "error": "Vui lòng cung cấp URL sản phẩm Shopee hoặc TikTok Shop!"}).encode("utf-8"))
                return
            raw_url = raw_url.strip()

            try:
                from crawler import scrape_product
                product = scrape_product(raw_url)
                prod_dict = product.to_dict()
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"success": True, "ok": True, "product": prod_dict}, ensure_ascii=False).encode("utf-8"))
            except Exception as exc:
                self.send_response(400)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"success": False, "ok": False, "error": f"Không thể trích xuất dữ liệu từ đường link này: {str(exc)}"}).encode("utf-8"))
            return

        if p == "api/scene/update":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                req_data = json.loads(body.decode("utf-8"))
            except Exception as e:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Invalid JSON: {e}"}).encode("utf-8"))
                return

            job_id = req_data.get("job_id")
            try:
                scene_id = int(req_data.get("scene_id", 1))
            except:
                scene_id = 1
            image_prompt = req_data.get("image_prompt")
            motion_prompt = req_data.get("motion_prompt")
            dialogue = req_data.get("dialogue")

            job = JOBS.get(job_id)
            if not job:
                jfile = WORK_DIR / job_id / "job.json"
                if jfile.exists():
                    try:
                        job = json.loads(jfile.read_text())
                        JOBS[job_id] = job
                    except Exception:
                        pass
            if not job:
                self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Job {job_id} not found"}).encode("utf-8"))
                return

            scenes = job.get("scenes", [])
            if scene_id < 1 or scene_id > len(scenes):
                self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Scene {scene_id} not found"}).encode("utf-8"))
                return

            if image_prompt is not None:
                sc["image_generation_prompt"] = sanitize_safety_content(image_prompt.strip())
            if dialogue is not None:
                sc["audio_dialogue"] = sanitize_safety_content(dialogue.strip())
            if motion_prompt is not None:
                sc["video_motion_prompt"] = sanitize_safety_content(motion_prompt.strip())

            # Maintain strict lip synchronization between video prompt Say clause and audio dialogue
            if sc.get("audio_dialogue") and sc.get("video_motion_prompt"):
                voice_key = job.get("voice", "female_north")
                v_info = VOICE_PROFILES.get(voice_key, VOICE_PROFILES["female_north"])
                sc["video_motion_prompt"] = sync_dialogue_to_motion_prompt(
                    sc["video_motion_prompt"],
                    sc["audio_dialogue"],
                    v_info.get("say_clause", "")
                )

            update_job(job_id, scenes=scenes)
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({
                "ok": True,
                "message": f"Đã cập nhật prompt Cảnh {scene_id} thành công!",
                "scene": sc
            }, ensure_ascii=False).encode("utf-8"))
            return

        if p.startswith("api/job/resume"):
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                req_data = json.loads(body.decode("utf-8"))
            except Exception as e:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Invalid JSON: {e}"}).encode("utf-8"))
                return

            job_id = req_data.get("job_id")
            job = JOBS.get(job_id)
            if not job:
                jfile = WORK_DIR / job_id / "job.json"
                if jfile.exists():
                    try:
                        job = json.loads(jfile.read_text())
                        JOBS[job_id] = job
                    except Exception:
                        pass

            if not job:
                self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Job {job_id} not found"}).encode("utf-8"))
                return

            update_job(job_id, status="RESUMING", error=None, message="Đang tiếp tục tạo các phân cảnh còn thiếu...")
            threading.Thread(target=run_pipeline_worker, args=(job_id,), daemon=True).start()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "PROCESSING",
                "job_id": job_id,
                "message": "Đang tiếp tục tạo các phân cảnh còn thiếu..."
            }).encode("utf-8"))
            return

        if p.startswith("api/scene/regen-keyframe"):
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                req_data = json.loads(body.decode("utf-8"))
            except Exception as e:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Invalid JSON: {e}"}).encode("utf-8"))
                return

            job_id = req_data.get("job_id")
            try:
                scene_id = int(req_data.get("scene_id", 1))
            except:
                scene_id = 1
            image_prompt = req_data.get("image_prompt", "").strip()

            job = JOBS.get(job_id)
            if not job:
                jfile = WORK_DIR / job_id / "job.json"
                if jfile.exists():
                    try:
                        job = json.loads(jfile.read_text())
                        JOBS[job_id] = job
                    except Exception:
                        pass

            if not job:
                self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Job {job_id} not found"}).encode("utf-8"))
                return

            if job.get("regen_status", {}).get("status") == "REGENERATING":
                self.send_response(409)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Một tác vụ tái tạo khác đang chạy, vui lòng chờ trong giây lát."}).encode("utf-8"))
                return

            threading.Thread(
                target=run_keyframe_regeneration_worker,
                args=(job_id, scene_id, image_prompt),
                daemon=True
            ).start()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "PROCESSING",
                "job_id": job_id,
                "scene_id": scene_id,
                "message": f"Đang vẽ lại riêng Keyframe Cảnh {scene_id}..."
            }).encode("utf-8"))
            return

        if p.startswith("api/scene/regen"):
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                req_data = json.loads(body.decode("utf-8"))
            except Exception as e:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Invalid JSON: {e}"}).encode("utf-8"))
                return

            job_id = req_data.get("job_id")
            try:
                scene_id = int(req_data.get("scene_id", 1))
            except:
                scene_id = 1
            image_prompt = req_data.get("image_prompt", "").strip()
            motion_prompt = req_data.get("motion_prompt", "").strip()
            dialogue = req_data.get("dialogue", "").strip()
            regen_keyframe = bool(req_data.get("regen_keyframe", True))
            if req_data.get("video_engine"):
                job_engine = req_data.get("video_engine").strip().lower()
            else:
                job_engine = None

            job = JOBS.get(job_id)
            if not job:
                jfile = WORK_DIR / job_id / "job.json"
                if jfile.exists():
                    try:
                        job = json.loads(jfile.read_text())
                        JOBS[job_id] = job
                    except Exception:
                        pass

            if not job:
                self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Job {job_id} not found"}).encode("utf-8"))
                return

            if job_engine:
                job["video_engine"] = job_engine

            if job.get("regen_status", {}).get("status") == "REGENERATING":
                self.send_response(409)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Một cảnh khác đang được tái tạo, vui lòng chờ trong giây lát."}).encode("utf-8"))
                return

            threading.Thread(
                target=run_scene_regeneration_worker,
                args=(job_id, scene_id, image_prompt, motion_prompt, dialogue, regen_keyframe),
                daemon=True
            ).start()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "PROCESSING",
                "job_id": job_id,
                "scene_id": scene_id,
                "message": f"Bắt đầu tái tạo Phân Cảnh {scene_id}..."
            }).encode("utf-8"))
            return

        if p.startswith("api/scene/ai-fix-prompt") or p.startswith("api/scene/ai-fix"):
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                req_data = json.loads(body.decode("utf-8"))
            except Exception as e:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "error": f"Invalid JSON: {e}"}).encode("utf-8"))
                return

            job_id = req_data.get("job_id")
            try:
                scene_id = int(req_data.get("scene_id", 1))
            except:
                scene_id = 1
            auto_render = bool(req_data.get("auto_render", True))
            motion_override = req_data.get("motion_prompt", "").strip()
            dialogue_override = req_data.get("dialogue", "").strip()

            job = JOBS.get(job_id)
            if not job:
                jfile = WORK_DIR / job_id / "job.json"
                if jfile.exists():
                    try:
                        job = json.loads(jfile.read_text())
                        JOBS[job_id] = job
                    except Exception:
                        pass

            if not job:
                self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "error": f"Job {job_id} not found"}).encode("utf-8"))
                return

            scenes = job.get("scenes", [])
            if scene_id < 1 or scene_id > len(scenes):
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "error": f"Scene {scene_id} invalid"}).encode("utf-8"))
                return

            sc = scenes[scene_id - 1]
            motion_prompt = motion_override or sc.get("video_motion_prompt", "")
            dialogue = dialogue_override or sc.get("audio_dialogue", "")
            error_reason = sc.get("error", "")

            # Call AI fix prompt
            fix_res = ai_fix_scene_prompt(
                motion_prompt=motion_prompt,
                dialogue=dialogue,
                error_reason=error_reason,
                flow_mode=job.get("flow_mode", "store_review")
            )

            fixed_motion = fix_res.get("fixed_motion_prompt", motion_prompt)
            fixed_dialogue = fix_res.get("fixed_dialogue", dialogue)
            voice_key = job.get("voice", "female_north")
            v_info = VOICE_PROFILES.get(voice_key, VOICE_PROFILES["female_north"])
            fixed_motion = sync_dialogue_to_motion_prompt(fixed_motion, fixed_dialogue, v_info.get("say_clause", ""))
            explanation = fix_res.get("explanation", "AI đã tối ưu hóa prompt an toàn thành công.")

            # Update scene
            sc["video_motion_prompt"] = fixed_motion
            sc["audio_dialogue"] = fixed_dialogue
            sc["status_text"] = "AI đã sửa prompt an toàn. Đang render lại..."
            update_job(job_id, scenes=scenes)

            if auto_render:
                threading.Thread(
                    target=run_scene_regeneration_worker,
                    args=(job_id, scene_id, "", fixed_motion, fixed_dialogue, False),
                    daemon=True
                ).start()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "ok": True,
                "job_id": job_id,
                "scene_id": scene_id,
                "fixed_motion_prompt": fixed_motion,
                "fixed_dialogue": fixed_dialogue,
                "explanation": explanation,
                "auto_rendered": auto_render,
                "message": f"AI đã sửa prompt: {explanation}" + (" Đang tự động render lại video..." if auto_render else "")
            }).encode("utf-8"))
            return

        if p == "api/batch/refine-prompt":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                data = json.loads(body.decode("utf-8"))
            except Exception:
                data = {}
            prompt = data.get("prompt", "")
            refined = bis.refine_fashion_prompt(prompt)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True, "refined_prompt": refined}).encode("utf-8"))
            return

        if p.startswith("api/tvc/retry") or p.startswith("api/job/retry"):
            parts = p.split("/")
            req_job_id = parts[-1] if len(parts) >= 4 else None
            if not req_job_id or req_job_id in ["retry"]:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length > 0 else b"{}"
                try:
                    data = json.loads(body.decode("utf-8"))
                    req_job_id = data.get("job_id")
                except Exception:
                    pass
            if not req_job_id or req_job_id not in JOBS:
                self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "error": f"Job {req_job_id} not found"}).encode("utf-8"))
                return
            update_job(req_job_id, status="QUEUED", message="Đang khởi động lại pipeline...", error=None)
            threading.Thread(target=run_pipeline_worker, args=(req_job_id,), daemon=True).start()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True, "job_id": req_job_id, "message": "Đang thử lại pipeline..."}).encode("utf-8"))
            return

        if p == "api/batch/retry":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                data = json.loads(body.decode("utf-8"))
            except Exception:
                data = {}
            bid = data.get("batch_id")
            if data.get("all_failed") is True:
                with bis._batch_lock(bid):
                    batch = bis.BATCH_JOBS.get(bid, {})
                    items = batch.get("items", [])
                    busy = any(v.get("status") not in {"FAILED", "COMPLETED"}
                               or v.get("video_status") in {"SUBMITTING", "RENDERING", "GENERATING"}
                               for v in items)
                    retry_items = [v for v in items if v.get("status") == "FAILED" and v.get("retry_safe") is not False]
                    if busy or not retry_items:
                        self.send_response(409)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps({"ok": False, "error": "Batch đang chạy hoặc không có ảnh đủ điều kiện thử lại."}).encode("utf-8"))
                        return
                    for item in retry_items:
                        item["status"] = "PENDING"
                    batch["queue_version"] = 1
                    bis.save_batch_job(bid)
                    threading.Thread(target=bis.start_batch_pipeline, args=(bid,), daemon=True).start()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": True, "item_ids": [v["item_id"] for v in retry_items]}).encode("utf-8"))
                return
            item_id = int(data.get("item_id", 1))
            item = next((v for v in bis.BATCH_JOBS.get(bid, {}).get("items", []) if v.get("item_id") == item_id), None)
            if not item or item.get("status") != "FAILED" or item.get("retry_safe") is False:
                self.send_response(409)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "error": "Cần kiểm tra kết quả hiện có trước khi tạo lại ảnh."}).encode("utf-8"))
                return
            with bis._batch_lock(bid):
                item["status"] = "PENDING"
                bis.BATCH_JOBS[bid]["queue_version"] = 1
                bis.save_batch_job(bid)
            threading.Thread(target=bis.start_batch_pipeline, args=(bid,), daemon=True).start()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True, "message": f"Đang thử lại item #{item_id}..."}).encode("utf-8"))
            return

        if p == "api/batch/transfer-video":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                data = json.loads(body.decode("utf-8"))
            except Exception:
                data = {}
            bid = data.get("batch_id")
            item_id = int(data.get("item_id", 1))
            prompt = data.get("motion_prompt", "")
            if not bis.queue_batch_video(bid, item_id, prompt):
                self.send_response(409)
                self.end_headers()
                self.wfile.write(b'Video already active or requires reconciliation.')
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True, "message": f"Đang chuyển item #{item_id} sang Veo 3.1..."}).encode("utf-8"))
            return

        if p == "api/batch-image/create":
            content_type = self.headers.get("Content-Type", "")
            if "boundary=" not in content_type:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Expected multipart boundary")
                return
            boundary = content_type.split("boundary=")[1].strip().encode()
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            fields, files = bis.parse_multipart_form(body, boundary)

            face_list = files.get("face_image") or files.get("model") or []
            if not face_list:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Vui lòng tải lên ít nhất 1 ảnh mẫu (Face ID / Model)!"}).encode("utf-8"))
                return

            outfit_list = files.get("outfit_files") or files.get("outfit_images") or files.get("products") or []
            if not outfit_list:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Vui lòng tải lên ít nhất 1 ảnh sản phẩm / trang phục!"}).encode("utf-8"))
                return

            face_fname, face_bytes = face_list[0]
            batch_id = bis.create_batch_image_job(
                face_image_bytes=face_bytes,
                face_filename=face_fname,
                outfit_files=outfit_list,
                config=fields
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            b = bis.BATCH_JOBS.get(batch_id, {})
            self.wfile.write(json.dumps({
                "ok": True,
                "batch_id": batch_id,
                "total_items": len(b.get("items", []))
            }).encode("utf-8"))
            return

        if p == "api/batch-outfit/create":
            content_type = self.headers.get("Content-Type", "")
            if "boundary=" not in content_type:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Expected multipart boundary")
                return
            boundary = content_type.split("boundary=")[1].strip().encode()
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            fields, files = bis.parse_multipart_form(body, boundary)

            model_list = files.get("model_files") or files.get("model_images") or []
            outfit_list = files.get("outfit_files") or files.get("outfit_images") or []
            if not model_list or not outfit_list:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Vui lòng tải lên cả ảnh người mẫu và bộ sưu tập trang phục!"}).encode("utf-8"))
                return

            batch_id = bis.create_batch_outfit_job(
                model_files=model_list,
                outfit_files=outfit_list,
                config=fields
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            b = bis.BATCH_JOBS.get(batch_id, {})
            self.wfile.write(json.dumps({
                "ok": True,
                "batch_id": batch_id,
                "total_items": len(b.get("items", []))
            }).encode("utf-8"))
            return

        # Fashion Lookbook Create & Regen Endpoints
        if p == "api/fashion-lookbook/create":
            content_type = self.headers.get("Content-Type", "")
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length) if length > 0 else b""

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
                    req_json = json.loads(body.decode("utf-8")) if body else {}
                except Exception as e:
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": False, "error": f"Invalid JSON: {e}"}).encode("utf-8"))
                    return
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
                outfit_b64 = req_json.get("outfit_base64") or req_json.get("outfit_file_base64")
                if outfit_b64:
                    if "," in outfit_b64:
                        outfit_b64 = outfit_b64.split(",", 1)[1]
                    try:
                        outfit_bytes = base64.b64decode(outfit_b64)
                    except Exception:
                        pass
                model_b64 = req_json.get("model_base64") or req_json.get("model_file_base64")
                if model_b64:
                    if "," in model_b64:
                        model_b64 = model_b64.split(",", 1)[1]
                    try:
                        model_bytes = base64.b64decode(model_b64)
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
                self.send_response(400)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "error": "Vui lòng tải lên file ảnh trang phục hoặc truyền link outfit_image_url!"}, ensure_ascii=False).encode("utf-8"))
                return

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
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({
                "ok": True,
                "job_id": job_id,
                "status": "QUEUED",
                "message": "Đang khởi tạo pipeline Lookbook điện ảnh...",
                "template_id": template_id,
                "num_scenes": num_scenes,
                "total_duration_seconds": num_scenes * scene_duration,
                "aspect_ratio": aspect_ratio,
                "created_at": time.time()
            }, ensure_ascii=False).encode("utf-8"))
            return

        if p.startswith("api/fashion-lookbook/scene/") and p.endswith("/regen"):
            parts = p.split("/")
            if len(parts) >= 6:
                jid = parts[3]
                try:
                    sid = int(parts[4])
                except Exception:
                    sid = 1
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length > 0 else b"{}"
                try:
                    req_json = json.loads(body.decode("utf-8")) if body else {}
                except Exception:
                    req_json = {}
                override = req_json.get("camera_motion_override", "")
                result = fls.regen_lookbook_scene(jid, sid, override)
                self.send_response(200 if result.get("ok") else 400)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps(result, ensure_ascii=False).encode("utf-8"))
                return

        content_type = self.headers.get("Content-Type", "")
        if "boundary=" not in content_type:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Expected multipart boundary")
            return

        boundary = content_type.split("boundary=")[1].strip().encode()
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)

        job_id = str(uuid.uuid4())[:8]
        job_dir = WORK_DIR / job_id
        job_dir.mkdir(parents=True, exist_ok=True)

        parts = body.split(b"--" + boundary)
        tenant_id = ""
        if "?" in self.path:
            try:
                from urllib.parse import parse_qs, urlparse
                qs = parse_qs(urlparse(self.path).query)
                if "tenant_id" in qs and qs["tenant_id"]:
                    tenant_id = str(qs["tenant_id"][0]).strip()
            except Exception:
                pass
        if not tenant_id:
            tenant_id = str(self.headers.get("X-Tenant-Id") or "").strip()

        flow_mode = "pov"
        video_engine = "veo"
        resolution = "720p"
        num_scenes = 3
        num_threads = 5
        scene_duration = 8
        voice = "female_north"
        bgm = "tiktok_upbeat"
        script_style = "tiktok_shop"
        product_image_url = ""
        product_name = ""
        product_price = ""
        product_discount = ""
        product_shop = ""
        product_highlights = ""

        for part in parts:
            if b'name="tenant_id"' in part and b"\r\n\r\n" in part:
                _, val = part.split(b"\r\n\r\n", 1)
                t_val = val.strip().decode(errors="ignore")
                if t_val:
                    tenant_id = t_val
            elif b'name="flow_mode"' in part and b"\r\n\r\n" in part:
                _, val = part.split(b"\r\n\r\n", 1)
                m_str = val.strip().decode(errors="ignore").lower()
                if m_str in ["pov", "unboxing", "demo", "ugc", "store_review", "tvc", "fashion", "fashion_lookbook", "lookbook", "tryon", "virtual_tryon"]:
                    if m_str == "tvc":
                        flow_mode = "store_review"
                    elif m_str in ["fashion_lookbook", "lookbook", "tryon", "virtual_tryon"]:
                        flow_mode = "fashion"
                    else:
                        flow_mode = m_str
            elif b'name="script_style"' in part and b"\r\n\r\n" in part:
                _, val = part.split(b"\r\n\r\n", 1)
                s_style = val.strip().decode(errors="ignore")
                if s_style in ["tiktok_shop", "pov_ugc", "luxury"]:
                    script_style = s_style
            elif b'name="product_image_url"' in part and b"\r\n\r\n" in part:
                _, val = part.split(b"\r\n\r\n", 1)
                product_image_url = val.strip().decode(errors="ignore")
            elif b'name="product_name"' in part and b"\r\n\r\n" in part:
                _, val = part.split(b"\r\n\r\n", 1)
                product_name = val.strip().decode(errors="ignore")
            elif b'name="product_price"' in part and b"\r\n\r\n" in part:
                _, val = part.split(b"\r\n\r\n", 1)
                product_price = val.strip().decode(errors="ignore")
            elif b'name="product_discount"' in part and b"\r\n\r\n" in part:
                _, val = part.split(b"\r\n\r\n", 1)
                product_discount = val.strip().decode(errors="ignore")
            elif b'name="product_shop"' in part and b"\r\n\r\n" in part:
                _, val = part.split(b"\r\n\r\n", 1)
                product_shop = val.strip().decode(errors="ignore")
            elif b'name="product_highlights"' in part and b"\r\n\r\n" in part:
                _, val = part.split(b"\r\n\r\n", 1)
                product_highlights = val.strip().decode(errors="ignore")
            elif b'name="video_engine"' in part and b"\r\n\r\n" in part:
                _, val = part.split(b"\r\n\r\n", 1)
                ve_str = val.strip().decode(errors="ignore").lower()
                if "grok" in ve_str:
                    video_engine = "grok_10" if ("10" in ve_str or "v1" in ve_str) else "grok_15"
                else:
                    video_engine = "veo"
            elif b'name="video_model"' in part and b"\r\n\r\n" in part:
                _, val = part.split(b"\r\n\r\n", 1)
                vm_str = val.strip().decode(errors="ignore").lower()
                if "grok" in vm_str:
                    video_engine = "grok_10" if ("10" in vm_str or "v1" in vm_str) else "grok_15"
                else:
                    video_engine = "veo"
            elif b'name="resolution"' in part and b"\r\n\r\n" in part:
                _, val = part.split(b"\r\n\r\n", 1)
                r_str = val.strip().decode(errors="ignore").lower()
                if r_str in ["480p", "720p", "1080p"]:
                    resolution = r_str
            elif b'name="num_scenes"' in part and b"\r\n\r\n" in part:
                _, val = part.split(b"\r\n\r\n", 1)
                try:
                    num_scenes = max(1, min(5, int(val.strip())))
                except:
                    num_scenes = 3
            elif b'name="scene_duration"' in part and b"\r\n\r\n" in part:
                _, val = part.split(b"\r\n\r\n", 1)
                try:
                    val_int = int(val.strip())
                    if val_int in [4, 6, 8, 10]:
                        scene_duration = val_int
                except:
                    scene_duration = 6 if "grok" in video_engine else 8
            elif b'name="voice"' in part and b"\r\n\r\n" in part:
                _, val = part.split(b"\r\n\r\n", 1)
                v_str = val.strip().decode(errors="ignore")
                if v_str in VOICE_PROFILES:
                    voice = v_str
            elif b'name="bgm"' in part and b"\r\n\r\n" in part:
                _, val = part.split(b"\r\n\r\n", 1)
                b_str = val.strip().decode(errors="ignore")
                if b_str in BGM_PROFILES:
                    bgm = b_str
            elif b'name="num_threads"' in part and b"\r\n\r\n" in part:
                _, val = part.split(b"\r\n\r\n", 1)
                try:
                    num_threads = max(1, min(10, int(val.strip())))
                except:
                    num_threads = 5
            elif b'name="product"' in part and b"\r\n\r\n" in part:
                _, data = part.split(b"\r\n\r\n", 1)
                data = data.rstrip(b"\r\n")
                if data:
                    (job_dir / "product.jpg").write_bytes(data)
            elif b'name="model"' in part and b"\r\n\r\n" in part:
                _, data = part.split(b"\r\n\r\n", 1)
                data = data.rstrip(b"\r\n")
                if data:
                    (job_dir / "model.jpg").write_bytes(data)
            elif b'name="background"' in part and b"\r\n\r\n" in part:
                _, data = part.split(b"\r\n\r\n", 1)
                data = data.rstrip(b"\r\n")
                if data:
                    (job_dir / "background.jpg").write_bytes(data)

        # Fallback: if product image wasn't uploaded via file, download from product_image_url
        if not (job_dir / "product.jpg").exists() and product_image_url:
            if not is_safe_image_url(product_image_url):
                print(f"[TVC Server] Rejected unsafe product_image_url (SSRF guard): {product_image_url}")
            else:
                try:
                    from curl_cffi import requests as cffi_req
                    img_res = cffi_req.get(product_image_url, timeout=15)
                    if img_res.status_code == 200 and len(img_res.content) > 1000:
                        (job_dir / "product.jpg").write_bytes(img_res.content)
                except Exception as e_dl:
                    print(f"[TVC Server] Error downloading product_image_url {product_image_url}: {e_dl}")

        if not (job_dir / "product.jpg").exists():
            import shutil
            shutil.rmtree(job_dir, ignore_errors=True)
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": False, "error": "Vui lòng tải lên ít nhất 1 ảnh sản phẩm hoặc chọn ảnh từ gallery!"}).encode("utf-8"))
            return

        if "veo" in video_engine:
            resolution = "720p"

        JOBS[job_id] = {
            "job_id": job_id,
            "tenant_id": str(tenant_id).strip() if tenant_id else "",
            "status": "QUEUED",
            "message": "Đang khởi động pipeline tự động...",
            "flow_mode": flow_mode,
            "script_style": script_style,
            "product_name": product_name,
            "product_price": product_price,
            "product_discount": product_discount,
            "product_shop": product_shop,
            "product_highlights": product_highlights,
            "product_image_url": product_image_url,
            "video_engine": video_engine,
            "resolution": resolution,
            "num_scenes": num_scenes,
            "scene_duration": scene_duration,
            "voice": voice,
            "voice_bible": VOICE_BIBLE.get(voice, VOICE_BIBLE["female_north"]),
            "bgm": bgm,
            "num_threads": num_threads,
            "step": 0,
            "total_steps": 5,
            "created_at": time.time()
        }
        try:
            (job_dir / "job.json").write_text(json.dumps(JOBS[job_id], indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

        threading.Thread(target=run_pipeline_worker, args=(job_id,), daemon=True).start()

        accept = self.headers.get("Accept", "")
        if "application/json" in accept or p.startswith("api/") or p in ["api/tvc/create", "api/job/create"] or self.headers.get("X-Requested-With") == "XMLHttpRequest":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({
                "ok": True,
                "job_id": job_id,
                "status": "QUEUED",
                "message": "Đang khởi động pipeline tự động..."
            }).encode("utf-8"))
            return

        self.send_response(303)
        self.send_header("Location", f"/?job_id={job_id}")
        self.end_headers()

HTML_UI = r"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>FlowKit Studio - TVC & UGC AI 100% Tự Động (Zero-Touch)</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        :root { --bg: #0b0f19; --card: #1e293b; --border: #334155; --accent: #38bdf8; --green: #10b981; --purple: #a855f7; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--bg); color: #f8fafc; margin: 0; padding: 24px; display: flex; flex-direction: column; align-items: center; }
        .container { max-width: 1100px; width: 100%; }
        h1 { color: var(--accent); text-align: center; margin-bottom: 6px; font-size: 26px; font-weight: 800; letter-spacing: -0.5px; }
        .subtitle { text-align: center; color: #94a3b8; font-size: 14px; margin-bottom: 26px; }
        .card { background: var(--card); border: 1px solid var(--border); border-radius: 16px; padding: 28px; box-shadow: 0 10px 30px rgba(0,0,0,0.5); }
        .grid { display: grid; gap: 16px; margin-bottom: 24px; }
        .upload-box { border: 2px dashed #475569; border-radius: 12px; padding: 18px; text-align: center; background: #0f172a; cursor: pointer; transition: 0.2s; }
        .upload-box:hover { border-color: var(--accent); }
        .upload-box label { font-size: 13px; font-weight: 700; color: #cbd5e1; display: block; margin-bottom: 8px; }
        .upload-box small { font-size: 11px; color: #94a3b8; display: block; margin-top: 6px; }
        input[type="file"] { width: 100%; font-size: 12px; color: #94a3b8; }
        .options { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; background: #0f172a; padding: 16px 20px; border-radius: 12px; margin-bottom: 24px; border: 1px solid var(--border); }
        select { background: var(--card); color: #fff; border: 1px solid var(--border); padding: 9px 12px; border-radius: 6px; font-size: 13px; outline: none; width: 100%; }
        .btn-submit { width: 100%; padding: 16px; background: linear-gradient(135deg, #0284c7, #0369a1); color: white; border: none; border-radius: 10px; font-size: 16px; font-weight: 700; cursor: pointer; transition: 0.2s; box-shadow: 0 4px 15px rgba(2,132,199,0.4); }
        .btn-submit:hover { opacity: 0.95; transform: translateY(-1px); }
        
        /* 6 Modes Tab Switcher */
        .mode-container { display: grid; grid-template-columns: repeat(6, 1fr); gap: 10px; margin-bottom: 20px; }
        .mode-tab { padding: 14px 8px; border-radius: 12px; border: 2px solid var(--border); background: #0f172a; color: #94a3b8; font-size: 13px; font-weight: 700; cursor: pointer; transition: 0.2s; text-align: center; }
        .mode-tab:hover { border-color: var(--accent); color: #fff; }
        .mode-tab.active { background: linear-gradient(135deg, #0284c7, #0369a1); color: #fff; border-color: #38bdf8; box-shadow: 0 4px 15px rgba(2,132,199,0.4); }
        .mode-tab.active-unboxing { background: linear-gradient(135deg, #15803d, #166534); color: #fff; border-color: #4ade80; box-shadow: 0 4px 15px rgba(34,197,94,0.4); }
        .mode-tab.active-ugc { background: linear-gradient(135deg, #7c3aed, #6d28d9); color: #fff; border-color: #c084fc; box-shadow: 0 4px 15px rgba(124,58,237,0.4); }
        .mode-tab.active-demo { background: linear-gradient(135deg, #d97706, #b45309); color: #fff; border-color: #fbbf24; box-shadow: 0 4px 15px rgba(217,119,6,0.4); }
        .mode-tab.active-fashion { background: linear-gradient(135deg, #c026d3, #9333ea); color: #fff; border-color: #f0abfc; box-shadow: 0 4px 15px rgba(192,38,211,0.4); }
        @media (max-width: 990px) { .mode-container { grid-template-columns: repeat(3, 1fr); } }
        @media (max-width: 600px) { .mode-container { grid-template-columns: 1fr; } }
        
        .notice-box { padding: 12px 18px; border-radius: 10px; font-size: 13px; margin-bottom: 22px; line-height: 1.5; }
        
        /* Progress Box */
        .progress-box { margin-top: 30px; display: none; background: #0f172a; border: 1px solid var(--border); border-radius: 14px; padding: 24px; }
        .p-bar-bg { background: #1e293b; border-radius: 8px; height: 12px; overflow: hidden; margin-bottom: 14px; }
        .p-bar-fill { background: linear-gradient(90deg, #38bdf8, #10b981); height: 100%; width: 5%; transition: width 0.4s ease; }
        .p-status { font-size: 15px; font-weight: 600; color: var(--accent); margin-bottom: 4px; }
        .p-msg { font-size: 13px; color: #94a3b8; }
        
        /* Results */
        .result-box { margin-top: 30px; display: none; }
        .video-player { width: 320px; border-radius: 14px; border: 2px solid var(--accent); background: #000; margin: 0 auto; display: block; box-shadow: 0 10px 30px rgba(0,0,0,0.7); }
        .btn-download { display: block; text-align: center; max-width: 320px; margin: 14px auto 0; padding: 12px; background: var(--green); color: #fff; border-radius: 8px; font-weight: 700; text-decoration: none; font-size: 14px; }
        .asset-card { background: #0f172a; border: 1px solid var(--border); border-radius: 10px; padding: 10px; text-align: center; }
        .asset-card img { width: 100%; aspect-ratio: 9/16; object-fit: cover; border-radius: 6px; margin-bottom: 6px; border: 1px solid #334155; }

        /* Scene Studio & Editor */
        .scene-editor-card { background: #0f172a; border: 1px solid var(--border); border-radius: 14px; margin-bottom: 24px; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.4); transition: border-color 0.2s ease; }
        .scene-editor-card:hover { border-color: #38bdf8; }
        .scene-header { background: #1e293b; padding: 12px 18px; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); flex-wrap: wrap; gap: 10px; }
        .scene-title-group { display: flex; align-items: center; gap: 10px; }
        .scene-num-badge { background: #0284c7; color: #fff; font-weight: 700; font-size: 13px; padding: 4px 10px; border-radius: 6px; }
        .scene-dur-badge { background: #334155; color: #94a3b8; font-size: 12px; font-weight: 600; padding: 4px 8px; border-radius: 6px; }
        .scene-status-badge { font-size: 12px; font-weight: 600; padding: 4px 10px; border-radius: 6px; background: #064e3b; color: #6ee7b7; }
        .scene-body { display: grid; grid-template-columns: 240px 1fr; gap: 20px; padding: 18px; }
        @media (max-width: 840px) { .scene-body { grid-template-columns: 1fr; } }
        .scene-media-col { display: flex; flex-direction: column; gap: 14px; }
        .scene-media-box { background: #0b0f19; border: 1px solid #334155; border-radius: 8px; padding: 10px; text-align: center; }
        .scene-media-box label { font-size: 11px; font-weight: 700; color: #94a3b8; display: block; margin-bottom: 6px; text-align: left; }
        .scene-media-box video { width: 100%; aspect-ratio: 9/16; background: #000; border-radius: 6px; display: block; border: 1px solid #1e293b; }
        .scene-media-box img { width: 100%; aspect-ratio: 9/16; object-fit: cover; border-radius: 6px; display: block; border: 1px solid #1e293b; }
        .scene-prompt-col { display: flex; flex-direction: column; gap: 14px; }
        .prompt-group { display: flex; flex-direction: column; gap: 5px; }
        .prompt-group label { font-size: 12px; font-weight: 700; color: #cbd5e1; display: flex; justify-content: space-between; align-items: center; }
        .prompt-group label small { color: #64748b; font-weight: 400; font-size: 11px; }
        .prompt-group textarea { width: 100%; box-sizing: border-box; background: #0b0f19; color: #f8fafc; border: 1px solid #334155; border-radius: 8px; padding: 10px 12px; font-size: 12px; line-height: 1.5; font-family: inherit; resize: vertical; transition: border-color 0.2s; }
        .prompt-group textarea:focus { outline: none; border-color: #38bdf8; }
        .scene-btn-row { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-top: 6px; flex-wrap: wrap; }
        .btn-regen-scene { background: linear-gradient(135deg, #2563eb, #1d4ed8); color: white; border: none; border-radius: 8px; padding: 10px 18px; font-size: 13px; font-weight: 700; cursor: pointer; transition: 0.2s; display: inline-flex; align-items: center; gap: 6px; box-shadow: 0 4px 12px rgba(37,99,235,0.3); }
        .btn-regen-scene:hover { background: linear-gradient(135deg, #3b82f6, #2563eb); transform: translateY(-1px); }
        .btn-regen-scene:disabled { opacity: 0.6; cursor: not-allowed; transform: none; box-shadow: none; }
        .regen-msg { font-size: 12px; font-weight: 600; margin-top: 6px; }

        /* Studio Navigation Tabs */
        .studio-nav { display: flex; gap: 10px; justify-content: center; margin-bottom: 24px; flex-wrap: wrap; }
        .nav-btn { background: #1e293b; color: #94a3b8; border: 2px solid #334155; padding: 12px 20px; border-radius: 12px; font-size: 14px; font-weight: 700; cursor: pointer; transition: 0.2s; box-shadow: 0 4px 12px rgba(0,0,0,0.3); }
        .nav-btn:hover { border-color: #38bdf8; color: #fff; transform: translateY(-1px); }
        .nav-btn.active { background: linear-gradient(135deg, #0284c7, #0369a1); color: #fff; border-color: #38bdf8; box-shadow: 0 4px 18px rgba(2,132,199,0.5); }
        .nav-btn.active-batch-a { background: linear-gradient(135deg, #d97706, #b45309); color: #fff; border-color: #fbbf24; box-shadow: 0 4px 18px rgba(217,119,6,0.5); }
        .nav-btn.active-batch-b { background: linear-gradient(135deg, #059669, #047857); color: #fff; border-color: #34d399; box-shadow: 0 4px 18px rgba(16,185,129,0.5); }

        /* Preset Chips */
        .preset-chip { background: #1e293b; color: #cbd5e1; border: 1px solid #334155; padding: 6px 14px; border-radius: 20px; font-size: 12px; font-weight: 600; cursor: pointer; transition: 0.2s; }
        .preset-chip:hover { border-color: #38bdf8; color: #fff; }
        .preset-chip.active { background: #0284c7; border-color: #38bdf8; color: #fff; font-weight: 700; }

        /* Chips for uploaded files */
        .thumb-chip { position: relative; width: 64px; height: 84px; border-radius: 8px; overflow: hidden; border: 1px solid #334155; background: #0b0f19; flex-shrink: 0; }
        .thumb-chip img { width: 100%; height: 100%; object-fit: cover; }
        .thumb-chip-del { position: absolute; top: 2px; right: 2px; width: 18px; height: 18px; background: rgba(239,68,68,0.9); color: #fff; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 11px; font-weight: bold; cursor: pointer; border: none; }

        /* Gallery Cards */
        .gallery-card { background: #0f172a; border: 1px solid #334155; border-radius: 12px; padding: 14px; display: flex; flex-direction: column; gap: 10px; transition: border-color 0.2s; }
        .gallery-card:hover { border-color: #38bdf8; }
        .gallery-img-box { position: relative; width: 100%; aspect-ratio: 9/16; border-radius: 8px; overflow: hidden; background: #000; cursor: pointer; border: 1px solid #1e293b; }
        .gallery-img-box img { width: 100%; height: 100%; object-fit: cover; display: block; transition: transform 0.2s; }
        .gallery-img-box:hover img { transform: scale(1.02); }
        .gallery-refs { display: flex; gap: 8px; align-items: center; background: #1e293b; padding: 6px 10px; border-radius: 8px; }
        .ref-mini { width: 42px; height: 56px; object-fit: cover; border-radius: 4px; border: 1px solid #475569; }
    </style>
</head>
<body>
    <div class="container">
        <h1>STUDIO SẢN XUẤT VIDEO AI THƯƠNG MẠI (ZERO-TOUCH)</h1>
        <div class="subtitle">Engine Google Gemini 3.8 Flash (Vision & Biên kịch) &bull; Google Banana Pro 2 &bull; Veo 3.1 &bull; FFmpeg Audio Ducking</div>

        <div style="display: flex; justify-content: center; gap: 12px; margin-bottom: 22px; flex-wrap: wrap;">
            <button onclick="openUnusualModal()" style="background: #1e293b; color: #38bdf8; border: 1px solid #334155; padding: 8px 16px; border-radius: 8px; font-size: 13px; font-weight: 700; cursor: pointer; display: inline-flex; align-items: center; gap: 8px; transition: 0.2s;" onmouseover="this.style.borderColor='#38bdf8'" onmouseout="this.style.borderColor='#334155'">
                🛡️ Nhật Ký Unusual Activity & Proxy
                <span id="unusualBadge" style="background: #ef4444; color: #fff; font-size: 11px; padding: 2px 7px; border-radius: 10px; display: none;">0</span>
            </button>
            <button onclick="rotateProxiesNow()" id="btnRotateTop" style="background: #1e293b; color: #34d399; border: 1px solid #334155; padding: 8px 16px; border-radius: 8px; font-size: 13px; font-weight: 700; cursor: pointer; display: inline-flex; align-items: center; gap: 8px; transition: 0.2s;" onmouseover="this.style.borderColor='#34d399'" onmouseout="this.style.borderColor='#334155'">
                🔄 Xoay Proxy Thủ Công
            </button>
        </div>

        <!-- Master Studio Top Navigation Switcher -->
        <div class="studio-nav">
            <button class="nav-btn active" id="btnNavTvc" onclick="switchStudioView('tvc')">
                🎬 1. STUDIO TVC VIDEO (Zero-Touch)
            </button>
            <button class="nav-btn" id="btnNavBatchImage" onclick="switchStudioView('batch_image')">
                👗 2. MODULE A: TẠO ẢNH MẪU HÀNG LOẠT (1 Mẫu x N SP)
            </button>
            <button class="nav-btn" id="btnNavBatchOutfit" onclick="switchStudioView('batch_outfit')">
                ✨ 3. MODULE B: THAY ĐỒ VIRTUAL TRY-ON (All-x-All / 1-1)
            </button>
        </div>

        <!-- VIEW 1: TVC VIDEO STUDIO -->
        <div id="viewTvc" class="studio-view">
            <div class="card">
                <!-- 5 Commercial Modes -->
                <div class="mode-container">
                <div id="tabPov" class="mode-tab active" onclick="setFlowMode('pov')">
                    📦 1. FLOW POV TRÊN TAY<br>
                    <small style="font-weight:normal;opacity:0.85;">Chỉ 1 SP &bull; 2 Bàn tay &bull; Rủi ro 0%</small>
                </div>
                <div id="tabUnboxing" class="mode-tab" onclick="setFlowMode('unboxing')">
                    🎁 2. UNBOXING STUDIO<br>
                    <small style="font-weight:normal;opacity:0.85;">Chỉ 1 SP &bull; 100% Sản phẩm &bull; Không người</small>
                </div>
                <div id="tabDemo" class="mode-tab" onclick="setFlowMode('demo')">
                    ✨ 3. DEMO CÔNG DỤNG<br>
                    <small style="font-weight:normal;opacity:0.85;">1 SP (+ Mẫu) &bull; Thao tác &bull; Hiệu quả</small>
                </div>
                <div id="tabUgc" class="mode-tab" onclick="setFlowMode('ugc')">
                    📱 4. FLOW UGC NGƯỜI THẬT<br>
                    <small style="font-weight:normal;opacity:0.85;">1 SP + 1 Mẫu &bull; Phòng riêng &bull; Tâm sự</small>
                </div>
                <div id="tabReview" class="mode-tab" onclick="setFlowMode('store_review')">
                    🏪 5. REVIEW CỬA HÀNG<br>
                    <small style="font-weight:normal;opacity:0.85;">1 SP + 1 Mẫu &bull; Showroom &bull; Uy tín</small>
                </div>
                <div id="tabFashion" class="mode-tab" onclick="setFlowMode('fashion')">
                    👗 6. THỜI TRANG AI LOOKBOOK<br>
                    <small style="font-weight:normal;opacity:0.85;">1 Trang phục (+ Mẫu) &bull; 15 Dáng Studio &bull; Try-On</small>
                </div>
            </div>

            <!-- Dynamic Guide Notice -->
            <div id="modeNotice" class="notice-box" style="background:#1e1b4b;border:1px solid #4338ca;color:#c7d2fe;">
                <!-- Injected via JS -->
            </div>

            <form id="tvcForm" method="POST" enctype="multipart/form-data">
                <input type="hidden" name="flow_mode" id="flowModeInput" value="pov">
                
                <div class="grid" id="uploadGrid" style="grid-template-columns: 1fr 1fr;">
                    <div class="upload-box">
                        <label>1. Ảnh Sản Phẩm (Bắt buộc)</label>
                        <input type="file" name="product" accept="image/*" required>
                        <small>Gấu bông, mỹ phẩm, đồ chơi, gia dụng, thiết bị...</small>
                    </div>
                    <div class="upload-box" id="modelBox" style="display:none;">
                        <label id="modelLabel">2. Ảnh Người Mẫu / Creator</label>
                        <input type="file" name="model" id="modelInput" accept="image/*">
                        <small id="modelSubtext">Chân dung người đại diện hoặc creator</small>
                    </div>
                    <div class="upload-box" id="bgBox">
                        <label>3. Ảnh Bối Cảnh (Tùy chọn)</label>
                        <input type="file" name="background" accept="image/*">
                        <small id="bgSubtext">Bỏ trống: AI tự tạo mặt bàn đập hộp cực đẹp</small>
                    </div>
                </div>

                <div class="options">
                    <div>
                        <label style="font-size: 13px; font-weight: 700; color: #cbd5e1; display: block; margin-bottom: 6px;">🎙️ Giọng nói MC / KOL:</label>
                        <select name="voice" id="selVoice">
                            <option value="female_north" selected>👩 Nữ Miền Bắc (Thanh lịch, truyền cảm)</option>
                            <option value="female_south">👩 Nữ Miền Nam (Ngọt ngào, gần gũi)</option>
                            <option value="male_north">👨 Nam Miền Bắc (Trầm ấm, uy tín)</option>
                            <option value="male_south">👨 Nam Miền Nam (Năng động, hào sảng)</option>
                        </select>
                    </div>
                    <div>
                        <label style="font-size: 13px; font-weight: 700; color: #cbd5e1; display: block; margin-bottom: 6px;">⏱️ Thời lượng mỗi cảnh:</label>
                        <select name="scene_duration" id="selDuration">
                            <option value="8" selected>8 Giây (Chuẩn: 34-36 từ thoại)</option>
                            <option value="6">6 Giây (Năng động: 21-25 từ thoại)</option>
                            <option value="4">4 Giây (Siêu nhanh: 13-16 từ thoại)</option>
                        </select>
                    </div>
                    <div>
                        <label style="font-size: 13px; font-weight: 700; color: #cbd5e1; display: block; margin-bottom: 6px;">🎬 Số phân cảnh (1 - 5):</label>
                        <select name="num_scenes" id="selScenes">
                            <option value="1">1 Phân Cảnh (Test nhanh siêu tốc)</option>
                            <option value="2">2 Phân Cảnh (Hook + Giải pháp)</option>
                            <option value="3" selected>3 Phân Cảnh (Chuẩn TikTok: Hook - Demo - CTA)</option>
                            <option value="4">4 Phân Cảnh (Hook - Pain - Demo - CTA)</option>
                            <option value="5">5 Phân Cảnh (TVC thương mại điện ảnh)</option>
                        </select>
                    </div>
                    <div>
                        <label style="font-size: 13px; font-weight: 700; color: #cbd5e1; display: block; margin-bottom: 6px;">🎵 Nhạc nền BGM (Audio Ducking):</label>
                        <select name="bgm" id="selBgm">
                            <option value="tiktok_upbeat" selected>🔥 TikTok Upbeat Vui Tươi (Carefree - Đập Hộp/Review)</option>
                            <option value="tiktok_snitch">🕵️ Tò Mò & Bất Ngờ (Sneaky Snitch - Cú Lừa Thị Giác)</option>
                            <option value="tiktok_vlog">🎸 Acoustic Vlog Thực Tế (Daily Beetle - Chân Thật)</option>
                            <option value="acoustic_soft">🎵 Acoustic Nhẹ Nhàng (Dịu êm thư giãn)</option>
                            <option value="none">🔇 Tắt nhạc nền (Chỉ giọng AI)</option>
                        </select>
                    </div>
                    <div>
                        <label style="font-size: 13px; font-weight: 700; color: #cbd5e1; display: block; margin-bottom: 6px;">⚡ Tốc độ render song song:</label>
                        <select name="num_threads" id="selThreads">
                            <option value="5" selected>⚡ 5 Luồng song song (Siêu tốc)</option>
                            <option value="10">🚀 10 Luồng song song (Cực đại)</option>
                            <option value="3">✨ 3 Luồng song song (Cân bằng)</option>
                            <option value="1">🛡️ 1 Luồng (Tuần tự)</option>
                        </select>
                    </div>
                </div>

                <div style="margin-top: -8px; margin-bottom: 22px; font-size: 13px; color: #38bdf8; background: #0b1329; border: 1px solid #1e3a8a; border-radius: 8px; padding: 10px 16px; display: flex; align-items: center; justify-content: space-between;">
                    <span>✨ <strong>Thiết lập:</strong> <span id="summaryText">Đang tải...</span></span>
                    <span style="background: #1e293b; color: #a5f3fc; padding: 3px 8px; border-radius: 4px; font-size: 11px;">Veo 3.1 Low Priority &bull; Banana Pro 2</span>
                </div>

                <button type="submit" class="btn-submit" id="btnSubmit">&#9658; BẮT ĐẦU TẠO VIDEO TỰ ĐỘNG (ZERO-TOUCH)</button>
            </form>

            <div class="progress-box" id="pBox">
                <div class="p-status" id="pStatus">Đang xử lý...</div>
                <div class="p-msg" id="pMsg">Hệ thống đang chạy ngầm...</div>
                <div class="p-bar-bg" style="margin-top: 14px;">
                    <div class="p-bar-fill" id="pFill"></div>
                </div>
            </div>

            <div class="result-box" id="rBox">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
                    <h2 style="margin: 0; color: var(--green); font-size: 20px;">&#10004; VIDEO ĐÃ HOÀN TẤT XUẤT SẮC!</h2>
                    <a href="/" style="color: var(--accent); text-decoration: none; font-size: 14px; font-weight: 600; padding: 6px 12px; background: #0f172a; border-radius: 6px; border: 1px solid var(--border);">&#8635; Tạo Video Mới</a>
                </div>

                <div style="display: flex; flex-wrap: wrap; gap: 24px; align-items: flex-start;">
                    <!-- Left: Player -->
                    <div style="flex: 0 0 320px; margin: 0 auto;">
                        <video id="resVideo" class="video-player" controls autoplay loop playsinline></video>
                        <a id="resDl" href="#" download="final_video.mp4" class="btn-download">&#11015; Tải Video Về Máy</a>
                    </div>

                    <!-- Right: Visual Lock & Script -->
                    <div style="flex: 1; min-width: 320px;">
                        <h3 style="color: var(--accent); margin-top: 0; font-size: 16px;">📸 Tài Nguyên Khóa Hình Ảnh (Banana Pro 2 Anchors)</h3>
                        <div id="assetGrid" style="display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: 12px; margin-bottom: 20px;">
                            <!-- Injected by JS -->
                        </div>

                        <h3 style="color: var(--accent); margin-bottom: 8px; font-size: 16px;">📋 Hồ Sơ Bóc Tách & Kịch Bản 4 Trường</h3>
                        <div id="scriptBox" style="background: #0f172a; padding: 16px; border-radius: 10px; border: 1px solid var(--border); font-size: 13px; line-height: 1.6; color: #cbd5e1;">
                            <!-- Injected by JS -->
                        </div>
                    </div>
                </div>

                <!-- Scene Studio Section -->
                <div id="sceneStudioSection" style="margin-top: 32px; border-top: 2px solid var(--border); padding-top: 24px;">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 18px; flex-wrap: wrap; gap: 10px;">
                        <div>
                            <h2 style="color: var(--accent); font-size: 19px; margin: 0; display: flex; align-items: center; gap: 8px;">
                                <span>🎬 CHI TIẾT TỪNG PHÂN CẢNH & TÙY CHỈNH TÁI TẠO (SCENE STUDIO)</span>
                                <span style="font-size: 11px; background: #0369a1; color: #bae6fd; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Interactive Studio</span>
                            </h2>
                            <div style="color: #94a3b8; font-size: 13px; margin-top: 4px;">
                                Xem lại clip từng phân cảnh, kiểm tra ảnh keyframe & lời thoại, tinh chỉnh prompt và bấm tái tạo riêng lẻ từng cảnh nếu muốn.
                            </div>
                        </div>
                    </div>
                    <div id="scenesContainer">
                        <!-- Injected by JS -->
                    </div>
                </div>
            </div>
        </div>
    </div> <!-- /viewTvc -->

    <!-- VIEW 2: MODULE A - TẠO ẢNH MẪU HÀNG LOẠT (ImageCreateView) -->
    <div id="viewBatchImage" class="studio-view" style="display: none;">
        <div class="card">
            <div style="margin-bottom: 22px;">
                <h2 style="margin: 0 0 6px; color: #f59e0b; font-size: 21px; display: flex; align-items: center; gap: 8px;">
                    👗 MODULE A: TẠO ẢNH MẪU &amp; THỬ ĐỒ HÀNG LOẠT (ImageCreateView)
                </h2>
                <div style="font-size: 13px; color: #94a3b8; line-height: 1.5;">
                    Khóa 100% gương mặt mẫu ruột (Face ID) và lần lượt mặc thử / cầm trên tay ma trận sản phẩm mới với Google Banana Pro 2 (Dual-Reference Binding).
                </div>
            </div>

            <form id="formBatchImage" onsubmit="submitBatchImage(event)">
                <!-- 2 Upload Zones -->
                <div class="grid" style="grid-template-columns: 1fr 1fr;">
                    <div class="upload-box" onclick="document.getElementById('inputFaceImage').click()">
                        <label>👤 1. Ảnh Mẫu Gốc (Face ID - Khóa Khuôn Mặt &amp; Dáng Mẫu)</label>
                        <input type="file" id="inputFaceImage" name="face_image" accept="image/*" required style="display:none;" onchange="previewFaceFile(this)">
                        <div id="facePreviewBox" style="margin-top: 10px; display: none;">
                            <img id="facePreviewImg" style="width: 110px; height: 140px; object-fit: cover; border-radius: 8px; border: 2px solid #38bdf8; display: inline-block;">
                            <div id="faceFileName" style="font-size: 11px; color: #38bdf8; margin-top: 4px;"></div>
                        </div>
                        <small id="facePromptText">Bấm để tải 1 ảnh mẫu ruột của shop (Giữ nguyên gương mặt &amp; phong thái)</small>
                    </div>

                    <div class="upload-box" onclick="document.getElementById('inputOutfitFiles').click()">
                        <label>📦 2. Danh Sách Sản Phẩm / Trang Phục (Tối đa 50)</label>
                        <input type="file" id="inputOutfitFiles" name="outfit_files" accept="image/*" multiple required style="display:none;" onchange="previewOutfitFiles(this)">
                        <div id="outfitChipsBox" style="margin-top: 10px; display: flex; flex-wrap: wrap; gap: 8px; justify-content: center; max-height: 150px; overflow-y: auto;"></div>
                        <small id="outfitPromptText">Bấm để chọn nhiều ảnh sản phẩm/đồ (Giữ Ctrl/Shift để chọn tối đa 50 ảnh)</small>
                    </div>
                </div>

                <!-- Presets & Prompt Refiner -->
                <div style="background: #0f172a; border: 1px solid var(--border); border-radius: 12px; padding: 18px; margin-bottom: 20px;">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; flex-wrap: wrap; gap: 10px;">
                        <label style="font-size: 13px; font-weight: 700; color: #cbd5e1;">
                            🎨 Phong Cách &amp; Bối Cảnh (Prompt Director &amp; Presets):
                        </label>
                        <div style="display: flex; gap: 6px; flex-wrap: wrap;">
                            <button type="button" class="preset-chip active" id="chipPreset_fashion" onclick="applyPreset('fashion_studio')">👔 Fashion Studio</button>
                            <button type="button" class="preset-chip" id="chipPreset_store" onclick="applyPreset('store_context')">🏪 Store Context</button>
                            <button type="button" class="preset-chip" id="chipPreset_unboxing" onclick="applyPreset('unboxing')">🎁 Unboxing Style</button>
                            <button type="button" class="preset-chip" id="chipPreset_custom" onclick="applyPreset('custom')">✍️ Tùy Chỉnh</button>
                        </div>
                    </div>

                    <div style="position: relative;">
                        <textarea id="batchPromptInput" rows="3" style="width:100%; box-sizing:border-box; background:#0b0f19; border:1px solid #334155; border-radius:8px; color:#fff; padding:10px 14px; font-size:13px; font-family:inherit; resize:vertical;" placeholder="Nhập mô tả bối cảnh, ánh sáng, góc chụp..."></textarea>
                        <button type="button" id="btnRefinePrompt" onclick="handleRefinePromptClick()" style="margin-top: 8px; background: #1e293b; border: 1px solid #a855f7; color: #d8b4fe; padding: 6px 14px; border-radius: 6px; font-size: 12px; font-weight: 700; cursor: pointer; display: inline-flex; align-items: center; gap: 6px; transition: 0.2s;">
                            ✨ Tối ưu Prompt bằng Gemini AI (Pro Prompt Refiner)
                        </button>
                    </div>
                </div>

                <!-- BatchConfig Parameters -->
                <div class="options" style="grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));">
                    <div>
                        <label style="font-size: 12px; font-weight: 700; color: #cbd5e1; display: block; margin-bottom: 6px;">🤖 Model AI:</label>
                        <select id="selBatchModel">
                            <option value="Nano Banana Pro" selected>🍌 Nano Banana Pro (Mặc định)</option>
                            <option value="Nano Banana 2">🍌 Nano Banana 2 (Tốc độ)</option>
                        </select>
                    </div>
                    <div>
                        <label style="font-size: 12px; font-weight: 700; color: #cbd5e1; display: block; margin-bottom: 6px;">📐 Tỷ Lệ Khung Hình:</label>
                        <select id="selBatchAspect">
                            <option value="9:16" selected>9:16 (TikTok Shop, Reels)</option>
                            <option value="1:1">1:1 (Shopee, Catalog)</option>
                            <option value="16:9">16:9 (YouTube, Landscape)</option>
                        </select>
                    </div>
                    <div>
                        <label style="font-size: 12px; font-weight: 700; color: #cbd5e1; display: block; margin-bottom: 6px;">🔍 Độ Phân Giải:</label>
                        <select id="selBatchRes">
                            <option value="1K">1K Standard</option>
                            <option value="2K" selected>2K HD (Khuyên dùng)</option>
                            <option value="4K">4K Ultra (Chi tiết vải)</option>
                        </select>
                    </div>
                    <div>
                        <label style="font-size: 12px; font-weight: 700; color: #cbd5e1; display: block; margin-bottom: 6px;">📸 Biến Thể / Sản Phẩm:</label>
                        <select id="selBatchCount" onchange="updateBatchCalculation()">
                            <option value="1" selected>1 ảnh (Góc chính diện)</option>
                            <option value="2">2 ảnh (Chính diện + Góc nghiêng)</option>
                            <option value="4">4 ảnh (Đa góc thời trang)</option>
                        </select>
                    </div>
                    <div>
                        <label style="font-size: 12px; font-weight: 700; color: #cbd5e1; display: block; margin-bottom: 6px;">⚡ Tốc Độ Xử Lý:</label>
                        <select id="selBatchConcurrency">
                            <option value="5" selected>Tự động — xếp lượt công bằng</option>
                        </select>
                    </div>
                </div>

                <!-- Auto Transfer to Video Switch -->
                <div style="background: #1e1b4b; border: 1px solid #4338ca; border-radius: 10px; padding: 12px 18px; margin-bottom: 22px; display: flex; align-items: center; justify-content: space-between; gap: 14px; flex-wrap: wrap;">
                    <div>
                        <strong style="color: #c7d2fe; font-size: 13px;">🎬 Cầu nối Tự động hóa Sang Video (autoTransferToVideo):</strong>
                        <div style="font-size: 12px; color: #94a3b8; margin-top: 2px;">
                            Tự động chuyển ảnh sinh ra làm frame đầu cho Google Veo 3.1 tạo luôn video TikTok Shop 8s tương ứng!
                        </div>
                    </div>
                    <label style="display: flex; align-items: center; gap: 8px; cursor: pointer;">
                        <input type="checkbox" id="checkAutoTransferVideo" style="width: 18px; height: 18px; cursor: pointer;">
                        <span style="font-size: 13px; font-weight: 700; color: #a5b4fc;">BẬT TỰ ĐỘNG SINH VIDEO VEO</span>
                    </label>
                </div>

                <!-- Dynamic calculation banner -->
                <div id="batchCalcBanner" style="background: #064e3b; border: 1px solid #059669; color: #a7f3d0; padding: 12px 16px; border-radius: 10px; font-size: 13px; font-weight: 700; margin-bottom: 20px; text-align: center;">
                    📊 Ma trận ước tính: 0 sản phẩm × 1 biến thể = 0 ảnh thành phẩm
                </div>

                <button type="submit" id="btnSubmitBatchImage" class="btn-submit" style="background: linear-gradient(135deg, #d97706, #b45309);">
                    🚀 BẮT ĐẦU TẠO MA TRẬN ẢNH HÀNG LOẠT (BANANA PRO 2)
                </button>
            </form>
        </div>
    </div>

    <!-- VIEW 3: MODULE B - THAY ĐỒ VIRTUAL TRY-ON (OutfitCreateView) -->
    <div id="viewBatchOutfit" class="studio-view" style="display: none;">
        <div class="card">
            <div style="margin-bottom: 22px;">
                <h2 style="margin: 0 0 6px; color: #10b981; font-size: 21px; display: flex; align-items: center; gap: 8px;">
                    ✨ MODULE B: THAY ĐỒ VIRTUAL TRY-ON HÀNG LOẠT (OutfitCreateView)
                </h2>
                <div style="font-size: 13px; color: #94a3b8; line-height: 1.5;">
                    Thử bộ sưu tập quần áo mới lên người mẫu ở các tư thế &amp; bối cảnh khác nhau (Outfit Swap) với Dual-Reference Binding.
                </div>
            </div>

            <!-- 2 Modes: All-x-All vs 1-1 -->
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 20px;">
                <div id="tabOutfitAll" class="mode-tab active" onclick="setOutfitPairMode('all-x-all')">
                    🔄 1. TẤT CẢ x TẤT CẢ (All-x-All)<br>
                    <small style="font-weight:normal;opacity:0.85;">M Mẫu × N Đồ (Thử toàn bộ catalog)</small>
                </div>
                <div id="tabOutfitOne" class="mode-tab" onclick="setOutfitPairMode('one-to-one')">
                    🎯 2. GHÉP CẶP 1-1 (One-to-One)<br>
                    <small style="font-weight:normal;opacity:0.85;">Cặp 1-1 = Min(M, N) (Thử theo danh sách định sẵn)</small>
                </div>
            </div>

            <form id="formBatchOutfit" onsubmit="submitBatchOutfit(event)">
                <input type="hidden" id="outfitPairModeInput" value="all-x-all">

                <!-- 2 Upload Zones -->
                <div class="grid" style="grid-template-columns: 1fr 1fr;">
                    <div class="upload-box" onclick="document.getElementById('inputModelFiles').click()">
                        <label>👥 1. Danh Sách Người Mẫu / Bối Cảnh Gốc (M Mẫu)</label>
                        <input type="file" id="inputModelFiles" name="model_files" accept="image/*" multiple required style="display:none;" onchange="previewModelFiles(this)">
                        <div id="modelChipsBox" style="margin-top: 10px; display: flex; flex-wrap: wrap; gap: 8px; justify-content: center; max-height: 150px; overflow-y: auto;"></div>
                        <small id="modelPromptText">Bấm để chọn 1 hoặc nhiều ảnh người mẫu trong các bối cảnh khác nhau</small>
                    </div>

                    <div class="upload-box" onclick="document.getElementById('inputOutfitSwapFiles').click()">
                        <label>👗 2. Bộ Sưu Tập Trang Phục Cần Mặc Thử (N Đồ)</label>
                        <input type="file" id="inputOutfitSwapFiles" name="outfit_files" accept="image/*" multiple required style="display:none;" onchange="previewOutfitSwapFiles(this)">
                        <div id="outfitSwapChipsBox" style="margin-top: 10px; display: flex; flex-wrap: wrap; gap: 8px; justify-content: center; max-height: 150px; overflow-y: auto;"></div>
                        <small id="outfitSwapPromptText">Bấm để chọn 1 hoặc nhiều ảnh trang phục cần thay</small>
                    </div>
                </div>

                <!-- Detailed Styling Note -->
                <div style="background: #0f172a; border: 1px solid var(--border); border-radius: 12px; padding: 18px; margin-bottom: 20px;">
                    <label style="font-size: 13px; font-weight: 700; color: #cbd5e1; display: block; margin-bottom: 8px;">
                        ✂️ Ghi Chú May Mặc &amp; Styling Chi Tiết (outfitNote):
                    </label>
                    <textarea id="outfitNoteInput" rows="2" style="width:100%; box-sizing:border-box; background:#0b0f19; border:1px solid #334155; border-radius:8px; color:#fff; padding:10px 14px; font-size:13px; font-family:inherit; resize:vertical;" placeholder="Ví dụ: Sơ vin áo vào quần, xắn tay áo lên khuỷu tay, quần dài chấm mắt cá chân, giữ nguyên phụ kiện vòng cổ..."></textarea>
                </div>

                <!-- BatchConfig -->
                <div class="options" style="grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));">
                    <div>
                        <label style="font-size: 12px; font-weight: 700; color: #cbd5e1; display: block; margin-bottom: 6px;">🤖 Model AI:</label>
                        <select id="selOutfitModel">
                            <option value="Nano Banana Pro" selected>🍌 Nano Banana Pro</option>
                            <option value="Nano Banana 2">🍌 Nano Banana 2</option>
                        </select>
                    </div>
                    <div>
                        <label style="font-size: 12px; font-weight: 700; color: #cbd5e1; display: block; margin-bottom: 6px;">📐 Tỷ Lệ Khung Hình:</label>
                        <select id="selOutfitAspect">
                            <option value="9:16" selected>9:16 (TikTok Shop)</option>
                            <option value="1:1">1:1 (Shopee Catalog)</option>
                            <option value="16:9">16:9 (Landscape)</option>
                        </select>
                    </div>
                    <div>
                        <label style="font-size: 12px; font-weight: 700; color: #cbd5e1; display: block; margin-bottom: 6px;">🔍 Độ Phân Giải:</label>
                        <select id="selOutfitRes">
                            <option value="2K" selected>2K HD</option>
                            <option value="4K">4K Ultra</option>
                            <option value="1K">1K Standard</option>
                        </select>
                    </div>
                    <div>
                        <label style="font-size: 12px; font-weight: 700; color: #cbd5e1; display: block; margin-bottom: 6px;">⚡ Tốc Độ Xử Lý:</label>
                        <select id="selOutfitConcurrency">
                            <option value="5" selected>Tự động — xếp lượt công bằng</option>
                        </select>
                    </div>
                </div>

                <!-- Auto Transfer to Video Switch -->
                <div style="background: #1e1b4b; border: 1px solid #4338ca; border-radius: 10px; padding: 12px 18px; margin-bottom: 22px; display: flex; align-items: center; justify-content: space-between; gap: 14px; flex-wrap: wrap;">
                    <div>
                        <strong style="color: #c7d2fe; font-size: 13px;">🎬 Cầu nối Tự động hóa Sang Video (autoTransferToVideo):</strong>
                        <div style="font-size: 12px; color: #94a3b8; margin-top: 2px;">
                            Tự động chuyển ảnh sang Google Veo 3.1 tạo luôn video clip người mẫu 8s chuyển động mượt mà!
                        </div>
                    </div>
                    <label style="display: flex; align-items: center; gap: 8px; cursor: pointer;">
                        <input type="checkbox" id="checkOutfitAutoTransferVideo" style="width: 18px; height: 18px; cursor: pointer;">
                        <span style="font-size: 13px; font-weight: 700; color: #a5b4fc;">BẬT TỰ ĐỘNG SINH VIDEO VEO</span>
                    </label>
                </div>

                <div id="outfitCalcBanner" style="background: #064e3b; border: 1px solid #059669; color: #a7f3d0; padding: 12px 16px; border-radius: 10px; font-size: 13px; font-weight: 700; margin-bottom: 20px; text-align: center;">
                    📊 Ma trận tác vụ: 0 mẫu × 0 trang phục = 0 ảnh thử đồ
                </div>

                <button type="submit" id="btnSubmitBatchOutfit" class="btn-submit" style="background: linear-gradient(135deg, #10b981, #059669);">
                    🚀 BẮT ĐẦU THAY ĐỒ VIRTUAL TRY-ON HÀNG LOẠT
                </button>
            </form>
        </div>
    </div>

    <!-- COMMON BATCH DASHBOARD & LIVE GALLERY -->
    <div id="batchDashboard" style="margin-top: 30px; display: none; width: 100%;">
        <!-- Top Progress Card -->
        <div class="card" style="margin-bottom: 24px;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 14px; flex-wrap: wrap; gap: 10px;">
                <div>
                    <span id="batchModuleTitle" style="font-size: 16px; font-weight: 700; color: #38bdf8;">Đang xử lý ma trận...</span>
                    <span id="batchIdBadge" style="font-size: 11px; background: #334155; padding: 3px 8px; border-radius: 6px; margin-left: 8px; color: #cbd5e1;"></span>
                </div>
                <div id="batchMsg" style="font-size: 13px; color: #94a3b8;">Đang khởi động...</div>
            </div>

            <div class="p-bar-bg">
                <div id="batchBarFill" class="p-bar-fill" style="width: 0%;"></div>
            </div>

            <!-- Metric Badges -->
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: 10px; margin-top: 14px;">
                <div style="background: #0f172a; border: 1px solid #334155; padding: 10px; border-radius: 8px; text-align: center;">
                    <div style="font-size: 11px; color: #94a3b8;">Tổng Số Tác Vụ</div>
                    <div id="batchStatTotal" style="font-size: 18px; font-weight: 800; color: #fff;">0</div>
                </div>
                <div style="background: #064e3b; border: 1px solid #059669; padding: 10px; border-radius: 8px; text-align: center;">
                    <div style="font-size: 11px; color: #6ee7b7;">Đã Hoàn Tất</div>
                    <div id="batchStatDone" style="font-size: 18px; font-weight: 800; color: #34d399;">0</div>
                </div>
                <div style="background: #78350f; border: 1px solid #d97706; padding: 10px; border-radius: 8px; text-align: center;">
                    <div style="font-size: 11px; color: #fde68a;">Đang Xử Lý</div>
                    <div id="batchStatRunning" style="font-size: 18px; font-weight: 800; color: #fbbf24;">0</div>
                </div>
                <div style="background: #0f172a; border: 1px solid #334155; padding: 10px; border-radius: 8px; text-align: center;">
                    <div style="font-size: 11px; color: #94a3b8;">Chờ Lượt</div>
                    <div id="batchStatWaiting" style="font-size: 18px; font-weight: 800; color: #fff;">0</div>
                </div>
                <div style="background: #450a0a; border: 1px solid #b91c1c; padding: 10px; border-radius: 8px; text-align: center;">
                    <div style="font-size: 11px; color: #fca5a5;">Thất Bại</div>
                    <div id="batchStatFailed" style="font-size: 18px; font-weight: 800; color: #f87171;">0</div>
                </div>
            </div>
        </div>

        <!-- Gallery Grid -->
        <div class="card">
            <h3 style="margin: 0 0 16px; color: #cbd5e1; font-size: 18px; display: flex; align-items: center; justify-content: space-between;">
                <span>🖼️ Gallery Ảnh Thành Phẩm &amp; Video Chuyển Tiếp</span>
                <button onclick="if(currentBatchId) pollBatchStatus(currentBatchId)" style="background: #1e293b; border: 1px solid #334155; color: #38bdf8; font-size: 12px; padding: 5px 12px; border-radius: 6px; cursor: pointer;">🔄 Làm Mới</button>
            </h3>
            <div id="batchGalleryGrid" style="display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 16px;">
                <!-- Rendered by JS -->
            </div>
        </div>
    </div>

    <!-- Image Zoom Modal -->
    <div id="imgZoomModal" onclick="this.style.display='none'" style="display:none; position:fixed; inset:0; background:rgba(0,0,0,0.85); z-index:9999; align-items:center; justify-content:center; cursor:zoom-out; padding:20px;">
        <img id="imgZoomTarget" style="max-width:90vw; max-height:90vh; border-radius:12px; box-shadow:0 10px 40px rgba(0,0,0,0.8); border:2px solid #38bdf8;">
    </div>
    </div> <!-- /container -->

    <script>
        // ── Studio Navigation (TVC vs Batch Image vs Virtual Try-on) ──
        function switchStudioView(viewName) {
            const btnTvc = document.getElementById('btnNavTvc');
            const btnImg = document.getElementById('btnNavBatchImage');
            const btnOutfit = document.getElementById('btnNavBatchOutfit');
            const viewTvc = document.getElementById('viewTvc');
            const viewImg = document.getElementById('viewBatchImage');
            const viewOutfit = document.getElementById('viewBatchOutfit');

            btnTvc.classList.remove('active', 'active-batch-a', 'active-batch-b');
            btnImg.classList.remove('active', 'active-batch-a', 'active-batch-b');
            btnOutfit.classList.remove('active', 'active-batch-a', 'active-batch-b');

            viewTvc.style.display = 'none';
            viewImg.style.display = 'none';
            viewOutfit.style.display = 'none';

            if (viewName === 'tvc') {
                btnTvc.classList.add('active');
                viewTvc.style.display = 'block';
            } else if (viewName === 'batch_image') {
                btnImg.classList.add('active-batch-a');
                viewImg.style.display = 'block';
                updateBatchCalculation();
            } else if (viewName === 'batch_outfit') {
                btnOutfit.classList.add('active-batch-b');
                viewOutfit.style.display = 'block';
                updateOutfitCalculation();
            }
        }

        function zoomImage(src) {
            const modal = document.getElementById('imgZoomModal');
            const target = document.getElementById('imgZoomTarget');
            if (modal && target) {
                target.src = src;
                modal.style.display = 'flex';
            }
        }

        // ── MODULE A: Tạo Ảnh Mẫu Hàng Loạt (ImageCreateView) ──
        let batchFaceFile = null;
        let batchOutfitFiles = [];

        function previewFaceFile(input) {
            if (input.files && input.files[0]) {
                batchFaceFile = input.files[0];
                const img = document.getElementById('facePreviewImg');
                const box = document.getElementById('facePreviewBox');
                const name = document.getElementById('faceFileName');
                const prompt = document.getElementById('facePromptText');
                img.src = URL.createObjectURL(batchFaceFile);
                name.innerText = `✅ ${batchFaceFile.name} (${(batchFaceFile.size/1024).toFixed(0)} KB)`;
                box.style.display = 'block';
                prompt.style.display = 'none';
            }
        }

        function previewOutfitFiles(input) {
            if (!input.files) return;
            const newFiles = Array.from(input.files);
            for (const f of newFiles) {
                if (batchOutfitFiles.length >= 50) {
                    alert('Hệ thống hỗ trợ tối đa 50 sản phẩm trong một ma trận!');
                    break;
                }
                batchOutfitFiles.push({
                    file: f,
                    url: URL.createObjectURL(f),
                    name: f.name
                });
            }
            input.value = '';
            renderOutfitChips();
            updateBatchCalculation();
        }

        function removeOutfitFile(index, event) {
            if (event) event.stopPropagation();
            batchOutfitFiles.splice(index, 1);
            renderOutfitChips();
            updateBatchCalculation();
        }

        function renderOutfitChips() {
            const container = document.getElementById('outfitChipsBox');
            const prompt = document.getElementById('outfitPromptText');
            if (!container) return;
            if (batchOutfitFiles.length === 0) {
                container.innerHTML = '';
                if (prompt) prompt.style.display = 'block';
                return;
            }
            if (prompt) prompt.style.display = 'none';
            let html = '';
            batchOutfitFiles.forEach((item, idx) => {
                html += `
                    <div class="thumb-chip" title="${escapeHtml(item.name)}">
                        <img src="${item.url}" alt="${idx + 1}">
                        <button type="button" class="thumb-chip-del" onclick="removeOutfitFile(${idx}, event)">&times;</button>
                    </div>
                `;
            });
            container.innerHTML = html;
        }

        function updateBatchCalculation() {
            const banner = document.getElementById('batchCalcBanner');
            const count = batchOutfitFiles.length;
            const variants = parseInt(document.getElementById('selBatchCount')?.value || '1');
            const total = count * variants;
            if (banner) {
                banner.innerText = `📊 Ma trận ước tính: ${count} sản phẩm × ${variants} biến thể = ${total} ảnh thành phẩm`;
                if (total > 0) {
                    banner.style.background = "#064e3b";
                    banner.style.borderColor = "#059669";
                } else {
                    banner.style.background = "#1e293b";
                    banner.style.borderColor = "#334155";
                }
            }
        }

        const BATCH_PRESETS = {
            fashion_studio: "High-end fashion studio lighting, minimalist background.",
            store_context: "Professional retail store background, organized shelves with products, soft warm lighting, realistic store atmosphere.",
            unboxing: "Close up on hands opening a premium package on a clean white desk, cinematic focus.",
            custom: ""
        };

        function applyPreset(presetKey) {
            ['fashion', 'store', 'unboxing', 'custom'].forEach(k => {
                const chip = document.getElementById(`chipPreset_${k}`);
                if (chip) chip.classList.remove('active');
            });
            const keyMap = { fashion_studio: 'fashion', store_context: 'store', unboxing: 'unboxing', custom: 'custom' };
            const chip = document.getElementById(`chipPreset_${keyMap[presetKey]}`);
            if (chip) chip.classList.add('active');

            const area = document.getElementById('batchPromptInput');
            if (area) {
                if (presetKey === 'custom') {
                    area.placeholder = 'Nhập mô tả bối cảnh và chỉ đạo mỹ thuật tùy chỉnh...';
                    area.focus();
                } else {
                    area.value = BATCH_PRESETS[presetKey] || '';
                }
            }
        }

        async function handleRefinePromptClick() {
            const area = document.getElementById('batchPromptInput');
            const btn = document.getElementById('btnRefinePrompt');
            if (!area || !btn) return;
            const currentText = area.value.trim() || 'Fashion studio photo';
            btn.disabled = true;
            btn.innerHTML = '⏳ Đang gọi Gemini AI tối ưu...';
            try {
                const res = await fetch('/api/batch/refine-prompt', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ prompt: currentText })
                });
                const d = await res.json();
                if (d.ok && d.refined_prompt) {
                    area.value = d.refined_prompt;
                    btn.innerHTML = '✅ Đã tối ưu Pro Prompt!';
                    setTimeout(() => {
                        btn.disabled = false;
                        btn.innerHTML = '✨ Tối ưu Prompt bằng Gemini AI (Pro Prompt Refiner)';
                    }, 2500);
                } else {
                    throw new Error(d.error || 'Không nhận được phản hồi');
                }
            } catch (e) {
                alert('Lỗi tối ưu prompt: ' + e.message);
                btn.disabled = false;
                btn.innerHTML = '✨ Tối ưu Prompt bằng Gemini AI (Pro Prompt Refiner)';
            }
        }

        async function submitBatchImage(event) {
            event.preventDefault();
            if (!batchFaceFile) {
                alert('Vui lòng chọn 1 ảnh mẫu gốc (Face ID) để khóa danh tính!');
                return;
            }
            if (batchOutfitFiles.length === 0) {
                alert('Vui lòng chọn ít nhất 1 ảnh sản phẩm / trang phục!');
                return;
            }

            const btn = document.getElementById('btnSubmitBatchImage');
            btn.disabled = true;
            btn.innerText = '⏳ Đang nạp ma trận vào hàng đợi...';

            const fd = new FormData();
            fd.append('face_image', batchFaceFile, batchFaceFile.name);
            batchOutfitFiles.forEach((item) => {
                fd.append('outfit_files', item.file, item.name);
            });

            fd.append('imageModel', document.getElementById('selBatchModel').value);
            fd.append('aspectRatio', document.getElementById('selBatchAspect').value);
            fd.append('imageResolution', document.getElementById('selBatchRes').value);
            fd.append('imageCountPerOutfit', document.getElementById('selBatchCount').value);
            fd.append('imageRunMode', document.getElementById('selBatchConcurrency').value);
            fd.append('autoTransferToVideo', document.getElementById('checkAutoTransferVideo').checked ? 'true' : 'false');
            fd.append('customPrompt', document.getElementById('batchPromptInput').value);

            try {
                const res = await fetch('/api/batch-image/create', {
                    method: 'POST',
                    body: fd
                });
                const d = await res.json();
                if (!res.ok || !d.ok) {
                    throw new Error(d.error || 'Lỗi server khởi tạo batch');
                }

                currentBatchId = d.batch_id;
                document.getElementById('batchDashboard').style.display = 'block';
                document.getElementById('batchDashboard').scrollIntoView({ behavior: 'smooth' });
                pollBatchStatus(currentBatchId);
            } catch (err) {
                alert('Khởi tạo ma trận thất bại: ' + err.message);
            } finally {
                btn.disabled = false;
                btn.innerText = '🚀 BẮT ĐẦU TẠO MA TRẬN ẢNH HÀNG LOẠT (BANANA PRO 2)';
            }
        }

        // ── MODULE B: Thay Đồ Virtual Try-On (OutfitCreateView) ──
        let batchModelFiles = [];
        let batchOutfitSwapFiles = [];
        let currentOutfitPairMode = 'all-x-all';

        function setOutfitPairMode(mode) {
            currentOutfitPairMode = mode;
            document.getElementById('outfitPairModeInput').value = mode;
            const tabAll = document.getElementById('tabOutfitAll');
            const tabOne = document.getElementById('tabOutfitOne');
            if (mode === 'all-x-all') {
                tabAll.classList.add('active');
                tabOne.classList.remove('active');
            } else {
                tabOne.classList.add('active');
                tabAll.classList.remove('active');
            }
            updateOutfitCalculation();
        }

        function previewModelFiles(input) {
            if (!input.files) return;
            const newFiles = Array.from(input.files);
            for (const f of newFiles) {
                if (batchModelFiles.length >= 50) break;
                batchModelFiles.push({ file: f, url: URL.createObjectURL(f), name: f.name });
            }
            input.value = '';
            renderModelChips();
            updateOutfitCalculation();
        }

        function removeModelFile(index, event) {
            if (event) event.stopPropagation();
            batchModelFiles.splice(index, 1);
            renderModelChips();
            updateOutfitCalculation();
        }

        function renderModelChips() {
            const container = document.getElementById('modelChipsBox');
            const prompt = document.getElementById('modelPromptText');
            if (!container) return;
            if (batchModelFiles.length === 0) {
                container.innerHTML = '';
                if (prompt) prompt.style.display = 'block';
                return;
            }
            if (prompt) prompt.style.display = 'none';
            let html = '';
            batchModelFiles.forEach((item, idx) => {
                html += `
                    <div class="thumb-chip" title="${escapeHtml(item.name)}">
                        <img src="${item.url}" alt="${idx + 1}">
                        <button type="button" class="thumb-chip-del" onclick="removeModelFile(${idx}, event)">&times;</button>
                    </div>
                `;
            });
            container.innerHTML = html;
        }

        function previewOutfitSwapFiles(input) {
            if (!input.files) return;
            const newFiles = Array.from(input.files);
            for (const f of newFiles) {
                if (batchOutfitSwapFiles.length >= 50) break;
                batchOutfitSwapFiles.push({ file: f, url: URL.createObjectURL(f), name: f.name });
            }
            input.value = '';
            renderOutfitSwapChips();
            updateOutfitCalculation();
        }

        function removeOutfitSwapFile(index, event) {
            if (event) event.stopPropagation();
            batchOutfitSwapFiles.splice(index, 1);
            renderOutfitSwapChips();
            updateOutfitCalculation();
        }

        function renderOutfitSwapChips() {
            const container = document.getElementById('outfitSwapChipsBox');
            const prompt = document.getElementById('outfitSwapPromptText');
            if (!container) return;
            if (batchOutfitSwapFiles.length === 0) {
                container.innerHTML = '';
                if (prompt) prompt.style.display = 'block';
                return;
            }
            if (prompt) prompt.style.display = 'none';
            let html = '';
            batchOutfitSwapFiles.forEach((item, idx) => {
                html += `
                    <div class="thumb-chip" title="${escapeHtml(item.name)}">
                        <img src="${item.url}" alt="${idx + 1}">
                        <button type="button" class="thumb-chip-del" onclick="removeOutfitSwapFile(${idx}, event)">&times;</button>
                    </div>
                `;
            });
            container.innerHTML = html;
        }

        function updateOutfitCalculation() {
            const banner = document.getElementById('outfitCalcBanner');
            const M = batchModelFiles.length;
            const N = batchOutfitSwapFiles.length;
            const total = currentOutfitPairMode === 'all-x-all' ? (M * N) : Math.min(M, N);
            if (banner) {
                banner.innerText = `📊 Ma trận tác vụ: ${M} mẫu × ${N} trang phục = ${total} ảnh thử đồ (${currentOutfitPairMode === 'all-x-all' ? 'Tất Cả x Tất Cả' : 'Ghép Cặp 1-1'})`;
                if (total > 0) {
                    banner.style.background = "#064e3b";
                    banner.style.borderColor = "#059669";
                } else {
                    banner.style.background = "#1e293b";
                    banner.style.borderColor = "#334155";
                }
            }
        }

        async function submitBatchOutfit(event) {
            event.preventDefault();
            if (batchModelFiles.length === 0) {
                alert('Vui lòng chọn ít nhất 1 ảnh người mẫu!');
                return;
            }
            if (batchOutfitSwapFiles.length === 0) {
                alert('Vui lòng chọn ít nhất 1 bộ trang phục!');
                return;
            }

            const btn = document.getElementById('btnSubmitBatchOutfit');
            btn.disabled = true;
            btn.innerText = '⏳ Đang nạp ma trận thay đồ...';

            const fd = new FormData();
            batchModelFiles.forEach(item => {
                fd.append('model_files', item.file, item.name);
            });
            batchOutfitSwapFiles.forEach(item => {
                fd.append('outfit_files', item.file, item.name);
            });

            fd.append('outfitPairMode', currentOutfitPairMode);
            fd.append('outfitNote', document.getElementById('outfitNoteInput').value);
            fd.append('imageModel', document.getElementById('selOutfitModel').value);
            fd.append('aspectRatio', document.getElementById('selOutfitAspect').value);
            fd.append('imageResolution', document.getElementById('selOutfitRes').value);
            fd.append('imageRunMode', document.getElementById('selOutfitConcurrency').value);
            fd.append('autoTransferToVideo', document.getElementById('checkOutfitAutoTransferVideo').checked ? 'true' : 'false');

            try {
                const res = await fetch('/api/batch-outfit/create', {
                    method: 'POST',
                    body: fd
                });
                const d = await res.json();
                if (!res.ok || !d.ok) {
                    throw new Error(d.error || 'Lỗi khởi tạo batch outfit');
                }

                currentBatchId = d.batch_id;
                document.getElementById('batchDashboard').style.display = 'block';
                document.getElementById('batchDashboard').scrollIntoView({ behavior: 'smooth' });
                pollBatchStatus(currentBatchId);
            } catch (err) {
                alert('Khởi tạo thay đồ thất bại: ' + err.message);
            } finally {
                btn.disabled = false;
                btn.innerText = '🚀 BẮT ĐẦU THAY ĐỒ VIRTUAL TRY-ON HÀNG LOẠT';
            }
        }

        // ── Batch Polling & Live Gallery Engine ──
        let currentBatchId = null;

        async function pollBatchStatus(batchId) {
            currentBatchId = batchId;
            try {
                const res = await fetch(`/api/batch/status/${batchId}`);
                if (!res.ok) return;
                const b = await res.json();

                const titleEl = document.getElementById('batchModuleTitle');
                const badgeEl = document.getElementById('batchIdBadge');
                const msgEl = document.getElementById('batchMsg');
                const fillEl = document.getElementById('batchBarFill');

                if (titleEl) titleEl.innerText = b.module_title || 'Tiến Trình Ma Trận';
                if (badgeEl) badgeEl.innerText = `Batch #${batchId}`;
                if (msgEl) msgEl.innerText = b.message || 'Đang xử lý...';

                const s = b.stats || {};
                const pct = s.progress_percent || 0;
                if (fillEl) fillEl.style.width = `${pct}%`;

                document.getElementById('batchStatTotal').innerText = s.total || 0;
                document.getElementById('batchStatDone').innerText = s.completed || 0;
                document.getElementById('batchStatRunning').innerText = s.processing || 0;
                document.getElementById('batchStatFailed').innerText = s.failed || 0;
                document.getElementById('batchStatWaiting').innerText = (s.pending || 0) + (s.video_pending || 0);

                renderBatchGallery(b);

                if (!s.is_done) {
                    setTimeout(() => pollBatchStatus(batchId), 2500);
                }
            } catch (e) {
                console.error("Batch poll err:", e);
                setTimeout(() => pollBatchStatus(batchId), 3000);
            }
        }

        function renderBatchGallery(b) {
            const grid = document.getElementById('batchGalleryGrid');
            if (!grid || !b.items) return;

            let html = '';
            b.items.forEach(item => {
                const statusColor = item.status === 'COMPLETED' ? '#10b981' : (item.status === 'FAILED' ? '#ef4444' : '#f59e0b');
                const statusText = item.status === 'COMPLETED' ? '✅ Hoàn Tất' : (item.status === 'FAILED' ? '❌ Thất Bại' : `⏳ ${item.message || 'Đang tạo'}`);
                const hasImg = item.status === 'COMPLETED' && item.image_url;

                html += `
                    <div class="gallery-card">
                        <!-- Dual References Mini Header -->
                        <div class="gallery-refs">
                            <div style="display:flex;align-items:center;gap:6px;flex:1;">
                                <img src="/batch/${b.batch_id}/ref/face/${item.face_index}" class="ref-mini" title="Ref 1: Khuôn mặt / Mẫu" onerror="this.style.display='none'">
                                <span style="font-size:11px;color:#94a3b8;">+</span>
                                <img src="/batch/${b.batch_id}/ref/outfit/${item.outfit_index}" class="ref-mini" title="Ref 2: Sản phẩm / Đồ" onerror="this.style.display='none'">
                            </div>
                            <span style="font-size:11px;font-weight:700;color:${statusColor};">${statusText}</span>
                        </div>

                        <!-- Image Preview Box -->
                        <div class="gallery-img-box" onclick="${hasImg ? `zoomImage('${item.image_url}')` : ''}">
                            ${hasImg ? `
                                <img src="${item.image_url}" alt="Item ${item.item_id}">
                            ` : `
                                <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;color:#64748b;font-size:13px;padding:20px;text-align:center;">
                                    ${item.status === 'FAILED' ? `❌ Lỗi: ${escapeHtml(item.error || 'Thất bại')}` : '⏳ Google Banana Pro 2 đang sinh ảnh...'}
                                </div>
                            `}
                        </div>

                        <!-- Title and Actions -->
                        <div style="display:flex;justify-content:space-between;align-items:center;margin-top:2px;">
                            <span style="font-size:12px;font-weight:700;color:#e2e8f0;">${escapeHtml(item.title)}</span>
                            ${hasImg ? `
                                <a href="${item.image_url}" download="batch_${b.batch_id}_item_${item.item_id}.jpg" style="background:#1e293b;border:1px solid #334155;color:#38bdf8;padding:4px 8px;border-radius:6px;font-size:11px;text-decoration:none;font-weight:600;">📥 Tải</a>
                            ` : ''}
                        </div>

                        <!-- Video Player or Transfer Video Button -->
                        ${item.video_url ? `
                            <div style="margin-top:6px;border-top:1px solid #334155;padding-top:8px;">
                                <div style="font-size:11px;color:#34d399;font-weight:700;margin-bottom:4px;">🎬 Video Veo 3.1 (8s):</div>
                                <video src="${item.video_url}" controls playsinline style="width:100%;aspect-ratio:9/16;border-radius:6px;background:#000;"></video>
                            </div>
                        ` : (hasImg ? `
                            <div style="margin-top:4px;">
                                ${['QUEUED', 'RENDERING', 'SUBMITTING'].includes(item.video_status) ? `
                                    <div style="font-size:11px;color:#fbbf24;text-align:center;padding:6px;background:#1e1b4b;border-radius:6px;">⏳ ${item.video_status === 'QUEUED' ? 'Video đang chờ lượt' : 'Veo 3.1 đang xử lý video 8s...'}</div>
                                ` : `
                                    <button type="button" onclick="transferItemToVideo('${b.batch_id}', ${item.item_id})" style="width:100%;background:#1e293b;border:1px solid #4338ca;color:#c7d2fe;padding:6px 10px;border-radius:6px;font-size:11px;font-weight:700;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;gap:6px;transition:0.2s;" onmouseover="this.style.background='#312e81'" onmouseout="this.style.background='#1e293b'">
                                        🎬 Tạo Video Veo 3.1 (8s)
                                    </button>
                                `}
                            </div>
                        ` : '')}

                        <!-- Retry button if failed -->
                        ${item.status === 'FAILED' && item.retry_safe !== false ? `
                            <button type="button" onclick="retryBatchItem('${b.batch_id}', ${item.item_id})" style="background:#7f1d1d;border:1px solid #ef4444;color:#fecaca;padding:6px 10px;border-radius:6px;font-size:11px;font-weight:700;cursor:pointer;margin-top:4px;">
                                🔄 Thử Lại Lần Nữa
                            </button>
                        ` : ''}
                    </div>
                `;
            });
            grid.innerHTML = html;
        }

        async function retryBatchItem(batchId, itemId) {
            try {
                const response = await fetch('/api/batch/retry', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ batch_id: batchId, item_id: itemId })
                });
                if (!response.ok) {
                    const detail = await response.json();
                    throw new Error(detail.error || 'Không thể thử lại tác vụ này.');
                }
                pollBatchStatus(batchId);
            } catch (e) {
                alert('Lỗi khi thử lại: ' + e.message);
            }
        }

        async function transferItemToVideo(batchId, itemId) {
            try {
                await fetch('/api/batch/transfer-video', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ batch_id: batchId, item_id: itemId })
                });
                pollBatchStatus(batchId);
            } catch (e) {
                alert('Lỗi tạo video: ' + e.message);
            }
        }

        const MODES_CONFIG = {
            pov: {
                tabClass: 'active',
                noticeBg: '#1e1b4b',
                noticeBorder: '#4338ca',
                noticeColor: '#c7d2fe',
                noticeHtml: '✨ <strong>Chế độ 1: POV Trên Tay (Point-of-View):</strong> Chỉ cần <strong>1 ảnh sản phẩm thô duy nhất</strong>. Hai bàn tay tự nhiên tương tác trên mặt bàn (mở khóa kéo, nắn bóp chất vải, thử độ đàn hồi), <strong>tuyệt đối không có mặt người</strong> &bull; Rủi ro méo mặt = 0%, chi tiết sản phẩm chân thực!',
                needModel: false,
                bgSubtext: 'Bỏ trống: AI tự tạo mặt bàn gỗ aesthetic hoặc bàn unboxing cực sạch'
            },
            unboxing: {
                tabClass: 'active-unboxing',
                noticeBg: '#052e16',
                noticeBorder: '#16a34a',
                noticeColor: '#bbf7d0',
                noticeHtml: '✨ <strong>Chế độ 2: Unboxing Studio (100% Sản Phẩm - Không người):</strong> Chỉ cần <strong>1 ảnh sản phẩm thô duy nhất</strong>. Góc quay cận cảnh Macro/Close-up điện ảnh, mở hộp quà sang trọng hé lộ sản phẩm đặt trên bục trưng bày studio, <strong>TUYỆT ĐỐI KHÔNG CÓ NGƯỜI & KHÔNG CÓ BÀN TAY</strong> &bull; Tôn vinh 100% chi tiết, chất liệu và sự tinh xảo hoàn hảo của sản phẩm!',
                needModel: false,
                bgSubtext: 'Bỏ trống: AI tự tạo hộp quà sang trọng & bục trưng bày studio ánh sáng nghệ thuật'
            },
            demo: {
                tabClass: 'active-demo',
                noticeBg: '#451a03',
                noticeBorder: '#d97706',
                noticeColor: '#fde68a',
                noticeHtml: '✨ <strong>Chế độ 3: Demo Công Dụng (Thao tác & Hiệu quả thực tế):</strong> Chỉ cần <strong>1 ảnh sản phẩm thô duy nhất</strong> (hoặc tải thêm ảnh mẫu tùy ý). Góc quay cận cảnh Macro/Close-up, 2 bàn tay tự nhiên tương tác thực tế (bơm kem, thoa thử, bấm nút, test công năng, kết quả trước/sau) &bull; Khách hàng tận mắt thấy công năng sản phẩm hoạt động trực quan!',
                needModel: false,
                optionalModel: true,
                modelLabel: '2. Ảnh Người Mẫu (Tùy chọn - không bắt buộc)',
                modelSubtext: 'Bỏ trống: AI tập trung 2 bàn tay tương tác sản phẩm. Tải lên: Xuất hiện mẫu thao tác',
                bgSubtext: 'Bỏ trống: AI tự tạo mặt bàn studio / bối cảnh demo công năng sạch sẽ chuẩn quốc tế'
            },
            ugc: {
                tabClass: 'active-ugc',
                noticeBg: '#2e1065',
                noticeBorder: '#7c3aed',
                noticeColor: '#e9d5ff',
                noticeHtml: '✨ <strong>Chế độ 4: UGC (User Generated Content):</strong> Cần <strong>1 Ảnh SP + 1 Ảnh Mẫu</strong>. Góc máy cận diện đối diện / selfie, creator ngồi phòng riêng ấm cúng hoặc bàn làm việc cá nhân, lời thoại tâm sự đời thường, khuyên dùng chân thực &bull; Tỉ lệ chuyển đổi đơn hàng TikTok Shop cực cao!',
                needModel: true,
                modelLabel: '2. Ảnh Người Mẫu / Creator UGC (Bắt buộc)',
                modelSubtext: 'Chân dung Creator / Bạn trẻ với phong cách gần gũi, tự nhiên',
                bgSubtext: 'Bỏ trống: AI tự sinh phòng ngủ cá nhân / bàn làm việc ấm cúng đèn vàng'
            },
            store_review: {
                tabClass: 'active',
                noticeBg: '#082f49',
                noticeBorder: '#0284c7',
                noticeColor: '#bae6fd',
                noticeHtml: '✨ <strong>Chế độ 5: Review Cửa Hàng (Showroom):</strong> Cần <strong>1 Ảnh SP + 1 Ảnh Mẫu</strong>. Góc máy ngang tầm mắt, KOL lịch sự chuyên nghiệp, bối cảnh quầy kệ showroom / boutique sang trọng &bull; Phù hợp thương hiệu cao cấp, mỹ phẩm, đồ hiệu!',
                needModel: true,
                modelLabel: '2. Ảnh Người Mẫu KOL (Bắt buộc)',
                modelSubtext: 'Chân dung KOL / Chuyên gia mặc trang phục lịch sự',
                bgSubtext: 'Bỏ trống: AI tự sinh showroom sang trọng, kệ trưng bày hiện đại'
            },
            fashion: {
                tabClass: 'active-fashion',
                noticeBg: '#3b0764',
                noticeBorder: '#c026d3',
                noticeColor: '#f5d0fe',
                noticeHtml: '👗 <strong>Chế độ 6: Thời Trang AI (Fashion Lookbook & Virtual Try-On):</strong> Cần <strong>1 ảnh sản phẩm thời trang/quần áo</strong> (hoặc tải thêm ảnh người mẫu tùy chọn). Tự động kích hoạt 15 Studio Poses điện ảnh, giữ trọn form dáng, chất vải và chuyển động sàn catwalk high-fashion cực chuẩn. (Bỏ trống ảnh mẫu: Tự động dùng siêu mẫu chuẩn AI studio)!',
                needModel: false,
                optionalModel: true,
                modelLabel: '2. Ảnh Người Mẫu (Tùy chọn - Tự động có mẫu mặc định)',
                modelSubtext: 'Bỏ trống: Hệ thống dùng ảnh siêu mẫu chuẩn studio. Tải lên: Mẫu riêng của bạn',
                bgSubtext: 'Bỏ trống: AI tự tạo studio lookbook tối giản cao cấp (Cyclorama trắng / sàn gỗ tối)'
            }
        };

        function setFlowMode(mode) {
            document.getElementById('flowModeInput').value = mode;
            const tabPov = document.getElementById('tabPov');
            const tabUnboxing = document.getElementById('tabUnboxing');
            const tabDemo = document.getElementById('tabDemo');
            const tabUgc = document.getElementById('tabUgc');
            const tabReview = document.getElementById('tabReview');
            const tabFashion = document.getElementById('tabFashion');
            const modelBox = document.getElementById('modelBox');
            const modelInput = document.getElementById('modelInput');
            const modelLabel = document.getElementById('modelLabel');
            const modelSubtext = document.getElementById('modelSubtext');
            const bgSubtext = document.getElementById('bgSubtext');
            const modeNotice = document.getElementById('modeNotice');
            const uploadGrid = document.getElementById('uploadGrid');

            [tabPov, tabUnboxing, tabDemo, tabUgc, tabReview, tabFashion].forEach(t => {
                if (t) {
                    t.classList.remove('active');
                    t.classList.remove('active-unboxing');
                    t.classList.remove('active-demo');
                    t.classList.remove('active-ugc');
                    t.classList.remove('active-fashion');
                }
            });

            const cfg = MODES_CONFIG[mode] || MODES_CONFIG.pov;
            if (mode === 'pov') {
                tabPov?.classList.add(cfg.tabClass);
            } else if (mode === 'unboxing') {
                tabUnboxing?.classList.add(cfg.tabClass);
            } else if (mode === 'demo') {
                tabDemo?.classList.add(cfg.tabClass);
            } else if (mode === 'ugc') {
                tabUgc?.classList.add(cfg.tabClass);
            } else if (mode === 'fashion') {
                tabFashion?.classList.add(cfg.tabClass);
            } else {
                tabReview?.classList.add(cfg.tabClass);
            }

            modeNotice.style.background = cfg.noticeBg;
            modeNotice.style.borderColor = cfg.noticeBorder;
            modeNotice.style.color = cfg.noticeColor;
            modeNotice.innerHTML = cfg.noticeHtml;
            bgSubtext.innerText = cfg.bgSubtext;

            if (cfg.needModel) {
                modelBox.style.display = 'block';
                modelInput.setAttribute('required', 'required');
                modelLabel.innerText = cfg.modelLabel;
                modelSubtext.innerText = cfg.modelSubtext;
                uploadGrid.style.gridTemplateColumns = 'repeat(3, 1fr)';
            } else if (cfg.optionalModel) {
                modelBox.style.display = 'block';
                modelInput.removeAttribute('required');
                modelLabel.innerText = cfg.modelLabel;
                modelSubtext.innerText = cfg.modelSubtext;
                uploadGrid.style.gridTemplateColumns = 'repeat(3, 1fr)';
            } else {
                modelBox.style.display = 'none';
                modelInput.removeAttribute('required');
                uploadGrid.style.gridTemplateColumns = '1fr 1fr';
            }

            updateSummary();
        }

        function updateSummary() {
            const mode = document.getElementById('flowModeInput')?.value || 'pov';
            let modeName = '📦 POV Trên Tay (1 SP)';
            if (mode === 'unboxing') modeName = '🎁 Unboxing Studio 100% SP (1 SP)';
            else if (mode === 'demo') modeName = '✨ Demo Công Dụng (Thao tác & Hiệu quả)';
            else if (mode === 'ugc') modeName = '📱 UGC Người Thật (1 SP + 1 Mẫu)';
            else if (mode === 'store_review') modeName = '🏪 Review Cửa Hàng (1 SP + 1 Mẫu)';
            else if (mode === 'fashion') modeName = '👗 Thời Trang AI Lookbook & Try-On (1 Trang phục)';

            const v = document.getElementById('selVoice');
            const vText = v ? v.options[v.selectedIndex].text.split('(')[0].trim() : 'Nữ Miền Bắc';
            const d = parseInt(document.getElementById('selDuration')?.value || '8');
            const s = parseInt(document.getElementById('selScenes')?.value || '3');
            const bgm = document.getElementById('selBgm');
            const bgmText = bgm ? bgm.options[bgm.selectedIndex].text.split('(')[0].trim() : 'Acoustic';
            const t = document.getElementById('selThreads')?.value || '5';
            const total = s * d;

            const el = document.getElementById('summaryText');
            if (el) {
                el.innerHTML = `<strong>${modeName}</strong> &bull; ${s} Cảnh &bull; ${d}s/cảnh &bull; <strong>Tổng video ~${total}s</strong> &bull; ${vText} &bull; ${bgmText} &bull; ${t} Luồng`;
            }
        }

        document.getElementById('selVoice')?.addEventListener('change', updateSummary);
        document.getElementById('selDuration')?.addEventListener('change', updateSummary);
        document.getElementById('selScenes')?.addEventListener('change', updateSummary);
        document.getElementById('selBgm')?.addEventListener('change', updateSummary);
        document.getElementById('selThreads')?.addEventListener('change', updateSummary);
        
        // Initialize default view
        setFlowMode('pov');

        const urlParams = new URLSearchParams(window.location.search);
        const jobId = urlParams.get('job_id');
        if (jobId) {
            document.getElementById('pBox').style.display = 'block';
            document.getElementById('btnSubmit').disabled = true;
            document.getElementById('btnSubmit').style.opacity = '0.5';
            pollStatus(jobId);
        }

        async function pollStatus(jid) {
            try {
                const res = await fetch(`/api/status/${jid}`);
                const data = await res.json();
                
                const pStatus = document.getElementById('pStatus');
                const pMsg = document.getElementById('pMsg');
                const pFill = document.getElementById('pFill');

                pStatus.innerText = data.status || 'Đang xử lý...';
                pMsg.innerText = data.message || '';

                const step = data.step || 0;
                const total = data.total_steps || 5;
                const pct = Math.max(5, Math.min(100, Math.round((step / total) * 100)));
                pFill.style.width = pct + '%';

                if (data.status === 'COMPLETED') {
                    pFill.style.width = '100%';
                    document.getElementById('rBox').style.display = 'block';
                    document.getElementById('resVideo').src = data.final_video_url;
                    document.getElementById('resDl').href = data.final_video_url;
                    document.getElementById('resVideo').play();

                    // Render Visual Assets
                    const aGrid = document.getElementById('assetGrid');
                    let kfCards = '';
                    if (data.keyframes && data.keyframes.length > 0) {
                        data.keyframes.forEach(kf => {
                            kfCards += `
                                <div class="asset-card">
                                    <img src="${kf.url}" alt="Keyframe ${kf.scene_id}" onerror="this.style.display='none'">
                                    <div class="label" style="color:var(--accent);font-size:12px;font-weight:600;">Keyframe Cảnh ${kf.scene_id}</div>
                                </div>
                            `;
                        });
                    }
                    let assetHtml = `
                        <div class="asset-card">
                            <img src="/job/${jid}/product" alt="Product" onerror="this.style.display='none'">
                            <div class="label" style="font-size:12px;font-weight:600;">Sản Phẩm Gốc</div>
                        </div>
                    `;
                    if (data.flow_mode === 'ugc' || data.flow_mode === 'store_review') {
                        assetHtml += `
                            <div class="asset-card">
                                <img src="/job/${jid}/model" alt="Model" onerror="this.parentElement.style.display='none'">
                                <div class="label" style="font-size:12px;font-weight:600;">Người Mẫu Gốc</div>
                            </div>
                            <div class="asset-card">
                                <img src="/job/${jid}/bg" alt="Background" onerror="this.parentElement.style.display='none'">
                                <div class="label" style="font-size:12px;font-weight:600;">Bối Cảnh AI</div>
                            </div>
                        `;
                    } else if (data.flow_mode === 'fashion') {
                        assetHtml += `
                            <div class="asset-card">
                                <img src="/job/${jid}/model" alt="Model" onerror="this.parentElement.style.display='none'">
                                <div class="label" style="font-size:12px;font-weight:600;">Mẫu Lookbook</div>
                            </div>
                            <div class="asset-card">
                                <img src="/job/${jid}/bg" alt="Background" onerror="this.parentElement.style.display='none'">
                                <div class="label" style="font-size:12px;font-weight:600;">Bối Cảnh Studio</div>
                            </div>
                        `;
                    } else if (data.flow_mode === 'demo') {
                        assetHtml += `
                            <div class="asset-card">
                                <img src="/job/${jid}/model" alt="Model" onerror="this.parentElement.style.display='none'">
                                <div class="label" style="font-size:12px;font-weight:600;">Người Mẫu (Tùy chọn)</div>
                            </div>
                            <div class="asset-card">
                                <img src="/job/${jid}/bg" alt="Background" onerror="this.parentElement.style.display='none'">
                                <div class="label" style="font-size:12px;font-weight:600;">Bối Cảnh Demo</div>
                            </div>
                        `;
                    }
                    assetHtml += kfCards;
                    aGrid.innerHTML = assetHtml;

                    // Render Script Box
                    const sBox = document.getElementById('scriptBox');
                    let scriptHtml = '';
                    if (data.profile && data.profile.product) {
                        scriptHtml += `<div style="margin-bottom:12px;">
                            <strong style="color:var(--accent);">Sản Phẩm:</strong> ${data.profile.product.brand || ''} - ${data.profile.product.name || ''}<br>
                            <span style="font-size:12px;color:#94a3b8;">${data.profile.product.details || data.profile.product.features || ''}</span>
                            ${data.profile.pain_points ? `<br><span style="font-size:11px;color:#f472b6;">💡 Pain Point: ${data.profile.pain_points}</span>` : ''}
                        </div>`;
                    }

                    let modeBadge = '📦 POV Trên Tay';
                    let badgeBg = '#4c1d95';
                    if (data.flow_mode === 'unboxing') {
                        modeBadge = '🎁 Unboxing Studio (100% SP)';
                        badgeBg = '#166534';
                    } else if (data.flow_mode === 'demo') {
                        modeBadge = '✨ Demo Công Dụng';
                        badgeBg = '#d97706';
                    } else if (data.flow_mode === 'ugc') {
                        modeBadge = '📱 UGC Người Thật';
                        badgeBg = '#7c3aed';
                    } else if (data.flow_mode === 'store_review') {
                        modeBadge = '🏪 Review Cửa Hàng';
                        badgeBg = '#0369a1';
                    } else if (data.flow_mode === 'fashion') {
                        modeBadge = '👗 Thời Trang AI Lookbook';
                        badgeBg = '#9333ea';
                    }

                    scriptHtml += `<div style="margin-bottom:12px;display:flex;gap:8px;flex-wrap:wrap;">
                        <span style="background:${badgeBg};color:#fff;padding:3px 10px;border-radius:6px;font-size:12px;font-weight:600;">${modeBadge}</span>
                        <span style="background:#1e3a8a;color:#93c5fd;padding:3px 10px;border-radius:6px;font-size:12px;font-weight:600;">🎙️ ${data.voice_label || 'Giọng AI'}</span>
                        <span style="background:#064e3b;color:#6ee7b7;padding:3px 10px;border-radius:6px;font-size:12px;font-weight:600;">⏱️ ${data.scene_duration || 8}s/cảnh</span>
                        <span style="background:#312e81;color:#c7d2fe;padding:3px 10px;border-radius:6px;font-size:12px;font-weight:600;">🎬 ${data.num_scenes || 3} Cảnh</span>
                        <span style="background:#831843;color:#fbcfe8;padding:3px 10px;border-radius:6px;font-size:12px;font-weight:600;">🎵 ${data.bgm_label || 'Acoustic BGM'}</span>
                    </div>`;

                    if (data.scenes && data.scenes.length > 0) {
                        scriptHtml += `<strong style="color:#10b981;">Kịch Bản 4 Trường Dữ Liệu Chi Tiết:</strong>`;
                        data.scenes.forEach((sc, i) => {
                            scriptHtml += `<div style="margin-top:10px;padding:12px;background:#1e293b;border-radius:8px;border:1px solid #334155;">
                                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
                                    <strong style="color:var(--accent);">Phân Cảnh ${i+1} (${data.scene_duration || 8}s):</strong>
                                    <audio controls src="/job/${jid}/tts/${i+1}" style="height:26px;width:160px;"></audio>
                                </div>
                                <div style="color:#f8fafc;font-size:13px;line-height:1.5;"><strong>Thoại:</strong> "${sc.audio_dialogue || ''}"</div>
                                <div style="font-size:11px;color:#cbd5e1;margin-top:6px;"><strong>🎬 Visual Plan:</strong> ${sc.visual_plan || ''}</div>
                                <div style="font-size:11px;color:#94a3b8;margin-top:4px;"><strong>🔒 Keyframe Prompt:</strong> ${sc.image_generation_prompt ? sc.image_generation_prompt.substring(0, 160) + '...' : ''}</div>
                            </div>`;
                        });
                    }
                    sBox.innerHTML = scriptHtml;

                    // Render Scene Studio
                    const sCont = document.getElementById('scenesContainer');
                    if (sCont && (!sCont.dataset.rendered || sCont.dataset.jobId !== jid)) {
                        renderScenes(jid, data);
                        sCont.dataset.rendered = "true";
                        sCont.dataset.jobId = jid;
                    }
                    if (data.regen_status && data.regen_status.status === 'REGENERATING') {
                        if (!isRegenerating) {
                            pollRegenStatus(jid, data.regen_status.scene_id);
                        }
                    }

                } else if (data.status === 'FAILED') {
                    pStatus.style.color = '#ef4444';
                    pStatus.innerText = 'Xử lý tạm gián đoạn';
                    pMsg.innerHTML = `
                        <div style="background:#450a0a;border:1px solid #b91c1c;padding:14px 18px;border-radius:10px;margin-top:10px;color:#fecaca;line-height:1.6;">
                            <strong>⚠️ Trạng thái gián đoạn:</strong> ${data.error || 'Google phát hiện unusual activity hoặc kết nối proxy bị gián đoạn.'}<br>
                            <span style="font-size:12px;color:#e2e8f0;">Các phân cảnh &amp; keyframe đã tạo xong trước đó đều đã được lưu trữ an toàn 100% trên ổ cứng. Sau khi bạn đổi proxy hoặc kiểm tra kết nối FlowKit Gateway, hãy bấm nút dưới đây để tiếp tục render nốt các cảnh còn thiếu:</span>
                            <div style="margin-top:12px;display:flex;gap:12px;align-items:center;flex-wrap:wrap;">
                                <button onclick="resumeJob('${jid}')" id="btnResume" style="background:linear-gradient(135deg, #16a34a, #15803d);color:#fff;border:none;padding:10px 20px;border-radius:8px;font-weight:700;font-size:13px;cursor:pointer;box-shadow:0 4px 12px rgba(22,163,74,0.3);display:inline-flex;align-items:center;gap:6px;">▶️ Tiếp Tục Tạo Các Cảnh Còn Thiếu (Resume)</button>
                                <span id="resumeNotice" style="font-size:12px;color:#94a3b8;">Hệ thống sẽ bỏ qua các cảnh đã xong và chỉ render tiếp cảnh bị thiếu.</span>
                            </div>
                        </div>
                    `;

                    // Render partial visual assets & scenes if available so user can view or regen individual scenes
                    if (data.scenes && data.scenes.length > 0) {
                        document.getElementById('rBox').style.display = 'block';
                        const sCont = document.getElementById('scenesContainer');
                        if (sCont && (!sCont.dataset.rendered || sCont.dataset.jobId !== jid)) {
                            renderScenes(jid, data);
                            sCont.dataset.rendered = "true";
                            sCont.dataset.jobId = jid;
                        }
                    }
                } else {
                    setTimeout(() => pollStatus(jid), 3000);
                }
            } catch (e) {
                setTimeout(() => pollStatus(jid), 4000);
            }
        }

        async function resumeJob(jid) {
            const btn = document.getElementById('btnResume');
            const not = document.getElementById('resumeNotice');
            if (btn) {
                btn.disabled = true;
                btn.innerText = '⏳ Đang khởi động tiếp tục...';
            }
            if (not) not.innerText = 'Đang gọi server tiếp tục render...';
            try {
                const res = await fetch('/api/job/resume', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ job_id: jid })
                });
                const d = await res.json();
                if (res.ok) {
                    pollStatus(jid);
                } else {
                    alert('Lỗi khi tiếp tục: ' + (d.error || 'Vui lòng thử lại'));
                    if (btn) btn.disabled = false;
                }
            } catch (e) {
                alert('Lỗi kết nối server: ' + e);
                if (btn) btn.disabled = false;
            }
        }

        let isRegenerating = false;

        function escapeHtml(str) {
            if (!str) return '';
            return String(str)
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
                .replace(/"/g, "&quot;")
                .replace(/'/g, "&#039;");
        }

        function syncDialogueToMotion(sIdx) {
            const dArea = document.getElementById(`promptDialogue_${sIdx}`);
            const mArea = document.getElementById(`promptMotion_${sIdx}`);
            if (!dArea || !mArea) return;
            const dText = dArea.value.trim();
            let mText = mArea.value;
            if (mText.includes('Say: "')) {
                mText = mText.replace(/Say:\s*".*?"/s, `Say: "${dText}"`);
            } else if (mText.includes('Say: \\"')) {
                mText = mText.replace(/Say:\s*\\".*?\\"/s, `Say: \\"${dText}\\"`);
            } else {
                mText += ` Say: "${dText}"`;
            }
            mArea.value = mText;
        }

        function renderScenes(jid, data) {
            const sCont = document.getElementById('scenesContainer');
            if (!sCont || !data.scenes) return;
            let html = '';
            data.scenes.forEach((sc, i) => {
                const sIdx = i + 1;
                const kfObj = (data.keyframes || []).find(k => k && k.scene_id === sIdx);
                const kfUrl = kfObj ? kfObj.url : `/job/${jid}/kf/${sIdx}`;
                const clipUrl = `/job/${jid}/clip/${sIdx}`;
                const ttsUrl = `/job/${jid}/tts/${sIdx}`;

                html += `
                <div class="scene-editor-card" id="sceneCard_${sIdx}">
                    <div class="scene-header">
                        <div class="scene-title-group">
                            <span class="scene-num-badge">Phân Cảnh ${sIdx}</span>
                            <span class="scene-dur-badge">⏱️ ${data.scene_duration || 8}s</span>
                            <span class="scene-status-badge" id="sceneBadge_${sIdx}">✅ Sẵn sàng</span>
                        </div>
                        <div style="display: flex; align-items: center; gap: 8px;">
                            <span style="font-size: 11px; color: #94a3b8;">Thoại AI:</span>
                            <audio id="sceneAudio_${sIdx}" controls src="${ttsUrl}" style="height: 28px; width: 170px;"></audio>
                        </div>
                    </div>

                    <div class="scene-body">
                        <!-- Left: Media Previews -->
                        <div class="scene-media-col">
                            <div class="scene-media-box">
                                <label>🎬 Video Clip Cảnh ${sIdx}:</label>
                                <video id="sceneVideo_${sIdx}" src="${clipUrl}" controls playsinline preload="metadata"></video>
                            </div>
                            <div class="scene-media-box">
                                <label>🖼️ Keyframe Khởi Đầu (Banana Pro 2):</label>
                                <img id="sceneKf_${sIdx}" src="${kfUrl}" alt="Keyframe ${sIdx}" onerror="this.style.opacity='0.4'">
                                <button type="button" onclick="regenerateKeyframeOnly('${jid}', ${sIdx})" style="margin-top:8px;width:100%;padding:7px 10px;background:#1e293b;border:1px solid #d97706;color:#fde68a;font-size:12px;font-weight:700;border-radius:6px;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;gap:6px;transition:0.2s;" onmouseover="this.style.background='#78350f'" onmouseout="this.style.background='#1e293b'">
                                    🎨 Vẽ lại riêng ảnh Keyframe này
                                </button>
                            </div>
                        </div>

                        <!-- Right: Editable Prompts -->
                        <div class="scene-prompt-col">
                            <div class="prompt-group">
                                <label>
                                    <span>🔒 1. Prompt Ảnh Keyframe (Nano Banana Pro 2):</span>
                                    <small>Visual chi tiết sản phẩm, góc quay, mặt bàn</small>
                                </label>
                                <textarea id="promptImg_${sIdx}" rows="3">${escapeHtml(sc.image_generation_prompt || '')}</textarea>
                            </div>

                            <div class="prompt-group">
                                <label>
                                    <span>🎬 2. Prompt Chuyển Động Video (Veo 3.1):</span>
                                    <small>Chuyển động máy quay, góc lia & hành động</small>
                                </label>
                                <textarea id="promptMotion_${sIdx}" rows="3">${escapeHtml(sc.video_motion_prompt || '')}</textarea>
                            </div>

                            <div class="prompt-group">
                                <label>
                                    <span>🎙️ 3. Lời Thoại Tiếng Việt (Edge-TTS & Lip-sync):</span>
                                    <button type="button" onclick="syncDialogueToMotion(${sIdx})" style="background:none;border:none;color:#38bdf8;font-size:11px;cursor:pointer;text-decoration:underline;">🔄 Đồng bộ thoại vào Prompt Veo</button>
                                </label>
                                <textarea id="promptDialogue_${sIdx}" rows="2">${escapeHtml(sc.audio_dialogue || '')}</textarea>
                            </div>

                            <div class="scene-btn-row" style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
                                <button type="button" class="btn-regen-kf" id="btnRegenKf_${sIdx}" onclick="regenerateKeyframeOnly('${jid}', ${sIdx})" style="background:linear-gradient(135deg, #d97706, #b45309);color:white;border:none;border-radius:8px;padding:10px 16px;font-size:13px;font-weight:700;cursor:pointer;display:inline-flex;align-items:center;gap:6px;box-shadow:0 4px 12px rgba(217,119,6,0.3);transition:0.2s;">
                                    <span>🎨 VẼ LẠI RIÊNG KEYFRAME (~10s)</span>
                                </button>
                                <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
                                    <label style="display:flex;align-items:center;gap:6px;font-size:12px;color:#cbd5e1;cursor:pointer;">
                                        <input type="checkbox" id="regenKfCheck_${sIdx}" checked style="cursor:pointer;">
                                        <span>Kèm vẽ lại Keyframe</span>
                                    </label>
                                    <button type="button" class="btn-regen-scene" id="btnRegen_${sIdx}" onclick="regenerateScene('${jid}', ${sIdx})">
                                        <span>⚡ TÁI TẠO PHÂN CẢNH ${sIdx} & GHÉP LẠI TVC</span>
                                    </button>
                                </div>
                            </div>
                            <div class="regen-msg" id="regenMsg_${sIdx}"></div>
                        </div>
                    </div>
                </div>
                `;
            });
            sCont.innerHTML = html;
        }

        async function regenerateKeyframeOnly(jid, sIdx) {
            if (isRegenerating) {
                alert("Một tác vụ tái tạo đang chạy, vui lòng chờ trong giây lát!");
                return;
            }
            const btn = document.getElementById(`btnRegenKf_${sIdx}`);
            const fullBtn = document.getElementById(`btnRegen_${sIdx}`);
            const msgEl = document.getElementById(`regenMsg_${sIdx}`);
            const badgeEl = document.getElementById(`sceneBadge_${sIdx}`);
            const imgPrompt = document.getElementById(`promptImg_${sIdx}`)?.value || '';

            if (btn) {
                btn.disabled = true;
                btn.innerHTML = `<span>⏳ Đang gọi Banana Pro 2...</span>`;
            }
            if (fullBtn) fullBtn.disabled = true;
            isRegenerating = true;
            if (badgeEl) {
                badgeEl.innerText = "⏳ Đang vẽ Keyframe...";
                badgeEl.style.background = "#78350f";
                badgeEl.style.color = "#fef08a";
            }
            if (msgEl) {
                msgEl.innerHTML = `<span style="color:#f59e0b;">⏳ Google Banana Pro 2 đang vẽ lại Keyframe Cảnh ${sIdx} (~10s)...</span>`;
            }

            try {
                const res = await fetch('/api/scene/regen-keyframe', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        job_id: jid,
                        scene_id: sIdx,
                        image_prompt: imgPrompt
                    })
                });
                const resp = await res.json();
                if (!res.ok) {
                    throw new Error(resp.error || "Lỗi server khi vẽ lại keyframe");
                }
                pollRegenStatus(jid, sIdx);
            } catch (err) {
                if (btn) {
                    btn.disabled = false;
                    btn.innerHTML = `<span>🎨 VẼ LẠI RIÊNG KEYFRAME (~10s)</span>`;
                }
                if (fullBtn) fullBtn.disabled = false;
                isRegenerating = false;
                if (badgeEl) {
                    badgeEl.innerText = "❌ Lỗi";
                    badgeEl.style.background = "#7f1d1d";
                    badgeEl.style.color = "#fca5a5";
                }
                if (msgEl) {
                    msgEl.innerHTML = `<span style="color:#ef4444;">❌ Lỗi: ${err.message}</span>`;
                }
            }
        }

        async function regenerateScene(jid, sIdx) {
            if (isRegenerating) {
                alert("Một phân cảnh đang được tái tạo, vui lòng chờ trong giây lát!");
                return;
            }
            const btn = document.getElementById(`btnRegen_${sIdx}`);
            const kfBtn = document.getElementById(`btnRegenKf_${sIdx}`);
            const msgEl = document.getElementById(`regenMsg_${sIdx}`);
            const badgeEl = document.getElementById(`sceneBadge_${sIdx}`);
            const kfCheck = document.getElementById(`regenKfCheck_${sIdx}`);
            
            const imgPrompt = document.getElementById(`promptImg_${sIdx}`)?.value || '';
            const motionPrompt = document.getElementById(`promptMotion_${sIdx}`)?.value || '';
            const dialogue = document.getElementById(`promptDialogue_${sIdx}`)?.value || '';
            const regenKf = kfCheck ? kfCheck.checked : true;

            btn.disabled = true;
            btn.innerHTML = `<span>⏳ Đang tái tạo...</span>`;
            if (kfBtn) kfBtn.disabled = true;
            isRegenerating = true;
            badgeEl.innerText = "⏳ Đang tái tạo...";
            badgeEl.style.background = "#78350f";
            badgeEl.style.color = "#fef08a";
            msgEl.innerHTML = `<span style="color:#38bdf8;">⏳ Đang gửi yêu cầu tái tạo Cảnh ${sIdx}...</span>`;

            try {
                const res = await fetch('/api/scene/regen', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        job_id: jid,
                        scene_id: sIdx,
                        regen_keyframe: regenKf,
                        image_prompt: imgPrompt,
                        motion_prompt: motionPrompt,
                        dialogue: dialogue
                    })
                });
                const resp = await res.json();
                if (!res.ok) {
                    throw new Error(resp.error || "Lỗi server");
                }
                pollRegenStatus(jid, sIdx);
            } catch (err) {
                btn.disabled = false;
                btn.innerHTML = `<span>⚡ TÁI TẠO PHÂN CẢNH ${sIdx} & GHÉP LẠI TVC</span>`;
                if (kfBtn) kfBtn.disabled = false;
                isRegenerating = false;
                badgeEl.innerText = "❌ Lỗi";
                badgeEl.style.background = "#7f1d1d";
                badgeEl.style.color = "#fca5a5";
                msgEl.innerHTML = `<span style="color:#ef4444;">❌ Lỗi: ${err.message}</span>`;
            }
        }

        async function pollRegenStatus(jid, sIdx) {
            const btn = document.getElementById(`btnRegen_${sIdx}`);
            const kfBtn = document.getElementById(`btnRegenKf_${sIdx}`);
            const msgEl = document.getElementById(`regenMsg_${sIdx}`);
            const badgeEl = document.getElementById(`sceneBadge_${sIdx}`);

            try {
                const res = await fetch(`/api/status/${jid}`);
                const data = await res.json();
                const rStat = data.regen_status;

                if (rStat && rStat.scene_id === sIdx) {
                    if (rStat.status === 'REGENERATING') {
                        isRegenerating = true;
                        if (btn) {
                            btn.disabled = true;
                            btn.innerHTML = `<span>⏳ ${rStat.step || 'Đang render'}...</span>`;
                        }
                        if (kfBtn) {
                            kfBtn.disabled = true;
                            kfBtn.innerHTML = `<span>⏳ ${rStat.step || 'Đang vẽ'}...</span>`;
                        }
                        if (badgeEl) {
                            badgeEl.innerText = rStat.type === 'KEYFRAME_ONLY' ? "⏳ Đang vẽ Keyframe..." : "⏳ Đang tái tạo...";
                            badgeEl.style.background = "#78350f";
                            badgeEl.style.color = "#fef08a";
                        }
                        if (msgEl) {
                            msgEl.innerHTML = `<span style="color:#38bdf8;">⏳ ${rStat.message || 'Đang xử lý...'}</span>`;
                        }

                        // Immediate Keyframe Preview during full scene regen as soon as Keyframe step completes!
                        if (rStat.step === 'VIDEO' || rStat.step === 'TTS' || rStat.step === 'CONCAT') {
                            const t = Date.now();
                            const k = document.getElementById(`sceneKf_${sIdx}`);
                            if (k && !k.dataset.refreshedForStep) {
                                k.src = `/job/${jid}/kf/${sIdx}?t=${t}`;
                                k.dataset.refreshedForStep = "true";
                                const aGridImgs = document.querySelectorAll('#assetGrid img');
                                aGridImgs.forEach(img => {
                                    if (img.src && img.src.includes(`/kf/${sIdx}`)) {
                                        img.src = `/job/${jid}/kf/${sIdx}?t=${t}`;
                                    }
                                });
                            }
                        }

                        setTimeout(() => pollRegenStatus(jid, sIdx), 2500);
                        return;
                    } else if (rStat.status === 'COMPLETED') {
                        isRegenerating = false;
                        if (btn) {
                            btn.disabled = false;
                            btn.innerHTML = `<span>⚡ TÁI TẠO PHÂN CẢNH ${sIdx} & GHÉP LẠI TVC</span>`;
                        }
                        if (kfBtn) {
                            kfBtn.disabled = false;
                            kfBtn.innerHTML = `<span>🎨 VẼ LẠI RIÊNG KEYFRAME (~10s)</span>`;
                        }
                        if (badgeEl) {
                            badgeEl.innerText = rStat.type === 'KEYFRAME_ONLY' ? "✅ Keyframe Mới" : "✅ Hoàn tất";
                            badgeEl.style.background = "#064e3b";
                            badgeEl.style.color = "#6ee7b7";
                        }
                        if (msgEl) {
                            msgEl.innerHTML = `<span style="color:#10b981;">🎉 ${rStat.message || 'Thành công!'}</span>`;
                        }

                        // Refresh media with cache buster
                        const t = Date.now();
                        const k = document.getElementById(`sceneKf_${sIdx}`);
                        if (k) { k.src = `/job/${jid}/kf/${sIdx}?t=${t}`; delete k.dataset.refreshedForStep; }

                        // Also refresh anchor thumbnail in top asset grid
                        const aGridImgs = document.querySelectorAll('#assetGrid img');
                        aGridImgs.forEach(img => {
                            if (img.src && img.src.includes(`/kf/${sIdx}`)) {
                                img.src = `/job/${jid}/kf/${sIdx}?t=${t}`;
                            }
                        });

                        if (rStat.type !== 'KEYFRAME_ONLY') {
                            const v = document.getElementById(`sceneVideo_${sIdx}`);
                            if (v) { v.src = `/job/${jid}/clip/${sIdx}?t=${t}`; v.load(); v.play(); }
                            const a = document.getElementById(`sceneAudio_${sIdx}`);
                            if (a) { a.src = `/job/${jid}/tts/${sIdx}?t=${t}`; a.load(); }

                            // Refresh top final TVC player & download link
                            const topV = document.getElementById('resVideo');
                            if (topV) { topV.src = `/job/${jid}/final?t=${t}`; topV.load(); topV.play(); }
                            const topDl = document.getElementById('resDl');
                            if (topDl) { topDl.href = `/job/${jid}/final?t=${t}`; }
                        }
                        return;
                    } else if (rStat.status === 'FAILED') {
                        isRegenerating = false;
                        if (btn) {
                            btn.disabled = false;
                            btn.innerHTML = `<span>⚡ TÁI TẠO PHÂN CẢNH ${sIdx} & GHÉP LẠI TVC</span>`;
                        }
                        if (kfBtn) {
                            kfBtn.disabled = false;
                            kfBtn.innerHTML = `<span>🎨 VẼ LẠI RIÊNG KEYFRAME (~10s)</span>`;
                        }
                        if (badgeEl) {
                            badgeEl.innerText = "❌ Lỗi";
                            badgeEl.style.background = "#7f1d1d";
                            badgeEl.style.color = "#fca5a5";
                        }
                        if (msgEl) {
                            msgEl.innerHTML = `<span style="color:#ef4444;">❌ Lỗi: ${rStat.message || rStat.error}</span>`;
                        }
                        return;
                    }
                }
            } catch (e) {
                console.error("Poll regen error:", e);
                setTimeout(() => pollRegenStatus(jid, sIdx), 3000);
            }
        }


        // ── Unusual Activity & Diagnostic Dashboard ──
        function openUnusualModal() {
            document.getElementById('unusualModalOverlay').style.display = 'flex';
            fetchUnusualAudit();
        }

        function closeUnusualModal() {
            document.getElementById('unusualModalOverlay').style.display = 'none';
        }

        async function fetchUnusualAudit() {
            try {
                const resp = await fetch('/api/diagnostics/unusual-audit');
                const data = await resp.json();
                if (!data.ok) return;

                const s = data.summary || {};
                const recent = data.recent_events || [];

                // Update badge
                const badge = document.getElementById('unusualBadge');
                const count24h = s.incidents_last_24h || 0;
                if (badge) {
                    if (count24h > 0) {
                        badge.innerText = count24h;
                        badge.style.display = 'inline-block';
                    } else {
                        badge.style.display = 'none';
                    }
                }

                // Update KPI Cards
                const elTot24 = document.getElementById('statTotal24h');
                if (elTot24) elTot24.innerText = count24h;
                const elTotAll = document.getElementById('statTotalAll');
                if (elTotAll) elTotAll.innerText = `Tổng lịch sử: ${s.total_incidents_recorded || 0} lần`;

                const topProxies = Object.entries(s.breakdown_by_proxy_ip_24h || {});
                const elP = document.getElementById('statTopProxy');
                const elPC = document.getElementById('statTopProxyCount');
                if (topProxies.length > 0) {
                    if (elP) elP.innerText = topProxies[0][0];
                    if (elPC) elPC.innerText = `${topProxies[0][1]} lần bị chặn`;
                } else {
                    if (elP) elP.innerText = "Không có";
                    if (elPC) elPC.innerText = "-";
                }

                const topRpcs = Object.entries(s.breakdown_by_rpc_24h || {});
                const elR = document.getElementById('statTopRpc');
                const elRC = document.getElementById('statTopRpcCount');
                if (topRpcs.length > 0) {
                    if (elR) elR.innerText = topRpcs[0][0];
                    if (elRC) elRC.innerText = `${topRpcs[0][1]} lần bị cờ`;
                } else {
                    if (elR) elR.innerText = "Không có";
                    if (elRC) elRC.innerText = "-";
                }

                const topCauses = Object.entries(s.breakdown_by_cause_24h || {});
                const elC = document.getElementById('statTopCause');
                const elCD = document.getElementById('statTopCauseDesc');
                if (topCauses.length > 0) {
                    if (elC) elC.innerText = topCauses[0][0];
                    if (elCD) elCD.innerText = `${topCauses[0][1]} lần ghi nhận`;
                } else {
                    if (elC) elC.innerText = "Bình thường";
                    if (elCD) elCD.innerText = "Hệ thống sạch";
                }

                // Render Table
                const tbody = document.getElementById('auditTableBody');
                if (tbody) {
                    if (recent.length === 0) {
                        tbody.innerHTML = `<tr><td colspan="6" style="padding:28px; text-align:center; color:#10b981; font-weight:700;">
                            ✅ Tuyệt vời! Chưa có sự kiện Unusual Activity nào được ghi nhận. Hệ thống đang hoạt động an toàn.
                        </td></tr>`;
                    } else {
                        tbody.innerHTML = recent.map(e => {
                            const diag = e.diagnosis || {};
                            const rec = e.recovery || {};
                            const p = e.proxy || {};
                            const tm = e.timing || {};

                            let recHtml = '';
                            if (rec.rotation_triggered) {
                                if (rec.retry_success) {
                                    recHtml = `<span style="color:#34d399; font-weight:700;">✅ Đổi sang ${rec.new_proxy_ip || 'IP mới'} & retry THÀNH CÔNG</span>`;
                                } else if (rec.retry_attempted) {
                                    recHtml = `<span style="color:#f87171;">⚠️ Đổi sang ${rec.new_proxy_ip || 'IP mới'} nhưng retry vẫn bị chặn</span>`;
                                } else {
                                    recHtml = `<span style="color:#fbbf24;">🔄 Đã xoay proxy sang ${rec.new_proxy_ip || 'IP mới'}</span>`;
                                }
                            } else {
                                recHtml = `<span style="color:#94a3b8;">Không kích hoạt đổi IP</span>`;
                            }

                            return `
                            <tr style="border-bottom:1px solid #334155;">
                                <td style="padding:10px 14px; font-family:monospace; color:#94a3b8;">${e.timestamp_vn || e.timestamp_utc}</td>
                                <td style="padding:10px 14px;">
                                    <strong style="color:#cbd5e1;">${e.worker_id}</strong><br>
                                    <span style="color:#fbbf24; font-family:monospace; font-size:11px;">${p.ip}:${p.port}</span>
                                </td>
                                <td style="padding:10px 14px;">
                                    <span style="background:#0284c7; color:#fff; padding:2px 6px; border-radius:4px; font-size:11px; font-weight:700;">${e.rpc_id}</span><br>
                                    <small style="color:#94a3b8;">${e.action_name}</small>
                                </td>
                                <td style="padding:10px 14px; color:#94a3b8;">
                                    Cách trước: <strong style="color:#fff;">${tm.gap_seconds_since_last_req}s</strong><br>
                                    Burst 10s: <span style="color:${tm.burst_in_last_10s >= 3 ? '#f87171' : '#34d399'}; font-weight:700;">${tm.burst_in_last_10s} reqs</span>
                                </td>
                                <td style="padding:10px 14px; line-height:1.4;">
                                    <strong style="color:#a78bfa;">[${diag.primary_cause || 'CỜ GOOGLE'}]</strong><br>
                                    <span style="color:#e2e8f0; font-size:11px;">${diag.explanation || 'Google trả về Unusual Activity.'}</span>
                                </td>
                                <td style="padding:10px 14px; font-size:11px;">
                                    ${recHtml}
                                </td>
                            </tr>
                            `;
                        }).join('');
                    }
                }

                // Render Proxy Health
                const poolDiv = document.getElementById('proxyPoolContainer');
                if (poolDiv) {
                    const proxies = s.proxy_pool_health || [];
                    if (proxies.length === 0) {
                        poolDiv.innerHTML = `<div style="color:#94a3b8; font-size:12px;">Chưa có dữ liệu thống kê dải proxy pool.</div>`;
                    } else {
                        poolDiv.innerHTML = proxies.map(px => `
                            <div style="background:#1e293b; border:1px solid ${px.consecutive_errors > 0 ? '#ef4444' : '#334155'}; border-radius:8px; padding:10px 14px;">
                                <div style="display:flex; justify-content:space-between; align-items:center;">
                                    <strong style="color:#38bdf8; font-family:monospace;">${px.ip}:${px.port}</strong>
                                    <span style="font-size:10px; padding:2px 6px; border-radius:4px; ${px.consecutive_errors > 0 ? 'background:#7f1d1d; color:#fca5a5;' : 'background:#064e3b; color:#6ee7b7;'}">
                                        ${px.consecutive_errors > 0 ? 'BỊ CỜ ' + px.consecutive_errors + ' LẦN' : 'HOẠT ĐỘNG TỐT'}
                                    </span>
                                </div>
                                <div style="margin-top:6px; font-size:11px; color:#94a3b8; display:flex; justify-content:space-between;">
                                    <span>Tỉ lệ thành công: <strong style="color:#fff;">${px.success_rate}%</strong> (${px.successful_requests}/${px.total_requests})</span>
                                </div>
                            </div>
                        `).join('');
                    }
                }
            } catch (err) {
                console.error("Error fetching unusual audit:", err);
            }
        }

        async function runPreflightCheck() {
            const btn = document.getElementById('btnPreflightModal');
            if (btn) btn.innerText = "⏳ Đang kiểm tra 10 IP...";
            const poolDiv = document.getElementById('proxyPoolContainer');
            if (poolDiv) poolDiv.innerHTML = `<div style="padding:20px; text-align:center; color:#38bdf8; grid-column: 1 / -1;">⏳ Đang gửi 3-tier probe tới Google / Google Labs kiểm tra captcha & blacklist cho toàn bộ dải proxy...</div>`;
            try {
                const resp = await fetch('/api/diagnostics/preflight-proxies');
                const data = await resp.json();
                if (!data.ok) throw new Error(data.error || "Failed");
                
                poolDiv.innerHTML = data.results.map(px => {
                    const isClean = px.status === 'CLEAN';
                    const isBlocked = px.status === 'CAPTCHA_BLOCKED' || px.status === 'HTTP_429';
                    const borderColor = isClean ? '#10b981' : (isBlocked ? '#ef4444' : '#f59e0b');
                    const badgeBg = isClean ? '#064e3b' : (isBlocked ? '#7f1d1d' : '#78350f');
                    const badgeColor = isClean ? '#6ee7b7' : (isBlocked ? '#fca5a5' : '#fcd34d');
                    const badgeText = isClean ? '🟢 SẠCH TRÊN GOOGLE' : (isBlocked ? '🔴 BỊ GOOGLE CHẶN' : '⚠️ ' + px.status);

                    return `
                        <div style="background:#1e293b; border:1px solid ${borderColor}; border-radius:8px; padding:12px 14px;">
                            <div style="display:flex; justify-content:space-between; align-items:center;">
                                <strong style="color:#f8fafc; font-family:monospace; font-size:13px;">${px.masked}</strong>
                                <span style="font-size:10px; padding:3px 8px; border-radius:4px; font-weight:bold; background:${badgeBg}; color:${badgeColor};">
                                    ${badgeText}
                                </span>
                            </div>
                            <div style="margin-top:8px; font-size:11px; color:#94a3b8; display:flex; justify-content:space-between; align-items:center;">
                                <span>Ping Google: <strong style="color:#fff;">${px.latency_ms}ms</strong></span>
                                <span>Google Labs: <strong style="color:${px.labs_accessible ? '#34d399' : '#f87171'}">${px.labs_accessible ? 'Sẵn sàng' : 'Chưa mở'}</strong></span>
                            </div>
                            ${px.error ? `<div style="margin-top:6px; font-size:11px; color:#f87171;">❌ ${px.error}</div>` : ''}
                        </div>
                    `;
                }).join('');
            } catch (err) {
                alert("❌ Lỗi kiểm tra proxy: " + err.message);
            } finally {
                if (btn) btn.innerText = "🩺 Pre-flight Check Google";
            }
        }

        async function rotateProxiesNow() {
            const btnTop = document.getElementById('btnRotateTop');
            const btnModal = document.getElementById('btnRotateModal');
            if (btnTop) btnTop.innerText = "⏳ Đang đổi IP...";
            if (btnModal) btnModal.innerText = "⏳ Đang đổi IP...";
            try {
                const resp = await fetch('/api/diagnostics/rotate-proxies');
                const data = await resp.json();
                alert(`✅ Đã xoay Proxy thành công!\n\n• Nick-a: ${data['nick-a']?.proxy || 'OK'}\n• Nick-b: ${data['Nick-b']?.proxy || 'OK'}`);
                fetchUnusualAudit();
            } catch (err) {
                alert("❌ Lỗi xoay proxy: " + err.message);
            } finally {
                if (btnTop) btnTop.innerText = "🔄 Xoay Proxy Thủ Công";
                if (btnModal) btnModal.innerText = "🔄 Xoay Proxy Ngay";
            }
        }

        // Auto poll badge on page load & every 15s
        document.addEventListener('DOMContentLoaded', () => {
            fetchUnusualAudit();
            setInterval(fetchUnusualAudit, 15000);
        });
    </script>

    <!-- Modal: Unusual Activity & Proxy Diagnostic Dashboard -->
    <div id="unusualModalOverlay" style="display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.8); z-index:9999; backdrop-filter:blur(5px); align-items:center; justify-content:center;">
        <div style="background:#0f172a; border:1px solid #334155; border-radius:16px; width:94%; max-width:1150px; max-height:92vh; display:flex; flex-direction:column; box-shadow:0 25px 60px rgba(0,0,0,0.9); overflow:hidden;">
            <!-- Modal Header -->
            <div style="background:#1e293b; padding:18px 24px; display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid #334155;">
                <div>
                    <h2 style="margin:0; font-size:18px; color:#38bdf8; display:flex; align-items:center; gap:8px;">
                        🛡️ Nhật Ký Hoạt Động Bất Thường (Google Unusual Activity Audit)
                    </h2>
                    <div style="color:#94a3b8; font-size:12px; margin-top:4px;">
                        Ghi nhận chi tiết mọi sự kiện Google trả về PUBLIC_ERROR_UNUSUAL_ACTIVITY &bull; Phân tích nguyên nhân &bull; Theo dõi sức khỏe Proxy
                    </div>
                </div>
                <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
                    <a href="/api/diagnostics/download-audit-log" target="_blank" style="background:#0284c7; color:#fff; text-decoration:none; padding:8px 14px; border-radius:6px; font-size:12px; font-weight:700; display:inline-flex; align-items:center; gap:4px;">
                        📥 Tải Log .jsonl
                    </a>
                    <button onclick="runPreflightCheck()" id="btnPreflightModal" style="background:#1e40af; color:#93c5fd; border:1px solid #3b82f6; padding:8px 14px; border-radius:6px; font-size:12px; font-weight:700; cursor:pointer;">
                        🩺 Pre-flight Check Google
                    </button>
                    <button onclick="rotateProxiesNow()" id="btnRotateModal" style="background:#065f46; color:#34d399; border:1px solid #059669; padding:8px 14px; border-radius:6px; font-size:12px; font-weight:700; cursor:pointer;">
                        🔄 Xoay Proxy Ngay
                    </button>
                    <button onclick="fetchUnusualAudit()" style="background:#334155; color:#f8fafc; border:none; padding:8px 12px; border-radius:6px; font-size:12px; cursor:pointer;">
                        🔄 Làm mới
                    </button>
                    <button onclick="closeUnusualModal()" style="background:transparent; color:#94a3b8; border:none; font-size:22px; cursor:pointer; padding:0 6px;">✖</button>
                </div>
            </div>

            <!-- Modal Content Scrollable -->
            <div style="padding:22px 24px; overflow-y:auto; flex:1; display:flex; flex-direction:column; gap:20px;">
                <!-- 4 Summary KPI Cards -->
                <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(220px, 1fr)); gap:14px;">
                    <div style="background:#1e293b; border:1px solid #334155; border-radius:10px; padding:14px 18px;">
                        <div style="font-size:11px; color:#94a3b8; text-transform:uppercase; font-weight:700;">Tổng sự kiện (24h qua)</div>
                        <div id="statTotal24h" style="font-size:24px; font-weight:800; color:#f87171; margin-top:4px;">0</div>
                        <div id="statTotalAll" style="font-size:11px; color:#64748b; margin-top:2px;">Tổng lịch sử: 0 lần</div>
                    </div>
                    <div style="background:#1e293b; border:1px solid #334155; border-radius:10px; padding:14px 18px;">
                        <div style="font-size:11px; color:#94a3b8; text-transform:uppercase; font-weight:700;">Proxy bị cờ nhiều nhất</div>
                        <div id="statTopProxy" style="font-size:15px; font-weight:800; color:#fbbf24; margin-top:8px; word-break:break-all;">Không có</div>
                        <div id="statTopProxyCount" style="font-size:11px; color:#64748b; margin-top:2px;">-</div>
                    </div>
                    <div style="background:#1e293b; border:1px solid #334155; border-radius:10px; padding:14px 18px;">
                        <div style="font-size:11px; color:#94a3b8; text-transform:uppercase; font-weight:700;">Tác vụ bị ảnh hưởng</div>
                        <div id="statTopRpc" style="font-size:15px; font-weight:800; color:#38bdf8; margin-top:8px;">Không có</div>
                        <div id="statTopRpcCount" style="font-size:11px; color:#64748b; margin-top:2px;">-</div>
                    </div>
                    <div style="background:#1e293b; border:1px solid #334155; border-radius:10px; padding:14px 18px;">
                        <div style="font-size:11px; color:#94a3b8; text-transform:uppercase; font-weight:700;">Nguyên nhân chính</div>
                        <div id="statTopCause" style="font-size:15px; font-weight:800; color:#a78bfa; margin-top:8px;">Bình thường</div>
                        <div id="statTopCauseDesc" style="font-size:11px; color:#64748b; margin-top:2px;">Hệ thống sạch</div>
                    </div>
                </div>

                <!-- Section: Live Event Table -->
                <div>
                    <h3 style="font-size:15px; color:#cbd5e1; margin-bottom:10px; display:flex; align-items:center; gap:8px;">
                        📋 Danh Sách Sự Kiện Chi Tiết (Mới nhất)
                    </h3>
                    <div style="background:#1e293b; border:1px solid #334155; border-radius:10px; overflow-x:auto;">
                        <table style="width:100%; border-collapse:collapse; font-size:12px; text-align:left;">
                            <thead>
                                <tr style="background:#0b0f19; border-bottom:1px solid #334155; color:#94a3b8;">
                                    <th style="padding:10px 14px; min-width:130px;">Thời Gian (VN)</th>
                                    <th style="padding:10px 14px; min-width:120px;">Worker & Proxy</th>
                                    <th style="padding:10px 14px; min-width:140px;">Tác Vụ (RPC)</th>
                                    <th style="padding:10px 14px; min-width:110px;">Nhịp Gửi (Burst)</th>
                                    <th style="padding:10px 14px;">Phân Tích Nguyên Nhân</th>
                                    <th style="padding:10px 14px; min-width:150px;">Xử Lý Tự Động</th>
                                </tr>
                            </thead>
                            <tbody id="auditTableBody">
                                <tr>
                                    <td colspan="6" style="padding:24px; text-align:center; color:#94a3b8;">
                                        Đang tải dữ liệu audit...
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>

                <!-- Section: Proxy Pool Health -->
                <div>
                    <h3 style="font-size:15px; color:#cbd5e1; margin-bottom:10px; display:flex; align-items:center; gap:8px;">
                        🌐 Sức Khỏe Dải Proxy Pool Hiện Tại
                    </h3>
                    <div id="proxyPoolContainer" style="display:grid; grid-template-columns:repeat(auto-fit, minmax(280px, 1fr)); gap:12px;">
                        <!-- Injected by JS -->
                    </div>
                </div>
            </div>
        </div>
    </div>
</body>
</html>
"""

def orphan_op_sweeper():
    """Reap scenes stuck RENDERING_VIDEO whose pipeline thread died.

    A submitted Veo op outlives the thread that created it — once a job file
    has been untouched beyond the poll budget, its poller is gone for good
    and the scene would sit forever. The regen worker already knows how to
    adopt a live op (operation_name) or resubmit cleanly, so each stale
    scene is handed to it.
    """
    stale_after = FLOWKIT_VIDEO_WAIT_SECONDS + 120
    while True:
        time.sleep(120)
        try:
            cutoff = time.time() - stale_after
            for jfile in WORK_DIR.glob("*/job.json"):
                try:
                    job = json.loads(jfile.read_text())
                except Exception:
                    continue
                if job.get("updated_at", 0) >= cutoff:
                    continue
                if (job.get("regen_status") or {}).get("status") == "REGENERATING":
                    continue
                job_id = jfile.parent.name
                for sc in job.get("scenes") or []:
                    if sc.get("status") != "RENDERING_VIDEO":
                        continue
                    sid = sc.get("scene_id")
                    cp = jfile.parent / f"clip_{sid}.mp4"
                    if cp.exists() and cp.stat().st_size > 50000:
                        continue
                    print(f"[SWEEPER] Job {job_id}: scene {sid} stuck RENDERING_VIDEO >{int(stale_after)}s — dispatching regen (adopts op {str(sc.get('operation_name'))[:12]} if still live).")
                    threading.Thread(
                        target=run_scene_regeneration_worker,
                        args=(job_id, sid, "", "", "", False),
                        daemon=True,
                    ).start()
                    break  # one regen per job per sweep; REGENERATING flag serializes
        except Exception as se:
            print(f"[SWEEPER] error: {se}")


if __name__ == "__main__":
    if WORK_DIR.exists():
        for p in WORK_DIR.iterdir():
            if p.is_dir() and (p / "job.json").exists():
                try:
                    loaded_job = json.loads((p / "job.json").read_text())
                    if loaded_job.get("status") not in ["COMPLETED", "FAILED"]:
                        loaded_job["status"] = "FAILED"
                        loaded_job["error"] = "Tiến trình bị gián đoạn do server khởi động lại."
                    JOBS[p.name] = loaded_job
                except Exception:
                    pass
    print(f"Loaded {len(JOBS)} past jobs from {WORK_DIR}")
    bis.load_all_batch_jobs()
    bis.batch_scheduler().recover()
    fls.load_all_lookbook_jobs()

    # Proxy Health Daemon is already managed by FlowKit API Service (port 8100).
    # We do not start a duplicate daemon here to avoid doubling network probes on the proxy pool.

    threading.Thread(target=orphan_op_sweeper, daemon=True).start()

    server = HTTPServer(("0.0.0.0", 8089), AutoTvcHandler)
    print("Auto-TVC Studio running at http://0.0.0.0:8089")
    server.serve_forever()
