import json
import urllib.request
import time
from pathlib import Path

BASE_URL = "http://127.0.0.1:8100"
MAU_DIR = Path("/home/pc/flowkit/mau")
PRODUCT_MEDIA_ID = "a7842822-7a0f-40e5-91ec-716ffb76f371"
HUMAN_MEDIA_ID = "2eae4860-ccce-4fd4-9944-f5ab8d146799"
PROJECT_ID = "9fd3eefc-cbda-443a-a999-4ff16002a419"

def generate_keyframe(prompt: str, scene_idx: int) -> tuple[str, str]:
    payload = {
        "prompt": prompt,
        "modelDisplayName": "Nano Banana Pro",
        "referenceImageMediaIds": [PRODUCT_MEDIA_ID, HUMAN_MEDIA_ID],
        "aspectRatio": "9:16"
    }
    print(f"\n[Scene {scene_idx}] Generating keyframe with Nano Banana Pro...")
    req = urllib.request.Request(
        f"{BASE_URL}/api/flow/generate-image",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        res = json.loads(resp.read().decode("utf-8"))
        media = (res.get("media") or [])[0]
        mid = media.get("name") or media.get("image", {}).get("generatedImage", {}).get("mediaId")
        fife_url = media.get("image", {}).get("generatedImage", {}).get("fifeUrl") or f"{BASE_URL}/api/flow/image/{mid}"
        print(f"[Scene {scene_idx}] Keyframe created! Media ID: {mid}")
        # Download keyframe
        kf_path = MAU_DIR / f"keyframe_scene_{scene_idx}.jpg"
        urllib.request.urlretrieve(fife_url, str(kf_path))
        print(f"[Scene {scene_idx}] Saved keyframe to {kf_path}")
        return mid, fife_url

def generate_video(keyframe_mid: str, motion_prompt: str, scene_idx: int) -> Path:
    payload = {
        "start_image_media_id": keyframe_mid,
        "prompt": motion_prompt,
        "project_id": PROJECT_ID,
        "scene_id": f"scene_{scene_idx}",
        "aspect_ratio": "VIDEO_ASPECT_RATIO_PORTRAIT",
        "model_family": "veo",
        "duration_s": 8
    }
    print(f"\n[Scene {scene_idx}] Submitting video generation to Veo...")
    req = urllib.request.Request(
        f"{BASE_URL}/api/flow/generate-video",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        res = json.loads(resp.read().decode("utf-8"))
        ops = res.get("operations") or (res.get("data") or {}).get("operations") or []
        op_name = (ops[0].get("operation") or {}).get("name") or ops[0].get("name")
        print(f"[Scene {scene_idx}] Operation ID: {op_name}")

    print(f"[Scene {scene_idx}] Polling Veo render...")
    start_t = time.time()
    for _ in range(120):
        time.sleep(5)
        poll_body = {"operations": [{"operation": {"name": op_name}}]}
        p_req = urllib.request.Request(
            f"{BASE_URL}/api/flow/check-status",
            data=json.dumps(poll_body).encode("utf-8"),
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(p_req, timeout=30) as p_resp:
            p_data = json.loads(p_resp.read().decode("utf-8"))
            curr = (p_data.get("operations") or [{}])[0]
            status = curr.get("status")
            elapsed = int(time.time() - start_t)
            print(f"[Scene {scene_idx}] [{elapsed}s] {status}")
            
            # Extract video URL
            metadata = (curr.get("operation") or {}).get("metadata", {})
            v_meta = metadata.get("video") or {}
            fife = v_meta.get("fifeUrl")
            
            if status == "MEDIA_GENERATION_STATUS_SUCCESSFUL" or fife:
                print(f"[Scene {scene_idx}] Video finished in {elapsed}s! URL: {fife}")
                v_path = MAU_DIR / f"clip_scene_{scene_idx}.mp4"
                urllib.request.urlretrieve(fife, str(v_path))
                print(f"[Scene {scene_idx}] Downloaded clip to {v_path} ({v_path.stat().st_size} bytes)")
                return v_path
            elif "FAIL" in str(status).upper():
                raise RuntimeError(f"[Scene {scene_idx}] Video generation failed: {curr}")
    raise TimeoutError(f"[Scene {scene_idx}] Video timed out after {int(time.time() - start_t)}s")

if __name__ == "__main__":
    # We already have scene 1 clip_scene_1.mp4!
    # Run Scene 2
    prompt_kf_2 = (
        "PRODUCT REFERENCE LOCK — HIGHEST PRIORITY. Copy the reference product EXACTLY. "
        "Keep identical shape, size, proportions, color, material, packaging, label, logo, and texture. "
        "Replicate the second plush toy variant from the product reference: adorable white plush cat emerging from a golden-brown Taiyaki waffle fish pouch with fish scales and tail. "
        "DO NOT redesign, replace, add, remove or modify any product detail. "
        "Keep the reference face and hairstyle unchanged: an attractive young East Asian female with long straight black hair, smiling lovingly. "
        "The model is holding the Taiyaki cat plush toy gently with both hands near her cheek, feeling its soft plush fabric. "
        "Exactly 2 natural hands, 5 fingers each, hands must not cover the plush face. "
        "Photorealistic, clean bright commercial lighting, shallow depth of field, 8k resolution, 9:16 vertical portrait. NO text overlay."
    )
    motion_prompt_2 = (
        "PRODUCT LOCK — HIGHEST PRIORITY. The product must remain EXACTLY identical to the reference image in EVERY FRAME: "
        "same shape, size, proportions, color, material, plush Taiyaki fish texture, and logo. "
        "NEVER redesign, morph, distort or change the product. KEEP PRODUCT ALMOST STATIC. "
        "Subtle camera push-in zoom, gentle breathing, soft eye blinks, affectionate warm smile, natural speaking articulation. "
        'Say: "Chưa hết đâu nha, nhà Bemori còn có cả phiên bản bé mèo bánh cá Taiyaki nướng vàng ruộm này nữa nè! '
        'Vải nhung lông thỏ mềm mướt, ôm bao phê, không hề rụng lông đâu ạ!" in Vietnamese female accent'
    )
    
    kf2_mid, _ = generate_keyframe(prompt_kf_2, 2)
    clip2_path = generate_video(kf2_mid, motion_prompt_2, 2)
    
    # Run Scene 3
    prompt_kf_3 = (
        "PRODUCT REFERENCE LOCK — HIGHEST PRIORITY. Copy the reference product EXACTLY. "
        "Keep identical shape, size, proportions, color, material, packaging, label, logo, and texture. "
        "Replicate the cute Bemori plush toy from the reference: white plush bunny in orange carrot. "
        "Keep the reference face and hairstyle unchanged: an attractive young East Asian female with long straight black hair, wearing yellow and green sporty crop top, smiling enthusiastically at the camera. "
        "The model is holding the plush toy with one hand in front of her chest and gently gesturing toward the camera with a cheerful call-to-action smile. "
        "Exactly 2 natural hands, 5 fingers each. Photorealistic, clean bright studio lighting, vibrant colors, 8k resolution, 9:16 vertical portrait. NO text overlay."
    )
    motion_prompt_3 = (
        "PRODUCT LOCK — HIGHEST PRIORITY. The product must remain EXACTLY identical to the reference image in EVERY FRAME: "
        "same shape, size, proportions, color, material, plush texture. "
        "NEVER redesign, morph, distort or change the product. KEEP PRODUCT ALMOST STATIC. "
        "Subtle camera zoom, enthusiastic cheerful smile, soft eye blinks, natural speaking lip movement, lively body movement. "
        'Say: "Mua tặng bạn thân hay người yêu dịp sinh nhật này là ghi điểm tuyệt đối luôn đó. '
        'Mọi người nhanh tay bấm vào giỏ hàng góc trái rinh ngay một em về ôm ngủ nha!" in Vietnamese female accent'
    )
    kf3_mid, _ = generate_keyframe(prompt_kf_3, 3)
    clip3_path = generate_video(kf3_mid, motion_prompt_3, 3)
    
    print("\nALL 3 SCENES COMPLETED! Ready for FFmpeg concat!")
