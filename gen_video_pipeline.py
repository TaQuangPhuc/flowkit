import json
import urllib.request
import time
from pathlib import Path

BASE_URL = "http://127.0.0.1:8100"
KEYFRAME_MEDIA_ID = "9bce4ae0-80d6-4daa-9455-ec21d30e1a5d"
PROJECT_ID = "9fd3eefc-cbda-443a-a999-4ff16002a419"

prompt = (
    "PRODUCT LOCK — HIGHEST PRIORITY. The product must remain EXACTLY identical to the reference image in EVERY FRAME: "
    "same shape, size, proportions, color, material, plush carrot texture, and logo. "
    "NEVER redesign, morph, distort or change the product. KEEP PRODUCT ALMOST STATIC. "
    "Subtle camera push-in zoom, gentle breathing, soft eye blinks, cheerful warm smile, speaking articulation. "
    'Say: "Trời ơi xem em gấu bông biến hình của nhà Bemori này, kéo khóa ra là em thỏ cà rốt siêu cưng luôn! '
    'Chất nhung mịn mềm ôm cực thích, làm quà tặng người yêu thì chỉ có đổ đứ đừ thôi nha!" in Vietnamese female accent'
)

payload = {
    "start_image_media_id": KEYFRAME_MEDIA_ID,
    "prompt": prompt,
    "project_id": PROJECT_ID,
    "scene_id": "scene_1",
    "aspect_ratio": "VIDEO_ASPECT_RATIO_PORTRAIT",
    "model_family": "veo",
    "duration_s": 8
}

print("1. Submitting video generation to Veo 3.1 Low Priority...")
req = urllib.request.Request(
    f"{BASE_URL}/api/flow/generate-video",
    data=json.dumps(payload).encode("utf-8"),
    headers={"Content-Type": "application/json"}
)

with urllib.request.urlopen(req, timeout=120) as resp:
    res = json.loads(resp.read().decode("utf-8"))
    print("Submit response:")
    print(json.dumps(res, indent=2))

ops = res.get("operations") or (res.get("data") or {}).get("operations") or []
if not ops:
    raise RuntimeError(f"No operations returned: {res}")

op_name = (ops[0].get("operation") or {}).get("name") or ops[0].get("name")
print(f"Operation ID: {op_name}")

print("\n2. Polling operation status...")
video_url = None
media_id = None
start_time = time.time()

for i in range(120): # up to 10-15 minutes
    time.sleep(5)
    poll_body = {"operations": [{"operation": {"name": op_name}}]}
    poll_req = urllib.request.Request(
        f"{BASE_URL}/api/flow/check-status",
        data=json.dumps(poll_body).encode("utf-8"),
        headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(poll_req, timeout=30) as p_resp:
            p_data = json.loads(p_resp.read().decode("utf-8"))
            p_ops = p_data.get("operations") or []
            if not p_ops:
                continue
            curr = p_ops[0]
            status = curr.get("status") or curr.get("state")
            elapsed = int(time.time() - start_time)
            print(f"[{elapsed}s] Poll status: {status}")
            
            # Check for media / video URL
            media = curr.get("media") or (curr.get("operation") or {}).get("media") or {}
            v_meta = media.get("video") or {}
            url = v_meta.get("fifeUrl") or v_meta.get("url")
            
            if status in ("MEDIA_GENERATION_STATUS_SUCCESSFUL", "SUCCESSFUL", "SUCCEEDED") or url:
                video_url = url
                media_id = media.get("name") or media.get("id")
                print(f"Generation SUCCESS in {elapsed}s!")
                print(f"Video URL: {video_url}")
                print(f"Media ID: {media_id}")
                break
            elif "FAIL" in str(status).upper():
                print(f"Generation FAILED: {curr}")
                break
    except Exception as e:
        print(f"Poll warning: {e}")

if video_url:
    print("\n3. Downloading video...")
    out_path = Path("/home/pc/flowkit/mau/clip_scene_1.mp4")
    urllib.request.urlretrieve(video_url, str(out_path))
    print(f"Downloaded video to {out_path} ({out_path.stat().st_size} bytes)")
