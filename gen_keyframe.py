import json
import urllib.request
import time

BASE_URL = "http://127.0.0.1:8100"

prompt = (
    "PRODUCT REFERENCE LOCK — HIGHEST PRIORITY. Copy the reference product EXACTLY. "
    "Keep identical shape, size, proportions, color, material, packaging, label, logo, text and visible details. "
    "Replicate the cute plush toy from the product reference: adorable white plush bunny emerging from an orange carrot pouch. "
    "DO NOT redesign, replace, add, remove or modify any product detail. "
    "Keep the reference face and hairstyle unchanged: an attractive young East Asian female with long straight black hair, smiling warmly. "
    "The model is holding the plush toy gently with both hands in front of her chest. "
    "Exactly 2 natural hands, 5 fingers each, hands must not cover the product. "
    "Photorealistic, clean bright commercial lighting, sharp details, 8k resolution, 9:16 vertical portrait. NO text overlay."
)

payload = {
    "prompt": prompt,
    "modelDisplayName": "Nano Banana Pro",
    "referenceImageMediaIds": [
        "a7842822-7a0f-40e5-91ec-716ffb76f371", # Product
        "2eae4860-ccce-4fd4-9944-f5ab8d146799"  # Human face & style
    ],
    "aspectRatio": "9:16"
}

print("Submitting generate-image to Nano Banana Pro...")
req = urllib.request.Request(
    f"{BASE_URL}/api/flow/generate-image",
    data=json.dumps(payload).encode("utf-8"),
    headers={"Content-Type": "application/json"}
)

try:
    with urllib.request.urlopen(req, timeout=180) as resp:
        res = json.loads(resp.read().decode("utf-8"))
        print("Response received:")
        print(json.dumps(res, indent=2))
        with open("/home/pc/flowkit/mau/keyframe_result.json", "w") as f:
            json.dump(res, f, indent=2)
except Exception as e:
    print(f"Error: {e}")
    if hasattr(e, "read"):
        print(e.read().decode("utf-8"))
