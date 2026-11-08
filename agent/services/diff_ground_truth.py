import json
import os
import re

import agent.services.flow_batch as fb

def main():
    capture_file = ".omc/research/ground_truth_capture.json"
    data = json.load(open(capture_file, encoding="utf-8"))
    
    gt_item = None
    for it in data:
        if "ogiZ0b" in str(it.get("url", "")):
            gt_item = it
            break
            
    if not gt_item:
        print("No ogiZ0b item found in capture!")
        return

    print("=== 1. URL & PARAMETERS ===")
    print("Ground Truth URL:", gt_item.get("url"))
    
    # Parse URL query params
    from urllib.parse import urlparse, parse_qs
    parsed_url = urlparse(gt_item.get("url"))
    qs = parse_qs(parsed_url.query)
    print("Query params:")
    for k, v in qs.items():
        print(f"  {k}: {v[0]}")
        
    print("\n=== 2. reCAPTCHA ACTION & TOKEN ===")
    # Check recaptcha calls in capture
    for it in data:
        if it.get("kind") == "recaptcha" or it.get("options"):
            print("Captured reCAPTCHA execution:", it.get("options"))
            
    print("\n=== 3. GROUND TRUTH PAYLOAD STRUCTURE ===")
    gt_freq = gt_item.get("freq")
    gt_envelope = json.loads(gt_freq)
    gt_rpc = gt_envelope[0][0]
    gt_inner = json.loads(gt_rpc[1])
    
    def mask_tokens(obj):
        if isinstance(obj, list):
            return [mask_tokens(x) for x in obj]
        elif isinstance(obj, dict):
            return {k: mask_tokens(v) for k, v in obj.items()}
        elif isinstance(obj, str) and len(obj) > 100:
            return f"<TOKEN_LEN_{len(obj)}>"
        return obj

    gt_masked = mask_tokens(gt_inner)
    print("Ground Truth Inner Root structure (masked):")
    print(json.dumps(gt_masked, indent=2))
    
    print("\n=== 4. FLOWKIT FLOW_BATCH BUILDER COMPARISON ===")
    project_id = "3763ad76-68fb-4e3e-b056-d6e164243f74"
    our_freq = fb.image_request("Tạo ảnh con khỉ", project_id, count=2, aspect=fb.ASPECT_LANDSCAPE, model="NARWHAL")
    our_envelope = json.loads(our_freq)
    our_inner = json.loads(our_envelope[0][0][1])
    our_masked = mask_tokens(our_inner)
    
    print("Our Builder Inner Root structure (masked):")
    print(json.dumps(our_masked, indent=2))
    
    print("\n=== 5. KEY DIFFERENCES SUMMARY ===")
    print(f"GT Context:  {gt_masked[3]}")
    print(f"Our Context: {our_masked[3]}")
    
    # Compare Item 0
    gt_it0 = gt_masked[1][0]
    our_it0 = our_masked[1][0]
    print(f"GT Item 0 len: {len(gt_it0)}, Our Item 0 len: {len(our_it0)}")
    for idx in range(max(len(gt_it0), len(our_it0))):
        gt_v = gt_it0[idx] if idx < len(gt_it0) else "<MISSING>"
        our_v = our_it0[idx] if idx < len(our_it0) else "<MISSING>"
        if gt_v != our_v:
            print(f"  Field [{idx}] MISMATCH: GT={gt_v} vs OUR={our_v}")
        else:
            print(f"  Field [{idx}] MATCH: {gt_v}")

if __name__ == "__main__":
    main()
