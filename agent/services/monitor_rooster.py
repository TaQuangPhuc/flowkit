import asyncio
import json
import os
import time
import urllib.request
from agent.services.ios_safari_transport import IOSSafariTransport
import agent.services.flow_batch as fb

async def main():
    transport = IOSSafariTransport(proxy_port=9322)
    await transport.connect()
    project_id = "3763ad76-68fb-4e3e-b056-d6e164243f74"
    
    print("Monitoring project media for newly generated image...")
    t0 = time.time()
    
    for attempt in range(40):
        await asyncio.sleep(2.0)
        freq = fb.project_media_request(project_id)
        res = await transport.batch_rpc({"rpcid": fb.RPC_PROJECT_MEDIA, "freq": freq})
        text = res.get("text", "")
        
        # Look for media entries
        if "rooster" in text.lower() or "gà" in text.lower() or "majestic" in text.lower() or "feather" in text.lower() or "farm" in text.lower():
            print(f"[{time.time()-t0:.1f}s] Found rooster generation entry in project media!")
            print("Preview:", text[:400])
        
        # Parse all media items in payload
        if "Zzl0ze" in text:
            # Let's extract all media IDs from the text
            # Structure: [["media_id", null, null, ["prompt", ...], "project_id"], ...]
            try:
                payload = fb.first_payload(text, fb.RPC_PROJECT_MEDIA)
                if isinstance(payload, list) and len(payload) > 1 and isinstance(payload[1], list):
                    items = payload[1]
                    for item in items:
                        if isinstance(item, list) and len(item) > 3:
                            mid = item[0]
                            prompt_info = item[3]
                            p_text = prompt_info[0] if isinstance(prompt_info, list) else ""
                            gen_mid = prompt_info[4] if isinstance(prompt_info, list) and len(prompt_info) > 4 else None
                            
                            if "rooster" in str(p_text).lower() or "majestic" in str(p_text).lower() or "farm" in str(p_text).lower():
                                target_mid = gen_mid or mid
                                print(f"[{time.time()-t0:.1f}s] Found matching media item: {target_mid} (prompt: {p_text})")
                                
                                # Fetch media record
                                freq_m = fb.media_request(target_mid)
                                res_m = await transport.batch_rpc({"rpcid": fb.RPC_MEDIA, "freq": freq_m})
                                text_m = res_m.get("text", "")
                                if "flow-content.google" in text_m or "storage.googleapis" in text_m:
                                    payload_m = fb.first_payload(text_m, fb.RPC_MEDIA)
                                    images = fb.read_images(payload_m)
                                    for img in images:
                                        if img.url:
                                            print(f"[{time.time()-t0:.1f}s] SUCCESS! Download URL: {img.url}")
                                            os.makedirs("output", exist_ok=True)
                                            dest = os.path.abspath("output/iphone_generated_rooster.jpg")
                                            req = urllib.request.Request(img.url, headers={"User-Agent": "Mozilla/5.0"})
                                            with urllib.request.urlopen(req, timeout=15) as resp:
                                                with open(dest, "wb") as f:
                                                    f.write(resp.read())
                                            print(f"Saved rooster image to {dest} ({os.path.getsize(dest)} bytes)")
                                            return
            except Exception as e:
                print("Parse error:", e)
        print(f"[{time.time()-t0:.1f}s] Waiting for image generation to finish rendering...")

if __name__ == "__main__":
    asyncio.run(main())
