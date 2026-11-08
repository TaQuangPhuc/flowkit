import asyncio
import json
import os
import time
from agent.services.ios_safari_transport import IOSSafariTransport

DUMP_DIR = os.path.abspath(".omc/research")
DUMP_FILE = os.path.join(DUMP_DIR, "ground_truth_capture.json")

async def read_cap():
    transport = IOSSafariTransport(proxy_port=9322)
    await transport.connect()
    
    js = """
    (() => {
        return window.__cap || [];
    })()
    """
    res = await transport.evaluate(js)
    return res.get("value") or []

async def poll_for_user_gen():
    os.makedirs(DUMP_DIR, exist_ok=True)
    print("Waiting for user to generate an image on iPhone Safari...", flush=True)
    
    last_count = 0
    t0 = time.time()
    
    while time.time() - t0 < 120:
        await asyncio.sleep(1.0)
        items = await read_cap()
        if len(items) > last_count:
            print(f"[{time.time()-t0:.1f}s] Captured new items! Total count: {len(items)}", flush=True)
            last_count = len(items)
            
            # Write to disk
            with open(DUMP_FILE, "w", encoding="utf-8") as f:
                json.dump(items, f, indent=2, ensure_ascii=False)
            print(f"Dumped {len(items)} items to {DUMP_FILE}", flush=True)
            
            # Check if there is any batchexecute or recaptcha call
            gen_calls = [it for it in items if "ogiZ0b" in str(it.get("url", "")) or "StreamChat" in str(it.get("url", "")) or it.get("kind") == "recaptcha"]
            if gen_calls:
                print(f"FOUND GENERATE / RECAPTCHA CALLS! Count: {len(gen_calls)}", flush=True)
                for c in gen_calls:
                    print(f"  - Kind: {c.get('kind')}, URL: {c.get('url', '')[:80]}, Status: {c.get('status')}")
                    if c.get("options"):
                        print(f"    reCAPTCHA Action: {c.get('options')}")

if __name__ == "__main__":
    asyncio.run(poll_for_user_gen())
