import asyncio
import json
import os
import time
import urllib.request
from agent.services.ios_safari_transport import IOSSafariTransport
import agent.services.flow_batch as fb

async def run_step5():
    transport = IOSSafariTransport(proxy_port=9322)
    await transport.connect()
    project_id = "3763ad76-68fb-4e3e-b056-d6e164243f74"
    
    print("=== TESTING STEP 5c: UI-DRIVE TRANSPORT ===")
    prompt = "A cute little red squirrel eating a walnut in the autumn forest, photorealistic"
    
    # 1. Capture initial project media count
    freq_init = fb.project_media_request(project_id)
    res_init = await transport.batch_rpc({"rpcid": fb.RPC_PROJECT_MEDIA, "freq": freq_init})
    text_init = res_init.get("text", "")
    known_mids = set()
    try:
        p_init = fb.first_payload(text_init, fb.RPC_PROJECT_MEDIA)
        if isinstance(p_init, list) and len(p_init) > 1 and isinstance(p_init[1], list):
            for item in p_init[1]:
                known_ids = known_mids.add(item[0])
    except Exception:
        pass
    print(f"Initial known media count: {len(known_mids)}")
    
    # 2. UI-Drive: ProseMirror range selection + execCommand + pointer/touch click
    js_uidrive = f"""
    (() => {{
        // a) Synthetic touch / focus
        window.dispatchEvent(new PointerEvent('pointermove', {{ clientX: 200, clientY: 300, bubbles: true }}));
        window.dispatchEvent(new TouchEvent('touchmove', {{
            bubbles: true,
            touches: [new Touch({{ identifier: 1, target: document.body, clientX: 200, clientY: 300 }})]
        }}));
        
        // b) Set text in ProseMirror
        const pm = document.querySelector('.ProseMirror');
        if (!pm) return {{ error: 'no prosemirror' }};
        pm.focus();
        const sel = window.getSelection();
        const range = document.createRange();
        range.selectNodeContents(pm);
        sel.removeAllRanges();
        sel.addRange(range);
        
        document.execCommand('insertText', false, {json.dumps(prompt)});
        
        // c) Click button
        const btn = document.querySelector('.generate-icon-button, button[aria-label*="Bắt đầu tạo"], button[aria-label*="Generate"]');
        if (!btn) return {{ error: 'no button' }};
        if (btn.disabled) return {{ error: 'btn is disabled' }};
        
        btn.click();
        return {{ ok: true, text: pm.innerText, clicked: true }};
    }})()
    """
    
    res = await transport.evaluate(js_uidrive)
    print("UI-Drive Execution Result:", json.dumps(res.get("value"), indent=2))
    
    # 3. Poll for the squirrel generation
    t0 = time.time()
    squirrel_mid = None
    image_url = None
    
    for attempt in range(35):
        await asyncio.sleep(2.0)
        freq_p = fb.project_media_request(project_id)
        res_p = await transport.batch_rpc({"rpcid": fb.RPC_PROJECT_MEDIA, "freq": freq_p})
        text_p = res_p.get("text", "")
        
        try:
            payload = fb.first_payload(text_p, fb.RPC_PROJECT_MEDIA)
            if isinstance(payload, list) and len(payload) > 1 and isinstance(payload[1], list):
                for item in payload[1]:
                    if isinstance(item, list) and len(item) > 3:
                        mid = item[0]
                        prompt_info = item[3]
                        p_text = prompt_info[0] if isinstance(prompt_info, list) else ""
                        gen_id = prompt_info[4] if isinstance(prompt_info, list) and len(prompt_info) > 4 else None
                        
                        if mid not in known_mids or "squirrel" in str(p_text).lower() or "walnut" in str(p_text).lower():
                            target = gen_id or mid
                            if not squirrel_mid:
                                squirrel_mid = target
                                print(f"[{time.time()-t0:.1f}s] FOUND SQUIRREL MEDIA RECORD: {target} (prompt: {p_text})")
                            
                            # Check if media has rendered
                            freq_m = fb.media_request(target)
                            res_m = await transport.batch_rpc({"rpcid": fb.RPC_MEDIA, "freq": freq_m})
                            text_m = res_m.get("text", "")
                            if "flow-content.google" in text_m or "storage.googleapis" in text_m:
                                payload_m = fb.first_payload(text_m, fb.RPC_MEDIA)
                                images = fb.read_images(payload_m)
                                for img in images:
                                    if img.url:
                                        image_url = img.url
                                        print(f"[{time.time()-t0:.1f}s] SUCCESS! Download URL: {img.url}")
                                        break
        except Exception as e:
            pass
            
        if image_url:
            break
        print(f"[{time.time()-t0:.1f}s] Rendering in progress...")
        
    if image_url:
        os.makedirs("output", exist_ok=True)
        dest = os.path.abspath("output/iphone_generated_squirrel.jpg")
        req = urllib.request.Request(image_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            with open(dest, "wb") as f:
                f.write(resp.read())
        print(f"=== RESULT: UI-DRIVE PASS! Saved squirrel to {dest} ({os.path.getsize(dest)} bytes) ===")
    else:
        print("=== RESULT: UI-Drive timed out ===")

if __name__ == "__main__":
    asyncio.run(run_step5())
