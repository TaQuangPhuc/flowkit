import asyncio
import json
import os
import time
import urllib.request
from agent.services.ios_safari_transport import IOSSafariTransport
import agent.services.flow_batch as fb

async def main():
    transport = IOSSafariTransport(proxy_port=9322)
    connected = await transport.connect()
    print("Connected to Safari:", connected, flush=True)
    project_id = "3763ad76-68fb-4e3e-b056-d6e164243f74"
    
    prompt = "A colorful rooster standing proudly in a sunny green farm yard, cinematic 4k, photorealistic"
    print(f"Setting prompt: {prompt}", flush=True)
    
    # 1. Capture current media IDs in project
    freq_init = fb.project_media_request(project_id)
    res_init = await transport.batch_rpc({"rpcid": fb.RPC_PROJECT_MEDIA, "freq": freq_init})
    text_init = res_init.get("text", "")
    known_ids = set()
    try:
        p_init = fb.first_payload(text_init, fb.RPC_PROJECT_MEDIA)
        if isinstance(p_init, list) and len(p_init) > 1 and isinstance(p_init[1], list):
            for item in p_init[1]:
                if isinstance(item, list) and len(item) > 0:
                    known_ids.add(item[0])
    except Exception:
        pass
    print(f"Initial known project media count: {len(known_ids)}", flush=True)
    
    # 2. Insert text via ProseMirror range selection and execCommand, then click generate
    js_submit = f"""
    (() => {{
        const pm = document.querySelector('.ProseMirror');
        if (!pm) return {{ error: 'no prosemirror' }};
        
        pm.focus();
        const sel = window.getSelection();
        const range = document.createRange();
        range.selectNodeContents(pm);
        sel.removeAllRanges();
        sel.addRange(range);
        
        const ok = document.execCommand('insertText', false, {json.dumps(prompt)});
        
        const btn = document.querySelector('.generate-icon-button, button[aria-label*="Bắt đầu tạo"], button[aria-label*="Generate"]');
        if (!btn) return {{ error: 'no button' }};
        if (btn.disabled) return {{ error: 'button disabled after insert' }};
        
        btn.click();
        return {{ ok: true, text: pm.innerText, clicked: true }};
    }})()
    """
    res_sub = await transport.evaluate(js_submit)
    print("Submit result:", json.dumps(res_sub.get("value"), indent=2), flush=True)
    
    # 3. Poll for the new media entry
    t0 = time.time()
    new_media_id = None
    image_url = None
    
    print("Polling project for new rooster generation...", flush=True)
    for attempt in range(45):
        await asyncio.sleep(2.0)
        freq_poll = fb.project_media_request(project_id)
        res_poll = await transport.batch_rpc({"rpcid": fb.RPC_PROJECT_MEDIA, "freq": freq_poll})
        text_poll = res_poll.get("text", "")
        
        try:
            payload = fb.first_payload(text_poll, fb.RPC_PROJECT_MEDIA)
            if isinstance(payload, list) and len(payload) > 1 and isinstance(payload[1], list):
                for item in payload[1]:
                    if isinstance(item, list) and len(item) > 3:
                        mid = item[0]
                        prompt_info = item[3]
                        p_text = prompt_info[0] if isinstance(prompt_info, list) else ""
                        gen_id = prompt_info[4] if isinstance(prompt_info, list) and len(prompt_info) > 4 else None
                        
                        if mid not in known_ids or "rooster" in str(p_text).lower():
                            target = gen_id or mid
                            if not new_media_id:
                                new_media_id = target
                                print(f"[{time.time()-t0:.1f}s] Detected rooster media in progress: {target} (prompt: {p_text})", flush=True)
                            
                            # Check if media is rendered
                            freq_m = fb.media_request(target)
                            res_m = await transport.batch_rpc({"rpcid": fb.RPC_MEDIA, "freq": freq_m})
                            text_m = res_m.get("text", "")
                            if "flow-content.google" in text_m or "storage.googleapis" in text_m:
                                payload_m = fb.first_payload(text_m, fb.RPC_MEDIA)
                                images = fb.read_images(payload_m)
                                for img in images:
                                    if img.url:
                                        image_url = img.url
                                        print(f"[{time.time()-t0:.1f}s] Image ready! URL: {img.url}", flush=True)
                                        break
        except Exception as e:
            pass
            
        if image_url:
            break
        print(f"[{time.time()-t0:.1f}s] Rendering in progress...", flush=True)
        
    if image_url:
        os.makedirs("output", exist_ok=True)
        dest = os.path.abspath("output/iphone_generated_rooster.jpg")
        req = urllib.request.Request(image_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            with open(dest, "wb") as f:
                f.write(resp.read())
        print(f"SUCCESS! Downloaded rooster image to {dest} ({os.path.getsize(dest)} bytes)", flush=True)
    else:
        print("Timeout waiting for image.", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
