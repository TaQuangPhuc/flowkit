import asyncio
import json
import os
import time
import urllib.request
from agent.services.ios_safari_transport import IOSSafariTransport
import agent.services.flow_batch as fb

async def run():
    transport = IOSSafariTransport(proxy_port=9322)
    connected = await transport.connect()
    print("Connected to Safari:", connected, flush=True)
    project_id = "3763ad76-68fb-4e3e-b056-d6e164243f74"
    
    prompt = "A majestic colorful rooster with brilliant red comb standing in a sunny barnyard, photorealistic 8k, ultra detailed"
    print(f"Submitting prompt: {prompt}", flush=True)
    
    # Check initial media count
    freq_init = fb.project_media_request(project_id)
    res_init = await transport.batch_rpc({"rpcid": fb.RPC_PROJECT_MEDIA, "freq": freq_init})
    text_init = res_init.get("text", "")
    known_mids = set()
    for match in text_init.split('"'):
        if len(match) == 36 and match.count("-") == 4:
            known_mids.add(match)
    print(f"Initial known media IDs: {len(known_mids)}", flush=True)
    
    # Enter prompt and trigger submission in UI
    js_submit = f"""
    (() => {{
        const div = document.querySelector('div[contenteditable="true"]');
        if (!div) return {{ error: 'no contenteditable div' }};
        div.focus();
        
        // Clear existing
        document.execCommand('selectAll', false, null);
        document.execCommand('insertText', false, {json.dumps(prompt)});
        div.dispatchEvent(new InputEvent('input', {{ bubbles: true, inputType: 'insertText', data: {json.dumps(prompt)} }}));
        div.dispatchEvent(new Event('change', {{ bubbles: true }}));
        
        // Find generate button
        const btn = document.querySelector('.generate-icon-button, button[aria-label*="Bắt đầu tạo"], button[aria-label*="Generate"]');
        if (!btn) return {{ error: 'no generate button' }};
        
        btn.click();
        
        // Also simulate Enter key on the input
        div.dispatchEvent(new KeyboardEvent('keydown', {{ bubbles: true, cancelable: true, key: 'Enter', code: 'Enter', keyCode: 13, which: 13 }}));
        div.dispatchEvent(new KeyboardEvent('keypress', {{ bubbles: true, cancelable: true, key: 'Enter', code: 'Enter', keyCode: 13, which: 13 }}));
        div.dispatchEvent(new KeyboardEvent('keyup', {{ bubbles: true, cancelable: true, key: 'Enter', code: 'Enter', keyCode: 13, which: 13 }}));
        
        return {{ ok: true, text: div.innerText, btnDisabled: btn.disabled }};
    }})()
    """
    
    sub_res = await transport.evaluate(js_submit)
    print("Submit result:", json.dumps(sub_res.get("value"), indent=2), flush=True)
    
    # Poll project media for new rooster item
    t0 = time.time()
    new_mid = None
    image_url = None
    
    for i in range(40):
        await asyncio.sleep(2.0)
        freq_p = fb.project_media_request(project_id)
        res_p = await transport.batch_rpc({"rpcid": fb.RPC_PROJECT_MEDIA, "freq": freq_p})
        text_p = res_p.get("text", "")
        
        # Check for new items in media list
        try:
            payload = fb.first_payload(text_p, fb.RPC_PROJECT_MEDIA)
            if isinstance(payload, list) and len(payload) > 1 and isinstance(payload[1], list):
                for item in payload[1]:
                    if isinstance(item, list) and len(item) > 3:
                        mid = item[0]
                        prompt_info = item[3]
                        p_text = prompt_info[0] if isinstance(prompt_info, list) else ""
                        gen_id = prompt_info[4] if isinstance(prompt_info, list) and len(prompt_info) > 4 else None
                        
                        if mid not in known_mids or "rooster" in str(p_text).lower() or "barnyard" in str(p_text).lower():
                            target = gen_id or mid
                            if not new_mid:
                                new_mid = target
                                print(f"[{time.time()-t0:.1f}s] Found new generation item: {target} (prompt: {p_text})", flush=True)
                            
                            # Check if media is ready
                            freq_m = fb.media_request(target)
                            res_m = await transport.batch_rpc({"rpcid": fb.RPC_MEDIA, "freq": freq_m})
                            text_m = res_m.get("text", "")
                            if "flow-content.google" in text_m or "storage.googleapis" in text_m:
                                payload_m = fb.first_payload(text_m, fb.RPC_MEDIA)
                                images = fb.read_images(payload_m)
                                for img in images:
                                    if img.url:
                                        image_url = img.url
                                        print(f"[{time.time()-t0:.1f}s] Ready! Image URL: {img.url}", flush=True)
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
        print(f"SUCCESS! Saved rooster image to {dest} ({os.path.getsize(dest)} bytes)", flush=True)
    else:
        print("Timeout waiting for rooster image render.", flush=True)

if __name__ == "__main__":
    asyncio.run(run())
