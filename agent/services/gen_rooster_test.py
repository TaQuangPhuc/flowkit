import asyncio
import json
import os
import time
import urllib.request

from agent.services.ios_safari_transport import IOSSafariTransport
import agent.services.flow_batch as fb


async def generate_rooster():
    transport = IOSSafariTransport(proxy_port=9322)
    connected = await transport.connect()
    print("Safari connected:", connected)
    project_id = "3763ad76-68fb-4e3e-b056-d6e164243f74"

    prompt = "A beautiful majestic colorful rooster standing proudly in a sunny farm yard, sharp focus, 8k resolution, cinematic lighting"
    print(f"Setting prompt on Safari tab: {prompt}")

    # 1. First record known media IDs
    seen_media_ids = set()
    freq_init = fb.project_media_request(project_id)
    res_init = await transport.batch_rpc({"rpcid": fb.RPC_PROJECT_MEDIA, "freq": freq_init})
    text_init = res_init.get("text", "")
    for match in text_init.split('"'):
        if len(match) == 36 and match.count("-") == 4:
            seen_media_ids.add(match)
    print("Initial known media IDs count:", len(seen_media_ids))

    # 2. Type prompt into contenteditable div and trigger generate button
    js_trigger = """
    ((promptText) => {
        const div = document.querySelector('div[contenteditable="true"]');
        if (!div) return { error: 'no contenteditable div' };
        div.focus();
        document.execCommand('selectAll', false, null);
        document.execCommand('insertText', false, promptText);
        div.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertText', data: promptText }));
        
        const btn = document.querySelector('.generate-icon-button, button[aria-label*="Bắt đầu tạo"], button[aria-label*="Generate"]');
        if (!btn) return { error: 'no generate button' };
        btn.click();
        return { clicked: true, text: div.innerText };
    })
    """
    expr = f"({js_trigger})({json.dumps(prompt)})"
    res = await transport.evaluate(expr)
    print("Trigger result:", res.get("value"))

    # 3. Wait and poll project media list until new media item appears and finishes rendering
    print("Waiting for image generation to complete...")
    t0 = time.time()
    new_media_id = None
    image_url = None

    for attempt in range(30):
        await asyncio.sleep(2.0)
        freq_poll = fb.project_media_request(project_id)
        res_poll = await transport.batch_rpc({"rpcid": fb.RPC_PROJECT_MEDIA, "freq": freq_poll})
        text_poll = res_poll.get("text", "")

        # Check for new media ID
        for match in text_poll.split('"'):
            if len(match) == 36 and match.count("-") == 4 and match != project_id and match not in seen_media_ids:
                if not new_media_id:
                    new_media_id = match
                    print(f"Detected new media item: {new_media_id} (elapsed {time.time()-t0:.1f}s)")
                break

        if new_media_id:
            freq_media = fb.media_request(new_media_id)
            res_media = await transport.batch_rpc({"rpcid": fb.RPC_MEDIA, "freq": freq_media})
            text_media = res_media.get("text", "")
            if "flow-content.google" in text_media or "storage.googleapis" in text_media:
                try:
                    payload = fb.first_payload(text_media, fb.RPC_MEDIA)
                    images = fb.read_images(payload)
                    for img in images:
                        if img.url:
                            image_url = img.url
                            print(f"SUCCESS! Got image URL in {time.time()-t0:.1f}s")
                            break
                except Exception as e:
                    print("Parse media error:", e)

        if image_url:
            break
        print(f"Polling generation progress... ({time.time()-t0:.1f}s)")

    if image_url:
        os.makedirs("output", exist_ok=True)
        dest = os.path.abspath("output/iphone_generated_rooster.jpg")
        req = urllib.request.Request(image_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            with open(dest, "wb") as f:
                f.write(resp.read())
        print("DOWNLOAD COMPLETE!")
        print("File path:", dest)
        print("File size:", os.path.getsize(dest), "bytes")
        return dest
    else:
        print("Could not obtain image URL within timeout.")
        return None


if __name__ == "__main__":
    asyncio.run(generate_rooster())
