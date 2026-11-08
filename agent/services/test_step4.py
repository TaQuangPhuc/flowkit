import asyncio
import json
import time
from agent.services.ios_safari_transport import IOSSafariTransport
import agent.services.flow_batch as fb

async def test_step4():
    transport = IOSSafariTransport(proxy_port=9322)
    await transport.connect()
    project_id = "3763ad76-68fb-4e3e-b056-d6e164243f74"
    
    # 1. Mint token
    token = await transport.mint_recaptcha("IMAGE_GENERATION")
    print(f"Minted token len: {len(token)}")
    
    # 2. Build freq exactly with NARWHAL, aspect 3, count 2
    prompt = "A cute small red robot playing with a ball, high quality 3d render"
    freq = fb.image_request(prompt, project_id, count=2, aspect=fb.ASPECT_LANDSCAPE, model="NARWHAL")
    freq = freq.replace("__CAPTCHA__", token)
    
    # 3. Build custom URL with source-path and matching hl
    custom_path = f"/_/AiSandboxAngularFrontend/data/batchexecute?rpcids=ogiZ0b&source-path=%2Fproject%2F{project_id}"
    
    t0 = time.time()
    res = await transport.batch_rpc({
        "rpcid": "ogiZ0b",
        "freq": freq,
        "customPath": custom_path
    })
    
    print(f"Result in {time.time()-t0:.2f}s:")
    print("Status:", res.get("status"))
    text = res.get("text", "")
    print("Preview:", text[:500])

if __name__ == "__main__":
    asyncio.run(test_step4())
