import asyncio
import json
import random
import time
import urllib.request
import uuid
import os

from agent.services.ios_safari_transport import IOSSafariTransport
import agent.services.flow_batch as fb

RPC_INTENT = "WuwhI"
RPC_GEN = "ogiZ0b"

def _uuid():
    return str(uuid.uuid4()).upper()

async def run_pipeline():
    transport = IOSSafariTransport(proxy_port=9322)
    await transport.connect()
    project_id = "3763ad76-68fb-4e3e-b056-d6e164243f74"
    
    prompt = "A majestic colorful rooster standing in a sunny barnyard, 8k cinematic photorealistic"
    seed = random.randint(100000000, 999999999)
    client_uuid1 = _uuid()
    client_uuid2 = _uuid()
    intent_uuid = _uuid()
    
    print(f"=== TEST: PRE-FLIGHT WuwhI -> MINT -> ogiZ0b ===")
    print(f"Prompt: {prompt}")
    print(f"Seed: {seed}")
    
    # 1. Step 1: Fire Intent RPC (WuwhI)
    intent_settings = json.dumps({
        "imageModelKey": "NARWHAL",
        "aspectRatio": 3,
        "seed": seed,
        "count": 2,
        "structuredPrompt": {
            "parts": [{"text": prompt}]
        }
    }, separators=(',', ':'))
    
    now_ts = int(time.time())
    now_ns = int((time.time() - now_ts) * 1e9)
    
    experiment_ids = "1706538,106751568,106104244,106259075,106262194,1714242,106430287,106430149,106077941,106299202,106556077,106238955,106225453,106592798,106676866,106577881,105746691,106681111,106714532,106536611,106281924,106486771,119157484,106184493,106210719,106210711,106667487,106747630,106611001,106322568,106432686,106625630,106599983,106585383,105484652,106501717,106615367,106437280,106297879,106751560,106430279,106430141,106299194,106556069,106592790,106676858,106577873,106681103,106714524,106536603,106486763,106210380,106210378,106667479,106747622,106625362,106599975,106585375,106615359"
    
    ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.6.1 Mobile/15E148 Safari/604.1"
    
    inner_intent = [
        [
            [
                "MEDIA_GENERATION",
                [
                    intent_uuid,
                    [now_ts, now_ns],
                    None,
                    [
                        ["IS_MOBILE", ["type.googleapis.com/google.protobuf.BoolValue", [1]]],
                        ["IS_TABLET", ["type.googleapis.com/google.protobuf.BoolValue", []]],
                        ["IS_DESKTOP", ["type.googleapis.com/google.protobuf.BoolValue", []]],
                        ["IS_ANGULAR", ["type.googleapis.com/google.protobuf.BoolValue", [1]]],
                        ["USER_AGENT", ["type.googleapis.com/google.protobuf.StringValue", [ua]]],
                        ["TOOL_NAME", ["type.googleapis.com/google.protobuf.StringValue", ["PINHOLE"]]],
                        ["MEDIA_GENERATION_TYPE", ["type.googleapis.com/google.protobuf.StringValue", ["image"]]],
                        ["MEDIA_GENERATION_SETTINGS", ["type.googleapis.com/google.protobuf.StringValue", [intent_settings]]],
                        ["PROJECT_ID", ["type.googleapis.com/google.protobuf.StringValue", [project_id]]],
                        ["MEDIA_GENERATION_PAYGATE_TIER", ["type.googleapis.com/google.protobuf.StringValue", ["PAYGATE_TIER_TWO"]]]
                    ],
                    experiment_ids
                ]
            ]
        ]
    ]
    
    freq_intent = json.dumps([[[RPC_INTENT, json.dumps(inner_intent, separators=(',', ':'), ensure_ascii=False), None, "generic"]]], separators=(',', ':'), ensure_ascii=False)
    
    t0 = time.time()
    res_intent = await transport.batch_rpc({
        "rpcid": RPC_INTENT,
        "freq": freq_intent,
        "customPath": f"/_/AiSandboxAngularFrontend/data/batchexecute?rpcids={RPC_INTENT}&source-path=%2Fproject%2F{project_id}"
    })
    print(f"Step 1: Intent RPC returned in {time.time()-t0:.2f}s, status={res_intent.get('status')}")
    
    # 2. Step 2: Mint fresh in-page reCAPTCHA Enterprise token
    t0 = time.time()
    token = await transport.mint_recaptcha("IMAGE_GENERATION")
    print(f"Step 2: Minted reCAPTCHA token in {time.time()-t0:.2f}s (len={len(token)})")
    
    # 3. Step 3: Build and fire matching ogiZ0b
    ctx = [None, 22, None, None, None, project_id, None, None, None, None, [token, 1]]
    
    item0 = [
        None, None, None, seed, 3, "NARWHAL", None,
        ctx,
        [[[prompt]]],
        None, None, None,
        client_uuid1, client_uuid2
    ]
    item1 = [
        None, None, None, seed + 1000000, 3, "NARWHAL", None,
        ctx,
        [[[prompt]]],
        None, None, None,
        _uuid(), _uuid()
    ]
    
    inner_gen = [
        None,
        [item0, item1],
        1,
        ctx,
        [_uuid()]
    ]
    
    freq_gen = json.dumps([[[RPC_GEN, json.dumps(inner_gen, separators=(',', ':'), ensure_ascii=False), None, "generic"]]], separators=(',', ':'), ensure_ascii=False)
    
    t0 = time.time()
    res_gen = await transport.batch_rpc({
        "rpcid": RPC_GEN,
        "freq": freq_gen,
        "customPath": f"/_/AiSandboxAngularFrontend/data/batchexecute?rpcids={RPC_GEN}&source-path=%2Fproject%2F{project_id}"
    })
    
    print(f"Step 3: ogiZ0b returned in {time.time()-t0:.2f}s, status={res_gen.get('status')}")
    text_gen = res_gen.get("text", "")
    print(f"ogiZ0b response preview:\n{text_gen[:600]}")
    
    # Parse images if successful
    if "PUBLIC_ERROR" not in text_gen and text_gen:
        try:
            payload = fb.first_payload(text_gen, RPC_GEN)
            images = fb.read_images(payload)
            print(f"SUCCESS! Returned {len(images)} generated images:")
            for img in images:
                print(f"  - Media ID: {img.media_id}, URL: {img.url}")
                if img.url:
                    os.makedirs("output", exist_ok=True)
                    dest = os.path.abspath("output/iphone_pipeline_rooster.jpg")
                    req = urllib.request.Request(img.url, headers={"User-Agent": "Mozilla/5.0"})
                    with urllib.request.urlopen(req, timeout=15) as resp:
                        with open(dest, "wb") as f:
                            f.write(resp.read())
                    print(f"Saved generated rooster to: {dest} ({os.path.getsize(dest)} bytes)")
        except Exception as e:
            print("Parse images error:", e)

if __name__ == "__main__":
    asyncio.run(run_pipeline())
