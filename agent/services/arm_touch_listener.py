import asyncio
import json
import time
from agent.services.ios_safari_transport import IOSSafariTransport

async def main():
    transport = IOSSafariTransport(proxy_port=9322)
    await transport.connect()
    
    # 1. Clear capture log
    await transport.evaluate("(() => { window.__cap = []; })()")
    
    prompt = "A cute little baby penguin wearing a red wool scarf on ice, photorealistic 8k"
    
    # 2. Insert prompt into ProseMirror and arm touch listener
    js_arm = f"""
    (() => {{
        const pm = document.querySelector('.ProseMirror');
        if (!pm) return {{ error: 'no prosemirror' }};
        
        pm.focus();
        const sel = window.getSelection();
        const range = document.createRange();
        range.selectNodeContents(pm);
        sel.removeAllRanges();
        sel.addRange(range);
        document.execCommand('insertText', false, {json.dumps(prompt)});
        
        const btn = document.querySelector('.generate-icon-button, button[aria-label*="Bắt đầu tạo"], button[aria-label*="Generate"]');
        if (!btn) return {{ error: 'no button' }};
        
        window.__touch_triggered = false;
        const onTouch = (e) => {{
            window.__touch_triggered = true;
            setTimeout(() => {{
                btn.click();
            }}, 50);
        }};
        window.addEventListener('touchend', onTouch, {{ once: true, passive: true }});
        
        return {{
            armed: true,
            text: pm.innerText,
            btnDisabled: btn.disabled
        }};
    }})()
    """
    
    res = await transport.evaluate(js_arm)
    print("Touch trigger armed:", json.dumps(res.get("value"), indent=2))

if __name__ == "__main__":
    asyncio.run(main())
