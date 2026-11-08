import asyncio
import json
import time
import urllib.request
import os
from agent.services.ios_safari_transport import IOSSafariTransport
import agent.services.flow_batch as fb

async def main():
    transport = IOSSafariTransport(proxy_port=9322)
    await transport.connect()
    
    prompt = "A majestic rooster with bright red and golden feathers on a farm, photorealistic 8k"
    
    # Check Angular form control on prompt box
    js = f"""
    (() => {{
        const div = document.querySelector('div[contenteditable="true"]');
        if (!div) return {{ error: 'no contenteditable' }};
        
        div.focus();
        div.textContent = {json.dumps(prompt)};
        div.dispatchEvent(new Event('input', {{ bubbles: true }}));
        div.dispatchEvent(new Event('change', {{ bubbles: true }}));
        div.dispatchEvent(new KeyboardEvent('keyup', {{ bubbles: true, key: 'a' }}));
        
        // Find generate button
        const btn = document.querySelector('.generate-icon-button, button[aria-label*="Bắt đầu tạo"], button[aria-label*="Generate"]');
        let btnInfo = null;
        if (btn) {{
            btnInfo = {{
                disabled: btn.disabled,
                className: btn.className,
                aria: btn.getAttribute('aria-label')
            }};
            if (!btn.disabled || btn.className.includes('mat-mdc-button-disabled')) {{
                // Remove disabled class or click
                btn.removeAttribute('disabled');
                btn.click();
            }}
        }}
        return {{ divText: div.textContent, btnInfo }};
    }})()
    """
    res = await transport.evaluate(js)
    print("UI Trigger result:", json.dumps(res.get("value"), indent=2))

if __name__ == "__main__":
    asyncio.run(main())
