import asyncio
import json
from agent.services.ios_safari_transport import IOSSafariTransport

async def main():
    transport = IOSSafariTransport(proxy_port=9322)
    await transport.connect()
    
    js = """
    (() => {
        const btn = document.querySelector('.generate-icon-button, button[aria-label*="Bắt đầu tạo"], button[aria-label*="Generate"]');
        if (!btn) return { error: 'no button' };
        
        // Find Angular event listeners
        const listeners = getEventListeners ? getEventListeners(btn) : null;
        
        // Find parent Angular component
        let cur = btn;
        let foundComp = null;
        while (cur && !foundComp) {
            if (cur.__ngContext__) {
                foundComp = {
                    tag: cur.tagName,
                    class: cur.className,
                    ctxLen: cur.__ngContext__.length
                };
            }
            cur = cur.parentElement;
        }
        
        return {
            btnTag: btn.tagName,
            btnClass: btn.className,
            disabled: btn.disabled,
            listeners: listeners ? Object.keys(listeners) : 'no getEventListeners',
            foundComp
        };
    })()
    """
    res = await transport.evaluate(js)
    print("Button event details:", json.dumps(res.get("value"), indent=2))

if __name__ == "__main__":
    asyncio.run(main())
