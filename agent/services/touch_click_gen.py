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
        
        // Full event sequence for mobile Safari
        const rect = btn.getBoundingClientRect();
        const opts = {
            bubbles: true,
            cancelable: true,
            view: window,
            clientX: rect.left + rect.width / 2,
            clientY: rect.top + rect.height / 2
        };
        
        btn.dispatchEvent(new PointerEvent('pointerdown', { ...opts, pointerType: 'touch', isPrimary: true }));
        btn.dispatchEvent(new TouchEvent('touchstart', {
            bubbles: true, cancelable: true,
            touches: [new Touch({ identifier: 1, target: btn, clientX: opts.clientX, clientY: opts.clientY })]
        }));
        btn.dispatchEvent(new PointerEvent('pointerup', { ...opts, pointerType: 'touch', isPrimary: true }));
        btn.dispatchEvent(new TouchEvent('touchend', {
            bubbles: true, cancelable: true,
            changedTouches: [new Touch({ identifier: 1, target: btn, clientX: opts.clientX, clientY: opts.clientY })]
        }));
        btn.dispatchEvent(new MouseEvent('mousedown', opts));
        btn.dispatchEvent(new MouseEvent('mouseup', opts));
        btn.dispatchEvent(new MouseEvent('click', opts));
        
        // Also simulate form submit if inside form
        const form = btn.closest('form');
        if (form) form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
        
        return {
            clicked: true,
            aria: btn.getAttribute('aria-label'),
            className: btn.className,
            disabled: btn.disabled
        };
    })()
    """
    res = await transport.evaluate(js)
    print("Click result:", json.dumps(res.get("value"), indent=2))

if __name__ == "__main__":
    asyncio.run(main())
