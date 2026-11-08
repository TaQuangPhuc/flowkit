import asyncio
import json
from agent.services.ios_safari_transport import IOSSafariTransport

async def main():
    transport = IOSSafariTransport(proxy_port=9322)
    await transport.connect()
    
    js = """
    (() => {
        const pm = document.querySelector('.ProseMirror');
        if (!pm) return { error: 'no prosemirror' };
        
        pm.focus();
        const sel = window.getSelection();
        const range = document.createRange();
        range.selectNodeContents(pm);
        sel.removeAllRanges();
        sel.addRange(range);
        
        const ok = document.execCommand('insertText', false, 'A colorful rooster on a farm');
        
        const btn = document.querySelector('.generate-icon-button, button[aria-label*="Bắt đầu tạo"], button[aria-label*="Generate"]');
        return {
            execOk: ok,
            innerText: pm.innerText,
            btnFound: !!btn,
            btnDisabled: btn ? btn.disabled : null,
            btnClass: btn ? btn.className : null
        };
    })()
    """
    res = await transport.evaluate(js)
    print("ExecCommand result:", json.dumps(res.get("value"), indent=2))

if __name__ == "__main__":
    asyncio.run(main())
