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
        
        // Walk parents up to 5 levels
        const parents = [];
        let cur = btn;
        for (let i = 0; i < 5 && cur; i++) {
            parents.push({
                level: i,
                tag: cur.tagName,
                className: cur.className,
                attributes: Array.from(cur.attributes || []).map(a => ({ name: a.name, value: a.value })),
                innerHTML_preview: cur.innerHTML.slice(0, 300)
            });
            cur = cur.parentElement;
        }
        
        // Find form or prompt box container
        const promptContainer = document.querySelector('.prompt-box, flow-prompt-box, .prompt-input-container, form');
        
        // Check all interactive elements inside prompt container
        const interactive = [];
        if (promptContainer) {
            promptContainer.querySelectorAll('button, input, select, [contenteditable], mat-select, [role="button"]').forEach(el => {
                interactive.push({
                    tag: el.tagName,
                    class: el.className,
                    disabled: el.disabled,
                    aria: el.getAttribute('aria-label'),
                    text: (el.innerText || el.value || '').trim().slice(0, 50)
                });
            });
        }
        
        return {
            button: {
                tag: btn.tagName,
                class: btn.className,
                disabled: btn.disabled,
                aria: btn.getAttribute('aria-label'),
                outerHTML: btn.outerHTML
            },
            parents,
            promptContainerHTML: promptContainer ? promptContainer.outerHTML.slice(0, 1000) : null,
            interactive
        };
    })()
    """
    
    res = await transport.evaluate(js)
    data = res.get("value") or {}
    print("DOM Snapshot:")
    print(json.dumps(data, indent=2))
    with open(".omc/research/button_dom_snapshot.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    asyncio.run(main())
