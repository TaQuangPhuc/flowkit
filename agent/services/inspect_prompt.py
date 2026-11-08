import asyncio
import json
from agent.services.ios_safari_transport import IOSSafariTransport

async def main():
    transport = IOSSafariTransport(proxy_port=9322)
    await transport.connect()
    
    js = """
    (() => {
        const el = document.querySelector('div[contenteditable="true"]');
        if (!el) return { error: 'no element' };
        
        // Check surrounding form / parent components
        const parent = el.closest('form, [class*="prompt"], [class*="input"], [class*="box"]');
        
        return {
            tagName: el.tagName,
            className: el.className,
            attributes: Array.from(el.attributes).map(a => ({ name: a.name, value: a.value })),
            parentClass: parent ? parent.className : null,
            parentHTML: parent ? parent.outerHTML.slice(0, 500) : null
        };
    })()
    """
    res = await transport.evaluate(js)
    print("DOM Inspection:", json.dumps(res.get("value"), indent=2))

if __name__ == "__main__":
    asyncio.run(main())
