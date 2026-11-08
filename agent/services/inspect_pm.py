import asyncio
import json
from agent.services.ios_safari_transport import IOSSafariTransport

async def main():
    transport = IOSSafariTransport(proxy_port=9322)
    await transport.connect()
    
    js = """
    (() => {
        const el = document.querySelector('.ProseMirror');
        if (!el) return { error: 'no prosemirror' };
        
        // Find view on element or parent
        let view = null;
        for (let k in el) {
            if (k.startsWith('pmView') || k.includes('View') || el[k]?.dispatch) {
                view = k;
            }
        }
        
        const parent = el.closest('flow-rich-text-editor');
        let parentKeys = [];
        if (parent) {
            for (let k in parent) {
                parentKeys.push(k);
            }
        }
        
        return {
            elKeys: Object.keys(el),
            foundKey: view,
            parentKeys: parentKeys.slice(0, 20)
        };
    })()
    """
    res = await transport.evaluate(js)
    print("ProseMirror Inspection:", json.dumps(res.get("value"), indent=2))

if __name__ == "__main__":
    asyncio.run(main())
