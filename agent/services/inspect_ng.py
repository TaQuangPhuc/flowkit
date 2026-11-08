import asyncio
import json
from agent.services.ios_safari_transport import IOSSafariTransport

async def main():
    transport = IOSSafariTransport(proxy_port=9322)
    await transport.connect()
    
    js = """
    (() => {
        const parent = document.querySelector('flow-rich-text-editor');
        const ctx = parent ? parent.__ngContext__ : null;
        if (!ctx) return { error: 'no __ngContext__' };
        
        const items = [];
        for (let i = 0; i < ctx.length; i++) {
            const item = ctx[i];
            if (item && typeof item === 'object') {
                const ctor = item.constructor ? item.constructor.name : typeof item;
                const keys = Object.keys(item).slice(0, 15);
                items.push({ index: i, ctor, keys });
            }
        }
        return { items };
    })()
    """
    res = await transport.evaluate(js)
    print("NgContext items:", json.dumps(res.get("value"), indent=2))

if __name__ == "__main__":
    asyncio.run(main())
