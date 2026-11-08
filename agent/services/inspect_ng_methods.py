import asyncio
import json
from agent.services.ios_safari_transport import IOSSafariTransport

async def main():
    transport = IOSSafariTransport(proxy_port=9322)
    await transport.connect()
    
    js = """
    (() => {
        const baseBox = document.querySelector('flow-base-prompt-box');
        const genBtnComp = document.querySelector('flow-generate-icon-button');
        const editorComp = document.querySelector('flow-rich-text-editor');
        
        // Find Angular component instances using Angular debug helpers or context
        function inspectEl(el) {
            if (!el) return null;
            const ctx = el.__ngContext__;
            const res = { tag: el.tagName, keys: [] };
            if (Array.isArray(ctx)) {
                for (let x of ctx) {
                    if (x && typeof x === 'object' && x !== el) {
                        res.keys.push({
                            ctor: x.constructor?.name,
                            props: Object.keys(x).slice(0, 15),
                            hasGenerate: typeof x.generate === 'function' || typeof x.submit === 'function' || typeof x.onSubmit === 'function'
                        });
                    }
                }
            }
            return res;
        }
        
        return {
            baseBox: inspectEl(baseBox),
            genBtnComp: inspectEl(genBtnComp),
            editorComp: inspectEl(editorComp)
        };
    })()
    """
    
    res = await transport.evaluate(js)
    print("Angular Component Details:")
    print(json.dumps(res.get("value"), indent=2))

if __name__ == "__main__":
    asyncio.run(main())
