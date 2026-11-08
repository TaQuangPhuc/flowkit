import asyncio
import json
from agent.services.ios_safari_transport import IOSSafariTransport

async def main():
    transport = IOSSafariTransport(proxy_port=9322)
    await transport.connect()
    
    js = """
    (() => {
        const el = document.querySelector('.ProseMirror');
        const desc = el ? el.pmViewDesc : null;
        let view = null;
        if (desc) {
            view = desc.view || desc.pmView || desc.editorView || desc.root;
        }
        
        const parent = el ? el.closest('flow-rich-text-editor') : null;
        const ngCtx = parent ? parent.__ngContext__ : null;
        
        let ngComp = null;
        if (Array.isArray(ngCtx)) {
            ngComp = ngCtx.find(x => x && typeof x === 'object' && (x.prompt || x.value || x.editor || x.editorView || x.submit));
        }
        
        return {
            descKeys: desc ? Object.keys(desc) : null,
            hasView: !!view,
            viewKeys: view ? Object.keys(view) : null,
            ngCompKeys: ngComp ? Object.keys(ngComp) : null
        };
    })()
    """
    res = await transport.evaluate(js)
    print("PM Details:", json.dumps(res.get("value"), indent=2))

if __name__ == "__main__":
    asyncio.run(main())
