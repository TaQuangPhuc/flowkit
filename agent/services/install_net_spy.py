import asyncio
import json
from agent.services.ios_safari_transport import IOSSafariTransport

async def main():
    transport = IOSSafariTransport(proxy_port=9322)
    await transport.connect()
    
    js = """
    (() => {
        window.__network_log = window.__network_log || [];
        if (!window.__network_spy_installed) {
            window.__network_spy_installed = true;
            
            const origFetch = window.fetch;
            window.fetch = async function(...args) {
                const url = typeof args[0] === 'string' ? args[0] : args[0]?.url;
                const init = args[1] || {};
                let body = init.body;
                window.__network_log.push({
                    type: 'fetch',
                    url,
                    method: init.method || 'GET',
                    body: typeof body === 'string' ? body.slice(0, 500) : null,
                    time: Date.now()
                });
                return origFetch.apply(this, args);
            };
            
            const origXhrSend = XMLHttpRequest.prototype.send;
            XMLHttpRequest.prototype.send = function(body) {
                window.__network_log.push({
                    type: 'xhr',
                    url: this.__flowChatUrl || this.responseURL,
                    body: typeof body === 'string' ? body.slice(0, 500) : null,
                    time: Date.now()
                });
                return origXhrSend.apply(this, arguments);
            };
        }
        return { installed: true, count: window.__network_log.length };
    })()
    """
    res = await transport.evaluate(js)
    print("Network spy installed:", json.dumps(res.get("value"), indent=2))

if __name__ == "__main__":
    asyncio.run(main())
