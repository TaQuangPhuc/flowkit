import asyncio
import json
from agent.services.ios_safari_transport import IOSSafariTransport

async def main():
    transport = IOSSafariTransport(proxy_port=9322)
    await transport.connect()
    
    js = """
    (() => {
        return {
            title: document.title,
            text: document.body ? document.body.innerText.slice(0, 1000) : '',
            snackbars: Array.from(document.querySelectorAll('.mat-mdc-snack-bar-label, [role="alert"], .error, .toast')).map(e => e.innerText)
        };
    })()
    """
    res = await transport.evaluate(js)
    print("Page state:", json.dumps(res.get("value"), indent=2))

if __name__ == "__main__":
    asyncio.run(main())
