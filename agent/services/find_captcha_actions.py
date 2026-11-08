import asyncio
import json
from agent.services.ios_safari_transport import IOSSafariTransport

async def main():
    transport = IOSSafariTransport(proxy_port=9322)
    await transport.connect()
    
    # Intercept grecaptcha.enterprise.execute to log every call made by the page
    js = """
    (() => {
        if (!window.__original_grecaptcha_exec && window.grecaptcha?.enterprise?.execute) {
            window.__original_grecaptcha_exec = window.grecaptcha.enterprise.execute;
            window.__grecaptcha_calls = [];
            window.grecaptcha.enterprise.execute = async function(siteKey, options) {
                window.__grecaptcha_calls.push({
                    siteKey,
                    options,
                    time: Date.now(),
                    stack: new Error().stack
                });
                return window.__original_grecaptcha_exec.apply(this, arguments);
            };
        }
        return {
            installed: !!window.__original_grecaptcha_exec,
            past_calls: window.__grecaptcha_calls || []
        };
    })()
    """
    res = await transport.evaluate(js)
    print("reCAPTCHA spy installed:", json.dumps(res.get("value"), indent=2))

if __name__ == "__main__":
    asyncio.run(main())
