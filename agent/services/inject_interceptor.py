import asyncio
import json
import time
from agent.services.ios_safari_transport import IOSSafariTransport

INTERCEPTOR_CODE = """
(() => {
    window.__cap = window.__cap || [];
    
    // 1. Hook grecaptcha.enterprise.execute
    if (window.grecaptcha?.enterprise?.execute && !window.__grecaptcha_hooked) {
        window.__grecaptcha_hooked = true;
        const origGrecaptcha = window.grecaptcha.enterprise.execute;
        window.grecaptcha.enterprise.execute = async function(siteKey, options) {
            const t0 = Date.now();
            try {
                const token = await origGrecaptcha.apply(this, arguments);
                window.__cap.push({
                    kind: 'recaptcha',
                    time: t0,
                    duration_ms: Date.now() - t0,
                    siteKey,
                    options,
                    token_len: (token || '').length,
                    token_preview: (token || '').slice(0, 30) + '...' + (token || '').slice(-15),
                    token_full: token
                });
                return token;
            } catch (err) {
                window.__cap.push({
                    kind: 'recaptcha_error',
                    time: t0,
                    siteKey,
                    options,
                    error: String(err)
                });
                throw err;
            }
        };
    }
    
    // 2. Hook window.fetch
    if (!window.__fetch_hooked) {
        window.__fetch_hooked = true;
        const origFetch = window.fetch;
        window.fetch = async function(...args) {
            const t0 = Date.now();
            const input = args[0];
            const init = args[1] || {};
            const url = typeof input === 'string' ? input : input?.url || '';
            let body = init.body;
            
            let req_body_str = null;
            if (typeof body === 'string') req_body_str = body;
            else if (body instanceof URLSearchParams) req_body_str = body.toString();
            
            let freq_param = null;
            let at_param = null;
            if (req_body_str) {
                try {
                    const params = new URLSearchParams(req_body_str);
                    freq_param = params.get('f.req');
                    at_param = params.get('at');
                } catch (e) {}
            }
            
            const promise = origFetch.apply(this, args);
            promise.then(async (resp) => {
                try {
                    const clone = resp.clone();
                    const text = await clone.text();
                    window.__cap.push({
                        kind: 'fetch',
                        time: t0,
                        duration_ms: Date.now() - t0,
                        url,
                        method: init.method || 'GET',
                        headers: init.headers,
                        status: resp.status,
                        body_raw: req_body_str,
                        freq: freq_param,
                        at: at_param,
                        response_preview: text.slice(0, 500),
                        response_full: text
                    });
                } catch (e) {}
            }).catch(err => {
                window.__cap.push({
                    kind: 'fetch_error',
                    time: t0,
                    url,
                    error: String(err)
                });
            });
            return promise;
        };
    }
    
    // 3. Hook XMLHttpRequest
    if (!window.__xhr_hooked) {
        window.__xhr_hooked = true;
        const origOpen = XMLHttpRequest.prototype.open;
        const origSend = XMLHttpRequest.prototype.send;
        
        XMLHttpRequest.prototype.open = function(method, url, ...rest) {
            this.__cap_method = method;
            this.__cap_url = url;
            return origOpen.call(this, method, url, ...rest);
        };
        
        XMLHttpRequest.prototype.send = function(body) {
            const t0 = Date.now();
            const url = this.__cap_url;
            const method = this.__cap_method;
            let body_str = typeof body === 'string' ? body : null;
            
            let freq_param = null;
            let at_param = null;
            if (body_str) {
                try {
                    const params = new URLSearchParams(body_str);
                    freq_param = params.get('f.req');
                    at_param = params.get('at');
                } catch (e) {}
            }
            
            this.addEventListener('load', () => {
                window.__cap.push({
                    kind: 'xhr',
                    time: t0,
                    duration_ms: Date.now() - t0,
                    url,
                    method,
                    status: this.status,
                    body_raw: body_str,
                    freq: freq_param,
                    at: at_param,
                    response_preview: (this.responseText || '').slice(0, 500),
                    response_full: this.responseText
                });
            });
            return origSend.apply(this, arguments);
        };
    }
    
    return {
        ready: true,
        cap_count: window.__cap.length,
        has_grecaptcha: !!window.grecaptcha?.enterprise?.execute,
        fetch_hooked: window.__fetch_hooked,
        xhr_hooked: window.__xhr_hooked
    };
})()
"""

async def inject():
    transport = IOSSafariTransport(proxy_port=9322)
    await transport.connect()
    res = await transport.evaluate(INTERCEPTOR_CODE)
    print("Interceptor Injection Result:", json.dumps(res.get("value"), indent=2))

if __name__ == "__main__":
    asyncio.run(inject())
