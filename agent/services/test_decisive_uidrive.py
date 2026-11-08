import asyncio
import json
import time
from agent.services.ios_safari_transport import IOSSafariTransport

INTERCEPTOR_JS = """
(() => {
    window.__cap = []; // reset capture log
    
    // 1. Hook grecaptcha
    if (window.grecaptcha?.enterprise?.execute && !window.__grecaptcha_hooked) {
        window.__grecaptcha_hooked = true;
        const origG = window.grecaptcha.enterprise.execute;
        window.grecaptcha.enterprise.execute = async function(k, opt) {
            const t0 = Date.now();
            try {
                const tok = await origG.apply(this, arguments);
                window.__cap.push({ kind: 'recaptcha', time: t0, opt, token_len: (tok||'').length, token: tok });
                return tok;
            } catch(e) {
                window.__cap.push({ kind: 'recaptcha_err', time: t0, error: String(e) });
                throw e;
            }
        };
    }
    
    // 2. Hook fetch
    if (!window.__fetch_hooked) {
        window.__fetch_hooked = true;
        const origF = window.fetch;
        window.fetch = async function(...args) {
            const t0 = Date.now();
            const url = typeof args[0] === 'string' ? args[0] : args[0]?.url || '';
            const init = args[1] || {};
            const req_body = typeof init.body === 'string' ? init.body : null;
            const prom = origF.apply(this, args);
            prom.then(async (r) => {
                try {
                    const txt = await r.clone().text();
                    window.__cap.push({
                        kind: 'fetch', time: t0, url, status: r.status, body: req_body,
                        freq: req_body && req_body.includes('f.req=') ? new URLSearchParams(req_body).get('f.req') : null,
                        resp_preview: txt.slice(0, 400), resp_full: txt
                    });
                } catch(e) {}
            }).catch(e => {
                window.__cap.push({ kind: 'fetch_err', time: t0, url, error: String(e) });
            });
            return prom;
        };
    }
    
    // 3. Hook XHR
    if (!window.__xhr_hooked) {
        window.__xhr_hooked = true;
        const origOpen = XMLHttpRequest.prototype.open;
        const origSend = XMLHttpRequest.prototype.send;
        XMLHttpRequest.prototype.open = function(m, u, ...rest) {
            this.__c_m = m;
            this.__c_u = u;
            return origOpen.call(this, m, u, ...rest);
        };
        XMLHttpRequest.prototype.send = function(b) {
            const t0 = Date.now();
            const url = this.__c_u;
            const b_str = typeof b === 'string' ? b : null;
            this.addEventListener('load', () => {
                window.__cap.push({
                    kind: 'xhr', time: t0, url, status: this.status, body: b_str,
                    freq: b_str && b_str.includes('f.req=') ? new URLSearchParams(b_str).get('f.req') : null,
                    resp_preview: (this.responseText || '').slice(0, 400),
                    resp_full: this.responseText
                });
            });
            return origSend.apply(this, arguments);
        };
    }
    
    return { ok: true, cap_len: window.__cap.length };
})()
"""

async def run_decisive_test():
    transport = IOSSafariTransport(proxy_port=9322)
    connected = await transport.connect()
    print("Connected to Safari:", connected)
    
    # 1. Ensure interceptor is active and clean
    init_res = await transport.evaluate(INTERCEPTOR_JS)
    print("Interceptor status:", json.dumps(init_res.get("value"), indent=2))
    
    # 2. UI-Drive: Insert prompt via ProseMirror + click
    prompt = "A cute little baby penguin wearing a red wool scarf on ice, photorealistic 8k"
    print(f"Setting prompt: {prompt}")
    
    js_drive = f"""
    (() => {{
        const pm = document.querySelector('.ProseMirror');
        if (!pm) return {{ error: 'no prosemirror' }};
        
        pm.focus();
        const sel = window.getSelection();
        const range = document.createRange();
        range.selectNodeContents(pm);
        sel.removeAllRanges();
        sel.addRange(range);
        
        const execOk = document.execCommand('insertText', false, {json.dumps(prompt)});
        
        const btn = document.querySelector('.generate-icon-button, button[aria-label*="Bắt đầu tạo"], button[aria-label*="Generate"]');
        if (!btn) return {{ error: 'no button' }};
        
        const before = {{
            disabled: btn.disabled,
            className: btn.className,
            aria: btn.getAttribute('aria-label')
        }};
        
        // Trigger click
        btn.click();
        
        return {{
            execOk,
            text: pm.innerText,
            btnBefore: before,
            btnAfter: {{
                disabled: btn.disabled,
                className: btn.className
            }},
            clicked: true
        }};
    }})()
    """
    
    drive_res = await transport.evaluate(js_drive)
    print("Drive result:", json.dumps(drive_res.get("value"), indent=2))
    
    # 3. Wait and read captured network traffic
    print("Waiting 6s to read all network captures...")
    await asyncio.sleep(6.0)
    
    js_read_cap = "(() => window.__cap || [])()"
    cap_res = await transport.evaluate(js_read_cap)
    items = cap_res.get("value") or []
    
    print(f"\n=== CAPTURED ITEMS ({len(items)}) ===")
    for i, it in enumerate(items):
        kind = it.get("kind")
        url = it.get("url", "")
        status = it.get("status")
        print(f"[{i}] {kind.upper()} | Status: {status} | URL: {url[:100]}")
        if "ogiZ0b" in url:
            print("  >>> ogiZ0b FIRED!")
            print("  Response Preview:", it.get("resp_preview"))
        if "WuwhI" in url:
            print("  >>> WuwhI FIRED!")
            print("  Response Preview:", it.get("resp_preview"))
        if kind == "recaptcha":
            print(f"  >>> reCAPTCHA MINTED! len={it.get('token_len')}")
            
    # Save full dump
    with open(".omc/research/decisive_test_capture.json", "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2, ensure_ascii=False)
    print("\nSaved full capture to .omc/research/decisive_test_capture.json")

if __name__ == "__main__":
    asyncio.run(run_decisive_test())
