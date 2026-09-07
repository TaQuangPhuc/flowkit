/**
 * Injected into the page's MAIN world on flow.google.com (and an old pinned
 * labs.google tab) — has access to window.grecaptcha.
 *
 * The reCAPTCHA site key survived the September 2026 migration unchanged. The
 * TRPC fetch intercept below did not: it belongs to the labs.google frontend
 * and is inert on flow.google.com, where media urls come back inline on the
 * generate call and from the media rpc.
 *
 * StreamChat (r2v) is scoped to the creation-agent conversation GN0Bre binds
 * on page load. A random uuid there is rejected as PUBLIC_ERROR_UNUSUAL_ACTIVITY,
 * so we stash that session on the window for the extension's batch runner.
 */
const SITE_KEY = '6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV';

function _bodyToText(body) {
  if (body == null) return null;
  if (typeof body === 'string') return body;
  if (body instanceof URLSearchParams) return body.toString();
  if (body instanceof ArrayBuffer) return new TextDecoder().decode(body);
  if (ArrayBuffer.isView(body)) return new TextDecoder().decode(body);
  return null;
}

function _stashChatSession(url, body) {
  try {
    const u = String(url || '');
    if (!u.includes('rpcids=GN0Bre') && !u.includes('rpcids%3DGN0Bre')) return;
    const text = _bodyToText(body);
    if (!text) return;
    let freq = null;
    if (text.includes('f.req=')) freq = new URLSearchParams(text).get('f.req');
    else if (text.trim().startsWith('[')) freq = text;
    if (!freq) return;
    const envelope = JSON.parse(freq);
    let inner = envelope[0][0][1];
    if (typeof inner === 'string') inner = JSON.parse(inner);
    const sid = inner && inner[0];
    if (typeof sid === 'string' && /^[0-9a-f-]{36}$/i.test(sid)) {
      window.__FLOW_CHAT_SESSION__ = sid;
    }
  } catch {}
}

const _originalFetch = window.fetch;
window.fetch = async function (...args) {
  try {
    const input = args[0];
    const init = args[1] || {};
    const url = typeof input === 'string' ? input : input?.url || '';
    let body = init.body;
    if (body == null && typeof Request !== 'undefined' && input instanceof Request) {
      try { body = await input.clone().text(); } catch { body = null; }
    }
    _stashChatSession(url, body);
  } catch {}
  const response = await _originalFetch.apply(this, args);
  try {
    const url = typeof args[0] === 'string' ? args[0] : args[0]?.url || '';
    // Only intercept TRPC calls on labs.google that return project/flow data
    if (url.includes('/fx/api/trpc/') && response.ok) {
      const clone = response.clone();
      clone.text().then(text => {
        if (text.includes('storage.googleapis.com/ai-sandbox-videofx/')) {
          window.dispatchEvent(new CustomEvent('TRPC_MEDIA_URLS', {
            detail: { url, body: text },
          }));
        }
      }).catch(() => {});
    }
  } catch {}
  return response;
};

const _xhrOpen = XMLHttpRequest.prototype.open;
const _xhrSend = XMLHttpRequest.prototype.send;
XMLHttpRequest.prototype.open = function (method, url, ...rest) {
  this.__flowChatUrl = url;
  return _xhrOpen.call(this, method, url, ...rest);
};
XMLHttpRequest.prototype.send = function (body) {
  try { _stashChatSession(String(this.__flowChatUrl || ''), body); } catch {}
  return _xhrSend.call(this, body);
};

window.addEventListener('GET_CAPTCHA', async ({ detail }) => {
  const { requestId, pageAction } = detail;
  try {
    await waitForGrecaptcha();
    const token = await window.grecaptcha.enterprise.execute(SITE_KEY, {
      action: pageAction,
    });
    window.dispatchEvent(new CustomEvent('CAPTCHA_RESULT', {
      detail: { requestId, token },
    }));
  } catch (e) {
    window.dispatchEvent(new CustomEvent('CAPTCHA_RESULT', {
      detail: { requestId, error: e.message },
    }));
  }
});

function waitForGrecaptcha(timeout = 22000) {   // it loads lazily; 10s was optimistic
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const check = () => {
      if (window.grecaptcha?.enterprise?.execute) return resolve();
      if (Date.now() - start > timeout) return reject(new Error('grecaptcha not available'));
      setTimeout(check, 200);
    };
    check();
  });
}
