/**
 * Content script — bridge between background.js and injected.js
 * Injects injected.js into MAIN world to access window.grecaptcha
 */
(function () {
  const s = document.createElement('script');
  s.src = chrome.runtime.getURL('injected.js');
  s.onload = () => s.remove();
  (document.head || document.documentElement).appendChild(s);
})();

window.addEventListener('message', (event) => {
  if (event.source !== window) return;
  const sid = event.data && event.data.session;
  if (event.data && event.data.type === 'FLOW_CHAT_SESSION' && typeof sid === 'string') {
    chrome.runtime.sendMessage({ type: 'FLOW_CHAT_SESSION', session: sid }).catch(() => {});
  }
});

chrome.runtime.onMessage.addListener((msg, _, reply) => {
  if (msg.type === 'BATCH_RPC' || msg.type === 'FLOW_PAGE_STATUS') {
    const requestId = msg.requestId || `batch-${Date.now()}`;
    const handler = (event) => {
      if (event.source !== window) return;
      const data = event.data;
      if (!data || data.type !== 'FLOW_BATCH_RPC_RESULT' || data.requestId !== requestId) return;
      window.removeEventListener('message', handler);
      clearTimeout(timer);
      reply(data.result || { error: 'NO_BATCH_RESULT' });
    };
    const timer = setTimeout(() => {
      window.removeEventListener('message', handler);
      reply({ error: 'BATCH_TIMEOUT' });
    }, msg.type === 'FLOW_PAGE_STATUS' ? 1000 : Math.max(1000, Math.min(125000, (msg.expiresAt || (Date.now() + 60000)) - Date.now())));
    window.addEventListener('message', handler);
    window.postMessage({
      type: msg.type === 'FLOW_PAGE_STATUS' ? 'FLOW_PAGE_STATUS' : 'FLOW_BATCH_RPC',
      requestId,
      rpcid: msg.rpcid,
      freq: msg.freq,
      maxText: msg.maxText,
      match: msg.match || null,
      path: msg.path || null,
      documentId: msg.documentId || null,
      expiresAt: msg.expiresAt,
    }, '*');
    return true;
  }

  if (msg.type !== 'GET_CAPTCHA') return;

  const { requestId, pageAction, recaptchaCode, recaptchaError } = msg;

  const handler = (e) => {
    if (e.detail?.requestId === requestId) {
      window.removeEventListener('CAPTCHA_RESULT', handler);
      clearTimeout(timer);
      reply({ token: e.detail.token, error: e.detail.error, documentId: e.detail.documentId });
    }
  };

  const timer = setTimeout(() => {
    window.removeEventListener('CAPTCHA_RESULT', handler);
    reply({ error: 'CONTENT_TIMEOUT' });
  }, 25000);

  window.addEventListener('CAPTCHA_RESULT', handler);

  window.dispatchEvent(new CustomEvent('GET_CAPTCHA', {
    detail: { requestId, pageAction, recaptchaCode, recaptchaError },
  }));

  return true; // keep channel open for async reply
});

// ─── TRPC Media URL Monitor ─────────────────────────────────
// Forward intercepted TRPC responses with media URLs to background.js
window.addEventListener('TRPC_MEDIA_URLS', (e) => {
  const { url, body } = e.detail || {};
  if (!body) return;
  chrome.runtime.sendMessage({
    type: 'TRPC_MEDIA_URLS',
    trpcUrl: url,
    body,
  }).catch(() => {});
});
