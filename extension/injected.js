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
const FLOW_DOCUMENT_ID = crypto.randomUUID();
const flowBatchRequests = new Map();

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
    const text = _bodyToText(body);
    const blob = u + ' ' + (text || '');
    if (
      !blob.includes('GN0Bre')
      && !u.includes('StreamChat')
      && !u.includes('CreationAgent')
    ) return;
    if (!text) return;
    let freq = null;
    if (text.includes('f.req=')) freq = new URLSearchParams(text).get('f.req');
    else if (text.trim().startsWith('[')) freq = text;
    if (!freq) return;
    const envelope = JSON.parse(freq);
    const re = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    let sid = null;
    if (Array.isArray(envelope) && envelope[0] === null && typeof envelope[1] === 'string') {
      const inner = JSON.parse(envelope[1]);
      if (typeof inner?.[0] === 'string') sid = inner[0];
    } else {
      let inner = envelope[0][0][1];
      if (typeof inner === 'string') inner = JSON.parse(inner);
      if (typeof inner?.[0] === 'string') sid = inner[0];
    }
    if (typeof sid === 'string' && re.test(sid)) {
      window.__FLOW_CHAT_SESSION__ = sid;
      try { window.postMessage({ type: 'FLOW_CHAT_SESSION', session: sid }, '*'); } catch {}
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

window.__flowRunBatch = async function (rpcid, freqStr, maxText, match, customPath, timeoutMs = 120000) {
  try {
    const wiz = window.WIZ_global_data || {};
    const at = wiz.SNlM0e;
    const sid = wiz.FdrFJe;
    const bl = wiz.cfb2h;
    if (!at) return { error: 'NO_AT_TOKEN' };
    if (typeof freqStr === 'string' && freqStr.includes('__CHAT_SESSION__')) {
      const reExact = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
      const fromBag = (bag, keyed) => {
        try {
          for (let i = 0; i < bag.length; i++) {
            const k = bag.key(i) || '';
            const v = (bag.getItem(k) || '').trim();
            if (keyed && !/session|conversation|chat|thread|agent/i.test(k + v)) continue;
            if (reExact.test(v)) return v;
          }
        } catch {}
        return null;
      };
      const found = window.__FLOW_CHAT_SESSION__
        || fromBag(localStorage, true) || fromBag(sessionStorage, true);
      if (!found) return { error: 'NO_CHAT_SESSION' };
      freqStr = freqStr.split('__CHAT_SESSION__').join(found);
    }
    const isStreamChat = !!(customPath && String(customPath).includes('StreamChat'));
    const hl = isStreamChat ? 'en' : 'en-AU';
    const reqid = Math.floor(Math.random() * 900000) + 100000;
    const base = customPath
      || (`/_/AiSandboxAngularFrontend/data/batchexecute?rpcids=${encodeURIComponent(rpcid)}`);
    const join = String(base).includes('?') ? '&' : '?';
    const qs = isStreamChat
      ? `bl=${encodeURIComponent(bl || '')}&f.sid=${encodeURIComponent(sid || '')}&hl=${hl}&_reqid=${reqid}&rt=c`
      : `f.sid=${encodeURIComponent(sid || '')}&bl=${encodeURIComponent(bl || '')}&hl=${hl}&_reqid=${reqid}&rt=c`;
    const url = `${base}${join}${qs}`;
    const body = new URLSearchParams({ 'f.req': freqStr, at }).toString();
    const cap = typeof maxText === 'number' && maxText > 0 ? maxText : 32000000;
    let status;
    let text;
    if (isStreamChat) {
      const xhrResult = await new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.open('POST', url, true);
        xhr.withCredentials = true;
        xhr.setRequestHeader('content-type', 'application/x-www-form-urlencoded;charset=UTF-8');
        xhr.setRequestHeader('x-same-domain', '1');
        xhr.onload = () => resolve({ status: xhr.status, text: xhr.responseText || '' });
        xhr.onerror = () => reject(new Error('XHR_FAILED'));
        xhr.timeout = timeoutMs;
        xhr.ontimeout = () => reject(new Error('SUBMISSION_OUTCOME_UNKNOWN: XHR_TIMEOUT'));
        xhr.send(body);
      });
      status = xhrResult.status;
      text = xhrResult.text;
    } else {
      const resp = await _originalFetch(url, {
        method: 'POST',
        credentials: 'include',
        headers: {
          'content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
          'x-same-domain': '1',
        },
        body,
        signal: AbortSignal.timeout(timeoutMs),
      });
      status = resp.status;
      text = await resp.text();
    }
    if (match) {
      const found = text.indexOf(match);
      const from = found === -1 ? 0 : Math.max(0, found - 700);
      return {
        status,
        matched: found !== -1,
        text: found === -1 ? '' : text.slice(from, found + 800),
      };
    }
    return { status, text: text.slice(0, cap) };
  } catch (e) {
    return { error: e?.message || 'BATCH_PAGE_FAILED' };
  }
};

window.__flowExecuteBatchOnce = function (request) {
  if (request.documentId && request.documentId !== FLOW_DOCUMENT_ID) {
    return Promise.resolve({ error: 'FLOW_DOCUMENT_CHANGED_NOT_SUBMITTED' });
  }
  if (Date.now() >= (request.expiresAt || Infinity)) {
    return Promise.resolve({ error: 'FLOW_DEADLINE_NOT_SUBMITTED' });
  }
  const run = () => window.__flowRunBatch(request.rpcid, request.freq,
    request.maxText, request.match, request.path,
    Math.max(1, Math.min(120000, (request.expiresAt || (Date.now() + 60000)) - Date.now())));
  // Read-only lookups need no replay cache (some listings exceed 17 MB).
  if (!request.documentId) return run();
  const prior = flowBatchRequests.get(request.requestId);
  if (prior) return prior.promise;
  for (const [id, entry] of flowBatchRequests) {
    if (entry.finishedAt && Date.now() - entry.finishedAt > 300000) flowBatchRequests.delete(id);
  }
  if (flowBatchRequests.size >= 256) return Promise.resolve({ error: 'FLOW_BUSY_NOT_SUBMITTED' });
  const entry = { promise: null, finishedAt: 0 };
  entry.promise = Promise.resolve().then(run);
  flowBatchRequests.set(request.requestId, entry);
  entry.promise.then((result) => {
    entry.finishedAt = Date.now();
    if ((result?.text || '').length > 65536) {
      entry.promise = Promise.resolve({ error: 'DUPLICATE_REQUEST_ALREADY_SUBMITTED' });
    }
  }, () => { entry.finishedAt = Date.now(); });
  return entry.promise;
};

function classifyFlowPage() {
  // Kernel-level visibility: WHY the page yields no AT token. WIZ_global_data
  // only exists once the Flow app actually boots; everything else is a wall,
  // a sign-in gate, or a dead load — and each needs a different remedy.
  const wizReady = !!window.WIZ_global_data?.SNlM0e;
  const text = (document.body?.innerText || '').slice(0, 4000);
  const title = (document.title || '').slice(0, 120);
  const hasCaptcha = !!document.querySelector(
    'iframe[src*="recaptcha"], iframe[src*="captcha"], iframe[src*="/sorry/"], #captcha'
  );
  const unusual = hasCaptcha ||
    /unusual traffic|unusual activity|systems have detected|not a robot|verify you.{0,25}human|automated queries/i.test(text);
  const signin = /sign in to (your )?google|sign in to continue|đăng nhập/i.test(text) ||
    !!document.querySelector('a[href*="accounts.google.com/ServiceLogin"], a[href*="accounts.google.com/signin"]');
  const errorPage = /something went wrong|can’t be reached|can't be reached|took too long|err_[a-z_]+/i.test(text);
  let state = 'unknown';
  if (wizReady) state = 'app_ready';
  else if (unusual) state = 'unusual_wall';
  else if (signin) state = 'signed_out';
  else if (errorPage) state = 'error_page';
  else if (document.readyState !== 'complete') state = 'loading';
  return { ready: wizReady, documentId: FLOW_DOCUMENT_ID, page_state: state,
           url: location.href.slice(0, 200), title };
}

window.addEventListener('message', async (event) => {
  if (event.source !== window) return;
  const data = event.data;
  if (!data || !['FLOW_BATCH_RPC', 'FLOW_PAGE_STATUS'].includes(data.type)) return;
  const result = data.type === 'FLOW_PAGE_STATUS'
    ? classifyFlowPage()
    : await window.__flowExecuteBatchOnce(data);
  window.postMessage({
    type: 'FLOW_BATCH_RPC_RESULT',
    requestId: data.requestId,
    result,
  }, '*');
});

window.addEventListener('GET_CAPTCHA', async ({ detail }) => {
  const { requestId, pageAction, recaptchaCode, recaptchaError } = detail;
  try {
    await waitForGrecaptcha(recaptchaCode, recaptchaError);
    const token = await window.grecaptcha.enterprise.execute(SITE_KEY, {
      action: pageAction,
    });
    window.dispatchEvent(new CustomEvent('CAPTCHA_RESULT', {
      detail: { requestId, token, documentId: FLOW_DOCUMENT_ID },
    }));
  } catch (e) {
    window.dispatchEvent(new CustomEvent('CAPTCHA_RESULT', {
      detail: { requestId, error: e.message },
    }));
  }
});

let _recaptchaPolicy = null;
let _recaptchaLoading = null;

function recaptchaPolicy() {
  const tt = window.trustedTypes;
  if (!tt) return null;
  if (_recaptchaPolicy) return _recaptchaPolicy;
  try {
    _recaptchaPolicy = tt.createPolicy('flowkitRecaptcha', {
      createScript: (code) => code,
      createScriptURL: (url) => url,
    });
  } catch {
    _recaptchaPolicy = tt.defaultPolicy;
  }
  return _recaptchaPolicy;
}

function runRecaptchaCode(code) {
  const policy = recaptchaPolicy();
  if (policy && policy.createScript) {
    (0, eval)(policy.createScript(code));
    return;
  }
  (0, eval)(code);
}

function installRecaptchaStub() {
  const cfg = window.___grecaptcha_cfg = window.___grecaptcha_cfg || {};
  const api = window.grecaptcha = window.grecaptcha || {};
  const enterprise = api.enterprise = api.enterprise || {};
  enterprise.ready = enterprise.ready || function (fn) {
    (cfg.fns = cfg.fns || []).push(fn);
  };
  window.__recaptcha_api = 'https://www.google.com/recaptcha/enterprise/';
  (cfg.enterprise = cfg.enterprise || []).push(true);
  (cfg.enterprise2fa = cfg.enterprise2fa || []).push(true);
  (cfg.render = cfg.render || []).push(SITE_KEY);
  (cfg['anchor-ms'] = cfg['anchor-ms'] || []).push(20000);
  (cfg['execute-ms'] = cfg['execute-ms'] || []).push(30000);
  window.__google_recaptcha_client = true;
}

async function ensureGrecaptchaScript(recaptchaCode, recaptchaError) {
  if (window.grecaptcha?.enterprise?.execute) return;
  if (_recaptchaLoading) return _recaptchaLoading;
  _recaptchaLoading = (async () => {
    let code = recaptchaCode;
    if (!code) {
      if (recaptchaError) throw new Error(recaptchaError);
      const loaderUrl = 'https://www.google.com/recaptcha/enterprise.js?render=' + encodeURIComponent(SITE_KEY);
      const loader = await (await fetch(loaderUrl, { credentials: 'omit' })).text();
      const match = loader.match(/https:\/\/www\.gstatic\.com\/recaptcha\/releases\/[^'"\s]+\/recaptcha__\w+\.js/);
      if (!match) throw new Error('recaptcha release url missing');
      code = await (await fetch(match[0], { credentials: 'omit' })).text();
    }
    installRecaptchaStub();
    runRecaptchaCode(code);
  })();
  return _recaptchaLoading;
}

async function waitForGrecaptcha(recaptchaCode, recaptchaError, timeout = 22000) {
  let loadError = null;
  try {
    await ensureGrecaptchaScript(recaptchaCode, recaptchaError);
  } catch (err) {
    loadError = err;
  }
  const start = Date.now();
  while (!window.grecaptcha?.enterprise?.execute) {
    if (Date.now() - start > timeout) {
      const extra = loadError ? `: ${loadError.message}` : '';
      throw new Error('grecaptcha not available' + extra);
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
}
