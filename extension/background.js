/**
 * Flow Kit — Chrome Extension Background Service Worker
 *
 * Connects to local Python agent via WebSocket (agent runs WS server).
 * Mints reCAPTCHA and runs Flow's batchexecute RPCs inside the Flow tab.
 *
 * Flow moved to flow.google.com in September 2026 and stopped minting the
 * `Bearer ya29.…` the old REST host needed. The current path is `batch_rpc`:
 * the agent builds an `f.req` envelope, this worker mints a captcha for it and
 * runs the POST in the page's MAIN world, where the `at` CSRF token lives.
 * The bearer capture and `api_request` proxy below are the legacy path, kept
 * for USE_BATCH_RPC=0 and for an old pinned labs.google tab.
 */

const AGENT_WS_URL = 'ws://127.0.0.1:9222';
// Nick copies rewrite this to "nick-a" so handshake does not wait on profile.json.
const BAKED_PROFILE_ID = null;
// NOTE: This is a browser-restricted public API key — safe to ship in extension bundles.
const API_KEY = 'AIzaSyBtrm0o5ab1c-Ec8ZuLcGt3oJAA5VWt3pY';

// labs.google/fx/tools/flow still resolves but redirects here, so in practice a
// signed-in tab is only ever flow.google.com/*. The legacy patterns stay for an
// old pinned tab. Every tab lookup in this file goes through this list.
const flowUrls = [
  'https://flow.google.com/*',
  'https://labs.google/fx/tools/flow*',
  'https://labs.google/fx/*/tools/flow*',
];
const FLOW_TAB_URL = 'https://flow.google.com/';

let ws = null;
let flowKey = null;
let callbackSecret = null;  // Auth secret for HTTP callback, received from server on WS connect
let state = 'off'; // off | idle | running
let manualDisconnect = false;
// Cached so extension_ready can go out on onopen without awaiting storage.
let profileId = null;
let flowProjectId = null;
let metrics = {
  tokenCapturedAt: null,
  requestCount: 0,   // captcha-consuming requests only (gen image/video/upscale)
  successCount: 0,
  failedCount: 0,
  lastError: null,
};

// ─── URL → Log Type Classifier ─────────────────────────────

// Visible log types — only these appear in the request log
const _VISIBLE_TYPES = new Set(['GEN_IMG', 'GEN_VID', 'GEN_VID_REF', 'UPSCALE', 'TRACKING', 'URL_REFRESH']);

function _classifyApiUrl(url) {
  if (url.includes('uploadImage'))                     return 'UPLOAD';
  if (url.includes('batchGenerateImages'))              return 'GEN_IMG';
  if (url.includes('UpsampleVideo'))                   return 'UPSCALE';
  if (url.includes('ReferenceImages'))                 return 'GEN_VID_REF';
  if (url.includes('batchAsyncGenerateVideo'))          return 'GEN_VID';
  if (url.includes('batchCheckAsync'))                  return 'POLL';
  if (url.includes('upsampleImage'))                   return 'UPS_IMG';
  if (url.includes('/media/'))                         return 'MEDIA';
  if (url.includes('/credits'))                        return 'CREDITS';
  return 'API';
}

// ─── Request Log ────────────────────────────────────────────

let requestLog = [];

function addRequestLog(entry) {
  requestLog.unshift(entry);
  if (requestLog.length > 100) requestLog.pop();
  broadcastRequestLog();
}

function updateRequestLog(id, updates) {
  const entry = requestLog.find((e) => e.id === id);
  if (entry) Object.assign(entry, updates);
  broadcastRequestLog();
}

function broadcastRequestLog() {
  chrome.runtime.sendMessage({ type: 'REQUEST_LOG_UPDATE', log: requestLog }).catch(() => {});
}

// ─── Startup ────────────────────────────────────────────────

let initializationPromise = null;

chrome.runtime.onInstalled.addListener(() => {
  void ensureInitialized();
});
chrome.runtime.onStartup.addListener(() => {
  void ensureInitialized();
});
chrome.alarms.onAlarm.addListener(async (alarm) => {
  await ensureInitialized();
  if (alarm.name === 'reconnect') connectToAgent();
  if (alarm.name === 'keepAlive') keepAlive();
  if (alarm.name === 'token-refresh') {
    await captureTokenFromFlowTab();
  }
});

function ensureInitialized() {
  if (!initializationPromise) {
    initializationPromise = initialize().catch((error) => {
      initializationPromise = null;
      console.error('[FlowAgent] Initialization failed', error);
      throw error;
    });
  }
  return initializationPromise;
}

async function loadBakedProfileId() {
  try {
    const url = chrome.runtime.getURL('profile.json');
    const res = await fetch(url);
    if (!res.ok) return null;
    const baked = await res.json();
    const id = String(baked?.profileId || '').trim();
    return id || null;
  } catch {
    return null;
  }
}

async function initialize() {
  const data = await chrome.storage.local.get(['flowKey', 'metrics', 'callbackSecret', 'profileId', 'flowProjectId']);
  if (data.flowKey) flowKey = data.flowKey;
  if (data.metrics) Object.assign(metrics, data.metrics);
  if (data.callbackSecret) callbackSecret = data.callbackSecret;
  if (BAKED_PROFILE_ID) profileId = BAKED_PROFILE_ID;
  else if (data.profileId) profileId = data.profileId;
  if (data.flowProjectId) flowProjectId = data.flowProjectId;
  if (!profileId) {
    const baked = await loadBakedProfileId();
    if (baked) {
      profileId = baked;
      chrome.storage.local.set({ profileId });
    }
  } else {
    chrome.storage.local.set({ profileId });
  }
  connectToAgent();
  chrome.alarms.create('keepAlive', { periodInMinutes: 0.4 });
}

const FLOW_PROJECT_RE = /flow\.google\.com\/project\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/i;

function flowProjectFromUrl(url) {
  const m = String(url || '').match(FLOW_PROJECT_RE);
  return m ? m[1] : null;
}

function sendProfileUpdate() {
  if (ws?.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify({
    type: 'profile_update',
    profileId,
    flowProjectId,
  }));
}

async function refreshFlowProject() {
  try {
    const tabs = await chrome.tabs.query({ url: flowUrls });
    let found = null;
    for (const tab of tabs) {
      const pid = flowProjectFromUrl(tab.url);
      if (pid) {
        found = pid;
        break;
      }
    }
    if (found && found !== flowProjectId) {
      flowProjectId = found;
      chrome.storage.local.set({ flowProjectId });
      sendProfileUpdate();
    }
  } catch {
    // tabs.query is unavailable in some test fakes
  }
}

// MV3 workers can be suspended and restarted without onStartup firing.
// Rehydrate the persisted Flow key on every worker start.
void ensureInitialized();

// ─── Token Capture ──────────────────────────────────────────

chrome.webRequest.onBeforeSendHeaders.addListener(
  (details) => {
    if (!details?.requestHeaders?.length) return;
    const authHeader = details.requestHeaders.find(
      (h) => h.name?.toLowerCase() === 'authorization',
    );
    const value = authHeader?.value || '';
    if (!value.startsWith('Bearer ya29.')) return;

    const token = value.replace(/^Bearer\s+/i, '').trim();
    if (!token) return;

    // Always update — even if same token string, refresh the timestamp
    flowKey = token;
    metrics.tokenCapturedAt = Date.now();
    chrome.storage.local.set({ flowKey, metrics });
    console.log('[FlowAgent] Bearer token captured');

    // Notify agent
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'token_captured', flowKey }));
    }
  },
  { urls: ['https://aisandbox-pa.googleapis.com/*', 'https://labs.google/*'] },
  ['requestHeaders', 'extraHeaders'],
);

// ─── Flow RPC netlog (r2v session + payload capture) ────────
// Observes the signed-in tab's POSTs under /_/. Polls / project listing /
// media fetches are skipped so the log stays readable. GN0Bre / StreamChat
// session ids are stashed on the page and forwarded to the agent.
const NETLOG_HOSTS = ['https://flow.google.com/_/*'];
const NETLOG_SKIP = /rpcids=(jwpduf|Zzl0ze|as29s)|rpcids%3D(jwpduf|Zzl0ze|as29s)/;
const NETLOG_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const NETLOG_FREQ_MAX = 24000;
const netlogPending = new Map();
let _stashedChatSession = null;

function netlogRpcid(url) {
  const u = String(url || '');
  if (u.includes('StreamChat')) return 'StreamChat';
  if (u.includes('CreationAgent')) return 'CreationAgent';
  const m = u.match(/rpcids=([A-Za-z0-9]+)/) || u.match(/rpcids%3D([A-Za-z0-9]+)/);
  return m ? m[1] : null;
}

function netlogDecodeBody(requestBody) {
  if (!requestBody) return null;
  if (requestBody.formData) {
    const freq = requestBody.formData['f.req'];
    if (Array.isArray(freq) && freq[0]) return String(freq[0]);
    try { return JSON.stringify(requestBody.formData); } catch { return null; }
  }
  const raw = requestBody.raw;
  if (!raw?.length || !raw[0]?.bytes) return null;
  try {
    return new TextDecoder().decode(new Uint8Array(raw[0].bytes));
  } catch {
    return null;
  }
}

function netlogFreq(body) {
  if (!body) return null;
  const text = String(body);
  if (text.includes('f.req=')) {
    try { return new URLSearchParams(text).get('f.req'); } catch { return null; }
  }
  const trimmed = text.trim();
  if (trimmed.startsWith('[')) return trimmed;
  return text.slice(0, NETLOG_FREQ_MAX);
}

function isChatRpc(url, freq) {
  const blob = String(url || '') + ' ' + String(freq || '');
  return /GN0Bre|StreamChat|CreationAgent|FlowCreationAgent/i.test(blob);
}

function firstUuid(text) {
  const m = String(text || '').match(
    /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i,
  );
  return m ? m[0] : null;
}

function extractChatSession(url, freq) {
  if (!freq) return null;
  try {
    const env = JSON.parse(freq);
    if (Array.isArray(env) && env.length >= 2 && env[0] === null && typeof env[1] === 'string') {
      const inner = JSON.parse(env[1]);
      if (typeof inner?.[0] === 'string' && NETLOG_UUID.test(inner[0])) return inner[0];
    }
    const item = env?.[0]?.[0];
    const rpcid = item?.[0];
    let inner = item?.[1];
    if (typeof inner === 'string') inner = JSON.parse(inner);
    const fromUrl = String(url || '').includes('GN0Bre');
    if ((rpcid === 'GN0Bre' || fromUrl) && typeof inner?.[0] === 'string' && NETLOG_UUID.test(inner[0])) {
      return inner[0];
    }
  } catch {}
  if (isChatRpc(url, freq)) return firstUuid(freq);
  return null;
}

function stashChatSession(sid) {
  if (!sid || sid === _stashedChatSession) return;
  _stashedChatSession = sid;
  chrome.tabs.query({ url: flowUrls }).then((tabs) => {
    for (const tab of tabs) {
      if (!tab.id || tab.discarded) continue;
      chrome.scripting.executeScript({
        target: { tabId: tab.id },
        world: 'MAIN',
        args: [sid],
        func: (id) => { globalThis.__FLOW_CHAT_SESSION__ = id; },
      }).catch(() => {});
    }
  }).catch(() => {});
}

function reportChatSession(sid) {
  if (!sid || !NETLOG_UUID.test(sid)) return;
  const fresh = sid !== _stashedChatSession;
  stashChatSession(sid);
  if (!fresh && ws?.readyState !== WebSocket.OPEN) return;
  if (ws?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({
      type: 'chat_session',
      session: sid,
      profileId,
      flowProjectId,
    }));
  }
}

function installPageChatHook() {
  if (globalThis.__FLOW_CHAT_HOOK__) return globalThis.__FLOW_CHAT_SESSION__ || null;
  globalThis.__FLOW_CHAT_HOOK__ = true;
  const re = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const interesting = (url, text) => /GN0Bre|StreamChat|CreationAgent|FlowCreationAgent/i.test(
    String(url || '') + ' ' + String(text || ''),
  );
  const bodyToText = (body) => {
    if (body == null) return null;
    if (typeof body === 'string') return body;
    if (typeof URLSearchParams !== 'undefined' && body instanceof URLSearchParams) return body.toString();
    return null;
  };
  const pull = (url, body) => {
    try {
      const text = bodyToText(body);
      if (!interesting(url, text) || !text) return;
      let freq = null;
      if (text.includes('f.req=')) freq = new URLSearchParams(text).get('f.req');
      else if (text.trim().startsWith('[')) freq = text;
      if (!freq) return;
      const m = String(freq).match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i);
      if (!m) return;
      globalThis.__FLOW_CHAT_SESSION__ = m[0];
      try { globalThis.postMessage({ type: 'FLOW_CHAT_SESSION', session: m[0] }, '*'); } catch {}
    } catch {}
  };
  const origFetch = globalThis.fetch;
  if (typeof origFetch === 'function') {
    globalThis.fetch = async function (...args) {
      try {
        const input = args[0];
        const init = args[1] || {};
        const url = typeof input === 'string' ? input : input?.url || '';
        let body = init.body;
        if (body == null && typeof Request !== 'undefined' && input instanceof Request) {
          try { body = await input.clone().text(); } catch { body = null; }
        }
        pull(url, body);
      } catch {}
      return origFetch.apply(this, args);
    };
  }
  if (globalThis.XMLHttpRequest) {
    const xo = XMLHttpRequest.prototype.open;
    const xs = XMLHttpRequest.prototype.send;
    XMLHttpRequest.prototype.open = function (method, url, ...rest) {
      this.__flowChatUrl = url;
      return xo.call(this, method, url, ...rest);
    };
    XMLHttpRequest.prototype.send = function (body) {
      try { pull(this.__flowChatUrl, body); } catch {}
      return xs.call(this, body);
    };
  }
  const fromBag = (bag, keyed) => {
    try {
      for (let i = 0; i < bag.length; i++) {
        const k = bag.key(i) || '';
        const v = (bag.getItem(k) || '').trim();
        if (keyed && !/session|conversation|chat|thread|agent/i.test(k + v)) continue;
        if (re.test(v)) return v;
      }
    } catch {}
    return null;
  };
  const found = globalThis.__FLOW_CHAT_SESSION__
    || fromBag(localStorage, true) || fromBag(sessionStorage, true);
  if (typeof found === 'string' && re.test(found)) {
    globalThis.__FLOW_CHAT_SESSION__ = found;
  }
  return globalThis.__FLOW_CHAT_SESSION__ || null;
}

async function probeChatSession() {
  if (_stashedChatSession) reportChatSession(_stashedChatSession);
  const tabs = await chrome.tabs.query({ url: flowUrls });
  for (const tab of tabs) {
    if (!tab.id || tab.discarded) continue;
    try {
      const [inj] = await chrome.scripting.executeScript({
        target: { tabId: tab.id },
        world: 'MAIN',
        func: installPageChatHook,
      });
      const sid = inj?.result;
      if (typeof sid === 'string' && NETLOG_UUID.test(sid)) {
        reportChatSession(sid);
        return;
      }
    } catch {}
  }
}

function postNetlog(rec) {
  fetch('http://127.0.0.1:8100/api/ext/netlog', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ ...rec, profileId }),
  }).catch(() => {});
}

chrome.webRequest.onBeforeRequest.addListener((d) => {
  if (d.method && d.method !== 'POST') return;
  if (NETLOG_SKIP.test(d.url || '')) return;
  const body = netlogDecodeBody(d.requestBody);
  const freq = netlogFreq(body);
  const url = d.url || '';
  const session = extractChatSession(url, freq);
  if (session) reportChatSession(session);
  if (netlogPending.size > 80) netlogPending.clear();
  netlogPending.set(d.requestId, {
    ts: new Date().toISOString(),
    url,
    rpcid: netlogRpcid(url),
    session,
    freq: freq ? String(freq).slice(0, NETLOG_FREQ_MAX) : null,
  });
}, { urls: NETLOG_HOSTS }, ['requestBody']);

chrome.webRequest.onCompleted.addListener((d) => {
  const rec = netlogPending.get(d.requestId);
  if (!rec) return;
  netlogPending.delete(d.requestId);
  postNetlog({ ...rec, statusCode: d.statusCode });
}, { urls: NETLOG_HOSTS });

chrome.webRequest.onErrorOccurred.addListener((d) => {
  netlogPending.delete(d.requestId);
}, { urls: NETLOG_HOSTS });

let _openingFlowTab = false;

async function captureTokenFromFlowTab() {
  const tabs = await chrome.tabs.query({ url: flowUrls });
  if (!tabs.length) {
    if (_openingFlowTab) {
      console.log('[FlowAgent] Flow tab already opening, skipping');
      return;
    }
    _openingFlowTab = true;
    try {
      console.log('[FlowAgent] No Flow tab found — opening one in background');
      await chrome.tabs.create({ url: FLOW_TAB_URL, active: false });
      await sleep(3000);
      const retryTabs = await chrome.tabs.query({ url: flowUrls });
      if (!retryTabs.length) {
        console.log('[FlowAgent] Flow tab not ready yet after open');
        return;
      }
      await chrome.scripting.executeScript({
        target: { tabId: retryTabs[0].id },
        files: ['content.js'],
      });
      console.log('[FlowAgent] Token refresh triggered on newly opened Flow tab');
    } catch (e) {
      console.error('[FlowAgent] Token refresh failed after opening tab:', e);
    } finally {
      _openingFlowTab = false;
    }
    return;
  }
  try {
    await chrome.scripting.executeScript({
      target: { tabId: tabs[0].id },
      files: ['content.js'],
    });
    console.log('[FlowAgent] Token refresh triggered on Flow tab');
  } catch (e) {
    console.error('[FlowAgent] Token refresh failed:', e);
  }
}

// ─── WebSocket to Agent ─────────────────────────────────────

function connectToAgent() {
  if (manualDisconnect) return;
  if (ws?.readyState === WebSocket.CONNECTING) return;
  if (ws?.readyState === WebSocket.OPEN) return;

  try {
    ws = new WebSocket(AGENT_WS_URL);
  } catch (e) {
    console.error('[FlowAgent] WS connect error:', e);
    scheduleReconnect();
    return;
  }

  ws.onopen = () => {
    console.log('[FlowAgent] Connected to agent');
    chrome.alarms.clear('reconnect');
    setState('idle');

    // Token refresh alarm — 45 min gives buffer before ~60 min expiry
    chrome.alarms.create('token-refresh', { periodInMinutes: 45 });

    // Send current state + resend token if we have one.
    // Extra fields are optional; keep this send synchronous so a cold MV3
    // worker is ready before the first RPC.
    ws.send(JSON.stringify({
      type: 'extension_ready',
      flowKeyPresent: !!flowKey,
      tokenAge: flowKey && metrics.tokenCapturedAt ? Date.now() - metrics.tokenCapturedAt : null,
      profileId,
      flowProjectId,
    }));
    if (flowKey) {
      ws.send(JSON.stringify({ type: 'token_captured', flowKey }));
    }
    if (_stashedChatSession) reportChatSession(_stashedChatSession);
    void refreshFlowProject();
    void probeChatSession();
  };

  ws.onmessage = async ({ data }) => {
    try {
      const msg = JSON.parse(data);

      if (msg.method === 'batch_rpc') {
        await handleBatchRpc(msg);
      } else if (msg.method === 'api_request') {
        await handleApiRequest(msg);
      } else if (msg.method === 'trpc_request') {
        await handleTrpcRequest(msg);
      } else if (msg.method === 'solve_captcha') {
        await handleSolveCaptcha(msg);
      } else if (msg.method === 'reload_flow_tab') {
        await handleReloadFlowTab(msg);
      } else if (msg.method === 'get_status') {
        sendToAgent({
          id: msg.id,
          result: {
            state,
            flowKeyPresent: !!flowKey,
            manualDisconnect,
            tokenAge: metrics.tokenCapturedAt ? Date.now() - metrics.tokenCapturedAt : null,
            metrics,
            profileId,
            flowProjectId,
          },
        });
      } else if (msg.type === 'callback_secret') {
        callbackSecret = msg.secret;
        chrome.storage.local.set({ callbackSecret: msg.secret });
        console.log('[FlowAgent] Received callback secret');
      } else if (msg.type === 'pong') {
        // keepalive response
      }
    } catch (e) {
      console.error('[FlowAgent] Message error:', e);
    }
  };

  ws.onclose = () => {
    setState('off');
    chrome.alarms.clear('token-refresh');
    if (!manualDisconnect) scheduleReconnect();
  };

  ws.onerror = (e) => {
    console.error('[FlowAgent] WS error:', e);
    metrics.lastError = 'WS_ERROR';
    chrome.storage.local.set({ metrics });
  };
}

function scheduleReconnect() {
  chrome.alarms.create('reconnect', { delayInMinutes: 0.083 }); // ~5s
}

function keepAlive() {
  if (ws?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type: 'ping' }));
    void refreshFlowProject();
    void probeChatSession();
  } else {
    connectToAgent();
  }
}

function sendToAgent(msg) {
  // API responses (with msg.id) go via HTTP — immune to WS disconnect
  if (msg.id) {
    fetch('http://127.0.0.1:8100/api/ext/callback', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(msg),
    }).catch(() => {
      // HTTP failed — fallback to WS
      if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
    });
    return;
  }
  // Non-response messages (ping, status) or no secret yet — use WS
  if (ws?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(msg));
  }
}

// ─── reCAPTCHA Solving ──────────────────────────────────────

const RECAPTCHA_SITE_KEY = '6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV';
let _recaptchaCode = null;

async function loadRecaptchaCode() {
  if (_recaptchaCode) return _recaptchaCode;
  const loaderUrl = 'https://www.google.com/recaptcha/enterprise.js?render='
    + encodeURIComponent(RECAPTCHA_SITE_KEY);
  const loaderResp = await fetch(loaderUrl);
  if (!loaderResp.ok) throw new Error(`recaptcha loader HTTP ${loaderResp.status}`);
  const loader = await loaderResp.text();
  const match = loader.match(/https:\/\/www\.gstatic\.com\/recaptcha\/releases\/[^'"\s]+\/recaptcha__\w+\.js/);
  if (!match) throw new Error('recaptcha release url missing');
  const codeResp = await fetch(match[0]);
  if (!codeResp.ok) throw new Error(`recaptcha en HTTP ${codeResp.status}`);
  _recaptchaCode = await codeResp.text();
  return _recaptchaCode;
}

async function requestCaptchaFromTab(tabId, requestId, pageAction) {
  let recaptchaCode = null;
  let recaptchaError = null;
  try {
    recaptchaCode = await loadRecaptchaCode();
  } catch (error) {
    recaptchaError = error?.message || String(error);
  }
  const payload = {
    type: 'GET_CAPTCHA',
    requestId,
    pageAction,
    recaptchaCode,
    recaptchaError,
  };
  try {
    return await chrome.tabs.sendMessage(tabId, payload);
  } catch (error) {
    const msg = error?.message || '';
    const shouldInject =
      msg.includes('Receiving end does not exist') ||
      msg.includes('Could not establish connection');
    if (!shouldInject) throw error;

    // Inject content script and retry
    try {
      await chrome.scripting.executeScript({
        target: { tabId },
        files: ['content.js'],
      });
    } catch (e) {
      const execMsg = e?.message || '';
      if (execMsg.includes('Frame with ID 0 is showing error page') || execMsg.includes('cannot be scripted')) {
        try { await chrome.tabs.update(tabId, { url: FLOW_TAB_URL }); } catch {}
        return { error: 'ERROR_PAGE_NAVIGATED', isErrorPage: true };
      }
      throw e;
    }
    await sleep(200);
    return await chrome.tabs.sendMessage(tabId, payload);
  }
}

/** Helper to wait until a tab finishes loading its HTML without error page. */
async function waitForTabLoad(tabId, timeoutMs = 8000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const t = await chrome.tabs.get(tabId);
      if (t && t.status === 'complete' && t.url && !t.url.startsWith('chrome-error://') && !t.url.includes('chromewebdata')) {
        return t;
      }
    } catch {}
    await sleep(350);
  }
  return null;
}

/** Try to wake a discarded Flow tab so `sendMessage` can reach it.
 *  Chrome auto-discards backgrounded tabs to save memory; the tab still shows
 *  up in `chrome.tabs.query` but cross-context calls fail with "No current
 *  window" / "No tab with id". A reload re-hydrates it. */
async function reviveTabIfNeeded(tab) {
  if (!tab?.discarded) return tab;
  try {
    await chrome.tabs.reload(tab.id);
    await sleep(2500);
    return await chrome.tabs.get(tab.id);
  } catch {
    return null;
  }
}

function captchaFromTab(tabId, requestId, captchaAction) {
  return Promise.race([
    requestCaptchaFromTab(tabId, requestId, captchaAction),
    new Promise((_, rej) => setTimeout(() => rej(new Error('CAPTCHA_TIMEOUT')), 30000)),
  ]);
}

async function solveCaptcha(requestId, captchaAction) {
  let tabs = await chrome.tabs.query({ url: flowUrls });

  // If no Flow tab, check if any tab was knocked to chrome-error:// or chromewebdata
  if (!tabs.length) {
    try {
      const allTabs = await chrome.tabs.query({});
      for (const t of allTabs) {
        if (t.url && (t.url.startsWith('chrome-error://') || t.url.includes('chromewebdata'))) {
          try {
            await chrome.tabs.update(t.id, { url: FLOW_TAB_URL, active: true });
            await waitForTabLoad(t.id, 6000);
          } catch {}
        }
      }
      tabs = await chrome.tabs.query({ url: flowUrls });
    } catch {}
  }

  // Still no Flow tab — spawn one and wait for it to settle
  if (!tabs.length) {
    try {
      const created = await chrome.tabs.create({ url: FLOW_TAB_URL, active: false });
      await waitForTabLoad(created.id, 8000);
      tabs = await chrome.tabs.query({ url: flowUrls });
    } catch (e) {
      return { error: e.message || 'NO_FLOW_TAB' };
    }
    if (!tabs.length) return { error: 'NO_FLOW_TAB' };
  }

  // Try each Flow tab in turn
  const errors = [];
  for (const candidate of tabs) {
    // If candidate tab is on an error page, redirect it and try next candidate
    if (candidate.url && (candidate.url.startsWith('chrome-error://') || candidate.url.includes('chromewebdata'))) {
      try { await chrome.tabs.update(candidate.id, { url: FLOW_TAB_URL }); } catch {}
      continue;
    }

    const tab = await reviveTabIfNeeded(candidate);
    if (!tab) continue;
    try {
      try { await chrome.tabs.update(tab.id, { active: true }); } catch {}
      const resp = await captchaFromTab(tab.id, requestId, captchaAction);
      if (resp?.isErrorPage) {
        errors.push(resp?.error || 'ERROR_PAGE');
        continue;
      }
      if (!resp?.token) {
        errors.push(resp?.error || 'NO_TOKEN');
        continue;
      }
      return resp;
    } catch (e) {
      const msg = e?.message || '';
      errors.push(msg);
      if (
        msg.includes('No current window') ||
        msg.includes('No tab with id') ||
        msg.includes('Receiving end does not exist') ||
        msg.includes('Frame with ID 0 is showing error page') ||
        msg.includes('cannot be scripted')
      ) {
        if ((msg.includes('Frame with ID 0 is showing error page') || msg.includes('cannot be scripted')) && tab?.id) {
          try { await chrome.tabs.update(tab.id, { url: FLOW_TAB_URL }); } catch {}
        }
        continue;
      }
      return { error: msg };
    }
  }

  // Every candidate failed — last-ditch, spawn a fresh tab and wait for it to load completely
  try {
    const createdTab = await chrome.tabs.create({ url: FLOW_TAB_URL, active: true });
    const readyTab = (await waitForTabLoad(createdTab.id, 8000)) || createdTab;
    await sleep(1000);
    const resp = await captchaFromTab(readyTab.id, requestId, captchaAction);
    if (resp?.token) return resp;
    return resp || { error: 'NO_TOKEN_AFTER_FRESH_TAB' };
  } catch (e) {
    return { error: e?.message || errors[0] || 'NO_FLOW_TAB' };
  }
}

async function handleSolveCaptcha(msg) {
  const { id, params } = msg;
  const result = await solveCaptcha(id, params?.captchaAction || 'VIDEO_GENERATION');

  // Standalone captcha solve counts as captcha-consuming
  metrics.requestCount++;
  if (result?.token) {
    metrics.successCount++;
  } else {
    metrics.failedCount++;
    metrics.lastError = result?.error || 'NO_TOKEN';
  }
  chrome.storage.local.set({ metrics });

  sendToAgent({ id, result });
}

async function handleReloadFlowTab(msg) {
  const { id } = msg;
  try {
    const tabs = await chrome.tabs.query({ url: flowUrls });
    let reloadedCount = 0;
    for (const tab of tabs) {
      if (!tab.discarded) {
        try {
          await chrome.tabs.reload(tab.id);
          reloadedCount++;
        } catch (_) {}
      }
    }
    if (reloadedCount === 0) {
      await chrome.tabs.create({ url: FLOW_TAB_URL, active: false });
      reloadedCount = 1;
    }
    // Give page 3.5 seconds to reload DOM, WIZ_global_data, and re-init grecaptcha
    await sleep(3500);
    sendToAgent({ id, result: { ok: true, reloaded: reloadedCount } });
  } catch (err) {
    sendToAgent({ id, result: { ok: false, error: err?.message || String(err) } });
  }
}

// ─── Page-context RPC runner (the current path) ─────────────
//
// Flow's frontend signs its calls with cookies and a per-page `at` token, and
// every generate carries a single-use reCAPTCHA. None of that can be replayed
// from the service worker, so the request has to be issued by the Flow page
// itself: mint a fresh captcha through the grecaptcha bridge, then run the
// batchexecute POST in the page's MAIN world, where at / f.sid / bl live.

const CAPTCHA_SLOT = '__CAPTCHA__';
const MAX_RPC_TEXT = 32000000; // the project listing alone is past 17 MB

async function ensureLoadedFlowTab(timeoutMs = 8000) {
  const tabs = await chrome.tabs.query({ url: flowUrls });
  for (const t of tabs) {
    if (!t.discarded && t.url && !t.url.startsWith('chrome-error://') && !t.url.includes('chromewebdata')) {
      return t;
    }
  }
  try {
    const created = await chrome.tabs.create({ url: FLOW_TAB_URL, active: true });
    return (await waitForTabLoad(created.id, timeoutMs)) || created;
  } catch {
    return null;
  }
}

async function runBatchRpc(cmd) {
  const tabs = await chrome.tabs.query({ url: flowUrls });
  let candidate = tabs.find((t) => !t.discarded && !t.url?.startsWith('chrome-error://') && !t.url?.includes('chromewebdata')) || tabs[0];
  if (!candidate || candidate.url?.startsWith('chrome-error://') || candidate.url?.includes('chromewebdata')) {
    if (candidate) {
      try { await chrome.tabs.update(candidate.id, { url: FLOW_TAB_URL }); } catch {}
    }
    candidate = await ensureLoadedFlowTab(7000);
    if (!candidate) return { error: 'NO_FLOW_TAB' };
  }
  // Chrome discards backgrounded tabs; executeScript throws on a dead one.
  const tab = await reviveTabIfNeeded(candidate);
  if (!tab) return { error: 'FLOW_TAB_DISCARDED' };

  let freq = cmd.freq;
  if (cmd.captchaAction) {
    const solved = await solveCaptcha(cmd.id, cmd.captchaAction);
    if (!solved?.token) return { error: `CAPTCHA_FAILED: ${solved?.error || 'no token'}` };
    freq = freq.split(CAPTCHA_SLOT).join(solved.token);
  }

  const args = [cmd.rpcid, freq, MAX_RPC_TEXT, cmd.match || null, cmd.path || null];

  // Prefer the content-script bridge: Chrome 152 seeded unpacked copies often
  // resolve executeScript with an empty result, which became NO_INJECTION_RESULT
  // on every r2v bind. injected.js already sits in MAIN and owns WIZ / at.
  try {
    const viaContent = await chrome.tabs.sendMessage(tab.id, {
      type: 'BATCH_RPC',
      requestId: cmd.id,
      rpcid: cmd.rpcid,
      freq,
      maxText: MAX_RPC_TEXT,
      match: cmd.match || null,
      path: cmd.path || null,
    });
    if (viaContent && (viaContent.text || viaContent.error || viaContent.status)) {
      return viaContent;
    }
  } catch (_) {
    // no content script on this tab — fall through to executeScript
  }

  let results;
  try {
    results = await chrome.scripting.executeScript({
      target: { tabId: tab.id },
      world: 'MAIN',
      args,
      func: async (rpcid, freqStr, maxText, match, customPath) => {
        if (typeof globalThis.__flowRunBatch === 'function') {
          return globalThis.__flowRunBatch(rpcid, freqStr, maxText, match, customPath);
        }
        return { error: 'NO_BATCH_RUNNER' };
      },
    });
  } catch (e) {
    const execErr = e?.message || '';
    if (execErr.includes('Frame with ID 0 is showing error page') || execErr.includes('cannot be scripted')) {
      // Auto-recover error page
      try { await chrome.tabs.update(tab.id, { url: FLOW_TAB_URL }); } catch {}
      // Retry once on a freshly loaded Flow tab
      const fallbackTab = await ensureLoadedFlowTab(7000);
      if (fallbackTab && fallbackTab.id !== tab.id) {
        try {
          const retryResults = await chrome.scripting.executeScript({
            target: { tabId: fallbackTab.id },
            world: 'MAIN',
            args,
            func: async (rpcid, freqStr, maxText, match, customPath) => {
              if (typeof globalThis.__flowRunBatch === 'function') {
                return globalThis.__flowRunBatch(rpcid, freqStr, maxText, match, customPath);
              }
              return { error: 'NO_BATCH_RUNNER' };
            },
          });
          const injectedRetry = (retryResults || []).find((row) => row && row.result != null);
          if (injectedRetry?.result) return injectedRetry.result;
        } catch (_) {}
      }
    }
    return { error: execErr || 'INJECT_FAILED' };
  }
  const injected = (results || []).find((row) => row && row.result != null);
  return injected?.result || { error: 'NO_INJECTION_RESULT' };
}

async function handleBatchRpc(msg) {
  const { id, params } = msg;
  const { rpcid, freq, captchaAction, match, path } = params || {};
  if (!rpcid || !freq) {
    sendToAgent({ id, status: 400, error: 'INVALID_BATCH_RPC' });
    return;
  }

  setState('running');
  const hasCaptcha = !!captchaAction;
  if (hasCaptcha) metrics.requestCount++;
  // Polls and listing lookups run constantly; only the generates are worth
  // a row in the log the popup shows.
  const visible = hasCaptcha;
  if (visible) {
    addRequestLog({
      id, type: `RPC:${rpcid}`, time: new Date().toISOString(),
      status: 'processing', error: null, outputUrl: null, url: rpcid,
      payloadSummary: freq.slice(0, 200),
    });
  }

  try {
    const out = await runBatchRpc({ id, rpcid, freq, captchaAction, match, path });
    if (out.error) {
      if (hasCaptcha) { metrics.failedCount++; metrics.lastError = out.error; }
      if (visible) updateRequestLog(id, { status: 'failed', error: out.error });
      sendToAgent({ id, status: 502, error: out.error });
    } else {
      if (hasCaptcha) { metrics.successCount++; metrics.lastError = null; }
      if (visible) {
        updateRequestLog(id, {
          status: 'success', httpStatus: out.status,
          responseSummary: (out.text || '').slice(0, 300),
        });
      }
      sendToAgent({ id, status: out.status, data: out.text });
    }
  } catch (e) {
    const err = e?.message || 'BATCH_RPC_FAILED';
    if (hasCaptcha) { metrics.failedCount++; metrics.lastError = err; }
    if (visible) updateRequestLog(id, { status: 'failed', error: err });
    sendToAgent({ id, status: 500, error: err });
  }

  chrome.storage.local.set({ metrics });
  setState('idle');
}

// ─── API Request Proxy ──────────────────────────────────────

async function handleTrpcRequest(msg) {
  const { id, params } = msg;
  const { url, method = 'POST', headers = {}, body, responseMode = 'json' } = params;

  if (!url || !url.startsWith('https://labs.google/')) {
    sendToAgent({ id, error: 'INVALID_TRPC_URL' });
    return;
  }

  setState('running');
  // TRPC calls don't consume captcha — don't count in metrics

  const logId = id;
  const logType = url.includes('createProject') ? 'CREATE_PROJECT' : 'TRPC';
  // TRPC calls are silent — don't show in request log

  const fetchHeaders = { 'Content-Type': 'application/json', ...headers };
  if (flowKey) {
    fetchHeaders['authorization'] = `Bearer ${flowKey}`;
  }

  try {
    const resp = await fetch(url, {
      method,
      headers: fetchHeaders,
      body: body ? JSON.stringify(body) : undefined,
      credentials: 'include',
    });
    let data;
    if (responseMode === 'url') {
      // fetch() has already followed the authenticated Flow redirect. Return
      // only the final signed URL and cancel the body so large videos are not
      // buffered in the extension or copied through the WebSocket bridge.
      data = {
        url: resp.url,
        contentType: resp.headers.get('content-type'),
      };
      await resp.body?.cancel();
    } else {
      data = await resp.json();
    }
    chrome.storage.local.set({ metrics });
    updateRequestLog(logId, { status: 'success' });
    sendToAgent({ id, status: resp.status, data });
  } catch (e) {
    console.error('[FlowAgent] tRPC request failed:', e);
    chrome.storage.local.set({ metrics });
    updateRequestLog(logId, { status: 'failed', error: e.message || 'TRPC_FETCH_FAILED' });
    sendToAgent({ id, error: e.message || 'TRPC_FETCH_FAILED' });
  } finally {
    setState('idle');
  }
}

// Legacy REST proxy against aisandbox-pa. Reachable only with USE_BATCH_RPC=0
// on a profile that still holds a `Bearer ya29.…`; Flow stopped minting those.
async function handleApiRequest(msg) {
  const { id, params } = msg;
  const { url, method, headers, body, captchaAction } = params;

  if (!url) {
    sendToAgent({ id, error: 'MISSING_URL' });
    return;
  }

  if (!url.startsWith('https://aisandbox-pa.googleapis.com/')) {
    sendToAgent({ id, error: 'INVALID_URL' });
    return;
  }

  setState('running');
  const hasCaptcha = !!captchaAction;
  if (hasCaptcha) metrics.requestCount++;

  const logId = id;
  const logType = _classifyApiUrl(url);
  if (_VISIBLE_TYPES.has(logType)) {
    const payloadSummary = body ? JSON.stringify(body).slice(0, 200) : null;
    addRequestLog({ id: logId, type: logType, time: new Date().toISOString(), status: 'processing', error: null, outputUrl: null, url, payloadSummary });
  }

  try {
    // Step 1: Solve captcha if needed
    let captchaToken = null;
    if (captchaAction) {
      const captchaResult = await solveCaptcha(id, captchaAction);
      captchaToken = captchaResult?.token || null;
      if (!captchaToken) {
        // Cannot proceed without captcha — API will 403
        const err = captchaResult?.error || 'CAPTCHA_FAILED';
        console.error(`[FlowAgent] Captcha failed for ${captchaAction}: ${err}`);
        sendToAgent({ id, status: 403, error: `CAPTCHA_FAILED: ${err}` });
        if (hasCaptcha) { metrics.failedCount++; metrics.lastError = `CAPTCHA_FAILED: ${err}`; }
        chrome.storage.local.set({ metrics });
        updateRequestLog(logId, { status: 'failed', error: `CAPTCHA_FAILED: ${err}` });
        setState('idle');
        return;
      }
    }

    // Step 2: Inject captcha token into body
    let finalBody = body;
    if (captchaToken && finalBody) {
      finalBody = JSON.parse(JSON.stringify(finalBody)); // deep clone
      if (finalBody.clientContext?.recaptchaContext) {
        finalBody.clientContext.recaptchaContext.token = captchaToken;
      }
      if (finalBody.requests && Array.isArray(finalBody.requests)) {
        for (const req of finalBody.requests) {
          if (req.clientContext?.recaptchaContext) {
            req.clientContext.recaptchaContext.token = captchaToken;
          }
        }
      }
    }

    // Step 3: Use flowKey for auth
    const activeFlowKey = flowKey;
    if (!activeFlowKey) {
      sendToAgent({ id, status: 503, error: 'NO_FLOW_KEY' });
      if (hasCaptcha) { metrics.failedCount++; metrics.lastError = 'NO_FLOW_KEY'; }
      chrome.storage.local.set({ metrics });
      updateRequestLog(logId, { status: 'failed', error: 'NO_FLOW_KEY' });
      setState('idle');
      return;
    }

    const fetchHeaders = { ...(headers || {}) };
    fetchHeaders['authorization'] = `Bearer ${activeFlowKey}`;

    // Step 4: Make the API call from browser context
    const response = await fetch(url, {
      method: method || 'POST',
      headers: fetchHeaders,
      credentials: 'include',
      body: method === 'GET' ? undefined : JSON.stringify(finalBody),
    });

    let responseData;
    const responseText = await response.text();
    try {
      responseData = JSON.parse(responseText);
    } catch {
      responseData = responseText;
    }

    sendToAgent({
      id,
      status: response.status,
      data: responseData,
    });

    const responseSummary = responseText ? responseText.slice(0, 300) : null;
    if (response.ok) {
      if (hasCaptcha) { metrics.successCount++; metrics.lastError = null; }
      updateRequestLog(logId, { status: 'success', httpStatus: response.status, responseSummary });
    } else {
      if (hasCaptcha) { metrics.failedCount++; metrics.lastError = `API_${response.status}`; }
      updateRequestLog(logId, { status: 'failed', error: `API_${response.status}`, httpStatus: response.status, responseSummary });
    }
  } catch (e) {
    sendToAgent({
      id,
      status: 500,
      error: e.message || 'API_REQUEST_FAILED',
    });
    if (hasCaptcha) { metrics.failedCount++; metrics.lastError = e.message; }
    updateRequestLog(logId, { status: 'failed', error: e.message || 'API_REQUEST_FAILED' });
  }

  chrome.storage.local.set({ metrics });
  setState('idle');
}

// ─── State & Popup ──────────────────────────────────────────

function setState(newState) {
  state = newState;
  const badges = { idle: '●', running: '▶', off: '○' };
  const colors = { idle: '#22c55e', running: '#f59e0b', off: '#6b7280' };
  chrome.action.setBadgeText({ text: badges[state] || '' });
  chrome.action.setBadgeBackgroundColor({ color: colors[state] || '#000' });
  broadcastStatus();
}

function broadcastStatus() {
  chrome.runtime.sendMessage({ type: 'STATUS_PUSH' }).catch(() => {});
}

if (chrome.tabs?.onUpdated) {
  chrome.tabs.onUpdated.addListener((_tabId, changeInfo, tab) => {
    if (changeInfo.status === 'complete') void probeChatSession();
    if (!changeInfo.url && changeInfo.status !== 'complete') return;
    const pid = flowProjectFromUrl(changeInfo.url || tab?.url);
    if (!pid || pid === flowProjectId) return;
    flowProjectId = pid;
    chrome.storage.local.set({ flowProjectId });
    sendProfileUpdate();
  });
}

chrome.runtime.onMessage.addListener((msg, _, reply) => {
  if (msg.type === 'FLOW_CHAT_SESSION') {
    reportChatSession(msg.session);
    reply({ ok: true });
    return;
  }

  if (msg.type === 'STATUS') {
    reply({
      connected: ws?.readyState === WebSocket.OPEN,
      agentConnected: ws?.readyState === WebSocket.OPEN,
      flowKeyPresent: !!flowKey,
      manualDisconnect,
      tokenAge: metrics.tokenCapturedAt ? Date.now() - metrics.tokenCapturedAt : null,
      metrics: {
        requestCount: metrics.requestCount,
        successCount: metrics.successCount,
        failedCount: metrics.failedCount,
        lastError: metrics.lastError,
      },
      state,
      profileId,
      flowProjectId,
    });
  }

  if (msg.type === 'SET_PROFILE_ID') {
    profileId = String(msg.profileId || '').trim() || null;
    chrome.storage.local.set({ profileId });
    sendProfileUpdate();
    reply({ ok: true, profileId });
    return true;
  }

  if (msg.type === 'DISCONNECT') {
    manualDisconnect = true;
    if (ws) ws.close();
    reply({ ok: true });
    return true;
  }

  if (msg.type === 'RECONNECT') {
    manualDisconnect = false;
    connectToAgent();
    reply({ ok: true });
    return true;
  }

  if (msg.type === 'REQUEST_LOG') {
    reply({ log: requestLog });
    return true;
  }

  if (msg.type === 'OPEN_FLOW_TAB') {
    chrome.tabs.query({ url: flowUrls }).then((tabs) => {
      if (tabs.length) {
        chrome.tabs.update(tabs[0].id, { active: true });
        reply({ ok: true, tabId: tabs[0].id });
      } else {
        chrome.tabs.create({ url: FLOW_TAB_URL })
          .then((tab) => reply({ ok: true, tabId: tab.id }))
          .catch((e) => reply({ error: e.message }));
      }
    }).catch((e) => reply({ error: e.message }));
    return true;
  }

  if (msg.type === 'REFRESH_TOKEN') {
    captureTokenFromFlowTab()
      .then(() => reply({ ok: true }))
      .catch((e) => reply({ error: e.message }));
    return true;
  }

  if (msg.type === 'TEST_CAPTCHA') {
    solveCaptcha(`test-${Date.now()}`, msg.pageAction || 'IMAGE_GENERATION')
      .then((r) => reply(r))
      .catch((e) => reply({ error: e.message }));
    return true;
  }

  if (msg.type === 'TRPC_MEDIA_URLS') {
    handleTrpcMediaUrls(msg.trpcUrl, msg.body);
    reply({ ok: true });
    return true;
  }

  return true;
});

// ─── TRPC Media URL Extractor ──────────────────────────────

function handleTrpcMediaUrls(trpcUrl, bodyText) {
  try {
    // Extract all fresh GCS signed URLs
    const urlRegex = /https:\/\/storage\.googleapis\.com\/ai-sandbox-videofx\/(?:image|video)\/[0-9a-f-]{36}\?[^"'\s]+/g;
    const matches = bodyText.match(urlRegex) || [];
    if (!matches.length) return;

    // Deduplicate and parse
    const urlMap = {};
    for (const rawUrl of matches) {
      // Unescape JSON-escaped URLs
      const url = rawUrl.replace(/\\u0026/g, '&').replace(/\\/g, '');
      const mediaMatch = url.match(/\/(image|video)\/([0-9a-f-]{36})\?/);
      if (mediaMatch) {
        const [, mediaType, mediaId] = mediaMatch;
        // Keep last occurrence (freshest)
        urlMap[mediaId] = { mediaType, url, mediaId };
      }
    }

    const entries = Object.values(urlMap);
    if (!entries.length) return;

    console.log(`[FlowAgent] Captured ${entries.length} fresh media URLs from TRPC`);
    // URL refresh is silent — don't show in request log

    // Forward to agent for DB update
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'media_urls_refresh',
        urls: entries,
      }));
    }
  } catch (e) {
    console.error('[FlowAgent] Failed to extract TRPC media URLs:', e);
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ─── Human-like Telemetry ──────────────────────────────────
// Periodically send tracking events to Google's analytics endpoints
// to mimic normal browser behavior.

const _UA = navigator.userAgent;
let _telemetrySessionId = `;${Date.now()}`;

function _rand(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }

function _buildBatchLogPayload() {
  const events = [];
  const types = ['FLOW_IMAGE_LATENCY', 'FLOW_VIDEO_LATENCY'];
  const count = _rand(1, 3);
  for (let i = 0; i < count; i++) {
    events.push({
      event: types[_rand(0, types.length - 1)],
      eventProperties: [
        { key: 'CURRENT_TIME_MS', doubleValue: Date.now() },
        { key: 'DURATION_MS', doubleValue: _rand(150, 800) },
        { key: 'USER_AGENT', stringValue: _UA },
        { key: 'IS_DESKTOP', booleanValue: true },
      ],
      eventMetadata: { sessionId: _telemetrySessionId },
      eventTime: new Date().toISOString(),
    });
  }
  return { appEvents: events };
}

function _buildFrontendEventsPayload() {
  const eventTypes = [
    'FLOW_IMAGE_LATENCY', 'FLOW_VIDEO_LATENCY', 'GRID_SCROLL_DEPTH',
    'FLOW_PROJECT_OPEN', 'FLOW_SCENE_VIEW',
  ];
  const count = _rand(1, 4);
  const events = [];
  for (let i = 0; i < count; i++) {
    const et = eventTypes[_rand(0, eventTypes.length - 1)];
    const params = {
      USER_AGENT: { '@type': 'type.googleapis.com/google.protobuf.StringValue', value: _UA },
      IS_DESKTOP: { '@type': 'type.googleapis.com/google.protobuf.StringValue', value: 'true' },
    };
    if (et.includes('LATENCY')) {
      params.CURRENT_TIME_MS = { '@type': 'type.googleapis.com/google.protobuf.StringValue', value: String(Date.now()) };
      params.DURATION_MS = { '@type': 'type.googleapis.com/google.protobuf.StringValue', value: String(_rand(100, 600)) };
    }
    if (et === 'GRID_SCROLL_DEPTH') {
      params.MEDIA_GENERATION_PAYGATE_TIER = { '@type': 'type.googleapis.com/google.protobuf.StringValue', value: 'PAYGATE_TIER_TWO' };
    }
    events.push({
      eventType: et,
      metadata: {
        sessionId: _telemetrySessionId,
        createTime: new Date().toISOString(),
        additionalParams: params,
      },
    });
  }
  return { events };
}

async function sendTelemetry() {
  // Legacy-path camouflage: these endpoints want the bearer Flow no longer
  // mints, so on the batch path there is no flowKey and this is a no-op.
  if (!flowKey || state === 'off') return;

  const headers = {
    'Content-Type': 'text/plain;charset=UTF-8',
    'authorization': `Bearer ${flowKey}`,
  };

  // Telemetry is silent — don't show in request log
  try {
    if (Math.random() < 0.5) {
      await fetch(`https://aisandbox-pa.googleapis.com/v1:batchLog`, {
        method: 'POST', headers, credentials: 'include',
        body: JSON.stringify(_buildBatchLogPayload()),
      });
    } else {
      await fetch(`https://aisandbox-pa.googleapis.com/v1/flow:batchLogFrontendEvents`, {
        method: 'POST', headers, credentials: 'include',
        body: JSON.stringify(_buildFrontendEventsPayload()),
      });
    }
  } catch {}
}

// Send telemetry at random intervals (45-120s) to look organic
function scheduleTelemetry() {
  const delay = _rand(45, 120) * 1000;
  setTimeout(async () => {
    await sendTelemetry();
    scheduleTelemetry(); // reschedule with new random interval
  }, delay);
}

// Refresh session ID every ~30min like a real user
setInterval(() => { _telemetrySessionId = `;${Date.now()}`; }, _rand(25, 35) * 60 * 1000);

scheduleTelemetry();

console.log('[FlowAgent] Extension loaded');
