const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { webcrypto } = require('node:crypto');
const { FlowRequestGate } = require('../extension/flow_guard.js');
const read = (name) => fs.readFileSync(path.join(__dirname, '..', 'extension', name), 'utf8');
const tick = () => new Promise(setImmediate);
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };

test('submits serialize, polls overlap, and accepted renders do not occupy a slot', async () => {
  const gate = new FlowRequestGate();
  const response = deferred();
  const events = [];
  const first = gate.run(async () => { events.push('submit1'); await response.promise; }, { submit: true });
  const second = gate.run(async () => { events.push('submit2'); }, { submit: true });
  await gate.run(async () => { events.push('poll'); });
  assert.deepEqual(events, ['submit1', 'poll']);
  response.resolve();
  await Promise.all([first, second]);
  assert.deepEqual(events, ['submit1', 'poll', 'submit2']);
  assert.equal(gate.active, 0);
});

test('maintenance drains active requests and blocks newcomers through reload', async () => {
  const gate = new FlowRequestGate();
  const response = deferred(), reload = deferred();
  const first = gate.run(() => response.promise, { submit: true });
  const paused = gate.pause('lease');
  let ran = false;
  const next = gate.run(async () => { ran = true; });
  await tick();
  assert.equal(gate.active, 1);
  assert.equal(ran, false);
  response.resolve();
  await Promise.all([first, paused]);
  const resumed = gate.resume('lease', () => reload.promise);
  await tick();
  assert.equal(ran, false);
  reload.resolve();
  await Promise.all([resumed, next]);
  assert.equal(ran, true);
});

test('expired queued work never starts later', async () => {
  const gate = new FlowRequestGate({ queueMs: 10 });
  await gate.pause('lease');
  let submitted = false;
  await assert.rejects(gate.run(async () => { submitted = true; }, { submit: true }), /NOT_SUBMITTED/);
  await gate.resume('lease');
  await tick();
  assert.equal(submitted, false);
  assert.equal(gate.submits.length, 0);
});

test('drain failure preserves active request and releases maintenance', async () => {
  const gate = new FlowRequestGate({ drainMs: 10 });
  const response = deferred();
  const work = gate.run(() => response.promise);
  await assert.rejects(gate.pause('lease'), /DRAIN_TIMEOUT/);
  assert.equal(gate.active, 1);
  assert.equal(gate.maintenance, null);
  response.resolve();
  await work;
});

test('lost maintenance client expires; wrong lease cannot release another owner', async () => {
  const gate = new FlowRequestGate({ leaseMs: 10 });
  await gate.pause('lease');
  await assert.rejects(gate.resume('wrong'), /EXPIRED/);
  await gate.run(async () => {});
  assert.equal(gate.maintenance, null);
});

function background() {
  const event = { addListener() {} };
  const chrome = {
    action: { setBadgeBackgroundColor() {}, setBadgeText() {} },
    alarms: { clear() {}, create() {}, onAlarm: event },
    runtime: { onInstalled: event, onMessage: event, onStartup: event, sendMessage: async () => {} },
    scripting: { executeScript: async () => [] },
    storage: { local: { get: async () => ({}), set: async () => {} } },
    tabs: { query: async () => [{ id: 10, url: 'https://flow.google.com/', status: 'complete' }],
      update: async () => {}, sendMessage: async () => ({ status: 200, text: 'accepted' }), onUpdated: event },
    webRequest: { onBeforeSendHeaders: event, onBeforeRequest: event, onCompleted: event, onErrorOccurred: event },
  };
  class WebSocket { static OPEN = 1; static CONNECTING = 0; constructor() { this.readyState = 0; } }
  const context = vm.createContext({ chrome, WebSocket, URL, console,
    setTimeout(fn, ms) { const timer = setTimeout(fn, ms); if (ms >= 45000) timer.unref(); return timer; }, clearTimeout,
    setInterval() {}, clearInterval() {}, fetch: async () => ({ ok: true }),
    navigator: { userAgent: 'FlowkitTest' },
    importScripts(name) { vm.runInContext(read(name), context); },
  });
  vm.runInContext(read('background.js'), context);
  context.captchaFromTab = async (tabId) => ({ token: `token-tab-${tabId}`, documentId: 'doc-10' });
  return { context, chrome };
}
const command = { id: 'one', rpcid: 'video', freq: '__CAPTCHA__', captchaAction: 'VIDEO_GENERATION' };

test('captcha and POST stay on one tab and carry document identity', async () => {
  const { context, chrome } = background();
  let request;
  chrome.tabs.sendMessage = async (tab, msg) => { request = { tab, msg }; return { status: 200, text: 'accepted' }; };
  await context.runBatchRpc(command);
  assert.equal(request.tab, 10);
  assert.equal(request.msg.freq, 'token-tab-10');
  assert.equal(request.msg.documentId, 'doc-10');
});

test('lost content response never retries through executeScript', async () => {
  const { context, chrome } = background();
  let injections = 0;
  chrome.tabs.sendMessage = async () => { throw new Error('The message port closed before a response was received.'); };
  chrome.scripting.executeScript = async () => { injections++; };
  const result = await context.runBatchRpc(command);
  assert.match(result.error, /SUBMISSION_OUTCOME_UNKNOWN/);
  assert.equal(injections, 0);
});

test('only a proven missing receiver allows same-tab fallback', async () => {
  const { context, chrome } = background();
  chrome.tabs.sendMessage = async () => { throw new Error('Receiving end does not exist'); };
  let target;
  chrome.scripting.executeScript = async (request) => { target = request; return [{ result: { status: 200, text: 'ok' } }]; };
  await context.runBatchRpc(command);
  assert.equal(target.target.tabId, 10);
  assert.equal(target.args[0].documentId, 'doc-10');
});

test('request deadline is checked again after token minting', async () => {
  const { context, chrome } = background();
  let sent = false;
  chrome.tabs.sendMessage = async () => { sent = true; };
  const result = await context.runBatchRpc({ ...command, expiresAt: Date.now() - 1 });
  assert.match(result.error, /NOT_SUBMITTED/);
  assert.equal(sent, false);
});

test('reload waits for a new ready document, not stale complete tab status', async () => {
  const { context, chrome } = background();
  let probes = 0;
  chrome.tabs.reload = async () => {};
  chrome.tabs.sendMessage = async () => ({ ready: true, documentId: ++probes < 3 ? 'old' : 'new' });
  context.waitForTabLoad = async () => ({ status: 'complete' });
  const result = await context.reloadFlowTab();
  assert.equal(result.ok, true);
  assert.equal(probes, 3);
});

function injected() {
  const listeners = new Map();
  const window = { fetch: async () => ({ status: 200, text: async () => 'accepted' }),
    WIZ_global_data: { SNlM0e: 'at', FdrFJe: 'sid' },
    addEventListener(type, fn) { listeners.set(type, fn); }, postMessage() {} };
  class XMLHttpRequest { open() {} send() {} }
  const context = vm.createContext({ window, crypto: webcrypto, XMLHttpRequest, URLSearchParams,
    ArrayBuffer, TextDecoder, AbortSignal, console, setTimeout, clearTimeout, URL });
  vm.runInContext(read('injected.js'), context);
  return { window, documentId: vm.runInContext('FLOW_DOCUMENT_ID', context) };
}

test('duplicate request IDs submit once; navigation invalidates old token', async () => {
  const { window, documentId } = injected();
  let sends = 0;
  window.__flowRunBatch = async () => { sends++; return { status: 200, text: 'accepted' }; };
  const request = { requestId: 'one', documentId, rpcid: 'video', freq: 'token' };
  await Promise.all([window.__flowExecuteBatchOnce(request), window.__flowExecuteBatchOnce(request)]);
  assert.equal(sends, 1);
  const wrong = await window.__flowExecuteBatchOnce({ ...request, requestId: 'two', documentId: 'old-page' });
  assert.match(wrong.error, /NOT_SUBMITTED/);
  assert.equal(sends, 1);
});
