const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(
  path.join(__dirname, '..', 'extension', 'background.js'),
  'utf8',
);

const lifecycleListeners = { alarm: [], installed: [], startup: [] };
const sockets = [];
const networkListeners = { before: [], error: [] };
const networkPosts = [];
let storageReads = 0;

class FakeWebSocket {
  static CONNECTING = 0;
  static OPEN = 1;

  constructor(url) {
    this.url = url;
    this.readyState = FakeWebSocket.CONNECTING;
    this.messages = [];
    sockets.push(this);
  }

  send(message) {
    this.messages.push(JSON.parse(message));
  }
}

function event(bucket) {
  return {
    addListener(listener) {
      if (bucket) bucket.push(listener);
    },
  };
}

const chrome = {
  action: { setBadgeBackgroundColor() {}, setBadgeText() {} },
  alarms: { clear() {}, create() {}, onAlarm: event(lifecycleListeners.alarm) },
  runtime: {
    onInstalled: event(lifecycleListeners.installed),
    onMessage: event(),
    onStartup: event(lifecycleListeners.startup),
    sendMessage: async () => {},
  },
  scripting: { executeScript: async () => {} },
  storage: {
    local: {
      async get() {
        storageReads += 1;
        return {
          callbackSecret: 'persisted-secret',
          flowKey: 'persisted-flow-key',
          metrics: { tokenCapturedAt: 1234 },
        };
      },
      async set() {},
    },
  },
  tabs: {
    create: async () => ({}),
    query: async () => [],
    sendMessage: async () => {},
    update: async () => {},
    onUpdated: event(),
  },
  webRequest: {
    onBeforeSendHeaders: event(),
    onBeforeRequest: event(networkListeners.before),
    onCompleted: event(),
    onErrorOccurred: event(networkListeners.error),
  },
};

const context = vm.createContext({
  importScripts(file) {
    vm.runInContext(fs.readFileSync(path.join(__dirname, '..', 'extension', file), 'utf8'), context);
  },
  URL,
  WebSocket: FakeWebSocket,
  chrome,
  clearInterval() {},
  clearTimeout() {},
  console,
  fetch: async (url, options) => {
    if (url.endsWith('/api/ext/netlog')) networkPosts.push(JSON.parse(options.body));
    return { ok: true };
  },
  navigator: { userAgent: 'FlowkitBootstrapTest/1.0' },
  setInterval() { return 1; },
  setTimeout() { return 1; },
});

vm.runInContext(source, context, { filename: 'background.js' });

setImmediate(async () => {
  await Promise.resolve();
  await Promise.resolve();

  assert.equal(storageReads, 1, 'cold worker start must hydrate storage exactly once');
  assert.equal(sockets.length, 1, 'cold worker start must connect after hydration');

  await lifecycleListeners.startup[0]();
  await lifecycleListeners.installed[0]();
  await lifecycleListeners.alarm[0]({ name: 'keepAlive' });
  assert.equal(storageReads, 1, 'lifecycle events must reuse initialization');
  assert.equal(sockets.length, 1, 'lifecycle events must not duplicate the socket');

  const socket = sockets[0];
  socket.readyState = FakeWebSocket.OPEN;
  socket.onopen();

  assert.equal(socket.messages[0].type, 'extension_ready');
  assert.equal(socket.messages[0].flowKeyPresent, true);
  assert.ok(socket.messages[0].tokenAge > 0);
  assert.deepEqual(socket.messages[1], {
    type: 'token_captured',
    flowKey: 'persisted-flow-key',
  });

  const request = { requestId: '123.4', method: 'POST', timeStamp: 1000,
    url: 'https://flow.google.com/_/AiSandboxAngularFrontend/data/batchexecute?rpcids=ogiZ0b&at=secret#private',
    requestBody: { formData: { 'f.req': ['private-prompt'] } } };
  networkListeners.before[0](request);
  networkListeners.error[0]({ ...request, timeStamp: 1250, error: 'net::ERR_CONNECTION_RESET' });
  const failure = networkPosts.at(-1);
  assert.equal(failure.networkError, 'net::ERR_CONNECTION_RESET');
  assert.equal(failure.requestId, '123.4');
  assert.equal(failure.rpcid, 'ogiZ0b');
  assert.equal(failure.elapsedMs, 250);
  assert.equal(failure.url, 'https://flow.google.com/_/AiSandboxAngularFrontend/data/batchexecute');
  assert.equal(JSON.stringify(failure).includes('secret'), false);
  assert.equal(JSON.stringify(failure).includes('private'), false);
  assert.equal('freq' in failure, false);
  networkListeners.error[0]({ ...request, requestId: 'lost', timeStamp: 1400, error: 'sensitive arbitrary error' });
  assert.equal(networkPosts.at(-1).networkError, 'UNKNOWN_NETWORK_ERROR');
  assert.equal(networkPosts.at(-1).elapsedMs, null);

  console.log('Flowkit MV3 cold-start bootstrap regression test passed');
});
