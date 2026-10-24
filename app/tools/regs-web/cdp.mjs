// T41: a COPY of app/design/regs-v11/tools/cdp.mjs (tools/ never imports from design/): a minimal headless-Chrome driver over
// the DevTools protocol (no npm dependencies; Node 22+ for WebSocket). Adds `dpr` (device pixels per CSS px), `mobile`, and
// `console` (every console error / uncaught exception the page raises, for the walk's "no console errors").
import { spawn } from 'node:child_process';
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path';

export const CHROME = process.env.CHROME || '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';

export async function open(url, { width = 440, height = 900, init = '', offline = false, dpr = 1, mobile = false, network = false } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'regs-web-'));
  const proc = spawn(CHROME, ['--headless=new', ...(process.env.CI ? ['--no-sandbox'] : []), '--remote-debugging-port=0', `--user-data-dir=${dir}`, '--no-first-run',
    '--allow-file-access-from-files', `--window-size=${width},${height}`, 'about:blank'], { stdio: ['ignore', 'ignore', 'pipe'] });
  const wsBrowser = await new Promise((res, rej) => { let buf = '';
    proc.stderr.on('data', d => { buf += d; const m = buf.match(/DevTools listening on (ws:\/\/\S+)/); if (m) res(m[1]); });
    proc.on('exit', c => rej(new Error('chrome exited ' + c))); });
  const port = new URL(wsBrowser).port;
  const list = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
  const ws = new WebSocket(list.find(t => t.type === 'page').webSocketDebuggerUrl);
  await new Promise(r => ws.addEventListener('open', r, { once: true }));
  let id = 0; const wait = new Map();
  const errors = [];
  // T129 `network`: every request the page makes, in order — url, and when it finished, the bytes on the wire
  // (`encodedDataLength`: headers + the body as sent, i.e. gzip for a .gz file) — or `failed`
  const requests = [], byId = new Map();
  const onNetwork = (m) => {
    if (m.method === 'Network.requestWillBeSent' && !m.params.redirectResponse) {
      const r = { url: m.params.request.url, method: m.params.request.method, range: m.params.request.headers.Range ?? null, bytes: null, status: null,
                  wall: m.params.wallTime ?? null };   // T186: when it was sent (epoch s), to split a prefetch off first paint
      byId.set(m.params.requestId, r); requests.push(r);
    } else if (m.method === 'Network.responseReceived') { const r = byId.get(m.params.requestId); if (r) r.status = m.params.response.status; }
    else if (m.method === 'Network.loadingFinished') { const r = byId.get(m.params.requestId); if (r) r.bytes = m.params.encodedDataLength; }
    else if (m.method === 'Network.loadingFailed') { const r = byId.get(m.params.requestId); if (r) r.failed = m.params.errorText; }
  };
  ws.addEventListener('message', e => { const m = JSON.parse(e.data); if (network && m.method?.startsWith('Network.')) onNetwork(m);
    if (m.id && wait.has(m.id)) { const [res, rej] = wait.get(m.id); wait.delete(m.id); m.error ? rej(new Error(m.error.message)) : res(m.result); }
    else if (m.method === 'Runtime.consoleAPICalled' && m.params.type === 'error') errors.push(m.params.args.map(a => a.value ?? a.description ?? '').join(' '));
    else if (m.method === 'Runtime.exceptionThrown') errors.push(m.params.exceptionDetails.exception?.description || m.params.exceptionDetails.text); });
  const send = (method, params = {}) => new Promise((res, rej) => { const i = ++id; wait.set(i, [res, rej]); ws.send(JSON.stringify({ id: i, method, params })); });
  const evaluate = async (expr) => { const r = await send('Runtime.evaluate', { expression: expr, awaitPromise: true, returnByValue: true });
    if (r.exceptionDetails) throw new Error(r.exceptionDetails.exception?.description || r.exceptionDetails.text); return r.result.value; };
  await send('Page.enable'); await send('Runtime.enable');
  await send('Emulation.setDeviceMetricsOverride', { width, height, deviceScaleFactor: dpr, mobile });
  if (init) await send('Page.addScriptToEvaluateOnNewDocument', { source: init });
  if (network) await send('Network.enable', {});
  if (offline) { await send('Network.enable'); await send('Network.setBlockedURLs', { urls: ['http://*', 'https://*'] }); }
  await send('Page.navigate', { url });
  for (let t = 0; t < 600; t++) { if (await evaluate('document.readyState').catch(() => '') === 'complete') break; await new Promise(r => setTimeout(r, 100)); }
  const close = async () => { try { ws.close(); } catch {} const gone = new Promise(r => proc.once('exit', r)); proc.kill(); await gone;
    try { fs.rmSync(dir, { recursive: true, force: true, maxRetries: 5 }); } catch {} };
  return { send, evaluate, close, errors, requests };
}
