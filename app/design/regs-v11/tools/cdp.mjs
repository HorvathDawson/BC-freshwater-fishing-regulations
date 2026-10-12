// Minimal headless-Chrome driver over the DevTools protocol (no npm dependencies; Node 22+ for WebSocket).
import { spawn } from 'node:child_process';
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path';

export const CHROME = process.env.CHROME || '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';

export async function open(url, { width = 440, height = 900, init = '', offline = false } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'regs-v11-'));
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
  ws.addEventListener('message', e => { const m = JSON.parse(e.data); if (m.id && wait.has(m.id)) { const [res, rej] = wait.get(m.id); wait.delete(m.id); m.error ? rej(new Error(m.error.message)) : res(m.result); } });
  const send = (method, params = {}) => new Promise((res, rej) => { const i = ++id; wait.set(i, [res, rej]); ws.send(JSON.stringify({ id: i, method, params })); });
  const evaluate = async (expr) => { const r = await send('Runtime.evaluate', { expression: expr, awaitPromise: true, returnByValue: true });
    if (r.exceptionDetails) throw new Error(r.exceptionDetails.exception?.description || r.exceptionDetails.text); return r.result.value; };
  await send('Page.enable'); await send('Runtime.enable');
  await send('Emulation.setDeviceMetricsOverride', { width, height, deviceScaleFactor: 1, mobile: false });
  if (init) await send('Page.addScriptToEvaluateOnNewDocument', { source: init });
  if (offline) { await send('Network.enable'); await send('Network.setBlockedURLs', { urls: ['http://*', 'https://*'] }); }
  await send('Page.navigate', { url });
  for (let t = 0; t < 600; t++) { if (await evaluate('document.readyState').catch(() => '') === 'complete') break; await new Promise(r => setTimeout(r, 100)); }
  const close = async () => { try { ws.close(); } catch {} const gone = new Promise(r => proc.once('exit', r)); proc.kill(); await gone;
    try { fs.rmSync(dir, { recursive: true, force: true, maxRetries: 5 }); } catch {} };
  return { send, evaluate, close };
}
