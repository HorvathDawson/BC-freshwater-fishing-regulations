#!/usr/bin/env node
// (T242) A LOCAL CRASH WORKER for the test bed: the feeds worker's OWN `POST /crash` handler (workers/feeds/src/crashes.ts,
// loaded as TypeScript by Node's type stripping) over an in-memory bucket, on this machine. The test-bed APK built with
// `session.sh --crash-stub` sends its crash reports here (`adb reverse` makes it the phone's localhost), so a forced
// error is proved to pass the real worker's validation and fold into a stored crash kind — and nothing is written to
// staging or production.
//
//   node app/tools/testbed/crash-stub/server.mjs [--port 39256] [--log <file.jsonl>]
//
// Each POST is appended to the log as {at, status, answer, body}; GET /__stub/kinds lists the stored kinds (the bucket).
import http from 'node:http';
import fs from 'node:fs';
import { register } from 'node:module';

// the worker's sources import each other without an extension (bundler style); Node needs one: a relative import that
// does not resolve as written is tried again with `.ts`
const HOOK = `export async function resolve(spec, ctx, next) {
  try { return await next(spec, ctx); }
  catch (e) { if (/^\\.\\.?\\//.test(spec) && !/\\.[cm]?[jt]s$/.test(spec)) return next(spec + '.ts', ctx); throw e; }
}`;
register(`data:text/javascript,${encodeURIComponent(HOOK)}`);
const { handleCrash } = await import('../../../workers/feeds/src/crashes.ts');

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; };
const PORT = Number(opt('--port', 39256));
const LOG = opt('--log', null);

class MemBucket {
  m = new Map();
  async get(k) { const v = this.m.get(k); return v === undefined ? null : { text: async () => v }; }
  async put(k, b) { this.m.set(k, b); }
  async delete(k) { this.m.delete(k); }
  async list(o) { return { objects: [...this.m.keys()].filter((k) => k.startsWith(o.prefix)).sort().map((key) => ({ key })), truncated: false }; }
}
// FEEDS_ENV staging: a `test: true` payload lands under crashes-test/, as on the staging worker
const env = { BUCKET: new MemBucket(), REPORT_SALT: 'testbed-stub', FEEDS_ENV: 'staging' };

http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  if (req.method === 'GET' && url.pathname === '/__stub/kinds') {
    const kinds = [...env.BUCKET.m.entries()].filter(([k]) => /crashes(-test)?\/[0-9a-f]+\.json$/.test(k)).map(([key, v]) => ({ key, ...JSON.parse(v) }));
    res.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify(kinds));
    return;
  }
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const body = Buffer.concat(chunks).toString('utf8');
  // the per-IP gate keys on CF-Connecting-IP: every report from the emulator is one "IP", as from one phone
  const r = await handleCrash(new Request(`http://localhost:${PORT}${url.pathname}`, { method: req.method, body: req.method === 'POST' ? body : undefined,
    headers: { 'Content-Type': req.headers['content-type'] ?? 'application/json', 'CF-Connecting-IP': '10.0.2.15' } }), env);
  const answer = await r.text();
  const line = { at: new Date().toISOString(), status: r.status, answer, body: (() => { try { return JSON.parse(body); } catch { return body; } })() };
  if (LOG) fs.appendFileSync(LOG, `${JSON.stringify(line)}\n`);
  console.log(`[crash-stub] ${req.method} ${url.pathname} → ${r.status} ${answer}`);
  res.writeHead(r.status, { 'Content-Type': 'application/json' }).end(answer);
}).listen(PORT, () => console.log(`[crash-stub] listening on :${PORT}`));
