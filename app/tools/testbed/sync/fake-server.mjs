#!/usr/bin/env node
// (T255) A FAKE ACCOUNT SERVER for the test bed — the shape T223's spot sync will talk to, so its scenarios can be
// written before the real one exists. In memory, one process, no auth beyond a fake bearer token. The emulator reaches
// the Mac at 10.0.2.2:<port>.
//
//   node app/tools/testbed/sync/fake-server.mjs [--port 39255]
//
// API (v0, a proposal for T223 — change it there and here together):
//   POST /v1/session                  {email}            → {token}                    (any email; no password)
//   GET  /v1/spots?since=<rev>                           → {rev, spots:[…], deleted:[ids]}
//   PUT  /v1/spots/<id>   If-Match: <rev of that spot>   → 200 {rev} | 409 {spot}      (last write wins is NOT assumed)
//   DELETE /v1/spots/<id>                                → 200 {rev}
//   PUT  /v1/photos/<spotId>/<photoId>  (bytes)          → 200 {bytes, sha256}
// Test levers (the test bed only):
//   POST /__testbed/down {on:true|false}   every /v1 call answers 503 (a server outage, the device still online)
//   GET  /__testbed/state                  everything held (spots, photos' sizes, the request log)
//   POST /__testbed/reset
import http from 'node:http';
import crypto from 'node:crypto';

export function createFakeServer() {
  let rev = 0, down = false;
  const spots = new Map(), photos = new Map(), deleted = new Set(), log = [];
  const send = (res, code, body) => { res.writeHead(code, { 'content-type': 'application/json' }); res.end(JSON.stringify(body)); };
  const read = (req) => new Promise((ok) => { const b = []; req.on('data', (c) => b.push(c)); req.on('end', () => ok(Buffer.concat(b))); });
  return http.createServer(async (req, res) => {
    const u = new URL(req.url, 'http://x');
    const body = await read(req);
    log.push({ at: new Date().toISOString(), method: req.method, path: u.pathname, bytes: body.length });
    if (u.pathname === '/__testbed/state') return send(res, 200, { rev, down, spots: [...spots.values()], deleted: [...deleted],
      photos: [...photos].map(([k, v]) => ({ k, bytes: v.length })), log: log.slice(-200) });
    if (u.pathname === '/__testbed/down') { down = !!JSON.parse(body.toString() || '{}').on; return send(res, 200, { down }); }
    if (u.pathname === '/__testbed/reset') { rev = 0; spots.clear(); photos.clear(); deleted.clear(); log.length = 0; return send(res, 200, { ok: true }); }
    if (down) return send(res, 503, { error: 'down (test bed)' });
    if (u.pathname === '/v1/session' && req.method === 'POST') return send(res, 200, { token: crypto.randomUUID() });
    if (!/^Bearer \S+/.test(req.headers.authorization ?? '')) return send(res, 401, { error: 'no session' });
    const m = u.pathname.match(/^\/v1\/spots(?:\/([\w-]+))?$/);
    if (m && req.method === 'GET' && !m[1]) {
      const since = Number(u.searchParams.get('since') ?? 0);
      return send(res, 200, { rev, spots: [...spots.values()].filter((s) => s.rev > since), deleted: [...deleted] });
    }
    if (m && m[1] && req.method === 'PUT') {
      const have = spots.get(m[1]);
      const ifMatch = req.headers['if-match'];
      if (have && ifMatch !== undefined && Number(ifMatch) !== have.rev) return send(res, 409, { spot: have });
      const spot = { ...JSON.parse(body.toString()), id: m[1], rev: ++rev };
      spots.set(m[1], spot); deleted.delete(m[1]);
      return send(res, 200, { rev: spot.rev });
    }
    if (m && m[1] && req.method === 'DELETE') { spots.delete(m[1]); deleted.add(m[1]); return send(res, 200, { rev: ++rev }); }
    const p = u.pathname.match(/^\/v1\/photos\/([\w-]+)\/([\w.-]+)$/);
    if (p && req.method === 'PUT') {
      photos.set(`${p[1]}/${p[2]}`, body);
      return send(res, 200, { bytes: body.length, sha256: crypto.createHash('sha256').update(body).digest('hex') });
    }
    send(res, 404, { error: 'no route' });
  });
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const i = process.argv.indexOf('--port');
  const port = i > 0 ? Number(process.argv[i + 1]) : 39255;
  createFakeServer().listen(port, () => console.log(`[fake-server] http://localhost:${port} (emulator: http://10.0.2.2:${port})`));
}
