// STORE SHOTS SITE (T1076): the web site the store pictures are shot from, built and served HERE, never staging's.
//   import { resolveSite } from './store-shots/site.mjs';   const s = await resolveSite({ site, reuse, exportDir }); ...; await s.close();
// The Spots and Help scenes need a web export built with EXPO_PUBLIC_STORE_SHOTS=1 (`@app/ui-native` storeShots.ts), which the
// deployed sites never carry. So with no explicit site this builds one: `expo export --platform web` of THIS checkout into
// app/dist/web-store-shots (its own folder, never the deploy folders dist/web*), served from a static server on a free
// port this process opens and closes (its own server only).
// PINNED: the build's code (this checkout), EXPO_PUBLIC_STORE_SHOTS=1, EXPO_PUBLIC_CONTACT as export:web:staging, the app
// environment `staging` (EXPO_PUBLIC_APP_ENV=staging: the data and feeds hosts staging's own build reads, as export:web:staging
// does; the pictures are of the same app the staging site shows). LIVE: the data/feeds those hosts serve now (gauge readings,
// forecast, DFO salmon notices, the data release staging holds); the checks assert the content of each shot before capture.
// THE MARKER: the export carries `store-shots-build.json` ({ storeShots: true, ... }) at its root, written here after the
// build. Before any scene, `assertStoreShotsSite(url)` fetches <site>/store-shots-build.json and REFUSES (throws, exit 1 at the
// caller) when it is not that JSON: a deployed site answers 404 or its page (HTML), never the marker.
// REUSE: --reuse (or an existing export with its marker and --no-build) serves the existing export without rebuilding;
//   node app/tools/store-shots/site.mjs serve [dir]   serves it and prints the URL (for --site), until stopped (its own PID).
import fs from 'node:fs'; import path from 'node:path'; import http from 'node:http';
import { spawnSync } from 'node:child_process';

const HERE = path.dirname(new URL(import.meta.url).pathname), APP = path.resolve(HERE, '../..');
export const MARKER = 'store-shots-build.json';
export const DEFAULT_EXPORT = path.join(APP, 'dist/web-store-shots');

const INSTRUCTION = (site) => `store-shots: ${site} is not a store-shots web build (no ${MARKER} marker). Its Spots and Help scenes would fail two frames in. `
  + `Run the tool with no --site/STORE_SHOTS_SITE: it builds and serves its own export (EXPO_PUBLIC_STORE_SHOTS=1) into app/dist/web-store-shots; `
  + `or serve an existing one with "node app/tools/store-shots/site.mjs serve" and pass that URL as --site.`;

/** the marker's text -> its facts, or throws: pure, unit-tested */
export function parseMarker(text, site = 'the site') {
  let m; try { m = JSON.parse(text); } catch { throw new Error(INSTRUCTION(site)); }
  if (!m || m.storeShots !== true) throw new Error(INSTRUCTION(site));
  return m;
}
/** refuse, before any scene, a site that is not a store-shots build */
export async function assertStoreShotsSite(site, fetchFn = fetch) {
  let res; try { res = await fetchFn(`${site.replace(/\/$/, '')}/${MARKER}`); } catch (e) { throw new Error(`store-shots: cannot reach ${site}: ${e.message}`); }
  if (!res.ok) throw new Error(INSTRUCTION(site));
  return parseMarker(await res.text(), site);
}

/** what a site says about itself, for the report: its entry bundle (the build's hash), its Last-Modified, and the marker's commit if it has one.
 *  A deployed site shows the RELEASED app, not this checkout; this is how the report says which one a scene came from. */
export async function describeSite(site, fetchFn = fetch) {
  const base = site.replace(/\/$/, ''), out = { site: base, bundle: null, lastModified: null, storeShotsCommit: null };
  try { const r = await fetchFn(`${base}/`); out.lastModified = r.headers.get('last-modified'); const m = /_expo\/static\/js\/web\/([^"'<> ]+\.js)/.exec(await r.text()); out.bundle = m ? m[1] : null; } catch (e) { out.error = e.message; }
  try { const r = await fetchFn(`${base}/${MARKER}`); if (r.ok) out.storeShotsCommit = JSON.parse(await r.text()).commit ?? null; } catch { /* a deployed site has none */ }
  return out;
}

const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.json': 'application/json', '.css': 'text/css', '.png': 'image/png', '.svg': 'image/svg+xml',
  '.ttf': 'font/ttf', '.woff2': 'font/woff2', '.woff': 'font/woff', '.map': 'application/json', '.ico': 'image/x-icon', '.wasm': 'application/wasm', '.pmtiles': 'application/octet-stream' };
/** a static server for dir on a free port (127.0.0.1); `/` and unknown extension-less paths give index.html */
export function serveDir(dir, port = 0) {
  const root = path.resolve(dir);
  const srv = http.createServer((req, res) => {
    const u = decodeURIComponent(new URL(req.url, 'http://x').pathname); let f = path.join(root, u === '/' ? 'index.html' : u);
    if (!f.startsWith(root + path.sep) && f !== root) { res.writeHead(403).end(); return; }
    if (!fs.existsSync(f) || fs.statSync(f).isDirectory()) { if (path.extname(u)) { res.writeHead(404).end(); return; } f = path.join(root, 'index.html'); }
    res.writeHead(200, { 'content-type': TYPES[path.extname(f)] ?? 'application/octet-stream', 'cache-control': 'no-store' }); fs.createReadStream(f).pipe(res);
  });
  return new Promise((ok, no) => { srv.once('error', no); srv.listen(port, '127.0.0.1', () => ok({ url: `http://127.0.0.1:${srv.address().port}`, close: () => new Promise((r) => srv.close(() => r())) })); });
}

/** build the store-shots export of this checkout into dir (heavy: wrap the caller in scripts/heavy.sh) */
export function buildExport(dir = DEFAULT_EXPORT) {
  const env = { ...process.env, EXPO_PUBLIC_STORE_SHOTS: '1', EXPO_PUBLIC_CONTACT: 'support@canifishthis.ca', EXPO_PUBLIC_APP_ENV: 'staging' };
  const mobile = path.join(APP, 'apps/mobile');
  const run = (cmd, args) => { const r = spawnSync(cmd, args, { stdio: 'inherit', cwd: mobile, env }); if (r.status !== 0) throw new Error(`store-shots: ${cmd} ${args.join(' ')} failed (exit ${r.status}); no export, nothing shot`); };
  console.log(`store-shots: building the store-shots web export into ${dir} (a few minutes)`);
  fs.rmSync(dir, { recursive: true, force: true });
  run('npx', ['expo', 'export', '--clear', '--platform', 'web', '--output-dir', dir, '--source-maps']);
  run(process.execPath, [path.join(APP, 'tools/web-shell.mjs'), dir]);
  const sha = spawnSync('git', ['rev-parse', 'HEAD'], { cwd: APP, encoding: 'utf8' }).stdout.trim();
  fs.writeFileSync(path.join(dir, MARKER), JSON.stringify({ storeShots: true, EXPO_PUBLIC_STORE_SHOTS: '1', appEnv: 'staging', commit: sha, builtAt: new Date().toISOString() }, null, 2) + '\n');
}

/** { url, close, built }: an explicit site is checked and used; otherwise the export (rebuilt unless `reuse` and one is there) is served here */
export async function resolveSite({ site = null, reuse = false, exportDir = DEFAULT_EXPORT } = {}) {
  if (site) { await assertStoreShotsSite(site); return { url: site.replace(/\/$/, ''), close: async () => {}, built: false }; }
  const have = fs.existsSync(path.join(exportDir, MARKER));
  if (reuse && !have) throw new Error(`store-shots: --reuse but ${exportDir} has no store-shots export (${MARKER}); run once without --reuse`);
  if (!reuse) buildExport(exportDir);
  const s = await serveDir(exportDir);
  try { await assertStoreShotsSite(s.url); } catch (e) { await s.close(); throw e; }
  console.log(`store-shots: serving ${exportDir} at ${s.url}`);
  return { ...s, built: !reuse };
}

if (process.argv[1] && path.resolve(process.argv[1]) === new URL(import.meta.url).pathname && process.argv[2] === 'serve') {
  const dir = path.resolve(process.argv[3] ?? DEFAULT_EXPORT);
  if (!fs.existsSync(path.join(dir, MARKER))) { console.error(`store-shots/site: ${dir} has no ${MARKER}; not a store-shots export`); process.exit(1); }
  const s = await serveDir(dir); console.log(`${s.url}   (serving ${dir}; stop with Ctrl-C or kill this PID ${process.pid})`);
  process.on('SIGINT', async () => { await s.close(); process.exit(0); }); process.on('SIGTERM', async () => { await s.close(); process.exit(0); });
}
