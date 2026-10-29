// T241 — THE STAGING SMOKE TEST: after every staging deploy, is https://staging.canifishthis.ca whole? HTTP checks
// (feeds status + freshness, manifest, synopsis, a /water/ page, a legacy /waterbody/ 301, private/ not served) and a
// headless Chrome (regs-web/cdp.mjs — the repo's browser driver, no npm dependency) on the live app: the map paints
// (phone + desktop), the attribution names OpenStreetMap, the Salmon and Run timing modes repaint it, a tap opens a
// named water's regulations, a tap opens an unnamed stream, Conditions shows a gauge reading, the rule book PDF opens.
//
//   node app/tools/smoke-staging.mjs [--url https://staging.canifishthis.ca] [--shots <dir>] [--slug vedder-river]
//                                    [--only http,browser,desktop]
//   → prints one PASS/WARN/FAIL line per check; exit 1 on any FAIL, with the failing step's screenshot path.
//   Screenshots of every browser step land in --shots (default: a fresh temp dir; the path is printed).
//
// STAGING ONLY: every host is a staging one and the script refuses any other (a check against prod is not this tool).
// `--slug nope` is the deliberate break (the /water/ page check then FAILs). Runs at the end of deploy/staging/site.sh
// and upload.sh (SMOKE=0 skips), and by hand in GitHub Actions (.github/workflows/staging-smoke.yml).
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { open } from './regs-web/cdp.mjs';

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; };
const SITE = opt('--url', 'https://staging.canifishthis.ca').replace(/\/$/, '');
const DATA = opt('--data', 'https://data-staging.canifishthis.ca').replace(/\/$/, '');
const FEEDS = opt('--feeds', 'https://bc-fishing-feeds-staging.horvath-dawson.workers.dev').replace(/\/$/, '');
const SLUG = opt('--slug', 'vedder-river');
const ONLY = opt('--only', 'http,browser,desktop').split(',');
const SHOTS = opt('--shots', null) ?? fs.mkdtempSync(path.join(os.tmpdir(), 'smoke-staging-'));
fs.mkdirSync(SHOTS, { recursive: true });
for (const u of [SITE, DATA, FEEDS])
  if (!/^https:\/\/([a-z0-9-]+\.)*(staging\.canifishthis\.ca|data-staging\.canifishthis\.ca|[a-z0-9-]+-staging\.[a-z0-9-]+\.workers\.dev)$/.test(u)
      && !/^http:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/.test(u)) {
    console.error(`smoke-staging: ${u} is not a staging host — refusing`); process.exit(2);
  }

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const results = [];
const say = (level, name, detail = '') => { results.push({ level, name, detail }); console.log(`${level.padEnd(4)} ${name}${detail ? ` — ${detail}` : ''}`); };
async function check(name, fn) {
  const t0 = Date.now();
  try { const r = await fn(); if (r && r.warn) say('WARN', name, r.warn); else say('PASS', name, `${r ?? ''}${r ? ' · ' : ''}${((Date.now() - t0) / 1000).toFixed(1)} s`); }
  catch (e) { say('FAIL', name, e.message); }
}
const must = (cond, msg) => { if (!cond) throw new Error(msg); };
const get = (u, init = {}) => fetch(u, { redirect: 'manual', headers: { 'User-Agent': 'canifishthis-smoke/1 (staging)' }, ...init });
const json = async (u) => { const r = await get(u); must(r.ok, `${u} → HTTP ${r.status}`); return r.json(); };
const ageH = (iso) => (Date.now() - Date.parse(iso)) / 3.6e6;

// ---------------------------------------------------------------- HTTP
async function http() {
  let manifest = null;
  await check('feeds /status: every feed ok', async () => {
    const st = await json(`${FEEDS}/status.json`);
    const feeds = Object.entries(st.feeds ?? {});
    must(feeds.length >= 3, `only ${feeds.length} feeds in /status.json`);
    const bad = feeds.filter(([, f]) => !['ok', 'degraded'].includes(f.state)).map(([k, f]) => `${k}=${f.state}${f.lastError ? ` (${String(f.lastError).slice(0, 80)})` : ''}`);
    must(!bad.length, bad.join('; '));
    const deg = feeds.filter(([, f]) => f.state === 'degraded').map(([k]) => k);
    return deg.length ? { warn: `degraded: ${deg.join(', ')} (publishing, some parts held — see ${FEEDS}/status)` } : feeds.map(([k]) => k).join(' ');
  });
  await check('daily R2 backup ran (< 30 h, ok)', async () => {
    const bk = (await json(`${FEEDS}/status.json`)).backup;   // T246: the feeds worker's last backup run
    if (!bk) return { warn: 'no backup run recorded yet (POST /run/backup, or wait for the 09:25 UTC cron)' };
    must(bk.ok, `last backup ${bk.at} failed: ${String(bk.error).slice(0, 120)}`);
    must(ageH(bk.at) < 30, `last backup ${bk.at} (${ageH(bk.at).toFixed(0)} h ago)`);
    return `${bk.day}: ${bk.copied} objects, ${(bk.bytes / 1e6).toFixed(1)} MB`;
  });
  await check('feeds fresh: gauges < 3 h, in-season < 30 h', async () => {
    const live = await json(`${DATA}/feeds/live/index.json`);
    must(live.fetchedAt && ageH(live.fetchedAt) < 3, `feeds/live/index.json fetchedAt ${live.fetchedAt} (${ageH(live.fetchedAt).toFixed(1)} h old)`);
    must(Object.keys(live.stations ?? {}).length > 100, `feeds/live/index.json has ${Object.keys(live.stations ?? {}).length} stations`);
    const ins = await json(`${DATA}/feeds/inseason/index.json`);
    must(ins.fetchedAt && ageH(ins.fetchedAt) < 30, `feeds/inseason/index.json fetchedAt ${ins.fetchedAt}`);
    const dfo = await json(`${DATA}/feeds/dfo/index.json`);
    must(Object.keys(dfo.locs ?? {}).length > 0, 'feeds/dfo/index.json has no locations');
    return `gauges ${ageH(live.fetchedAt).toFixed(1)} h, in-season ${ageH(ins.fetchedAt).toFixed(1)} h, dfo ${Object.keys(dfo.locs).length} locs`;
  });
  await check('release manifest + synopsis PDF', async () => {
    manifest = await json(`${DATA}/release/manifest.json`);
    must(manifest.data_release, 'manifest has no data_release');
    const syn = `${DATA}/release/${manifest.synopsis.url}`;
    const r = await get(syn, { headers: { Range: 'bytes=0-4' } });
    must(r.status === 206 || r.status === 200, `${syn} → HTTP ${r.status}`);
    const head = Buffer.from(await r.arrayBuffer()).subarray(0, 5).toString();
    must(head === '%PDF-', `${syn} does not start with %PDF- (${JSON.stringify(head)})`);
    return `${manifest.data_release}, ${manifest.synopsis.url}`;
  });
  await check('run-timing index matches the release tiles', async () => {
    const rt = await json(`${DATA}/runtiming/index.json`);
    const tiles = manifest?.tiles?.current?.id;
    must(!tiles || rt.tile_version === tiles, `runtiming tile_version ${rt.tile_version} ≠ manifest tiles ${tiles}`);
    return rt.tile_version;
  });
  await check(`/water/${SLUG}: 200 + canonical + noindex`, async () => {
    const r = await get(`${SITE}/water/${SLUG}`);
    must(r.status === 200, `HTTP ${r.status}`);
    const html = await r.text();
    must(html.includes(`rel="canonical" href="https://canifishthis.ca/water/${SLUG}`), 'no canonical to https://canifishthis.ca/water/<slug>');
    must(/noindex/.test(r.headers.get('x-robots-tag') ?? ''), 'staging page without X-Robots-Tag: noindex');
    return `${html.length} B`;
  });
  await check('legacy /waterbody/328939712/ → one 301 → 200', async () => {
    const r = await get(`${SITE}/waterbody/328939712/`);
    must(r.status === 301, `HTTP ${r.status} (want 301)`);
    const to = new URL(r.headers.get('location'), SITE).href;
    must(to.endsWith('/water/nehounlee-marsh'), `→ ${to}`);
    const r2 = await get(to);
    must(r2.status === 200, `${to} → HTTP ${r2.status} (want 200, one hop)`);
    return to.replace(SITE, '');
  });
  await check('private/ is not served by the data worker', async () => {
    for (const k of ['private/feeds/status.json', 'private/feeds/notify/state.json', 'private/', 'private%2Ffeeds%2Fstatus.json']) {
      const r = await get(`${DATA}/${k}`);
      must(r.status === 404 || r.status === 403, `${DATA}/${k} → HTTP ${r.status}`);
    }
  });
}

// ---------------------------------------------------------------- browser
const NO_POPUP = `(() => { window.__pdf = null; window.open = (u) => { if (u) window.__pdf = String(u);
  return { document: { body: {} }, close() {}, location: { set href(v) { window.__pdf = String(v); } } }; }; })();`;
const tidQ = (id) => `document.querySelector('[data-testid=${JSON.stringify(id)}]')`;
async function until(b, expr, ms, what) {
  for (let t = 0; t < ms; t += 200) { if (await b.evaluate(expr).catch(() => false)) return; await sleep(200); }
  throw new Error(`timed out (${ms / 1000} s) waiting for ${what}`);
}
const press = (b, label, { prefix = false } = {}) => b.evaluate(`(() => { const e = [...document.querySelectorAll('[aria-label]')].find((e) => {
  const l = e.getAttribute('aria-label') || ''; return (${prefix ? `l.startsWith(${JSON.stringify(label)})` : `l === ${JSON.stringify(label)}`}) && e.getBoundingClientRect().width > 0; });
  if (!e) return false; e.click(); return true; })()`);
async function tapAt(b, x, y) { for (const type of ['mouseMoved', 'mousePressed', 'mouseReleased']) await b.send('Input.dispatchMouseEvent', { type, x, y, button: 'left', clickCount: 1 }); }
/** the first-run notice ("I understand") and the in-season sheet ("Done") cover the map on a fresh profile */
async function dismiss(b) {
  for (let i = 0; i < 6; i++) {
    const n = (await press(b, 'I understand') ? 1 : 0) + (await press(b, 'Done') ? 1 : 0);
    if (!n && i > 1) return; await sleep(700);
  }
}
async function shot(b, name) {
  const { data } = await b.send('Page.captureScreenshot', { format: 'png' });
  const file = path.join(SHOTS, `${name}.png`);
  fs.writeFileSync(file, Buffer.from(data, 'base64'));
  return { file, data };
}
/** decode two screenshots IN THE PAGE (a canvas: no PNG library): distinct colours of `a` and, with `b`, the share of
 *  pixels that differ (sampled every 4th pixel each way) */
const pixels = (b, a, other = null) => b.evaluate(`(async () => {
  const load = (d) => new Promise((res, rej) => { const i = new Image(); i.onload = () => res(i); i.onerror = rej; i.src = 'data:image/png;base64,' + d; });
  const px = async (d) => { const i = await load(d); const c = document.createElement('canvas'); c.width = i.width; c.height = i.height;
    const g = c.getContext('2d'); g.drawImage(i, 0, 0); return g.getImageData(0, 0, i.width, i.height); };
  const A = await px(${JSON.stringify(a)}); const B = ${other ? `await px(${JSON.stringify(other)})` : 'null'};
  const cols = new Set(); let diff = 0, n = 0;
  for (let y = 0; y < A.height; y += 4) for (let x = 0; x < A.width; x += 4) { const o = (y * A.width + x) * 4; n++;
    cols.add((A.data[o] >> 3) << 10 | (A.data[o + 1] >> 3) << 5 | (A.data[o + 2] >> 3));
    if (B && Math.abs(A.data[o] - B.data[o]) + Math.abs(A.data[o + 1] - B.data[o + 1]) + Math.abs(A.data[o + 2] - B.data[o + 2]) > 24) diff++; }
  return { colours: cols.size, diff: B ? diff / n : null };
})()`);
/** the map's canvas, cropped from a screenshot (the whole viewport: the map is full-bleed behind the chrome) */
async function mapPaints(b, name) {
  await until(b, `!!${tidQ('map-day')}`, 120000, 'the map (map-day)');
  await dismiss(b);
  await until(b, `!!document.querySelector('canvas.maplibregl-canvas, canvas')`, 30000, 'the map canvas');
  await sleep(5000);  // tiles + style settle
  const s = await shot(b, name);
  const p = await pixels(b, s.data);
  must(p.colours >= 150, `the map looks blank: ${p.colours} distinct colours (shot ${s.file})`);
  const reqs = b.requests.filter((r) => /^https?:/.test(r.url));
  const tiles = reqs.filter((r) => /\.pmtiles(\?|$)/.test(r.url));
  must(tiles.length > 0, 'no .pmtiles request');
  const badTiles = tiles.filter((r) => r.failed || (r.status && ![200, 206, 304].includes(r.status)));
  must(!badTiles.length, `tile requests failed: ${badTiles.slice(0, 3).map((r) => `${r.url} ${r.status ?? r.failed}`).join(', ')}`);
  const bad = reqs.filter((r) => (r.status >= 400 || r.failed) && !/favicon\.ico$/.test(r.url) && !/cdn-cgi|cloudflareinsights/.test(r.url));
  must(!bad.length, `failed requests: ${bad.slice(0, 4).map((r) => `${r.url} ${r.status ?? r.failed}`).join(', ')}`);
  return { shot: s, detail: `${p.colours} colours, ${tiles.length} tile requests, ${reqs.length} requests` };
}
const titleOf = (b) => b.evaluate(`${tidQ('water-title')}?.innerText ?? ''`);
async function navigate(b, url) {
  await b.send('Page.navigate', { url });
  await sleep(500);
  await until(b, `document.readyState === 'complete'`, 60000, `${url} to load`);
}

async function browser() {
  const b = await open(`${SITE}/`, { width: 390, height: 844, dpr: 1, mobile: true, network: true, init: NO_POPUP });
  let last = 'phone-0';
  const step = async (name, fn) => check(name, async () => {
    try { return await fn(); }
    catch (e) { const s = await shot(b, `FAIL-${last}`).catch(() => null); throw new Error(`${e.message}${s ? ` [shot ${s.file}]` : ''}`); }
  });
  try {
    let standard = null;
    await step('phone: the map paints', async () => { last = '01-map'; const r = await mapPaints(b, '01-map'); standard = r.shot; return r.detail; });
    await step('phone: attribution names OpenStreetMap', async () => {
      const t = await b.evaluate(`[...document.querySelectorAll('.maplibregl-ctrl-attrib, [class*=attrib]')].map((e) => e.textContent).join(' ')`);
      must(/OpenStreetMap/.test(t), `attribution text: ${JSON.stringify(t.slice(0, 160))}`);
    });
    for (const [label, id, file, re] of [['Salmon', 'regs-paint-salmon', '02-salmon', /salmon/i], ['Run timing', 'regs-paint-runs', '03-run-timing', /\/runtiming\/index\.json/]]) {
      await step(`phone: ${label} mode paints`, async () => {
        last = file;
        must(standard, 'no standard map to compare against');
        const before = b.requests.length;
        must(await b.evaluate(`(() => { const e = ${tidQ(id)}; if (!e) return false; e.click(); return true; })()`), `no ${id} control`);
        await until(b, `(() => { const e = ${tidQ(id)}; return e && (e.getAttribute('aria-selected') === 'true' || e.getAttribute('aria-checked') === 'true' || e.getAttribute('aria-pressed') === 'true'); })()`, 15000, `${label} selected`).catch(() => {});
        await sleep(5000);
        const read = b.requests.slice(before).filter((r) => re.test(r.url));
        const bad = b.requests.slice(before).filter((r) => r.status >= 400 || r.failed);
        must(!bad.length, `failed requests: ${bad.slice(0, 3).map((r) => `${r.url} ${r.status ?? r.failed}`).join(', ')}`);
        const s = await shot(b, file);
        const p = await pixels(b, s.data, standard.data);
        must(p.colours >= 150, `the ${label} map looks blank (${p.colours} colours)`);
        must(p.diff > 0.003, `the ${label} map is the Standard map (${(p.diff * 100).toFixed(2)}% pixels differ)`);
        return `${(p.diff * 100).toFixed(1)}% pixels changed, ${read.length} ${label.toLowerCase()} reads`;
      });
    }
    await b.evaluate(`${tidQ('regs-paint-standard')}?.click()`); await sleep(2500);

    await step('phone: tap a named water → its regulations', async () => {
      last = '04-tap-named';
      await tapAt(b, 102, 260);
      await until(b, `!!${tidQ('water-title')}?.innerText.trim()`, 60000, 'a water to open from the tap at 102,260');
      const t = await titleOf(b);
      must(t.startsWith('Gravel Slough'), `the tap opened "${t.split('\n')[0]}", not Gravel Slough`);
      await until(b, `!!${tidQ('regs-header')}`, 60000, 'the Regulations of Gravel Slough');
      await shot(b, '04-tap-named');
      return t.split('\n')[0];
    });
    await b.evaluate(`${tidQ('water-close')}?.click()`); await sleep(2000);
    await step('phone: tap an unnamed stream → its regulations', async () => {
      last = '05-tap-unnamed';
      await until(b, `!${tidQ('water-title')}`, 10000, 'the water to close');
      await tapAt(b, 319, 602);
      await until(b, `!!${tidQ('water-title')}?.innerText.trim()`, 60000, 'a water to open from the tap at 319,602');
      const t = await titleOf(b);
      must(t.startsWith('Unnamed stream · Region 2'), `the tap opened "${t.split('\n')[0]}", not an unnamed stream`);
      await until(b, `!!${tidQ('regs-header')}`, 60000, 'the unnamed stream\'s Regulations');
      await shot(b, '05-tap-unnamed');
      return t.split('\n')[0];
    });
    await b.evaluate(`${tidQ('water-close')}?.click()`); await sleep(1500);
    await step('phone: Conditions tab', async () => {
      last = '06-conditions-map';
      must(await press(b, 'Conditions'), 'no Conditions tab');
      await until(b, `!!${tidQ('map-horizons')}`, 30000, 'the Conditions map (map-horizons)');
      await sleep(3000);
      await shot(b, '06-conditions-map');
    });
    await step('phone: Pitt River Conditions shows a gauge reading', async () => {
      last = '07-conditions-pitt';
      await navigate(b, `${SITE}/?regs=gnis:7551&view=conditions`);
      await until(b, `/observed \\d{4}-\\d{2}-\\d{2} at/.test(document.body.innerText)`, 90000, 'a gauge reading ("observed <date> at")');
      await dismiss(b);
      const txt = await b.evaluate('document.body.innerText');
      must(/m³\/s|\bm stage\b/.test(txt), 'no flow (m³/s) or stage value on the page');
      const obs = /observed (\d{4}-\d{2}-\d{2}) at ([\d:]+)/.exec(txt);
      await shot(b, '07-conditions-pitt');
      return obs ? `observed ${obs[1]} ${obs[2]}` : '';
    });
    await step('phone: rule book link opens the synopsis PDF', async () => {
      last = '08-pdf';
      await navigate(b, `${SITE}/?regs=gnis:3062`);
      await until(b, `!!${tidQ('regs-header')}`, 90000, 'the Vedder River regulations');
      await dismiss(b);
      const before = b.requests.length;
      await until(b, `[...document.querySelectorAll('[aria-label^="Rule book page"]')].some((e) => e.getBoundingClientRect().width > 0)`, 20000, 'a "Rule book page N" link');
      must(await press(b, 'Rule book page', { prefix: true }), 'no "Rule book page" link');
      await until(b, `typeof window.__pdf === 'string' && window.__pdf.startsWith('blob:') && window.__pdf.includes('#page=')`, 60000, 'the PDF to open (a blob URL at #page=N)');
      const pdf = b.requests.slice(before).find((r) => /synopsis-[0-9a-f]+\.pdf/.test(r.url));
      must(!pdf || pdf.status === 200 || pdf.status === 304 || pdf.status === null, `synopsis request ${pdf?.url} → ${pdf?.status}`);
      await shot(b, '08-pdf');
      return (await b.evaluate('window.__pdf')).replace(/^blob:[^#]+/, 'blob:…');
    });
    const ex = b.errors.filter((e) => !/favicon/.test(e));
    if (ex.length) say('WARN', 'phone: console errors', ex.slice(0, 3).join(' | ').slice(0, 300));
  } finally { await b.close(); }
}

async function desktop() {
  const b = await open(`${SITE}/`, { width: 1440, height: 900, dpr: 1, network: true });
  try {
    await check('desktop: the map paints', async () => {
      try { return (await mapPaints(b, '09-desktop-map')).detail; }
      catch (e) { const s = await shot(b, 'FAIL-09-desktop-map').catch(() => null); throw new Error(`${e.message}${s ? ` [shot ${s.file}]` : ''}`); }
    });
  } finally { await b.close(); }
}

const t0 = Date.now();
console.log(`smoke: ${SITE} (data ${DATA}, feeds ${FEEDS}); shots → ${SHOTS}`);
if (ONLY.includes('http')) await http();
if (ONLY.includes('browser')) await browser().catch((e) => say('FAIL', 'phone session', e.message));
if (ONLY.includes('desktop')) await desktop().catch((e) => say('FAIL', 'desktop session', e.message));
const fails = results.filter((r) => r.level === 'FAIL');
console.log(`\n${fails.length ? 'SMOKE FAILED' : 'smoke ok'}: ${results.filter((r) => r.level === 'PASS').length} pass, ` +
  `${results.filter((r) => r.level === 'WARN').length} warn, ${fails.length} fail in ${((Date.now() - t0) / 1000).toFixed(0)} s; shots ${SHOTS}`);
for (const f of fails) console.log(`  ✗ ${f.name}: ${f.detail}`);
process.exit(fails.length ? 1 : 0);
