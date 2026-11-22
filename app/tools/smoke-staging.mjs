// T241 — THE STAGING SMOKE TEST: after every staging deploy, is https://staging.canifishthis.ca whole? HTTP checks
// (feeds status + freshness, manifest, synopsis, a /water/ page, a legacy /waterbody/ 301, private/ not served) and a
// headless Chrome (regs-web/cdp.mjs — the repo's browser driver, no npm dependency) on the live app: the map paints
// (phone + desktop), the attribution names OpenStreetMap, the Salmon and Run timing modes repaint it, a tap opens a
// named water's regulations, a tap opens an unnamed stream, Conditions shows a gauge reading, the rule book PDF opens; (T737) `--only firsttap` (opt-in, not in the default run: it is a timing
// measure): on a fresh profile the Fraser's Region 2 part is tapped 3 s after paint and again after a pan to Lillooet;
// each is WARNed over 4 s to readable and FAILs over 10 s (the complete time is logged).
//
//   node app/tools/smoke-staging.mjs [--url https://staging.canifishthis.ca] [--shots <dir>] [--slug vedder-river]
//                                    [--only http,browser,desktop,firsttap]   (firsttap is OPT-IN: slow networks, ~40 s)
//   → prints one PASS/WARN/FAIL line per check; exit 1 on any FAIL, with the failing step's screenshot path.
//   Screenshots of every browser step land in --shots (default: a fresh temp dir; the path is printed).
//
// STAGING by default: every host is a staging one and the script refuses any other. (T404) `--production` checks
// production instead (canifishthis.ca, data.canifishthis.ca, the bc-fishing-feeds worker) — read-only GETs, run by the
// OWNER after a prod step (handoff/PLAN/release/PRODUCTION.md); there a page must NOT say noindex.
// `--slug nope` is the deliberate break (the /water/ page check then FAILs). Runs at the end of deploy/staging/site.sh
// and upload.sh (SMOKE=0 skips), and by hand (the GitHub workflow was deleted 2026-11-21, owner).
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { open } from './regs-web/cdp.mjs';
import { project, TARGETS, HOME } from './map-tap.mjs';
import { waitForRuleBook } from './smoke-rulebook.mjs';

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; };
const PROD = args.includes('--production');
const SITE = opt('--url', PROD ? 'https://canifishthis.ca' : 'https://staging.canifishthis.ca').replace(/\/$/, '');
const DATA = opt('--data', PROD ? 'https://data.canifishthis.ca' : 'https://data-staging.canifishthis.ca').replace(/\/$/, '');
const FEEDS = opt('--feeds', PROD ? 'https://feeds.canifishthis.ca'
                                  : 'https://bc-fishing-feeds-staging.horvath-dawson.workers.dev').replace(/\/$/, '');
const SLUG = opt('--slug', 'vedder-river');
const ONLY = opt('--only', 'http,browser,desktop').split(',');
const SHOTS = opt('--shots', null) ?? fs.mkdtempSync(path.join(os.tmpdir(), 'smoke-staging-'));
fs.mkdirSync(SHOTS, { recursive: true });
const PROD_HOST = /^https:\/\/(canifishthis\.ca|data\.canifishthis\.ca|feeds\.canifishthis\.ca|bc-fishing-feeds\.horvath-dawson\.workers\.dev)$/;   // T404: the prod feeds custom domain (its default)
for (const u of [SITE, DATA, FEEDS])
  if (PROD) { if (!PROD_HOST.test(u)) { console.error(`smoke-staging --production: ${u} is not a production host — refusing`); process.exit(2); } }
  else if (!/^https:\/\/([a-z0-9-]+\.)*(staging\.canifishthis\.ca|data-staging\.canifishthis\.ca|[a-z0-9-]+-staging\.[a-z0-9-]+\.workers\.dev)$/.test(u)
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
  // (T544) the site names the release it was built on (deploy/staging/site.sh writes /site-release.json). A different
  // release is a warning while the min app version is the same, a failure when it moved (the site's app could refuse
  // the data). Both ids are printed. Production's site has no stamp (deploy/prod/site.sh): there the check is skipped.
  if (!PROD) await check('site release matches the manifest release', async () => {
    const st = await json(`${SITE}/site-release.json`);
    must(st.release, `${SITE}/site-release.json names no release`);
    // (2026-11-14) and the commit it was built from (site-all.sh refuses a dirty app/ unless SITE_DIRTY_OK): dirty warns
    const built = st.commit ? `built from ${st.commit.slice(0, 9)}${st.dirty ? ' + UNCOMMITTED app/ edits' : st.dirty === false ? ' (clean)' : ''}` : 'no commit stamp';
    const said = `site ${st.release} (min app ${st.minAppVersion ?? 'none'}), manifest ${manifest.data_release} (min app ${manifest.minAppVersion ?? 'none'}); ${built}`;
    if (st.release === manifest.data_release) return st.dirty ? { warn: `the site was built from uncommitted code: ${said}` } : said;
    must((st.minAppVersion ?? null) === (manifest.minAppVersion ?? null), `the site is on another release and the min app version moved: ${said}`);
    return { warn: `the site is on another release, same min app version: ${said}` };
  });
  await check('run-timing index matches the release tiles', async () => {
    const rt = await json(`${DATA}/runtiming/index.json`);
    const tiles = manifest?.tiles?.current?.id;
    must(!tiles || rt.tile_version === tiles, `runtiming tile_version ${rt.tile_version} ≠ manifest tiles ${tiles}`);
    return rt.tile_version;
  });
  await check(`/water/${SLUG}: 200 + canonical + ${PROD ? 'indexable' : 'noindex'}`, async () => {
    const r = await get(`${SITE}/water/${SLUG}`);
    must(r.status === 200, `HTTP ${r.status}`);
    const html = await r.text();
    must(html.includes(`rel="canonical" href="https://canifishthis.ca/water/${SLUG}`), 'no canonical to https://canifishthis.ca/water/<slug>');
    if (PROD) must(!r.headers.get('x-robots-tag'), `production page sends X-Robots-Tag: ${r.headers.get('x-robots-tag')}`);
    else must(/noindex/.test(r.headers.get('x-robots-tag') ?? ''), 'staging page without X-Robots-Tag: noindex');
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
/** a fresh profile gets the "Before you fish" notice (T236) and the coach-mark tour (T257/T248): seed both as seen, from
 *  the versions in the source (the way a11y-walk.mjs does), and the in-season sheet as seen */
const rd = (f, re) => re.exec(fs.readFileSync(path.join(path.dirname(new URL(import.meta.url).pathname), '..', f), 'utf8'))?.[1];
const SEEN = `try { localStorage.setItem('canifishthis.firstRun.v1', ${JSON.stringify(rd('packages/core/src/menu/legal.ts', /FIRST_RUN_VERSION = "([^"]+)"/) ?? 'disclaimer-1')});
  localStorage.setItem('canifishthis.walkthrough.v1', ${JSON.stringify([rd('packages/core/src/help.ts', /WALKTHROUGH_VERSION = "([^"]+)"/) ?? '1'].join('\n'))});
  localStorage.setItem('canifishthis.inseasonSeen.v1', '["*"]'); } catch {}`;
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
    const n = (await press(b, 'I understand') ? 1 : 0) + (await press(b, 'Done') ? 1 : 0) + (await press(b, 'Skip') ? 1 : 0);
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
  const b = await open(`${SITE}/`, { width: 390, height: 844, dpr: 1, mobile: true, network: true, init: NO_POPUP + SEEN });
  let last = 'phone-0';
  const step = async (name, fn) => check(name, async () => {
    try { return await fn(); }
    catch (e) { const s = await shot(b, `FAIL-${last}`).catch(() => null); throw new Error(`${e.message}${s ? ` [shot ${s.file}]` : ''}`); }
  });
  try {
    let standard = null;
    await step('phone: the map paints', async () => { last = '01-map'; const r = await mapPaints(b, '01-map'); standard = r.shot; return r.detail; });
    await step('phone: attribution names OpenStreetMap', async () => {
      await b.evaluate(`document.querySelector('[data-testid=map-credits-chip]')?.click()`);
      const t = await b.evaluate(`document.querySelector('[data-testid=map-credits-card]')?.textContent ?? ''`);
      must(/OpenStreetMap/.test(t), `attribution text: ${JSON.stringify(t.slice(0, 160))}`);
      // (T729) done reading: close it, and the card AND its scrim must be gone, or the next map tap lands on the scrim
      await b.evaluate(`document.querySelector('[data-testid=map-credits-close]')?.click()`);
      await sleep(300);
      const left = await b.evaluate(`['map-credits-card','map-credits-scrim'].filter((i) => document.querySelector('[data-testid='+i+']'))`);
      must(left.length === 0, `the credits card is still in the page after Close: ${JSON.stringify(left)}`);
    });
    for (const [label, id, file, re, tab] of [['Salmon', 'regs-paint-salmon', '02-salmon', /salmon/i, null], ['Run timing', null, '03-run-timing', /\/runtiming\/index\.json/, 'Runs']]) {
      await step(`phone: ${label} mode paints`, async () => {
        last = file;
        must(standard, 'no standard map to compare against');
        const before = b.requests.length;
        if (tab) must(await press(b, tab), `no ${tab} tab`);   // T261: Run timing is the Runs bottom tab
        else {
          must(await b.evaluate(`(() => { const e = ${tidQ(id)}; if (!e) return false; e.click(); return true; })()`), `no ${id} control`);
          await until(b, `(() => { const e = ${tidQ(id)}; return e && (e.getAttribute('aria-selected') === 'true' || e.getAttribute('aria-checked') === 'true' || e.getAttribute('aria-pressed') === 'true'); })()`, 15000, `${label} selected`).catch(() => {});
        }
        await sleep(5000);
        await dismiss(b);
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
    must(await b.evaluate(`(() => { const e = [...document.querySelectorAll('[aria-label="Map"]')].filter((e) => { const r = e.getBoundingClientRect(); return r.width > 0 && r.top > innerHeight * 0.8; }).pop();
      if (!e) return false; e.click(); return true; })()`), 'no Map tab (bottom bar)');
    await until(b, `!!${tidQ('map-day')}`, 15000, 'the Map tab (map-day)'); await sleep(1500);
    await b.evaluate(`${tidQ('regs-paint-standard')}?.click()`); await sleep(2500);

    await step('phone: tap a named water → its regulations', async () => {
      last = '04-tap-named';
      const at = await project((e) => b.evaluate(e), TARGETS.named);   // projected from a vertex of the water, not a fixed pixel
      await tapAt(b, at.x, at.y);
      await until(b, `!!${tidQ('water-title')}?.innerText.trim()`, 60000, `a water to open from the tap at ${at.x},${at.y}`);
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
      const at = await project((e) => b.evaluate(e), TARGETS.unnamed);
      await tapAt(b, at.x, at.y);
      await until(b, `!!${tidQ('water-title')}?.innerText.trim()`, 60000, `a water to open from the tap at ${at.x},${at.y}`);
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
    // (2026-11-04) the Vedder, not the Pitt: since T369 a link naming a water of ONE freshwater part opens that part, and
    // the Pitt's only one (Pitt Lake up to Garibaldi Park) has no gauge that may speak for it — its gauge is on the
    // tidal lower Pitt, which is outside the freshwater regulations
    await step('phone: Vedder River Conditions shows a gauge reading', async () => {
      last = '07-conditions-vedder';
      await navigate(b, `${SITE}/?regs=gnis:3062&view=conditions`);
      await until(b, `/observed \\d{4}-\\d{2}-\\d{2} at/.test(document.body.innerText)`, 90000, 'a gauge reading ("observed <date> at")');
      await dismiss(b);
      const txt = await b.evaluate('document.body.innerText');
      must(/m³\/s|\bm stage\b/.test(txt), 'no flow (m³/s) or stage value on the page');
      const obs = /observed (\d{4}-\d{2}-\d{2}) at ([\d:]+)/.exec(txt);
      await shot(b, '07-conditions-vedder');
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
      // (T736) on a slow line the app asks before fetching 11.7 MB: answered like a user would, and said in the line
      const asked = await waitForRuleBook(b, { press, sleep, ms: 60000 });
      const pdf = b.requests.slice(before).find((r) => /synopsis-[0-9a-f]+\.pdf/.test(r.url));
      must(!pdf || pdf.status === 200 || pdf.status === 304 || pdf.status === null, `synopsis request ${pdf?.url} → ${pdf?.status}`);
      await shot(b, '08-pdf');
      return `${(await b.evaluate('window.__pdf')).replace(/^blob:[^#]+/, 'blob:…')}${asked ? ` (${asked})` : ''}`;
    });
    const ex = b.errors.filter((e) => !/favicon/.test(e));
    // (T494) a TypeError is our code (or the map's) breaking, never the network: a FAIL. Anything else stays a WARN.
    const broke = ex.filter((e) => /TypeError/.test(e));
    if (broke.length) say('FAIL', 'phone: TypeError in the console', broke.slice(0, 3).join(' | ').slice(0, 300));
    else if (ex.length) say('WARN', 'phone: console errors', ex.slice(0, 3).join(' | ').slice(0, 300));
  } finally { await b.close(); }
}

async function desktop() {
  const b = await open(`${SITE}/`, { width: 1440, height: 900, dpr: 1, network: true, init: SEEN });
  try {
    await check('desktop: the map paints', async () => {
      try { return (await mapPaints(b, '09-desktop-map')).detail; }
      catch (e) { const s = await shot(b, 'FAIL-09-desktop-map').catch(() => null); throw new Error(`${e.message}${s ? ` [shot ${s.file}]` : ''}`); }
    });
  } finally { await b.close(); }
}

// ---------------------------------------------------------------- first tap (T737)
// A FRESH profile (a new browser: empty cache, no service worker, no storage), the Fraser's Region 2 part tapped 3 s
// after the map paints, then again after a pan to Lillooet (Region 3). Each tap is timed to READABLE (the Regulations
// header on screen and on top, not merely in the page: the held pane is in the DOM, hidden) and to COMPLETE (every
// release file / picture of the page in, quiet for 1.5 s); over 4 s to readable WARNs and over 10 s FAILs; the complete time is printed.
// These are the first-click numbers of handoff/PLAN/app-integration/tickets/T737-web-first-tap-waits-for-prefetch.md.
const FIRST_TAP_MS = 4000;        // the goal: a WARN above it
const FIRST_TAP_FAIL_MS = 10000;  // the loose cap: a FAIL above it (a slow line is not a broken page)
// vertices of the Fraser River's own line (stream_name layer, name "Fraser River"), clear of any mouth
const FRASER = { lng: -122.204312, lat: 49.131767 };      // below Mission, a Region 2 part
const LILLOOET = { lng: -121.871932, lat: 50.856968 };    // north of Lillooet, Region 3
const visQ = (id) => `(() => { const e = ${tidQ(id)}; if (!e) return false; const r = e.getBoundingClientRect(); if (!(r.width > 0 && r.height > 0)) return false;
  const x = Math.min(innerWidth - 1, Math.max(0, r.left + Math.min(r.width / 2, 40))), y = Math.min(innerHeight - 1, Math.max(0, r.top + r.height / 2));
  const h = document.elementFromPoint(x, y); return !!h && (e.contains(h) || h.contains(e)); })()`;
const mercX = (lo, z) => (lo + 180) / 360 * 512 * 2 ** z;
const mercY = (la, z) => (1 - Math.log(Math.tan(Math.PI / 4 + la * Math.PI / 360)) / Math.PI) / 2 * 512 * 2 ** z;
async function firstTap() {
  const b = await open(`${SITE}/`, { width: 1440, height: 900, dpr: 1, network: true, init: SEEN });
  let centre = { ...HOME };
  const canvasBox = () => b.evaluate(`(() => { const c = document.querySelector('canvas.maplibregl-canvas') || document.querySelector('canvas'); if (!c) return null;
    const r = c.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2, w: r.width, h: r.height }; })()`);
  const panTo = async ({ lng, lat }) => {   // drag the map as a person pans, in steps of 40% of the canvas
    const r = await canvasBox(); const z = HOME.zoom;
    let dx = mercX(lng, z) - mercX(centre.lon, z), dy = mercY(lat, z) - mercY(centre.lat, z);
    const step = Math.min(r.w, r.h) * 0.4;
    while (Math.abs(dx) > 0.5 || Math.abs(dy) > 0.5) {
      const sx = Math.max(-step, Math.min(step, dx)), sy = Math.max(-step, Math.min(step, dy));
      await b.send('Input.dispatchMouseEvent', { type: 'mouseMoved', x: r.x, y: r.y });
      await b.send('Input.dispatchMouseEvent', { type: 'mousePressed', x: r.x, y: r.y, button: 'left', clickCount: 1 });
      for (let k = 1; k <= 12; k++) { await b.send('Input.dispatchMouseEvent', { type: 'mouseMoved', x: r.x - sx * k / 12, y: r.y - sy * k / 12, button: 'left', buttons: 1 }); await sleep(16); }
      await sleep(350);
      await b.send('Input.dispatchMouseEvent', { type: 'mouseReleased', x: r.x - sx, y: r.y - sy, button: 'left', clickCount: 1 });
      dx -= sx; dy -= sy; await sleep(500);
    }
    centre = { lon: lng, lat };
  };
  /** tap `at` now; the ms to readable and to complete (null when it never completes inside 30 s) */
  const timedTap = async (at, label, sweep, part) => {
    await shot(b, `FT-before-${label}`);
    const reqStart = b.requests.length;
    let t0 = Date.now();
    const box = await canvasBox(); const z = HOME.zoom;
    // After a PAN the camera is a few tens of pixels off what was asked (drags lose some of their distance), so the tap is
    // swept sideways until it opens the Fraser: a tap the page shows no sign of (no water frame within 1.5 s) missed
    // every line; one that opens ANOTHER water (a creek beside it) is closed and the sweep goes on. The time counts from
    // the tap that opened the Fraser. The first tap of a fresh page (`sweep` false) is never repeated: it is the test.
    const nudges = sweep ? [0, ...Array.from({ length: 30 }, (_, i) => (i % 2 ? 1 : -1) * 6 * (Math.floor(i / 2) + 1))] : [0];
    let swept = 0;
    for (const nudge of nudges) {
      t0 = Date.now();
      await tapAt(b, Math.round(box.x + mercX(at.lng, z) - mercX(centre.lon, z)) + nudge, Math.round(box.y + mercY(at.lat, z) - mercY(centre.lat, z)));
      let seen = false;
      for (let i = 0; i < 38 && !seen; i++) { seen = await b.evaluate(`!!${tidQ('water-frame')}`).catch(() => false); if (!seen) await sleep(40); }
      if (!seen) continue;
      if (!sweep) break;
      await until(b, `!!${tidQ('water-title')}?.innerText.trim()`, 8000, 'the tapped water').catch(() => {});
      if (part.test(await b.evaluate(`${tidQ('water-title')}?.innerText + ' ' + document.body.innerText`).catch(() => ''))) break;
      swept++;
      await b.evaluate(`${tidQ('water-close')}?.click()`);
      await until(b, `!${tidQ('water-frame')}`, 8000, 'the wrong water to close').catch(() => {});
      await sleep(300);
    }
    let readable = null, complete = null;
    while (Date.now() - t0 < 30000) {
      if (readable === null && await b.evaluate(visQ('regs-header')).catch(() => false)) readable = Date.now() - t0;
      if (readable !== null) {
        const rel = b.requests.slice(reqStart).filter((r) => /\/release\/|\.webp|\.png|\.jpg/.test(r.url));
        const last = Math.max(t0 + readable, ...rel.map((r) => r.doneAt ?? 0));
        if (!rel.some((r) => !r.doneAt) && Date.now() - last > 1500) { complete = last - t0; break; }
      }
      await sleep(40);
    }
    await shot(b, `FT-${label}`);
    return { readable, complete, swept, title: ((await b.evaluate(`(${tidQ('water-title')}?.innerText ?? '') + ' · ' + document.body.innerText`).catch(() => '')) || '').replace(/\n+/g, ' · ').slice(0, 240) };
  };
  try {
    await until(b, `!!${tidQ('map-day')} && !!document.querySelector('canvas')`, 120000, 'the map (map-day)');
    await dismiss(b);
    await sleep(3000);   // the click comes 3 s after paint, while the background download is under way
    for (const [name, at, label, part] of [['Fraser River, Region 2 part, 3 s after paint', FRASER, 'fraser-r2', /Fraser River[\s\S]*CPR Bridge at Mission/],
                                           ['Fraser River at Lillooet (Region 3 part), after a pan', LILLOOET, 'fraser-lillooet', /Fraser River[\s\S]*Bridge River Rapids/]]) {
      await check(`first tap: ${name} readable within ${FIRST_TAP_MS / 1000} s`, async () => {
        if (at === LILLOOET) { await b.evaluate(`${tidQ('water-close')}?.click()`); await sleep(1200); await panTo(LILLOOET); await sleep(2000); }
        const r = await timedTap(at, label, at === LILLOOET, part);
        must(r.readable !== null, `the Regulations never became readable in 30 s (the tap opened "${r.title}")`);
        must(part.test(r.title), `the tap opened "${r.title}", not the expected part of the Fraser River (${part})`);
        const where = ([...r.title.matchAll(/Fraser River · ([^·]+)/g)].pop() ?? [r.title.slice(0, 80)])[0].trim();
        const said = `${where}: readable ${(r.readable / 1000).toFixed(1)} s, complete ${r.complete === null ? 'not in 30 s' : (r.complete / 1000).toFixed(1) + ' s'}, ${r.swept} sweep tap(s)`;
        must(r.readable <= FIRST_TAP_FAIL_MS, `readable after ${(r.readable / 1000).toFixed(1)} s (cap ${FIRST_TAP_FAIL_MS / 1000} s): ${said}`);
        // a sweep tap means a miss or a wrong water came first: the files the right tap read were already warm, so its time is the weaker measure
        if (r.readable > FIRST_TAP_MS) return { warn: `readable after ${(r.readable / 1000).toFixed(1)} s (goal ${FIRST_TAP_MS / 1000} s): ${said}` };
        if (r.swept > 0) return { warn: `${r.swept} sweep tap(s) came first, so this tap read warm files: ${said}` };
        return said;
      });
    }
  } finally { await b.close(); }
}

const t0 = Date.now();
console.log(`smoke: ${SITE} (data ${DATA}, feeds ${FEEDS}); shots → ${SHOTS}`);
if (ONLY.includes('http')) await http();
if (ONLY.includes('browser')) await browser().catch((e) => say('FAIL', 'phone session', e.message));
if (ONLY.includes('desktop')) await desktop().catch((e) => say('FAIL', 'desktop session', e.message));
if (ONLY.includes('firsttap')) await firstTap().catch((e) => say('FAIL', 'first-tap session', e.message));
const fails = results.filter((r) => r.level === 'FAIL');
console.log(`\n${fails.length ? 'SMOKE FAILED' : 'smoke ok'}: ${results.filter((r) => r.level === 'PASS').length} pass, ` +
  `${results.filter((r) => r.level === 'WARN').length} warn, ${fails.length} fail in ${((Date.now() - t0) / 1000).toFixed(0)} s; shots ${SHOTS}`);
for (const f of fails) console.log(`  ✗ ${f.name}: ${f.detail}`);
process.exit(fails.length ? 1 : 0);
