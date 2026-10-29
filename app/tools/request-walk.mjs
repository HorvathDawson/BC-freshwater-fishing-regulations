// T129 — THE WEB REQUEST WALK: what a web visitor fetches, moment by moment, on the running app (Expo web) over the
// live release. Each walk is a cold browser (a fresh profile: no HTTP cache, no Cache Storage), so every byte is a
// first visit's. Prints the table and checks it against the PINNED walk (`request-walk.pin.json`): the release files
// each moment fetches, by manifest name. A file that starts loading earlier or later than pinned is a FAIL — that is
// the drift the release's total gate cannot see (a 2.7 MB `water` slipping onto first paint looks like nothing there).
// The bytes of those files are gated by `packages/data/src/release/budget.ts` (`release-budget.test.ts` reads this
// pin with the release's manifest), so the walk pins WHICH files and the test prices them.
//
//   EXPO_PUBLIC_DATA_BASE=http://localhost:<tiles> npx expo start --web --port <yours>   (app/apps/mobile)
//   node app/tools/request-walk.mjs [--url http://localhost:8081] [--only visit,taps,conditions,spot,regions]
//                                   [--json out.json] [--pin]      (--pin: write the pin from this run)
//   → exit 1 on any FAIL line
//
// T186 (Q164): THE PREFETCH after first paint (the region's regs file always; core + sql-wasm when the browser is idle)
// is recorded as a moment of its own, `prefetch after paint`: the release files (and the wasm) a first-paint window
// fetched AFTER the app's first `release-prefetch` performance mark. So `first paint` stays what painting needed — the
// T129 gate — and each later moment shows what it still had to fetch with the prefetch done. `tap named (save-data)`
// is the same tap with `navigator.connection.saveData` on: the idle prefetch is skipped, the region's file is not.
//
// Tiles, glyphs and sprites are recorded (bytes, count) but never pinned: what they cost depends on the viewport and the
// zoom, not on the app's choice of what to load. The app bundle and fonts are a development build's (Metro, unminified):
// recorded as `app`, not pinned, and not what production ships.
import fs from 'node:fs';
import { open } from './regs-web/cdp.mjs';

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; };
const URL0 = opt('--url', 'http://localhost:8081').replace(/\/$/, '');
const ONLY = opt('--only', 'visit,taps,conditions,spot,regions').split(',');
const PIN = new URL('./request-walk.pin.json', import.meta.url);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** A water per region whose every part's answer key is in that region only (the largest such, by size; the
 *  2026-10-24 release) — so its first open fetches exactly one region file. Item ids are durable (AGENTS 5). */
const REGION_WATERS = {
  1: 'gnis:21141', 2: 'gnis:15333', 3: 'gnis:39492', 4: 'gnis:37414', 5: 'gnis:13744', 6: 'gnis:15430',
  '7a': 'gnis:8018', '7b': 'gnis:14619', 8: 'gnis:32069',
};
/** A saved spot on the Vedder (gnis:3062) at Vedder Crossing (the web keeps spots in localStorage; `spots/store.web.ts`). */
const SPOT = { id: 't129-walk', createdAt: 1729000000000, updatedAt: 1729000000000, visitedAt: 1729000000000,
  lat: 49.0985, lon: -121.9585, item: 'gnis:3062', waterName: 'Vedder River', title: 'Walk spot', notes: '', photos: [],
  reading: null, weather: null, trace: null, panel: null };

/** What a request is: a release file (by its manifest name), the manifest, or a kind that is recorded, not pinned. */
function kindOf(url) {
  const u = new URL(url), p = u.pathname;
  let m;
  if (p.endsWith('/release/manifest.json')) return { kind: 'manifest', name: 'manifest' };
  if ((m = /\/release\/r-[0-9a-f]+\/(.+)-[0-9a-f]{12}\.[a-z]+\.gz$/.exec(p))) return { kind: 'release', name: m[1] };
  if (/\.pmtiles$/.test(p)) return { kind: /basemap/.test(p) ? 'basemap' : 'tiles', name: null };
  if (/\/map-assets-[^/]+\//.test(p)) return { kind: 'map-assets', name: null };
  if (p.endsWith('bc_outside.geojson')) return { kind: 'mask', name: 'mask' };
  if ((m = /\/feeds\/live\/([\w-]+)\.json$/.exec(p))) return { kind: 'feed', name: m[1] === 'index' ? 'feed:index' : 'feed:station' };
  if (p.endsWith('.wasm')) return { kind: 'wasm', name: 'wasm' };
  if (u.origin === new URL(URL0).origin) return { kind: 'app', name: null };
  return { kind: 'external', name: u.hostname };
}

/** The page says Save-Data (T186): what a reader who asked the browser to save data gets. */
const SAVE_DATA = `try { Object.defineProperty(Navigator.prototype, 'connection', { configurable: true, get: () => ({ saveData: true, effectiveType: '4g' }) }); } catch {}`;

/** (T253) The background prefetch (every file an unnamed tap reads, after idle) is off in the walk: each moment pins
 *  what a COLD visitor fetches. `tools/prefetch-measure.mjs` measures the tap with it on. */
const NO_BACKGROUND = `globalThis.__releasePrefetchBackground = false;`;

async function session(url, { seedSpot = false, saveData = false } = {}) {
  const init = NO_BACKGROUND + (seedSpot ? `try { if (!localStorage.getItem('canifishthis.spots.v1')) localStorage.setItem('canifishthis.spots.v1', ${JSON.stringify(JSON.stringify([SPOT]))}); } catch {}` : '')
    + (saveData ? SAVE_DATA : '');
  const b = await open(url, { width: 390, height: 844, network: true, init });
  const take = () => b.requests.splice(0).filter((r) => /^https?:/.test(r.url));
  /** quiet: no request started or finished for `quiet` ms, and none in flight */
  const idle = async (quiet = 3500, max = 90000) => {
    let sig = '', last = Date.now(); const t0 = Date.now();
    const web = () => b.requests.filter((r) => /^https?:/.test(r.url));
    while (Date.now() - t0 < max) {
      const all = web(), done = all.filter((r) => r.bytes !== null || r.failed).length, s = `${all.length}/${done}`;
      if (s !== sig) { sig = s; last = Date.now(); } else if (done === all.length && Date.now() - last > quiet) return;
      await sleep(200);
    }
    throw new Error(`no quiet within ${max} ms: in flight ${web().filter((r) => r.bytes === null && !r.failed).map((r) => r.url).join(' ')}`);
  };
  const until = async (expr, ms = 90000, what = expr) => {
    for (let t = 0; t < ms; t += 200) { if (await b.evaluate(expr).catch(() => false)) return; await sleep(200); }
    throw new Error(`timed out waiting for ${what}`);
  };
  const press = async (label, { prefix = false } = {}) => {
    const ok = await b.evaluate(`(() => { const e = [...document.querySelectorAll('[aria-label]')].find((e) => {
      const l = e.getAttribute('aria-label') || ''; return ${prefix ? `l.startsWith(${JSON.stringify(label)})` : `l === ${JSON.stringify(label)}`}; });
      if (!e) return false; e.click(); return true; })()`);
    if (!ok) throw new Error(`nothing to press: ${label}`);
  };
  return { b, take, idle, until, press };
}

const results = [];   // { walk, moment, requests: [{ kind, name, bytes, url }] }
const record = (walk, moment, reqs) => results.push({ walk, moment,
  requests: reqs.map((r) => ({ ...kindOf(r.url), bytes: r.bytes ?? 0, url: r.url, failed: r.failed ?? null })) });
/** When the app first marked a prefetch (epoch ms), or null: none yet. */
const PREFETCH_AT = `(() => { const e = performance.getEntriesByType('mark').filter((m) => m.name.startsWith('release-prefetch'));
  return e.length ? performance.timeOrigin + Math.min(...e.map((m) => m.startTime)) : null; })()`;
/**
 * First paint, then the prefetch it starts (T186), as two moments: everything sent before the app's first prefetch
 * mark is first paint's; a release file or the wasm sent at or after it is the prefetch's (tiles, the feed and the app
 * stay with first paint — the prefetch fetches only release files).
 */
const firstPaint = async (s, walk) => {
  await s.until(`!!document.querySelector('[data-testid=map-day]') || !!document.querySelector('[data-testid=regs-header]')`, 120000, 'the map');
  await s.idle(4000);
  // the region's file is always prefetched once the map has painted; wait for its mark, then for the quiet after it
  await s.until(`${PREFETCH_AT} !== null`, 30000, 'the prefetch after first paint').catch(() => {});
  await s.idle(4000);
  const at = await s.b.evaluate(PREFETCH_AT);
  const reqs = s.take();
  const ahead = (r) => at !== null && r.wall !== null && r.wall * 1000 >= at - 1 && ['release', 'wasm'].includes(kindOf(r.url).kind);
  record(walk, 'first paint', reqs.filter((r) => !ahead(r)));
  record(walk, 'prefetch after paint', reqs.filter(ahead));
};

async function visit() {
  const s = await session(`${URL0}/`);
  try {
    await firstPaint(s, 'visit');
    await s.press('Search waters and towns'); await sleep(600);
    await s.b.evaluate(`document.querySelector('input').focus()`);
    await s.b.send('Input.insertText', { text: 'Thompson' });
    await s.until(`[...document.querySelectorAll('[aria-label]')].some((e) => (e.getAttribute('aria-label') || '').startsWith('Show Thompson River,'))`);
    await s.idle(); record('visit', 'first search', s.take());
    await s.press('Show Thompson River,', { prefix: true }); await s.idle(); record('visit', 'search: show a water', s.take());
    await s.press('Open Thompson River,', { prefix: true });
    // T203: a search result lands on the Map; the water's title shows there, and the Regs page is a further step
    await s.until(`!!document.querySelector('[data-testid=water-title]')?.innerText.trim()`);
    // it opens on the water's Map tab; the walk goes on to the Regulations, as an angler reading them would
    await s.until(`!!document.querySelector('[data-testid=water-map-regs]')`, 60000, 'the water\'s map');
    await s.press('Show regulations for this part');
    await s.until(`!!document.querySelector('[data-testid=regs-header]')`, 60000, 'the water\'s Regulations'); await s.idle();
    record('visit', 'search: open the water (R3)', s.take());
  } finally { await s.b.close(); }
}

async function conditions() {
  const s = await session(`${URL0}/`);
  try {
    await firstPaint(s, 'conditions');
    await s.press('Conditions'); await s.until(`!!document.querySelector('[data-testid=map-horizons]')`); await s.idle();
    record('conditions', 'open Conditions', s.take());
    await s.press('Expected change over the next 10 days'); await s.idle();
    record('conditions', 'the Change horizon', s.take());
  } finally { await s.b.close(); }
}

async function spot() {
  const s = await session(`${URL0}/`, { seedSpot: true });
  try {
    await firstPaint(s, 'spot');
    await s.press('Spots'); await s.until(`!!document.querySelector('[data-testid=spots-screen]')`); await s.idle();
    record('spot', 'the Spots tab', s.take());
    await s.press('Walk spot', { prefix: true }); await s.idle();
    record('spot', 'open a spot', s.take());
  } finally { await s.b.close(); }
}

/**
 * A TAP ON THE MAP, on the default camera at 390 × 844 (a fresh profile's first view, the Chilliwack valley): a named
 * water and an unnamed stream, each the first tap of a cold visit. The header's name is checked, so a moved camera or
 * a retiled map fails here by name rather than pinning some other water's files.
 */
const MAP_TAPS = [
  { walk: 'tap named', x: 102, y: 260, name: 'Gravel Slough', moment: 'map tap: a named water (R2)' },
  { walk: 'tap unnamed', x: 319, y: 602, name: 'Unnamed stream · Region 2', moment: 'map tap: an unnamed stream (R2)' },
  // T186: the named tap again with Save-Data on — no idle prefetch, so the tap fetches core + wasm itself
  { walk: 'tap named (save-data)', x: 102, y: 260, name: 'Gravel Slough', moment: 'map tap: a named water (R2)', saveData: true },
];
async function taps() {
  for (const t of MAP_TAPS) {
    const s = await session(`${URL0}/`, { saveData: !!t.saveData });
    try {
      await firstPaint(s, t.walk);
      for (const type of ['mouseMoved', 'mousePressed', 'mouseReleased'])
        await s.b.send('Input.dispatchMouseEvent', { type, x: t.x, y: t.y, button: 'left', clickCount: 1 });
      await s.until(`!!document.querySelector('[data-testid=water-title]')?.innerText.trim()`, 60000, `a tap at ${t.x},${t.y} to open a water`);
      const head = await s.b.evaluate(`document.querySelector('[data-testid=water-title]')?.innerText ?? ''`);
      if (!head.startsWith(t.name)) throw new Error(`the tap at ${t.x},${t.y} opened "${head.split('\n')[0]}", not ${t.name}`);
      // the water opens on its Regulations tab (T203: the title shows first, the page after)
      await s.until(`!!document.querySelector('[data-testid=regs-header]')`, 60000, `the Regulations of ${t.name}`);
      await s.idle(); record(t.walk, t.moment, s.take());
    } finally { await s.b.close(); }
  }
}

async function regions() {
  for (const [r, item] of Object.entries(REGION_WATERS)) {
    const s = await session(`${URL0}/?regs=${item}`);
    try {
      await s.until(`!!document.querySelector('[data-testid=regs-header]')`, 120000, 'the Regulations page');
      await s.idle(4000);
      record(`region ${r}`, `cold open ${item} (start link)`, s.take());
    } finally { await s.b.close(); }
  }
}

const WALKS = { visit, taps, conditions, spot, regions };
for (const w of ONLY) { if (!WALKS[w]) throw new Error(`no walk ${w}`); await WALKS[w](); }

// ---- the table ----
const kb = (n) => (n / 1000).toFixed(1).padStart(8);
const sum = (rs, f) => rs.filter(f).reduce((a, r) => a + r.bytes, 0);
console.log(`${'walk'.padEnd(22)} ${'moment'.padEnd(40)} ${'release kB'.padStart(10)} ${'wasm'.padStart(8)} ${'feed'.padStart(8)} ${'tiles+map'.padStart(9)} ${'other'.padStart(8)}  release files`);
for (const m of results) {
  const rs = m.requests;
  const rel = sum(rs, (r) => r.kind === 'release' || r.kind === 'manifest' || r.kind === 'mask');
  console.log(`${m.walk.padEnd(22)} ${m.moment.padEnd(40)} ${kb(rel)}   ${kb(sum(rs, (r) => r.kind === 'wasm'))} ${kb(sum(rs, (r) => r.kind === 'feed'))} ${kb(sum(rs, (r) => ['tiles', 'basemap', 'map-assets'].includes(r.kind)))} ${kb(sum(rs, (r) => ['app', 'external'].includes(r.kind)))}  ${[...new Set(rs.filter((r) => r.name).map((r) => r.name))].join(' ')}`);
}
const failed = results.flatMap((m) => m.requests.filter((r) => r.failed && r.kind !== 'app').map((r) => `${m.walk}/${m.moment}: ${r.url} ${r.failed}`));
if (opt('--json', null)) fs.writeFileSync(opt('--json'), JSON.stringify(results, null, 1));

// ---- the pin: which named files each moment fetches, sorted, a file fetched twice listed twice (a duplicate request
// is drift too: the manifest was read twice per visit until T129); tiles, glyphs, the app and other hosts never ----
const pinOf = (m) => m.requests.filter((r) => r.name && r.kind !== 'external').map((r) => r.name).sort();
const now = Object.fromEntries(results.map((m) => [`${m.walk} | ${m.moment}`, pinOf(m)]));
let bad = failed.length;
for (const f of failed) console.log(`FAIL request failed — ${f}`);
if (args.includes('--pin')) {
  const old = fs.existsSync(PIN) ? JSON.parse(fs.readFileSync(PIN, 'utf8')) : {};
  fs.writeFileSync(PIN, JSON.stringify({ ...old, ...now }, null, 1) + '\n');
  console.log(`pinned ${Object.keys(now).length} moments → ${PIN.pathname}`);
} else {
  const pin = JSON.parse(fs.readFileSync(PIN, 'utf8'));
  for (const [k, files] of Object.entries(now)) {
    const want = pin[k];
    if (!want) { console.log(`FAIL ${k} — not pinned (run with --pin)`); bad++; continue; }
    const same = JSON.stringify(want) === JSON.stringify(files);
    console.log(`${same ? 'PASS' : 'FAIL'} ${k} — ${same ? files.join(' ') : `fetched [${files.join(' ')}], pinned [${want.join(' ')}]`}`);
    if (!same) bad++;
  }
}
process.exit(bad ? 1 : 0);
