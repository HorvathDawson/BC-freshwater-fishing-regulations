// T254 — THE BAD-CONNECTION HARNESS: the web app on a rural B.C. connection (1–2 bars, flaky), measured. Each step is a
// cold headless Chrome (a fresh profile: no HTTP cache, no Cache Storage) on a phone screen, with DevTools' network
// throttle in force from the first byte. Repeatable: the same profiles, the same waters, a seeded drop pattern.
//
//   node app/tools/bad-net.mjs [--url http://localhost:39260] [--profiles slow-3g,2g,flaky,offline-back]
//                              [--steps paint,named,unnamed,link,conditions,pdf] [--json out.json] [--md out.md]
//                              [--seed 254] [--low on|off] [--shots <dir>] [--desktop] [--water <slug>]
//
//   --url      a local production web build (`expo export --platform web`, served statically: `--serve <dir>` does
//              it on :39260) or staging (https://staging.canifishthis.ca). Never production.
//   --low      sets the app's own Data setting before the visit (T254 low-bandwidth mode: on / off / auto); absent,
//              the app decides for itself.
//
// PROFILES (down / up / round trip)
//   slow-3g        400 kbit/s / 400 kbit/s / 400 ms                (DevTools' "Slow 3G", rounded)
//   2g             100 kbit/s /  50 kbit/s / 800 ms                ("2G-ish": one bar on a highway pull-out)
//   flaky          1 Mbit/s / 384 kbit/s / 300 ms, and: 8 % of data requests reset, 5 % stall 45 s then time out,
//                  a 4 s blackout every 30 s (in-flight requests die mid-body). Seeded (`--seed`): same pattern each run.
//   offline-back   slow-3g; the map paints, then the line drops: a named tap while offline (what is shown?), then the
//                  line comes back (does the page recover by itself, after a re-tap, or not at all?).
// Each profile names its connection type to the page (`cellular3g` / `cellular2g`): what a phone's browser reports.
//
// STEPS (each its own cold visit; `paint` is every map visit's first)
//   paint        load → the app's map chrome (map-day), and → the map painted (the first region report, a perf mark)
//   named        paint, then a tap on a named water (Gravel Slough, R2) → its Regulations on screen; the tap is
//                repeated every 2.5 s until the water opens (a tap before its tile has arrived hits nothing)
//   unnamed      the same on an unnamed stream (Region 2)
//   link         a start link (/?regs=gnis:3062, the Vedder): no map needed → Regulations on screen
//   conditions   paint, then Conditions → its horizons on screen, and → its data landed (the feed + release files the
//                press started, all finished)
//   chart        (T256) the link's water on its Conditions view (`&view=conditions`) → its 72-hour chart drawn, and →
//                the chart's normal range (the climate envelope) drawn on it
//   pdf          the link's page, then "Rule book page N" → the highlighted PDF opened, an offer to download it, or the
//                cap (with how far the download got)
//
// Every step records failed requests (by kind) and the app's console errors. A step that does not finish within its
// profile's cap is "timeout". Exit 0 always: this measures, it does not gate (the policy is pinned by unit tests).
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import zlib from 'node:zlib';
import { open } from './regs-web/cdp.mjs';
import { project, TARGETS } from './map-tap.mjs';

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; };
const has = (k) => args.includes(k);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export const PROFILES = {
  'slow-3g': { label: 'Slow 3G', down: 400e3 / 8, up: 400e3 / 8, latency: 400, type: 'cellular3g', cap: 240_000 },
  '2g': { label: '2G-ish', down: 100e3 / 8, up: 50e3 / 8, latency: 800, type: 'cellular2g', cap: 600_000 },
  flaky: { label: 'Flaky', down: 1e6 / 8, up: 384e3 / 8, latency: 300, type: 'cellular3g', cap: 300_000,
           fail: 0.08, stall: 0.05, stallMs: 45_000, blackout: { every: 30_000, for: 4_000 } },
  // (T282) DevTools' "Fast 3G" preset (1.44 Mbit/s down after its 0.9 factor, 675 kbit/s up, 562.5 ms)
  'fast-3g': { label: 'Fast 3G', down: 1.44e6 / 8, up: 675e3 / 8, latency: 562.5, type: 'cellular3g', cap: 240_000 },
  'offline-back': { label: 'Offline, then back', down: 400e3 / 8, up: 400e3 / 8, latency: 400, type: 'cellular3g',
                    cap: 240_000, offlineBack: true },
};
const STEPS = ['paint', 'named', 'unnamed', 'link', 'conditions', 'chart', 'pdf'];
// (T282) not in the default set: `--steps takeover` (a water page needs the SEO site: staging, or a served site dir)

/** A small seeded RNG (mulberry32): the flaky profile's drops are the same each run. */
export function rng(seed) {
  let a = seed >>> 0;
  return () => { a = (a + 0x6d2b79f5) >>> 0; let t = a; t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
}

/** What a request is, for the failure tally (the request walk's kinds, coarser). */
export function kindOf(url, site) {
  const u = new URL(url), p = u.pathname;
  if (p.endsWith('/release/manifest.json')) return 'manifest';
  if (/\/release\/r-[0-9a-f]+\/synopsis-[0-9a-f]+\.pdf$/.test(p)) return 'pdf';
  if (/\/release\/r-[0-9a-f]+\//.test(p)) return 'release';
  if (/\.pmtiles$/.test(p) || /\/map-assets-[^/]+\//.test(p) || p.endsWith('bc_outside.geojson')) return 'tiles';
  if (/\/feeds\//.test(p) || /feeds/.test(u.hostname)) return 'feed';
  if (p.endsWith('.wasm')) return 'wasm';
  if (site && u.origin === new URL(site).origin) return 'app';
  return 'other';
}
/** The data a flaky line may drop: never the app's own page and code (a run with no app measures nothing). */
const droppable = (url, site) => !['app'].includes(kindOf(url, site)) && /^https?:/.test(url);

const TAPS = TARGETS;   // map-tap.mjs: real vertices, projected at tap time (a fixed pixel broke with T275)
const LINK = { item: 'gnis:3062', name: 'Vedder River' };
const WATCH = ['map-day', 'regs-header', 'water-title', 'map-horizons', 'synopsis-loading', 'synopsis-retry', 'boot-slow',
               'synopsis-offer', 'net-status', 'regs-slow', 'salmon-pending'];

/** (T256) the "Before you fish" notice, the coach-mark tour and the in-season notice seen, as the request walk seeds
 *  them (versions read from the source: a bumped version would cover the map again) */
const rdv = (f, re) => re.exec(fs.readFileSync(new URL(`../${f}`, import.meta.url), 'utf8'))?.[1];
const SEEN = `try { localStorage.setItem('canifishthis.firstRun.v1', ${JSON.stringify(rdv('packages/core/src/menu/legal.ts', /FIRST_RUN_VERSION = "([^"]+)"/) ?? 'disclaimer-1')});
  localStorage.setItem('canifishthis.walkthrough.v1', ${JSON.stringify(rdv('packages/core/src/help.ts', /WALKTHROUGH_VERSION = "([^"]+)"/) ?? '1')});
  localStorage.setItem('canifishthis.inseasonSeen.v1', '["*"]'); } catch {}`;

/** Before any page script: the first-run notice accepted, the T254 setting (when asked), and the watchers. */
function initScript(low) {
  return `${SEEN}
${low ? `try { localStorage.setItem('canifishthis.lowData.v1', ${JSON.stringify(low)}); } catch {}` : ''}
window.__t254 = { seen: {}, pdfAt: null, pdfBytes: null };
(() => {
  const ids = ${JSON.stringify(WATCH)};
  const look = () => { for (const id of ids) if (!(id in window.__t254.seen) && document.querySelector('[data-testid="' + id + '"]'))
    window.__t254.seen[id] = performance.now();
    // (T282) a water page's prerendered pane: seen, then dropped by the hand-over once the app's own pane is up
    const pre = document.getElementById('prerender');
    if (pre && !('prerender' in window.__t254.seen)) window.__t254.seen.prerender = performance.now();
    if (!pre && 'prerender' in window.__t254.seen && !('handover' in window.__t254.seen)) window.__t254.seen.handover = performance.now(); };
  new MutationObserver(look).observe(document, { subtree: true, childList: true });
  const make = URL.createObjectURL.bind(URL);
  URL.createObjectURL = (b) => { if (b && b.type === 'application/pdf') { window.__t254.pdfAt = performance.now(); window.__t254.pdfBytes = b.size; } return make(b); };
})();`;
}

const PAINTED = `(() => { const e = performance.getEntriesByType('mark').filter((m) => m.name.startsWith('release-prefetch'));
  return e.length ? Math.min(...e.map((m) => m.startTime)) : null; })()`;

async function session(site, url, profile, o) {
  const P = PROFILES[profile];
  const random = rng(o.seed);
  const held = new Set();
  let b = null, offline = false, blackoutTimer = null, send = null, closed = false;
  const conditions = (off) => ({ offline: off, latency: P.latency, downloadThroughput: P.down, uploadThroughput: P.up,
                                 connectionType: P.type });
  const onEvent = (m) => {
    // answered through `send` (set before the page is navigated): the document itself is paused too, and must not
    // wait for `open()` to return (it waits for the document)
    if (m.method !== 'Fetch.requestPaused' || !send || closed) return;
    const { requestId, request } = m.params;
    const r = droppable(request.url, site) ? random() : 1;
    if (P.fail && r < P.fail) { send('Fetch.failRequest', { requestId, errorReason: 'ConnectionReset' }).catch(() => {}); return; }
    if (P.stall && r < P.fail + P.stall) {
      held.add(requestId);
      setTimeout(() => { if (closed) return; held.delete(requestId); send('Fetch.failRequest', { requestId, errorReason: 'TimedOut' }).catch(() => {}); }, P.stallMs);
      return;
    }
    send('Fetch.continueRequest', { requestId }).catch(() => {});
  };
  const width = o.desktop ? 1280 : 390, height = o.desktop ? 800 : 844;
  b = await open(url, {
    width, height, network: true, init: initScript(o.low), onEvent, mobile: !o.desktop, dpr: o.desktop ? 1 : 2,
    setup: async (sendNow) => {
      send = sendNow;
      await send('Network.enable', {});
      await send('Network.emulateNetworkConditions', conditions(false));
      if (P.fail || P.stall) await send('Fetch.enable', { patterns: [{ urlPattern: '*', requestStage: 'Request' }] });
    },
  });
  const t0 = Date.now();
  if (P.blackout) {
    const cycle = async () => {
      if (!b) return;
      await b.send('Network.emulateNetworkConditions', conditions(true)).catch(() => {});
      await sleep(P.blackout.for);
      if (!b) return;
      if (!offline) await b.send('Network.emulateNetworkConditions', conditions(false)).catch(() => {});
      blackoutTimer = setTimeout(cycle, P.blackout.every - P.blackout.for);
    };
    blackoutTimer = setTimeout(cycle, P.blackout.every);
  }
  // a page wedged by the line must not wedge the harness: every evaluate gives up after 30 s
  const evaluate = b.evaluate;
  b.evaluate = (expr) => Promise.race([evaluate(expr), new Promise((_, no) => setTimeout(() => no(new Error('evaluate: no answer in 30 s')), 30_000))]);
  const s = {
    b, t0, P,
    setOffline: async (off) => { offline = off; await b.send('Network.emulateNetworkConditions', conditions(off)); },
    seen: async () => (await b.evaluate('window.__t254 && window.__t254.seen').catch(() => null)) ?? {},
    /** page time (performance.now) → ms since this session's start, on node's clock */
    pageNow: async () => b.evaluate('performance.now()'),
    until: async (expr, ms) => { const end = Date.now() + ms;
      while (Date.now() < end) { if (await b.evaluate(expr).catch(() => false)) return true; await sleep(200); } return false; },
    press: async (label, { prefix = false } = {}) => b.evaluate(`(() => { const e = [...document.querySelectorAll('[aria-label]')].find((e) => {
      const l = e.getAttribute('aria-label') || ''; return ${prefix ? `l.startsWith(${JSON.stringify(label)})` : `l === ${JSON.stringify(label)}`}; });
      if (!e) return false; e.click(); return true; })()`),
    closeSheets: async () => b.evaluate(`(() => { const e = [...document.querySelectorAll('[aria-label],[role=button]')]
      .find((e) => /^(Done|I understand|Skip)$/.test((e.getAttribute('aria-label') || e.innerText || '').trim())); if (!e) return false; e.click(); return true; })()`).catch(() => false),
    tap: async (x, y) => { for (const type of ['mouseMoved', 'mousePressed', 'mouseReleased'])
      await b.send('Input.dispatchMouseEvent', { type, x, y, button: 'left', clickCount: 1 }); },
    tapWater: async (t) => { const p = await project((e) => b.evaluate(e), t); await s.tap(p.x, p.y); },
    shot: async (name) => { if (!o.shots) return; fs.mkdirSync(o.shots, { recursive: true });
      const r = await b.send('Page.captureScreenshot', { format: 'png' });
      fs.writeFileSync(path.join(o.shots, `${profile}-${name}${o.desktop ? '-desktop' : ''}.png`), Buffer.from(r.data, 'base64')); },
    text: async () => (await b.evaluate('document.body.innerText').catch(() => '')).replace(/\s+/g, ' ').slice(0, 400),
    tally: async () => {
      const seenNow = await b.evaluate('window.__t254 && window.__t254.seen').catch(() => null) ?? {};
      const failed = {}, kB = {};
      for (const r of b.requests) {
        const k = kindOf(r.url, site);
        if (r.failed) failed[k] = (failed[k] ?? 0) + 1;
        kB[k] = Math.round((kB[k] ?? 0) + (r.bytes ?? 0) / 1000);
      }
      const files = b.requests.filter((r) => kindOf(r.url, site) === 'release' && r.bytes !== null)
        .map((r) => /\/release\/r-[0-9a-f]+\/(.+)-[0-9a-f]{12}\./.exec(new URL(r.url).pathname)?.[1]).filter(Boolean);
      return { failed, kB, files, requests: b.requests.length, consoleErrors: b.errors.length,
               // the slow states the page put up (T254): the banner, a waiting line, a part still coming, the PDF offer
               slowStates: ['net-status', 'regs-slow', 'salmon-pending', 'synopsis-offer', 'boot-slow'].filter((k) => k in seenNow) };
    },
    close: async () => { closed = true; clearTimeout(blackoutTimer); for (const id of held) await b.send('Fetch.failRequest', { requestId: id, errorReason: 'Aborted' }).catch(() => {});
      const x = b; b = null; await x.close(); },
  };
  return s;
}

/** load → map chrome, → painted. ms since the navigation (page clock). */
async function paint(s) {
  const ok = await s.until(`!!document.querySelector('[data-testid=map-day]')`, s.P.cap);
  const seen = await s.seen();
  const chrome = ok ? Math.round(seen['map-day']) : null;
  const painted = ok && await s.until(`${PAINTED} !== null`, s.P.cap) ? Math.round(await s.b.evaluate(PAINTED)) : null;
  return { chrome, painted, status: painted !== null ? 'ok' : 'timeout', ...(painted === null ? { note: (await s.text()).slice(0, 80) } : {}) };
}

/** tap (again every 2.5 s) until the water opens, then until its Regulations show; ms from the opening tap. */
async function tapUntilRegs(s, t, cap) {
  const end = Date.now() + cap;
  let tapAt = null, opened = false;
  while (Date.now() < end && !opened) {
    await s.closeSheets();
    tapAt = await s.pageNow();
    // (T254) a banner over the app moves the map: the projection reads the canvas as it is now, so it follows
    await s.tapWater(t);
    // the water's frame is up (its name may come later: an unnamed reach is named by the store it is waiting for)
    opened = await s.until(`!!document.querySelector('[data-testid=water-title]')`, 2500);
  }
  if (!opened) return { status: 'timeout', note: 'the tap never opened a water' };
  const shown = await s.until(`!!document.querySelector('[data-testid=regs-header]')`, Math.max(1000, end - Date.now()));
  const head = await s.b.evaluate(`document.querySelector('[data-testid=water-title]')?.innerText ?? ''`);
  if (shown && !head.startsWith(t.name)) return { status: 'wrong', note: `opened "${head.split('\n')[0]}"` };
  const seen = await s.seen();
  return shown ? { status: 'ok', ms: Math.round(seen['regs-header'] - tapAt), titleMs: Math.round(seen['water-title'] - tapAt) }
               : { status: 'timeout', note: (await s.text()).slice(0, 160) };
}

async function stepMap(site, profile, o, then) {
  const s = await session(site, `${site}/`, profile, o);
  try {
    const p = await paint(s);
    await s.shot('paint');
    const out = { paint: p };
    if (p.status === 'ok' && then) Object.assign(out, await then(s));
    return { ...out, ...(await s.tally()) };
  } finally { await s.close(); }
}

const STEP = {
  paint: (site, profile, o) => stepMap(site, profile, o, null),
  named: (site, profile, o) => stepMap(site, profile, o, async (s) => {
    const r = { regs: await tapUntilRegs(s, TAPS.named, s.P.cap) };
    await s.shot('named-regs');
    return r;
  }),
  unnamed: (site, profile, o) => stepMap(site, profile, o, async (s) => {
    const r = { regs: await tapUntilRegs(s, TAPS.unnamed, s.P.cap) };
    await s.shot('unnamed-regs');
    return r;
  }),
  async link(site, profile, o) {
    const s = await session(site, `${site}/?regs=${LINK.item}`, profile, o);
    try {
      // a shot a few seconds in: what a reader on this line sees while the page is on its way
      const early = s.until(`!!document.querySelector('[data-testid=regs-header]')`, 8000).then(() => s.shot('link-early'));
      const ok = await s.until(`!!document.querySelector('[data-testid=regs-header]')`, s.P.cap);
      await early;
      const seen = await s.seen();
      await s.shot('link-regs');
      return { regs: ok ? { status: 'ok', ms: Math.round(seen['regs-header']), titleMs: Math.round(seen['water-title'] ?? NaN) }
                        : { status: 'timeout', note: (await s.text()).slice(0, 160) }, ...(await s.tally()) };
    } finally { await s.close(); }
  },
  conditions: (site, profile, o) => stepMap(site, profile, o, async (s) => {
    await s.closeSheets();
    const at = Date.now(), pageAt = await s.pageNow();
    if (!await s.press('Conditions')) return { conditions: { status: 'no-button' } };
    const shown = await s.until(`!!document.querySelector('[data-testid=map-horizons]')`, s.P.cap);
    // the data the press started: feed + release files sent after it, all finished (or failed); 3 s of nothing new
    const mine = () => s.b.requests.filter((r) => r.at >= at && ['feed', 'release'].includes(kindOf(r.url, site)));
    let quietSince = Date.now(), n = -1;
    while (Date.now() - at < s.P.cap) {
      const rs = mine(), open = rs.filter((r) => r.doneAt === null).length;
      if (rs.length !== n || open) { n = rs.length; quietSince = Date.now(); }
      else if (Date.now() - quietSince > 3000) break;
      await sleep(250);
    }
    const rs = mine();
    const seen = await s.seen();
    await s.shot('conditions');
    const last = rs.reduce((a, r) => Math.max(a, r.doneAt ?? Infinity), at);
    return { conditions: { status: !shown ? 'timeout' : rs.some((r) => r.doneAt === null) ? 'timeout' : 'ok',
      ms: shown ? Math.round(seen['map-horizons'] - pageAt) : null, dataMs: Number.isFinite(last) ? last - at : null,
      files: rs.length, failed: rs.filter((r) => r.failed).length } };
  }),
  async chart(site, profile, o) {
    const s = await session(site, `${site}/?regs=${LINK.item}&view=conditions`, profile, o);
    try {
      // the chart is the image labelled with its span (core `chartLabel`); its normal range, the key's "10th–90th"
      const probe = `(() => ({ t: performance.now(), chart: !!document.querySelector('[aria-label="the last 72 hours"]'),
        band: (document.body.innerText || '').includes('10th–90th') }))()`;
      let chartMs = null, bandMs = null;
      const end = Date.now() + s.P.cap;
      while (Date.now() < end && bandMs === null) {
        const r = await s.b.evaluate(probe).catch(() => null);
        if (r?.chart && chartMs === null) chartMs = Math.round(r.t);
        if (r?.chart && r.band) bandMs = Math.round(r.t);
        await sleep(250);
      }
      await s.shot('chart');
      return { chart: { status: bandMs !== null ? 'ok' : chartMs !== null ? 'no-band' : 'timeout', ms: chartMs, bandMs },
               ...(await s.tally()) };
    } finally { await s.close(); }
  },
  /**
   * (T282) WHEN THE APP TAKES OVER: a water's own page (`/water/<slug>`, `--water`) → its prerendered pane handed over
   * to the app's (the hand-over drops `#prerender`), and `/` → the app's map chrome. ms from the navigation, plus the
   * app's own bytes fetched (JS + CSS: kind `app`).
   */
  async takeover(site, profile, o) {
    const out = {};
    const w = await session(site, `${site}/water/${o.water}`, profile, o);
    try {
      const ok = await w.until(`'handover' in window.__t254.seen`, w.P.cap);
      const seen = await w.seen(), t = await w.tally();
      out.water = ok ? { status: 'ok', ms: Math.round(seen.handover), paneMs: Math.round(seen.prerender ?? NaN), appKB: t.kB.app }
                     : { status: 'timeout', note: (await w.text()).slice(0, 120), appKB: t.kB.app };
    } finally { await w.close(); }
    const h = await session(site, `${site}/`, profile, o);
    try {
      const ok = await h.until(`!!document.querySelector('[data-testid=map-day]')`, h.P.cap);
      const seen = await h.seen(), t = await h.tally();
      out.home = ok ? { status: 'ok', ms: Math.round(seen['map-day']), appKB: t.kB.app } : { status: 'timeout', appKB: t.kB.app };
    } finally { await h.close(); }
    return out;
  },
  async pdf(site, profile, o) {
    const s = await session(site, `${site}/?regs=${LINK.item}`, profile, o);
    try {
      if (!await s.until(`!!document.querySelector('[data-testid=regs-header]')`, s.P.cap)) return { pdf: { status: 'timeout', note: 'no regulations page' }, ...(await s.tally()) };
      await sleep(1500);
      const pageAt = await s.pageNow(), at = Date.now();
      if (!await s.press('Rule book page', { prefix: true })) return { pdf: { status: 'no-link' }, ...(await s.tally()) };
      const cap = Math.min(s.P.cap, 180_000);
      const done = await s.until(`window.__t254.pdfAt !== null || !!document.querySelector('[data-testid=synopsis-retry]') || !!document.querySelector('[data-testid=synopsis-offer]')`, cap);
      await s.shot('pdf');
      const st = await s.b.evaluate('window.__t254');
      const seen = st.seen ?? {};
      const got = s.b.requests.filter((r) => r.at >= at && kindOf(r.url, site) === 'pdf');
      const pct = await s.b.evaluate(`(document.querySelector('[data-testid=synopsis-loading]')?.innerText.match(/(\\d+)%/) || [])[1] ?? null`).catch(() => null);
      const pdf = st.pdfAt !== null ? { status: 'ok', ms: Math.round(st.pdfAt - pageAt), bytes: st.pdfBytes }
        : seen['synopsis-offer'] ? { status: 'offered', ms: Math.round(seen['synopsis-offer'] - pageAt), note: (await s.b.evaluate(`document.querySelector('[data-testid=synopsis-offer]')?.innerText ?? ''`)).replace(/\s+/g, ' ') }
        : seen['synopsis-retry'] ? { status: 'failed', ms: Math.round(seen['synopsis-retry'] - pageAt) }
        : { status: 'timeout', note: `${pct ?? '?'}% after ${Math.round(cap / 1000)} s` };
      return { pdf: { ...pdf, requests: got.length }, ...(await s.tally()) };
    } finally { await s.close(); }
  },
};

/** offline-back: paint online, the line drops, a named tap; the line comes back. */
async function offlineBack(site, o) {
  const s = await session(site, `${site}/`, 'offline-back', o);
  try {
    const p = await paint(s);
    if (p.status !== 'ok') return { paint: p, ...(await s.tally()) };
    await s.closeSheets();
    await s.setOffline(true);
    let opened = false;
    for (let i = 0; i < 6 && !opened; i++) { await s.closeSheets(); await s.tapWater(TAPS.named);
      opened = await s.until(`!!document.querySelector('[data-testid=water-title]')?.innerText.trim()`, 2500); }
    await sleep(15_000 - 6 * 2500 > 0 ? 15_000 - 6 * 2500 : 0);
    const offlineShown = await s.text();
    const offlineRegs = await s.b.evaluate(`!!document.querySelector('[data-testid=regs-header]')`);
    await s.shot('offline');
    await s.setOffline(false);
    const backAt = await s.pageNow();
    let recovered = 'stuck';
    if (await s.until(`!!document.querySelector('[data-testid=regs-header]')`, 60_000)) recovered = 'by itself';
    else {
      await s.closeSheets(); await s.tapWater(TAPS.named);
      if (await s.until(`!!document.querySelector('[data-testid=regs-header]')`, 60_000)) recovered = 'after a re-tap';
    }
    const seen = await s.seen();
    await s.shot('back');
    return { paint: p, offline: { opened, regsWhileOffline: offlineRegs, shown: offlineShown.slice(0, 200) },
             back: { status: recovered, ms: seen['regs-header'] ? Math.max(0, Math.round(seen['regs-header'] - backAt)) : null }, ...(await s.tally()) };
  } finally { await s.close(); }
}

/** A static server for an exported web build (SPA fallback to index.html), gzipping text and fonts as a CDN does
 *  (staging's Cloudflare does: an uncompressed 3.7 MB bundle would measure the server, not the app). */
export function serve(dir, port) {
  const packed = new Map();
  const gz = (p) => { let b = packed.get(p); if (!b) { b = zlib.gzipSync(fs.readFileSync(p), { level: 6 }); packed.set(p, b); } return b; };
  const types = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json',
                  '.wasm': 'application/wasm', '.png': 'image/png', '.svg': 'image/svg+xml', '.ico': 'image/x-icon',
                  '.ttf': 'font/ttf', '.woff2': 'font/woff2' };
  return new Promise((res) => {
    const srv = http.createServer((req, rsp) => {
      let p = path.join(dir, decodeURIComponent(new URL(req.url, 'http://x').pathname));
      if (!p.startsWith(dir) || !fs.existsSync(p) || fs.statSync(p).isDirectory()) p = path.join(dir, 'index.html');
      const type = types[path.extname(p)] ?? 'application/octet-stream';
      if (/gzip/.test(req.headers['accept-encoding'] ?? '') && /^(text|font)\/|json|svg/.test(type)) {
        const body = gz(p);
        rsp.writeHead(200, { 'content-type': type, 'content-encoding': 'gzip', 'content-length': body.length });
        return rsp.end(body);
      }
      rsp.writeHead(200, { 'content-type': type });
      fs.createReadStream(p).pipe(rsp);
    }).listen(port, () => res(srv));
  });
}

const fmt = (ms) => (ms === null || ms === undefined || Number.isNaN(ms) ? '—' : ms >= 10_000 ? `${(ms / 1000).toFixed(0)} s` : `${(ms / 1000).toFixed(1)} s`);
const cell = (r) => (!r ? '—' : r.status === 'ok' ? fmt(r.ms) : r.status === 'offered' ? `offered (${fmt(r.ms)})` : `${r.status}${r.note ? ` (${r.note})` : ''}`);

export function table(rows) {
  const out = ['| profile | map chrome | map painted | named tap → regs | unnamed tap → regs | link → regs | Conditions shown / data | water chart / its normal range | PDF | failed requests | console errors |',
               '|---|---|---|---|---|---|---|---|---|---|---|'];
  for (const r of rows) {
    const S = r.steps;
    const paints = Object.values(S).map((x) => x?.paint).filter((p) => p?.status === 'ok');
    const med = (k) => { const v = paints.map((p) => p[k]).sort((a, b) => a - b); return v.length ? v[Math.floor(v.length / 2)] : null; };
    const failed = {}; let errs = 0;
    for (const x of Object.values(S)) { for (const [k, n] of Object.entries(x?.failed ?? {})) failed[k] = (failed[k] ?? 0) + n; errs += x?.consoleErrors ?? 0; }
    const c = S.conditions?.conditions;
    const ob = S['offline-back'];
    out.push(`| ${PROFILES[r.profile].label} | ${fmt(med('chrome'))} | ${fmt(med('painted'))} | ${cell(S.named?.regs)} | ${cell(S.unnamed?.regs)} | `
      + `${cell(S.link?.regs)} | ${c ? (c.status === 'ok' ? `${fmt(c.ms)} / ${fmt(c.dataMs)}` : cell(c)) : '—'} | `
      + `${S.chart?.chart ? `${fmt(S.chart.chart.ms)} / ${S.chart.chart.bandMs === null ? S.chart.chart.status : fmt(S.chart.chart.bandMs)}` : '—'} | ${cell(S.pdf?.pdf)} | `
      + `${Object.entries(failed).map(([k, n]) => `${k} ${n}`).join(', ') || '0'} | ${errs} |`
      + (ob ? `\n|  ↳ offline tap | opened: ${ob.offline?.opened ? 'yes' : 'no'} | regs offline: ${ob.offline?.regsWhileOffline ? 'yes' : 'no'} | back: ${ob.back?.status} | ${fmt(ob.back?.ms)} after | | | | | | |` : ''));
  }
  return out.join('\n');
}

async function main() {
  const seed = Number(opt('--seed', '254'));
  let site = opt('--url', 'http://localhost:39260').replace(/\/$/, '');
  if (/^https:\/\/(www\.)?canifishthis\.ca/.test(site)) throw new Error('never production: use a local build or staging');
  const dir = opt('--serve', null);
  const srv = dir ? await serve(path.resolve(dir), Number(new URL(site).port || 39260)) : null;
  const o = { seed, low: opt('--low', null), shots: opt('--shots', null), desktop: has('--desktop'), water: opt('--water', 'vedder-river') };
  const profiles = opt('--profiles', 'slow-3g,2g,flaky,offline-back').split(',');
  const steps = opt('--steps', STEPS.join(',')).split(',');
  const rows = [];
  for (const profile of profiles) {
    if (!PROFILES[profile]) throw new Error(`no profile ${profile}`);
    const row = { profile, steps: {} };
    if (PROFILES[profile].offlineBack) {
      row.steps['offline-back'] = await offlineBack(site, o);
      console.log(profile, JSON.stringify(row.steps['offline-back']));
    } else for (const st of steps) {
      if (!STEP[st]) throw new Error(`no step ${st}`);
      try { row.steps[st] = await STEP[st](site, profile, o); }
      catch (e) { row.steps[st] = { error: String(e?.message ?? e) }; }
      console.log(profile, st, JSON.stringify(row.steps[st]));
    }
    rows.push(row);
  }
  const md = table(rows);
  console.log(`\n${site} · ${new Date().toISOString().slice(0, 16)} · low=${o.low ?? 'app decides'}\n${md}`);
  if (opt('--json', null)) fs.writeFileSync(opt('--json'), JSON.stringify({ site, at: new Date().toISOString(), low: o.low, rows }, null, 1));
  if (opt('--md', null)) fs.writeFileSync(opt('--md'), md + '\n');
  srv?.close();
}

if (import.meta.url === `file://${process.argv[1]}`) await main();
