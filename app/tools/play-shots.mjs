#!/usr/bin/env node
// GOOGLE PLAY SCREENSHOTS — headless Chrome (never a visible window) on the STAGING site, light theme, first-run notice /
// walkthrough / in-season sheet seeded as seen (as smoke-staging.mjs does). Phone: 360x640 CSS px at dpr 3 = 1080x1920.
// Tablets: 7" 1200x1920 (600x960 @2), 10" 1600x2560 (800x1280 @2). App Store sizes: iphone67, iphone55, ipad13 (T319).
//   node tools/play-shots.mjs [outDir] [--only a,b,...]
// The saved-spot scene seeds one sample spot (localStorage), the way spot-shots.mjs does; everything else is live data.
import fs from 'node:fs'; import path from 'node:path';
import { open } from './regs-web/cdp.mjs';
import { checkScene } from './store-shots/checks.mjs';
const args = process.argv.slice(2);
const ONLY = args.includes('--only') ? args[args.indexOf('--only') + 1].split(',') : null;
const OUT = path.resolve(args.find((a, i) => !a.startsWith('--') && args[i - 1] !== '--only') ?? '../handoff/PLAN/app-integration/store/graphics');
const SITE = process.env.STORE_SHOTS_SITE ?? 'https://staging.canifishthis.ca';
// (T704) THE CLOCK IS PINNED: PIN_AT (an ISO instant) is what the page's Date says when it loads, then it runs on (timers and the
// map's animations keep their pace). Unset = the page's own clock (the old behaviour). store-shots/run.mjs always sets it.
const PIN = process.env.PIN_AT ? Date.parse(process.env.PIN_AT) : null;
if (process.env.PIN_AT && Number.isNaN(PIN)) throw new Error('play-shots: PIN_AT is not a date: ' + process.env.PIN_AT);
const PIN_JS = PIN === null ? '' : `(() => { const R = Date, off = ${PIN} - R.now(); const N = () => R.now() + off;
  class D extends R { constructor(...a) { a.length ? super(...a) : super(N()); } static now() { return N(); } } globalThis.Date = D; })();`;
fs.mkdirSync(OUT, { recursive: true });
const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const rd = (f, re) => re.exec(fs.readFileSync(path.join(ROOT, f), 'utf8'))?.[1];
const visit = Date.now() - 4 * 86400000, iso = (t) => new Date(t).toISOString();
const sample = (t, c) => ({ at: iso(t), tempC: c, cloudPct: 40, pressureHpa: 1012, humidityPct: 63 });
const SPOT = { id: 'play-spot', createdAt: visit, updatedAt: visit, visitedAt: visit, lat: 49.0985, lon: -121.9585, item: 'gnis:3062', waterName: 'Vedder River',
  title: 'Bridge pool', notes: 'Fished the tail-out below the bridge from 7 to 10. Three rainbow on a small pheasant tail, one bull trout on a streamer. Water a little tea-coloured, still dropping.',
  photos: [], reading: { station: 'Vedder River at Vedder Crossing', discharge: 41.2, level: null, parameter: null, percentile: 0.42, at: iso(visit) },
  weather: { tempC: 14, windKph: 9, windDir: 250, rain3h: 0, pressureHpa: 1012, code: 2, at: iso(visit), window: { before: sample(visit - 3600e3, 13), at: sample(visit, 14), after: sample(visit + 3600e3, 15) } }, trace: null, panel: null };
const SEEN = `try { localStorage.setItem('canifishthis.firstRun.v1', ${JSON.stringify(rd('packages/core/src/menu/legal.ts', /FIRST_RUN_VERSION = "([^"]+)"/))});
  localStorage.setItem('canifishthis.walkthrough.v1', ${JSON.stringify([rd('packages/core/src/help.ts', /WALKTHROUGH_VERSION = "([^"]+)"/)].join('\n'))});
  localStorage.setItem('canifishthis.inseasonSeen.v1', '["*"]');
  localStorage.setItem('canifishthis.spots.v1', ${JSON.stringify(JSON.stringify([SPOT]))}); } catch {}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const until = async (b, expr, ms = 60000) => { for (let t = 0; t < ms; t += 250) { if (await b.evaluate(expr).catch(() => false)) return; await sleep(250); } throw new Error('timed out: ' + expr); };
const find = (label, sel = '[aria-label]') => `[...document.querySelectorAll(${JSON.stringify(sel)})].find((e) => (e.getAttribute('aria-label') || e.textContent).trim().startsWith(${JSON.stringify(label)}) && e.getBoundingClientRect().width > 0)`;
async function click(b, expr) {
  const at = await b.evaluate(`(() => { const e = ${expr}; if (!e) return null; const r = e.getBoundingClientRect(); return [r.x + r.width / 2, r.y + r.height / 2]; })()`);
  if (!at) throw new Error('nothing to press: ' + expr);
  for (const type of ['mouseMoved', 'mousePressed', 'mouseReleased']) await b.send('Input.dispatchMouseEvent', { type, x: at[0], y: at[1], button: 'left', clickCount: 1 });
  await sleep(600);
}
const tab = (b, name) => click(b, find(name, '[role=tab],[aria-label]'));
const mapReady = (b) => until(b, `!!document.querySelector('[data-testid=map-day]')`, 120000);

const SCENES = {
  '01-map': async (b) => { await mapReady(b); await sleep(9000); },
  '02-regulations': async (b) => { await until(b, `!!document.querySelector('[data-testid=water-title]')?.innerText.trim()`, 120000); await sleep(4000); },
  '03-conditions': async (b) => { await until(b, `!!document.querySelector('[data-testid=water-title]')?.innerText.trim()`, 120000); await sleep(9000); },
  '04-salmon': async (b) => { await mapReady(b); await sleep(4000); await click(b, `document.querySelector('[data-testid=regs-paint-salmon]')`); await sleep(9000); },
  '05-runs': async (b) => { await mapReady(b); await sleep(3000); await click(b, find('Runs', '[role=tab]')); await sleep(9000); },
  '06-search': async (b) => { await mapReady(b); await sleep(3000); await click(b, `document.querySelector('[data-testid=search-field]')`);
    await b.send('Input.insertText', { text: 'Vedder' }); await sleep(4000); },
  '07-saved-spot': async (b) => { await mapReady(b); await sleep(2000); await click(b, find('Spots', '[role=tab]')); await until(b, `!!document.querySelector('[data-testid=spots-screen]')`);
    await click(b, find('Bridge pool', '[role=button]')); await until(b, `!!document.querySelector('[data-testid=spot-when]')`); await sleep(6000); },
  '08-glossary': async (b) => { await mapReady(b); await sleep(2000); await click(b, find('More', '[role=tab]')); await sleep(1500);
    await click(b, find('Glossary', '[role=button],[role=link],[aria-label]')); await sleep(4000); },
  // (T319, T704) the "works offline" shot: the dated list of what the phone holds
  '09-offline': async (b) => { await mapReady(b); await sleep(2000); await click(b, find('More', '[role=tab]')); await sleep(1500);
    // (T703) the offline / mobile-data controls live under More -> Settings and the phone-only mobile-data control is not drawn on the
    // web, so the offline story is the More data list: every source with its date, scrolled so the dated rows are whole
    await b.evaluate(`(() => { const el = [...document.querySelectorAll('*')].find((e) => e.children.length === 0 && e.textContent.trim() === 'Synopsis' && e.getBoundingClientRect().width > 0);
      el.scrollIntoView({ block: 'start' }); const sc = (() => { let n = el.parentElement; while (n && n.scrollHeight <= n.clientHeight + 1) n = n.parentElement; return n; })(); if (sc) sc.scrollTop -= 330; })()`);
    await sleep(1500); },
};
// (T319, owner 2026-11-12) the second set: conditions as big as rules. Each scene is one half or one whole of a store shot;
// the store-shots tool stacks two captures (same browser size, same camera) into one picture where a shot says `compose`.
const scrollTo = (b, id) => b.evaluate(`document.querySelector('[data-testid=${id}]').scrollIntoView({ block: 'start' })`);
const text = (b) => b.evaluate('document.body.innerText');
// the Conditions map framed one zoom out (reaches coloured for the Chilliwack / Harrison watersheds), after the watershed
// days have loaded; every scene that composes uses this same camera
async function condMap(b) {
  await mapReady(b); await sleep(2000);
  await click(b, find('Conditions', '[role=tab]')); await sleep(3000);
  // zoom out by the wheel until the scale bar reads 10 km (the same camera for every scene that composes)
  const scale = () => b.evaluate(`document.querySelector('.maplibregl-ctrl-scale')?.textContent.replace(/\\s+/g, ' ').trim()`);
  for (let i = 0; i < 12 && (await scale()) !== '10 km'; i++) {
    for (let k = 0; k < 3; k++) { await b.send('Input.dispatchMouseEvent', { type: 'mouseWheel', x: 180, y: 250, deltaX: 0, deltaY: 100 }); await sleep(250); }
    await sleep(2500); }
  if ((await scale()) !== '10 km') throw new Error('condMap: the map did not reach the 10 km scale, it reads ' + JSON.stringify(await scale()));
  await until(b, `!/still loading/.test(document.body.innerText) && /reaches coloured/.test(document.body.innerText)`, 90000); await sleep(8000);
}
Object.assign(SCENES, {
  // the rules tab: the year bar and the species table in one view (scrolled so the bar sits under the water's title)
  // (owner 2026-11-12: "salmon are a thing too") scrolled so the SALMON (DFO) section — its own year bar, Open/Closed today and its
  // Salmon row — sits under the water's tabs with the tail of the game-fish table above it: trout/char rows and salmon in one view.
  // The Vedder is open for salmon in the fall; out of season the salmon bar and row still show, closed.
  // (owner 2026-11-12 "Stacked: trout top, salmon bottom") store shot 10 is two captures of the Vedder, same day: 18 = trout/game-fish year bar
  // and first rows, 19 = the SALMON (DFO) section. The composite is named 10-regs-bar-table in store-shots.json.
  '18-regs-trout': async (b) => { await until(b, `!!document.querySelector('[data-testid=keep-strip]')`, 120000); await sleep(3000); await scrollTo(b, 'keep-strip'); await sleep(1500); },
  '19-regs-salmon': async (b) => { await until(b, `!!document.querySelector('[data-testid=keep-strip]')`, 120000); await sleep(3000);
    const at = await b.evaluate(`(() => { const e = [...document.querySelectorAll('*')].filter((x) => x.children.length === 0 && /^salmon \\(dfo\\)$/i.test(x.textContent.trim())).pop(); if (!e) return 'no SALMON (DFO) section';
      let p = e.parentElement; while (p && p.scrollHeight <= p.clientHeight + 2) p = p.parentElement; if (!p) return 'nothing scrolls';
      p.scrollTop += e.getBoundingClientRect().top - 275; return 'ok'; })()`);
    if (at !== 'ok') throw new Error('19-regs-salmon: ' + at);
    await sleep(1500); },
  // the gauge's chart with the forecast drawn on it (the model lines are the default on the chart: CLEVER, COFFEE, ELF)
  '11-cond-chart': async (b) => { await until(b, `!!document.querySelector('[data-testid=gauge-card]')`, 120000); await sleep(6000);
    await until(b, `!!document.querySelector('[data-testid=chart-model-CLEVER]')`); await b.evaluate(`document.querySelector('[data-testid=chart-legend]').scrollIntoView({ block: 'end' })`); await sleep(2500); },
  '12-cond-change': async (b) => { await condMap(b); await click(b, `document.querySelector('[data-testid=map-horizons-change]')`); await sleep(7000); },
  '13-cond-forecast': async (b) => { await condMap(b); await click(b, `document.querySelector('[data-testid=map-horizons-days]')`); await sleep(1500);
    // the fourth day out: far enough that the forecast differs from today
    await click(b, `[...document.querySelectorAll('[role=button],button')].filter((e) => /^(Mon|Tue|Wed|Thu|Fri|Sat|Sun) \\d+$/.test(e.textContent.trim()) && e.getBoundingClientRect().width > 0)[3]`);
    await sleep(7000); },
  // a river split into its sections: the main map framed on the water (its pane closed), then one section's own rules
  '14-split-map': async (b) => { await until(b, `!!document.querySelector('[data-testid=water-close]')`, 120000); await sleep(3000);
    await click(b, `document.querySelector('[data-testid=water-close]')`); await mapReady(b); await sleep(25000); },
  '15-split-rules': async (b) => { await until(b, `!!document.querySelector('[data-testid=water-title]')?.innerText.trim()`, 120000); await sleep(5000);
    // the tab press can land before the page is live: press again until the part stepper is there
    for (let i = 0; i < 6 && !(await b.evaluate(`!!document.querySelector('[data-testid=water-map-step-up]')`)); i++) { await click(b, `document.querySelector('[data-testid=water-view-map]')`); await sleep(8000); }
    await until(b, `!!document.querySelector('[data-testid=water-map-step-up]')`, 30000); await sleep(3000);
    b.facts.parts = Number((await b.evaluate('document.body.innerText')).match(/PART \d+ OF (\d+)/i)?.[1] ?? 0);
    for (let i = 0; i < 2; i++) { await click(b, `document.querySelector('[data-testid=water-map-step-up]')`); await sleep(2500); }
    await click(b, `document.querySelector('[data-testid=water-map-regs]')`);
    await until(b, `!!document.querySelector('[data-testid=keep-strip]')`, 60000); await sleep(3000); await scrollTo(b, 'keep-strip'); await sleep(1500); },
});
const URLS = { '02-regulations': '/water/vedder-river', '03-conditions': '/?regs=gnis:3062&view=conditions',
  '18-regs-trout': '/water/vedder-river', '19-regs-salmon': '/water/vedder-river', '11-cond-chart': '/?regs=gnis:3062&view=conditions', '14-split-map': '/water/chilliwack-river', '15-split-rules': '/water/chilliwack-river' };
const SIZES = { phone: { width: 360, height: 640, dpr: 3, mobile: true, pre: '' }, tablet7: { width: 600, height: 960, dpr: 2, mobile: true, pre: 'tablet7-' }, tablet10: { width: 800, height: 1280, dpr: 2, mobile: true, pre: 'tablet10-' },
  // (T319) App Store: 6.7" iPhone 1290x2796 (430x932 @3), 5.5" 1242x2208 (414x736 @3), 13" iPad 2048x2732 (1024x1366 @2)
  iphone67: { width: 430, height: 932, dpr: 3, mobile: true, pre: 'iphone67-' }, iphone55: { width: 414, height: 736, dpr: 3, mobile: true, pre: 'iphone55-' },
  ipad13: { width: 1024, height: 1366, dpr: 2, mobile: true, pre: 'ipad13-' } };
const sizeNames = (process.env.SIZES ?? 'phone').split(',');
let failed = 0; const report = {};   // (T704) scene -> 'ok' | the reason it was not captured, written beside the shots
for (const sn of sizeNames) for (const [name, fn] of Object.entries(SCENES)) {
  if (ONLY && !ONLY.includes(name.slice(0, 2)) && !ONLY.includes(name)) continue;
  const z = SIZES[sn];
  const b = await open(SITE + (URLS[name] ?? '/'), { ...z, init: PIN_JS + SEEN });
  try {
    b.facts = {}; await fn(b);
    await checkScene(b, name);   // (T704) the page holds the shot's content, or nothing is captured
    const { data } = await b.send('Page.captureScreenshot', { format: 'png' });
    const f = path.join(OUT, `${z.pre}${name}.png`); fs.writeFileSync(f, Buffer.from(data, 'base64')); console.log(f); report[`${sn}/${name}`] = 'ok';
  } catch (e) { failed++; report[`${sn}/${name}`] = e.message; console.error(sn, name, e.message); }
  finally { await b.close(); }
}
fs.writeFileSync(path.join(OUT, 'play-shots-report.json'), JSON.stringify({ site: SITE, pinAt: process.env.PIN_AT ?? null, scenes: report }, null, 2) + '\n');
process.exit(failed ? 1 : 0);
