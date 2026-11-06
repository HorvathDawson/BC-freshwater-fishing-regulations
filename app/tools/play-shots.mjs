#!/usr/bin/env node
// GOOGLE PLAY SCREENSHOTS — headless Chrome (never a visible window) on the STAGING site, light theme, first-run notice /
// walkthrough / in-season sheet seeded as seen (as smoke-staging.mjs does). Phone: 360x640 CSS px at dpr 3 = 1080x1920.
// Tablets: 7" 1200x1920 (600x960 @2), 10" 1600x2560 (800x1280 @2). App Store sizes: iphone67, iphone55, ipad13 (T319).
//   node tools/play-shots.mjs [outDir] [--only a,b,...]
// The saved-spot scene seeds one sample spot (localStorage), the way spot-shots.mjs does; everything else is live data.
import fs from 'node:fs'; import path from 'node:path';
import { open } from './regs-web/cdp.mjs';
const args = process.argv.slice(2);
const ONLY = args.includes('--only') ? args[args.indexOf('--only') + 1].split(',') : null;
const OUT = path.resolve(args.find((a, i) => !a.startsWith('--') && args[i - 1] !== '--only') ?? '../handoff/PLAN/app-integration/store/graphics');
const SITE = 'https://staging.canifishthis.ca';
fs.mkdirSync(OUT, { recursive: true });
const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const rd = (f, re) => re.exec(fs.readFileSync(path.join(ROOT, f), 'utf8'))?.[1];
// (T319) the mode hints (TAB_TOURS: "Where you are" over the bottom bar, Runs, Conditions, Salmon) seen too, one per line
const TOURS = [...(rd('packages/core/src/help.ts', /TAB_TOURS = \{([^}]*)\}/) ?? '').matchAll(/"(tour-[a-z]+-\d+)"/g)].map((m) => m[1]);
if (!TOURS.length) throw new Error('play-shots: no TAB_TOURS ids in packages/core/src/help.ts');
const visit = Date.now() - 4 * 86400000, iso = (t) => new Date(t).toISOString();
const sample = (t, c) => ({ at: iso(t), tempC: c, cloudPct: 40, pressureHpa: 1012, humidityPct: 63 });
const SPOT = { id: 'play-spot', createdAt: visit, updatedAt: visit, visitedAt: visit, lat: 49.0985, lon: -121.9585, item: 'gnis:3062', waterName: 'Vedder River',
  title: 'Bridge pool', notes: 'Fished the tail-out below the bridge from 7 to 10. Three rainbow on a small pheasant tail, one bull trout on a streamer. Water a little tea-coloured, still dropping.',
  photos: [], reading: { station: 'Vedder River at Vedder Crossing', discharge: 41.2, level: null, parameter: null, percentile: 0.42, at: iso(visit) },
  weather: { tempC: 14, windKph: 9, windDir: 250, rain3h: 0, pressureHpa: 1012, code: 2, at: iso(visit), window: { before: sample(visit - 3600e3, 13), at: sample(visit, 14), after: sample(visit + 3600e3, 15) } }, trace: null, panel: null };
const SEEN = `try { localStorage.setItem('canifishthis.firstRun.v1', ${JSON.stringify(rd('packages/core/src/menu/legal.ts', /FIRST_RUN_VERSION = "([^"]+)"/))});
  localStorage.setItem('canifishthis.walkthrough.v1', ${JSON.stringify([rd('packages/core/src/help.ts', /WALKTHROUGH_VERSION = "([^"]+)"/), ...TOURS].join('\n'))});
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
  // (T319) what is kept on the phone, region by region: the "works offline" shot
  '09-offline': async (b) => { await mapReady(b); await sleep(2000); await click(b, find('More', '[role=tab]')); await sleep(1500);
    await click(b, find('Offline maps', '[role=button],[role=link],[aria-label]')); await sleep(4000); },
};
const URLS = { '02-regulations': '/water/vedder-river', '03-conditions': '/?regs=gnis:3062&view=conditions' };
const SIZES = { phone: { width: 360, height: 640, dpr: 3, mobile: true, pre: '' }, tablet7: { width: 600, height: 960, dpr: 2, mobile: true, pre: 'tablet7-' }, tablet10: { width: 800, height: 1280, dpr: 2, mobile: true, pre: 'tablet10-' },
  // (T319) App Store: 6.7" iPhone 1290x2796 (430x932 @3), 5.5" 1242x2208 (414x736 @3), 13" iPad 2048x2732 (1024x1366 @2)
  iphone67: { width: 430, height: 932, dpr: 3, mobile: true, pre: 'iphone67-' }, iphone55: { width: 414, height: 736, dpr: 3, mobile: true, pre: 'iphone55-' },
  ipad13: { width: 1024, height: 1366, dpr: 2, mobile: true, pre: 'ipad13-' } };
const sizeNames = (process.env.SIZES ?? 'phone').split(',');
let failed = 0;
for (const sn of sizeNames) for (const [name, fn] of Object.entries(SCENES)) {
  if (ONLY && !ONLY.includes(name.slice(0, 2)) && !ONLY.includes(name)) continue;
  const z = SIZES[sn];
  const b = await open(SITE + (URLS[name] ?? '/'), { ...z, init: SEEN });
  try {
    await fn(b);
    const { data } = await b.send('Page.captureScreenshot', { format: 'png' });
    const f = path.join(OUT, `${z.pre}${name}.png`); fs.writeFileSync(f, Buffer.from(data, 'base64')); console.log(f);
  } catch (e) { failed++; console.error(sn, name, e.message); }
  finally { await b.close(); }
}
process.exit(failed ? 1 : 0);
