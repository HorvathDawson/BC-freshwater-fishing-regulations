#!/usr/bin/env node
// GOOGLE PLAY SCREENSHOTS — headless Chrome (never a visible window) on a STORE-SHOTS web export (store-shots/site.mjs, T1076), light theme, first-run notice /
// walkthrough / in-season sheet seeded as seen (as smoke-staging.mjs does). Phone: 360x640 CSS px at dpr 3 = 1080x1920.
// Tablets: 7" 1200x1920 (600x960 @2), 10" 1600x2560 (800x1280 @2). App Store sizes: iphone67, iphone55, ipad13 (T319).
//   node tools/play-shots.mjs [outDir] [--only a,b,...]
// The saved-spot scene seeds one sample spot (localStorage), the way spot-shots.mjs does; everything else is live data.
import fs from 'node:fs'; import path from 'node:path';
import { open } from './regs-web/cdp.mjs';
import { checkScene } from './store-shots/checks.mjs';
import { assertStoreShotsSite, describeSite } from './store-shots/site.mjs';
import { needsBuild, NEEDS_STORE_BUILD } from './store-shots/scenes.mjs';
const args = process.argv.slice(2);
const ONLY = args.includes('--only') ? args[args.indexOf('--only') + 1].split(',') : null;
const OUT = path.resolve(args.find((a, i) => !a.startsWith('--') && args[i - 1] !== '--only') ?? '../handoff/PLAN/app-integration/store/graphics');
// (T1076) no default: a scene that needs the store-shots build (store-shots/scenes.mjs) is shot from STORE_SHOTS_SITE, which must be a store-shots build
// (store-shots/site.mjs builds and serves one; store-shots.mjs / run.mjs pass it); anything without the marker is refused before the first scene.
// (T1072, owner 2026-11-25 "could use website too for most pictures much quicker") STORE_SHOTS_WEB is a DEPLOYED site (staging or production): every scene
// that does NOT need the build is shot from it, so no export has to be built for them. Without it every scene comes from STORE_SHOTS_SITE, as before.
// A deployed site shows the RELEASED app, not this checkout: the report says, per scene, which site it came from and what that site says of itself.
const WEB = process.env.STORE_SHOTS_WEB ? process.env.STORE_SHOTS_WEB.replace(/\/$/, '') : null;
const SITE = process.env.STORE_SHOTS_SITE ? process.env.STORE_SHOTS_SITE.replace(/\/$/, '') : null;
const selected = (name) => !ONLY || ONLY.includes(name.slice(0, 2)) || ONLY.includes(name);
const siteFor = (name) => (WEB && !needsBuild(name) ? WEB : SITE);
const sceneNames = () => [...Object.keys(SCENES)];   // (SCENES is defined below; read lazily)
// (T704) THE CLOCK IS PINNED: PIN_AT (an ISO instant) is what the page's Date says when it loads, then it runs on (timers and the
// map's animations keep their pace). Unset = the page's own clock (the old behaviour). store-shots/run.mjs always sets it.
const PIN = process.env.PIN_AT ? Date.parse(process.env.PIN_AT) : null;
if (process.env.PIN_AT && Number.isNaN(PIN)) throw new Error('play-shots: PIN_AT is not a date: ' + process.env.PIN_AT);
const PIN_JS = PIN === null ? '' : `(() => { const R = Date, off = ${PIN} - R.now(); const N = () => R.now() + off;
  class D extends R { constructor(...a) { a.length ? super(...a) : super(N()); } static now() { return N(); } } globalThis.Date = D; })();`;
fs.mkdirSync(OUT, { recursive: true });
const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const rd = (f, re) => re.exec(fs.readFileSync(path.join(ROOT, f), 'utf8'))?.[1];
// (T319) the visit: 8:30 four days before the pinned day, B.C. time (permanent UTC-7), rated 4 of 5
const bcDay = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Vancouver', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date((PIN ?? Date.now()) - 4 * 86400000));
const visit = Date.parse(`${bcDay}T08:30:00-07:00`), iso = (t) => new Date(t).toISOString();
const sample = (t, c) => ({ at: iso(t), tempC: c, cloudPct: 40, pressureHpa: 1012, humidityPct: 63 });
const SPOT = { id: 'play-spot', createdAt: visit, updatedAt: visit, visitedAt: visit, lat: 49.0985, lon: -121.9585, item: 'gnis:3062', waterName: 'Vedder River',
  title: 'Bridge pool', rating: 4, notes: 'Fished the tail-out below the bridge from 7 to 10. Three rainbow on a small pheasant tail, one bull trout on a streamer. Water a little tea-coloured, still dropping.',
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
// (T1072, owner 2026-11-25 "pictures should be captured quicker") WAIT ON THE PAGE, not a fixed sleep: `idle` returns when three successive
// small captures of the page are identical (tiles, labels and layers have stopped arriving; the app draws no per-frame animation), after
// at least `min` ms and at most `max` ms (the content checks still run before every capture, so a page that never settles fails there).
const idle = async (b, min = 1500, max = 30000) => { const t0 = Date.now(); let prev = null, same = 0;
  for (;;) { const { data } = await b.send('Page.captureScreenshot', { format: 'jpeg', quality: 25 }); same = data === prev ? same + 1 : 0; prev = data;
    if ((same >= 2 && Date.now() - t0 >= min) || Date.now() - t0 >= max) return; await sleep(350); } };
// the map's ground width per CSS px, from the scale bar (label / bar width); null when there is no bar
const groundPerPx = (b) => b.evaluate(`(() => { const e = document.querySelector('.maplibregl-ctrl-scale'); if (!e) return null; const m = /^([\\d.]+)\\s*(km|m)$/.exec(e.textContent.replace(/\\s+/g, ' ').trim()); const w = e.getBoundingClientRect().width;
  return m && w > 0 ? Number(m[1]) * (m[2] === 'km' ? 1000 : 1) / w : null; })()`);
// ONE wheel event at a time (MapLibre piles up close events: three 50 ms apart jumped half a level), the map settled between: the scale
// bar's reading (text + width) the same twice 300 ms apart
const notch = async (b, dy) => { const z = b.size; await b.send('Input.dispatchMouseEvent', { type: 'mouseWheel', x: z.width / 2, y: z.height / 2, deltaX: 0, deltaY: dy });
  let prev = null; for (let i = 0; i < 20; i++) { await sleep(300); const t = await b.evaluate(`(() => { const e = document.querySelector('.maplibregl-ctrl-scale'); return e ? e.textContent + '|' + Math.round(e.getBoundingClientRect().width) : null; })()`); if (t === prev) return; prev = t; } };
// (T1072, owner 2026-11-25 "zoom them out a little more so it shows more content on the map views") the map views are shot ZOOM_OUT times
// wider than the camera they open on (2 = one zoom step: double the ground width), the wheel turned one event at a time until the scale bar's ground
// per pixel has grown by that factor. The cursor is the view's centre, so the same waters stay in frame.
const ZOOM_OUT = Number(process.env.ZOOM_OUT ?? 2);
// (T1072, owner review 2026-11-25) NO MAP FRAME MAY SHOW THE OUT-OF-COVERAGE BAND: south of the border the basemap is a flat grey (and a darker green-grey)
// where the data stops, which showed at the foot of the zoomed-out map views. It is found in a capture of the page, not assumed: rows whose sampled pixels
// (the left 5-70 % of the width, clear of the map controls) are that grey (a flat low-saturation grey of mid luminance, or the darker green-grey over it); 12+ rows in a row are a band. `band` returns [top, bottom] in CSS px or null.
const band = async (b) => { const { data } = await b.send('Page.captureScreenshot', { format: 'png' });
  return b.evaluate(`(async () => { const bm = await createImageBitmap(await (await fetch('data:image/png;base64,${data}')).blob()); const c = document.createElement('canvas'); c.width = bm.width; c.height = bm.height;
    const g = c.getContext('2d', { willReadFrequently: true }); g.drawImage(bm, 0, 0); const k = bm.width / innerWidth, x0 = Math.round(bm.width * 0.05), x1 = Math.round(bm.width * 0.7), n = 24;
    // the band's colours are dark, nearly neutral greys and green-greys (142,146,139) (125,135,123) (119,134,119) (126,143,126); land is lighter (lum 165+)
    const grey = (r, gg, bb) => { const hi = Math.max(r, gg, bb), lo = Math.min(r, gg, bb), lum = (r + gg + bb) / 3; return hi - lo <= 34 && lum >= 112 && lum <= 152 && gg >= r - 2; };
    const white = (r, gg, bb) => r > 235 && gg > 235 && bb > 235;
    const rows = [], wh = []; for (let y = 0; y < bm.height; y += Math.max(1, Math.round(k))) { const row = g.getImageData(x0, y, x1 - x0, 1).data; let m = 0, w = 0;
      for (let i = 0; i < n; i++) { const o = 4 * Math.floor(i * (x1 - x0) / n); if (grey(row[o], row[o + 1], row[o + 2])) m++; if (white(row[o], row[o + 1], row[o + 2])) w++; } rows.push(m >= n * 0.55); wh.push(w >= n * 0.8); }
    // a band is a run of 12+ grey rows that ENDS AT THE MAP'S FOOT (the legend panel's white rows, or the image's last rows): grey terrain elsewhere in the map is not it
    let best = null, run = 0; for (let y = 0; y <= rows.length; y++) { if (rows[y]) run++; else { if (run >= 12) { const e = y, foot = e >= rows.length - 2 || wh.slice(e, e + 8).some(Boolean); if (foot && !best) best = [e - run, e]; } run = 0; } } return best; })()`); };
// drag the map `dy` CSS px DOWN (the camera moves north), in small steps and with a pause before the release so the map keeps no momentum
async function dragDown(b, dy, fy = 0.3) { const z = b.size, x = Math.round(z.width * 0.3), y = Math.round(z.height * fy), mv = (type, yy) => b.send('Input.dispatchMouseEvent', { type, x, y: yy, button: type === 'mouseMoved' ? 'none' : 'left', buttons: type === 'mouseReleased' ? 0 : 1, clickCount: 1 });
  await mv('mouseMoved', y); await mv('mousePressed', y); const steps = 12; for (let i = 1; i <= steps; i++) { await mv('mouseMoved', y + Math.round(dy * i / steps)); await sleep(25); } await sleep(250); await mv('mouseReleased', y + dy); }
// move the camera north until the band is out of the view (the zoom stays); records the result in b.facts.band for the content check (null = none)
async function clearBand(b) { for (let i = 0; i < 4; i++) { const bd = await band(b); if (process.env.BAND_DEBUG) console.log('band', b.size.width, i, JSON.stringify(bd)); if (!bd) { if (process.env.BAND_DEBUG) { const { data: d1 } = await b.send('Page.captureScreenshot', { format: 'png' }); fs.writeFileSync(path.join(OUT, `band-null-${b.size.width}.png`), Buffer.from(d1, 'base64')); await sleep(5000); console.log('band again after 5s', JSON.stringify(await band(b))); } b.facts.band = null; return; } await dragDown(b, Math.min(b.size.height * 0.5, bd[1] - bd[0] + 16)); await idle(b, 1500, 20000); }
  b.facts.band = await band(b);
  if (b.facts.band) { const { data } = await b.send('Page.captureScreenshot', { format: 'png' }); fs.writeFileSync(path.join(OUT, `band-left-${b.size.width}x${b.size.height}.png`), Buffer.from(data, 'base64')); } }
async function zoomOut(b, factor = ZOOM_OUT) { const base = await groundPerPx(b); if (base === null) throw new Error('zoomOut: no scale bar on the map');
  for (let i = 0; i < 30 && (await groundPerPx(b)) < base * factor * 0.97; i++) await notch(b, 250);
  if ((await groundPerPx(b)) < base * factor * 0.97) throw new Error(`zoomOut: the map did not widen by ${factor}x`); }

const SCENES = {
  '01-map': async (b) => { await mapReady(b); await idle(b, 2500); await zoomOut(b); await idle(b, 1500); await clearBand(b); },
  '02-regulations': async (b) => { await until(b, `!!document.querySelector('[data-testid=water-title]')?.innerText.trim()`, 120000); await sleep(4000); },
  '03-conditions': async (b) => { await until(b, `!!document.querySelector('[data-testid=water-title]')?.innerText.trim()`, 120000); await sleep(9000); },
  '04-salmon': async (b) => { await mapReady(b); await idle(b, 1500); await click(b, `document.querySelector('[data-testid=regs-paint-salmon]')`); await idle(b, 2500); await zoomOut(b); await idle(b, 1500); await clearBand(b); },
  '05-runs': async (b) => { await mapReady(b); await idle(b, 1500); await click(b, find('Runs', '[role=tab]')); await idle(b, 2500); await zoomOut(b); await idle(b, 1500); await clearBand(b); },
  '06-search': async (b) => { await mapReady(b); await idle(b, 2000); await zoomOut(b); await idle(b, 1500); await clearBand(b); await dragDown(b, 70); await idle(b, 1500);   // (T1072) the map behind the results (the wide layouts show it beside the list) is zoomed out and band-free like the others (the extra drag: the wide layouts show the map's right side, which the band measure, taken on the left, does not see)
    await click(b, `document.querySelector('[data-testid=search-field]')`);
    await b.send('Input.insertText', { text: 'Vedder' });
    // (T1072) wait for the results themselves (the named-waters list), not a fixed sleep: a slow search index once left the list empty at tablet10
    await until(b, `/named waters/i.test(document.body.innerText) && /Vedder River/.test(document.body.innerText)`, 45000); await idle(b, 1000, 10000); },
  '07-saved-spot': async (b) => { await mapReady(b); await sleep(2000); await click(b, find('Spots', '[role=tab]')); await until(b, `!!document.querySelector('[data-testid=spots-screen]')`);
    await click(b, find('Bridge pool', '[role=button]')); await until(b, `!!document.querySelector('[data-testid=spot-when]')`); await sleep(6000); },
  '08-glossary': async (b) => { await mapReady(b); await sleep(2000); await click(b, find('More', '[role=tab]')); await sleep(1500);
    await click(b, find('Glossary', '[role=button],[role=link],[aria-label]')); await sleep(4000); },
  // (T319) the Help page of the store-shots web build (the app's "Offline and updates" lines), scrolled so the lower map key and
  // those lines are in view
  '20-help': async (b) => { await mapReady(b); await sleep(2000); await click(b, find('More', '[role=tab]')); await sleep(1500);
    await b.evaluate(`document.querySelector('[data-testid=menu-item-help]').scrollIntoView({ block: 'center' })`); await sleep(800);
    await click(b, `document.querySelector('[data-testid=menu-item-help]')`); await until(b, `!!document.querySelector('[data-testid=help-facts]')`, 30000);
    await b.evaluate(`document.querySelector('[data-testid=help-facts]').scrollIntoView({ block: 'center' })`); await sleep(2000); },
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
// the Conditions map at the 20 km scale (owner 2026-11-25: one step wider than the old 10 km), after the watershed days have loaded; every scene that composes uses this camera.
// (T319) Opened on the day's river (`river()`, store-shots/pick-river.mjs): its page closed leaves the map framed on it
// the scale bar's label depends on the latitude: a pan changes it, so frameWatershed calls this again after each move
async function condScale(b, target = '20 km') {
  // (T1072) DETERMINISTIC. The scale bar's label holds for a whole zoom level (10 km: 10 <= bar < 20 km; 20 km: 20 <= bar < 30). MapLibre's
  // wheel handler piles up close events, which made the old pair-of-notches loop step over the level and swing 5 km <-> 20 km at tablet10. So
  // ONE wheel event at a time (about a fifth of a level), the map settled between. Out until the bar reads past the target, then in to the
  // first reading of the target: always arriving from the far side, so every size and run lands at the same edge. The target is 20 km since
  // owner 2026-11-25 ("zoom them out a little more"): one step wider than the old 10 km camera.
  const scale = () => b.evaluate(`document.querySelector('.maplibregl-ctrl-scale')?.textContent.replace(/\\s+/g, ' ').trim()`);
  const km = (t) => { const m = /^([\d.]+)\s*(km|m)$/.exec(t ?? ''); return m ? Number(m[1]) / (m[2] === 'm' ? 1000 : 1) : null; };
  const want = km(target);
  for (let i = 0; i < 40 && !((km(await scale()) ?? 0) >= want * 2); i++) await notch(b, 400);
  for (let i = 0; i < 40 && (await scale()) !== target; i++) await notch(b, -400);
  if ((await scale()) !== target) throw new Error(`condMap: the map did not reach the ${target} scale, it reads ` + JSON.stringify(await scale()));
}
async function condMap(b, target = '20 km') {
  await until(b, `!!document.querySelector('[data-testid=water-close]')`, 120000); await sleep(1500);
  await click(b, `document.querySelector('[data-testid=water-close]')`);
  await mapReady(b); await sleep(2000);
  await click(b, find('Conditions', '[role=tab]')); await sleep(1500);
  await condScale(b, target);
  await until(b, `!/still loading/.test(document.body.innerText) && /reaches coloured/.test(document.body.innerText)`, 90000); await idle(b, 2500, 30000);
}
// (T1072, owner 2026-11-25 "fix picture") the Change panel under the chart names the watershed with the LARGEST SHARE OF THE MAP in view (ui/src/basin.ts
// `useViewBasin`, by design), so on a tall view the 20 km camera held more of the next watershed north (Lillooet) than the river's own (Lower Fraser). The
// camera is moved (zoom kept) until the panel names the river's watershed: `STORE_WATERSHED` (default "Lower Fraser", Kanaka Creek's), recorded in b.facts.watershed.
const WATERSHED = process.env.STORE_WATERSHED ?? 'Lower Fraser';
const watershedName = (b) => b.evaluate(`(() => { const m = /^(.+) watershed$/m.exec(document.body.innerText); return m ? m[1].trim() : null; })()`);
async function frameWatershed(b) {
  const seen = [];
  for (let i = 0; i < 8; i++) {
    await until(b, `!/still loading/.test(document.body.innerText) && /reaches coloured/.test(document.body.innerText)`, 90000); await idle(b, 2500, 30000);
    const n = await watershedName(b); seen.push(n); if (n === WATERSHED) { b.facts.watershed = n; return; }
    await dragDown(b, -Math.round(b.size.height * 0.12), 0.5); await sleep(1500); await condScale(b);
  }
  b.facts.watershed = seen.at(-1); b.facts.watershedSeen = seen;
}
Object.assign(SCENES, {
  // the rules tab: the year bar and the species table in one view (scrolled so the bar sits under the water's title)
  // (owner 2026-11-12: "salmon are a thing too") scrolled so the SALMON (DFO) section — its own year bar, Open/Closed today and its
  // Salmon row — sits under the water's tabs with the tail of the game-fish table above it: trout/char rows and salmon in one view.
  // The Vedder is open for salmon in the fall; out of season the salmon bar and row still show, closed.
  // (owner 2026-11-12 "Stacked: trout top, salmon bottom") store shot 10 is two captures of the Vedder, same day: 18 = trout/game-fish year bar
  // and first rows, 19 = the SALMON (DFO) section. The composite is named 10-regs-bar-table in store-shots.json.
  // (T810 R) switched by what the page shows: the trout table's fold (`glance:trout`: after the cutover, or a build with
  // EXPO_PUBLIC_TROUT_TABLE=1 on a release carrying the `trout` section) is opened so the shot holds the table under the
  // calendar; the old card needs no press. The compose crop of 10-regs-bar-table was set on the old card: re-pick it.
  '18-regs-trout': async (b) => { await until(b, `!!document.querySelector('[data-testid=keep-strip]')`, 120000); await sleep(3000);
    const fold = `document.querySelector('[data-testid="glance:trout"] [role=button]')`;
    if (await b.evaluate(`!!${fold}`)) { await b.evaluate(`${fold}.scrollIntoView({ block: 'center' })`); await sleep(500); await click(b, fold); await until(b, `!!document.querySelector('[data-testid=keep-trout]')`, 10000); await sleep(1000); }
    // (T1072, owner 2026-11-24 "what can you keep should show the table not the schedule calendar") the page's own scroller
    // (regs-scroll: the water's name and tabs stay above it) is scrolled so the table's own heading ("Keep N trout and char today") is its first line:
    // the season calendar and the species list are above the picture, the limits table (species rows and sizes) fills it
    const at = await b.evaluate(`(() => { const sc = document.querySelector('[data-testid=regs-scroll]'); const h = [...document.querySelectorAll('*')].find((x) => x.children.length === 0 && /^Keep \\d+ trout and char today/.test(x.textContent.trim()) && x.getBoundingClientRect().width > 0);
      if (!sc || !h) return 'no regs-scroll or no "Keep N trout and char today" heading'; let p = h.parentElement; while (p && p !== sc && p.scrollHeight <= p.clientHeight + 2) p = p.parentElement; if (!p) return 'nothing scrolls';
      p.scrollTop += h.getBoundingClientRect().top - sc.getBoundingClientRect().top - 4; return 'ok'; })()`);
    if (at !== 'ok') throw new Error('18-regs-trout: ' + at);
    await sleep(1500); },
  '19-regs-salmon': async (b) => { await until(b, `!!document.querySelector('[data-testid=keep-strip]')`, 120000); await sleep(3000);
    const at = await b.evaluate(`(() => { const e = [...document.querySelectorAll('*')].filter((x) => x.children.length === 0 && /^salmon \\(dfo\\)$/i.test(x.textContent.trim())).pop(); if (!e) return 'no SALMON (DFO) section';
      let p = e.parentElement; while (p && p.scrollHeight <= p.clientHeight + 2) p = p.parentElement; if (!p) return 'nothing scrolls';
      p.scrollTop += e.getBoundingClientRect().top - 275; return 'ok'; })()`);
    if (at !== 'ok') throw new Error('19-regs-salmon: ' + at);
    await sleep(1500); },
  // the gauge's chart with the forecast drawn on it (the model lines are the default on the chart: CLEVER, COFFEE, ELF).
  // (T319, owner 2026-11-19 "it should be showing the forecast on the chart") the Coquitlam at Port Coquitlam (gnis:19986): on
  // 2026-11-19 its forecast rose clearly ahead of today where the Vedder's ran flat. A forecast moves daily: look before the final
  '11-cond-chart': async (b) => { await until(b, `!!document.querySelector('[data-testid=gauge-card]')`, 120000); await sleep(6000);
    await until(b, `!!document.querySelector('[data-testid=chart-model-CLEVER]')`); await b.evaluate(`document.querySelector('[data-testid=chart-legend]').scrollIntoView({ block: 'end' })`); await sleep(2500);
    // (T319) every forecast model the gauge has, drawn (T895 colours each): the chips that are off are turned on
    for (const m of ['CLEVER', 'COFFEE', 'ELF']) {
      const sel = `document.querySelector('[data-testid=chart-model-${m}]')`;
      if (await b.evaluate(`!!${sel} && ${sel}.getAttribute('aria-checked') === 'false'`)) { await click(b, sel); await sleep(1500); }
    }
    // (T1072, owner review 2026-11-25) the WHOLE chart (its forecast lines, its axis) and the legend chips under it (CLEVER / COFFEE / ELF) are what the part must
    // hold, so the page is scrolled until the chart's own first line ("Scroll to zoom...") is the first line of the scroller: the gauge's name, its reading and the
    // toggles are above the picture (the water's name stays in the fixed header)
    const at = await b.evaluate(`(() => { const h = [...document.querySelectorAll('*')].find((x) => x.children.length === 0 && /^Scroll to zoom/.test(x.textContent.trim()) && x.getBoundingClientRect().width > 0);
      if (!h) return 'no chart line ("Scroll to zoom")'; let p = h.parentElement; while (p && p.scrollHeight <= p.clientHeight + 2) p = p.parentElement; if (!p) return 'nothing scrolls';
      p.scrollTop += h.getBoundingClientRect().top - p.getBoundingClientRect().top - 4; return 'ok'; })()`);
    if (at !== 'ok') throw new Error('11-cond-chart: ' + at);
    await sleep(2000); },
  '12-cond-change': async (b) => { await condMap(b); await frameWatershed(b); await click(b, `document.querySelector('[data-testid=map-horizons-change]')`); await sleep(7000); },
  '13-cond-forecast': async (b) => { await condMap(b); await click(b, `document.querySelector('[data-testid=map-horizons-days]')`); await sleep(1500);
    // the fourth day out: far enough that the forecast differs from today
    await click(b, `[...document.querySelectorAll('[role=button],button')].filter((e) => /^(Mon|Tue|Wed|Thu|Fri|Sat|Sun) \\d+$/.test(e.textContent.trim()) && e.getBoundingClientRect().width > 0)[3]`);
    await sleep(7000); },
  // a river split into its sections: the main map framed on the water (its pane closed), then one section's own rules
  '14-split-map': async (b) => { await until(b, `!!document.querySelector('[data-testid=water-close]')`, 120000); await sleep(3000);
    await click(b, `document.querySelector('[data-testid=water-close]')`); await mapReady(b); await idle(b, 5000, 45000); await zoomOut(b); await idle(b, 2500, 30000); await clearBand(b); },
  '15-split-rules': async (b) => { await until(b, `!!document.querySelector('[data-testid=water-title]')?.innerText.trim()`, 120000); await sleep(5000);
    // the tab press can land before the page is live: press again until the part stepper is there
    for (let i = 0; i < 6 && !(await b.evaluate(`!!document.querySelector('[data-testid=water-map-step-up]')`)); i++) { await click(b, `document.querySelector('[data-testid=water-view-map]')`); await sleep(8000); }
    await until(b, `!!document.querySelector('[data-testid=water-map-step-up]')`, 30000); await sleep(3000);
    b.facts.parts = Number((await b.evaluate('document.body.innerText')).match(/PART \d+ OF (\d+)/i)?.[1] ?? 0);
    for (let i = 0; i < 2; i++) { await click(b, `document.querySelector('[data-testid=water-map-step-up]')`); await sleep(2500); }
    await click(b, `document.querySelector('[data-testid=water-map-regs]')`);
    await until(b, `!!document.querySelector('[data-testid=keep-strip]')`, 60000); await sleep(3000); await scrollTo(b, 'keep-strip'); await sleep(1500); },
});
// (T319) the Vedder by the app's own link (`/?regs=`), not its /water/ page: that page is the SEO build's, so a bare web export
// served locally (the commit under test) has none
// (T319, owner 2026-11-19 "choose the stream automatically based on what rivers are doing") the chart and the change map
// open the day's river: STORE_RIVER (an item id) or <out>/river-pick.json, which store-shots/pick-river.mjs writes
const river = () => { if (process.env.STORE_RIVER) return process.env.STORE_RIVER;
  const f = path.join(OUT, 'river-pick.json'); if (!fs.existsSync(f)) throw new Error(`play-shots: no STORE_RIVER and no ${f} (node tools/store-shots/pick-river.mjs ${OUT})`);
  return JSON.parse(fs.readFileSync(f, 'utf8')).pick.item_id; };
const URLS = { '02-regulations': '/?regs=gnis:3062', '03-conditions': '/?regs=gnis:3062&view=conditions',
  '18-regs-trout': '/?regs=gnis:3062', '19-regs-salmon': '/?regs=gnis:3062', '11-cond-chart': () => `/?regs=${river()}&view=conditions`,
  '12-cond-change': () => `/?regs=${river()}`, '13-cond-forecast': () => `/?regs=${river()}`,
  '14-split-map': '/?regs=gnis:8634', '15-split-rules': '/?regs=gnis:8634' };
const SIZES = { phone: { width: 360, height: 640, dpr: 3, mobile: true, pre: '' }, tablet7: { width: 600, height: 960, dpr: 2, mobile: true, pre: 'tablet7-' }, tablet10: { width: 800, height: 1280, dpr: 2, mobile: true, pre: 'tablet10-' },
  // (T319) App Store: 6.7" iPhone 1290x2796 (430x932 @3), 5.5" 1242x2208 (414x736 @3), 13" iPad 2048x2732 (1024x1366 @2)
  // (T319, 2026-11-18) 6.9" iPhone 1320x2868 (440x956 @3), the App Store's required iPhone size; its 6.5" set is framed from it
  iphone69: { width: 440, height: 956, dpr: 3, mobile: true, pre: 'iphone69-' },
  iphone67: { width: 430, height: 932, dpr: 3, mobile: true, pre: 'iphone67-' }, iphone55: { width: 414, height: 736, dpr: 3, mobile: true, pre: 'iphone55-' },
  ipad13: { width: 1024, height: 1366, dpr: 2, mobile: true, pre: 'ipad13-' } };
const sizeNames = (process.env.SIZES ?? 'phone').split(',');
// the sites the chosen scenes will use: a scene that needs the build is refused unless its site carries the marker (T1076); a --web site is described
const chosen = sceneNames().filter(selected), sites = [...new Set(chosen.map(siteFor))];
for (const name of chosen) if (needsBuild(name) && !SITE) throw new Error(`play-shots: ${name} needs the store-shots build (${NEEDS_STORE_BUILD[name]}) and STORE_SHOTS_SITE is not set. Run node app/tools/store-shots.mjs (it builds and serves the export), or serve one with node app/tools/store-shots/site.mjs serve and set STORE_SHOTS_SITE`);
for (const name of chosen) if (!siteFor(name)) throw new Error('play-shots: STORE_SHOTS_SITE is not set (and no STORE_SHOTS_WEB for this scene)');
const SOURCES = {};   // site -> what it says of itself
for (const st of sites) {
  if (st === SITE) await assertStoreShotsSite(st);   // the export: the marker, or refused with the reason
  SOURCES[st] = await describeSite(st);
  console.log(`play-shots: ${st === WEB ? 'WEB' : 'export'} site ${st}: bundle ${SOURCES[st].bundle ?? '?'}, last-modified ${SOURCES[st].lastModified ?? '?'}${SOURCES[st].storeShotsCommit ? `, store-shots build of ${SOURCES[st].storeShotsCommit.slice(0, 9)}` : ''}`);
}
let failed = 0; const report = {}, sources = {};   // (T704) scene -> 'ok' | the reason it was not captured, written beside the shots
for (const sn of sizeNames) for (const [name, fn] of Object.entries(SCENES)) {
  if (ONLY && !ONLY.includes(name.slice(0, 2)) && !ONLY.includes(name)) continue;
  const z = SIZES[sn];
  const t0 = Date.now(), u = URLS[name] ?? '/', b = await open(siteFor(name) + (typeof u === 'function' ? u() : u), { ...z, init: PIN_JS + SEEN });
  try {
    b.facts = {}; b.size = z; await fn(b);
    await checkScene(b, name);   // (T704) the page holds the shot's content, or nothing is captured
    const { data } = await b.send('Page.captureScreenshot', { format: 'png' });
    const f = path.join(OUT, `${z.pre}${name}.png`); fs.writeFileSync(f, Buffer.from(data, 'base64')); console.log(f, `${((Date.now() - t0) / 1000).toFixed(1)}s`, `from ${siteFor(name)}`); report[`${sn}/${name}`] = 'ok'; sources[`${sn}/${name}`] = SOURCES[siteFor(name)];
  } catch (e) { failed++; report[`${sn}/${name}`] = e.message; console.error(sn, name, e.message); }
  finally { await b.close(); }
}
fs.writeFileSync(path.join(OUT, process.env.PLAY_REPORT ?? 'play-shots-report.json'), JSON.stringify({ site: SITE, web: WEB, pinAt: process.env.PIN_AT ?? null, scenes: report, sources }, null, 2) + '\n');
process.exit(failed ? 1 : 0);
