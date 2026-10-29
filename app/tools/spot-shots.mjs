#!/usr/bin/env node
// T284 — screenshots of a saved spot's page on the web, headless Chrome (never a visible window): one spot with photos,
// one without, at 390 px and 1280 px. Seeds the spots the way tools/request-walk.mjs does (localStorage) and the photos
// into the web photo store (IndexedDB). The app is the running Expo web server (APP_URL, default http://localhost:39218).
//
//   node tools/spot-shots.mjs <outDir> <prefix>      e.g. before / after
import fs from 'node:fs'; import path from 'node:path';
import { open } from './regs-web/cdp.mjs';

const APP = process.env.APP_URL || 'http://localhost:39218';
const [outDir, prefix = 'shot'] = process.argv.slice(2);
if (!outDir) { console.error('usage: spot-shots.mjs <outDir> <prefix>'); process.exit(2); }
fs.mkdirSync(outDir, { recursive: true });
const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const read = (f, re) => re.exec(fs.readFileSync(path.join(ROOT, f), 'utf8'))?.[1];
const SEEN = `try { localStorage.setItem('canifishthis.firstRun.v1', ${JSON.stringify(read('packages/core/src/menu/legal.ts', /FIRST_RUN_VERSION = "([^"]+)"/))});
  localStorage.setItem('canifishthis.walkthrough.v1', ${JSON.stringify(read('packages/core/src/help.ts', /WALKTHROUGH_VERSION = "([^"]+)"/))});
  localStorage.setItem('canifishthis.inseasonSeen.v1', '["*"]'); } catch {}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const visit = Date.now() - 9 * 86400000;
const iso = (t) => new Date(t).toISOString();
const sample = (t, c) => ({ at: iso(t), tempC: c, cloudPct: 40, pressureHpa: 1012, humidityPct: 63 });
const base = (id, title, photos, notes) => ({ id, createdAt: visit, updatedAt: visit, visitedAt: visit, lat: 49.0985, lon: -121.9585,
  item: 'gnis:3062', waterName: 'Vedder River', title, notes, photos,
  reading: { station: 'Vedder River at Vedder Crossing', discharge: 41.2, level: null, parameter: null, percentile: 0.42, at: iso(visit) },
  weather: { tempC: 14, windKph: 9, windDir: 250, rain3h: 0, pressureHpa: 1012, code: 2, at: iso(visit),
             window: { before: sample(visit - 3600e3, 13), at: sample(visit, 14), after: sample(visit + 3600e3, 15) } },
  trace: null, panel: null });
const SPOTS = [
  base('shot-photos', 'Bridge pool', ['photos/shot-photos-1.png', 'photos/shot-photos-2.png', 'photos/shot-photos-3.png'],
       'Fished the tail-out below the bridge from 7 to 10. Three rainbow on a small pheasant tail, one bull trout on a streamer. Water was a little tea-coloured, still dropping after the rain earlier in the week. Waded across at the gravel bar.'),
  base('shot-plain', 'Gravel bar', [], ''),
];
const INIT = SEEN + `try { localStorage.setItem('canifishthis.spots.v1', ${JSON.stringify(JSON.stringify(SPOTS))}); } catch {}
(async () => { try {
  const db = await new Promise((res, rej) => { const r = indexedDB.open('canifishthis.photos', 1);
    r.onupgradeneeded = () => { if (!r.result.objectStoreNames.contains('photos')) r.result.createObjectStore('photos'); };
    r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error); });
  const hues = [200, 28, 140];
  for (let i = 0; i < 3; i++) {
    const c = new OffscreenCanvas(480, 360), g = c.getContext('2d');
    const grd = g.createLinearGradient(0, 0, 480, 360); grd.addColorStop(0, 'hsl(' + hues[i] + ',45%,62%)'); grd.addColorStop(1, 'hsl(' + hues[i] + ',50%,28%)');
    g.fillStyle = grd; g.fillRect(0, 0, 480, 360);
    const blob = await c.convertToBlob({ type: 'image/png' });
    await new Promise((res, rej) => { const tx = db.transaction('photos', 'readwrite'); tx.objectStore('photos').put(blob, 'photos/shot-photos-' + (i + 1) + '.png'); tx.oncomplete = res; tx.onerror = () => rej(tx.error); });
  }
} catch (e) { console.error('photo seed', e); } })();`;

const SIZES = { phone360: { width: 360, height: 2000, dpr: 1, mobile: true }, phone: { width: 390, height: 2000, dpr: 1, mobile: true }, desktop: { width: 1280, height: 1500, dpr: 1, mobile: false } };
async function until(b, expr, ms = 60000) { for (let t = 0; t < ms; t += 250) { if (await b.evaluate(expr).catch(() => false)) return; await sleep(250); } throw new Error('timed out: ' + expr); }
async function click(b, find) {
  const at = await b.evaluate(`(() => { const e = ${find}; if (!e) return null; const r = e.getBoundingClientRect(); return [r.x + r.width / 2, r.y + r.height / 2]; })()`);
  if (!at) throw new Error('nothing to press: ' + find);
  for (const type of ['mouseMoved', 'mousePressed', 'mouseReleased']) await b.send('Input.dispatchMouseEvent', { type, x: at[0], y: at[1], button: 'left', clickCount: 1 });
}
const lab = (sel, s) => `[...document.querySelectorAll(${JSON.stringify(sel)})].find((e) => (e.getAttribute('aria-label') || e.textContent).trim().startsWith(${JSON.stringify(s)}))`;

let failed = 0;
for (const [sn, size] of Object.entries(SIZES)) for (const spot of SPOTS) {
  const b = await open(APP + '/', { ...size, init: INIT, network: true });
  try {
    await until(b, `!!document.querySelector('[role=tablist]')`, 120000);
    await sleep(1500);
    await click(b, lab('[role=tab]', 'Spots'));
    await until(b, `!!document.querySelector('[data-testid=spots-screen]')`);
    await click(b, lab('[role=button]', spot.title));
    await until(b, `!!document.querySelector('[data-testid=spot-when]')`);
    await sleep(6000);   // the forecast strip fetches; photos resolve
    const shot = await b.send('Page.captureScreenshot', { format: 'png' });
    const f = path.join(outDir, `${prefix}-${sn}-${spot.id === 'shot-photos' ? 'photos' : 'plain'}.png`);
    fs.writeFileSync(f, Buffer.from(shot.data, 'base64')); console.log(f);
    if (b.errors.length) console.log('  console errors:', b.errors.slice(0, 3).join(' | '));
  } catch (e) { failed++; console.error(sn, spot.id, e.message); }
  finally { await b.close(); }
}
process.exit(failed ? 1 : 0);
