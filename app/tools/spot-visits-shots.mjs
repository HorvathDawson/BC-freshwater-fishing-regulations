#!/usr/bin/env node
// T294 — screenshots of a saved spot with THREE RATED VISITS on the web, headless Chrome (never a visible window), at
// 390 px and 1280 px: the page (visits, the shown visit's stars, the coming days predicted from the ratings) and the
// "Add a visit" page. Seeds the spot the way tools/spot-shots.mjs does (localStorage). The app is a running Expo web
// server (APP_URL, default http://localhost:39218).
//
//   node tools/spot-visits-shots.mjs <outDir>
import fs from 'node:fs'; import path from 'node:path';
import { open } from './regs-web/cdp.mjs';

const APP = process.env.APP_URL || 'http://localhost:39218';
const [outDir] = process.argv.slice(2);
if (!outDir) { console.error('usage: spot-visits-shots.mjs <outDir>'); process.exit(2); }
fs.mkdirSync(outDir, { recursive: true });
const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const read = (f, re) => re.exec(fs.readFileSync(path.join(ROOT, f), 'utf8'))?.[1];
const SEEN = `try { localStorage.setItem('canifishthis.firstRun.v1', ${JSON.stringify(read('packages/core/src/menu/legal.ts', /FIRST_RUN_VERSION = "([^"]+)"/))});
  localStorage.setItem('canifishthis.walkthrough.v1', ${JSON.stringify(read('packages/core/src/help.ts', /WALKTHROUGH_VERSION = "([^"]+)"/))});
  localStorage.setItem('canifishthis.inseasonSeen.v1', '["*"]'); } catch {}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const DAY = 86400000;
const hourOf = (t, h) => { const d = new Date(t); d.setHours(h, 0, 0, 0); return d.getTime(); };
const iso = (t) => new Date(t).toISOString();
const ymd = (t) => { const d = new Date(t); return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`; };
const sample = (t, c, cloud, hpa) => ({ at: iso(t), tempC: c, cloudPct: cloud, pressureHpa: hpa, humidityPct: 63 });
/** One visit's frozen record: the reading, the weather at the hour, and the week before (rain per day). */
const record = (t, { p, c, cloud, hpa, wind, rain }) => ({
  visitedAt: t, createdAt: t,
  reading: { station: '08MH001', discharge: 20 + p * 60, level: null, parameter: 'discharge', percentile: p, at: iso(t) },
  weather: { tempC: c, windKph: wind, windDir: 250, rain3h: 0, pressureHpa: hpa, code: cloud > 70 ? 3 : 2, at: iso(t),
             window: { before: sample(t - 3600e3, c - 1, cloud, hpa + 1), at: sample(t, c, cloud, hpa), after: sample(t + 3600e3, c + 1, cloud, hpa - 1) } },
  trace: null, panel: null,
  leadUp: { station: '08MH001', gauge: null,
            weather: [7, 6, 5, 4, 3, 2, 1].map((k, i) => ({ date: ymd(t - k * DAY), maxC: c + 3, minC: c - 5,
              precipMm: i >= 4 ? rain / 3 : 0, windMaxKph: wind + 6, code: 2 })) },
});
const now = Date.now();
const V1 = hourOf(now - 41 * DAY, 8), V2 = hourOf(now - 23 * DAY, 15), V3 = hourOf(now - 9 * DAY, 7);
const SPOT = {
  id: 'shot-visits', updatedAt: V3, lat: 49.0985, lon: -121.9585, item: 'gnis:3062', waterName: 'Vedder River',
  title: 'Bridge pool', photos: [],
  notes: 'Tail-out below the bridge, 8 to 11. Water a little tea-coloured, dropping after the rain.',
  ...record(V1, { p: 0.38, c: 12, cloud: 80, hpa: 1009, wind: 6, rain: 9 }), rating: 5,
  visits: [
    { id: 'shot-v2', rating: 2, notes: 'Bright and low. Nothing moving in the afternoon.', photos: [],
      ...record(V2, { p: 0.12, c: 21, cloud: 10, hpa: 1021, wind: 14, rain: 0 }) },
    { id: 'shot-v3', rating: 4, notes: 'Overcast morning, river up a little.', photos: [],
      ...record(V3, { p: 0.55, c: 9, cloud: 90, hpa: 1006, wind: 8, rain: 14 }) },
  ],
};
const INIT = SEEN + `try { localStorage.setItem('canifishthis.spots.v1', ${JSON.stringify(JSON.stringify([SPOT]))}); } catch {}`;

const SIZES = { phone: { width: 390, height: 2400, dpr: 1, mobile: true }, desktop: { width: 1280, height: 1500, dpr: 1, mobile: false } };
async function until(b, expr, ms = 60000) { for (let t = 0; t < ms; t += 250) { if (await b.evaluate(expr).catch(() => false)) return; await sleep(250); } throw new Error('timed out: ' + expr); }
async function click(b, find) {
  const at = await b.evaluate(`(() => { const e = ${find}; if (!e) return null; e.scrollIntoView({ block: 'center' }); const r = e.getBoundingClientRect(); return [r.x + r.width / 2, r.y + r.height / 2]; })()`);
  if (!at) throw new Error('nothing to press: ' + find);
  for (const type of ['mouseMoved', 'mousePressed', 'mouseReleased']) await b.send('Input.dispatchMouseEvent', { type, x: at[0], y: at[1], button: 'left', clickCount: 1 });
}
const lab = (sel, s) => `[...document.querySelectorAll(${JSON.stringify(sel)})].find((e) => (e.getAttribute('aria-label') || e.textContent).trim().startsWith(${JSON.stringify(s)}))`;
const shoot = async (b, name) => {
  const shot = await b.send('Page.captureScreenshot', { format: 'png' });
  const f = path.join(outDir, name); fs.writeFileSync(f, Buffer.from(shot.data, 'base64')); console.log(f);
};

let failed = 0;
for (const [sn, size] of Object.entries(SIZES)) {
  const b = await open(APP + '/', { ...size, init: INIT, network: true });
  try {
    await until(b, `!!document.querySelector('[role=tablist]')`, 120000);
    await sleep(1500);
    await click(b, lab('[role=tab]', 'Spots'));
    await until(b, `!!document.querySelector('[data-testid=spots-screen]')`);
    await click(b, lab('[role=button]', SPOT.title));
    await until(b, `!!document.querySelector('[data-testid=spot-visits]')`);
    await sleep(6000);   // the forecast strip fetches
    await b.evaluate(`document.querySelectorAll('*').forEach((e) => { if (e.scrollTop) e.scrollTop = 0; })`);
    await shoot(b, `visits-${sn}.png`);
    // the days' predictions and reasons, scrolled to (a phone reads them under the visit)
    if (sn === 'phone') {
      await b.evaluate(`document.querySelector('[data-testid=spot-next-days]').scrollIntoView({ block: 'start' })`);
      await sleep(400);
      await shoot(b, `visits-${sn}-days.png`);
    }
    // the first visit shown: its own record, its stars
    await click(b, `document.querySelector('[data-testid=visit-shot-visits]')`);
    await sleep(800);
    await b.evaluate(`document.querySelectorAll('*').forEach((e) => { if (e.scrollTop) e.scrollTop = 0; })`);
    await shoot(b, `visits-${sn}-first.png`);
    await click(b, `document.querySelector('[data-testid=visit-add]')`);
    await until(b, `document.body.textContent.includes('When was this visit?')`);
    await sleep(500);
    await shoot(b, `add-visit-${sn}.png`);
    if (b.errors.length) console.log('  console errors:', b.errors.slice(0, 3).join(' | '));
  } catch (e) { failed++; console.error(sn, e.message); }
  finally { await b.close(); }
}
process.exit(failed ? 1 : 0);
