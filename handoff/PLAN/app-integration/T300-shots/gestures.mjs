// T300 gesture check on web (headless Chrome, touch emulation): a one-finger vertical drag, a diagonal drag and a two-finger
// pinch on the Conditions view's route map. Reports how far the MAP moved (centre, zoom) and whether the PAGE scrolled instead.
//   node gestures.mjs <appUrl>
import fs from 'node:fs'; import path from 'node:path';
import { open } from '../../../../app/tools/regs-web/cdp.mjs';
const [APP, VIEW = 'Conditions'] = process.argv.slice(2);
const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '../../../../app');
const read = (f, re) => re.exec(fs.readFileSync(path.join(ROOT, f), 'utf8'))?.[1];
const FIRST_RUN = read('packages/core/src/menu/legal.ts', /FIRST_RUN_VERSION = "([^"]+)"/);
const WALK = read('packages/core/src/help.ts', /WALKTHROUGH_VERSION = "([^"]+)"/);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const init = `try { localStorage.setItem('canifishthis.firstRun.v1', ${JSON.stringify(FIRST_RUN)}); localStorage.setItem('canifishthis.walkthrough.v1', ${JSON.stringify(WALK)}); localStorage.setItem('canifishthis.inseasonSeen.v1', '["*"]'); } catch {}`;
const byText = (sel, text) => `[...document.querySelectorAll(${JSON.stringify(sel)})].find((e) => e.textContent.trim().startsWith(${JSON.stringify(text)}))`;
const b = await open(APP + '/?regs=gnis:3062&date=2026-06-15', { width: 390, height: 844, dpr: 2, mobile: true, init });
const until = async (expr, ms = 90000) => { for (let t = 0; t < ms; t += 250) { if (await b.evaluate(expr).catch(() => false)) return; await sleep(250); } throw new Error('timeout ' + expr); };
try {
  await b.send('Emulation.setTouchEmulationEnabled', { enabled: true, maxTouchPoints: 5 });
  await until(`document.body.textContent.includes('Vedder')`);
  await sleep(3000);
  const at = await b.evaluate(`(() => { const e = ${byText('[role=tab],[role=button]', VIEW)}; const r = e.getBoundingClientRect(); return [r.x + r.width / 2, r.y + r.height / 2]; })()`);
  for (const type of ['mouseMoved', 'mousePressed', 'mouseReleased']) await b.send('Input.dispatchMouseEvent', { type, x: at[0], y: at[1], button: 'left', clickCount: 1 });
  await until(`!!document.querySelector('.maplibregl-canvas') && !!globalThis.__map`);
  await sleep(2000);
  await sleep(6000);
  // bring the map to the middle of the screen
  await b.evaluate(`window.__cv = [...document.querySelectorAll('.maplibregl-canvas')].filter((e) => e.getBoundingClientRect().width > 0 && e.offsetParent !== null).sort((a, b) => a.clientHeight - b.clientHeight)[0]; window.__cv.scrollIntoView({ block: 'center' })`);
  await sleep(1500);
  const state = () => b.evaluate(`(() => { const m = globalThis.__map; const c = m.getCenter();
    const sc = [...document.querySelectorAll('div')].filter((e) => e.scrollHeight > e.clientHeight + 40 && /auto|scroll/.test(getComputedStyle(e).overflowY)).map((e) => e.scrollTop);
    return { lng: +c.lng.toFixed(5), lat: +c.lat.toFixed(5), zoom: +m.getZoom().toFixed(2), scroll: sc, ta: getComputedStyle(window.__cv.parentElement).touchAction }; })()`);
  const rect = await b.evaluate(`(() => { const r = window.__cv.getBoundingClientRect(); return { x: r.x, y: r.y, w: r.width, h: r.height }; })()`);
  const touch = (type, pts) => b.send('Input.dispatchTouchEvent', { type, touchPoints: pts.map(([x, y], i) => ({ x, y, id: i + 1 })) });
  const drag = async (from, to, steps = 14) => {
    await touch('touchStart', [from]);
    for (let i = 1; i <= steps; i++) { await touch('touchMove', [[from[0] + (to[0] - from[0]) * i / steps, from[1] + (to[1] - from[1]) * i / steps]]); await sleep(16); }
    await touch('touchEnd', []);
    await sleep(1200);
  };
  const cx = rect.x + rect.w / 2, cy = rect.y + rect.h / 2;
  const out = { rect, start: await state() };
  await drag([cx, cy + 60], [cx, cy - 60]); out.afterVertical = await state();
  await drag([cx - 80, cy], [cx + 40, cy]); out.afterHorizontal = await state();
  await drag([cx - 40, cy + 40], [cx + 40, cy - 40]); out.afterDiagonal = await state();
  const pinch = async () => {
    await touch('touchStart', [[cx - 20, cy], [cx + 20, cy]]);
    for (let i = 1; i <= 12; i++) { await touch('touchMove', [[cx - 20 - i * 6, cy], [cx + 20 + i * 6, cy]]); await sleep(16); }
    await touch('touchEnd', []); await sleep(1200);
  };
  await pinch(); out.afterPinch = await state();
  console.log(JSON.stringify(out, null, 1));
} finally { await b.close(); }
