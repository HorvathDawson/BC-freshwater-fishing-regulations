#!/usr/bin/env node
// T257 — before/after shots of the app's screens on Expo web, and a pixel diff per pair.
//
//   node tools/polish-shots.mjs shoot <outDir> [scene…]     every scene × (desktop, phone) × (light, dark)
//   node tools/polish-shots.mjs diff <beforeDir> <afterDir> <report.md>
//
// The app is the running Expo web server (APP_URL, default http://localhost:39218); the map tiles are its own tile server.
// No npm dependencies: headless Chrome over the DevTools protocol (regs-web/cdp.mjs), and the diff is a canvas in Chrome.
import fs from 'node:fs'; import path from 'node:path';
import { open } from './regs-web/cdp.mjs';

const APP = process.env.APP_URL || 'http://localhost:39218';
// APP_ROOT: the tree the build was made from (its stored-version words); default this one
const ROOT = process.env.APP_ROOT || path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const read = (f, re) => re.exec(fs.readFileSync(path.join(ROOT, f), 'utf8'))?.[1];
const FIRST_RUN = read('packages/core/src/menu/legal.ts', /FIRST_RUN_VERSION = "([^"]+)"/);
const WALK = read('packages/core/src/help.ts', /WALKTHROUGH_VERSION = "([^"]+)"/);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const SIZES = { desktop: { width: 1280, height: 800, dpr: 1, mobile: false }, phone: { width: 390, height: 844, dpr: 2, mobile: true } };
// Math.random pinned: the search field's placeholder is picked at random, and a diff should see only real changes
const seed = (firstRun, walk) => `Math.random = () => 0.42; try { ${firstRun ? `localStorage.setItem('canifishthis.firstRun.v1', ${JSON.stringify(FIRST_RUN)});` : "localStorage.removeItem('canifishthis.firstRun.v1');"}
  ${walk ? `localStorage.setItem('canifishthis.walkthrough.v1', ${JSON.stringify(WALK)});` : "localStorage.removeItem('canifishthis.walkthrough.v1');"}
  localStorage.setItem('canifishthis.inseasonSeen.v1', '["*"]'); } catch {}`;

const byText = (sel, text) => `[...document.querySelectorAll(${JSON.stringify(sel)})].find((e) => e.textContent.trim().startsWith(${JSON.stringify(text)}))`;
const tab = (name) => byText('[role=tab]', name);
async function click(b, find) {
  const at = await b.evaluate(`(() => { const e = ${find}; if (!e) return null; const r = e.getBoundingClientRect(); return [r.x + r.width / 2, r.y + r.height / 2]; })()`);
  if (!at) throw new Error(`nothing to press: ${find}`);
  for (const type of ['mouseMoved', 'mousePressed', 'mouseReleased'])
    await b.send('Input.dispatchMouseEvent', { type, x: at[0], y: at[1], button: 'left', clickCount: 1 });
}
async function until(b, expr, ms = 60000) {
  for (let t = 0; t < ms; t += 250) { if (await b.evaluate(expr).catch(() => false)) return; await sleep(250); }
  throw new Error(`timed out: ${expr}`);
}
const MAP_READY = `!!document.querySelector('[role=tablist]')`;

/** name → [start query, firstRun accepted, walkthrough seen, steps after load] */
export const SCENES = {
  map: ['', true, true, async () => {}],
  firstrun: ['', false, false, async (b) => { await until(b, `!!document.querySelector('[data-testid=first-run-notice]')`); }],
  tour: ['', true, false, async (b) => { await until(b, `!!document.querySelector('[data-testid=walkthrough], [data-testid^=coach-step-]')`); }],
  regs: ['?regs=gnis:3062&date=2026-06-15', true, true, async () => {}],
  conditions: ['', true, true, async (b) => { await click(b, tab('Conditions')); }],
  spots: ['', true, true, async (b) => { await click(b, tab('Spots')); }],
  more: ['', true, true, async (b) => { await click(b, tab('More')); }],
  help: ['', true, true, async (b) => { await click(b, tab('More')); await sleep(800); await click(b, byText('[role=button]', 'Help')); }],
  layers: ['', true, true, async (b) => { await click(b, `document.querySelector('[aria-label^="Choose layers"]')`); }],
  // the tour's later steps (after only: the cards had no such steps)
  ...Object.fromEntries([2, 3, 4, 5].map((n) => [`tour${n}`, ['', true, false, async (b) => {
    await until(b, `!!document.querySelector('[data-testid^=coach-step-]')`);
    for (let i = 1; i < n; i++) { await click(b, byText('[role=button]', 'Next')); await sleep(700); }
  }]])),
};

async function shoot(out, names) {
  fs.mkdirSync(out, { recursive: true });
  for (const name of names) for (const [size, m] of Object.entries(SIZES)) for (const theme of ['light', 'dark']) {
    const [q, fr, wk, steps] = SCENES[name];
    const b = await open(APP + '/' + q, { ...m, init: seed(fr, wk) });
    try {
      await b.send('Emulation.setEmulatedMedia', { features: [{ name: 'prefers-color-scheme', value: theme }, { name: 'prefers-reduced-motion', value: 'reduce' }] });
      await b.send('Page.reload', {});
      await sleep(1500);
      await until(b, name === 'regs' ? `document.body.textContent.includes('Vedder River')` : MAP_READY, 120000);
      await b.evaluate('document.fonts.ready.then(() => 1)');
      await sleep(5000);
      await steps(b);
      await sleep(2500);
      const { data } = await b.send('Page.captureScreenshot', { format: 'png' });
      const f = path.join(out, `${name}-${size}-${theme}.png`);
      fs.writeFileSync(f, Buffer.from(data, 'base64'));
      console.log('✓', f);
    } catch (e) { console.log('✗', name, size, theme, e.message); }
    finally { await b.close(); }
  }
}

async function diff(before, after, report) {
  const b = await open('about:blank', { width: 200, height: 200 });
  const rows = [];
  try {
    for (const f of fs.readdirSync(before).filter((x) => x.endsWith('.png')).sort()) {
      const g = path.join(after, f);
      if (!fs.existsSync(g)) { rows.push([f, 'no after shot']); continue; }
      const u = (p) => 'data:image/png;base64,' + fs.readFileSync(p).toString('base64');
      const r = await b.evaluate(`(async () => {
        const load = (s) => new Promise((res) => { const i = new Image(); i.onload = () => res(i); i.src = s; });
        const [x, y] = await Promise.all([load(${JSON.stringify(u(path.join(before, f)))}), load(${JSON.stringify(u(g))})]);
        if (x.width !== y.width || x.height !== y.height) return { size: true };
        const px = (i) => { const c = document.createElement('canvas'); c.width = i.width; c.height = i.height; const k = c.getContext('2d'); k.drawImage(i, 0, 0); return k.getImageData(0, 0, i.width, i.height).data; };
        const a = px(x), d = px(y); let n = 0;
        for (let k = 0; k < a.length; k += 4) if (Math.abs(a[k] - d[k]) + Math.abs(a[k+1] - d[k+1]) + Math.abs(a[k+2] - d[k+2]) > 24) n++;
        return { pct: 100 * n / (a.length / 4) };
      })()`);
      rows.push([f, r.size ? 'size differs' : r.pct.toFixed(2) + '%']);
    }
  } finally { await b.close(); }
  const md = ['| shot | pixels differing |', '|---|---|', ...rows.map(([f, v]) => `| ${f} | ${v} |`)].join('\n') + '\n';
  fs.writeFileSync(report, md);
  console.log(md);
}

const [cmd, ...a] = process.argv.slice(2);
if (cmd === 'shoot') await shoot(a[0], a.length > 1 ? a.slice(1) : Object.keys(SCENES));
else if (cmd === 'diff') await diff(a[0], a[1], a[2]);
else console.log('usage: shoot <outDir> [scene…] | diff <before> <after> <report.md>');
