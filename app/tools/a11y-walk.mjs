#!/usr/bin/env node
// T243 — the web app with axe-core and a keyboard: every scene × (desktop, phone) × (light, dark) gets an axe run
// (WCAG 2.x A + AA rules, colour contrast included), and on the desktop the keyboard walks the page with Tab, checking
// that each control it lands on shows a focus mark. Shots of each scene go to the out folder.
//
//   node tools/a11y-walk.mjs <outDir> [scene…]
//
// The app is the running Expo web server (APP_URL, default http://localhost:39218). axe-core is fetched at run time from
// cdnjs (AXE_URL; pinned 4.10.2) and injected into the page — nothing installed (deps.md). No other dependencies:
// headless Chrome over the DevTools protocol (regs-web/cdp.mjs). The axe and focus findings print and land in
// <outDir>/a11y-walk.json; the exit code is 1 when any scene has a violation.
import fs from 'node:fs'; import path from 'node:path';
import { open } from './regs-web/cdp.mjs';

const APP = process.env.APP_URL || 'http://localhost:39218';
const AXE_URL = process.env.AXE_URL || 'https://cdnjs.cloudflare.com/ajax/libs/axe-core/4.10.2/axe.min.js';
const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const read = (f, re) => re.exec(fs.readFileSync(path.join(ROOT, f), 'utf8'))?.[1];
const FIRST_RUN = read('packages/core/src/menu/legal.ts', /FIRST_RUN_VERSION = "([^"]+)"/);
const WALK = read('packages/core/src/help.ts', /WALKTHROUGH_VERSION = "([^"]+)"/);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const SIZES = { desktop: { width: 1280, height: 800, dpr: 1, mobile: false }, phone: { width: 390, height: 844, dpr: 2, mobile: true } };
const SEED = `try { localStorage.setItem('canifishthis.firstRun.v1', ${JSON.stringify(FIRST_RUN)});
  localStorage.setItem('canifishthis.walkthrough.v1', ${JSON.stringify(WALK)});
  localStorage.setItem('canifishthis.inseasonSeen.v1', '["*"]'); } catch {}`;

const byText = (sel, text) => `[...document.querySelectorAll(${JSON.stringify(sel)})].find((e) => (e.getAttribute('aria-label') || e.textContent).trim().startsWith(${JSON.stringify(text)}))`;
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
const tab = (name) => byText('[role=tab]', name);
const MAP_READY = `!!document.querySelector('[role=tablist]')`;
const REGS_READY = `!!document.querySelector('[data-testid=regs-header]')`;

/** name → [start query, ready check, steps after load] */
const SCENES = {
  map: ['', MAP_READY, async () => {}],
  conditions: ['', MAP_READY, async (b) => { await click(b, tab('Conditions')); }],
  spots: ['', MAP_READY, async (b) => { await click(b, tab('Spots')); }],
  more: ['', MAP_READY, async (b) => { await click(b, tab('More')); }],
  offline: ['', MAP_READY, async (b) => { await click(b, tab('More')); await sleep(800); await click(b, byText('[role=button]', 'Offline maps')); }],
  help: ['', MAP_READY, async (b) => { await click(b, tab('More')); await sleep(800); await click(b, byText('[role=button]', 'Help')); }],
  sources: ['', MAP_READY, async (b) => { await click(b, tab('More')); await sleep(800); await click(b, byText('[role=button]', 'Sources and licences')); }],
  layers: ['', MAP_READY, async (b) => { await click(b, `document.querySelector('[aria-label^="Choose layers"]')`); }],
  search: ['', MAP_READY, async (b) => {
    await click(b, `document.querySelector('input')`); await sleep(300);
    await b.send('Input.insertText', { text: 'Chilliwack' });
  }],
  regs: ['?regs=gnis:3062&date=2026-06-15', REGS_READY, async () => {}],
};

/** axe's WCAG A/AA run, reduced to rule → [impact, count, a few targets] */
const AXE_RUN = `(async () => {
  const r = await axe.run(document, { runOnly: { type: 'tag', values: ['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'] }, resultTypes: ['violations'] });
  return r.violations.map((v) => ({ id: v.id, impact: v.impact, help: v.help, n: v.nodes.length,
    at: v.nodes.slice(0, 4).map((n) => n.target.join(' ') + ' :: ' + (n.failureSummary || '').split('\\n').slice(1, 2).join(' ').trim()) }));
})()`;

/** Tab through the page: what each stop is, and whether it shows a focus mark (an outline or a ring) */
const FOCUS_STOP = `(() => {
  const e = document.activeElement; if (!e || e === document.body) return null;
  const cs = getComputedStyle(e);
  const mark = (cs.outlineStyle !== 'none' && parseFloat(cs.outlineWidth) > 0) || (cs.boxShadow && cs.boxShadow !== 'none')
    || e.tagName === 'INPUT' || e.tagName === 'TEXTAREA';
  return { what: (e.getAttribute('role') || e.tagName.toLowerCase()) + ' ' + (e.getAttribute('aria-label') || e.textContent || '').trim().slice(0, 50), mark };
})()`;

async function walk(out, names, axeSrc) {
  fs.mkdirSync(out, { recursive: true });
  const results = [];
  for (const name of names) for (const [size, m] of Object.entries(SIZES)) for (const theme of ['light', 'dark']) {
    const [q, ready, steps] = SCENES[name];
    const b = await open(APP + '/' + q, { ...m, init: SEED });
    const row = { scene: name, size, theme, axe: null, focus: null, error: null };
    try {
      await b.send('Emulation.setEmulatedMedia', { features: [{ name: 'prefers-color-scheme', value: theme }, { name: 'prefers-reduced-motion', value: 'reduce' }] });
      await b.send('Page.reload', {});
      await sleep(1500);
      await until(b, ready, 120000);
      await b.evaluate('document.fonts.ready.then(() => 1)');
      await sleep(4000);
      await steps(b);
      await sleep(2500);
      const { data } = await b.send('Page.captureScreenshot', { format: 'png' });
      fs.writeFileSync(path.join(out, `${name}-${size}-${theme}.png`), Buffer.from(data, 'base64'));
      await b.evaluate(axeSrc + '; 1');
      row.axe = await b.evaluate(AXE_RUN);
      if (size === 'desktop' && theme === 'light') {
        const stops = [];
        for (let i = 0; i < 30; i++) {
          for (const type of ['keyDown', 'keyUp'])
            await b.send('Input.dispatchKeyEvent', { type, key: 'Tab', code: 'Tab', windowsVirtualKeyCode: 9 });
          await sleep(60);
          const s = await b.evaluate(FOCUS_STOP);
          if (s) stops.push(s);
        }
        row.focus = { stops: stops.length, unmarked: [...new Set(stops.filter((s) => !s.mark).map((s) => s.what))] };
      }
    } catch (e) { row.error = e.message; }
    finally { await b.close(); }
    results.push(row);
    const v = row.axe ? row.axe.map((x) => `${x.id}(${x.impact})×${x.n}`).join(', ') || 'clean' : '—';
    console.log(`${row.error ? '✗' : '✓'} ${name} ${size} ${theme}: axe ${v}` +
      (row.focus ? ` · focus ${row.focus.stops} stops, unmarked ${row.focus.unmarked.length}` : '') + (row.error ? ` · ${row.error}` : ''));
  }
  fs.writeFileSync(path.join(out, 'a11y-walk.json'), JSON.stringify(results, null, 1) + '\n');
  return results.some((r) => r.error || (r.axe && r.axe.length) || (r.focus && r.focus.unmarked.length));
}

const [out, ...names] = process.argv.slice(2);
if (!out) { console.log('usage: node tools/a11y-walk.mjs <outDir> [scene…]'); process.exit(2); }
const axeSrc = await (await fetch(AXE_URL)).text();
process.exit(await walk(out, names.length ? names : Object.keys(SCENES), axeSrc) ? 1 : 0);
