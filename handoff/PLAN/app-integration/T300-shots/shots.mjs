// T300 shots, headless Chrome over CDP (app/tools/regs-web/cdp.mjs), never a visible window.
//   node shots.mjs <appUrl> <outPrefix> <scene…>      scenes: conditions notes legend-obstacles runs more
// No Math.random pin (it greys the map: T283).
import fs from 'node:fs'; import path from 'node:path';
import { open } from '../../../../app/tools/regs-web/cdp.mjs';
const [APP, PREFIX, ...SCENES] = process.argv.slice(2);
const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '../../../../app');
const read = (f, re) => re.exec(fs.readFileSync(path.join(ROOT, f), 'utf8'))?.[1];
const FIRST_RUN = read('packages/core/src/menu/legal.ts', /FIRST_RUN_VERSION = "([^"]+)"/);
const WALK = read('packages/core/src/help.ts', /WALKTHROUGH_VERSION = "([^"]+)"/);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const SIZES = { desktop: { width: 1280, height: 800, dpr: 1, mobile: false }, phone: { width: 390, height: 844, dpr: 2, mobile: true } };
const init = `try { localStorage.setItem('canifishthis.firstRun.v1', ${JSON.stringify(FIRST_RUN)}); localStorage.setItem('canifishthis.walkthrough.v1', ${JSON.stringify(WALK)}); localStorage.setItem('canifishthis.inseasonSeen.v1', '["*"]'); } catch {}`;
const byText = (sel, text) => `[...document.querySelectorAll(${JSON.stringify(sel)})].find((e) => e.textContent.trim().startsWith(${JSON.stringify(text)}))`;
async function click(b, find, scroll = true) {
  const at = await b.evaluate(`(() => { const e = ${find}; if (!e) return null; ${scroll ? "e.scrollIntoView({block:'center'});" : ''} const r = e.getBoundingClientRect(); return [r.x + r.width / 2, r.y + r.height / 2]; })()`);
  if (!at) throw new Error(`nothing to press: ${find}`);
  for (const type of ['mouseMoved', 'mousePressed', 'mouseReleased']) await b.send('Input.dispatchMouseEvent', { type, x: at[0], y: at[1], button: 'left', clickCount: 1 });
}
async function until(b, expr, ms = 90000) { for (let t = 0; t < ms; t += 250) { if (await b.evaluate(expr).catch(() => false)) return; await sleep(250); } throw new Error(`timed out: ${expr}`); }
const shot = async (b, name) => { const { data } = await b.send('Page.captureScreenshot', { format: 'png' }); fs.writeFileSync(`${PREFIX}-${name}.png`, Buffer.from(data, 'base64')); console.log('ok', name); };
const tab = (n) => byText('[role=tab]', n);
const SC = {
  // the Vedder's Conditions view: the gauge chart (default model), its About toggle, the notes fold at the foot
  conditions: async (b, size) => {
    await until(b, `document.body.textContent.includes('Vedder')`);
    await sleep(3000);
    await click(b, byText('[role=tab],[role=button]', 'Conditions'));
    await sleep(9000);
    await shot(b, `conditions-${size}`);
    // the chart block and what is under it, then About this forecast and Sources and notes opened (the labels differ before / after)
    const press = async (label) => { try { await click(b, byText('[role=button]', label)); await sleep(900); return true; } catch { return false; } };
    await b.evaluate(`(() => { const t = (x) => [...document.querySelectorAll('div,span')].find((e) => e.children.length === 0 && e.textContent.trim() === x); const e = t('Whole chart'); if (e) e.scrollIntoView({ block: 'start' }); })()`);
    await sleep(1200);
    await shot(b, `chart-${size}`);
    await b.evaluate(`(() => { const e = ${byText('[role=button]', 'Sources and notes')} || ${byText('[role=button]', 'Notes and sources')}; if (e) e.scrollIntoView({ block: 'end' }); })()`);
    await sleep(900);
    await shot(b, `foot-closed-${size}`);
    const a1 = await press('About this forecast');
    const a2 = (await press('Sources and notes')) || (await press('Notes and sources'));
    await b.evaluate(`(() => { const e = ${byText('[role=button]', 'Sources and notes')} || ${byText('[role=button]', 'Notes and sources')}; if (e) e.scrollIntoView({ block: 'center' }); })()`);
    await sleep(900);
    await shot(b, `foot-open-${size}`);
  },
  runs: async (b, size) => {
    await until(b, `!!document.querySelector('[role=tablist]')`);
    await sleep(4000);
    await click(b, tab('Runs'));
    await sleep(9000);
    try { await click(b, byText('[role=button]', 'Got it')); await sleep(1500); } catch {}
    await shot(b, `runs-${size}`);
  },
  more: async (b, size) => {
    await until(b, `!!document.querySelector('[role=tablist]')`);
    await sleep(2500);
    await click(b, tab('More'));
    await sleep(2500);
    await shot(b, `more-${size}`);
  },
  legend: async (b, size) => {
    await until(b, `!!document.querySelector('[role=tablist]')`);
    await sleep(8000);
    await shot(b, `legend-${size}`);
  },
  obstacles: async (b, size) => {
    await until(b, `!!document.querySelector('[role=tablist]')`);
    await sleep(5000);
    await click(b, `document.querySelector('[aria-label^="Choose layers"]')`, false);
    await until(b, `!!document.querySelector('[data-testid="layer-overlay.obstacles"]')`);
    await sleep(2500);
    await b.evaluate(`document.querySelector('[data-testid="layer-overlay.obstacles"]').click()`);
    await sleep(1500);
    if (process.env.DEBUG_SHOTS) await shot(b, `obstacles-sheet-${size}`);
    await click(b, byText('[role=button]', 'Done'), false);
    await sleep(3500);
    await shot(b, `obstacles-${size}`);
  },
};
const URLS = { conditions: '?regs=gnis:3062&date=2026-06-15' };
for (const name of SCENES) for (const [size, m] of Object.entries(SIZES)) {
  const b = await open(APP + '/' + (URLS[name] ?? ''), { ...m, init });
  try { await b.send('Emulation.setEmulatedMedia', { features: [{ name: 'prefers-reduced-motion', value: 'reduce' }] }); await b.send('Page.reload', {}); await sleep(1500); await SC[name](b, size); }
  catch (e) { console.log('FAIL', name, size, e.message); try { await shot(b, `fail-${name}-${size}`); } catch {} }
  finally { await b.close(); }
}
