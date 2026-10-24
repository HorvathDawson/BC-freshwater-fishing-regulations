// T41 — driving the app's Regulations page on Expo web (react-native-web: `data-testid` from `testID`, roles and names
// from accessibilityRole / accessibilityLabel). Shared by regs-walk.mjs, compare-regs.mjs and regs-census.mjs.
import { open } from './cdp.mjs';
import { PHONE } from './states.mjs';

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
export const tid = (id) => `document.querySelector('[data-testid=${JSON.stringify(id)}]')`;

/**
 * The page's clock set to a state's day (local noon, still ticking): the prototype's snapshot is "today" on its golden
 * day, so the app is put on the same day — its header then says "Today" as the prototype's does. `null`: the real clock.
 */
export function clockInit(isoDay) {
  if (!isoDay) return '';
  const [y, m, d] = isoDay.split('-').map(Number);
  return `(() => { const D = Date, off = new D(${y}, ${m - 1}, ${d}, 12).getTime() - D.now();
    function F(...a) { if (!new.target) return new D(D.now() + off).toString(); return a.length ? new D(...a) : new D(D.now() + off); }
    F.prototype = D.prototype; F.now = () => D.now() + off; F.UTC = D.UTC; F.parse = D.parse; globalThis.Date = F; })();`;
}

/** Open the app at a start link and wait until its Regulations page stands still (fonts, pictures, a settle). */
export async function openApp(url, { width = PHONE.width, height = PHONE.height, dpr = PHONE.dpr, day = null, timeout = 180000 } = {}) {
  const b = await open(url, { width, height, dpr, init: clockInit(day) });
  try { await ready(b, timeout); } catch (e) { await b.close(); throw e; }
  return b;
}

export async function until(b, expr, ms = 20000, what = expr) {
  for (let t = 0; t < ms; t += 200) { if (await b.evaluate(expr).catch(() => false)) return; await sleep(200); }
  throw new Error(`timed out (${ms} ms) waiting for ${what}`);
}

export async function ready(b, ms = 180000) {
  await until(b, `!!${tid('regs-header')} && !!${tid('keep-tab')}`, ms, 'the Regulations page');
  await b.evaluate('document.fonts.ready.then(() => 1)');
  await until(b, `[...document.images].every((i) => i.complete)`, 20000, 'the pictures');
  await sleep(800);
}

/** a trusted tap at an element's centre (the expression finds it); `scroll`: brought into view first */
export async function tap(b, find, what = find, { scroll = false } = {}) {
  const box = await b.evaluate(`(() => { const e = ${find}; if (!e) return null;${scroll ? " e.scrollIntoView({ block: 'center' });" : ''} const r = e.getBoundingClientRect(); return [r.x + r.width / 2, r.y + r.height / 2]; })()`);
  if (!box) throw new Error(`nothing to tap: ${what}`);
  await tapAt(b, box[0], box[1]);
}

/** a trusted tap at a point (CSS px) */
export async function tapAt(b, x, y) {
  for (const type of ['mouseMoved', 'mousePressed', 'mouseReleased'])
    await b.send('Input.dispatchMouseEvent', { type, x, y, button: 'left', clickCount: 1 });
}

/** the one scroll's DOM node, and setting its offset (instant) */
export const SCROLL = tid('regs-scroll');
export async function scrollTo(b, y) {
  // scrollTop, not scrollTo(): react-native-web's node ignores an instant scrollTo() from outside
  const at = await b.evaluate(`(() => { const s = ${SCROLL}; s.scrollTop = ${y}; return s.scrollTop; })()`);
  await sleep(350);
  return at;
}

/** a rect as plain numbers */
export const RECT = `const R = (e) => { const r = e.getBoundingClientRect(); return { x: r.x, y: r.y, w: r.width, h: r.height, top: r.top, bottom: r.bottom, left: r.left, right: r.right }; };`;

/** PNG of a clip (CSS px) at the page's DPR; `capture` lets a clip reach below the viewport */
export async function shoot(b, clip) {
  const r = await b.send('Page.captureScreenshot', { format: 'png', captureBeyondViewport: true, clip: { ...clip, scale: 1 } });
  return Buffer.from(r.data, 'base64');
}

/** the viewport's size (CSS px) — a tall shot grows it so the whole page is laid out at once */
export async function resize(b, width, height, dpr = PHONE.dpr) {
  await b.send('Emulation.setDeviceMetricsOverride', { width, height: Math.ceil(height), deviceScaleFactor: dpr, mobile: false });
  await sleep(700);
}
