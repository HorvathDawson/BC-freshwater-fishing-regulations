// (T603) THE SCREEN AS NODES: uiautomator's dump read into { id, cls, text, desc, box } rows, so a scenario can ask
// where an element is, whether it is on screen, whether its words fit its box, and scroll to it. Host side (adb), no
// Maestro: a flow can tap an id but cannot say where it sits or whether a label is squeezed.
//
// A SELECTOR is uiautomator's own attribute, written whole: 'resource-id="alert-form-line"'. Written that way on
// purpose: app/tools/testbed-static.test.ts reads every `resource-id="…"` in a scenario and requires the id in the app
// source, so a renamed testID fails in seconds, not in an emulator run. `{ prefix: true }` matches ids that START with
// it ('resource-id="visit-"'); `{ ends }` adds the variable tail of a built id.
//
// The pure parts (parse, find, shown, inside, squeezed, blankShare, bcDay) are tested in screen.test.mjs.
import { readFileSync } from 'node:fs';
import { readPng } from './png.mjs';

const unxml = (s) => s.replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>')
  .replace(/&#10;/g, '\n').replace(/&amp;/g, '&');

/** uiautomator XML → nodes, in document order (a parent before its children). */
export function parse(xml) {
  const out = [];
  for (const m of xml.matchAll(/<node\b[^>]*>/g)) {
    const g = (k) => unxml(new RegExp(`\\b${k}="([^"]*)"`).exec(m[0])?.[1] ?? '');
    const b = /\bbounds="\[(-?\d+),(-?\d+)\]\[(-?\d+),(-?\d+)\]"/.exec(m[0]);
    if (!b) continue;
    out.push({ id: g('resource-id'), cls: g('class').split('.').pop(), text: g('text'), desc: g('content-desc'),
               box: { x0: +b[1], y0: +b[2], x1: +b[3], y1: +b[4] } });
  }
  return out;
}

/** 'resource-id="x"' → 'x' (the one selector form; anything else is a mistake in the scenario). */
export function idOf(sel) {
  const m = /^resource-id="([^"]+)"$/.exec(sel);
  if (!m) throw new Error(`screen: a selector is written resource-id="<testID>" (got ${sel})`);
  return m[1];
}

/**
 * Every node the selector names. `prefix`: ids that start with it; `ends`: and end with this — for an id the app builds
 * from a variable (`alert-form-p${pct}`, `map-day-${key}`): the static check can prove only the fixed part, so the
 * selector carries only that and the variable part is said beside it.
 */
export function all(nodes, sel, { prefix = false, ends = null } = {}) {
  const id = idOf(sel);
  if (ends !== null) return nodes.filter((n) => n.id.startsWith(id) && n.id.endsWith(ends) && n.id.length >= id.length + ends.length);
  return nodes.filter((n) => (prefix ? n.id.startsWith(id) : n.id === id));
}
export const find = (nodes, sel, opts) => all(nodes, sel, opts)[0] ?? null;

/** Laid out with a real size. A node scrolled out under a header comes back with its bottom above its top. */
export const shown = (n) => !!n && n.box.x1 > n.box.x0 && n.box.y1 > n.box.y0;
const w = (n) => n.box.x1 - n.box.x0;
const h = (n) => n.box.y1 - n.box.y0;

/** `inner` lies inside `outer` (a pixel of slack: boxes are rounded). */
export const inside = (inner, outer, slack = 1) =>
  inner.box.x0 >= outer.box.x0 - slack && inner.box.x1 <= outer.box.x1 + slack
  && inner.box.y0 >= outer.box.y0 - slack && inner.box.y1 <= outer.box.y1 + slack;

/** The nodes drawn inside `parent` that come after it in the dump (its subtree, near enough for a chip or a row). */
export function within(nodes, parent) {
  const at = nodes.indexOf(parent);
  return nodes.slice(at + 1).filter((n) => shown(n) && inside(n, parent));
}

/**
 * A ONE-LINE label given less width than its words need. uiautomator reports the full string even when the phone draws
 * "Lower than 8 da…", so the only sign is the box: under `minPerChar` px a character on a single line (the app's
 * smallest reading type runs ~13–16 px a character on the 420 dpi test phone; 8 is far under any of it).
 */
export function squeezed(n, { minPerChar = 8, lineMax = 70 } = {}) {
  const len = [...n.text].length;
  return len >= 4 && h(n) <= lineMax && w(n) / len < minPerChar;
}

/** Why a label is "missing or cut off", or null when it reads whole: empty, not drawn, off the screen's sides, squeezed. */
export function labelProblem(n, { screenW, what = n?.id || 'label' } = {}) {
  if (!n) return `${what}: not on screen`;
  if (!shown(n)) return `${what}: not drawn (box ${JSON.stringify(n.box)})`;
  if (!(n.text || n.desc).trim()) return `${what}: no words`;
  if (n.box.x0 < 0 || (screenW && n.box.x1 > screenW)) return `${what}: runs off the screen's side (box ${JSON.stringify(n.box)}, "${n.text}")`;
  if (n.text && squeezed(n)) return `${what}: cut off, ${w(n)} px for "${n.text}"`;
  return null;
}

/** A chip (a pressable with one label inside): the chip is drawn, on screen, and its label lies inside it, whole. */
export function chipProblem(nodes, chip, { screenW, what = chip?.id || 'chip' } = {}) {
  if (!chip || !shown(chip)) return `${what}: not on screen`;
  if (chip.box.x0 < 0 || (screenW && chip.box.x1 > screenW)) return `${what}: runs off the screen's side (box ${JSON.stringify(chip.box)})`;
  const at = nodes.indexOf(chip);
  // its label: the first text node after it that overlaps it (inside → fine; sticking out → cut by the chip's edge)
  const label = nodes.slice(at + 1, at + 6).find((n) => n.text && shown(n) && n.box.x0 < chip.box.x1 && n.box.x1 > chip.box.x0
    && n.box.y0 < chip.box.y1 && n.box.y1 > chip.box.y0);
  if (!label) return `${what}: no label inside it`;
  if (!inside(label, chip)) return `${what}: its label "${label.text}" runs past the chip (label ${JSON.stringify(label.box)}, chip ${JSON.stringify(chip.box)})`;
  return labelProblem(label, { screenW, what: `${what} label` });
}

// ---------- the device ----------
/** The screen now, as nodes. Throws when uiautomator cannot dump (a spinner turning): the caller waits and asks again. */
export function nodes(device) {
  const r = device.sh('uiautomator dump /sdcard/tb-screen.xml 2>&1');
  if (!/dumped to/.test(r)) throw new Error(`uiautomator: ${r.trim().slice(0, 120)}`);
  return parse(device.sh('cat /sdcard/tb-screen.xml'));
}
/** `nodes`, tried again while the screen will not go idle (5 tries, 700 ms apart). */
export async function look(device) {
  let last;
  for (let i = 0; i < 5; i++) {
    try { return nodes(device); } catch (e) { last = e; await device.sleep(700); }   // poll interval
  }
  throw last;
}

export function screenSize(device) {
  const [W, H] = device.sh('wm size').match(/(\d+)x(\d+)/).slice(1).map(Number);
  return { W, H };
}

/** Tap a node's middle. */
export function tap(device, n) {
  if (!shown(n)) throw new Error(`tap: ${n?.id || n?.text || 'node'} is not on screen`);
  device.sh(`input tap ${Math.round((n.box.x0 + n.box.x1) / 2)} ${Math.round((n.box.y0 + n.box.y1) / 2)}`);
}

/**
 * Scroll the page until the selector's node sits whole between `top` and `bottom` (px: under the screen's fixed header,
 * above its foot), then return it. A drag of `step` px a time, slow enough not to fling; it turns round once at the
 * page's end. Throws naming the id when it never shows. The drag runs down the screen's LEFT EDGE (`x`), in the page's
 * gutter: a drag that starts on a map or a chart is that view's own (it pans the map) and the page does not move.
 */
export async function scrollTo(device, sel, { prefix = false, top = 720, bottom = 2150, step = 420, max = 30, x = 16, pick } = {}) {
  const get = async () => { const ns = await look(device); const hit = pick ? all(ns, sel, { prefix }).find((n) => pick(n, ns)) : find(ns, sel, { prefix }); return { ns, hit }; };
  const mid = Math.round((top + bottom) / 2);
  let dir = 1, turned = false, prev = '';
  for (let i = 0; i < max; i++) {
    const { ns, hit } = await get();
    if (hit && shown(hit) && hit.box.y0 >= top && hit.box.y1 <= bottom) return hit;
    // the node is in the tree above the band (scrolled under the header) or cut by an edge: go its way
    let d = dir;
    if (hit && shown(hit)) d = hit.box.y1 > bottom ? 1 : -1;
    else if (hit && hit.box.y1 <= hit.box.y0) d = -1;
    const sig = ns.filter(shown).map((n) => `${n.id}${n.text}${n.box.y0}`).join('|');
    if (sig === prev) { if (turned) break; turned = true; dir = -dir; d = dir; }   // the page's end: once, the other way
    prev = sig;
    const dy = Math.round(step / 2) * d;
    device.sh(`input swipe ${x} ${mid + dy} ${x} ${mid - dy} 450`);
    await device.sleep(500);   // poll interval: the drag's own settle
  }
  throw new Error(`scrollTo: ${idOf(sel)}${prefix ? '…' : ''} never came on screen between y ${top} and ${bottom}`);
}

// ---------- pixels ----------
/**
 * How much of a box is ONE colour: the share of sampled pixels (every 4th each way) within 12 of the box's commonest
 * colour, and that colour. A map that drew has water, land and lines (the route map's share is ~0.4–0.8); a map that did
 * not is one flat fill (≥ 0.985).
 */
export function blankShare(img, box) {
  const x0 = Math.max(0, box.x0), y0 = Math.max(0, box.y0), x1 = Math.min(img.w, box.x1), y1 = Math.min(img.h, box.y1);
  const seen = new Map();
  const px = [];
  for (let y = y0; y < y1; y += 4) for (let x = x0; x < x1; x += 4) {
    const i = (y * img.w + x) * img.ch;
    const r = img.px[i], g = img.px[i + 1], b = img.px[i + 2];
    px.push(r, g, b);
    const k = ((r >> 3) << 10) | ((g >> 3) << 5) | (b >> 3);
    seen.set(k, (seen.get(k) ?? 0) + 1);
  }
  const n = px.length / 3;
  if (!n) return { share: 1, colour: 'none', n: 0 };
  const [top] = [...seen].sort((a, b) => b[1] - a[1])[0];
  const c = [((top >> 10) & 31) << 3, ((top >> 5) & 31) << 3, (top & 31) << 3].map((v) => v + 4);
  let near = 0;
  for (let i = 0; i < px.length; i += 3) if (Math.abs(px[i] - c[0]) + Math.abs(px[i + 1] - c[1]) + Math.abs(px[i + 2] - c[2]) <= 36) near++;
  return { share: near / n, colour: `rgb(${c.join(',')})`, n };
}
export const BLANK_AT = 0.985;
/** `blankShare` of a screenshot file's box. */
export const blankShareOf = (file, box) => blankShare(readPng(file), box);

// ---------- B.C.'s day ----------
const WD = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
// the month names come from the one calendar spec (generated, test_dataflow_gates gate 4), never typed again here
const MO = JSON.parse(readFileSync(new URL('../../../packages/core/src/calendar.generated.json', import.meta.url), 'utf8')).months;
/**
 * "Thu 12 Nov": the day `offset` days from B.C.'s today, as the app names a day (core `dayWords`). B.C. is on daylight
 * time all year (UTC−7, settled), whatever zone the emulator's own clock shows.
 */
export function bcDay(offset = 0, now = Date.now()) {
  const d = new Date(now - 7 * 3_600_000 + offset * 86_400_000);
  return `${WD[d.getUTCDay()]} ${d.getUTCDate()} ${MO[d.getUTCMonth()]}`;
}
