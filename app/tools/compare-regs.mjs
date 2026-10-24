// T41 (layer D) — THE SIDE-BY-SIDE: the same state in the prototype (staged at the port target's ref) and in the app on
// Expo web, 390 × 844 at DPR 2, as one PNG [prototype | app | per-pixel diff (red where |Δ| > 24)], both columns aligned
// at the regs header's top (D2: the app's water frame — back and the Regulations · Conditions · Map strip — sits above
// it). Prints the diff % per shot; no pass/fail (the reader gate is the human's: tests.md §4).
//
//   node app/tools/compare-regs.mjs --state vedder_1010 [--tab gear] [--sheet 'pos:vedder_1010|pos'] [--theme dark]
//        [--who u16,non_resident] [--ref <ref>] [--url http://localhost:8081] [--clock proto|real] [--out DIR]
//        [--scale 0.4 --jpeg 0.72]
//   node app/tools/compare-regs.mjs --all [--out DIR]         every WALK_STATES state × tab (light), Vedder dark, the sheet set
//   node app/tools/compare-regs.mjs --reader OUT_DIR [--theme dark]  the reader set (app/fixtures/reader-states.json), app only, with the
//                                                            prototype's PNG names: OUT/TALL, OUT/FRAMES, OUT/TALL/NEW
// Writes OUT/<id>-<tab>[-<sheet>]-<theme>.png and OUT/compare.json (every shot's diff %).
import fs from 'node:fs'; import path from 'node:path'; import { fileURLToPath } from 'node:url';
import { RECT, SCROLL, openApp, resize, scrollTo, shoot, sleep, tap, tid, until } from './regs-web/drive.mjs';
import { APP_URL, PHONE, PROTO_TODAY, REF, TABS, WALK_STATES, appKeyOf, stateOf, urlsOf } from './regs-web/states.mjs';
import { PROTO_GEO, openProto, stageProto } from './regs-web/proto.mjs';
import { closeCompose, compose, stack } from './regs-web/compose.mjs';
import { END_BOTTOMS, tallEnd } from './regs-web/tall.mjs';
import { rowTopExpr } from './regs-web/rows.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; };
const url = opt('--url', APP_URL), clock = opt('--clock', 'proto'), ref = opt('--ref', REF);
// --scale 0.4 --jpeg 0.75: the smaller copies a report commits (the diff % is measured at full size either way)
const scale = Number(opt('--scale', '1')), jpeg = opt('--jpeg', null) === null ? null : Number(opt('--jpeg')), EXT = jpeg === null ? 'png' : 'jpg';
const TAB_NAMES = { keep: 'What you can keep', gear: 'Gear', methods: 'Methods', licence: 'Licence' };
const whoOf = (s) => { if (!s) return null; const [age, res] = s.split(','); return { age: age || 'adult', res: res || 'resident' }; };
const slug = (k) => k.replace(/[^A-Za-z0-9_-]+/g, '_').replace(/_+$/, '');
/** the app's clock: the prototype's today (its snapshot day, PROTO_TODAY), or `--clock real` */
const dayOf = () => (clock === 'real' ? null : PROTO_TODAY);

/** press a header tab as a reader does; the page scrolls to its section */
async function toTab(b, t) {
  await tap(b, `[...document.querySelectorAll('[role=tab]')].find((e) => e.innerText.trim() === ${JSON.stringify(TAB_NAMES[t])} && e.closest('[data-testid=regs-header]'))`, `tab ${t}`);
  await sleep(1200);
}
const SHEET_OPEN = `!!(${tid('sheet-body')})`;
async function closeSheet(b) {
  await tap(b, `[...document.querySelectorAll('[role=button]')].find((e) => e.innerText.trim() === 'Done' || e.getAttribute('aria-label') === 'Done')`, 'Done');
  await until(b, `!${SHEET_OPEN}`, 4000, 'the sheet to close');
}
/** the app column: from the regs header's top to the viewport's foot */
async function appColumn(b) {
  const top = await b.evaluate(`${tid('regs-header')}.getBoundingClientRect().top`);
  return shoot(b, { x: 0, y: top, width: PHONE.width, height: PHONE.height - top });
}
/** the prototype column: from its regs header's top to the .app box's foot */
async function protoColumn(s, hash) {
  const b = await openProto(s, hash);
  try { const g = await b.evaluate(PROTO_GEO); return await shoot(b, { x: g.app.x, y: g.hdr.top, width: g.app.w, height: g.app.bottom - g.hdr.top }); }
  finally { await b.close(); }
}

// ── side-by-side ──────────────────────────────────────────────────────────────────────────────────────────────────
async function sideBySide(out, list) {
  const s = stageProto(ref);
  const log = [];
  try {
    // one app page per (state, theme, who): the tabs pressed and the sheets opened in it, as a reader would
    const groups = new Map();
    for (const x of list) { const k = `${x.id}|${x.theme}|${x.who ? `${x.who.age},${x.who.res}` : ''}`; (groups.get(k) ?? groups.set(k, []).get(k)).push(x); }
    for (const items of groups.values()) {
      const { id, theme, who } = items[0];
      const b = await openApp(urlsOf(id, { theme, who, app: url }).app, { day: dayOf(id) });
      try {
        for (const x of items) {
          const name = `${id}-${x.tab}${x.sheet ? `-${slug(appKeyOf(x.sheet, id))}` : ''}-${theme}${who ? `-${who.age}-${who.res}` : ''}.${EXT}`;
          await scrollTo(b, 0);
          if (x.tab !== 'keep') await toTab(b, x.tab);
          if (x.sheet) {
            const ok = await b.evaluate(`globalThis.__regsDebug?.openSheet(${JSON.stringify(appKeyOf(x.sheet, id))}) ?? false`);
            if (!ok) throw new Error(`the app cannot open ${x.sheet} (dev hook missing or a bad key)`);
            await until(b, SHEET_OPEN, 5000, `sheet ${x.sheet}`); await sleep(700);
          }
          const appPng = await appColumn(b);
          if (x.sheet) await closeSheet(b);
          const protoPng = await protoColumn(s, urlsOf(id, { tab: x.tab, sheet: x.sheet, who, theme }).proto);
          const { png, pct } = await compose(protoPng, appPng, { labels: [`prototype ${s.version} @ ${s.sha}`, 'app (Expo web)', 'diff (red: |Δ| > 24)'],
            theme, scale, jpeg });
          fs.writeFileSync(path.join(out, name), png);
          log.push({ file: name, id, tab: x.tab, sheet: x.sheet ?? null, theme, who, diff: +pct.toFixed(2) });
          console.log(`${name}  diff ${pct.toFixed(2)} %`);
        }
        if (b.errors.length) console.log(`  console errors on ${id}/${theme}: ${b.errors.join(' | ').slice(0, 300)}`);
      } finally { await b.close(); }
    }
  } finally { s.cleanup(); await closeCompose(); }
  fs.writeFileSync(path.join(out, 'compare.json'), JSON.stringify({ ref, url, clock, shots: log }, null, 1));
}

/** --all: every walk state × tab (light), the Vedder's four tabs in dark, and a sheet of every kind */
function allShots() {
  const list = [];
  for (const id of WALK_STATES) for (const tab of TABS) list.push({ id, tab, theme: 'light' });
  for (const tab of TABS) list.push({ id: 'vedder_1010', tab, theme: 'dark' });
  const SHEETS = [['vedder_1010', 'pos:vedder_1010|pos'], ['vedder_1010', 'hatch'], ['vedder_1010', 'gl:single_barbless_hook'],
    ['vedder_1010', 'licsrc:lic|vedder_1010|adult'], ['vedder_1010', 'srcs:bait|vedder_1010'], ['vedder_1010', 'srcs:gl|vedder_1010|hook'],
    ['vedder_1010', 'other:vedder_1010|o0'], ['klwa_1010', 'other:klwa_1010|o0'], ['bc1_501', 'parts'], ['vedder_1010', 'pos:vedder_1010|pos', 'dark']];
  for (const [id, sheet, theme = 'light'] of SHEETS) list.push({ id, tab: 'keep', sheet, theme });
  // the first rule: and sp: sheets the Vedder's golden holds
  const st = JSON.parse(fs.readFileSync(path.join(HERE, '../fixtures/golden/golden.json'), 'utf8')).states.vedder_1010.sheets;
  for (const re of [/^rule:/, /^sp:/, /^src:/]) { const k = Object.keys(st).find((x) => re.test(x)); if (k) list.push({ id: 'vedder_1010', tab: 'keep', sheet: k, theme: 'light' }); }
  return list;
}

// ── the reader set: the prototype's PNG names, the app's screens (tests.md §4) ─────────────────────────────────────
const READER = path.join(HERE, '../fixtures/reader-states.json');
async function reader(out) {
  const set = JSON.parse(fs.readFileSync(READER, 'utf8'));
  for (const d of ['TALL', 'FRAMES', 'TALL/NEW']) fs.mkdirSync(path.join(out, d), { recursive: true });
  for (const r of set) {
    const who = r.who ?? null;
    const b = await openApp(urlsOf(r.state, { who, theme: opt('--theme', null) ?? r.theme ?? 'light', app: url }).app, { day: dayOf(r.state) });
    try {
      if (r.folder === 'FRAMES') await frames(b, r, path.join(out, 'FRAMES'));
      else fs.writeFileSync(path.join(out, r.folder === 'NEW' ? 'TALL/NEW' : 'TALL', `${r.name}.png`), await tall(b, r));
      console.log(`${r.folder}/${r.name}${r.mode === 'frames' ? '-fNN' : ''}.png  (${r.state} ${r.mode}${r.tab ? ' ' + r.tab : ''}${r.sheet ? ' ' + r.sheet : ''})`);
      if (b.errors.length) console.log(`  console errors: ${b.errors.join(' | ').slice(0, 300)}`);
    } finally { await b.close(); }
  }
}

/** TALL: the header on top, then the cards / the whole Keep tab / one tab's section / the open sheet, laid out whole */
async function tall(b, r) {
  const head = await b.evaluate(`(() => { ${RECT} return R(${tid('regs-header')}); })()`);
  if (r.mode === 'sheet') {
    const h = await shoot(b, { x: 0, y: head.top, width: PHONE.width, height: head.h });   // the header undimmed, before the sheet
    const ok = await b.evaluate(`globalThis.__regsDebug?.openSheet(${JSON.stringify(appKeyOf(r.sheet, r.state))}) ?? false`);
    if (!ok) throw new Error(`cannot open ${r.sheet}`);
    await until(b, SHEET_OPEN, 5000, r.sheet); await sleep(600);
    // the sheet's whole body: the viewport grown to its content, then the header + the sheet (handle, title, body)
    const need = await b.evaluate(`(() => { const sc = ${tid('sheet-scroll')} || ${tid('sheet-body')}.parentElement; return sc.scrollHeight - sc.clientHeight; })()`);
    if (need > 0) { await resize(b, PHONE.width, PHONE.height + need + 40); await sleep(500); }
    // the sheet itself: from its handle (else its body) down to its body's foot — not the scrim and the page under it
    const g = await b.evaluate(`(() => { ${RECT} const body = ${tid('sheet-body')}, top = ${tid('sheet-handle')} || body; const t = R(top).top, f = R(body).bottom; return { top: t, h: f - t + 16 }; })()`);
    const sh = await shoot(b, { x: 0, y: g.top, width: PHONE.width, height: g.h });
    return stack([h, sh]);
  }
  if (r.mode === 'tab' || r.mode === 'licence') {
    // the viewport grown to the section's height, then its tab pressed as a reader would: the section lands under the
    // header and its tab is the selected one
    const sec = r.tab ?? 'licence';
    const sh = await b.evaluate(`${tid(`regs-${sec}`)}.getBoundingClientRect().height`);
    await resize(b, PHONE.width, head.bottom + sh + 4);
    await toTab(b, sec);
    const g = await b.evaluate(`(() => { ${RECT} return R(${tid(`regs-${sec}`)}); })()`);
    return shoot(b, { x: 0, y: head.top, width: PHONE.width, height: g.bottom - head.top });
  }
  // grow the viewport so the whole page is laid out unscrolled, then clip
  const content = await b.evaluate(`${tid('regs-content')}.getBoundingClientRect().height`);
  await resize(b, PHONE.width, head.bottom + content + 40);
  // cards: down to the last card or the Other fish list under them (T137, the prototype's tall.mjs); keep: the tab's foot
  const end = tallEnd(r.mode, await b.evaluate(END_BOTTOMS), await b.evaluate(`${tid('keep-tab')}.getBoundingClientRect().bottom`));
  return shoot(b, { x: 0, y: head.top, width: PHONE.width, height: end - head.top + 8 });
}

/**
 * FRAMES: consecutive screenfuls from the regs header down, 90 px of overlap, stepped back so no text line straddles the
 * pin bar's foot (frames.mjs's rule); `row`: the one screenful with that fish's row just under the pin bar.
 */
async function frames(b, r, dir) {
  const top = await b.evaluate(`${tid('regs-header')}.getBoundingClientRect().top`);
  const frame = () => shoot(b, { x: 0, y: top, width: PHONE.width, height: PHONE.height - top });
  const barFoot = `(() => { const s = ${SCROLL}, bar = ${tid('pin-bar')}; const sb = s.getBoundingClientRect().top; return bar && bar.getBoundingClientRect().height > 0 ? bar.getBoundingClientRect().bottom : sb; })()`;
  const straddle = `(() => { const foot = ${barFoot}; const w = document.createTreeWalker(${tid('regs-content')}, NodeFilter.SHOW_TEXT); let n;
    while ((n = w.nextNode())) { if (!n.textContent.trim()) continue; const rg = document.createRange(); rg.selectNodeContents(n);
      for (const q of rg.getClientRects()) if (q.top < foot - 1 && q.bottom > foot + 1) return foot - q.top + 4; } return 0; })()`;
  if (r.mode === 'row') {
    // (T138) the row whose own picture strip carries the fish, with its tier head when it opens the tier (rows.mjs), 8 px
    // under the pin bar's foot. The bar's height changes with the scroll (its chain follows the rows), so settle in steps.
    let at = await b.evaluate(`${SCROLL}.scrollTop`);
    for (let k = 0; k < 4; k++) {
      const d = await b.evaluate(rowTopExpr(barFoot, r.row));
      if (d === null) throw new Error(`${r.state}: no row with picture ${r.row}`);
      if (Math.abs(d - 8) < 1) break;
      at = await scrollTo(b, Math.max(0, at + d - 8));
    }
    fs.writeFileSync(path.join(dir, `${r.name}.png`), await frame());
    return;
  }
  const geo = await b.evaluate(`(() => { const s = ${SCROLL}; return { step: s.clientHeight - 90, max: s.scrollHeight - s.clientHeight }; })()`);
  let i = 0, last = -1;
  for (let want = 0; ; i++) {
    let at = await scrollTo(b, Math.min(want, geo.max));
    for (let k = 0; k < 3 && at > 0; k++) { const back = await b.evaluate(straddle); if (!back) break; at = await scrollTo(b, Math.max(0, at - back)); }
    if (i && at <= last) at = await scrollTo(b, Math.min(last + Math.round(geo.step / 2), geo.max));
    last = at;
    fs.writeFileSync(path.join(dir, `${r.name}-f${String(i + 1).padStart(2, '0')}.png`), await frame());
    if (at >= geo.max || (r.limit && i + 1 >= r.limit)) break;
    want = at + geo.step;
  }
}

// ── main ───────────────────────────────────────────────────────────────────────────────────────────────────────────
const out = path.resolve(opt('--reader', null) ?? opt('--out', path.join(process.cwd(), 'compare-regs')));
fs.mkdirSync(out, { recursive: true });
if (args.includes('--reader')) { try { await reader(out); } finally { await closeCompose(); } }   // the canvas page holds the process open
else if (args.includes('--all')) await sideBySide(out, allShots());
else {
  const id = opt('--state', 'vedder_1010');
  stateOf(id);
  await sideBySide(out, [{ id, tab: opt('--tab', 'keep'), sheet: opt('--sheet', null), theme: opt('--theme', 'light'), who: whoOf(opt('--who', null)) }]);
}
console.log(`→ ${out}`);
