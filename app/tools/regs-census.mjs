// T39 (layer D, manual): THE PALETTE CENSUS ON THE RENDERED APP — the prototype's census (app/design/regs-v11/tools/
// census.mjs `CENSUS`: per section, every visible element with its own text, its COMPUTED colour) run against Expo web,
// with the selectors the app's testID convention gives (contracts.md §2.7: `st:` status words, `n:` numbers, `chip:`,
// `btn:`; the four sections `keep-tab`, `regs-gear`, `regs-methods`, `regs-licence`). It drives the app as a reader would:
// the menu's Display page picks the palette, Search finds the water, the eye frames it on the map, a tap on the framed
// water opens its Regulations. Colours are named by the THEME'S TOKENS (packages/map/style/themes/<theme>.json), never by
// a hex typed here, and printed per tab next to the prototype's bound and its census (port/golden/tokens.json .census).
//
//   pnpm -C app/apps/mobile web          (or: EXPO_PUBLIC_DATA_BASE=… npx expo start --web --port <yours>)
//   node app/tools/regs-census.mjs [--url http://localhost:8081] [--theme light|dark|cvd] [--water "Vedder River" …] [--shot keep.png] [out.json]
//   --shot: a 390 × 844 shot of the first water's Keep tab with its fish pictures in view (D4: the owner looks once, in dark)
//
// States: `--water` names (repeatable), else `app/design/regs-v11/port/reader/states.json` `[{ "water": "…" }]` when T41 has
// written it, else the Vedder River. The day is the app's (today): the census is about colour, not the day's answer.
// Exit 1 when a tab breaks a rule (the same rules as ui-native/src/regs/testing/census.ts).
import fs from 'node:fs'; import path from 'node:path'; import { fileURLToPath } from 'node:url';
import { open } from '../design/regs-v11/tools/cdp.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args.splice(i, 2)[1] : d; };
const url = opt('--url', 'http://localhost:8081'), theme = opt('--theme', 'light'), shot = opt('--shot');
const waters = []; for (let w; (w = opt('--water'));) waters.push(w);
const out = args.find((a) => !a.startsWith('--'));
if (!['light', 'dark', 'cvd'].includes(theme)) { console.error(`--theme light|dark|cvd, not ${theme}`); process.exit(2); }
const statesFile = path.join(HERE, '../design/regs-v11/port/reader/states.json');
const STATES = waters.length ? waters
  : fs.existsSync(statesFile) ? [...new Set(JSON.parse(fs.readFileSync(statesFile, 'utf8')).map((s) => s.water).filter(Boolean))] : ['Vedder River'];

/** The theme's tokens, aliases (`@color.ui.card`) resolved: Palette key → hex. The same keys `paletteFor` reads. */
function tokens(name) {
  const v = JSON.parse(fs.readFileSync(path.join(HERE, `../packages/map/style/themes/${name}.json`), 'utf8')).values;
  const get = (k, n = 0) => { const x = v[k]; return typeof x === 'string' && x.startsWith('@') && n < 8 ? get(x.slice(1), n + 1) : x; };
  const KEYS = { ink: 'color.ui.ink', sub: 'color.ui.sub', faint: 'color.ui.faint', accent: 'color.ui.accent', onAccent: 'color.ui.on-accent',
    card: 'color.ui.card', tint: 'color.ui.tint', wash: 'color.ui.wash', page: 'color.ui.page', line: 'color.ui.line', line2: 'color.ui.line2',
    rule: 'color.ui.rule', quiet: 'color.ui.quiet', open: 'color.status.open', closed: 'color.status.closed', restricted: 'color.status.restricted' };
  return Object.fromEntries(Object.entries(KEYS).map(([k, t]) => [k, get(t)]).filter(([, x]) => typeof x === 'string'));
}
const TOK = tokens(theme);
const BOUND = { keep: 7, gear: 5, methods: 5, licence: 4 };   // check.mjs r20g BOUND at v38 (census.ts MAX_COLOURS)

// The prototype's CENSUS, the app's selectors: per section the text colours (named by token), plus the role rules.
const CENSUS = (tok) => `(() => {
  const TOK = ${JSON.stringify(tok)};
  const rgb = (h) => { const m = /^#(..)(..)(..)$/.exec(h); return m ? 'rgb(' + [1, 2, 3].map(i => parseInt(m[i], 16)).join(', ') + ')' : h; };
  const names = {}; for (const [k, h] of Object.entries(TOK)) (names[rgb(h)] ??= []).push(k);
  const PREF = ['ink', 'sub', 'accent', 'open', 'closed', 'restricted', 'rule', 'onAccent', 'card'];
  const nameOf = (c) => { const n = names[c] || []; return PREF.find(p => n.includes(p)) || n[0] || c; };
  const vis = e => { const r = e.getBoundingClientRect(); const cs = getComputedStyle(e); return r.width > 0 && r.height > 0 && cs.visibility !== 'hidden' && cs.display !== 'none'; };
  const textEls = root => [...root.querySelectorAll('*')].filter(e => vis(e) && !e.closest('svg') && [...e.childNodes].some(n => n.nodeType === 3 && n.textContent.trim()));
  const role = e => { const t = e.closest('[data-testid^="st:"],[data-testid^="n:"],[data-testid^="chip:"],[data-testid^="btn:"]'); const id = t && t.dataset.testid || '';
    return id.startsWith('st:') ? 'status' : id.startsWith('n:') ? 'number' : id.startsWith('chip:') ? 'chip' : id.startsWith('btn:') ? 'button' : 'text'; };
  const TEXT = ['ink', 'sub', 'accent', 'open', 'closed', 'restricted', 'rule', 'onAccent', 'card'], STATUS = ['open', 'closed', 'restricted'];
  const sect = (id, root) => { if (!root) return null; const els = textEls(root), bad = [], seen = new Map();
    for (const e of els) { const c = getComputedStyle(e).color, n = names[c] || [], r = role(e), t = (e.textContent || '').trim().slice(0, 30);
      seen.set(c, (seen.get(c) || 0) + 1);
      if (!n.some(x => TEXT.includes(x))) bad.push(id + ': ' + c + ' is no text token, on “' + t + '”');
      if (n.some(x => STATUS.includes(x)) && r !== 'status') bad.push(id + ': ' + nameOf(c) + ' on a non-status ' + r + ' “' + t + '”');
      if (n.some(x => x === 'card' || x === 'onAccent') && r !== 'button') bad.push(id + ': ' + nameOf(c) + ' text off a filled control “' + t + '”');
      if (r === 'number' && !n.includes(e.closest('[data-testid="tier:size"]') ? 'rule' : 'ink')) bad.push(id + ': a number in ' + nameOf(c) + ' “' + t + '”');
      const bg = getComputedStyle(e).backgroundColor; if (!/rgba\\(0, 0, 0, 0\\)|transparent/.test(bg)) { const b = names[bg] || [];
        if (b.includes('tint') ? (r !== 'chip' && e.dataset.testid !== 'note:paper') : !b.includes('card')) bad.push(id + ': ' + (b[0] || bg) + ' behind a ' + r + ' “' + t + '”'); }
      if (e.closest('[data-testid="st:release"]') && parseInt(getComputedStyle(e).fontWeight) < 700 && !/_[7-9]00/.test(getComputedStyle(e).fontFamily)) bad.push(id + ': release not bold “' + t + '”'); }
    document.querySelectorAll('[data-testid="keep-group"], [data-testid="keep-plain"], [data-testid="pin-bar"]').forEach(g => { if (!root.contains(g)) return;
      [g, ...g.querySelectorAll('*')].forEach(e => { const bg = getComputedStyle(e).backgroundColor; if (/rgba\\(0, 0, 0, 0\\)|transparent/.test(bg) || (names[bg] || []).includes('card') || role(e) === 'chip' || e.closest('svg')) return;
        bad.push(id + ': ' + (names[bg] || [bg])[0] + ' paints a surface in ' + g.dataset.testid); }); });
    root.querySelectorAll('[data-testid^="pic:"]').forEach(p => [p.parentElement, p, ...p.querySelectorAll('*')].forEach(e => { const bg = getComputedStyle(e).backgroundColor;
      if (!/rgba\\(0, 0, 0, 0\\)|transparent/.test(bg)) bad.push(id + ': a tile (' + ((names[bg] || [bg])[0]) + ') at ' + p.dataset.testid); }));
    return { colours: [...seen].map(([c, k]) => [nameOf(c), k]).sort((a, b) => b[1] - a[1]), count: seen.size, roles: Object.fromEntries(['status', 'number', 'chip', 'button'].map(r => [r, els.filter(e => role(e) === r).length])),
      pics: root.querySelectorAll('[data-testid^="pic:"]').length, bad }; };
  const title = document.querySelector('[data-testid=water-title]');
  return JSON.stringify({ water: title ? title.innerText.split('\\n')[0] : null, date: (document.querySelector('[data-testid=regs-date]')?.innerText || '').replace(/\\s+/g, ' '),
    keep: sect('keep', document.querySelector('[data-testid=keep-tab]')), gear: sect('gear', document.querySelector('[data-testid=regs-gear]')),
    methods: sect('methods', document.querySelector('[data-testid=regs-methods]')), licence: sect('licence', document.querySelector('[data-testid=regs-licence]')) });
})()`;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function until(b, expr, ms = 20000) {
  for (let t = 0; t < ms; t += 250) { if (await b.evaluate(expr).catch(() => false)) return true; await sleep(250); }
  throw new Error(`timed out waiting for ${expr}`);
}
/** a trusted tap at the centre of the first element the expression finds */
async function tap(b, find, what) {
  const box = JSON.parse(await b.evaluate(`(() => { const e = ${find}; if (!e) return 'null'; e.scrollIntoView({ block: 'center' }); const r = e.getBoundingClientRect(); return JSON.stringify([r.x + r.width / 2, r.y + r.height / 2]); })()`));
  if (!box) throw new Error(`no ${what} to tap`);
  await tapAt(b, box[0], box[1]);
}
async function tapAt(b, x, y) {
  await b.send('Input.dispatchMouseEvent', { type: 'mouseMoved', x, y });
  await b.send('Input.dispatchMouseEvent', { type: 'mousePressed', x, y, button: 'left', clickCount: 1 });
  await b.send('Input.dispatchMouseEvent', { type: 'mouseReleased', x, y, button: 'left', clickCount: 1 });
}
const byLabel = (re) => `[...document.querySelectorAll('[aria-label]')].find(e => ${re}.test(e.getAttribute('aria-label')))`;

const LABEL = { light: /^Light/, dark: /^Dark/, cvd: /^Colour-blind/ };
const result = { url, theme, tokens: TOK, states: [] };
const b = await open(url, { width: 390, height: 844 });
try {
  await until(b, `!!${byLabel('/^Open menu$/')}`, 60000);
  // the palette, as a reader picks it: Menu → Display → the choice → close
  await tap(b, byLabel('/^Open menu$/'), 'menu'); await until(b, `!!document.querySelector('[data-testid=menu-item-display]')`);
  await tap(b, `document.querySelector('[data-testid=menu-item-display]')`, 'Display');
  const radio = `[...document.querySelectorAll('[role=radio]')].find(e => ${String(LABEL[theme])}.test(e.getAttribute('aria-label')))`;
  await until(b, `!!${radio}`);
  for (let i = 0; i < 5 && (await b.evaluate(`${radio}.getAttribute('aria-checked')`)) !== 'true'; i++) { await sleep(600); await tap(b, radio, theme); }   // the page slides in: tap once it has stopped
  await until(b, `${radio}.getAttribute('aria-checked') === 'true'`, 3000);
  while (await b.evaluate(`!!${byLabel('/^(Close menu|Back)/')}`)) { await tap(b, byLabel('/^(Close menu|Back)/'), 'close'); await sleep(300); }
  for (const water of STATES) {
    await tap(b, `[...document.querySelectorAll('[role=tab]')].find(e => /^Search$/i.test(e.innerText.trim()) || e.getAttribute('aria-label') === 'Search')`, 'Search tab');
    await until(b, `!!document.querySelector('input')`);
    await b.evaluate(`(() => { const i = document.querySelector('input'); const set = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
      set.call(i, ${JSON.stringify(water)}); i.dispatchEvent(new Event('input', { bubbles: true })); })()`);
    const row = `RegExp('^Show ' + ${JSON.stringify(water.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))} + ',')`;
    await until(b, `!!${byLabel(row)}`);
    await tap(b, byLabel(row), `result ${water}`);
    const eye = `RegExp('^View ' + ${JSON.stringify(water.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))} + ',')`;
    await until(b, `!!${byLabel(eye)}`); await tap(b, byLabel(eye), `the eye of ${water}`);
    await sleep(2500);   // the map frames the water and draws it
    // the water is framed at the map's middle: tap outwards from there until its Regulations open (and are this water's)
    const [w, h] = JSON.parse(await b.evaluate('JSON.stringify([innerWidth, innerHeight])'));
    let opened = false;
    for (let ring = 0; ring <= 14 && !opened; ring++) for (let k = 0; k < Math.max(1, ring * 8) && !opened; k++) {
      const a = (2 * Math.PI * k) / Math.max(1, ring * 8), x = w / 2 + Math.cos(a) * ring * 10, y = h * 0.42 + Math.sin(a) * ring * 10;
      if (!(await b.evaluate(`document.elementFromPoint(${x}, ${y})?.tagName === 'CANVAS'`))) continue;
      await tapAt(b, x, y); await sleep(350);
      if (await b.evaluate(`!!document.querySelector('[data-testid=regs-content]')`)) {
        const name = await b.evaluate(`document.querySelector('[data-testid=water-title]')?.innerText.split('\\n')[0]`);
        if (name === water) opened = true;
        else { await tap(b, byLabel('/^Back|‹/'), 'back'); await sleep(500); }
      }
    }
    if (!opened) throw new Error(`could not open ${water}'s Regulations from the map`);
    await sleep(1500);
    const s = JSON.parse(await b.evaluate(CENSUS(TOK)));
    if (shot && !result.states.length) {
      await b.evaluate(`document.querySelector('[data-testid^="pic:"]')?.scrollIntoView({ block: 'center' })`); await sleep(800);
      fs.writeFileSync(shot, Buffer.from((await b.send('Page.captureScreenshot', { format: 'png' })).data, 'base64'));
      await b.evaluate(`document.querySelector('[data-testid=regs-scroll]')?.scrollTo?.(0, 0)`);
    }
    result.states.push(s);
    console.log(`\n${s.water} · ${s.date} · ${theme}`);
    for (const t of ['keep', 'gear', 'methods', 'licence']) {
      const x = s[t];
      if (!x) { console.log(`  ${t.padEnd(8)} (not drawn)`); continue; }
      console.log(`  ${t.padEnd(8)} ${String(x.count).padStart(2)} colours (prototype ≤ ${BOUND[t]})  ${x.colours.map(([n, k]) => `${n}×${k}`).join(' ')}  · roles ${JSON.stringify(x.roles)}${t === 'keep' ? ` · pictures ${x.pics}` : ''}`);
      if (x.count > BOUND[t]) x.bad.unshift(`${t}: ${x.count} text colours > ${BOUND[t]}`);
      for (const f of x.bad) console.log(`    FAULT ${f}`);
    }
    await tap(b, byLabel('/^Back|‹/'), 'back'); await sleep(800);
  }
} finally { await b.close(); }

// the prototype's census for the same theme (light for cvd, which the prototype has no column for): the tokens its text wears
const proto = JSON.parse(fs.readFileSync(path.join(HERE, '../design/regs-v11/port/golden/tokens.json'), 'utf8'));
const pc = proto.census[theme === 'dark' ? 'dark' : 'light'] ?? [];
const pt = [...new Set(pc.filter((r) => r.n > 0 && r.color).map((r) => { const n = r.color.split('|').map((c) => c.replace('--', '').replace('on-accent', 'onAccent')); return ['ink', 'sub', 'accent', 'open', 'closed', 'restricted', 'rule', 'onAccent', 'card'].find((p) => n.includes(p)) ?? n[0]; }))].sort();
console.log(`\nprototype ${proto.meta.version} census (${theme === 'dark' ? 'dark' : 'light'}): text tokens ${pt.join(' ')} · bound per tab ${JSON.stringify(BOUND)}`);
result.prototype = { version: proto.meta.version, textTokens: pt, bound: BOUND };
if (out) fs.writeFileSync(out, JSON.stringify(result, null, 1));
const faults = result.states.flatMap((s) => ['keep', 'gear', 'methods', 'licence'].flatMap((t) => s[t]?.bad ?? []));
console.log(faults.length ? `\n${faults.length} fault(s)` : '\nno faults');
process.exit(faults.length ? 1 : 0);
