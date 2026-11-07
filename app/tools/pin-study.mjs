#!/usr/bin/env node
// T481 — THE STICKY-HEADER DESIGN STUDY: a mock harness, never the app's default. It opens the running Expo web app on a
// water, opens a group, scrolls so the pin stack is full, then REPLACES the pin bar's drawing with one study option (the
// page under it is the real page, scrolled). Today's app code is untouched: the options are this file's DOM, built from
// the census of what can pin (the stored tiers' words + numbers) — what the app would need is listed in the ticket.
//
//   node tools/pin-study.mjs shoot <outDir> <census.json>  every case × option × width (390, 360) × theme (light, dark)
//   node tools/pin-study.mjs sheet <outDir>        one contact sheet per width (options across, cases down)
//   node tools/pin-study.mjs words <census.json>   every option's words over every stack the census holds (longest each)
//
// APP_URL (default http://localhost:39218): the Expo web server, reading the release you want (EXPO_PUBLIC_DATA_BASE).
// census.json: one row per water × part × segment × group (the pin line, every tier's short/line/n/relation/exception),
// written by a one-off vitest pass over the release (see the T481 ticket).
import fs from 'node:fs'; import path from 'node:path';
import { open } from './regs-web/cdp.mjs';

const APP = process.env.APP_URL || 'http://localhost:39218';
const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const read = (f, re) => re.exec(fs.readFileSync(path.join(ROOT, f), 'utf8'))?.[1];
const FIRST_RUN = read('packages/core/src/menu/legal.ts', /FIRST_RUN_VERSION = "([^"]+)"/);
const WALK = read('packages/core/src/help.ts', /WALKTHROUGH_VERSION = "([^"]+)"/);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const INIT = `try { localStorage.setItem('canifishthis.firstRun.v1', ${JSON.stringify(FIRST_RUN)});
  localStorage.setItem('canifishthis.walkthrough.v1', ${JSON.stringify(WALK)}); localStorage.setItem('canifishthis.inseasonSeen.v1', '["*"]'); } catch {}`;

// ── the stack: what the pin area holds at one scroll offset, as the stored facts ─────────────────────────────────────
/** Every root → leaf path of tiers, as the pin chain stacks them (form a's share equal to the lead says nothing new). */
export function chainsOf(row) {
  const by = new Map();
  for (const t of row.tiers) { const k = t.path.join('/'); if (!by.has(k)) by.set(k, []); by.get(k).push(t); }
  const out = [];
  const walk = (prefix, acc) => {
    const kids = by.get(prefix.join('/')) ?? [];
    if (!kids.length) { out.push(acc); return; }
    for (const t of kids) walk([...prefix, t.tid], row.form === 'a' && t.tier === 'share' && t.n === row.leadN ? acc : [...acc, t]);
  };
  walk([], []);
  return out;
}

/**
 * The facts one stack shows: the group (name; form a's share lead over the region total, or the root's own short line)
 * and the tiers on the path. The app holds every one of these already (GroupVM.pinLine, TierVM.short/line/n/relation/
 * exception); `outShare` / `inTotal` are read from the stored relation here — the ticket proposes them as stored flags.
 */
const sp = (x) => (x == null ? x : x.replace(/\u00a0/g, ' '));
export function stackOf(row0, chain0) {
  // the app's words carry no-break spaces (nbsp'd numbers and units); the study reads and draws plain ones
  const row = { ...row0, pin: { text: sp(row0.pin.text), sub: sp(row0.pin.sub) } };
  const chain = chain0.map((t) => ({ ...t, short: sp(t.short), line: sp(t.line), relation: sp(t.relation) }));
  const [, name, rootShort] = /^(.*?) · (.*)$/.exec(row.pin.text) ?? [null, row.pin.text, ''];
  const sub = row.pin.sub ? /\(of (\d+) (?:a day )?across (.+)\)/.exec(row.pin.sub) : null;
  const across = / across (.+)$/.exec(rootShort);
  const total = sub ? { n: +sub[1], across: sub[2] } : across ? { n: +/^(\d+)/.exec(rootShort)[1], across: across[1] } : null;
  const lead = row.form === 'a' ? { n: row.leadN, from: /from (streams|lakes|this stream|this lake)/.exec(rootShort)?.[1] ?? 'streams' } : null;
  return {
    name, rootShort, total, lead, groupText: row.pin.text, groupSub: row.pin.sub,
    tiers: chain.map((t) => {
      const rel = (t.relation ?? '').toLowerCase();
      // "1 of them (brook trout, …) under X's rule" | "4 hatchery rainbow trout a day under X's rule, not part of the 2 …"
      const own = t.tier !== 'own' ? null : /^(\d+) of them(?: \((.+)\))? under (.+)$/.exec(t.short)?.slice(0, 4)
        ?? (/^(\d+) (.*?)(?: a day)? under (.+?)(?:, not part of .*)?$/.exec(t.short) ?? []).slice(0, 4);
      return { tier: t.tier, n: t.n, short: t.short, line: t.line, exc: !!t.exc,
        names: own ? (own[2] || null) : null, whose: own ? own[3] : null, ofThem: / of them/.test(t.short),
        outShare: /not part of the \d+ from/.test(rel),
        // the share it stands outside ("not part of the 2 from streams"): its own number, never the group's lead assumed
        outOf: (/not part of the (\d+) from (this stream|this lake|streams|lakes)/.exec(t.short ?? '') ?? []).slice(1), inTotal: /; part of the \d+ a day across/.test(rel) };
    }),
  };
}

// ── the options: words (and, for the shots, a drawing) for one stack ────────────────────────────────────────────────
const lc1 = (s) => s.charAt(0).toLowerCase() + s.slice(1);
/** a size header's line, shorter: "(20 in)" stays in the card (the pin keeps the cm the book leads with) */
const sizeShort = (t) => t.short;   // "of those, 1 over 50 cm (2 if one is a hatchery steelhead)": the card keeps the inches
const countShort = (short) => short.replace(/^of those, Up to/, 'of those, up to');
const ownPart = (t) => t.ofThem ? `up to ${t.n} of them${t.names ? ` (${t.names})` : ''} under ${t.whose}` : `up to ${t.n} ${t.names ?? ''} under ${t.whose}`;

/** A: ONE SENTENCE. The region total leads; every part is "up to N …" inside it, so nothing reads as added on. */
export function wordsA(st) {
  const head = st.total ? `${st.name}: ${st.total.n} a day across ${st.total.across}` : `${st.name}: ${st.rootShort}`;
  const parts = [];
  const out = st.tiers.find((t) => t.tier === 'own' && t.outShare);
  if (out) parts.push(`here up to ${out.n} ${out.names}, and no more than ${out.outOf[0]} others from ${out.outOf[1]}`);
  else if (st.lead) parts.push(`no more than ${st.lead.n} from ${st.lead.from}`);
  for (const t of st.tiers) {
    if (t === out) continue;
    if (t.tier === 'size') parts.push(sizeShort(t).replace(/^of those, (\d)/, 'of those, up to $1'));
    else if (t.tier === 'count') parts.push(countShort(t.short));
    else if (t.tier === 'own') parts.push(ownPart(t));
    else if (t.tier === 'share') parts.push(`no more than ${t.n} from streams`);
  }
  return [parts.length ? `${head} — ${parts.join('; ')}` : head];
}

/** B: NUMBER-LED. The total on line 1, then each limit inside it as "max N …" (the number first, heavy). */
export function wordsB(st) {
  const head = st.total ? `${st.name} · ${st.total.n} a day, all of ${st.total.across}` : `${st.name} · ${st.rootShort}`;
  const rows = [];
  const out = st.tiers.find((t) => t.tier === 'own' && t.outShare);
  if (out) { rows.push(`max ${out.n} ${out.names} (${out.whose})`); rows.push(`max ${out.outOf[0]} of any other from ${out.outOf[1]}`); }
  else if (st.lead) rows.push(`max ${st.lead.n} from ${st.lead.from}`);
  for (const t of st.tiers) {
    if (t === out) continue;
    if (t.tier === 'size') rows.push(`max ${sizeShort(t).replace(/^of those, /, '')}`);
    else if (t.tier === 'count') rows.push(`max ${countShort(t.short).replace(/^of those, up to /, '')}`);
    else if (t.tier === 'own') rows.push(`max ${t.n}${t.ofThem ? ' of them' : ''}${t.names ? ` ${t.ofThem ? `(${t.names})` : t.names}` : ''}, ${t.whose}`);
    else if (t.tier === 'share') rows.push(`max ${t.n} from streams`);
  }
  return [head, ...rows];
}

/** C: TOP LINE ONLY. The group line alone (its "(of N across …)" inline); the card's own lines never pin. A group whose
 *  path holds a water's own exception says so (the pinned number is not the whole story there). */
export function wordsC(st) {
  const base = st.groupSub ? `${st.groupText} ${st.groupSub}` : st.groupText;
  return [st.tiers.some((t) => t.exc || t.outShare) ? `${base} · exception below` : base];
}

/** D: TODAY'S WORDS on one solid surface (the size header drawn in the bar, not sliding under it), a fade at its edge. */
export function wordsD(st) {
  return [st.groupSub ? `${st.groupText} ${st.groupSub}` : st.groupText,
    ...st.tiers.map((t) => t.tier === 'size' ? t.line : `↳ ${t.short}`)];
}
export const OPTIONS = { A: wordsA, B: wordsB, C: wordsC, D: wordsD };
export const OPTION_NAMES = { now: 'Today', A: 'A one sentence', B: 'B number-led', C: 'C top line only', D: 'D solid + fade' };

// ── words: every option over every stack the census holds ───────────────────────────────────────────────────────────
function kindsOf(row, chain) {
  const tg = [row.pin.sub ? 'group 2-line' : 'group 1-line'];
  for (const t of chain) tg.push(t.tier === 'size' ? 'size header' : t.tier === 'own'
    ? `own${t.exc ? '+exception' : ''}${/hatchery/.test(t.short) ? '+hatchery' : ''}` : t.tier);
  if (row.headExc) tg.push('head exception');
  return tg.join(' + ');
}
function words(census) {
  const { rows } = JSON.parse(fs.readFileSync(census, 'utf8'));
  const combos = new Map();
  for (const row of rows) for (const chain of chainsOf(row)) {
    const k = kindsOf(row, chain), st = stackOf(row, chain);
    const c = combos.get(k) ?? { waters: new Set(), worst: {} }; combos.set(k, c); c.waters.add(row.item);
    for (const [o, f] of Object.entries({ now: wordsD, ...OPTIONS })) {
      const w = o === 'now' ? [row.pin.text, ...(row.pin.sub ? [row.pin.sub] : []), ...chain.map((t) => t.tier === 'size' ? t.line : `↳ ${t.short}`)] : f(st);
      const n = w.join(' ').length;
      if (!c.worst[o] || n > c.worst[o].n) c.worst[o] = { n, lines: w.length, words: w, at: `${row.name} (${row.item} p${row.part} ${row.md})` };
    }
  }
  const out = [...combos].sort((a, b) => b[1].waters.size - a[1].waters.size);
  for (const [k, c] of out) {
    console.log(`\n${String(c.waters.size).padStart(5)} waters · ${k}`);
    for (const [o, w] of Object.entries(c.worst)) console.log(`   ${o.padEnd(4)} ${String(w.n).padStart(3)} ch ${w.lines} ln  ${w.words.join(' ⏎ ')}   [${w.at}]`);
  }
}

// ── the shots ─────────────────────────────────────────────────────────────────────────────────────────────────────
const THEMES = {
  light: { ink: '#15181C', sub: '#555C62', card: '#FFFFFF', line: '#E5E6E1', line2: '#D3D5CF', rule: '#46627F', tint: '#EEEFEB', accent: '#5F26E0' },
  dark: { ink: '#F0F2F0', sub: '#B0B8BF', card: '#15181B', line: '#252A2F', line2: '#343B42', rule: '#93A8BE', tint: '#1E2227', accent: '#A97CFF' },
};
/** the cases: a water, its day, the group to open, what must stand pinned, and the census stack drawn there */
export const CASES = [
  { id: 'vedder-own', item: 'gnis:3062', date: '2026-10-10', md: '7-1', group: 'Trout and char', pinned: ['hatchery rainbow'], chain: ['own'] },
  { id: 'vedder-size', item: 'gnis:3062', date: '2026-10-10', md: '7-1', group: 'Trout and char', pinned: [], size: true, chain: ['size'] },
  { id: 'andrea-longest', item: 'gnis:10036', date: '2026-06-15', md: '6-15', group: 'Trout and char', pinned: ['of them'], chain: ['own'] },
  { id: 'bigbend-deepest', item: 'gnis:12427', date: '2026-07-01', md: '7-1', group: 'Trout and char', pinned: ['of those'], size: true, chain: ['size', 'count'] },
];

const esc = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;');
const heavyNums = (s, all) => { let first = true; return esc(s).replace(/\b\d+\b/g, (n) => (all || first ? (first = false, `<b style="font-family:Archivo_800ExtraBold">${n}</b>`) : n)); };
/** One option's drawing for the bar (HTML), in the theme's colours. */
function drawing(o, w, c) {
  const T = (size, lh, fam = 'Archivo_700Bold') => `font-family:${fam};font-size:${size}px;line-height:${lh}px;color:${c.ink}`;
  // every option ends in the same 16 px fade (card → transparent): a line cut by the bar's edge no longer reads on from it
  const fade = `<div style="position:absolute;left:0;right:0;top:100%;height:16px;background:linear-gradient(${c.card},${c.card}00);pointer-events:none"></div>`;
  const box = (inner) => `<div style="padding:6px 16px 7px">${inner}</div>${fade}`;
  if (o === 'A') return box(`<div style="${T(14, 19, 'Archivo_600SemiBold')}">${heavyNums(w[0], true).replace(/^([^:]+:)/, '<span style="font-family:Archivo_700Bold">$1</span>')}</div>`);
  if (o === 'B') return box(`<div style="${T(15, 19.5)}">${heavyNums(w[0])}</div>` + w.slice(1).map((r) => {
    const m = /^max (\d+) (.*)$/.exec(r) ?? [null, '', r];
    return `<div style="display:flex;gap:8px;margin-top:3px;${T(13, 17, 'Archivo_600SemiBold')}"><span style="flex:0 0 46px;color:${c.sub}">max <b style="font-family:Archivo_800ExtraBold;color:${c.ink};font-size:15px">${m[1]}</b></span><span style="flex:1">${esc(m[2])}</span></div>`;
  }).join(''));
  if (o === 'C') {
    const exc = / · exception below$/.test(w[0]), t = w[0].replace(/ · exception below$/, '');
    return box(`<div style="${T(15, 19.5)}">${heavyNums(t)}${exc ? ` <span style="font-family:Archivo_700Bold;font-size:11px;letter-spacing:1.2px;text-transform:uppercase;color:${c.ink};background:${c.tint};padding:1px 6px;white-space:nowrap">Exception below ↓</span>` : ''}</div>`);
  }
  // D: today's words, one surface, a fade under it
  return `<div style="padding:3px 0">` + w.map((l, i) => {
    const size = !l.startsWith('↳') && i > 0;
    const style = i === 0 ? `padding:3px 16px;${T(15, 19.5)}` : size
      ? `margin:3px 16px 3px 13px;padding-left:13px;border-left:3px solid ${c.rule};${T(12.48, 16.22)};color:${c.rule}` : `padding:3px 16px 3px 26px;${T(12.48, 16.22)}`;
    return `<div style="${style}">${heavyNums(l)}</div>`;
  }).join('') + `</div>${fade}`;
}

const PAGE = {
  scroller: `(() => { const k = document.querySelector('[data-testid=keep-tab]'); let s = k; while (s && !(s.scrollHeight > s.clientHeight + 4 && getComputedStyle(s).overflowY !== 'visible')) s = s.parentElement; window.__sc = s; return !!s; })()`,
  state: `(() => { const bar = document.querySelector('[data-testid=pin-bar]'); const pins = bar ? [...bar.querySelectorAll('[data-testid^=pin-]')].map((e) => e.innerText) : [];
    const bb = bar ? bar.getBoundingClientRect().bottom : window.__sc.getBoundingClientRect().top;
    const stuck = [...document.querySelectorAll('[data-testid=size-box] [data-testid=khead]')].some((h) => { const r = h.getBoundingClientRect(), b = h.closest('[data-testid=size-box]').getBoundingClientRect(); return Math.abs(r.top - bb) < 3 && r.top - b.top > 4; });
    return { pins, stuck, top: window.__sc.scrollTop, max: window.__sc.scrollHeight - window.__sc.clientHeight }; })()`,
};

async function tapText(b, find) {
  const at = await b.evaluate(`(() => { const e = ${find}; if (!e) return null; e.scrollIntoView({ block: 'center' }); const r = e.getBoundingClientRect(); return [r.x + r.width / 2, r.y + r.height / 2]; })()`);
  if (!at) throw new Error(`nothing to press: ${find}`);
  for (const type of ['mousePressed', 'mouseReleased']) await b.send('Input.dispatchMouseEvent', { type, x: at[0], y: at[1], button: 'left', clickCount: 1 });
}

async function shoot(outDir, census) {
  fs.mkdirSync(outDir, { recursive: true });
  const { rows } = JSON.parse(fs.readFileSync(census, 'utf8'));
  for (const cs of CASES.filter((c) => !process.env.ONLY || process.env.ONLY.split(',').includes(c.id))) {
    const row = rows.find((r) => r.item === cs.item && r.part === 0 && r.md === cs.md && r.group === cs.group);
    const chain = row && chainsOf(row).find((ch) => ch.map((t) => t.tier).join() === cs.chain.join());
    if (!chain) { console.log('✗ no census stack for', cs.id); continue; }
    const st = stackOf(row, chain);
    for (const width of [390, 360]) for (const theme of ['light', 'dark']) {
      const b = await open(`${APP}/?regs=${cs.item}&date=${cs.date}`, { width, height: 844, dpr: 2, mobile: true, init: INIT });
      try {
        await b.send('Emulation.setEmulatedMedia', { features: [{ name: 'prefers-color-scheme', value: theme }, { name: 'prefers-reduced-motion', value: 'reduce' }] });
        // the app reads the scheme when it starts: load again under the emulated one
        await b.send('Page.reload', {}); await sleep(1500);
        for (let t = 0; t < 400 && !(await b.evaluate(`!!document.querySelector('[data-testid=keep-tab]')`).catch(() => false)); t++) await sleep(500);
        await b.evaluate('document.fonts.ready.then(() => 1)'); await sleep(2500);
        await tapText(b, `[...document.querySelectorAll('[role=button]')].find((x) => x.innerText.trim().startsWith(${JSON.stringify(cs.group)}) && /Details/.test(x.innerText))`);
        await sleep(1500);
        await b.evaluate(PAGE.scroller);
        // the first offset where the case's stack stands pinned (the pin store recomputes on each scroll event)
        let found = null;
        for (let y = 0; ; y += 10) {
          await b.evaluate(`window.__sc.scrollTop = ${y}`); await sleep(60);
          const s = await b.evaluate(PAGE.state);
          const ok = cs.pinned.every((p) => s.pins.some((x) => x.includes(p))) && (!!cs.size === s.stuck) && s.pins.length > 0
            && (cs.chain.includes('own') || !s.pins.some((x) => /hatchery rainbow/.test(x)));
          if (ok) { found = y; break; }
          if (y > s.max) break;
        }
        if (found == null) { console.log('✗ never pinned', cs.id, width, theme); continue; }
        await b.evaluate(`window.__sc.scrollTop = ${found + 36}`); await sleep(900);
        const c = THEMES[theme];
        for (const o of ['now', 'A', 'B', 'C', 'D']) {
          if (o !== 'now') {
            await b.evaluate(`(() => { const bar = document.querySelector('[data-testid=pin-bar]');
              for (const ch of bar.children) ch.style.display = 'none';
              const d = document.createElement('div'); d.id = 'pin-study'; d.innerHTML = ${JSON.stringify(drawing(o, OPTIONS[o](st), c))}; bar.appendChild(d);
              bar.style.overflow = 'visible';
              const css = document.createElement('style'); css.id = 'pin-study-css';
              css.textContent = '[data-testid=size-box] [data-testid=khead]{position:static !important}'; document.head.appendChild(css); })()`);
          }
          await sleep(700);
          const { data } = await b.send('Page.captureScreenshot', { format: 'png' });
          const f = path.join(outDir, `${cs.id}-${width}-${theme}-${o}.png`);
          fs.writeFileSync(f, Buffer.from(data, 'base64')); console.log('✓', f);
          await b.evaluate(`(() => { document.getElementById('pin-study')?.remove(); document.getElementById('pin-study-css')?.remove();
            const bar = document.querySelector('[data-testid=pin-bar]'); if (bar) for (const ch of bar.children) ch.style.display = ''; })()`);
        }
      } finally { await b.close(); }
    }
  }
}

/** One contact sheet per width: the options across, the cases (light, then dark) down — a Chrome canvas, no dependencies. */
async function sheet(outDir) {
  const b = await open('about:blank', { width: 400, height: 400 });
  try {
    for (const width of [390, 360]) {
      const cells = [];
      for (const cs of CASES) for (const theme of ['light', 'dark']) cells.push(['now', 'A', 'B', 'C', 'D'].map((o) => path.join(outDir, `${cs.id}-${width}-${theme}-${o}.png`)));
      const u = (p) => fs.existsSync(p) ? 'data:image/png;base64,' + fs.readFileSync(p).toString('base64') : null;
      const png = await b.evaluate(`(async () => {
        const grid = ${JSON.stringify(cells.map((r) => r.map(u)))}, heads = ${JSON.stringify(['now', 'A', 'B', 'C', 'D'].map((o) => OPTION_NAMES[o]))};
        const rows = ${JSON.stringify(CASES.flatMap((c) => ['light', 'dark'].map((t) => `${c.id} · ${t}`)))};
        const load = (s) => new Promise((res) => { if (!s) return res(null); const i = new Image(); i.onload = () => res(i); i.src = s; });
        const imgs = await Promise.all(grid.flat().map(load));
        const S = 0.5, cw = ${width} * 2 * S, ch = 844 * 2 * S * 0.62, pad = 16, hh = 40, lw = 0;
        const c = document.createElement('canvas'); c.width = pad + heads.length * (cw + pad); c.height = hh + rows.length * (ch + pad + 22) + pad;
        const k = c.getContext('2d'); k.fillStyle = '#9a9a96'; k.fillRect(0, 0, c.width, c.height);
        k.fillStyle = '#000'; k.font = 'bold 22px sans-serif'; heads.forEach((h, i) => k.fillText(h, pad + i * (cw + pad), 28));
        rows.forEach((r, j) => { const y = hh + j * (ch + pad + 22); k.font = '16px sans-serif'; k.fillStyle = '#000'; k.fillText(r + ' (${width} px)', pad, y + 16);
          heads.forEach((_, i) => { const im = imgs[j * heads.length + i]; if (!im) return;
            // the top 62 % of the phone: the header, the pin area and what scrolls under it
            k.drawImage(im, 0, 0, im.width, im.height * 0.62, pad + i * (cw + pad), y + 22, cw, ch); }); });
        return c.toDataURL('image/png').slice(22); })()`);
      const f = path.join(outDir, `contact-sheet-${width}.png`);
      fs.writeFileSync(f, Buffer.from(png, 'base64')); console.log('✓', f);
    }
  } finally { await b.close(); }
}

const [cmd, ...a] = process.argv.slice(2);
if (cmd === 'words') words(a[0]);
else if (cmd === 'shoot') await shoot(a[0], a[1]);
else if (cmd === 'sheet') await sheet(a[0]);
else if (cmd) console.log('usage: words <census.json> | shoot <outDir> <census.json> | sheet <outDir>');
