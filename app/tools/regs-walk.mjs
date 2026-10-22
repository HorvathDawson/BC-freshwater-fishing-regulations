// T41 (layer D, release gate) — THE WALK: every state of WALK_STATES on Expo web at 390 × 844, light and dark: the px
// checks tests.md §2 routes to the browser (the check.mjs names, so a reader can match them), every tab pressed, every
// golden sheet of the state opened and closed, and no console error anywhere. Prints `PASS|FAIL <name> <state> — why`.
//
//   Expo web (`pnpm -C app/apps/mobile web`, or `EXPO_PUBLIC_DATA_BASE=http://localhost:<tiles> npx expo start --web --port <yours>`)
//   node app/tools/regs-walk.mjs [--url http://localhost:8081] [--states vedder_1010,…] [--theme light|dark] [--clock proto|real]
//                                [--no-sheets] [--json out.json]
//   → exit 1 on any FAIL line
//
// The app is put on each state's day by the start link (`?regs=…&date=…`) and, with `--clock proto` (the default), its
// clock on the prototype's today (PROTO_TODAY, its snapshot day) — so the header words match (`Today` vs a weekday).
import fs from 'node:fs';
import { RECT, SCROLL, openApp, scrollTo, sleep, tap, tid, until } from './regs-web/drive.mjs';
import { APP_URL, METHODS_COMPACT, PHONE, PROTO_TODAY, WALK_STATES, appKeyOf, urlsOf } from './regs-web/states.mjs';
import golden from '../design/regs-v11/port/golden/golden.json' with { type: 'json' };
import manifest from '../apps/mobile/assets/fish/manifest.json' with { type: 'json' };

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; };
const url = opt('--url', APP_URL), clock = opt('--clock', 'proto'), jsonOut = opt('--json', null);
const DAY = clock === 'real' ? null : PROTO_TODAY;
const STATES = opt('--states', null)?.split(',') ?? WALK_STATES;
const THEMES = opt('--theme', null) ? [opt('--theme')] : ['light', 'dark'];
const SHEETS = !args.includes('--no-sheets');

const results = [];
const say = (ok, name, state, why) => { results.push({ ok, name, state, why }); console.log(`${ok ? 'PASS' : 'FAIL'} ${name} ${state} — ${why}`); };
const TAB_NAMES = { keep: 'What you can keep', gear: 'Gear', methods: 'Methods', licence: 'Licence' };
const ASPECT = Object.fromEntries(manifest.images.map((m) => [m.file, m.width / m.height]));
/** a golden sheet's title ("@": the shared one in golden.sheets) */
const titleOf = (id, k) => { const s = golden.states[id].sheets[k]; return (s === '@' ? golden.sheets[k] : s)?.title ?? null; };

// ── the page's geometry, read in the page ────────────────────────────────────────────────────────────────────────
const HEADER = `(() => { ${RECT} const h = ${tid('regs-header')}, n = ${tid('water-title')}, d = ${tid('regs-date')}, a = ${tid('regs-action')}, p = ${tid('regs-place')}, s = ${SCROLL};
  return { h: R(h), name: n && R(n), date: d && R(d), action: a && R(a), label: p && R(p), lh: p ? parseFloat(getComputedStyle(p).lineHeight) || 0 : 0, text: h.innerText, scroll: R(s) }; })()`;

const PIN = `(() => { ${RECT} const bar = ${tid('pin-bar')}, s = ${SCROLL};
  if (!bar || bar.getBoundingClientRect().height < 1) return { bar: null, scroll: R(s) };
  const lines = [...bar.querySelectorAll('*')].filter((e) => [...e.childNodes].some((c) => c.nodeType === 3 && c.textContent.trim()) && !e.parentElement.closest('[data-pinline]'))
    .map((e) => ({ r: R(e), lh: parseFloat(getComputedStyle(e).lineHeight) || 20, text: e.innerText.replace(/\\s+/g, ' ').trim() }));
  return { bar: R(bar), lines, text: bar.innerText.replace(/\\s+/g, ' ').trim(), scroll: R(s), store: globalThis.__regsDebug?.pinLines() ?? null }; })()`;

/** each keep group: its top/bottom in CONTENT px, its title, its head block's height, and its first kind row's content y */
const GROUPS = `(() => { ${RECT} const s = ${SCROLL}, top0 = s.getBoundingClientRect().top - s.scrollTop;
  return [...document.querySelectorAll('[data-testid=keep-group]')].map((g) => { const head = g.firstElementChild, t = g.querySelector('[role=heading], h1, h2, h3');
    const r = R(g), seen = new Set(), rows = [];
    /* the kind rows: each picture strip's first picture (one per strip), as the prototype's .krow.kt */
    for (const p of g.querySelectorAll('[data-testid^="pic:"]')) { if (seen.has(p.parentElement)) continue; seen.add(p.parentElement); rows.push({ code: p.dataset.testid.slice(4), y: R(p).top - top0 }); }
    return { top: r.top - top0, bottom: r.bottom - top0, title: t ? ([...t.childNodes].find((c) => c.textContent.trim())?.textContent ?? '').trim() : '', head: head ? R(head).h : 0, rows: rows.slice(0, 4) }; }); })()`;

/** pictures: overlaps > 1 px, and each one's natural aspect vs the manifest's (± 3 %), drawn whole (contain) */
const PICTURES = `(() => { ${RECT} const ASPECT = ${JSON.stringify(ASPECT)}, bad = [];
  const imgs = [...document.querySelectorAll('[data-testid^="pic:"] img')].filter((i) => i.getBoundingClientRect().width > 0);
  const rs = imgs.map(R);
  for (let i = 0; i < rs.length; i++) for (let j = i + 1; j < rs.length; j++) { const a = rs[i], b = rs[j];
    const ox = Math.min(a.right, b.right) - Math.max(a.left, b.left), oy = Math.min(a.bottom, b.bottom) - Math.max(a.top, b.top);
    if (ox > 1 && oy > 1) bad.push('overlap ' + imgs[i].closest('[data-testid^="pic:"]').dataset.testid + ' × ' + imgs[j].closest('[data-testid^="pic:"]').dataset.testid); }
  for (const i of imgs) { const f = decodeURIComponent(i.currentSrc || i.src).split('/').pop().split('?')[0].replace(/\\.[0-9a-f]{8,}(?=\\.)/, ''), want = ASPECT[f];
    const got = i.naturalWidth / i.naturalHeight, fit = getComputedStyle(i.parentElement.firstElementChild || i).backgroundSize;
    if (want && Math.abs(got / want - 1) > 0.03) bad.push(f + ' aspect ' + got.toFixed(3) + ' ≠ ' + want.toFixed(3));
    if (fit && fit !== 'contain' && fit !== 'auto') bad.push(f + ' drawn ' + fit); }
  return { n: imgs.length, bad }; })()`;

/** r20f-headers: inside every keep group, each block's first text starts at the head's text edge (± 1 px) */
const EDGE = `(() => { const bad = [];
  const firstLeft = (el) => { const w = document.createTreeWalker(el, NodeFilter.SHOW_TEXT); let n, best = Infinity;
    while ((n = w.nextNode())) { if (!n.textContent.trim()) continue; const p = n.parentElement; if (p.closest('[data-testid^="pic:"], [data-testid^="chip:"], svg')) continue;
      const rg = document.createRange(); rg.selectNodeContents(n); const r = rg.getClientRects()[0]; if (r && r.width > 0) best = Math.min(best, r.left); }
    return best; };
  document.querySelectorAll('[data-testid=keep-group]').forEach((g) => { const head = g.firstElementChild; if (!head) return; const edge = firstLeft(head);
    for (const blk of [...g.children].slice(1)) { const l = firstLeft(blk); if (l !== Infinity && Math.abs(l - edge) > 1) bad.push((g.querySelector('h1,[role=heading]')?.innerText || 'group') + ': a block at ' + l.toFixed(1) + ' vs ' + edge.toFixed(1) + ' “' + blk.innerText.slice(0, 30).replace(/\\s+/g, ' ') + '”'); } });
  return bad; })()`;

const SHEET = `(() => { ${RECT} const d = ${tid('sheet-dialog')} || ${tid('sheet-body')}?.closest('[role=dialog]') || document.querySelector('[role=dialog]');
  if (!d) return null; const hs = [...d.querySelectorAll('[role=heading], h1, h2')].map((e) => e.innerText.trim()); const body = ${tid('sheet-body')};
  return { title: hs[0] ?? null, body: body ? R(body) : null }; })()`;

// ── the walk ─────────────────────────────────────────────────────────────────────────────────────────────────────
async function walkState(id, theme) {
  const tag = `${id}/${theme}`;
  const b = await openApp(urlsOf(id, { theme, app: url }).app, { day: DAY });
  try {
    // 1 header-compact · 3 header-label-two-lines
    const h0 = await b.evaluate(HEADER);
    const rowOk = h0.date && h0.action ? Math.abs(h0.date.top - h0.name.top) <= 4 && Math.abs(h0.action.top - h0.name.top) <= 4 : !!h0.date;
    say(h0.h.h <= 120 && rowOk, 'header-compact', tag, `header ${h0.h.h.toFixed(1)} px (≤ 120); name/date/action tops ${[h0.name, h0.date, h0.action].map((r) => r ? r.top.toFixed(1) : '–').join(' / ')}`);
    const lab = h0.label ? h0.label.h <= 2 * h0.lh + 2 : true, act = h0.action ? h0.action.w >= 44 && h0.action.h >= 44 : true;
    say(lab && act, 'header-label-two-lines', tag, `label ${h0.label ? h0.label.h.toFixed(1) : '–'} px (≤ 2 × ${h0.lh} + 2); action ${h0.action ? `${h0.action.w}×${h0.action.h}` : 'none'} (≥ 44×44)`);
    // 2 header-one-shape
    await scrollTo(b, 900);
    const h9 = await b.evaluate(HEADER);
    say(h9.h.h === h0.h.h && h9.text === h0.text, 'header-one-shape', tag, `height ${h0.h.h} → ${h9.h.h} at 900; text ${h9.text === h0.text ? 'same' : 'CHANGED'}`);
    await scrollTo(b, 0);

    // group-head-compact (Vedder's first block ≤ 150 px, reported for every state)
    const groups = await b.evaluate(GROUPS);
    if (groups.length) say(groups[0].head <= 150, 'group-head-compact', tag, `first group head ${groups[0].head.toFixed(1)} px (≤ 150)`);
    // r20f-headers / r20d-flat: one text edge
    const edge = await b.evaluate(EDGE);
    say(edge.length === 0, 'r20f-headers', tag, edge.length ? edge.slice(0, 3).join(' · ') : `${groups.length} group(s): every block on the head's text edge`);

    // 4 r20d-pinned-stack: each group's first kind row at 60 % of the viewport, and just past the group's head
    for (const g of groups) {
      const view = h0.scroll.h;
      const samples = [['head passed', g.top + g.head + 4], ...g.rows.map((r) => [`row ${r.code}@60%`, r.y - 0.6 * view, r.y])];
      for (const [why, y, rowY] of samples) {
        if (y <= 0) continue;
        const at = await scrollTo(b, Math.round(y));
        const p = await b.evaluate(PIN);
        const passed = g.top + g.head <= at;   // the group's head has gone under the header: its line must be pinned
        if (!p.bar) { say(!passed, 'r20d-pinned-stack', `${tag} ${g.title} ${why}`, passed ? 'the head is gone and no pin bar' : 'head still in view: no bar'); continue; }
        const under = rowY !== undefined && rowY - at + p.scroll.top < p.bar.bottom - 1;
        const lines = p.lines.every((l) => l.r.h <= 2.6 * l.lh + 0.5);
        const top = Math.abs(p.bar.top - p.scroll.top) <= 1.5, tall = p.bar.h <= 90;
        const words = p.store === null || p.store.join(' ').replace(/\s+/g, ' ').trim() === p.text;
        const own = !passed || p.text.startsWith(g.title);
        say(lines && top && tall && words && own && !under, 'r20d-pinned-stack', `${tag} ${g.title} ${why}`,
          `bar top ${p.bar.top.toFixed(1)} vs scroll top ${p.scroll.top.toFixed(1)}; ${p.bar.h.toFixed(1)} px (≤ 90); lines ${lines ? '≤ 2.6 LH' : 'TOO TALL'};` +
          ` “${p.text.slice(0, 80)}”${words ? '' : ` ≠ store “${(p.store || []).join(' ')}”`}${own ? '' : ` — not this group's (${g.title})`}${under ? ' — the row is under the bar' : ''}`);
      }
    }
    await scrollTo(b, 0);

    // 5 pictures-whole
    const pics = await b.evaluate(PICTURES);
    say(pics.bad.length === 0, 'pictures-whole', tag, pics.bad.length ? pics.bad.slice(0, 4).join(' · ') : `${pics.n} pictures, none overlapping, aspects = manifest`);

    // 6 bait-ways-compact
    if (METHODS_COMPACT.includes(id)) {
      // the whole Methods slot (regs-methods): since T41 Q3 the report line sits once at the page's foot, not under Methods
      const m = await b.evaluate(`(() => { const s = ${tid('regs-methods')}; return s ? s.getBoundingClientRect().height : null; })()`);
      const room = PHONE.height - h0.h.bottom;
      say(m !== null && m <= room, 'bait-ways-compact', tag, `Methods ${m === null ? 'absent' : `${m.toFixed(1)} px`} (≤ ${room.toFixed(1)}: the viewport under the header)`);
    }

    // 9 tabs: a press lands its section head ≤ 3 px under the header, unless the page ends first
    for (const t of ['gear', 'methods', 'licence', 'keep']) {
      const has = await b.evaluate(`!!${tid(t === 'keep' ? 'keep-tab' : `regs-${t}`)}`);
      if (!has) { say(true, 'tabs', `${tag} ${t}`, 'no such section on this page (its tab still listed)'); continue; }
      await tap(b, `[...document.querySelectorAll('[role=tab]')].find((e) => e.innerText.trim() === ${JSON.stringify(TAB_NAMES[t])} && e.closest('[data-testid=regs-header]'))`, `tab ${t}`);
      await sleep(1100);
      const r = await b.evaluate(`(() => { const s = ${SCROLL}, sec = ${tid(t === 'keep' ? 'keep-tab' : `regs-${t}`)}, tab = [...document.querySelectorAll('[role=tab]')].find((e) => e.innerText.trim() === ${JSON.stringify(TAB_NAMES[t])} && e.closest('[data-testid=regs-header]'));
        return { gap: sec.getBoundingClientRect().top - s.getBoundingClientRect().top, end: s.scrollTop >= s.scrollHeight - s.clientHeight - 1, sel: tab.getAttribute('aria-selected') }; })()`);
      // at the page's end the section cannot reach the header: the tab in view is then the scroll's (tabInView), as the prototype's
      say(r.end ? true : Math.abs(r.gap) <= 3 && r.sel === 'true', 'tabs', `${tag} ${t}`, `section head ${r.gap.toFixed(1)} px from the header${r.end ? ' (page end)' : ''}; selected ${r.sel}`);
    }
    await scrollTo(b, 0);

    // 8 sheets open: every golden sheet of the state, by its app key, through the dev hook (no reload); Done closes it
    if (SHEETS) {
      await until(b, '!!globalThis.__regsDebug', 5000, 'the dev walk hook');
      const keys = Object.keys(golden.states[id].sheets || {});
      const bad = [];
      for (const k of keys) {
        const ak = appKeyOf(k, id), want = titleOf(id, k);
        const ok = await b.evaluate(`globalThis.__regsDebug.openSheet(${JSON.stringify(ak)})`);
        if (!ok) { bad.push(`${ak}: not a key`); continue; }
        try { await until(b, `!!(${SHEET})`, 5000, `sheet ${ak}`); } catch { bad.push(`${ak}: did not open`); continue; }
        const sh = await b.evaluate(SHEET);
        if (want !== null && sh.title !== want) bad.push(`${ak}: “${sh.title}” ≠ “${want}”`);
        await tap(b, `[...document.querySelectorAll('[role=button]')].find((e) => /^Done$/.test(e.innerText.trim()) || e.getAttribute('aria-label') === 'Done')`, 'Done');
        try { await until(b, `!(${SHEET})`, 4000, 'the sheet to close'); } catch { bad.push(`${ak}: Done did not close it`); await b.evaluate(`globalThis.__regsDebug && 0`); }
      }
      say(bad.length === 0, 'sheets open', tag, bad.length ? `${bad.length}/${keys.length}: ${bad.slice(0, 4).join(' · ')}` : `${keys.length} sheets: heading = golden title, Done closes`);
    }
    // 10 no console errors
    say(b.errors.length === 0, 'no console errors', tag, b.errors.length ? b.errors.slice(0, 3).join(' | ').slice(0, 300) : 'none');
  } finally { await b.close(); }
}

/** 7 r20g-possession-fold: the pos:pos sheet opened by the START LINK (the link's sheet path), its body < 300 px */
async function possession(theme) {
  const id = 'vedder_1010';
  const b = await openApp(urlsOf(id, { theme, sheet: `pos:${id}|pos`, app: url }).app, { day: DAY });
  try {
    await until(b, `!!(${SHEET})`, 8000, 'the possession sheet');
    const sh = await b.evaluate(SHEET);
    say(sh.body && sh.body.h < 300 && sh.title === titleOf(id, `pos:${id}|pos`), 'r20g-possession-fold', `${id}/${theme}`,
      `“${sh.title}” body ${sh.body ? sh.body.h.toFixed(1) : '–'} px (< 300), opened by the start link`);
    say(b.errors.length === 0, 'no console errors', `${id}/${theme} start-link sheet`, b.errors.join(' | ').slice(0, 300) || 'none');
  } finally { await b.close(); }
}

for (const theme of THEMES) {
  await possession(theme).catch((e) => say(false, 'r20g-possession-fold', `vedder_1010/${theme}`, e.message));
  for (const id of STATES) await walkState(id, theme).catch((e) => say(false, 'walk', `${id}/${theme}`, e.message));
}
const fails = results.filter((r) => !r.ok);
console.log(`\n${results.length - fails.length} PASS · ${fails.length} FAIL (${STATES.length} states × ${THEMES.join('+')})`);
if (jsonOut) fs.writeFileSync(jsonOut, JSON.stringify({ url, clock, states: STATES, themes: THEMES, results }, null, 1));
process.exit(fails.length ? 1 : 0);
