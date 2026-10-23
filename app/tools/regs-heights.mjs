// T41 R1 — THE SECTION HEIGHTS: Gear / Methods / Licence measured in the prototype (staged at the port target's ref) and
// in the app on Expo web, 390 px wide, the same state and day. Prints one row per state × section: prototype px, app px,
// app − prototype, and the app's own pieces that are not the section's words (the report line at the page's foot).
// A measurement, never a gate: the walk's checks (regs-walk.mjs) are the gates.
//
// T82 O5 — `--sheets`: every golden sheet of each state, opened in both at 390 × 844 — the prototype's `#sheet.on` and the
// app's `sheet-panel` — one row per sheet: the sheet's height on screen, its chrome (handle + title row + sub line; the
// app's foot padding), its body's content height, and whether the body hit its cap (scrolls). Then the summary
// `sheetSummary` (regs-web/sheets.mjs) prints.
//
//   node app/tools/regs-heights.mjs [--url http://localhost:8081] [--states vedder_1010,klwa_1010,bc1_501] [--json out.json]
//   node app/tools/regs-heights.mjs --sheets [--url …] [--states …] [--json out.json]
import fs from 'node:fs';
import { openApp, sleep, until, tid } from './regs-web/drive.mjs';
import { APP_URL, PROTO_TODAY, REF, appKeyOf, stateOf, urlsOf } from './regs-web/states.mjs';
import { sheetSummary } from './regs-web/sheets.mjs';
import { openProto, stageProto } from './regs-web/proto.mjs';

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; };
const url = opt('--url', APP_URL), jsonOut = opt('--json', null);
const STATES = opt('--states', 'vedder_1010,klwa_1010,bc1_501').split(',');
const SECTIONS = ['gear', 'methods', 'licence'];

const appHeights = `(() => { const o = {}; for (const k of ${JSON.stringify(SECTIONS)}) { const e = document.querySelector('[data-testid="regs-' + k + '"]'); o[k] = e ? e.getBoundingClientRect().height : null; }
  const r = document.querySelector('[data-testid^="report-link"]'); o.report = r ? r.getBoundingClientRect().height + parseFloat(getComputedStyle(r).marginTop) + parseFloat(getComputedStyle(r).marginBottom) : 0;
  o.reports = document.querySelectorAll('[data-testid^="report-link"]').length; return o; })()`;
const protoHeights = `(() => { const o = {}; for (const k of ${JSON.stringify(SECTIONS)}) { const e = document.getElementById(k); o[k] = e ? e.getBoundingClientRect().height : null; } return o; })()`;

const rows = [];
const s = stageProto(REF);
if (args.includes('--sheets')) {
  // the prototype: the sheet panel (.sheet.on, max-height 88 % of the .app box), its head, its body's content height
  const protoSheet = `(() => { const sh = document.querySelector('#sheet.on'); if (!sh) return null; const hd = sh.querySelector('.shead'), bd = sh.querySelector('.sbody');
    const R = (e) => e.getBoundingClientRect().height; return { sheet: R(sh), chrome: R(sh) - R(bd), body: bd.scrollHeight, scrolls: bd.scrollHeight > bd.clientHeight + 1, app: R(document.querySelector('.app')) }; })()`;
  // the app: the sheet panel (its body capped at 0.68 of the window), the handle block, the scroll's content height
  const appSheet = `(() => { const p = ${tid('sheet-panel')}, sc = ${tid('sheet-scroll')}; if (!p || !sc) return null; const R = (e) => e.getBoundingClientRect().height;
    return { sheet: R(p), chrome: R(p) - R(sc), body: sc.scrollHeight, scrolls: sc.scrollHeight > sc.clientHeight + 1, app: innerHeight }; })()`;
  try {
    for (const id of STATES) {
      const a = await openApp(urlsOf(id, { app: url }).app, { day: PROTO_TODAY });
      try {
        for (const k of stateOf(id).sheets) {
          const p = await openProto(s, urlsOf(id, { sheet: k, app: url }).proto);
          let ph; try { ph = await p.evaluate(protoSheet); } finally { await p.close(); }
          const ok = await a.evaluate(`globalThis.__regsDebug?.openSheet(${JSON.stringify(appKeyOf(k, id))}) ?? false`);
          let ah = null;
          if (ok) { await until(a, `!!${tid('sheet-panel')}`, 5000, k); await sleep(700); ah = await a.evaluate(appSheet);
            await a.evaluate(`[...document.querySelectorAll('[role=button]')].find((e) => e.getAttribute('aria-label') === 'Done')?.click()`); await sleep(700); }
          const row = { id, sheet: k, proto: ph, app: ah };
          rows.push(row);
          const f = (v) => (v == null ? '   -  ' : v.toFixed(0).padStart(6));
          console.log(`${id.padEnd(11)} ${k.slice(0, 44).padEnd(44)} proto ${f(ph?.sheet)} (chrome ${f(ph?.chrome)}, body ${f(ph?.body)}${ph?.scrolls ? ' ⇕' : '  '})  app ${f(ah?.sheet)} (chrome ${f(ah?.chrome)}, body ${f(ah?.body)}${ah?.scrolls ? ' ⇕' : '  '})`);
        }
      } finally { await a.close(); }
    }
  } finally { s.cleanup(); }
  console.log(JSON.stringify(sheetSummary(rows), null, 1));
  if (jsonOut) fs.writeFileSync(jsonOut, JSON.stringify(rows, null, 1));
  process.exit(0);
}
try {
  for (const id of STATES) {
    const u = urlsOf(id, { app: url });
    const p = await openProto(s, u.proto);
    let ph; try { ph = await p.evaluate(protoHeights); } finally { await p.close(); }
    const a = await openApp(u.app, { day: PROTO_TODAY });
    let ah; try { ah = await a.evaluate(appHeights); } finally { await a.close(); }
    for (const k of SECTIONS) {
      const row = { id, section: k, proto: ph[k], app: ah[k], diff: ah[k] != null && ph[k] != null ? ah[k] - ph[k] : null,
        pct: ah[k] != null && ph[k] ? (100 * (ah[k] - ph[k])) / ph[k] : null };
      rows.push(row);
      console.log(`${id.padEnd(12)} ${k.padEnd(8)} proto ${ph[k]?.toFixed(1).padStart(7)}  app ${ah[k]?.toFixed(1).padStart(7)}  Δ ${row.diff?.toFixed(1).padStart(7)} (${row.pct?.toFixed(1)} %)`);
    }
    console.log(`${id.padEnd(12)} report links ${ah.reports} (${ah.report.toFixed(1)} px each with margins)`);
  }
} finally { s.cleanup(); }
if (jsonOut) fs.writeFileSync(jsonOut, JSON.stringify(rows, null, 1));
