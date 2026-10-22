// T41 R1 — THE SECTION HEIGHTS: Gear / Methods / Licence measured in the prototype (staged at the port target's ref) and
// in the app on Expo web, 390 px wide, the same state and day. Prints one row per state × section: prototype px, app px,
// app − prototype, and the app's own pieces that are not the section's words (the report line at the page's foot).
// A measurement, never a gate: the walk's checks (regs-walk.mjs) are the gates.
//
//   node app/tools/regs-heights.mjs [--url http://localhost:8081] [--states vedder_1010,klwa_1010,bc1_501] [--json out.json]
import fs from 'node:fs';
import { openApp } from './regs-web/drive.mjs';
import { APP_URL, PROTO_TODAY, REF, urlsOf } from './regs-web/states.mjs';
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
