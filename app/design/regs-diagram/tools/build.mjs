#!/usr/bin/env node
// regs-diagram: pull the waters the diagram page draws out of the page-v36 snapshot (regs-v11/v36snap.js, itself
// script-made from pipeline/deliver/answers/reference/page_v36.html) into regs-diagram/data.js. Nothing here is
// hand-written: every number and word the page draws is copied from the snapshot's rows/group fields, the card's own
// HTML (row titles, pills, summaries), the display strip legend and the rule info (book pages, titles).
//   node app/design/regs-diagram/tools/build.mjs
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const SNAP_PATH = path.resolve(here, '../../regs-v11/v36snap.js');
const OUT = path.resolve(here, '../data.js');

const src = fs.readFileSync(SNAP_PATH, 'utf8');
const SNAP = JSON.parse(src.slice(src.indexOf('window.SNAP =') + 13).replace(/;\s*$/, ''));

// the waters and days the owner asked to see (state ids are the snapshot's own)
const PICK = [
  { w: 'vedder', label: 'Vedder River', days: [{ md: 1010, id: 'vedder_1010' }] },
  { w: 'chilliwack2', label: 'Chilliwack River · part 2', days: [{ md: 1010, id: 'chw2_1010' }] },
  { w: 'bellacoola', label: 'Bella Coola River · mainstem', days: [{ md: 501, id: 'bc0_501' }] },
  { w: 'bennett', label: 'Bennett Lake', days: [{ md: 1010, id: 'x35p_1010' }] },
  { w: 'klwa', label: 'Kootenay Lake — Lower West Arm', days: [{ md: 1010, id: 'klwa_1010' }, { md: 1012, id: 'klwa_1012' }] },
];

const strip = s => String(s || '').replace(/<[^>]+>/g, ' ').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, '’').replace(/\s+/g, ' ').trim();

// the card's own words per row: the title, its scope tag, the status pill and the one-line summary
function cardWords(html) {
  const out = {};
  for (const art of html.split(/(?=<article class="row)/)) {
    const m = art.match(/data-row="(r\d+)"/); if (!m) continue;
    const head = (art.match(/<div class="rhead">([\s\S]*?)<\/div>\s*<div class="rsum">/) || [])[1] || '';
    const title = (head.match(/<span class="rtitle">([\s\S]*?)(?=<span class="scope|<\/span>)/) || [])[1] || '';
    const scope = (head.match(/<span class="scope[^"]*">([\s\S]*?)<\/span>/) || [])[1] || '';
    const pill = (head.match(/<span class="pill[^"]*">([\s\S]*?)<\/span>\s*(?:<button|<\/div>)/) || [])[1] || '';
    const num = (head.match(/<div class="num[^"]*">([\s\S]*?)<\/div>/) || [])[1] || '';
    const sum = (art.match(/<p class="sum[^"]*">([\s\S]*?)<\/p>/) || [])[1] || '';
    out[m[1]] = { title: strip(title), scope: strip(scope), pill: strip(pill).replace(/^[↩✕✓→⊘]\s*/, ''), num: strip(num), sum: strip(sum) };
  }
  return out;
}

// every rule id a row's group names, so the page can say "Rule book p.N" and the rule's title beside each number
function ruleIds(row) {
  const ids = new Set();
  const walk = v => { if (Array.isArray(v)) v.forEach(walk); else if (v && typeof v === 'object') { for (const [k, x] of Object.entries(v)) { if ((k === 'by' || k === 'r' || k === 'of') && typeof x === 'number') ids.add(x); else if (k === 'rules' && Array.isArray(x)) x.forEach(n => typeof n === 'number' && ids.add(n)); else walk(x); } } };
  walk(row);
  return [...ids];
}

function trimRow(r, words) {
  const g = r.group || null;
  const group = g && g.tree ? {
    header: g.header, tree: g.tree, table: g.table, not_kept: g.not_kept || [], origin: g.origin || null, total_line: g.total_line || null,
    headline: g.headline || null, lifts: (g.lifts || []).map(l => ({ says: l.says, by: l.by, kinds: l.kinds, n: l.n })),
    combined_words: g.combined_words || null,
    caps: (g.caps || []).map(c => ({ take: c.take, scope: c.scope, by: c.by, says: c.says, if_kept_here: c.if_kept_here })),
    worked: (g.worked || []).map(w => ({ q: w.q, a: w.a })),
  } : (g && g.not_kept && g.not_kept.length ? { not_kept: g.not_kept } : null);
  return { kind: r.kind, members: r.members, daily: r.daily, week: r.week || null, elsewhere: r.elsewhere || null, presence: r.presence || null,
    restates: r.restates || null, words: words || null, group };
}

const fish = Object.fromEntries(Object.entries(SNAP.fish).map(([c, f]) => [c, f.name]));
const waters = [];
for (const p of PICK) {
  for (const d of p.days) {
    const st = SNAP.states[d.id];
    if (!st) throw new Error(`snapshot has no state ${d.id}`);
    const words = cardWords(st.cardHTML);
    const rows = st.rows.map((r, i) => trimRow(r, words['r' + i]));
    const rules = {};
    for (const r of st.rows) for (const id of ruleIds(r)) {
      const key = st.ruleKey?.[id]; if (!key) continue;
      const info = SNAP.ruleInfo[key] || {}, rule = SNAP.rules[key] || {};
      rules[id] = { book: info.book || [], title: rule.title || info.title || '', sub: rule.sub || '', applies: info.applies || '' };
    }
    waters.push({ id: d.id, w: p.w, label: p.label, md: d.md, name: st.name, kind: st.kind, sub: st.sub, part: st.part,
      parts: (st.parts || []).map(x => ({ v: x.v, t: x.t, short: x.short })), runs: st.runs, gear: st.summary?.says || '', lines: st.lines?.says || '',
      steelLine: st.steelLine || null, rows, rules });
  }
}

const out = { read: SNAP.read, from: SNAP.from, generated: new Date().toISOString(), strip: SNAP.strip, fish, pick: PICK.map(p => ({ w: p.w, label: p.label, days: p.days.map(d => d.md) })), waters };
const banner = `/* regs-diagram: the waters the diagram page draws, copied from regs-v11/v36snap.js (page v36, read ${SNAP.read}) by
   regs-diagram/tools/build.mjs on ${out.generated}. Nothing here is hand-written. */\n`;
fs.writeFileSync(OUT, banner + 'window.DIAG = ' + JSON.stringify(out) + ';\n');
console.log(`wrote ${path.relative(process.cwd(), OUT)}: ${waters.length} states, ${(fs.statSync(OUT).size / 1024).toFixed(0)} KB`);
