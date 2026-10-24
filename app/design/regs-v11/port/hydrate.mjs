// The hydrate oracle for the app port: what page v36's OWN decode layer says of every golden state — describe() of every
// rule a state cites, settle() (the day's ladder context) and buildModel() (the card's rows) — so the TS port of page_v36.js
// lines 1–242 (tickets T06–T08) is tested against the page's code, not against a re-reading of it.
//   node app/design/regs-v11/port/hydrate.mjs [--ref HEAD|<sha>|WORKTREE] [--out dir]
//   -> <out>/hydrate.json   { meta, describe: {<rix>: string}, rules: {<rix>: RuleFacts}, states: {<id>: State} }
// Node only: no Chrome, no network, no .venv. page_v36.js (staged at --ref by stage.mjs), page_v36.html's #data/#answers/#cases
// JSON and calendar.generated.js (read at the same ref with `git show`) are evaluated with a stub `document`, up to the end of
// `strip` (the line before `function segBar`), with YEAR = taps.json meta.year (2026). States come from golden/taps.json
// (item id, part index, md); each state's `s` must equal taps.json's — printed, and a mismatch exits 1. The page's parts are
// the PROTOTYPE's: a state the release renumbered is hydrated at taps.json `goldenPartIx`, and a state whose part the release
// merged away (taps.json `gone`, T115) is hydrated too (the page still has it; there is no store `s` to check it against),
// all in golden.json's order.
// Rule numbers are the export's rix (page_data.py: D.rid = rix of the full export = the store's rule.rix).
import fs from 'node:fs'; import path from 'node:path'; import { execFileSync } from 'node:child_process';
import { stage, args, REPO, HERE } from './stage.mjs';

const A = args(); fs.mkdirSync(A.out, { recursive: true });
const S = stage(A.ref);
const REF = 'app/design/regs-v11/reference/';
const INPUTS = ['page_v36.js', 'page_v36.html', 'calendar.generated.js'].map(f => REF + f);
const atRef = f => A.ref === 'WORKTREE' ? fs.readFileSync(path.join(REPO, f), 'utf8')
  : execFileSync('git', ['-C', REPO, 'show', `${A.ref}:${f}`], { encoding: 'utf8', maxBuffer: 1 << 30 });

try {
  const html = atRef(REF + 'page_v36.html');
  const block = id => { const m = html.match(new RegExp(`<script[^>]*id="${id}"[^>]*>([\\s\\S]*?)</script>`)); if (!m) throw new Error(`page_v36.html has no #${id}`); return m[1]; };
  const cal = atRef(REF + 'calendar.generated.js');
  const js = fs.readFileSync(path.join(S.root, REF + 'page_v36.js'), 'utf8').split('\n');
  const end = js.findIndex(l => /^function segBar\(/.test(l));
  if (end < 0) throw new Error('page_v36.js: no `function segBar` — the decode layer moved; update hydrate.mjs');
  const taps = JSON.parse(fs.readFileSync(path.join(A.out, 'taps.json'), 'utf8'));
  const YEAR = taps.meta.year ?? 2026;
  const body = `${cal}\n${js.slice(0, end).join('\n')}\nYEAR = ${YEAR};\n` +
    'return { D, A, makePlace, settle, describe, buildModel, dayStatus, strip, RX };';
  const P = new Function('document', body)({ getElementById: id => ({ textContent: block(id) }) });

  const inf = x => x === Infinity ? 'Infinity' : x;
  const ix = r => r == null ? null : r.ix;
  const ruleFacts = r => ({ key: r.key, kind: r.k, baseRank: r.baseRank, species: r.species, speciesExcept: r.speciesExcept,
    wins: r.wins, tcond: r.tcond, part: r.part, notes: r.notes });
  const describe = {}, rules = {};
  for (const r of Object.values(P.D.rules)) { describe[r.ix] = P.describe(r); rules[r.ix] = ruleFacts(r); }

  const states = {}; let same = 0; const bad = [];
  const order = Object.keys(JSON.parse(fs.readFileSync(path.join(A.out, 'golden.json'), 'utf8')).states);
  for (const id of order) {
    const t = taps.states[id] ?? taps.gone?.[id];
    if (!t) { bad.push(`${id}: in golden.json, neither tapped nor gone in taps.json`); continue; }
    const wi = P.D.waters.findIndex(w => w.id === t.item);
    if (wi < 0) { bad.push(`${id}: no water ${t.item} in page_v36.html`); continue; }
    const w = P.makePlace(wi, t.goldenPartIx ?? t.partIx);
    if (!w.key) { bad.push(`${id}: no key`); continue; }
    const m = P.buildModel(w, t.md), c = m.ctx;
    if (taps.gone?.[id]) { /* no store part: nothing to check s against */ }
    else if (c.s === t.s) same++; else bad.push(`${id}: s ${c.s}, taps.json ${t.s}`);
    const cites = new Set();
    const add = r => { if (r && r.ix != null) cites.add(r.ix); };
    [...c.speaks, ...c.beside, ...c.shown, ...c.broad].forEach(add); c.inpart.forEach(add);
    for (const [a, b] of c.lifted) { cites.add(a); if (b != null) cites.add(b); }
    for (const row of m.rows) { add(row.pool); add(row.win); add(row.narrow); row.wins.forEach(add); }
    for (const v of Object.values(m.R)) for (const h of [v.H, v.W]) if (h) { add(h.win); add(h.narrow); }
    const via = {}, ranks = {};
    for (const r of w.cands) { if (r.via === 'trib') via[r.ix] = 'trib'; if (r.rank !== r.baseRank) ranks[r.ix] = r.rank; }
    const res = h => h && { status: h.status, daily: inf(h.daily), win: ix(h.win), narrow: ix(h.narrow) };
    states[id] = {
      s: c.s, via, ranks,
      cites: [...cites].sort((a, b) => a - b),
      settle: { status: c.status?.status ?? null, broad: c.broad.map(ix), speaks: c.speaks.map(ix), beside: c.beside.map(ix),
        shown: c.shown.map(ix), inpart: c.inpart.map(r => [r.ix, r.now]), lifted: [...c.lifted], timed: c.timed.map(ix),
        side: c.side.map(ix), standing: c.standing.map(ix), anglerclosed: c.anglerclosed.map(ix) },
      model: { spp: m.spp, line: m.line, missingRows: m.missingRows, status: m.status, broad: m.broad.map(ix),
        rows: m.rows.map(r => ({ kind: r.kind, pool: ix(r.pool), win: ix(r.win), daily: inf(r.daily), narrow: ix(r.narrow), members: r.members })),
        R: Object.fromEntries(Object.entries(m.R).map(([k, v]) => [k, { H: res(v.H), W: res(v.W) }])) },
      dayStatus: m.rows.map(r => P.strip(w, r.members)),
    };
  }
  // meta.sha: the last commit of the files this oracle is computed from (at --ref), not the ref's tip — so regenerating at
  // HEAD after an unrelated commit (including the one that commits this file) rewrites nothing.
  const sha = A.ref === 'WORKTREE' ? 'WORKTREE' : execFileSync('git', ['-C', REPO, 'log', '-1', '--format=%h', A.ref, '--',
    ...INPUTS], { encoding: 'utf8' }).trim();
  const out = { meta: { generator: 'app/design/regs-v11/port/hydrate.mjs', ref: A.ref, sha,
    golden: taps.meta.golden, version: taps.meta.version, year: YEAR, decodeLines: `1-${end}` }, describe, rules, states };
  fs.writeFileSync(path.join(A.out, 'hydrate.json'), JSON.stringify(out, null, 1));
  const n = Object.keys(taps.states).length;
  console.log(`hydrate: s equals taps.json on ${same}/${n} tapped states (+ ${Object.keys(taps.gone ?? {}).length} gone), ${Object.keys(describe).length} rules described -> ${path.relative(REPO, path.join(A.out, 'hydrate.json'))}`);
  if (bad.length) { console.error(bad.join('\n')); process.exitCode = 1; }
} finally { S.cleanup(); }
