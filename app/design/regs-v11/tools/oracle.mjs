// The cap system's own answers for check.mjs (round 16): seeded random baskets (and each cap at its take and one past it)
// over the data's cap kinds of a few cards, judged by the pipeline's evaluator (pipeline.deliver.answers.caps.accepts,
// which the card gate proves equal to the engine) and the most of each table cell's kinds kept here alone
// (caps.most_here). check.mjs runs the page's own checker on the same baskets and needs no Python.
//   node app/design/regs-v11/tools/oracle.mjs      (after snap.mjs)   -> app/design/regs-v11/oracle.json
import fs from 'node:fs'; import path from 'node:path'; import { fileURLToPath } from 'node:url'; import { execFileSync } from 'node:child_process';
const HERE = path.dirname(fileURLToPath(import.meta.url)), REPO = path.resolve(HERE, '../../../..');
const STATES = ['bc0_501', 'bc1_501', 'vedder_1010', 'x35p_1010'];
const src = fs.readFileSync(path.join(HERE, '..', 'v36snap.js'), 'utf8'), SNAP = JSON.parse(src.slice(src.indexOf('{'), src.lastIndexOf('}') + 1));
let seed = 16; const rnd = n => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed % n; };
const PL = ['here', 'stream', 'lake'], jobs = [];
for (const id of STATES) { const st = SNAP.states[id]; if (!st) throw new Error('oracle: no state ' + id);
  st.rows.forEach((r, ri) => { const g = r.group; if (!g?.caps?.length) return;
    const ids = g.cap_kinds.map(k => k.id), baskets = [];
    for (let i = 0; i < 150; i++) { const b = {}; for (let j = 1 + rnd(4); j--;) { const p = PL[rnd(5) < 3 ? 0 : 1 + rnd(2)], k = ids[rnd(ids.length)]; (b[p] = b[p] || {})[k] = (b[p][k] || 0) + 1 + rnd(2); } baskets.push(b); }
    g.caps.forEach(c => PL.forEach(p => { if (!c.kinds[p].length) return; for (const n of [c.take, c.take + 1]) { const b = {[p]: {}};
      for (let i = 0; i < n; i++) { const k = c.kinds[p][i % c.kinds[p].length]; b[p][k] = (b[p][k] || 0) + 1; } if (p !== 'here' && c.kinds.here.length) b.here = {[c.kinds.here[0]]: 1}; baskets.push(b); } }));
    const cells = Object.entries(g.table.bands).flatMap(([sp, bs]) => bs.map((b, i) => { const c = g.table.cells[sp][i], T = c.as || sp, to = b.to ?? 1e9;
      return {sp, i, ids: g.cap_kinds.filter(k => k.species === T && (k.from || 0) < to && (k.to == null || k.to > (b.from || 0)) && (!c.origin || !k.origin || k.origin === c.origin)).map(k => k.id)}; }));
    jobs.push({id, ri, caps: g.caps, baskets, cells}); }); }
const py = `import json, sys
from pipeline.deliver.answers.caps import accepts, most_here
out = []
for j in json.load(sys.stdin):
    out.append({"id": j["id"], "ri": j["ri"], "baskets": [{"b": b, "ok": accepts(j["caps"], b)} for b in j["baskets"]],
                "cells": [{**c, "most": most_here(j["caps"], c["ids"])} for c in j["cells"]]})
print(json.dumps(out))`;
const out = JSON.parse(execFileSync(path.join(REPO, '.venv/bin/python'), ['-c', py], { cwd: REPO, input: JSON.stringify(jobs), encoding: 'utf8', maxBuffer: 1 << 28 }));
const file = path.join(HERE, '..', 'oracle.json');
fs.writeFileSync(file, JSON.stringify({ read: SNAP.read, from: 'pipeline.deliver.answers.caps (accepts, most_here)', groups: out }));
console.log(`wrote ${path.relative(REPO, file)}: ${out.length} groups, ${out.reduce((t, g) => t + g.baskets.length, 0)} baskets (${out.reduce((t, g) => t + g.baskets.filter(b => !b.ok).length, 0)} refused)`);
