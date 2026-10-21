// Proof that the recorded pin inputs are a sufficient fixture for a PURE pin chain (ticket T28): re-implements the
// prototype's pinUpdate (regs-v11.html, v38 @ 7fb6809f, round UR6) over pins.json's anchors (port/pins.mjs: each tier's
// head and `end` = secEnd, each drawn line's measured height) with golden.json's strings (the group line, each tier's
// short line), and replays every golden state's scroll.
//   node app/design/regs-v11/port/pinreplay.mjs [golden.json] [pins.json]
// pinChain below is the reference the TypeScript port (app/packages/core/src/regs/pins.ts) transcribes. Coordinates are
// content offsets (scrollTop 0); "R(e).top < y" in the prototype is "e.top - scrollY < header".
import fs from 'node:fs'; import path from 'node:path'; import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const G = JSON.parse(fs.readFileSync(process.argv[2] || path.join(HERE, 'golden', 'golden.json'), 'utf8'));
const P = JSON.parse(fs.readFileSync(process.argv[3] || path.join(HERE, 'golden', 'pins.json'), 'utf8'));

export function pinChain(anchors, scrollY, vp, hOf) {
  const { header: hh, LH, ROW } = vp, top = e => e.top - scrollY;
  const art = anchors.find(a => top(a) < hh && top(a) + a.height > hh + LH);
  if (!art || !art.group || !art.tree || !art.groupLine || !art.pin || !(top(art.pin) < hh)) return [];   // v35: a group-less row pins nothing
  const lines = [art.groupLine]; let B = hh + hOf(art.groupLine);
  for (const t of art.tiers) {
    if (!t.short || !t.head) continue;
    if (art.formA && t.tier === 'share' && t.n === art.headlineN) continue;
    const line = { cls: 'c', text: t.short, sub: null }, h = hOf(line);
    if (top(t.head) < B && t.end != null && t.end - scrollY > B + h + ROW) { lines.push(line); B += h; }
  }
  return lines;
}

const key = l => `${l.cls}|${l.text}|${l.sub || ''}`, k3 = ls => JSON.stringify(ls.map(l => [l.cls, l.text, l.sub]));
const HC = {}; for (const s of Object.values(P.states)) for (const x of s.samples) for (const l of x.lines) HC[key(l)] = l.h;
const hOf = l => HC[key(l)] ?? (l.sub ? 2 * 26 : 26);
let pass = 0, fail = 0; const bad = [];
for (const [id, p] of Object.entries(P.states)) {
  const g = G.states[id].pins;
  const anchors = p.anchors.map((a, i) => ({ ...a, groupLine: g.anchors[i].groupLine, tiers: a.tiers.map((t, j) => ({ ...t, short: g.anchors[i].tiers[j].short })) }));
  const lim = p.samples.length ? p.samples[p.samples.length - 1].y : 0, out = []; let last = null;
  for (let y = 0; y <= lim; y += 8) { const k = k3(pinChain(anchors, y, p.viewport, hOf)); if (k !== last) { out.push({ y, k }); last = k; } }
  const want = g.samples.map(x => ({ y: x.y, k: k3(x.lines) }));
  if (JSON.stringify(out) === JSON.stringify(want)) pass++; else { fail++; bad.push(id); }
}
console.log(`pin replay: ${pass} / ${pass + fail} states reproduce every golden pin transition (regs ${P.meta.version} @ ${P.meta.sha})${bad.length ? '; differ: ' + bad.slice(0, 12).join(' ') : ''}`);
