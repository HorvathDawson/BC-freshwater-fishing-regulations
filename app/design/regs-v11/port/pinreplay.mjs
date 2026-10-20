// Proof that golden pins.anchors are a sufficient fixture for a PURE pin chain (ticket T28): re-implements the prototype's
// pinUpdate (regs-v11.html, v33 @ 04765ab5) over the recorded anchors and replays every golden state's scroll.
//   node app/design/regs-v11/port/pinreplay.mjs [golden.json]
// pinChain below is the reference the TypeScript port (app/packages/core/src/regs/pins.ts) transcribes. Coordinates are
// content offsets (scrollTop 0); "R(e).top < y" in the prototype is "e.top - scrollY < header".
import fs from 'node:fs'; import path from 'node:path'; import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const G = JSON.parse(fs.readFileSync(process.argv[2] || path.join(HERE, 'golden', 'golden.json'), 'utf8'));

export function pinChain(anchors, scrollY, vp) {
  const { header: hh, height: H, LH, DROP } = vp, lines = [];
  const top = e => e.top - scrollY;
  const art = anchors.find(a => top(a) < hh && top(a) + a.height > hh + LH);
  if (!art) return { lines, skip: false };
  if (!art.group) return { lines, skip: true };          // the prototype throws here (no group): the bar keeps its lines
  if (!(art.tree && art.pin && top(art.pin) < hh && art.groupLine)) return { lines, skip: false };
  lines.push({ cls: art.groupLine.cls, text: art.groupLine.text, sub: art.groupLine.sub });
  for (const t of art.tiers) {
    const nextIn = t.nextHead != null && t.nextHead - scrollY < H - LH;
    if (art.formA && t.tier === 'share' && t.n === art.headlineN) continue;
    if (t.short && t.head && top(t) < hh && top(t) + t.height > hh + LH + DROP && !nextIn && top(t.head) < hh + lines.length * LH)
      lines.push({ cls: 'c', text: t.short, sub: null });
  }
  return { lines, skip: false };
}

let pass = 0, fail = 0, skipped = 0; const bad = [];
for (const [id, s] of Object.entries(G.states)) {
  const P = s.pins, out = []; let last = null, cur = [];
  const end = P.samples.length ? Math.max(...P.samples.map(x => x.y)) + 8 * 400 : 0;
  for (let y = 0; y <= end; y += 8) {
    const r = pinChain(P.anchors, y, P.viewport); if (!r.skip) cur = r.lines;
    const k = JSON.stringify(cur.map(l => [l.cls, l.text, l.sub]));
    if (k !== last) { out.push({ y, k }); last = k; }
  }
  const want = P.samples.map(x => ({ y: x.y, k: JSON.stringify(x.lines.map(l => [l.cls, l.text, l.sub])) }));
  // compare the transitions inside the sampled range
  const lim = want.length ? want[want.length - 1].y : 0, got = out.filter(x => x.y <= lim);
  const ok = JSON.stringify(got) === JSON.stringify(want);
  if (P.error) skipped++;
  if (ok) pass++; else { fail++; bad.push(id); }
}
console.log(`pin replay: ${pass} / ${pass + fail} states reproduce every golden pin transition (${skipped} carry the prototype's pinUpdate error)${bad.length ? '; differ: ' + bad.slice(0, 12).join(' ') : ''}`);
