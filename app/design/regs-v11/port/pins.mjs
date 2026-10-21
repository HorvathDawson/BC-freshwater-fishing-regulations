// The pin bar's oracle at regs v35+ (ticket T28): golden.json's `pins` block records the v33 anchors (nextHead, DROP), but
// round UR6 rewrote pinUpdate — a tier's line now stays until its section's LAST row (`secEnd`: the bottom of the last
// .krow / .tier in the section) has gone up under a stack whose height is MEASURED (each line's height cached by its
// words). This records, per state, exactly the inputs that pinUpdate reads and the bar it draws, from the RENDERED page:
//   node app/design/regs-v11/port/pins.mjs [--ref HEAD|<sha>|WORKTREE] [--out dir] [--only <state-id regex>]
//   -> <out>/pins.json   { meta, states: {<id>: { viewport, anchors: [... tiers[].end ...], samples: [... lines[].h ...] }} }
// Coordinates are content offsets at scrollTop 0 (as golden.json), kept to 0.01 px (pinUpdate compares unrounded rects).
// Every sample's lines equal golden.json's (the conformance test checks it), so the two files are one prototype run's truth.
import fs from 'node:fs'; import path from 'node:path'; import { pathToFileURL } from 'node:url';
import { open } from '../tools/cdp.mjs';
import { stage, args } from './stage.mjs';

const A = args(); fs.mkdirSync(A.out, { recursive: true });
const S = stage(A.ref);

function EXTRACT(onlyRe) {
  const norm = t => (t || '').replace(/\s+/g, ' ').trim(), r2 = v => Math.round(v * 100) / 100;
  const sc = document.getElementById('sc');
  const pins = () => { const hdr = document.getElementById('hdr'), keep = document.getElementById('keep'), bar = document.querySelector('#pins .pinbar');
    sc.scrollTop = 0; const top0 = sc.getBoundingClientRect().top, Y = e => r2(e.getBoundingClientRect().top - top0 + sc.scrollTop), H = e => r2(e.getBoundingClientRect().height);
    const st = ST(), byRow = a => (st.rows[+(a.dataset.row || 'r0').slice(1)] || {}).group;
    const anchors = [...keep.querySelectorAll('article.row.ledger')].map(a => { const g = byRow(a), root = g && g.tree ? [].concat(g.tree)[0] : null, pin = a.querySelector(':scope > .ghd.gpin');
      const byId = {}; (function walk(ns) { (ns || []).forEach(x => { if (x.node === 'tier') { byId[x.id] = x; walk(x.children); } }); })(root ? root.children : []);
      const name = norm(a.querySelector('.rtitle')?.firstChild?.textContent || ''), totA = !!pin?.querySelector('[data-tot="a"]'), hl = g ? g.headline : null;
      return { row: a.dataset.row, group: !!g, tree: !!root, name, formA: totA, headlineN: hl ? hl.n : null, top: Y(a), height: H(a),
        pin: pin ? { top: Y(pin), height: H(pin) } : null,
        tiers: [...a.querySelectorAll('section.tnode')].map(tn => { const t = byId[tn.dataset.tid], tl = tn.querySelector(':scope > .tier'), e = secEnd(tn);
          return { tid: tn.dataset.tid, tier: tn.dataset.tnode, n: t ? t.n : null, head: tl ? { top: Y(tl), height: H(tl) } : null,
            end: Number.isFinite(e) ? r2(e - top0 + sc.scrollTop) : null }; }) }; });
    const end = keep.offsetTop + keep.offsetHeight, samples = []; let last = null, error = null;
    for (let y = 0; y <= end; y += 8) { sc.scrollTop = y; try { pinUpdate(); } catch (e) { error = error || { y, error: String(e) }; continue; }
      const ls = bar.hidden ? [] : [...bar.querySelectorAll('.pl')].map(p => { const c = p.cloneNode(true), s = c.querySelector('.pls'); const sub = s ? norm(s.textContent) : null; s?.remove();
        return { cls: p.className.replace(/^pl\s*/, ''), text: norm(c.textContent), sub, h: Math.round(p.getBoundingClientRect().height) }; });
      const key = JSON.stringify(ls); if (key !== last) { samples.push({ y: sc.scrollTop, lines: ls, height: bar.hidden ? 0 : Math.round(bar.getBoundingClientRect().height) }); last = key; } }
    sc.scrollTop = 0; try { pinUpdate(); } catch (e) {}
    return { error, viewport: { width: sc.clientWidth, height: sc.clientHeight, header: hdr.offsetHeight, LH: 26, ROW: PIN_ROW }, anchors, samples }; };
  const list = []; for (const [w, W] of Object.entries(WATERS)) for (const [d, x] of Object.entries(W.snap)) [].concat(x).forEach((id, p) => { if (id && SNAP.states[id]) list.push({ id, w, d: +d, p }); });
  const one = ({ id, w, d, p }) => { go({ w, d, p, sec: 'keep', sheet: '', map: 0, age: 'adult', res: 'resident', tot: '', chg: '', theme: '' }); return { id, pins: pins() }; };
  window.__PINS = { list: list.filter(x => !onlyRe || new RegExp(onlyRe).test(x.id)), one: i => JSON.stringify(one(window.__PINS.list[i])) };
  return window.__PINS.list.length;
}

const b = await open(`${pathToFileURL(S.page).href}#shot=1`, { offline: true });
try {
  await new Promise(r => setTimeout(r, 600));
  const n = await b.evaluate(`(${EXTRACT.toString()})(${JSON.stringify(A.only || '')})`);
  const out = { states: {} };
  for (let i = 0; i < n; i++) { const r = JSON.parse(await b.evaluate(`window.__PINS.one(${i})`)); out.states[r.id] = r.pins; }
  const meta = { generator: 'app/design/regs-v11/port/pins.mjs', ref: S.ref, sha: S.sha, version: S.version, states: Object.keys(out.states).length };
  const file = path.join(A.out, 'pins.json');
  fs.writeFileSync(file, JSON.stringify({ meta, ...out }, null, 1));
  console.log(`pins: ${meta.states} states, regs ${meta.version} @ ${meta.sha} -> ${path.relative(process.cwd(), file)} (${(fs.statSync(file).size / 1e6).toFixed(2)} MB)`);
} finally { await b.close(); S.cleanup(); }
