
/* ================= THE LADDER, per fish (guide.ladder) =================
   effective(members, kind, md, S, opts) -> Map(key -> { state, partly, by })
   members: [{ r, via }] the rules a stretch carries; kind: 'stream' | 'lake'; md: MMDD number; S: a fish code.
   state: speaks | beside | shown | not_yet_mapped | displaced | lifted  (absent = does not speak for this fish / not in force) */
const LAD = (() => {
  let GROUPS = {}, FISHES = [];
  const init = sp => { GROUPS = sp.groups; FISHES = Object.keys(sp.fish); };
  const expand = codes => { const out = new Set(); (codes || []).forEach(c => { const g = GROUPS[c]; if (g){ if (g.open) out.add('*' + c); else (g.members || []).forEach(m => out.add(m)); } else out.add(c); }); return out; };
  const md = (m, d) => m * 100 + d;
  const wins = w => (w?.dates || []).map(d => [md(d.from_month, d.from_day), md(d.to_month, d.to_day)]);
  const inW = (W, x) => !W.length || W.some(([a, b]) => a <= b ? x >= a && x <= b : x >= a || x <= b);
  const partTime = w => !!(w && (w.hours || w.weekdays || w.unparsed || w.times));

  function norm(r){
    if (r._n) return r._n;
    const f = r.fields;
    const n = { f, sp: f.species ? expand(f.species) : null, ex: expand(f.species_except), tg: f.when_targeting ? expand(f.when_targeting) : null,
      wins: wins(f.when), part: partTime(f.when), closure: f.take === 0 && f.may_target === false,
      lifts: (f.exempts || []).map(e => ({ key:`${e.entry_id}::${e.rule_id}`, sp: e.species ? expand(e.species) : null, wins: wins(e.when),
        partial: !!(e.origin || e.lengths || e.while || e.when_targeting), e })) };
    n.release = f.take === 0 && !f.lengths && !f.while && !f.when_targeting && !f.within;
    n.keeps = r.type === 'retention_limit' && !n.closure && (f.take > 0 || f.unlimited || (f.take == null && (f.lengths || []).length > 0));
    return (r._n = n);
  }
  function speaksFor(r, S){
    const n = norm(r);
    if (n.ex.has(S)) return false;
    if (n.tg && !n.tg.has(S) && !n.tg.has('*ALL_FIN_FISH')) return false;
    if (!n.sp) return true;
    if (n.sp.has(S) || n.sp.has('*ALL_FIN_FISH')) return true;
    return false;
  }
  const names = (r, S, root) => norm(r).closure || (root.fields.species || []).includes(S);
  const region = r => r.entry_id.split(':')[0];
  const zone = x => x.rank >= 2;
  const zoneEntry = r => /^z/.test(r.entry_id);
  const stmt = r => { const f = r.fields; return JSON.stringify([[...(f.species || [])].sort(), (f.species_except || []).filter(c => c !== 'CHAR').sort(), f.lengths || null, f.origin || null, f.water || null, f.while || null, f.when_targeting || null, f.period || null]); };

  function effective(members, kind, day, S, opts = {}){
    const out = new Map(), live = [];
    const byRid = {}; members.forEach(m => byRid[m.r.entry_id + '::' + m.r.rule_id] = m.r);
    const rootOf = r => { let x = r, g = 0; while (x.fields.within && g++ < 8){ const p = byRid[x.entry_id + '::' + x.fields.within]; if (!p) break; x = p; } return x; };
    // 1. which rules are about this fish, today, here
    for (const { r, via } of members){
      const n = norm(r), f = r.fields;
      const key = r.entry_id + '::' + r.rule_id;
      if (!speaksFor(r, S)) continue;
      if (f.water && f.water !== kind) continue;
      if (!inW(n.wins, day)) continue;
      if (r.dimension === 'lift') continue;
      if (opts.steelheadWater && S === 'RB' && (f.lengths || []).length && f.lengths.every(l => (l.min_cm ?? 0) >= 50)) continue;
      const pr = (r.provenance || r.prov).rank, rank = via === 'trib' && pr >= 0 ? 1 : pr;
      const x = { r, key, n, rank, via, root: null, state: null, partly: false };
      if (f.standing || r.family === 'information') x.state = 'shown';
      else if (r.binds === 'sections_in_part' || r.not_yet_mapped) x.state = 'not_yet_mapped';
      else if (n.part || f.side) x.state = 'beside';
      out.set(key, x); live.push(x);
    }
    live.forEach(x => x.root = rootOf(x.r));
    // 2. lifts, from lifters in force here (the lift-only rules too)
    const lifters = members.filter(({ r }) => { const n = norm(r); return n.lifts.length && inW(n.wins, day) && r.binds !== 'sections_in_part' && !(r.fields.water && r.fields.water !== kind); });
    for (const { r } of lifters) for (const L of norm(r).lifts){
      const t = out.get(L.key); if (!t || t.key === r.entry_id + '::' + r.rule_id) continue;
      if (!inW(L.wins, day)) continue;
      if (L.sp && !L.sp.has(S)) continue;
      // asked for one origin: a lift for that origin alone lifts it outright, a lift for the other origin not at all
      let partial = L.partial;
      if (opts.origin && L.e.origin){ if (L.e.origin !== opts.origin) continue; partial = !!(L.e.lengths || L.e.while || L.e.when_targeting); }
      if (partial) t.partly = true; else { t.state = 'lifted'; t.by = r.entry_id + '::' + r.rule_id; }
    }
    // 3. competition within (type, dimension)
    const comp = live.filter(x => !x.state);
    const beats = (a, b) => {
      if (a === b || a.root === b.root || a.r.entry_id === b.r.entry_id && (a.root === b.r || b.root === a.r)) return false;
      if (b.n.closure) return false;                                       // closures only lift
      if (a.rank === -1 && b.rank !== -1) return true;
      if (b.rank === -1) return false;
      const la = names(a.r, S, a.root), lb = names(b.r, S, b.root);
      // two quotas that let the fish be kept, one of the water and one of the zone: only the same statement replaces
      // two quotas that let the fish be kept: only the same statement replaces (the water's number over the zone's);
      // different statements sit beside each other, whatever naming says
      if (a.n.keeps && b.n.keeps && !(zone(a) && zone(b) && a.rank === b.rank && region(a.r) !== region(b.r))) return (a.rank < b.rank || a.rank === b.rank && zoneEntry(b.r) && !zoneEntry(a.r)) && stmt(a.root) === stmt(b.root) && stmt(a.r) === stmt(b.r);   // a zone line naming the water restates the row: the row's speaks (zone_line_restated)
      // two regions' tables on one lake: the stricter
      if (zone(a) && zone(b) && a.rank === b.rank && region(a.r) !== region(b.r)){
        if (a.n.closure) return true;
        if (a.n.release && b.n.keeps) return true;
        if (a.n.keeps && b.n.keeps) return stmt(a.r) === stmt(b.r) && (a.r.fields.take ?? 1e9) < (b.r.fields.take ?? 1e9);
        return false;
      }
      // a water row's release is never beaten by a zone rule that lets the fish be kept
      if (b.rank <= 1 && b.n.release && zone(a) && a.n.keeps) return false;
      // (B) a dated zone release stands beside a water quota printing no dates of its own
      if (zone(a) && a.n.wins.length && a.r.fields.take === 0 && b.rank <= 1 && b.n.keeps && !b.n.wins.length) return false;
      //     and naming and place never let such an undated water quota displace it: only the exact same statement on the same dates
      if (zone(b) && b.n.wins.length && b.r.fields.take === 0 && a.rank <= 1 && a.n.keeps && !a.n.wins.length) return false;
      // (A) a water rule printing its own dates for the fish overrides a dated zone release or quota on the days both hold
      if (a.rank <= 1 && a.n.wins.length && zone(b) && b.n.wins.length && !b.n.closure && (la || stmt(a.r) === stmt(b.r)) && (a.n.keeps || a.r.fields.take === 0)) return true;
      if (la !== lb) return la;
      return a.rank < b.rank;
    };
    const groups = new Map(); comp.forEach(x => { const k = x.r.type + '|' + x.r.dimension; (groups.get(k) || groups.set(k, []).get(k)).push(x); });
    // a rule that is itself displaced displaces nothing: settle from the rules nobody still speaking beats
    for (const G of groups.values()){
      const st = new Map(G.map(x => [x, null])), bt = new Map(G.map(b => [b, G.filter(a => beats(a, b))]));
      let moved = true;
      while (moved){ moved = false;
        for (const b of G){ if (st.get(b)) continue;
          const live = bt.get(b).filter(a => st.get(a) !== 'displaced');
          const win = live.find(a => st.get(a) === 'speaks');
          if (win){ st.set(b, 'displaced'); b.by = win.key; moved = true; }
          else if (!live.length){ st.set(b, 'speaks'); moved = true; } } }
      for (const b of G){ if (st.get(b) === 'displaced' || (!st.get(b) && bt.get(b).length)){ b.state = 'displaced'; b.by = b.by || bt.get(b)[0].key; } }
    }
    // 3b. two regions' tables on one lake: a closure of one beats any open retention rule of the other, whatever its dimension
    const zc = live.filter(x => x.state === null && zone(x) && x.n.closure && x.r.type === 'retention_limit');
    for (const x of live) if (x.state === null && zone(x) && x.r.type === 'retention_limit' && !x.n.closure){
      const a = zc.find(c => c.rank === x.rank && region(c.r) !== region(x.r)); if (a){ x.state = 'displaced'; x.by = a.key; }
    }
    // 3c. a zone release printed for this kind of water (streams or lakes), in force, displaces its own region's table rules that let the fish be kept
    const zrel = live.filter(x => x.state === null && zone(x) && x.n.release && !x.n.closure && x.r.type === 'retention_limit' && x.r.fields.water && x.r.fields.water === kind);
    for (const y of live) if (y.state === null && zone(y) && y.r.type === 'retention_limit' && y.n.keeps && y.r.fields.take != null){
      const a = zrel.find(x => x.rank === y.rank && region(x.r) === region(y.r) && (!x.r.fields.origin || x.r.fields.origin === y.r.fields.origin));
      if (a){ y.state = 'displaced'; y.by = a.key; }
    }
    // 4. a water's outright release silences zone rules of any dimension that would let this fish be kept
    const rels = live.filter(x => x.rank <= 1 && x.rank >= 0 && x.n.release && x.r.type === 'retention_limit' && (x.state === null || x.state === 'displaced'));
    if (rels.length){
      const relAll = live.filter(x => x.n.release && x.r.type === 'retention_limit' && (x.state === null || x.state === 'displaced'));
      const covered = o => relAll.some(x => !x.r.fields.origin || x.r.fields.origin === o) && rels.some(x => !x.r.fields.origin || x.r.fields.origin === o);
      for (const x of live) if (x.state === null && zone(x) && x.r.type === 'retention_limit' && x.n.keeps){
        const need = x.r.fields.origin ? [x.r.fields.origin] : ['hatchery', 'wild'];
        if (need.every(covered)){ x.state = 'displaced'; x.by = rels[0].key; }
      }
    }
    live.forEach(x => { if (!x.state) x.state = 'speaks'; });
    return out;
  }
  return { init, effective, norm, expand, speaksFor };
})();
if (typeof module !== 'undefined') module.exports = LAD;
/* ================= fish illustrations: drawn here, no outside images =================
   One parametric side-view fish, tuned per species. Numbered marks point at the features that tell it apart;
   the numbers match the "What to look for" list under the picture. */
const FISHART = (() => {
  const VB_W = 400, VB_H = 150, CY = 76;
  // a smooth closed path through points (Catmull-Rom to cubic Bezier)
  const smooth = (pts, closed = true) => {
    const p = closed ? [pts[pts.length - 1], ...pts, pts[0], pts[1]] : [pts[0], ...pts, pts[pts.length - 1]];
    let d = `M${pts[0][0].toFixed(1)} ${pts[0][1].toFixed(1)}`;
    for (let i = 1; i < p.length - 2; i++){
      const [p0, p1, p2, p3] = [p[i - 1], p[i], p[i + 1], p[i + 2]];
      const c1 = [p1[0] + (p2[0] - p0[0]) / 6, p1[1] + (p2[1] - p0[1]) / 6], c2 = [p2[0] - (p3[0] - p1[0]) / 6, p2[1] - (p3[1] - p1[1]) / 6];
      d += ` C${c1[0].toFixed(1)} ${c1[1].toFixed(1)} ${c2[0].toFixed(1)} ${c2[1].toFixed(1)} ${p2[0].toFixed(1)} ${p2[1].toFixed(1)}`;
    }
    return d + (closed ? 'Z' : '');
  };
  // seeded random so each species always draws the same spots
  const rng = seed => () => { seed = (seed * 16807) % 2147483647; return (seed - 1) / 2147483646; };

  function body(c){
    const x0 = c.x0 ?? 28, xp = c.xp ?? 318, L = xp - x0, h = c.h, ph = c.ph ?? h * 0.24, b = c.belly ?? 1, s = c.snout ?? 0.1;
    const t = c.top || [[0, s * h], [0.04, -0.44], [0.15, -0.86], [0.32, -1], [0.52, -0.9], [0.72, -0.6], [0.9, -0.34]];
    const u = c.bot || [[0.9, 0.32], [0.74, 0.52], [0.54, 0.84], [0.34, 0.96], [0.17, 0.84], [0.05, 0.48]];
    const pts = [...t.map(([f, y], i) => [x0 + f * L, CY + (i === 0 ? y : y * h)]), [xp, CY - ph], [xp, CY + ph], ...u.map(([f, y]) => [x0 + f * L, CY + y * h * b])];
    return { d:smooth(pts), x0, xp, L, h, ph, at:(f, yf) => [x0 + f * L, CY + yf * h] };
  }
  function tail(c, B){
    const tl = c.tailLen ?? 58, th = c.tailH ?? B.h * 0.95, fork = c.fork ?? 0.45, xp = B.xp, ph = B.ph, xt = xp + tl;
    if (c.tailShape === 'round') return `M${xp} ${CY - ph} C${xp + tl * 0.6} ${CY - th} ${xt + 6} ${CY - th * 0.5} ${xt} ${CY} C${xt + 6} ${CY + th * 0.5} ${xp + tl * 0.6} ${CY + th} ${xp} ${CY + ph}Z`;
    if (c.tailShape === 'shark') return `M${xp - 4} ${CY - ph} Q${xp + tl * 0.5} ${CY - th * 0.7} ${xt + 14} ${CY - th * 1.25} Q${xp + tl * 0.62} ${CY - th * 0.1} ${xp + tl * 0.5} ${CY + th * 0.35} Q${xp + tl * 0.2} ${CY + th * 0.2} ${xp - 4} ${CY + ph}Z`;
    const notch = xp + tl * (1 - fork);
    return `M${xp - 2} ${CY - ph} Q${xp + tl * 0.45} ${CY - ph * 1.1} ${xt} ${CY - th} Q${xt - tl * 0.12} ${CY - th * 0.45} ${notch} ${CY} Q${xt - tl * 0.12} ${CY + th * 0.45} ${xt} ${CY + th} Q${xp + tl * 0.45} ${CY + ph * 1.1} ${xp - 2} ${CY + ph}Z`;
  }
  // a fin as a curved polygon from base start to base end, with its tip offset
  const fin = (a, b, tip, bulge = 0.25) => { const mx = (a[0] + b[0]) / 2, my = (a[1] + b[1]) / 2; return `M${a[0]} ${a[1]} Q${a[0] + (tip[0] - a[0]) * 0.2} ${tip[1] + (a[1] - tip[1]) * bulge} ${tip[0]} ${tip[1]} Q${b[0] - (b[0] - tip[0]) * 0.1} ${tip[1] + (b[1] - tip[1]) * 0.55} ${b[0]} ${b[1]}Z`; };
  const rays = (a, b, tip, n = 6) => { let s = ''; for (let i = 1; i < n; i++){ const f = i / n, bx = a[0] + (b[0] - a[0]) * f, by = a[1] + (b[1] - a[1]) * f, tx = tip[0] + (b[0] - tip[0]) * f * 0.8, ty = tip[1] + (b[1] - tip[1]) * f * 0.8; s += `M${bx.toFixed(1)} ${by.toFixed(1)} L${tx.toFixed(1)} ${ty.toFixed(1)}`; } return s; };

  function draw(key, opts = {}){
    const c = SPECIES[key]; if (!c) return '';
    const id = 'f' + key + (opts.variant || '') + Math.random().toString(36).slice(2, 6);
    const B = body(c), R = rng(c.seed || 7);
    const P = [], D = [], F = [];
    // colours
    const grad = `<linearGradient id="${id}g" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="${c.back}"/><stop offset="0.42" stop-color="${c.side}"/><stop offset="0.78" stop-color="${c.belly2 || c.side}"/><stop offset="1" stop-color="${c.bellyC || '#f4f1ea'}"/></linearGradient>
      <linearGradient id="${id}s" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#fff" stop-opacity=".0"/><stop offset=".35" stop-color="#fff" stop-opacity=".28"/><stop offset=".6" stop-color="#fff" stop-opacity="0"/></linearGradient>
      <clipPath id="${id}c"><path d="${B.d}"/></clipPath>`;
    const finC = c.fin || c.back, finO = c.finO ?? 0.85;
    // fins behind the body
    const dorsal = c.dorsal || [0.42, 0.6, 1.0];            // start, end (fraction of body), height (× h)
    const dA = B.at(dorsal[0], -0.97), dB = B.at(dorsal[1], -0.8), dT = [dA[0] + (dB[0] - dA[0]) * (c.dorsalLean ?? 0.3), CY - B.h * (0.97 + dorsal[2])];
    if (c.spiny){ // spiny front dorsal, soft rear
      const sA = B.at(c.spiny[0], -0.96), sB = B.at(c.spiny[1], -0.99); let sp = `M${sA[0]} ${sA[1]}`;
      const n = c.spinyN || 9; for (let i = 0; i <= n; i++){ const f = i / n, x = sA[0] + (sB[0] - sA[0]) * f, y = sA[1] + (sB[1] - sA[1]) * f; sp += ` L${x - 3} ${y - B.h * c.spiny[2] * (0.7 + 0.3 * Math.sin(Math.PI * f))} L${x + 2} ${y - 2}`; }
      F.push(`<path d="${sp} L${sB[0]} ${sB[1]}Z" fill="${c.spinyC || finC}" opacity="${finO}"/>`);
    }
    if (c.dorsal !== false){
      if (c.sail) { const sa = B.at(dorsal[0], -0.95), sb = B.at(dorsal[1], -0.85); F.push(`<path d="M${sa[0]} ${sa[1]} C${sa[0] + 5} ${sa[1] - B.h * 1.5} ${sb[0] - 20} ${sb[1] - B.h * 1.6} ${sb[0] + 6} ${sb[1] - 4} L${sb[0]} ${sb[1]}Z" fill="${c.sailC || finC}" opacity=".9"/>`); for (let i = 0; i < 16; i++) F.push(`<circle cx="${sa[0] + 8 + R() * (sb[0] - sa[0] - 10)}" cy="${sa[1] - 6 - R() * B.h * 1.1}" r="1.8" fill="${c.sailSpot || '#3b6ea8'}" opacity=".8"/>`); }
      else { F.push(`<path d="${fin(dA, dB, dT)}" fill="${finC}" opacity="${finO}"/><path d="${rays(dA, dB, dT)}" stroke="#000" stroke-opacity=".12" stroke-width=".7" fill="none"/>`); }
    }
    const ADI = [];
    if (c.adipose !== false && c.adipose !== undefined){
      const env = f => 1 - (f - 0.32) * 1.1, aA = B.at(c.adipose[0], -env(c.adipose[0]) + 0.04), aB = B.at(c.adipose[0] + 0.055, -env(c.adipose[0] + 0.055) + 0.04);
      const F = ADI;
      if (opts.clipped) F.push(`<path d="M${aA[0]} ${aA[1] + 1} Q${(aA[0] + aB[0]) / 2} ${aA[1] - 3} ${aB[0]} ${aB[1] + 1}" stroke="#8b5a4a" stroke-width="2.2" fill="none" stroke-linecap="round"/>`);
      else F.push(`<path d="M${aA[0]} ${aA[1] + 1} Q${aA[0] + 5} ${aA[1] - B.h * 0.36} ${aB[0] + 4} ${aB[1] - 2} L${aB[0]} ${aB[1] + 1}Z" fill="${c.adiC || finC}" stroke="rgba(0,0,0,.25)" stroke-width=".6"/>`);
    }
    const anal = c.anal || [0.66, 0.76, 0.55];
    const nA = B.at(anal[0], 0.7 * (c.belly ?? 1)), nB = B.at(anal[1], 0.52 * (c.belly ?? 1)), nT = [nA[0] + (nB[0] - nA[0]) * 0.35, CY + B.h * (0.72 + anal[2])];
    F.push(`<path d="${fin(nA, nB, nT)}" fill="${c.lowFin || finC}" opacity="${finO}"/>`);
    if (c.dorsal2){ const a = B.at(c.dorsal2[0], -0.72), b = B.at(c.dorsal2[1], -0.45); F.push(`<path d="M${a[0]} ${a[1]} Q${(a[0] + b[0]) / 2} ${a[1] - B.h * c.dorsal2[2]} ${b[0]} ${b[1]}Z" fill="${finC}" opacity="${finO}"/>`); }
    const pel = c.pelvic || [0.44, 0.52, 0.42], pA = B.at(pel[0], 0.95 * (c.belly ?? 1)), pB = B.at(pel[1], 0.9 * (c.belly ?? 1)), pT = [pA[0] + 12, CY + B.h * (0.95 + pel[2])];
    F.push(`<path d="${fin(pA, pB, pT)}" fill="${c.lowFin || finC}" opacity="${finO}"/>`);
    if (c.whiteEdge) F.push(`<path d="M${pA[0]} ${pA[1]} L${pT[0]} ${pT[1]} M${nA[0]} ${nA[1]} L${nT[0]} ${nT[1]}" stroke="#fff" stroke-width="2.4" stroke-linecap="round"/>`);
    // body
    const tr = []; if (!c.tailShape){ const tl = c.tailLen ?? 58, th = c.tailH ?? B.h * 0.95; for (let i = -4; i <= 4; i++) if (i) tr.push(`M${B.xp + 2} ${CY + i * 1.2} L${B.xp + tl * 0.82} ${CY + i * th * 0.2}`); }
    const bodyShapes = [`<path d="${tail(c, B)}" fill="${c.tailC || finC}" opacity="${c.tailO ?? 0.95}"/><path d="${tr.join('')}" stroke="#000" stroke-opacity=".12" stroke-width=".7"/>`, `<path d="${B.d}" fill="url(#${id}g)" stroke="${c.edge || 'rgba(0,0,0,.35)'}" stroke-width="1"/>`];
    // patterns, clipped to the body
    const inBody = (x, y) => { const t = (x - B.x0) / B.L; if (t < 0.02 || t > 1) return false; const half = B.h * (t < 0.32 ? 0.35 + 2 * t : 1 - (t - 0.32) * 1.1); return Math.abs(y - CY) < Math.max(half, B.ph); };
    const scatter = (n, fx0, fx1, fy0, fy1, fn) => { for (let i = 0; i < n; i++){ const x = B.x0 + (fx0 + R() * (fx1 - fx0)) * B.L, y = CY + (fy0 + R() * (fy1 - fy0)) * B.h; if (inBody(x, y)) P.push(fn(x, y, R())); } };
    if (c.band){ const a = B.x0 + B.L * 0.1, m = B.x0 + B.L * 0.45, e = B.xp + 8, w = B.h * 0.3; P.push(`<path d="M${a} ${CY + 2} C${a + 30} ${CY - w} ${m} ${CY - w * 1.1} ${e} ${CY - w * 0.35} L${e} ${CY + w * 0.4} C${m} ${CY + w * 1.2} ${a + 30} ${CY + w} ${a} ${CY + 2}Z" fill="${c.band}" opacity="${c.bandO || 0.45}"/>`); }
    (c.patterns || []).forEach(p => {
      if (p.t === 'spots') scatter(p.n, p.x[0], p.x[1], p.y[0], p.y[1], (x, y, r) => `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${(p.r * (0.7 + r * 0.6)).toFixed(2)}" fill="${p.c}" opacity="${p.o ?? 0.9}"/>`);
      if (p.t === 'halo') scatter(p.n, p.x[0], p.x[1], p.y[0], p.y[1], (x, y, r) => `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${(p.r * 1.9).toFixed(2)}" fill="${p.h}" opacity=".85"/><circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${p.r.toFixed(2)}" fill="${r < (p.mix || 0) ? p.c2 : p.c}"/>`);
      if (p.t === 'worms') scatter(p.n, p.x[0], p.x[1], p.y[0], p.y[1], (x, y, r) => `<path d="M${x.toFixed(1)} ${y.toFixed(1)} q${4 + r * 5} ${-3 + r * 6} ${9 + r * 6} ${r * 3}" stroke="${p.c}" stroke-width="${p.w || 2.2}" fill="none" stroke-linecap="round" opacity=".85"/>`);
      if (p.t === 'bars'){ for (let i = 0; i < p.n; i++){ const x = B.x0 + B.L * (p.x[0] + (p.x[1] - p.x[0]) * i / (p.n - 1)); P.push(`<path d="M${x} ${CY - B.h * 1.1} Q${x + 4} ${CY} ${x - 2} ${CY + B.h * (p.down || 0.45)}" stroke="${p.c}" stroke-width="${p.w || 8}" stroke-opacity="${p.o || 0.55}" fill="none" stroke-linecap="round"/>`); } }
      if (p.t === 'stripe') P.push(`<path d="M${B.x0 + B.L * 0.18} ${CY} L${B.xp + 4} ${CY}" stroke="${p.c}" stroke-width="${p.w || 7}" stroke-opacity="${p.o || 0.7}" stroke-dasharray="${p.dash || ''}" fill="none"/>`);
      if (p.t === 'scales'){ for (let x = B.x0 + B.L * 0.24; x < B.xp; x += 9) for (let y = CY - B.h; y < CY + B.h; y += 7){ const yy = y + ((x / 9) % 2) * 3.5; if (inBody(x, yy)) P.push(`<path d="M${x - 4} ${yy} q4 5 8 0" stroke="${p.c}" stroke-opacity=".35" stroke-width=".8" fill="none"/>`); } }
      if (p.t === 'blotch') scatter(p.n, p.x[0], p.x[1], p.y[0], p.y[1], (x, y, r) => `<ellipse cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" rx="${(p.r * (1 + r)).toFixed(1)}" ry="${(p.r * 0.6).toFixed(1)}" fill="${p.c}" opacity="${p.o || 0.5}"/>`);
      if (p.t === 'scutes'){ for (let i = 0; i < 13; i++){ const f = 0.18 + i * 0.058, [x, y] = B.at(f, -0.78 + (f > 0.6 ? (f - 0.6) * 0.8 : 0)); P.push(`<path d="M${x - 5} ${y + 2} L${x} ${y - 4} L${x + 5} ${y + 2}Z" fill="#e9ecee" stroke="#6b7479" stroke-width=".7"/>`); } for (let i = 0; i < 16; i++){ const [x, y] = B.at(0.2 + i * 0.05, 0.1); P.push(`<path d="M${x - 3} ${y + 1} L${x} ${y - 2.5} L${x + 3} ${y + 1}Z" fill="#d7dcdf" opacity=".9"/>`); } }
    });
    // lateral line, gill cover, eye, mouth
    const eye = c.eye || [0.075, -0.18], er = B.h * (c.eyeR ?? 0.17), [ex, ey] = B.at(eye[0], eye[1]);
    const gx = B.x0 + B.L * (c.gill ?? 0.19);
    D.push(`<path d="M${gx} ${CY - B.h * 0.62} Q${gx + 10} ${CY} ${gx - 2} ${CY + B.h * 0.72}" stroke="rgba(0,0,0,.28)" stroke-width="1.3" fill="none"/>`);
    D.push(`<path d="M${B.x0 + B.L * 0.21} ${CY - B.h * 0.18} Q${B.x0 + B.L * 0.6} ${CY - B.h * 0.14} ${B.xp} ${CY - 1}" stroke="rgba(0,0,0,.2)" stroke-width=".8" fill="none"/>`);
    const mouth = c.mouth ?? 0.08, [mx, my] = B.at(mouth, c.mouthY ?? 0.12);
    D.push(`<path d="M${B.x0 + 1} ${CY + (c.snout ?? 0.1) * B.h * 0.8} Q${(B.x0 + mx) / 2} ${my + 1} ${mx} ${my}" stroke="rgba(0,0,0,.55)" stroke-width="1.3" fill="none" stroke-linecap="round"/>`);
    if (c.slash) D.push(`<path d="M${B.x0 + B.L * 0.035} ${CY + B.h * 0.42} Q${B.x0 + B.L * 0.08} ${CY + B.h * 0.55} ${B.x0 + B.L * 0.13} ${CY + B.h * 0.5}" stroke="#d6362e" stroke-width="3" fill="none" stroke-linecap="round"/>`);
    if (c.barbel) (Array.isArray(c.barbel) ? c.barbel : [[0.05, 0.3, 10]]).forEach(([f, y, l]) => { const [bx, by] = B.at(f, y); D.push(`<path d="M${bx} ${by} q-2 ${l * 0.6} -4 ${l}" stroke="#5a4e3e" stroke-width="1.4" fill="none" stroke-linecap="round"/>`); });
    D.push(`<circle cx="${ex}" cy="${ey}" r="${er}" fill="${c.eyeC || '#d9c27a'}" stroke="rgba(0,0,0,.5)" stroke-width=".8"/><circle cx="${ex + er * 0.1}" cy="${ey}" r="${er * 0.6}" fill="#111"/><circle cx="${ex - er * 0.25}" cy="${ey - er * 0.3}" r="${er * 0.22}" fill="#fff" opacity=".85"/>`);
    // pectoral fin, over the body
    const [px, py] = B.at((c.gill ?? 0.19) + 0.02, 0.45);
    D.push(`<path d="M${px} ${py} Q${px + 22} ${py + 2} ${px + 30} ${py + 14} Q${px + 12} ${py + 12} ${px} ${py + 6}Z" fill="${c.lowFin || finC}" opacity=".8"/>`);
    // numbered marks
    const marks = (opts.marks === false ? [] : (c.marks || [])).map(([f, yf], i) => { const [mx2, my2] = B.at(f, yf); return `<g><circle cx="${mx2}" cy="${my2}" r="8.5" fill="#0e5a73" stroke="#fff" stroke-width="2"/><text x="${mx2}" y="${my2 + 3.6}" text-anchor="middle" font-size="10.5" font-weight="700" fill="#fff" font-family="system-ui,sans-serif">${i + 1}</text></g>`; });
    const adiMark = opts.adiMark && c.adipose ? (() => { const [ax, ay] = B.at(c.adipose[0] + 0.025, -0.62); return `<circle cx="${ax}" cy="${ay}" r="13" fill="none" stroke="#0e8fb3" stroke-width="2.2"/><path d="M${ax} ${ay - 13} L${ax} ${ay - 28}" stroke="#0e8fb3" stroke-width="1.6"/>`; })() : '';
    return `<svg viewBox="0 0 ${VB_W} ${VB_H}" class="fishsvg" role="img" aria-label="${c.name}${opts.clipped ? ', adipose fin clipped' : ''}"><defs>${grad}</defs>
      <ellipse cx="${(B.x0 + B.xp) / 2 + 20}" cy="${CY + B.h * 1.25 + 10}" rx="${B.L * 0.5}" ry="5" fill="#000" opacity=".07"/>${F.join('')}${bodyShapes.join('')}<g clip-path="url(#${id}c)">${P.join('')}<path d="${B.d}" fill="url(#${id}s)"/></g>${ADI.join('')}${D.join('')}${adiMark}${marks.join('')}</svg>`;
  }

  /* per species: shape, colours, pattern, and marks (anchor points for the numbered tips, in body fractions) */
  const SPECIES = {
    RB:{ name:'Rainbow trout', h:30, back:'#4e6b52', side:'#c3cbc0', belly2:'#e6e4dc', band:'#e0708a', bandO:.5, adipose:[0.8], fork:.3, seed:11,
      patterns:[{ t:'spots', n:95, x:[.08, 1], y:[-1, -0.05], r:1.5, c:'#1a1a1a' }, { t:'spots', n:22, x:[.55, 1], y:[0, .5], r:1.2, c:'#1a1a1a' }],
      tips:['Pink or red band along the side','Small black spots, mostly above the middle of the side','Rows of black spots on the tail'], marks:[[0.45, 0.05], [0.4, -0.6], [1.12, -0.35]] },
    ST:{ name:'Steelhead', h:31, back:'#526e7a', side:'#d5dcdf', belly2:'#eef0ef', band:'#e0808f', bandO:.18, adipose:[0.8], fork:.3, seed:13, xp:322,
      patterns:[{ t:'spots', n:55, x:[.1, 1], y:[-1, -0.3], r:1.3, c:'#1a1a1a' }],
      tips:['A rainbow trout that went to sea: bright silver, 50 cm or longer','Faint spots, mostly on the back and tail','Hatchery steelhead are missing this small fin (the adipose fin)'], marks:[[0.5, 0.2], [0.35, -0.7], [0.83, -0.72]] },
    CT:{ name:'Cutthroat trout', h:29, back:'#5d6537', side:'#c8b683', belly2:'#e2d7b3', adipose:[0.8], fork:.25, slash:true, mouth:.14, seed:17,
      patterns:[{ t:'spots', n:150, x:[.06, 1], y:[-1, .8], r:1.35, c:'#1c1c14' }],
      tips:['Red or orange slash under the lower jaw (may be faint)','Large mouth, reaching well past the eye','Black spots all over, more toward the tail'], marks:[[0.075, 0.62], [0.14, 0.15], [0.78, 0.35]] },
    GB:{ name:'Brown trout', h:30, back:'#5c4526', side:'#c7985a', belly2:'#e8cf8e', bellyC:'#f3e2b2', adipose:[0.8], adiC:'#c7743a', fork:.08, tailC:'#7a6038', seed:19,
      patterns:[{ t:'halo', n:60, x:[.08, .95], y:[-.95, .45], r:1.9, c:'#1b140c', c2:'#c4362b', mix:.3, h:'#efe0c0' }],
      tips:['Black and red spots, many with pale halos','Tail with few or no spots','Orange-rimmed adipose fin'], marks:[[0.5, -0.2], [1.12, 0], [0.83, -0.72]] },
    KO:{ name:'Kokanee', h:28, back:'#2d5b86', side:'#cfd8df', belly2:'#eef1f3', adipose:[0.8], fork:.5, anal:[0.64, 0.8, 0.5], seed:23,
      patterns:[], tips:['No black spots on the sides','Dark blue back, bright silver sides','Long fin under the tail end (13 or more rays)'], marks:[[0.45, 0.2], [0.35, -0.75], [0.72, 1.05]] },
    DV:{ name:'Dolly Varden/bull trout', h:27, back:'#4d5a4c', side:'#8f9a87', belly2:'#c9cbbf', adipose:[0.8], fork:.15, whiteEdge:true, lowFin:'#b3695b', seed:29,
      patterns:[{ t:'spots', n:70, x:[.1, 1], y:[-0.9, .6], r:1.8, c:'#f1d7cc', o:.85 }, { t:'spots', n:18, x:[.2, .95], y:[-.2, .5], r:1.4, c:'#e57a74', o:.8 }],
      tips:['Pale pink or cream spots, smaller than the pupil','No worm-like markings on the back fin','White front edges on the lower fins','Bull trout: big, broad, flat head'], marks:[[0.5, -0.4], [0.5, -1.35], [0.5, 1.3], [0.09, -0.55]] },
    LT:{ name:'Lake trout', h:30, back:'#46544d', side:'#7f8c84', belly2:'#c6ccc4', adipose:[0.8], fork:.62, tailLen:62, seed:31,
      patterns:[{ t:'spots', n:170, x:[.06, 1], y:[-1, .7], r:1.9, c:'#e6e3cf', o:.85 }],
      tips:['Pale spots all over a dark body, no red','Worm-like light markings on the back and back fin','Deeply forked tail'], marks:[[0.45, 0.3], [0.4, -0.75], [1.14, -0.5]] },
    EB:{ name:'Brook trout', h:30, back:'#3f4a28', side:'#7c7f47', belly2:'#c98548', bellyC:'#e4974f', adipose:[0.8], fork:.05, whiteEdge:true, lowFin:'#d9683a', tailC:'#5d6034', seed:37,
      patterns:[{ t:'worms', n:70, x:[.08, .95], y:[-1, -0.45], c:'#c9c083' }, { t:'spots', n:40, x:[.15, .95], y:[-.4, .5], r:2, c:'#e8d27a' }, { t:'halo', n:14, x:[.2, .9], y:[-.25, .45], r:1.6, c:'#d8372c', h:'#5c86c9' }],
      tips:['Worm-like markings on the back and back fin','Red spots with blue halos','Orange lower fins with white front edges'], marks:[[0.3, -0.8], [0.55, 0.15], [0.5, 1.3]] },
    GR:{ name:'Arctic grayling', h:26, back:'#4f566b', side:'#a7acb8', belly2:'#dcdde2', adipose:[0.82], fork:.45, sail:true, dorsal:[0.3, 0.6, 1], sailC:'#5a4b6f', sailSpot:'#58a3c9', seed:41,
      patterns:[{ t:'spots', n:14, x:[.18, .4], y:[-.5, .2], r:1.6, c:'#1f1f25' }],
      tips:['Very tall, sail-like back fin with blue-purple spots','A few black spots near the head','Small mouth'], marks:[[0.45, -2.1], [0.3, -0.1], [0.06, 0.25]] },
    LW:{ name:'Whitefish', h:27, back:'#5f6448', side:'#c9ccc2', belly2:'#e9eae4', adipose:[0.8], fork:.5, mouth:.05, snout:.25, seed:43,
      patterns:[{ t:'scales', c:'#3f4636' }],
      tips:['Large scales, silver sides','Small mouth under a blunt snout','Has an adipose fin; no spots'], marks:[[0.5, 0.1], [0.04, 0.3], [0.83, -0.72]] },
    WSG:{ name:'White sturgeon', h:20, back:'#6d7479', side:'#b2b8bb', belly2:'#dde0e1', dorsal:[0.8, 0.88, 0.7], anal:[0.8, 0.87, 0.5], tailShape:'shark', tailLen:60, x0:18, xp:322, snout:.1,
      top:[[0, 0.1], [0.07, -0.55], [0.25, -0.95], [0.5, -1], [0.75, -0.75], [0.9, -0.45]], bot:[[0.9, 0.45], [0.7, 0.7], [0.45, 0.9], [0.2, 0.8], [0.05, 0.45]], barbel:[[0.05, 0.45, 9], [0.07, 0.5, 9]], eyeR:.12, eye:[0.09, -0.3], mouth:.1, mouthY:.62, seed:47,
      patterns:[{ t:'scutes' }], tips:['Rows of bony plates, no scales','Barbels under a pointed snout','Upper lobe of the tail much longer'], marks:[[0.4, -1.05], [0.06, 1.15], [1.2, -1.5]] },
    BB:{ name:'Burbot', h:18, back:'#5d4a2f', side:'#9a8057', belly2:'#cdbb95', dorsal:[0.48, 0.97, 0.35], dorsal2:[0.28, 0.42, 0.9], anal:[0.5, 0.97, 0.3], tailShape:'round', tailLen:34, adipose:false, x0:22, xp:338,
      top:[[0, 0.15], [0.05, -0.6], [0.18, -0.95], [0.45, -1], [0.75, -0.85], [0.92, -0.55]], bot:[[0.92, 0.55], [0.72, 0.85], [0.42, 1], [0.18, 0.95], [0.05, 0.6]], barbel:[[0.06, 0.55, 11]], seed:53,
      patterns:[{ t:'blotch', n:60, x:[.05, 1], y:[-1, .8], r:4, c:'#3a2c18', o:.5 }],
      tips:['One barbel under the chin','Two back fins, the second very long','Long, eel-like mottled body'], marks:[[0.06, 1.5], [0.7, -1.6], [0.6, 0.2]] },
    NP:{ name:'Northern pike', h:21, back:'#3f5130', side:'#78875a', belly2:'#d9dcc4', dorsal:[0.8, 0.9, 0.9], anal:[0.8, 0.9, 0.7], fork:.35, snout:0.02, mouth:.13, mouthY:.1, x0:16, xp:326, lowFin:'#8a7a4a', finO:.9,
      top:[[0, 0.02], [0.08, -0.35], [0.2, -0.75], [0.45, -0.95], [0.7, -0.95], [0.9, -0.6]], bot:[[0.9, 0.6], [0.7, 0.9], [0.45, 0.95], [0.2, 0.75], [0.07, 0.3]], eye:[0.11, -0.35], seed:59,
      patterns:[{ t:'blotch', n:80, x:[.15, 1], y:[-.9, .7], r:3.2, c:'#e7e6b8', o:.75 }],
      tips:['Long, flat, duck-bill snout','Back fin set far back, near the tail','Rows of pale bean-shaped spots'], marks:[[0.04, -0.2], [0.85, -1.9], [0.5, 0.2]] },
    WP:{ name:'Walleye', h:26, back:'#5f5a2e', side:'#b8a55f', belly2:'#e2d9b0', spiny:[0.3, 0.5, 0.9], spinyC:'#6b6337', dorsal:[0.55, 0.74, 0.7], anal:[0.68, 0.8, 0.5], fork:.4, eyeR:.2, eyeC:'#e8e2c4', mouth:.14, seed:61, tailC:'#8a8154',
      patterns:[{ t:'bars', n:5, x:[.3, .85], c:'#3a3417', w:9, o:.45, down:.1 }],
      tips:['Big, glassy eye','Spiny front back fin','White tip on the lower tail lobe','Sharp, fang-like teeth'], marks:[[0.075, -0.6], [0.4, -2.1], [1.18, 0.9], [0.12, 0.45]] },
    YP:{ name:'Yellow perch', h:30, back:'#4c5a23', side:'#d6c24a', belly2:'#efe5a8', spiny:[0.28, 0.5, 0.9], spinyC:'#7b7a3a', dorsal:[0.55, 0.72, 0.7], anal:[0.68, 0.79, 0.5], fork:.35, lowFin:'#e78a2e', finO:.95, seed:67,
      patterns:[{ t:'bars', n:7, x:[.22, .9], c:'#2e3312', w:8, o:.6, down:.6 }],
      tips:['Six to nine dark bars down the sides','Orange lower fins','Spiny front back fin; no fang-like teeth'], marks:[[0.5, 0.1], [0.45, 1.3], [0.38, -2.1]] },
    BCB:{ name:'Black crappie', h:44, back:'#43533f', side:'#b9c0ad', belly2:'#e2e4d8', dorsal:[0.4, 0.7, 0.6], anal:[0.45, 0.75, 0.5], fork:.2, tailH:40, ph:12, mouth:.12, mouthY:-.05, snout:-0.1,
      top:[[0, -0.1], [0.08, -0.55], [0.25, -0.95], [0.45, -1], [0.66, -0.8], [0.86, -0.42]], bot:[[0.86, 0.42], [0.66, 0.85], [0.42, 1], [0.2, 0.85], [0.06, 0.4]], x0:60, xp:300, seed:71,
      patterns:[{ t:'blotch', n:120, x:[.05, 1], y:[-1, 1], r:2.6, c:'#1d2419', o:.7 }],
      tips:['Deep, flat body covered in black speckles','Large, rounded back and bottom fins','Mouth reaches the front of the eye'], marks:[[0.5, 0.2], [0.55, -1.5], [0.12, -0.05]] },
    SMB:{ name:'Smallmouth bass', h:31, back:'#4a4226', side:'#9c8650', belly2:'#dccf9f', spiny:[0.3, 0.52, 0.65], spinyC:'#5f5634', dorsal:[0.52, 0.75, 0.65], anal:[0.66, 0.8, 0.5], fork:.2, eyeC:'#c9432e', mouth:.12, seed:73,
      patterns:[{ t:'bars', n:9, x:[.25, .9], c:'#2c2612', w:6, o:.5, down:.5 }],
      tips:['Bronze-brown with faint vertical bars','Jaw reaches about the middle of the eye','Red or orange eye'], marks:[[0.55, 0.15], [0.12, 0.3], [0.075, -0.55]] },
    LMB:{ name:'Largemouth bass', h:31, back:'#3e5226', side:'#9fb07a', belly2:'#e2e6cd', spiny:[0.3, 0.48, 0.6], spinyC:'#4d5d31', dorsal:[0.53, 0.75, 0.65], anal:[0.66, 0.8, 0.5], fork:.2, mouth:.17, seed:79,
      patterns:[{ t:'stripe', c:'#1f2a12', w:9, o:.7, dash:'12 5' }],
      tips:['Dark stripe along the side','Jaw reaches past the back of the eye','Front and back fins almost separate'], marks:[[0.6, 0.02], [0.17, 0.3], [0.5, -1.5]] },
    GE:{ name:'Goldeye', h:32, back:'#5a6b76', side:'#d9dee0', belly2:'#f0f1ef', dorsal:[0.62, 0.72, 0.6], anal:[0.55, 0.8, 0.45], fork:.55, eyeC:'#e8b93a', eyeR:.2, seed:83,
      patterns:[], tips:['Large golden eye','Deep, flat silver body','Back fin set behind the middle'], marks:[[0.075, -0.6], [0.4, 0.2], [0.67, -1.6]] },
    IN:{ name:'Inconnu', h:28, back:'#5b6a6a', side:'#d0d6d4', belly2:'#eef0ee', adipose:[0.8], fork:.5, mouth:.12, mouthY:.05, snout:.25, seed:89,
      patterns:[{ t:'scales', c:'#3d4a4a' }], tips:['Large mouth with a jutting lower jaw','Big silver scales','Has an adipose fin'], marks:[[0.06, 0.35], [0.5, 0.1], [0.83, -0.72]] },
  };
  SPECIES.MW = { ...SPECIES.LW, name:'Mountain whitefish', h:24, snout:.3, seed:97 };
  return { draw, SPECIES, has: k => !!SPECIES[k] };
})();
const D = JSON.parse(document.getElementById('data').textContent);
const MON = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const DIM = [31,28,31,30,31,30,31,31,30,31,30,31];
const DAYS = []; DIM.forEach((n,m)=>{for(let d=1;d<=n;d++)DAYS.push((m+1)*100+d);});
const RANK = {'-1':{t:'Federal / parks',c:'--r-1'},'0':{t:'This water',c:'--r0'},'1':{t:'From downstream',c:'--r1'},'2':{t:'Named area',c:'--r2'},'3':{t:'Region',c:'--r3'},'4':{t:'Province',c:'--r4'}};
const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const fmtMd = md => MON[Math.floor(md/100)-1] + ' ' + (md%100);
const TODAY = 923;
const cap = s => s ? s.charAt(0).toUpperCase() + s.slice(1) : s;
const clean = s => String(s || '').replace(/\*+/g, '').replace(/\s+/g, ' ').trim();

/* ---------- species ---------- */
const FISH = D.species.fish, GROUPS = D.species.groups; LAD.init(D.species);
const expand = codes => { const out = new Set(); (codes || []).forEach(c => { const g = GROUPS[c]; if (g) (g.members || []).forEach(m => out.add(m)); else out.add(c); }); return [...out]; };
const PROT = D.species.protected || {}, isProt = c => !!PROT[c];
const SALM = D.species.salmon || {};
const spName = c => FISH[c]?.name || GROUPS[c]?.name || PROT[c]?.name || SALM[c]?.name || c;
const lc = s => /^(Dolly|Arctic|Nooksack|Salish|Cultus|Enos|Coastal|Westslope|Misty|Vananda|Paxton|Hadley|Morrison|Rocky|Speckled|Charlotte)/.test(s) ? s : s.charAt(0).toLowerCase() + s.slice(1);
function groupFor(codes){ const s = new Set(codes); if (s.size < 2) return null; for (const [g, v] of Object.entries(GROUPS)){ if (g === 'SA' || v.open) continue; if (v.members.length === s.size && v.members.every(m => s.has(m))) return v.name; } return null; }
const join = (n, conj=' and ') => n.length < 2 ? n.join('') : n.slice(0,-1).join(', ') + conj + n[n.length-1];
const lcNames = (codes, conj, max=Infinity) => { const g = groupFor(codes); if (g) return lc(g); const n = codes.map(c => lc(spName(c))); if (n.length > max) return n.slice(0, max-1).join(', ') + ` + ${n.length-max+1} more`; return join(n, conj); };
const Names = (codes, conj, max) => cap(lcNames(codes, conj, max));
const writtenName = (r, conj) => { const w = r.f.species || []; if (w.length === 1 && w[0] === 'TROUT_CHAR' && (r.f.species_except || []).includes('CHAR')) return 'trout'; return w.length ? join(w.map(c => lc(spName(c))), conj) : 'fish'; };

/* ---------- time ---------- */
const whenDates = w => (w?.dates || []).map(d => [d.from_month*100 + d.from_day, d.to_month*100 + d.to_day]);
const inDates = (wins, md) => !wins.length || wins.some(([a, b]) => a <= b ? md >= a && md <= b : md >= a || md <= b);
const rangeTxt = (a, b) => a === b ? fmtMd(a) : Math.floor(a/100) === Math.floor(b/100) ? `${fmtMd(a)}–${b%100}` : `${fmtMd(a)}–${fmtMd(b)}`;
function timeCond(w){
  if (!w) return '';
  const bits = [];
  if (w.weekdays?.length) bits.push(w.weekdays.map(d => d.slice(0,3)).join(', '));
  if (w.hours){ const t = x => x.at ? x.at : `${x.offset_min ? Math.abs(x.offset_min) + ' min ' + (x.offset_min < 0 ? 'before ' : 'after ') : ''}${x.solar}`; bits.push(`${t(w.hours.start)}–${t(w.hours.end)}`); }
  return bits.join(', ');
}
const winTxt = r => r.wins.length ? ', ' + r.wins.map(([a, b]) => rangeTxt(a, b)).join(', ') : '';

/* ---------- normalise every rule once ---------- */
const RULES = D.rules;
for (const [k, r] of Object.entries(RULES)){
  const f = r.f = r.fields;
  r.key = k; r.baseRank = r.prov.rank;
  r.species = expand(f.species); r.speciesExcept = expand(f.species_except);
  r.openSpecies = (f.species || []).some(c => GROUPS[c]?.open);
  r.wins = whenDates(f.when); r.tcond = timeCond(f.when); r.unparsed = !!f.when?.unparsed;
  // a lift may hold only for some fish (applied per fish), only on some days (applied by date), or only for some
  // anglers/means (unknown angler: shown beside the rule, never removing it)
  r.lifts = (f.exempts || []).map(e => ({ key:`${e.entry_id}::${e.rule_id}`, sp: e.species ? expand(e.species) : null, origin: e.origin || null, wins: whenDates(e.when), qual: e.when_targeting || e.while || e.lengths ? e : null }));
  r.inpart = r.binds === 'sections_in_part'; r.nowhere = r.binds === 'nowhere';
  r.part = f.undrawn_part || null;
  r.k = kindOf(r);
  const par = f.condition_of && RULES[`${r.entry_id}::${f.condition_of}`];
  r.condMethods = par ? (par.fields.gear || []).filter(c => c.slot === 'method').flatMap(c => c.allow || c.only || []).filter(m => m !== 'angling') : [];
  r.notes = [];
  if (r.prov.uncertain) r.notes.push('Uncertain placement: ' + (r.prov.why || 'no reason given'));
  if (f.review_reason) r.notes.push('Curator still to settle: ' + f.review_reason);
}
function kindOf(r){
  const f = r.f;
  if (r.family === 'gear_and_method') return 'gear';
  if (r.family === 'conduct') return 'conduct';
  if (r.family === 'vessel') return 'vessel';
  if (r.type === 'angler_closure') return 'anglerclosure';
  if (r.type === 'stop_fishing_after_quota') return 'duty';
  if (r.dimension === 'lift' || (f.exempts && f.take == null && !f.unlimited && !f.lengths)) return 'exempt';
  if (f.standing) return 'standing';
  if (f.while) return 'while';
  if (f.per_daily) return 'possession';
  if (f.period && f.period !== 'daily') return f.take > 0 ? 'annual' : 'duty';
  const L = f.lengths && f.lengths.length;
  if (f.within) return L ? 'sizecap' : 'subcap';
  if (f.take === 0 && !L) return 'gate';
  if (f.take > 0 || f.unlimited) return 'pool';
  if (L) return 'size';
  return 'duty';
}
const closedGate = r => r.k === 'gate' && r.f.may_target === false;

/* size bands: `lengths` is ordered, first range that holds a length answers */
function bands(r){
  const L = r.f.lengths || []; if (!L.length) return [];
  const pts = new Set([0]); L.forEach(x => { if (x.min_cm != null) pts.add(x.min_cm); if (x.max_cm != null) pts.add(x.max_cm); });
  const P = [...pts].sort((a, b) => a - b); P.push(Infinity);
  const uncovered = r.f.within || (r.f.period && r.f.period !== 'daily') ? null : (r.f.take > 0 || r.f.unlimited ? 0 : null);
  const segs = [];
  for (let i = 0; i < P.length - 1; i++){
    const a = P[i], b = P[i+1], m = b === Infinity ? a + 1 : (a + b) / 2;
    const hit = L.find(x => (x.min_cm == null || m >= x.min_cm) && (x.max_cm == null || m <= x.max_cm));
    const take = hit ? (hit.take ?? r.f.take ?? null) : uncovered;
    const last = segs[segs.length-1];
    if (last && last.take === take) last.b = b; else segs.push({ a, b, take });
  }
  return segs;
}
const bandTxt = (a, b) => a === 0 ? `under ${b} cm` : b === Infinity ? `over ${a} cm` : `${a}–${b} cm`;

function describe(r){
  const f = r.f, sp = writtenName(r), o = f.origin ? f.origin + ' ' : '', w = f.water ? (f.water === 'stream' ? ' from streams' : ' from lakes') : '';
  const t = winTxt(r) + (r.tcond ? ` (${r.tcond})` : '');
  const bs = bands(r);
  switch (r.k){
    case 'gate': return (f.may_target === false ? `No fishing for ${o}${sp.replace(/^all game fish$/, 'any game fish')}` : `Release every ${o}${sp}`) + w + t;
    case 'pool': return (f.unlimited ? `No limit on ${sp}${w}` : `Keep ${f.take} ${o}${sp}${w} a day${r.species.length > 1 ? ', shared' : ''}`) + bs.filter(s => s.take === 0).map(s => `, none ${bandTxt(s.a, s.b)}`).join('') + t;
    case 'subcap': return `Inside the day’s total: at most ${f.take} ${o}${writtenName(r, ' or ')}${w}` + t;
    case 'sizecap': return 'Inside the day’s total: ' + bs.filter(s => s.take != null).map(s => s.take === 0 ? `none ${bandTxt(s.a, s.b)}` : `at most ${s.take} ${bandTxt(s.a, s.b)}`).join(', ') + ` (${sp})` + t;
    case 'size': return `${cap(o + sp)}${w}: ` + bs.filter(s => s.take === 0).map(s => `release any ${bandTxt(s.a, s.b)}`).join(', ') + t;
    case 'annual': { const c = bs.find(s => s.take > 0); return `${f.take} ${o}${sp}${c && c.a > 0 ? ' ' + bandTxt(c.a, c.b) : ''} a ${f.period === 'monthly' ? 'month' : f.period === 'possession' ? 'possession limit' : 'year'}${c && c.a > 0 ? '. Smaller fish don’t count' : ''}`; }
    case 'possession': return `You may possess up to ${f.per_daily}× the daily limit`;
    default: return clean(r.verbatim);
  }
}

/* ---------- a "place" is one ruleset of one water: every section in it carries the same rules ---------- */
const WATERS = D.waters;
const cutWords = (t, n) => t.length <= n ? t : t.slice(0, n).replace(/[\s,;:·–-]+\S*$/, '') + '…';
function partLabel(water, i){
  const p = water.parts[i];
  if (water.parts.length === 1) return 'Whole water';
  const counts = {}; water.parts.forEach(q => q.rules.forEach(([k]) => counts[k] = (counts[k] || 0) + 1));
  const own = p.rules.filter(([k]) => counts[k] < water.parts.length && RULES[k].family !== 'information' && (RULES[k].prov.rank >= 0 && RULES[k].prov.rank <= 2 || (RULES[k].prov.rank === -1 && (RULES[k].f.species || []).includes('ALL_GAME_FISH')))).map(([k]) => RULES[k]);
  if (!own.length) return 'The rest of the water';
  // the most distinctive rule names the part (fewest other parts share it; a full closure first among equals)
  // a place name first: readers pick a stretch by where they stand, not by its rule
  const score = r => counts[r.key] * 10 - (r.k === 'gate' && r.f.species?.includes('ALL_GAME_FISH') ? 5 : 0) - (r.dimension === 'lift' ? -3 : 0) - (r.parts?.where ? 8 : 0);
  own.sort((x, y) => score(x) - score(y));
  const pick = own[0], pp = pick.parts || {};
  const anyWhere = own.find(r => r.parts?.where);
  const base = cutWords(clean(pp.where && pp.what ? `${cap(pp.where)}: ${pp.what.toLowerCase()}${pick.wins.length && !/\d/.test(pp.what) ? winTxt(pick) : ''}` : anyWhere ? `${cap(anyWhere.parts.where)}: ${clean(pp.what || pick.label).toLowerCase()}` : `Stretch with: ${clean(pick.verbatim).replace(/^No Fishing/, 'no fishing')}`), 130);
  // then say what else sets it apart: the other rule's place if it is a different place, else what it says
  const key = w => clean(w).toLowerCase().replace(/^(from|between|within|in) /, '');
  const seen = new Set([key(pp.where || '')]);
  const extra = [];
  own.slice(1).forEach(r => { const w = clean(r.parts?.where || ''), t = w && !seen.has(key(w)) ? w.replace(/^within /, '') : clean(r.parts?.what || r.label).toLowerCase(); if (!extra.includes(t)) extra.push(t); seen.add(key(w)); });
  return base + (extra.length ? ` · also ${cutWords(extra[0], 70)}${extra.length > 1 ? ` (+${extra.length - 1} more)` : ''}` : '');
}
// where a water has several regulation entries (Thompson "upstream of Kamloops Lake" / "downstream of …"; Fraser by region),
// the entry a part comes from is its best place name: the export carries it as the entry's scope note or heading
function partPlace(water, i){
  const ents = id => [...new Set(water.parts[id].rules.map(([k]) => RULES[k]).filter(r => r && (r.prov?.rank ?? 4) === 0 && D.entries[r.entry_id]?.kind === 'water').map(r => r.entry_id))];
  const all = new Set(water.parts.flatMap((_, j) => ents(j))); if (all.size < 2) return '';
  const say = e => { const E = D.entries[e] || {}; const sc = (E.scope_note || (E.full_name || '').match(/\(([^)]+)\)/)?.[1] || '').replace(/\.$/, '');
    const reg = (e.match(/^r(\d+[ab]?)/) || [])[1]; return sc && sc.split(' ').length > 1 ? cap(sc) : reg ? `In Region ${reg.toUpperCase()}` : ''; };
  const regs = new Set([...all].map(e => (e.match(/^r(\d+[ab]?)/) || [])[1]));
  const mine = [...new Set(ents(i).map(say).filter(Boolean).filter(t => !(regs.size < 2 && /^In Region /.test(t))))];
  if (mine.length > 1 && mine.every(t => /^In Region /.test(t))) return `Where ${mine.map(t => t.replace(/^In /, '')).join(' and ')} meet`;
  return mine.join(' / ');
}
// where a part runs, said from the export's runs: "From the CNR bridge to the boundary signs"
function endName(e, side){
  if (!e) return '';
  if (e === 'mouth') return 'the mouth'; if (e === 'source') return side === 'from' ? 'the top of the river' : 'the source'; if (e === 'bc_border') return 'the B.C. border';
  const sp = D.splits?.[e]; if (sp){ const dup = sp.water_id && sp.km != null && Object.entries(D.splits).some(([k, o]) => o !== sp && k !== sp.same_place_as && o.same_place_as !== e && o.water_id === sp.water_id && o.name.toLowerCase() === sp.name.toLowerCase());
    const nm0 = sp.name.replace(/\bu\/s\b/g, 'upstream').replace(/\bd\/s\b/g, 'downstream');   // the export's display name (re-cased, unique per water); never the id
    return (/^(area|region_line):/.test(e) || /^\d|^[A-Z][\w.]* ?[\w.]*'s /.test(nm0) ? '' : 'the ') + nm0.replace(/^the /i, '') + (dup ? ` (km ${Math.round(sp.km)})` : ''); }
  const i = e.indexOf(':'), k = i < 0 ? e : e.slice(0, i), id = i < 0 ? '' : e.slice(i + 1); const nm = id ? D.wnames?.[id] : '';
  if (k === 'confluence') return nm ? `the ${nm} confluence` : 'a tributary confluence';
  if (k === 'lake_inlet') return nm ? nm : 'a lake';
  if (k === 'lake_outlet') return nm ? `the ${nm} outlet` : 'a lake outlet';
  if (k === 'area') return id; if (k === 'region_line') return `the Region ${id} line`;
  return e.replace(/_/g, ' ');
}
function runTxt1(r){
  if (r.lakes) return runTxt1({ ...r, lakes:0 }) + ` (through ${r.lakes} lake${r.lakes > 1 ? 's' : ''})`;
  if (r.polygon) return r.polygon === 'whole' ? '' : r.polygon;
  if (r.from && r.from === r.to && /^area:/.test(r.from)) return `Within ${endName(r.from).replace(/ boundary$/, '')}`;
  let a = endName(r.from, 'from'), b = endName(r.to, 'to');
  // two different cuts can carry the same short name (two "CNR bridges"): tell them apart by distance from the mouth
  // said from the mouth upward, the way an angler walks a river from the road/bridge at the bottom
  const t = `From ${b} up to ${a}`;
  return r.branch ? `Side channel (${t.charAt(0).toLowerCase() + t.slice(1)})` : t;
}
function runsLabel(part){
  const rs = (part.runs || []).filter(r => !r.polygon || r.polygon !== 'whole');
  if (!rs.length) return '';
  const main0 = rs.filter(r => !r.branch).sort((a, b) => (b.km_from ?? 0) - (a.km_from ?? 0)), side = rs.filter(r => r.branch);   // joined upstream→down, said down→up below
  // a river that only passes through lakes is one stretch: join runs that end at a lake inlet and restart at its outlet
  const main = [];
  for (const r of main0){ const last = main[main.length - 1];
    if (last && /^lake_inlet/.test(last.to || '') && /^lake_outlet/.test(r.from || '')){ last.to = r.to; last.km_to = r.km_to; last.lakes = (last.lakes || 0) + 1; }
    else main.push({ ...r }); }
  // a river through small lakes reads as many runs: keep the long ones, say the rest exist
  const bits = main.slice().reverse().map(runTxt1).filter(Boolean);
  const low = x => x.charAt(0).toLowerCase() + x.slice(1);
  let t = !bits.length ? '' : bits.length === 1 ? bits[0] : bits.length === 2 ? `Two stretches: ${low(bits[0])}, and ${low(bits[1])}` : `${bits.length} stretches, including ${low(bits[0])}`;
  if (!t && side.length) t = side.length === 1 ? runTxt1(side[0]) : `${side.length} side channels`;
  else if (side.length) t += ` and ${side.length} side channel${side.length > 1 ? 's' : ''}`;
  return t;
}
const partKm = p => { const km = rs => rs.map(r => r.km_from).filter(v => v != null), m = km((p.runs || []).filter(r => !r.branch)), b = km((p.runs || []).filter(r => r.branch));
  return m.length ? Math.max(...m) : b.length ? Math.max(...b) : 1e9; };   // side channels sort where they join; a part with no measure goes last
// two parts can still read the same: add what the second has that the first doesn't
function partLabels(water){
  if (water._labels) return water._labels;
  // a part is named by where it runs (export runs); what sets it apart follows as a hint
  const L = water.parts.map((_, i) => { const pl = partPlace(water, i), l = partLabel(water, i), rl = runsLabel(water.parts[i]);
    if (rl){ // where it runs is the name; what sets it apart is added only if two parts run alike
      const same = water.parts.some((q, j) => j !== i && runsLabel(q) === rl);
      const hint = !same || /^(The rest of the water|Whole water)/.test(l) ? '' : cutWords(l.replace(/^Stretch with: /, '').replace(/^[^:·]{3,90}: /, '').split(' · also ')[0], 60);
      return (pl ? pl + ' · ' : '') + rl + (hint ? ` — ${hint}` : ''); }
    if (!pl) return l;
    return /^(Stretch with: |The rest of the water)/.test(l) ? `${pl}: ${l.replace(/^Stretch with: /, '').replace(/^The rest of the water/, 'the rest')}` : `${pl} · ${l}`; });
  const core = l => l.replace(/ \(\+\d+ more\)$/, '');
  L.forEach((l, i) => { const j = L.findIndex((m, k) => k < i && core(m) === core(l)); if (j < 0) return;
    const mine = new Set(water.parts[i].rules.map(([k]) => k)), theirs = new Set(water.parts[j].rules.map(([k]) => k));
    const diff = [...mine].filter(k => !theirs.has(k)).map(k => RULES[k]), less = [...theirs].filter(k => !mine.has(k)).map(k => RULES[k]);
    const say = r => clean(r.parts?.what || r.label).toLowerCase();
    L[i] = core(l) + (diff.length ? ` · plus ${cutWords(say(diff[0]), 48)}` : less.length ? ` · without ${cutWords(say(less[0]), 48)}` : ' · (same rules, a separate stretch)'); });
  return (water._labels = L);
}
// stretches closed to all fishing on every day of the year read the same to an angler: show them as one choice.
// (The export has no adjacency between stretches, so this merges every such stretch of the water, neighbours or not.)
function closedAllYear(wi, pi){ const pl = makePlace(wi, pi); return DAYS.every(md => settle(pl, md).active.some(isBroad)); }
function partGroups(water){
  if (water._groups) return water._groups;
  const wi = WATERS.indexOf(water), shut = [], open = [];
  water.parts.forEach((p, i) => (water.parts.length > 1 && closedAllYear(wi, i) ? shut : open).push(i));
  const G = open.map(i => ({ idx:[i], closed:false, sections:water.parts[i].sections }));
  if (shut.length) G.push({ idx:shut, closed:true, sections:shut.reduce((n, i) => n + water.parts[i].sections, 0) });
  // keep the water's own order (biggest part first)
  G.sort((a, b) => a.idx[0] - b.idx[0]);
  return (water._groups = G);
}
const groupOf = (water, pi) => partGroups(water).find(g => g.idx.includes(pi));
function makePlace(wi, pi){
  const water = WATERS[wi], p = water.parts[pi];
  const cands = p.rules.map(([k, via]) => { const c = Object.create(RULES[k]); c.via = via; c.rank = via === 'trib' && RULES[k].baseRank >= 0 ? 1 : RULES[k].baseRank; return c; });
  return { water, part:p, name:water.name, kind:water.kind, cands, _settle:{}, _eval:{} };
}
function applies(r, w){ return !(r.f.water && r.f.water !== w.kind); }
function settle(w, md){
  if (w._settle[md]) return w._settle[md];
  const here = w.cands.filter(r => applies(r, w));
  const dated = here.filter(r => inDates(r.wins, md) && !r.unparsed);
  const timed = dated.filter(r => r.tcond);
  const whole = dated.filter(r => !r.tcond);
  const lifted = new Map(), liftNotes = new Map(), spLift = new Map();
  // a rule placed on the water but holding only in an undrawn part never decides the water: it is shown as a note
  // a rule on one half of the channel stands beside the rest; the other half follows the water's other rules
  const beside = whole.filter(r => r.f.side);
  const decides = whole.filter(r => !r.inpart && !r.nowhere && !r.f.side);
  for (const r of decides) for (const L of r.lifts){
    if (L.key === r.key || !inDates(L.wins, md)) continue;
    if (L.qual) (liftNotes.get(L.key) || liftNotes.set(L.key, []).get(L.key)).push({ by:r, q:L.qual });
    else if (L.sp || L.origin) (spLift.get(L.key) || spLift.set(L.key, []).get(L.key)).push({ sp:L.sp, origin:L.origin, by:r.key });
    else lifted.set(L.key, r.key);
  }
  const active = decides.filter(r => !lifted.has(r.key) && r.k !== 'standing');
  const inpart = here.filter(r => r.inpart);
  return (w._settle[md] = { w, md, active, lifted, liftNotes, spLift, timed:timed.filter(r => !r.inpart), here, inpart, beside });
}

/* ---------- settling one species (the ladder: smaller rank speaks; closures only lift by exempts) ---------- */
const covers = (r, S) => r.species.includes(S) && !r.speciesExcept.includes(S);
const spec = r => (r.f.origin ? 1 : 0) + (r.f.water ? 1 : 0) + (r.wins.length ? 1 : 0);
const isHard = r => closedGate(r) || (r.k === 'gate' && r.rank === -1);
const tk = r => r.k === 'gate' ? -1 : (r.f.unlimited ? 1e9 : r.f.take);
const cmp = (a, b) => a.rank - b.rank || spec(b) - spec(a) || a.species.length - b.species.length || tk(a) - tk(b);
const KEEPISH = s => s === 'keep' || s === 'nolimit';
const isBroad = r => isHard(r) && (r.f.species || []).includes('ALL_GAME_FISH');

function sizeLines(r, poolTake, into){
  bands(r).forEach(s => {
    if (s.take === 0) into.push({ t:'rel', a:s.a, b:s.b, r });
    else if (s.take > 0 && (r.f.within || s.take < poolTake)) into.push({ t:'cap', a:s.a, b:s.b, take:s.take, r });
  });
}
/* The ladder is decided by ladder.js (a port of the reference, checked against every case in guide.cases);
   here we read its answer for one fish and one origin and turn the rules that speak into a number and lines. */
function ladderAt(ctx, S, o){
  const w = ctx.w;
  // the answer changes only where some rule's or lift's dates start or end: cache per stretch of days between those
  if (!w._bp){ const b = new Set([101]); const add = W => W.forEach(([a, z]) => { b.add(a); b.add(DAYS[(DAYS.indexOf(z) + 1) % 365]); });
    w.cands.forEach(r => { add(r.wins); (r.f.exempts || []).forEach(e => add(whenDates(e.when))); }); w._bp = [...b].sort((x, y) => x - y); }
  let seg = 0; for (let i = 0; i < w._bp.length; i++) if (w._bp[i] <= ctx.md) seg = i;
  const ck = 'L' + seg + S + o;
  if (!w._eval[ck]) w._eval[ck] = LAD.effective(w.cands.map(r => ({ r, via:r.via })), w.kind, ctx.md, S, { steelheadWater: !!w.part.anadromous_rainbow, origin:o });
  return w._eval[ck];
}
function evalSp(ctx, S, o){
  const ck = ctx.md + S + o, w = ctx.w;
  if (w._eval[ck] !== undefined) return w._eval[ck];
  const eff = ladderAt(ctx, S, o), st = r => eff.get(r.key);
  const roles = new Map(); const set = (r, role, by) => { if (!roles.has(r.key)) roles.set(r.key, { role, by }); };
  const inScope = r => (r.type === 'retention_limit' || r.k === 'duty') && !['while','exempt','standing'].includes(r.k) && r.dimension !== 'lift' && (!r.f.origin || r.f.origin === o);
  for (const r of w.cands){ const x = st(r); if (!x || !inScope(r)) continue; if (x.state === 'displaced') set(r, 'replaced', x.by); else if (x.state === 'lifted') set(r, 'lifted', x.by); }
  const A = w.cands.filter(r => inScope(r) && st(r)?.state === 'speaks');
  const partly = A.filter(r => st(r).partly);
  const steelhead = S === 'RB' && w.part.anadromous_rainbow;
  const pools = A.filter(r => r.k === 'pool');
  const closed = A.filter(closedGate).sort((a, b) => a.rank - b.rank), rel = A.filter(r => r.k === 'gate' && !closedGate(r)).sort((a, b) => a.rank - b.rank);
  const byTake = pools.slice().sort((a, b) => (a.f.unlimited ? 1e9 : a.f.take) - (b.f.unlimited ? 1e9 : b.f.take) || a.rank - b.rank);
  const win = closed[0] || rel[0] || byTake[0];
  if (!win) return (w._eval[ck] = null);
  const status = win.k === 'gate' ? (win.f.may_target === false ? 'closed' : 'release') : (win.f.unlimited ? 'nolimit' : 'keep');
  set(win, 'governs');
  [...closed, ...rel].forEach(r => r !== win && set(r, 'agrees', win.key));
  // a bigger total this fish also counts toward: a group total, or the zone's day total beside the water's own number
  const outers = win.k === 'pool' ? pools.filter(p => p !== win && ((p.species.length > win.species.length && win.species.every(x => p.species.includes(x))) || (p.rank >= 2 && win.rank <= 1 && (p.f.unlimited || p.f.take >= win.f.take)))) : [];
  outers.forEach(O => set(O, 'contains'));
  const also = win.k === 'pool' ? byTake.filter(p => p !== win && !outers.includes(p)) : [];
  also.forEach(p => set(p, 'also'));
  if (win.k === 'gate') pools.forEach(p => set(p, 'moot', win.key));
  const sideOk = () => true;
  const res = { S, o, status, win, daily:null, lines:[], roles, liftNotes: ctx.liftNotes.get(win.key) || [] };
  if (KEEPISH(status)){
    const P = win; let daily = P.f.unlimited ? Infinity : P.f.take;
    const chain = root => { const ids = new Set([root.rule_id]); const cl = []; let g = true; while (g){ g = false; for (const r of A) if (r.f.within && r.entry_id === root.entry_id && ids.has(r.f.within) && !cl.includes(r)){ cl.push(r); ids.add(r.rule_id); g = true; } } return cl; };
    const cl = chain(P);
    for (const r of cl.filter(r => r.k === 'subcap')){
      if (P.species.every(x => r.species.includes(x))){ daily = Math.min(daily, r.f.take); set(r, 'narrows'); res.narrow = r; }
      else { set(r, 'limit'); res.lines.push({ t:'subcap', r }); }
    }
    // size clauses inside the day: the narrowest species set speaks for its fish (a carve-out)
    const sc = cl.filter(r => r.k === 'sizecap').sort((a, b) => a.species.length - b.species.length);
    const seen = [];
    for (const r of sc){ const key = JSON.stringify(r.f.lengths); const beat = seen.find(x => x.key === key); if (beat){ set(r, 'replaced', beat.r.key); continue; } seen.push({ key, r }); set(r, 'limit'); const tmp = []; sizeLines(r, daily, tmp); tmp.forEach(l => { if (sc.length > 1 && r.species.length < Math.max(...sc.map(x => x.species.length))) l.carve = true; res.lines.push(l); }); }
    A.filter(r => r.k === 'size' && sideOk(r)).forEach(r => { set(r, 'floor'); sizeLines(r, daily, res.lines); });
    also.forEach(p => {
      const big = p.f.unlimited || p.f.take > daily;
      if (big && p.rank >= 2 && P.rank <= 1) res.lines.push({ t:'outer', r:p });          // the zone's day total still counts these fish
      else res.lines.push({ t:'also', r:p, capped: big });                                   // a water's own number, capped by the total
    });
    if (steelhead) res.lines.push({ t:'steel', r:P });
    sizeLines(P, daily, res.lines);
    if (steelhead) res.lines = res.lines.filter(l => !(l.t === 'cap' || l.t === 'rel') || l.a < 50);
    for (const O of outers){
      res.lines.push({ t:'outer', r:O });
      chain(O).forEach(r => { if (r.k === 'subcap' && !O.species.every(x => r.species.includes(x))){ set(r, 'limit'); res.lines.push({ t:'outercap', r, outer:O }); } else if (r.k === 'sizecap'){ set(r, 'limit'); const tmp = []; sizeLines(r, O.f.take, tmp); tmp.forEach(l => res.lines.push({ ...l, t: l.t === 'cap' ? 'outersize' : l.t, outer:O })); } else if (r.k === 'subcap'){ set(r, 'narrows'); if (r.f.take < daily) daily = r.f.take; } });
    }
    const inChain = new Set([P, ...outers].flatMap(x => [x, ...chain(x)]));
    // a clause whose own total was replaced still binds, unless the day's number here is already that small
    A.filter(r => (r.k === 'subcap' || r.k === 'sizecap') && !inChain.has(r) && !(r.f.take >= daily)).forEach(r => { set(r, 'limit'); if (r.k === 'subcap'){ if (r.f.take < daily && P.species.every(x => r.species.includes(x))) daily = r.f.take; else res.lines.push({ t:'orphan', r }); } else sizeLines(r, daily, res.lines); });
    partly.forEach(r => res.lines.push({ t:'partly', r }));
    A.forEach(r => (r.f.exempts || []).filter(e => e.caution).slice(0, 1).forEach(e => res.lines.push({ t:'caution', r, says:e.caution.says })));
    if ((P.f.species || []).includes('TROUT_CHAR')) res.lines.push({ t:'tnote', r:P, only:(P.f.species_except || []).includes('CHAR') });
    A.filter(r => r.k === 'annual' && sideOk(r)).forEach(r => { set(r, 'season'); res.lines.push({ t:'annual', r }); });
    A.filter(r => r.k === 'duty').forEach(r => { set(r, 'duty'); res.lines.push({ t:'duty', r }); });
    if (A.some(r => r.f.record_retention)) A.filter(r => r.f.record_retention).forEach(r => res.lines.push({ t:'record', r }));
    if (S === 'ST' && w.part.anadromous_rainbow) res.lines = res.lines.filter(l => !(l.t === 'rel' && l.a === 0 && l.b <= 50));
    const mins = res.lines.filter(l => l.t === 'rel' && l.a === 0);
    if (mins.length > 1){ const top = Math.max(...mins.map(l => l.b)); res.lines = res.lines.filter(l => !(l.t === 'rel' && l.a === 0 && l.b < top)); }
    res.daily = daily;
  }
  for (const r of A){
    if (roles.has(r.key)) continue;
    if (r.k === 'possession') set(r, 'possession');
    else if (r.f.within){ const p = RULES[r.entry_id + '::' + r.f.within]; set(r, 'falls', p ? p.key : null); }
    else if (['size','subcap','sizecap','annual','duty'].includes(r.k)) set(r, 'moot', win.key);
  }
  return (w._eval[ck] = res);
}

/* ---------- rows ---------- */
function speciesAt(w){
  const s = new Set();
  w.cands.forEach(r => { if (applies(r, w) && r.type === 'retention_limit' && ['gate','pool','subcap','sizecap','size','annual'].includes(r.k) && !(r.f.species || []).includes('ALL_GAME_FISH')) r.species.forEach(x => { if (!isProt(x)) s.add(x); }); });
  return [...s];
}
const mainRes = (H, W) => (H && KEEPISH(H.status)) ? H : (W && KEEPISH(W.status)) ? W : (H || W);
const lineKey = res => res ? res.status + res.win.key + res.daily + res.lines.map(l => l.t + l.r.key + (l.a ?? '')).join() : '-';
const factKey = l => l.t === 'origin2' ? `o2${l.o}${l.daily}${l.min}${l.max}` : l.t === 'tnote' ? 'tnote' + l.only : l.t === 'caution' ? 'caution' + l.says : l.t === 'rel' ? `rel${l.a}-${l.b}` : l.t === 'annual' ? `yr${l.r.f.take}|${JSON.stringify(l.r.f.lengths || '')}|${l.r.f.origin || ''}` : l.t === 'cap' ? `cap${l.a}-${l.b}-${l.take}-${l.r.key}` : l.t + l.r.key;
function buildModel(ctx){
  // steelhead appear only where the export says they may be (part.steelhead known | possible) AND a rule names them:
  // a trout-and-char quota reaching a lake or an interior river never makes a steelhead row; a water known only
  // by the curated list carries no steelhead rule, so it gets a presence line instead (card)
  const w = ctx.w, steelNamed = w.cands.some(r => (r.f.species || []).includes('ST')),
    spp = speciesAt(w).filter(S => S !== 'ST' || (w.part.steelhead && steelNamed && w.part.steelhead_rules !== false));
  const R = {}; spp.forEach(S => { R[S] = { H: evalSp(ctx, S, 'hatchery'), W: evalSp(ctx, S, 'wild') }; });
  const broad = ctx.active.filter(isBroad).sort((a, b) => a.rank - b.rank);
  const keepRows = new Map(), rest = new Map();
  for (const S of spp){ const m = mainRes(R[S].H, R[S].W); if (m && KEEPISH(m.status)){ const k = m.win.key; if (!keepRows.has(k)) keepRows.set(k, { kind:m.status, pool:m.win, members:[], exc:[], xref:[] }); keepRows.get(k).members.push(S); } }
  for (const S of spp){
    const m = mainRes(R[S].H, R[S].W); if (!m) continue;
    if (KEEPISH(m.status)){ for (const [k, v] of m.roles) if ((v.role === 'replaced' || v.role === 'contains') && keepRows.has(k) && k !== m.win.key) keepRows.get(k).xref.push({ S, by:m.win, daily:m.daily }); continue; }
    let parent = null; for (const want of ['moot', 'replaced']) if (!parent) for (const [k, v] of m.roles) if (v.role === want && keepRows.has(k)){ parent = k; break; }
    if (parent) keepRows.get(parent).exc.push({ S, res:m });
    else { const k = m.status + '|' + m.win.key; if (!rest.has(k)) rest.set(k, { kind:m.status, win:m.win, members:[] }); rest.get(k).members.push(S); }
  }
  const rows = [];
  for (const row of keepRows.values()){
    row.members.sort((a, b) => spName(a).localeCompare(spName(b)));
    const M = row.members, facts = new Map();
    const add = (l, S) => { const k = factKey(l); if (!facts.has(k)) facts.set(k, { ...l, rules:[], members:new Set() }); const f = facts.get(k); f.members.add(S); if (!f.rules.includes(l.r)) f.rules.push(l.r); };
    let narrow = null;
    for (const S of M){
      const { H, W } = R[S]; const m = mainRes(H, W);
      if (m.narrow) narrow = m.narrow;
      if (H && W && lineKey(H) !== lineKey(W)){ const other = m === H ? W : H;
        if (!KEEPISH(other.status)) add({ t:'origin', o:other.o, keepO:m.o, r:other.win, status:other.status }, S);
        // both origins may be kept, but under different limits: say what the other origin gets
        else if (other.daily !== m.daily || other.win !== m.win){ const mn = other.lines.filter(l => l.t === 'rel' && l.a === 0).map(l => l.b), mx = other.lines.filter(l => l.t === 'rel' && l.b === Infinity).map(l => l.a);
          add({ t:'origin2', o:other.o, daily:other.daily, min: mn.length ? Math.max(...mn) : null, max: mx.length ? Math.min(...mx) : null, r:other.narrow || other.win }, S); } }
      m.lines.forEach(l => add(l, S));
    }
    row.exc.forEach(e => add({ t:'exc', status:e.res.status, r:e.res.win }, e.S));
    row.xref.forEach(x => add({ t:'xref', r:x.by, daily:x.daily }, x.S));
    const list = [...facts.values()];
    list.filter(l => l.t === 'cap').forEach(l => { const c = list.find(o => o !== l && o.t === 'cap' && o.a === l.a && o.b === l.b && o.members.size < l.members.size); if (c){ l.general = true; c.carveOf = l; } });
    const ORDER = ['origin','origin2','caution','steel','exc','xref','rel','cap','also','subcap','outer','outercap','outersize','orphan','partly','annual','record','duty','tnote'];
    list.sort((a, b) => ORDER.indexOf(a.t) - ORDER.indexOf(b.t));
    const everyone = [], groups = new Map();
    for (const l of list){
      if (l.general || (l.members.size === M.length && l.t !== 'exc' && l.t !== 'xref')){ everyone.push(l); continue; }
      const key = [...l.members].sort().join(',');
      if (!groups.has(key)) groups.set(key, { members:[...l.members], facts:[] });
      groups.get(key).facts.push(l);
    }
    const first = mainRes(R[M[0]].H, R[M[0]].W);
    rows.push({ kind:row.kind, pool:row.pool, members:M, daily:first.daily, narrow, everyone, groups:[...groups.values()].sort((a, b) => a.members.length - b.members.length), allMembers:[...M, ...row.exc.map(e => e.S)], liftNotes:first.liftNotes });
  }
  rows.sort((a, b) => b.members.length - a.members.length || a.pool.rank - b.pool.rank);
  // closures and releases of an open subject (protected species): no fish list, so a row of their own
  ctx.active.filter(r => r.type === 'retention_limit' && r.k === 'gate' && (r.f.species || []).length && r.f.species.every(c => GROUPS[c]?.open && c !== 'ALL_FIN_FISH' || isProt(c)) && !r.f.while && applies(r, w))
    .sort((a, b) => a.rank - b.rank).forEach(r => { const pr = r.f.species.every(isProt), k = 'open|' + (pr ? 'PROT' : r.f.species.join()) + r.f.may_target;
      // the province's and a region's protected lists are one row: every fish either names
      if (!rest.has(k)) rest.set(k, { kind: r.f.may_target === false ? 'closed' : 'release', win:r, members:[], prot: pr ? [] : null, wins:[] });
      const x = rest.get(k); x.wins.push(r); if (pr) r.f.species.forEach(c => { if (!x.prot.includes(c)) x.prot.push(c); }); });
  [...rest.values()].sort((a, b) => (a.kind === 'closed') - (b.kind === 'closed') || a.win.rank - b.win.rank)
    .forEach(r => rows.push({ kind:r.kind, win:r.win, members:r.members.sort((a, b) => spName(a).localeCompare(spName(b))), allMembers:r.members, everyone:[], groups:[], prot: r.prot ? r.prot.sort((a, b) => spName(a).localeCompare(spName(b))) : null, wins: r.wins, liftNotes:ctx.liftNotes.get(r.win.key) || [] }));
  // list order: trout and char, then salmon (kokanee), whitefish and grayling, other fish, crayfish, then open groups (protected species)
  const SPRANK = S => { const f = FISH[S]?.family; return f === 'TROUT' || f === 'CHAR' ? 0 : S === 'KO' ? 1 : f === 'WHITEFISH' || S === 'GR' || S === 'IN' ? 2 : S === 'CRA' ? 4 : 3; };
  const rowRank = r => r.members.length ? Math.min(...r.members.map(SPRANK)) : 5;
  rows.forEach((r, i) => r._i = i); rows.sort((a, b) => rowRank(a) - rowRank(b) || a._i - b._i);
  return { R, rows, broad, spp };
}

/* ---------- through the year ---------- */
const RANKSTAT = { keep:3, nolimit:3, release:2, closed:1 };
function strip(w, members){
  const segs = []; let cur = null, len = 0;
  for (const md of DAYS){ const ctx = settle(w, md); let best = 'none', bv = 0;
    for (const S of members){ const m = mainRes(evalSp(ctx, S, 'hatchery'), evalSp(ctx, S, 'wild')); if (m && RANKSTAT[m.status] > bv){ bv = RANKSTAT[m.status]; best = m.status === 'nolimit' ? 'keep' : m.status; } }
    if (best === cur) len++; else { if (cur) segs.push([cur, len]); cur = best; len = 1; } }
  segs.push([cur, len]); return segs;
}
function segBar(segs, md, cls, label){
  const idx = DAYS.indexOf(md);
  return `<div class="sbar ${cls}">${segs.map(([s, n]) => `<i class="s-${s}${n <= 14 ? ' short' : ''}" style="flex:${n} 0 0"></i>`).join('')}<span class="today" style="left:calc(${(idx/365*100).toFixed(2)}% - 1px)">${label ? `<em>${label}</em>` : ''}</span></div>`;
}
function shortWindows(segs){ let d = 0; const out = []; segs.forEach(([s, n]) => { if (s === 'keep' && n <= 14 && segs.length > 1) out.push(rangeTxt(DAYS[d], DAYS[d + n - 1])); d += n; }); return out.length ? `<div class="swin">Keeping allowed only ${out.join(' and ')}</div>` : ''; }
const MONTHS_ROW = `<div class="smonths">${MON.map(m => `<span>${m[0]}</span>`).join('')}</div>`;
function stripHtml(w, members, md){
  const segs = strip(w, members), has = new Set(segs.map(s => s[0]));
  const lab = { keep:'can keep', release:'release only', closed:'closed', none:'no rule' };
  const rt = typeof runTxt === 'function' ? runTxt(segs, md) : '';
  return `<div class="strip"><div class="lbl">Through the year</div>${rt ? `<p class="swin">${esc(rt)}</p>` : ''}${segBar(segs, md, '')}${shortWindows(segs)}${MONTHS_ROW}<div class="slegend">${[...has].map(s => `<span><i class="s-${s}"></i>${lab[s]}</span>`).join('')}</div></div>`;
}
// only the fish that can be kept here belong in an "only N can be …" line
const keepNames = (codes, r) => { const k = codes.filter(S => KEEPISH(mainRes(MODEL.R[S]?.H, MODEL.R[S]?.W)?.status)); return k.length && k.length < codes.length ? lcNames(k, ' or ') : r ? writtenName(r, ' or ') : lcNames(codes, ' or '); };
/* ================= shared UI ================= */
const state = { wi:0, pi:0, lpi:0, md:TODAY, open:new Set(), dd:new Set(), all:false, tab:'card', kit:'licence', who:{ residency:'resident', age:'16_plus', guidance:'non_guided', status:'none' } };
let PLACE = null, MODEL = null, REOPEN = null;
function rowTitle(row){
  if (row.pool){ const w = row.pool.f.species || []; if (w.length === 1 && GROUPS[w[0]]) return w[0] === 'TROUT_CHAR' && (row.pool.f.species_except || []).includes('CHAR') ? 'Trout' : GROUPS[w[0]].name; return Names(row.members, ' and '); }
  if (row.prot) return 'Protected species';
  if (!row.members.length && row.win) return cap(spName((row.win.f.species || [])[0]).toLowerCase());
  return Names(row.members, ' and ');
}
function nextOpen(w, md){ const i = DAYS.indexOf(md); for (let k = 1; k < 366; k++){ const d = DAYS[(i + k) % 365]; if (!settle(w, d).active.some(isBroad)) return d; } return null; }
const prevDay = md => DAYS[(DAYS.indexOf(md) + 364) % 365];
function waterSegs(w){ if (w._wsegs) return w._wsegs; const segs = []; let cur = null, len = 0;
  for (const d of DAYS){ const s = settle(w, d).active.some(isBroad) ? 'closed' : 'keep'; if (s === cur) len++; else { if (cur) segs.push([cur, len]); cur = s; len = 1; } }
  segs.push([cur, len]); return (w._wsegs = segs); }
function waterStrip(w, md){
  if (w.water?.tidal) return `<div class="banner"><div class="big">Tidal water</div><p>${esc(w.water.tidal.guide || 'Federal tidal regulations apply.')}</p></div>` + waterStrip0(w, md);
  return waterStrip0(w, md);
}
function waterStrip0(w, md){
  const segs = waterSegs(w), openNow = !settle(w, md).active.some(isBroad), has = new Set(segs.map(s => s[0]));
  const partShut = openNow && (settle(w, md).inpart || []).some(r => inDates(r.wins, md) && r.family === 'retention');
  return `<div class="wstrip"><div class="wsline"><span class="wsstat ${openNow ? 'open' : 'closed'}">${openNow ? (partShut ? 'Open, except some parts' : 'Open') : 'Closed'}</span><span class="muted small">${md === TODAY ? 'today' : 'on ' + fmtMd(md)}</span>${(() => { if (!openNow){ const n = nextOpen(w, md); return n ? `<span class="wsoon open">Opens ${fmtMd(n)}</span>` : ''; } const i = DAYS.indexOf(md); for (let k = 1; k <= 21; k++){ const d = DAYS[(i + k) % 365]; if (settle(w, d).active.some(isBroad)) return `<span class="wsoon">Closed from ${fmtMd(d)}</span>`; }
      const tr = (typeof MODEL !== 'undefined' && MODEL && w === PLACE) ? MODEL.rows.find(r => !r.pool && ['closed','release'].includes(r.kind) && r.members.some(S => ['RB','CT','EB','GB','LT','DV'].includes(S))) : null;
      if (tr){ const ru = runOf(strip(w, tr.members), md); return `<span class="wsoon">Trout ${tr.kind === 'closed' ? 'closed' : 'release only'}${ru.all ? ' all year' : ` until ${fmtMd(ru.until)}`}</span>`; }
      return ''; })()}</div>
    ${segBar(segs, md, 'big', fmtMd(md))}${MONTHS_ROW}${has.size > 1 ? `<div class="slegend"><span><i class="s-keep"></i>open</span><span><i class="s-closed"></i>closed: no fishing for any game fish</span></div>` : ''}</div>`;
}
function miniStrip(w, members, md){
  const segs = strip(w, members); if (segs.length < 2) return '';
  const toDays = ss => ss.flatMap(([s, n]) => Array(n).fill(s)); const a = toDays(segs), b = toDays(waterSegs(w)), base = segs.find(s => s[0] !== 'closed')?.[0] || 'closed';
  if (a.every((s, i) => s === (b[i] === 'closed' ? 'closed' : base))) return '';
  return segBar(segs, md, 'mini') + shortWindows(segs);
}
// when several stretches closed all year are shown as one, list them with the rule that closes each
function mergedHtml(){
  const water = WATERS[state.wi], g = groupOf(water, state.pi);
  if (!g || !g.closed || g.idx.length < 2) return '';
  const L = partLabels(water);
  return `<div class="merged"><div class="lbl">Closed all year on ${g.idx.length} stretches (${g.sections} sections)</div><ul>${g.idx.map(i => { const pl = makePlace(state.wi, i), b = settle(pl, state.md).active.filter(isBroad).sort((x, y) => x.rank - y.rank)[0];
    return `<li><span>${esc(L[i])} <span class="muted">· ${water.parts[i].sections} section${water.parts[i].sections > 1 ? 's' : ''}</span></span>${b ? ` <button class="srcbtn inline" type="button" data-rule="${esc(b.key)}">${esc(RANK[String(b.rank)].t)}</button>` : ''}</li>`; }).join('')}</ul></div>`;
}

// how long today's state lasts, and what comes next (back-to-back closures read as one run)
function runOf(segs, md){
  const days = segs.flatMap(([st, n]) => Array(n).fill(st)), i = DAYS.indexOf(md), now = days[i];
  let a = i, b = i, g = 0; while (days[(a + 364) % 365] === now && g++ < 365) a = (a + 364) % 365;
  g = 0; while (days[(b + 1) % 365] === now && g++ < 365) b = (b + 1) % 365;
  if (g >= 365) return { now, all:true };
  return { now, from:DAYS[a], until:DAYS[b], next:DAYS[(b + 1) % 365], then:days[(b + 1) % 365] };
}
const STATE_TXT = { keep:'you can keep them', release:'release only', closed:'closed', none:'no rule' };
// "Release only until Dec 31. Then closed Jan 1–Jun 30. You can keep them from Jul 1."
// the first sentence alone ("until Oct 31") can be folded into a headline: runUntil + runTxt(…, true)
// a fish that can never be kept: just say when fishing for it closes
function closedRuns(segs){
  let d = 0; const runs = []; segs.forEach(([st, n]) => { if (st === 'closed') runs.push([d, d + n - 1]); d += n; });
  if (!runs.length) return '';
  if (runs.length === 1 && runs[0][0] === 0 && runs[0][1] === 364) return 'All year.';
  // a closure running over New Year is one closure
  if (runs.length > 1 && runs[0][0] === 0 && runs[runs.length - 1][1] === 364){ const last = runs.pop(); runs[0] = [last[0], runs[0][1]]; }
  const wsg = waterSegs(PLACE).flatMap(([st, n]) => Array(n).fill(st));
  return runs.map(([a, b]) => { const whole = wsg[a] === 'closed' && wsg[(b + 365) % 365] === 'closed'; return `${whole ? 'The whole water is closed' : 'Closed'} ${rangeTxt(DAYS[a], DAYS[(b + 365) % 365])}.`; }).join(' '); }
const nextYr = (d, md) => DAYS.indexOf(d) < DAYS.indexOf(md) ? ' next year' : '';
function runUntil(segs, md){ const r = runOf(segs, md); return r.all ? '' : fmtMd(r.until) + nextYr(r.until, md); }
function runTxt(segs, md, tail){
  const r = runOf(segs, md); if (r.all) return tail ? '' : r.now === 'keep' ? '' : 'All year.';
  const NOW = { release:'Release only', closed:'Closed', keep:'You can keep them', none:'No rule' }, THEN = { release:'release only', closed:'closed', keep:'you can keep them', none:'no rule' };
  if (r.now === 'keep') return `You can keep them until ${fmtMd(r.until)}. Then ${THEN[r.then]} from ${fmtMd(r.next)}.`;
  let out = tail ? '' : `${NOW[r.now]} until ${fmtMd(r.until)}${nextYr(r.until, md)}.`, cur = r, k = 0;
  const wsg = waterSegs(PLACE), wClosed = d => runOf(wsg, d).now === 'closed';
  while (cur.then !== 'keep' && k++ < 3){ const nx = runOf(segs, cur.next); if (nx.all) break; out += nx.now === 'closed' && wClosed(nx.from) ? ` Then the whole water is closed ${rangeTxt(nx.from, nx.until)}.` : ` Then ${THEN[nx.now]} ${rangeTxt(nx.from, nx.until)}.`; cur = nx; if (nx.next === r.from) break; }
  if (cur.then === 'keep') out += ` You can keep them from ${fmtMd(cur.next)}${nextYr(cur.next, md)}.`; else if (!segs.some(([st]) => st === 'keep')) out += ' No keeping at any time of year.';
  out = out.trim();
  return out;
}
function closedBanner(what){
  if (!MODEL.broad.length) return '';
  const b = MODEL.broad[0];
  // one closure can run straight into the next: list each one between today and the day it reopens
  const chain = []; if (REOPEN){ let d = state.md, g = 0; while (d !== REOPEN && g++ < 366){ const r = settle(PLACE, d).active.filter(isBroad).sort((x, y) => x.rank - y.rank)[0]; if (r && !chain.includes(r)) chain.push(r); d = DAYS[(DAYS.indexOf(d) + 1) % 365]; } }
  const list = chain.length > 1 ? chain : [b];
  const run = REOPEN ? runOf(waterSegs(PLACE), state.md) : null;
  return `<div class="banner"><div class="big">${REOPEN ? `Closed · opens again ${fmtMd(REOPEN)}` : 'Closed all year'}</div>
    ${REOPEN ? `<p><b>No fishing for any game fish ${esc(rangeTxt(run.from, run.until))}.</b>${chain.length > 1 ? ` Together these closures cover it:` : ''}</p>` : ''}
    <ul class="closechain">${list.map(r => `<li>${esc(describe(r))}. <span class="muted small">${esc(RANK[String(r.rank)].t)}</span> <button class="srcbtn inline" type="button" data-rule="${esc(r.key)}">Source</button></li>`).join('')}</ul>
    ${REOPEN ? `<button class="golink" type="button" data-md="${REOPEN}">Opens again ${fmtMd(REOPEN)}. See the ${what || 'rules'} from then ›</button>` : ''}${mergedHtml()}</div>`;
}
function timedHtml(ctx){
  const t = ctx.timed.filter(r => r.type === 'retention_limit' || r.type === 'angler_closure');
  return t.length ? `<div class="caveats top"><div class="lbl">⚠ At certain times</div><ul>${t.map(r => `<li><button class="factbtn" type="button" data-rule="${esc(r.key)}"><b>${esc(cap(describe(r).replace(/ \(.*\)$/, '')))}</b><span class="where">${esc(r.tcond)}</span></button></li>`).join('')}</ul></div>` : '';
}
function anglerClosures(ctx){
  const a = ctx.active.filter(r => r.k === 'anglerclosure');
  return a.length ? `<div class="caveats top"><div class="lbl">⚠ Closed to some anglers</div><ul>${a.map(r => `<li><button class="factbtn" type="button" data-rule="${esc(r.key)}"><b>${esc(r.label)}</b></button></li>`).join('')}</ul></div>` : '';
}
function uncertainHtml(kinds){
  const u = (WATERS[state.wi].uncertain || []).map(k => RULES[k]).filter(r => r && kinds.includes(r.family));
  return u.length ? `<div class="caveats top check"><div class="lbl">? Check: rules for parts of this water that couldn’t be placed on the map</div><ul>${u.map(r => `<li><button class="factbtn" type="button" data-rule="${esc(r.key)}"><b>${esc(clean(r.label))}</b><span class="where">${esc(friendlyWhy(r.prov.why || ''))}</span></button></li>`).join('')}</ul></div>` : '';
}
function inPartHtml(ctx, fams){
  const u = (ctx.inpart || []).filter(r => fams.includes(r.family));
  if (!u.length) return '';
  const md = ctx.md, now = u.filter(r => inDates(r.wins, md)), later = u.filter(r => !inDates(r.wins, md));
  const boat = r => r.family === 'vessel';
  const hot = now.some(r => !boat(r)), onlyBoat = now.length && now.every(boat);
  const where = r => { const p = cap(r.part || '').replace(/\(map ([A-Z])\)/, '(map $1 in the regulations)'); return /various|do not identify|buoys and signs/i.test(p) ? 'Where: some spots, marked by buoys and signs. Look for signs.' : 'Where: ' + p; };
  const li = r => `<li><button class="factbtn" type="button" data-rule="${esc(r.key)}"><b>${esc(clean(r.parts?.what || r.label).replace(/ — in part:.*$/, ''))}${r.wins.length ? `<span class="muted"> · ${esc(winTxt(r).slice(2))}</span>` : ''}</b><span class="where">${esc(where(r))}</span></button></li>`;
  const title = hot ? 'Fishing is different in some spots today' : onlyBoat ? 'Boat rules in some spots' : 'Some spots have their own rules';
  const calm = hot ? 'Everywhere else, follow this page.' : onlyBoat ? `Fishing rules are the same everywhere on this ${PLACE.kind === 'lake' ? 'lake' : 'river'}.` : 'Not in force today. The map can’t draw these places yet, so check where you are.';
  return `<details class="inpart${hot ? ' hot' : ''}"${hot ? ' open' : ''}><summary><span class="ic">ⓘ</span>${title}</summary><p class="calm">${calm}</p><ul class="plain">${(now.length ? now : later).map(li).join('')}</ul>${now.length && later.length ? `<details class="inl later"><summary>Other dates (${later.length})</summary><ul class="plain later">${later.map(li).join('')}</ul></details>` : ''}</details>`;
}
function sideHtml(ctx, fams){
  const u = (ctx.beside || []).filter(r => fams.includes(r.family));
  return u.length ? `<div class="caveats top"><div class="lbl">⚠ On one side of the channel only</div><ul>${u.map(r => `<li><button class="factbtn" type="button" data-rule="${esc(r.key)}"><b>${esc(clean(r.parts?.what || r.label))}</b><span class="where">${esc(cap(r.parts?.side || r.f.side + ' half only'))}${r.parts?.where ? ' · ' + esc(r.parts.where) : ''}</span></button></li>`).join('')}</ul></div>` : '';
}
function standingHtml(ctx){
  const s = ctx.here.filter(r => r.k === 'standing');
  return s.length ? `<div class="caveats"><div class="lbl">Anywhere in B.C.</div><ul>${s.map(r => `<li><button class="factbtn" type="button" data-rule="${esc(r.key)}">No fishing: ${esc(lc(clean(r.verbatim).replace(/\.$/, '')))}</button></li>`).join('')}</ul></div>` : '';
}
function possessionHtml(ctx){ const p = ctx.active.filter(r => r.k === 'possession'); return p.length ? `<div class="foot">Possession: up to ${p[0].f.per_daily} days’ worth of these limits. <button class="srcbtn inline" type="button" data-rule="${esc(p[0].key)}">Source</button></div>` : ''; }
const scopeOf = p => p.rank >= 3 ? (p.rank === 4 ? 'B.C.' : (p.prov.entry_name || 'the region')) : null;
function scopeBadge(row){ if (!row.pool || row.kind !== 'keep') return ''; const s = scopeOf(row.pool); if (s && row.narrow && row.narrow.f.water && row.narrow.f.water === PLACE.kind && row.narrow.f.take < row.pool.f.take) return `<span class="scope wide">${esc(s)} · ${esc(row.narrow.f.water)}s</span>`; return s ? `<span class="scope wide">${esc(s)}-wide</span>` : `<span class="scope here">This water</span>`; }
function scopeLine(row){
  if (!row.pool || row.kind !== 'keep') return '';
  const s = scopeOf(row.pool), n = row.narrow;
  const apart = (row.pool.f.exempts || []).some(e => { const t = RULES[`${e.entry_id}::${e.rule_id}`]; return t && t.k === 'pool' && t.baseRank >= 2; });
  if (!s) return `<p class="scopenote">This is ${esc(PLACE.name)}’s own limit.${apart ? ' These fish are counted apart from the region’s total: they don’t use it up.' : ''}</p>`;
  if (n && n.f.take < row.pool.f.take && n.f.water) return `<p class="scopenote wide">The ${row.pool.f.take} a day is one count across every stream and lake in ${esc(s)}. No more than ${n.f.take} of them may come from ${n.f.water}s${n.f.origin ? `, and those must be ${n.f.origin}` : ''}. Fish you already kept elsewhere today count.</p>`;
  return `<p class="scopenote wide">One count across every stream and lake in ${esc(s)}. Fish you already kept elsewhere today count toward this ${row.pool.f.take}.</p>`;
}

/* ---------- plain facts ---------- */
function fact(l, row){
  const r = l.r;
  switch (l.t){
    case 'origin': return { m:'✕', c:'rel', long: l.o === 'wild' ? 'Hatchery fish only. Carefully release every wild one.' : 'Wild fish only. Carefully release every hatchery one.', short:`${cap(l.keepO)} only` };
    case 'subcap': return { m:r.f.take, c:'cap', long:`At most ${r.f.take} of your ${row.daily} can be ${lcNames([...l.members], ' or ')}`, short:`Max ${r.f.take}` };
    case 'cap': return l.carveOf ? { m:l.take, c:'cap', long:`Up to ${l.take} ${bandTxt(l.a, l.b)} (instead of ${l.carveOf.take})`, short:`${l.take} ${bandTxt(l.a, l.b)}` } : { m:l.take, c:'cap', long:`At most ${l.take} ${bandTxt(l.a, l.b)}`, short:`only ${l.take} ${bandTxt(l.a, l.b)}` };
    case 'rel': return { m:'✕', c:'rel', long:`Release any ${bandTxt(l.a, l.b)}`, short: l.a === 0 ? `${l.b} cm minimum` : l.b === Infinity ? `${l.a} cm maximum` : `none ${l.a}–${l.b} cm` };
    case 'annual': if (r.f.period === 'possession') return { m:'P', c:'cal', long:`In possession: at most ${r.f.take}${r.f.lengths ? ' (' + describe(r).replace(/^.*? a possession limit/, '').trim() + ')' : ''}`, short:`${r.f.take} in possession` }; { const c = bands(r).find(s => s.take > 0); const co = c && c.a > 0; return { m:'yr', c:'cal', long:`Yearly limit: ${r.f.take}${co ? ` ${bandTxt(c.a, c.b)}. Smaller ones don’t count toward it` : ' a year'}${r.f.origin ? ` (${r.f.origin} fish)` : ''}`, short:`${r.f.take}${co ? ' ' + bandTxt(c.a, c.b) : ''} a year` }; }
    case 'record': { const mn = (r.f.lengths || []).find(x => x.min_cm)?.min_cm; return { m:'!', c:'cal', long:`Record each one you keep${mn ? ` over ${mn} cm` : ''} on your licence`, short: mn ? `record ones over ${mn} cm` : 'record it' }; }
    case 'duty': return { m:'!', c:'cal', long:clean(r.verbatim), short: r.type === 'stop_fishing_after_quota' ? 'stop fishing once you have your limit' : /record/i.test(r.parts?.duty || r.verbatim || '') ? 'record it' : clean(r.parts?.duty || r.parts?.what || 'see rule').toLowerCase().slice(0, 48) };
    case 'exc': return { m:'✕', c: l.status === 'closed' ? 'closed' : 'rel', long:(l.status === 'closed' ? 'Closed. Don’t fish for them' : 'Release every one') + winTxt(r), short: l.status === 'closed' ? 'Closed' : 'Release' };
    case 'xref': return { m:'→', c:'cap', long:`Has its own limit of ${l.daily === Infinity ? 'no limit' : l.daily + ' a day'} (its own row), and still counts toward this ${row.daily}`, short:`own limit ${l.daily}` };
    case 'outer': return { m:'⊂', c:'cap', long:`Also counts toward ${r.rank >= 3 ? scopeOf(r) + '’s' : 'the'} total of ${r.f.take} ${writtenName(r)} a day${r.rank >= 3 ? ', all waters there together' : ''}. It never lets you keep more than the number here`, short:`counts toward ${r.f.take} ${writtenName(r)}` };
    case 'also': return l.capped ? { m:r.f.unlimited ? '∞' : r.f.take, c:'cap', long:`${r.rank <= 1 ? 'This water’s own limit' : 'Its own limit'} is ${r.f.unlimited ? 'no limit' : r.f.take + ' a day'}, but the ${row.daily} total caps it`, short:`own ${r.f.unlimited ? 'no limit' : r.f.take}, capped` } : { m:r.f.take, c:'cap', long:`Another limit also applies: ${describe(r)}`, short:`also max ${r.f.take}` };
    case 'origin2': return { m:l.daily === Infinity ? '∞' : l.daily, c:'cap', long:`${cap(l.o)} fish: ${l.daily === Infinity ? 'no limit' : l.daily + ' a day'}${l.min ? `, none under ${l.min} cm` : ''}${l.max ? `, none over ${l.max} cm` : ''} (${clean(l.r.parts?.what || l.r.label)})`, short:`${l.o} ${l.daily === Infinity ? 'no limit' : l.daily}` };
    case 'orphan': return { m:r.f.take, c:'cap', long:`${cap(describe(r).replace(/^Inside the day’s total: /, ''))}`, short:`max ${r.f.take}` };
    case 'partly': return { m:'~', c:'cal', long:`Partly lifted here: “${clean(r.parts?.what || r.label)}” still holds, except for the fish or anglers a lift names`, short:null };
    case 'caution': return { m:'?', c:'cal', long:`Unclear in the book: this limit ${l.says}`, short:'unclear: size' };
    case 'tnote': return { m:'i', c:'cal', long: l.only ? 'This regulation lists char separately, so “trout” here means trout only (rainbow, steelhead, cutthroat, brown). Char follow their own lines.' : 'Trout includes char (Dolly Varden/bull trout, lake trout, brook trout) unless the regulation lists char separately.', short:null };
    case 'steel': return { m:'ST', c:'cal', long:'Over 50 cm, a rainbow here counts as a steelhead: the steelhead rules apply to it', short:'over 50 cm = steelhead' };
    case 'outercap': return { m:r.f.take, c:'cap', long:`In that ${l.outer.f.take}, at most ${r.f.take} can be ${writtenName(r, ' or ')}`, short:null };
    case 'outersize': return { m:l.take, c:'cap', long:`In that ${l.outer.f.take}, at most ${l.take} ${bandTxt(l.a, l.b)}`, short:null };
  }
  return { m:'·', c:'cal', long:clean(r.verbatim), short:null };
}
const factLi = (l, row) => { const f = fact(l, row); const rs = l.rules || [l.r]; const rk = RANK[String(Math.min(...rs.map(x => x.rank)))];
  return `<li><span class="m ${f.c}">${f.m}</span><button class="factbtn" type="button" data-rule="${esc(rs[0].key)}">${esc(f.long)}<span class="src">${rk.t}${rs.length > 1 ? ` +${rs.length-1}` : ''}</span></button></li>`; };
// the smallest fish anyone may keep: every kind here has its own minimum, so the smallest of them holds for all
function minFloor(row){
  if (row.everyone.some(l => l.t === 'rel' && l.a === 0)) return null;
  const steelWater = !!PLACE.part?.anadromous_rainbow;
  const mins = row.members.map(S => { const rs = ['H', 'W'].map(o => MODEL.R[S]?.[o]).filter(r => r && KEEPISH(r.status));
    if (!rs.length) return { S, skip:true };
    const m = Math.min(...rs.map(r => { const x = r.lines.find(l => l.t === 'rel' && l.a === 0); return x ? x.b : (S === 'ST' && steelWater ? 50 : 0); }));
    return { S, m }; }).filter(x => !x.skip);
  if (!mins.length || mins.some(x => !x.m)) return null;
  const floor = Math.min(...mins.map(x => x.m));
  const higher = new Map(); mins.filter(x => x.m > floor && !(x.S === 'ST' && steelWater)).forEach(x => (higher.get(x.m) || higher.set(x.m, []).get(x.m)).push(x.S));
  return { floor, higher };
}
function rulerHtml(row){
  const mf = minFloor(row);
  const full = [...row.everyone.filter(l => l.t === 'rel' || l.t === 'cap'), ...(mf ? [{ t:'rel', a:0, b:mf.floor }] : [])];
  if (!full.length) return '';
  const pts = new Set([0]); full.forEach(l => { if (l.a) pts.add(l.a); if (l.b !== Infinity) pts.add(l.b); });
  if (mf) mf.higher.forEach((_, m) => pts.add(m));
  const top = Math.max(...pts), mx = Math.ceil((top + Math.max(20, top * .4)) / 10) * 10; pts.add(mx);
  const P = [...pts].sort((a, b) => a - b), segs = [];
  for (let i = 0; i < P.length - 1; i++){
    const a = P[i], b = P[i+1], m = (a + b) / 2; let st = 'keep', label = 'Keep';
    if (full.some(l => l.t === 'rel' && m >= l.a && m <= l.b)){ st = 'rel'; label = 'Release'; }
    else { const c = full.find(l => l.t === 'cap' && m >= l.a && m <= l.b); if (c){ st = 'cap'; label = `Max ${c.take}`; } }
    const last = segs[segs.length-1]; if (last && last.st === st && last.label === label) last.b = b; else segs.push({ a, b, st, label });
  }
  const pc = v => (v / mx * 100).toFixed(2), ticks = []; for (let v = 10; v < mx; v += 10) if (!pts.has(v)) ticks.push(v);
  return `<div class="ruler2"><div class="lbl">Size of the fish</div><div class="r2marks">${(() => { let prevUp = false; return P.slice(1, -1).map((v, i, a) => { const up = i > 0 && !prevUp && (v - a[i - 1]) / mx < .14; prevUp = up; return `<span class="${up ? 'up' : ''}" style="left:${pc(v)}%">${v} cm</span>`; }).join(''); })()}</div>
    <div class="r2track">${segs.map(s => `<div class="b-${s.st}" style="width:${pc(s.b - s.a)}%"><span>${s.label}</span><small>${s.a === 0 ? `under ${s.b}` : s.b === mx ? `over ${s.a}` : `${s.a}–${s.b}`} cm</small></div>`).join('')}${P.slice(1, -1).map(v => `<i class="r2line" style="left:${pc(v)}%"></i>`).join('')}</div>
    <div class="r2ticks">${ticks.map(v => `<i style="left:${pc(v)}%"></i>`).join('')}<span style="left:0">0</span><span style="left:100%">${mx}+</span></div>${mf && mf.higher.size ? `<div class="r2note">Release anything under ${mf.floor} cm. Some need to be longer: ${[...mf.higher].sort((a, b) => a[0] - b[0]).map(([m, ss]) => `${lcNames(ss, ' and ')} ${m} cm`).join('; ')}.</div>` : ''}</div>`;
}
function liftNoteHtml(row){ return (row.liftNotes || []).map(n => `<div class="flag"><b>!</b><span>${esc(`Lifted only ${n.q.when_targeting ? 'when fishing for ' + lcNames(expand(n.q.when_targeting)) : n.q.while ? 'while ' + n.q.while.join(', ').replace(/_/g, ' ') : n.q.lengths ? 'for fish ' + n.q.lengths.map(x => bandTxt(x.min_cm ?? 0, x.max_cm ?? Infinity)).join(', ') : 'for ' + lcNames(expand(n.q.species))}: “${clean(n.by.verbatim)}”`)}</span></div>`).join(''); }

/* ---------- today's card ---------- */
function valueHtml(row, big){
  if (row.kind === 'keep') return big ? `<div class="num">${row.daily}<small>${row.members.length > 1 ? 'a day, shared' : 'a day'}</small></div>` : `<span class="gv">${row.daily}<small> a day</small></span>`;
  if (row.kind === 'nolimit') return `<span class="pill nolimit">No limit</span>`;
  return `<span class="pill ${row.kind}">${row.kind === 'closed' ? 'Closed' : 'Release'}</span>`;
}
function groupTags(g, row, id, gi){
  const shorts = g.facts.map(l => fact(l, row).short).filter(Boolean), dut = 0;
  if (!shorts.length) return '';
  const bits = [...new Set(shorts.filter(s => s !== 'must-do'))]; if (dut) bits.push(`${dut} must-do${dut > 1 ? 's' : ''}`);
  return `<button class="spec" type="button" data-toggle="${id}" data-g="${gi}"><b>${esc(Names(g.members, ' and '))}</b><span>${esc(bits.join(' · '))}</span></button>`;
}
const moreBtn = id => `<button class="rmore" type="button" data-toggle="${id}">Details <span aria-hidden="true">▾</span></button>`;
const lessBtn = id => `<button class="rless" type="button" data-toggle="${id}"><span aria-hidden="true">▴</span> Back to summary</button>`;
const narrowAll = row => !!row.narrow && row.members.every(S => (row.narrow.species || []).includes(S) && !(row.narrow.speciesExcept || []).includes(S));
const nar0 = row => !!(row.pool && narrowAll(row) && row.narrow.f.water === PLACE.kind && row.narrow.f.take < row.pool.f.take && scopeOf(row.pool));
const ddOpen = k => state.dd.has(k) ? ' open' : '';
const shortsOf = (g, row) => [...new Set(g.facts.map(l => fact(l, row).short).filter(Boolean).filter(s => s !== 'must-do'))];
const factsUl = (ls, row) => `<ul class="facts">${ls.map(l => factLi(l, row)).join('')}</ul>`;
// one species (or pair) and its short rules; open it for the full lines behind them
function specDD(g, row, id, gi){
  const sh = shortsOf(g, row); if (!sh.length) return '';
  const pics = g.members.filter(hasPic);
  return `<details class="sdd" data-dd="${id}g${gi}"${ddOpen(id + 'g' + gi)}><summary><b>${esc(Names(g.members, ' and '))}</b><span>${esc(sh.join(' · '))}</span></summary>
    <div class="ddbody">${factsUl(g.facts, row)}${g.facts.some(l => l.t === 'origin') ? howTell() : ''}${pics.length ? `<div class="fcs">${pics.map(S => `<button class="fc in" type="button" data-fish="${S}"><span><i class="eye" aria-hidden="true"></i>${esc(spName(S))}</span></button>`).join('')}</div>` : ''}</div></details>`;
}
// everything that used to be on the detail view but belongs to the whole row
function moreDD(row, id){
  let h = '';
  if (row.pool){
    h += nar0(row) ? '' : (scopeOf(row.pool) ? countBox(row) : scopeLine(row));
    if (row.everyone.length) h += `<div class="fgroup"><div class="lbl">For all of them</div>${factsUl(row.everyone, row)}${row.everyone.some(l => l.t === 'origin') ? howTell() : ''}</div>`;
    row.groups.forEach((g, gi) => { if (!shortsOf(g, row).length && g.facts.length) h += `<div class="fgroup"><div class="ghead">${esc(Names(g.members, ' and '))}</div>${factsUl(g.facts, row)}</div>`; });
  } else {
    h += `<ul class="facts"><li><span class="m ${row.kind === 'closed' ? 'closed' : 'rel'}">✕</span><button class="factbtn" type="button" data-rule="${esc(row.win.key)}">${esc(describe(row.win))}<span class="src">${RANK[String(row.win.rank)].t}</span></button></li></ul>`;
  }
  h += liftNoteHtml(row);
  if (row.members.length > 1) h += `<div class="fgroup"><div class="lbl">Which fish (${row.members.length})</div><div class="fcs">${row.members.map(S => `<button class="fc in" type="button" data-fish="${S}"><span>${hasPic(S) ? '<i class="eye" aria-hidden="true"></i>' : ''}${esc(spName(S))}</span></button>`).join('')}</div></div>`;
  if (miniStrip(PLACE, row.members, state.md)) h += stripHtml(PLACE, row.members, state.md);
  h += `<button class="srcbtn" type="button" data-src="${id}">Sources for this row</button>`;
  const label = row.pool ? 'Whole-group rules, which fish and sources' : 'Why, and sources';
  return `<details class="mdd" data-dd="${id}m"${ddOpen(id + 'm')}><summary>${label}</summary><div class="ddbody">${h}</div></details>`;
}
function summaryHtml(row, id){
  if (!row.pool) return `<div class="sum">${row.kind === 'closed' ? 'Don’t fish for them. If one bites anyway, carefully release it.' : 'You can fish for them, but carefully release every one.'}${row.win.wins.length ? ' ' + esc(cap(winTxt(row.win).slice(2))) + '.' : ''}${!row.members.length && namedIn(row.win) ? ` <span class="muted">(${esc(namedIn(row.win))})</span>` : ''}</div>${miniStrip(PLACE, row.members, state.md)}${moreDD(row, id)}`;
  const bits = [...new Set(row.everyone.map(l => fact(l, row).short).filter(Boolean))];
  const hat = [...row.everyone, ...row.groups.flatMap(g => g.facts)].some(l => l.t === 'origin');
  const nar = narrowAll(row) && row.narrow.f.water === PLACE.kind && row.narrow.f.take < row.pool.f.take && scopeOf(row.pool);
  const rc = rowConds(row);
  const keep = row.kind === 'keep' && rc.n <= 10 ? `<div class="keeprow2">${pips(0, rc.n).replace(/<i><\/i>/g, '<i class="k"></i>') || ''}<span>${esc(rc.keepLine)}</span></div>` : '';
  const links = row.members.some(hasPic) ? `<div class="sumlinks"><button class="linkbtn" type="button" data-fishgroup="${row.members.join(',')}">What they look like ›</button></div>` : '';
  return (nar ? countBox(row) : '') + rulerHtml(row) + ruleCards(row, rc, id, true) + links + miniStrip(PLACE, row.members, state.md) + moreDD(row, id);
}
function rowHtml(row, i){
  const id = 'r' + i;
  return `<article class="row" data-row="${id}"><div class="rhead"><span class="rtitle">${esc(rowTitle(row))}${scopeBadge(row)}</span>${valueHtml(row, false)}</div>
    <div class="rsum">${summaryHtml(row, id)}</div></article>`;
}
// trout and char first, in full; every other fish folds into one short list (readers: "70% of the scroll")
const TROUTISH = ['RB','CT','EB','GB','LT','DV','ST','BT','CT_ws','CT_c'];
function rowsHtml(){
  const all = MODEL.rows.map((r, i) => [r, i]);
  const main = all.filter(([r]) => (r.allMembers || r.members).some(S => TROUTISH.includes(S)));
  const rest = all.filter(x => !main.includes(x));
  if (!main.length || rest.length < 2) return all.map(([r, i]) => rowHtml(r, i)).join('');
  const short = r => r.kind === 'keep' ? `${r.daily} a day` : r.kind === 'nolimit' ? 'no limit' : r.kind === 'closed' ? 'closed' : 'release';
  return main.map(([r, i]) => rowHtml(r, i)).join('') +
    `<details class="others" data-dd="others"${ddOpen('others')}><summary><span class="otitle">Other fish</span><span class="olist">${rest.map(([r]) => `<span class="orow"><span class="on">${esc(rowTitle(r))}</span><b class="k-${r.kind}">${esc(short(r))}</b></span>`).join('')}</span></summary>${rest.map(([r, i]) => rowHtml(r, i)).join('')}</details>`;
}
function renderCard(){
  const ctx = settle(PLACE, state.md);
  let h = waterStrip(PLACE, state.md) + closedBanner();
  if (!MODEL.broad.length){
    h += inPartHtml(ctx, ['retention','access']) + sideHtml(ctx, ['retention','access']) + anglerClosures(ctx) + timedHtml(ctx) + rowsHtml() + uncertainHtml(['retention','access']);
    if (!MODEL.rows.length) h += `<p class="foot">No quota rules reach this part of the water.</p>`;
    h += standingHtml(ctx) + possessionHtml(ctx);
  }
  document.getElementById('card').innerHTML = h;
}

/* ---------- shared: how today's count works, how to tell hatchery from wild ---------- */
function countBox(row){
  const s = row.pool && scopeOf(row.pool), n = row.daily;
  if (!s || row.kind !== 'keep') return '';
  if (narrowAll(row) && row.narrow.f.water && row.narrow.f.water === PLACE.kind && row.narrow.f.take < row.pool.f.take){
    const wk = row.narrow.f.water, other = wk === 'stream' ? 'lakes' : 'streams', a = n, b = row.pool.f.take;
    return `<div class="howcount"><div class="lbl">How today’s count works</div><ul><li>Up to <b>${a}</b> a day from ${wk}s in ${esc(s)}, this one included.</li><li>Up to <b>${b}</b> a day in ${esc(s)} in total, ${other} included.</li></ul><div class="bnote">Example: you kept 1 at a ${other.replace(/s$/, '')} this morning. You can still keep ${Math.min(a, b - 1)} here.</div></div>`;
  }
  return `<div class="bnote">This ${n} a day is for all of ${esc(s)} together: fish you kept at other waters there today count.</div>`;
}
const howTell = (open = false) => `<details class="howtell"${open ? ' open' : ''}><summary><b>How to tell hatchery from wild:</b> a hatchery fish is missing the small fin on its back just before the tail (the adipose fin). <b>Not sure? Treat it as wild.</b> <span class="pic">See picture ▸</span></summary>${adiposeHtml()}</details>`;
// the conditions on keeping from one row, in plain words (shared by the card and the bag view)
function rowConds(row){
    const all = [...row.everyone, ...row.groups.flatMap(g => g.facts)];
  const n = row.daily;
  const whoN = l => !l.members || l.general || l.members.size === row.members.length ? null : [...l.members];
  const groupWord = lc(rowTitle(row)).replace(/ and /g, ' or ');
  const fishWord = (l, plural) => { const w = whoN(l); const o = l.r.f.origin ? l.r.f.origin + ' ' : ''; return o + (w ? lcNames(w, ' or ') : row.members.length > 1 ? groupWord : lcNames(row.members)); };
  const limits0 = all.filter(l => ['subcap','cap','outercap','outersize','origin2'].includes(l.t));
  // a limit no bigger than the bag itself says nothing new
  const limits = limits0.filter(l => l.t === 'origin2' || !((l.t === 'outercap' ? l.r.f.take : l.take ?? l.r.f.take) >= n && l.t !== 'cap') && !(l.t === 'outersize' && l.take >= n));
  // a size cap is moot when, for every fish and origin in the bag, the day's number is already that small or those sizes go back anyway
  const mootCap = l => (l.t === 'cap' || l.t === 'outersize') && l.b === Infinity && [...(l.members || row.members)].every(S => ['H', 'W'].every(o => { const r = MODEL.R[S]?.[o]; return !r || !KEEPISH(r.status) || r.daily <= l.take || r.lines.some(x => x.t === 'rel' && x.a <= l.a && x.b === Infinity); }));
  for (let i = limits.length - 1; i >= 0; i--) if (!limits[i].carveOf && !limits.some(x => x.carveOf === limits[i]) && mootCap(limits[i])) limits.splice(i, 1);
  // a stream-only share of the day that holds here is a real limit, not a footnote
  if (row.narrow && row.narrow.f.water === PLACE.kind && row.narrow.f.take < n) limits.push({ t:'streamcap', r:row.narrow });
  const carves = limits.filter(l => l.carveOf);
  const back = all.filter(l => ['origin','rel','exc'].includes(l.t)).sort((x, y) => ['origin','exc','rel'].indexOf(x.t) - ['origin','exc','rel'].indexOf(y.t));
  // each throw-back line stands on its own: which fish, and why
  const backTxt = l => { const w = whoN(l);
    if (l.t === 'origin'){ const nm = w ? lcNames(w, ' or ') : groupWord; return l.o === 'wild' ? `Every wild ${nm}, any size. Only hatchery ${nm} can be kept.` : `Every hatchery ${nm}, any size. Only wild ${nm} can be kept.`; }
    if (l.t === 'exc') return `Every ${lcNames(w || row.members, ' or ')}, any size${l.status === 'closed' ? ' (closed: don’t fish for them)' : ''}.`;
    return `Any ${fishWord(l)} ${bandTxt(l.a, l.b)}.`; };
  const limitTxt = l => { const w = whoN(l);
    if (l.t === 'origin2') return `${cap(l.o)} ${w ? lcNames(w, ' and ') : groupWord}: only ${l.daily === Infinity ? 'unlimited' : l.daily} a day${l.min ? `, none under ${l.min} cm` : ''}${l.max ? `, none over ${l.max} cm` : ''}.`;
    if (l.t === 'streamcap'){ const tr = l.r.species.filter(x => !l.r.speciesExcept.includes(x));
      const lifted = o => S => MODEL.R[S]?.[o]?.roles.get(l.r.key)?.role === 'lifted';
      const out = tr.map(S => lifted('H')(S) && lifted('W')(S) ? lc(spName(S)) : lifted('H')(S) ? 'hatchery ' + lc(spName(S)) : lifted('W')(S) ? 'wild ' + lc(spName(S)) : null).filter(Boolean);
      // name exactly who this share is for: 'wild rainbow trout' where the hatchery ones are counted apart
      const keepO = (S, o) => KEEPISH(MODEL.R[S]?.[o]?.status);
      const who = tr.filter(S => !(lifted('H')(S) && lifted('W')(S))).filter(S => lifted('H')(S) ? keepO(S, 'W') : lifted('W')(S) ? keepO(S, 'H') : (keepO(S, 'H') || keepO(S, 'W'))).map(S => lifted('H')(S) ? 'wild ' + lc(spName(S)) : lifted('W')(S) ? 'hatchery ' + lc(spName(S)) : lc(spName(S)));
      const L = who.length > 1 ? who.slice(0, -1).join(', ') + ' or ' + who[who.length - 1] : who[0];
      return `Only ${l.r.f.take} can be a ${L}${l.r.wins.length ? ', ' + winTxt(l.r).slice(2) : ''}. This rule is for ${l.r.f.water}s.`; }
    if (l.t === 'subcap') return `Only ${l.r.f.take} of them can be ${keepNames([...l.members])}.`;
    if (l.t === 'outercap') return `Only ${l.r.f.take} can be ${keepNames(l.r.species || [], l.r)}.`;
    if (l.t === 'outersize') return `Only ${l.take} can be ${bandTxt(l.a, l.b)}.`;
    const c = carves.filter(x => x !== l && x.carveOf === l);
    return `Only ${l.take} can be ${bandTxt(l.a, l.b)}${w ? ` (${lcNames(w, ' and ')})` : ''}${c.length ? `, except ${c.map(x => `${lcNames(whoN(x) || [], ' and ')}: up to ${x.take}`).join('; ')}` : ''}.`; };
  const extra = all.filter(l => ['annual','outer'].includes(l.t)).map(l => l.t === 'annual' ? (() => { const m = fact(l, row).short.replace(/ a year$/, '').match(/^(\d+)(.*)$/) || ['', '', '']; return `Per licence year: ${m[1]} ${whoN(l) ? lcNames(whoN(l), ' and ') : writtenName(l.r)}${m[2]}`; })() : `Also counts toward ${l.r.rank >= 3 ? scopeOf(l.r) + '’s' : 'a'} total of ${l.r.f.take} ${writtenName(l.r)} a day, all waters there together. It never lets you keep more than ${n} here.`);
  const s = scopeOf(row.pool);
  const po = row.pool.f.origin ? row.pool.f.origin + ' ' : '';
  const keepLine = row.kind === 'nolimit' ? 'Keep as many as you like' : row.members.length > 1 ? `Keep up to ${n}${po ? ' ' + po.trim() : ''}, any mix` : `Keep up to ${n} ${po}${lcNames(row.members)}`;
  const shareLine = row.kind === 'keep' && s ? (row.narrow && row.narrow.f.take < row.pool.f.take && row.narrow.f.water ? (PLACE.kind === row.narrow.f.water ? `COUNT|${n}|${row.pool.f.take}|${row.narrow.f.water}` : `Counts fish you kept anywhere in ${s} today: ${row.pool.f.take} a day in total, and only ${row.narrow.f.take} ${writtenName(row.narrow)} from ${row.narrow.f.water}s.`) : `Counts fish you kept anywhere in ${s} today, not just here.`) : '';
  const hw = back.some(l => l.t === 'origin');
  // every condition on keeping, one line each, grouped by the fish it is about
  const conds = [];
  const everyone = back.filter(l => !whoN(l)), bySet = new Map();
  back.filter(l => whoN(l)).forEach(l => { const k = whoN(l).slice().sort().join(','); (bySet.get(k) || bySet.set(k, []).get(k)).push(l); });
  const goesBack = S => { const m = mainRes(MODEL.R[S]?.H, MODEL.R[S]?.W); return m && !KEEPISH(m.status); };
  limits.filter(l => l.t === 'subcap' && whoN(l)).forEach(l => { const keepers = [...l.members].filter(S => !goesBack(S)); if (!keepers.length) return; const k = keepers.sort().join(','); (bySet.get(k) || bySet.set(k, []).get(k)).push(l); });
  everyone.forEach(l => {
    if (l.t === 'origin') conds.push({ c:'origin', t: l.o === 'wild' ? `Hatchery fish only. Carefully release every wild one.` : `Wild fish only. Carefully release every hatchery one.` });
    else if (l.t === 'rel'){ const nn = (po ? po : '') + (row.members.length > 1 ? groupWord.replace(/ or /g, ' and ') : lcNames(row.members)); conds.push({ c:'size', t: l.a === 0 ? `Keep only ${nn} ${l.b} cm or longer.` : l.b === Infinity ? `Keep only ${nn} ${l.a} cm or shorter.` : `Carefully release any ${l.a}–${l.b} cm.` }); }
  });
  for (const [k, L] of bySet){ const names = cap(lcNames(k.split(','), ' and '));
    if (L.some(l => l.t === 'exc')){ const x = L.find(l => l.t === 'exc'); conds.push({ c:'back', who:k.split(','), ph:`carefully release every one${x.r.wins.length ? ` (${winTxt(x.r).slice(2)})` : ''}${x.status === 'closed' ? ' (closed: don’t fish for them)' : ''}`, t:`${names}: carefully release every one${x.r.wins.length ? ` (${winTxt(x.r).slice(2)})` : ''}${x.status === 'closed' ? ' (closed: don’t fish for them)' : ''}.` }); continue; }
    const parts = [];
    if (L.some(l => l.t === 'origin')) parts.push(L.find(l => l.t === 'origin').o === 'wild' ? 'hatchery only (carefully release wild ones)' : 'wild only (carefully release hatchery ones)');
    L.filter(l => l.t === 'rel').forEach(l => parts.push(l.a === 0 ? `${l.b} cm or longer` : l.b === Infinity ? `${l.a} cm or shorter` : `not ${l.a}–${l.b} cm`));
    const sc = L.find(l => l.t === 'subcap'), p0 = parts.slice();
    if (sc) parts.push(k.includes(',') ? `only ${sc.r.f.take} in total of these together` : `only ${sc.r.f.take}`);
    if (parts.length) conds.push({ c:'group', who:k.split(','), ph:p0.join(', '), sub: sc ? sc.r.f.take : null, t:`${names}: ${join(parts, ', and ')}.` });
  }
  limits.filter(l => !l.carveOf && !(l.t === 'subcap' && whoN(l))).forEach(l => {
    const c2 = carves.filter(x => x.carveOf === l);
    if (l.t === 'cap' && c2.length){ const cv = c2.flatMap(x => whoN(x) || []); conds.push({ c:'cap', t:`Only ${l.take} ${l.take > 1 ? 'fish' : 'fish'} ${bandTxt(l.a, l.b)} that ${l.take > 1 ? "aren't" : "isn't a"} ${lcNames(cv, ' or ')}${l.take > 1 ? '' : ''}.` }); c2.forEach(x => conds.push({ c:'cap', who:whoN(x), ph:`${bandTxt(x.a, x.b)}: up to ${x.take}`, sub:x.take, t:`${cap(lcNames(whoN(x) || [], ' and '))} ${bandTxt(x.a, x.b)}: up to ${x.take}.` })); conds[conds.length - 1 - c2.length].sub = l.take; }
    else conds.push({ c:l.t, r:l.r, t:limitTxt(l), sub: l.t === 'streamcap' ? null : (l.t === 'outercap' ? l.r.f.take : l.take ?? l.r?.f?.take ?? null) });
  });
  return { conds, hw, keepLine, extra, n, s };
}
const pips = (k, n) => n > 1 && n <= 10 && k != null && k < n ? `<span class="pips2" aria-label="${k} of ${n}">${'<i class="on"></i>'.repeat(k)}${'<i></i>'.repeat(n - k)}</span>` : '';
const sameSet = (a, b) => a && b && a.length === b.length && a.every(x => b.includes(x));
function ruleCards(row, rc, id, expand){
  const n = rc.n, general = rc.conds.filter(c => !c.who), withWho = rc.conds.filter(c => c.who), used = new Set();
  const COVER = ['cap','rel','subcap','origin','outercap','outersize','tnote','partly'];
  const card = (members, cs, facts, key) => {
    const others = [...new Set((facts || []).filter(l => !cs.length || !COVER.includes(l.t)).map(l => fact(l, row).short).filter(Boolean).filter(x => x !== 'must-do'))];
    const lines = [...new Set([...cs.filter(c => !(c.c === 'cap' && c.sub != null && c.sub < n)).map(c => cap(c.ph != null ? c.ph : c.t)).filter(Boolean), ...(cs.some(c => c.c === 'back') ? [] : others.map(cap))])];
    const sub = cs.find(c => c.sub != null && c.c === 'group');
    if (!lines.length && !sub) return '';
    const pipLine = sub ? `<span class="subq">${pips(sub.sub, n)}Only ${sub.sub} of your ${n} can be ${members.length > 1 ? 'these' : lc(spName(members[0]))}${members.length > 1 ? ' together' : ''}</span>` : '';
    const carve = cs.find(c => c.sub != null && c.c === 'cap' && c.sub < n);
    const head = `<b>${esc(Names(members, ' and '))}</b><span>${esc(lines.join(' · '))}</span>${pipLine}${carve && !sub ? `<span class="subq">${pips(carve.sub, n)}${esc(cap(carve.ph))}</span>` : ''}`;
    if (!expand || !facts) return `<div class="sdd static"><div class="sddh">${head}</div></div>`;
    const pics = members.filter(hasPic);
    return `<details class="sdd" data-dd="${id}${key}"${ddOpen(id + key)}><summary>${head}</summary><div class="ddbody">${factsUl(facts, row)}${facts.some(l => l.t === 'origin') ? howTell() : ''}${pics.length ? `<div class="fcs">${pics.map(S => `<button class="fc in" type="button" data-fish="${S}"><span><i class="eye" aria-hidden="true"></i>${esc(spName(S))}</span></button>`).join('')}</div>` : ''}</div></details>`;
  };
  let specs = row.groups.map((g, gi) => { const cs = withWho.filter(c => sameSet(c.who, g.members)); cs.forEach(c => used.add(c)); return card(g.members, cs, g.facts, 'g' + gi); }).join('');
  specs += withWho.filter(c => !used.has(c)).map((c, i) => card(c.who, [c], null, 'x' + i)).join('');
  const tell = `<button class="linkbtn tell" type="button" data-howtell="1">How to tell ›</button>`;
  const gl = general.map(c => `<li class="g-${c.c}"><span>${esc(c.t)}</span>${c.c === 'origin' ? ' ' + tell : ''}${c.sub != null && c.c !== 'origin' ? pips(c.sub, n) : ''}</li>`).join('');
  return (gl ? `<ul class="glist">${gl}</ul>` : '') + (specs ? `<div class="specs">${specs}</div>` : '');
}
const condBox = (row, rc) => rc.conds.length ? `<div class="condbox"><div class="lbl">Only if</div><ul class="conds">${rc.conds.map(x => `<li class="c-${x.c}">${esc(x.t)}</li>`).join('')}</ul>${rc.hw ? howTell() : ''}</div>` : '';
/* ---------- bag view ---------- */
// which fish a group means, in the book's own families (p.86)
const FAMS = D.species.families || {};
function groupMeaning(r){
  const w = r.f.species || [], ex = r.f.species_except || [];
  if (w.length !== 1 || !GROUPS[w[0]] || GROUPS[w[0]].open) return '';
  const g = w[0], names = cs => join(cs.map(c => lc(spName(c))), ' and ');
  if (g === 'TROUT_CHAR'){
    const trout = FAMS.TROUT ? FAMS.TROUT.members : [], char = FAMS.CHAR ? FAMS.CHAR.members : [];
    if (ex.includes('CHAR')) return `“Trout” here means trout only: ${names(trout)}. This table lists char apart, so char follow their own lines.`;
    return `Trout and char means trout (${names(trout)}) and char (${names(char)}). In the regulations, “trout” includes char unless char are listed apart.`;
  }
  return `${GROUPS[g].name} means ${names(GROUPS[g].members.filter(c => !ex.includes(c)))}.`;
}
// every fish the bag's rule names, and what each one does here: in the bag, its own bag inside it, counted apart, thrown back
function bagFish(row){
  const pool = row.pool, out = [];
  for (const S of pool.species){
    if (pool.speciesExcept.includes(S) || !MODEL.R[S]) continue;
    const m = mainRes(MODEL.R[S].H, MODEL.R[S].W); if (!m) continue;
    let st, note = '';
    if (row.members.includes(S)){ st = 'in'; const H = MODEL.R[S].H, W = MODEL.R[S].W; if (H && W && KEEPISH(H.status) !== KEEPISH(W.status)) note = KEEPISH(H.status) ? 'hatchery only' : 'wild only'; }
    else if (!KEEPISH(m.status)) { st = m.status; note = m.status === 'closed' ? 'closed' : 'release'; }
    else if ([...m.roles].some(([k, v]) => k === pool.key && (v.role === 'contains' || v.role === 'also'))) { st = 'nested'; note = `own bag of ${m.daily === Infinity ? 'any number' : m.daily}, counts here`; }
    else { st = 'apart'; note = `own bag of ${m.daily === Infinity ? 'any number' : m.daily}, counted apart`; }
    out.push({ S, st, note, target: MODEL.rows.findIndex(r => r.pool && r.pool === m.win) });
  }
  // a note every fish in the bag shares says nothing about any one of them: drop it (the row already says it)
  const ins = out.filter(f => f.st === 'in');
  if (ins.length > 1 && ins.every(f => f.note === ins[0].note)) ins.forEach(f => f.note = '');
  const order = { in:0, nested:1, apart:2, release:3, closed:4 };
  return out.sort((a, b) => order[a.st] - order[b.st] || spName(a.S).localeCompare(spName(b.S)));
}
// pictures of each fish, cut from the species sheets supplied for the page (the labels on them are the book's identification tips)
const FISH_IMG = { RB:[['RB']], CT:[['CT','Coastal cutthroat trout'],['CT_ws','Westslope cutthroat trout']], GB:[['GB']], ST:[['ST']], KO:[['KO']],
  DV:[['DV','Dolly Varden'],['DV_bull','Bull trout (counted as Dolly Varden)']], LT:[['LT']], EB:[['EB']], GR:[['GR']], LW:[['WF','Whitefish']], MW:[['WF','Whitefish']],
  WSG:[['WSG']], BB:[['BB']], NP:[['NP']], WP:[['WP']], YP:[['YP']], BCB:[['BCB']], SMB:[['SMB']] };
const hasPic = S => FISHART.has(S);
const FISH_ID = {
  RB:['Small black spots, mostly above the lateral line','Radiating rows of spots on the tail','No teeth in the throat at the back of the tongue'],
  CT:['Red slash under the lower jaw (may be faint)','Large mouth, reaching well past the eye','Teeth in the throat at the back of the tongue'],
  GB:['Black or brown spots, many with light halos','Adipose fin with spots','Tail with few or no spots'],
  ST:['A rainbow trout that went to sea: silvery, fork length 50 cm or more','No teeth in the throat at the back of the tongue','Hatchery steelhead are missing the adipose fin'],
  KO:['No distinct black spots on the sides','Long anal fin (13 or more rays)'],
  DV:['Whitish to pinkish spots, the largest smaller than the pupil','No worm-like markings on the back fin','White leading edges on the lower fins','Bull trout: large, broad, flattened head; upper jaw curves down'],
  LT:['Worm-like markings on the back and back fin','Tail deeply forked'],
  EB:['Red spots with blue halos','Worm-like markings on the back and back fin','Pinkish-orange paired fins edged in white'],
  GR:['Long, sail-like back fin (more than 17 rays)'],
  LW:['Large scales','Small mouth; teeth weak or absent','Has an adipose fin'], MW:['Large scales','Small mouth; teeth weak or absent','Has an adipose fin'],
  WSG:['Barbels under the snout','A row of 11 to 14 bony plates along the back','Sides usually with small white spots'],
  BB:['Single barbel on the chin','Two back fins; one long anal fin'],
  NP:['Long, flattened snout','Back fin set far back, near the tail'],
  WP:['Sharp, fang-like teeth','Spiny front back fin','White corner on the lower half of the tail'],
  YP:['Six to nine dark vertical bars','Paired fins amber to bright orange','Spiny front back fin; no fang-like teeth'],
  BCB:['Deep, flat body','Mouth reaches the front edge of the pupil'],
  SMB:['Jaw reaches about the middle of the eye','Bronze-brown with faint vertical bars'],
};
function openFish(S, target){
  const c = FISHART.SPECIES[S], fam = Object.values(FAMS).find(f => f.members.includes(S));
  const bag = target != null && target >= 0 ? `<button class="srcbtn" type="button" data-bagjump="r${target}" data-closesheet="1">Go to its bag ›</button>` : '';
  openSheet(spName(S), fam ? `${fam.name}${S === 'DV' ? ' · a bull trout is counted as a Dolly Varden' : ''}` : '',
    c ? `<figure class="fishart">${FISHART.draw(S)}</figure><div class="idlist"><div class="lbl">What to look for</div><ol>${c.tips.map(t => `<li>${esc(t)}</li>`).join('')}</ol></div>${bag}` : `<p class="muted">No drawing for this fish yet.</p>${bag}`);
}
// every fish of a row, drawn, in one sheet
function openFishGroup(codes){
  openSheet('What they look like', '', codes.map(S => FISHART.has(S) ? `<div class="fgpic"><div class="lbl">${esc(spName(S))}</div><figure class="fishart">${FISHART.draw(S)}</figure><ol class="small">${FISHART.SPECIES[S].tips.map(t => `<li>${esc(t)}</li>`).join('')}</ol></div>` : `<div class="fgpic"><div class="lbl">${esc(spName(S))}</div><p class="muted small">No drawing yet.</p></div>`).join(''));
}
function fishChips(row){
  const F = bagFish(row); if (F.length < 2 && !F.some(f => f.st !== 'in')) return '';
  const chip = f => { const link = f.target >= 0 && f.st !== 'in', inner = `<span>${hasPic(f.S) ? '<i class="eye" aria-hidden="true"></i>' : ''}${esc(spName(f.S))}</span>${f.note ? `<small>${esc(f.note)}</small>` : ''}`;
    return `<button class="fc ${f.st}" type="button" data-fish="${f.S}"${link ? ` data-target="${f.target}"` : ''}>${inner}</button>`; };
  const meaning = groupMeaning(row.pool);
  return `<div class="fishin"><div class="lbl">Which fish <span class="muted small">· tap one to see what it looks like</span></div><div class="fcs">${F.map(chip).join('')}</div>${meaning ? `<details class="gq"><summary>What counts as ${esc(lc(rowTitle(row)))}?</summary><div class="gmean">${esc(meaning)}</div></details>` : ''}</div>`;
}
// a bag whose rule lifts a zone total for its fish: say that those fish don't use the zone's bag
function apartNote(row){
  const lifted = (row.pool.f.exempts || []).map(e => RULES[`${e.entry_id}::${e.rule_id}`]).filter(t => t && t.k === 'pool' && t.baseRank >= 2 && PLACE.cands.some(c => c.key === t.key));
  if (!lifted.length) return '';
  const t = lifted[0], idx = MODEL.rows.findIndex(r => r.pool && r.pool.key === t.key);
  return `<div class="gmean">Counted apart: ${esc(lcNames(row.members, ' and '))} kept here don’t use up the ${esc(writtenName(t))} bag of ${t.f.take}${scopeOf({ ...t, rank:t.baseRank }) ? ` (${esc(scopeOf({ ...t, rank:t.baseRank }))})` : ''}.${idx >= 0 ? ` <button class="linkbtn" type="button" data-bagjump="r${idx}">See that bag ›</button>` : ''}</div>`;
}
const FISH_LEGEND = `<div class="flegend" aria-label="Key"><span><i class="in"></i>In this bag</span><span><i class="nested"></i>Own bag, counts toward this one</span><span><i class="apart"></i>Own bag, counted apart</span><span><i class="release"></i>Carefully release</span><span><i class="closed"></i>Don’t fish for</span></div>`;
// hatchery or wild: the adipose fin. A generic trout outline; the only difference between the two is that small fin.
const adiposeHtml = () => `<div class="adipose2"><figure class="fishart"><figcaption><b>Wild</b> · adipose fin there</figcaption>${FISHART.draw('RB', { marks:false, adiMark:true })}</figure><figure class="fishart"><figcaption><b>Hatchery</b> · clipped, a healed scar instead</figcaption>${FISHART.draw('RB', { marks:false, adiMark:true, clipped:true })}</figure></div><p class="small muted">Look on the back, just in front of the tail. A small fin there means wild. Only a scar means hatchery. If you can’t tell, treat the fish as wild.</p>`;
// what you can keep, one line per shared limit (never a number per fish that could be added up)
function glanceHtml(){
  const keep = MODEL.rows.map((r, i) => ({ r, i })).filter(({ r }) => r.pool);
  if (!keep.length) return '';
  const li = ({ r, i }) => { const bits = [...new Set(r.everyone.map(l => fact(l, r).short).filter(Boolean))].filter(b => b !== 'must-do');
    const tag = S => { const H = MODEL.R[S]?.H, W = MODEL.R[S]?.W; return H && W && KEEPISH(H.status) !== KEEPISH(W.status) ? ` (${KEEPISH(H.status) ? 'hatchery' : 'wild'} only)` : ''; };
    const who = cap(join(r.members.slice().sort((a, b) => spName(a).localeCompare(spName(b))).map(S => lc(spName(S)) + tag(S)), ' and '));
    const num = r.kind === 'nolimit' ? 'no limit' : r.members.length > 1 ? `${r.daily} a day in total` : `${r.daily} a day`;
    return `<li><div><b>${esc(who)}</b>${bits.length ? `<span class="gbits">${esc(bits.join(' · '))}</span>` : ''}</div><span class="gv2 keep">${esc(num)}</span><button class="linkbtn gj" type="button" data-bagjump="r${i}" aria-label="Go to its bag">›</button></li>`; };
  return `<div class="glance"><div class="lbl">What you can keep today</div><ul>${keep.map(li).join('')}</ul><div class="bnote">“In total” means all those fish together, not each.${(() => { const seen = new Map(); keep.forEach(({ r }) => (r.allMembers || r.members).forEach(S => seen.set(S, (seen.get(S) || 0) + 1))); const dup = [...seen].filter(([, n]) => n > 1).map(([S]) => lc(spName(S))); return dup.length ? ` ${cap(join(dup))} ${dup.length > 1 ? 'count' : 'counts'} toward more than one line: all of them apply.` : ''; })()} Tap › for the details.</div></div>`;
}
const namedIn = r => { const m = clean(r.verbatim).match(/includ\w*:?\s*(.+)$/i); return m ? m[1].replace(/\s*·\s*/g, ', ').replace(/\.$/, '') : ''; };
// what goes back today, for every fish, in one place: the first thing anyone should read
function throwStrip(){
  const rel = [], shut = [];
  MODEL.spp.forEach(S => { const m = mainRes(MODEL.R[S].H, MODEL.R[S].W); if (!m) return; if (m.status === 'release') rel.push(S); if (m.status === 'closed') shut.push(S); });
  const open = MODEL.rows.filter(r => !r.pool && !r.members.length).map(r => ({ r, t:rowTitle(r), n:namedIn(r.win) || (/listed below/i.test(r.win.verbatim) ? 'the list in the regulations' : '') }));
  const nm = a => a.sort((x, y) => spName(x).localeCompare(spName(y))).map(S => `<button class="tfish" type="button" data-fish="${S}">${esc(spName(S))}</button>`).join('');
  const openTxt = k => open.filter(o => o.r.kind === k).map(o => `<span class="tfish">${esc(o.t)}${o.n ? ` <small>(${esc(o.n)})</small>` : ''}</span>`).join('');
  if (!rel.length && !shut.length && !open.length) return '';
  return `<div class="tstrip">${rel.length || openTxt('release') ? `<div class="trow rel"><div class="thead"><span class="ttl">Carefully release every one</span><span class="pill release">Release</span></div><div class="tfishes">${nm(rel)}${openTxt('release')}</div></div>` : ''}${shut.length || openTxt('closed') ? `<div class="trow closed"><div class="thead"><span class="ttl">Don’t fish for these</span><span class="pill closed">Closed</span></div><div class="tfishes">${nm(shut)}${openTxt('closed')}</div><div class="bnote">Don’t try to catch them. If one bites anyway, carefully release it.</div></div>` : ''}</div>`;
}
function fishGuide(){
  const fams = Object.values(FAMS);
  if (!fams.length) return '';
  const notes = [
    'Trout includes char (Dolly Varden/bull trout, lake trout, brook trout) unless the regulation lists char apart. “Trout and char” always includes both.',
    'A bull trout is counted as a Dolly Varden. The regulations treat them as one fish.',
    'A fish in a group counts toward the group’s total, even when it also has its own smaller limit.',
    'A fish the book gives its own, larger number is counted apart: it doesn’t use up the group’s total.',
        ...(PLACE.part.anadromous_rainbow ? ['On this water a rainbow over 50 cm is a steelhead, and the steelhead rules apply to it.'] : []),
    'Salmon are not covered here yet.'
  ];
  return `<details class="fishguide"><summary>What each fish looks like</summary>${fams.map(f => `<div class="famrow"><b>${esc(f.name)}</b><div class="thumbs">${f.members.map(S => hasPic(S) ? `<button class="thumb" type="button" data-fish="${S}">${FISHART.draw(S, { marks:false })}<span>${esc(spName(S))}</span></button>` : `<span class="thumb none"><span>${esc(spName(S))}</span></span>`).join('')}</div></div>`).join('')}<ul>${notes.map(n => `<li>${esc(n)}</li>`).join('')}</ul></details>`;
}
const slots = (n, cls='') => `<span class="slots">${`<i class="slot ${cls}"></i>`.repeat(Math.min(n, 10))}</span>`;
function renderDiagram(){
  const ctx = settle(PLACE, state.md);
  let h = waterStrip(PLACE, state.md) + closedBanner();
  if (MODEL.broad.length){ document.getElementById('diagram').innerHTML = h; return; }
  h += inPartHtml(ctx, ['retention','access']) + sideHtml(ctx, ['retention','access']) + anglerClosures(ctx) + timedHtml(ctx);
  h += fishGuide() + FISH_LEGEND;
  const unc = uncertainHtml(['retention','access']);
  const rel = [], cls = [], bags = [];
  const top = h; h = '';
  MODEL.rows.forEach((row, i) => {
    const id = 'r' + i;
    if (!row.pool){ const named = !row.members.length ? (namedIn(row.win) || (/listed below/i.test(row.win.verbatim) ? 'the list in the regulations' : '')) : '';
      h += `<div class="bag" data-row="${id}"><div class="bh2"><div><b>${esc(rowTitle(row))}</b>${named ? `<div class="bnote">${esc(named)}</div>` : ''}</div><span class="pill ${row.kind}">${row.kind === 'closed' ? 'Closed' : 'Release'}</span></div>
        <div class="bnote">${row.kind === 'closed' ? 'Don’t fish for them. If one bites anyway, carefully release it.' : 'You can fish for them, but carefully release every one.'}${row.win.wins.length ? ' ' + esc(cap(winTxt(row.win).slice(2))) + '.' : ''}</div>
        ${row.members.length > 1 || (row.members[0] && hasPic(row.members[0])) ? `<div class="fcs">${row.members.map(S => `<button class="fc ${row.kind}" type="button" data-fish="${S}"><span>${hasPic(S) ? '<i class="eye" aria-hidden="true"></i>' : ''}${esc(spName(S))}</span></button>`).join('')}</div>` : ''}<button class="linkbtn srcl" type="button" data-src="${id}">Where this comes from ›</button></div>`; return; }
    const { conds, hw, keepLine, extra, n } = rowConds(row);
    h += `<div class="bag" data-row="${id}">
      <div class="bh2"><div><b>${esc(rowTitle(row))}</b>${scopeBadge(row)}</div><div class="bnum">${row.kind === 'nolimit' ? '<span class="nl">No limit</span>' : `${n}<small>a day</small>`}</div></div>
      <div class="keeprow">${row.kind === 'keep' && n <= 10 ? slots(n) : ''}<span class="keeptxt">${esc(keepLine)}</span></div>
      ${ruleCards(row, { conds, hw, n }, 'b' + i, false)}
      ${fishChips(row)}${apartNote(row)}
      ${countBox(row)}
      ${PLACE.part.anadromous_rainbow && row.members.includes('ST') ? `<div class="bnote"><b>On this water a rainbow trout over 50 cm counts as a steelhead.</b></div>` : ''}
      ${extra.map(t => `<div class="bnote">${esc(t)}</div>`).join('')}${liftNoteHtml(row)}
      ${miniStrip(PLACE, row.members, state.md)}
      <button class="linkbtn srcl" type="button" data-src="${id}">Where this comes from ›</button></div>`;
  });
  const listBag = (items, title, cl, sub) => items.length ? `<div class="bag ${cl}"><div class="bh"><b>${title}</b></div>${sub ? `<div class="bnote">${sub}</div>` : ''}<div class="back">${items.map(({ row, id }) => { const t = rowTitle(row), all = Names(row.members, ' and '); const named = !row.members.length ? namedIn(row.win) : ''; return `<button class="tb ${cl === 'closedbag' ? 'closed' : ''}" type="button" data-src="${id}">${esc(t)}${row.members.length > 1 && all !== t ? ` <small>(${esc(all)})</small>` : ''}${named ? ` <small>(${esc(named)})</small>` : ''}${row.win.wins.length ? ' · ' + esc(winTxt(row.win).slice(2)) : ''}</button>`; }).join('')}</div><div class="muted small">Tap a name for its source.</div></div>` : '';
  h = top + h + unc + standingHtml(ctx) + possessionHtml(ctx);
  document.getElementById('diagram').innerHTML = h;
}

/* ---------- sources ---------- */
const ROLE = { governs:'Sets the number', contains:'The group total this counts toward', narrows:'Lowers the number', limit:'A limit inside the total', floor:'Size limit', season:'Yearly limit', duty:'Something you must do', possession:'Possession limit',
  also:'Also applies (a different limit)', replaced:'Beaten by', agrees:'Says the same as', falls:'Falls away with', moot:'Doesn’t matter today because of', lifted:'Lifted by' };
const WINS = ['also','governs','contains','narrows','limit','floor','season','duty','possession'];
function rowSources(row){
  const ctx = settle(PLACE, state.md), agg = new Map();
  for (const S of row.allMembers){ const R = MODEL.R[S]; if (!R) continue;
    for (const res of [R.H, R.W]) if (res) for (const [k, v] of res.roles){ const key = k + '|' + v.role + '|' + (v.by || ''); if (!agg.has(key)) agg.set(key, { r:res.roles && ctx.active.find(x => x.key === k) || RULES[k], role:v.role, by:v.by, spp:new Set() }); agg.get(key).spp.add(S); } }
  for (const [k, by] of ctx.lifted) if (RULES[k] && RULES[k].species.some(s => row.allMembers.includes(s))) agg.set(k + 'lift', { r:RULES[k], role:'lifted', by, spp:new Set() });
  const best = new Map(); [...agg.values()].sort((a, b) => WINS.includes(b.role) - WINS.includes(a.role)).forEach(it => { if (!best.has(it.r.key)) best.set(it.r.key, it); });
  return [...best.values()].sort((a, b) => (WINS.includes(b.role) - WINS.includes(a.role)) || (a.r.rank ?? a.r.baseRank) - (b.r.rank ?? b.r.baseRank));
}
function srcCard(r, roleTxt, won, extra){
  const rk = RANK[String(r.rank ?? r.baseRank ?? r.prov?.rank ?? 4)] || RANK['4'];
  return `<div class="sc${won === false ? ' lost' : ''}"><span class="badge" style="background:var(${rk.c})">${rk.t}${r.via === 'trib' ? ' · via tributary' : ''}</span><div class="scq">“${esc(clean(r.verbatim))}”</div>
    <div class="muted small">${esc(r.prov?.who || r.prov?.entry_name || '')}</div>${roleTxt ? `<div class="scrole ${won ? 'won' : ''}">${esc(roleTxt)}</div>` : ''}${(r.notes || []).map(n => `<div class="note">⚑ ${esc(n)}</div>`).join('')}${extra || ''}</div>`;
}
const fieldsPre = f => `<details class="fraw"><summary>Fields</summary><pre class="gjson">${esc(JSON.stringify(f, null, 1))}</pre></details>`;
const scrim = document.getElementById('scrim');
function openSheet(title, sub, body){ document.getElementById('shTitle').textContent = title; document.getElementById('shSub').textContent = sub; document.getElementById('shBody').innerHTML = body; scrim.hidden = false; document.getElementById('shClose').focus(); }
function openRowSources(id){
  const row = MODEL.rows[+id.slice(1)]; if (!row) return;
  const items = rowSources(row), won = items.filter(i => WINS.includes(i.role)), lost = items.filter(i => !WINS.includes(i.role));
  const card = it => { let t = ROLE[it.role] || it.role; if (it.role === 'governs' && it.r.k === 'gate') t = it.r.f.may_target === false ? 'Closes it' : 'Release every one'; if (it.by && RULES[it.by]) t += ` “${clean(RULES[it.by].verbatim)}”`; if (it.spp.size && it.spp.size < row.allMembers.length) t += ` · for ${lcNames([...it.spp])}`; return srcCard(it.r, t, WINS.includes(it.role), fieldsPre(it.r.f)); };
  openSheet(rowTitle(row), `${PLACE.name} · ${fmtMd(state.md)}. The more specific rule speaks first: this water, then from downstream, a named area, the region, then the province.`,
    `<div class="lbl">These decide it (${won.length})</div>${won.map(card).join('')}` + (lost.length ? `<details class="lostlist"><summary class="lbl">Overruled (${lost.length})</summary>${lost.map(card).join('')}</details>` : ''));
}
function openRule(key){ const r = PLACE.cands.find(x => x.key === key) || RULES[key] || LIC[key]; if (!r) return; openSheet('Source', r.prov?.entry_name || '', srcCard(r, r.label, null, `<div class="muted small">${esc(key)}</div>` + fieldsPre(r.fields || r.f))); }
/* ================= Today's card: ledger layout (overrides the earlier summary/row functions) =================
   One row per daily limit. Top: the group limit (how many, how the count works, rules for every fish).
   Then, on a rail, each fish's own rules with a small ruler of the sizes it can be kept at.
   Bottom: "How this was decided", a short ladder of the rules that set each part. */
const levelTag = r => { const k = r.rank ?? r.baseRank ?? 4; if (k === 4) return 'B.C. rule'; if (k === 3){ const m = (r.entry_id || '').match(/^z(\d+)/); return m ? `Region ${m[1]} rule` : 'Region rule'; } if (k === 2) return 'Named area rule'; if (k === 1) return 'Downstream rule'; if (k === -1) return 'Federal or park rule'; return 'This water’s rule'; };
const levelHead = k => k === 4 ? 'All of B.C.' : k === 3 ? 'The region' : k === 2 ? 'A named area' : k === 1 ? 'Downstream waters' : k === -1 ? 'Federal law' : PLACE.name || 'This water';
const ICON = {
  fin:'<svg class="ic" viewBox="0 0 20 20" aria-hidden="true"><path d="M2 11c3-4 8-5 12-3l4-3v10l-4-3c-4 2-9 1-12-1z" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linejoin="round"/><path d="M11 7.5l1.5-2.5" stroke="currentColor" stroke-width="1.6"/></svg>',
  ruler:'<svg class="ic" viewBox="0 0 20 20" aria-hidden="true"><rect x="1.5" y="6.5" width="17" height="7" rx="1.5" fill="none" stroke="currentColor" stroke-width="1.6"/><path d="M5 6.5v3M8 6.5v2M11 6.5v3M14 6.5v2" stroke="currentColor" stroke-width="1.4"/></svg>',
  cal:'<svg class="ic" viewBox="0 0 20 20" aria-hidden="true"><rect x="2.5" y="4" width="15" height="13" rx="2" fill="none" stroke="currentColor" stroke-width="1.6"/><path d="M2.5 8h15M7 2.5v3M13 2.5v3" stroke="currentColor" stroke-width="1.6"/></svg>',
  pen:'<svg class="ic" viewBox="0 0 20 20" aria-hidden="true"><path d="M4 16l1-4 8-8 3 3-8 8z" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linejoin="round"/></svg>',
  hand:'<svg class="ic" viewBox="0 0 20 20" aria-hidden="true"><path d="M6 10V5a1.3 1.3 0 012.6 0v4V3.5a1.3 1.3 0 012.6 0V9V4.5a1.3 1.3 0 012.6 0V11c0 4-2 6-5 6s-4.5-2-6-5l-1-2a1.2 1.2 0 012-1.2z" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round"/></svg>',
  water:'<svg class="ic" viewBox="0 0 20 20" aria-hidden="true"><path d="M2 8c2.5-2 5.5 2 8 0s5.5 2 8 0M2 13c2.5-2 5.5 2 8 0s5.5 2 8 0" fill="none" stroke="currentColor" stroke-width="1.6"/></svg>',
};
// "Brook trout, brown trout, cutthroat trout and rainbow trout" -> "Brook, brown, cutthroat and rainbow trout"
function shortNames(members){
  const ns = members.map(S => spName(S).replace(/\//g, ', '));
  if (ns.length < 2) return ns[0] || '';
  const last = w => w.split(' ').pop(), tail = last(ns[0]);
  const same = ns.every(n => n.includes(' ') && last(n) === tail && !n.includes(','));
  const parts = same ? ns.map((n, i) => i === ns.length - 1 ? lc(n) : lc(n.replace(new RegExp(' ' + tail + '$'), ''))) : ns.map(lc);
  const out = parts.length > 1 ? parts.slice(0, -1).join(', ') + ' and ' + parts[parts.length - 1] : parts[0];
  return cap(out);
}
// a condition sentence split into a short head, chips (dates, water) and one muted sub-line
function condParts(t){
  let x = t.trim(), sub = '', chips = [];
  const nm = x.match(/^Only (\d+) can be (over|under) (\d+) cm \((.+?)\)\.$/);
  if (nm) return { head:`Only ${nm[1]} ${nm[4].replace(/ and /g, ' or ')} ${nm[2]} ${nm[3]} cm`, sub:'', chips, ic:'ruler' };
  let m = x.match(/^Only (\d+) fish (over|under) (\d+) cm that (?:isn't|isn’t) an? (.+?)\.$/);
  if (m) return { head:`Only ${m[1]} ${m[2]} ${m[3]} cm`, sub: /steelhead/i.test(m[4]) ? 'Steelhead don’t count' : `Not counting ${m[4]}`, chips, ic:'ruler' };
  const cov = x.match(/It doesn[’']t cover (.+?): they have their own bag\.?/); if (cov){ sub = `${cap(cov[1])} don’t count toward this`; x = x.replace(cov[0], ''); }
  const wat = x.match(/This rule is for (streams|lakes)\.?/); if (wat){ chips.push({ ic:'water', t:`${cap(wat[1])} only` }); x = x.replace(wat[0], ''); }
  const dt = x.match(/, ((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d+[–-](?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)?[a-z]* ?\d+)/); if (dt){ chips.push({ ic:'cal', t:dt[1] }); x = x.replace(dt[0], ''); }
  x = x.replace(/\s+/g, ' ').trim().replace(/\.$/, '');
  return { head:x, sub, chips, ic: /cm/.test(x) ? 'ruler' : null };
}
const chipHtml = cs => cs.length ? `<ul class="chips">${cs.map(c => `<li>${c.ic ? ICON[c.ic] : ''}${esc(c.t)}</li>`).join('')}</ul>` : '';
function extraChips(extra){
  return extra.map(t => /^(hatchery|wild) only/i.test(t) ? { ic:'fin', t:cap(t.match(/^(hatchery|wild) only/i)[0]) } : /a year/.test(t) ? { ic:'cal', t:cap(t.replace(/^(\d+) over (\d+) cm a year$/, '$1 a year over $2 cm')) } : /record/i.test(t) ? { ic:'pen', t: /over \d+/.test(t) ? cap(t.replace('record ones', 'Record ones')) + ' on your licence' : 'Record it on your licence' } : /stop fishing/i.test(t) ? { ic:'hand', t:'Stop fishing at limit' } : { ic:null, t:cap(t) });
}
// rule labels shortened for the ladder; the exact words stay in the rule sheet
function shortLabel(label, row){
  let x = clean(label || '');
  const t = rowTitle(row); if (t && x.toLowerCase().startsWith(t.toLowerCase() + ' — ')) x = x.slice(t.length + 3);
  return cap(x.replace(/ per day/g, ' a day').replace(/, all species combined/, '').replace(/\(none under (\d+) cm\)/, '$1 cm+').replace(/\(no more than (\d+) over (\d+) cm\)/, ': max $1 over $2 cm').replace(/possession quota is 2 daily quotas/, 'hold up to 2 days’ limits').replace(/ — /g, ': ').replace(/\s+:/g, ':').replace(/\s+/g, ' ').trim());
}
const friendlyWhy = s => !s ? '' : /^[a-z_]+:/.test(s) ? 'the regulations don’t say exactly where' : s;

// the fish in a row, grouped as the card shows them, with their own rules in plain words
function speciesItems(row, rc){
  const n = rc.n, withWho = rc.conds.filter(c => c.who), used = new Set();
  const COVER = ['cap','rel','subcap','origin','outercap','outersize','tnote','partly'];
  let items = row.groups.map((g, gi) => { const cs = withWho.filter(c => sameSet(c.who, g.members)); cs.forEach(c => used.add(c)); return { members:g.members, cs, facts:g.facts, key:'g' + gi }; });
  items = items.concat(withWho.filter(c => !used.has(c)).map((c, i) => ({ members:c.who, cs:[c], facts:null, key:'x' + i })));
  items.forEach(it => {
    const others = [...new Set((it.facts || []).filter(l => !it.cs.length || !COVER.includes(l.t)).map(l => fact(l, row).short).filter(Boolean).filter(x => x !== 'must-do'))];
    it.sub = it.cs.find(c => c.sub != null && c.c === 'group');
    it.back = it.cs.find(c => c.c === 'back');
    it.extra = [...(it.cs.some(c => /hatchery only|wild only/.test(c.ph || '')) ? [cap((it.cs.find(c => /hatchery only|wild only/.test(c.ph || '')).ph.match(/(hatchery|wild) only[^,]*/) || [''])[0])] : []), ...(it.back ? [] : others.map(cap))];
    it.lines = [...new Set([...it.cs.map(c => c.ph != null ? c.ph : c.t).filter(Boolean).map(cap), ...(it.cs.some(c => c.c === 'back') ? [] : others.map(cap))])];
    if (it.sub) it.lines.push(`Only ${it.sub.sub} of your ${n}${it.members.length > 1 ? ' can be these' : ''}`);
  });
  // a fish whose only line points elsewhere ("over 50 cm = steelhead") folds into the group it belongs to
  items = items.filter(it => {
    if (!it.facts || it.cs.length || !it.facts.every(l => ['steel','tnote'].includes(l.t))) return true;
    const home = items.find(o => o !== it && it.members.every(S => o.members.includes(S)));
    if (!home) return true;
    if (it.facts.some(l => l.t === 'steel')){ home.lines.push(`A ${lc(spName(it.members[0]))} over 50 cm counts as a steelhead`); (home.notes = home.notes || []).push('Rainbows over 50 cm count as steelhead'); }
    return false;
  });
  items = items.filter(it => it.lines.length).sort((a, b) => b.members.length - a.members.length);
  // steelhead are sea-run rainbows: their line sits right after the line with rainbow trout
  const sti = items.findIndex(it => it.members.length === 1 && it.members[0] === 'ST'), rbi = items.findIndex(it => it.members.includes('RB'));
  if (sti >= 0 && rbi >= 0 && sti !== rbi + 1){ const [st] = items.splice(sti, 1); items.splice(items.findIndex(it => it.members.includes('RB')) + 1, 0, st); }
  const seen = new Set(items.flatMap(it => it.members));
  const rest = row.allMembers.filter(S => !seen.has(S) && MODEL.R[S] && KEEPISH(mainRes(MODEL.R[S].H, MODEL.R[S].W)?.status));
  if (rest.length) items.push({ members:rest, cs:[], facts:null, key:'rest', lines:['No extra rules'], extra:[], plain:true });
  return items;
}
// the sizes each fish can be kept at, drawn on one shared scale per row
function keepRange(members){
  const steelWater = !!PLACE.part?.anadromous_rainbow;
  let lo = 0, hi = Infinity, any = false;
  for (const S of members){ const m = mainRes(MODEL.R[S]?.H, MODEL.R[S]?.W); if (!m || !KEEPISH(m.status)) continue; any = true;
    m.lines.filter(l => l.t === 'rel').forEach(l => { if (l.a === 0) lo = Math.max(lo, l.b); if (l.b === Infinity) hi = Math.min(hi, l.a); });
    if (S === 'ST' && steelWater && lo < 50) lo = 50; }
  return any ? { lo, hi } : null;
}
function quotaLines(it, row, rc){
  if (row.kind !== 'keep' || it.back || (it.facts || []).some(l => l.t === 'xref')) return null;
  const r = keepRange(it.members); if (!r) return null;
  const n = rc.n; let base = Math.min(n, it.sub ? it.sub.sub : n);
  // a share of the day for some of these fish on this kind of water (e.g. only 1 trout from streams)
  const nw = row.narrow; if (nw && nw.f.water === PLACE.kind && inDates(nw.wins, state.md) && it.members.every(S => covers(nw, S) && MODEL.R[S] && ['H', 'W'].some(o => MODEL.R[S][o]?.roles.get(nw.key)?.role !== 'lifted'))) base = Math.min(base, nw.f.take);
  // "only N of them can be …" caps that cover every fish in this item bound it too
  [...(it.facts || []), ...row.everyone].filter(l => ['outercap','subcap'].includes(l.t) && l.r?.f?.take != null && it.members.every(S => (l.members ? l.members.has(S) : true) && covers(l.r, S))).forEach(l => { base = Math.min(base, l.r.f.take); });
  const own = (it.facts || []).filter(l => l.t === 'cap' && l.take != null);
  const gen = row.everyone.filter(l => l.t === 'cap' && l.take != null && !own.some(o => o.a === l.a && o.b === l.b));
  const caps = [...own, ...gen];
  const pts = new Set([r.lo, r.hi]); caps.forEach(c => { [c.a, c.b].forEach(v => { if (v != null && v > r.lo && v < r.hi) pts.add(v); }); });
  const P = [...pts].sort((a, b) => a - b), bands = [];
  for (let i = 0; i < P.length - 1; i++){ const a = P[i], b = P[i + 1], mid = b === Infinity ? a + 1 : (a + b) / 2;
    const mx = Math.min(base, ...caps.filter(c => mid >= (c.a ?? 0) && mid <= (c.b ?? Infinity)).map(c => c.take));
    const last = bands[bands.length - 1]; if (last && last.mx === mx) last.b = b; else bands.push({ a, b, mx }); }
  const txt = (a, b, first) => a === 0 && b === Infinity ? 'any size' : a === 0 ? `up to ${b} cm` : b === Infinity ? (first ? `${a} cm or longer` : `over ${a} cm`) : `${a}–${b} cm`;
  return bands.map((x, i) => ({ a:x.a, b:x.b, mx:x.mx, t:txt(x.a, x.b, i === 0) }));
}
function miniRuler(r, mx, mark){
  if (!r || (!r.lo && r.hi === Infinity && !mark)) return '';
  const pc = v => Math.min(100, v / mx * 100).toFixed(1), hi = r.hi === Infinity ? mx : r.hi;
  const txt = r.lo && r.hi !== Infinity ? `Keep ${r.lo}–${r.hi} cm` : r.lo ? `Keep ${r.lo} cm or longer` : r.hi !== Infinity ? `Keep up to ${r.hi} cm` : 'Any size';
  return `<div class="mr" role="img" aria-label="${txt}"><div class="mrt"><i class="fill" style="left:${pc(r.lo)}%;width:${(pc(hi) - pc(r.lo)).toFixed(1)}%"></i>${mark ? `<i class="mark" style="left:${pc(mark)}%"></i>` : ''}</div><div class="mrl"><span>0</span>${r.lo ? `<b style="left:${pc(r.lo)}%">${r.lo}</b>` : ''}${r.hi !== Infinity ? `<b style="left:${pc(r.hi)}%">${r.hi}</b>` : ''}${[r.lo, r.hi].some(v => v && v !== Infinity && v / mx > .6) ? '' : `<span>${mx}+ cm</span>`}</div></div>`;
}
const plainFact = (l, row) => { const f = fact(l, row), rs = l.rules || [l.r];
  return `<li><button class="pline" type="button" data-rule="${esc(rs[0].key)}"><span>${esc(f.long)}</span><small>${esc(levelTag(rs[0]))}</small></button></li>`; };

// Region allows N, but when every kind has its own smaller cap the real most is their sum (Shuswap: 1 rainbow + 1 char = 2)
const VAR = (() => { try { return new URLSearchParams(location.search).get('v') || 'C'; } catch (e) { return 'C'; } })();
function effCap(row, rc){
  if (row.kind !== 'keep' || !row.pool) return null;
  const n = row.daily, items = speciesItems(row, rc); if (!items.length) return null;
  const all = items.flatMap(it => it.members); if (new Set(all).size !== all.length) return null;   // size bands overlap kinds: no simple sum
  const caps = items.map(it => { const q = quotaLines(it, row, rc), xref = (it.facts || []).find(l => l.t === 'xref');
    const lo = q ? q[0].a : xref ? Math.min(...it.members.map(S => Math.min(Infinity, ...originLines(S).filter(x => !x.rel).map(x => x.lo || 0)))) : 0;
    return { it, lo, c: q ? Math.min(n, Math.max(...q.map(x => x.mx))) : xref ? Math.min(xref.daily, n) : it.back ? 0 : n }; });
  const capped = caps.filter(x => x.c < n && x.c > 0), open = caps.filter(x => x.c >= n);
  let sum = capped.reduce((a, x) => a + x.c, 0), big = '';
  // a shared "only K over X cm" binds kinds whose every keepable fish is over X (Shuswap: rainbow 50+, char 60+, only 1 over 50)
  const gc = rc.conds.find(c => c.c === 'cap' && !c.who && !/\(/.test(c.t) && /^Only \d+ (?:fish |can be )?over \d+ cm/.test(c.t)), gm = gc && gc.t.match(/^Only (\d+) (?:fish |can be )?over (\d+) cm/);
  if (gm){ const K = +gm[1], X = +gm[2], over = capped.filter(x => x.lo >= X && !(x.it.members.includes('ST') && isSteelRiver())), os = over.reduce((a, x) => a + x.c, 0);
    if (over.length > 1 && os > K){ sum -= os - K; big = ` Every one you could keep is over ${X} cm, and only ${K} fish over ${X} cm is allowed.`; } }
  if (!capped.length || sum >= n && !open.length) return null;
  return { n, big, rb: capped.some(x => x.it.members.includes('RB')), all: !open.length, sum: open.length ? null : sum, cappedSum: sum, capped: (ps => ps.some(t => / or |, /.test(t)) ? lcNames(capped.flatMap(x => x.it.members), ' or ') : join(ps, ' or '))(capped.map(x => lcNames(x.it.members, ' or '))), open: (o => o.length > 3 ? 'other trout or char' : lcNames(o, ' or '))(open.flatMap(x => x.it.members)) };
}
function groupBlock(row, rc){
  const s = scopeOf(row.pool), n = row.daily, facts = [];
  const gf = (num, main, sub) => `<div class="gf"><div class="gdt">${num}</div><div class="gdd">${main}${sub ? `<span>${sub}</span>` : ''}</div></div>`;
  const num = k => `<b>${k}</b><small>a day</small>`;
  let ex = '';
  const tot = row.kind === 'keep' && n > 1 && row.allMembers.length > 1 ? `<b>${n} a day in total, all kinds together.</b> ` : '';
  if (row.kind === 'nolimit') facts.push(gf('<b>∞</b>', 'No daily limit'));
  else if (nar0(row) && row.narrow.f.water === PLACE.kind){
    const wk = row.narrow.f.water, other = wk === 'stream' ? 'lakes' : 'streams', b = row.pool.f.take;
    facts.push(`<p class="gline">${tot}Shared with every ${esc(s)} ${wk} you fish today.</p>`);
    ex = `<ul class="exs"><li><span>Kept ${n} on another ${wk}?</span> <strong>Keep 0 here</strong></li><li class="sep"><span>Kept some at a ${other.replace(/s$/, '')}?</span> <strong>Those don’t count here, but stop at ${b} for the whole day</strong></li></ul>`;
  } else if (s){ const lakeToo = !row.pool.f.water; facts.push(`<p class="gline">${tot}Shared with everywhere in ${esc(s)} you fish today${lakeToo ? ', lakes and streams alike' : ''}.</p>`);
    if (row.kind === 'keep' && n > 1){ const k = n > 2 ? 2 : 1; ex = `<ul class="exs"><li><span>Kept ${k} elsewhere today?</span> <strong>Keep ${n - k} more here</strong></li></ul>`; } }
  else facts.push(`<p class="gline">${tot}Only fish kept on this ${PLACE.kind === 'stream' ? 'river' : 'lake'} count.</p>`);
  const ec = effCap(row, rc);
  if (ec && ec.all) facts.push(`<p class="gline warnline"><b>Really ${ec.sum} a day here.</b> ${esc(scopeOf(row.pool) || 'The region')} allows ${n}, but each kind below has its own smaller limit.</p>`);
  else if (ec && ec.cappedSum < n && ec.rb) facts.push(`<p class="gline warnline"><b>Only ${ec.cappedSum} of the ${n} can be ${esc(ec.capped)}.</b><span class="wsub">${esc(ec.big.trim())} The other ${n - ec.cappedSum} would have to be ${esc(ec.open)}.</span></p>`);
  const conds = [], oth = [];
  const org = rc.conds.find(c => c.c === 'origin' && !c.who);
  const H = org && /^Hatchery/.test(org.t);
  if (org && VAR === 'B') conds.push(`<li>${ICON.fin}<div><b>${H ? 'Hatchery fish only' : 'Wild fish only'}</b> <span class="in">· release every ${H ? 'wild' : 'hatchery'} one</span> <button class="linkbtn" type="button" data-howtell="1">How to tell ›</button></div></li>`);
  if (org && VAR === 'C') conds.push(`<li><i class="ck">✓</i><div><b>${H ? 'Hatchery only' : 'Wild only'}</b><span class="in">, release ${H ? 'wild' : 'hatchery'} ones</span> <button class="linkbtn" type="button" data-howtell="1">How to tell ›</button></div></li>`);
  const exemptOf = c => { if (!c.r || !['cap','outersize'].includes(c.c)) return [];
    if (/\(/.test(c.t)) return [];
    return [...new Set([...row.allMembers, ...row.groups.flatMap(g => g.members)])].filter(S => covers(c.r, S)).filter(S => { const rs = ['H', 'W'].map(o => MODEL.R[S]?.[o]).filter(r => r && KEEPISH(r.status)); return rs.length && rs.every(r => ['lifted', undefined].includes(r.roles.get(c.r.key)?.role) && !r.lines.some(l => l.r === c.r)); }); };
  const seenT = new Set();
  rc.conds.filter(c => !c.who && c.c !== 'origin').forEach(c => { if (seenT.has(c.t)) return; seenT.add(c.t); const t = c.t; const p = condParts(t);
    const ex = exemptOf(c); if (ex.length && !p.sub) p.sub = `Not counting ${lcNames(ex, ' or ')}`;
    if (p.sub === 'Steelhead don’t count') p.sub = 'Not counting steelhead';
    // on a steelhead river every rainbow over 50 cm is a steelhead, which has its own rule
    if (/^Not counting /.test(p.sub || '')) p.sub = p.sub.replace(/^Not counting (rainbow trout or steelhead|steelhead or rainbow trout)$/, 'Not counting steelhead').replace(/^Not counting (.+)$/, '$1 don’t count');
    const sub = [p.chips.map(c => c.t).join(' · '), p.sub].filter(Boolean).join('. ');
    const hd = p.head.replace(/^Only (\d+) (can be )?over/, 'Only $1 fish over');
    const gen = c.c === 'cap' && !/\(/.test(t);
    (gen || VAR === 'B' ? conds : oth).push(VAR === 'B' ? `<li>${p.ic ? ICON[p.ic] : '<span class="ic"></span>'}<div><b>${esc(hd)}</b>${sub ? `<span class="in">, ${esc(sub)}</span>` : ''}</div></li>`
      : `<li>${gen ? '<i class="ck">✓</i>' : p.ic ? ICON[p.ic] : '<span class="ic"></span>'}<div><b>${esc(gen && /^Only \d+ fish over/.test(hd) && !/ (total|all)/.test(hd) ? hd.replace(/ fish over (\d+ cm)$/, ' fish over $1 in total') : !gen && /^\(?(.+?)\)?$/.test(sub) && /^Only \d+ fish over/.test(hd) ? hd.replace(/ fish over/, ' ' + sub.replace(/[()]/g, '') + ' over') : hd)}</b>${sub && !(!gen && /^Only \d+ fish over/.test(hd)) ? `<span class="in">${/don’t count$/.test(sub) ? ` (${esc(sub)})` : ', ' + esc(sub)}</span>` : ''}</div></li>`); });
  const apart = MODEL.rows.filter(r => r !== row && r.pool && row.pool && r.members.length && r.members.every(S => { const m = mainRes(MODEL.R[S]?.H, MODEL.R[S]?.W); return m && m.roles.get(row.pool.key)?.role === 'lifted'; }));
  apart.forEach(r => (VAR === 'B' ? conds : oth).push(`<li><span class="ic"></span><div><b>${esc(rowTitle(r))}: own limit of ${r.daily} a day</b><span class="sub">Not part of these ${n}. See its row below.</span></div></li>`));
  const sp = (row.allMembers.includes('ST') || (row.allMembers.includes('RB') && !MODEL.rows.some(r => r.allMembers.includes('ST')))) ? steelPresence() : '';
  return `<section class="gblock">${facts.join('')}${ex}${conds.length ? (VAR === 'B' ? `<ul class="gconds">${conds.join('')}</ul>` : `<div class="gfor"><div class="lbl">${VAR === 'C' ? 'For every kind' : 'Across all kinds'}</div><ul class="gconds v2">${conds.join('')}</ul></div>`) : ''}${oth.length ? `<div class="gfor">${conds.length ? '<div class="lbl">Also</div>' : ''}<ul class="gconds v2 oth">${oth.join('')}</ul></div>` : ''}${sp ? `<p class="gquiet">${esc(sp)}</p>` : ''}</section>`;
}
function qbar(q, mx){
  const pc = v => (Math.min(v, mx) / mx * 100), segs = [], lo = q[0].a, hi = q[q.length - 1].b;
  if (lo > 0) segs.push({ a:0, b:lo, rel:true });
  q.forEach(x => segs.push({ a:x.a, b:x.b === Infinity ? mx : x.b, mx:x.mx }));
  if (hi !== Infinity) segs.push({ a:hi, b:mx, rel:true });
  const ticks = [...new Set(segs.flatMap(g => [g.a, g.b]).filter(v => v > 0 && v < mx))];
  let prevUp = false;
  const tick = (v, i, a) => { const up = i > 0 && !prevUp && (v - a[i - 1]) / mx < .12; prevUp = up; return `<b class="${up ? 'up' : ''}" style="left:${pc(v).toFixed(1)}%">${v}</b>`; };
  const alt = q.map(x => `up to ${x.mx} at ${x.t}`).join(', ');
  return `<div class="qb" role="img" aria-label="Keep ${esc(alt)}"><div class="qbt">${segs.map(g => { const w = pc(g.b) - pc(g.a);
    return `<i class="${g.rel ? 'rel' : 'keep'}" style="width:${w.toFixed(1)}%">${g.rel ? (w > 16 ? 'release' : '') : `<em>${g.mx}</em>`}</i>`; }).join('')}</div>
    <div class="qbl">${ticks.map((v, i, a) => tick(v, i, a).replace(`>${v}</b>`, `>${v}${i === a.length - 1 ? ' cm' : ''}</b>`)).join('')}</div></div>`;
}
// the keep sentence: "Keep 1 of the 2 · 60 cm or longer · hatchery only"
const sent = (num, of, segs, o) => { const sg = segs.filter(Boolean); return `<div class="sl">${o ? `<span class="o">${o}</span>` : ''}<span class="k">Keep${of && num >= of ? ' up to' : ''}</span> <b class="n">${num}</b>${of && num < of ? ` <span class="of">of your ${of}</span>` : ''}${sg.length ? `<span class="segs">${sg.map(x => `<span class="seg">${esc(x)}</span>`).join(' · ')}</span>` : ''}</div>`; };
const rngTxt = (lo, hi) => lo && hi !== Infinity ? `${lo}–${hi} cm` : lo ? `${lo} cm or longer` : hi !== Infinity ? `up to ${hi} cm` : 'any size';
// per origin, when hatchery and wild are kept differently (a fish with its own row)
function originLines(S){
  const R = MODEL.R[S]; if (!R) return [];
  const one = (r, o) => { if (!r) return null; if (!KEEPISH(r.status)) return { o, rel:true };
    let lo = Math.max(0, ...r.lines.filter(l => l.t === 'rel' && l.a === 0).map(l => l.b)); const his = r.lines.filter(l => l.t === 'rel' && l.b === Infinity).map(l => l.a);
    if (S === 'ST' && PLACE.part?.anadromous_rainbow && lo < 50) lo = 50;   // under 50 cm it is a rainbow trout here
    if (S === 'RB' && PLACE.part?.anadromous_rainbow && r.lines.some(l => l.t === 'steel')) his.push(50);   // over 50 cm it is a steelhead
    return { o, n:r.daily, lo, hi: his.length ? Math.min(...his) : Infinity }; };
  return [one(R.H, 'Hatchery'), one(R.W, 'Wild')].filter(Boolean);
}
// the export flags the book's steelhead streams (a rainbow over 50 cm is a steelhead there): no page heuristic
function isSteelRiver(){ return !!PLACE.part?.anadromous_rainbow; }
// how sure we are that steelhead are here (export: part.steelhead / water.steelhead_source)
function steelPresence(){
  const s = PLACE.part?.steelhead; if (!s) return '';
  const hasRow = PLACE.part.steelhead_rules !== false && MODEL.rows.some(r => r.allMembers.includes('ST'));
  if (s === 'possible') return hasRow ? 'Steelhead rules apply here; steelhead may not be present in this water.' : '';
  return hasRow ? 'Steelhead are known to be in this water.' : 'Steelhead have been recorded here, but no steelhead rule applies: treat any rainbow, however big, as a rainbow trout.';
}
// "Only 2 at 30–50 cm. Only 1 over 50 cm." reads as 3: a cap that runs on to the top is "of those"
function capLines(q, topN){
  const out = [], used = new Set();
  q.forEach((x, i) => { if (x.mx >= topN || used.has(i)) return; const y = q[i + 1];
    if (y && x.b !== Infinity && y.a === x.b && y.b === Infinity && y.mx < x.mx){ out.push(`Only ${x.mx} over ${x.a} cm, and only ${y.mx} of those over ${y.a} cm.`); used.add(i + 1); return; }
    out.push(`Only ${x.mx} ${/^(over|up to)/.test(x.t) ? x.t : 'at ' + x.t}.`); });
  return out;
}
function speciesBlock(row, rc, id){
  let items = speciesItems(row, rc);
  // fish with no rules of their own add nothing when they're the whole list (bass, whitefish…)
  if (items.length === 1 && items[0].plain) items = [];
  if (!items.length) return '';
  const qs = items.map(it => quotaLines(it, row, rc));
  const pts = qs.filter(Boolean).flatMap(q => q.flatMap(x => [x.a, x.b === Infinity ? 0 : x.b]));
  const top = Math.max(0, ...pts), mx = Math.max(40, Math.ceil(((top || 30) * 1.5) / 10) * 10);
  const org = rc.conds.find(c => c.c === 'origin' && !c.who), gOrigin = org ? (/^Hatchery/.test(org.t) ? 'hatchery only' : 'wild only') : '';
  const n = rc.n, many = n > 1 && row.kind === 'keep' && items.length > 1;
  const body = items.map((it, i) => {
    const stOnly = it.members.length === 1 && it.members[0] === 'ST' && isSteelRiver();
    const q = qs[i], xref = (it.facts || []).find(l => l.t === 'xref');
    let main = '', s2 = [], chips = extraChips((it.extra || []).filter(t => !/^own limit/i.test(t) && !/^(hatchery|wild) only/i.test(t))).filter(c => ['cal','pen','hand'].includes(c.ic));
    const itOrigin = (it.extra || []).find(t => /^(hatchery|wild) only/i.test(t));
    const both = q && it.members.every(S => ['H', 'W'].every(o => KEEPISH(MODEL.R[S]?.[o]?.status)));
    const origin = gOrigin || (itOrigin ? lc(itOrigin.match(/^(hatchery|wild) only/i)[0]) : both && !gOrigin ? 'wild or hatchery' : '');
    if (q){
      const lo = q[0].a, hi = q[q.length - 1].b, topN = Math.max(...q.map(x => x.mx));
      main = sent(topN, topN < n || many ? n : null, [it.members.length === 1 && it.members[0] === 'ST' && isSteelRiver() && lo === 50 && hi === Infinity ? 'over 50 cm' : rngTxt(lo, hi), gOrigin && VAR === 'B' ? '' : origin]);
      capLines(q, topN).forEach(t => s2.push(t));
      if (VAR !== 'B' && !stOnly){ const gc = rc.conds.find(c => c.c === 'cap' && !c.who && !/\(/.test(c.t) && /^Only \d+ (?:fish |can be )?over \d+ cm/.test(c.t)); const gm = gc && gc.t.match(/^Only (\d+) (?:fish |can be )?over (\d+) cm/);
        if (gm && lo >= +gm[2] && (!gc.r || it.members.some(S => covers(gc.r, S)))) s2.push(`All are over ${gm[2]} cm: each one uses your ${gm[1]} fish over ${gm[2]} cm.`); }
      // on a steelhead river a fish under 50 cm is a rainbow, not a small steelhead: no "release under 50"
      if (stOnly && lo <= 50) s2.push('Under 50 cm, it’s a rainbow trout.');
    } else if (xref){
      const S = it.members[0], ol = it.members.length === 1 ? originLines(S) : [];
      const keep = ol.filter(x => !x.rel);
      if (keep.length > 1 && keep.some(x => x.n !== keep[0].n || x.lo !== keep[0].lo || x.hi !== keep[0].hi)) main = keep.map(x => sent(Math.min(x.n, n), n, [rngTxt(x.lo, x.hi)], x.o)).join('');
      else { const x = keep[0]; main = sent(Math.min(xref.daily, n), n, [x ? (S === 'ST' && isSteelRiver() && x.lo === 50 && x.hi === Infinity ? 'over 50 cm' : rngTxt(x.lo, x.hi)) : '', keep.length === 1 && ol.length === 2 ? lc(x.o) + ' only' : keep.length === 2 ? 'wild or hatchery' : '']); }
      const other = MODEL.rows.find(r => r !== row && r.pool && it.members.every(S => r.members.includes(S)));
      if (other){ const orc = rowConds(other); chips = extraChips([...new Set(speciesItems(other, orc).flatMap(o => o.extra || []).concat(other.everyone.map(l => fact(l, other).short).filter(t => t && /a year|record|stop fishing/i.test(t))))]).filter(c => ['cal','pen','hand'].includes(c.ic)); }
      if (VAR !== 'B' && !(S === 'ST' && isSteelRiver())){ const gc = rc.conds.find(c => c.c === 'cap' && !c.who && !/\(/.test(c.t) && /^Only \d+ (?:fish |can be )?over \d+ cm/.test(c.t)), gm = gc && gc.t.match(/^Only (\d+) (?:fish |can be )?over (\d+) cm/);
        const klo = Math.min(...it.members.map(M => Math.min(Infinity, ...originLines(M).filter(x => !x.rel).map(x => x.lo || 0))));
        if (gm && klo !== Infinity && klo >= +gm[2]) s2.push(`All are over ${gm[2]} cm: each one uses your ${gm[1]} fish over ${gm[2]} cm.`); }
      if (ol.some(x => x.rel) && !(keep.length === 1 && ol.length === 2)) s2.push(`<span class="rl">Release:</span> ${ol.filter(x => x.rel).map(x => lc(x.o)).join(', ')} ones.`);
    } else if (it.back){ const sg = strip(PLACE, it.members), never = !sg.some(([st]) => st === 'keep'), un = never ? '' : runUntil(sg, state.md), rt = never ? ('No keeping at any time of year. ' + closedRuns(sg)).trim() : runTxt(sg, state.md, true); if (rt) s2.push(rt);
      main = `<div class="sl"><span class="rl">${it.members.every(S => mainRes(MODEL.R[S]?.H, MODEL.R[S]?.W)?.status === 'closed') ? 'Closed: don’t fish for them' : 'Release every one'}${un ? ' until ' + un : ''}</span>${it.members.includes('ST') && isSteelRiver() ? ' <span class="of">· includes any rainbow over 50 cm</span>' : ''}</div>`; }
    else main = `<div class="sl">${esc(it.lines.join('. '))}.</div>`;
    const nw = row.narrow, share = nw && nw.f.water === PLACE.kind && inDates(nw.wins, state.md) && !nar0(row) && it.members.some(S => covers(nw, S) && ['H', 'W'].some(o => KEEPISH(MODEL.R[S]?.[o]?.status) && MODEL.R[S][o].roles.get(nw.key)?.role !== 'lifted'));
    if (share){ const txt = rc.conds.find(c => c.c === 'streamcap'); const hd = txt ? condParts(txt.t).head.replace(/^Only (\d+) can be an? /, 'only $1 in all: ') : ''; const k = (txt ? condParts(txt.t).head : '').match(/^Only (\d+)/); const own = q ? Math.max(...q.map(x => x.mx)) : null; if (k && own === +k[1]) {} else if (k) s2.push(`Wild ones share the limit of ${k[1]} above.`); else if (hd) s2.push(`Shares ${hd}.`); }
    const topK = q ? Math.max(...q.map(x => x.mx)) : xref ? Math.min(xref.daily, n) : null;
    chips = chips.map(c => c.ic === 'hand' && topK ? { ic:'hand', t:`Stop fishing for the day after ${topK}` } : c);
    const stM = mainRes(MODEL.R.ST?.H, MODEL.R.ST?.W), steelWater = isSteelRiver();
    if (steelWater && it.members.includes('RB') && !(it.notes || []).some(t => /steelhead/.test(t)) && !(it.lines || []).some(t => /counts as a steelhead/.test(t)))
      (it.notes = it.notes || []).push(stM && KEEPISH(stM.status) ? 'Rainbows over 50 cm count as steelhead' : 'Rainbows over 50 cm count as steelhead: release them');
    if (steelWater && it.members.includes('RB')){ const rest = it.members.filter(S => S !== 'RB');
      s2 = s2.map(t => t.replace(/^Only (\d+) over 50 cm\.$/, (m, k) => rest.length ? `Only ${k} ${lc(shortNames(rest)).replace(/ and ([^ ]+ trout)$/, ' or $1')} over 50 cm.` : '')).filter(Boolean); }
    const notes = [...(it.notes || [])].map(t => `<p class="s2">${esc(t)}.</p>`).join('');
    const nx = '';
    const head = `<b class="nm">${esc(shortNames(it.members))}</b>${main}${s2.length ? `<p class="s2">${s2.join(' ')}</p>` : ''}${notes}${chips.length ? `<ul class="mini">${[...new Set(chips.map(c => c.t))].filter((t, _, a) => !(/^\d+ a year$/.test(t) && a.some(u => u !== t && u.startsWith(t + ' ')))).map(t => `<li>${esc(t)}</li>`).join('')}</ul>` : ''}${nx}`;
    if (!it.facts) return `<div class="sp2 static">${head}</div>`;
    const at = t => /^(over|up to|any)/.test(t) ? t : 'at ' + t;
    const resTop = q ? Math.max(...q.map(x => x.mx)) : 0, rLo = q ? q[0].a : 0, rHi = q ? q[q.length - 1].b : Infinity;
    const res = q ? `Keep up to ${resTop}, ${stOnly && rLo === 50 ? 'over 50 cm' : rngTxt(rLo, rHi)}${q.filter(x => x.mx < resTop).map(x => `. Only ${x.mx} ${/^(over|up to)/.test(x.t) ? x.t : 'at ' + x.t}`).join('')}. They count toward your ${n}` : null;
    const ladRow = xref ? (MODEL.rows.find(r => r !== row && r.pool && it.members.every(S => r.members.includes(S))) || row) : row;
    const res2 = res || (it.back ? (it.members.every(S => mainRes(MODEL.R[S]?.H, MODEL.R[S]?.W)?.status === 'closed') ? 'Closed here: don’t fish for them' : 'Release every one here') : xref ? (() => { const ol = it.members.length === 1 ? originLines(it.members[0]).filter(x => !x.rel) : []; const ST1 = it.members[0] === 'ST' && isSteelRiver();
      const one = x => `${Math.min(x.n, n)}${ol.length > 1 ? ' ' + lc(x.o) : ''}, ${ST1 && x.lo === 50 ? 'over 50 cm' : rngTxt(x.lo, x.hi)}`;
      const rg = x => ST1 && x.lo === 50 ? 'over 50 cm' : rngTxt(x.lo, x.hi);
      if (ol.length === 2 && ol[0].n === ol[1].n && rg(ol[0]) === rg(ol[1])) return `Keep up to ${Math.min(ol[0].n, n)}, ${rg(ol[0])}, wild or hatchery. Each one counts toward your ${n}`;
      if (ol.length === 2){ const [a, b] = ol[0].n >= ol[1].n ? ol : [ol[1], ol[0]]; return `Keep up to ${Math.min(a.n, n)}: ${lc(a.o)} ${rg(a)}. Only ${b.n} can be ${lc(b.o)} (${rg(b)}). Each one counts toward your ${n}`; }
      return ol.length ? `Keep up to ${one(ol[0])}${originLines(it.members[0]).length === 2 ? `, ${lc(ol[0].o)} only` : ''}. Each one counts toward your ${n}` : `Each one counts toward your ${n}`; })() : null);
    return `<details class="sp2" data-dd="${id}${it.key}"${ddOpen(id + it.key)}><summary>${head}</summary><div class="minidec"><div class="lbl">How this was decided</div>${ladderHtml(ladRow, id, it.members, res2)}</div></details>`;
  }).join('');
  return `<section class="sblock"><div class="lbl">Each kind${many ? ` <span class="lbl2">· every fish counts toward the ${n}</span>` : ''}${VAR === 'D' && gOrigin ? ` <button class="linkbtn lblbtn" type="button" data-howtell="1">Hatchery or wild? ›</button>` : ''}</div><div class="rail">${body}</div></section>`;
}
// "How this was decided": one short ladder per question, from B.C. down to this water
const SAYS = { governs:'Daily limit', contains:'Daily limit', narrows:'Lower daily limit', also:'Also applies', floor:'Size', limit:'Extra cap', season:'Yearly limit', duty:'Must do', possession:'Carry limit' };
const QORDER = ['governs','contains','narrows','also','floor','limit','season','duty','possession'];
const LOSES = { replaced:'Replaced', agrees:'Same as another rule', falls:'Not needed here', moot:'Doesn’t matter today', lifted:'Replaced' };
const byName = k => { const b = RULES[k]; if (!b) return 'a rule above'; const rk = b.rank ?? b.baseRank ?? b.prov?.rank ?? 4; return rk === 0 ? `this ${PLACE.kind === 'lake' ? 'lake' : 'river'}’s rule` : rk === 3 ? `the ${b.prov?.entry_name || 'region'} rule` : rk === 4 ? 'a B.C.-wide rule' : 'a rule above'; };

// a regulation line said as a plain sentence: "In streams, keep 2 trout and char a day, hatchery only."
function sayRule(r, names){
  const f = r.f || {}, t = r.type;
  if (t === 'stop_fishing_after_quota') return `Once you’ve kept your daily limit of ${f.origin ? f.origin + ' ' : ''}${writtenName(r, ' and ')}, stop fishing this water for the rest of the day.`;
  if (t !== 'retention_limit') return '';
  if (f.extent_text || f.side || f.standing) return '';
  const nm = conj => writtenName(r, conj).replace(/^all /, '').replace(/^Dolly Varden\/bull trout$/, 'Dolly Varden or bull trout').replace(/Dolly Varden\/bull trout/g, 'Dolly Varden, bull trout').replace(conj === ' or ' ? /^trout and char/ : /^$/, 'trout or char');
  let sp = names || nm(' and ');
  const ex = (f.species_except || []).filter(S => !(S === 'CHAR' && sp === 'trout'));
  const exTxt = ex.length && !names ? ` (not ${join(ex.map(S => lc(spName(S))), ' or ')})` : '';
  const many = (f.species || []).length > 1 || /^(trout|char|trout and char|game fish|fish|bass|whitefish|salmon)\b/.test(sp) && !/^trout$/.test(sp) ? true : (f.species || []).some(S => GROUPS[S]);
  const where = [f.water === 'stream' ? 'In streams' : f.water === 'lake' ? 'In lakes' : '', f.tributaries_only ? 'In tributaries' : ''].filter(Boolean)[0] || '';
  const dates = whenDates(f.when).map(([a, b]) => rangeTxt(a, b)).join(', ');
  const org = f.origin ? `${f.origin} ` : '';
  const ln = f.lengths || [], min = ln.find(l => l.min_cm != null && l.take == null), floor = ln.find(l => l.max_cm != null && l.take === 0), ceil = ln.find(l => l.min_cm != null && l.take === 0);
  const tail = [dates].filter(Boolean).join(', ');
  const wrap = x => { let y = (where ? `${where}, ${x.charAt(0).toLowerCase() + x.slice(1)}` : x); if (tail) y += `, ${tail}`; return y + '.'; };
  if (f.while) return '';
  if (f.may_target === false) return wrap(/game fish|^fish/.test(sp) ? 'No fishing' : `No fishing for ${sp}`);
  if (f.per_daily) return wrap(`Don’t carry more than ${f.per_daily} days’ worth of ${sp}`);
  if (f.record_retention) return wrap(`Record each ${org}${sp.replace(/^adult /, 'adult ')}${min ? ` over ${min.min_cm} cm` : ''} on your licence right away`);
  if (f.unlimited) return wrap(`No daily limit for ${sp}`);
  if (f.take === 0) return wrap(`Release every ${org}${sp}`);
  if (f.period === 'annual') return wrap(`Keep up to ${f.take} ${sp}${min ? ` over ${min.min_cm} cm` : ''} a licence year${(f.species || []).length > 1 ? ', all kinds together' : ''}`);
  if (f.take != null && min && !floor) return wrap(`Only ${f.take} ${org}${names ? names.replace(/ and ([^,]+)$/, ' or $1') : nm(' or ')} over ${min.min_cm} cm a day${exTxt}`);
  sp += exTxt;
  const bits = [];
  if (f.take != null) bits.push(`Keep up to ${f.take} ${f.take === 1 && !names ? nm(' or ') + exTxt : sp} a day${many && f.take > 1 ? ', all kinds together' : ''}`);
  else bits.push(`Keep only ${sp}${floor ? ` ${floor.max_cm} cm or longer` : ''}`);
  if (floor && f.take != null) bits.push((f.species || []).join() === 'ST' && floor.max_cm === 50 ? 'over 50 cm' : `${floor.max_cm} cm or longer`);
  if (ceil) bits.push(`none over ${ceil.min_cm} cm`);
  if (f.origin) bits.push(`${f.origin} only`);
  if (bits.length === 1 && f.take == null && !floor) return '';
  return wrap(bits.join(', '));
}
function ladderHtml(row, id, members, res){
  const items = rowSources(row).filter(it => !members || !it.spp.size || [...it.spp].some(S => members.includes(S)));
  if (!items.length && row.win) items.push({ r:row.win, role:'governs', spp:new Set() });
  items.forEach(it => { if (it.r.f?.record_retention && WINS.includes(it.role)) it.role = 'duty'; });
  const Q = [['What you can keep', ['governs','contains','narrows','also']], ['Size', ['floor']], ['Other limits', ['limit']], ['Other rules', ['season','duty','possession']]];
  const forTxt = it => it.spp.size && it.spp.size < row.allMembers.length && !(members && [...it.spp].every(S => members.includes(S))) && !(members && sameSet([...it.spp].filter(S => members.includes(S)), members)) ? ` · for ${lcNames([...it.spp].filter(S => !members || members.includes(S)))}` : '';
  const line = (it, won) => { let st = won ? SAYS[it.role] || it.role : (LOSES[it.role] || 'Doesn’t apply');
    if (!won && LOSES[it.role] === 'Replaced') st = `Replaced by ${byName(it.by)}`;
    if (won && it.r.f?.record_retention) st = 'Record it';
    if (won && it.r.type === 'stop_fishing_after_quota') st = 'Stop fishing';
    if (won && it.role === 'governs' && it.r.k === 'gate') st = it.r.f.may_target === false ? 'Closed' : 'Release';
    return `<li class="${won ? 'won' : 'lost'}"><button class="pline pl2" type="button" data-rule="${esc(it.r.key)}"><span>${esc((forTxt(it) && sayRule(it.r, forTxt(it).replace(/^ · for /, ''))) || (members && (it.r.species || []).some(S => !row.allMembers.includes(S) && MODEL.R[S]) && (it.r.species || []).some(S => members.includes(S)) && sayRule(it.r, lcNames((it.r.species || []).filter(S => members.includes(S)), ' and '))) || sayRule(it.r) || shortLabel(it.r.label, row))}${forTxt(it) && !sayRule(it.r, 'x') ? `<small>${esc(forTxt(it).replace(/^ · /, ''))}</small>` : ''}</span><em>${esc(st)}</em></button></li>`; };
  const steps = list => { const by = new Map(); list.sort((a, b) => (b.r.rank ?? b.r.baseRank ?? 4) - (a.r.rank ?? a.r.baseRank ?? 4)).forEach(it => { const k = it.r.rank ?? it.r.baseRank ?? 4; (by.get(k) || by.set(k, []).get(k)).push(it); });
    return [...by].map(([k, its]) => `<li class="step"><span class="lvl">${esc(k === 3 ? (its[0].r.prov?.entry_name || 'The region') : k === 2 && (its[0].r.prov?.entry_name || '').length < 40 ? its[0].r.prov.entry_name : levelHead(k))}</span><ul>${its.map(it => line(it, true)).join('')}</ul></li>`).join(''); };
  const s = row.pool && scopeOf(row.pool);
  const result = {
    'What you can keep': res ? res : row.kind === 'nolimit' ? 'No daily limit here.' : row.pool ? `${row.daily} a day here${s && row.pool.f.take !== row.daily ? `, and they count toward ${row.pool.f.take} a day in ${s}` : s ? `, for all of ${s} together` : ''}.` : (row.kind === 'closed' ? 'Closed here.' : 'Release every one here.'),
  };
  let h = '';
  if (members){
    const won = items.filter(i => WINS.includes(i.role) && i.role !== 'possession').sort((a, b) => QORDER.indexOf(a.role) - QORDER.indexOf(b.role));
    h += `${res ? `<div class="lres">✓ ${esc(res)}.</div>` : ''}<ol class="ladder2">${steps(won)}</ol>`;
  } else for (const [q, roles] of Q){ const its = items.filter(i => roles.includes(i.role)); if (!its.length) continue;
    h += `<div class="lq"><div class="lbl">${q}</div>${result[q] ? `<div class="lres">✓ ${esc(result[q])}</div>` : ''}<ol class="ladder2">${steps(its)}</ol></div>`; }
  const wonSay = new Set(items.filter(i => WINS.includes(i.role)).map(i => sayRule(i.r)).filter(Boolean));
  const lost = members ? [] : items.filter(i => !WINS.includes(i.role) && i.role !== 'agrees' && !wonSay.has(sayRule(i.r)) && !(i.by && RULES[i.by] && shortLabel(RULES[i.by].label, row) === shortLabel(i.r.label, row)));
  if (lost.length) h += `<details class="inl"><summary>Rules that don’t apply here (${lost.length})</summary><ul class="plines">${lost.map(it => line(it, false)).join('')}</ul></details>`;
  return h + (members ? '' : `<button class="linkrow" type="button" data-src="${id}">See every regulation line ›</button>`);
}

function summaryHtml(row, id){
  const decided = `<details class="decide" data-dd="${id}d"${ddOpen(id + 'd')}><summary>How this was decided</summary><div class="ddbody">${ladderHtml(row, id)}</div></details>`;
  if (!row.pool){ const named = row.prot ? row.prot.map(spName).join(', ') : !row.members.length ? namedIn(row.win) : ''; const sg = row.members.length ? strip(PLACE, row.members) : null, never = sg && !sg.some(([st]) => st === 'keep'), un = sg && !never ? runUntil(sg, state.md) : '', rt = sg ? (never ? ((row.kind === 'closed' ? '' : 'No keeping at any time of year. ') + closedRuns(sg)).trim() : runTxt(sg, state.md, true)) : '';
    return `<p class="sum">${row.kind === 'closed' ? `Don’t fish for them${un ? ' until ' + un : ''}. If one bites, carefully release it.` : `You can fish for them, but release every one${un ? ' until ' + un : ''}.`}${rt ? ' ' + esc(rt) : un ? '' : row.win.wins.length ? ' ' + esc(cap(winTxt(row.win).slice(2))) + '.' : ''}</p>${named ? (nl => nl.length > 5 ? `<details class="names"><summary>Which fish (${nl.length})</summary><p class="names">${esc(nl.join(' · '))}</p></details>` : `<p class="names">${esc(nl.join(' · '))}</p>`)(row.prot ? row.prot.map(spName) : named.split(/,\s*/)) : ''}${miniStrip(PLACE, row.members, state.md)}${decided}`; }
  const rc = rowConds(row);
  const pics = row.allMembers.filter(hasPic);
  const photos = pics.length ? `<details class="looks" data-dd="${id}p"${ddOpen(id + 'p')}><summary>${pics.length > 1 ? `What they look like <span class="lc">${pics.length} fish</span>` : `What ${/^[aeiou]/i.test(spName(pics[0])) ? 'an' : 'a'} ${esc(spName(pics[0]).toLowerCase())} looks like`}</summary><div class="lstrip">${pics.map(S => `<button class="lthumb" type="button" data-fish="${S}">${FISHART.draw(S, { marks:false })}<span>${esc(spName(S))}</span></button>`).join('')}</div></details>` : '';
  const more = [liftNoteHtml(row), miniStrip(PLACE, row.members, state.md) ? stripHtml(PLACE, row.members, state.md) : ''].join('');
  const sb = speciesBlock(row, rc, id);
  const srcLink = `<button class="linkrow" type="button" data-src="${id}">See every regulation line ›</button>`;
  const s = scopeOf(row.pool);
  let single = '';
  if (!sb && row.kind === 'keep' && row.members.length){
    const facts = [...row.everyone, ...row.groups.flatMap(g => g.facts)];
    const it = { members:row.members, cs:[], facts, extra:[] };
    const q = quotaLines(it, row, rc);
    const both = row.members.every(S => ['H', 'W'].every(o => KEEPISH(MODEL.R[S]?.[o]?.status)));
    const org = rc.conds.find(c => c.c === 'origin' && !c.who);
    const chips = extraChips([...new Set(facts.map(l => fact(l, row).short).filter(t => t && /a year|record|stop fishing/i.test(t)))]).filter(c => ['cal','pen','hand'].includes(c.ic));
    if (q){ const lo = q[0].a, hi = q[q.length - 1].b, topN = Math.max(...q.map(x => x.mx)), s2 = [];
      capLines(q, topN).forEach(t => s2.push(t));
      const rel = [lo ? `under ${lo} cm` : '', hi !== Infinity ? `over ${hi} cm` : ''].filter(Boolean); if (rel.length) s2.push(`<span class="rl">Release:</span> ${rel.join(', ')}.`);
      single = `<div class="sp2 static solo">${sent(topN, null, [rngTxt(lo, hi), org ? (/^Hatchery/.test(org.t) ? 'hatchery only' : 'wild only') : both ? 'wild or hatchery' : ''])}${s2.length ? `<p class="s2">${s2.join(' ')}</p>` : ''}${chipHtml(chips)}</div>`; }
  }
  const simple = !sb && !rc.conds.length && !nar0(row) && row.kind === 'keep';
  if (simple && single && /class="seg"/.test(single) && !/class="s2"|class="chips"/.test(single)){
    const segs = [...single.matchAll(/<span class="seg">· ([^<]+)<\/span>/g)].map(m => m[1]);
    return `<p class="gsub">${segs.length ? esc(cap(segs.join(', '))) + '.' : ''}${s ? `${segs.length ? ' ' : ''}Shared with all of ${esc(s)} today.` : ''}</p>` + photos + more + decided; }
  const gb = simple ? (s ? `<p class="gsub">Shared with all of ${esc(s)} today.</p>` : '') : groupBlock(row, rc);
  return gb + single + sb + photos + more + (sb ? srcLink : decided);
}
function parentRow(row){ return row.pool ? MODEL.rows.find(r => r !== row && r.pool && row.members.every(S => r.allMembers.includes(S) || r.groups.some(g => g.members.includes(S))) && r.groups.some(g => g.facts.some(l => l.t === 'xref' && row.members.some(S => g.members.includes(S))))) : null; }
function rowHtml(row, i){
  const id = 'r' + i, rc = row.pool ? rowConds(row) : null;
  const origin = rc && rc.conds.find(c => c.c === 'origin' && !c.who);
  const sub = '';
  const P0 = parentRow(row);
  const A0 = !P0 && row.pool ? MODEL.rows.find(r => r !== row && r.pool && row.members.every(S => { const m = mainRes(MODEL.R[S]?.H, MODEL.R[S]?.W); return m && m.roles.get(r.pool.key)?.role === 'lifted'; })) : null;
  if (P0) return ''; if (false) return `<article class="row ledger stub" data-row="${id}"><div class="rhead"><span class="rtitle">${esc(rowTitle(row))}<span class="scope inside">Part of your ${P0.daily} ${esc(lc(rowTitle(P0)))}</span></span>${valueHtml(row, true).replace('a day, shared', 'a day').replace('<small>a day</small>', '<small>a day at most</small>')}</div><div class="rsum"><p class="gsub">Counted inside your ${P0.daily} ${esc(lc(rowTitle(P0)))} above. Its limits are shown there.</p><details class="decide" data-dd="${id}d"${ddOpen(id + 'd')}><summary>How this was decided</summary><div class="ddbody">${ladderHtml(row, id)}</div></details></div></article>`;
  return `<article class="row ledger" data-row="${id}"><div class="rhead"><span class="rtitle">${esc(rowTitle(row))}${A0 ? `<span class="scope inside">Separate from your ${A0.daily} ${esc(lc(rowTitle(A0)))}</span>` : ''}${A0 ? '' : (() => { const P = parentRow(row); return P ? `<span class="scope inside">Part of your ${P.daily} ${esc(lc(rowTitle(P)))}</span>` : (nar0(row) ? scopeBadge(row).replace(/>([^<]+) · streams</, (m, x) => `>All ${x} streams<`) : scopeBadge(row).replace(/>([^<]+) · streams</, (m, x) => `>${x}-wide<`)); })()}${sub ? `<span class="rsub">${sub}</span>` : ''}</span>${(() => { const v = valueHtml(row, !!row.pool && row.kind === 'keep'), ec = rc && effCap(row, rc); return ec && ec.all ? v.replace(`<div class="num">${row.daily}<`, `<div class="num">${ec.sum}<`) : v; })().replace('a day, shared', 'a day').replace('>Release<', '><span aria-hidden="true">↩ </span>Release<').replace('>Closed<', '><span aria-hidden="true">⊘ </span>Closed<')}</div>
    <div class="rsum">${summaryHtml(row, id)}</div></article>`;
}
// a single rule, in plain words first; the legal text and technical detail below
function openRule(key){
  const r = PLACE.cands.find(x => x.key === key) || RULES[key] || LIC[key]; if (!r) return;
  openSheet(clean(r.label || 'Rule'), `${levelTag(r)}${r.prov?.entry_name ? ' · ' + r.prov.entry_name : ''}`,
    `<div class="lbl">The exact words</div><blockquote class="exact">${esc(clean(r.verbatim || ''))}</blockquote>${(r.notes || []).map(n => `<div class="note">⚑ ${esc(n)}</div>`).join('')}
     <details class="fraw"><summary>Technical detail</summary><div class="mono">${esc(key)}</div><pre class="gjson">${esc(JSON.stringify(r.fields || r.f, null, 1))}</pre></details>`);
}
/* ================= GEAR, read from the bundle's own `gear` clauses =================
   Clauses on different slots all apply. Within one rule, clauses on one slot are ordered: the first
   whose `when` matches wins, and a clause with no `when` is the last word. Across rules, the ladder speaks. */
const SLOT_LABEL = { lines_per_angler:'Lines', terminal_attachments_per_line:'Hook, lure or fly per line', hooks_per_line:'Hooks per line', flies_per_line:'Flies per line',
  points_per_hook:'Hook points', hook_gap_mm:'Hook gap', weight_per_line_kg:'Weight on the line', bait_possession_kg:'Bait you can carry', light_to_hook_mm:'Light to hook' };
const PARENT = { angling:'any_method', fly_fishing:'any_method', ice_fishing:'any_method', set_lining:'any_method', spear_fishing:'any_method', crayfish_trapping:'any_method', netting:'any_method', snagging:'any_method', chumming:'any_method', roe:'any_bait', invertebrate:'any_bait', fin_fish:'any_bait', dead_fin_fish:'fin_fish', live_fin_fish:'fin_fish', artificial_fly:'any_lure', artificial_lure:'any_lure', barbed:'any_barb', barbless:'any_barb' };
const ELEM = { roe:'Roe (fish eggs)', invertebrate:'Water insects and crayfish', worms:'Worms', fin_fish:'Fish or fish parts', dead_fin_fish:'dead fish', live_fin_fish:'live fish', artificial_fly:'flies', artificial_lure:'lures',
  angling:'Angling (rod and line)', fly_fishing:'Fly fishing', ice_fishing:'Ice fishing', set_lining:'Set lines', spear_fishing:'Spear or bow', crayfish_trapping:'Crayfish traps', netting:'Nets', snagging:'Snagging', chumming:'Chumming', downrigger:'Downrigger', light:'Light to attract fish' };
const WHILE_TXT = { set_lining:'set lining', ice_fishing:'ice fishing', spear_fishing:'spearing', crayfish_trapping:'trapping crayfish', snagging:'snagging', downrigger:'using a downrigger', light:'using a light', alone_in_boat:'alone in a boat', in_boat:'in a boat', in_powered_boat:'in a powered boat', from_shore:'fishing from shore', downrigger_weight:'the weight is a downrigger’s' };
const MUST = { quick_release_to_line:'attached to your line by a quick-release', submerged:'submerged', attached_to_line:'attached to the line' };
const ancestors = e => { const a = [e]; while (PARENT[a[a.length-1]]) a.push(PARENT[a[a.length-1]]); return a; };
const siblingsOf = e => Object.keys(PARENT).filter(k => PARENT[k] === PARENT[e]);
function saysAbout(c, el){
  const anc = ancestors(el);
  if (c.ban && c.ban.some(x => anc.includes(x))) return (c.except || []).some(x => anc.includes(x)) ? null : 'ban';
  if (c.allow && c.allow.some(x => anc.includes(x))) return 'allow';
  if (c.only){ if (c.only.some(x => anc.includes(x))) return 'allow'; if (c.only.some(x => siblingsOf(x).includes(el))) return 'ban'; }
  return null;
}
const circOf = (r, c) => { const w = c.when || {}; return [...(r.f.while || []), ...(r.condMethods || []), w.method, w.angler, w.gear_in_use].filter(Boolean); };
function settleGear(w, md){
  const ctx = settle(w, md);
  const rules = ctx.active.filter(r => r.f.gear || r.f.conduct);
  const entries = [];
  for (const r of rules) (r.f.gear || []).forEach((c, i) => {
    if (c.when?.water && c.when.water !== w.kind) return;
    entries.push({ r, c, i, circ:circOf(r, c), targeting:c.when?.targeting ? expand(c.when.targeting) : null, note:c.when?.note });
  });
  const main = entries.filter(e => !e.circ.length && !e.targeting && !e.note);
  const order = (a, b) => a.r.rank - b.r.rank || spec(b.r) - spec(a.r);
  const counts = {}, specs = {}, elems = {};
  for (const e of main){
    const s = e.c.slot;
    if (e.c.must_be || e.c.requires) (specs[s] = specs[s] || []).push(e);
    else if ('max' in e.c || 'min' in e.c || e.c.unlimited) (counts[s] = counts[s] || []).push(e);
  }
  for (const s of Object.keys(counts)) counts[s].sort((x, y) => order(x, y) || ((x.c.max ?? 1e9) - (y.c.max ?? 1e9)));
  const setEl = (slot, el) => {
    const hits = main.filter(e => e.c.slot === slot).map(e => ({ ...e, s:saysAbout(e.c, el) })).filter(h => h.s);
    hits.sort((x, y) => order(x, y) || (x.s === 'ban' ? -1 : 1) - (y.s === 'ban' ? -1 : 1));
    return hits.length ? hits[0] : null;
  };
  [['bait',['roe','invertebrate','fin_fish']],['lure',['artificial_fly','artificial_lure']],['barb',['barbed']],['method',['angling','fly_fishing','ice_fishing','set_lining','spear_fishing','crayfish_trapping','netting','snagging','chumming']]]
    .forEach(([slot, list]) => list.forEach(el => { const x = setEl(slot, el); if (x) elems[slot + ':' + el] = x; }));
  const circ = entries.filter(e => e.circ.length || e.targeting || e.note);
  const conduct = rules.filter(r => r.f.conduct);
  const whileRet = ctx.active.filter(r => r.k === 'while');
  const timed = ctx.timed.filter(r => r.f.gear);
  return { counts, specs, elems, circ, conduct, whileRet, timed, rules, entries };
}
const gSrc = r => `<button class="src gsrc" type="button" data-rule="${esc(r.key)}">${RANK[String(r.rank)].t}</button>`;
function countTxt(slot, c){
  if (c.unlimited) return 'no limit';
  const n = c.max ?? c.min, pre = 'min' in c ? 'at least ' : '';
  switch (slot){
    case 'points_per_hook': return n === 1 ? 'Single point only (no trebles)' : `${pre}${n} points`;
    case 'hook_gap_mm': return `${pre}${n / 10} cm`;
    case 'weight_per_line_kg': case 'bait_possession_kg': return `${pre || 'up to '}${n} kg`;
    case 'light_to_hook_mm': return `within ${n / 1000} m of the hook`;
    default: return `${pre}${n}`;
  }
}
const circTxt = e => e.targeting ? `when fishing for ${lcNames(e.targeting)}` : e.note ? e.note : 'while ' + e.circ.map(x => WHILE_TXT[x] || x.replace(/_/g, ' ')).join(', ');
const PLAIN = { do_not_high_grade:'Don’t high-grade: once you keep a fish, don’t swap it for a better one later', release_immediately:'Release a fish you can’t keep right away, where you caught it', leave_head_tail_and_fins_until_residence:'Keep the head, tail and fins on your fish until you get home', do_not_can_bottle_or_fillet_away_from_residence:'Don’t can, jar or fillet your fish until you get home', do_not_freeze_in_unrecognizable_block:'Freeze fish so each one can still be identified and counted', do_not_possess_or_move_live_fish:'Don’t keep or move live fish or other live water animals (like crayfish)', return_unfit_fish_gently:'Put a fish you can’t keep back in the water gently', do_not_release_harmfully:'Don’t harm a fish you release' };
const ALWAYS_ORDER = ['return_unfit_fish_gently','release_immediately','do_not_release_harmfully','do_not_high_grade','do_not_waste_catch','leave_head_tail_and_fins_until_residence','transport_no_more_than_legal_limit','do_not_keep_catch_alive','do_not_enter_land_without_permission'];
const GROUPWORD = { ALL_GAME_FISH:'game fish', SALMON:'salmon', PROTECTED_SPECIES:'protected species', ALL_FIN_FISH:'any fish' };
const tgtTxt = codes => { const a = codes.map(c => GROUPWORD[c] || lcNames(expand([c]))); return a.length > 1 ? a.slice(0, -1).join(', ') + ' or ' + a[a.length - 1] : a[0]; };
function gRow(label, val, sub, r, strict){ return `<div class="grow${strict ? ' strict' : ''}"><div class="glabel">${label}</div><div class="gval"><b>${val}</b>${sub ? `<span>${sub}</span>` : ''}${r ? ' ' + gSrc(r) : ''}</div></div>`; }
// gear as named pieces, so the combined "Gear & licence" view can show one piece at a time
function gearParts(){
  const w = PLACE, md = state.md, P = {};
  if (MODEL.broad.length) return null;
  let h = '';
  const G = settleGear(w, md), C = G.counts, E = G.elems;
  const top = s => C[s]?.[0];
  const methodOk = m => !E['method:' + m] || E['method:' + m].s === 'allow';
  const METHOD_TOKENS = ['fly_fishing','ice_fishing','set_lining','spear_fishing','crayfish_trapping','netting','snagging','chumming','downrigger','light','ice_hut'];
  const circFor0 = s => G.circ.filter(e => e.c.slot === s && !e.circ.some(x => METHOD_TOKENS.includes(x)));
  // "no limit from a boat" covers "2 alone in a boat": show only the wider one
  const circFor = s => { const L = circFor0(s); const boatAll = L.some(e => e.circ.includes('in_boat') && e.c.unlimited); return boatAll ? L.filter(e => !e.circ.includes('alone_in_boat')) : L; };
  const tags = [], L = top('lines_per_angler');
  if (L) tags.push(`${countTxt('lines_per_angler', L.c)} line${L.c.max > 1 ? 's' : ''}${circFor('lines_per_angler').map(e => ` (${countTxt('lines_per_angler', e.c)} ${circTxt(e).replace('while ', '')})`).join('')}`);
  const pts = top('points_per_hook'), barbed = E['barb:barbed'];
  tags.push(pts || barbed?.s === 'ban' ? `${pts?.c.max === 1 ? 'Single' : 'Any'}${barbed?.s === 'ban' ? ' barbless' : ''} hook` : 'Trebles and barbs OK');
  const bb = ['roe','invertebrate','fin_fish'].map(e => E['bait:' + e]);
  tags.push(bb.every(x => x && x.s === 'ban') ? 'No bait' : 'Some bait OK');
  if (E['lure:artificial_lure']?.s === 'ban' && E['lure:artificial_fly']?.s === 'allow') tags.push('Artificial fly only: floats and sinkers OK');
  if (E['method:fly_fishing']?.s === 'allow' && E['method:fly_fishing'].c.only) tags.push('Fly fishing only: no float or sinker');
  h += (G.timed.length ? `<div class="caveats top"><div class="lbl">⚠ At certain times</div><ul>${G.timed.map(r => `<li><button class="factbtn" type="button" data-rule="${esc(r.key)}"><b>${esc(r.label)}</b><span class="where">${esc(r.tcond)}</span></button></li>`).join('')}</ul></div>` : '');
  h += uncertainHtml(['gear_and_method']) + inPartHtml(settle(w, md), ['gear_and_method','vessel','conduct']);
  P.pre = h; P.tags = tags; h = '';
  const row = (slot, sub) => { const t = top(slot); if (!t) return ''; return gRow(SLOT_LABEL[slot] || slot, countTxt(slot, t.c), [sub ? sub(t) : '', t.c.members ? 'one ' + t.c.members.map(m => m.replace(/_/g, ' ').replace('artificial ', '')).join(', or one ') : '', t.c.unless ? 'not when ' + t.c.unless.map(u => WHILE_TXT[u.gear_in_use] || JSON.stringify(u)).join(', ') : '', ...circFor(slot).map(e => `${countTxt(slot, e.c)} ${circTxt(e)}`)].filter(Boolean).join(' · '), t.r, slot === 'points_per_hook'); };
  let line = row('lines_per_angler') + row('terminal_attachments_per_line');
  line += top('points_per_hook') ? row('points_per_hook', t => t.r.f.water ? `on ${t.r.f.water}s` : '') : gRow('Hook points', 'Trebles allowed', 'any hook');
  line += barbed?.s === 'ban' ? gRow('Barbs', 'Barbless only', (barbed.r.f.water ? `on ${barbed.r.f.water}s · ` : '') + 'pinch barbs flat or buy barbless', barbed.r, true) : gRow('Barbs', 'Barbs allowed', '');
  const lure = E['lure:artificial_fly'], lureNo = E['lure:artificial_lure'];
  line += lureNo?.s === 'ban' ? gRow('Lures', 'Artificial flies only', 'floats and sinkers may be on the line', lureNo.r, true) : gRow('Lures', 'Any lure or fly', '');
  line += (top('terminal_attachments_per_line') ? '' : row('flies_per_line')) + row('weight_per_line_kg') + row('hooks_per_line');
  P.line = `<div class="gsec">${line}</div>`;
  const pos = top('bait_possession_kg');
  P.baitBan = bb.every(x => x && x.s === 'ban');
  P.bait = `<div class="gsec">${P.baitBan ? '<p class="kitnote">Bait ban: no bait of any kind.</p>' : ''}<div class="baitgrid">${['worms','roe','invertebrate','fin_fish'].map(e => {
    const anyBan = ['roe','invertebrate','fin_fish'].map(k => E['bait:' + k]).find(y => y && y.s === 'ban' && y.c.ban?.includes('any_bait'));
    const x = e === 'worms' ? anyBan : E['bait:' + e], ok = x?.s === 'allow';
    const exc = G.circ.filter(c => c.c.slot === 'bait' && ['allow','only'].some(k => c.c[k]) && saysAbout(c.c, e === 'fin_fish' ? 'dead_fin_fish' : e) === 'allow' && c.circ.every(m => methodOk(m)));
    const sub = x ? (x.s === 'ban' && x.c.ban?.includes('any_bait') ? 'bait ban' : x.r.f.water ? (x.s === 'ban' ? `not on ${x.r.f.water}s` : `on ${x.r.f.water}s`) : x.s === 'ban' && (x.c.except || []).includes('roe') ? 'banned · roe is still OK' : x.s === 'ban' ? '' : 'allowed') : 'allowed';
    const extra = e === 'roe' && ok && pos && (pos.c.of || []).includes('roe') ? `carry up to ${pos.c.max} kg` : '';
    (P.baitList = P.baitList || []).push({ e, ok: ok || !x });
    return `<button class="bait ${ok || !x ? 'ok' : 'no'}" type="button" data-rule="${x ? esc(x.r.key) : ''}"><i>${ok || !x ? '✓' : '✕'}</i><b>${ELEM[e]}</b><span>${esc([sub, extra].filter(Boolean).join(' · '))}</span>${exc.map(c => `<small>✓ dead fish ${esc(circTxt(c))}</small>`).join('')}</button>`; }).join('')}</div></div>`;
  // ways to fish
  const methodInfo = m => {
    const bits = [];
    G.circ.filter(e => e.circ.includes(m) && e.c.slot !== 'bait').forEach(e => bits.push(e.c.must_be ? `must be ${e.c.must_be.map(x => MUST[x] || x.replace(/_/g, ' ')).join(', ')}` : `${(SLOT_LABEL[e.c.slot] || e.c.slot).toLowerCase()}: ${countTxt(e.c.slot, e.c)}`));
    G.conduct.filter(r => (r.f.while || []).includes(m)).forEach(r => r.f.conduct.forEach(c => bits.push(D.conduct[c] || c)));
    G.whileRet.filter(r => (r.f.while || []).includes(m)).forEach(r => bits.push(/other than crayfish/i.test(r.label) ? 'release anything in the trap that isn’t a crayfish' : clean(r.label)));
    return [...new Set(bits)].join(' · ');
  };
  // the province lists the lawful ways to sport fish; anything no rule allows is not permitted anywhere
  const deviceTxt = s => { const l = G.specs[s]; if (!l) return ''; const extra = s === 'light' && top('light_to_hook_mm') ? ', ' + countTxt('light_to_hook_mm', top('light_to_hook_mm').c) : ''; return `with a ${s}: ${l.flatMap(e => e.c.must_be || []).map(x => MUST[x] || x.replace(/_/g, ' ')).join(', ')}${extra}`; };
  const flyOnly = E['method:fly_fishing']?.s === 'allow' && E['method:fly_fishing'].c.only;
  const chips = ['angling', ...(w.kind === 'stream' ? [] : ['ice_fishing']),'set_lining','spear_fishing','crayfish_trapping','netting','snagging','chumming'].map(k => {
    let x = E['method:' + k];
    if (k === 'angling' && flyOnly) return { k, allow:true, r:E['method:fly_fishing'].r, info:'fly fishing only: nothing but the fly on the line, no float, sinker or attractor' };
    if (!x) return { k, allow:false, r:null, info: D.province_methods.includes(k) ? 'not allowed here' : 'not a lawful way to sport fish' };
    if (flyOnly && k !== 'angling' && x.s === 'ban' && x.c.only) return { k, allow:false, r:x.r, info:'fly fishing only here' };
    const tg = G.circ.filter(e => e.c.slot === 'method' && e.c.when?.targeting && saysAbout(e.c, k));
    const tgBan = tg.filter(e => saysAbout(e.c, k) === 'ban' && !e.r.lifted), tgOk = tg.filter(e => saysAbout(e.c, k) === 'allow');
    const tgInfo = x.s === 'allow' && tg.length ? [tgBan.length ? 'not for ' + tgtTxt([...new Set(tgBan.flatMap(e => e.c.when.targeting))]) : '', tgOk.length ? tgtTxt([...new Set(tgOk.flatMap(e => e.c.when.targeting))]) + ' allowed' : ''].filter(Boolean).join(' · ') : '';
    return { k, allow:x.s === 'allow', r:x.r, info:x.s === 'allow' ? [tgInfo, methodInfo(k), ...(k === 'angling' ? ['downrigger','light'].map(deviceTxt) : [])].filter(Boolean).join(' · ') : '' };
  });
  const chip = o => `<button class="mchip ${o.allow ? (o.info ? 'limited' : 'allowed') : 'banned'}" type="button" data-rule="${o.r ? esc(o.r.key) : ''}"><i>${o.allow ? (/not for game fish/.test(o.info || '') ? '!' : '✓') : '✕'}</i>${esc(ELEM[o.k] || o.k)}${o.allow && /not for game fish/.test(o.info || '') ? ' — not for trout or other game fish' : ''}${o.info ? `<small>${esc(o.info)}</small>` : ''}</button>`;
  P.waysNo = chips.filter(o => !o.allow);
  P.ways = `<div class="gsec"><div class="lbl">Allowed</div><div class="wlist">${chips.filter(o => o.allow).map(o => `<button class="wrow" type="button" data-rule="${o.r ? esc(o.r.key) : ''}"><i class="${/not for game fish/.test(o.info || '') ? 'lim' : 'ok'}">${/not for game fish/.test(o.info || '') ? '!' : '✓'}</i><span><b>${esc(ELEM[o.k] || o.k)}${/not for game fish/.test(o.info || '') ? ' — not for trout or other game fish' : ''}</b>${o.info ? `<small>${esc(o.info)}</small>` : ''}</span></button>`).join('')}</div>${chips.some(o => !o.allow) ? `<div class="lbl">Not allowed</div><div class="wno">${chips.filter(o => !o.allow).map(o => `<button class="nochip" type="button" data-rule="${o.r ? esc(o.r.key) : ''}"><i>✕</i>${esc(cap(lc(ELEM[o.k] || o.k)))}</button>`).join('')}</div>` : ''}</div>`;
  const ao = c => { const i = ALWAYS_ORDER.indexOf(c); return i < 0 ? 99 : i; };
  const always = G.conduct.filter(r => !r.f.while).flatMap(r => r.f.conduct.map(c => ({ r, c }))).sort((a, b) => ao(a.c) - ao(b.c));
  const li = ({ r, c }) => `<li><button class="factbtn" type="button" data-rule="${esc(r.key)}">${esc(PLAIN[c] || D.conduct[c] || c)}</button></li>`;
  const BOATS_HTML = () => {
  // boats: vessel rules in force today (speed, engine power, no powered boats); shown as they are printed
  const ctxV = settle(w, md), boats = ctxV.active.filter(r => r.family === 'vessel'), boatsTimed = ctxV.timed.filter(r => r.family === 'vessel');
  return (boats.length || boatsTimed.length) ? `<div class="gsec"><div class="lbl">Boats</div><ul class="plain">${[...boats, ...boatsTimed].map(r => `<li><button class="factbtn" type="button" data-rule="${esc(r.key)}">${esc(clean(r.label))} <span class="src">${RANK[String(r.rank)].t}</span></button></li>`).join('')}</ul></div>` : '';
  };
  P.boats = BOATS_HTML().replace('<div class="lbl">Boats</div>', '');
  // handling rules in four short groups, each rule a few words; tap one for the regulation's own wording
  const HG = [
    ['When you release a fish', { release_immediately:'Right away, where you caught it', return_unfit_fish_gently:'Gently, back in the water', do_not_release_harmfully:'Without harming it' }],
    ['When you keep a fish', { do_not_high_grade:'It stays kept: no swapping for a bigger one', do_not_waste_catch:'Don’t waste it', do_not_keep_catch_alive:'Don’t hold it alive (no stringer or livewell)', leave_head_tail_and_fins_until_residence:'Leave head, tail and fins on until home', do_not_can_bottle_or_fillet_away_from_residence:'No canning or filleting until home', do_not_freeze_in_unrecognizable_block:'Don’t freeze fish into one block: each must stay countable', keep_catch_identifiable:'Each fish must stay identifiable and measurable' }],
    ['Carrying fish', { transport_no_more_than_legal_limit:'Never more than your legal limit', keep_licence_handy_while_travelling:'Licence at hand while you travel with fish', produce_licence_on_request:'Show your licence when an officer asks', carry_paper_licence:'Carry your paper licence', carry_signed_letter_when_transporting_for_another:'Carrying someone else’s fish? Have their signed letter', show_letter_when_exporting:'Taking fish out of B.C.? Show the letter if asked', keep_signed_letter_for_gifted_fish:'Given fish? Keep the signed letter until eaten' }],
    ['Never', { do_not_buy_sell_or_barter_catch:'Sell, buy or trade your catch', do_not_possess_or_move_live_fish:'Keep or move live fish or crayfish', do_not_release_aquarium_fish:'Release aquarium fish', no_gear_in_water_during_closure:'Put gear in the water during a closure', do_not_interfere_with_furbearer_trap:'Interfere with a fur trap', do_not_enter_land_without_permission:'Cross private, posted or reserve land without permission' }],
  ];
  const byC = new Map(always.map(x => [x.c, x])), usedC = new Set();
  const cards = HG.map(([h, m]) => { const items = Object.entries(m).filter(([c]) => byC.has(c)); items.forEach(([c]) => usedC.add(c)); return items.length ? `<div class="hcard"><div class="hh">${h}</div><ul>${items.map(([c, t]) => `<li><button class="factbtn" type="button" data-rule="${esc(byC.get(c).r.key)}">${esc(t)}</button></li>`).join('')}</ul></div>` : ''; }).join('');
  const left = always.filter(x => !usedC.has(x.c));
  P.always = always.length ? `<div class="hcards">${cards}${left.length ? `<div class="hcard"><div class="hh">Also</div><ul>${left.map(li).join('')}</ul></div>` : ''}</div>` : '';
  P.alwaysN = always.length;
  P.waysOk = chips.filter(o => o.allow).length;
  P.waysNames = chips.filter(o => o.allow).map(o => ELEM[o.k] || o.k);
  { const ctxV = settle(w, md); P.boatFirst = ctxV.active.filter(r => r.family === 'vessel').map(r => clean(r.parts?.what || r.label).replace(/ — .*$/, ''))[0] || ''; }
  return P;
}
function openGearAll(){
  const G = settleGear(PLACE, state.md), won = new Set();
  Object.values(G.counts).forEach(l => won.add(l[0].r)); Object.values(G.elems).forEach(e => won.add(e.r)); Object.values(G.specs).forEach(l => l.forEach(e => won.add(e.r))); G.circ.forEach(e => won.add(e.r)); G.conduct.forEach(r => won.add(r));
  const lost = G.rules.filter(r => !won.has(r));
  openSheet('Gear sources', `${PLACE.name} · ${fmtMd(state.md)}`, `<div class="lbl">These decide it (${won.size})</div>${[...won].sort((a, b) => a.rank - b.rank).map(r => srcCard(r, r.label, true, fieldsPre(r.f))).join('')}` + (lost.length ? `<details class="lostlist"><summary class="lbl">Overruled or repeated (${lost.length})</summary>${lost.map(r => srcCard(r, r.label, false, fieldsPre(r.f))).join('')}</details>` : ''));
}
/* ================= LICENCES =================
   Licensing never opens or closes a water; it is asked only once the water is open. The angler is always unknown,
   so every answer is for a chosen angler, and the rules for other anglers stay listed. Any ONE path satisfies a
   requirement; a `hold` path needs ALL its documents. An unresolved record reads "check", never "none needed". */
const LIC = D.licensing;
for (const [k, l] of Object.entries(LIC)){ l.key = k; l.f = l.fields; l.wins = whenDates(l.f.when); l.prov = l.prov || {}; l.verbatim = l.verbatim || ''; }
const DOC = d => D.licences[d]?.name || d.replace(/_/g, ' ');
const AXES = { residency:[['resident','B.C. resident'],['non_resident','Canadian, other province'],['non_resident_alien','From outside Canada']], age:[['16_plus','16 or older'],['under_16','Under 16']], guidance:[['non_guided','Not guided'],['guided','Guided']], status:[['none','No status'],['indian_bc_resident','Status First Nations, living in B.C.'],['metis','Métis'],['disabled','Disabled'],['aged_65_plus','65 or older']] };
function whoMatch(who, P){
  if (!who) return true;
  return Object.entries(who).every(([axis, list]) => { const a = Array.isArray(list) ? list : [list]; return a.includes(P[axis]); });
}
const whoTxt = who => !who ? 'every angler' : Object.entries(who).map(([a, l]) => (Array.isArray(l) ? l : [l]).map(v => { const t = String(AXES[a]?.find(x => x[0] === v)?.[1] || v); return a === 'status' ? (v === 'indian_bc_resident' ? 'a status First Nations person living in B.C.' : t) : t.replace(/^[A-Z](?=[a-z])/, c => c.toLowerCase()); }).join(' or ')).join(', ');
function actTxt(d){
  const sp = (d.species ? lcNames(expand(d.species)) || d.species.map(c => (GROUPS[c]?.name || c).toLowerCase()).join(', ') : '') || 'fish', len = d.lengths?.length ? ' ' + d.lengths.map(x => x.min_cm != null && x.max_cm != null ? `${x.min_cm}–${x.max_cm} cm` : x.min_cm != null ? `over ${x.min_cm} cm` : `under ${x.max_cm} cm`).join(', ') : '';
  switch (d.act){ case 'fishing': return 'To fish here'; case 'targeting': return `To fish for ${sp}`; case 'retaining': return `To keep ${sp}${len}`; case 'retaining_recorded': return 'When you keep a fish you must record'; case 'guiding': return 'To guide anglers'; default: return d.act; }
}
function licPlace(){
  const water = WATERS[state.wi];
  const local = (PLACE.part.lic || []).map(([k, via]) => ({ l:LIC[k], via }));
  // a province-wide record stops where its extent says (outside_area_kind): on a part inside a national park it does not hold
  const exc = PLACE.part.province_except || [];
  const stops = l => (l.f.extents || []).some(e => e.outside_area_kind && exc.includes(e.outside_area_kind));
  const global = Object.values(LIC).filter(l => ['province','on_designation','not_placed'].includes(l.placement) && !stops(l)).map(l => ({ l, via:l.placement }));
  const unresolved = Object.values(LIC).filter(l => l.placement === 'unresolved' && water.entries.includes(l.entry_id)).map(l => ({ l, via:'unresolved' }));
  return [...local, ...global, ...unresolved];
}
function settleLic(md, P){
  const all = licPlace(), ctx = settle(PLACE, md);
  const activeRuleIds = new Set(ctx.active.map(r => r.key));
  const des = all.filter(x => x.l.kind === 'designation').map(x => x.l).filter(d => inDates(d.wins, md) && !(d.f.suspended_while || []).some(s => activeRuleIds.has(`${d.entry_id}::${s.rule_id}`)));
  const contested = all.some(x => x.via === 'contested') || all.some(x => x.l.kind === 'not_classified');
  const classPeriod = des.length > 0;
  const stampPeriod = des.some(d => d.f.steelhead_stamp_during && inDates(whenDates(d.f.steelhead_stamp_during.when), md) && !d.f.steelhead_stamp_waived);
  const released = new Set(Object.values(LIC).filter(l => l.kind === 'exemption' && whoMatch(l.f.who, P)).flatMap(l => l.f.documents));
  const exemptions = Object.values(LIC).filter(l => l.kind === 'exemption' && whoMatch(l.f.who, P));
  const alts = all.filter(x => x.l.kind === 'alternative').map(x => x.l);
  // a row that restates a provincial record reads its `who` and `satisfied_by` as that record's
  let reqs = all.filter(x => x.l.kind === 'requirement').map(x => { const t = x.l.f.restates && LIC[`${x.l.f.restates.entry_id}#${x.l.f.restates.id}`]; return t ? { ...x, l:{ ...x.l, f:{ ...x.l.f, who:t.f.who, satisfied_by:t.f.satisfied_by } } } : { ...x }; });
  // no steelhead in lakes: a stamp needed only to fish for steelhead doesn't apply on a lake
  reqs = reqs.filter(({ l }) => !(PLACE.kind === 'lake' && (l.f.doing?.species || []).length && l.f.doing.species.every(S => S === 'ST')));
  reqs = reqs.filter(({ l }) => (!l.f.water || l.f.water === PLACE.kind) && inDates(l.wins, md) && (!l.f.on || (l.f.on === 'classified_period' ? classPeriod : stampPeriod)));
  // a row that restates a provincial record: show it once, carrying both sources
  const restated = new Set(reqs.filter(({ l }) => l.f.restates).map(({ l }) => `${l.f.restates.entry_id}#${l.f.restates.id}`));
  reqs.forEach(x => { x.also = reqs.filter(y => y.l.f.restates && `${y.l.f.restates.entry_id}#${y.l.f.restates.id}` === x.l.key).map(y => y.l); });
  reqs = reqs.filter(x => !(x.l.f.restates && reqs.some(y => y.l.key === `${x.l.f.restates.entry_id}#${x.l.f.restates.id}`)));
  const waived = new Set(des.flatMap(d => d.stamp_waiver?.outright ? (d.stamp_waiver.lifts || []).map(x => typeof x === 'string' ? x : x.id || `${x.entry_id}#${x.record_id}`) : []));
  reqs = reqs.filter(({ l }) => !waived.has(l.key));
  const nym = reqs.filter(({ l }) => l.not_yet_mapped); reqs = reqs.filter(({ l }) => !l.not_yet_mapped);
  const superior = reqs.some(({ l }) => l.f.authority === 'superior');
  reqs.forEach(x => { x.displaced = superior && x.l.f.authority !== 'superior' && (x.l.f.satisfied_by || []).length > 0; x.mine = whoMatch(x.l.f.who, P) && !(x.l.f.who_except && whoMatch(x.l.f.who_except, P)); x.presumesFreed = (x.l.f.presumes || []).length > 0 && x.l.f.presumes.every(d => released.has(d)); x.presumesBy = (x.l.f.presumes || []).length ? Object.values(LIC).find(l => l.kind === 'exemption' && x.l.f.presumes.every(d => l.f.documents.includes(d))) : null;
    x.paths = [...(x.l.f.satisfied_by || []), ...alts.filter(a => `${a.f.alternative_to.entry_id}#${a.f.alternative_to.id}` === x.l.key).flatMap(a => a.f.satisfied_by.map(p => ({ ...p, alt:a })))].map(p => ({ ...p, need:(p.hold || []).filter(d => !released.has(d)), freed:(p.hold || []).filter(d => released.has(d)) })); });
  const terms = Object.values(LIC).filter(l => l.kind === 'licence_terms');
  const termsFor = doc => terms.filter(t => t.f.document === doc && whoMatch(t.f.who, P) && (!t.f.classified || des.some(d => d.f.classified === t.f.classified)) && (!t.f.units || des.some(d => t.f.units.includes(d.f.unit))));
    // the fee table, for the chosen angler: an annual class written for them (65+, disabled) over the general one, then the day licences
  const feeKey = P.residency;
  const prices = doc => { const ts = termsFor(doc).filter(t => t.f.fees_cad && t.f.fees_cad[feeKey] != null);
    const yr = ts.filter(t => t.f.sold === 'per_licence_year').sort((a, b) => !!b.f.who - !!a.f.who)[0];
    const bits = []; if (yr) bits.push(`$${yr.f.fees_cad[feeKey].toFixed(2)} a year`);
    ts.filter(t => t.f.valid_days === 1 || t.f.sold === 'per_day').forEach(t => bits.push(`$${t.f.fees_cad[feeKey].toFixed(2)} a day`));
    ts.filter(t => t.f.valid_days === 8).forEach(t => bits.push(`$${t.f.fees_cad[feeKey].toFixed(2)} for 8 days`));
    return [...new Set(bits)].join(' · '); };
  return { des, contested, classPeriod, stampPeriod, exemptions, released, reqs, nym, termsFor, prices, all, unresolved:all.filter(x => x.via === 'unresolved') };
}
function pathHtml(p, x){
  const bits = [];
  if (p.hold) bits.push(p.need.length ? p.need.map(d => `<span class="doc">${esc(DOC(d))}</span>`).join(' + ') : `<span class="doc ok">Nothing to buy: you’re exempt</span>`);
  if (p.accompanied_by) bits.push(`be with someone who is ${esc(whoTxt(p.accompanied_by.who || p.accompanied_by))} and holds what this fishing needs${p.quota === 'counts_to_companion' ? ' <em>(fish you keep count toward their limit)</em>' : ''}`);
  if (p.as) bits.push(`meet the rules as ${esc(whoTxt(p.as.who || p.as))}`);
  return bits.join(' ') + (p.alt ? ` <span class="muted small">(also accepted here)</span>` : '');
}
function reqCard(x, S){
  const l = x.l, paths = x.paths;
  const docs = paths.length ? paths.map(p => `<div class="lpath">${pathHtml(p, x)}</div>`).join('<div class="lor">or</div>') : '';
  const cond = (l.f.conduct || []).map(c => `<div class="lpath">${esc(D.conduct[c] || c)}</div>`).join('');
  const terms = [...new Set(paths.flatMap(p => p.need || []))].flatMap(d => S.termsFor(d)).filter(t => !t.f.fees_cad).map(t => `<li><button class="factbtn" type="button" data-lic="${esc(t.key)}">${esc(t.label)}</button></li>`).join('');
  return `<div class="lreq${x.displaced ? ' lost' : ''}"><button class="lhead" type="button" data-lic="${esc(l.key)}"><span>${esc(l.label)}</span><span class="src">${l.placement === 'province' ? 'Province' : l.placement === 'on_designation' ? 'Classified water' : l.f.authority === 'superior' ? 'Federal / parks' : 'This water'}${x.also.length ? ' +' + x.also.length : ''}</span></button>
    ${x.displaced ? `<div class="muted small">Not valid here: a federal or park authority’s requirement replaces provincial licences.</div>` : ''}${docs}${x.presumesFreed ? `<div class="doc ok">Not for you: you don’t need the licence this is about</div>` : cond}${!x.presumesFreed && x.presumesBy ? `<div class="muted small">Not needed if you are ${esc(whoTxt(x.presumesBy.f.who).replace(/\.$/, ''))}.</div>` : ''}${terms ? `<ul class="plain small lterms">${terms}</ul>` : ''}</div>`;
}
function profileHtml(){
  return `<div class="profile"><div class="lbl">Who is fishing?</div>${Object.entries(AXES).map(([a, opts]) => `<label class="psel"><span class="sr">${a}</span><select data-axis="${a}" id="who-${a}">${opts.map(([v, t]) => `<option value="${v}"${state.who[a] === v ? ' selected' : ''}>${t}</option>`).join('')}</select></label>`).join('')}</div>`;
}
function licParts(){
  const water = WATERS[state.wi], md = state.md;
  if (MODEL.broad.length) return null;
  let h = '';
  const S = settleLic(md, state.who);
  h += profileHtml();
  if (S.des.length) h += S.des.map(d => `<div class="classbox"><b>${esc(d.period?.says || `Class ${d.f.classified} Classified Water`)}</b><span>Licence unit: ${esc(d.f.unit_name || d.f.unit)}${d.f.steelhead_stamp_during ? `. Steelhead Stamp needed ${esc(whenDates(d.f.steelhead_stamp_during.when).map(([a, b]) => rangeTxt(a, b)).join(', '))}, whatever you fish for` : ''}${d.stamp_waiver ? '. ' + d.stamp_waiver.says.replace(/\.$/, '') : d.f.steelhead_stamp_waived ? '. Steelhead Stamp not needed here unless you fish for steelhead' : ''}.</span> <button class="srcbtn inline" type="button" data-lic="${esc(d.key)}">Source</button></div>`).join('');
  if (S.contested) h += `<div class="flag"><b>?</b><span>Check: part of this water is also marked “not a Classified Water”.</span></div>`;
  S.unresolved.forEach(x => { h += `<div class="flag"><b>?</b><span>Check: “${esc(x.l.label)}” couldn’t be placed on the map (${esc(friendlyWhy(x.l.prov.why || x.l.f.review_reason || '') || 'the place isn’t clear')}).</span></div>`; });
  if (S.exemptions.length) h += `<div class="scopenote wide">You’re exempt from: ${esc([...S.released].map(DOC).join(', '))}. <button class="srcbtn inline" type="button" data-lic="${esc(S.exemptions[0].key)}">Source</button></div>`;
  const isGuide = x => (x.l.f.doing || {}).act === 'guiding';
  const guides = S.reqs.filter(isGuide), mine = S.reqs.filter(x => x.mine && !isGuide(x)), others = S.reqs.filter(x => !x.mine && !isGuide(x));
  const fishNeeds = mine.filter(x => actTxt(x.l.f.doing || { act:'fishing' }) === 'To fish here' && !x.presumesFreed);
  const needsAny = fishNeeds.some(x => x.paths.some(p => (p.need || []).length || p.accompanied_by || p.as));
  if (!needsAny) h += `<div class="classbox"><b>No licence needed to fish here</b><span>${state.who.age === 'under_16' ? 'Anglers under 16 who live in B.C. don’t need a basic angling licence.' : S.exemptions.length ? 'You’re exempt from the basic angling licence.' : 'No licence rule applies to this angler.'} The fishing rules and limits still apply.</span></div>`;
  const groups = new Map(); mine.forEach(x => { const t = actTxt(x.l.f.doing || { act:'fishing' }); if (!groups.has(t)) groups.set(t, []); groups.get(t).push(x); });
  const buy = [];
  mine.forEach(x => { let t = actTxt(x.l.f.doing || { act:'fishing' }); if (/^To (keep|fish for)( fish)?$/.test(t)){ const m = (x.l.label || '').match(/to (keep|fish for) ([^.]+)/i); if (m) t = `To ${m[1].toLowerCase()} ${m[2]}`; } x.paths.forEach(p => (p.need || []).forEach(d => { if (!buy.some(b => b.d === d)) buy.push({ d, t, on:x.l.f.on }); })); });
  const perSays = S.des.map(d => d.period?.says).filter(Boolean)[0];
  const whenTxt = (t, on) => on === 'classified_period' && perSays ? `Needed while it’s classified: ${lc(perSays.replace(/^Classified \(Class (I+)\)/, 'Class $1'))}` : t === 'To fish here' ? 'Needed to fish at all' : t.startsWith('To fish for') ? 'Only if you ' + lc(t.replace(/^To /, '')) + ', even to release them' : t.startsWith('To keep') ? 'Only if you ' + lc(t.replace(/^To /, '')) : t;
  const out = { buy:buy.map(b => ({ name:cap(DOC(b.d)), when:whenTxt(b.t, b.on), base:b.t === 'To fish here' })), none:!needsAny };
  if (buy.length) h += `<div class="needs"><div class="lbl">You need</div><ul>${buy.sort((a, b) => (a.t === 'To fish here' ? 0 : 1) - (b.t === 'To fish here' ? 0 : 1)).map(b => { const pr = S.prices(b.d); return `<li><b>${esc(cap(DOC(b.d)))}</b><span>${esc(whenTxt(b.t, b.on))}</span>${pr ? `<span class="price">${esc(pr)}</span>` : ''}</li>`; }).join('')}</ul>${buy.some(b => S.prices(b.d)) ? '<p class="pricenote">Prices as printed for 2025–2027, before tax. Today’s prices: gov.bc.ca/fish-licence</p>' : ''}</div>`;
  // the paper licence: only the record duties that reach this water ('Keep a hatchery steelhead? Carry your paper licence.')
  const paper = mine.find(x => (x.l.f.doing || {}).act === 'retaining_recorded' && (x.l.records || []).length);
  if (paper){ const here = new Set(PLACE.cands.filter(r => applies(r, PLACE)).map(r => r.key));
    const fishOf = r => { const f = r.f || r.fields, min = (f.lengths || []).find(l => l.min_cm != null); return `${f.life_stage === 'adult' ? 'adult ' : ''}${f.origin ? f.origin + ' ' : ''}${lcNames(expand(f.species || []), ' or ')}${min ? ` over ${min.min_cm} cm` : ''}`; };
    const fish = [...new Set(paper.l.records.filter(k => here.has(k) && RULES[k]).map(k => fishOf(RULES[k])))];
    if (fish.length) h += `<p class="paper">Keep ${/^[aeiou]/.test(fish[0]) ? 'an' : 'a'} ${esc(join(fish, ' or '))}? <b>Carry your paper licence</b> (16 and over) and record it right away.</p>`; }
  if (S.nym.length) h += S.nym.map(x => `<div class="nymbox"><div class="lbl">In one part of this water</div><p><b>${esc(cap(x.l.parts?.in_part || x.l.not_yet_mapped.part))}:</b> you need ${esc(x.l.parts?.need || DOC((x.l.f.satisfied_by || [])[0]?.hold?.[0] || ''))} ${esc(x.l.parts?.doing || 'to fish')}.</p><p class="muted small">That part isn’t drawn on the map yet. It doesn’t apply to the rest of the water.</p></div>`).join('');
  const orderAct = t => t === 'To fish here' ? 0 : t.startsWith('To fish for') ? 1 : t.startsWith('To keep') ? 2 : 3;
  h += `<details class="lostlist"><summary class="lbl">Where each one comes from</summary>`;
  [...groups.entries()].sort((a, b) => orderAct(a[0]) - orderAct(b[0])).forEach(([t, list]) => {
    h += `<div class="gsec"><div class="lbl">${esc(t)}</div>${list.map(x => reqCard(x, S)).join('')}</div>`;
  });
  h += `</details>`;
  if (!mine.length && needsAny) h += `<p class="muted">Nothing is required of this angler here.</p>`;
  if (guides.length) h += `<details class="lostlist"><summary class="lbl">If you are guiding other anglers</summary>${guides.map(x => reqCard(x, S)).join('')}</details>`;
  if (others.length) h += `<details class="lostlist"><summary class="lbl">Rules for other anglers (${others.length})</summary>${others.map(x => `<div class="muted small">For ${esc(whoTxt(x.l.f.who))}${x.l.f.who_except ? ` (not ${esc(whoTxt(x.l.f.who_except))})` : ''}:</div>${reqCard(x, S)}`).join('')}</details>`;
  h += `<button class="srcbtn" type="button" data-licsrc="1">All licence sources</button>`;
  out.body = h; return out;
}
function licPartLabel(water, i){
  const p = water.lparts[i], des = p.records.map(([k]) => LIC[k]).filter(l => l.kind === 'designation');
  if (des.length) return des.map(d => `Class ${d.f.classified}${d.wins.length ? ' ' + d.wins.map(([a, b]) => rangeTxt(a, b)).join(', ') : ''}`).join('; ');
  const other = p.records.map(([k]) => LIC[k]).find(l => l.kind !== 'designation');
  return other ? other.label.slice(0, 60) : 'Area ' + (i + 1);
}
function openLic(key){ const l = LIC[key]; if (!l) return; openSheet('Licence source', l.prov.entry_name || '', srcCard({ ...l, prov:{ who:l.placement === 'province' ? 'Provincial · province-wide' : l.placement === 'on_designation' ? 'Wherever a classified designation is in force' : l.placement === 'not_placed' ? 'Not bound to a place' : l.prov.entry_name } , rank:l.placement === 'province' ? 4 : 0, notes:[l.f.review_reason && 'Curator still to settle: ' + l.f.review_reason, l.prov.uncertain && 'Unresolved: ' + l.prov.why].filter(Boolean) }, `${l.kind}: ${l.label}`, true, `<div class="muted small">${esc(key)} · placement: ${esc(l.placement)}</div>` + fieldsPre(l.f))); }
function openLicAll(){
  const S = settleLic(state.md, state.who);
  const card = l => srcCard({ ...l, prov:{ who:l.placement }, rank:l.placement === 'province' ? 4 : 0, notes:[] }, `${l.kind}: ${l.label}`, true, fieldsPre(l.f));
  openSheet('Licence sources', `${PLACE.name} · ${fmtMd(state.md)}`, `<div class="lbl">Records considered (${S.all.length})</div>${S.all.map(x => card(x.l)).join('')}`);
}
/* ---------- Gear & licence: one view. A row of answer tiles; tap one to see its detail. ---------- */
const KIT_TABS = [
  ['licence', 'Licence'], ['line', 'Line & hooks'], ['bait', 'Bait'], ['ways', 'Ways to fish'], ['boats', 'Boats'], ['always', 'Always']
];
const shortDoc = n => n.replace(/^Conservation Surcharge Stamp for (.+)$/i, '$1 stamp').replace(/^(.+?) Conservation Surcharge Stamp$/i, (m, x) => lc(x) + ' stamp').replace(/^Conservation Surcharge Stamp for Kootenay Lake rainbow trout.*$/i, 'Kootenay rainbow stamp');
function renderKit(){
  const el = document.getElementById('kit'); if (!el) return;
  let h = waterStrip(PLACE, state.md);
  if (MODEL.broad.length){
    h += closedBanner('rules') + `<p class="muted small">No gear may go in the water and no licence is needed while fishing is closed.</p>`;
    el.innerHTML = h; return;
  }
  const G = gearParts(), L = licParts();
  const base = L.buy.filter(b => b.base), extra = L.buy.filter(b => !b.base);
  const tiles = {
    licence: { v: L.none ? 'None needed' : base.length ? base.map(b => b.name.replace(/^Basic angling licence$/i, 'Basic licence')).join(' + ') : 'Basic licence', s: extra.length ? '+ ' + shortDoc(extra[0].name) + (extra.length > 1 ? ` and ${extra.length - 1} more` : '') + ' if needed' : 'Nothing extra' },
    line: { v: G.tags[1], s: [G.tags[0], ...G.tags.slice(3)].filter(Boolean).join(' · ') },
    bait: { v: G.baitBan ? 'No bait' : 'Some bait OK', s: G.baitBan ? 'Bait ban' : (G.baitList || []).map(b => `${({ worms:'Worms', roe:'Roe', invertebrate:'Insects', fin_fish:'Fish parts' })[b.e] || b.e}\u00a0${b.ok ? '✓' : '✕'}`).join(' · '), html: !G.baitBan && (G.baitList || []).length ? (G.baitList || []).map(b => `<span class="bk ${b.ok ? 'ok' : 'no'}">${esc(({ worms:'Worms', roe:'Roe', invertebrate:'Insects', fin_fish:'Fish parts' })[b.e] || b.e)}\u00a0<i>${b.ok ? '✓' : '✕'}</i></span>`).join('') : null },
    ways: { v: G.waysNames[0] ? cap(lc(G.waysNames[0]).replace(/ \(rod and line\)/, '')) : 'None', s: G.waysNames.length > 1 ? 'Also ' + G.waysNames.slice(1).map(x => lc(x)).join(', ') : 'Nothing else' },
    boats: G.boats ? { v: G.boatFirst ? cap(G.boatFirst) : 'Rules apply', s: 'Tap for all boat rules' } : null,
    always: G.alwaysN ? { v: 'Handling fish', s: `${G.alwaysN} rules that apply everywhere` } : null,
  };
  const tabs = KIT_TABS.filter(([k]) => tiles[k]);
  if (!tiles[state.kit]) state.kit = 'licence';
  h += G.pre;
  h += `<div class="kgrid" role="tablist" aria-label="Gear and licence">${tabs.map(([k, t]) => `<button class="ktile${state.kit === k ? ' on' : ''}" role="tab" aria-selected="${state.kit === k}" type="button" data-kit="${k}"><span class="kl">${t}</span><b class="kv">${esc(tiles[k].v)}</b><span class="ks">${tiles[k].html || esc(tiles[k].s)}</span></button>`).join('')}</div>`;
  const body = { licence: L.body, line: G.line, bait: G.bait, ways: G.ways, boats: G.boats, always: G.always }[state.kit];
  const ti = tabs.findIndex(([k]) => k === state.kit), prev = tabs[ti - 1], next = tabs[ti + 1];
  h += `<section class="kpane" role="tabpanel"><div class="khead"><h3>${KIT_TABS.find(([k]) => k === state.kit)[1]}</h3><span class="knav">${prev ? `<button type="button" data-kit="${prev[0]}">‹ ${prev[1]}</button>` : ''}${next ? `<button type="button" data-kit="${next[0]}">${next[1]} ›</button>` : ''}</span></div>${body}${state.kit !== 'licence' ? '<button class="srcbtn" type="button" data-gearsrc="1">All gear sources</button>' : ''}</section>`;
  el.innerHTML = h;
}
/* ================= reference cases: the page's ladder against guide.cases ================= */
const CASES = JSON.parse(document.getElementById('cases').textContent);
const SHOWN = ['speaks', 'beside', 'shown', 'not_yet_mapped'];
function runCase(c){
  if (!c.ruleset) return { c, rows:[], ok: c.expect.length === 0, outside:true };
  const rs = CASES.rulesets[c.ruleset];
  const mem = [...(rs.reach || []).map(k => ({ r:CASES.rules[k], via:'reach' })), ...(rs.trib || []).map(k => ({ r:CASES.rules[k], via:'trib' }))];
  const [m, d] = c.date.split('-').map(Number);
  const res = LAD.effective(mem, c.water.kind, m * 100 + d, c.fish, { steelheadWater: !!c.anadromous_rainbow });
  const exp = new Map(c.expect.map(e => [e.id, e]));
  const rows = mem.map(({ r, via }) => {
    const k = r.entry_id + '::' + r.rule_id, e = exp.get(k), g = res.get(k);
    const es = e ? e.state + (e.partly_lifted ? ' · partly lifted' : '') : '—';
    const gs = g && SHOWN.includes(g.state) ? g.state + (g.partly ? ' · partly lifted' : '') : '—';
    const why = g && !SHOWN.includes(g.state) ? (g.state === 'lifted' ? 'lifted by ' : 'displaced by ') + (CASES.rules[g.by]?.label || g.by || '') : '';
    return { r, via, k, es, gs, why, same: es === gs };
  });
  return { c, rows, ok: rows.every(x => x.same) };
}
// our own cases, from rulings made while reviewing: each names only the rules it is about
function runOurs(c){
  const rs = CASES.rulesets[c.ruleset];
  const mem = [...(rs.reach || []).map(k => ({ r:CASES.rules[k], via:'reach' })), ...(rs.trib || []).map(k => ({ r:CASES.rules[k], via:'trib' }))];
  const [m, d] = c.date.split('-').map(Number);
  const res = LAD.effective(mem, c.water.kind, m * 100 + d, c.fish, {});
  const rows = Object.entries(c.expect).map(([k, es]) => { const g = res.get(k), gs = g && SHOWN.includes(g.state) ? g.state : '—'; const e2 = es === '-' ? '—' : es;
    return { r:CASES.rules[k], via:'reach', k, es:e2, gs, why: g && !SHOWN.includes(g.state) ? (g.state === 'lifted' ? 'lifted by ' : 'displaced by ') + (CASES.rules[g.by]?.label || '') : '', same: e2 === gs }; });
  return { c:{ ...c, shows:c.id.replace(/_/g, ' '), what_to_show:c.what }, rows, ok: rows.every(x => x.same), ours:true };
}
let CASE_RESULTS = null;
function renderCases(){
  const el = document.getElementById('cases-list'); if (!el) return;
  if (!CASE_RESULTS) CASE_RESULTS = [...(CASES.ours || []).map(runOurs), ...CASES.cases.map(runCase)];
  const all = document.getElementById('casesAll')?.checked;
  const n = CASE_RESULTS.filter(x => x.ok).length;
  const no = (CASES.ours || []).length;
  let h = `<div class="casesum"><b>${n} of ${CASE_RESULTS.length}</b> cases pass: ${CASE_RESULTS.length - no} from the export (the pipeline’s own answers, <code>effective_rules</code>) and ${no} of ours, written from rulings made during review (see Reference notes). Each case is one water, one day and one fish.</div>`;
  h += `<label class="small muted"><input type="checkbox" id="casesAll"${all ? ' checked' : ''}> Show gear, conduct and province-wide rules too</label>`;
  h += CASE_RESULTS.map(({ c, rows, ok, outside, ours }) => {
    const fams = all ? null : ['retention', 'access'];
    const vis = rows.filter(x => all || (fams.includes(x.r.family) && !x.k.startsWith('zp:')) || !x.same);
    const inPlay = vis.filter(x => x.gs !== '—' || x.es !== '—');
    const quiet = vis.filter(x => x.gs === '—' && x.es === '—');
    const rowHtml = x => `<tr class="${x.same ? '' : 'miss'}"><td><span class="cst ${x.gs.split(' ')[0].replace('—', 'none')}">${esc(x.gs)}</span>${x.same ? '' : `<div class="small">reference: ${esc(x.es)}</div>`}</td><td>${esc(RANK[String(x.via === 'trib' && x.r.provenance.rank >= 0 ? 1 : x.r.provenance.rank)]?.t || '')}</td><td>${esc(x.r.label)}${x.why ? `<div class="small muted">${esc(x.why)}</div>` : ''}</td></tr>`;
    return `<details class="case${ok ? '' : ' bad'}"><summary><span class="cok">${ok ? '✓' : '✕'}</span>${ours ? '<span class="ourtag">ours</span>' : ''}<span><b>${esc(cap(c.shows))}</b><span class="muted small"> · ${esc(c.water.name)} · ${esc(fmtMd(+c.date.replace('-', '')))} · ${esc(FISH[c.fish]?.name || c.fish)}</span></span></summary>
      <p class="small">${esc(c.what_to_show)}</p>${c.ruling ? `<p class="small muted">Ruling: ${esc(c.ruling)}</p>` : ''}
      ${outside ? '<p class="small muted">Outside B.C.: no rules at all.</p>' : `<table class="ctab"><tbody>${inPlay.map(rowHtml).join('')}</tbody></table>
      ${quiet.length ? `<details class="small"><summary class="muted">${quiet.length} rules here that don’t speak for this fish on this day</summary><table class="ctab"><tbody>${quiet.map(rowHtml).join('')}</tbody></table></details>` : ''}`}
    </details>`;
  }).join('');
  el.innerHTML = h;
}
document.addEventListener('change', e => { if (e.target.id === 'casesAll') renderCases(); });
document.getElementById('tabs').addEventListener('click', e => { if (e.target.closest('[data-v="cases"]')) renderCases(); });
/* ================= chrome ================= */
function defaultPart(wi){
  // open on the biggest part that isn't closed all year
  const parts = WATERS[wi].parts.map((p, i) => ({ i, n:p.sections, pl:makePlace(wi, i) }));
  const ok = parts.filter(x => [115, 415, 715, 1015].some(md => !settle(x.pl, md).active.some(isBroad)));
  // a river opens on the stretch that reaches its mouth (where most people fish), else the biggest open part
  const mouth = ok.find(x => (WATERS[wi].parts[x.i].runs || []).some(r => r.to === 'mouth' && !r.branch));
  return (mouth || ok[0] || parts[0]).i;
}
function dateChips(){
  const s = new Set();
  PLACE.cands.forEach(r => { if (applies(r, PLACE) && r.wins.length && (r.type === 'retention_limit' || r.f.gear)) r.wins.forEach(([a, b]) => { s.add(a); s.add(DAYS[(DAYS.indexOf(b) + 1) % 365]); }); });
  const arr = [...s].filter(x => x !== 101 || s.size < 4).sort((a, b) => a - b).slice(0, 6);
  return `<button class="dchip now" data-md="${TODAY}" type="button">Today</button>` + arr.map(md => `<button class="dchip" data-md="${md}" type="button">${fmtMd(md)}</button>`).join('');
}
function renderParts(){
  const water = WATERS[state.wi], el = document.getElementById('partrow');
  if (water.parts.length < 2){ el.innerHTML = ''; return; }
  const G = partGroups(water), cur = groupOf(water, state.pi), L = partLabels(water);
  const label = g => g.closed ? (g.idx.length > 1 ? `Closed all year · ${g.idx.length} stretches` : `Closed all year · ${L[g.idx[0]]}`) : L[g.idx[0]];
  el.innerHTML = `<label class="partsel"><span class="lbl">Which part of ${esc(water.name)}? <span class="muted">(${G.length} choice${G.length > 1 ? 's' : ''}${G.length < water.parts.length ? `, ${water.parts.length} stretches` : ''}${water.outside_bc ? `; ${water.outside_bc} section${water.outside_bc > 1 ? 's' : ''} outside B.C., not governed here` : ''})</span></span><select id="part">${(() => {
    // parts from the same regulation entry sit under one heading: the place is said once, each choice says what's different
    const pl0 = g => { const pl = g.closed ? '' : partPlace(water, g.idx[0]); return pl ? cap(label(g).slice(pl.length).replace(/^( · |: )/, '')) : label(g); };
    const opt = (g, txt) => `<option value="${g.idx[0]}"${g === cur ? ' selected' : ''}>${esc(txt)} · ${g.sections} section${g.sections > 1 ? 's' : ''}</option>`;
    const G2 = [...G].sort((a, b) => (a.closed - b.closed) || partKm(water.parts[a.idx[0]]) - partKm(water.parts[b.idx[0]]));
    const heads = new Map(); G2.forEach(g => { const pl = g.closed ? '' : partPlace(water, g.idx[0]); (heads.get(pl) || heads.set(pl, []).get(pl)).push(g); });
    if (heads.size < 2) return G2.map(g => opt(g, pl0(g))).join('');
    return [...heads].map(([pl, gs]) => { const body = gs.map(g => opt(g, pl ? cap(label(g).slice(pl.length).replace(/^( · |: )/, '')) : label(g))).join(''); return pl ? `<optgroup label="${esc(pl)}">${body}</optgroup>` : body; }).join('');
  })()}</select></label>`;
}
function renderAll(keep){
  PLACE = makePlace(state.wi, state.pi);
  MODEL = buildModel(settle(PLACE, state.md));
  REOPEN = MODEL.broad.length ? nextOpen(PLACE, state.md) : null;
  const water = WATERS[state.wi];
  document.querySelectorAll('[data-f="name"]').forEach(e => e.textContent = water.name);
  document.querySelectorAll('[data-f="sub"]').forEach(e => e.textContent = `${water.kind} · ${(() => { const n = water.parts.length > 1 ? groupOf(water, state.pi).sections : water.sections; return `${n} section${n > 1 ? 's' : ''}`; })()}${water.parts.length > 1 ? (() => { const G = partGroups(water), g = groupOf(water, state.pi); const L = g.closed && g.idx.length > 1 ? 'closed all year' : partLabels(water)[state.pi]; const rl = g.closed ? '' : runsLabel(water.parts[state.pi]); if (rl) return ' · ' + cutWords(rl, 150); const pl = g.closed ? '' : partPlace(water, state.pi); const rest = cap(cutWords((pl ? L.slice(pl.length).replace(/^( · |: )/, '') : L).replace(/^Stretch with: /, '').split(/: | · /)[0], 70)); return ' · ' + (pl ? pl + ': ' : '') + rest; })() : ''}`);
  document.querySelectorAll('[data-f="date"]').forEach(e => e.textContent = fmtMd(state.md));
  document.getElementById('dchips').innerHTML = dateChips();
  if (!keep){ state.open = new Set(); state.dd = new Set(); }
  renderParts(); renderCard(); renderKit();
}
const wEl = document.getElementById('waters');
WATERS.forEach((w, i) => { const b = document.createElement('button'); b.type = 'button'; b.className = 'wbtn'; b.setAttribute('aria-pressed', i === state.wi);
  b.innerHTML = `<b>${esc(w.name)}</b><span>${esc(w.kind)} · ${w.parts.length > 1 ? w.parts.length + ' parts' : 'one set of rules'}${w.parts.some(p => (p.lic || []).some(([k]) => LIC[k].kind === 'designation')) ? ' · classified' : ''}${w.part_of ? ' · lake part' : ''}</span>`;
  b.onclick = () => { state.wi = i; state.pi = defaultPart(i); state.lpi = 0; [...wEl.children].forEach((c, j) => c.setAttribute('aria-pressed', j === i)); renderAll(); }; wEl.appendChild(b); });
const dIn = document.getElementById('dateIn');
function setMd(md){ state.md = md; dIn.value = `2026-${String(Math.floor(md/100)).padStart(2,'0')}-${String(md%100).padStart(2,'0')}`; renderAll(true); }
dIn.onchange = () => { const [, m, d] = dIn.value.split('-').map(Number); if (m && d) setMd(m === 2 && d === 29 ? 228 : m*100 + d); };
if (document.getElementById('cardMode')) document.getElementById('cardMode').addEventListener('click', e => { const b = e.target.closest('[data-m]'); if (!b) return; state.all = b.dataset.m === 'all'; if (!state.all) state.open = new Set(); document.querySelectorAll('#cardMode button').forEach(x => x.setAttribute('aria-pressed', x === b)); renderCard(); });
function setTab(v){ state.tab = v; document.querySelectorAll('#tabs button').forEach(b => b.setAttribute('aria-selected', b.dataset.v === v)); document.querySelectorAll('.view').forEach(s => s.classList.toggle('on', s.id === 'v-' + v)); }
document.getElementById('tabs').addEventListener('click', e => { const b = e.target.closest('[data-v]'); if (b) setTab(b.dataset.v); });
document.addEventListener('change', e => {
  if (e.target.id === 'part'){ state.pi = +e.target.value; renderAll(); }
  if (e.target.id === 'lpart'){ state.lpi = +e.target.value; renderKit(); }
  if (e.target.dataset.axis){ state.who[e.target.dataset.axis] = e.target.value; renderKit(); }
});
document.addEventListener('click', e => {
  const md = e.target.closest('[data-md]'); if (md){ setMd(+md.dataset.md); return; }
  const lic = e.target.closest('[data-lic]'); if (lic){ e.stopPropagation(); openLic(lic.dataset.lic); return; }
  if (e.target.closest('[data-licsrc]')){ openLicAll(); return; }
  if (e.target.closest('[data-gearsrc]')){ openGearAll(); return; }
  const rl = e.target.closest('[data-rule]'); if (rl && rl.dataset.rule){ e.stopPropagation(); openRule(rl.dataset.rule); return; }
  const src = e.target.closest('[data-src]'); if (src){ e.stopPropagation(); openRowSources(src.dataset.src); return; }
  const t = e.target.closest('[data-toggle]');
  if (t){ const id = t.dataset.toggle, g = t.dataset.g;
    if ((state.open.has(id) || state.all) && g === undefined){ state.open.delete(id); if (state.all){ state.all = false; state.open = new Set(MODEL.rows.map((_, i) => 'r' + i).filter(x => x !== id)); document.querySelectorAll('#cardMode button').forEach(x => x.setAttribute('aria-pressed', x.dataset.m === 'sum')); } }
    else state.open.add(id);
    const wasOpen = !state.open.has(id) && !state.all;
    renderCard();
    // keep the row in view: after opening, its header at the top; after closing, its summary where the eye was
    const art = document.querySelector(`#card [data-row="${id}"]`);
    if (art && g === undefined){ const r = art.getBoundingClientRect(); if (!wasOpen ? (r.top < 0 || r.top > innerHeight * 0.6) : r.top < 0) art.scrollIntoView({ block:'start', behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth' }); }
    if (g !== undefined){ const el = document.getElementById(`g-${id}-${g}`); if (el){ el.scrollIntoView({ block:'center', behavior:'smooth' }); el.classList.add('flash'); } }
    return; }
  const kt = e.target.closest('[data-kit]'); if (kt){ state.kit = kt.dataset.kit; renderKit(); if (kt.classList.contains('ktile')){ const pn = document.querySelector('.kpane'); if (pn && pn.getBoundingClientRect().top > innerHeight * .6) pn.scrollIntoView({ block:'start', behavior:'smooth' }); } return; }
  const fg = e.target.closest('[data-fishgroup]'); if (fg){ e.stopPropagation(); openFishGroup(fg.dataset.fishgroup.split(',')); return; }
  if (e.target.closest('[data-howtell]')){ e.stopPropagation(); openSheet('Hatchery or wild?', 'The adipose fin', adiposeHtml()); return; }
  const fish = e.target.closest('[data-fish]'); if (fish){ e.stopPropagation(); openFish(fish.dataset.fish, fish.dataset.target != null ? +fish.dataset.target : null); return; }
  const jump = e.target.closest('[data-bagjump]'); if (jump && jump.dataset.closesheet) scrim.hidden = true; if (jump){ e.stopPropagation(); const el = document.querySelector(`#diagram .bag[data-row="${jump.dataset.bagjump}"]`); if (el){ el.scrollIntoView({ block:'start', behavior:'smooth' }); el.classList.add('flash'); setTimeout(() => el.classList.remove('flash'), 1200); } return; }
  const bag = !e.target.closest('details, summary, button, a') && e.target.closest('#diagram [data-row]'); if (bag){ if (window.matchMedia('(max-width:760px)').matches) setTab('card'); const el = document.querySelector(`#card [data-row="${bag.dataset.row}"]`); if (el){ el.scrollIntoView({ block:'start', behavior:'smooth' }); el.classList.add('flash'); setTimeout(() => el.classList.remove('flash'), 1200); } }
});
document.addEventListener('keydown', e => { if (e.key === 'Escape') scrim.hidden = true; });
scrim.addEventListener('click', e => { if (e.target === scrim) scrim.hidden = true; });
document.getElementById('shClose').onclick = () => scrim.hidden = true;
// sticky row headers sit just under the sticky tab bar (phone layout)
const setStick = () => { const t = document.getElementById('tabs'); const on = t && getComputedStyle(t).position === 'sticky' && t.offsetHeight; document.documentElement.style.setProperty('--stick', on ? (t.offsetHeight + 6) + 'px' : '0px'); };
addEventListener('resize', setStick); setStick();
state.pi = defaultPart(0);
renderAll();

document.addEventListener('toggle', e => { const k = e.target.dataset && e.target.dataset.dd; if (k){ if (e.target.open) state.dd.add(k); else state.dd.delete(k); } }, true);

