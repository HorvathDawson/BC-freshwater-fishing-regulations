/* regs-v10: FISHART copied verbatim from page v36 (pipeline/deliver/answers/reference/page_v36.js). */
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
window.FISHART = FISHART;
