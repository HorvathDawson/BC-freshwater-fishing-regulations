#!/usr/bin/env node
/**
 * ICON PROPOSALS, ROUND 2 (owner: "a good one that works as favicon and launcher icon but looks human made").
 * PROPOSALS ONLY: nothing here is wired into the app.
 *
 *   node handoff/PLAN/app-integration/icon-proposals/round2/build.mjs
 *     -> <dir>/<direction>/{icon,foreground,monochrome,favicon}.svg + background.txt
 *     -> <dir>/round2.html   the comparison page (inline SVG, self-contained)
 *
 * Every mark is drawn the way a hand draws it, not from primitives: a tapered brush/gouge stroke along a spline
 * centreline, with a slow wobble in its width and a small one along its edge, seeded so a rebuild is identical.
 * Canvas = Android adaptive 108 x 108; the visible mask is the central 72; the art stays in the 66 safe circle.
 */
import { writeFileSync, mkdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const dir = fileURLToPath(new URL("./", import.meta.url));
const repo = fileURLToPath(new URL("../../../../../", import.meta.url));

/* ------------------------------------------------------------------ hand-drawing kit */
function rng(seed) {
  let s = seed >>> 0;
  return () => ((s = (Math.imul(s, 1664525) + 1013904223) >>> 0) / 4294967296);
}
/** Slow, non-repeating 1-D wobble in [-1, 1]: the hand's unsteadiness. */
function wobble(seed, freqs = [2.3, 5.1, 11.7, 23.9], amps = [0.45, 0.3, 0.17, 0.08]) {
  const r = rng(seed), ph = freqs.map(() => r() * 2 * Math.PI);
  return (u) => freqs.reduce((a, f, i) => a + amps[i] * Math.sin(2 * Math.PI * f * u + ph[i]), 0);
}
const add = (a, b) => [a[0] + b[0], a[1] + b[1]];
const sub = (a, b) => [a[0] - b[0], a[1] - b[1]];
const mul = (a, k) => [a[0] * k, a[1] * k];
const len = (a) => Math.hypot(a[0], a[1]);
const norm = (a) => { const l = len(a) || 1; return [a[0] / l, a[1] / l]; };

/** Catmull-Rom spline through control points, sampled densely. */
function spline(ctrl, closed = false, per = 24) {
  const n = ctrl.length, out = [];
  const P = (i) => closed ? ctrl[(i + n) % n] : ctrl[Math.max(0, Math.min(n - 1, i))];
  const segs = closed ? n : n - 1;
  for (let i = 0; i < segs; i++) {
    const p0 = P(i - 1), p1 = P(i), p2 = P(i + 1), p3 = P(i + 2);
    const c1 = add(p1, mul(sub(p2, p0), 1 / 6)), c2 = sub(p2, mul(sub(p3, p1), 1 / 6));
    for (let k = 0; k < per; k++) {
      const t = k / per, m = 1 - t;
      out.push([
        m * m * m * p1[0] + 3 * m * m * t * c1[0] + 3 * m * t * t * c2[0] + t * t * t * p2[0],
        m * m * m * p1[1] + 3 * m * m * t * c1[1] + 3 * m * t * t * c2[1] + t * t * t * p2[1],
      ]);
    }
  }
  if (!closed) out.push(ctrl[n - 1]);
  return out;
}
/** Width profile from [t, w] stops, eased between stops (cosine). */
function profile(stops) {
  return (t) => {
    for (let i = 1; i < stops.length; i++) {
      const [t0, w0] = stops[i - 1], [t1, w1] = stops[i];
      if (t <= t1) { const u = (t - t0) / (t1 - t0 || 1); const e = (1 - Math.cos(Math.PI * u)) / 2; return w0 + (w1 - w0) * e; }
    }
    return stops[stops.length - 1][1];
  };
}
/**
 * A tapered stroke as a closed outline: brush, pen or gouge. `w(t)` is the full width along the centreline;
 * `rough` is the edge wobble (canvas units), `breath` a slow swell of the width (fraction).
 */
function stroke(ctrl, w, { rough = 0.25, breath = 0.08, seed = 1, cap = "auto", per = 30, tmax = 1 } = {}) {
  const c = spline(ctrl, false, per);
  const L = [0];
  for (let i = 1; i < c.length; i++) L.push(L[i - 1] + len(sub(c[i], c[i - 1])));
  const T = L[L.length - 1];
  const eL = wobble(seed), eR = wobble(seed + 7), sw = wobble(seed + 13, [1.3, 3.1], [0.6, 0.4]);
  const left = [], right = [];
  for (let i = 0; i < c.length; i++) {
    const t = L[i] / T;
    if (t > tmax) break;
    const d = norm(sub(c[Math.min(i + 1, c.length - 1)], c[Math.max(i - 1, 0)]));
    const nrm = [-d[1], d[0]];
    const hw = Math.max(0, w(t) * (1 + breath * sw(t))) / 2;
    const taper = Math.min(1, hw / 0.6); // no edge wobble where the stroke is a hairline
    left.push(add(c[i], mul(nrm, hw + rough * eL(t * T / 30) * taper)));
    right.push(add(c[i], mul(nrm, -(hw + rough * eR(t * T / 30) * taper))));
  }
  const out = [...left];
  const endW = w(1) / 2;
  if (cap !== "flat" && tmax >= 1 && endW > 0.3) { // round end cap
    const d = norm(sub(c[c.length - 1], c[c.length - 2])), nrm = [-d[1], d[0]];
    for (let k = 1; k < 8; k++) { const a = (k / 8) * Math.PI; out.push(add(c[c.length - 1], add(mul(nrm, Math.cos(a) * endW), mul(d, Math.sin(a) * endW * 0.9)))); }
  }
  out.push(...right.reverse());
  const startW = w(0) / 2;
  if (cap !== "flat" && startW > 0.3) {
    const d = norm(sub(c[1], c[0])), nrm = [-d[1], d[0]];
    for (let k = 1; k < 8; k++) { const a = (k / 8) * Math.PI; out.push(add(c[0], add(mul(nrm, -Math.cos(a) * startW), mul(d, -Math.sin(a) * startW * 0.9)))); }
  }
  return out;
}
/** A closed hand-cut shape through control points, its edge wobbled. */
function blob(ctrl, { rough = 0.3, seed = 1, per = 18 } = {}) {
  const c = spline(ctrl, true, per), e = wobble(seed);
  let P = 0; const L = [0];
  for (let i = 1; i < c.length; i++) L.push((P += len(sub(c[i], c[i - 1]))));
  return c.map((p, i) => {
    const d = norm(sub(c[(i + 1) % c.length], c[(i - 1 + c.length) % c.length]));
    return add(p, mul([-d[1], d[0]], rough * e(L[i] / 30)));
  });
}
/** A hand-drawn circle: never quite round. */
function ring(cx, cy, r, { rough = 0.3, seed = 1, lean = 0.03 } = {}) {
  const k = [], n = 9, r0 = rng(seed);
  for (let i = 0; i < n; i++) {
    const a = (i / n) * 2 * Math.PI + r0() * 0.15;
    const rr = r * (1 + (r0() - 0.5) * lean * 2);
    k.push([cx + rr * Math.cos(a), cy + rr * Math.sin(a)]);
  }
  return blob(k, { rough, seed });
}
const area = (p) => p.reduce((a, q, i) => { const r = p[(i + 1) % p.length]; return a + q[0] * r[1] - r[0] * q[1]; }, 0) / 2;
/** Orient a contour: fills positive, holes negative, so one nonzero path can carry cut-outs. */
const orient = (p, hole) => ((area(p) > 0) === !hole ? p : [...p].reverse());

/** Closed contour -> smooth cubic path (Catmull-Rom through every other sample). */
function d(pts, step = 3) {
  const p = pts.filter((_, i) => i % step === 0);
  const n = p.length, f = (v) => +v.toFixed(2);
  let s = `M${f(p[0][0])} ${f(p[0][1])}`;
  for (let i = 0; i < n; i++) {
    const p0 = p[(i - 1 + n) % n], p1 = p[i], p2 = p[(i + 1) % n], p3 = p[(i + 2) % n];
    const c1 = add(p1, mul(sub(p2, p0), 1 / 6)), c2 = sub(p2, mul(sub(p3, p1), 1 / 6));
    s += `C${f(c1[0])} ${f(c1[1])} ${f(c2[0])} ${f(c2[1])} ${f(p2[0])} ${f(p2[1])}`;
  }
  return s + "Z";
}
/** One compound path: fills and holes (holes cut through, so the layer under shows). */
const compound = (fills, holes = []) => fills.map((p) => d(orient(p, false))).concat(holes.map((p) => d(orient(p, true)))).join("");

/** Affine helpers: rotate/scale about a point, applied to every contour of a direction. */
function affine({ rot = 0, s = 1, cx = 54, cy = 54, tx = 0, ty = 0 }) {
  const c = Math.cos((rot * Math.PI) / 180), sn = Math.sin((rot * Math.PI) / 180);
  return (p) => { const x = (p[0] - cx) * s, y = (p[1] - cy) * s; return [cx + tx + x * c - y * sn, cy + ty + x * sn + y * c]; };
}
/** Fit: rotate, then scale/translate so every point of `pts` sits inside radius `R` of the canvas centre. */
function fitter(all, { rot = 0, R = 31, dy = 0 } = {}) {
  const r = affine({ rot });
  const q = all.map(r);
  const xs = q.map((p) => p[0]), ys = q.map((p) => p[1]);
  const mx = (Math.min(...xs) + Math.max(...xs)) / 2, my = (Math.min(...ys) + Math.max(...ys)) / 2;
  const maxR = Math.max(...q.map((p) => Math.hypot(p[0] - mx, p[1] - my)));
  const s = R / maxR;
  return (p) => { const v = r(p); return [54 + (v[0] - mx) * s, 54 + dy + (v[1] - my) * s]; };
}
const X = (fn, contours) => contours.map((c) => c.map(fn));
const P = (fill, path, extra = "") => `<path fill="${fill}" d="${path}"${extra}/>`;

/* ------------------------------------------------------------------ 1 · Linocut trout */
function linocut(small = false) {
  const C = { paper: "#EFE6D0", ink: "#1D2925", coral: "#D4573B" };
  const s = small ? 0 : 1; // texture only at launcher sizes
  const body = stroke([[21.5, 55.5], [34, 53.4], [50, 52.8], [65, 53.8], [78.5, 55.6]],
    profile([[0, 0], [0.008, 4.2], [0.03, 8.4], [0.09, 14.6], [0.3, 21], [0.52, 19.2], [0.76, 11], [0.92, 7.2], [1, 6.8]]),
    { rough: 0.32 * s, breath: 0.05, seed: 11, cap: "flat" });
  const tail = blob([[75.6, 51.6], [80.6, 48.6], [85.6, 44.6], [89.6, 41.4], [88.6, 46.8], [86.8, 51.6], [86.2, 55.8], [86.9, 60.4], [88.8, 65], [90, 69.2], [85.6, 66.2], [80.4, 62.4], [75.6, 59.4]], { rough: 0.3 * s, seed: 12 });
  const dorsal = blob([[46.5, 45.5], [50.2, 39.2], [54.6, 37.1], [58.6, 38.4], [60, 44.6]], { rough: 0.25 * s, seed: 13 });
  const pect = blob([[34.4, 59.6], [38.4, 61.6], [43.4, 66.4], [38.6, 65.2], [34, 62.6]], { rough: 0.2 * s, seed: 14 });
  const pelvic = blob([[54.6, 62], [58.6, 64.6], [62.6, 67], [59.6, 61.2]], { rough: 0.2 * s, seed: 15 });
  const anal = blob([[67.4, 58.2], [70.6, 61.2], [75, 63.4], [73, 57.6]], { rough: 0.2 * s, seed: 16 });
  const adipose = blob([[70, 50.6], [72.2, 47.7], [74.4, 48.4], [74.6, 51]], { rough: 0.1 * s, seed: 17 });
  const stripe = stroke([[36.5, 56.4], [50, 56.1], [64, 56.5], [77.5, 56.9]],
    profile([[0, 0], [0.12, small ? 7 : 5.2], [0.6, small ? 6.4 : 4.8], [0.92, small ? 4 : 2.6], [1, 0]]), { rough: 0.2 * s, seed: 18 });
  const eyeR = small ? 3 : 2.35;
  const eye = ring(28.6, 52.3, eyeR, { rough: 0.05, seed: 19 });
  const pupil = ring(28.2, 52.5, small ? 1.4 : 1.15, { rough: 0.05, seed: 20 });
  // Gouge cuts: thin tapering slivers, the way a V-tool leaves them.
  const g = (pts, w, seed) => stroke(pts, profile([[0, 0], [0.35, w], [0.7, w * 0.8], [1, 0]]), { rough: 0.08, seed, breath: 0.15 });
  const cuts = small ? [] : [
    g([[33.4, 46.6], [36.9, 51.2], [37.5, 56.6], [35.3, 61.6]], 1.55, 21), // gill cover
    g([[22.6, 56.4], [25, 56.7], [27.4, 57.4]], 0.9, 22),                 // mouth
    ...[[40, 49.6], [45.5, 48.4], [51, 47.7], [56.5, 47.9], [62, 48.8], [67, 50.2]].map(([x, y], i) => g([[x, y + 0.9], [x + 2.4, y], [x + 4.8, y + 0.1]], 1.05, 30 + i)),
    ...[[41, 61.6], [47, 62.2], [53, 62.1], [62, 60.9], [67.5, 59.9]].map(([x, y], i) => g([[x, y - 0.4], [x + 2.6, y + 0.2], [x + 5, y - 0.2]], 0.95, 40 + i)),
    ...[[86.4, 45.4], [85.1, 51], [84.8, 57.6], [86, 63.4]].map(([x, y], i) => g([[79.4, 54.3 + i * 0.9], [(79.4 + x) / 2, (54.3 + y) / 2 + 0.3], [x, y]], 0.9, 50 + i)),
    g([[51.6, 40.6], [54.6, 38.9]], 0.7, 60), g([[52.6, 43], [57, 40.4]], 0.7, 61),
  ];
  const spots = small ? [] : [[44, 46.6], [49, 45.6], [59.5, 45.2], [64.5, 47], [69, 52.7], [82.4, 52.4], [84, 48.6], [83.8, 60], [82.6, 64.4], [55.6, 50.6]]
    .map(([x, y], i) => ring(x, y, 0.62 + (i % 3) * 0.12, { rough: 0.06, seed: 70 + i }));
  const fills = [body, tail, dorsal, pect, pelvic, anal, adipose];
  const all = fills.flat();
  const fit = fitter(all, { rot: small ? 30 : 18, R: small ? 41 : 32 });
  const F = (c) => X(fit, c);
  const inkD = compound(F(fills), F([eye, ...cuts, ...spots]));
  const pupilD = compound(F([pupil]));
  const stripeD = compound(F([stripe]));
  return {
    C,
    fg: (c = C) => P(c.ink, inkD) + P(c.coral, stripeD) + P(c.ink, pupilD),
    mono: (k) => P(k, inkD + pupilD),
  };
}

/* ------------------------------------------------------------------ 2 · Scratchboard dry fly */
function dryfly(small = false) {
  const C = { ground: "#1B2530", cream: "#EFE6D2", rust: "#C9663D", wing: "#9AA7AE" };
  const s = small ? 0 : 1;
  const tw = (w, a = 0.25) => profile([[0, w], [0.75, w * 0.85], [1, w * a]]);
  const hook = stroke([[79, 64], [62, 64.3], [46, 64.6], [38.4, 66], [34.6, 71], [35.8, 77.2], [41.4, 79.6], [46.8, 76.6], [48.3, 71.6]],
    tw(small ? 4.2 : 2.3, small ? 0.6 : 0.2), { rough: 0.12 * s, seed: 101, breath: 0.1 });
  const barb = stroke([[48.1, 72.6], [47.6, 75.2], [46.2, 76.4]], profile([[0, 1.4], [1, 0]]), { rough: 0, seed: 102 });
  const eyeO = ring(80.6, 63.4, small ? 3.6 : 2.9, { rough: 0.08, seed: 103 });
  const eyeI = ring(80.6, 63.4, small ? 1.5 : 1.55, { rough: 0.05, seed: 104 });
  const body = stroke([[42.5, 62.6], [50, 62.1], [57, 61.4], [62, 61]],
    profile([[0, small ? 5 : 3], [0.3, small ? 6.5 : 4.2], [1, small ? 8.5 : 6.6]]), { rough: 0.25 * s, seed: 105, breath: 0.12 });
  const rib = small ? [] : [[45.6], [49.8], [54], [58.2]].map(([x], i) =>
    stroke([[x - 0.6, 59.3 + i * 0.12], [x + 0.2, 62], [x + 1.1, 64.6 - i * 0.18]], profile([[0, 0.2], [0.5, 0.75], [1, 0.2]]), { rough: 0, seed: 110 + i }));
  const tails = small ? [stroke([[44, 62.4], [32, 59], [22, 56.4]], profile([[0, 3.4], [1, 1.4]]), { rough: 0, seed: 120 })]
    : [[20.6, 55.4], [19.6, 58.8], [21, 62.4], [23.2, 53]].map(([x, y], i) =>
      stroke([[44, 62.4], [(44 + x) / 2, (62.4 + y) / 2 - 0.9 + i * 0.25], [x, y]], profile([[0, 1.15], [0.8, 0.5], [1, 0.15]]), { rough: 0, seed: 120 + i, breath: 0.2 }));
  // Hackle: a collar of fibres wound round the hook, each one scratched out separately.
  const hc = [66.6, 61.6], fibres = [], r0 = rng(130);
  const nF = small ? 0 : 26;
  for (let i = 0; i < nF; i++) {
    const a = (i / nF) * 2 * Math.PI + (r0() - 0.5) * 0.18;
    const L = 15.4 + (r0() - 0.5) * 3.6, bend = (r0() - 0.5) * 0.25;
    const p = (rr, b) => [hc[0] + rr * Math.cos(a + b), hc[1] + rr * Math.sin(a + b) * 0.92];
    fibres.push(stroke([p(1.5, 0), p(L * 0.55, bend * 0.5), p(L, bend)], profile([[0, 1.6], [0.6, 1.05], [1, 0.18]]), { rough: 0, seed: 140 + i, breath: 0.2 }));
  }
  if (small) { // the hackle as one jagged disc
    const k = [];
    for (let i = 0; i < 22; i++) { const a = (i / 22) * 2 * Math.PI; const rr = i % 2 ? 9.6 : 13.6; k.push([hc[0] + rr * Math.cos(a), hc[1] + rr * Math.sin(a) * 0.92]); }
    fibres.push(blob(k, { rough: 0, seed: 131, per: 6 }));
  }
  const collar = ring(hc[0], hc[1], small ? 5 : 3.4, { rough: 0.1, seed: 132 });
  const wingP = profile([[0, 1.2], [0.15, small ? 8.4 : 6.2], [0.55, small ? 9.4 : 7.4], [0.88, small ? 6 : 4.4], [1, 0]]);
  const wingBack = stroke([[67.4, 57.5], [68.6, 46], [70.6, 34], [73.4, 26.2]], wingP, { rough: 0.2 * s, seed: 150 });
  const wingFront = stroke([[65.4, 57.5], [63.4, 45], [60.8, 33.6], [57.4, 25.6]], wingP, { rough: 0.2 * s, seed: 151 });
  const vein = (pts, seed) => stroke(pts, profile([[0, 0], [0.2, 0.75], [0.8, 0.55], [1, 0]]), { rough: 0, seed });
  const veins = small ? [] : [
    vein([[64.6, 51], [62.7, 41.4], [60.4, 31.6]], 152), vein([[63.4, 48], [61.4, 41], [59.5, 35.4]], 153), vein([[66.3, 48], [64.6, 40.5], [62.4, 34.6]], 154),
  ];
  const veinsBack = small ? [] : [vein([[68.6, 51], [69.6, 41.4], [71.6, 31.6]], 155)];
  const all = [hook, eyeO, body, ...tails, ...fibres, wingBack, wingFront].flat();
  const fit = fitter(all, { rot: -6, R: small ? 38.5 : 31.6 });
  const F = (c) => X(fit, c);
  const lineD = compound(F([hook, barb, eyeO, ...tails, ...fibres, collar]), F([eyeI]));
  const bodyD = compound(F([body]), F(rib));
  const backD = compound(F([wingBack]), F(veinsBack));
  const frontD = compound(F([wingFront]), F(veins));
  return {
    C,
    fg: (c = C) => P(c.cream, lineD) + P(c.rust, bodyD) + P(c.wing, backD) + P(c.cream, frontD),
    // Themed: one ink; the front wing gets a ground-coloured keyline so it parts from the back wing.
    mono: (k) => P(k, lineD + bodyD + backD + frontD),
  };
}

/* ------------------------------------------------------------------ 3 · Cloisonné sockeye badge */
function badge(small = false) {
  const C = { enamel: "#17604A", brass: "#E4C47E", red: "#C83E2C", head: "#6E8B3D", deep: "#0F4A39" };
  const s = small ? 0 : 1;
  // A spawning sockeye in a leap: hump back, hooked jaw, head left.
  const body = stroke([[25.4, 52.6], [35, 47.6], [48, 46.4], [61, 50.6], [72, 58.6], [79.6, 66.8]],
    profile([[0, 0], [0.012, 4.4], [0.05, 9.4], [0.2, 17.4], [0.38, 21.6], [0.6, 15], [0.84, 7.8], [1, 6.4]]),
    { rough: 0.22 * s, seed: 201, cap: "flat", breath: 0.04 });
  const hump = blob([[40.4, 43.4], [45, 35.6], [51.4, 33.6], [57.4, 36.6], [61, 42.6], [52, 46], [43, 46]], { rough: 0.12 * s, seed: 202 });
  const kype = stroke([[26.2, 51.6], [23.6, 53.2], [23.4, 55.8]], profile([[0, 2.6], [0.6, 1.8], [1, 0.6]]), { rough: 0.05 * s, seed: 203 });
  const tail = blob([[77.6, 63], [82.4, 66.4], [91, 66.2], [86.4, 71.2], [88.4, 80.6], [81.4, 74.6], [76.4, 71]], { rough: 0.2 * s, seed: 204 });
  const head = stroke([[25.4, 52.6], [35, 47.6], [48, 46.4], [61, 50.6], [72, 58.6], [79.6, 66.8]],
    profile([[0, 0], [0.012, 4.4], [0.05, 9.4], [0.2, 17.4], [0.38, 21.6], [0.6, 15], [0.84, 7.8], [1, 6.4]]),
    { rough: 0.22 * s, seed: 201, cap: "flat", breath: 0.04, tmax: 0.21 });
  const fins = [
    blob([[66.4, 58], [70.6, 52.6], [73.4, 52.4], [71.6, 59.6]], { rough: 0.1 * s, seed: 205 }), // adipose-ish dorsal tail
    blob([[44, 55.2], [46.4, 62.6], [50.8, 63.2], [51, 56.4]], { rough: 0.1 * s, seed: 206 }),   // pectoral
    blob([[63, 59], [63.4, 65.4], [67.6, 65.6], [68, 61]], { rough: 0.1 * s, seed: 207 }),       // anal
  ];
  const eye = ring(31.3, 50.8, small ? 2.2 : 1.55, { rough: 0.04, seed: 208 });
  const all = [body, hump, kype, tail, ...fins].flat();
  const fit = fitter(all, { rot: 8, R: small ? 41 : 23.4, dy: small ? 0 : -3.6 });
  const F = (c) => X(fit, c);
  const fishD = compound(F([body, hump, kype, tail, ...fins]));
  const headD = compound(F([head, kype]));
  const eyeD = compound(F([eye]));
  // Wires: the gill line, a lateral seam, and two hand-bent water wires under the leap.
  const wire = (pts, w, seed) => stroke(pts, profile([[0, w * 0.6], [0.15, w], [0.85, w], [1, w * 0.6]]), { rough: 0.05, seed, breath: 0.15 });
  const gill = stroke([[36.6, 39.6], [38.6, 46.6], [37.6, 55.6]], profile([[0, 0.3], [0.3, 1.2], [1, 0.4]]), { rough: 0, seed: 210 });
  const waves = small ? [] : [
    wire([[33.4, 76.6], [39, 74.4], [45, 76.8], [51, 74.6], [57, 76.6], [63, 74.4], [69, 76.4], [74.6, 74.8]], 1.35, 211),
    wire([[39.4, 81.6], [45, 79.6], [51, 81.6], [57, 79.6], [63, 81.4], [68.6, 79.8]], 1.2, 212),
  ];
  const rimO = ring(54, 54, 33, { rough: 0.12, seed: 220, lean: 0.006 }), rimI = ring(54, 54, 30.6, { rough: 0.12, seed: 221, lean: 0.006 });
  const rim2O = ring(54, 54, 28.9, { rough: 0.08, seed: 222, lean: 0.005 }), rim2I = ring(54, 54, 28.05, { rough: 0.08, seed: 223, lean: 0.005 });
  const rimD = small ? "" : compound([rimO, rim2O], [rimI, rim2I]);
  const wiresD = compound([...F([gill]), ...waves]);
  const sw = small ? 4.2 : 1.15;
  return {
    C,
    fg: (c = C) => (rimD ? P(c.brass, rimD) : "")
      + P(c.red, fishD, ` stroke="${c.brass}" stroke-width="${sw}" stroke-linejoin="round" paint-order="stroke"`)
      + P(c.head, headD, ` stroke="${c.brass}" stroke-width="${sw * 0.8}" stroke-linejoin="round"`)
      + P(c.brass, wiresD) + P(c.brass, eyeD),
    mono: (k) => (rimD ? P(k, rimD) : "") + P(k, fishD + (small ? "" : "") , "") + P(k, compound(waves)),
  };
}

/* ------------------------------------------------------------------ 4 · Rubber-stamp question mark + float */
function stamp(small = false) {
  const C = { kraft: "#D8BB8E", ink: "#1E2A45", red: "#C9372C", cream: "#F5ECDB" };
  const s = small ? 0 : 1;
  // "Can I Fish?" — the question, brush-lettered; its point is a fishing float.
  const q = stroke([[38.4, 41.4], [40.6, 33.4], [47.6, 28], [56.4, 27.2], [64.6, 30.6], [68.8, 37.8], [67.2, 45.2], [60.8, 50.6], [55.4, 55], [53.8, 60.6], [53.9, 66.2]],
    profile([[0, small ? 11.6 : 10], [0.05, small ? 9.4 : 7.8], [0.2, small ? 9.6 : 8], [0.5, small ? 11 : 10], [0.72, small ? 9 : 7.2], [0.86, small ? 8.4 : 6.6], [1, small ? 9 : 7.6]]),
    { rough: 0.38 * s, breath: 0.1, seed: 301 });
  // Stamp wear: a few specks where the rubber didn't take ink, kept off the thin parts.
  const specks = small ? [] : [[41, 37.6, 0.7], [59.6, 29.2, 0.55], [66.4, 39.4, 0.8], [63.2, 46.4, 0.5], [54.6, 63.4, 0.6], [47.4, 30.6, 0.45]]
    .map(([x, y, r], i) => ring(x, y, r, { rough: 0.15, seed: 310 + i, lean: 0.2 }));
  // A pear float (the classic red-topped bobber), point down: round head, tapering body, never a ball.
  const pear = [[54, 70.4], [58.8, 72.2], [60.4, 76.8], [58.8, 81.8], [56, 86], [54, 88.8], [52, 86], [49.2, 81.8], [47.6, 76.8], [49.2, 72.2]];
  const shrink = (pts, k, c = [54, 79.6]) => pts.map((p) => add(c, mul(sub(p, c), k)));
  const floatO = blob(small ? shrink(pear, 1.12) : pear, { rough: 0.2 * s, seed: 320 });
  const inner = shrink(pear, 0.74);
  const floatI = blob(inner, { rough: 0.15 * s, seed: 321 });
  // The red cap: the head of the float down to a slightly tilted hand line.
  const cap = blob([inner[9], inner[0], inner[1], inner[2], [60.2 - 1.4, 79.6], [54.4, 79.4], [48.6, 79]], { rough: 0.1 * s, seed: 322 });
  const peg = stroke([[54, 66], [54.1, 68.4], [54, 71.4]], profile([[0, small ? 3 : 2.2], [1, small ? 3.4 : 2.6]]), { rough: 0, seed: 323 });
  const band = stroke([[48, 80.8], [54.4, 81.4], [60.4, 80.4]], profile([[0, 0.5], [0.5, 1.2], [1, 0.5]]), { rough: 0, seed: 324 });
  const all = [q, floatO, peg].flat();
  const fit = fitter(all, { rot: 4, R: small ? 39 : 31.2 });
  const F = (c) => X(fit, c);
  const qD = compound(F([q]), F(specks));
  const floatD = small ? compound(F([floatO])) : compound(F([floatO]), F([floatI]));
  const pegD = compound(F([peg]));
  const capD = compound(F([cap]));
  const bandD = compound(F([band]));
  return {
    C,
    fg: (c = C) => P(c.ink, qD + pegD) + (small ? P(c.red, compound(F([floatO]))) : P(c.ink, compound(F([floatO]))) + P(c.red, capD)),
    mono: (k) => P(k, qD + pegD + (small ? floatD : compound(F([floatO])) )) + (small ? "" : ""),
  };
}

/* ------------------------------------------------------------------ directions */
const svg = (vb, body, w, h) => `<svg xmlns="http://www.w3.org/2000/svg" viewBox="${vb}"${w ? ` width="${w}" height="${h ?? w}"` : ""}>${body}</svg>`;

const mk = (id, name, craft, line, build, bgKey, favBg) => {
  const big = build(false), small = build(true);
  return { id, name, craft, line, big, small, bg: big.C[bgKey], favBg: favBg ?? big.C[bgKey] };
};
const DIRS = [
  mk("1-linocut-trout", "Linocut trout", "Two-block linocut",
    "A rainbow trout cut in lino: one ink block with V-gouge marks for gill, scales and tail rays, and a second coral block for the rainbow band. The 16 px cut drops the gouges and keeps fish + band + eye.",
    linocut, "paper"),
  mk("2-scratchboard-fly", "Scratchboard dry fly", "Scratchboard / pen",
    "A hand-tied dry fly scratched out of a black board: hackle fibres one by one, upright split wings, a rust dubbed body on the hook. The 16 px cut turns the hackle into one jagged disc.",
    dryfly, "ground"),
  mk("3-enamel-sockeye", "Enamel sockeye badge", "Cloisonné enamel pin",
    "A spawning sockeye (red body, green hooked head) leaping, set in brass wire on park-green enamel inside a hand-bent double rim, like a club or licence pin. 16 px drops rim and water.",
    badge, "enamel"),
  mk("4-stamp-question", "Stamped ? float", "Rubber stamp / brush letter",
    "The app's own question, \"Can I Fish?\": a brush-lettered question mark stamped in navy on kraft tag stock, its point a navy pear float with a red cap. Stamp-wear specks at large sizes only; 16 px keeps ? + red float.",
    stamp, "kraft"),
];

/* ------------------------------------------------------------------ files */
for (const D of DIRS) {
  const out = `${dir}${D.id}/`;
  mkdirSync(out, { recursive: true });
  writeFileSync(out + "icon.svg", svg("18 18 72 72", `<rect width="108" height="108" fill="${D.bg}"/>${D.big.fg()}`, 1024));
  writeFileSync(out + "foreground.svg", svg("0 0 108 108", D.big.fg(), 432));
  writeFileSync(out + "monochrome.svg", svg("0 0 108 108", D.big.mono("#000000"), 432));
  writeFileSync(out + "background.txt", `${D.bg}\n`);
  writeFileSync(out + "favicon.svg", svg("0 0 108 108", `<rect x="2" y="2" width="104" height="104" rx="22" fill="${D.favBg}"/>${D.small.fg()}`, 32));
}

/* ------------------------------------------------------------------ the comparison page */
const fav = readFileSync(repo + "app/apps/mobile/assets/icon/favicon.svg", "utf8");
const favInner = /<svg[^>]*>([\s\S]*)<\/svg>/.exec(fav)[1].replace(/id="[^"]*"/g, "");
const curPng = "data:image/png;base64," + readFileSync(repo + "app/apps/mobile/assets/icon/icon-1024.png").toString("base64");
const CUR = {
  id: "current", name: "Current", craft: "—", line: "Sky-blue outline fish with a pin (favicon.svg / icon-1024.png). Shown for comparison.",
  bg: "#FFFFFF", favBg: null,
  big: { fg: () => `<g transform="translate(54 54) scale(${54 / 22.7}) translate(-12.06 -11.85)">${favInner}</g>`,
    mono: (k) => `<g transform="translate(54 54) scale(${54 / 22.7}) translate(-12.06 -11.85)">${favInner.replaceAll("#0EA5E9", k)}</g>` },
  small: { fg: () => `<g transform="translate(54 54) scale(${100 / 24}) translate(-12 -12)">${favInner}</g>` },
};

function squircle(cx = 54, cy = 54, r = 36, n = 4.2) {
  const pts = [];
  for (let i = 0; i < 96; i++) {
    const t = (i / 96) * 2 * Math.PI, c = Math.cos(t), s = Math.sin(t);
    pts.push([cx + r * Math.sign(c) * Math.abs(c) ** (2 / n), cy + r * Math.sign(s) * Math.abs(s) ** (2 / n)]);
  }
  return "M" + pts.map(([x, y]) => `${x.toFixed(2)} ${y.toFixed(2)}`).join(" L") + " Z";
}
const SQ = squircle();
let uid = 0;
function launcher(D, px, shape = "squircle") {
  const id = `c${uid++}`;
  const clip = shape === "circle" ? `<circle cx="54" cy="54" r="36"/>` : shape === "square" ? `<rect x="18" y="18" width="72" height="72"/>` : `<path d="${SQ}"/>`;
  return svg("18 18 72 72", `<defs><clipPath id="${id}">${clip}</clipPath></defs><g clip-path="url(#${id})"><rect width="108" height="108" fill="${D.bg}"/>${D.big.fg()}</g>`, px);
}
function themed(D, px, dark) {
  const id = `t${uid++}`, g = dark ? "#2B3A33" : "#D8E8DC", k = dark ? "#BCE3C8" : "#1F4A31";
  return svg("18 18 72 72", `<defs><clipPath id="${id}"><circle cx="54" cy="54" r="36"/></clipPath></defs><g clip-path="url(#${id})"><rect width="108" height="108" fill="${g}"/>${D.big.mono(k)}</g>`, px);
}
function favicon(D, px) {
  if (!D.favBg) return svg("0 0 24 24", favInner, px);
  return svg("0 0 108 108", `<rect x="2" y="2" width="104" height="104" rx="22" fill="${D.favBg}"/>${D.small.fg()}`, px);
}
const enc = (s) => "data:image/svg+xml;base64," + Buffer.from(s).toString("base64");
/** A raster preview: the favicon drawn into an N px canvas, then blown up with hard pixels. */
const pix = (D, n, zoom) => `<canvas class="pix" width="${n}" height="${n}" style="width:${n * zoom}px;height:${n * zoom}px" data-src="${enc(favicon(D, n))}"></canvas>`;

function tab(D, dark) {
  return `<div class="tab ${dark ? "dk" : ""}"><div class="tabbar"><div class="t on">${favicon(D, 16)}<span>Can I Fish?</span><i>×</i></div><div class="t">${svg("0 0 16 16", `<circle cx="8" cy="8" r="6" fill="${dark ? "#555" : "#bbb"}"/>`, 16)}<span>Inbox</span><i>×</i></div></div><div class="url">canifish.app</div></div>`;
}
function phone(D, dark) {
  const ph = (c) => `<div class="app"><div class="ph" style="background:${c}"></div><span>&nbsp;</span></div>`;
  const cols = dark ? ["#3a3f46", "#4a4440", "#3b4640", "#463b48"] : ["#c9d4dc", "#e2d5c3", "#cfdccc", "#ddcfe0"];
  const cells = [];
  for (let i = 0; i < 12; i++) cells.push(i === 5 ? `<div class="app">${launcher(D, 46, "circle")}<span>Can I Fish?</span></div>` : ph(cols[i % 4]));
  return `<div class="phone ${dark ? "dk" : ""}"><div class="clock">9:41</div><div class="grid">${cells.join("")}</div><div class="dock">${ph(cols[0])}${ph(cols[1])}${ph(cols[2])}${ph(cols[3])}</div></div>`;
}
function block(D, cur = false) {
  const sizes = [192, 48, 32, 16].map((px) => `<figure>${cur && px === 192 ? `<img src="${curPng}" width="192" height="192">` : launcher(D, px, "square")}<figcaption>${px}</figcaption></figure>`).join("");
  return `<section class="dir">
  <header><h2>${D.name}</h2><span class="craft">${D.craft}</span>${cur ? "" : `<code>round2/${D.id}/</code>`}<p>${D.line}</p>${cur ? "" : `<p class="sw">ground <b style="background:${D.bg}"></b>${D.bg}</p>`}</header>
  <div class="row">
    <figure class="hero">${cur ? `<img src="${curPng}" width="360" height="360">` : launcher(D, 360, "square")}<figcaption>1024 master (shown at 360)</figcaption></figure>
    <div class="col">
      <div class="sizes">${sizes}</div>
      <div class="sizes"><figure>${favicon(D, 32)}<figcaption>favicon 32</figcaption></figure><figure>${favicon(D, 16)}<figcaption>favicon 16</figcaption></figure>
        <figure>${pix(D, 16, 5)}<figcaption>16 px, pixels ×5</figcaption></figure><figure>${pix(D, 32, 3)}<figcaption>32 px, pixels ×3</figcaption></figure></div>
      <div class="sizes"><figure>${launcher(D, 96, "circle")}<figcaption>circle</figcaption></figure><figure>${launcher(D, 96, "squircle")}<figcaption>squircle</figcaption></figure>
        <figure>${themed(D, 72, false)}<figcaption>themed light</figcaption></figure><figure>${themed(D, 72, true)}<figcaption>themed dark</figcaption></figure>
        <figure class="tabs">${tab(D, false)}${tab(D, true)}<figcaption>browser tab, light / dark</figcaption></figure></div>
    </div>
    <div class="phones">${phone(D, false)}${phone(D, true)}</div>
  </div>
</section>`;
}

const html = `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Icon Round Two</title>
<style>
:root{--bg:#F4F2EC;--card:#fff;--ink:#15181C;--sub:#5d625f;--line:#D3D5CF}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--bg:#F4F2EC;--card:#fff;--ink:#15181C;--sub:#5d625f;--line:#D3D5CF}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;padding:24px 16px}
main{max-width:1240px;margin:0 auto}h1{margin:0 0 4px;font-size:26px}.lede{color:var(--sub);max-width:820px;margin:0 0 20px}
.dir{background:var(--card);border:1px solid var(--line);padding:16px 18px 18px;margin:0 0 18px}
header h2{display:inline;margin:0 10px 0 0;font-size:19px}.craft{font-size:12px;text-transform:uppercase;letter-spacing:.06em;color:var(--sub);margin-right:10px}
code{font-size:12px;color:var(--sub)}header p{margin:6px 0 0;max-width:900px}.sw{font-size:12px;color:var(--sub)}.sw b{display:inline-block;width:12px;height:12px;border:1px solid #0003;vertical-align:-2px;margin:0 4px}
.row{display:flex;gap:22px;align-items:flex-start;margin-top:12px;flex-wrap:wrap}.col{display:flex;flex-direction:column;gap:14px;flex:1;min-width:340px}
.sizes{display:flex;gap:16px;align-items:flex-end;flex-wrap:wrap}figure{margin:0;display:flex;flex-direction:column;align-items:center;gap:4px}
figcaption{font-size:11px;color:var(--sub)}.hero svg,.hero img{border:1px solid var(--line)}
canvas.pix{image-rendering:pixelated;border:1px solid var(--line)}
.tabs{align-items:stretch;gap:6px}.tab{width:250px;background:#DEE1E6;border-radius:8px 8px 0 0;font:12px system-ui;color:#1f1f1f;overflow:hidden}
.tab.dk{background:#1F2023;color:#E8EAED}.tabbar{display:flex;gap:2px;padding:6px 6px 0}.t{display:flex;align-items:center;gap:7px;padding:7px 10px;border-radius:8px 8px 0 0;flex:1;min-width:0;opacity:.75}
.t.on{background:#fff;opacity:1}.dk .t.on{background:#35363A}.t span{flex:1;white-space:nowrap;overflow:hidden}.t i{font-style:normal;opacity:.6}.url{background:#fff;padding:5px 10px;color:#5f6368}.dk .url{background:#35363A;color:#bdc1c6}
.phones{display:flex;gap:12px}.phone{width:178px;height:330px;border-radius:24px;border:5px solid #111;background:linear-gradient(160deg,#E9E3D6,#C9D6D2);padding:10px;display:flex;flex-direction:column}
.phone.dk{background:linear-gradient(160deg,#20262B,#101315)}.clock{font:600 11px system-ui;color:#222;margin-bottom:16px}.dk .clock{color:#eee}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:16px 4px;flex:1;align-content:start}.app{display:flex;flex-direction:column;align-items:center;gap:3px}
.app span{font:9px system-ui;color:#222;white-space:nowrap}.dk .app span{color:#eee}.ph{width:34px;height:34px;border-radius:50%}.app svg{width:34px;height:34px}
.dock{display:grid;grid-template-columns:repeat(4,1fr);gap:4px;padding-top:6px}.dock .app span{display:none}
</style></head><body><main>
<h1>Icon proposals · round 2</h1>
<p class="lede">Four hand-made directions for the launcher icon and favicon, each from a real craft. Every line is a tapered stroke or a hand-cut shape with a slow wobble, not a circle-and-rectangle kit. Each has a 16/32 px cut that keeps the same mark with the texture dropped. Folders hold icon.svg, foreground.svg (adaptive, 108 canvas, art inside the 66 safe circle), background.txt, monochrome.svg and favicon.svg. Proposals only; nothing is wired in. Built by <code>round2/build.mjs</code>.</p>
${block(CUR, true)}
${DIRS.map((D) => block(D)).join("\n")}
</main>
<script>
document.querySelectorAll("canvas.pix").forEach(function(c){var i=new Image();i.onload=function(){var x=c.getContext("2d");x.drawImage(i,0,0,c.width,c.height)};i.src=c.dataset.src;});
</script></body></html>`;
writeFileSync(dir + "round2.html", html);
console.log("wrote", DIRS.map((D) => D.id).join(", "), "+ round2.html");
