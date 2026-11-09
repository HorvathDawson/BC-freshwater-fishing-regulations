#!/usr/bin/env node
/**
 * ICON PROPOSALS, ROUND 3 (owner on round 2: "more regulation map + fish themed. i like linocut one a bit").
 * PROPOSALS ONLY: nothing here is wired into the app. Four linocut variations on a regulation map + a trout.
 *
 *   node handoff/PLAN/app-integration/icon-proposals/round3/build.mjs
 *     -> <dir>/<direction>/{icon,foreground,monochrome,favicon}.svg + background.txt
 *     -> <dir>/round3.html   the comparison page (inline SVG, self-contained)
 *
 * The hand-drawing kit and the round-2 trout are copied from ../round2/build.mjs (kept standalone on purpose).
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
function stroke(ctrl, w, { rough = 0.25, breath = 0.08, seed = 1, cap = "auto", per = 30, tmin = 0, tmax = 1 } = {}) {
  const c = spline(ctrl, false, per);
  const L = [0];
  for (let i = 1; i < c.length; i++) L.push(L[i - 1] + len(sub(c[i], c[i - 1])));
  const T = L[L.length - 1];
  const eL = wobble(seed), eR = wobble(seed + 7), sw = wobble(seed + 13, [1.3, 3.1], [0.6, 0.4]);
  const left = [], right = [];
  for (let i = 0; i < c.length; i++) {
    const t = L[i] / T;
    if (t > tmax) break;
    if (t < tmin) continue;
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
  if (cap !== "flat" && tmin <= 0 && startW > 0.3) {
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


/* ================================================================== ROUND 3: regulation map + fish, all in lino */
// App map tokens (packages/map/style/themes/light.json + dark.json). On ink the DARK theme's set is used (it is the
// set made to sit on a dark ground), printed a touch muted the way a lino ink is; on paper the LIGHT set, as is.
const M = {
  paper: "#EFE6D0", ink: "#1D2925", coral: "#D4573B",
  openL: "#016890", ownL: "#D08404", closedL: "#8F0006", lakeDk: "#205579",
  waterD: "#38B6DE", ownD: "#F2C230", closedD: "#E8475A",
  waterP: "#56A9C4", ownP: "#E2AA2C", closedP: "#D4453A", // the same three, as a printer would mix them for lino
};
const BAND = [[36.5, 56.8], [43, 55.2], [50, 57.4], [57, 55.2], [64, 57.3], [71, 55.6], [77.5, 56.9]];
/** The round-2 trout, in its own frame (head left), as contours. */
function troutParts(s = 1) {
  const body = stroke([[21.5, 55.5], [34, 53.4], [50, 52.8], [65, 53.8], [78.5, 55.6]],
    profile([[0, 0], [0.008, 4.2], [0.03, 8.4], [0.09, 14.6], [0.3, 21], [0.52, 19.2], [0.76, 11], [0.92, 7.2], [1, 6.8]]),
    { rough: 0.32 * s, breath: 0.05, seed: 11, cap: "flat" });
  const fins = [
    blob([[75.6, 51.6], [80.6, 48.6], [85.6, 44.6], [89.6, 41.4], [88.6, 46.8], [86.8, 51.6], [86.2, 55.8], [86.9, 60.4], [88.8, 65], [90, 69.2], [85.6, 66.2], [80.4, 62.4], [75.6, 59.4]], { rough: 0.3 * s, seed: 12 }),
    blob([[46.5, 45.5], [50.2, 39.2], [54.6, 37.1], [58.6, 38.4], [60, 44.6]], { rough: 0.25 * s, seed: 13 }),
    blob([[34.4, 59.6], [38.4, 61.6], [43.4, 66.4], [38.6, 65.2], [34, 62.6]], { rough: 0.2 * s, seed: 14 }),
    blob([[54.6, 62], [58.6, 64.6], [62.6, 67], [59.6, 61.2]], { rough: 0.2 * s, seed: 15 }),
    blob([[67.4, 58.2], [70.6, 61.2], [75, 63.4], [73, 57.6]], { rough: 0.2 * s, seed: 16 }),
    blob([[70, 50.6], [72.2, 47.7], [74.4, 48.4], [74.6, 51]], { rough: 0.1 * s, seed: 17 }),
  ];
  const g = (pts, w, seed) => stroke(pts, profile([[0, 0], [0.35, w], [0.7, w * 0.8], [1, 0]]), { rough: 0.08, seed, breath: 0.15 });
  return {
    fills: [body, ...fins],
    eye: (r = 2.35) => ring(28.6, 52.3, r, { rough: 0.05, seed: 19 }),
    pupil: (r = 1.15) => ring(28.2, 52.5, r, { rough: 0.05, seed: 20 }),
    gill: g([[33.4, 46.6], [36.9, 51.2], [37.5, 56.6], [35.3, 61.6]], 1.55, 21),
    mouth: g([[22.6, 56.4], [25, 56.7], [27.4, 57.4]], 0.9, 22),
    back: [[40, 49.6], [45.5, 48.4], [51, 47.7], [56.5, 47.9], [62, 48.8], [67, 50.2]].map(([x, y], i) => g([[x, y + 0.9], [x + 2.4, y], [x + 4.8, y + 0.1]], 1.05, 30 + i)),
    belly: [[41, 61.6], [47, 62.2], [53, 62.1], [62, 60.9], [67.5, 59.9]].map(([x, y], i) => g([[x, y - 0.4], [x + 2.6, y + 0.2], [x + 5, y - 0.2]], 0.95, 40 + i)),
    rays: [[86.4, 45.4], [85.1, 51], [84.8, 57.6], [86, 63.4]].map(([x, y], i) => g([[79.4, 54.3 + i * 0.9], [(79.4 + x) / 2, (54.3 + y) / 2 + 0.3], [x, y]], 0.9, 50 + i)),
    fin: [g([[51.6, 40.6], [54.6, 38.9]], 0.7, 60), g([[52.6, 43], [57, 40.4]], 0.7, 61)],
    spots: [[44, 46.6], [49, 45.6], [59.5, 45.2], [64.5, 47], [69, 52.7], [82.4, 52.4], [84, 48.6], [83.8, 60], [82.6, 64.4], [55.6, 50.6]]
      .map(([x, y], i) => ring(x, y, 0.62 + (i % 3) * 0.12, { rough: 0.06, seed: 70 + i })),
    g,
  };
}
/** A straight-edged hand cut (map sheet, block): every edge nicked a little, corners kept. */
function cutPoly(pts, { rough = 0.3, seed = 1, parts = 7 } = {}) {
  const r = rng(seed), out = [];
  for (let i = 0; i < pts.length; i++) {
    const a = pts[i], b = pts[(i + 1) % pts.length], dir = norm(sub(b, a)), nr = [-dir[1], dir[0]];
    for (let k = 0; k < parts; k++) out.push(add(add(a, mul(sub(b, a), k / parts)), mul(nr, k ? (r() - 0.5) * 2 * rough : 0)));
  }
  return out;
}
const dL = (pts) => "M" + pts.map((p) => `${p[0].toFixed(2)} ${p[1].toFixed(2)}`).join("L") + "Z";
const ringLine = (cx, cy, rx, ry, w, seed) => {
  const k = []; for (let i = 0; i <= 10; i++) { const a = (i / 10) * 2 * Math.PI; k.push([cx + rx * Math.cos(a) * (1 + 0.06 * Math.sin(3 * a + seed)), cy + ry * Math.sin(a)]); }
  return stroke(k, () => w, { rough: 0.04, seed, breath: 0.25 });
};

/* A layer: { d, c (colour key or hex), mono: "on" | "off" | "skip", key (keyline width, ground-coloured), only }. */
let mid = 0;
function paint(layers, C) {
  return layers.filter((l) => l.only !== "mono").map((l) => `<path fill="${C[l.c] ?? l.c}" d="${l.d}"${l.key ? ` stroke="${C[l.keyc ?? "ground"]}" stroke-width="${l.key}" stroke-linejoin="round" paint-order="stroke"` : ""}/>`).join("");
}
/** Monochrome: the layers stacked as a mask (on = ink, off = cut through), tinted with one colour. */
function monoPaint(layers, k) {
  const id = `m${mid++}`;
  const body = layers.filter((l) => l.mono !== "skip" && l.only !== "colour").map((l) => `<path fill="${l.mono === "off" ? "#000" : "#fff"}" d="${l.d}"${l.key ? ` stroke="#000" stroke-width="${l.key}" stroke-linejoin="round" paint-order="stroke"` : ""}/>`).join("");
  return `<mask id="${id}" maskUnits="userSpaceOnUse" x="0" y="0" width="108" height="108">${body}</mask><rect width="108" height="108" fill="${k}" mask="url(#${id})"/>`;
}
const built = (C, layers) => ({ C, fg: (c = C) => paint(layers, c), mono: (k) => monoPaint(layers, k) });

/* ------------------------------------------------------------------ A · Sectioned band */
function sectioned(small = false) {
  const C = { ...M, ground: M.paper };
  const s = small ? 0 : 1, T = troutParts(s);
  const fit = fitter(T.fills.flat(), { rot: small ? 30 : 18, R: small ? 41 : 32 });
  const F = (c) => X(fit, c);
  const w = small ? 6.8 : 4.4, gap = small ? 0.045 : 0.018;
  const bandP = profile([[0, 0], [0.12, w], [0.6, w * 0.92], [0.92, w * 0.55], [1, 0]]);
  const seg = (t0, t1) => stroke(BAND, bandP, { rough: 0.2 * s, seed: 18, tmin: t0, tmax: t1, cap: "flat" });
  const cuts = small ? [T.eye(3)] : [T.eye(), T.gill, T.mouth, ...T.back, ...T.belly, ...T.rays, ...T.fin, ...T.spots];
  // Section ticks: a gouge straight across the band where one stretch's rules end and the next begin.
  const tick = (x, sd) => T.g([[x - 0.5, 52.2], [x, 56.3], [x + 0.5, 60.6]], 1.2, sd);
  const ticks = small ? [] : [tick(51.2, 81), tick(64.6, 82)];
  return built(C, [
    { d: compound(F(T.fills), F(cuts)), c: "ink", mono: "on" },
    { d: compound(F([seg(0, 0.355 - gap)])), c: "waterP", mono: "off" },
    { d: compound(F([seg(0.355 + gap, 0.675 - gap)])), c: "ownP", mono: "off" },
    { d: compound(F([seg(0.675 + gap, 1)])), c: "closedP", mono: "off" },
    ...(ticks.length ? [{ d: compound(F(ticks)), c: "paper", mono: "off" }] : []),
    { d: compound(F([T.pupil(small ? 1.4 : 1.15)])), c: "ink", mono: "on" },
  ]);
}

/* ------------------------------------------------------------------ B · The trout is the map */
function mapTrout(small = false) {
  const C = { ...M, ground: "#E4E9DE", paper: "#E4E9DE" };
  const s = small ? 0 : 1, T = troutParts(s);
  const fit = fitter(T.fills.flat(), { rot: small ? 30 : 18, R: small ? 41 : 32 });
  const F = (c) => X(fit, c);
  const riverC = [[29, 58.6], [36.5, 60.6], [44, 57.6], [52, 60.4], [60, 57.4], [68, 59.4], [75.6, 56.4], [81, 56.2], [86.6, 55.8]];
  const rw = small ? profile([[0, 2.6], [1, 3.6]]) : profile([[0, 0.7], [0.4, 1.5], [1, 2.4]]);
  const river = (t0 = 0, t1 = 1) => stroke(riverC, rw, { rough: 0.12 * s, seed: 401, tmin: t0, tmax: t1, cap: t0 > 0 || t1 < 1 ? "flat" : "auto" });
  const tribs = small ? [] : [
    stroke([[47.6, 45.4], [50.2, 50.4], [48.2, 54.6], [49.6, 58.6]], profile([[0, 0.3], [1, 1.1]]), { rough: 0.05, seed: 402 }),
    stroke([[59, 66.4], [57.4, 62.8], [58.4, 58.6]], profile([[0, 0.3], [1, 1]]), { rough: 0.05, seed: 403 }),
    stroke([[39.6, 47.4], [41.4, 52.4], [40.2, 57.6], [40.8, 59.6]], profile([[0, 0.25], [1, 0.9]]), { rough: 0.05, seed: 404 }),
  ];
  // Contours: a hill on the back, drawn as the map draws it, nested hand loops.
  const topo = small ? [] : [ringLine(57.6, 47.2, 7.6, 3, 0.55, 405), ringLine(57.9, 47.1, 4.6, 1.75, 0.55, 406), ringLine(58.1, 47, 1.7, 0.7, 0.55, 407),
    ringLine(70.6, 53.1, 3.2, 1.6, 0.5, 408)];
  const cuts = small ? [T.eye(3)] : [T.eye(), T.gill, T.mouth, ...T.rays.slice(0, 1), ...T.fin];
  return built(C, [
    { d: compound(F(T.fills), F(cuts)), c: "ink", mono: "on" },
    { d: compound(F([...topo, ...tribs])), c: "paper", mono: "off" },
    { d: compound(F([river()])), c: "paper", mono: "off" },
    { d: compound(F([river(small ? 0.5 : 0.46, small ? 1 : 0.7)])), c: "ownD", mono: "off" },
    ...(small ? [] : [{ d: compound(F([river(0.74, 1)])), c: "closedD", mono: "off" }]),
    { d: compound(F([T.pupil(small ? 1.4 : 1.15)])), c: "ink", mono: "on" },
  ]);
}

/* ------------------------------------------------------------------ C · Trout on a folded map */
function foldedMap(small = false) {
  const C = { ...M, ground: M.lakeDk, shade: "#D6C7A4" };
  const s = small ? 0 : 1, T = troutParts(s);
  const xs = small ? [16, 35, 54, 73, 92] : [26.5, 40.3, 54, 67.7, 81.5];
  const top = small ? [26, 30, 26, 30, 26] : [33.5, 37, 33.5, 37, 33.5];
  const bot = small ? [82, 86, 82, 86, 82] : [74.5, 78, 74.5, 78, 74.5];
  const panels = [0, 1, 2, 3].map((i) => cutPoly([[xs[i], top[i]], [xs[i + 1], top[i + 1]], [xs[i + 1], bot[i + 1]], [xs[i], bot[i]]], { rough: 0.25 * s, seed: 500 + i }));
  // The river on the sheet, in the map's own light-theme status colours.
  const rc = [[27, 67.2], [33.6, 63.6], [40.6, 67.8], [47.6, 65], [55, 69.6], [62.4, 66.4], [69.6, 70.8], [76, 67.8], [81, 69]];
  const rp = profile([[0, small ? 3.4 : 1.4], [1, small ? 4 : 2.6]]);
  const rs = (t0, t1, seed) => stroke(rc, rp, { rough: 0.1 * s, seed: 510, tmin: t0, tmax: t1, cap: "flat" });
  const fit = fitter(T.fills.flat(), { rot: 14, R: small ? 33 : 24.5, dy: small ? -5 : -5.4 });
  const F = (c) => X(fit, c);
  const cuts = small ? [T.eye(3)] : [T.eye(), T.gill, T.mouth, ...T.back.slice(1, 5), ...T.rays, ...T.fin];
  const folds = small ? [] : [1, 2, 3].map((i) => stroke([[xs[i], top[i] + 0.4], [xs[i], bot[i] - 0.4]], () => 0.7, { rough: 0, seed: 520 + i }));
  return built(C, [
    ...panels.map((p, i) => ({ d: dL(p), c: i % 2 ? "shade" : "paper", mono: "on" })),
    ...(folds.length ? [{ d: compound(folds), c: "shade", mono: "off", only: "mono" }] : []),
    { d: compound([rs(0, 0.42)]), c: "openL", mono: "off" },
    { d: compound([rs(0.45, 0.7)]), c: "ownL", mono: "off" },
    { d: compound([rs(0.73, 1)]), c: "closedL", mono: "off" },
    { d: compound(F(T.fills), F(cuts)), c: "ink", mono: "off" },
    { d: compound(F([T.pupil(small ? 1.4 : 1.15)])), c: "ink", mono: "off" },
  ]);
}

/* ------------------------------------------------------------------ D · Coral trout on a carved river block */
function riverBlock(small = false) {
  const C = { ...M, ground: M.paper };
  const s = small ? 0 : 1, T = troutParts(s);
  const fit = fitter(T.fills.flat(), { rot: small ? 30 : 14, R: small ? 41 : 21.6, dy: small ? 0 : -4 });
  const F = (c) => X(fit, c);
  const disk = ring(54, 54, 31.4, { rough: 0.45, seed: 601, lean: 0.025 });
  const main = [[22.8, 63], [31, 69.4], [41, 66.6], [50, 74], [60.6, 72.2], [70, 78.4], [77.6, 75.4]];
  const mp = profile([[0, 1.2], [1, 3.2]]);
  const mainS = (t0 = 0, t1 = 1) => stroke(main, mp, { rough: 0.15, seed: 602, tmin: t0, tmax: t1, cap: t0 > 0 || t1 < 1 ? "flat" : "auto" });
  const tribs = [
    stroke([[27.6, 38], [33.6, 45], [30.6, 53.4], [35, 61], [33, 67.6]], profile([[0, 0.4], [1, 1.6]]), { rough: 0.1, seed: 603 }),
    stroke([[84.6, 49], [79.4, 56.4], [81, 63.6], [74.6, 70.4], [72.6, 76.8]], profile([[0, 0.4], [1, 1.7]]), { rough: 0.1, seed: 604 }),
    stroke([[44, 80.8], [47, 77], [46.6, 71.4]], profile([[0, 0.3], [1, 1]]), { rough: 0.05, seed: 605 }),
  ];
  // Contour gouges round the top of the block.
  const arc = (r, a0, a1, w, seed) => { const k = []; for (let i = 0; i <= 6; i++) { const a = a0 + (a1 - a0) * (i / 6); k.push([54 + r * Math.cos(a), 54 + r * Math.sin(a)]); } return stroke(k, profile([[0, 0], [0.15, w], [0.85, w], [1, 0]]), { rough: 0.06, seed, breath: 0.2 }); };
  const topo = [arc(27.4, -2.75, -0.42, 0.75, 606), arc(24.4, -2.45, -0.7, 0.7, 607), arc(21.6, -2.1, -1.05, 0.65, 608)];
  const fishCuts = small ? [] : [T.gill, T.mouth, ...T.rays.slice(1, 3)];
  if (small) {
    return built({ ...C, ground: M.ink }, [
      { d: compound(F(T.fills)), c: "coral", mono: "on" },
      { d: compound(F([T.eye(3)])), c: "ink", mono: "off" },
    ]);
  }
  return built(C, [
    { d: compound([disk]), c: "ink", mono: "on" },
    { d: compound([...topo, ...tribs, mainS()]), c: "paper", mono: "off" },
    { d: compound([mainS(0.4, 0.66)]), c: "ownD", mono: "off" },
    { d: compound([T.g([[56.2, 69.6], [55.2, 73.4], [54.6, 77]], 1.3, 609), T.g([[66.6, 71.6], [65.2, 75.4], [64.8, 79.2]], 1.3, 610)].map((p) => p)), c: "paper", mono: "off" },
    { d: compound(F(T.fills)), c: "coral", key: 2.2, keyc: "paper", mono: "on" },
    { d: compound(F([...fishCuts, T.eye()])), c: "ink", mono: "off" },
    { d: compound(F([T.pupil()])), c: "coral", mono: "on" },
  ]);
}

const svg = (vb, body, w, h) => `<svg xmlns="http://www.w3.org/2000/svg" viewBox="${vb}"${w ? ` width="${w}" height="${h ?? w}"` : ""}>${body}</svg>`;
const mk = (id, name, craft, line, build, favBg) => {
  const big = build(false), small = build(true);
  return { id, name, craft, line, big, small, bg: big.C.ground, favBg: favBg ?? small.C.ground };
};
const DIRS = [
  mk("1-sectioned-band", "Sectioned band", "Two-block linocut",
    "The round-2 trout, but its rainbow band is a river line cut into three regulation stretches (open water blue, own-rules amber, closed red: the map's status tokens mixed down to lino inks) with gouged section ticks between them. 16 px: fish + three-colour band.",
    sectioned),
  mk("2-map-trout", "The trout is the map", "Linocut, map engraving",
    "The fish's body is a carved map: a river with tributaries runs nose to tail, contour loops make a hill on its back, and the lower river is marked own-rules then closed. 16 px: fish with one paper river and an amber stretch.",
    mapTrout),
  mk("3-folded-map", "Trout on a folded map", "Lino on a cut-paper map",
    "A hand-cut, four-panel folded map with the river in the light-theme open / own / closed colours, and the ink trout leaping across it, on lake blue. 16 px: map sheet + dark fish; monochrome turns the fish into a cut-out.",
    foldedMap),
  mk("4-river-block", "Coral trout on a river block", "Round lino block",
    "A round ink block carved with a river network, contour gouges and one ticked own-rules stretch; a coral trout lies over it, keylined in paper. 16 px: the coral trout alone on the ink.",
    riverBlock),
];
const REF = { ...mk("../round2/1-linocut-trout", "Round 2 · Linocut trout (liked a bit)", "Reference", "The direction the owner liked; round 3 grows from it.", linocut), bg: "#EFE6D0", favBg: "#EFE6D0" };
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
  <header><h2>${D.name}</h2><span class="craft">${D.craft}</span>${cur ? "" : `<code>round3/${D.id}/</code>`}<p>${D.line}</p>${cur ? "" : `<p class="sw">ground <b style="background:${D.bg}"></b>${D.bg}</p>`}</header>
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
<title>Icon Round Three</title>
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
<h1>Icon proposals · round 3</h1>
<p class="lede">Regulation map + fish, all in the linocut craft the owner liked in round 2. Each is cut the way a lino block is: tapered gouges, hand-nicked edges, two or three inks. Status colours are the app map's own tokens, used once per icon. Each has a 16/32 px cut that keeps the same mark. Folders hold icon.svg, foreground.svg (adaptive 108 canvas, art inside the 66 safe circle), background.txt, monochrome.svg and favicon.svg. Proposals only; nothing is wired in. Built by <code>round3/build.mjs</code>.</p>
${block(CUR, true)}
${block(REF)}
${DIRS.map((D) => block(D)).join("\n")}
</main>
<script>
document.querySelectorAll("canvas.pix").forEach(function(c){var i=new Image();i.onload=function(){var x=c.getContext("2d");x.drawImage(i,0,0,c.width,c.height)};i.src=c.dataset.src;});
</script></body></html>`;
writeFileSync(dir + "round3.html", html);
console.log("wrote", DIRS.map((D) => D.id).join(", "), "+ round3.html");
