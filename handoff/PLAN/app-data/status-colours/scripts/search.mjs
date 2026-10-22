// T47 candidate search: which closed / own / boundary colours keep the map's statuses apart AS DRAWN.
//
//   node handoff/PLAN/app-data/status-colours/scripts/search.mjs <theme> <fixed JSON> [samples]
//
// <fixed JSON> pins tokens the search must not move (e.g. {"base":"#0072B2"}) and may give hue families:
//   {"base":"#0072B2", "own":"orange", "closed":"red"}   families: red | orange | yellow | purple | any
// The score is the WORST ΔE2000 over: the three map statuses pairwise (thin line over every ground, lake fill, satellite),
// each boundary against each map status (thin line), region vs mu, and each status against its ground — across normal
// vision and protan/deutan/tritan under both models (colour-math.mjs). Constraints that the app's gates already hold are
// HARD (a candidate failing one is discarded): pill text 4.5:1 for closed on its tint, own/base 3:1 on the card, closed
// stays a red and base a blue (water-status-cvd.test.ts "base is blue, closed is red"), boundaries are neutral.
// Random restarts + coordinate descent; deterministic (seeded).
import { contrast, groundsFor, GROUNDS, LAKE_GROUND, LINE_ALPHA, over, themeTokens, VISIONS, dEUnder, parseHex } from "./colour-math.mjs";

const [theme = "light", fixedArg = "{}", nArg = "4000"] = process.argv.slice(2);
const fixed = JSON.parse(fixedArg);
const t = themeTokens(theme === "cvd" ? "cvd" : theme);
const card = t["color.ui.card"];
const lakeAlpha = Number(t["opacity.lake"]);
const grounds = Object.values(groundsFor(theme));
const sat = Object.values(GROUNDS.satellite);

let seed = 7;
const rnd = () => ((seed = (seed * 1103515245 + 12345) % 2147483648) / 2147483648);
const hex = (r, g, b) => `#${[r, g, b].map((v) => Math.max(0, Math.min(255, Math.round(v))).toString(16).padStart(2, "0")).join("")}`;

function hsl(h) { // hex -> [h deg, s, l]
  const [r, g, b] = parseHex(h).map((v) => v / 255);
  const mx = Math.max(r, g, b), mn = Math.min(r, g, b), l = (mx + mn) / 2, d = mx - mn;
  if (!d) return [0, 0, l];
  const s = d / (1 - Math.abs(2 * l - 1));
  const hh = mx === r ? ((g - b) / d) % 6 : mx === g ? (b - r) / d + 2 : (r - g) / d + 4;
  return [(hh * 60 + 360) % 360, s, l];
}
const FAMILY = {
  red: (h) => { const [r, g, b] = parseHex(h); return r > g * 1.8 && r > b * 1.8; },
  blue: (h) => { const [r, g, b] = parseHex(h); return b > r && b > g * 0.9; },
  orange: (h) => { const [hh, s] = hsl(h); return hh >= 22 && hh <= 45 && s > 0.6; },
  yellow: (h) => { const [hh, s] = hsl(h); return hh >= 45 && hh <= 62 && s > 0.6; },
  purple: (h) => { const [hh, s] = hsl(h); return hh >= 270 && hh <= 330 && s > 0.3; },
  neutral: (h) => { const [, s] = hsl(h); return s < 0.08; },
  any: () => true,
};
const famOf = (k) => (typeof fixed[k] === "string" && FAMILY[fixed[k]] ? fixed[k] : null);
const isFixedHex = (k) => typeof fixed[k] === "string" && fixed[k].startsWith("#");

const pairWorst = (a, b, gs, alphaA, alphaB) => {
  let m = Infinity;
  for (const v of VISIONS) for (const g of gs) m = Math.min(m, dEUnder(over(a, g, alphaA), over(b, g, alphaB), v));
  return m;
};

function score(c, explain = false) {
  const terms = [];
  const named = [];
  const push = (name, v) => { terms.push(v); if (explain) named.push([name, Number(v.toFixed(1))]); };
  const S = ["closed", "own", "base"];
  for (let i = 0; i < 3; i++)
    for (let j = i + 1; j < 3; j++) {
      const [a, b] = [c[S[i]], c[S[j]]];
      push(`${S[i]}/${S[j]} line`, pairWorst(a, b, grounds, LINE_ALPHA, LINE_ALPHA));
      push(`${S[i]}/${S[j]} lake`, pairWorst(a, b, [LAKE_GROUND[theme === "cvd" ? "cvd" : theme]], lakeAlpha, lakeAlpha));
      push(`${S[i]}/${S[j]} satellite`, pairWorst(a, b, sat, LINE_ALPHA, LINE_ALPHA));
    }
  for (const bnd of ["region", "mu"])
    for (const s of S) push(`${bnd}/${s} line`, pairWorst(c[bnd], c[s], grounds, 0.8, LINE_ALPHA));
  // region vs mu are told apart by width and dash as well; colour need only differ a little
  push("region/mu line (+10 for width+dash)", pairWorst(c.region, c.mu, grounds, 0.8, 0.8) + 10);
  // every line against its ground (pure colour vs ground): a status that vanishes into the land is no status
  for (const s of [...S, "region", "mu"]) for (const [gn, g] of Object.entries(groundsFor(theme))) for (const v of VISIONS)
    push(`${s} on ${gn} ${v} (+5)`, dEUnder(c[s], g, v) + 5);
  if (explain) return named.sort((a, b) => a[1] - b[1]).slice(0, 6);
  return Math.min(...terms);
}

function ok(c) {
  if (!FAMILY.red(c.closed) || !FAMILY.blue(c.base)) return false;
  for (const k of ["closed", "own", "region", "mu"]) { const f = famOf(k); if (f && !FAMILY[f](c[k])) return false; }
  if (!FAMILY.neutral(c.region) || !FAMILY.neutral(c.mu)) return false;
  if (contrast(c.closed, over(c.closed, card, 0.133)) < 4.5) return false;
  if (contrast(c.own, card) < 3 || contrast(c.base, card) < 3) return false;
  if (contrast(t["color.ui.on-accent"] ?? card, c.closed) < 4.5) return false;
  return true;
}

const start = { closed: t["color.status.closed"], own: t["color.status.own"], base: t["color.status.base"],
  region: "#555555", mu: "#777777" };
for (const k of Object.keys(start)) if (isFixedHex(k)) start[k] = fixed[k];
const free = Object.keys(start).filter((k) => !isFixedHex(k));

const randomFor = (k) => {
  for (let n = 0; n < 5000; n++) {
    const h = k === "region" || k === "mu" ? (() => { const v = rnd() * 255; return hex(v, v, v + (rnd() - 0.5) * 12); })()
      : hex(rnd() * 255, rnd() * 255, rnd() * 255);
    const fam = famOf(k) ?? (k === "closed" ? "red" : k === "base" ? "blue" : null);
    if (!fam || FAMILY[fam](h)) return h;
  }
  return start[k];
};

let best = { s: -Infinity, c: null };
const N = Number(nArg);
for (let n = 0; n < N; n++) {
  const c = { ...start };
  for (const k of free) c[k] = randomFor(k);
  if (!ok(c)) continue;
  const s = score(c);
  if (s > best.s) best = { s, c };
}
// coordinate descent on the best
let step = 16;
while (step >= 2 && best.c) {
  let improved = false;
  for (const k of free)
    for (const ch of [0, 1, 2])
      for (const d of [-step, step]) {
        const c = { ...best.c };
        const rgb = parseHex(c[k]);
        if (k === "region" || k === "mu") { for (let i = 0; i < 3; i++) rgb[i] += d; } else rgb[ch] += d;
        c[k] = hex(...rgb);
        if (!ok(c)) continue;
        const s = score(c);
        if (s > best.s + 0.01) { best = { s, c }; improved = true; }
      }
  if (!improved) step /= 2;
}
console.log(JSON.stringify({ theme, fixed, score: Number(best.s.toFixed(2)), ...best.c, weakest: best.c && score(best.c, true) }));
