// T47 colour arithmetic, shared by measure.mjs and the candidate search.
//
// THE SAME MATHS AS THE APP'S GATES: simulate / deltaE / contrast are imported from app/tools/cvd.ts (Node strips the
// types), so a number in T47-colours.md is the number tools/cvd.test.ts and tools/water-status-cvd.test.ts would compute.
//
// "AS DRAWN" is modelled, not guessed at:
//   pure   the token against the token.
//   line   a thin status line (1-2 px at z8-12) is antialiased: its pixels are the colour laid over the ground at partial
//          coverage. LINE_ALPHA = 0.6 is the mean coverage of a 1.2 px line rasterised at device-pixel ratio 1-2 (measured
//          on the baseline screenshots: the Vedder's #B37B00 renders as #af750a at the core and ~#c4a060 at the fringe).
//          Both colours of a pair are laid over the SAME ground — two lines meet on the same land — and the worst ground wins.
//   lake   a lake's fill is the status colour at `opacity.lake` over the basemap's WATER (light 0.4, dark 0.85): the status
//          lake layer is drawn on top of the basemap's own water polygon, so its ground is water, never land. Checked
//          against the renders (colour-math.test.mjs): Kootenay Lake in today's light shot is #94aa86 = own #B37B00 at
//          0.4 over #82cdd9 exactly; over land the model would say #c39b7a.
//   card   status words / swatches on the card (`color.ui.card`) and as pill text on a 13.3% tint of themselves.
// Every pair is reported as the WORST of normal vision and protan / deutan / tritan under Viénot 1999 AND Machado 2009 —
// the worst-of-both-models rule tools/water-status-cvd.test.ts already holds the map to.
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

export const REPO = join(dirname(fileURLToPath(import.meta.url)), "../../../../..");
const cvd = await import(join(REPO, "app/tools/cvd.ts"));
export const { simulate, deltaE, contrast, parseHex } = cvd;

export const VISIONS = ["normal", "protan", "deutan", "tritan"];
export const MODELS = ["vienot", "machado"];

/** `fill` laid over `under` at `alpha`. */
export function over(fill, under, alpha) {
  const [f, u] = [parseHex(fill), parseHex(under)];
  return `#${f.map((c, i) => Math.round(c * alpha + u[i] * (1 - alpha)).toString(16).padStart(2, "0")).join("")}`;
}

/** ΔE2000 under one vision (worst of the two models for a deficiency). */
export function dEUnder(a, b, vision) {
  if (vision === "normal") return deltaE(a, b);
  return Math.min(...MODELS.map((m) => deltaE(simulate(a, vision, m), simulate(b, vision, m))));
}

/** The worst vision and its ΔE. */
export function worst(a, b) {
  let w = { dE: Infinity, under: "" };
  for (const v of VISIONS) {
    const d = dEUnder(a, b, v);
    if (d < w.dE) w = { dE: d, under: v };
  }
  return w;
}

/**
 * A candidate's per-theme overrides LAID ON THE BASELINE (the palette before T47, written out in candidates.json).
 * Part 2 applied D to the theme files, so a candidate that names only what it changes (A, B, C) must sit on the
 * baseline, not on whatever the theme files hold now, to measure and render as it did in part 1.
 */
export function layered(cands, id) {
  const base = cands.baseline?.themes ?? {};
  const own = cands[id]?.themes ?? {};
  return Object.fromEntries(["light", "dark", "cvd"].map((th) => [th, { ...(base[th] ?? {}), ...(own[th] ?? {}) }]));
}

/** A theme's tokens with `@ref` values resolved, and any overrides applied first. */
export function themeTokens(name, overrides = {}) {
  const raw = JSON.parse(readFileSync(join(REPO, `app/packages/map/style/themes/${name}.json`), "utf8")).values;
  const v = { ...raw, ...overrides };
  const r = (x, depth = 0) => (typeof x === "string" && x.startsWith("@") && depth < 8 ? r(v[x.slice(1)], depth + 1) : x);
  return Object.fromEntries(Object.keys(v).map((k) => [k, r(v[k])]));
}

/**
 * THE GROUNDS A LINE IS DRAWN ON, sampled from the baseline screenshots (candidates/baseline/*), not from the protomaps
 * flavour: the app's basemap is its own (look.ts), and the flavour's #e2dfda earth is not what a reader sees.
 */
export const GROUNDS = {
  // light: Vedder z11 PNG + JPEG quantised (10 colours): land, forest green, river, open meadow. A fifth sample,
  // "forest-dark" #94aa86, was Cultus Lake's own fill (today's own at 0.4 over water) and is gone: it disappears from
  // the A renders when own changes (22 302 -> 3 351 pixels in vedder-z11), so it was a status pixel, not a ground.
  light: { land: "#d9cecc", forest: "#b0caa8", river: "#82cdd9", meadow: "#e9e7e1" },
  // dark: Coquihalla + Kootenay dark shots: the land is a warm near-black, darker hollows, the basemap's river
  dark: { land: "#372e2f", hollow: "#221e1e", river: "#31353f" },
  // satellite: Kootenay + Coquihalla satellite shots (Esri World Imagery): dark conifer, olive, shadow, brown scrub, the
  // theme's own imagery swatch for water, and snow from the Kootenay ridge tops
  satellite: { conifer: "#28290d", olive: "#353b1a", shadow: "#1c1a0b", scrub: "#61512c", water: "#2A333B", snow: "#e6e8ea" },
};

/** The ground under a status LAKE: the basemap's water (cvd rides on the light basemap). */
export const LAKE_GROUND = { light: GROUNDS.light.river, dark: GROUNDS.dark.river };
LAKE_GROUND.cvd = LAKE_GROUND.light;

export const STATUSES = ["closed", "own", "base", "restricted", "open"];
/** The water statuses (WATER_STATUSES): drawn as stream lines and lake fills next to each other on the Map tab. */
export const MAP_STATUSES = ["closed", "own", "base"];
export const BOUNDARIES = ["region", "mu"];

/**
 * WHICH PAIRS A READER SEES SIDE BY SIDE ON THE MAP (and so which pairs are held to a floor).
 *   water        closed / own / base, as stream lines and lake fills.
 *   permit       restricted is the permit-area hatch, outline and label (layers.source.json, `color.status.restricted`)
 *                laid over whatever water runs through it: restricted against every water status.
 *   temperature  the gauge dots in temperature mode (runtime-style.ts tempExpression): cool = open, warm = restricted,
 *                critical = closed, side by side.
 *   boundary     a region or unit boundary crosses or runs beside every one of them: boundary against all five statuses.
 * Not adjacent: own / base against open (open is only ever a temperature dot, and that view draws no status water).
 * `cond` "lake": only water statuses are ever lake fills, and boundaries never are.
 */
export function isMapPair(a, b, cond) {
  const isB = (n) => BOUNDARIES.includes(n);
  if (isB(a) && isB(b)) return false;
  if (cond === "lake") return MAP_STATUSES.includes(a) && MAP_STATUSES.includes(b);
  if (isB(a) || isB(b)) return true;
  const has = (x, y) => (a === x && b === y) || (a === y && b === x);
  if (MAP_STATUSES.includes(a) && MAP_STATUSES.includes(b)) return true;
  if (MAP_STATUSES.some((s) => has("restricted", s))) return true;
  return has("open", "restricted") || has("open", "closed");
}

export function colours(t) {
  return {
    closed: t["color.status.closed"], own: t["color.status.own"], base: t["color.status.base"],
    restricted: t["color.status.restricted"], open: t["color.status.open"],
    region: t["color.region.edge"], mu: t["color.mu.edge"],
  };
}

/** The ground set for a theme (cvd rides on the light basemap, as the app draws it). */
export const groundsFor = (theme) => (theme === "dark" ? GROUNDS.dark : GROUNDS.light);

/** A pill: the colour as text on a 13.3% tint of itself over the card (tools/cvd.test.ts tintOf). */
export const tintOf = (c, card) => over(c, card, 0.133);

/**
 * Every measurement for one palette (all three themes).
 *   pairs[theme][cond][pair] = { [vision]: dE, worst: {dE, under}, ground }
 *   cond: pure | line | lake | satellite
 */
export const LINE_ALPHA = 0.6;
export function measure(themeOverrides) {
  const out = { pairs: {}, card: {}, ground: {} };
  for (const theme of ["light", "dark", "cvd"]) {
    const t = themeTokens(theme, themeOverrides[theme] ?? {});
    const c = colours(t);
    const names = [...STATUSES, ...BOUNDARIES];
    const pairs = [];
    for (let i = 0; i < names.length; i++)
      for (let j = i + 1; j < names.length; j++)
        if (!(BOUNDARIES.includes(names[i]) && BOUNDARIES.includes(names[j]))) pairs.push([names[i], names[j]]);
    pairs.push(["region", "mu"]);
    const conds = {
      pure: [["-", null, 1]],
      line: Object.entries(groundsFor(theme)).map(([g, hex]) => [g, hex, LINE_ALPHA]),
      lake: [["water", LAKE_GROUND[theme], Number(t["opacity.lake"])]],
      satellite: Object.entries(GROUNDS.satellite).map(([g, hex]) => [g, hex, LINE_ALPHA]),
    };
    out.pairs[theme] = {};
    for (const [cond, grounds] of Object.entries(conds)) {
      out.pairs[theme][cond] = {};
      for (const [a, b] of pairs) {
        // a boundary is a line at full opacity; it is never a lake fill
        const isB = (n) => BOUNDARIES.includes(n);
        if (cond === "lake" && (isB(a) || isB(b))) continue;
        const row = {};
        let w = { dE: Infinity, under: "", ground: "" };
        for (const v of VISIONS) {
          let m = Infinity, mg = "";
          for (const [g, hex, alpha] of grounds) {
            const ca = hex ? over(c[a], hex, isB(a) ? Math.max(alpha, 0.8) : alpha) : c[a];
            const cb = hex ? over(c[b], hex, isB(b) ? Math.max(alpha, 0.8) : alpha) : c[b];
            const d = dEUnder(ca, cb, v);
            if (d < m) { m = d; mg = g; }
          }
          row[v] = m;
          if (m < w.dE) w = { dE: m, under: v, ground: mg };
        }
        row.worst = w;
        out.pairs[theme][cond][`${a}/${b}`] = row;
      }
    }
    // card: each status as a swatch / word on the card, and as pill text on its own tint
    const card = t["color.ui.card"];
    out.card[theme] = Object.fromEntries(STATUSES.map((s) => [s, {
      hex: c[s], onCard: contrast(c[s], card), onTint: contrast(c[s], tintOf(c[s], card)),
    }]));
    // ground: each status / boundary against each ground it is drawn on (pure colour vs ground, worst vision)
    out.ground[theme] = {};
    for (const s of [...STATUSES, ...BOUNDARIES]) {
      let w = { dE: Infinity, under: "", ground: "" };
      for (const [g, hex] of Object.entries(groundsFor(theme))) {
        const x = worst(c[s], hex);
        if (x.dE < w.dE) w = { ...x, ground: g };
      }
      out.ground[theme][s] = w;
    }
    out[theme] = c;
  }
  return out;
}
