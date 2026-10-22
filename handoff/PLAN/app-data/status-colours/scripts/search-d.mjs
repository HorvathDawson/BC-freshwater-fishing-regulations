// T47 part 2: search for palette D — all FIVE status colours at once, per theme, on top of candidate B's boundaries.
//
//   node handoff/PLAN/app-data/status-colours/scripts/search-d.mjs <light|dark|cvd> [fixed JSON] [samples] [seed]
//
// <fixed JSON> pins a status to a hex ('{"closed":"#940018"}') or to a hue family ('{"open":"coolblue"}', FAMILIES below).
// Pin all five to score one palette as it is (prints the rules it breaks and its weakest terms).
//
// WHAT IS MAXIMISED: the minimum ΔE2000 over every pair of statuses a reader sees side by side on the map
// (`isMapPair`: closed / own / base pairwise; restricted against each of them; open against restricted and closed) —
// as tokens, as thin lines over every ground of the theme, as lake fills over the basemap's water (water statuses
// only), and as thin lines over satellite imagery — the WORST of normal vision and protan / deutan / tritan under
// Viénot 1999 AND Machado 2009 (colour-math.mjs, the app's own cvd.ts). Each boundary (B's neutral grey) against each
// status, as a token and as a line, joins the minimum with an allowance (BOUNDARY_SLACK, BOUNDARY_LINE_SLACK): the
// boundaries are B's and fixed, so they must not drag the statuses together, but a status may not drift onto them.
//
// HARD CONSTRAINTS (a candidate failing one is discarded) — every rule the app's gates already hold:
//   card       every status >= 3:1 on color.ui.card (build-style.mjs); closed / restricted / open >= 4.5:1 as pill text
//              on a 13.3 % tint of themselves (tools/cvd.test.ts)
//   meaning    closed is red, base is blue (water-status-cvd.test.ts); every two statuses >= 20 apart in NORMAL vision
//              (one meaning per colour, pairs that are never adjacent included), >= CIE76 12 from each other and from
//              every map token some view draws with them (build-style.mjs's house floor, its "together" relation copied),
//              >= 15 (normal) from body ink and the accent (T47_CHROME_MIN); closed / own / base >= 20 as tokens
//              (water-status-cvd.test.ts); each status within its hue family; each status at least T47_BND_MIN (15)
//              from the boundary grey as a token and T47_BNDL_MIN (8; D used 5.5, B's own worst) as a line
//   neighbours light/dark: onAccent on closed >= 4.5:1; closed vs highlight and vs place >= 10; place vs restricted /
//              closed / own / base >= 15; closed / own / base vs the flavour's land, forest, water and the lake /
//              wetland / park fills >= 15 (water-status-cvd.test.ts)
//   cvd        closed / restricted / open: red-green >= 20 and tritan >= 18 under Viénot (tools/cvd.test.ts)
// Random sampling from per-colour pools (inside each family, in LCh), then hill climbing on a smooth minimum from the best
// few; deterministic (seeded). Environment knobs: T47_ITER / T47_STARTS (effort), T47_SAT_SLACK (3), T47_BND_SLACK (2),
// T47_BNDL_SLACK (6), T47_GROUND_SLACK (5), T47_SATG_SLACK (100 = satellite grounds reported, not searched),
// T47_CHROME_MIN (15), T47_BND_MIN / T47_BNDL_MIN, T47_TOKEN_MIN (every adjacent pair as tokens), T47_BND_TOKEN_CAP,
// T47_CO_LINE_MIN (closed/own as a thin line), T47_START (a palette JSON to climb from), T47_WHY=<status>:<hex> (why a
// colour is refused), and (after the review of the first D) T47_CO_BONUS (the owner's closed / own pair counts this much
// less in the minimum, so it is pushed hardest), T47_TERM_MIN (hard floors on named terms, '{"closed/own line":21}'),
// T47_INK_NORMAL / T47_INK_WORST (every status this far from body ink, normal vision 20 / every vision 15 by default).
// The temperature pairs (open vs restricted / closed) are gauge DOTS and are scored as tokens only; open, never a line,
// is held against the boundary as a token only.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { createRequire } from "node:module";
import { contrast, deltaE, GROUNDS, LAKE_GROUND, layered, LINE_ALPHA, MODELS, over, parseHex, REPO, simulate, themeTokens } from "./colour-math.mjs";

const require = createRequire(join(REPO, "app/package.json"));
const { namedFlavor } = await import(require.resolve("@protomaps/basemaps"));

const [theme = "light", fixedArg = "{}", nArg = "30000", seedArg = "11"] = process.argv.slice(2);
const fixed = JSON.parse(fixedArg);
const cands = JSON.parse(readFileSync(join(REPO, "handoff/PLAN/app-data/status-colours/candidates.json"), "utf8"));
const B = layered(cands, "B")[theme];   // B on the baseline (the theme files hold D now)
const t = themeTokens(theme, B);
const card = t["color.ui.card"];
const lakeA = Number(t["opacity.lake"]);
const lakeG = LAKE_GROUND[theme];
const lineG = Object.values(theme === "dark" ? GROUNDS.dark : GROUNDS.light);
const satG = Object.values(GROUNDS.satellite);
const region = t["color.region.edge"];

export const STATUS = ["closed", "own", "base", "restricted", "open"];
const WATER = ["closed", "own", "base"];
const TEMPERATURE = new Set(["open/restricted", "open/closed"]);
/** The statuses the map draws as lines (water, and the permit hatch). Open is never a line: a dot and a card word. */
const LINES = ["closed", "own", "base", "restricted"];
/** The pairs a reader sees side by side (colour-math.mjs isMapPair, statuses only). */
export const ADJ = [["closed", "own"], ["closed", "base"], ["own", "base"], ["restricted", "closed"], ["restricted", "own"],
  ["restricted", "base"], ["open", "restricted"], ["open", "closed"]];
const SAT_SLACK = Number(process.env.T47_SAT_SLACK ?? 3);             // satellite imagery joins the minimum 3 below the basemap grounds: held, but a photo's
                                 // near-black conifer would otherwise drive every status to an extreme
const GROUND_SLACK = Number(process.env.T47_GROUND_SLACK ?? 5);      // a status against the ground under it, 5 below
const SAT_GROUND_SLACK = Number(process.env.T47_SATG_SLACK ?? 100);  // satellite imagery as a ground: reported by measure.mjs, not searched (dark red on olive conifer is ~6 under deutan for EVERY red that is still a red)
const BOUNDARY_SLACK = Number(process.env.T47_BND_SLACK ?? 2);        // boundary vs status token joins the minimum 2 below it
const BOUNDARY_LINE_SLACK = Number(process.env.T47_BNDL_SLACK ?? 6);   // boundary vs status line, 6 below

// ---- cached maths
const simC = new Map();
const sim = (h, v, m) => { const k = `${h}${v}${m}`; let r = simC.get(k); if (!r) { r = simulate(h, v, m); simC.set(k, r); } return r; };
const DEF = ["protan", "deutan", "tritan"];
const worst = (a, b) => {
  let w = deltaE(a, b);
  for (const v of DEF) for (const m of MODELS) w = Math.min(w, deltaE(sim(a, v, m), sim(b, v, m)));
  return w;
};
const drawn = (a, b, gs, ca, cb) => Math.min(...gs.map((g) => worst(over(a, g, ca), over(b, g, cb))));
const sep = (a, b, model) => ({
  rg: Math.min(deltaE(a, b), ...["protan", "deutan"].map((v) => deltaE(sim(a, v, model), sim(b, v, model)))),
  tritan: deltaE(sim(a, "tritan", model), sim(b, "tritan", model)),
});
const lab = (hex) => {
  const [r, g, b] = parseHex(hex).map((v) => v / 255).map((c) => (c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4));
  const f = (x) => (x > 0.008856 ? Math.cbrt(x) : 7.787 * x + 16 / 116);
  const X = f((r * 0.4124 + g * 0.3576 + b * 0.1805) / 0.95047), Y = f(r * 0.2126 + g * 0.7152 + b * 0.0722);
  const Z = f((r * 0.0193 + g * 0.1192 + b * 0.9505) / 1.08883);
  return [116 * Y - 16, 500 * (X - Y), 200 * (Y - Z)];
};
const d76 = (a, b) => Math.hypot(...lab(a).map((v, i) => v - lab(b)[i]));
/** CIE LCh (D65) -> sRGB hex, or null when out of gamut. */
const fromLch = (L, C, H) => {
  const a = C * Math.cos(H * Math.PI / 180), b = C * Math.sin(H * Math.PI / 180);
  const fy = (L + 16) / 116, fx = fy + a / 500, fz = fy - b / 200;
  const inv = (f) => (f ** 3 > 0.008856 ? f ** 3 : (f - 16 / 116) / 7.787);
  const [X, Y, Z] = [inv(fx) * 0.95047, inv(fy), inv(fz) * 1.08883];
  const lin = [3.2406 * X - 1.5372 * Y - 0.4986 * Z, -0.9689 * X + 1.8758 * Y + 0.0415 * Z, 0.0557 * X - 0.204 * Y + 1.057 * Z];
  if (lin.some((v) => v < -0.001 || v > 1.001)) return null;
  const s = lin.map((v) => Math.max(0, Math.min(1, v))).map((v) => (v <= 0.0031308 ? 12.92 * v : 1.055 * v ** (1 / 2.4) - 0.055) * 255);
  return hex(...s);
};
const hex = (r, g, b) => `#${[r, g, b].map((v) => Math.max(0, Math.min(255, Math.round(v))).toString(16).padStart(2, "0")).join("")}`;
const tint = (c) => over(c, card, 0.133);

// ---- the other map tokens (house floor: build-style.mjs's own "some view paints both" relation, copied) and the grounds
// water-status-cvd.test.ts stands a status off
const src = JSON.parse(readFileSync(join(REPO, "app/packages/map/style/layers.source.json"), "utf8"));
const modeTokens = (m) => [m?.color, m?.missing, ...Object.values(m?.categories ?? {}), ...(m?.stops ?? []).map(([, r]) => r)]
  .filter((r) => r && typeof r === "object" && r.token).map((r) => r.token);
const srcById = new Map((src.layers ?? []).map((l) => [l.id, l]));
const inSomeMode = new Set((src.layers ?? []).flatMap((l) => Object.values(l.colorModes ?? {}).flatMap(modeTokens)));
const companion = (src.layers ?? []).flatMap((l) => [...(l.outline ? [l.outline.color?.token] : []),
  ...(l.label ? [l.label.color?.token, l.label.halo?.token] : [])]).filter(Boolean);
const viewSets = (src.views ?? []).map((v) => {
  const seen = new Set(companion);
  for (const [id, mode] of Object.entries(v.modes ?? {})) for (const x of modeTokens(srcById.get(id)?.colorModes?.[mode])) seen.add(x);
  return seen;
});
const together = (a, b) => !inSomeMode.has(a) || !inSomeMode.has(b) || viewSets.some((s) => s.has(a) && s.has(b));
const rawTheme = JSON.parse(readFileSync(join(REPO, `app/packages/map/style/themes/${theme}.json`), "utf8")).values;
const isAlias = (n) => typeof ({ ...rawTheme, ...B })[n] === "string" && ({ ...rawTheme, ...B })[n].startsWith("@");
/** status key -> the other map tokens it must stay CIE76 12 from (as build-style.mjs would compare them). */
const othersOf = Object.fromEntries(STATUS_KEYS().map((k) => [k, Object.entries(t)
  .filter(([n, v]) => typeof v === "string" && /^#[0-9a-f]{6}$/i.test(v) && !n.startsWith("color.ui.") && !n.startsWith("color.status.")
    && !isAlias(n) && together(`color.status.${k}`, n)).map(([, v]) => v)]));
function STATUS_KEYS() { return ["closed", "own", "base", "restricted", "open"]; }
const flavor = theme === "dark" ? namedFlavor("black") : namedFlavor("light");
const land = flavor.earth;
const fill = (c, o) => over(t[c], land, Number(t[o]));
const statusGround = [land, flavor.wood_b, flavor.water, fill("color.lake.fill", "opacity.lake"),
  fill("color.wetland.fill", "opacity.wetland"), fill("color.park.fill", "opacity.park")];
const place = t["color.label.place"], highlight = t["color.highlight"];
const onAccent = t["color.ui.on-accent"];
const strict = theme !== "cvd";   // water-status-cvd.test.ts holds light and dark

const FAMILY = {
  red: (h) => { const [r, g, b] = parseHex(h); return r > g * 1.8 && r > b * 1.8; },
  blue: (h) => { const [r, g, b] = parseHex(h); return b > r && b > g * 0.9; },
};

/**
 * HUE FAMILIES (CIE LCh hue in degrees, minimum chroma): a status keeps a colour a reader can NAME, so the five do not
 * become five arbitrary hexes that happen to measure apart. Defaults: closed red, own orange/amber, base blue,
 * restricted purple/magenta (a permit area: the map convention, and the one hue family the four others leave free), open
 * green-to-cyan (a "cool" temperature dot; off the mid green). Override per status in <fixed JSON> with a family name.
 */
const FAMILIES = {
  red: [10, 45, 50], orange: [45, 90, 40], blue: [235, 290, 40], purple: [315, 360, 30], cool: [130, 215, 25],
  coolblue: [180, 300, 20], cerulean: [180, 255, 25], deeppurple: [290, 345, 45, 18], aubergine: [290, 345, 35, 12], amber: [55, 90, 40], green: [125, 165, 25], teal: [165, 215, 25], any: [0, 360, 0],
};
const DEFAULT_FAMILY = { closed: "red", own: "orange", base: "blue", restricted: "purple", open: "cerulean" };
const famOf = (k) => FAMILIES[(typeof fixed[k] === "string" && FAMILIES[fixed[k]] ? fixed[k] : DEFAULT_FAMILY[k])];
const lch = (h) => { const [L, a, b] = lab(h); return [L, Math.hypot(a, b), (Math.atan2(b, a) * 180 / Math.PI + 360) % 360]; };
function inFamily(k, c) {
  const [lo, hi, cmin, lmin = 0] = famOf(k), [L, C, H] = lch(c);
  return C >= cmin && H >= lo && H <= hi && L >= lmin;
}

/**
 * THE BOUNDARY FLOOR: B's neutral grey is fixed, so the search may not trade a status into it. Every status stays at
 * least BND_MIN from the region grey as tokens and BNDL_MIN as lines (worst vision, worst ground): B's own worst was
 * 14.9 / 5.5 (light, against today's open green), so the defaults forbid any regression from B.
 */
const BND_MIN = Number(process.env.T47_BND_MIN ?? 15), BNDL_MIN = Number(process.env.T47_BNDL_MIN ?? 8);

const CHROME_MIN = Number(process.env.T47_CHROME_MIN ?? 15);
/**
 * A STATUS WORD IS NOT BODY TEXT, in any vision: body ink is a sixth meaning on the card, so every status stands
 * INK_NORMAL (20, the one-meaning floor) from it in normal vision and INK_WORST (15, the chrome rule) as the WORST of
 * normal and protan / deutan / tritan (review of the first D: a near-black restricted was 18.3 from ink in
 * normal vision and 10.9 under deutan, 1.08:1 — the release words, the zero-limit line and the restricted pill read as
 * plain text).
 */
const INK_NORMAL = Number(process.env.T47_INK_NORMAL ?? 20), INK_WORST = Number(process.env.T47_INK_WORST ?? 15);
/** Hard floors on named terms (as terms() names them), e.g. '{"closed/own line":21}'. */
const TERM_MIN = JSON.parse(process.env.T47_TERM_MIN ?? "{}");
/** The owner's pair first: closed/own terms count this much LESS in the minimum, so the search pushes them hardest. */
const CO_BONUS = Number(process.env.T47_CO_BONUS ?? 0);

/** Single-colour rules. */
function okOne(k, c) {
  if (!inFamily(k, c)) return false;
  if (worst(region, c) < BND_MIN || (LINES.includes(k) && drawn(region, c, lineG, 0.8, LINE_ALPHA) < BNDL_MIN)) return false;
  if (contrast(c, card) < 3) return false;
  // one meaning per colour against the app's own chrome too: a status word must not read as body ink or as the
  // "you chose this" accent (normal vision; the chrome is on every screen)
  if (deltaE(c, t["color.ui.ink"]) < CHROME_MIN || deltaE(c, t["color.ui.accent"]) < CHROME_MIN) return false;
  if (deltaE(c, t["color.ui.ink"]) < INK_NORMAL || worst(c, t["color.ui.ink"]) < INK_WORST) return false;
  if (["closed", "restricted", "open"].includes(k) && contrast(c, tint(c)) < 4.5) return false;
  if (k === "closed" && !FAMILY.red(c)) return false;
  if (k === "base" && !FAMILY.blue(c)) return false;
  for (const o of othersOf[k]) if (d76(c, o) < 12) return false;
  if (strict) {
    if (k === "closed" && (contrast(onAccent, c) < 4.5 || worst(c, highlight) < 10 || worst(c, place) < 10)) return false;
    if (["restricted", "closed", "own", "base"].includes(k) && worst(place, c) < 15) return false;
    if (WATER.includes(k)) for (const g of statusGround) if (worst(c, g) < 15) return false;
  }
  return true;
}

/** Pair rules. */
const BND_MIN_TOKEN_CAP = Number(process.env.T47_BND_TOKEN_CAP ?? 0);   // with TOKEN_MIN: the boundary token floor too
const CO_LINE_MIN = Number(process.env.T47_CO_LINE_MIN ?? 0);
const TOKEN_MIN = Number(process.env.T47_TOKEN_MIN ?? 0);   // optional: every adjacent pair at least this far apart as TOKENS
function okAll(c) {
  if (Object.keys(TERM_MIN).length) {
    const tv = Object.fromEntries(terms(c, true));
    for (const [n, m] of Object.entries(TERM_MIN)) if (!(n in tv) || tv[n] < m) return false;
  }
  if (TOKEN_MIN) for (const [a, b] of ADJ) if (worst(c[a], c[b]) < TOKEN_MIN) return false;
  // the owner's pair may not lose ground to B as a thin line (optional): T47_CO_LINE_MIN=20.3
  if (CO_LINE_MIN && drawn(c.closed, c.own, lineG, LINE_ALPHA, LINE_ALPHA) < CO_LINE_MIN) return false;
  // water-status-cvd.test.ts: closed / own / base >= 20 apart as tokens in light and dark — always hard
  for (const [a, b] of [["closed", "own"], ["closed", "base"], ["own", "base"]]) if (worst(c[a], c[b]) < 20) return false;
  if (BND_MIN_TOKEN_CAP) for (const s of STATUS) if (worst(region, c[s]) < BND_MIN_TOKEN_CAP) return false;
  for (let i = 0; i < STATUS.length; i++)
    for (let j = i + 1; j < STATUS.length; j++) {
      const [a, b] = [c[STATUS[i]], c[STATUS[j]]];
      if (deltaE(a, b) < 20 || d76(a, b) < 12) return false;
    }
  if (theme === "cvd")
    for (const [a, b] of [["closed", "restricted"], ["closed", "open"], ["restricted", "open"]]) {
      const s = sep(c[a], c[b], "vienot");
      if (s.rg < 20 || s.tritan < 18) return false;
    }
  return true;
}

export function terms(c, raw = false) {
  const out = [];
  for (const [a, b] of ADJ) {
    const off = !raw && a === "closed" && b === "own" ? CO_BONUS : 0;
    out.push([`${a}/${b} token`, worst(c[a], c[b]) - off]);
    // THE TEMPERATURE PAIRS ARE DOTS, NOT LINES: open sits beside restricted and closed only as gauge dots, 5 px filled
    // circles in a 2 px paper stroke (runtime-style.ts gauge-dot), so the dot's pixels are the token itself, never a line
    // antialiased into the ground or laid over satellite. Measuring them as thin lines (the first D) bound the whole
    // search on a drawing that does not exist and pushed restricted to near-black.
    if (TEMPERATURE.has(`${a}/${b}`)) continue;
    out.push([`${a}/${b} line`, drawn(c[a], c[b], lineG, LINE_ALPHA, LINE_ALPHA) - off]);
    if (WATER.includes(a) && WATER.includes(b)) out.push([`${a}/${b} lake`, drawn(c[a], c[b], [lakeG], lakeA, lakeA) - off]);
    out.push([`${a}/${b} satellite`, drawn(c[a], c[b], satG, LINE_ALPHA, LINE_ALPHA) + (raw ? 0 : SAT_SLACK) - off]);
  }
  // a status must stand off what it is drawn on, or it is no status: each pure colour against each ground
  for (const s of LINES) {
    for (const g of lineG) out.push([`${s} on ${g} (+${GROUND_SLACK})`, worst(c[s], g) + GROUND_SLACK]);
    for (const g of satG) out.push([`${s} on satellite ${g} (+${SAT_GROUND_SLACK})`, worst(c[s], g) + SAT_GROUND_SLACK]);
  }
  for (const s of STATUS) {
    out.push([`region/${s} token (+${BOUNDARY_SLACK})`, worst(region, c[s]) + BOUNDARY_SLACK]);
    if (LINES.includes(s)) out.push([`region/${s} line (+${BOUNDARY_LINE_SLACK})`, drawn(region, c[s], lineG, 0.8, LINE_ALPHA) + BOUNDARY_LINE_SLACK]);
  }
  return out;
}
const score = (c) => Math.min(...terms(c).map(([, v]) => v));
/** A smooth minimum (log-sum-exp, T = 0.5): climbs off the plateaus where only the single weakest pair counts. */
const soft = (c) => { const v = terms(c).map(([, x]) => x); const m = Math.min(...v); return m - 0.5 * Math.log(v.reduce((s, x) => s + Math.exp(-(x - m) / 0.5), 0)); };

// ---- search
let seed = Number(seedArg);
const rnd = () => {   // mulberry32
  seed = (seed + 0x6d2b79f5) | 0;
  let x = Math.imul(seed ^ (seed >>> 15), 1 | seed);
  x = (x + Math.imul(x ^ (x >>> 7), 61 | x)) ^ x;
  return ((x ^ (x >>> 14)) >>> 0) / 4294967296;
};
const gauss = () => Math.sqrt(-2 * Math.log(1 - rnd())) * Math.cos(2 * Math.PI * rnd());
const free = STATUS.filter((k) => !(typeof fixed[k] === "string" && fixed[k].startsWith("#")));
if (process.env.T47_WHY) {   // debug: why is one colour refused?  T47_WHY=open:#00796B
  const [k, c] = process.env.T47_WHY.split(":");
  const r = [];
  if (!inFamily(k, c)) r.push(`family ${lch(c).map((v) => v.toFixed(0))}`);
  if (contrast(c, card) < 3) r.push("card");
  const ink = t["color.ui.ink"], acc = t["color.ui.accent"];
  if (deltaE(c, ink) < INK_NORMAL || worst(c, ink) < INK_WORST) r.push(`ink ${deltaE(c, ink).toFixed(1)} / worst ${worst(c, ink).toFixed(1)}`);
  if (deltaE(c, ink) < CHROME_MIN || deltaE(c, acc) < CHROME_MIN) r.push(`chrome (accent ${deltaE(c, acc).toFixed(1)})`);
  if (worst(region, c) < BND_MIN || (LINES.includes(k) && drawn(region, c, lineG, 0.8, LINE_ALPHA) < BNDL_MIN)) r.push(`boundary ${worst(region, c).toFixed(1)}`);
  if (BND_MIN_TOKEN_CAP && worst(region, c) < BND_MIN_TOKEN_CAP) r.push(`boundary cap ${worst(region, c).toFixed(1)}`);
  if (["closed", "restricted", "open"].includes(k) && contrast(c, tint(c)) < 4.5) r.push(`pill ${contrast(c, tint(c)).toFixed(2)}`);
  for (const [n, v] of Object.entries(t)) if (othersOf[k].includes(v) && d76(c, v) < 12) r.push(`house ${n} ${v} ${d76(c, v).toFixed(1)}`);
  if (strict && ["restricted", "closed", "own", "base"].includes(k) && worst(place, c) < 15) r.push(`place ${worst(place, c).toFixed(1)}`);
  if (strict && WATER.includes(k)) for (const g of statusGround) if (worst(c, g) < 15) r.push(`ground ${g} ${worst(c, g).toFixed(1)}`);
  console.log(k, c, r.length ? r : "ok");
  process.exit(0);
}
if (!free.length) {   // every status pinned: score that palette as it is, and say which hard rule it breaks
  const broken = [...STATUS.filter((k) => !okOne(k, fixed[k])).map((k) => `${k} (single-colour rule)`), ...(okAll(fixed) ? [] : ["a pair rule"])];
  const weakest = terms(fixed).sort((a, b) => a[1] - b[1]).slice(0, 8).map(([n, v]) => `${n} ${v.toFixed(1)}`);
  const co = terms(fixed, true).filter(([n]) => n.startsWith("closed/own")).map(([n, v]) => `${n} ${v.toFixed(1)}`);
  console.log(JSON.stringify({ theme, score: +score(fixed).toFixed(2), ...fixed, broken, closedOwn: co, weakest }));
  process.exit(0);
}
const pool = {};
for (const k of STATUS) {
  if (fixed[k] && fixed[k].startsWith("#")) { pool[k] = [fixed[k]]; continue; }
  pool[k] = [];
  for (let n = 0; n < 2000000 && pool[k].length < 1500; n++) {
    const [lo, hi, cmin] = famOf(k);   // sample inside the family (LCh), so a narrow family still fills its pool
    const h = fromLch(5 + rnd() * 90, cmin + rnd() * (130 - cmin), lo + rnd() * (hi - lo));
    if (h && okOne(k, h)) pool[k].push(h);
  }
  if (!pool[k].length) { console.error(`no ${k} colour satisfies the single-colour rules in ${theme}`); process.exit(2); }
}
const N = Number(nArg);
console.error(`pools: ${STATUS.map((k) => `${k} ${pool[k].length}`).join(", ")}`);
const top = [];
for (let n = 0; n < N; n++) {
  const c = Object.fromEntries(STATUS.map((k) => [k, pool[k][Math.floor(rnd() * pool[k].length)]]));
  if (!okAll(c)) continue;
  const s = score(c);
  top.push({ s, c });
  if (top.length > 60) { top.sort((x, y) => y.s - x.s); top.length = 30; }
}
top.sort((x, y) => y.s - x.s);
if (process.env.T47_START) {   // refine a known palette: descend from it first (T47_START='{"closed":"#..",...}')
  const c = { ...JSON.parse(process.env.T47_START) };
  if (okAll(c) && STATUS.every((k) => okOne(k, c[k]))) top.unshift({ s: score(c), c });
  else console.error("T47_START breaks a hard rule; ignored");
}
/** Hill climbing from `start`: perturb one free colour in LCh (sigma shrinking), keep any move that raises the minimum. */
const descend = (start) => {
  let best = { ...start, f: soft(start.c) };
  const ITER = Number(process.env.T47_ITER ?? 2500);
  for (let n = 0; n < ITER; n++) {
    const k = free[Math.floor(rnd() * free.length)];
    const sig = 12 * (1 - n / ITER) + 1;
    const [L, C, H] = lch(best.c[k]);
    const h = fromLch(L + gauss() * sig, Math.max(0, C + gauss() * sig), (H + gauss() * sig + 360) % 360);
    if (!h || h === best.c[k]) continue;
    const c = { ...best.c, [k]: h };
    if (!okOne(k, h) || !okAll(c)) continue;
    const s = soft(c);
    if (s > best.f + 0.0005) best = { s: score(c), f: s, c };
  }
  return best;
};
let best = null;
for (const st of top.slice(0, Number(process.env.T47_STARTS ?? 20))) { const r = descend(st); if (!best || r.s > best.s) best = r; }
const weakest = terms(best.c).sort((a, b) => a[1] - b[1]).slice(0, 8).map(([n, v]) => `${n} ${v.toFixed(1)}`);
const co = terms(best.c, true).filter(([n]) => n.startsWith("closed/own")).map(([n, v]) => `${n} ${v.toFixed(1)}`);
console.log(JSON.stringify({ theme, fixed, score: +best.s.toFixed(2), ...best.c, closedOwn: co, weakest }));
