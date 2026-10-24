// T165 (owner decision): no public access on the map is a DOTTED GREY line. Search the grey per theme.
//   node grey.mjs <theme> [hex...]   — with hexes: measure them; without: every neutral (RGB spread <= 12), best first.
// A grey shares no hue with anything, so it is held by LIGHTNESS against what it is drawn beside, as the repo's gates
// measure (app/tools/cvd.ts, worst of normal / protan / deutan / tritan, Viénot + Machado): base, closed and own (tokens,
// thin lines, lake fills), the status mode's missing grey `water.unmapped` and the Conditions `water.ungauged` (one
// meaning per colour, normal vision), the under-lake route as drawn (`water.route` at `opacity.route`), the region / unit
// boundary (dashed too), body ink, and the flavour grounds (worst reader >= 15, tools/water-status-cvd.test.ts); 3:1 on
// the card (the legend swatch is a non-text mark).
const W = new URL("../../../..", import.meta.url).pathname.replace(/\/$/, "");
const cm = await import(W + "/handoff/PLAN/app-data/status-colours/scripts/colour-math.mjs");
const { over, worst, deltaE, contrast, themeTokens, GROUNDS } = cm;
const [th, ...hexes] = process.argv.slice(2);
const t = themeTokens(th);
const G = th === "dark" ? Object.values(GROUNDS.dark) : Object.values(GROUNDS.light);
const LAKE = th === "dark" ? GROUNDS.dark.river : GROUNDS.light.river;
const la = Number(t["opacity.lake"]);
const { createRequire } = await import("node:module");
const { namedFlavor } = await import(createRequire(W + "/app/package.json").resolve("@protomaps/basemaps"));
const fl = namedFlavor(th === "dark" ? "black" : "light");
const fo = (c, o) => over(t[c], fl.earth, Number(t[o]));
const FLAVOUR = [fl.earth, fl.wood_b, fl.water, fo("color.lake.fill", "opacity.lake"), fo("color.wetland.fill", "opacity.wetland"), fo("color.park.fill", "opacity.park")];
const drawn = (a, b, gs, ca, cb) => Math.min(...gs.map((g) => worst(over(a, g, ca), over(b, g, cb)).dE));
const route = t["color.water.route"], ro = Number(t["opacity.route"]);
// the style build's own one-meaning rule (tools/build-style.mjs): ΔE76 >= 12 from every other map colour token
const lab76 = (hex) => {
  const [r, g, b] = [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16) / 255)
    .map((c) => (c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4));
  const f = (v) => (v > 0.008856 ? Math.cbrt(v) : 7.787 * v + 16 / 116);
  const X = f((r * 0.4124 + g * 0.3576 + b * 0.1805) / 0.95047), Y = f(r * 0.2126 + g * 0.7152 + b * 0.0722);
  const Z = f((r * 0.0193 + g * 0.1192 + b * 0.9505) / 1.08883);
  return [116 * Y - 16, 500 * (X - Y), 200 * (Y - Z)];
};
const MAPCOLS = Object.entries(t).filter(([k, v]) => k.startsWith("color.") && !k.startsWith("color.ui.") &&
  k !== "color.status.no_access" && typeof v === "string" && v.startsWith("#"));
const style76 = (x) => Math.min(...MAPCOLS.map(([, v]) => Math.hypot(...lab76(x).map((a, i) => a - lab76(v)[i]))));
function terms(x) {
  const r = {};
  for (const s of ["closed", "own", "base"]) {
    const c = t[`color.status.${s}`];
    r[s] = Math.min(worst(x, c).dE, drawn(x, c, G, 0.6, 0.6), drawn(x, c, [LAKE], la, la));
  }
  r.unmapped = deltaE(x, t["color.water.unmapped"]);
  r.ungauged = deltaE(x, t["color.water.ungauged"]);
  r.route = Math.min(...G.map((g) => deltaE(over(x, g, 0.8), over(route, g, ro))));
  r.boundary = deltaE(x, t["color.region.edge"]);
  r.ink = deltaE(x, t["color.ui.ink"]);
  r.inkW = worst(x, t["color.ui.ink"]).dE;
  r.open = deltaE(x, t["color.status.open"]);
  r.restricted = deltaE(x, t["color.status.restricted"]);
  r.ground = Math.min(...FLAVOUR.map((g) => worst(x, g).dE));
  r.card = contrast(x, t["color.ui.card"]);
  r.style76 = style76(x);
  r.min = Math.min(r.closed, r.own, r.base, r.unmapped, r.ungauged, r.route, r.boundary, r.ink);
  return r;
}
const show = (x, r) => `${x} min ${r.min.toFixed(1)} | ` +
  Object.entries(r).filter(([k]) => k !== "min").map(([k, v]) => `${k} ${v.toFixed(1)}`).join(" ");
if (hexes.length) for (const x of hexes) console.log(th, show(x, terms(x)));
else {
  const all = [];
  const hex = (v) => Math.max(0, Math.min(255, v)).toString(16).padStart(2, "0");
  const TINTS = []; for (let a = -8; a <= 8; a += 4) for (let b = -8; b <= 8; b += 4) TINTS.push([a, 0, b]);
  for (let v = 0; v <= 255; v += 2) for (const [dr, dg, db] of TINTS) {
    const x = ("#" + hex(v + dr) + hex(v + dg) + hex(v + db)).toUpperCase();
    const r = terms(x);
    if (r.card < 3 || r.ground < 15 || r.open < 20 || r.restricted < 20 || r.ink < 20 || r.inkW < 15 || r.style76 < Number(process.env.S76 ?? 12)) continue;
    all.push({ x, r });
  }
  all.sort((a, b) => b.r.min - a.r.min);
  for (const { x, r } of all.slice(0, 8)) console.log(th, show(x, r));
}
