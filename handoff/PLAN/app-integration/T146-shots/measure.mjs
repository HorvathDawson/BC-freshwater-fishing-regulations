// T146: measure a no-access candidate in one theme, the way status-colours-distinct.test.ts does.
const W = new URL("../../../..", import.meta.url).pathname.replace(/\/$/, "");
const cm = await import(W + "/handoff/PLAN/app-data/status-colours/scripts/colour-math.mjs");
const cvd = await import(W + "/app/tools/cvd.ts");
const { over, worst, deltaE, contrast, themeTokens, GROUNDS } = cm;
const GROUND = { light: Object.values(GROUNDS.light), dark: Object.values(GROUNDS.dark), cvd: Object.values(GROUNDS.light) };
const SAT = Object.values(GROUNDS.satellite);
const LAKE = { light: GROUNDS.light.river, dark: GROUNDS.dark.river, cvd: GROUNDS.light.river };
const drawn = (a, b, gs, ca, cb) => Math.min(...gs.map((g) => worst(over(a, g, ca), over(b, g, cb)).dE));
const dh = (a, b) => { const d = Math.abs(a - b) % 360; return d > 180 ? 360 - d : d; };
const STAT = ["closed", "own", "base", "restricted", "open"];
export function terms(th, x, t = themeTokens(th)) {
  const c = (s) => t[`color.status.${s}`];
  const la = Number(t["opacity.lake"]);
  const r = {};
  for (const s of ["closed", "own", "base"]) {
    r[`${s}.tok`] = worst(x, c(s)).dE;
    r[`${s}.line`] = drawn(x, c(s), GROUND[th], 0.6, 0.6);
    r[`${s}.sat`] = drawn(x, c(s), SAT, 0.6, 0.6);
    r[`${s}.lake`] = drawn(x, c(s), [LAKE[th]], la, la);
  }
  r["restr.tok"] = worst(x, c("restricted")).dE;
  r["restr.line"] = drawn(x, c("restricted"), GROUND[th], 0.6, 0.6);
  r["restr.sat"] = drawn(x, c("restricted"), SAT, 0.6, 0.6);
  const e = t["color.region.edge"];
  r["bnd.tok"] = worst(e, x).dE;
  r["bnd.line"] = drawn(e, x, GROUND[th], 0.8, 0.6);
  r["meaning"] = Math.min(...STAT.map((s) => deltaE(x, c(s))), deltaE(x, t["color.temp.warm"]));
  r["ink.n"] = deltaE(x, t["color.ui.ink"]);
  r["ink.w"] = worst(x, t["color.ui.ink"]).dE;
  r["card"] = contrast(x, t["color.ui.card"]);
  const h = (z) => cvd.lch(z).h;
  r["hue.base"] = dh(h(x), h(c("base")));
  r["hueMin"] = Math.min(...STAT.map((s) => dh(h(x), h(c(s)))), dh(h(x), h(t["color.temp.warm"])));
  r["normal.base"] = deltaE(x, c("base"));
  return r;
}
export const fmt = (r) => Object.entries(r).map(([k, v]) => `${k}=${v.toFixed(1)}`).join(" ");
if (process.argv[1]?.endsWith("measure.mjs") && process.argv[2]) {
  const [th, x] = process.argv.slice(2);
  const t = themeTokens(th);
  const col = x ?? t["color.status.no_access"];
  console.log(th, col, "L/C/h", JSON.stringify(cvd.lch(col), (k, v) => typeof v === "number" ? Math.round(v) : v), fmt(terms(th, col)));
}
