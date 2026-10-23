// T146 adversarial review: the built magenta sat 10.4 (normal vision) from `color.highlight`, the selection outline, the
// same hue family (h 359 vs 351). This re-runs pick.mjs's search with the selection held as base is:
// LCh hue >= HUE off base AND off the highlight, normal ΔE >= 20 (one meaning) from the highlight, plus pick.mjs's rules,
// the T146 gate's normal-vision base floors as drawn (30) and the boundary line floor (BND, default 3.5).
//   node selection.mjs <theme> [step] [hue]
import { terms } from "./measure.mjs";
const W = new URL("../../../..", import.meta.url).pathname.replace(/\/$/, "");
const cm = await import(W + "/handoff/PLAN/app-data/status-colours/scripts/colour-math.mjs");
const cvd = await import(W + "/app/tools/cvd.ts");
const [th, stepArg = "4", hueArg = "45"] = process.argv.slice(2);
const step = Number(stepArg), HUE = Number(hueArg);
const t = cm.themeTokens(th);
const card = t["color.ui.card"], ink = t["color.ui.ink"], hl = t["color.highlight"];
const STAT = ["closed", "own", "base", "restricted", "open"].map((s) => t[`color.status.${s}`]).concat([t["color.temp.warm"]]);
const { createRequire } = await import("node:module");
const req = createRequire(W + "/app/package.json");
const { namedFlavor } = await import(req.resolve("@protomaps/basemaps"));
const fl = namedFlavor(th === "dark" ? "black" : "light");
const fillOver = (c, o) => cm.over(t[c], fl.earth, Number(t[o]));
const FLAVOUR = [fl.earth, fl.wood_b, fl.water, fillOver("color.lake.fill", "opacity.lake"),
  fillOver("color.wetland.fill", "opacity.wetland"), fillOver("color.park.fill", "opacity.park")];
const GROUND = Object.values(cm.GROUNDS[th === "dark" ? "dark" : "light"]);
const LAKE = cm.GROUNDS[th === "dark" ? "dark" : "light"].river;
const BND = Number(process.env.BND ?? 3.5);
const dh = (a, b) => { const d = Math.abs(a - b) % 360; return d > 180 ? 360 - d : d; };
const hex = (r, g, b) => "#" + [r, g, b].map((v) => Math.min(255, v).toString(16).padStart(2, "0")).join("").toUpperCase();
const all = [];
for (let r = 0; r <= 256; r += step) for (let g = 0; g <= 256; g += step) for (let b = 0; b <= 256; b += step) {
  const x = hex(r, g, b);
  const { C, h } = cvd.lch(x);
  if (C < 30) continue;
  if (dh(h, cvd.lch(t["color.status.base"]).h) < HUE || dh(h, cvd.lch(hl).h) < HUE) continue;
  if (cvd.deltaE(x, hl) < 20) continue;
  if (cm.contrast(x, card) < 3 || cvd.deltaE(x, ink) < 20 || STAT.some((s) => cvd.deltaE(x, s) < 25)) continue;
  if (FLAVOUR.some((g) => cm.worst(x, g).dE < 15)) continue;
  const m = terms(th, x, t);
  if (m["ink.w"] < 15 || m["normal.base"] < 35) continue;
  // the T146 gate's normal-vision base floors as drawn (thin line over every ground, lake fill) and the boundary line
  const base = t["color.status.base"], la = Number(t["opacity.lake"]);
  const nLine = Math.min(...GROUND.map((g) => cvd.deltaE(cm.over(x, g, 0.6), cm.over(base, g, 0.6))));
  const nLake = cvd.deltaE(cm.over(x, LAKE, la), cm.over(base, LAKE, la));
  if (nLine < 30 || nLake < 30 || m["bnd.line"] < BND) continue;
  const water = Math.min(...["closed", "own", "base"].flatMap((s) => [m[`${s}.tok`], m[`${s}.line`], m[`${s}.lake`]]));
  all.push({ x, m, water, hl: cvd.deltaE(x, hl), hlW: cm.worst(x, hl).dE });
}
all.sort((a, b) => b.water - a.water);
console.log(th, `hue >= ${HUE} off base and highlight`, all.length);
for (const c of all.slice(0, 8))
  console.log("  ", c.x, "lch", JSON.stringify(cvd.lch(c.x), (k, v) => typeof v === "number" ? Math.round(v) : v),
    "water", c.water.toFixed(1), "hl", c.hl.toFixed(1), "/", c.hlW.toFixed(1), "baseN", c.m["normal.base"].toFixed(1),
    "hueMin", c.m.hueMin.toFixed(0), ["closed", "own", "base"].map((s) => `${s} ${[".tok", ".line", ".lake"].map((k) => c.m[s + k].toFixed(1)).join("/")}`).join(" "));
