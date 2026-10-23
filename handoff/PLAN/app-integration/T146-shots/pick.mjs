// node pick.mjs <theme> <hlo> <hhi> <hueMin> [step]
// Inside a hue window, holding one meaning (normal ΔE >= 25 from every status and the warm dot, LCh hue >= hueMin from
// each), ink, and 3:1 on the card: maximise the worst-vision minimum over closed / own / base as tokens, thin lines and lake
// fills, then the boundary token. Prints the top 5.
import { terms } from "./measure.mjs";
const W = new URL("../../../..", import.meta.url).pathname.replace(/\/$/, "");
const cm = await import(W + "/handoff/PLAN/app-data/status-colours/scripts/colour-math.mjs");
const cvd = await import(W + "/app/tools/cvd.ts");
const [th, lo, hi, hm, stepArg = "4"] = process.argv.slice(2);
const step = Number(stepArg), hlo = Number(lo), hhi = Number(hi), hueMin = Number(hm);
const t = cm.themeTokens(th);
const card = t["color.ui.card"], ink = t["color.ui.ink"];
const STAT = ["closed", "own", "base", "restricted", "open"].map((s) => t[`color.status.${s}`]).concat([t["color.temp.warm"]]);
// the grounds tools/water-status-cvd.test.ts holds every status 15 off (worst vision): the flavour's land / forest / water
// and our lake / wetland / park fills over the land (cvd rides on the light ground, held by tools/cvd.test.ts instead)
const { createRequire } = await import("node:module");
const req = createRequire(W + "/app/package.json");
const { namedFlavor } = await import(req.resolve("@protomaps/basemaps"));
const fl = namedFlavor(th === "dark" ? "black" : "light");
const fillOver = (c, o) => cm.over(t[c], fl.earth, Number(t[o]));
const FLAVOUR = th === "cvd" ? [] : [fl.earth, fl.wood_b, fl.water, fillOver("color.lake.fill", "opacity.lake"),
  fillOver("color.wetland.fill", "opacity.wetland"), fillOver("color.park.fill", "opacity.park")];
const inHue =(h) => (hlo <= hhi ? h >= hlo && h <= hhi : h >= hlo || h <= hhi);
const hex = (r, g, b) => "#" + [r, g, b].map((v) => Math.min(255, v).toString(16).padStart(2, "0")).join("").toUpperCase();
const all = [];
for (let r = 0; r <= 256; r += step) for (let g = 0; g <= 256; g += step) for (let b = 0; b <= 256; b += step) {
  const x = hex(r, g, b);
  const { C, h } = cvd.lch(x);
  if (C < 30 || !inHue(h)) continue;
  if (cm.contrast(x, card) < 3 || cvd.deltaE(x, ink) < 20 || STAT.some((s) => cvd.deltaE(x, s) < 25)) continue;
  if (FLAVOUR.some((g) => cm.worst(x, g).dE < 15)) continue;
  const m = terms(th, x, t);
  if (m["ink.w"] < 15 || m.hueMin < hueMin) continue;
  const water = Math.min(...["closed", "own", "base"].flatMap((s) => [m[`${s}.tok`], m[`${s}.line`], m[`${s}.lake`]]));
  all.push({ x, m, water, score: water + 0.15 * Math.min(m["bnd.tok"], 20) + 0.1 * Math.min(m["restr.line"], 15) });
}
all.sort((a, b) => b.score - a.score);
const show = (c) => `${c.x} lch ${JSON.stringify(cvd.lch(c.x), (k, v) => typeof v === "number" ? Math.round(v) : v)} water ${c.water.toFixed(1)} ` +
  `baseN ${c.m["normal.base"].toFixed(1)} meaning ${c.m.meaning.toFixed(1)} hueBase ${c.m["hue.base"].toFixed(0)} hueMin ${c.m.hueMin.toFixed(0)} ` +
  ["closed", "own", "base"].map((s) => `${s} ${[".tok", ".line", ".lake", ".sat"].map((k) => c.m[s + k].toFixed(1)).join("/")}`).join(" ") +
  ` restr ${c.m["restr.tok"].toFixed(1)}/${c.m["restr.line"].toFixed(1)}/${c.m["restr.sat"].toFixed(1)} bnd ${c.m["bnd.tok"].toFixed(1)}/${c.m["bnd.line"].toFixed(1)} ink ${c.m["ink.n"].toFixed(1)}/${c.m["ink.w"].toFixed(1)} card ${c.m.card.toFixed(1)}`;
console.log(th, `[${hlo},${hhi}] hueMin>=${hueMin}`, all.length);
for (const c of all.slice(0, 5)) console.log("  ", show(c));
