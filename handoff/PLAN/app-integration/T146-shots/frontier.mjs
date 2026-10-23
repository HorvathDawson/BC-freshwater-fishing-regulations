// node frontier.mjs <theme> [step]: for each CVD floor T (worst-vision min over closed/own/base as tokens, lines, lakes),
// the candidate furthest from base IN NORMAL VISION (a combined score: normal ΔE to base, and LCh hue distance from base),
// holding the one-meaning, ink, card and boundary-token rules.
import { terms } from "./measure.mjs";
const W = new URL("../../../..", import.meta.url).pathname.replace(/\/$/, "");
const cm = await import(W + "/handoff/PLAN/app-data/status-colours/scripts/colour-math.mjs");
const cvd = await import(W + "/app/tools/cvd.ts");
const [th, stepArg = "8"] = process.argv.slice(2);
const step = Number(stepArg);
const t = cm.themeTokens(th);
const card = t["color.ui.card"], ink = t["color.ui.ink"];
const STAT = ["closed", "own", "base", "restricted", "open"].map((s) => t[`color.status.${s}`]).concat([t["color.temp.warm"]]);
const hex = (r, g, b) => "#" + [r, g, b].map((v) => Math.min(255, v).toString(16).padStart(2, "0")).join("").toUpperCase();
const all = [];
for (let r = 0; r <= 256; r += step) for (let g = 0; g <= 256; g += step) for (let b = 0; b <= 256; b += step) {
  const x = hex(r, g, b);
  if (cvd.lch(x).C < 25) continue;
  if (cm.contrast(x, card) < 3 || cvd.deltaE(x, ink) < 20 || STAT.some((s) => cvd.deltaE(x, s) < 20)) continue;
  const m = terms(th, x, t);
  if (m["ink.w"] < 15) continue;
  const water = Math.min(...["closed", "own", "base"].flatMap((s) => [m[`${s}.tok`], m[`${s}.line`], m[`${s}.lake`]]));
  all.push({ x, m, water });
}
const show = (c) => `${c.x} lch ${JSON.stringify(cvd.lch(c.x), (k, v) => typeof v === "number" ? Math.round(v) : v)} water ${c.water.toFixed(1)} ` +
  `baseN ${c.m["normal.base"].toFixed(1)} hueBase ${c.m["hue.base"].toFixed(0)} hueMin ${c.m.hueMin.toFixed(0)} ` +
  ["closed", "own", "base"].map((s) => `${s} ${[".tok", ".line", ".lake", ".sat"].map((k) => c.m[s + k].toFixed(0)).join("/")}`).join(" ") +
  ` restr ${c.m["restr.tok"].toFixed(0)}/${c.m["restr.line"].toFixed(0)}/${c.m["restr.sat"].toFixed(0)} bnd ${c.m["bnd.tok"].toFixed(0)}/${c.m["bnd.line"].toFixed(0)} card ${c.m.card.toFixed(1)}`;
for (const T of [22, 20, 18, 16, 14, 12, 10]) {
  const ok = all.filter((c) => c.water >= T && c.m["hue.base"] >= Number(process.env.HB ?? 40));
  ok.sort((a, b) => (b.m["hue.base"] + b.m["normal.base"]) - (a.m["hue.base"] + a.m["normal.base"]));
  console.log(`T=${T} n=${ok.length}`);
  for (const c of ok.slice(0, 3)) console.log("  ", show(c));
}
