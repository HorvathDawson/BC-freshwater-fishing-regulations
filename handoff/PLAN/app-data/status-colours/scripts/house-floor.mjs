// T47: the house style check's two palette floors, run on a candidate BEFORE it is written to the theme files.
//
//   node handoff/PLAN/app-data/status-colours/scripts/house-floor.mjs <theme> '{"color.status.closed":"#..", ...}'
//
// tools/build-style.mjs refuses a theme where (1) two map tokens share a hex, or (2) two map tokens that some view can
// draw together are under CIE76 ΔE 12 apart. This prints every map token within ΔE76 12 of a token the candidate
// changes — a SUPERSET of what build-style refuses (it ignores the per-view "drawn together" relief), so a clean run
// here is clean there. The authority stays tools/build-style.mjs: apply-palette.mjs + `node app/tools/build-style.mjs`.
import { parseHex, themeTokens } from "./colour-math.mjs";

const [theme, arg] = process.argv.slice(2);
const ov = JSON.parse(arg);
const t = themeTokens(theme, ov);
const lab = (hex) => {
  const [r, g, b] = parseHex(hex).map((v) => v / 255).map((c) => (c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4));
  const f = (x) => (x > 0.008856 ? Math.cbrt(x) : 7.787 * x + 16 / 116);
  const X = f((r * 0.4124 + g * 0.3576 + b * 0.1805) / 0.95047), Y = f(r * 0.2126 + g * 0.7152 + b * 0.0722);
  const Z = f((r * 0.0193 + g * 0.1192 + b * 0.9505) / 1.08883);
  return [116 * Y - 16, 500 * (X - Y), 200 * (Y - Z)];
};
const d76 = (a, b) => Math.hypot(...lab(a).map((v, i) => v - lab(b)[i]));
const map = Object.entries(t).filter(([n, v]) => typeof v === "string" && v.startsWith("#") && !n.startsWith("color.ui."));
const hits = [];
for (const k of Object.keys(ov).filter((k) => typeof ov[k] === "string" && ov[k].startsWith("#")))
  for (const [n, v] of map)
    if (n !== k && d76(t[k], v) < 12) hits.push(`${k} ${t[k]} ~ ${n} ${v}: ΔE76 ${d76(t[k], v).toFixed(1)}`);
console.log(hits.length ? hits.join("\n") : "clean");
