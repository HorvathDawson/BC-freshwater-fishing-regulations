// T47: which NEUTRAL boundary colour (or, with mode "any", which status colour) sits furthest from a given set of colours.
//
//   node handoff/PLAN/app-data/status-colours/scripts/grey-scan.mjs <theme> '{"closed":"#..","own":"#..","base":"#.."}' [grey|any] [min card contrast]
//
// Scans greys from near-black to near-white with a slight warm or cool cast (a cast of 0 is a pure grey; |cast| <= 6 still
// reads as neutral ink/slate/taupe) and prints the best few by the worst of: ΔE to every status as tokens (the ticket's
// gate) and as a thin line beside a thin status line on every ground, plus the boundary's own ΔE to its ground.
import { dEUnder, GROUNDS, groundsFor, LINE_ALPHA, over, VISIONS } from "./colour-math.mjs";

const [theme = "light", arg = "{}", mode = "grey", minCard = "0"] = process.argv.slice(2);
const { contrast, themeTokens } = await import("./colour-math.mjs");
const card = themeTokens(theme)["color.ui.card"];
const S = JSON.parse(arg);
const grounds = Object.values(groundsFor(theme));
const hex = (r, g, b) => `#${[r, g, b].map((v) => Math.max(0, Math.min(255, Math.round(v))).toString(16).padStart(2, "0")).join("")}`;
const rows = [];
// mode "any": every colour on a 15-step RGB grid that clears `minCard`:1 on the card (a status, not a boundary)
const candidates = [];
if (mode === "any") {
  for (let r = 0; r <= 255; r += 15) for (let gg = 0; gg <= 255; gg += 15) for (let b = 0; b <= 255; b += 15) {
    const h = hex(r, gg, b);
    if (contrast(h, card) >= Number(minCard)) candidates.push(h);
  }
} else for (let v = 16; v <= 236; v += 3) for (const cast of [-9, -6, -3, 0, 3, 6, 9, 12]) candidates.push(hex(v + cast, v, v - cast));
// THE HOUSE FLOOR: tools/build-style.mjs refuses two map tokens under CIE76 ΔE 12 (house-floor.mjs). Skip any colour that
// close to a map token it is not replacing (the statuses under test are given on the command line and replace theirs).
const lab76 = (h) => {
  const [r, g, b] = (h.match(/[0-9a-f]{2}/gi)).map((x) => parseInt(x, 16) / 255).map((c) => (c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4));
  const f = (x) => (x > 0.008856 ? Math.cbrt(x) : 7.787 * x + 16 / 116);
  const X = f((r * 0.4124 + g * 0.3576 + b * 0.1805) / 0.95047), Y = f(r * 0.2126 + g * 0.7152 + b * 0.0722);
  const Z = f((r * 0.0193 + g * 0.1192 + b * 0.9505) / 1.08883);
  return [116 * Y - 16, 500 * (X - Y), 200 * (Y - Z)];
};
const d76 = (a, b) => Math.hypot(...lab76(a).map((v, i) => v - lab76(b)[i]));
const tk = themeTokens(theme);
const replaced = new Set([...Object.keys(S).map((k) => `color.status.${k}`), "color.region.edge", "color.mu.edge"]);
const houseMap = [...Object.entries(tk).filter(([n, v]) => typeof v === "string" && v.startsWith("#") && !n.startsWith("color.ui.") && !replaced.has(n)).map(([, v]) => v),
                  ...Object.values(S)];
const exclude = process.env.AVOID ? process.env.AVOID.split(",") : [];
for (const g of candidates) {
  if (houseMap.some((v) => d76(g, v) < 12) || exclude.some((v) => d76(g, v) < 12)) continue;
  {
    let pure = Infinity, line = Infinity, ground = Infinity;
    for (const s of Object.values(S))
      for (const vis of VISIONS) {
        pure = Math.min(pure, dEUnder(g, s, vis));
        for (const gr of grounds) line = Math.min(line, dEUnder(over(g, gr, 0.8), over(s, gr, LINE_ALPHA), vis));
      }
    for (const gr of grounds) for (const vis of VISIONS) ground = Math.min(ground, dEUnder(g, gr, vis));
    rows.push({ g, pure: +pure.toFixed(1), line: +line.toFixed(1), ground: +ground.toFixed(1),
                score: Math.min(pure, line + 6, ground + 6) });
  }
}
rows.sort((a, b) => b.score - a.score);
console.log(rows.slice(0, 8).map((r) => JSON.stringify(r)).join("\n"));
