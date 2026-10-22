// Searches a water-label ink per theme: >= 4.5:1 on the paper, worst-reader ΔE >= floor from every status colour, and
// ΔE76 >= 12 (the style build's rule) from every other map colour. Prints the best few by distance from the current ink.
// Usage (from app/): node ../handoff/PLAN/app-data/map-polish/scripts/label-search.ts <theme> <floor>
import { contrast, deltaE, MODELS, worstSeparation } from "../../../../../app/tools/cvd.ts";
import { readFileSync } from "node:fs";

const [theme, floorArg] = process.argv.slice(2);
const floor = Number(floorArg ?? 15);
const raw = JSON.parse(readFileSync(`packages/map/style/themes/${theme}.json`, "utf8")).values as Record<string, unknown>;
const t: Record<string, string> = {};
for (const [k, v] of Object.entries(raw)) if (typeof v === "string") t[k] = v.startsWith("@") ? (raw[v.slice(1)] as string) : v;
const lab76 = (a: string, b: string) => {
  const L = (hex: string) => {
    const h = hex.replace("#", "");
    const [r, g, b] = [0, 2, 4].map((i) => parseInt(h.slice(i, i + 2), 16) / 255)
      .map((c) => (c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4));
    const f = (x: number) => (x > 0.008856 ? Math.cbrt(x) : 7.787 * x + 16 / 116);
    const X = f((r! * 0.4124 + g! * 0.3576 + b! * 0.1805) / 0.95047), Y = f(r! * 0.2126 + g! * 0.7152 + b! * 0.0722);
    const Z = f((r! * 0.0193 + g! * 0.1192 + b! * 0.9505) / 1.08883);
    return [116 * Y - 16, 500 * (X - Y), 200 * (Y - Z)];
  };
  const p = L(a), q = L(b);
  return Math.hypot(p[0]! - q[0]!, p[1]! - q[1]!, p[2]! - q[2]!);
};
const others = Object.entries(t).filter(([k, v]) => v.startsWith("#") && !k.startsWith("color.ui.")
  && k !== "color.label.water" && !k.startsWith("color.flow."));
const statuses = ["closed", "own", "base", "restricted", "open"].map((s) => t[`color.status.${s}`]!);
const cur = t["color.label.water"]!;
const hex = (n: number) => n.toString(16).padStart(2, "0");
const out: [number, string, number, number][] = [];
for (let r = 0; r < 256; r += 8) for (let g = 0; g < 256; g += 8) for (let b = 0; b < 256; b += 8) {
  const c = `#${hex(r)}${hex(g)}${hex(b)}`;
  if (b < r + 30 || b < g) continue;                                   // a blue: water reads as water
  const k = contrast(c, t["color.outside"]!);
  if (k < 4.5) continue;
  const w = Math.min(...statuses.map((s) => worstSeparation(c, s, MODELS).dE));
  if (w < floor) continue;
  if (others.some(([, v]) => lab76(c, v) < 12)) continue;
  out.push([deltaE(c, cur), c, w, k]);
}
out.sort((a, b) => a[0] - b[0]);
console.log(theme, "current", cur, out.length, "candidates");
for (const [d, c, w, k] of out.slice(0, 8)) console.log(c, `from current ΔE ${d.toFixed(1)}`, `status ${w.toFixed(1)}`, `${k.toFixed(2)}:1`);
