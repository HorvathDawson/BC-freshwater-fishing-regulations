/**
 * T44 — THE WATER FRAME OWNS THE LAYOUT, the part a render test cannot reach: no view of a water sets an outer gutter,
 * a width or a header of its own, so a new view (or a new block in one) cannot bring back the desktop jump the owner
 * saw ("switching between regulation and condition tab makes the margin on the left change and everything move").
 * `water-frame-layout.test.ts` measures the result; this pins the source.
 *
 *   one header      `<FaceBar` is drawn by `WaterFrame.tsx` and nowhere else (each view drew its own before T44)
 *   in the frame    each view draws inside `<WaterFrame` — WaterScreen (Regulations, every state), WaterConditions,
 *                   WaterMap; RegsScreen and WaterMapView are drawn inside those
 *   no own measure  no view names `maxWidth` (the column is `WATER_FRAME.maxWidth`, the frame's) or `alignSelf: "center"`
 *   no own gutter   every horizontal padding / margin / offset a view sets is listed below with what it is — a control's
 *                   inside, an overlay's place — never a view's outer box. An inset block is `<WaterInset>` (the frame's
 *                   gutter). A NEW ONE FAILS HERE until it is listed, and it may be listed only if it is not a gutter.
 *
 * The views: everything under `water/`, `regs/RegsScreen.tsx`, `WaterScreen.tsx` and `WaterViews.tsx`.
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const SRC = fileURLToPath(new URL("../packages/ui-native/src/", import.meta.url));
const VIEWS = [
  ...readdirSync(join(SRC, "water")).filter((f) => /\.tsx$/.test(f) && !/\.test\.tsx$/.test(f)).map((f) => `water/${f}`),
  "regs/RegsScreen.tsx", "WaterScreen.tsx", "WaterViews.tsx",
].sort();
const ALL = (dir = SRC, at = ""): string[] => readdirSync(dir, { withFileTypes: true }).flatMap((e) =>
  e.isDirectory() ? ALL(join(dir, e.name), `${at}${e.name}/`)
    : /\.tsx?$/.test(e.name) && !/\.test\.tsx?$/.test(e.name) ? [`${at}${e.name}`] : []).sort();

/** Comments out (a doc comment may say "FaceBar" or "maxWidth"); strings kept. */
const code = (src: string) => src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:"'`])\/\/.*$/gm, "$1").replace(/\{\s*\}/g, "{}");

/** Every horizontal spacing a view may set, and what it is. Keyed `file: prop: value`, in file order. */
const LISTED: Record<string, string> = {
  "water/WaterMapView.tsx: right: 10": "the ⤢ Whole river / lake button's place over the canvas (an overlay)",
  "water/WaterMapView.tsx: paddingHorizontal: 12": "the ⤢ button's inside",
  "water/WaterMapView.tsx: paddingHorizontal: SPACE.s2": "a ▼ / ▲ step button's inside",
  "water/WaterMapView.tsx: left: 10": "the mark pop / chooser card's place over the canvas (an overlay)",
  "water/WaterMapView.tsx: right: 10#2": "the mark pop / chooser card's place over the canvas (an overlay)",
  "water/WaterMapView.tsx: paddingLeft: 12": "the mark pop / chooser card's inside",
  "water/WaterMapView.tsx: paddingRight: 6": "the mark pop / chooser card's inside (its × button's slot)",
};
const SPACING = /\b(?:(?:padding|margin)(?:Horizontal|Left|Right|Start|End)?|left|right|start|end)\s*:\s*[^,}\s]+/g;

/** What the house rule finds in these sources (path → code); [] is clean. */
export function findings(files: Record<string, string>, all: Record<string, string>): string[] {
  const out: string[] = [];
  const faceBars = Object.keys(all).filter((p) => /<FaceBar\b/.test(code(all[p]!))).sort();
  if (JSON.stringify(faceBars) !== JSON.stringify(["WaterFrame.tsx"])) out.push(`FaceBar drawn by ${faceBars.join(", ")}`);
  for (const [p, src] of Object.entries(files)) {
    const c = code(src);
    if (/\bmaxWidth\s*:/.test(c)) out.push(`${p}: sets a maxWidth`);
    if (/alignSelf\s*:\s*["']center["']/.test(c)) out.push(`${p}: centres itself`);
    const seen = new Map<string, number>();
    for (const m of c.matchAll(SPACING)) {
      const k = `${p}: ${m[0].replace(/\s+/g, " ")}`;
      const n = (seen.get(k) ?? 0) + 1;
      seen.set(k, n);
      const key = n > 1 ? `${k}#${n}` : k;
      if (!(key in LISTED)) out.push(`${key} — not listed`);
    }
  }
  return out;
}

const read = (p: string) => readFileSync(join(SRC, p), "utf8");
const sources = (ps: string[]) => Object.fromEntries(ps.map((p) => [p, read(p)]));

describe("T44 house: the water frame owns the layout every view shares", () => {
  it("the views are the ones named (a new file under water/ is one)", () => {
    expect(VIEWS).toEqual(["regs/RegsScreen.tsx", "water/WaterMapView.tsx", "WaterScreen.tsx", "WaterViews.tsx"].sort());
  });

  it("no view draws a FaceBar, names a width of its own, or sets a gutter that is not listed", () => {
    expect(findings(sources(VIEWS), sources(ALL()))).toEqual([]);
  });

  it("every listed spacing is still there (a stale entry would hide a new one under its name)", () => {
    const c = Object.fromEntries(VIEWS.map((p) => [p, code(read(p))]));
    const present = Object.keys(LISTED).filter((k) => {
      const [p, rest] = [k.slice(0, k.indexOf(": ")), k.slice(k.indexOf(": ") + 2)];
      const [text, nth] = rest.split("#");
      return [...c[p]!.matchAll(SPACING)].filter((m) => m[0].replace(/\s+/g, " ") === text).length >= Number(nth ?? 1);
    });
    expect(present).toEqual(Object.keys(LISTED));
  });

  it("each view draws inside the frame: WaterScreen in every state, WaterConditions and WaterMap", () => {
    const count = (p: string) => code(read(p)).match(/<WaterFrame\b/g)?.length ?? 0;
    // WaterScreen's states go through ONE `frame(…)` helper; WaterViews has a frame per view
    expect({ screen: count("WaterScreen.tsx"), views: count("WaterViews.tsx"),
             helperUses: code(read("WaterScreen.tsx")).match(/\bframe\(/g)?.length ?? 0 })
      .toEqual({ screen: 1, views: 2, helperUses: 4 });
  });

  it("MUTATION: a view that adds its own gutter, its own measure or its own FaceBar fails", () => {
    const views = sources(VIEWS);
    const all = sources(ALL());
    const mutant = {
      ...views,
      "WaterViews.tsx": views["WaterViews.tsx"]!.replace("<WaterFrame palette={palette} face=\"conditions\"",
        "<View style={{ paddingHorizontal: GUTTER, maxWidth: 1280 }}><WaterFrame palette={palette} face=\"conditions\""),
      "regs/RegsScreen.tsx": views["regs/RegsScreen.tsx"]!.replace("const head = <RegsHeader",
        "const bar = <FaceBar palette={palette} face=\"regulations\" onFace={() => {}} />;\n  const head = <RegsHeader"),
    };
    expect(mutant["WaterViews.tsx"]).not.toBe(views["WaterViews.tsx"]);
    expect(mutant["regs/RegsScreen.tsx"]).not.toBe(views["regs/RegsScreen.tsx"]);
    expect(findings(mutant, { ...all, ...mutant })).toEqual([
      "FaceBar drawn by WaterFrame.tsx, regs/RegsScreen.tsx",
      "WaterViews.tsx: sets a maxWidth",
      "WaterViews.tsx: paddingHorizontal: GUTTER — not listed",
    ]);
  });
});
