/**
 * ONE LIST OF A WATER'S PARTS, AND NO BOLD ON ITS MAP (T150 / T151, owner 2026-10-23) — the parts a render test cannot
 * reach, because each is about what is NOT written anywhere else:
 *
 *   - the words "Parts of this water" are spelled once (core regs/words.ts `PARTS_TITLE`); the Map view's list
 *     (`WATER_MAP_WORDS.parts`) and the start-link Parts sheet read that one value, so they cannot drift apart;
 *   - no app surface opens the Parts sheet (`"parts"` sheet key) or draws a Parts button: the water's Map view list is
 *     the one place a reader picks a part by name (the sheet stays reachable only through the web start link
 *     `sheet=parts`, T150 "Left / follow-ups");
 *   - a boundary mark's label face is ONE value (`@app/map` marks.ts `MARK_LABEL_FONT`, weight 400) read by both maps —
 *     neither map spells a face or a weight of its own for it.
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const APP = fileURLToPath(new URL("..", import.meta.url));
const read = (p: string) => readFileSync(join(APP, p), "utf8");

function sources(dir: string, out: string[] = []): string[] {
  for (const f of readdirSync(dir)) {
    if (f === "node_modules" || f === "dist" || f.startsWith(".")) continue;
    const p = join(dir, f);
    if (statSync(p).isDirectory()) sources(p, out);
    else if (/\.(ts|tsx)$/.test(f) && !/\.test\.tsx?$/.test(f)) out.push(p);
  }
  return out;
}
const files = (roots: string[]) => roots.flatMap((r) => sources(join(APP, r))).map((p) => relative(APP, p)).sort();
const where = (roots: string[], re: RegExp) => files(roots).filter((p) => re.test(read(p)));

describe("(T150) one list of a water's parts", () => {
  it('"Parts of this water" is spelled once, in core regs/words.ts', () => {
    expect(where(["packages", "apps"], /Parts of this water/)).toEqual(["packages/core/src/regs/words.ts"]);
    expect(/parts: WORDS\.PARTS_TITLE,/.test(read("packages/core/src/waterNav.ts"))).toBe(true);
  });

  it("no app surface opens the Parts sheet or draws a Parts button (the Map view's list is the one picker by name)", () => {
    const ui = ["apps/mobile/src", "packages/ui/src", "packages/ui-native/src"];
    expect([where(ui, /["'`]parts["'`]\s*(as\s+SheetKey|\))|sheet:\s*["']parts["']|openSheet\(\s*["']parts/),
            where(ui, /\bonParts\b|PARTS_ARIA|WORDS\.PARTS\b/)]).toEqual([[], []]);
  });
});

describe("(T151) a mark's label face is one value, read by both maps", () => {
  // (T317) the marks are map-engine symbol layers now (`point-marks.ts`, read by Map.native and web/markers.ts): the
  // label face is ONE value, `MARK_LABEL_FONT_STACK`, in the layer; neither map draws a label view or spells its face
  it("the marks' layers carry MARK_LABEL_FONT_STACK; both maps take their layers from point-marks; neither spells a label face", () => {
    const pm = read("packages/map/src/point-marks.ts"), native = read("packages/map/src/Map.native.tsx");
    const webMarks = read("packages/map/src/web/markers.ts"), web = read("packages/map/src/Map.web.tsx");
    expect([/"text-font": MARK_LABEL_FONT_STACK/.test(pm), /marksLayers[^;]*from "\.\/point-marks"/.test(native),
            /marksLayers[^;]*from "\.\.\/point-marks"/.test(webMarks),
            /font:\s*\d00 12px/.test(web), /\{k\.label\}/.test(native)])
      .toEqual([true, true, true, false, false]);
  });
});
