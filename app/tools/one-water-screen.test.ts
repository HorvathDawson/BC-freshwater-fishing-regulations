/**
 * ONE WATER SCREEN, WHICHEVER MAP YOU TAPPED (owner rule 2026-10-21) — the parts a render test cannot reach: no other
 * path into a water can be added beside the one function, and no second Conditions screen can come back.
 *
 *   - an `openWater` route action is built ONLY by `@app/ui` openWater.ts (and named by the route that reduces it), so
 *     both maps' taps, search, spots and any deep path go through `openWater` / `openTap`;
 *   - a tapped section is resolved to its water (`.itemForSection(`) ONLY in openWater.ts;
 *   - a section's Conditions panel is drawn ONLY by the water screen's Conditions view (`WaterViews.tsx`), and the old
 *     browse-mode drill (`ConditionsScreen`, `condSection`) exists nowhere.
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const APP = fileURLToPath(new URL("..", import.meta.url));
const ROOTS = ["apps", "packages/ui/src", "packages/ui-native/src", "packages/map/src"];

function sources(dir: string, out: string[] = []): string[] {
  for (const f of readdirSync(dir)) {
    if (f === "node_modules" || f === "dist" || f.startsWith(".")) continue;
    const p = join(dir, f);
    if (statSync(p).isDirectory()) sources(p, out);
    else if (/\.(ts|tsx)$/.test(f) && !/\.test\.tsx?$/.test(f)) out.push(p);
  }
  return out;
}
const files = () => ROOTS.flatMap((r) => sources(join(APP, r))).map((p) => relative(APP, p)).sort();
const where = (re: RegExp) => files().filter((p) => re.test(readFileSync(join(APP, p), "utf8")));

describe("one water screen: one way in, one Conditions panel", () => {
  it("an openWater action is built only by openWater.ts (and reduced by the route)", () => {
    expect(where(/["']openWater["']/)).toEqual(["packages/ui/src/openWater.ts", "packages/ui/src/route.ts"]);
  });

  it("a tapped section is resolved to its water only in openWater.ts", () => {
    expect(where(/\.itemForSection\(/)).toEqual(["packages/ui/src/openWater.ts"]);
  });

  it("only the water screen's Conditions view draws a section's Conditions panel; the browse drill is gone", () => {
    expect({ panel: where(/<ConditionsPanel\b/), drill: where(/\bConditionsScreen\b|\bcondSection\b/) })
      .toEqual({ panel: ["packages/ui-native/src/WaterViews.tsx"], drill: [] });
  });
});
