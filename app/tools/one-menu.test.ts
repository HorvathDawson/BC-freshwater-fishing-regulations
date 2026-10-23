/**
 * ONE PLACE FOR EACH KIND OF THING (T48, owner 2026-10-22: "different ways to flow into the same menu [must be] accessing
 * the same menu and not having duplicate menus") — the parts a render test cannot reach: no second copy of a menu, a
 * spots list, a capture flow or a search can be added beside the one there is.
 *
 *   the menu      its page list (`menuVM(`) is drawn ONLY by the More tab (menu/MoreScreen.tsx); the hamburger and its
 *                 drawer are gone and stay gone (no `MenuButton`, `MenuDrawer`, "Open menu" anywhere)
 *   spots         the list's view model (`spotsMenu(`) is built only by the Shell and drawn only by the Spots tab
 *                 (`<SpotsScreen`); a spot capture starts ONLY through the route's `addSpot`, built only by mapRail.ts
 *                 (the long press, the tab's two adds) and reduced by route.ts — one capture flow
 *   search        results are grouped (`resultGroups(`) only for the one results sheet, which only the Shell mounts, over
 *                 the map the Shell already has: the sheet and the top bar import nothing from `@app/map` and draw no
 *                 `MiniMap` (no second MapLibre); the field (`<TopBar`) is mounted only by the Shell
 *   the rail      Layers alone: MapRail offers no Spots button
 *
 * The scan is a net, not a proof: what each surface DOES is pinned by its render tests (Search.shell, MapRail.shell,
 * Menu, apps/mobile back.test).
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const APP = fileURLToPath(new URL("..", import.meta.url));
const ROOTS = ["apps/mobile/src", "packages/ui/src", "packages/ui-native/src", "packages/map/src"];

function sources(dir: string, out: string[] = []): string[] {
  let names: string[];
  try { names = readdirSync(dir); } catch { return out; }
  for (const f of names) {
    if (f === "node_modules" || f === "dist" || f.startsWith(".") || f.startsWith("_")) continue;
    const p = join(dir, f);
    if (statSync(p).isDirectory()) sources(p, out);
    else if (/\.(ts|tsx)$/.test(f) && !/\.test\.tsx?$/.test(f) && !/\.d\.ts$/.test(f)) out.push(p);
  }
  return out;
}
/** Comments out (a doc comment may name what the code may not use); strings kept. */
const code = (src: string) => src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:"'`])\/\/.*$/gm, "$1");
const files = () => ROOTS.flatMap((r) => sources(join(APP, r))).map((p) => relative(APP, p)).sort();
const read = (p: string) => code(readFileSync(join(APP, p), "utf8"));
const where = (re: RegExp) => files().filter((p) => re.test(read(p)));

describe("one place for each kind of thing (T48)", () => {
  it("the menu's pages are listed only by the More tab; no hamburger, no drawer", () => {
    expect({ list: where(/\bmenuVM\(/), hamburger: where(/\bMenuButton\b|\bMenuDrawer\b|["']Open menu["']/) })
      .toEqual({ list: ["packages/ui-native/src/menu/MoreScreen.tsx"], hamburger: [] });
  });

  it("the spots list is defined in mapRail.ts, built by the Shell and drawn by the Spots tab alone", () => {
    expect({ vm: where(/\bspotsMenu\(/), drawn: where(/<SpotsScreen\b/) })
      .toEqual({ vm: ["packages/ui-native/src/Shell.tsx", "packages/ui/src/mapRail.ts"],
                drawn: ["packages/ui-native/src/Shell.tsx"] });
  });

  it("ONE capture flow: an addSpot route action is built only by mapRail.ts (and reduced by the route)", () => {
    expect(where(/["']addSpot["']/)).toEqual(["packages/ui/src/mapRail.ts", "packages/ui/src/route.ts"]);
  });

  it("ONE search: results grouped for the one sheet, mounted only by the Shell, with the field beside it", () => {
    expect({ grouped: where(/\bresultGroups\(/), sheet: where(/<SearchSheet\b/), field: where(/<TopBar\b/) }).toEqual({
      grouped: ["packages/ui-native/src/SearchSheet.tsx", "packages/ui/src/search.ts"],
      sheet: ["packages/ui-native/src/Shell.tsx"], field: ["packages/ui-native/src/Shell.tsx"] });
  });

  it("no second MapLibre for search: the sheet and the field import no map and draw no MiniMap", () => {
    const mapless = ["packages/ui-native/src/SearchSheet.tsx", "packages/ui-native/src/TopBar.tsx"]
      .filter((p) => /from\s+["']@app\/map["']|\bMiniMap\b|<Map\b/.test(read(p)));
    expect(mapless).toEqual([]);
  });

  it("the rail is Layers alone: no Spots button on a map", () => {
    expect(/\bonSpots\b|\bspotsLabel\b/.test(read("packages/ui-native/src/MapRail.tsx"))
      || /\bonSpots\b/.test(read("packages/ui-native/src/MapScreen.tsx"))).toBe(false);
  });

  it("MUTATION PIN: the scan sees through to a copy — a second list of the menu's pages would be found", () => {
    const fake = code(`// menuVM( in a comment does not count\nexport function Drawer() { return menuVM().items; }`);
    const comment = code(`/* menuVM( */ export const x = 1;`);
    expect([/\bmenuVM\(/.test(fake), /\bmenuVM\(/.test(comment)]).toEqual([true, false]);
  });
});
