/**
 * (T1164) EVERY SCREEN, PAGE AND PANEL IS MOUNTED THROUGH AN ERROR BOUNDARY, and the app itself through a last-resort one.
 *
 *   - the shell (`Shell.tsx`) mounts its four screen hosts (a spot, the add-a-spot flow, a water, the browse tabs) through its
 *     `screen(name, key, node)` helper, which wraps them in `ScreenBoundary`; the screens below them (Map, Conditions,
 *     Spots, More, the water's views) are drawn by those hosts;
 *   - the sheets and cards drawn over a screen (Layers, the day, the in-season bell, the menu's pages' host, …) are ONE
 *     `screen("sheet", …)` block; the search bar and its results, and the map's key, each have their own;
 *   - each page a menu opens (`MenuHost`) is inside its own `ScreenBoundary`;
 *   - `index.js` registers the app only through `withRootBoundary`.
 *
 * A NEW HOST, SHEET, PAGE OR POPUP MOUNTED IN THE SHELL OUTSIDE A BOUNDARY FAILS HERE: any component named `…Host`, `…Screen`,
 * `…Sheet(s)`, `…Popup`, `…Notice`, `…Viewer`, `…Curtain` (or the spot capture) in `Shell.tsx` must lie inside a `screen(…)` call
 * or a `<ScreenBoundary>`, or be listed in OUTSIDE with its reason. The files in `shell/` are mounted BY the shell, so
 * they are covered by its boundary: each is listed in CALLED_FROM_SHELL, and a new one that mounts such a component is not
 * until it is listed. The scan reads the files where screens are mounted: a screen mounted from some other file is a new
 * mount point, and this test says so only for `Shell.tsx`, `shell/*.tsx` and `menu/MenuHost.tsx`.
 */
import { describe, expect, it } from "vitest";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const APP = fileURLToPath(new URL("..", import.meta.url));
const UI = join(APP, "packages/ui-native/src");
const strip = (src: string): string =>
  src.replace(/\/\*[\s\S]*?\*\//g, (c) => c.replace(/[^\n]/g, "")).replace(/(^|[\s;{}),])\/\/.*$/gm, "$1");
const read = (p: string): string => strip(readFileSync(p, "utf8"));

/** The text index of the match of the opener at `open` (`(`): strings and templates skipped roughly. */
function closeOf(src: string, open: number): number {
  let depth = 0;
  for (let i = open; i < src.length; i += 1) {
    const c = src[i]!;
    if (c === '"' || c === "'" || c === "`") { for (i += 1; i < src.length && src[i] !== c; i += 1) if (src[i] === "\\") i += 1; continue; }
    if (c === "(") depth += 1;
    else if (c === ")") { depth -= 1; if (!depth) return i; }
  }
  return src.length;
}

/** Where a boundary is: the inside of every `screen(…)` call (and `extra(…)` helpers) and of every `<ScreenBoundary>…</ScreenBoundary>`. */
function covered(src: string, helpers: string[] = ["screen"]): [number, number][] {
  const out: [number, number][] = [];
  for (const h of helpers) for (const m of src.matchAll(new RegExp(`\\b${h}\\(`, "g"))) out.push([m.index!, closeOf(src, m.index! + h.length)]);
  for (const m of src.matchAll(/<ScreenBoundary\b/g)) {
    let depth = 0;
    for (const t of src.slice(m.index!).matchAll(/<(\/?)ScreenBoundary\b/g)) {
      depth += t[1] ? -1 : 1;
      if (!depth) { out.push([m.index!, m.index! + t.index!]); break; }
    }
  }
  return out;
}

const SCREENISH = /<([A-Z]\w*(?:Screen|Host|Sheets?|Popup|Notice|Viewer|Curtain)|SpotCapture)\b/g;
/** The screen-like components a file draws that lie outside every boundary. */
const uncovered = (src: string, helpers?: string[]): string[] => {
  const regions = covered(src, helpers);
  return [...src.matchAll(SCREENISH)].filter((m) => !regions.some(([a, b]) => m.index! > a && m.index! < b)).map((m) => m[1]!);
};
const drawn = (src: string): string[] => [...src.matchAll(SCREENISH)].map((m) => m[1]!);

/** Drawn in the shell on purpose outside any boundary, and why. */
const OUTSIDE: Record<string, string> = {
  ApplyCurtain: "the curtain of a release being put in use takes every tap and the back by design; it is the app's own state machine, and a fault in it is the root's",
};
/** The files in `shell/` the shell mounts inside its own boundaries (a screen host, the sheets' block). */
const CALLED_FROM_SHELL: Record<string, string> = {
  "BrowseHost.tsx": "mounted by Shell through screen(tab, …)",
  "WaterModeHost.tsx": "mounted by Shell through screen(\"water\", …)",
  "SpotHost.tsx": "mounted by Shell through screen(\"spot\", …)",
  "ShellSheets.tsx": "mounted by Shell inside the sheet block, screen(\"sheet\", …)",
};

describe("every screen, page and panel is mounted through a ScreenBoundary", () => {
  const shell = read(join(UI, "Shell.tsx"));

  it("the shell draws exactly these screen-like components, each inside a boundary but the listed ones", () => {
    expect(drawn(shell).sort()).toEqual([
      "AppUpdatePopup", "ApplyCurtain", "BrowseHost", "DaySheet", "FirstRunNotice", "GaugeScreen", "InSeasonNotice", "LayersSheet", "NotFishingSheet",
      "ObstacleSheet", "PdfViewer", "RunsSheet", "SearchSheet", "ShellSheets", "SpotCapture", "SpotHost", "SyncErrorSheet", "WaterModeHost"]);
    expect(uncovered(shell).sort(), "a screen, sheet or popup is mounted in Shell.tsx outside screen(…)").toEqual(Object.keys(OUTSIDE));
  });

  it("the four screen hosts are each the node of a screen(…) call, and the three bands (sheets, search, key) exist", () => {
    for (const host of ["BrowseHost", "WaterModeHost", "SpotHost", "SpotCapture"]) expect(shell, host).toMatch(new RegExp(`screen\\([^<\\n]*<${host}\\b`));
    expect(shell).toContain('screen("sheet", "sheets"');
    expect(shell).toContain('screen("search", "top"');
    expect(shell).toContain('screen("key", "foot"');
  });

  it("the helper wraps its node in ScreenBoundary, keyed, with a way out and a placing", () => {
    expect(shell).toMatch(/const screen = \(name: ScreenName, key: string, node: React\.ReactNode, goBack\?: \(\) => void, placing\?: "top" \| "bottom"\) => \(\s*<ScreenBoundary screen=\{name\} resetKey=\{key\} palette=\{palette\} goBack=\{goBack\} placing=\{placing\}>\{node\}<\/ScreenBoundary>\);/);
  });

  it("every file in shell/ that draws such a component is one the shell mounts inside its boundaries", () => {
    const files = readdirSync(join(UI, "shell")).filter((f) => /\.tsx$/.test(f) && !/\.test\./.test(f));
    const drawing = files.filter((f) => drawn(read(join(UI, "shell", f))).length > 0).sort();
    expect(drawing, "a new file in shell/ draws a screen: list it in CALLED_FROM_SHELL once the shell mounts it in a boundary")
      .toEqual(Object.keys(CALLED_FROM_SHELL).sort());
  });

  it("each page the menu opens (and the glossary) is inside its own ScreenBoundary in MenuHost", () => {
    const menu = read(join(UI, "menu/MenuHost.tsx"));
    expect(menu).toContain('<ScreenBoundary screen="page" resetKey={k} palette={palette} goBack={menu.closePage}>{node}</ScreenBoundary>');
    expect(menu).toContain('<ScreenBoundary screen="page" resetKey="glossary" palette={palette} goBack={menu.closePage}>');
    const pages = [...menu.matchAll(/<(?!MenuPage\b)([A-Z]\w*Page)\b/g)].filter((m) => !covered(menu, ["page"]).some(([a, b]) => m.index! > a && m.index! < b)
      && !covered(menu).some(([a, b]) => m.index! > a && m.index! < b));
    expect(pages.map((m) => m[1])).toEqual([]);
  });

  it("the scan catches a mount outside a boundary, and one inside (fixtures)", () => {
    expect(uncovered(`return <View><NewThingSheet a={1} /></View>;`)).toEqual(["NewThingSheet"]);
    expect(uncovered(`return screen("sheet", "s", <><NewThingSheet a={(1)} /></>, close, "bottom");`)).toEqual([]);
    expect(uncovered(`return <ScreenBoundary screen="page"><OddHost /></ScreenBoundary>;`)).toEqual([]);
    expect(uncovered(`return <ScreenBoundary screen="page"><A /></ScreenBoundary><OddHost />;`)).toEqual(["OddHost"]);
  });
});

describe("the app root is mounted inside the last-resort boundary", () => {
  const INDEX = read(join(APP, "apps/mobile/index.js"));

  it("index.js registers App only through withRootBoundary (inside the gesture root)", () => {
    expect(INDEX).toContain("registerRootComponent(withGestureRoot(withRootBoundary(App)));");
    expect(INDEX.match(/registerRootComponent\(/g)).toHaveLength(1);
  });

  it("the root boundary is a ScreenBoundary of the app's own line", () => {
    const src = readFileSync(join(APP, "apps/mobile/src/rootBoundary.tsx"), "utf8");
    expect(src).toContain('<ScreenBoundary screen="app" resetKey="app" palette={THEMES.light}>');
  });
});
