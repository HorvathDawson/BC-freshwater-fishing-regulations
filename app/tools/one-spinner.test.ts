/**
 * (T257, owner 2026-10-29) ONE SPINNER, ONE TIMING. Every waiting state in the app is the FishSpinner, drawn through
 * `LoadingState` / `useDelayedShow` (so none flashes on a quick load): this fails on
 *   - an `ActivityIndicator` anywhere in app code;
 *   - a `FishSpinner` used outside `LoadingState.tsx` (its own files and index export aside);
 *   - (T814) a `CohoLoader` the same way, and the coho asked of `LoadingState` anywhere but the start screen
 *     (apps/mobile `App.tsx` `BootScreen`) and the applying curtain (`ApplyCurtain.tsx`): owner 2026-11-17, the coho
 *     "will replace the loading circle on the splash page or curtain (it will still be used for all other loading)";
 *   - a hand-made turning loop: an `Animated.loop` that drives a `rotate`;
 *   - a waiting word drawn alone as a button label or chip ("Loading…", "Refreshing…", "…") — the busy state is the
 *     component's `busy`, which draws the spinner.
 * Reads source off disk (tools/, not the component package: the boundaries gate keeps node:fs out of it).
 */
import { describe, expect, it } from "vitest";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("..", import.meta.url));
const DIRS = ["packages/ui-native/src", "packages/ui/src", "apps/mobile/src"].map((d) => join(ROOT, d));
const files: [string, string][] = [];
const walk = (d: string) => {
  for (const f of readdirSync(d)) {
    const p = join(d, f);
    if (statSync(p).isDirectory()) { if (f !== "node_modules" && f !== "dist") walk(p); continue; }
    if (!/\.tsx?$/.test(f) || /\.test\.tsx?$/.test(f) || f.startsWith("_") || f.endsWith(".d.ts")) continue;
    files.push([relative(ROOT, p), readFileSync(p, "utf8").replace(/\/\*[\s\S]*?\*\//g, "").replace(/^\s*\/\/.*$/gm, "")]);
  }
};
for (const d of DIRS) walk(d);

const SPINNER_HOME = new Set(["packages/ui-native/src/LoadingState.tsx", "packages/ui-native/src/FishSpinner.web.tsx",
                              "packages/ui-native/src/FishSpinner.native.tsx", "packages/ui-native/src/index.ts",
                              "packages/ui-native/src/CohoLoader.web.tsx", "packages/ui-native/src/CohoLoader.native.tsx"]);
/** (T814) the only places the coho loader stands in for the circling fish */
const COHO_HOME = ["apps/mobile/src/App.tsx", "packages/ui-native/src/ApplyCurtain.tsx"];

describe("one spinner (T257)", () => {
  it("has sources", () => expect(files.length).toBeGreaterThan(50));

  it("no ActivityIndicator", () => {
    for (const [f, s] of files) expect([f, /\bActivityIndicator\b/.test(s)]).toEqual([f, false]);
  });

  it("the FishSpinner is drawn only through LoadingState (the delayed show)", () => {
    for (const [f, s] of files) if (!SPINNER_HOME.has(f)) expect([f, /<FishSpinner\b/.test(s)]).toEqual([f, false]);
  });

  it("the CohoLoader is drawn only through LoadingState, and asked for only by the start screen and the curtain (T814)", () => {
    for (const [f, s] of files) if (!SPINNER_HOME.has(f)) expect([f, /<CohoLoader\b/.test(s)]).toEqual([f, false]);
    const asking = files.filter(([, s]) => /<LoadingState\b[^>]*\bcoho\b/.test(s)).map(([f]) => f).sort();
    expect(asking).toEqual(COHO_HOME);
  });

  it("no hand-made turning loop", () => {
    for (const [f, s] of files)
      if (!SPINNER_HOME.has(f)) expect([f, /Animated\.loop[\s\S]{0,600}rotate|rotate[\s\S]{0,600}Animated\.loop/.test(s)]).toEqual([f, false]);
  });

  it("no waiting word as a whole label (busy draws the spinner instead)", () => {
    for (const [f, s] of files) {
      expect([f, /\?\s*"…"\s*:/.test(s)]).toEqual([f, false]);
      expect([f, /chip:\s*busy\s*\?/.test(s)]).toEqual([f, false]);
      expect([f, /refreshing\s*\?\s*SPOTS_MENU\.refreshing/.test(s)]).toEqual([f, false]);
    }
  });
});
