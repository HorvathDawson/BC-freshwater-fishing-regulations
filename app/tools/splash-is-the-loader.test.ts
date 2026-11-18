/**
 * (T843, owner 2026-11-17) THE NATIVE SPLASH IS THE BOOT LOADER'S FIRST FRAME, AT ITS SIZE. "the old loading icon shows
 * up before the new swimming fish one when the app is opened": T814 moved the boot screen to the coho and the splash
 * stayed the circling fish. `pnpm splash` holds the PNGs to the coho sheet; this holds the sizes, so the splash and the
 * boot screen's coho cannot drift apart again:
 *   - the boot screen draws the coho (`LoadingState … coho`) at `COHO_WIDTH`;
 *   - the splash plugin's `imageWidth` (app.config.js) is that width;
 *   - the splash PNGs are that width at 3x, the coho frame's aspect.
 * Reads source off disk (tools/: the boundaries gate keeps node:fs out of the packages).
 */
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { COHO_WIDTH, cohoHeight } from "../packages/ui-native/src/coho";

const at = (p: string) => fileURLToPath(new URL(`../${p}`, import.meta.url));
const pngSize = (p: string) => { const b = readFileSync(at(p)); return [b.readUInt32BE(16), b.readUInt32BE(20)]; };

describe("the native splash is the boot loader's frame 0 (T843)", () => {
  it("the boot screen draws the coho at COHO_WIDTH", () => {
    const app = readFileSync(at("apps/mobile/src/App.tsx"), "utf8");
    expect(app).toMatch(/<LoadingState[^>]*size=\{COHO_WIDTH\}[^>]*\bcoho\b/);
  });
  it("the splash plugin's imageWidth is COHO_WIDTH", () => {
    const cfg = readFileSync(at("apps/mobile/app.config.js"), "utf8");
    expect(Number(/imageWidth:\s*(\d+)/.exec(cfg)?.[1])).toBe(COHO_WIDTH);
  });
  it("both splash pictures are COHO_WIDTH x the coho's height, at 3x", () => {
    for (const t of ["light", "dark"]) {
      expect(pngSize(`apps/mobile/assets/splash/splash-${t}.png`)).toEqual([COHO_WIDTH * 3, cohoHeight(COHO_WIDTH) * 3]);
    }
  });
});
