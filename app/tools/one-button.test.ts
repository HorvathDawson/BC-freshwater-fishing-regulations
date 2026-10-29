/**
 * (T257, owner 2026-10-29) THE SHARED PIECES STAY SHARED. The consistency sweep extracted the copies the screens had typed
 * out into `Button` (solid / ghost / danger / small / link, busy, icon), `ErrorState` and `LoadingState`, and squared or
 * `round()`ed every corner; this keeps them from being typed out again:
 *   - no literal corner radius in the phone components (`palette.r.*`, `RADIUS.*` or `round(side)` for a circle);
 *   - no small action pill typed out (Button kind="small");
 *   - no error line typed out (an `alert` in `closed`: ErrorState) and no "Try again" link typed out (Button kind="link");
 *   - a ratchet on solid fills: `palette.onAccent` words (a filled accent control) only in the files that already draw
 *     one for a reason of their own (selected segments, the day grid, badges); a new screen uses Button.
 * Reads source off disk (tools/: the boundaries gate keeps node:fs out of the component package).
 */
import { describe, expect, it } from "vitest";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const SRC = fileURLToPath(new URL("../packages/ui-native/src", import.meta.url));
const files: [string, string][] = [];
const walk = (d: string) => {
  for (const f of readdirSync(d)) {
    const p = join(d, f);
    if (statSync(p).isDirectory()) { walk(p); continue; }
    if (!/\.tsx?$/.test(f) || /\.test\.tsx?$/.test(f) || f.startsWith("_") || f.endsWith(".d.ts")) continue;
    files.push([relative(SRC, p), readFileSync(p, "utf8").replace(/\/\*[\s\S]*?\*\//g, "").replace(/^\s*\/\/.*$/gm, "")]);
  }
};
walk(SRC);

/** the files that fill a control with the accent for a reason of their own (a chosen segment, a day, a badge, a mark) */
const ON_ACCENT = new Set(["Button.tsx", "CornerStrip.tsx", "DayCalendar.tsx", "Sheet.tsx", "LayersSheet.tsx", "MapRail.tsx",
  "SpotCapture.tsx", "TopBar.tsx", "shell/MapLegend.tsx", "regs/SalmonCard.tsx", "menu/ReportPage.tsx", "water/WaterMapView.tsx",
  // the Runs tab's play button (run-timing v2 lane, in flight 2026-10-29)
  "RunsControls.tsx"]);
/** in flight in another lane when this landed (2026-10-29): its circles and track are to become round() / palette.r.* */
const RADIUS_PENDING = new Set(["RunsControls.tsx", "RunsSheet.tsx"]);

describe("one button, one error line, square corners (T257)", () => {
  it("has sources", () => expect(files.length).toBeGreaterThan(50));

  it("no literal corner radius (a circle is round(side))", () => {
    for (const [f, s] of files) if (!f.startsWith("FishSpinner") && !RADIUS_PENDING.has(f))
      expect([f, /borderRadius:\s*["'\d]/.test(s)]).toEqual([f, false]);
  });

  it("no small action pill typed out", () => {
    for (const [f, s] of files) if (f !== "Button.tsx")
      expect([f, /<Pressable[\s\S]{0,500}paddingVertical: 8, paddingHorizontal: 14[\s\S]{0,200}borderColor: palette\.line2/.test(s)]).toEqual([f, false]);
  });

  it("no error line or retry link typed out", () => {
    for (const [f, s] of files) {
      if (f !== "ErrorState.tsx") expect([f, /accessibilityRole="alert"[^\n]*palette\.closed/.test(s)]).toEqual([f, false]);
      if (f !== "Button.tsx") expect([f, />\{NET_WORDS\.part\.retry\}</.test(s)]).toEqual([f, false]);
    }
  });

  it("filled accent controls only where they already were (a new screen uses Button)", () => {
    const now = files.filter(([, s]) => /palette\.onAccent/.test(s)).map(([f]) => f).sort();
    expect(now.filter((f) => !ON_ACCENT.has(f))).toEqual([]);
  });
});
