// @vitest-environment jsdom
/**
 * T1203 — Sheets, popups and the error / empty states
 *
 * T1201 / T1203 — THE APP'S SCREENS AT A LARGER SYSTEM TEXT SIZE, walked in Chrome (`system-text-walk.ts` does the walking; this
 * shard is one group of screens, each at 320, 360 and 412 wide and at the OS text sizes 1, 1.3 and 2.0). The phone's text scaling
 * is emulated (size and line height x min(OS scale, the Text's own cap), as `regs-text-scale.test.ts` does: react-native-web drops
 * both props); the DOM react-native-web drew is laid out by headless Chrome (`wide-measure.mjs`). The map canvas is a stand-in.
 *
 * THE GATE: 1x and 1.3x fail on a word cut by a clamp or hidden overflow, two lines of different words over each other, a text line
 * past the window's edge, sideways overflow, a one-word label broken in two, a designed clamp that shows more than two lines, and a
 * near-empty render. 2.0x fails on LOSS only (text over text, text past the edge, sideways overflow, a clamp past its design):
 * wrapping and clean cuts are fine there. The faults a screen may carry are the ALLOWED list in `system-text-walk.ts`, each with
 * its reason; nothing else is filtered.
 *
 * WHAT THE WALK DOES NOT COVER (the exclusions, in full):
 *  1. More -> Notifications: a phone-only page (not on the web); and the walkthrough card's PLACE: it is set from a measured target
 *     a static render does not give it (its words are in the page; the dimmed map under it is walked). Both: the phone check.
 *  2. The glossary's rows: cells of a virtual list, whose heights this render takes from one text size; a cell's clamp or one row
 *     crossing another there is not read (its other text, edges and words are). Its pictures show the rows growing; the phone check.
 *  3. The designed clamps are NOT skipped: the water map's part name and its step words, and the in-season inbox row's first lines,
 *     are cut to two lines at every size by design (T214 / T875 / T700); the walk asserts the clamp shows two lines at most.
 *  4. Sideways-scrolling rows (the Runs species chips, T902) are reached by scrolling: their far chips are not "past the edge".
 *  5. A page's words under a sheet's panel, and a page's words behind another layer, are not on screen and are not measured.
 *  6. A one-word broken-label check skips ids that cannot break (12+ letters or digits, with a digit: a tile version) and
 *     hyphenated words (they break at the hyphen).
 *  7. Yoga's own measuring on a phone, fonts as the phone draws them and the real OS text scaling: Chrome emulates them. The phone check.
 *
 * T1201_SHOTS=<dir> saves each laid-out page as a PNG; T1201_REPORT=<file> writes every fault found (scales 1, 1.15, 1.3, 1.5, 2);
 * T1203_SCALES=1,2 / T1203_WIDTHS=360 / T1203_ONLY=<ids> narrow a run.
 */
import "./render-setup.ts";
import { createElement as h, forwardRef } from "react";
import { vi } from "vitest";

// the walk module (system-text-walk.ts) reads the text size from here: its vi.mock()s must sit in the test file
const SCALE = vi.hoisted(() => { const s = { v: 1 }; (globalThis as unknown as { __T1203_SCALE: { v: number } }).__T1203_SCALE = s; return s; });
vi.mock("react-native", async (importOriginal) => {
  const rn = await importOriginal<typeof import("react-native")>();
  type P = React.ComponentProps<typeof rn.Text>;
  const Text = forwardRef<unknown, P>(function Text(p, ref) {
    const m = p.allowFontScaling === false ? 1 : Math.min(SCALE.v, p.maxFontSizeMultiplier ?? Infinity);
    const flat = (rn.StyleSheet.flatten(p.style) ?? {}) as { fontSize?: number; lineHeight?: number };
    const scaled = m === 1 ? p.style : [p.style, {
      ...(flat.fontSize != null ? { fontSize: flat.fontSize * m } : {}),
      ...(flat.lineHeight != null ? { lineHeight: flat.lineHeight * m } : {}),
    }];
    return h(rn.Text as never, { ...p, ref, style: scaled });
  });
  return { ...rn, Text };
});
// no WebGL in jsdom: the map itself is blank, the screen around it (rail, switch, legend, day bar) is the app's
vi.mock("@app/map", async (importOriginal) => {
  const { View, Pressable } = await import("react-native");
  const { useEffect } = await import("react");
  // a stand-in for the map canvas: a tinted card that fills its place and says its look is drawn (as the real map does), so the
  // screen around it (legend, day bar, switch) is laid out as on a phone
  const Map = (p: { lookKey?: string; onLookDrawn?: (k: string) => void; onPressFeature?: (l: string, id: number) => void }) => {
    useEffect(() => { if (p.lookKey !== undefined) p.onLookDrawn?.(p.lookKey); }, [p.lookKey, p.onLookDrawn]);
        // a one-pixel press target standing for "a tap on a stream" (no text, so it is no part of what is measured)
    return h(View, { testID: "map-canvas", style: { flex: 1, backgroundColor: (globalThis as unknown as { __T1203_CANVAS: string }).__T1203_CANVAS } },
             h(Pressable, { accessibilityRole: "button", accessibilityLabel: "tap a stream", onPress: () => { void p.onPressFeature?.("streams", 15); }, style: { width: 1, height: 1 } }));
  };
  return { ...(await importOriginal<typeof import("@app/map")>()), Map };
});
vi.mock("../packages/ui-native/src/FishSpinner", () => ({ FishSpinner: () => null }));

const { walk } = await import("./system-text-walk");
walk("sheets-states", ["sheet-not-fishing", "sheet-obstacle", "sheet-sync-error", "popup-app-update", "sheet-spot-drop", "state-error", "state-empty", "sheet-bell-news", "sheet-bell-news-detail"]);
