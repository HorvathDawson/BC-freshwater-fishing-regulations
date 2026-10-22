/**
 * BACK, EVERYWHERE (owner 2026-10-21: "make sure android back button works for entire app where a user expects it
 * to") — the parts a render test cannot reach: ONE back mechanism, and no surface that back would go under.
 *
 *   one mechanism   Android's `BackHandler` is named only by apps/mobile/src/systemBack.native.ts; the History API
 *                   only by systemBack.web.ts; a Modal's `onRequestClose` only by `<Sheet>` (where it is the one back).
 *                   Nothing else listens for a back of its own — two listeners is a double back.
 *   every surface   a file that draws something back must close first — a Modal, a dialog (`role="dialog"`,
 *                   `aria-modal`, `accessibilityViewIsModal`), a component taking `onClose`/`onDismiss`/`onCancel`, or
 *                   one named like a sheet, menu, drawer, dialog, popover, popup, overlay, viewer, lightbox, picker,
 *                   pop, confirm or prompt —
 *                   registers with the one back: it calls `useBackLayer(` or draws its surface in a `<Sheet>` (which
 *                   registers for it). A NEW SURFACE THAT DOES NEITHER FAILS HERE; see app/HANDOFF-ui.md "Back,
 *                   everywhere" for how to register one.
 *
 * The scan is a net, not a proof: what a back DOES on each surface is pinned by the render tests
 * (apps/mobile/src/back.test.tsx, through the app's own BackHandler wiring).
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const APP = fileURLToPath(new URL("..", import.meta.url));
const ROOTS = ["apps/mobile/src", "apps/web/src", "packages/ui/src", "packages/ui-native/src", "packages/map/src"];

function sources(dir: string, out: string[] = []): string[] {
  let names: string[];
  try { names = readdirSync(dir); } catch { return out; }
  for (const f of names) {
    if (f === "node_modules" || f === "dist" || f.startsWith(".")) continue;
    const p = join(dir, f);
    if (statSync(p).isDirectory()) sources(p, out);
    else if (/\.(ts|tsx)$/.test(f) && !/\.test\.tsx?$/.test(f) && !/\.d\.ts$/.test(f)) out.push(p);
  }
  return out;
}
/** Comments out (a doc comment may name `BackHandler`); strings kept. */
const code = (src: string) => src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:"'`])\/\/.*$/gm, "$1");
const files = () => ROOTS.flatMap((r) => sources(join(APP, r))).map((p) => relative(APP, p)).sort();
const read = (p: string) => code(readFileSync(join(APP, p), "utf8"));
const where = (re: RegExp) => files().filter((p) => re.test(read(p)));
/** The files that draw: components (.tsx). A hook (`useSheetDrag(onClose)`) draws nothing back could close. */
const drawing = () => files().filter((p) => p.endsWith(".tsx"));

/** Why a file draws something back must close first, or null. */
export function surfaceReason(src: string): string | null {
  if (/<Modal\b/.test(src) || /import\s*\{[^}]*\bModal\b[^}]*\}\s*from\s*["']react-native["']/.test(src)) return "a Modal";
  if (/role=["'](?:alert)?dialog["']|aria-modal|accessibilityViewIsModal/.test(src)) return "a dialog";
  const prop = /\b(on(?:Close|Dismiss|Cancel))\??\s*:/.exec(src);
  if (prop) return `takes ${prop[1]}`;
  for (const m of src.matchAll(/(?:function|const)\s+([A-Z]\w*)/g))
    if (/(?:Sheet|Menu|Drawer|Dialog|Popover|Popup|Overlay|Viewer|Lightbox|Picker|Pop|Confirm|Prompt)$/.test(m[1]!)) return `draws ${m[1]}`;
  return null;
}
/** Does it register with the one back? */
export const registers = (src: string) => /\buseBackLayer\s*\(/.test(src) || /<Sheet\b/.test(src);

/**
 * Surfaces that need no back layer, each with its reason. Empty today; an entry here is an owner decision, said in
 * app/HANDOFF-ui.md's table.
 */
const NOT_A_BACK_LAYER: Record<string, string> = {};

describe("back, everywhere: one mechanism", () => {
  it("Android's BackHandler is named only by the app's systemBack.native.ts", () => {
    expect(where(/\bBackHandler\b/)).toEqual(["apps/mobile/src/systemBack.native.ts"]);
  });

  it("the browser's history is touched only by the app's systemBack.web.ts", () => {
    expect(where(/\bpopstate\b|\bpushState\b|\breplaceState\b|history\.(?:go|back|forward)\s*\(/))
      .toEqual(["apps/mobile/src/systemBack.web.ts"]);
  });

  it("a Modal's own back (onRequestClose) is wired only by <Sheet>, to the one back", () => {
    expect(where(/\bonRequestClose\b/)).toEqual(["packages/ui-native/src/Sheet.tsx"]);
  });
});

describe("back, everywhere: every surface registers a back layer", () => {
  it("every file that draws a sheet, menu, dialog or overlay registers it (useBackLayer, or inside a <Sheet>)", () => {
    const missing = drawing().flatMap((p) => {
      const src = read(p);
      const why = surfaceReason(src);
      return why && !registers(src) && !(p in NOT_A_BACK_LAYER) ? [`${p}: ${why}, no useBackLayer`] : [];
    });
    expect(missing).toEqual([]);
  });

  it("the surfaces found today, so the net is seen to catch something", () => {
    const found = drawing().flatMap((p) => { const w = surfaceReason(read(p)); return w ? [`${p}: ${w}`] : []; });
    expect(found).toEqual([
      "packages/ui-native/src/LayersSheet.tsx: takes onClose",
      "packages/ui-native/src/Sheet.tsx: a Modal",
      "packages/ui-native/src/SpotCapture.tsx: takes onCancel",
      "packages/ui-native/src/SpotScreen.tsx: takes onCancel",
      "packages/ui-native/src/regs/DaySheet.tsx: takes onClose",
      "packages/ui-native/src/water/WaterMapView.tsx: a dialog",
    ]);
  });

  it("MUTATION PIN: an unregistered new surface is caught; registering it clears it", () => {
    const menu = `
      export function SpotsMenu({ open, onClose }: { open: boolean; onClose: () => void }) {
        return open ? <View role="dialog"><Pressable onPress={onClose} /></View> : null;
      }`;
    const sheet = "export function GlossarySheet({ term }: { term: string }) { return <View>{term}</View>; }";
    const pop = "const LongPressPop = ({ at }: { at: number }) => <View />;";
    // the map's long-press "add a spot here?" card, by whatever name: it can be cancelled, so back must cancel it
    const card = "export function AddHereCard({ at, onCancel }: { at: number; onCancel: () => void }) { return <View />; }";
    const hamburger = "export function AppMenu({ open }: { open: boolean }) { return open ? <View /> : null; }";
    expect([menu, sheet, pop, card, hamburger].map((s) => [surfaceReason(s), registers(s)])).toEqual([
      ["a dialog", false], ["draws GlossarySheet", false], ["draws LongPressPop", false],
      ["takes onCancel", false], ["draws AppMenu", false]]);
    const fixed = menu.replace("return open", "useBackLayer(open, onClose);\n        return open");
    expect([surfaceReason(fixed), registers(fixed)]).toEqual(["a dialog", true]);
    expect(registers('<Sheet open={open} onClose={onClose} title="Glossary" palette={palette}>')).toBe(true);
  });
});
