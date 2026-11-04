/**
 * (T342, owner 2026-10-31) PANE TEXT COPIES WITH THE PHONE'S OWN CONTROLS — one mechanism, not a prop per screen:
 * every ui-native component takes `Text` and `Pressable` from the app's own (`Text.tsx`: selectable inside a pane's
 * `<Selectable>`; `Pressable.tsx`: never under a tap target). A component that took react-native's would fall outside
 * it silently — a pane line that cannot be copied, or a button whose long-press selects its label.
 */
import { describe, expect, it } from "vitest";
import { files, read } from "./consistency";

const OWN = new Set(["packages/ui-native/src/Text.tsx", "packages/ui-native/src/Pressable.tsx"]);
const IMPORT = /import\s*\{([^}]*)\}\s*from\s*"react-native"/g;

/** The files that import `Text` or `Pressable` (the components, not types) from react-native. */
export function rnTextOrPressable(src: string): string[] {
  const out: string[] = [];
  for (const m of src.matchAll(IMPORT))
    for (const n of m[1]!.split(",").map((x) => x.trim()))
      if (n === "Text" || n === "Pressable" || /^(Text|Pressable) as /.test(n)) out.push(n);
  return out;
}

describe("T342: one selectable-text mechanism", () => {
  it("no ui-native component takes Text or Pressable from react-native (only the two wrappers do)", () => {
    const bad = files(["packages/ui-native/src"]).filter((f) => f.endsWith(".tsx") && !OWN.has(f))
      .flatMap((f) => rnTextOrPressable(read(f)).map((n) => `${f}: ${n}`));
    expect(bad).toEqual([]);
  });

  it("the panes open it, the map closes it", () => {
    for (const f of ["WaterFrame.tsx", "Sheet.tsx", "menu/MenuPage.tsx", "SpotScreen.tsx"])
      expect([f, /<Selectable>/.test(read(`packages/ui-native/src/${f}`))]).toEqual([f, true]);
    expect(/<Selectable on=\{false\}>\{children\}<\/Selectable>/.test(read("packages/ui-native/src/WaterViews.tsx"))).toBe(true);
  });

  it("MUTATION PIN: a planted react-native Text or Pressable is found; a type import and View are not", () => {
    expect(rnTextOrPressable(`import { Text, View } from "react-native";`)).toEqual(["Text"]);
    expect(rnTextOrPressable(`import { Pressable as P, type PressableProps } from "react-native";`)).toEqual(["Pressable as P"]);
    expect(rnTextOrPressable(`import { View, type TextStyle } from "react-native";`)).toEqual([]);
  });
});
