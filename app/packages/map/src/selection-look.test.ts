/**
 * (T735, owner 2026-11-16: look C) THE PART YOU ARE ON, in dark: a thin dark casing so the status colour carries, a soft
 * light glow outside it, and the in-season casing on the water's other parts at 0.35. Light (and cvd) are what they were:
 * the 2.5 px highlight / attention casing and no glow layer at all. All of it rides palette tokens, never a theme's name.
 * (T1024) Every opacity here is inside `casedOnly`: 0 on a stub line from its polygon's zoom (tools/stream-polygon-ring.test.ts).
 */
import { describe, expect, it } from "vitest";
import { baseAdapter, type MapHandle } from "./adapters/contract";
import { casedOnly, casingPaint, glowPaint, SELECTION_CASING, SELECTION_GLOW } from "./paint";
import { runtimeStyle } from "./runtime-style";
import { PMTILES } from "./tile-access";
import { resolveTheme } from "./style";

const FS = (k: string) => ["boolean", ["feature-state", k], false];
const ON = ["any", FS("selected"), FS("part")];
const at = (theme: string) => runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles" } as never, theme, {}, PMTILES).layers as unknown as
  { id: string; paint: Record<string, unknown> }[];

describe("(T735) look C: dark only, through tokens", () => {
  it("dark casing: 1.5 px each side, the ground's colour on the part, attention on the others, in-season others at 0.35", () => {
    const t = resolveTheme("dark");
    expect([t["width.selection.casing"], t["flag.selection.edge"], t["color.selection.edge"], t["opacity.selection.glow"],
            t["opacity.selection.casing.others"]]).toEqual([1.5, 1, t["color.ui.page"], 0.4, 0.35]);
    const p = casingPaint(2, t);
    expect(p["line-color"]).toEqual(["case", ON, t["color.ui.page"], t["color.ui.attention"]]);
    expect(p["line-opacity"]).toEqual(casedOnly(["case", ON, 1, ["all", FS("inseason"), FS("water")], 0.35,
      ["case", FS("selected"), 1, FS("part"), 1, FS("inseason"), 1, 0]]));
    expect(p["line-width"]).toEqual(["+", 2, 3]);                      // 2 + 2 * 1.5
  });

  it("dark glow: attention, blur 6, width = stream + 2 * 1.5 + 12, 0.4 on the part and 0 elsewhere", () => {
    const t = resolveTheme("dark");
    const g = glowPaint(2, t);
    expect(g["line-color"]).toBe(t["color.ui.attention"]);
    expect(g["line-blur"]).toBe(6);
    expect(g["line-opacity"]).toEqual(casedOnly(["case", ON, 0.4, 0]));
    expect(g["line-width"]).toEqual(["+", 2, 15]);                      // 2 + 2 * 1.5 + 12
  });

  it("dark style: the glow layer sits directly under the casing", () => {
    const ids = at("dark").map((l) => l.id);
    expect(ids.indexOf(SELECTION_GLOW)).toBeGreaterThan(-1);
    expect(ids.indexOf(SELECTION_GLOW) + 1).toBe(ids.indexOf(SELECTION_CASING));
  });

  it("light and cvd: no glow layer, and the casing is exactly today's", () => {
    for (const theme of ["light", "cvd"]) {
      const t = resolveTheme(theme);
      expect([t["width.selection.casing"], t["flag.selection.edge"], t["opacity.selection.glow"],
              t["opacity.selection.casing.others"]]).toEqual([2.5, 0, 0, 1]);
      const layers = at(theme);
      expect(layers.some((l) => l.id === SELECTION_GLOW)).toBe(false);
      const p = layers.find((l) => l.id === SELECTION_CASING)!.paint;
      expect(p["line-color"]).toEqual(["case", FS("inseason"), t["color.ui.attention"], t["color.highlight"]]);
      expect(p["line-opacity"]).toEqual(casedOnly(["case", FS("selected"), 1, FS("part"), 1, FS("inseason"), 1, 0]));
      expect(casingPaint(2, t)["line-width"]).toEqual(["+", 2, 5]);             // 2 + 2 * 2.5
    }
  });

  it("the adapter paints the glow at 0.4 on dark and at 0 on light, so a live map that changes theme follows", () => {
    const calls: Record<string, unknown> = {};
    const h: MapHandle = { setVisibility: () => {}, setPaint: (l, p, v) => { if (l === SELECTION_GLOW) calls[p] = v; },
                           setFeatureState: () => {}, clearFeatureStates: () => {} };
    const a = baseAdapter("native");
    a.setLayerMode(h, "stream", "plain", "dark");
    expect(calls["line-opacity"]).toEqual(casedOnly(["case", ON, 0.4, 0]));
    a.setLayerMode(h, "stream", "plain", "light");
    expect(calls["line-opacity"]).toEqual(casedOnly(["case", ON, 0, 0]));
  });
});
