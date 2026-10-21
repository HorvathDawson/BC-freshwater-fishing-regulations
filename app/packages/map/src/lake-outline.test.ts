/**
 * A LAKE'S OUTLINE WEARS ITS LAKE'S COLOUR (owner 2026-10-20: "the outlines of lakes should match their reg color").
 *
 * The style already said so — `lake.outline.follow` in layers.source.json, and the generated `lake__edge` carries every
 * one of the lake's colour modes — but nothing ever SET the edge's mode: the app names `lake` in `modes`, the adapter
 * painted `lake` alone, and the edge stayed on its plain water-blue in every view. Harrison Lake was filled by its
 * status and ringed in blue while the river leaving it wore the status colour.
 *
 * So the rule lives in the shared layer both renderers use: `STYLE_META.follows` (generated) names the edge's fill, the
 * adapter contract's `setLayerMode` paints a follower with its fill's mode, and `runtimeStyle` does the same for the
 * first frame. Map.web / Map.native need nothing.
 *
 * The expressions below are written out WHOLE, from the theme's tokens, so a follower that quietly fell back to plain,
 * or read a different property, or kept the fill's own plain colour (an outline drawn in the fill's colour vanishes
 * into it), fails here.
 */
import { describe, expect, it } from "vitest";
import { baseAdapter, type MapHandle } from "./adapters/contract";
import { nativeAdapter } from "./adapters/native";
import { webAdapter } from "./adapters/web";
import { STYLE_META, colorExpression, resolveTheme, themeNames } from "./style";
import { runtimeStyle } from "./runtime-style";

const AT = { atlas: "a.pmtiles", basemap: "b.pmtiles" };

/** Every setPaint, by layer and property — the last write wins, as on a real map. */
function painter() {
  const paint: Record<string, Record<string, unknown>> = {};
  const h: MapHandle = {
    setVisibility: () => {},
    setPaint: (l, p, v) => { (paint[l] ??= {})[p] = v; },
    setFeatureState: () => {},
    clearFeatureStates: () => {},
  };
  return { h, paint };
}

/** The status colour: the day's status from feature-state, `water.unmapped` where none was pushed. */
function statusExpr(theme: string): unknown {
  const t = resolveTheme(theme);
  return ["match", ["feature-state", "status"],
          "closed", t["color.status.closed"],
          "own", t["color.status.own"],
          "base", t["color.status.base"],
          t["color.water.unmapped"]];
}

/** The stocked colour, with the lake's own plain fill ("never", missing) replaced by the outline's neutral blue. */
function stockedEdgeExpr(theme: string): unknown {
  const t = resolveTheme(theme);
  return ["match", ["feature-state", "stocked"],
          "season", t["color.stock.season"],
          "year", t["color.stock.year"],
          "recent", t["color.stock.recent"],
          "decade", t["color.stock.decade"],
          "old", t["color.stock.old"],
          "never", t["color.water.mapped"],
          t["color.water.mapped"]];
}

function surveyedEdgeExpr(theme: string): unknown {
  const t = resolveTheme(theme);
  return ["match", ["feature-state", "survey"],
          "digitised", t["color.survey.digitised"],
          "sheet", t["color.survey.sheet"],
          t["color.water.mapped"]];
}

describe("a lake's outline follows its fill", () => {
  it("the style names exactly one follower: the lake's edge follows the lake", () => {
    expect(STYLE_META.follows).toEqual({ lake__edge: "lake" });
  });

  it("status: the edge is painted from the SAME feature-state expression as the lake's status colour", () => {
    for (const theme of themeNames()) {
      expect(colorExpression("lake", "status", resolveTheme(theme)), theme).toEqual(statusExpr(theme));
      expect(colorExpression("lake__edge", "status", resolveTheme(theme)), theme).toEqual(statusExpr(theme));
    }
  });

  it("standing: the edge reads the lake's own flow ramp, whole", () => {
    for (const theme of themeNames()) {
      const t = resolveTheme(theme);
      expect(colorExpression("lake__edge", "standing", t), theme)
        .toEqual(colorExpression("lake", "standing", t));
    }
  });

  it("stocked and depth: the mode's colours, and the neutral outline where the lake has none", () => {
    for (const theme of themeNames()) {
      const t = resolveTheme(theme);
      expect(colorExpression("lake__edge", "stocked", t), theme).toEqual(stockedEdgeExpr(theme));
      expect(colorExpression("lake__edge", "surveyed", t), theme).toEqual(surveyedEdgeExpr(theme));
    }
  });

  it("plain stays the neutral water-blue outline (the fill is lake blue; an edge in it would vanish)", () => {
    for (const theme of themeNames()) {
      const t = resolveTheme(theme);
      expect(colorExpression("lake__edge", "plain", t), theme).toBe(t["color.water.mapped"]);
    }
  });

  it("an edge never wears its lake's plain fill colour, in any mode or theme", () => {
    for (const theme of themeNames()) {
      const t = resolveTheme(theme);
      for (const mode of Object.keys(STYLE_META.colorModes["lake__edge"]!))
        expect(JSON.stringify(colorExpression("lake__edge", mode, t)), `${theme} ${mode}`)
          .not.toContain(String(t["color.lake.fill"]));
    }
  });
});

describe("setting the lake's mode paints its outline (both adapters, the contract itself)", () => {
  const cases: [string, (theme: string) => unknown][] = [
    ["status", statusExpr],
    ["stocked", stockedEdgeExpr],
    ["surveyed", surveyedEdgeExpr],
    ["plain", (theme) => resolveTheme(theme)["color.water.mapped"]],
  ];
  for (const adapter of [baseAdapter("web"), webAdapter(), nativeAdapter()])
    for (const [mode, expected] of cases)
      it(`${adapter.platform}: lake → ${mode}`, () => {
        for (const theme of ["light", "dark"]) {
          const { h, paint } = painter();
          adapter.setLayerMode(h, "lake", mode, theme);
          expect(paint["lake__edge"]?.["line-color"], theme).toEqual(expected(theme));
        }
      });

  it("leaving status for plain puts the neutral outline back", () => {
    const { h, paint } = painter();
    const a = baseAdapter("web");
    a.setLayerMode(h, "lake", "status", "light");
    a.setLayerMode(h, "lake", "plain", "light");
    expect(paint["lake__edge"]?.["line-color"]).toBe(resolveTheme("light")["color.water.mapped"]);
  });

  it("the outline keeps its own width in every mode (the colour follows, nothing else)", () => {
    const { h, paint } = painter();
    const a = baseAdapter("web");
    a.setLayerMode(h, "lake", "plain", "light");
    const plainWidth = paint["lake__edge"]?.["line-width"];
    a.setLayerMode(h, "lake", "status", "light");
    expect(paint["lake__edge"]?.["line-width"]).toEqual(plainWidth);
  });
});

describe("the first frame: runtimeStyle paints the outline from the lake's mode", () => {
  const edgeColour = (theme: string, modes: Record<string, string>) =>
    (runtimeStyle(AT, theme, modes).layers.find((l) => (l as { id: string }).id === "lake__edge") as
      { paint: Record<string, unknown> }).paint["line-color"];

  it("status, stocked, depth and plain, light and dark", () => {
    for (const theme of ["light", "dark"]) {
      expect(edgeColour(theme, { lake: "status" }), theme).toEqual(statusExpr(theme));
      expect(edgeColour(theme, { lake: "stocked" }), theme).toEqual(stockedEdgeExpr(theme));
      expect(edgeColour(theme, { lake: "surveyed" }), theme).toEqual(surveyedEdgeExpr(theme));
      expect(edgeColour(theme, { lake: "plain" }), theme).toBe(resolveTheme(theme)["color.water.mapped"]);
      expect(edgeColour(theme, {}), theme).toBe(resolveTheme(theme)["color.water.mapped"]);
    }
  });

  it("an edge named outright keeps what it was given", () => {
    expect(edgeColour("light", { lake: "status", lake__edge: "plain" }))
      .toBe(resolveTheme("light")["color.water.mapped"]);
  });
});
