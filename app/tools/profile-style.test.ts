/**
 * THE `profile` COLOUR MODE in the style contract (colour-latency AP; the verifier's condition 3).
 *
 * The day lives in the paint as one match over profile labels, built from @app/core `dayProfiles` and nothing else.
 * Pinned whole here, in every theme, for the layers the Map tab colours and for the lake's outline (which follows its
 * lake, 3f1a824a) — and run through MapLibre's own validator with real label sets, because a `match` whose labels mix
 * types or arrive as an empty list fails to PARSE and the layer is dropped with one console line.
 */
import { describe, expect, it } from "vitest";
import { validateStyleMin } from "@maplibre/maplibre-gl-style-spec";
import type { DayProfiles } from "@app/core";
import { baseAdapter, paintFor, type MapHandle } from "../packages/map/src/adapters/contract";
import { STYLE_META, colorExpression, resolveTheme, themeNames } from "../packages/map/src/style";
import { runtimeStyle } from "../packages/map/src/runtime-style";

const DAY: DayProfiles = { closed: ["p1"], own: ["p2", "p7"], base: ["b", "p0"], none: ["p3"] };
const NO_CLOSURES: DayProfiles = { closed: [], own: ["p2"], base: ["b", "p0", "p1"], none: [] };

/** The expression the mode must produce, written out from the theme's tokens. */
const expected = (theme: string, d: DayProfiles): unknown[] => {
  const t = resolveTheme(theme);
  const out: unknown[] = ["match", ["coalesce", ["feature-state", "p"], ""]];
  if (d.closed.length) out.push([...d.closed], t["color.status.closed"]);
  if (d.own.length) out.push([...d.own], t["color.status.own"]);
  if (d.base.length) out.push([...d.base], t["color.status.base"]);
  out.push(t["color.water.unmapped"]);
  return out;
};

describe("the profile colour mode", () => {
  it("is declared on every layer the status colours, and on its followers (lake outline, flow arrows), over water_status", () => {
    const has = Object.entries(STYLE_META.colorModes).filter(([, m]) => m["profile"]).map(([id]) => id).sort();
    expect(has).toEqual(["lake", "lake__edge", "lake_flowing", "stream", "stream__arrows", "wetland_flowing"]);
    for (const id of has)
      expect(JSON.stringify({ ...STYLE_META.colorModes[id]!["profile"], $comment: undefined }), id).toBe(JSON.stringify({
        label: "Status for the date", scale: "profile", property: "p", enum: "water_status",
        categories: { closed: { token: "color.status.closed" }, own: { token: "color.status.own" },
                      base: { token: "color.status.base" } },
        missing: { token: "color.water.unmapped" }, data: { provider: "status", field: "profile" } }));
  });

  it("paints the day as one match over its labels — closed, own, base; anything unplaced is unmapped grey", () => {
    for (const theme of themeNames())
      for (const layer of ["stream", "stream__arrows", "lake", "lake__edge", "lake_flowing", "wetland_flowing"]) {
        expect(colorExpression(layer, "profile", resolveTheme(theme), DAY), `${theme} ${layer}`)
          .toEqual(expected(theme, DAY));
        expect(colorExpression(layer, "profile", resolveTheme(theme), NO_CLOSURES), `${theme} ${layer}`)
          .toEqual(expected(theme, NO_CLOSURES));
      }
    const t = resolveTheme("light");
    expect(colorExpression("stream", "profile", t, DAY)).toEqual(
      ["match", ["coalesce", ["feature-state", "p"], ""], ["p1"], t["color.status.closed"],
       ["p2", "p7"], t["color.status.own"], ["b", "p0"], t["color.status.base"], t["color.water.unmapped"]]);
  });

  it("with no day installed paints exactly the plain mode — width, opacity, pattern and all", () => {
    for (const theme of themeNames())
      for (const layer of ["stream", "lake", "lake__edge", "lake_flowing", "wetland_flowing"])
        expect(paintFor(layer, "profile", resolveTheme(theme)), `${theme} ${layer}`)
          .toEqual(paintFor(layer, "plain", resolveTheme(theme)));
  });

  it("keeps the selected highlight on top of the day, and the line's own width", () => {
    const t = resolveTheme("light");
    const p = paintFor("stream", "profile", t, DAY);
    expect(p["line-color"]).toEqual(
      ["case", ["boolean", ["feature-state", "selected"], false], t["color.highlight"], expected("light", DAY)]);
    expect(p["line-width"]).toEqual(paintFor("stream", "plain", t)["line-width"]);
  });

  it("is accepted by MapLibre's validator with real label sets, in every theme", () => {
    const problems: string[] = [];
    for (const theme of themeNames())
      for (const day of [DAY, NO_CLOSURES]) {
        const paint: Record<string, Record<string, unknown>> = {};
        const h: MapHandle = { setVisibility: () => {}, setPaint: (l, p, v) => { (paint[l] ??= {})[p] = v; },
                               setFeatureState: () => {}, clearFeatureStates: () => {} };
        const a = baseAdapter("web");
        const modes = { stream: "profile", lake: "profile", lake_flowing: "profile", wetland_flowing: "profile" };
        for (const [l, m] of Object.entries(modes)) a.setLayerMode(h, l, m, theme);
        a.setStatusDay(h, day, theme);
        const style = runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles" }, theme, modes) as
          unknown as { layers: { id: string; paint?: Record<string, unknown> }[] };
        let patched = 0;
        for (const l of style.layers) if (paint[l.id]) { l.paint = { ...l.paint, ...paint[l.id] }; patched++; }
        expect(patched).toBe(6);   // the four, the lake's edge and the stream's flow arrows (followers)
        for (const e of validateStyleMin(style as never)) problems.push(`${theme}: ${e.message}`);
      }
    expect(problems).toEqual([]);
  });
});
