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
import { baseAdapter, type MapHandle } from "../packages/map/src/adapters/contract";
import { paintFor } from "../packages/map/src/paint";
import { NO_VALUES, STYLE_META, colorExpression, resolveTheme, themeNames, type ChannelValues }
  from "../packages/map/src/style";
import { runtimeStyle } from "../packages/map/src/runtime-style";

const DAY: DayProfiles = { closed: ["p1"], own: ["p2", "p7"], base: ["b", "p0"], no_access: ["n"], none: ["p3"] };
const NO_CLOSURES: DayProfiles = { closed: [], own: ["p2"], base: ["b", "p0", "p1"], no_access: ["n"], none: [] };
/** The channel values with only a day installed. */
const day = (d: DayProfiles): ChannelValues => ({ ...NO_VALUES, status: d });

/** The expression the mode must produce, written out from the theme's tokens. */
const expected = (theme: string, d: DayProfiles): unknown[] => {
  const t = resolveTheme(theme);
  const out: unknown[] = ["match", ["coalesce", ["feature-state", "p"], ""]];
  if (d.closed.length) out.push([...d.closed], t["color.status.closed"]);
  if (d.own.length) out.push([...d.own], t["color.status.own"]);
  if (d.base.length) out.push([...d.base], t["color.status.base"]);
  if (d.no_access.length) out.push([...d.no_access], t["color.status.no_access"]);
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
                      base: { token: "color.status.base" }, no_access: { token: "color.status.no_access" } },
        missing: { token: "color.water.unmapped" }, data: { provider: "status", field: "profile" } }));
  });

  it("paints the day as one match over its labels — closed, own, base, no access (T54); anything unplaced is unmapped grey", () => {
    for (const theme of themeNames())
      for (const layer of ["stream", "stream__arrows", "lake", "lake__edge", "lake_flowing", "wetland_flowing"]) {
        expect(colorExpression(layer, "profile", resolveTheme(theme), day(DAY)), `${theme} ${layer}`)
          .toEqual(expected(theme, DAY));
        expect(colorExpression(layer, "profile", resolveTheme(theme), day(NO_CLOSURES)), `${theme} ${layer}`)
          .toEqual(expected(theme, NO_CLOSURES));
      }
    const t = resolveTheme("light");
    expect(colorExpression("stream", "profile", t, day(DAY))).toEqual(
      ["match", ["coalesce", ["feature-state", "p"], ""], ["p1"], t["color.status.closed"],
       ["p2", "p7"], t["color.status.own"], ["b", "p0"], t["color.status.base"], ["n"], t["color.status.no_access"],
       t["color.water.unmapped"]]);
  });

  it("with no day installed paints exactly the plain mode — width, opacity, pattern and all", () => {
    for (const theme of themeNames())
      for (const layer of ["stream", "lake", "lake__edge", "lake_flowing", "wetland_flowing"])
        expect(paintFor(layer, "profile", resolveTheme(theme)), `${theme} ${layer}`)
          .toEqual(paintFor(layer, "plain", resolveTheme(theme)));
  });

  it("keeps the DAY'S colour on a selected stream (owner 2026-10-21: outlined, never recoloured), and the line's own width", () => {
    const t = resolveTheme("light");
    const p = paintFor("stream", "profile", t, day(DAY));
    expect(p["line-color"]).toEqual(expected("light", DAY));
    expect(p["line-width"]).toEqual(paintFor("stream", "plain", t)["line-width"]);
  });

  it("is accepted by MapLibre's validator with real label sets, in every theme", () => {
    const problems: string[] = [];
    for (const theme of themeNames())
      for (const d of [DAY, NO_CLOSURES]) {
        const paint: Record<string, Record<string, unknown>> = {};
        const h: MapHandle = { setVisibility: () => {}, setPaint: (l, p, v) => { (paint[l] ??= {})[p] = v; },
                               setFeatureState: () => {}, clearFeatureStates: () => {} };
        const a = baseAdapter("web");
        const modes = { stream: "profile", lake: "profile", lake_flowing: "profile", wetland_flowing: "profile" };
        for (const [l, m] of Object.entries(modes)) a.setLayerMode(h, l, m, theme);
        a.setChannel(h, "status", d, theme);
        const style = runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles" }, theme, modes) as
          unknown as { layers: { id: string; paint?: Record<string, unknown> }[] };
        let patched = 0;
        for (const l of style.layers) if (paint[l.id]) { l.paint = { ...l.paint, ...paint[l.id] }; patched++; }
        // the four, the lake's edge, the stream's flow arrows (followers), its "no baseline" dash (off), the no-access dots
        // (T165, on with the day's `no_access` labels) and the selection's outline (the stream's casing, the lake and
        // wetland rings), T175's water copy (the lake's fill, the stream's casing and line), and T213's gear marks (the
        // stream's two edges, the lake's two rings), T199's passing-run dash (`stream-runs-pass`, off outside `runs`)
        // — all validated with the real paint
        expect(patched).toBe(19);
        for (const e of validateStyleMin(style as never)) problems.push(`${theme}: ${e.message}`);
      }
    expect(problems).toEqual([]);
  });
});
