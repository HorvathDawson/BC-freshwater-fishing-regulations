/**
 * (T1024, staging report 3444007425e9d3bd: "Weird highlighting on the lake here", Gravel Slough) A STREAM'S POLYGON — a
 * slough, a river's wide reach (`lake_flowing` / `wetland_flowing`, tile `water` = "stream") — is NEVER RINGED by the
 * selection. Its route is in the `stream` layer under the same section id, so the casing already lights it; the ring drew a
 * second outline round the casing and a box round each section's piece of the polygon. A lake's and a wetland's polygons
 * keep their ring. Checked by EVALUATING each ring's filter with MapLibre's own evaluator, in every theme, against the
 * fills that draw the same polygons.
 */
import { describe, expect, it } from "vitest";
import { featureFilter } from "@maplibre/maplibre-gl-style-spec";
import { runtimeStyle } from "../packages/map/src/runtime-style";
import { SELECTION_CASING, SELECTION_RINGS } from "../packages/map/src/paint";

type Layer = { id: string; type: string; "source-layer"?: string; filter?: unknown };
const THEMES = ["light", "dark", "cvd"] as const;
/** the registry's water kinds a polygon tile carries, and one with none */
const KINDS = ["lake", "wetland", "stream", "reservoir", undefined] as const;

const layersOf = (theme: string) =>
  (runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles", outside: "o.geojson" } as never, theme, {}) as { layers: Layer[] }).layers;
const layer = (layers: Layer[], id: string) => {
  const l = layers.find((x) => x.id === id);
  if (!l) throw new Error(`no layer ${id} in the runtime style`);
  return l;
};
/** does `l`'s filter take a polygon whose `water` is `kind`? (no filter: every feature) */
const takes = (l: Layer, kind: string | undefined) => {
  const f = featureFilter(l.filter as never, `layers[${l.id}].filter`);
  return f.filter({ zoom: 14 } as never, { type: 3, properties: kind === undefined ? {} : { water: kind } } as never);
};

describe("(T1024) a stream's polygon is lit by its casing, never ringed", () => {
  for (const theme of THEMES) {
    it(`${theme}: each ring goes round exactly the polygons its standing-water fill draws`, () => {
      const ls = layersOf(theme);
      for (const [ring, fill, flowing] of [[SELECTION_RINGS.lake, "lake", "lake_flowing"],
                                           [SELECTION_RINGS.wetland, "wetland", "wetland_flowing"]] as const) {
        const r = layer(ls, ring), f = layer(ls, fill), s = layer(ls, flowing);
        expect([r["source-layer"], f["source-layer"], s["source-layer"]]).toEqual([fill, fill, fill]);
        expect(KINDS.map((k) => [k ?? "(none)", takes(r, k)])).toEqual(KINDS.map((k) => [k ?? "(none)", takes(f, k)]));
        // and none of the stream's: the flowing fill's polygons are out of the ring, every one
        expect(KINDS.filter((k) => takes(s, k)).map((k) => [k, takes(r, k)])).toEqual([["stream", false]]);
      }
    });
  }

  it("the slough's route is still lit: the casing on `stream` takes every line", () => {
    const c = layer(layersOf("light"), SELECTION_CASING);
    expect([c["source-layer"], c.filter ?? null, takes(c, "stream")]).toEqual(["stream", null, true]);
  });
});
