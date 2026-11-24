/**
 * (T1024, staging report 3444007425e9d3bd: "Weird highlighting on the lake here", Gravel Slough) A STREAM'S POLYGON — a
 * slough, a river's wide reach (`lake_flowing` / `wetland_flowing`, tile `water` = "stream") — is ringed by the selection
 * ONLY where its route line is not drawn. Its line is in the `stream` layer under the same section id from zoom `lz` (a
 * tile property; none when it never gets one, pipeline tiles/stream_polygon_lines.py), and from there the line's casing
 * lights it: a ring as well drew a second outline round the casing and a box round every section's piece of the polygon.
 * Below `lz` (Gravel Slough's polygon z10, line z12; Gun Creek z8 / z13) and with no `lz` at all (the Okanagan River
 * Oxbows) the ring is all the selection has, and it stays. A lake's and a wetland's polygons are ringed as before.
 *
 * Checked by EVALUATING the real runtime style's ring opacity (MapLibre's own evaluator) per tile zoom, every theme, for
 * the browse selection (`selected`), a water map's part (`part`) and an in-season outline (`inseason`) — all three are
 * the one `OUTLINE_SHOWN` — and the casing that takes over on the line.
 *
 * (The #81 review) A polygon whose route is a STUB has no `lz` and is ringed wherever it is drawn, so its stub's casing
 * drew a second outline inside the ring. The stub line carries `pz`, its polygon's first zoom; the casing (and look C's
 * glow) is opacity 0 on it from its `pz` — checked by EVALUATING their opacity per tile zoom, as the ring's.
 */
import { describe, expect, it } from "vitest";
import { createPropertyExpression, latest } from "@maplibre/maplibre-gl-style-spec";
import { runtimeStyle } from "../packages/map/src/runtime-style";
import { SELECTION_CASING, SELECTION_GLOW, SELECTION_RINGS } from "../packages/map/src/paint";

type Layer = { id: string; type: string; "source-layer"?: string; filter?: unknown; paint?: Record<string, unknown> };
const THEMES = ["light", "dark", "cvd"] as const;
const STATES = [{ selected: true }, { part: true }, { inseason: true }] as const;

const layersOf = (theme: string) =>
  (runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles", outside: "o.geojson" } as never, theme, {}) as { layers: Layer[] }).layers;
const layer = (layers: Layer[], id: string) => {
  const l = layers.find((x) => x.id === id);
  if (!l) throw new Error(`no layer ${id} in the runtime style`);
  return l;
};
const spec = (latest as unknown as Record<string, Record<string, unknown>>)["paint_line"]!["line-opacity"];
/** `layer`'s line-opacity at a zoom, for a feature with `props` in feature-state `state` */
function opacity(l: Layer, zoom: number, props: Record<string, unknown>, state: Record<string, unknown>): number {
  const e = createPropertyExpression(l.paint?.["line-opacity"], "line-opacity" as never, spec as never);
  if (e.result !== "success") throw new Error(`${l.id} line-opacity does not parse: ${JSON.stringify(e.value)}`);
  return e.value.evaluate({ zoom } as never, { type: 2, properties: props } as never, state as never) as number;
}
const ZOOMS = [4, 8, 9, 10, 11, 12, 13, 14, 16];
/** each zoom's opacity, as [zoom, value] (the failure names the zoom) */
const along = (l: Layer, props: Record<string, unknown>, state: Record<string, unknown>) =>
  ZOOMS.map((z) => [z, opacity(l, z, props, state)]);
const shown = (from: number, to = 99) => ZOOMS.map((z) => [z, z >= from && z < to ? 1 : 0]);

describe("(T1024) a stream's polygon is ringed only where its line is not drawn", () => {
  for (const theme of THEMES) {
    const ls = layersOf(theme);
    for (const [ring, kind] of [[SELECTION_RINGS.lake, "lake"], [SELECTION_RINGS.wetland, "wetland"]] as const) {
      const r = layer(ls, ring);
      it(`${theme} ${ring}: on the ${kind} source-layer, every polygon (no filter: the opacity decides)`, () => {
        expect([r["source-layer"], r.filter ?? null]).toEqual([kind, null]);
      });
      for (const state of STATES) {
        const lit = Object.keys(state)[0];
        it(`${theme} ${ring} ${lit}: standing water always, a stream's polygon below its line's zoom or with no line`, () => {
          expect({
            standing: along(r, { water: kind }, state),
            // Gravel Slough's piece 1360583: polygon z10, line z12
            slough: along(r, { water: "stream", lz: 12 }, state),
            // a line drawn before the polygon (most wide reaches): never ringed
            early: along(r, { water: "stream", lz: 4 }, state),
            // the Oxbows: no line at all
            lineless: along(r, { water: "stream" }, state),
            // not lit: nothing, whatever the water
            unlit: along(r, { water: "stream" }, {}),
          }).toEqual({ standing: shown(0), slough: shown(0, 12), early: shown(0, 4), lineless: shown(0), unlit: shown(99) });
        });
      }
    }
  }

  for (const theme of THEMES) {
    const ls = layersOf(theme);
    // (T735) look C's glow is drawn by the dark theme only (a theme with no glow has no such layer)
    for (const l of [layer(ls, SELECTION_CASING), ...ls.filter((x) => x.id === SELECTION_GLOW)]) {
      it(`${theme} ${l.id}: every line on \`stream\`, but a stub from its polygon's zoom (the ring is its outline)`, () => {
        // no filter: the casing stays in one bucket with the stream (the map lab's switch); the opacity decides
        expect([l["source-layer"], l.filter ?? null]).toEqual(["stream", null]);
        expect({
          // where the ring stops the line's casing lights the water: any line with no `pz`, at every zoom
          line: takenAlong(l, { ord: 3 }),
          sideChannel: takenAlong(l, { ord: 3, sc: 1 }),
          // the Oxbows' stub, its polygon first drawn at z10: cased below it only
          stub: takenAlong(l, { ord: 2, pz: 10 }),
          // a stub whose polygon is drawn from the layer's first zoom: never cased
          early: takenAlong(l, { ord: 2, pz: 4 }),
        }).toEqual({ line: shown(0), sideChannel: shown(0), stub: shown(0, 10), early: shown(0, 4) });
      });
    }
  }
  it("look C's glow exists in a theme (so the loop above checks it)", () => {
    expect(THEMES.some((t) => layersOf(t).some((l) => l.id === SELECTION_GLOW))).toBe(true);
  });
});

/** is `l` drawn on a SELECTED line with `props`, and on the PART you are on, at each zoom ([zoom, 1 | 0]: both, or neither) */
function takenAlong(l: Layer, props: Record<string, unknown>) {
  return ZOOMS.map((z) => {
    const [sel, part] = [{ selected: true }, { part: true }].map((st) => opacity(l, z, props, st) > 0);
    if (sel !== part) throw new Error(`${l.id} z${z}: selected ${sel} but part ${part}`);
    return [z, sel ? 1 : 0];
  });
}
