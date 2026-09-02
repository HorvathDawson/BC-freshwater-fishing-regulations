/**
 * Does MapLibre accept our style?
 *
 * Every other check compares the style to one of OUR artifacts — the tile contract, the
 * token palette, the view definitions. All of them passed while the style declared
 * `type: "polygon"` and `type: "point"`, which MapLibre does not have. The renderer
 * refused to load it, and the only way anyone found out was opening the app.
 *
 * So this asks the renderer's own validator. It is the one check whose authority is not us.
 */
import { describe, expect, it } from "vitest";
import { validateStyleMin } from "@maplibre/maplibre-gl-style-spec";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const style = JSON.parse(readFileSync(
  fileURLToPath(new URL("../packages/map/style/style.json", import.meta.url)), "utf8"));

describe("the generated style", () => {
  it("is a style MapLibre will load", () => {
    // The source is a pmtiles:// URL resolved by a protocol handler at runtime, and glyphs
    // are supplied when the style is composed — neither is knowable here, so validate the
    // layers against a source the spec can see.
    const errors = validateStyleMin({
      ...style,
      sources: { atlas: { type: "vector", tiles: ["https://example.invalid/{z}/{x}/{y}.pbf"] } },
      glyphs: "https://example.invalid/{fontstack}/{range}.pbf",
    } as Parameters<typeof validateStyleMin>[0]);
    expect(errors.map((e) => `${e.message}`)).toEqual([]);
  });

  it("uses only layer types the renderer has", () => {
    // Our vocabulary and MapLibre's are different words for different things: a layer IS a
    // polygon, and is DRAWN as a fill. build-style.mjs translates; this pins the result.
    const DRAWN = new Set(["fill", "line", "symbol", "circle", "heatmap",
                           "fill-extrusion", "raster", "hillshade", "background"]);
    for (const l of style.layers) expect(DRAWN, l.id).toContain(l.type);
  });

  it("gives every layer the paint property the adapter will set on it", () => {
    // baseAdapter picks line-color / fill-color / circle-color from the type. A layer whose
    // type it does not recognise silently gets circle-color, which a fill layer ignores —
    // so the area draws in its default colour and nothing reports a problem.
    for (const l of style.layers)
      expect(["line", "fill", "circle"], `${l.id} is drawn as ${l.type}`).toContain(l.type);
  });
});
