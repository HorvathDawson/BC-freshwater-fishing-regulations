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
      sources: {
        atlas: { type: "vector", tiles: ["https://example.invalid/{z}/{x}/{y}.pbf"] },
        // The imagery source as the runtime hands it over: our authoring keys stripped.
        ...Object.fromEntries(Object.entries(style.sources as Record<string, object>)
          .filter(([, v]) => (v as { type?: string }).type === "raster")
          .map(([k, v]) => [k, Object.fromEntries(Object.entries(v)
            .filter(([f]) => f !== "external" && !f.startsWith("$")))])),
      },
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
    // colourPropFor picks line-color / fill-color / text-color / circle-color from the
    // type. A layer whose type it does not recognise silently gets circle-color, which a
    // fill layer ignores — so the area draws in its default colour and nothing reports a
    // problem. "symbol" is here because water labels are text and text takes text-color.
    //
    // A RASTER is the one exception, and it is exempt only because the adapter never
    // paints it: it has no colour modes at all (the builder refuses one), so there is no
    // mode for `colourPropFor` to translate. Pinned here, so a raster that grew a colour
    // mode would fail rather than be handed `circle-color`.
    const meta = JSON.parse(readFileSync(fileURLToPath(
      new URL("../packages/map/style/style.meta.json", import.meta.url)), "utf8"));
    for (const l of style.layers) {
      if (l.type === "raster") {
        expect(meta.colorModes[l.id], `${l.id} is a raster with colour modes`).toBeUndefined();
        continue;
      }
      expect(["line", "fill", "circle", "symbol"], `${l.id} is drawn as ${l.type}`)
        .toContain(l.type);
    }
  });
});
