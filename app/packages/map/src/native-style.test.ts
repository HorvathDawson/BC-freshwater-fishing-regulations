/**
 * N2: the style the native map is handed. MapLibre Native gets the style as JSON (the binding's `mapStyle`), so every
 * look the web map sets imperatively — view, per-layer modes, the Conditions wash, hidden layers, groups, the ground —
 * is written into a copy of the SAME `runtimeStyle()` output through the SAME adapter and `applyLook`. These hold what
 * is native-only about it, and that the look reached the JSON.
 */
import { describe, expect, it } from "vitest";
import { nativeImageName, nativeImages } from "./images";
import { paintFor } from "./paint";
import { nativeStyle } from "./native-style";
import { runtimeStyle } from "./runtime-style";
import { MAP_STYLE, NO_VALUES, resolveTheme } from "./style";

type L = { id: string; type: string; paint?: Record<string, unknown>; layout?: Record<string, unknown>;
           minzoom?: number };
type S = { glyphs: string; sprite: string; layers: L[];
           sources: Record<string, { promoteId?: unknown; data?: unknown; url?: string }> };

const AT = { atlas: "file:///d/release/tiles/v/atlas-v.pmtiles", basemap: "file:///d/basemap.pmtiles",
             outside: "file:///d/release/tiles/v/bc_outside.geojson", assets: "file:///d/release/tiles/v/map-assets-1/" };
const base = { at: AT, theme: "light", view: "plain", modes: {}, hide: [], groups: {}, basemap: "map" as const,
               gauges: '{"type":"FeatureCollection","features":[]}' };
const layer = (s: S, id: string) => s.layers.find((l) => l.id === id)!;

describe("the native style", () => {
  it("is runtimeStyle with the release's own files: pmtiles://file:///, file:// glyphs and sprite", () => {
    const { style } = nativeStyle({ ...base, theme: "dark" });
    const s = style as unknown as S;
    expect({ atlas: s.sources.atlas!.url, basemap: s.sources.basemap!.url, outside: s.sources.outside!.data,
             glyphs: s.glyphs, sprite: s.sprite }).toEqual({
      atlas: "pmtiles://file:///d/release/tiles/v/atlas-v.pmtiles",
      basemap: "pmtiles://file:///d/basemap.pmtiles",
      outside: "file:///d/release/tiles/v/bc_outside.geojson",
      glyphs: "file:///d/release/tiles/v/map-assets-1/fonts/{fontstack}/{range}.pbf",
      sprite: "file:///d/release/tiles/v/map-assets-1/sprites/v4/dark",
    });
  });

  it("promotes nothing: MapLibre Native ignores promoteId, a section is its MVT id", () => {
    const web = runtimeStyle(AT, "light") as unknown as S;
    expect(web.sources.atlas!.promoteId).toBeUndefined();        // T202: the web promotes nothing on the atlas either
    const s = nativeStyle(base).style as unknown as S;
    expect(Object.keys(s.sources.atlas!).includes("promoteId")).toBe(false);
  });

  /*
   * THE GENERATED IMAGES ON A PHONE (T74, N2-F1): every hatch and the gauge pill are asked for under this theme's name,
   * which is exactly what `nativeImages` hands the binding's `Images` — a name the style asks for and nobody adds would
   * draw nothing (a patterned layer with no image vanishes).
   */
  it("asks for the hatches and the pill under the theme's names, each one an image the map is given", () => {
    for (const theme of ["light", "dark", "cvd"]) {
      const s = nativeStyle({ ...base, theme }).style as unknown as S;
      const given = new Set(Object.keys(nativeImages(theme)));
      const patterns = s.layers.filter((l) => l.paint?.["fill-pattern"] !== undefined);
      expect(patterns.map((l) => l.id).sort())
        .toEqual(["eco_reserve", "no_access", "no_access_permit", "park_closed", "wetland"]);
      for (const l of patterns) expect([theme, l.paint!["fill-pattern"]]).toEqual([theme, nativeImageName(`${l.id}-hatch`, theme)]);
      const pill = layer(s, "gauge-label").layout!["icon-image"];
      expect(pill).toBe(nativeImageName("gauge-pill", theme));
      const asked = JSON.stringify(s.layers.map((l) => [l.paint?.["fill-pattern"], l.layout?.["icon-image"]]));
      for (const name of asked.match(/"[a-z_-]+(?:-hatch|-pill|-here)\.[a-z]+"/g) ?? [])
        expect([theme, given.has(JSON.parse(name))]).toEqual([theme, true]);
    }
  });

  /*
   * FEATURE-STATE ONLY WHERE A PHONE HAS AN ID (N2 review). MapLibre Native ignores `promoteId`, so only the source-layers
   * whose features carry an MVT id (the sections) can be addressed. The basin field (Conditions, below z9) is keyed by
   * `basin_id`, a property: by feature-state it was blank on a phone, with no error. It reads its values from the style.
   */
  it("reads feature-state only on the section source-layers (the ones with an MVT id)", () => {
    const s = nativeStyle({ ...base, view: "conditions", modes: { stream: "standing", lake: "standing",
                                                                 basin: "standing" } }).style as unknown as S;
    const reading = s.layers.filter((l) => JSON.stringify([l.paint, l.layout]).includes('"feature-state"'))
      .map((l) => (l as { "source-layer"?: string; source?: string })["source-layer"]
                  ?? (l as { source?: string }).source);
    expect(reading.length).toBeGreaterThan(0);
    // (T175) and the water copy: a GeoJSON source whose features carry the section ids themselves
    expect([...new Set(reading)].sort()).toEqual(["lake", "stream", "water-copy", "wetland"]);
  });

  it("the basin field's values are written into the style, keyed by basin_id (validated: tools/style-valid)", () => {
    const areaData = { basin: { "BX-1": { standing: 42 }, "7": { standing: -100 } } };
    const s = nativeStyle({ ...base, view: "conditions", modes: { stream: "standing", basin: "standing" }, areaData })
      .style as unknown as S;
    const fill = JSON.stringify(layer(s, "basin").paint!["fill-color"]);
    expect(fill).not.toContain("feature-state");
    expect(fill).toContain(JSON.stringify(["get", ["to-string", ["get", "basin_id"]],
                                           ["literal", { "BX-1": 42, "7": -100 }]]));
    // no data yet: the same expression over an empty table, so every basin paints "no reading"
    const empty = nativeStyle({ ...base, view: "conditions", modes: { basin: "standing" } }).style as unknown as S;
    expect(JSON.stringify(layer(empty, "basin").paint!["fill-color"]))
      .toContain(JSON.stringify(["literal", {}]));
  });

  it("the field's seam reads the same table as the field (it follows the basin)", () => {
    const areaData = { basin: { "BX-1": { standing: 42 } } };
    const s = nativeStyle({ ...base, view: "conditions", modes: { basin: "standing" }, areaData }).style as unknown as S;
    const seam = JSON.stringify(layer(s, "basin__edge").paint!["line-color"]);
    expect(seam).not.toContain("feature-state");
    expect(seam).toBe(JSON.stringify(layer(s, "basin").paint!["fill-color"]));
  });

  it("carries the view and each layer's mode, as the web adapter would paint them", () => {
    const s = nativeStyle({ ...base, modes: { stream: "standing", lake: "stocked" } }).style as unknown as S;
    const t = resolveTheme("light");
    expect([layer(s, "stream").paint!["line-color"], layer(s, "lake").paint!["fill-color"]])
      .toEqual([paintFor("stream", "standing", t)["line-color"], paintFor("lake", "stocked", t)["fill-color"]]);
  });

  it("the Conditions wash and the view's own hidden layers, both ways", () => {
    const on = nativeStyle({ ...base, view: "conditions", modes: { stream: "standing" }, hide: ["mu", "region"] })
      .style as unknown as S;
    const off = nativeStyle(base).style as unknown as S;
    expect([layer(on, "basemap-wash").paint!["background-opacity"], layer(off, "basemap-wash").paint!["background-opacity"],
            layer(on, "mu").layout!.visibility, layer(off, "mu").layout!.visibility])
      .toEqual([0.42, 0, "none", "visible"]);
  });

  it("the ground: satellite shows the imagery, the map hides it", () => {
    const sat = nativeStyle({ ...base, basemap: "satellite" }).style as unknown as S;
    const map = nativeStyle(base).style as unknown as S;
    expect([layer(sat, "imagery").layout!.visibility, layer(map, "imagery").layout!.visibility])
      .toEqual(["visible", "none"]);
  });

  it("starts the gauge source with the gauges it is given", () => {
    const fc = '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[-122,49]},"properties":{"label":"x"}}]}';
    const s = nativeStyle({ ...base, gauges: fc }).style as unknown as S;
    expect(s.sources.gauges!.data).toEqual(JSON.parse(fc));
  });

  it("a caller's bad mode is reported, and the style is still made", () => {
    const { style, errors } = nativeStyle({ ...base, modes: { stream: "nope" } });
    expect(errors.map((e) => e.message))
      .toEqual(['layer "stream" has no colour mode "nope" (has plain, standing, profile, salmon, method)']);
    expect((style as unknown as S).layers.length).toBeGreaterThan(0);
  });

  it("the day's profiles go into the style through the web's own call (T74: the engine's values on a phone)", () => {
    const status = { closed: ["p0"], own: ["p1"], base: ["b"], no_access: [], none: [] };
    const values = { ...NO_VALUES, status } as never;
    const s = nativeStyle({ ...base, modes: { stream: "profile" }, values }).style as unknown as S;
    expect(layer(s, "stream").paint!["line-color"])
      .toEqual(paintFor("stream", "profile", resolveTheme("light"), values)["line-color"]);
    expect(nativeStyle({ ...base, modes: { stream: "profile" }, values }).errors).toEqual([]);
  });

  it("the basins' standings from the engine become the field's table", () => {
    const values = { ...NO_VALUES, standing: { section: () => NaN, basins: { "BX-1": 42 } } } as never;
    const s = nativeStyle({ ...base, view: "conditions", modes: { basin: "standing" }, values }).style as unknown as S;
    expect(JSON.stringify(layer(s, "basin").paint!["fill-color"])).toContain(JSON.stringify(["literal", { "BX-1": 42 }]));
  });

  it("writes into a copy: the generated style the web also reads is untouched", () => {
    const before = JSON.stringify(MAP_STYLE);
    nativeStyle({ ...base, modes: { stream: "profile" }, hide: ["mu"], basemap: "satellite" });
    expect(JSON.stringify(MAP_STYLE)).toBe(before);
  });
});
