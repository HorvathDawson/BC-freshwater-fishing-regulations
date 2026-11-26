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
import { maplibreDialect } from "./native/maplibre/dialect";
import { runtimeStyle } from "./runtime-style";
import { PMTILES } from "./tile-access";
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
    const { style } = nativeStyle({ ...base, theme: "dark" }, maplibreDialect);
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

  /*
   * (T1154) EVERY ARCHIVE THE PHONE READS IS ADDRESSED `pmtiles://file…`: the binding's patch reads a tile's feature ids
   * only for such an address (`MLRNVectorSource.kt`: `if (!u.startsWith("pmtiles://file")) return`). On any other (a
   * local http tile server, say) it sends no ids and the map loses all its colours, with no error shown. The address is
   * the dialect's (`maplibreDialect.tiles`); the archive's path stays the release's (`at.atlas`).
   */
  it("addresses every archive as pmtiles://file, the only address the patch's tile-id reader reads", () => {
    const s = nativeStyle(base, maplibreDialect).style as unknown as
      { sources: Record<string, { type?: string; url?: string }> };
    const vector = Object.entries(s.sources).filter(([, v]) => v.type === "vector");
    expect(vector.map(([id, v]) => [id, v.url!.startsWith("pmtiles://file")]))
      .toEqual([["basemap", true], ["atlas", true], ["basins", true], ["mus", true]]);
    // the atlas's own path is untouched by the address: the pairing key and the route reader's path
    expect(s.sources.atlas!.url).toBe(`pmtiles://${AT.atlas}`);
  });

  it("promotes nothing: MapLibre Native ignores promoteId, a section is its MVT id", () => {
    const web = runtimeStyle(AT, "light", {}, PMTILES) as unknown as S;
    expect(web.sources.atlas!.promoteId).toBeUndefined();        // T202: the web promotes nothing on the atlas either
    const s = nativeStyle(base, maplibreDialect).style as unknown as S;
    expect(Object.keys(s.sources.atlas!).includes("promoteId")).toBe(false);
    // (T1154) and the field's sources over the same archive, which the web DOES promote (`fieldSources`): the dialect
    // drops theirs, so a phone's style carries no promoteId at all
    expect(Object.keys((web.sources.basins ?? {}) as object).includes("promoteId")).toBe(true);
    expect(Object.entries(s.sources).filter(([, v]) => Object.keys(v).includes("promoteId")).map(([id]) => id)).toEqual([]);
  });

  /*
   * THE GENERATED IMAGES ON A PHONE (T74, N2-F1): every hatch and the gauge pill are asked for under this theme's name,
   * which is exactly what `nativeImages` hands the binding's `Images` — a name the style asks for and nobody adds would
   * draw nothing (a patterned layer with no image vanishes).
   */
  it("asks for the hatches and the pill under the theme's names, each one an image the map is given", () => {
    for (const theme of ["light", "dark", "cvd"]) {
      const s = nativeStyle({ ...base, theme }, maplibreDialect).style as unknown as S;
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
                                                                 basin: "standing" } }, maplibreDialect).style as unknown as S;
    const reading = s.layers.filter((l) => JSON.stringify([l.paint, l.layout]).includes('"feature-state"'))
      .map((l) => (l as { "source-layer"?: string; source?: string })["source-layer"]
                  ?? (l as { source?: string }).source);
    expect(reading.length).toBeGreaterThan(0);
    // (T175) and the water copy: a GeoJSON source whose features carry the section ids themselves
    expect([...new Set(reading)].sort()).toEqual(["lake", "stream", "water-copy", "wetland"]);
  });

  it("the basin field's values are written into the style, keyed by basin_id (validated: tools/style-valid)", () => {
    const areaData = { basin: { "BX-1": { standing: 42 }, "7": { standing: -100 } } };
    const s = nativeStyle({ ...base, view: "conditions", modes: { stream: "standing", basin: "standing" }, areaData }, maplibreDialect)
      .style as unknown as S;
    const fill = JSON.stringify(layer(s, "basin").paint!["fill-color"]);
    expect(fill).not.toContain("feature-state");
    expect(fill).toContain(JSON.stringify(["get", ["to-string", ["get", "basin_id"]],
                                           ["literal", { "BX-1": 42, "7": -100 }]]));
    // no data yet: the same expression over an empty table, so every basin paints "no reading"
    const empty = nativeStyle({ ...base, view: "conditions", modes: { basin: "standing" } }, maplibreDialect).style as unknown as S;
    expect(JSON.stringify(layer(empty, "basin").paint!["fill-color"]))
      .toContain(JSON.stringify(["literal", {}]));
  });

  it("the field's seam reads the same table as the field (it follows the basin)", () => {
    const areaData = { basin: { "BX-1": { standing: 42 } } };
    const s = nativeStyle({ ...base, view: "conditions", modes: { basin: "standing" }, areaData }, maplibreDialect).style as unknown as S;
    const seam = JSON.stringify(layer(s, "basin__edge").paint!["line-color"]);
    expect(seam).not.toContain("feature-state");
    expect(seam).toBe(JSON.stringify(layer(s, "basin").paint!["fill-color"]));
  });

  it("carries the view and each layer's mode, as the web adapter would paint them", () => {
    const s = nativeStyle({ ...base, modes: { stream: "standing", lake: "stocked" } }, maplibreDialect).style as unknown as S;
    const t = resolveTheme("light");
    expect([layer(s, "stream").paint!["line-color"], layer(s, "lake").paint!["fill-color"]])
      .toEqual([paintFor("stream", "standing", t)["line-color"], paintFor("lake", "stocked", t)["fill-color"]]);
  });

  it("the Conditions wash and the view's own hidden layers, both ways", () => {
    const on = nativeStyle({ ...base, view: "conditions", modes: { stream: "standing" }, hide: ["mu", "region"] }, maplibreDialect)
      .style as unknown as S;
    const off = nativeStyle(base, maplibreDialect).style as unknown as S;
    expect([layer(on, "basemap-wash").paint!["background-opacity"], layer(off, "basemap-wash").paint!["background-opacity"],
            layer(on, "mu").layout!.visibility, layer(off, "mu").layout!.visibility])
      .toEqual([0.42, 0, "none", "visible"]);
  });

  it("the ground: satellite shows the imagery, the map hides it", () => {
    const sat = nativeStyle({ ...base, basemap: "satellite" }, maplibreDialect).style as unknown as S;
    const map = nativeStyle(base, maplibreDialect).style as unknown as S;
    expect([layer(sat, "imagery").layout!.visibility, layer(map, "imagery").layout!.visibility])
      .toEqual(["visible", "none"]);
  });

  it("(T923) the drawn map's reserve and parcel edges are as before and its casings are not drawn; satellite gets the T923 paint", () => {
    for (const theme of ["light", "dark", "cvd"]) {
      const tk = resolveTheme(theme) as Record<string, unknown>;
      const map = nativeStyle({ ...base, theme }, maplibreDialect).style as unknown as S;
      const sat = nativeStyle({ ...base, theme, basemap: "satellite" }, maplibreDialect).style as unknown as S;
      for (const [edge, casing, colour] of [["indigenous_land__edge", "indigenous_land__casing", "color.indigenous.edge.imagery"],
                                            ["parcel_private__edge", "parcel_private__casing", "color.parcel.edge.imagery"]] as const) {
        // the drawn map: the edge is its plain paint, untouched; the casing is there but at opacity 0
        expect(layer(map, edge).paint, `${theme} ${edge} drawn`).toEqual(paintFor(edge, "plain", resolveTheme(theme)));
        expect(layer(map, casing).paint!["line-opacity"], `${theme} ${casing} drawn`).toBe(0);
        // satellite: the darker edge at full strength, its ramped width, and the casing drawn under it
        const e = layer(sat, edge).paint!, c = layer(sat, casing).paint!;
        expect([e["line-color"], e["line-opacity"]], `${theme} ${edge} satellite`).toEqual([tk[colour], tk["opacity.land.edge.imagery"]]);
        expect(e["line-width"], `${theme} ${edge} width`).toEqual(["interpolate", ["linear"], ["zoom"],
          ...[[4, 2.5], [11, 2.5], [16, 3.5]].flatMap(([z, k]) => [z, (tk["width.land.edge.imagery"] as number) * k!])]);
        expect([c["line-color"], c["line-opacity"]], `${theme} ${casing} satellite`).toEqual([tk["color.imagery.casing"], tk["opacity.land.casing.imagery"]]);
        // the casing's colour is one fixed light value in every theme (the photograph does not follow the theme)
        expect(tk["color.imagery.casing"]).toBe(resolveTheme("light")["color.imagery.casing"]);
        expect(String(tk["color.imagery.casing"]).startsWith("@")).toBe(false);
        // and it sits under the edge
        const ids = layer(sat, edge) && sat.layers.map((l) => l.id);
        expect(ids.indexOf(casing)).toBeLessThan(ids.indexOf(edge));
      }
    }
  });

  it("(T923) over satellite the reserve and parcel outlines are ONE look in every theme (the photograph does not change)", () => {
    const paints = (theme: string) => {
      const sat = nativeStyle({ ...base, theme, basemap: "satellite" }, maplibreDialect).style as unknown as S;
      return ["indigenous_land__edge", "indigenous_land__casing", "parcel_private__edge", "parcel_private__casing"]
        .map((id) => [id, layer(sat, id).paint]);
    };
    const light = paints("light");
    expect(paints("dark")).toEqual(light);
    expect(paints("cvd")).toEqual(light);
    // and they are really drawn: full-strength edge, casing visible
    expect(light.every(([, p]) => (p as Record<string, number>)["line-opacity"]! > 0)).toBe(true);
  });

  it("starts the gauge source with the gauges it is given", () => {
    const fc = '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[-122,49]},"properties":{"label":"x"}}]}';
    const s = nativeStyle({ ...base, gauges: fc }, maplibreDialect).style as unknown as S;
    expect(s.sources.gauges!.data).toEqual(JSON.parse(fc));
  });

  /*
   * (T831) THE ROUTE MAP ON A PHONE: every piece the web's route overlay draws its dots and pin with is in the phone's
   * style — the `trace-pins` source, its layers reading it, the pin's image under this theme's name and given to the map
   * — and the pins a route map opens with are IN that source (the binding adopts a source the style declares and never
   * pushes the data it mounted with). The web's `setTrace` writes `pinCollection(pins)`; this is the same collection.
   */
  it("(T831) carries the route overlay's pin source, layers and image, and starts it with the pins it is given", () => {
    const pins = [{ lat: 49.1, lon: -121.9, kind: "here" as const, tone: "tone-ink", label: "You are here", rank: 0 },
                  { lat: 49.2, lon: -122.1, kind: "gauge" as const, tone: "tone-blue", label: null, rank: 1 }];
    for (const theme of ["light", "dark"]) {
      const s = nativeStyle({ ...base, theme, tracePins: pins }, maplibreDialect).style as unknown as S;
      const onPins = (s.layers as (L & { source?: string })[]).filter((l) => l.source === "trace-pins").map((l) => l.id);
      expect([theme, onPins]).toEqual([theme, ["trace-gauge-halo", "trace-gauge", "trace-share", "trace-label", "trace-here", "trace-here-label"]]);
      const here = layer(s, "trace-here").layout!["icon-image"] as string;
      expect([theme, here, here in nativeImages(theme)]).toEqual([theme, nativeImageName("trace-here", theme), true]);
      expect([theme, s.sources["trace-pins"]!.data]).toEqual([theme, { type: "FeatureCollection", features: [
        { type: "Feature", properties: { kind: "here", tone: "tone-ink", rank: 0, label: "You are here" },
          geometry: { type: "Point", coordinates: [-121.9, 49.1] } },
        { type: "Feature", properties: { kind: "gauge", tone: "tone-blue", rank: 1 },
          geometry: { type: "Point", coordinates: [-122.1, 49.2] } }] }]);
    }
    expect((nativeStyle(base, maplibreDialect).style as unknown as S).sources["trace-pins"]!.data).toEqual({ type: "FeatureCollection", features: [] });
  });

  it("(T831) starts the route overlay's line source with the lines it is given, and its line layers read it", () => {
    const lines = { type: "FeatureCollection" as const, features: [{ type: "Feature" as const,
      properties: { role: "down" as const, tone: "tone-blue", kind: "line" as const, order: 1, on: false },
      geometry: { type: "MultiLineString" as const, coordinates: [[[-122, 49], [-121.9, 49.1]] as [number, number][]] } }] };
    const s = nativeStyle({ ...base, traceLines: lines }, maplibreDialect).style as unknown as S;
    const onLines = (s.layers as (L & { source?: string })[]).filter((l) => l.source === "trace").map((l) => l.id);
    expect([onLines, s.sources.trace!.data]).toEqual([["trace-line-down", "trace-line-up", "trace-arrows", "trace-join", "trace-join-label"], lines]);
    expect((nativeStyle(base, maplibreDialect).style as unknown as S).sources.trace!.data).toEqual({ type: "FeatureCollection", features: [] });
  });

  it("a caller's bad mode is reported, and the style is still made", () => {
    const { style, errors } = nativeStyle({ ...base, modes: { stream: "nope" } }, maplibreDialect);
    expect(errors.map((e) => e.message))
      .toEqual(['layer "stream" has no colour mode "nope" (has plain, standing, profile, salmon, runs)']);
    expect((style as unknown as S).layers.length).toBeGreaterThan(0);
  });

  it("the day's profiles go into the style through the web's own call (T74: the engine's values on a phone)", () => {
    const status = { closed: ["p0"], own: ["p1"], base: ["b"], no_access: [], none: [] };
    const values = { ...NO_VALUES, status } as never;
    const s = nativeStyle({ ...base, modes: { stream: "profile" }, values }, maplibreDialect).style as unknown as S;
    expect(layer(s, "stream").paint!["line-color"])
      .toEqual(paintFor("stream", "profile", resolveTheme("light"), values)["line-color"]);
    expect(nativeStyle({ ...base, modes: { stream: "profile" }, values }, maplibreDialect).errors).toEqual([]);
  });

  it("the basins' standings from the engine become the field's table", () => {
    const values = { ...NO_VALUES, standing: { section: () => NaN, basins: { "BX-1": 42 } } } as never;
    const s = nativeStyle({ ...base, view: "conditions", modes: { basin: "standing" }, values }, maplibreDialect).style as unknown as S;
    expect(JSON.stringify(layer(s, "basin").paint!["fill-color"])).toContain(JSON.stringify(["literal", { "BX-1": 42 }]));
  });

  it("(T261) the Runs tab's field: each watershed group's run state from the engine, in the field's table", () => {
    const values = { ...NO_VALUES, runs: { key: "6:44", basins: { LFRA: "pass", CHWK: "spawn" } } } as never;
    const s = nativeStyle({ ...base, modes: { stream: "runs", basin: "runs" }, values }, maplibreDialect).style as unknown as S;
    const fill = JSON.stringify(layer(s, "basin").paint!["fill-color"]);
    expect(fill).toContain(JSON.stringify(["literal", { LFRA: "pass", CHWK: "spawn" }]));
  });

  it("writes into a copy: the generated style the web also reads is untouched", () => {
    const before = JSON.stringify(MAP_STYLE);
    nativeStyle({ ...base, modes: { stream: "profile" }, hide: ["mu"], basemap: "satellite" }, maplibreDialect);
    expect(JSON.stringify(MAP_STYLE)).toBe(before);
  });
});
