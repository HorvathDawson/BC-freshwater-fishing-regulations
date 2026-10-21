/**
 * N2: the style the native map is handed. MapLibre Native gets the style as JSON (the binding's `mapStyle`), so every
 * look the web map sets imperatively — view, per-layer modes, the Conditions wash, hidden layers, groups, the ground —
 * is written into a copy of the SAME `runtimeStyle()` output through the SAME adapter and `applyLook`. These hold what
 * is native-only about it, and that the look reached the JSON.
 */
import { describe, expect, it } from "vitest";
import { paintFor } from "./adapters/contract";
import { nativeStyle } from "./native-style";
import { runtimeStyle } from "./runtime-style";
import { MAP_STYLE, STYLE_META, resolveTheme } from "./style";

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
    expect(web.sources.atlas!.promoteId).toBeDefined();          // the web keeps the area layers' promotion
    const s = nativeStyle(base).style as unknown as S;
    expect(Object.keys(s.sources.atlas!).includes("promoteId")).toBe(false);
  });

  it("draws no fill-pattern (generated images are web-only for now): the hatched layers fall back to their fill", () => {
    const s = nativeStyle(base).style as unknown as S;
    const patterned = s.layers.filter((l) => l.paint && "fill-pattern" in l.paint).map((l) => l.id);
    expect(patterned).toEqual([]);
    for (const id of Object.keys(STYLE_META.patterns ?? {}))
      expect(layer(s, id).paint?.["fill-color"], id).toEqual(paintFor(id, "plain", resolveTheme("light"))["fill-color"]);
  });

  it("carries the view and each layer's mode, as the web adapter would paint them", () => {
    const s = nativeStyle({ ...base, modes: { stream: "status", lake: "status" } }).style as unknown as S;
    const t = resolveTheme("light");
    expect([layer(s, "stream").paint!["line-color"], layer(s, "lake").paint!["fill-color"]])
      .toEqual([paintFor("stream", "status", t)["line-color"], paintFor("lake", "status", t)["fill-color"]]);
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
      .toEqual(['layer "stream" has no colour mode "nope" (has plain, standing, status)']);
    expect((style as unknown as S).layers.length).toBeGreaterThan(0);
  });

  it("writes into a copy: the generated style the web also reads is untouched", () => {
    const before = JSON.stringify(MAP_STYLE);
    nativeStyle({ ...base, modes: { stream: "status" }, hide: ["mu"], basemap: "satellite" });
    expect(JSON.stringify(MAP_STYLE)).toBe(before);
  });
});
