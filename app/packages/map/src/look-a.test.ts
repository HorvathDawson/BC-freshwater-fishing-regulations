/**
 * T1208 / T1209 — THE ZOOMED-OUT LOOK A (owner 2026-11-26: "build A"), held on the built style and the composed one: region
 * borders dashed, thinner and banded, named once per region from fixed points; small rule lines quieter zoomed out; the light
 * theme's softer ground and one water blue (dark unchanged); the towns in three tiers, a lake's name placed before a town's,
 * every region named at every tier's zoom and no two town names in each other's way there.
 */
import { describe, expect, it } from "vitest";
import { MAP_STYLE, resolveTheme, STYLE_META } from "./style";
import { runtimeStyle } from "./runtime-style";
import { PMTILES } from "./tile-access";
import { REGION_BAND, REGION_NAMES, REGION_POINTS, TIER_ZOOM, TOWN_DOTS, TOWN_LIST, TOWN_NAMES, TOWNS_UNTIL } from "./look-a";

type L = { id: string; type?: string; source?: string; filter?: unknown; minzoom?: number; maxzoom?: number;
           paint?: Record<string, unknown>; layout?: Record<string, unknown> };
const AT = { atlas: "a.pmtiles", basemap: "b.pmtiles", outside: "o.geojson" } as never;
const composed = (theme: string) => runtimeStyle(AT, theme, {}, PMTILES) as unknown as { layers: L[]; sources: Record<string, unknown> };
const ids = (s: { layers: L[] }) => s.layers.map((l) => l.id);
const meta = STYLE_META as unknown as { dashes: Record<string, string>; widthRamps?: Record<string, unknown>; views: { id: string; hide?: string[] }[] };
/** the fishing regions the regulations are organised by */
const REGIONS = ["1", "2", "3", "4", "5", "6", "7A", "7B", "8"];

describe("(T1208) region borders that read as borders", () => {
  it("the border is dashed (dash.region) in every theme; the tiles' per-piece number is gone", () => {
    expect(meta.dashes["region__edge"]).toBe("dash.region");
    for (const th of ["light", "dark", "cvd"]) expect(resolveTheme(th)["dash.region"]).toEqual([4, 2.5]);
    expect(MAP_STYLE.layers.map((l) => l.id)).not.toContain("region__label");
  });

  it("a soft band just inside each region's edge, under the dashes, to z10", () => {
    const s = composed("light"), band = s.layers.find((l) => l.id === REGION_BAND)!;
    expect(ids(s).indexOf(REGION_BAND)).toBe(ids(s).indexOf("region__edge") - 1);
    expect([band.source, (band as { "source-layer"?: string })["source-layer"], band.maxzoom]).toEqual(["atlas", "region", 10]);
    // a polygon ring's right side is its inside: a positive offset
    expect(JSON.stringify(band.paint!["line-offset"])).toBe(JSON.stringify(["interpolate", ["linear"], ["zoom"], 4, 1, 8, 2]));
  });

  it("\"Region n\" once per region, from a fixed point (no tile edge), z4 to z7; the Conditions map hides it and the band", () => {
    expect([...REGION_POINTS.map((r) => r.region_id)].sort()).toEqual([...REGIONS].sort());
    const s = composed("light"), names = s.layers.find((l) => l.id === REGION_NAMES)!;
    expect([names.source, names.minzoom, names.maxzoom, names.layout!["text-field"]])
      .toEqual(["region-labels", 4, 7, ["concat", "Region ", ["get", "region_id"]]]);
    expect(meta.views.find((v) => v.id === "conditions")!.hide).toEqual(expect.arrayContaining([REGION_BAND, REGION_NAMES]));
    // placed before the towns (over them in the stack): at province zoom a town's name moves round a region's, not the reverse
    expect(ids(s).indexOf(REGION_NAMES)).toBeGreaterThan(ids(s).indexOf(TOWN_NAMES));
    expect(names.layout!["text-variable-anchor"]).toEqual(["center", "top", "bottom"]);
  });
});

describe("(T1208) quieter rule lines zoomed out", () => {
  const stream = (MAP_STYLE as unknown as { layers: { id: string }[] }).layers.find((l) => l.id === "stream");
  it("thinner below z8: an order-9 river is under a pixel at z4 (it was 1.4)", () => {
    const src = STYLE_META as unknown as { widths?: Record<string, { ramp: number[][] }> };
    const ramp = (src.widths?.["stream"]?.ramp ?? []) as number[][];
    const z4 = ramp.find((r) => r[0] === 4)!;
    expect(stream).toBeTruthy();
    expect(z4[1]! + 9 * z4[2]!).toBeLessThan(1);
  });

  it("small orders recede from z5 (`fadeByOrder` z5 and z6 stops), never below T470's floor", () => {
    const fade = (STYLE_META as unknown as { orderFades: Record<string, { stops: [number, number[]][] }> }).orderFades["stream"]!;
    const z5 = fade.stops.find(([z]) => z === 5)![1];
    expect(z5.slice(0, 4).every((k) => k < 1)).toBe(true);
    expect(z5[4]).toBe(1);
    expect(Math.min(...fade.stops.flatMap(([, ks]) => ks))).toBeGreaterThanOrEqual(0.45);
  });
});

describe("(T1208) the base: a softer ground and one water blue in light; dark unchanged", () => {
  const layer = (s: { layers: L[] }, id: string) => s.layers.find((l) => l.id === id)!;
  it("light: the ground and every basemap water are the theme's tokens, the land cover quieter", () => {
    const s = composed("light"), t = resolveTheme("light");
    expect(layer(s, "earth").paint!["fill-color"]).toBe(t["color.ui.basemap.ground"]);
    expect(layer(s, "water").paint!["fill-color"]).toBe(t["color.ui.basemap.water"]);
    expect(JSON.stringify(layer(s, "landcover").paint!["fill-opacity"])).toContain(String(t["opacity.ground.cover"]));
  });
  it("the basemap's \"BRITISH COLUMBIA\" is gone (every reader knows where they are; it took a town's room)", () => {
    for (const th of ["light", "dark"]) expect(ids(composed(th))).not.toContain("places_region");
  });
  it("dark: the flavour's own ground and water (Protomaps black)", () => {
    const s = composed("dark");
    expect([layer(s, "earth").paint!["fill-color"], layer(s, "water").paint!["fill-color"]]).toEqual(["#141414", "#333333"]);
  });
});

/** Web-mercator pixels of a 512 px tile at zoom z */
const px = (lon: number, lat: number, z: number): [number, number] => {
  const n = 512 * 2 ** z, s = Math.sin((lat * Math.PI) / 180);
  return [((lon + 180) / 360) * n, (0.5 - Math.log((1 + s) / (1 - s)) / (4 * Math.PI)) * n];
};

describe("(T1209) the towns to find your way by", () => {
  it("drawn from our own file, under the water's names (a lake is named first) and over the basemap's", () => {
    const s = composed("light"), order = ids(s);
    expect(order.indexOf(TOWN_NAMES)).toBeGreaterThan(order.indexOf("places_locality"));
    expect(order.indexOf(TOWN_NAMES)).toBeLessThan(order.indexOf("lake__label"));
    expect(s.layers.find((l) => l.id === TOWN_DOTS)!.maxzoom).toBe(TOWNS_UNTIL);
    // the basemap never names one of ours twice below z10
    expect(JSON.stringify(s.layers.find((l) => l.id === "places_locality")!.filter)).toContain('["!",["in",["coalesce",["get","name:en"],["get","name"]]');
  });

  it("every region is named at every tier's zoom (what the map shows there: that tier and the ones above it)", () => {
    for (const tier of [1, 2, 3] as const) {
      const shown = TOWN_LIST.filter((t) => t.tier <= tier);
      const missing = REGIONS.filter((r) => !shown.some((t) => t.region === r));
      expect(missing, `tier ${tier} (from z${TIER_ZOOM[tier]})`).toEqual([]);
    }
  });

  it("at each tier's first zoom every town shown can be named without two names colliding (four anchors, as the map)", () => {
    const failed: string[] = [];
    for (const tier of [1, 2, 3] as const) {
      const z = TIER_ZOOM[tier], size = z >= 9 ? 12 : 11, pad = 3, off = 0.5 * size;
      const shown = TOWN_LIST.filter((t) => t.tier <= tier).map((t, i) => ({ t, i, p: px(t.lon, t.lat, z) }))
        .sort((a, b) => a.t.tier - b.t.tier || a.i - b.i);
      const boxes: [number, number, number, number][] = [];
      // the dots are obstacles too
      for (const { p } of shown) boxes.push([p[0] - 3, p[1] - 3, p[0] + 3, p[1] + 3]);
      const hit = (b: [number, number, number, number]) => boxes.some((o) => b[0] < o[2] && b[2] > o[0] && b[1] < o[3] && b[3] > o[1]);
      for (const { t, p } of shown) {
        const w = t.name.length * size * 0.56 + 2 * pad, h = size * 1.2 + 2 * pad;
        const tries: [number, number, number, number][] = [
          [p[0] + off, p[1] - h / 2, p[0] + off + w, p[1] + h / 2],           // left anchor: the name to the right
          [p[0] - off - w, p[1] - h / 2, p[0] - off, p[1] + h / 2],           // right anchor
          [p[0] - w / 2, p[1] + off, p[0] + w / 2, p[1] + off + h],           // top anchor: below
          [p[0] - w / 2, p[1] - off - h, p[0] + w / 2, p[1] - off],           // bottom anchor: above
        ];
        const fit = tries.find((b) => !hit(b));
        if (fit) boxes.push(fit); else failed.push(`z${z}: ${t.name}`);
      }
    }
    expect(failed).toEqual([]);
  });
});
