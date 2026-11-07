/**
 * (T420) The map lab at its defaults is nothing: the style handed to the map is today's, byte for byte. Each switch moves
 * only the layers it names.
 */
import { describe, expect, it } from "vitest";
import { LAB_DEFAULT, labStyle, type Lab } from "./lab";
import { nativeStyle } from "./native-style";

type L = { id: string; type: string; source?: string; "source-layer"?: string; filter?: unknown;
           minzoom?: number; maxzoom?: number; layout?: Record<string, unknown> };
type S = { layers: L[] };

const AT = { atlas: "file:///d/release/tiles/v/atlas-v.pmtiles", basemap: "file:///d/basemap.pmtiles",
             outside: "file:///d/release/tiles/v/bc_outside.geojson", assets: "file:///d/release/tiles/v/map-assets-1/" };
const base = { at: AT, theme: "light", view: "plain", modes: {}, hide: [], groups: {}, basemap: "map" as const,
               gauges: '{"type":"FeatureCollection","features":[]}' };
const today = () => nativeStyle(base).style as unknown as S;
const lab = (change: Partial<Lab>) => labStyle(today(), { ...LAB_DEFAULT, ...change });

/** What MapLibre groups into one bucket: same source, source-layer, zoom range, filter and layout. */
const buckets = (s: S, pick: (l: L) => boolean) => new Set(s.layers.filter(pick).map((l) =>
  JSON.stringify([l.type, l.source, l["source-layer"], l.minzoom, l.maxzoom, l.filter, l.layout ?? {}]))).size;
const streamLine = (l: L) => l.type === "line" && l.source === "atlas" && l["source-layer"] === "stream";
const visible = (l: L) => l.layout?.visibility !== "none";

/** The ids of the layers whose JSON differs between two styles. */
const moved = (a: S, b: S) => a.layers.filter((l, i) => JSON.stringify(l) !== JSON.stringify(b.layers[i])).map((l) => l.id);

describe("the map lab", () => {
  it("with skip-invisible off, hands back the very style, byte for byte", () => {
    const s = today(), off = { ...LAB_DEFAULT, hideInvisible: false };
    expect(labStyle(s, off)).toBe(s);
    expect(JSON.stringify(labStyle(today(), off))).toBe(JSON.stringify(today()));
    // the native switches (pixel cap, prefetch) never touch the style
    expect(labStyle(s, { ...off, pixelCap: false, prefetch: 2 })).toBe(s);
  });

  it("at its defaults moves only layers drawing nothing (every opacity the constant 0)", () => {
    const t = today(), s = labStyle(today(), LAB_DEFAULT);
    expect(moved(t, s)).toEqual(moved(t, lab({ hideInvisible: true })));
  });

  it("streams in one bucket: today four, the lab one, and nothing else moves", () => {
    // casing (round caps) · overlays (butt, round joins) · the line (defaults) · `minzoom: 0` vs none
    expect(buckets(today(), (l) => streamLine(l) && visible(l))).toBe(4);
    const s = lab({ streamsOneBucket: true });
    expect(buckets(s, (l) => streamLine(l) && visible(l))).toBe(1);
    const t = labStyle(today(), LAB_DEFAULT);
    expect(moved(t, s).every((id) => streamLine(t.layers.find((l) => l.id === id)!))).toBe(true);
    expect(s.layers.map((l) => l.id)).toEqual(t.layers.map((l) => l.id));
  });

  it("skip invisible: hides only layers whose every opacity is the constant 0", () => {
    const t = today(), s = lab({ hideInvisible: true });
    const ids = moved(t, s);
    expect(ids.length).toBeGreaterThan(0);
    for (const id of ids) {
      const l = t.layers.find((x) => x.id === id)! as L & { paint?: Record<string, unknown> };
      expect(Object.entries(l.paint ?? {}).filter(([k]) => k.endsWith("opacity")).every(([, v]) => v === 0)).toBe(true);
      expect(s.layers.find((x) => x.id === id)!.layout?.visibility).toBe("none");
    }
  });

  it.each([
    ["roads", { roads: false }, (l: L) => l.source === "basemap" && l["source-layer"] === "roads"],
    ["landuse", { landuse: false }, (l: L) => l.source === "basemap" && l["source-layer"] === "landuse"],
    ["labels", { labels: false }, (l: L) => l.type === "symbol"],
  ] as const)("%s off hides exactly those layers", (_, change, pick) => {
    const t = labStyle(today(), LAB_DEFAULT), s = lab(change);
    const named = t.layers.filter(pick).map((l) => l.id);
    expect(named.length).toBeGreaterThan(0);
    expect(s.layers.filter(pick).every((l) => l.layout?.visibility === "none")).toBe(true);
    expect(moved(t, s).sort()).toEqual(named.filter((id) => t.layers.find((l) => l.id === id)!.layout?.visibility !== "none").sort());
  });
});
