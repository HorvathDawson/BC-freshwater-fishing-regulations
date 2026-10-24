/**
 * T175 — THE WATER'S OWN LINE AT EVERY ZOOM, as plain code (`water-copy.ts`). Whole values throughout: what is kept,
 * what is drawn, where it stops, where the camera goes, which states reach the copy.
 */
import { describe, expect, it } from "vitest";
import { onceSink, type StateSink } from "./sections";
import { WaterCopy, copyIds, mirrorSink, probeZoom, zoomAround, type CopyTarget, type LitFeature } from "./water-copy";
import { baseAdapter, type MapHandle } from "./adapters/contract";
import { copyFillPaint, copyLinePaint, paintFor } from "./paint";
import { resolveTheme } from "./style";

/** A small creek (two sections, 501 and 502) and the river it joins (900); a lake (700). */
const CREEK_A: LitFeature = { id: 501, layer: "stream", properties: { ord: 1, name: "Small Creek" },
  geometry: { type: "LineString", coordinates: [[-121.5, 49.2], [-121.49, 49.21]] } };
/** 501 again, in the next tile (a tile's clip: one section comes back in pieces). */
const CREEK_A2: LitFeature = { id: 501, layer: "stream", properties: { ord: 1, name: "Small Creek" },
  geometry: { type: "LineString", coordinates: [[-121.49, 49.21], [-121.48, 49.215]] } };
const CREEK_B: LitFeature = { id: 502, layer: "stream", properties: { ord: 1, name: "Small Creek" },
  geometry: { type: "MultiLineString", coordinates: [[[-121.48, 49.215], [-121.47, 49.22]]] } };
const RIVER: LitFeature = { id: 900, layer: "stream", properties: { ord: 6 },
  geometry: { type: "LineString", coordinates: [[-121.6, 49.1], [-121.5, 49.2]] } };
const LAKE: LitFeature = { id: 700, layer: "lake", properties: {},
  geometry: { type: "Polygon", coordinates: [[[-121, 49], [-120.9, 49], [-120.9, 49.1], [-121, 49]]] } };
const MARSH: LitFeature = { id: 701, layer: "wetland", properties: {},
  geometry: { type: "Polygon", coordinates: [[[-121, 49], [-120.95, 49], [-121, 49.05], [-121, 49]]] } };

describe("copyIds: what a map keeps a copy of", () => {
  it("is what it lights and the water it holds, once each, as numbers, in order", () => {
    expect(copyIds([502, "501"], undefined, [501, 700, "x"], null)).toEqual([501, 502, 700]);
    expect(copyIds()).toEqual([]);
  });
});

describe("WaterCopy: kept from the tiles, the first time a zoom has them", () => {
  it("keeps only this water's ids, a stream's line and a lake's polygon, tile pieces merged (whole collection)", () => {
    const c = new WaterCopy();
    expect(c.reset([501, 502, 700, 701], "atlas-a")).toBe(true);
    expect(c.take([CREEK_A, RIVER, CREEK_A2, LAKE, MARSH], 12)).toEqual([501, 700]);
    expect(c.collection()).toEqual({ type: "FeatureCollection", features: [
      { type: "Feature", id: 501, properties: { ord: 1, name: "Small Creek", layer: "stream" },
        geometry: { type: "MultiLineString", coordinates: [
          [[-121.5, 49.2], [-121.49, 49.21]], [[-121.49, 49.21], [-121.48, 49.215]]] } },
      { type: "Feature", id: 700, properties: { layer: "lake" },
        geometry: { type: "MultiPolygon", coordinates: [
          [[[-121, 49], [-120.9, 49], [-120.9, 49.1], [-121, 49]]]] } },
    ] });
    // the river is another water (never kept); the marsh is not copied (a wetland)
    expect([c.has(900), c.has(701), c.layerOf(501), c.layerOf(700)]).toEqual([false, false, "stream", "lake"]);
  });

  it("a later pass does not replace what a zoom already gave — unless it saw MORE of the section", () => {
    const c = new WaterCopy();
    c.reset([501, 502]);
    expect(c.take([CREEK_A], 12.4)).toEqual([501]);         // half of 501 on screen
    expect(c.take([CREEK_A], 14)).toEqual([]);              // the same again, deeper: kept as it was
    expect(c.take([CREEK_A, CREEK_A2, CREEK_B], 13)).toEqual([501, 502]);   // all of 501 now: replaced; 502 new
    expect(c.ceiling("stream", 0)).toBe(13);
  });

  it("is dropped when the water changes — or the archive its ids belong to — and not otherwise", () => {
    const c = new WaterCopy();
    c.reset([501, 502], "a");
    c.take([CREEK_A], 12);
    expect(c.reset([502, 501], "a")).toBe(false);           // the same water, ids in another order
    expect(c.size).toBe(1);
    expect(c.reset([900], "a")).toBe(true);                  // another water: nothing of the creek survives
    expect([c.size, c.collection().features.length, [...c.ids]]).toEqual([0, 0, [900]]);
    c.take([RIVER], 9);
    expect(c.reset([900], "b")).toBe(true);                  // a new atlas: an id may be another section there
    expect(c.size).toBe(0);
  });

  it("stops where the atlas draws the water itself: the deepest kept zoom, or the view's floor if deeper", () => {
    const c = new WaterCopy();
    c.reset([501, 502, 700]);
    expect([c.ceiling("stream", 8), c.ceiling("lake", 0)]).toEqual([null, null]);
    c.take([CREEK_A], 12.7);
    c.take([LAKE], 9.2);
    c.take([CREEK_B], 11.1);
    expect([c.ceiling("stream", 8), c.ceiling("stream", 13), c.ceiling("lake", 0)]).toEqual([12, 13, 9]);
  });
});

describe("probeZoom: where no zoom has shown the water, step in", () => {
  const ask = (o: Partial<Parameters<typeof probeZoom>[0]>) =>
    probeZoom({ zoom: 8, found: false, kept: 0, rounds: 0, touched: false, ...o });
  it("one whole zoom at a time, until the tiles carry it, and no further than the atlas's z14", () => {
    expect([ask({}), ask({ zoom: 8.6 }), ask({ zoom: 13.2 }), ask({ zoom: 14 })]).toEqual([9, 9, 14, null]);
  });
  it("never once it is found or kept, against the reader's own move, or past the budget", () => {
    expect([ask({ found: true }), ask({ kept: 1 }), ask({ touched: true }), ask({ rounds: 10 }), ask({ rounds: 9 })])
      .toEqual([null, null, null, null, 9]);
  });
});

describe("zoomAround: the point on the water stays where it is on screen", () => {
  it("halves the anchor's distance to the centre per zoom step (Mercator), and keeps the centre with no anchor", () => {
    const at = zoomAround({ lon: -121, lat: 49 }, { lon: -122, lat: 49 }, 8, 9);
    expect([Math.round(at.lon * 1e6) / 1e6, Math.round(at.lat * 1e6) / 1e6, at.zoom]).toEqual([-121.5, 49, 9]);
    const two = zoomAround({ lon: -121, lat: 49 }, { lon: -122, lat: 49 }, 8, 10);
    expect(Math.round(two.lon * 1e6) / 1e6).toBe(-121.75);
    expect(zoomAround({ lon: -121, lat: 49 }, null, 8, 11)).toEqual({ lon: -121, lat: 49, zoom: 11 });
  });
});

describe("mirrorSink: the one pathway, written to the copy too", () => {
  const rig = () => {
    const log: string[] = [];
    const copy = new WaterCopy();
    copy.reset([501]);
    const inner: StateSink = {
      set: (t, id, s) => log.push(`set ${t.source}/${t.sourceLayer} ${id} ${JSON.stringify(s)}`),
      remove: (t, id, k) => log.push(`remove ${t.source}/${t.sourceLayer} ${id} ${k}`),
    };
    const target: CopyTarget = {
      set: (id, s) => log.push(`copy set ${id} ${JSON.stringify(s)}`),
      remove: (id, k) => log.push(`copy remove ${id} ${k ?? "*"}`),
    };
    return { log, copy, sink: onceSink(mirrorSink(inner, () => copy, () => target)) };
  };
  it("one selection (set on every per-section layer) reaches the copy ONCE; another water's state never", () => {
    const { log, sink } = rig();
    for (const sl of ["wetland", "lake", "stream"]) sink.set({ source: "atlas", sourceLayer: sl }, 501, { selected: true });
    sink.set({ source: "atlas", sourceLayer: "stream" }, 501, { standing: 42 });
    sink.set({ source: "atlas", sourceLayer: "stream" }, 900, { standing: 7 });
    sink.set({ source: "basins", sourceLayer: "basin" }, 501, { standing: 3 });
    expect(log).toEqual([
      'set atlas/wetland 501 {"selected":true}', 'copy set 501 {"selected":true}',
      'set atlas/lake 501 {"selected":true}', 'set atlas/stream 501 {"selected":true}',
      'set atlas/stream 501 {"standing":42}', 'copy set 501 {"standing":42}',
      'set atlas/stream 900 {"standing":7}',
      'set basins/basin 501 {"standing":3}',
    ]);
  });
  it("a cleared key is cleared on the copy once; a key it never held is not sent", () => {
    const { log, sink } = rig();
    sink.set({ source: "atlas", sourceLayer: "stream" }, 501, { selected: true });
    sink.set({ source: "atlas", sourceLayer: "lake" }, 501, { selected: true });
    log.length = 0;
    sink.remove({ source: "atlas", sourceLayer: "stream" }, 501, "selected");
    sink.remove({ source: "atlas", sourceLayer: "lake" }, 501, "selected");
    sink.remove({ source: "atlas", sourceLayer: "lake" }, 501, "water");
    expect(log).toEqual([
      "remove atlas/stream 501 selected", "copy remove 501 selected", "remove atlas/lake 501 selected",
    ]);
  });
  it("after the renderer dropped every state (a style swap) the next writes reach the copy again", () => {
    const { log, copy, sink } = rig();
    sink.set({ source: "atlas", sourceLayer: "stream" }, 501, { standing: 42 });
    copy.forgetStates();
    const again = mirrorSink({ set: () => {}, remove: () => {} }, () => copy,
                             () => ({ set: (id, s) => log.push(`copy set ${id} ${JSON.stringify(s)}`), remove: () => {} }));
    log.length = 0;
    again.set({ source: "atlas", sourceLayer: "stream" }, 501, { standing: 42 });
    expect(log).toEqual(['copy set 501 {"standing":42}']);
  });
});

describe("a low-order creek framed at z8 (the ticket's case, as the map runs it)", () => {
  it("steps in until its tiles carry it, keeps it, and draws it at z8 below its own zoom", () => {
    const c = new WaterCopy();
    c.reset([501, 502], "atlas");
    /** The atlas: the creek is in the tiles from z12 (a magnitude-5 creek, `MAGNITUDE_LADDER`). */
    const tiles = (z: number) => (Math.floor(z) >= 12 ? [CREEK_A, CREEK_A2, CREEK_B, RIVER] : [RIVER]);
    let zoom = 8, rounds = 0;
    const path: number[] = [];
    for (;;) {
      const found = tiles(zoom).filter((f) => c.ids.has(f.id));
      c.take(found, zoom);
      const next = probeZoom({ zoom, found: found.length > 0, kept: c.size, rounds, touched: false });
      if (next === null) break;
      rounds += 1; zoom = next; path.push(next);
    }
    expect(path).toEqual([9, 10, 11, 12]);
    expect(c.collection().features.map((f) => [f.id, f.geometry.type, f.geometry.coordinates.length]))
      .toEqual([[501, "MultiLineString", 2], [502, "MultiLineString", 1]]);
    // the field stands rivers down below z8; the copy draws until z12, where the atlas draws the creek itself
    expect(c.ceiling("stream", 8)).toBe(12);
    // the reader zooms back out to z8: nothing to probe (it is kept), and the copy is what draws there
    expect(probeZoom({ zoom: 8, found: false, kept: c.size, rounds: 0, touched: true })).toBe(null);
  });
});

describe("the copy's paint IS the water's own (one colouring pathway)", () => {
  const tokens = resolveTheme("light");
  it("a copied line: the stream's paint for its mode, the width floored at the highlight's", () => {
    const stream = paintFor("stream", "standing", tokens);
    const copy = copyLinePaint(stream, tokens);
    expect({ ...copy, "line-width": null }).toEqual({ ...stream, "line-width": null });
    const w = copy["line-width"] as unknown[];
    expect(w.slice(0, 3)).toEqual(["interpolate", ["linear"], ["zoom"]]);
    // every output of the zoom curve is the stream's own, under a max with the highlight width
    const own = stream["line-width"] as unknown[];
    for (let i = 4; i < w.length; i += 2) expect(w[i]).toEqual(["max", own[i], Number(tokens["width.stream.highlight"])]);
  });
  it("a copied lake: the lake's fill paint, and nothing that is not a fill's", () => {
    expect(copyFillPaint({ "fill-color": "the lake's", "fill-opacity": 0.5, "fill-pattern": "x", "line-width": 2 }))
      .toEqual({ "fill-color": "the lake's", "fill-opacity": 0.5 });
  });
  it("the adapter paints the copy in the SAME call as the stream and the lake, in every mode", () => {
    const calls: string[] = [];
    const h: MapHandle = { setVisibility: () => {}, setFeatureState: () => {}, clearFeatureStates: () => {},
                           setPaint: (l, p, v) => calls.push(`${l} ${p} ${JSON.stringify(v)}`) };
    const a = baseAdapter("web");
    a.setLayerMode(h, "stream", "standing", "light");
    a.setLayerMode(h, "lake", "standing", "light");
    const of = (layer: string, prop: string) => calls.filter((c) => c.startsWith(`${layer} ${prop} `)).at(-1)?.split(" ").slice(2).join(" ");
    expect(of("water-copy-line", "line-color")).toBe(of("stream", "line-color"));
    expect(of("water-copy-line", "line-opacity")).toBe(of("stream", "line-opacity"));
    expect(of("water-copy-fill", "fill-color")).toBe(of("lake", "fill-color"));
    expect(of("water-copy-casing", "line-color")).toBe(of("selection-casing", "line-color"));
  });
});
