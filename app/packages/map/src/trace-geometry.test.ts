/**
 * COPYING A ROUTE OUT OF THE TILES (owner 2026-10-21) — which tiles are read, which features
 * are taken, and that a second open reads nothing.
 *
 * The tiles are written by `mvt.fixture.ts`, so every test states the archive it reads.
 */
import { describe, expect, it } from "vitest";
import { encodeTile, type FixtureFeature } from "./mvt.fixture";
import {
  blockAround, clipLine, clipRing, collectionBbox, endFor, newTraceCache, project, readTrace,
  junction, routeBbox, tileOf, tilesCovering, traceCollection, zoomForBudget, type TileSource,
  type TraceRoute,
} from "./trace-geometry";

/** The centre of a tile, as lon/lat — a point whose padded box stays inside that tile at z12. */
const centre = (z: number, x: number, y: number) => {
  const [lon, lat] = project(z, x, y, 1, [0.5, 0.5]);
  return { lon, lat };
};

/** An archive held in memory, recording every tile asked of it. */
function archive(tiles: Record<string, Record<string, FixtureFeature[]>>) {
  const calls: string[] = [];
  const src: TileSource = {
    tile: async (z, x, y) => {
      const k = `${z}/${x}/${y}`;
      calls.push(k);
      return tiles[k] ? encodeTile(tiles[k]!, 4096) : null;
    },
  };
  return { src, calls };
}

const line = (id: number, pts: [number, number][]): FixtureFeature =>
  ({ id, type: 2, parts: [pts] });

const route = (over: Partial<TraceRoute>): TraceRoute =>
  ({ role: "down", tone: "tone", path: [], from: null, to: null, ...over });

describe("tile arithmetic", () => {
  it("finds the tile under Chilliwack at z8 and z12 (checked against the atlas)", () => {
    expect(tileOf(-121.95, 49.15, 8)).toEqual([41, 87]);
    expect(tileOf(-121.95, 49.15, 12)).toEqual([660, 1404]);
  });

  it("covers a box with every tile it touches, row by row", () => {
    const a = centre(12, 660, 1404), b = centre(12, 661, 1405);
    expect(tilesCovering([a.lon, b.lat, b.lon, a.lat], 12))
      .toEqual([[12, 660, 1404], [12, 661, 1404], [12, 660, 1405], [12, 661, 1405]]);
  });

  it("reads at the DEEPEST zoom the budget allows, and at the floor when none fits", () => {
    const a = centre(12, 660, 1404), b = centre(12, 663, 1404);       // 4 tiles wide at z12
    const box: [number, number, number, number] = [a.lon, a.lat - 0.001, b.lon, a.lat + 0.001];
    expect(zoomForBudget(box, 4)).toBe(12);
    expect(zoomForBudget(box, 3)).toBe(11);
    expect(zoomForBudget([-139, 48, -114, 60], 1)).toBe(8);
  });

  it("boxes the ends of every route with a margin, and nothing without an end", () => {
    expect(routeBbox([route({})])).toBeNull();
    expect(routeBbox([route({ from: { lon: -122, lat: 49 }, to: { lon: -121, lat: 50 } })]))
      .toEqual([-122.2, 48.8, -120.8, 50.2]);
    // Both ends on one point still reads its own reach: a floor of margin.
    expect(routeBbox([route({ from: { lon: -122, lat: 49 }, to: { lon: -122, lat: 49 } })]))
      .toEqual([-122.015, 48.99, -121.985, 49.01]);
  });

  it("seeks a lost section at the end of the chain it belongs to", () => {
    const r = route({ path: [1, 2, 3, 4], from: { lon: 1, lat: 1 }, to: { lon: 2, lat: 2 } });
    expect([0, 1, 2, 3].map((i) => endFor(r, i)))
      .toEqual([{ lon: 1, lat: 1 }, { lon: 1, lat: 1 }, { lon: 2, lat: 2 }, { lon: 2, lat: 2 }]);
    expect(blockAround(centre(13, 1320, 2808), 13)).toHaveLength(9);
  });
});

describe("clipping to the tile, so neighbouring tiles abut instead of overlapping", () => {
  it("cuts a line at the tile's edge and keeps the inside", () => {
    expect(clipLine([[-10, 50], [50, 50], [50, 150]], 100))
      .toEqual([[[0, 50], [50, 50], [50, 100]]]);
    // In, out, back in: two pieces, not one line across the gap.
    expect(clipLine([[10, 10], [10, 200], [20, 200], [20, 10]], 100))
      .toEqual([[[10, 10], [10, 100]], [[20, 100], [20, 10]]]);
    // Touching the edge from outside is a point, and a point is not drawn.
    expect(clipLine([[150, 50], [100, 50], [150, 60]], 100)).toEqual([]);
  });

  it("cuts a ring to the tile and closes it", () => {
    expect(clipRing([[-50, -50], [50, -50], [50, 50], [-50, 50], [-50, -50]], 100))
      .toEqual([[0, 0], [50, 0], [50, 50], [0, 50], [0, 0]]);
    expect(clipRing([[200, 200], [300, 200], [300, 300], [200, 200]], 100)).toEqual([]);
  });
});

describe("reading a route out of the tiles", () => {
  const z12 = centre(12, 660, 1404);
  const SPOT = { lon: z12.lon - 0.005, lat: z12.lat };
  const GAUGE = { lon: z12.lon + 0.005, lat: z12.lat };

  it("takes exactly the sections on the route, by id, and none of their neighbours", async () => {
    const { src, calls } = archive({ "12/660/1404": { stream: [
      line(11, [[100, 2048], [2048, 2048]]), line(12, [[2048, 2048], [4000, 2048]]),
      line(99, [[0, 0], [4096, 4096]]),                      // a tributary not on the route
    ] } });
    const got = await readTrace([route({ path: [11, 12], from: SPOT, to: GAUGE })], src,
                                newTraceCache());
    expect(calls).toEqual(["12/660/1404"]);
    expect([...got.shapes.keys()].sort()).toEqual([11, 12]);
    expect(got.missing).toEqual([]);
    expect(got.shapes.get(11)!.lines).toEqual([[project(12, 660, 1404, 4096, [100, 2048]),
                                                project(12, 660, 1404, 4096, [2048, 2048])]]);
  });

  it("reads nothing at all the second time — sections are cached by id", async () => {
    const { src, calls } = archive({ "12/660/1404": { stream: [
      line(11, [[100, 2048], [2048, 2048]]) ] } });
    const cache = newTraceCache();
    const r = [route({ path: [11], from: SPOT, to: GAUGE })];
    await readTrace(r, src, cache);
    const again = await readTrace(r, src, cache);
    expect(calls).toEqual(["12/660/1404"]);
    expect(again).toEqual({ shapes: again.shapes, missing: [], fetched: 0, asked: [] });
    expect([...again.shapes.keys()]).toEqual([11]);
  });

  it("answers a NEIGHBOURING route from the tile cache, without refetching", async () => {
    const { src, calls } = archive({ "12/660/1404": { stream: [
      line(11, [[100, 2048], [2048, 2048]]), line(12, [[2048, 2048], [4000, 2048]]) ] } });
    const cache = newTraceCache();
    await readTrace([route({ path: [11], from: SPOT, to: GAUGE })], src, cache);
    const next = await readTrace([route({ path: [12], from: SPOT, to: GAUGE })], src, cache);
    expect(calls).toEqual(["12/660/1404"]);
    expect(next.fetched).toBe(0);
    expect([...next.shapes.keys()]).toEqual([12]);
  });

  it("goes one zoom deeper AROUND THE END for a creek too small for the first tiles", async () => {
    // Section 5 (the spot's own creek) exists only at z13, in the tile under the spot.
    const [x13, y13] = tileOf(SPOT.lon, SPOT.lat, 13);
    const { src, calls } = archive({
      "12/660/1404": { stream: [line(6, [[100, 2048], [4000, 2048]])] },
      [`13/${x13}/${y13}`]: { stream: [line(5, [[10, 10], [500, 500]])] },
    });
    const got = await readTrace([route({ path: [5, 6], from: SPOT, to: GAUGE })], src,
                                newTraceCache());
    expect(got.missing).toEqual([]);
    expect(calls[0]).toBe("12/660/1404");
    // The second pass is the 3x3 block around the SPOT at z13 — and nothing else.
    expect(calls.slice(1).sort()).toEqual(blockAround(SPOT, 13).map((t) => t.join("/")).sort());
  });

  it("stops at the tile budget and says what it could not find", async () => {
    const { src, calls } = archive({});
    const got = await readTrace([route({ path: [1, 2], from: SPOT, to: GAUGE })], src,
                                newTraceCache(), { first: 16, max: 10 });
    expect(calls).toHaveLength(10);
    expect(got.missing).toEqual([1, 2]);
  });

  it("a tile the archive cannot serve is an empty tile, not a failure", async () => {
    const src: TileSource = { tile: async () => { throw new Error("offline"); } };
    const got = await readTrace([route({ path: [1], from: SPOT, to: GAUGE })], src,
                                newTraceCache(), { first: 1, max: 2 });
    expect(got.missing).toEqual([1]);
  });
});

describe("the overlay collection", () => {
  it("draws one line per route, tagged with its role and tone, heaviest on top", () => {
    const shapes = new Map([
      [1, { lines: [[[0, 0], [1, 1]] as [number, number][]], areas: [], z: 12 }],
      [2, { lines: [[[1, 1], [2, 2]] as [number, number][]], areas: [], z: 12 }],
      [3, { lines: [], areas: [[[[0, 0], [1, 0], [1, 1], [0, 0]] as [number, number][]]], z: 12 }],
    ]);
    const fc = traceCollection([
      route({ role: "down", tone: "tone-a", path: [1, 2] }),
      route({ role: "up", tone: "tone-b", path: [1, 3, 404] }),
    ], shapes);
    expect(fc).toEqual({ type: "FeatureCollection", features: [
      { type: "Feature", properties: { role: "down", tone: "tone-a", order: 2, kind: "line", on: false },
        geometry: { type: "MultiLineString", coordinates: [[[0, 0], [1, 1]], [[1, 1], [2, 2]]] } },
      { type: "Feature", properties: { role: "up", tone: "tone-b", order: 1, kind: "area", on: false },
        geometry: { type: "MultiPolygon", coordinates: [[[[0, 0], [1, 0], [1, 1], [0, 0]]]] } },
      { type: "Feature", properties: { role: "up", tone: "tone-b", order: 1, kind: "line", on: false },
        geometry: { type: "MultiLineString", coordinates: [[[0, 0], [1, 1]]] } },
    ] });
    expect(collectionBbox(fc)).toEqual([0, 0, 2, 2]);
  });

  // T252: the confluence is found in the tiles' own geometry and marked with the words handed in
  it("marks the confluence at the tributary's end against the confluence reach", () => {
    const P = (pts: number[][]) => pts as [number, number][];
    // the confluence reach runs (0,0)->(0,4); the tributary's line comes in from the east and ends at (0,2), mid-reach;
    // a tile clipped it at (5,2), an end far from the river
    const into = { lines: [P([[0, 0], [0, 2], [0, 4]])], areas: [], z: 12 };
    const trib = { lines: [P([[0, 2], [3, 2]]), P([[3, 2], [5, 2]])], areas: [], z: 12 };
    expect(junction(trib, into)).toEqual([0, 2]);
    expect(junction(undefined, into)).toBeNull();
    expect(junction({ lines: [], areas: [], z: 12 }, into)).toBeNull();
    const fc = traceCollection([
      route({ role: "up", tone: "tone-a", path: [7, 8], mark: { at: 8 as never, into: 7 as never,
                                                                 label: "joins at the mouth of Alouette River" } }),
    ], new Map([[7, into], [8, trib]]));
    expect(fc.features.at(-1)).toEqual({ type: "Feature",
      properties: { role: "up", tone: "tone-a", order: 1, kind: "join", on: false,
                    label: "joins at the mouth of Alouette River" },
      geometry: { type: "Point", coordinates: [0, 2] } });
    // a reach the tiles did not hold: no mark rather than a guessed one
    expect(traceCollection([route({ role: "up", tone: "t", path: [7], mark: { at: 9 as never, into: 7 as never,
                                                                              label: "x" } })],
                           new Map([[7, into]])).features.some((f) => f.properties.kind === "join")).toBe(false);
  });
});
