/**
 * THE GL INTERNALS THE WEB MAP READS, PINNED (colour-latency AP; the verifier's condition 4).
 *
 * `web/tileIds.ts` reads a tile's ids and the source's tile list out of maplibre-gl's private fields. A minor release
 * may rename any of them, and the failure would be quiet: the map would fall back to the public route (correct, and
 * slower) and nobody would know. So this fails on ANY other installed version, and checks that every private name the
 * reader relies on is still in the shipped bundle and its source — bump `GL_INTERNALS` only after re-reading them.
 */
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { GL_INTERNALS, sourceTiles, tileIds } from "../packages/map/src/web/tileIds";
import { glWorkerUrl } from "../packages/map/src/web/glWorker";

const require = createRequire(import.meta.url);
const GL = dirname(require.resolve("maplibre-gl/package.json"));
const read = (p: string) => readFileSync(join(GL, p), "utf8");

describe("maplibre-gl internals (pinned)", () => {
  it(`is maplibre-gl ${GL_INTERNALS}.x — re-read the internals before moving this`, () => {
    const v = (JSON.parse(read("package.json")) as { version: string }).version;
    expect(v.split(".").slice(0, 2).join(".")).toBe(GL_INTERNALS);
  });

  it("still has every private name the reader uses, in the bundle the app ships", () => {
    // (T1252) GL 6 is ES modules only: the page's code is `maplibre-gl.mjs` and the chunk it may share with the worker
    const dist = read("dist/maplibre-gl.mjs") + read("dist/maplibre-gl-shared.mjs");
    const missing = ["latestFeatureIndex", "loadVTLayers", "tileManagers", "_inViewTiles", "getAllTiles",
                     "_outOfViewCache"].filter((n) => !dist.includes(n));
    expect(missing).toEqual([]);
  });

  it("fires the tile's load event AFTER the tile's feature-state is initialised (so state set in it lands first frame)", () => {
    const src = read("src/tile/tile_manager.ts");
    const init = src.indexOf("this._state.initializeTileState(tile");
    // (T1252) GL 6's events are classes: the tile's load event is a MapSourceDataEvent (dataType 'source', set by its class)
    const fire = src.indexOf("this._source.fire(new MapSourceDataEvent('data', {tile, coord: tile.tileID}))");
    expect(init).toBeGreaterThan(0);
    expect(fire).toBeGreaterThan(init);
    expect(read("src/tile/tile.ts")).toContain("latestFeatureIndex: FeatureIndex | null;");
    expect(read("src/data/feature_index.ts")).toContain("loadVTLayers(): {[_: string]: VectorTileLayerLike}");
  });
});

describe("reading a tile shaped like 6.19's", () => {
  const layer = (ids: (number | undefined)[]) => ({ length: ids.length, feature: (i: number) => ({ id: ids[i] }) });
  const tile = { state: "loaded", latestFeatureIndex: { loadVTLayers: () => ({
    stream: layer([5, 7, undefined]), lake: layer([9]), contour: layer([1]) }) } };

  it("lists the ids of the layers asked for, skipping features with no id", () => {
    expect(tileIds(tile, ["stream", "lake", "wetland"])).toEqual({ stream: [5, 7], lake: [9] });
  });

  it("says null — take the public route — for anything that does not look like 6.19", () => {
    expect(tileIds({}, ["stream"])).toBeNull();
    expect(tileIds({ latestFeatureIndex: null }, ["stream"])).toBeNull();
    expect(tileIds({ latestFeatureIndex: { loadVTLayers: () => { throw new Error("x"); } } }, ["stream"])).toBeNull();
    expect(sourceTiles({ style: {} }, "atlas")).toBeNull();
  });

  it("lists in-view AND cached tiles, loaded ones only", () => {
    const map = { style: { tileManagers: { atlas: {
      _inViewTiles: { getAllTiles: () => [tile, { state: "loading" }] },
      _outOfViewCache: { data: { k: [{ value: { state: "loaded", n: 2 } }] } } } } } };
    expect(sourceTiles(map, "atlas")).toEqual([tile, { state: "loaded", n: 2 }]);
  });
});

describe("the worker GL 6 starts (T1252)", () => {
  // apps/mobile/gl-worker.js: what Metro copies into public/ at start (plain CommonJS)
  const { glWorkerDir, glWorkerFiles } = require("../apps/mobile/gl-worker.js") as {
    glWorkerDir: (v: string) => string; glWorkerFiles: (dist: string) => string[] };
  const version = (JSON.parse(read("package.json")) as { version: string }).version;

  it("is asked for where gl-worker.js puts it, in a folder named by GL's version", () => {
    expect(glWorkerUrl(version)).toBe(`/${glWorkerDir(version)}/maplibre-gl-worker.mjs`);
    expect(glWorkerUrl(version)).toBe(`/maplibre-gl-${version}/maplibre-gl-worker.mjs`);
  });

  it("is copied with every module it imports, each one in the package", () => {
    const files = glWorkerFiles(join(GL, "dist"));
    expect(files[0]).toBe("maplibre-gl-worker.mjs");
    for (const f of files) {
      const src = read(`dist/${f}`);
      for (const m of src.matchAll(/(?:\bfrom|\bimport)\s*["']\.\/([^"']+)["']/g)) expect(files).toContain(m[1]);
    }
  });
});
