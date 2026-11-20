/**
 * (T667 slice 8) THE MINI RELEASE as the app reads it: the committed release the real pipeline built from the invented
 * world (`pipeline/tests/fixtures/mini/`, `python -m pipeline.tools.mini_release --write`), opened by the app's own
 * loader — every file held to its manifest (length + sha256) — and read: its waters are the invented ones, by name.
 */
import { describe, expect, it } from "vitest";
import { buildSearchIndex, queryIndex } from "@app/core";
import { makeReleaseSource } from "../../release/source";
import { MINI_RELEASE_ROOT, miniReleaseDir, openMiniRelease, sectionNames } from "./releaseDir";

describe("the mini release (T667 slice 8)", () => {
  it("opens as a release: the folder LATEST names, its manifest's id, no tiles and no ground", () => {
    const dir = miniReleaseDir(), loader = openMiniRelease();
    expect(dir.startsWith(MINI_RELEASE_ROOT)).toBe(true);
    expect(dir.endsWith(loader.manifest.data_release)).toBe(true);
    expect([loader.manifest.basemap, loader.manifest.tiles?.current ?? null]).toEqual([null, null]);
  });

  it("holds the invented waters, read through the app's own tables", async () => {
    const loader = openMiniRelease(), water = await loader.db("water");
    const names = (await water.all(
      "SELECT s.text AS id FROM item i JOIN str s ON s.id = i.item_id ORDER BY s.text")).map((r) => String(r.id));
    expect(names).toEqual(expect.arrayContaining(["gnis:990001", "gnis:990002", "gnis:990004", "wbk:800000001",
      "wbk:800000006"]));
    // Fixture River's sections by blue line + metre (its mouth piece is tidal; the coast cut is at 1 km)
    const s = sectionNames(loader);
    const [mouth, low] = await s.streams([[900000001, 0], [900000001, 1000]]);
    expect(mouth).not.toBe(low);
  }, 30_000);

  it("(T927) its boundary place reaches search through the release reader: core's cut_place, as the index reads it", async () => {
    const data = await makeReleaseSource(openMiniRelease()).searchIndex();
    expect(data.cuts).toEqual([{ item: "gnis:990002", words: "Mock Creek Falls", alt: [], generic: false,
                                 blk: 900000002, m: 5999, lat: 52.264448, lon: -125.853228 }]);
    // and the in-memory index finds it by its words, on its water
    const ix = buildSearchIndex(data);
    expect(queryIndex(ix, "mock creek falls").cuts.map((c) => [c.words, c.water.name])).toEqual([["Mock Creek Falls", "Mock Creek"]]);
  }, 30_000);
});
