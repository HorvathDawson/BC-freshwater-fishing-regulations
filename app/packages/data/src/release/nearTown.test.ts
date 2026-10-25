/**
 * T182: A SEARCH ROW'S "NEAR X" FROM CORE ALONE. The release ships per water the ring town(s) at the smallest distance
 * + kind handicap (pipeline `build.core_near` → core `item_near`), weighed by the contract's handicaps; the app's
 * `describeBy` weighs by its own. They must be ONE table, or core would ship a town the app would not have named.
 * And the reader: with `item_near` the first search opens core only; a core cut before it reads every ring from near.
 */
import { describe, expect, it } from "vitest";
import { describeBy, HANDICAP_KM, UNKNOWN_HANDICAP_KM } from "@app/core";
import { NEAR_HANDICAP_KM, NEAR_HANDICAP_UNKNOWN_KM } from "../pack.generated";
import { memoryRelease, type MemorySeed } from "../sql/drivers/memoryRelease";
import { makeReleaseSource } from "./source";

describe("the release's town handicaps are the app's", () => {
  it("contract NEAR_HANDICAP_KM = @app/core HANDICAP_KM, and the unknown kind's too", () => {
    expect({ ...HANDICAP_KM }).toEqual(NEAR_HANDICAP_KM);
    expect(UNKNOWN_HANDICAP_KM).toBe(NEAR_HANDICAP_UNKNOWN_KM);
  });

  it("mutation: a town the release ships at the smallest cost is the one describeBy names from every ring", () => {
    // the Elk at Fernie: Mosquito Flats (a locality) 0.1 km off, Fernie (a city) 4.4 km — Fernie (4.4 < 6.1)
    const rings = [{ name: "Mosquito Flats", kind: "locality", lat: 0, lon: 0, km: 0.1 },
                   { name: "Fernie", kind: "city", lat: 0, lon: 0, km: 4.4 }];
    expect(describeBy(rings)?.name).toBe("Fernie");
    const cost = (r: { kind: string; km: number }) => Math.round(r.km * 100) + Math.round((NEAR_HANDICAP_KM[r.kind]
      ?? NEAR_HANDICAP_UNKNOWN_KM) * 100);
    const best = Math.min(...rings.map(cost));
    expect(rings.filter((r) => cost(r) === best).map((r) => r.name)).toEqual(["Fernie"]);
  });
});

/** core: the Elk (ord 1) and a lake (ord 2); places Fernie (1), Mosquito Flats (2); near: every ring. */
const seed = (withNear: boolean): MemorySeed => ({
  core: (d) => {
    d.exec("INSERT INTO item (ord, item_id, name, kind, pieces, area_ha) VALUES (1, 'gnis:1', 'Elk River', 'stream', 3, NULL), " +
           "(2, 'wbk:2', 'Elk Lake', 'lake', 1, 40)");
    d.exec("INSERT INTO place (place_id, osm, name, kind, pop, lat, lon) VALUES (1, 'a', 'Fernie', 'city', 5000, 49.5, -115.06), " +
           "(2, 'b', 'Mosquito Flats', 'locality', NULL, 49.4, -115.0)");
    if (withNear) d.exec("INSERT INTO item_near (ord, place_id, ckm) VALUES (1, 1, 440), (2, 2, 30)");
    else d.exec("DROP TABLE item_near");
  },
  near: (d) => {
    d.exec("INSERT INTO place_water (place_id, ckm, ord) VALUES (2, 10, 1), (1, 440, 1), (2, 30, 2), (1, 900, 2)");
  },
});

describe("searchNames: each result's town", () => {
  it("from core's item_near — the first search opens core and nothing else", async () => {
    const loader = memoryRelease(seed(true));
    const hits = await makeReleaseSource(loader).searchNames("elk", 10);
    expect(hits.map((h) => [h.item, h.near?.name, h.near?.km]).sort()).toEqual(
      [["gnis:1", "Fernie", 4.4], ["wbk:2", "Mosquito Flats", 0.3]]);
    expect(loader.opened()).toEqual(["core"]);
  });

  it("a core cut before item_near: the same towns, from every ring in near (core + near)", async () => {
    const loader = memoryRelease(seed(false));
    const hits = await makeReleaseSource(loader).searchNames("elk", 10);
    expect(hits.map((h) => [h.item, h.near?.name, h.near?.km]).sort()).toEqual(
      [["gnis:1", "Fernie", 4.4], ["wbk:2", "Mosquito Flats", 0.3]]);
    expect(loader.opened()).toEqual(["core", "near"]);
  });
});
