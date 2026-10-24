/**
 * (owner 2026-10-22) A GROUP NAME THAT ONLY POINTS, as the reader answers it: the `see_pointer` rows (the pipeline's —
 * `rules.see_pointer_rows`, copied whole into the release's `core`), one hit per pointer row, its waters in the book's
 * order, ordered and cut by core (`rankPointers`).
 *
 * No committed fixture prints such a row, so a memory release is given the real Tee Pee and Cameron rows.
 */
import { beforeAll, describe, expect, it } from "vitest";
import { memorySource } from "./memoryRelease";
import * as Q from "../queries";
import { RELEASE_QUERIES } from "../../release/queries";
import type { WaterSource } from "../../index";

const ROWS = [
  ["r7:cameron_lakes@7-31", "Cameron Lakes", 0, "r7:north_cameron_lake@7-31", "North Cameron Lake", "wbk:329657333"],
  ["r7:cameron_lakes@7-31", "Cameron Lakes", 1, "r7:south_cameron_lake@7-31", "South Cameron Lake", "wbk:329657328"],
  ["r8:tee_pee_lakes@8-6", "Tee Pee Lakes", 2, "r8:sunday_lake@8-6", "Sunday Lake", "wbk:329520017"],
  ["r8:tee_pee_lakes@8-6", "Tee Pee Lakes", 0, "r8:friday_lake@8-6", "Friday Lake", "wbk:329520049"],
  ["r8:tee_pee_lakes@8-6", "Tee Pee Lakes", 1, "r8:saturday_lake@8-6", "Saturday Lake", null],
] as const;

let src: WaterSource;
beforeAll(() => {
  src = memorySource({ core: (d) => {
    const ins = d.prepare("INSERT INTO see_pointer VALUES (?, ?, ?, ?, ?, ?)");
    for (const r of ROWS) ins.run(...r);
  } });
});

describe("the group names that only point", () => {
  it("one hit per pointer row, its waters in the book's order (a water that matched nothing has no item)", async () => {
    expect(await src.searchPointers("tee pee", 3)).toEqual([{ entry: "r8:tee_pee_lakes@8-6", name: "Tee Pee Lakes", see: [
      { name: "Friday Lake", item: "wbk:329520049" }, { name: "Saturday Lake", item: null },
      { name: "Sunday Lake", item: "wbk:329520017" }] }]);
  });

  it("found by its name or its start, case aside; never by a word inside it; two letters at least", async () => {
    expect([(await src.searchPointers("CAMERON LAKES", 3)).map((h) => h.name),
            (await src.searchPointers("Cam", 3)).map((h) => h.name),
            await src.searchPointers("lakes", 3), await src.searchPointers("c", 3), await src.searchPointers("cam", 0)])
      .toEqual([["Cameron Lakes"], ["Cameron Lakes"], [], [], []]);
  });

  it("the release asks its copy of the table with the bundle's SQL", () => {
    expect(RELEASE_QUERIES.SEARCH_POINTERS).toEqual({ file: "core", sql: Q.SEARCH_POINTERS, bundle: Q.SEARCH_POINTERS });
  });
});
