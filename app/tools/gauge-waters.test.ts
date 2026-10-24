/**
 * (T168) `gaugeWaters`: a section's waters — its own item and every water joined to it across `water_continue`, both
 * ways and transitively — and each station's own water and reach. Against a memory release (the release contract's
 * columns behind the app's loader and reader), since no committed fixture holds a continuation and a matched gauge.
 * Here in tools/ because only tools and the drivers may name node:sqlite (layers.json).
 *
 * Mutations (each run 2026-10-24, each failed here and was reverted):
 *   - `continueLinks` without its `next_ord` half: the Vedder (the link's far end) no longer reaches the Chilliwack.
 *   - `continuedOrds` stopping after one hop: the Sumas (two links away) is lost.
 */
import { describe, expect, it } from "vitest";
import type { RegsSource, SectionId, StationId } from "@app/data";
import { memorySource, type MemorySeed } from "../packages/data/src/sql/drivers/memoryRelease";

/** `continuation` false: a release with no water file (or one cut before `water_continue`). */
function release(continuation = true): RegsSource {
  const seed: MemorySeed = {
    core: (d) => d.exec(`
      INSERT INTO item (ord, item_id, name) VALUES (1, 'gnis:8634', 'Chilliwack River'), (2, 'gnis:3062', 'Vedder River'),
                                                   (3, 'gnis:SUMAS', 'Sumas River'), (4, 'gnis:39325', 'Fraser River');
      INSERT INTO item_section (ord, sid) VALUES (2, 10), (1, 11), (4, 12);`),
    conditions: (d) => d.exec(`
      INSERT INTO gauge (station, name, item_id, sid) VALUES
        ('08MH001', 'CHILLIWACK RIVER AT VEDDER CROSSING', 'gnis:8634', 11),
        ('08MF005', 'FRASER RIVER AT HOPE', 'gnis:39325', 12),
        ('UNNAMED', 'A CREEK', NULL, 10);`),
  };
  if (continuation)
    seed.water = (d) => d.exec(
      "INSERT INTO water_continue (ord, part, next_ord, next_part) VALUES (1, 0, 2, 0), (2, 3, 3, 0)");
  return memorySource(seed);
}

const ST = ["08MH001", "08MF005", "UNNAMED", "NOWHERE"] as StationId[];

describe("gaugeWaters", () => {
  it("the Vedder's section is one system with the Chilliwack and the Sumas; each station's water and reach", async () => {
    const w = await release().gaugeWaters(10 as SectionId, ST);
    expect(w.waters).toEqual(["gnis:3062", "gnis:8634", "gnis:SUMAS"]);
    expect([...w.gauges].sort(([a], [b]) => a.localeCompare(b))).toEqual([
      ["08MF005", { item: "gnis:39325", section: 12 }],
      ["08MH001", { item: "gnis:8634", section: 11 }],
      ["UNNAMED", { item: null, section: 10 }],
    ]);
  });

  it("transitively: from the Chilliwack, the Sumas is two links away and still the same system", async () => {
    const w = await release().gaugeWaters(11 as SectionId, []);
    expect(w.waters).toEqual(["gnis:3062", "gnis:8634", "gnis:SUMAS"]);
  });

  it("a release with no water_continue: each water is its own system", async () => {
    const w = await release(false).gaugeWaters(10 as SectionId, ST);
    expect(w.waters).toEqual(["gnis:3062"]);
  });

  it("a section of no water has no waters; no stations asks nothing of the gauges", async () => {
    const w = await release().gaugeWaters(99 as SectionId, []);
    expect(w).toEqual({ waters: [], gauges: new Map() });
  });
});
