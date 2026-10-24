/**
 * (T168) `gaugeWaters`: a section's waters — its own item and every water joined to it across `water_continue`, both
 * ways and transitively — and each station's own water and reach. Against a small bundle built in memory with the
 * bundle's own column names, since the development slice holds no continuation and no matched gauge. Here in tools/
 * because only tools and the drivers may name node:sqlite (layers.json).
 *
 * Mutations (each run 2026-10-24, each failed here and was reverted):
 *   - `continueLinks` without its `next_ord` half: the Vedder (the link's far end) no longer reaches the Chilliwack.
 *   - `continuedOrds` stopping after one hop: the Sumas (two links away) is lost.
 */
import { describe, expect, it } from "vitest";
import { DatabaseSync } from "node:sqlite";
import type { SectionId, StationId } from "@app/data";
import { makeBundleSource } from "@app/data/bundle";

type Db = Parameters<typeof makeBundleSource>[0];
type Row = Record<string, unknown>;

function bundle(continuation = true): Db {
  const db = new DatabaseSync(":memory:");
  db.exec(`CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT);
           CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT, name TEXT);
           CREATE TABLE item_section (ord INTEGER NOT NULL, sid INTEGER NOT NULL);
           CREATE TABLE gauge (station TEXT PRIMARY KEY, name TEXT, item_id TEXT, sid INTEGER);
           INSERT INTO item VALUES (1, 'gnis:8634', 'Chilliwack River'), (2, 'gnis:3062', 'Vedder River'),
                                   (3, 'gnis:SUMAS', 'Sumas River'), (4, 'gnis:39325', 'Fraser River');
           INSERT INTO item_section VALUES (2, 10), (1, 11), (4, 12);
           INSERT INTO gauge VALUES ('08MH001', 'CHILLIWACK RIVER AT VEDDER CROSSING', 'gnis:8634', 11),
                                    ('08MF005', 'FRASER RIVER AT HOPE', 'gnis:39325', 12),
                                    ('UNNAMED', 'A CREEK', NULL, 10);`);
  if (continuation)
    db.exec(`CREATE TABLE water_continue (ord INTEGER PRIMARY KEY, part INTEGER, next_ord INTEGER UNIQUE,
                                          next_part INTEGER);
             INSERT INTO water_continue VALUES (1, 0, 2, 0), (2, 3, 3, 0);`);
  return {
    all: async (sql, ...p) => db.prepare(sql).all(...(p as never[])) as unknown as Row[],
    get: async (sql, ...p) => db.prepare(sql).get(...(p as never[])) as unknown as Row | undefined,
    close: () => db.close(),
  };
}

const ST = ["08MH001", "08MF005", "UNNAMED", "NOWHERE"] as StationId[];

describe("gaugeWaters", () => {
  it("the Vedder's section is one system with the Chilliwack and the Sumas; each station's water and reach", async () => {
    const w = await makeBundleSource(bundle()).gaugeWaters(10 as SectionId, ST);
    expect(w.waters).toEqual(["gnis:3062", "gnis:8634", "gnis:SUMAS"]);
    expect([...w.gauges].sort(([a], [b]) => a.localeCompare(b))).toEqual([
      ["08MF005", { item: "gnis:39325", section: 12 }],
      ["08MH001", { item: "gnis:8634", section: 11 }],
      ["UNNAMED", { item: null, section: 10 }],
    ]);
  });

  it("transitively: from the Chilliwack, the Sumas is two links away and still the same system", async () => {
    const w = await makeBundleSource(bundle()).gaugeWaters(11 as SectionId, []);
    expect(w.waters).toEqual(["gnis:3062", "gnis:8634", "gnis:SUMAS"]);
  });

  it("a bundle cut before water_continue: each water is its own system", async () => {
    const w = await makeBundleSource(bundle(false)).gaugeWaters(10 as SectionId, ST);
    expect(w.waters).toEqual(["gnis:3062"]);
  });

  it("a section of no water has no waters; no stations asks nothing of the gauges", async () => {
    const w = await makeBundleSource(bundle()).gaugeWaters(99 as SectionId, []);
    expect(w).toEqual({ waters: [], gauges: new Map() });
  });
});
