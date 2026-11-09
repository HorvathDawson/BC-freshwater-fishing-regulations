/**
 * P15, store-free: the water map's two reads over an in-memory core + water — `siblingOrds` (a split lake's closure
 * areas, from one of them or from the lake itself) and `navRows` (microdegrees as degrees, a part with no box, a mark's
 * upper parts, the exit's receiving water named from core). The release's own rows: conformance/src/regs/waterGroup.
 */
import { describe, expect, it } from "vitest";
import { openInMemory } from "../sql/drivers/node";
import { markLabel, partName } from "@app/core";
import { makeReads, type ReadBase } from "./reads";
import { loadRawWaterGroup, type GroupStore } from "./waterGroup";

const core = openInMemory(`
  CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT NOT NULL UNIQUE, name TEXT NOT NULL, kind TEXT, part_of TEXT);
  CREATE INDEX item_by_parent ON item(part_of) WHERE part_of IS NOT NULL;`, `
  INSERT INTO item VALUES (1, 'gnis:1', 'River', 'stream', NULL), (2, 'gnis:2', 'Receiving River', 'stream', NULL),
    (10, 'wbk:9', 'Lake', 'lake', NULL), (11, 'wbk:-1', 'Lake — North', 'lake', 'wbk:9'),
    (12, 'wbk:-2', 'Lake — South', 'lake', 'wbk:9');`);
const water = openInMemory(`
  CREATE TABLE part_geo (ord INTEGER NOT NULL, part_ix INTEGER NOT NULL, mid_sid INTEGER NOT NULL,
    mid_lon INTEGER NOT NULL, mid_lat INTEGER NOT NULL, w INTEGER, s INTEGER, e INTEGER, n INTEGER, PRIMARY KEY (ord, part_ix));
  CREATE TABLE part_mark (ord INTEGER NOT NULL, mark_ix INTEGER NOT NULL, lon INTEGER NOT NULL, lat INTEGER NOT NULL,
    token TEXT, down_part INTEGER NOT NULL, up_parts TEXT NOT NULL, confidence TEXT, alt TEXT, PRIMARY KEY (ord, mark_ix));
  CREATE TABLE part_section (ord INTEGER NOT NULL, sid INTEGER NOT NULL, part_ix INTEGER NOT NULL, PRIMARY KEY (ord, sid));
  CREATE TABLE water_exit (ord INTEGER PRIMARY KEY, down_ord INTEGER NOT NULL, down_part INTEGER);
  CREATE TABLE water_continue (ord INTEGER PRIMARY KEY, part INTEGER NOT NULL, next_ord INTEGER NOT NULL UNIQUE,
    next_part INTEGER NOT NULL);`, `
  INSERT INTO part_geo VALUES (1, 1, 7, -121500000, 49250000, -121600000, 49200000, -121400000, 49300000),
                              (1, 0, 9, -121000000, 49000000, NULL, NULL, NULL, NULL);
  INSERT INTO part_mark VALUES (1, 0, -121450000, 49280000, 'split:bridge', 1, '[0, 2]', NULL, NULL),
                               (1, 1, -121, 49, NULL, 0, '[2]', 'low', NULL);
  INSERT INTO part_section VALUES (1, 9, 0), (1, 7, 1), (1, 8, 1);
  INSERT INTO water_exit VALUES (1, 2, 3), (2, 10, NULL);
  INSERT INTO water_continue VALUES (1, 1, 2, 0), (12, 4, 1, 0);`);
const reads = () => makeReads({ core: async () => core, water: async () => water, geo: async () => water } as unknown as ReadBase);
describe("the water map's reads", () => {
  it("siblingOrds: a closure area -> its lake's areas; the lake itself -> itself and its areas; a river -> itself", async () => {
    const r = reads();
    expect(await Promise.all([11, 12, 10, 1].map((o) => r.siblingOrds(o)))).toEqual([[11, 12], [11, 12], [10, 11, 12], [1]]);
  });

  it("navRows: degrees in river order (`part_ix`), a part with no box, a mark's upper parts and confidence, the exit named from core", async () => {
    expect(await reads().navRows(1)).toEqual({
      geo: [{ partIx: 0, midSid: 9, mid: [-121, 49], box: null },
            { partIx: 1, midSid: 7, mid: [-121.5, 49.25], box: [-121.6, 49.2, -121.4, 49.3] }],
      marks: [{ lon: -121.45, lat: 49.28, token: "split:bridge", down: 1, up: [0, 2] },
              { lon: -0.000121, lat: 0.000049, token: null, down: 0, up: [2], confidence: "low" }],
      sections: [{ sid: 7, partIx: 1 }, { sid: 8, partIx: 1 }, { sid: 9, partIx: 0 }],
      exit: { ord: 2, part: 3, item: "gnis:2", name: "Receiving River" },
      continuesAs: { ord: 2, part: 1, otherPart: 0, item: "gnis:2", name: "Receiving River" },
      continuesFrom: { ord: 12, part: 0, otherPart: 4, item: "wbk:-2", name: "Lake — South" },
    });
    // a receiving water met at no stored part: its ord and no part; none stored: null
    expect([(await reads().navRows(2)).exit, (await reads().navRows(10)).exit]).toEqual([
      { ord: 10, part: null, item: "wbk:9", name: "Lake" }, null]);
  });

  it("an exit or a continuation into an item core does not hold is a damaged release, said — never an exit with no name", async () => {
    const bad = (sql: string) => openInMemory(`
      CREATE TABLE part_geo (ord INTEGER, part_ix INTEGER, mid_sid INTEGER, mid_lon INTEGER, mid_lat INTEGER, w INTEGER,
        s INTEGER, e INTEGER, n INTEGER);
      CREATE TABLE part_mark (ord INTEGER, mark_ix INTEGER, lon INTEGER, lat INTEGER, token TEXT, down_part INTEGER,
        up_parts TEXT, confidence TEXT, alt TEXT);
      CREATE TABLE part_section (ord INTEGER, sid INTEGER, part_ix INTEGER);
      CREATE TABLE water_exit (ord INTEGER PRIMARY KEY, down_ord INTEGER NOT NULL, down_part INTEGER);
      CREATE TABLE water_continue (ord INTEGER PRIMARY KEY, part INTEGER NOT NULL, next_ord INTEGER NOT NULL UNIQUE,
        next_part INTEGER NOT NULL);`, sql);
    const r = (w: ReturnType<typeof bad>) => makeReads({ core: async () => core, water: async () => w, geo: async () => w } as unknown as ReadBase);
    await expect(r(bad("INSERT INTO water_exit VALUES (1, 99, NULL);")).navRows(1))
      .rejects.toThrow("regs store: water 1's water_exit names item 99, which core does not hold");
    await expect(r(bad("INSERT INTO water_continue VALUES (98, 4, 1, 0);")).navRows(1))
      .rejects.toThrow("regs store: water 1's water_continue names item 98, which core does not hold");
  });

  it("navRows: `water_continue` both ways — the water it goes on as, the one it goes on from, none", async () => {
    const r = reads();
    expect([(await r.navRows(2)).continuesFrom, (await r.navRows(2)).continuesAs, (await r.navRows(10)).continuesAs,
            (await r.navRows(10)).continuesFrom]).toEqual([
      { ord: 1, part: 0, otherPart: 1, item: "gnis:1", name: "River" }, null, null, null]);
    expect((await r.navRows(12)).continuesAs).toEqual({ ord: 1, part: 4, otherPart: 0, item: "gnis:1", name: "River" });
  });

  it("T182: a split release's boxes are the geo file's (`base.geo`); the rest stays water's", async () => {
    const geo = openInMemory(`
      CREATE TABLE part_geo (ord INTEGER NOT NULL, part_ix INTEGER NOT NULL, mid_sid INTEGER NOT NULL,
        mid_lon INTEGER NOT NULL, mid_lat INTEGER NOT NULL, w INTEGER, s INTEGER, e INTEGER, n INTEGER,
        PRIMARY KEY (ord, part_ix));`, `
      INSERT INTO part_geo VALUES (1, 0, 9, -120000000, 50000000, NULL, NULL, NULL, NULL);`);
    const r = makeReads({ core: async () => core, water: async () => water, geo: async () => geo } as unknown as ReadBase);
    const nav = await r.navRows(1);
    expect([nav.geo, nav.sections.length]).toEqual([[{ partIx: 0, midSid: 9, mid: [-120, 50], box: null }], 3]);
  });
});

describe("(T517) the group's rows: an UNKEYED part is said as its water — the one deliberate name (owner 2026-11-09)", () => {
  it("a keyed part keeps its end tokens; an unkeyed one (no facts: outside B.C.) is its water's name; a tidal piece is left out", async () => {
    const facts = (lo: string, hi: string) => ({ order: 0, label: `${lo}-${hi}`, lo, hi, runs: "", place: null, km: 1,
                                                 closed_all_year: false, paper_licence: [] });
    const part = (partIx: number, f: ReturnType<typeof facts> | null, tidal = false) =>
      ({ itemOrd: 1, partIx, akey: f ? 1 : null, facts: f, sections: 1, grp: partIx, run: 0, tidal });
    const store: GroupStore = {
      itemOrd: async () => 1, siblingOrds: async () => [1],
      item: async () => ({ id: "gnis:1", name: "Blanchard River", kind: "stream" }) as never,
      parts: async () => [part(0, null), part(1, facts("bc", "bc2")), part(2, null, true)] as never,
      navRows: async () => ({ geo: [0, 1, 2].map((p) => ({ partIx: p, midSid: 10 + p, mid: [0, 0], box: null })),
                              marks: [{ lon: 0, lat: 0, token: "bc", down: 0, up: [1] },
                                      { lon: 1, lat: 0, token: "bc2", down: 2, up: [0] }],
                              sections: [0, 1, 2].map((p) => ({ sid: 10 + p, partIx: p })), exit: null,
                              continuesAs: null, continuesFrom: null }),
      statics: async () => ({ ends: { bc: ["B.C. border"], bc2: ["B.C. border (km 9)"] } }) as never,
    };
    const g = await loadRawWaterGroup(store, "gnis:1");
    expect(g.parts.map((_, i) => partName(g, i))).toEqual(["Blanchard River", "B.C. border → B.C. border (km 9)"]);
    expect(g.marks.map((m) => markLabel(g, m))).toEqual(["B.C. border"]);
  });
});
