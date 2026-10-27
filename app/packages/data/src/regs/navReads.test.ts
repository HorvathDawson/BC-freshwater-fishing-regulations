/**
 * P15, store-free: the water map's two reads over an in-memory core + water — `siblingOrds` (a split lake's closure
 * areas, from one of them or from the lake itself) and `navRows` (microdegrees as degrees, a part with no box, a mark's
 * upper parts, the exit's receiving water named from core). The release's own rows: conformance/src/regs/waterGroup.
 */
import { describe, expect, it } from "vitest";
import { openInMemory } from "../sql/drivers/node";
import { makeReads, type ReadBase } from "./reads";

const core = openInMemory(`
  CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT NOT NULL UNIQUE, name TEXT NOT NULL, kind TEXT, part_of TEXT);
  CREATE INDEX item_by_parent ON item(part_of) WHERE part_of IS NOT NULL;`, `
  INSERT INTO item VALUES (1, 'gnis:1', 'River', 'stream', NULL), (2, 'gnis:2', 'Receiving River', 'stream', NULL),
    (10, 'wbk:9', 'Lake', 'lake', NULL), (11, 'wbk:-1', 'Lake — North', 'lake', 'wbk:9'),
    (12, 'wbk:-2', 'Lake — South', 'lake', 'wbk:9');`);
const water = openInMemory(`
  CREATE TABLE part_geo (ord INTEGER NOT NULL, part_ix INTEGER NOT NULL, rank INTEGER NOT NULL, mid_sid INTEGER NOT NULL,
    mid_lon INTEGER NOT NULL, mid_lat INTEGER NOT NULL, w INTEGER, s INTEGER, e INTEGER, n INTEGER, PRIMARY KEY (ord, part_ix));
  CREATE TABLE part_mark (ord INTEGER NOT NULL, mark_ix INTEGER NOT NULL, lon INTEGER NOT NULL, lat INTEGER NOT NULL,
    label TEXT, down_part INTEGER NOT NULL, up_parts TEXT NOT NULL, PRIMARY KEY (ord, mark_ix));
  CREATE TABLE part_section (ord INTEGER NOT NULL, sid INTEGER NOT NULL, part_ix INTEGER NOT NULL, PRIMARY KEY (ord, sid));
  CREATE TABLE water_exit (ord INTEGER PRIMARY KEY, down_ord INTEGER NOT NULL, down_part INTEGER);
  CREATE TABLE water_continue (ord INTEGER PRIMARY KEY, part INTEGER NOT NULL, next_ord INTEGER NOT NULL UNIQUE,
    next_part INTEGER NOT NULL);`, `
  INSERT INTO part_geo VALUES (1, 1, 0, 7, -121500000, 49250000, -121600000, 49200000, -121400000, 49300000),
                              (1, 0, 1, 9, -121000000, 49000000, NULL, NULL, NULL, NULL);
  INSERT INTO part_mark VALUES (1, 0, -121450000, 49280000, 'The bridge', 1, '[0, 2]'), (1, 1, -121, 49, NULL, 0, '[2]');
  INSERT INTO part_section VALUES (1, 9, 0), (1, 7, 1), (1, 8, 1);
  INSERT INTO water_exit VALUES (1, 2, 3), (2, 99, NULL);
  INSERT INTO water_continue VALUES (1, 1, 2, 0), (98, 4, 1, 0);`);
const reads = () => makeReads({ core: async () => core, water: async () => water } as unknown as ReadBase);
/** A release cut before `water_continue` (r-1bb06fa28daf and older): the same tables without it. */
const older = openInMemory(`
  CREATE TABLE part_geo (ord INTEGER NOT NULL, part_ix INTEGER NOT NULL, rank INTEGER NOT NULL, mid_sid INTEGER NOT NULL,
    mid_lon INTEGER NOT NULL, mid_lat INTEGER NOT NULL, w INTEGER, s INTEGER, e INTEGER, n INTEGER, PRIMARY KEY (ord, part_ix));
  CREATE TABLE part_mark (ord INTEGER NOT NULL, mark_ix INTEGER NOT NULL, lon INTEGER NOT NULL, lat INTEGER NOT NULL,
    label TEXT, down_part INTEGER NOT NULL, up_parts TEXT NOT NULL, PRIMARY KEY (ord, mark_ix));
  CREATE TABLE part_section (ord INTEGER NOT NULL, sid INTEGER NOT NULL, part_ix INTEGER NOT NULL, PRIMARY KEY (ord, sid));
  CREATE TABLE water_exit (ord INTEGER PRIMARY KEY, down_ord INTEGER NOT NULL, down_part INTEGER);`, "");

describe("the water map's reads", () => {
  it("siblingOrds: a closure area -> its lake's areas; the lake itself -> itself and its areas; a river -> itself", async () => {
    const r = reads();
    expect(await Promise.all([11, 12, 10, 1].map((o) => r.siblingOrds(o)))).toEqual([[11, 12], [11, 12], [10, 11, 12], [1]]);
  });

  it("navRows: degrees, a part with no box, a mark's upper parts, the exit named from core", async () => {
    expect(await reads().navRows(1)).toEqual({
      geo: [{ partIx: 1, rank: 0, midSid: 7, mid: [-121.5, 49.25], box: [-121.6, 49.2, -121.4, 49.3] },
            { partIx: 0, rank: 1, midSid: 9, mid: [-121, 49], box: null }],
      marks: [{ lon: -121.45, lat: 49.28, label: "The bridge", down: 1, up: [0, 2] },
              { lon: -0.000121, lat: 0.000049, label: null, down: 0, up: [2] }],
      sections: [{ sid: 7, partIx: 1 }, { sid: 8, partIx: 1 }, { sid: 9, partIx: 0 }],
      stretches: [],
      exit: { ord: 2, part: 3, item: "gnis:2", name: "Receiving River" },
      continuesAs: { ord: 2, part: 1, otherPart: 0, item: "gnis:2", name: "Receiving River" },
      continuesFrom: { ord: 98, part: 0, otherPart: 4, item: null, name: null },
    });
    // a receiving water core does not hold: its ord and part, no id or name (no exit to open); none stored: null
    expect([(await reads().navRows(2)).exit, (await reads().navRows(10)).exit]).toEqual([
      { ord: 99, part: null, item: null, name: null }, null]);
  });

  it("navRows: `water_continue` both ways — the water it goes on as, the one it goes on from, none", async () => {
    const r = reads();
    expect([(await r.navRows(2)).continuesFrom, (await r.navRows(2)).continuesAs, (await r.navRows(10)).continuesAs,
            (await r.navRows(10)).continuesFrom]).toEqual([
      { ord: 1, part: 0, otherPart: 1, item: "gnis:1", name: "River" }, null, null, null]);
  });

  it("T182: a split release's boxes are the geo file's (`base.geo`); the rest stays water's", async () => {
    const geo = openInMemory(`
      CREATE TABLE part_geo (ord INTEGER NOT NULL, part_ix INTEGER NOT NULL, rank INTEGER NOT NULL, mid_sid INTEGER NOT NULL,
        mid_lon INTEGER NOT NULL, mid_lat INTEGER NOT NULL, w INTEGER, s INTEGER, e INTEGER, n INTEGER,
        PRIMARY KEY (ord, part_ix));`, `
      INSERT INTO part_geo VALUES (1, 0, 0, 9, -120000000, 50000000, NULL, NULL, NULL, NULL);`);
    const r = makeReads({ core: async () => core, water: async () => water, geo: async () => geo } as unknown as ReadBase);
    const nav = await r.navRows(1);
    expect([nav.geo, nav.sections.length]).toEqual([[{ partIx: 0, rank: 0, midSid: 9, mid: [-120, 50], box: null }], 3]);
  });

  it("T204: a part's stretches from geo's `part_stretch`, and each mark's stretches from `part_mark`", async () => {
    const geo = openInMemory(`
      CREATE TABLE part_geo (ord INTEGER NOT NULL, part_ix INTEGER NOT NULL, rank INTEGER NOT NULL, mid_sid INTEGER NOT NULL,
        mid_lon INTEGER NOT NULL, mid_lat INTEGER NOT NULL, w INTEGER, s INTEGER, e INTEGER, n INTEGER,
        PRIMARY KEY (ord, part_ix));
      CREATE TABLE part_stretch (ord INTEGER NOT NULL, part_ix INTEGER NOT NULL, stretch_ix INTEGER NOT NULL,
        rank INTEGER NOT NULL, mid_sid INTEGER NOT NULL, mid_lon INTEGER NOT NULL, mid_lat INTEGER NOT NULL, w INTEGER,
        s INTEGER, e INTEGER, n INTEGER, sids TEXT NOT NULL, PRIMARY KEY (ord, part_ix, stretch_ix));`, `
      INSERT INTO part_stretch VALUES (5, 0, 1, 2, 9, -122000000, 49000000, -122100000, 48900000, -121900000, 49100000, '[9, 10]'),
                                      (5, 0, 0, 0, 8, -123000000, 49000000, NULL, NULL, NULL, NULL, '[8]');`);
    const w5 = openInMemory(`
      CREATE TABLE part_mark (ord INTEGER NOT NULL, mark_ix INTEGER NOT NULL, lon INTEGER NOT NULL, lat INTEGER NOT NULL,
        label TEXT, down_part INTEGER NOT NULL, up_parts TEXT NOT NULL, confidence TEXT, down_stretch INTEGER,
        up_stretches TEXT, PRIMARY KEY (ord, mark_ix));
      CREATE TABLE part_section (ord INTEGER NOT NULL, sid INTEGER NOT NULL, part_ix INTEGER NOT NULL, PRIMARY KEY (ord, sid));
      CREATE TABLE water_exit (ord INTEGER PRIMARY KEY, down_ord INTEGER NOT NULL, down_part INTEGER);`, `
      INSERT INTO part_mark VALUES (5, 0, -1, 1, 'Bridge', 0, '[1]', NULL, NULL, NULL),
                                   (5, 1, -2, 2, 'Falls', 1, '[0]', 'low', 0, '[1]');`);
    const r = makeReads({ core: async () => core, water: async () => w5, geo: async () => geo } as unknown as ReadBase);
    const nav = await r.navRows(5);
    expect(nav.stretches).toEqual([
      { partIx: 0, stretch: 0, rank: 0, mid: [-123, 49], box: null, sids: [8] },
      { partIx: 0, stretch: 1, rank: 2, mid: [-122, 49], box: [-122.1, 48.9, -121.9, 49.1], sids: [9, 10] }]);
    expect(nav.marks.map((m) => [m.label, m.downStretch, m.upStretches, m.confidence])).toEqual([
      ["Bridge", undefined, undefined, undefined], ["Falls", 0, [1], "low"]]);
  });

  it("navRows on a release cut before `water_continue`: no continuation, everything else as before", async () => {
    const r = makeReads({ core: async () => core, water: async () => older } as unknown as ReadBase);
    expect(await r.navRows(1)).toEqual({ geo: [], marks: [], sections: [], stretches: [], exit: null, continuesAs: null,
                                         continuesFrom: null });
  });
});
