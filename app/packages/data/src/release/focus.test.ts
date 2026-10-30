/**
 * (T256 item 1) Which reads a slice may answer, read off their SQL (`sliceKeys`) — pinned on the app's own queries — and
 * `covers` on a hand-cut slice. A read the table does not know (search, counts, a scan) is the whole file's.
 */
import { describe, expect, it } from "vitest";
import type { Db } from "../sql/db";
import { openInMemory } from "../sql/drivers/node";
import * as B from "../sql/queries";
import { covers, sliceKeys } from "./focus";
import { CROSS, RELEASE_QUERIES as R } from "./queries";

describe("sliceKeys", () => {
  it("names the keys of the reads a tap makes", () => {
    expect(sliceKeys("core", R.ITEM!.sql, ["gnis:1"])).toEqual({ items: ["gnis:1"] });
    expect(sliceKeys("core", R.ITEM_FOR_SECTION!.sql, [4097])).toEqual({ sids: [4097] });
    expect(sliceKeys("core", R.SECTIONS_FOR_ITEM!.sql, ["gnis:1"])).toEqual({ items: ["gnis:1"] });
    expect(sliceKeys("core", CROSS.ITEM_ORD.sql, ["gnis:1"])).toEqual({ items: ["gnis:1"] });
    expect(sliceKeys("core", CROSS.GEO_ORDS.sql, ["wbk:2"])).toEqual({ items: ["wbk:2"] });
    expect(sliceKeys("core", CROSS.ITEMS_FOR_ORDS(2).sql, [3, 4])).toEqual({ ords: [3, 4] });
    expect(sliceKeys("core", CROSS.NEAR_TOWNS_FOR_ORDS(2).sql, [3, 4])).toEqual({ ords: [3, 4] });
    expect(sliceKeys("core", CROSS.PLACES(1).sql, [7])).toEqual({ places: [7] });
    expect(sliceKeys("core", CROSS.ITEMS_FOR_SECTIONS(2).sql, [1, 2])).toEqual({ sids: [1, 2] });
    expect(sliceKeys("core", CROSS.NAME_SOURCES_FOR_ITEMS(1).sql, ["a"])).toEqual({ items: ["a"] });
    expect(sliceKeys("water", CROSS.CONTINUE_LINKS(2).sql, [3, 4, 3, 4])).toEqual({ ords: [3, 4] });
    expect(sliceKeys("water", CROSS.AKEYS_FOR_ORD.sql, [3])).toEqual({ ords: [3] });
    // the store's reads (regs/reads.ts)
    expect(sliceKeys("core", "SELECT ord FROM item WHERE item_id = ?", ["x"])).toEqual({ items: ["x"] });
    expect(sliceKeys("water", "SELECT part_ix, akey, ord, (SELECT j FROM xpart WHERE id = part.xp) AS xj FROM part " +
                              "WHERE item_ord = ? ORDER BY part_ix", [9])).toEqual({ ords: [9] });
    expect(sliceKeys("water", "SELECT id, text FROM str WHERE id IN (?, ?)", [1, 2])).toEqual({ strs: [1, 2] });
    expect(sliceKeys("water", "SELECT part_ix FROM part_section WHERE ord = ? AND sid = ?", [9, 77])).toEqual({ ords: [9] });
    expect(sliceKeys("water", "SELECT next_part AS part, ord AS other FROM water_continue WHERE next_ord = ?", [9]))
      .toEqual({ ords: [9] });
  });

  it("reads of tables a slice holds whole name no keys", () => {
    expect(sliceKeys("water", "SELECT r.rix AS rix, r.j AS e, d.j AS f FROM rule r LEFT JOIN display_rule d ON d.id = r.rix", []))
      .toEqual({});
    expect(sliceKeys("water", "SELECT j FROM static WHERE k = ?", ["rows.strips"])).toEqual({});
    expect(sliceKeys("water", "SELECT akey, region FROM akey_region ORDER BY akey, region", [])).toEqual({});
    expect(sliceKeys("water", "SELECT k, v FROM meta", [])).toEqual({});
  });

  it("anything else is the whole file's: search, counts, scans, tables or columns no slice is keyed by", () => {
    expect(sliceKeys("core", R.SEARCH!.sql, ["%a%", 10])).toBeNull();
    expect(sliceKeys("core", CROSS.COUNTS.sql, [])).toBeNull();
    expect(sliceKeys("core", CROSS.INDEX_ITEMS.sql, [])).toBeNull();
    expect(sliceKeys("core", CROSS.INDEX_AREA.sql, [])).toBeNull();
    expect(sliceKeys("core", CROSS.INDEX_NEAR.sql, [])).toBeNull();
    expect(sliceKeys("core", R.SEARCH_POINTERS!.sql, ["a", 1])).toBeNull();
    expect(sliceKeys("water", "SELECT sid FROM part_section", [])).toBeNull();
    expect(sliceKeys("water", "SELECT e, id FROM entry", [])).toBeNull();
    expect(sliceKeys("core", "SELECT ord FROM item WHERE name = ?", ["x"])).toBeNull();
    expect(sliceKeys("water", "SELECT j FROM xpart WHERE id = ?", [1])).toBeNull();      // follows a part, never alone
    expect(sliceKeys("core", B.ITEM, [null])).toBeNull();
  });
});

// (layers.json: data may not name node:sqlite; the node driver layer opens it)
const db = (sql: string): Db => openInMemory(sql, "");

describe("covers", () => {
  const bits = new Uint8Array(512);
  bits[10 >> 3]! |= 1 << (10 & 7);              // sid 10 (block 0) is the region's
  const core = db(`CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT);
                   CREATE TABLE item_section (sid INTEGER, ord INTEGER);
                   CREATE TABLE place (place_id INTEGER PRIMARY KEY);
                   CREATE TABLE cover_sid (block INTEGER PRIMARY KEY, b BLOB);
                   INSERT INTO item VALUES (1, 'gnis:1'); INSERT INTO item_section VALUES (10, 1), (5000, 1);
                   INSERT INTO place VALUES (7);
                   INSERT INTO cover_sid VALUES (0, x'${Buffer.from(bits).toString("hex")}');`);
  it("a key present is covered; one absent is the whole file's", async () => {
    expect(await covers("core", core, { ords: [1], items: ["gnis:1"], places: [7] })).toBe(true);
    expect(await covers("core", core, { ords: [1, 2] })).toBe(false);
    expect(await covers("core", core, { items: ["gnis:9"] })).toBe(false);
    expect(await covers("core", core, {})).toBe(true);
  });
  it("a section: the region's (bit set, even with no water), or one a held water holds", async () => {
    expect(await covers("core", core, { sids: [10] })).toBe(true);
    expect(await covers("core", core, { sids: [5000] })).toBe(true);    // outside the region, its water held
    expect(await covers("core", core, { sids: [11] })).toBe(false);     // outside, no held water: maybe another's
  });
  it("a key a file is never keyed by is not covered", async () => {
    expect(await covers("core", core, { strs: [1] })).toBe(false);
  });
});
