/**
 * (T1011) WHICH WATERS GET A PAGE, the SQL half of the one rule (`PAGE_ORDS_SQL`): `item.outside_bc` counts the water's
 * sections outside B.C. A water partly outside (the Columbia: 2 of its 140 sections) gets a page; one wholly outside does
 * not; an unshown water, or one with no keyed part, never does. Read as a flag, the first case was dropped (156 waters).
 */
import { describe, expect, it } from "vitest";
import { openInMemory } from "../sql/drivers/node";
import { PAGE_ORDS_SQL } from "./pageable";

const db = () => openInMemory("", `
  CREATE TABLE item (item_ord INTEGER PRIMARY KEY, shown INTEGER NOT NULL, sections INTEGER NOT NULL, outside_bc INTEGER NOT NULL);
  CREATE TABLE part (item_ord INTEGER NOT NULL, part_ix INTEGER NOT NULL, akey INTEGER, ord INTEGER);
  INSERT INTO item VALUES (1, 1, 140, 0), (2, 1, 140, 2), (3, 1, 3, 3), (4, 0, 9, 0), (5, 1, 9, 0), (6, 1, 2, 1);
  INSERT INTO part VALUES (1, 0, 7, 0), (2, 0, 7, 0), (2, 1, NULL, NULL), (3, 0, 7, 0), (4, 0, 7, 0), (5, 0, NULL, NULL), (6, 0, 7, 0);`);

describe("(T1011) PAGE_ORDS_SQL: outside_bc is a count of sections", () => {
  it("in B.C. and partly outside get a page; wholly outside, unshown or keyless do not", async () => {
    const ords = (await db().all(PAGE_ORDS_SQL)).map((r) => Number(r.ord));
    // 1: all in B.C.; 2: 2 of 140 outside (the Columbia); 6: 1 of 2 outside — 3: 3 of 3 outside; 4: not shown; 5: no keyed part
    expect(ords).toEqual([1, 2, 6]);
  });
});
