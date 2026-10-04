/**
 * A LAKE CUT INTO PARTS IS FOUND THROUGH ITS PARTS, NEVER AS THE WHOLE (user ruling 2026-10-03:
 * "a lake fully covered by its parts is not searchable as a whole — search finds the parts").
 *
 * In the bundle the whole owns no section (`item.part_of` names it; the production bundler refuses
 * a whole that keeps one), and `SEARCH` asks only for waters that own a section — in BOTH halves of
 * its union, the name match and the alias match. Built in memory from the schema, because the
 * development fixture is one valley and has no such lake.
 */
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { openInMemory } from "./node";
import { makeBundleSource } from "../source";
import * as Q from "../queries";

const SCHEMA = readFileSync(new URL("../../../../../../pipeline/deliver/bundle/schema.sql",
                                    import.meta.url), "utf8");
const SEED = `
  INSERT INTO item (ord, item_id, name, kind, part_of) VALUES
    (0, 'wbk:1', 'Kootenay Lake', 'lake', NULL),
    (1, 'wbk:-20', 'Kootenay Lake — Main Body', 'lake', 'wbk:1'),
    (2, 'wbk:-21', 'Kootenay Lake — Upper West Arm', 'lake', 'wbk:1');
  INSERT INTO alias VALUES ('wbk:1', 'Kootenai Lake');
  INSERT INTO item_section (ord, sid) VALUES (1, 1), (2, 2);`;

describe("a lake cut into parts", () => {
  it("is found through its parts, never as the whole", async () => {
    const db = openInMemory(SCHEMA, SEED);
    const s = makeBundleSource(db);
    const byName = (await s.searchNames("Kootenay", 10)).map((h) => h.item);
    expect(byName.sort()).toEqual(["wbk:-20", "wbk:-21"]);
    // the whole's alias finds nothing either: it is not a water a reader can open
    expect(await s.searchNames("Kootenai", 10)).toEqual([]);
    db.close?.();
  });

  it("MUTATION: the owns-a-section clause is what keeps the whole out", async () => {
    const db = openInMemory(SCHEMA, SEED);
    const clause = "AND EXISTS (SELECT 1 FROM item_section s WHERE s.ord = i.ord)";
    expect(Q.SEARCH.split(clause).length).toBe(3);          // once per half of the union
    const loose = Q.SEARCH.replaceAll(clause, "");
    const rows = await db.all(Q.withArea(loose, true), "Kootenay", 10);
    expect(rows.map((r) => r.item_id)).toContain("wbk:1");
    db.close?.();
  });
});
