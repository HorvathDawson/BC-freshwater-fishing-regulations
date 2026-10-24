/**
 * A LAKE CUT INTO PARTS IS FOUND THROUGH ITS PARTS, NEVER AS THE WHOLE (user ruling 2026-10-03:
 * "a lake fully covered by its parts is not searchable as a whole — search finds the parts").
 *
 * The whole owns no section (`item.part_of` names it; the production bundler refuses a whole that keeps one), so its
 * `item.pieces` is 0, and the release's `SEARCH` asks only for waters with a piece — in BOTH halves of its union, the
 * name match and the alias match. Built in memory from the release contract, because no committed fixture has such a
 * lake.
 */
import { describe, expect, it } from "vitest";
import type { DatabaseSync } from "node:sqlite";
import { memoryRelease, memorySource } from "./memoryRelease";
import * as R from "../../release/queries";
import * as Q from "../queries";

const seed = (d: DatabaseSync) => d.exec(`
  INSERT INTO item (ord, item_id, name, kind, part_of, name_key, pieces) VALUES
    (0, 'wbk:1', 'Kootenay Lake', 'lake', NULL, 'kootenay lake', 0),
    (1, 'wbk:-20', 'Kootenay Lake — Main Body', 'lake', 'wbk:1', 'kootenay lake — main body', 1),
    (2, 'wbk:-21', 'Kootenay Lake — Upper West Arm', 'lake', 'wbk:1', 'kootenay lake — upper west arm', 1);
  INSERT INTO alias (item_id, alias, alias_key) VALUES ('wbk:1', 'Kootenai Lake', 'kootenai lake');
  INSERT INTO item_section (ord, sid) VALUES (1, 1), (2, 2);`);

describe("a lake cut into parts", () => {
  it("is found through its parts, never as the whole", async () => {
    const s = memorySource({ core: seed, near: () => {} });
    const byName = (await s.searchNames("Kootenay", 10)).map((h) => h.item);
    expect(byName.sort()).toEqual(["wbk:-20", "wbk:-21"]);
    // the whole's alias finds nothing either: it is not a water a reader can open
    expect(await s.searchNames("Kootenai", 10)).toEqual([]);
  });

  it("MUTATION: the has-a-piece clause is what keeps the whole out", async () => {
    const clause = "WHERE i.pieces > 0 AND ";
    expect(R.SEARCH.split(clause).length).toBe(3);          // once per half of the union
    const loose = R.SEARCH.replaceAll(clause, "WHERE ");
    const core = await memoryRelease({ core: seed }).db("core");
    const rows = await core.all(loose, Q.nameNeedle("Kootenay"), 10);
    expect(rows.map((r) => r.item_id)).toContain("wbk:1");
  });
});
