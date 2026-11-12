/**
 * (T672, T462 audit G11) NO LICENCE TAB SAYS THE SAME BOX TWICE, on the release under test: for every answer key that
 * holds a classified-water designation (one part of each), the Licence tab's boxes are all different. Two designation
 * records that word the same box (one licence unit printed on two rows: the Elk's tributaries, the Stellako) drew it
 * twice on 59 parts. Asked of the whole release, so a new row that repeats a unit is covered with no edit here.
 */
import { describe, expect, it } from "vitest";
import { buildRegsPage, licenceBlockLines, tapDay } from "@app/core";
import { loadRegsInputs, openRegsStore } from "@app/data/regs";
import { liveRelease, openReleaseDir } from "../fixtureRelease";

const LIVE = liveRelease();
const DAY = { month: 7, day: 15 }, YEAR = 2026;

describe.skipIf(!LIVE)(`T672 a licence box is said once (${LIVE ?? "no live release"})`, () => {
  it("every key with a classified-water designation: no two boxes of its Licence tab read the same", async () => {
    const loader = openReleaseDir(LIVE!);
    const store = await openRegsStore(loader, { tiles: null });
    const water = await loader.db("water");
    // one part of each key that has a licensing set (a key without one holds no designation)
    const rows = await water.all("SELECT s.text AS id, MIN(p.part_ix) AS part, p.akey AS akey FROM part p JOIN akey a ON a.akey = p.akey " +
      "JOIN item i ON i.item_ord = p.item_ord JOIN str s ON s.id = i.item_id WHERE a.licensing_set IS NOT NULL AND a.tidal = 0 " +
      "AND p.item_ord = (SELECT MIN(q.item_ord) FROM part q WHERE q.akey = p.akey) GROUP BY p.akey ORDER BY p.akey");
    const twice: string[] = [];
    let boxed = 0;
    for (const r of rows) {
      const inp = await loadRegsInputs(store, { kind: "part", item: String(r.id), partIx: Number(r.part) }, DAY, tapDay(DAY, YEAR).weekday, YEAR);
      const page = buildRegsPage(inp, { today: DAY, who: { age: "adult", residency: "resident" } });
      const boxes = (page.licence?.blocks ?? []).filter((b) => b.cls === "classbox").map((b) => licenceBlockLines(b).join(" "));
      if (boxes.length) boxed++;
      for (const b of new Set(boxes.filter((x, i) => boxes.indexOf(x) !== i))) twice.push(`${page.header.name}: ${b}`);
    }
    expect(twice).toEqual([]);
    expect(boxed, "the release has pages with a class box").toBeGreaterThan(0);
  }, 120_000);
});
