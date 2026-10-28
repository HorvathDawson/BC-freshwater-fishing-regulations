/**
 * T224 (owner 2026-10-28): A NAME SAYS WHERE IT CAME FROM. core's `name_source` holds, for every shown name and alias
 * that is not the official (gazetted) one, its source's words; search puts them on the row ("also Lonzo Creek (North
 * Fork) · ECCC hydrometric station name") and a water carries its shown name's. A core cut before the table: nothing.
 */
import { describe, expect, it } from "vitest";
import { memoryRelease, type MemorySeed } from "../sql/drivers/memoryRelease";
import { makeReleaseSource } from "./source";

const seed = (withTable: boolean): MemorySeed => ({
  core: (d) => {
    d.exec("INSERT INTO item (ord, item_id, name, kind, pieces, area_ha) VALUES " +
           "(1, 'wsc:100-064535', 'Kilgard Creek', 'stream', 2, NULL), (2, 'gnis:3', 'Kilgard Lake', 'lake', 1, 4)");
    d.exec("INSERT INTO alias (item_id, alias) VALUES ('wsc:100-064535', 'Lonzo Creek (North Fork)')");
    if (withTable) {
      d.exec("INSERT INTO name_source (item_id, name, src, label) VALUES " +
             "('wsc:100-064535', 'Kilgard Creek', 'municipal', 'City of Abbotsford stream layer'), " +
             "('wsc:100-064535', 'Lonzo Creek (North Fork)', 'gauge', 'ECCC hydrometric station name')");
    } else d.exec("DROP TABLE name_source");
  },
  near: () => {},
});

describe("searchNames + water: a name's source", () => {
  it("the shown name's and the matched alias's source; an official name has none", async () => {
    const src = makeReleaseSource(memoryRelease(seed(true)));
    const kil = await src.searchNames("kilgard", 10);
    const by = new Map(kil.map((h) => [h.item, h]));
    expect(by.get("wsc:100-064535" as never)?.nameSource).toBe("City of Abbotsford stream layer");
    expect(by.get("gnis:3" as never)?.nameSource).toBeNull();
    const lon = await src.searchNames("lonzo", 10);
    expect(lon.map((h) => [h.matchedAs, h.matchedSource])).toEqual(
      [["Lonzo Creek (North Fork)", "ECCC hydrometric station name"]]);
    expect((await src.water("wsc:100-064535" as never))?.nameSource).toBe("City of Abbotsford stream layer");
    expect((await src.water("gnis:3" as never))?.nameSource).toBeNull();
  });

  it("a core cut before name_source: the same hits, no source", async () => {
    const src = makeReleaseSource(memoryRelease(seed(false)));
    const hits = await src.searchNames("lonzo", 10);
    expect(hits.map((h) => [h.matchedAs, h.matchedSource ?? null])).toEqual([["Lonzo Creek (North Fork)", null]]);
  });
});
