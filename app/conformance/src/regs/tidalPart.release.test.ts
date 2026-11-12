/**
 * (T672, T462 audit W1) EVERY TIDAL PIECE OF THE RELEASE UNDER TEST READS AS TIDAL WATER, in the words the release's own
 * tidal water page says — never "outside B.C.: no B.C. rules apply" (the Fraser below Mission is in B.C., under DFO's
 * tidal rules). Asked of the whole release, by the part's KIND (the store's `part.tidal`), so a release that makes more
 * creeks tidal is covered with no edit here.
 *
 * - every key-less tidal part's page: status "tidal", the "Tidal water" banner alone, only the Keep tab;
 * - its note IS the stored note of the release's keyed tidal water (the book's own tidal row), word for word: the app's
 *   copy (`TIDAL_SHEET_WORDS`) and the pipeline's `TIDAL_NOTE` cannot drift apart unseen;
 * - a search hit on a water that is tidal throughout opens that page (`targetFor` with no section);
 * - a key-less part the store does not mark tidal still reads outside B.C. (the flag decides, not the missing key).
 */
import { describe, expect, it } from "vitest";
import { buildRegsPage, tapDay, TIDAL_SHEET_WORDS, type RegsPage } from "@app/core";
import { loadRegsInputs, openRegsStore, targetFor } from "@app/data/regs";
import { liveRelease, openReleaseDir } from "../fixtureRelease";

const LIVE = liveRelease();
const DAY = { month: 7, day: 15 }, YEAR = 2026, WHO = { age: "adult", residency: "resident" } as const;
const OUTSIDE = "This part of the water is outside B.C.: no B.C. rules apply.";

describe.skipIf(!LIVE)(`T672 tidal pieces read as tidal water (${LIVE ?? "no live release"})`, () => {
  const open = async () => {
    const loader = openReleaseDir(LIVE!);
    const store = await openRegsStore(loader, { tiles: null });
    const water = await loader.db("water");
    const items = (await water.all("SELECT s.text AS id, i.item_ord AS ord FROM item i JOIN str s ON s.id = i.item_id " +
      "WHERE EXISTS (SELECT 1 FROM part p WHERE p.item_ord = i.item_ord AND p.akey IS NULL) ORDER BY i.item_ord"))
      .map((r) => ({ id: String(r.id), ord: Number(r.ord) }));
    const page = async (item: string, part: number): Promise<RegsPage> =>
      buildRegsPage(await loadRegsInputs(store, { kind: "part", item, partIx: part }, DAY, tapDay(DAY, YEAR).weekday, YEAR), { today: DAY, who: WHO });
    return { store, water, items, page };
  };

  it("every key-less tidal part: the tidal banner alone, in the stored tidal note; none says outside B.C.", async () => {
    const { store, water, items, page } = await open();
    // the release's own words for tidal water: the display note of a keyed tidal water's page
    const keyed = await water.get("SELECT s.text AS id, p.part_ix AS part FROM part p JOIN akey a ON a.akey = p.akey " +
      "JOIN item i ON i.item_ord = p.item_ord JOIN str s ON s.id = i.item_id WHERE a.tidal = 1 ORDER BY i.item_ord LIMIT 1");
    expect(keyed, "the release has a keyed tidal water (the book's tidal row)").toBeTruthy();
    const stored = (await page(String(keyed!.id), Number(keyed!.part))).keep.banner!;
    expect([stored.kind, stored.big, stored.paras]).toEqual(["tidal", ["Tidal water"], [TIDAL_SHEET_WORDS.body.join(" ")]]);

    const wrong: string[] = [];
    let tidal = 0, outside = 0;
    for (const it of items) for (const p of await store.parts(it.ord)) {
      if (p.akey !== null) continue;
      const pg = await page(it.id, p.partIx);
      if (!p.tidal) {
        outside++;
        if (pg.status !== "outside" || pg.keep.outside !== OUTSIDE) wrong.push(`${it.id} (not tidal): ${pg.status}`);
        continue;
      }
      tidal++;
      const b = pg.keep.banner;
      const ok = pg.status === "tidal" && pg.keep.outside === null && JSON.stringify(pg.keep.order) === '["banner"]'
        && !!b && JSON.stringify([b.kind, b.big, b.paras]) === JSON.stringify([stored.kind, stored.big, stored.paras])
        && pg.gear === null && pg.methods === null && pg.licence === null && pg.header.tabs.map((t) => t.tab).join() === "keep";
      if (!ok) wrong.push(`${it.id} ${pg.header.name}: status ${pg.status}, says ${pg.keep.outside ?? b?.paras.join(" ")}`);
    }
    expect(wrong).toEqual([]);
    expect(tidal, "the release has tidal pieces").toBeGreaterThan(0);
    expect(outside, "the release has parts outside B.C.").toBeGreaterThan(0);
  });

  it("a water that is tidal throughout, opened with no section (a search hit): the tidal page", async () => {
    const { store, items, page } = await open();
    let whole = 0;
    const wrong: string[] = [];
    for (const it of items) {
      const parts = await store.parts(it.ord);
      if (!parts.every((p) => p.tidal)) continue;
      whole++;
      const t = await targetFor(store, it.id, null);
      const pg = t.kind === "part" ? await page(it.id, t.partIx) : null;
      if (pg?.status !== "tidal" || pg.keep.banner?.big[0] !== "Tidal water") wrong.push(`${it.id}: ${pg?.status}`);
    }
    expect(wrong).toEqual([]);
    expect(whole, "the release has waters that are tidal throughout").toBeGreaterThan(0);
  });
});
