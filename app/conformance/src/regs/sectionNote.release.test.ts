/**
 * (T796, owner 2026-11-16: "take out the lakes but add a note") EVERY SECTION NOTE OF THE RELEASE UNDER TEST IS SHOWN ON
 * ITS WATER'S PAGE, in Python's own words. Core `section_note` (`bundle.outside_note`) holds one sentence per section of a
 * water taken out of a reserve or park by name ("Mostly outside Gladys Lake Ecological Reserve: its closure (rule book
 * p.9) does not apply here."). Asked of the whole release, row by row, so a release with more notes is covered with no edit:
 *
 * - the store reads exactly the rows the release's whole core holds (none from a core cut before the table);
 * - each noted section's page — its water's part when a water holds it (`targetFor`), else the section itself (an unnamed
 *   lake) — leads its Keep tab with the note, as a `note` caveat, word for word, on an open day and on a closed one;
 * - a page none of whose sections has a note shows none.
 */
import { describe, expect, it } from "vitest";
import { buildRegsPage, tapDay, type Day, type RegsPage, type RegsTarget } from "@app/core";
import { loadRegsInputs, openRegsStore, targetFor } from "@app/data/regs";
import { liveRelease, openReleaseDir } from "../fixtureRelease";

const LIVE = liveRelease();
const YEAR = 2026, WHO = { age: "adult", residency: "resident" } as const;
const DAYS: readonly Day[] = [{ month: 7, day: 15 }, { month: 1, day: 1 }];

describe.skipIf(!LIVE)(`T796 section notes on their pages (${LIVE ?? "no live release"})`, () => {
  it("every core section_note row leads its water's Keep tab, word for word; the store reads exactly those rows", async () => {
    const loader = openReleaseDir(LIVE!);
    const store = await openRegsStore(loader, { tiles: null });
    const core = await loader.db("core");
    const has = !!(await core.get("SELECT 1 AS x FROM sqlite_master WHERE type = 'table' AND name = 'section_note'"));
    const rows = has ? (await core.all("SELECT sid, note FROM section_note ORDER BY sid")).map((r) => [Number(r.sid), String(r.note)] as const) : [];
    expect([...(await store.sectionNotes())]).toEqual(rows);

    const wrong: string[] = [];
    for (const [sid, note] of rows) {
      const held = await core.get("SELECT i.item_id AS id FROM item_section x JOIN item i USING (ord) WHERE x.sid = ?", sid);
      const target: RegsTarget = held ? await targetFor(store, String(held.id), sid) : { kind: "section", handle: sid };
      for (const day of DAYS) {
        const inp = await loadRegsInputs(store, target, day, tapDay(day, YEAR).weekday, YEAR);
        const page: RegsPage = buildRegsPage(inp, { today: day, who: WHO });
        const first = page.keep.order[0];
        const lead = first && typeof first === "object" && "caveat" in first ? page.keep.caveats[first.caveat] : null;
        if (!inp.notes.includes(note) || lead?.kind !== "note" || !page.keep.caveats.some((c) => c.kind === "note" && c.title === note))
          wrong.push(`${page.header.name} (${JSON.stringify(target)}, ${day.month}/${day.day}): leads with ${lead?.kind ?? String(first)}`);
      }
    }
    expect(wrong).toEqual([]);
  });

  it("a page with no noted section shows no note (the first water the release holds)", async () => {
    const loader = openReleaseDir(LIVE!);
    const store = await openRegsStore(loader, { tiles: null });
    const notes = await store.sectionNotes();
    const core = await loader.db("core");
    // the first water none of whose sections carries a note
    for (const r of await core.all("SELECT item_id AS id, ord FROM item ORDER BY ord LIMIT 50")) {
      const sids = (await core.all("SELECT sid FROM item_section WHERE ord = ?", Number(r.ord))).map((s) => Number(s.sid));
      if (!sids.length || sids.some((s) => notes.has(s)) || (await store.itemOrd(String(r.id))) === null) continue;
      const day = DAYS[0]!;
      const inp = await loadRegsInputs(store, await targetFor(store, String(r.id), null), day, tapDay(day, YEAR).weekday, YEAR);
      expect(inp.notes).toEqual([]);
      expect(buildRegsPage(inp, { today: day, who: WHO }).keep.caveats.filter((c) => c.kind === "note")).toEqual([]);
      return;
    }
    throw new Error("the release holds no water without a note among its first 50");
  });
});
