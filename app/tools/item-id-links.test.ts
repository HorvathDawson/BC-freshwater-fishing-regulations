/**
 * (T1083) EVERY KIND OF ITEM ID GOES THROUGH A LINK AND COMES BACK. The web start link read only `^[a-z]+:-?\d+$`, while
 * the app wrote `?regs=wsc:100-019698-831000` into its own address: a reload or a shared link of a `wsc:` water opened
 * the bare map (production 0.9.0). Each id is written the ways the app writes one — the address bar and Share link
 * (`routeSearch`), a water page's query (`startLink.web.ts` puts its `cift:item` in `regs=`), the map-at-a-water link
 * (`?show=`, the SEO pages) — and read back by every reader: the web's `parseStartLink`, the phone's `parseAppLink` and
 * `itemOfWaterPage`. The writer and the readers ask one list, `@app/data` `ITEM_ID_KINDS`.
 *
 * Two halves. `KINDS` below is one real id of each kind the bundle holds (both signs where it holds both), so dropping a
 * kind from the list fails here on every machine. And on the release under test, every item id of its `core` — the
 * kinds read from the data, not from the list — goes round the same trip; a kind a new release mints and the list
 * leaves out fails by name.
 */
import { describe, expect, it } from "vitest";
import { setFailureSink } from "@app/core";
import { ITEM_ID_KINDS, itemIdKind, type ItemId } from "@app/data";
import { HOME, itemOfWaterPage, openWater, parseAppLink, parseStartLink, reduce, routeSearch, waterFrame } from "@app/ui";
import { liveReleaseAt, openReleaseDir } from "../conformance/src/fixtureRelease";

const NOW = new Date(2026, 10, 24, 9, 30);

/** One real id per kind, from the release of 2026-11-24 (r-0aec88ed3edb): the kinds the bundle holds. */
const KINDS: Readonly<Record<string, readonly string[]>> = {
  gnis: ["gnis:3062"],                                    // Vedder River
  wbk: ["wbk:328939712", "wbk:-1"],                       // Nehounlee Marsh; Redsand Lake (a water the atlas adds)
  wsc: ["wsc:100-019698-831000",                          // Crayfish Creek: the production link of T1083
        "wsc:100-019698-561179-672700-176000-321000-316300-394900"],   // Stoney Trib.3-1-1-2-2-2: the longest code
  blk: ["blk:360871596", "blk:-2200000296"],              // Paul Said Brook; Kyle Creek (a stream the atlas adds)
};

/** What goes wrong when `item` is written and read back, each way, as words ([] = the trip is whole). */
function roundTrip(item: string): string[] {
  const wrong: string[] = [];
  const reported: unknown[] = [];
  const off = setFailureSink((e) => reported.push(e));
  try {
    const route = reduce(HOME, openWater({ item: item as ItemId, part: null, view: "regs" }));
    const address = routeSearch(route, "", null, null, NOW);
    if (reported.length) wrong.push(`the address writer reported ${String(reported[0])}`);
    const writes = {
      address,
      "a water page's query": `?${new URLSearchParams({ regs: item }).toString()}`,
    };
    for (const [how, q] of Object.entries(writes)) {
      try {
        const l = parseStartLink(q, NOW);
        if (l?.arrive !== item || waterFrame(l.route)?.item !== item) wrong.push(`web start link (${how} ${q}) opened ${l?.arrive ?? "nothing"}`);
      } catch (e) { wrong.push(`web start link (${how} ${q}): ${(e as Error).message}`); }
      try {
        const a = parseAppLink(`https://canifishthis.ca/${q}`, NOW);
        if (a?.item !== item) wrong.push(`phone app link (${how} ${q}) named ${a?.item ?? "nothing"}`);
      } catch (e) { wrong.push(`phone app link (${how} ${q}): ${(e as Error).message}`); }
    }
    try {
      const l = parseStartLink(`?show=${item}`, NOW);
      if (l?.arrive !== item) wrong.push(`web start link (?show=${item}) arrived at ${l?.arrive ?? "nothing"}`);
    } catch (e) { wrong.push(`web start link (?show=${item}): ${(e as Error).message}`); }
    try {
      const got = itemOfWaterPage(`<head><meta name="cift:item" content="${item}"></head>`);
      if (got !== item) wrong.push(`phone water page read ${got}`);
    } catch (e) { wrong.push(`phone water page: ${(e as Error).message}`); }
  } finally { off(); }
  return wrong;
}

describe("(T1083) every kind of item id the bundle holds survives a link", () => {
  it("one real id of each kind: written by the address, read by the web and the phone", () => {
    expect(Object.fromEntries(Object.values(KINDS).flat().map((id) => [id, roundTrip(id)])))
      .toEqual(Object.fromEntries(Object.values(KINDS).flat().map((id) => [id, []])));
  });

  it("the list holds exactly the kinds the bundle holds, each id read as its own kind", () => {
    expect(Object.keys(ITEM_ID_KINDS).sort()).toEqual(Object.keys(KINDS).sort());
    expect(Object.entries(KINDS).flatMap(([k, ids]) => ids.filter((id) => itemIdKind(id) !== k)))
      .toEqual([]);
  });

  it("an id of no kind is still refused by every reader, naming it (a typo never opens some other water)", () => {
    const refused = ["wsc:100--019698", "wsc:-100-019698", "gnis:-5", "area:12", "Vedder", "gnis:", "name:Vedder River"];
    expect(refused.map((id) => itemIdKind(id))).toEqual(refused.map(() => null));
    expect(() => parseStartLink(`?regs=${refused[0]}`, NOW)).toThrow(`start link: regs=${refused[0]} is not an item id`);
    expect(() => parseAppLink(`https://canifishthis.ca/?regs=${refused[0]}`, NOW)).toThrow(`app link: regs=${refused[0]} is not an item id`);
    // the writer says so where it writes one
    const reported: unknown[] = [];
    const off = setFailureSink((e) => reported.push(e));
    try { routeSearch(reduce(HOME, openWater({ item: "area:12" as ItemId, part: null, view: "regs" })), "", null, null, NOW); }
    finally { off(); }
    expect(reported.map(String)).toEqual(["Error: address: regs=area:12 is not an item id the start link reads (ITEM_ID_KINDS)"]);
  });
});

const at = liveReleaseAt();

it(`T1083 every item id of the live release survives a link${at.dir ? "" : ` — SKIPPED: ${at.reason}`}`, () => {
  if (!at.dir) console.warn(`T1083 skipped: ${at.reason}`);
});

describe.skipIf(!at.dir)("(T1083) on the release under test: every item id, every kind it holds", () => {
  it("every id round-trips, and every kind the release mints is in the list", async () => {
    const core = await openReleaseDir(at.dir!).db("core");
    const ids = (await core.all("SELECT item_id FROM item ORDER BY ord")).map((r) => String(r.item_id));
    // the kinds, read from the data: the prefix of each id
    const kinds = [...new Set(ids.map((id) => id.slice(0, Math.max(0, id.indexOf(":")))))].sort();
    expect(kinds).toEqual(Object.keys(ITEM_ID_KINDS).sort());
    // the test reaches what it was written for: a `wsc:` water (T1083's own)
    expect(ids).toContain("wsc:100-019698-831000");
    const wrong = ids.map((id) => [id, roundTrip(id)] as const).filter(([, w]) => w.length);
    expect(wrong.slice(0, 5)).toEqual([]);
  });
});
