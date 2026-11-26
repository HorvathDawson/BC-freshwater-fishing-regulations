/**
 * (T1140, owner 2026-11-25, on production: https://canifishthis.ca/water/squamish-river opened the Regulations tab on one
 * part — the short reach inside Baynes Island Ecological Reserve, "Closed all year" — and nothing said which; then: "itshould
 * have the real name. there should be no partt x of n nameas anywhewre in the app") ON THE RELEASE UNDER TEST:
 *   1. the Squamish's and the Gordon's side-channel parts are named by the area they lie wholly inside ("Squamish River
 *      inside Baynes Island Ecological Reserve", "Gordon River inside Pacific Rim National Park Reserve"), on the head and
 *      on the map alike (one name function) — a release cut before this deliver change fails here: re-cut it;
 *      and a side channel in no one area by the release's own words for it ("Side channel, joining 12 km from the mouth");
 *   2. province-wide, no plain link's head numbers a part, and every water the map lists as several says the line to
 *      the other parts wherever another part has other rules;
 *   3. the prerendered pages of the Squamish and the Capilano draw the name and the line, and their title and description
 *      never call the whole water Closed or Open from one part.
 */
import { beforeAll, describe, expect, it } from "vitest";
import { entryNames, entryPlaces, headerOf, mergeNav, partAt, partName, WATER_MAP_WORDS } from "@app/core";
import { loadRawWaterGroup, loadRegsInputs, openRegsStore, targetFor, type RegsStore } from "@app/data/regs";
import { openReleaseDir } from "@app/data/release/node";
import { salmonKeyOf } from "../../packages/ui/src/salmonKey";
import { liveReleaseAt } from "../../conformance/src/fixtureRelease";
import { buildAll } from "./build";
import { buildDayOf } from "./model";

const at = liveReleaseAt();
it(`T1140 part labels on the release under test${at.dir ? "" : ` — SKIPPED: ${at.reason}`}`, () => {
  if (!at.dir) console.warn(`T1140 skipped: ${at.reason}`);
});

const SQUAMISH = "gnis:25671", CAPILANO = "gnis:5922", GORDON = "gnis:18167", COMOX = "gnis:37437";
const COUNT = /\bpart \d+ of \d+\b/i;
const DAY = buildDayOf(new Date("2026-11-25T12:00:00"));

describe.skipIf(!at.dir)("(T1140) a part of a water of several is named, never numbered", () => {
  let st: RegsStore;
  const loader = at.dir ? openReleaseDir(at.dir) : null;
  beforeAll(async () => { st = await openRegsStore(loader!); });

  /** the plain link's head: the lead part's header over the map's own entries (the live salmon's join), and the map's name for it */
  async function lead(item: string) {
    const key = await salmonKeyOf(loader!, st, item as never);
    const g = mergeNav(await loadRawWaterGroup(st, item as never), key);
    const t = await targetFor(st, item, null);
    if (t.kind !== "part") throw new Error(`${item}: its plain link opens no part`);
    const inp = await loadRegsInputs(st, t, DAY.day, DAY.weekday, DAY.year);
    const h = headerOf(inp, { today: DAY.day, status: "unknown", names: entryNames(g, item), places: entryPlaces(g, item, key) });
    const at = partAt(g, item, t.partIx), places = entryPlaces(g, item, key);
    return { g, partIx: t.partIx, h, map: partName(g, at), differs: places.get(t.partIx)?.differs ?? false };
  }

  it("the Squamish and the Gordon: their side channels named by the area they lie inside, head and map alike", async () => {
    const [sq, go] = [await lead(SQUAMISH), await lead(GORDON)];
    expect([sq.map, sq.h.place?.short, sq.h.others]).toEqual(["Squamish River inside Baynes Island Ecological Reserve",
      "Squamish River inside Baynes Island Ecological Reserve", WATER_MAP_WORDS.partsOthers]);
    expect([go.map, go.h.place?.short]).toEqual(["Gordon River inside Pacific Rim National Park Reserve",
      "Gordon River inside Pacific Rim National Park Reserve"]);
  }, 60_000);

  it("a side channel in no one area is said by the release's own words for it (owner 17:31: \"The data's own label\")", async () => {
    const co = await lead(COMOX);
    expect([co.map, co.h.place?.short]).toEqual(["Side channel, joining 12 km from the mouth", "Side channel, joining 12 km from the mouth"]);
  }, 60_000);

  it("province-wide: no plain link's head numbers a part; the line wherever another part has other rules", async () => {
    const ords = (await (await st.water()).all("SELECT item_ord FROM item WHERE shown = 1")).map((r) => Number(r.item_ord));
    const bad: string[] = [];
    let many = 0;
    for (const ord of ords) {
      const parts = await st.parts(ord);
      if (parts.filter((p) => p.akey !== null && p.facts && !p.tidal).length < 2) continue;
      const it = await st.item(ord);
      const { g, h, differs, partIx } = await lead(it.id);
      if (COUNT.test(JSON.stringify(h))) bad.push(`${it.id} part ${partIx}: the head numbers it`);
      if (g.parts.length < 2) { if (h.others) bad.push(`${it.id}: one entry on the map, yet the line`); continue; }
      many += 1;
      if (!!h.others !== differs) bad.push(`${it.id} part ${partIx}: differs ${differs}, the line ${!!h.others}`);
    }
    // r-71d09ad82e81 (T1140 step 1): 456 waters of several entries with every piece read alike; the live salmon parts more
    expect(many).toBeGreaterThan(400);
    expect(bad).toEqual([]);
  }, 600_000);

  it("the prerendered pages: the head and the line drawn; title and description never Closed or Open for the whole water", async () => {
    const built = await buildAll({ loader: openReleaseDir(at.dir!), on: DAY, only: new Set([SQUAMISH, CAPILANO]), gauges: null });
    const text = (html: string) => html.replace(/<style[\s\S]*?<\/style>|<script[\s\S]*?<\/script>/g, " ").replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ");
    for (const [item, name] of [[SQUAMISH, "Squamish River inside Baynes Island Ecological Reserve"],
                                [CAPILANO, "From the CN Railway Bridge (tidal boundary) up to the Cable Pool Bridge footbridge"]] as const) {
      const page = built.build.pages.find((p) => p.path === `/water/${built.build.slugMap.waters[item]!.slug}`)!;
      const body = text(page.html);
      expect([item, body.includes(name), body.includes(WATER_MAP_WORDS.partsOthers), COUNT.test(body)]).toEqual([item, true, true, false]);
      expect([item, /\b(closed|open)\b/i.test(page.title), /\b(closed|open)\b/i.test(page.description)]).toEqual([item, false, false]);
    }
  }, 300_000);
});
