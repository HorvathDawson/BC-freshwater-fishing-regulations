/**
 * (T1140, owner 2026-11-25, on production: https://canifishthis.ca/water/squamish-river opened the Regulations tab on part
 * 1 of 2 — the short reach inside Baynes Island Ecological Reserve, "Closed all year" — and nothing said it was one part)
 * ON THE RELEASE UNDER TEST:
 *   1. province-wide, every water the map lists as several entries heads its plain link's page (`targetFor`'s lead, the
 *      `/water/<slug>` way in) with "Part n of m" — the Map tab's count over the same live-salmon join (`partWhere`);
 *   2. the Squamish (the lead closed all year, the rest open) and the Capilano (the lead open, parts 2 and 3 closed all
 *      year): the prerendered page (the app's own pane) says "Part 1 of m" and the line to the other parts, and its title
 *      and description never call the whole water Closed or Open from one part.
 */
import { beforeAll, describe, expect, it } from "vitest";
import { entryNames, entryPlaces, headerOf, mergeNav, partAt, partWhere } from "@app/core";
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

const SQUAMISH = "gnis:25671", CAPILANO = "gnis:5922";
const DAY = buildDayOf(new Date("2026-11-25T12:00:00"));

describe.skipIf(!at.dir)("(T1140) a water of several parts says which part of how many", () => {
  let st: RegsStore;
  const loader = at.dir ? openReleaseDir(at.dir) : null;
  beforeAll(async () => { st = await openRegsStore(loader!); });

  /** the plain link's head: the lead part's header over the map's own entries (the live salmon's join), and the map's bar */
  async function lead(item: string) {
    const key = await salmonKeyOf(loader!, st, item as never);
    const g = mergeNav(await loadRawWaterGroup(st, item as never), key);
    const t = await targetFor(st, item, null);
    if (t.kind !== "part") throw new Error(`${item}: its plain link opens no part`);
    const inp = await loadRegsInputs(st, t, DAY.day, DAY.weekday, DAY.year);
    const h = headerOf(inp, { today: DAY.day, status: "unknown", names: entryNames(g, item), places: entryPlaces(g, item, key) });
    return { g, partIx: t.partIx, h, bar: partWhere(g, partAt(g, item, t.partIx)).split(" · ")[0]! };
  }

  it("the Squamish and the Capilano: 'Part 1 of 2' / 'Part 1 of 3' and the line, as their Map tabs say", async () => {
    const [sq, cap] = [await lead(SQUAMISH), await lead(CAPILANO)];
    expect([sq.h.name, sq.h.part, sq.bar]).toEqual(["Squamish River",
      { where: "Part 1 of 2", others: "Other parts of this water have different rules" }, "Part 1 of 2"]);
    expect([cap.h.name, cap.h.part, cap.bar]).toEqual(["Capilano River",
      { where: "Part 1 of 3", others: "Other parts of this water have different rules" }, "Part 1 of 3"]);
  }, 60_000);

  it("province-wide: every plain link to a water the map lists as several entries heads with the map's 'Part n of m'", async () => {
    const ords = (await (await st.water()).all("SELECT item_ord FROM item WHERE shown = 1")).map((r) => Number(r.item_ord));
    const bad: string[] = [];
    let many = 0;
    for (const ord of ords) {
      const parts = await st.parts(ord);
      if (parts.filter((p) => p.akey !== null && p.facts && !p.tidal).length < 2) continue;
      const it = await st.item(ord);
      const { g, h, bar, partIx } = await lead(it.id);
      if (g.parts.length < 2) { if (h.part) bad.push(`${it.id}: one entry on the map, yet "${h.part.where}"`); continue; }
      many += 1;
      if (h.part?.where !== bar) bad.push(`${it.id} part ${partIx}: the map says "${bar}", the head "${h.part?.where ?? "(nothing)"}"`);
    }
    // r-71d09ad82e81 (T1140 step 1): 456 waters of several entries with every piece read alike; the live salmon parts more
    expect(many).toBeGreaterThan(400);
    expect(bad).toEqual([]);
  }, 600_000);

  it("the prerendered pages: the head and the line drawn; title and description never Closed or Open for the whole water", async () => {
    const built = await buildAll({ loader: openReleaseDir(at.dir!), on: DAY, only: new Set([SQUAMISH, CAPILANO]), gauges: null });
    const text = (html: string) => html.replace(/<style[\s\S]*?<\/style>|<script[\s\S]*?<\/script>/g, " ").replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ");
    for (const [item, where] of [[SQUAMISH, "Part 1 of 2"], [CAPILANO, "Part 1 of 3"]] as const) {
      const page = built.build.pages.find((p) => p.path === `/water/${built.build.slugMap.waters[item]!.slug}`)!;
      const body = text(page.html);
      expect([item, body.includes(where), body.includes("Other parts of this water have different rules")]).toEqual([item, true, true]);
      expect([item, /\b(closed|open)\b/i.test(page.title), /\b(closed|open)\b/i.test(page.description)]).toEqual([item, false, false]);
    }
  }, 300_000);
});
