/**
 * T53 on the LIVE release (skipped with the reason when this machine has none): the ticket's three unnamed streams, each
 * answered by its SECTION the way the water screen asks (`{kind: "section", handle}` → T09 → core's page), with the
 * header's downstream line from the release's own flow pointers (`downstreamWater`). Rule keys are read from the store's
 * rule set for the section; the words are the page core builds from the store's answer — nothing here assembles a rule.
 *
 *   1408  Region 1, flows into the Eve River: the Eve's tributary closure (Dec 1–May 31) shows.
 *   64035 Region 2, flows into the Salmon River (Langley): the Salmon's tributary closure shows.
 *   59506 Region 6, flows into another unnamed section: the region's base alone, no downstream line.
 */
import { beforeAll, describe, expect, it } from "vitest";
import { buildRegsPage, type RegsPage, type Weekday } from "@app/core";
import { loadRegsInputs, openRegsStore, type RegsStore } from "@app/data/regs";
import { makeReleaseSource } from "@app/data/release";
import { downstreamWater, type RegsSource, type SectionId } from "@app/data";
import { liveReleaseAt, openReleaseDir } from "../fixtureRelease";

const at = liveReleaseAt();
/** The page's default angler (decisions.md D8, `@app/ui` DEFAULT_WHO). */
const ADULT_RESIDENT = { age: "adult", residency: "resident" } as const;

it(`T53 unnamed streams on the live release${at.dir ? "" : ` — SKIPPED: ${at.reason}`}`, () => {
  if (!at.dir) console.warn(`T53 unnamed skipped: ${at.reason}`);
});

describe.skipIf(!at.dir)(`T53 an unnamed stream's regulations on the live release (${at.dir ?? at.reason})`, () => {
  let st: RegsStore, src: RegsSource;
  beforeAll(async () => {
    const loader = openReleaseDir(at.dir!);
    st = await openRegsStore(loader);
    src = makeReleaseSource(loader);
  });

  /** The page for a tap on `handle` on a day, as the water screen builds it; the store's trib / reach rule keys. */
  async function tap(handle: number, month: number, day: number, weekday: Weekday) {
    const inp = await loadRegsInputs(st, { kind: "section", handle }, { month, day }, weekday, 2026);
    const down = await downstreamWater(src, handle as SectionId);
    const key = (r: number) => { const e = inp.rules.get(r)!.export; return `${e.entry_id}::${e.rule_id}`; };
    const page: RegsPage = buildRegsPage(inp, { today: { month, day }, who: ADULT_RESIDENT, downstream: down?.name ?? null });
    return { page, item: await src.itemForSection(handle as SectionId), trib: inp.ruleset!.trib.map(key),
             reach: inp.ruleset!.reach.map(key) };
  }
  const banner = (p: RegsPage) => p.keep.banner && {
    big: p.keep.banner.big, paras: p.keep.banner.paras,
    chain: p.keep.banner.chain.map((c) => ({ text: c.text, scope: c.scope, rule: c.rule })) };

  it("1408 (Region 1, into the Eve River): Tributary of Eve River; the Eve's tributary closure is the day's banner", async () => {
    const jan = await tap(1408, 1, 15, "Thursday");
    expect([jan.item, jan.page.header.name, jan.page.header.place, jan.trib]).toEqual([null, "Unnamed stream · Region 1",
      { full: "Tributary of Eve River", short: "Tributary of Eve River" }, ["r1:eve_river@1-10::eve_river.r1"]]);
    expect([jan.page.status, banner(jan.page)]).toEqual(["closed", {
      big: [], paras: ["No fishing for any game fish Dec 1–May 31."],
      chain: [{ text: "Dec 1–May 31.", scope: "From downstream", rule: jan.trib[0] }] }]);
    // out of its window the same section is open under the region's base (no banner)
    const oct = await tap(1408, 10, 22, "Thursday");
    expect([oct.page.status, banner(oct.page)]).toEqual(["open", null]);
  });

  it("64035 (Region 2, into the Salmon River): Tributary of Salmon River; closed all year by the Salmon's tributary rule", async () => {
    const t = await tap(64035, 10, 22, "Thursday");
    expect([t.item, t.page.header.name, t.page.header.place, t.trib]).toEqual([null, "Unnamed stream · Region 2",
      { full: "Tributary of Salmon River", short: "Tributary of Salmon River" }, ["r2:salmon_river@2-4::salmon_river.r1"]]);
    expect([t.page.status, banner(t.page)]).toEqual(["closed", {
      big: ["Closed all year"], paras: [],
      chain: [{ text: "No fishing for any game fish.", scope: "From downstream", rule: t.trib[0] }] }]);
  });

  it("59506 (Region 6, into an unnamed section): the region's base alone; no downstream line", async () => {
    const t = await tap(59506, 10, 22, "Thursday");
    expect([t.item, t.page.header.name, t.page.header.place, t.trib, t.reach.filter((k) => !/^z(6|p):/.test(k)),
            t.page.status, banner(t.page)])
      .toEqual([null, "Unnamed stream · Region 6", null, [], [], "open", null]);
    // every line the page cites is the base's
    expect(t.page.keep.whereNot!.items.map((x) => x.rule).filter((k) => !/^z(6|p):/.test(k))).toEqual([]);
  });
});
