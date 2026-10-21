/**
 * banner.ts over hand-built inputs (no store, no golden). The 134-state parity against the golden keep.banner is
 * app/conformance/src/regs/banner.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import { bannerOf, chainOf, chainText, closersAt, mergedOf, rawBanner, sayOnce, type RawBanner } from "./banner";
import type { DisplayFrame, MergedPart, RegsInputs, RuleIndex, RuleView, Statics } from "./inputs";
import type { Day, StripVM } from "./model";
import type { Settled } from "./settle";
import type { SpeciesBook } from "./names";

const D = (month: number, day: number): Day => ({ month, day });
const BOOK: SpeciesBook = {
  fish: { RB: { name: "Rainbow trout" }, CT: { name: "Cutthroat trout" }, WSG: { name: "White sturgeon" } },
  groups: { ALL_GAME_FISH: { name: "all game fish", members: ["RB", "CT", "WSG"] } },
};
/** Closure rules: 1–3 close the game fish over three windows (3 is "from streams"), 4 closes white sturgeon only, 5 is
 *  printed "unless opened", 6 repeats 1's words at 1's level, 7 closes all game fish all year, 8 repeats 1's words at
 *  another level (Region), 9 closes kokanee (not a game fish here). */
const gate = (rix: number, rank: number, wins: [number, number][], o: { water?: string; species?: string[]; unless?: string } = {}) => ({
  rix, key: `e${rix}::r${rix}`, rank, kind: "gate", species: o.species ?? ["RB", "CT", "WSG"], speciesExcept: [],
  f: { species: o.species ?? ["ALL_GAME_FISH"], ...(o.water ? { water: o.water } : {}) }, wins, tcond: "",
  facts: { kind: "gate", closure: true, book: [[20 + rix, 22 + rix]], unless_opened: o.unless ?? null },
  export: { verbatim: "", family: "closure", fields: {} },
}) as unknown as RuleView;
const RULES: Record<number, RuleView> = {
  1: gate(1, 0, [[601, 615]]), 2: gate(2, 0, [[616, 630]]), 3: gate(3, 2, [[616, 630]], { water: "stream" }),
  4: gate(4, 3, [[601, 630]], { species: ["WSG"] }), 5: gate(5, -1, [[701, 705]], { unless: "Closed unless opened by the park." }),
  6: gate(6, 0, [[601, 615]]), 7: gate(7, 0, []), 8: gate(8, 3, [[601, 615]]), 9: gate(9, 0, [], { species: ["KO"] }),
};
const R: RuleIndex = { get: (ix) => RULES[ix]!, has: (ix) => ix in RULES, size: 9 };
const OPEN = { status: "base", closing: [], strip: 0 } as unknown as DisplayFrame;
/** A closed day's stored frame: `closers` is the pipeline's decided choice (display 18), written out per fixture. */
const shut = (closing: [number, string[]][], unless: number[] = [], closers: number[] = closing.map(([ix]) => ix)) =>
  ({ status: "closed", closing, closers, unless_opened: unless, strip: 1 }) as unknown as DisplayFrame;
const ALL = ["RB", "CT", "WSG"];
/** leap days: Jun 1–15 = 153–167, Jun 16–30 = 168–182, Jul 1–5 = 183–187 */
function year(): (DisplayFrame | null)[] {
  return Array.from({ length: 366 }, (_, i) => {
    const d = i + 1;
    if (d >= 153 && d <= 167) return shut([[1, ALL], [6, ALL], [8, ALL], [4, ["WSG"]]], [], [1, 6, 8]);
    if (d >= 168 && d <= 182) return shut([[2, ALL], [3, ALL], [4, ["WSG"]]], [], [2, 3]);
    if (d >= 183 && d <= 187) return shut([[5, ALL]], [5]);
    return OPEN;
  });
}
function inputs(o: { day?: Day; byDay?: (DisplayFrame | null)[]; tidal?: boolean; merged?: MergedPart[] | null; closedGroup?: boolean } = {}): RegsInputs {
  return {
    day: o.day ?? D(6, 20), key: { tidal: !!o.tidal }, rules: R, partIx: 0,
    statics: { exportSpecies: BOOK } as unknown as Statics,
    item: { picker: { choices: [{ parts: [0, 1], closed: o.closedGroup ?? false, sections: 8, heading: "", text: "" }] } },
    mergedParts: o.merged ?? null,
    year: { displayByDay: o.byDay ?? year() },
  } as unknown as RegsInputs;
}
const settled = (status: DisplayFrame | null, broad: number[] = []): Settled =>
  ({ status, broad: broad.map((ix) => R.get(ix)) }) as unknown as Settled;
const strip = (...texts: string[]): StripVM => ({ line: texts.map((text) => ({ kind: "status", text })) }) as unknown as StripVM;

describe("closersAt: the day's stored closers (display 18 — the pipeline's choice, never re-chosen here)", () => {
  it("reads the frame's closers, in their order, as this part's rules", () => {
    expect(closersAt(inputs(), shut([[1, ALL], [4, ["WSG"]], [6, ALL]], [], [1, 6])).map((r) => r.rix)).toEqual([1, 6]);
  });
  it("lists exactly what is stored — a choice the old species-coverage port would not make is kept as stored", () => {
    // page v36's own rule would give [1, 6] here; the store says [4]: the app draws the store's answer
    expect(closersAt(inputs(), shut([[1, ALL], [4, ["WSG"]], [6, ALL]], [], [4])).map((r) => r.rix)).toEqual([4]);
  });
  it("an open frame or no frame: none", () => {
    expect([closersAt(inputs(), OPEN), closersAt(inputs(), null)]).toEqual([[], []]);
  });
});

describe("chainOf: the run's closures, day by day, each once; the frames' unless_opened", () => {
  it("Jun 1–Jul 5 closed: 1 and 6 (Jun 1–15), 2 and 3 (Jun 16–30), 5 (Jul 1–5, unless opened)", () => {
    const c = chainOf(inputs(), settled(year()[170]!), D(6, 20), D(7, 6));
    expect([c.rules.map((r) => r.rix), [...c.unless], c.run]).toEqual([[1, 6, 8, 2, 3, 5], [5], { from: D(6, 1), until: D(7, 5) }]);
  });
  it("no reopening day (closed all year): no run, no chain", () => {
    expect(chainOf(inputs(), settled(year()[170]!), D(6, 20), null)).toEqual({ rules: [], unless: new Set(), run: null });
  });
});

describe("chainText: the prototype's closechain rewrite", () => {
  it("drops the shared lead, capitalises 'from streams,', keeps a sentence the rewrite would empty", () => {
    expect([chainText("No fishing for any game fish, Jun 1–15."), chainText("No fishing for any game fish from streams, Jun 16–30."),
      chainText("No fishing for any game fish."), chainText("Closed unless opened by the park.")])
      .toEqual(["Jun 1–15.", "From streams, Jun 16–30.", "No fishing for any game fish.", "Closed unless opened by the park."]);
  });
});

describe("rawBanner / bannerOf", () => {
  it("a closed day: headline, run sentence, the chain deduped by words and level, the link", () => {
    const inp = inputs(), b = rawBanner(inp, settled(year()[170]!, [2, 3]))!;
    expect([b.kind, b.big, b.para, b.golink, b.reopen]).toEqual(["closed", "Closed · opens again Jul 6",
      "No fishing for any game fish Jun 1–Jul 5. Together these closures cover it:", "Opens again Jul 6. See the rules from then ›", D(7, 6)]);
    expect(b.chain.map((c) => [c.text, c.scope, c.rule])).toEqual([
      ["Jun 1–15.", "This water", "e1::r1"], ["Jun 1–15.", "Region", "e8::r8"], ["Jun 16–30.", "This water", "e2::r2"],
      ["From streams, Jun 16–30.", "Named area", "e3::r3"], ["Closed unless opened by the park.", "Federal / parks", "e5::r5"]]);
    expect(b.chain[0]!.book).toEqual([{ t: "book", printed: 21, pdf: 23 }]);
  });
  it("closed all year: 'Closed all year', no paragraph, the day's first closure", () => {
    const all = Array.from({ length: 366 }, () => shut([[7, ALL]]));
    const b = rawBanner(inputs({ byDay: all }), settled(all[0]!, [7]))!;
    expect([b.big, b.para, b.golink, b.reopen, b.chain.map((c) => [c.text, c.scope])])
      .toEqual(["Closed all year", null, null, null, [["No fishing for any game fish.", "This water"]]]);
  });
  it("closed all year 'unless opened': today's frame names it, so its stored proviso is the line", () => {
    const all = Array.from({ length: 366 }, () => shut([[5, ALL]], [5]));
    expect(rawBanner(inputs({ byDay: all }), settled(all[0]!, [5]))!.chain.map((c) => [c.text, c.scope]))
      .toEqual([["Closed unless opened by the park.", "Federal / parks"]]);
  });
  it("no decided closure today: 'Closed' only (page v36 prints its missing note)", () => {
    const b = rawBanner(inputs(), settled(year()[170]!, []))!;
    expect([b.big, b.para, b.chain, b.reopen]).toEqual(["Closed", null, [], null]);
  });
  it("an open day or no frame: none; tidal water: the tidal banner in the frame's words", () => {
    expect([rawBanner(inputs(), settled(OPEN)), rawBanner(inputs(), settled(null))]).toEqual([null, null]);
    const tidal = { status: "tidal", tidal: true, note: "Tidal water: see DFO.", see: [], licence: "" } as DisplayFrame;
    expect(bannerOf(inputs({ tidal: true }), settled(tidal), null))
      .toEqual({ kind: "tidal", big: ["Tidal water"], paras: ["Tidal water: see DFO."], chain: [], reopen: null, merged: null });
  });
  it("mergedOf: a closed choice of ≥ 2 stretches lists each with its closure's level and book", () => {
    const merged = [{ partIx: 0, label: "Lower", sections: 7, broad: RULES[1]! }, { partIx: 1, label: "Upper", sections: 1, broad: null }];
    expect(mergedOf(inputs({ closedGroup: true, merged }))).toEqual({ label: "Closed all year on 2 stretches (8 sections)", items: [
      { text: "Lower · 7 sections", level: "This water", rule: "e1::r1", book: [{ t: "book", printed: 21, pdf: 23 }] },
      { text: "Upper · 1 section", level: null, rule: null, book: [] }] });
    expect(mergedOf(inputs({ closedGroup: false, merged }))).toBeNull();
  });
});

describe("sayOnce: each sentence once, the strip's first", () => {
  const raw = (o: Partial<RawBanner>): RawBanner => ({ kind: "closed", big: null, para: null, golink: null, chain: [], reopen: null, merged: null, ...o });
  it("the strip said 'Closed' and 'Opens Jul 1': the headline goes, the link keeps 'See the rules from then ›'", () => {
    const v = sayOnce(strip("Closed", "on Jun 15", "Opens Jul 1"),
      raw({ big: "Closed · opens again Jul 1", golink: "Opens again Jul 1. See the rules from then ›", reopen: D(7, 1) }));
    expect([v.big, v.reopen]).toEqual([[], { text: "See the rules from then ›", day: D(7, 1) }]);
  });
  it("a fresh phrase stays; a paragraph said before goes", () => {
    const v = sayOnce(strip("Closed"), raw({ big: "Closed · opens again Jul 1", para: "Opens again: Jul 1!" }));
    expect([v.big, v.paras]).toEqual([["opens again Jul 1"], []]);
    expect(sayOnce(null, raw({ big: "Closed all year", para: "No fishing for any game fish Jun 1–30." })))
      .toMatchObject({ big: ["Closed all year"], paras: ["No fishing for any game fish Jun 1–30."] });
  });
});
