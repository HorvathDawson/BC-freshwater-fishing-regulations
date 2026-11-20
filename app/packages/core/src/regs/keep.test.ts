/**
 * T21 unit: the Keep composition over small hand-built inputs (no golden, no store) — the draw order of every block
 * combination the goldens show and the ones they do not (the unplaced-rules caveat after the rows), the outside-B.C. foot,
 * the Other fish key, and the plain-row seams bound to the merged presenters. The 134-state golden parity is
 * conformance/src/regs/keep.parity.test.ts (retired, T439).
 */
import { describe, expect, it } from "vitest";
import { groupOf, keepOf, keepOrder, plainRowSeams, sectionNotesOf, type KeepEnv } from "./keep";
import { drawnRows } from "./grouphead";
import { cardOf, INP, RIX, rv, TROUT_ROW } from "./testing/keepFixtures";
import { closeForFinfish } from "./finfish";
import { otherFishKeyOf } from "./others";
import { runOf } from "./strip";
import { FISH } from "./fish.generated";
import type { CaveatVM, KeepVM, OtherFishRowVM, RegsPage } from "./model";
import type { RegsInputs, RuleView } from "./inputs";

const none = { strip: false, inherited: false, banner: false, caveats: [], rows: [], otherFish: false, possession: false, whereNot: false };

describe("keepOrder: page v36 renderCard + prototype buildKeep draw order", () => {
  it("an open day with one group (vedder_1010's shape)", () => {
    expect(keepOrder({ ...none, strip: true, rows: [{ group: 0 }], otherFish: true, possession: true, whereNot: true }))
      .toEqual(["strip", { group: 0 }, "otherFish", "possession", "whereNot"]);
  });

  it("a closed day: strip, banner, where-not", () => {
    expect(keepOrder({ ...none, strip: true, banner: true, whereNot: true })).toEqual(["strip", "banner", "whereNot"]);
  });

  it("the inherited line right after the strip, before the banner; groups and plain rows in row order", () => {
    expect(keepOrder({ ...none, strip: true, inherited: true, banner: true, rows: [{ group: 0 }, { plain: 0 }, { group: 1 }, { plain: 1 }],
      otherFish: true, possession: true, whereNot: true }))
      .toEqual(["strip", "inherited", "banner", { group: 0 }, { plain: 0 }, { group: 1 }, { plain: 1 }, "otherFish", "possession", "whereNot"]);
  });

  it("every caveat before the rows (T378: no unplaced block after them any more)", () => {
    expect(keepOrder({ ...none, strip: true, caveats: [{ kind: "inpart" }, { kind: "side" }, { kind: "timed" }],
      rows: [{ group: 0 }], whereNot: true }))
      .toEqual(["strip", { caveat: 0 }, { caveat: 1 }, { caveat: 2 }, { group: 0 }, "whereNot"]);
  });

  it("(T623 F4) the part-closed warning leads the tab, above the strip; the other caveats keep their place", () => {
    expect(keepOrder({ ...none, strip: true, banner: true, caveats: [{ kind: "partclosed" }, { kind: "inpart" }], rows: [{ group: 0 }] }))
      .toEqual([{ caveat: 0 }, "strip", "banner", { caveat: 1 }, { group: 0 }]);
  });

  it("(T796) a section's notes lead the tab, before the part-closed warning, above the strip — on a closed day too", () => {
    expect(keepOrder({ ...none, strip: true, banner: true, rows: [{ group: 0 }],
      caveats: [{ kind: "note" }, { kind: "partclosed" }, { kind: "inpart" }, { kind: "note" }] }))
      .toEqual([{ caveat: 0 }, { caveat: 3 }, { caveat: 1 }, "strip", "banner", { caveat: 2 }, { group: 0 }]);
    expect(keepOrder({ ...none, strip: true, banner: true, caveats: [{ kind: "note" }] }))
      .toEqual([{ caveat: 0 }, "strip", "banner"]);
  });

  it("nothing at all draws nothing", () => {
    expect(keepOrder(none)).toEqual([]);
  });
});

describe("keepOf: the days with no card", () => {
  it("outside B.C. (no key): the one foot line, every block empty", () => {
    const k = keepOf({ key: null } as unknown as RegsInputs, {} as KeepEnv);
    expect(k).toEqual({ order: ["outside"], outside: "This part of the water is outside B.C.: no B.C. rules apply.", strip: null,
      inherited: null, banner: null, groups: [], plainRows: [], otherFish: null, otherFishKey: [], caveats: [], possession: null, whereNot: null });
  });
});

const GLADYS = "Mostly outside Gladys Lake Ecological Reserve: its closure (rule book p.9) does not apply here.";
const TOW = "Mostly outside Tow Hill Ecological Reserve: its closure (rule book p.9) does not apply here.";

describe("(T796) sectionNotesOf: the section's notes as caveats, Python's words as they are", () => {
  it("one `note` caveat per note, in order, the sentence its title, nothing to open", () => {
    expect(sectionNotesOf({ notes: [GLADYS, TOW] } as unknown as RegsInputs)).toEqual([
      { kind: "note", hot: false, title: GLADYS, calm: null, items: [], later: null },
      { kind: "note", hot: false, title: TOW, calm: null, items: [], later: null }]);
    expect(sectionNotesOf({ notes: [] } as unknown as RegsInputs)).toEqual([]);
  });

  it("a DFO finfish closure closes the page and keeps the note (it is about the place, not the day)", () => {
    const note = sectionNotesOf({ notes: [GLADYS] } as unknown as RegsInputs)[0]!;
    const side: CaveatVM = { kind: "side", hot: false, title: "On one side", calm: null, items: [], later: null };
    const keep = { order: [{ caveat: 0 }, "strip", { caveat: 1 }], strip: { status: "open", line: [], runs: [], todayAt: 0, months: [], legend: [] },
      inherited: null, banner: null, groups: [], plainRows: [], otherFish: null, otherFishKey: [], caveats: [note, side],
      possession: null, whereNot: null, outside: null } as unknown as KeepVM;
    const page = { status: "open", header: { dot: "open" }, keep, licence: {} } as unknown as RegsPage;
    const shut = closeForFinfish(page, ["Finfish closure"]);
    expect(shut.keep.caveats).toEqual([note]);
    expect(shut.keep.order).toEqual([{ caveat: 0 }, "strip", "banner"]);
  });
});

describe("otherFishKeyOf: prototype okey", () => {
  const inp = { statics: { glossary: { terms: [
    { id: "catch_and_release", says: "You may fish for it, but every one you catch must go back. " },
    { id: "no_fishing_for", says: "You may not fish for it at all." }] } } } as unknown as RegsInputs;
  const row = (tone: OtherFishRowVM["tone"]): OtherFishRowVM => ({ thumb: null, name: "X", value: "v", tone, week: null, sizeHint: null, open: "other:o0", stamps: [], back: [] });

  it("one line per status word a row shows, release first, the glossary's words trimmed", () => {
    expect(otherFishKeyOf([row("closed"), row("keep"), row("release")], inp)).toEqual([
      { status: "release", says: "You may fish for it, but every one you catch must go back.", term: "catch_and_release" },
      { status: "closed", says: "You may not fish for it at all.", term: "no_fishing_for" }]);
  });

  it("(T670 K2) a keep row with a fish that must go back shows the release word's line too", () => {
    expect(otherFishKeyOf([{ ...row("keep"), back: [{ status: "release", says: "Release every mountain whitefish.", rule: "e::r" }] }], inp)).toEqual([
      { status: "release", says: "You may fish for it, but every one you catch must go back.", term: "catch_and_release" }]);
  });

  it("no list, no release/closed row, or a term the glossary lacks: no line", () => {
    expect(otherFishKeyOf(null, inp)).toEqual([]);
    expect(otherFishKeyOf([row("keep"), row("nolimit")], inp)).toEqual([]);
    expect(otherFishKeyOf([row("closed")], { statics: { glossary: { terms: [] } } } as unknown as RegsInputs)).toEqual([]);
  });
});

describe("plainRowSeams: T19b's seams bound to the merged presenters", () => {
  const inp = { statics: { exportSpecies: { fish: { RB: { name: "Rainbow trout" } }, groups: {} } } } as unknown as RegsInputs;
  const seams = plainRowSeams(inp, FISH);

  it("runOf over a per-day strip = T13 runOf over its runs", () => {
    const perDay = [...Array<string>(100).fill("keep"), ...Array<string>(265).fill("release")];
    expect(seams.runOf(perDay, { month: 3, day: 1 })).toEqual(runOf([["keep", 100], ["release", 265]], { month: 3, day: 1 }));
    expect(seams.runOf(perDay, { month: 3, day: 1 })).toMatchObject({ now: "keep", all: false, from: { month: 1, day: 1 }, until: { month: 4, day: 10 } });
  });

  it("the fish names and pictures: the species book's name first; a fish with no picture is a silhouette", () => {
    expect([seams.fishName("RB"), seams.hasFish("RB"), seams.hasFish("ZZ"), seams.more]).toEqual(["Rainbow trout", true, false, "See every regulation line ›"]);
    expect(seams.pictures(["ZZ"])).toEqual([{ code: "ZZ", name: "ZZ", sil: true }]);
  });
});

describe("(T892, review 2-2) groupOf: a quota group decided by another water's tributary rule carries its fold's line", () => {
  // keepFixtures' Vedder-like trout group; rule 10 is the Chilliwack River's row reaching this part by its tributaries
  const TRIB = { ...rv(10), missing: false, rank: 1, baseRank: 0, via: "trib", part: null, export: { prov: { rank: 0, entry_name: "Chilliwack River" } } } as unknown as RuleView;
  const rules = { ...RIX, get: (i: number) => (i === 10 ? TRIB : rv(i)), has: (i: number) => i === 10 || RIX.has(i) };
  const fish = TROUT_ROW.all_members;
  const groupWith = (roles: (S: string) => [number, string, null][]) => {
    const base = cardOf([TROUT_ROW], { R: Object.fromEntries(fish.map((S) => [S, {}])) as never });
    const card = { ...base, ctx: { ...base.ctx, s: 0 } } as typeof base;
    const inp = { ...INP(), rules, year: { rowsBySeg: [{ fish: Object.fromEntries(fish.map((S) =>
      [S, { lead: "hatchery", hatchery: { win: roles(S)[0]![0], narrow: null, roles: roles(S) }, wild: { win: roles(S)[0]![0], narrow: null, roles: roles(S) } }])) }] } } as unknown as RegsInputs;
    const env = { card, settled: { lifted: new Map(), day: { month: 10, day: 10 } }, fish: FISH, today: { month: 10, day: 10 }, tot: "a" } as unknown as KeepEnv;

    return groupOf(0, inp, env, drawnRows(card), plainRowSeams(inp, FISH));
  };
  it("every fish and origin decided by the tributary rule: the line, whole; the region's rows: none", () => {
    expect(groupWith(() => [[10, "governs", null], [1, "replaced", null]]).from).toBe("Chilliwack River’s rule for its tributaries");
    expect(groupWith(() => [[1, "governs", null]]).from).toBeUndefined();
    expect(groupWith((S) => (S === "RB" ? [[10, "governs", null]] : [[1, "governs", null]])).from).toBeUndefined();
  });
});
