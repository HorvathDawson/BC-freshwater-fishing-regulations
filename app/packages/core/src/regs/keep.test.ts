/**
 * T21 unit: the Keep composition over small hand-built inputs (no golden, no store) — the draw order of every block
 * combination the goldens show and the ones they do not (the unplaced-rules caveat after the rows), the outside-B.C. foot,
 * the Other fish key, and the plain-row seams bound to the merged presenters. The 134-state golden parity is
 * conformance/src/regs/keep.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import { keepOf, keepOrder, plainRowSeams, type KeepEnv } from "./keep";
import { otherFishKeyOf } from "./others";
import { runOf } from "./strip";
import { FISH } from "./fish.generated";
import type { OtherFishRowVM } from "./model";
import type { RegsInputs } from "./inputs";

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

  it("caveats before the rows, except the unplaced rules (page v36 uncertainHtml), drawn after them", () => {
    expect(keepOrder({ ...none, strip: true, caveats: [{ kind: "inpart" }, { kind: "side" }, { kind: "uncertain" }, { kind: "timed" }],
      rows: [{ group: 0 }], whereNot: true }))
      .toEqual(["strip", { caveat: 0 }, { caveat: 1 }, { caveat: 3 }, { group: 0 }, { caveat: 2 }, "whereNot"]);
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

describe("otherFishKeyOf: prototype okey", () => {
  const inp = { statics: { glossary: { terms: [
    { id: "catch_and_release", says: "You may fish for it, but every one you catch must go back. " },
    { id: "no_fishing_for", says: "You may not fish for it at all." }] } } } as unknown as RegsInputs;
  const row = (tone: OtherFishRowVM["tone"]): OtherFishRowVM => ({ thumb: null, name: "X", value: "v", tone, week: null, sizeHint: null, open: "other:o0", stamps: [] });

  it("one line per status word a row shows, release first, the glossary's words trimmed", () => {
    expect(otherFishKeyOf([row("closed"), row("keep"), row("release")], inp)).toEqual([
      { status: "release", says: "You may fish for it, but every one you catch must go back.", term: "catch_and_release" },
      { status: "closed", says: "You may not fish for it at all.", term: "no_fishing_for" }]);
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
