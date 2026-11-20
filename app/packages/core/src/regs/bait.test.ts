/**
 * T31 unit cases over hand-built gear answers and 365-day strips (the golden-driven cases are app/conformance/src/regs/
 * bait.parity.test.ts (retired, T439)): the item words, note2, the allowance move, the dated ban runs and the all-year line.
 */
import { describe, expect, it } from "vitest";
import { allowanceRules, allYearBan, baitItems, banHeading, datedBans, moveAllowances, note2, noteWithAllowances, type BaitItem } from "./bait";
import type { GearAnswer, RuleIndex, RuleView, StripState } from "./inputs";

const LABELS: StripState[] = [
  { state: "fly_only", bit: 4, label: "fly fishing only", says: "" }, { state: "bait_ban", bit: 16, label: "bait ban", says: "" },
];
const year = (f: (i: number) => number | null) => Array.from({ length: 365 }, (_, i) => f(i));

describe("baitItems (page v36 gearParts bait)", () => {
  it("names each element; its note is the ban it is excepted from, else why, else 'allowed'; then the carry limit", () => {
    const bait: GearAnswer["bait"] = [
      { element: "worms", ok: true, by: null },
      { element: "roe", ok: true, by: [3, 0], excepted_from: { element: "fin_fish", by: [4, 0] } as never },
      { element: "invertebrate", ok: true, by: [5, 0], why: "on_streams", carry_kg: 1 },
      { element: "fin_fish", ok: false, by: [4, 0], why: "something_new" },
      { element: "worms", ok: false, by: [6, 0] },
    ];
    expect(baitItems({ bait } as GearAnswer)).toEqual([
      { ok: true, name: "Worms", s: "allowed", rix: null },
      { ok: true, name: "Roe (fish eggs)", s: "allowed", rix: 3 },   // (T922) roe reads as roe: no "ban excepts it" note
      { ok: true, name: "Water insects and crayfish", s: "on streams · carry up to 1 kg", rix: 5 },
      { ok: false, name: "Fish or fish parts", s: "something_new", rix: 4 },
      { ok: false, name: "Worms", s: "", rix: 6 },
    ]);
  });
});

describe("note2", () => {
  it("drops a leading allowed / banned / bait ban and its separator, nothing else", () => {
    expect(["allowed", "allowed · the fish or fish parts ban excepts it", "banned · roe is still OK", "bait ban", "on streams"].map(note2))
      .toEqual(["", "the fish or fish parts ban excepts it", "roe is still OK", "", "on streams"]);
  });
});

describe("moveAllowances (UR2 7 / round 20h)", () => {
  const items = (okS: string): BaitItem[] => [{ ok: false, name: "Fish or fish parts", s: "banned · roe is still OK", rix: 1 }, { ok: true, name: "Roe (fish eggs)", s: okS, rix: 2 }];
  it("(T922) '<x> is still OK' on the ban leaves it; roe, the item it names, says nothing more than its name", () => {
    const xs = items("");
    moveAllowances(xs);
    expect(xs.map((x) => note2(x.s))).toEqual(["", ""]);
  });
  it("moves '<x> is still OK' to the allowed item it names (any item but roe)", () => {
    const xs: BaitItem[] = [{ ok: false, name: "Fish or fish parts", s: "banned · worms is still OK", rix: 1 }, { ok: true, name: "Worms", s: "", rix: 2 }];
    moveAllowances(xs);
    expect(xs.map((x) => note2(x.s))).toEqual(["", "worms is still OK"]);
  });
  it("only drops it when the allowed item already names the ban", () => {
    const xs = items("allowed · the fish or fish parts ban excepts it");
    moveAllowances(xs);
    expect(xs.map((x) => x.s)).toEqual(["", "allowed · the fish or fish parts ban excepts it"]);
  });
  it("no allowed item of that name: nothing moves", () => {
    const xs: BaitItem[] = [{ ok: false, name: "Fish or fish parts", s: "banned · roe is still OK", rix: 1 }];
    moveAllowances(xs);
    expect(xs[0]!.s).toBe("banned · roe is still OK");
  });
});

describe("datedBans (prototype dated(st, 16)) and allYearBan (UR4 fix 6)", () => {
  it("each ban run that is not closed and not the whole year; Now while the day is in it", () => {
    const bits = year((i) => (i < 59 ? 16 : i >= 120 && i < 151 ? 20 : i >= 151 && i < 160 ? 17 : 0));
    expect(datedBans(bits, 130, LABELS)).toEqual([
      { chip: "Next", when: "Jan 1–Feb 28", says: "bait ban", inForce: false },
      { chip: "Now", when: "May 1–31", says: "bait ban", inForce: true },
    ]);
  });
  it("a ban all year is no change; the all-year line says it in the first banned item's words", () => {
    const bits = year(() => 16);
    expect(datedBans(bits, 0, LABELS)).toEqual([]);
    const items: BaitItem[] = [{ ok: true, name: "Worms", s: "", rix: null }, { ok: false, name: "Roe (fish eggs)", s: "bait ban", rix: 1 }];
    expect(allYearBan(bits, items)).toEqual({ chip: "Now", when: "All year", says: "bait ban", inForce: true });
    expect(allYearBan(bits, [{ ok: false, name: "Roe (fish eggs)", s: "banned · bait ban", rix: 1 }])?.says).toBe("bait ban");
  });
  it("no all-year line: a day without the ban, a day with no frame, nothing banned, or a banned item that does not say ban", () => {
    const no: BaitItem[] = [{ ok: false, name: "Worms", s: "bait ban", rix: 1 }];
    expect(allYearBan(year((i) => (i ? 16 : 0)), no)).toBeNull();
    expect(allYearBan(year((i) => (i ? 16 : null)), no)).toBeNull();
    expect(allYearBan(year(() => 16), [{ ok: true, name: "Worms", s: "", rix: null }])).toBeNull();
    expect(allYearBan(year(() => 16), [{ ok: false, name: "Worms", s: "on lakes", rix: 1 }])).toBeNull();
  });
});

describe("banHeading (T515): one line — what, when, status, ⓘ", () => {
  const line = (l: ReturnType<typeof banHeading>) => l.map((r) => r.map((x) => (x.t === "text" ? x.s : "ⓘ")).join(""));
  const none: BaitItem[] = ["Worms", "Roe (fish eggs)"].map((name) => ({ ok: false, name, s: "bait ban", rix: 1 }));
  const some: BaitItem[] = [{ ok: true, name: "Roe (fish eggs)", s: "", rix: 1 }, { ok: false, name: "Fish or fish parts", s: "bait ban", rix: 2 }];
  const all = { chip: "Now", when: "All year", says: "bait ban", inForce: true } as const;
  const now = { chip: "Now", when: "May 1–31", says: "bait ban", inForce: true } as const;
  const next = { chip: "Next", when: "Sep 1–Dec 31", says: "bait ban", inForce: false } as const;
  it("all year: no 'in force today', no chip", () => expect(line(banHeading(true, none, all, [], true))).toEqual(["No natural bait — all year ⓘ"]));
  it("seasonal, in season: names the season once, says it is on", () =>
    expect(line(banHeading(true, none, null, [now], true))).toEqual(["No natural bait May 1–31 · in force today ⓘ"]));
  it("seasonal, out of season: the dates, 'not today' (bait is allowed now)", () =>
    expect(line(banHeading(false, some, null, [next, now].slice(0, 1), true))).toEqual(["Bait ban Sep 1–Dec 31 · not today ⓘ"]));
  it("several runs join; any of them in force says today", () =>
    expect(line(banHeading(true, none, null, [now, next], false))).toEqual(["No natural bait May 1–31, Sep 1–Dec 31 · in force today"]));
  it("partial (an exception stays ✓): 'Bait ban', never 'No natural bait'", () =>
    expect(line(banHeading(true, some, all, [], true))).toEqual(["Bait ban — all year ⓘ"]));
  it("(T670 G2) salmon-only day: the day's frame bans bait, so a dated ban reads in force today", () => {
    expect(line(banHeading(true, none, null, [next], true, true))).toEqual(["No natural bait Sep 1–Dec 31 · in force today ⓘ"]);
    expect(line(banHeading(false, none, null, [next], true, true))).toEqual(["No natural bait Sep 1–Dec 31 · not today ⓘ"]);
    expect(line(banHeading(true, none, null, [next], true))).toEqual(["No natural bait Sep 1–Dec 31 · not today ⓘ"]);
  });
  it("only some baits banned, no season: no heading line at all", () => expect(banHeading(false, some, null, [], true)).toEqual([]));
});

describe("T797 item 2 (owner 2026-11-16 'Show it', D18 b): an element's stored allowance is said under it, in its rule's words", () => {
  const LABEL = "Dead fin fish may be used, when fishing for white sturgeon — upstream of the CPR Bridge at Mission";
  const rules: RuleIndex = { get: (ix) => ({ rix: ix, export: { label: ix === 574 ? `${LABEL}*` : "other" } }) as unknown as RuleView,
    has: (ix) => ix === 574, size: 1 };
  const sturgeon: GearAnswer["bait"] = [{ element: "worms", ok: true, by: null },
    { element: "fin_fish", ok: false, by: [4, 0], why: "banned_roe_ok", also_allowed: [{ clause: [574, 0], targeting: ["WSG"] }, { clause: [574, 0] }] }];
  it("a sturgeon water: the fin-fish item's rules, each once; its note then says the label (cleaned), after the item's own words", () => {
    const also = allowanceRules({ bait: sturgeon } as GearAnswer);
    expect(also).toEqual([[], [574]]);
    expect(noteWithAllowances("", also[1]!, rules)).toEqual([LABEL]);
    expect(noteWithAllowances("bait ban", also[1]!, rules)).toEqual(["bait ban", LABEL]);
    expect(noteWithAllowances("on streams · carry up to 1 kg", [], rules)).toEqual(["on streams", "carry up to 1 kg"]);
  });
  it("elsewhere (no allowance): nothing is added", () => {
    const also = allowanceRules({ bait: [{ element: "fin_fish", ok: false, by: [4, 0], why: "banned_roe_ok" }] } as unknown as GearAnswer);
    expect(also).toEqual([[]]);
    expect(noteWithAllowances("roe is still OK", also[0]!, rules)).toEqual(["roe is still OK"]);
  });
});
