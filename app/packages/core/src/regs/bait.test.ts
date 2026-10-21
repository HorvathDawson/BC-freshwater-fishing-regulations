/**
 * T31 unit cases over hand-built gear answers and 365-day strips (the golden-driven cases are app/conformance/src/regs/
 * bait.parity.test.ts): the item words, note2, the allowance move, the dated ban runs and the all-year line.
 */
import { describe, expect, it } from "vitest";
import { allYearBan, baitItems, datedBans, moveAllowances, note2, type BaitItem } from "./bait";
import type { GearAnswer, StripState } from "./inputs";

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
      { ok: true, name: "Roe (fish eggs)", s: "allowed · the fish or fish parts ban excepts it", rix: 3 },
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
  it("moves '<x> is still OK' to the allowed item it names", () => {
    const xs = items("");
    moveAllowances(xs);
    expect(xs.map((x) => note2(x.s))).toEqual(["", "roe is still OK"]);
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
