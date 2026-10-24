/**
 * T05 unit cases, store-free. The segment literals are taps.json's (app/fixtures/golden/taps.json); the licence
 * dims/profiles are the regs store's `static licence.profile_dims` / `licence.profiles`, copied. The store-backed parity
 * over all 134 golden states (and the Feb 29 cases) is app/conformance/src/regs/tap.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import type { Moment } from "../answers.generated";
import { isLeapYear, LEAP_YEAR_CASES, WEEKDAYS } from "../calendar.generated";
import { AGE_DIM, daySegments, profileIndex, segmentIndex, tapDay, TapError, yearSegments } from "./tap";

const ALL = WEEKDAYS;
const MON_THU = ["Monday", "Tuesday", "Wednesday", "Thursday"] as const;
const FRI_SUN = ["Friday", "Saturday", "Sunday"] as const;
const SUNSET = { end: { offset_min: -60, solar: "sunrise" }, start: { offset_min: 60, solar: "sunset" } } as const;
const NOON = { end: { at: "00:00", offset_min: 0 }, start: { at: "12:00", offset_min: 0 } } as const;
const m = (weekdays: readonly string[], hours: null | Record<string, unknown> = null) => ({ weekdays, hours }) as unknown as Moment;

const VEDDER = [1, 122, 153, 183];
const KLWA = { starts: [1, 1], moments: [m(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]), m(["Saturday", "Sunday"])] };
const X15P7 = { starts: [1, 1, 122, 122, 228, 228, 306, 306],
  moments: [0, 1, 2, 3, 4, 5, 6, 7].map((i) => m(ALL, { ...SUNSET, in: i % 2 === 1 })) };
const X32P0 = { starts: [1, 92, 167, 197, 197, 245, 306],
  moments: [m(ALL), m(ALL), m(ALL), m(ALL, { ...NOON, in: false }), m(ALL, { ...NOON, in: true }), m(ALL), m(ALL)] };
/** The Zymoetz below Limonite Creek (gnis:3137 part 0, akey 904): segment_moments [7,8,7,8,2,2,2,7,8,7,8,7,8,7,8]. */
const ZYMOETZ = { starts: [1, 1, 136, 136, 153, 168, 183, 206, 206, 259, 259, 306, 306, 336, 336],
  moments: [MON_THU, FRI_SUN, MON_THU, FRI_SUN, ALL, ALL, ALL, MON_THU, FRI_SUN, MON_THU, FRI_SUN, MON_THU, FRI_SUN, MON_THU, FRI_SUN]
    .map((w) => m(w)) };

const DIMS: readonly (readonly [string, readonly string[]])[] = [["residency", ["resident", "non_resident", "non_resident_alien"]],
  ["age", ["16_plus", "under_16"]], ["guidance", ["non_guided", "guided"]],
  ["status", ["none", "indian_bc_resident", "metis", "disabled", "aged_65_plus"]]];
const PROFILES: readonly string[] = [
  "resident/16_plus/non_guided/none", "resident/16_plus/non_guided/indian_bc_resident", "resident/16_plus/non_guided/metis", "resident/16_plus/non_guided/disabled", "resident/16_plus/non_guided/aged_65_plus",
  "resident/16_plus/guided/none", "resident/16_plus/guided/indian_bc_resident", "resident/16_plus/guided/metis", "resident/16_plus/guided/disabled", "resident/16_plus/guided/aged_65_plus",
  "resident/under_16/non_guided/none", "resident/under_16/non_guided/indian_bc_resident", "resident/under_16/non_guided/metis", "resident/under_16/non_guided/disabled", "resident/under_16/non_guided/aged_65_plus",
  "resident/under_16/guided/none", "resident/under_16/guided/indian_bc_resident", "resident/under_16/guided/metis", "resident/under_16/guided/disabled", "resident/under_16/guided/aged_65_plus",
  "non_resident/16_plus/non_guided/none", "non_resident/16_plus/non_guided/indian_bc_resident", "non_resident/16_plus/non_guided/metis", "non_resident/16_plus/non_guided/disabled", "non_resident/16_plus/non_guided/aged_65_plus",
  "non_resident/16_plus/guided/none", "non_resident/16_plus/guided/indian_bc_resident", "non_resident/16_plus/guided/metis", "non_resident/16_plus/guided/disabled", "non_resident/16_plus/guided/aged_65_plus",
  "non_resident/under_16/non_guided/none", "non_resident/under_16/non_guided/indian_bc_resident", "non_resident/under_16/non_guided/metis", "non_resident/under_16/non_guided/disabled", "non_resident/under_16/non_guided/aged_65_plus",
  "non_resident/under_16/guided/none", "non_resident/under_16/guided/indian_bc_resident", "non_resident/under_16/guided/metis", "non_resident/under_16/guided/disabled", "non_resident/under_16/guided/aged_65_plus",
  "non_resident_alien/16_plus/non_guided/none", "non_resident_alien/16_plus/non_guided/indian_bc_resident", "non_resident_alien/16_plus/non_guided/metis", "non_resident_alien/16_plus/non_guided/disabled", "non_resident_alien/16_plus/non_guided/aged_65_plus",
  "non_resident_alien/16_plus/guided/none", "non_resident_alien/16_plus/guided/indian_bc_resident", "non_resident_alien/16_plus/guided/metis", "non_resident_alien/16_plus/guided/disabled", "non_resident_alien/16_plus/guided/aged_65_plus",
  "non_resident_alien/under_16/non_guided/none", "non_resident_alien/under_16/non_guided/indian_bc_resident", "non_resident_alien/under_16/non_guided/metis", "non_resident_alien/under_16/non_guided/disabled", "non_resident_alien/under_16/non_guided/aged_65_plus",
  "non_resident_alien/under_16/guided/none", "non_resident_alien/under_16/guided/indian_bc_resident", "non_resident_alien/under_16/guided/metis", "non_resident_alien/under_16/guided/disabled", "non_resident_alien/under_16/guided/aged_65_plus",
];

describe("tapDay: the reading day and the real weekday (THE CALENDAR SPEC, P06)", () => {
  it("1. Vedder's tap days", () => {
    expect([tapDay({ month: 5, day: 15 }, 2026), tapDay({ month: 6, day: 15 }, 2026), tapDay({ month: 10, day: 10 }, 2026)])
      .toEqual([{ day: 136, weekday: "Friday" }, { day: 167, weekday: "Monday" }, { day: 284, weekday: "Saturday" }]);
  });
  it("6. weekdays in the selected year", () => {
    expect([tapDay({ month: 10, day: 12 }, 2026).weekday, tapDay({ month: 2, day: 29 }, 2028).weekday]).toEqual(["Monday", "Tuesday"]);
  });
  it("Feb 29 reads Feb 28's day (59) on its OWN weekday; Mar 1 is 61 in every year", () => {
    expect([tapDay({ month: 2, day: 28 }, 2036), tapDay({ month: 2, day: 29 }, 2036), tapDay({ month: 3, day: 1 }, 2036),
      tapDay({ month: 3, day: 1 }, 2027)])
      .toEqual([{ day: 59, weekday: "Thursday" }, { day: 59, weekday: "Friday" }, { day: 61, weekday: "Saturday" },
        { day: 61, weekday: "Monday" }]);
  });
  it("a date that does not exist is a programmer error, not a guessed day", () => {
    expect(() => tapDay({ month: 2, day: 29 }, 2027)).toThrow(new TapError("regs: 2027-2-29 is not a date"));
    expect(() => tapDay({ month: 2, day: 29 }, 2100)).toThrow(new TapError("regs: 2100-2-29 is not a date"));
    expect(() => tapDay({ month: 4, day: 31 }, 2026)).toThrow(new TapError("regs: 2026-4-31 is not a date"));
    expect(() => tapDay({ month: 13, day: 1 }, 2026)).toThrow(new TapError("regs: 2026-13-1 is not a date"));
    expect(() => tapDay({ month: 1, day: 0 }, 2026)).toThrow(new TapError("regs: 2026-1-0 is not a date"));
    expect(tapDay({ month: 2, day: 29 }, 2000)).toEqual({ day: 59, weekday: "Tuesday" });
  });
  it("Feb 29 exists exactly in the calendar spec's leap years (calendar_spec.is_leap_year, generated — not re-derived)", () => {
    expect(LEAP_YEAR_CASES.map((c) => [c.year, isLeapYear(c.year)])).toEqual(LEAP_YEAR_CASES.map((c) => [c.year, c.leap]));
    expect(LEAP_YEAR_CASES.map((c) => {
      try { return [c.year, tapDay({ month: 2, day: 29 }, c.year).day]; } catch (e) { return [c.year, (e as Error).message]; }
    })).toEqual([[1900, "regs: 1900-2-29 is not a date"], [2000, 59], [2026, "regs: 2026-2-29 is not a date"], [2028, 59],
      [2036, 59], [2100, "regs: 2100-2-29 is not a date"], [2400, 59]]);
  });
});

describe("segmentIndex (encode.segment_index)", () => {
  it("1. no moments: bisect", () => {
    expect([136, 167, 284, 121, 122, 1, 366].map((d) => segmentIndex(VEDDER, d, null, null))).toEqual([1, 2, 3, 0, 1, 0, 3]);
  });
  it("2. weekday split (klwa)", () => {
    expect(segmentIndex(KLWA.starts, 284, KLWA.moments, "Saturday")).toBe(1);
    expect(segmentIndex(KLWA.starts, 286, KLWA.moments, "Monday")).toBe(0);
    expect(() => segmentIndex(KLWA.starts, 284, KLWA.moments, null))
      .toThrow(new TapError("regs: day 284 reads differently by weekday or hour — name one"));
  });
  it("3. hours window (x15p7)", () => {
    expect(segmentIndex(X15P7.starts, 284, X15P7.moments, "Saturday")).toBe(4);
    expect(segmentIndex(X15P7.starts, 284, X15P7.moments, "Saturday", true)).toBe(5);
    expect(segmentIndex(X15P7.starts, 1, X15P7.moments, "Thursday")).toBe(0);
  });
  it("4. x32p0: an unrepeated start bisects even when the key has moments", () => {
    expect(segmentIndex(X32P0.starts, 284, X32P0.moments, null)).toBe(5);
    expect(segmentIndex(X32P0.starts, 200, X32P0.moments, "Monday")).toBe(3);
    expect(segmentIndex(X32P0.starts, 200, X32P0.moments, "Monday", true)).toBe(4);
  });
  it("5. day out of range", () => {
    expect(() => segmentIndex([1], 0, null, null)).toThrow(new TapError("regs: day 0 is not 1..366"));
    expect(() => segmentIndex([1], 367, null, null)).toThrow(new TapError("regs: day 367 is not 1..366"));
    expect(() => segmentIndex([1], 1.5, null, null)).toThrow(new TapError("regs: day 1.5 is not 1..366"));
  });
  it("no segment starts by the day (starts not beginning at 1): refused, never segment -1", () => {
    expect(() => segmentIndex([5, 9], 3, null, null)).toThrow(new TapError("regs: no segment starts by day 3"));
    expect(() => daySegments([5, 9], 3)).toThrow(new TapError("regs: no segment starts by day 3"));
    expect(segmentIndex([5, 9], 5, null, null)).toBe(0);                 // mutation pin: the first start itself is a segment
  });
  it("a repeated start with fewer moments than segments: refused, naming the missing moment", () => {
    expect(() => segmentIndex([1, 1], 5, [m(["Monday"])], "Sunday")).toThrow(new TapError("regs: no moment 1 for day 5"));
    expect(segmentIndex([1, 1], 5, [m(["Monday"])], "Monday")).toBe(0);   // a moment found before the gap is still read
  });
  it("no moment holds the weekday", () => {
    expect(() => segmentIndex([1, 1], 5, [m(["Monday"]), m(["Tuesday"])], "Sunday"))
      .toThrow(new TapError("regs: no moment of day 5 holds Sunday (inside=false)"));
  });
  it("Zymoetz: Fri 2036-02-29 reads day 59 on a Friday (the Fri–Sun moment); Thu Feb 28 and Tue 2028-02-29 do not", () => {
    const at = (month: number, day: number, y: number) => {
      const t = tapDay({ month, day }, y);
      return segmentIndex(ZYMOETZ.starts, t.day, ZYMOETZ.moments, t.weekday);
    };
    expect([at(2, 29, 2036), at(2, 28, 2036), at(2, 29, 2028), at(2, 28, 2028), at(3, 1, 2036)]).toEqual([1, 0, 0, 0, 1]);
  });
});

describe("daySegments (page_v36 daySegs)", () => {
  it("7. every segment sharing the day's start", () => {
    expect(daySegments([1, 1, 122, 122], 130)).toEqual([2, 3]);
    expect(daySegments([1, 122, 153, 183], 167)).toEqual([2]);
    expect(daySegments(ZYMOETZ.starts, 60)).toEqual([0, 1]);
  });
});

describe("profileIndex (static licence.profiles)", () => {
  const ix = (age: "adult" | "u16", residency: "resident" | "non_resident" | "non_resident_alien") =>
    profileIndex({ age, residency }, DIMS, PROFILES);
  it("8. the six app profiles, and guided / a status", () => {
    expect([ix("adult", "resident"), ix("u16", "resident"), ix("adult", "non_resident"), ix("u16", "non_resident"),
      ix("adult", "non_resident_alien"), ix("u16", "non_resident_alien")]).toEqual([0, 10, 20, 30, 40, 50]);
    expect(PROFILES[ix("u16", "non_resident_alien")]).toBe("non_resident_alien/under_16/non_guided/none");
    expect(profileIndex({ age: "adult", residency: "resident" }, DIMS, PROFILES, { guidance: "guided" })).toBe(5);
    expect(profileIndex({ age: "adult", residency: "non_resident" }, DIMS, PROFILES, { status: "aged_65_plus" })).toBe(24);
    expect(AGE_DIM).toEqual({ adult: "16_plus", u16: "under_16" });
  });
  it("reads the profile's position, not a computed one", () => {
    // mutation pin: an index computed from the dims (20·res + 10·age + 5·guidance + status) would still say 0 here
    expect(profileIndex({ age: "adult", residency: "resident" }, DIMS, [...PROFILES].reverse())).toBe(59);
  });
  it("refuses dims it does not know, a value outside a dim, and an absent profile", () => {
    expect(() => profileIndex({ age: "adult", residency: "resident" }, [...DIMS, ["boat", ["yes"]]], PROFILES))
      .toThrow(new TapError('regs: licence profile dims are ["residency","age","guidance","status","boat"], not ["residency","age","guidance","status"]'));
    expect(() => profileIndex({ age: "adult", residency: "resident" }, [DIMS[1]!, DIMS[0]!, DIMS[2]!, DIMS[3]!], PROFILES))
      .toThrow(new TapError('regs: licence profile dims are ["age","residency","guidance","status"], not ["residency","age","guidance","status"]'));
    expect(() => profileIndex({ age: "adult", residency: "resident" }, DIMS, PROFILES, { status: "veteran" }))
      .toThrow(new TapError('regs: status "veteran" is not one of ["none","indian_bc_resident","metis","disabled","aged_65_plus"]'));
    expect(() => profileIndex({ age: "adult", residency: "resident" }, DIMS, PROFILES.slice(1)))
      .toThrow(new TapError("regs: no licence profile resident/16_plus/non_guided/none"));
  });
});

describe("yearSegments (T09 displayByDay): every leap-calendar day's segment, each on its own weekday", () => {
  it("a key without moments: the bisect of each day, 366 long (the Vedder: Jun 1–30 is segment 2)", () => {
    const y = yearSegments(VEDDER, null, 2026);
    expect(y.length).toBe(366);
    expect([y[0], y[120], y[121], y[151], y[152], y[181], y[182], y[365]]).toEqual([0, 0, 1, 1, 2, 2, 3, 3]);
  });
  it("weekday moments read each day's weekday in the year asked (Kalamalka: Sat Oct 10 2026 → 1, Mon Oct 12 → 0)", () => {
    const y26 = yearSegments(KLWA.starts, KLWA.moments, 2026), y27 = yearSegments(KLWA.starts, KLWA.moments, 2027);
    expect([y26[283], y26[284], y26[285]]).toEqual([1, 1, 0]);      // Oct 10 Sat, Oct 11 Sun, Oct 12 Mon 2026
    expect([y27[283], y27[284], y27[285]]).toEqual([1, 0, 0]);      // Oct 10 Sun, Oct 11 Mon, Oct 12 Tue 2027
  });
  it("day 60 (no date reads it) is at its first moment, not at Mar 1's weekday", () => {
    const y = yearSegments(KLWA.starts, KLWA.moments, 2026);
    expect(y[59]).toBe(0);
    expect(y[60]).toBe(1);                                         // Sun Mar 1 2026
    expect(segmentIndex(KLWA.starts, 60, KLWA.moments, "Sunday")).toBe(1);   // pin: a weekday for day 60 would say 1
  });
  it("hours windows read outside the window (x15p7: segment 4 of Oct 10, not 5)", () => {
    expect(yearSegments(X15P7.starts, X15P7.moments, 2026)[283]).toBe(4);
  });
});
