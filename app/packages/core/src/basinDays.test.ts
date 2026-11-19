/**
 * (T377 item 1) One watershed group's standing per day: the horizon chips' days, the app's standing words, the group's
 * name as sourced with core's word around it, and a day no gauge reads said — never blank.
 */
import { describe, expect, it } from "vitest";
import { BASIN_DAYS_WORDS, basinDaysSummary, basinDaysVM } from "./basinDays";
import { DAY_HORIZONS, type DayHorizon } from "./conditions";

const TUE = new Date(2026, 10, 3, 12);   // Tue 3 Nov 2026

describe("basinDaysVM", () => {
  const st = new Map<DayHorizon, number | null>([[0, 0.05], [1, 0.2], [2, 0.5], [3, 0.8], [4, 0.95], [5, null]]);
  const vm = basinDaysVM("Chilliwack River", st, 3, "discharge", TUE);

  it("the group's name as sourced, core's word around it; the scale by quantity", () => {
    expect(vm.title).toBe("Chilliwack River watershed");
    expect(vm.scale).toBe("Flow vs. past years, by day");
    expect(vm.verdict).toBe("Fri 6 · above normal");
    expect(basinDaysVM("Chilliwack River", st, "change", "discharge", TUE).verdict).toBeNull();
    expect(basinDaysVM("Chilliwack River", st, 0, "level", TUE).scale).toBe("Water level vs. past years, by day");
    expect(vm.list).toBe("Chilliwack River watershed, standing by day");
  });

  it("one row per day the horizon chips offer (Now, +1 … +10), dated as the chips are", () => {
    expect(vm.days.map((d) => d.day)).toEqual(DAY_HORIZONS);
    expect(vm.days.map((d) => d.chip).slice(0, 4)).toEqual(["Now", "Wed 4", "Thu 5", "Fri 6"]);
  });

  it("each day in the standing words the map's bands use; the whole name aloud", () => {
    expect(vm.days.slice(0, 5).map((d) => d.word))
      .toEqual(["Much below normal", "Below normal", "Normal", "Above normal", "Much above normal"]);
    expect(vm.days[0]!.label).toBe("Conditions now: much below normal");
    expect(vm.days[3]!.label).toBe("Forecast 3 days ahead: above normal, showing");
    expect(vm.days.filter((d) => d.on).map((d) => d.day)).toEqual([3]);
  });

  it("a day no gauge reads (null, or not in the map at all) says so, and the key says what the dash means", () => {
    for (const d of [vm.days[5]!, vm.days[9]!]) {
      expect(d.standing).toBeNull();
      expect(d.percentile).toBeNull();
      expect(d.word).toBe("No gauge reading");
    }
    expect(vm.days[5]!.label).toBe("Forecast 5 days ahead: no gauge reading");
    expect(vm.noneKey).toBe(BASIN_DAYS_WORDS.noneKey);
    const all = new Map(DAY_HORIZONS.map((d) => [d, 0.5] as const));
    expect(basinDaysVM("X", all, 0, "discharge", TUE).noneKey).toBeNull();
  });

  it("Change lights no day", () => {
    expect(basinDaysVM("X", st, "change", "discharge", TUE).days.some((d) => d.on)).toBe(false);
  });

  describe("(T872) the folded line: the first day, then only the days the standing changes", () => {
    const WED18 = new Date(2026, 10, 18, 20);   // Wed 18 Nov 2026: Now, Thu 19 … Sat 28
    const run = (ps: readonly (number | null)[]) =>
      basinDaysVM("Squamish River", new Map(ps.map((p, i) => [DAY_HORIZONS[i]!, p] as const)), 0, "discharge", WED18);

    it("the owner's Mamquam card: seven days Normal, three Much above normal, one with no reading", () => {
      const vm = run([0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.95, 0.95, 0.95, null]);
      expect(vm.days.map((d) => d.chip)).toEqual(["Now", "Thu 19", "Fri 20", "Sat 21", "Sun 22", "Mon 23", "Tue 24",
                                                   "Wed 25", "Thu 26", "Fri 27", "Sat 28"]);
      expect(vm.summary).toBe("Now: Normal · Much above normal from Wed 25 · No gauge reading from Sat 28");
      expect(vm.toggle)
        .toBe("Squamish River watershed, by day: Now: Normal · Much above normal from Wed 25 · No gauge reading from Sat 28");
    });

    it("one standing all the way says so, to the last day", () => {
      expect(run(Array(11).fill(0.5)).summary).toBe("Now: Normal · the same through Sat 28");
      expect(run(Array(11).fill(null)).summary).toBe("Now: No gauge reading · the same through Sat 28");
    });

    it("no reading now is said first; a return to an earlier band is a change of its own", () => {
      expect(run([null, 0.2, 0.2, 0.5, 0.5, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2]).summary)
        .toBe("Now: No gauge reading · Below normal from Thu 19 · Normal from Sat 21 · Below normal from Mon 23");
    });

    it("past three changes the rest are counted, never listed", () => {
      expect(run([0.5, 0.95, 0.5, 0.95, 0.5, 0.95, 0.5, 0.5, 0.5, 0.5, 0.5]).summary)
        .toBe("Now: Normal · Much above normal from Thu 19 · Normal from Fri 20 · Much above normal from Sat 21 · 3 more changes");
      expect(run([0.5, 0.95, 0.5, 0.95, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]).summary)
        .toBe("Now: Normal · Much above normal from Thu 19 · Normal from Fri 20 · Much above normal from Sat 21 · 1 more change");
    });

    it("the folded line does not move with the day shown", () => {
      const st = new Map(DAY_HORIZONS.map((d, i) => [d, i < 7 ? 0.5 : 0.95] as const));
      const a = basinDaysVM("X", st, 0, "discharge", WED18).summary;
      expect(basinDaysVM("X", st, 8, "discharge", WED18).summary).toBe(a);
      expect(basinDaysVM("X", st, "change", "discharge", WED18).summary).toBe(a);
      expect(basinDaysSummary([])).toBe("");
    });
  });
});
