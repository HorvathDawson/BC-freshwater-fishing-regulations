/**
 * (T377 item 1) One watershed group's standing per day: the horizon chips' days, the app's standing words, the group's
 * name as sourced with core's word around it, and a day no gauge reads said — never blank.
 */
import { describe, expect, it } from "vitest";
import { BASIN_DAYS_WORDS, basinDaysVM } from "./basinDays";
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
});
