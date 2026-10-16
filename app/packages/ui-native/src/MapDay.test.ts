import { describe, expect, it } from "vitest";
import { dayLabel, shiftDay } from "./MapDay";

describe("the map's day stepper", () => {
  it("steps a day and says it in words", () => {
    expect(dayLabel(shiftDay(new Date(2026, 9, 10), 1))).toBe("Oct 11");
    expect(dayLabel(shiftDay(new Date(2026, 0, 1), -1))).toBe("Dec 31");
  });
  it("steps a month keeping the day, clamped to the month's end", () => {
    expect(dayLabel(shiftDay(new Date(2026, 9, 10), 1, true))).toBe("Nov 10");
    expect(dayLabel(shiftDay(new Date(2026, 0, 31), 1, true))).toBe("Feb 28");
  });
});
