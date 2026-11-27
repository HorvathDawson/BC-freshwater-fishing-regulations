/** (T1194) The trend word beside the big reading: the measured flow now against about a day before, by the forecast's own "little change" factor. */
import { describe, expect, it } from "vitest";
import { GAUGE_PAGE_WORDS, gaugeTrend, observedTrend } from "./gaugePage";

const HOUR = 3_600_000, T0 = Date.parse("2026-11-26T12:00:00Z");
const line = (hoursBack: number[], vals: (number | null)[]) => ({ at: hoursBack.map((h) => new Date(T0 - h * HOUR).toISOString()), values: vals });

describe("observedTrend", () => {
  it("a quarter more is rising, a fifth less is falling, within 1.25x either way is steady; no earlier flow or no reading: none", () => {
    expect(observedTrend(10, 12.5)).toBe("rise");
    expect(observedTrend(10, 12.4)).toBe("steady");
    expect(observedTrend(10, 8)).toBe("fall");
    expect(observedTrend(10, 8.1)).toBe("steady");
    expect(observedTrend(0, 5)).toBeNull();
    expect(observedTrend(10, -1)).toBeNull();
  });
  it("the words", () => { expect(GAUGE_PAGE_WORDS.trend).toEqual({ rise: "rising", steady: "steady", fall: "falling" }); });
});

describe("gaugeTrend (a measured line)", () => {
  it("the last value against the latest one at least a day earlier", () => {
    const { at, values } = line([48, 30, 24, 12, 0], [20, 14, 12, 9, 8]);
    expect(gaugeTrend(at, values)).toBe("fall");                  // 8 against the value 24 h back (12)
  });
  it("a trailing gap is skipped; a gap where the day-back value would be is passed over for the one before it", () => {
    const { at, values } = line([48, 24, 1, 0], [10, null, 11, null]);
    expect(gaugeTrend(at, values)).toBe("steady");                // 11 against the 48 h-back 10
  });
  it("a line that does not reach back a day, an empty line and unreadable stamps say nothing (never a guess)", () => {
    expect(gaugeTrend(...Object.values(line([10, 5, 0], [9, 9, 9])) as [string[], number[]])).toBeNull();
    expect(gaugeTrend([], [])).toBeNull();
    expect(gaugeTrend(["not a time", "nor this"], [1, 2])).toBeNull();
  });
});
