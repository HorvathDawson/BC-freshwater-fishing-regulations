/** (T1194) The trend word beside the big reading: the measured flow now against about a day before, by the forecast's own "little change" factor. */
import { describe, expect, it } from "vitest";
import { GAUGE_PAGE_WORDS, LEVEL_DEADBAND_M, TREND_SLACK_MS, gaugeLevelTrend, gaugeTrend, observedTrend } from "./gaugePage";

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
  it("NEVER ACROSS A GAP: a gap at the line's end, or where the day-back value would be, says nothing (the look-back stays within 3 h of the 24 h mark)", () => {
    expect(TREND_SLACK_MS).toBe(3 * HOUR);
    const trailing = line([50, 26, 25, 1, 0], [10, 10, 10, 11, null]);                 // the latest value is 1 h before the end: fine
    expect(gaugeTrend(trailing.at, trailing.values)).toBe("steady");
    const endGap = line([48, 30, 12, 6, 0], [10, 10, 10, null, null]);          // no value for the last 12 h: the reading is not the line's end
    expect(gaugeTrend(endGap.at, endGap.values)).toBeNull();
    const midGap = line([48, 30, 12, 0], [10, null, 12, 12]);                   // nothing between 48 h and 12 h back: the 24 h mark is in a gap
    expect(gaugeTrend(midGap.at, midGap.values)).toBeNull();
  });
  it("a line that does not reach back a day, an empty line and unreadable stamps say nothing (never a guess)", () => {
    expect(gaugeTrend(...Object.values(line([10, 5, 0], [9, 9, 9])) as [string[], number[]])).toBeNull();
    expect(gaugeTrend([], [])).toBeNull();
    expect(gaugeTrend(["not a time", "nor this"], [1, 2])).toBeNull();
  });
});

describe("gaugeLevelTrend (a measured LEVEL line): the change in the stage, not a ratio", () => {
  const lvl = (before: number, now: number) => line([30, 24, 12, 0], [before, before, (before + now) / 2, now]);
  const word = (before: number, now: number) => { const { at, values } = lvl(before, now); return gaugeLevelTrend(at, values); };
  it("rising, falling and steady by the change over the day", () => {
    expect(word(1.0, 1.1)).toBe("rise");
    expect(word(1.1, 1.0)).toBe("fall");
    expect(word(1.0, 1.0)).toBe("steady");
  });
  it("INSIDE THE DEADBAND (2 cm either way, sensor noise) is steady; just past it is not", () => {
    expect(LEVEL_DEADBAND_M).toBe(0.02);
    expect(word(531.25, 531.268)).toBe("steady");          // a lake: 1.8 cm
    expect(word(531.25, 531.232)).toBe("steady");
    expect(word(531.25, 531.2701)).toBe("rise");
    expect(word(531.25, 531.2299)).toBe("fall");
    expect(word(0.384, 0.384)).toBe("steady");             // a ratio of stages would call 0.384 -> 0.5 "rising" at a far smaller change than it means: the CHANGE is judged
  });
  it("a ratio is never used: the same 5 cm is 'rising' at 0.2 m and at 531 m", () => {
    expect(word(0.2, 0.25)).toBe("rise");
    expect(word(531.2, 531.25)).toBe("rise");
  });
  it("a gap, a short line, an empty line: none (never a guess)", () => {
    const g = line([48, 30, 12, 6, 0], [1, 1, 1, null, null]);
    expect(gaugeLevelTrend(g.at, g.values)).toBeNull();
    const short = line([10, 5, 0], [1, 1.1, 1.2]);
    expect(gaugeLevelTrend(short.at, short.values)).toBeNull();
    expect(gaugeLevelTrend([], [])).toBeNull();
  });
});
