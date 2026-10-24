/**
 * T52 — the Change view's words, whole (AGENTS 33), on the runs the feed carried 2026-10-22T22:28:51Z
 * (`pipeline/tests/fixtures/gauge_feed/forecast_change/`, the numbers `pipeline/tests/test_forecast_change.py` pins on the
 * publisher's side). Today is Thu 22 Oct 2026.
 */
import { describe, expect, it } from "vitest";
import { CHANGE_LEGEND, CHANGE_POLICY, CHANGE_STOP_RATIOS, changeDay, changeDotLabel, changeSparkWords, changeHeadline,
         changeLine, changeRange, changeRatio, changeReachNote, changeReads, changeSentence, changeSourceLine,
         changeSourceName, CHANGE_LOW_WATER, changeLowWaterNote, codedChange, ratioWords, type SourceChange,
         type RunChange } from "./change";
import { CONDITIONS_WORDS, donorReads, donorReason, estimateHeadline, estimateRange, estimateSentence,
         HORIZON_LABELS, isAhead, aheadDays, silentLine, type DonorFacts } from "./conditions";
import { STALE_AFTER } from "./staleness";
import type { Estimate } from "./trust";

const TODAY = "2026-10-22";
const none = { day: TODAY, ratio: 1, value: 1, back: null };

/** 08HA003, CLEVER: a 23-fold spike ("about 25×": whole numbers to 20, then fives) on Mon 26 Oct — day four, between +3 and +5 — back down by Tue. */
const SPIKE: RunChange = {
  dir: "rise", base: 0.467,
  peak: { day: "2026-10-26", ratio: 22.7944, value: 10.645, percentile: 0.7717, points: 0.6717, back: "2026-10-27" },
  trough: { ...none, value: 0.467, percentile: 0.1, points: 0 },
  days: [["2026-10-22", 0.467, 0.467], ["2026-10-23", 0.467, 0.467], ["2026-10-24", 0.467, 0.467],
         ["2026-10-25", 0.467, 10.617], ["2026-10-26", 1.391, 10.645], ["2026-10-27", 0.469, 1.253],
         ["2026-10-28", 0.468, 0.469], ["2026-10-29", 0.468, 2.151], ["2026-10-30", 0.642, 2.152],
         ["2026-10-31", 0.47, 0.615]],
};
/** 08MH029, CLEVER: up 2.8x by Wed 28 Oct and holding to the end of the run. */
const STEP: RunChange = {
  dir: "rise", base: 0.768,
  peak: { day: "2026-10-28", ratio: 2.7695, value: 2.127, percentile: 0.5882, points: 0.3929, back: null },
  trough: { day: "2026-10-27", ratio: 0.8737, value: 0.671, percentile: 0.1026, points: -0.0927, back: "2026-10-28" },
  days: [],
};
/** 08EE020, ELF: falling to 0.59x by Sun 1 Nov, a quarter of the record down for the date. */
const FALL: RunChange = {
  dir: "fall", base: 11.2111,
  peak: { ...none, value: 11.2111, percentile: 0.4052, points: 0 },
  trough: { day: "2026-11-01", ratio: 0.5884, value: 6.5968, percentile: 0.1502, points: -0.2551, back: null },
  days: [],
};

describe("one model's expected change, said whole", () => {
  it("a spike between the sampled days: dated, and when it is back down", () => {
    expect(changeLine("CLEVER", SPIKE, TODAY)).toBe(
      "CLEVER: rises to about 25× today's flow on Mon (high for the time of year), back down by Tue.");
  });

  it("a step: dated, and that it stays up", () => {
    expect(changeLine("CLEVER", STEP, TODAY)).toBe(
      "CLEVER: rises to about 3× today's flow by Wed (about normal for the time of year) and stays up.");
  });

  it("a fall a week or more out names the date as well as the day", () => {
    expect(changeLine("ELF", FALL, TODAY)).toBe(
      "ELF: falls to about 60% of today's flow by Sun 1 Nov (low for the time of year).");
  });

  it("a move still going on the last day shown is never said to stay up: the forecast stops there (T52 review)", () => {
    // 08EE020's ELF run as read: its window ends Sun 1 Nov, the very day of its lowest
    const fallDays: RunChange["days"] = [["2026-10-31", 6.7, 6.7], ["2026-11-01", 6.5968, 6.5968]];
    expect(changeLine("ELF", { ...FALL, days: fallDays }, TODAY)).toBe(
      "ELF: falls to about 60% of today's flow by Sun 1 Nov (low for the time of year), still falling on the last day "
      + "shown.");
    // a COFFEE run (five days) still climbing on its last
    const open: RunChange = { ...STEP, days: [["2026-10-27", 1.5, 1.6], ["2026-10-28", 2.0, 2.127]] };
    expect(changeLine("COFFEE", open, TODAY)).toBe(
      "COFFEE: rises to about 3× today's flow by Wed (about normal for the time of year), still rising on the last day "
      + "shown.");
    // the same peak with a day after it, held: a step
    expect(changeLine("COFFEE", { ...open, days: [...open.days, ["2026-10-29", 2.1, 2.12]] }, TODAY)).toBe(
      "COFFEE: rises to about 3× today's flow by Wed (about normal for the time of year) and stays up.");
  });

  it("steady says so, and says nothing of the record", () => {
    expect(changeLine("COFFEE", { ...FALL, dir: "steady" }, TODAY)).toBe("COFFEE: little change expected.");
  });

  it("both, in the order they happen", () => {
    const both: RunChange = { ...SPIKE, dir: "both",
      trough: { day: "2026-10-30", ratio: 0.4, value: 0.19, back: null } };
    expect(changeLine("CLEVER", both, TODAY)).toBe(
      "CLEVER: rises to about 25× today's flow by Mon (high for the time of year), "
      + "then falls to about 40% of today's flow by Fri 30 Oct.");
  });

  it("the record is named only when the move against it is big (a quarter of the record)", () => {
    const small = { ...SPIKE, peak: { ...SPIKE.peak, points: CHANGE_POLICY.bigPoints - 0.01 } };
    expect(changeLine("CLEVER", small, TODAY)).toBe(
      "CLEVER: rises to about 25× today's flow on Mon, back down by Tue.");
  });
});

describe("the Change block's other words, owned here (AGENTS: core owns wording)", () => {
  it("the day line's mark and its screen-reader name", () => {
    expect(changeSparkWords(false, "2026-10-26", "2026-10-22", "2026-10-31", TODAY)).toEqual({
      mark: "Peak Mon", label: "Each day's highest forecast flow against today's, from today to Sat 31 Oct" });
    expect(changeSparkWords(true, "2026-11-01", "2026-10-22", "2026-11-01", TODAY)).toEqual({
      mark: "Lowest Sun 1 Nov", label: "Each day's lowest forecast flow against today's, from today to Sun 1 Nov" });
  });

  it("no run for the charted gauge, and the legend bar's name", () => {
    expect(CONDITIONS_WORDS.changeNone).toBe("No forecast is running for this gauge now.");
    expect(CHANGE_LEGEND.describe)
      .toBe("Expected change, next 10 days: colours from falling to rising; steady is little change");
  });
});

describe("the pieces", () => {
  it("names a forecast day from today", () => {
    expect(["2026-10-22", "2026-10-23", "2026-10-24", "2026-10-28", "2026-10-29", "2026-11-01"]
      .map((d) => changeDay(d, TODAY))).toEqual(["today", "tomorrow", "Sat", "Wed", "Thu 29 Oct", "Sun 1 Nov"]);
  });

  it("words a ratio to today's flow", () => {
    expect([150, 22.79, 2, 1.94, 1.4, 1.24, 1, 0.81, 0.79, 0.5884, 0.02].map(ratioWords)).toEqual([
      "more than 100× today's flow", "about 25× today's flow", "about 2× today's flow",
      "about 95% more than today's flow", "about 40% more than today's flow", "about today's flow",
      "about today's flow", "about today's flow", "about 80% of today's flow", "about 60% of today's flow",
      "about 5% of today's flow"]);
  });

  it("heads a section's change by the publisher's thresholds", () => {
    expect([2, 1.99, 1.25, 1.2, 0.81, 0.8, 0.5].map(changeHeadline)).toEqual([
      "A big rise expected", "Rising", "Rising", "Little change expected", "Little change expected", "Falling",
      "A big fall expected"]);
  });

  it("decodes what the publisher coded (Φ(log2 r / 2)), both ways", () => {
    // the publisher's own codes (test_forecast_change.py): 1x 0.5, 2x 0.691462, half 0.308538, 4x 0.841345
    expect([0.5, 0.691462, 0.308538, 0.841345].map(changeRatio)).toEqual(
      [1, 2, 0.5, 4].map((r) => expect.closeTo(r, 4)));
    // 08HA003's index row: discharge 0.9879 is its 22.79x, decoded within the code's 4 decimals
    expect(changeRatio(0.9879)).toBeCloseTo(22.8, 0);
    expect(CHANGE_STOP_RATIOS).toEqual([0.25, 0.5, 1, 2, 4]);
    expect(CHANGE_STOP_RATIOS.map(codedChange)).toEqual(
      [0.158655, 0.308538, 0.5, 0.691462, 0.841345].map((p) => expect.closeTo(p, 5)));
  });

  it("goes stale on the same line as every forecast run", () => {
    expect(CHANGE_POLICY.staleHours * 3_600_000).toBe(STALE_AFTER.forecast.ms);
  });

  it("names its legend", () => {
    expect([CHANGE_LEGEND.title, CHANGE_LEGEND.low, CHANGE_LEGEND.mid, CHANGE_LEGEND.high])
      .toEqual(["Expected change, next 10 days", "Falling", "Steady", "Rising"]);
    expect(CHANGE_LEGEND.steady).toEqual([expect.closeTo(0.4361, 4), expect.closeTo(0.5639, 4)]);
  });
});

describe("the Change horizon on the conditions surfaces", () => {
  const e = (ratio: number): Estimate => ({ percentile: codedChange(ratio), plusMinus: 12, trust: "close",
                                            spread: 4, donors: 2 } as Estimate);

  it("is the last chip, after +10, and is ahead without being a day", () => {
    expect(HORIZON_LABELS.change).toEqual({ chip: "Change", heading: "EXPECTED CHANGE · NEXT 10 DAYS",
                                            accessible: "Expected change over the next 10 days" });
    expect([isAhead("change"), aheadDays("change"), isAhead(0), aheadDays(5)]).toEqual([true, 0, false, 5]);
  });

  it("heads, says and ranges a section's expected change", () => {
    expect(estimateHeadline(e(3), "change")).toBe("A big rise expected");
    expect(estimateSentence(e(3), "change")).toBe(
      "At its highest, about 3× today's flow — fairly confident, forecast from 2 gauges nearby.");
    expect(estimateSentence(e(1.1), "change")).toBe(
      "Staying near today's flow — fairly confident, forecast from 2 gauges nearby.");
    expect(estimateRange(e(3), new Date(2026, 9, 22), "change", 50)).toBe(
      "Between 1.8× and 6× today's flow over the next 10 days · this spot drains 50 km²");
    expect(changeRange(0.5, 0.5)).toBe("About 1.0× today's flow over the next 10 days");
  });

  it("refuses honestly when no donor has a forecast running — never steady", () => {
    expect(donorReason("no-forecast", "change")).toBe(
      "No forecast is running for the gauges here now. The River Forecast Centre's models are seasonal, so out of "
      + "season there is no expected change to show.");
    expect(donorReason("no-forecast", 3)).toBe("No forecast for the gauges here reaches this far ahead.");
  });

  it("words each donor's own expectation, and a silent one", () => {
    const row = { station: "08HA003", percentile: codedChange(22.79) } as DonorFacts;
    expect(donorReads(row, { horizon: "change" })).toBe("Expects about 25× today's flow at its highest.");
    expect(donorReads({ ...row, percentile: 0.5 }, { horizon: "change" })).toBe("Expects little change.");
    expect(changeReads(0.59)).toBe("Expects about 60% of today's flow at its lowest.");
    const quiet = { ...row, percentile: null, observedAt: null, noData: false } as unknown as DonorFacts;
    expect(donorReads(quiet, { horizon: "change" })).toBe("No forecast running for this gauge now.");
    expect(silentLine([quiet], { ref: Date.UTC(2026, 9, 22), horizon: "change" })).toBe(
      "No forecast running: 08HA003 (last report not known)");
  });

  it("says the caveat the spike implies", () => {
    expect(CONDITIONS_WORDS.changeExplainer).toBe(
      "Each gauge's forecast is read day by day for its largest rise or fall over the next 10 days, against its own "
      + "forecast for today. The figure for this spot combines them the way every estimate here does, so the gauges "
      + "behind it may peak on different days.");
  });

  it("a sentence for one gauge", () => {
    expect(changeSentence(0.59, "roughly", 1)).toBe(
      "At its lowest, about 60% of today's flow — roughly, forecast from one gauge nearby.");
  });
});

describe("a gauge dot under Change (T126)", () => {
  it("says the station's own ratio to today, short", () => {
    expect([2, 0.5, 1, 22.7944, 544].map((r) => changeDotLabel(r)))
      .toEqual(["2× today", "0.5× today", "1.0× today", "23× today", "100×+ today"]);
  });

  it("marks a gauge whose change is ELF's alone, and the legend says what the mark means (T157)", () => {
    // mutation: `lowWater` ignored -> the first fails; the legend line not naming the mark -> the second fails
    expect(changeDotLabel(0.88, true)).toBe("0.9× today · low-water model");
    expect(CHANGE_LEGEND.lowWater).toBe('"low-water model": forecast by ELF alone, which has no rain in it and cannot '
      + "see a rise coming.");
  });
});

describe("the section's low-water caveat (T157)", () => {
  it("names the gauges whose change is ELF's alone, whole, and says whether that is all of it", () => {
    expect(changeLowWaterNote(["08MG013"], false)).toBe("Part of this change comes from ELF alone (08MG013). "
      + "ELF is a low-water model: it carries the river's recent fall forward and has no rain or snowmelt in it, so it "
      + "cannot see a rise coming.");
    expect(changeLowWaterNote(["08MG013", "08MG005", "08MF040"], true)).toBe("This change comes from ELF alone "
      + "(08MG013, 08MG005 and 08MF040). ELF is a low-water model: it carries the river's recent fall forward and has "
      + "no rain or snowmelt in it, so it cannot see a rise coming.");
    expect(changeLowWaterNote([], true)).toBeNull();
  });
});

/** 08HB025's station change (T144): CLEVER 47x and COFFEE 544x joined, 161x on Sun 25 Oct, back by Mon; COFFEE stops Mon. */
const JOINED: SourceChange = {
  models: ["CLEVER", "COFFEE"], lowWater: false, reach: { CLEVER: "2026-10-31", COFFEE: "2026-10-26" },
  dir: "rise", base: 1,
  peak: { day: "2026-10-25", ratio: 160.6636, value: 160.6636, percentile: 0.95, points: 0.9, back: "2026-10-26" },
  trough: { day: "2026-10-27", ratio: 0.9833, value: 0.9833, percentile: 0.05, points: 0, back: "2026-10-30" },
  days: [["2026-10-22", 1, 1.0878], ["2026-10-25", 94.7657, 160.6636], ["2026-10-26", 12.2515, 41.8312],
         ["2026-10-31", 1.15, 1.25]],
};
/** ELF alone, falling to 0.4x by Sun. */
const LOW: SourceChange = {
  models: ["ELF"], lowWater: true, reach: { ELF: "2026-10-25" }, dir: "fall", base: 1,
  peak: { ...none }, trough: { day: "2026-10-25", ratio: 0.4, value: 0.4, back: null },
  days: [["2026-10-22", 1, 1], ["2026-10-25", 0.4, 0.4]],
};

describe("the station's change, by the models the publisher chose (T144)", () => {
  it("names who it is from", () => {
    expect([["CLEVER", "COFFEE"], ["CLEVER"], ["ELF"]].map(changeSourceName))
      .toEqual(["CLEVER and COFFEE together", "CLEVER", "ELF"]);
  });

  it("says a joined change as one line", () => {
    expect(changeSourceLine(JOINED, TODAY)).toBe(
      "CLEVER and COFFEE together: rises to more than 100× today's flow on Sun (very high for the time of year), "
      + "back down by Mon.");
  });

  // T144 review: 08HD027 on the 2026-10-23 feed — COFFEE 11x on its last day (Mon), CLEVER 1.06x; joined 3.5x, and the
  // next day CLEVER alone. The handover is not a return: the publisher marks the end `until` Mon, never `back` Tue.
  it("says a joined move still on where one model stops, never 'back down' from the handover", () => {
    const held: SourceChange = { ...JOINED, peak: { day: "2026-10-26", ratio: 3.4797, value: 3.4797, back: null,
                                                    until: "2026-10-26" } };
    expect(changeSourceLine(held, TODAY)).toBe("CLEVER and COFFEE together: rises to about 3× today's flow by Mon, "
      + "still up when COFFEE's forecast ends.");
    const low: SourceChange = { ...JOINED, dir: "fall", trough: { day: "2026-10-25", ratio: 0.4, value: 0.4,
                                                                  back: null, until: "2026-10-26" } };
    expect(changeSourceLine(low, TODAY)).toBe("CLEVER and COFFEE together: falls to about 40% of today's flow by Sun, "
      + "still down when COFFEE's forecast ends.");
  });

  it("says where a joined change becomes one model's alone", () => {
    expect(changeReachNote(JOINED, TODAY)).toBe("COFFEE runs to Mon; after that, CLEVER alone.");
    expect(changeReachNote({ ...JOINED, reach: { CLEVER: "2026-10-26", COFFEE: "2026-10-26" } }, TODAY)).toBeNull();
    expect(changeReachNote(LOW, TODAY)).toBeNull();
  });

  it("says ELF alone with its own name, and the low-water caveat in full", () => {
    expect(changeSourceLine(LOW, TODAY)).toBe("ELF: falls to about 40% of today's flow by Sun, "
      + "still falling on the last day shown.");
    expect(CHANGE_LOW_WATER).toBe("ELF is a low-water model: it carries the river's recent fall forward and has no "
      + "rain or snowmelt in it, so it cannot see a rise coming.");
  });
});
