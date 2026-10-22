/**
 * The conditions sentences, pinned WHOLE (AGENTS 33).
 *
 * These were composed inline in `DonorPanel` and `ConditionsPanel`, where a test could only
 * match a fragment of them — and a fragment you interpolated yourself cannot fail. Every
 * branch of every sentence is pinned here as the finished string a reader sees.
 */
import { describe, expect, it } from "vitest";
import {
  CONDITIONS_WORDS, chartCaption, chartFigure, chartLabel, chartReadoutLine, disagreeNote, donorBecause,
  donorFacts, donorLastReport,
  donorKeyLabel, donorPinTitle, donorReads, donorReason, donorRowLabel, donorShare,
  donorWeightLine, estimateHeadline, estimateRange, estimateSentence, forecastChartLabel,
  forecastModelOption, forecastRunLine, HORIZON_LABELS, HORIZONS, horizonChipLabel,
  noChartSentence, noGaugeSentence, readingHeading, readingStandingLine, regulatedNote,
  shortRecordNote, sizeApart, stageLine, unplacedNote, type DonorFacts,
} from "./conditions";
import type { Estimate, NoEstimate } from "./trust";

const row = (over: Partial<DonorFacts> = {}): DonorFacts => ({
  station: "08MH001", role: "up", percentile: 0.12, weight: 0.6, areaRatio: 2.4,
  areaKm2: 120, years: 40, regulated: false,
  factors: { share: 0.42, role: 1, record: 1 }, ...over,
});

const est = (over: Partial<Estimate> = {}): Estimate => ({
  percentile: 0.15, plusMinus: 12, trust: "close", spread: 6, donors: 2, ...over,
});

describe("the horizons", () => {
  it("are the publisher's days, and every one has its words", () => {
    // Mirrors HORIZONS in pipeline/gauges/feed/publish.py.
    expect(HORIZONS).toEqual([0, 1, 3, 5, 10]);
    // Object key order puts integer keys ascending, which is HORIZONS' own order.
    expect(Object.keys(HORIZON_LABELS).map(Number)).toEqual([...HORIZONS]);
  });

  it("names ten days ahead in full (owner 2026-10-20)", () => {
    expect(HORIZON_LABELS[10]).toEqual({ chip: "+10d", heading: "FORECAST · 10 DAYS AHEAD",
                                         accessible: "Forecast 10 days ahead" });
    expect(horizonChipLabel(10, true)).toBe("Forecast 10 days ahead, showing");
    expect(horizonChipLabel(10, false)).toBe("Forecast 10 days ahead");
  });

  it("names now and each forecast day in full", () => {
    expect(HORIZON_LABELS[0]).toEqual({ chip: "Now", heading: "ESTIMATE FOR THIS SPOT",
                                        accessible: "Conditions now" });
    expect(HORIZON_LABELS[1]).toEqual({ chip: "+1d", heading: "FORECAST · 1 DAY AHEAD",
                                        accessible: "Forecast 1 day ahead" });
    expect(HORIZON_LABELS[3]).toEqual({ chip: "+3d", heading: "FORECAST · 3 DAYS AHEAD",
                                        accessible: "Forecast 3 days ahead" });
    expect(HORIZON_LABELS[5]).toEqual({ chip: "+5d", heading: "FORECAST · 5 DAYS AHEAD",
                                        accessible: "Forecast 5 days ahead" });
  });

  it("says which chip is the one showing", () => {
    expect(horizonChipLabel(3, true)).toBe("Forecast 3 days ahead, showing");
    expect(horizonChipLabel(0, false)).toBe("Conditions now");
  });
});

describe("donorReason: why there is no estimate, per reason", () => {
  const WHY: Record<NoEstimate, string> = {
    "no-station": "No gauge on this water is close enough in size to speak for it.",
    "no-record": "None of the gauges that speak for this spot has a reading today that can be "
               + "ranked against its record.",
    "regulated": "The only gauges within reach are on regulated water, where a reading "
               + "describes a release schedule rather than rainfall.",
    "too-uncertain": "The gauges that can speak for this spot disagree too much to tell a "
                   + "low river from a high one.",
    "offline": "The live readings did not arrive.",
    // The chosen day is past every forecast run for these gauges (the owner's +10 day often
    // is). Said as that, not as "no gauge close enough", which would be false.
    "no-forecast": "No forecast for the gauges here reaches this far ahead.",
  };
  it.each(Object.entries(WHY))("%s", (why, sentence) => {
    expect(donorReason(why as NoEstimate)).toBe(sentence);
  });
});

describe("sizeApart", () => {
  it("says same size below 1.5×, one decimal below 10×, whole above", () => {
    expect(sizeApart(1.49)).toBe("same size");
    expect(sizeApart(1.5)).toBe("1.5× apart");
    expect(sizeApart(9.94)).toBe("9.9× apart");
    expect(sizeApart(10)).toBe("10× apart");
    expect(sizeApart(155.4)).toBe("155× apart");
    expect(sizeApart(Infinity)).toBe("—");
  });
});

describe("donorShare: what a donor contributed", () => {
  it("is quiet, never 0%, when the gauge is not reporting", () => {
    expect(donorShare(row({ percentile: null, weight: 0 }))).toBe("quiet");
  });
  it("never rounds a real share to nothing", () => {
    expect(donorShare(row({ weight: 0.003 }))).toBe("<1%");
    expect(donorShare(row({ weight: 0.01 }))).toBe("1%");
    expect(donorShare(row({ weight: 0 }))).toBe("0%");
    expect(donorShare(row({ weight: 0.6 }))).toBe("60%");
  });
  it("says it as a labelled line under the bar", () => {
    expect(donorWeightLine(row())).toBe("60% of this estimate");
    expect(donorWeightLine(row({ weight: 0.003 }))).toBe("<1% of this estimate");
    expect(donorWeightLine(row({ percentile: null }))).toBe("not counted today");
  });
});

describe("donorBecause: the model's three factors in words", () => {
  it("near-identical country, nothing discounted", () => {
    expect(donorBecause(row({ factors: { share: 0.995, role: 1, record: 1 } })))
      .toBe("Counts this much because it drains almost exactly the same country.");
  });
  it("every factor at once", () => {
    expect(donorBecause(row({ years: 10, factors: { share: 0.42, role: 0.85, record: 0.5 } })))
      .toBe("Counts this much because it is 42% as informative as a gauge on this very "
            + "water; it sits downstream, so it carries extra water; it has only 10 years "
            + "of record, so it counts 50%.");
  });
  it("one year is a year", () => {
    expect(donorBecause(row({ years: 1, factors: { share: 0.42, role: 1, record: 0.1 } })))
      .toBe("Counts this much because it is 42% as informative as a gauge on this very "
            + "water; it has only 1 year of record, so it counts 10%.");
  });
});

describe("the donor row's other lines", () => {
  it("says what the gauge itself reads", () => {
    expect(donorReads(row({ percentile: 0.2 }))).toBe("Reads lower than 8 days in 10 at "
                                                      + "this time of year.");
    expect(donorReads(row({ percentile: null }))).toBe("Not reporting today.");
  });

  it("states role, size and route as facts", () => {
    expect(donorFacts(row())).toBe("upstream · drains 120 km² · 2.4× apart");
    expect(donorFacts(row({ role: "down", regulated: true })))
      .toBe("downstream · drains 120 km² · 2.4× apart · dam-controlled");
    expect(donorFacts(row({ route: { path: ["a"], name: null } })))
      .toBe("upstream · drains 120 km² · 2.4× apart · on this reach");
    expect(donorFacts(row({ route: { path: ["a", "b"], name: null } })))
      .toBe("upstream · drains 120 km² · 2.4× apart · 1 reach away");
    expect(donorFacts(row({ route: { path: ["a", "b", "c"], name: null } })))
      .toBe("upstream · drains 120 km² · 2.4× apart · 2 reaches away");
    expect(donorFacts(row({ route: { path: [], name: null } })))
      .toBe("upstream · drains 120 km² · 2.4× apart");
  });

  it("describes the row for a screen reader", () => {
    expect(donorRowLabel(row(), false, false))
      .toBe("08MH001, upstream, 2.4× apart, 60 per cent of the answer");
    expect(donorRowLabel(row(), true, false))
      .toBe("08MH001, upstream, 2.4× apart, 60 per cent of the answer. Show this gauge's "
            + "chart");
    expect(donorRowLabel(row({ percentile: null, role: "down" }), true, true))
      .toBe("08MH001, downstream, 2.4× apart, not reporting today, so it counts for "
            + "nothing. Showing this gauge's chart");
  });

  it("labels its pin and its key, absorbing 'you are here' when it is the spot", () => {
    expect(donorPinTitle(row(), false)).toBe("08MH001 · 60% of the answer");
    expect(donorPinTitle(row(), true)).toBe("you are here · 08MH001 · 60% of the answer");
    expect(donorKeyLabel(row(), false)).toBe("08MH001 · 60%");
    expect(donorKeyLabel(row({ percentile: null }), true)).toBe("you are here · 08MH001 · quiet");
  });
});

describe("the estimate's headline, sentence and range", () => {
  it("leads with plain words", () => {
    expect(estimateHeadline(est())).toBe("Low for the time of year");
  });

  it("says now and forecast, one gauge and several", () => {
    expect(estimateSentence(est(), 0))
      .toBe("Lower than 8 days in 10 at this time of year — fairly confident, from 2 gauges "
            + "nearby.");
    expect(estimateSentence(est({ donors: 1, plusMinus: 22 }), 3))
      .toBe("Lower than 8 days in 10 at this time of year — very roughly, forecast from one "
            + "gauge nearby.");
  });

  it("gives the clamped range for the chosen day, plus the spot's catchment", () => {
    const day = new Date(2026, 8, 4, 12);            // 4 September, local
    expect(estimateRange(est(), day, 0, 50))
      .toBe("3rd–27th percentile for early September · this spot drains 50 km²");
    // The horizon moves the day the range is FOR: 4 Sep + 5 = 9 Sep, still early; the
    // day itself is the caller's, never the clock's.
    expect(estimateRange(est(), new Date(2026, 8, 8, 12), 5, null))
      .toBe("3rd–27th percentile for mid September");
    // Clamped to 1st–99th: the envelope stops short of the record's extremes.
    expect(estimateRange(est({ percentile: 0.02, plusMinus: 20 }), day, 0, null))
      .toBe("1st–22nd percentile for early September");
  });
});

describe("the notes under the working", () => {
  it("counts the gauges that could not be placed", () => {
    expect(unplacedNote(2)).toBe("2 of these gauges could not be placed on the map. They "
                                 + "still count toward the answer.");
    expect(unplacedNote(0)).toBeNull();
  });

  it("warns when most of the answer rests on a short record", () => {
    expect(shortRecordNote([row({ years: 6, weight: 0.8 }), row({ years: 60, weight: 0.2 })]))
      .toBe("Most of this rests on a gauge with only 6 years of record. That is few enough "
            + "that \"normal for this week\" is itself a rough idea, which is part of why the "
            + "range above is wide.");
    expect(shortRecordNote([row({ years: 1, weight: 1 })]))
      .toBe("Most of this rests on a gauge with only 1 year of record. That is few enough "
            + "that \"normal for this week\" is itself a rough idea, which is part of why the "
            + "range above is wide.");
    expect(shortRecordNote([row({ years: 6, weight: 0.2 }), row({ years: 60, weight: 0.8 })]))
      .toBeNull();
    expect(shortRecordNote([row({ years: 6, weight: 0.8, percentile: null })])).toBeNull();
    expect(shortRecordNote([])).toBeNull();
  });

  it("says a dam once, only for a gauge that is reporting", () => {
    expect(regulatedNote([row({ regulated: true })]))
      .toBe("This water is controlled by a dam. The reading is real, but it follows a "
            + "release schedule as much as the weather — a low river here can mean the gate "
            + "is shut rather than that the country is dry.");
    expect(regulatedNote([row({ regulated: true, percentile: null })])).toBeNull();
    expect(regulatedNote([row()])).toBeNull();
  });

  it("warns in words when the gauges disagree, above 25 points", () => {
    expect(disagreeNote(est({ spread: 44 })))
      .toBe("These gauges disagree by 44 points. That is a catchment doing more than one "
            + "thing, and the range above is the honest answer rather than their average.");
    expect(disagreeNote(est({ spread: 25 }))).toBeNull();
  });
});

describe("the gauge reading under the estimate", () => {
  it("names whose reading it is", () => {
    expect(readingHeading("Vedder River at Vedder Crossing"))
      .toBe("AT VEDDER RIVER AT VEDDER CROSSING");
  });

  it("ranks it with core's band word, or says there is nothing to rank against", () => {
    expect(readingStandingLine(0.15, "below")).toBe("BELOW NORMAL — 15th percentile for the date");
    expect(readingStandingLine(0.004, null))
      .toBe("NO RECORD — 0.4th percentile for the date");
    expect(readingStandingLine(null, "below"))
      .toBe("There is a reading here, but no record to compare it against.");
  });

  it("gives the stage beside the flow", () => {
    expect(stageLine(1.23)).toBe("1.23 m stage");
  });

  it("refuses in words when no gauge may speak", () => {
    expect(noGaugeSentence()).toBe("No gauge is entitled to speak for this water. A station "
      + "draining a far larger watershed would have given a number, and the number would "
      + "have been wrong.");
  });
});

describe("the chart's words", () => {
  it("labels each span, and its caption", () => {
    expect(chartLabel("72h")).toBe("the last 72 hours");
    expect(chartLabel("year")).toBe("the whole year against its record");
    expect(chartCaption("72h")).toBeUndefined();
    expect(chartCaption("year")).toBe(
      "Bands are this station's whole record for each five-day period. The bold line is "
      + "this year to date, which the feed accumulates day by day — it starts a month long "
      + "and grows, because the published daily record lags a year behind. The thin lines "
      + "are the last complete years out of that record. The frame ends a month past today, "
      + "where the longest forecast does.");
  });

  it("explains an empty chart per span", () => {
    expect(noChartSentence("year")).toBe("This station has no envelope in this quantity, so "
                                         + "there is no season to draw it against.");
    expect(noChartSentence("72h")).toBe("This station published no recent readings.");
  });

  it("names a forecast run, its issue time local and labelled (P16)", () => {
    const VAN = "America/Vancouver";
    expect(forecastRunLine({ model: "CLEVER", issuedAt: "2026-09-04T06:00:00Z",
                             horizonDays: 10 }, VAN))
      .toBe("CLEVER run issued 2026-09-03 at 23:00 PDT · 10-day outlook");
    // An offset is converted to the reader's zone, not printed as if it were theirs.
    expect(forecastRunLine({ model: "ELF", issuedAt: "2026-09-04T06:00:00-04:00",
                             horizonDays: 30 }, VAN))
      .toBe("ELF run issued 2026-09-04 at 03:00 PDT · 30-day outlook");
    expect(forecastRunLine({ model: "ELF", issuedAt: "garbage", horizonDays: 30 }))
      .toBe("ELF run · 30-day outlook");
    expect(forecastChartLabel({ model: "CLEVER", horizonDays: 10 })).toBe("CLEVER · 10 days");
    expect(forecastModelOption({ model: "CLEVER", horizonDays: 10 })).toBe("CLEVER · 10d");
  });

  it("keeps the control words in one table", () => {
    expect(CONDITIONS_WORDS.quantity).toEqual({ discharge: "Flow", level: "Level" });
    expect(CONDITIONS_WORDS.span).toEqual({ "72h": "Last 72 hours", year: "This year" });
    expect(CONDITIONS_WORDS.unit).toEqual({ discharge: "m³/s", level: "m" });
  });
});

describe("a quiet gauge: what it says and when it last spoke (owner 2026-10-21)", () => {
  const VAN = "America/Vancouver";
  const REF = Date.parse("2026-10-21T05:00:00Z");
  it("says not reporting when its last report is more than a day old, or unknown", () => {
    expect(donorReads(row({ percentile: null, observedAt: "2026-10-18T20:30:00-08:00" }), { ref: REF }))
      .toBe("Not reporting today.");
    expect(donorReads(row({ percentile: null }), { ref: REF })).toBe("Not reporting today.");
  });
  it("does not call a gauge that reported today silent", () => {
    expect(donorReads(row({ percentile: null, observedAt: "2026-10-20T20:30:00-08:00" }), { ref: REF }))
      .toBe("Reporting, but its reading cannot be ranked today.");
    // MUTATION PIN: without the day the answer is for, the old words stand.
    expect(donorReads(row({ percentile: null, observedAt: "2026-10-20T20:30:00-08:00" })))
      .toBe("Not reporting today.");
  });
  it("a station ECCC does not list as real-time is not a real-time gauge, never 'not reporting'", () => {
    // `realtime: false` comes from the feed index's `listed`; a listed gauge that is silent keeps today's words.
    expect(donorReads(row({ percentile: null, realtime: false }), { ref: REF }))
      .toBe("Not a real-time gauge.");
    expect(donorReads(row({ percentile: null, realtime: false, observedAt: "2026-10-20T20:30:00-08:00" }),
                      { ref: REF, horizon: 3 }))
      .toBe("Not a real-time gauge.");
    expect(donorReads(row({ percentile: null, realtime: true }), { ref: REF })).toBe("Not reporting today.");
    expect(donorRowLabel(row({ percentile: null, realtime: false }), false, false))
      .toBe("08MH001, upstream, 2.4× apart, not a real-time gauge, so it counts for nothing");
    expect(donorRowLabel(row({ percentile: null, realtime: true }), false, false))
      .toBe("08MH001, upstream, 2.4× apart, not reporting today, so it counts for nothing");
  });
  it("a day ahead with no forecast is a missing forecast", () => {
    expect(donorReads(row({ percentile: null }), { ref: REF, horizon: 3 }))
      .toBe("No forecast for this gauge that far ahead.");
    expect(donorReads(row({ percentile: 0.2 }), { ref: REF, horizon: 3 }))
      .toBe("Reads lower than 8 days in 10 at this time of year.");
  });
  it("names the last report local and labelled, or says it is not known", () => {
    expect(donorLastReport("2026-10-18T20:30:00-08:00", VAN)).toBe("Last reported 2026-10-18 at 21:30 PDT.");
    expect(donorLastReport(null, VAN)).toBe("When it last reported is not known.");
    expect(donorLastReport("garbage", VAN)).toBe("When it last reported is not known.");
  });
  it("says a day-only report (a daily mean) as its own day, never the evening before", () => {
    expect(donorLastReport("2026-09-25", VAN)).toBe("Last reported on 2026-09-25.");
    // MUTATION PIN: parsed as an instant it would be 2026-09-24 at 17:00 PDT.
    expect(donorLastReport("2026-09-25", VAN)).not.toContain("09-24");
  });
});

describe("the chart's read-out and controls", () => {
  const VAN = "America/Vancouver";
  it("rounds to what a reader reads", () => {
    expect([1234.5, 123.4, 12.34, 1.234, 0.3349].map(chartFigure)).toEqual(["1235", "123", "12.3", "1.23", "0.33"]);
  });
  it("says a reading with its time, unit and the median, whole", () => {
    expect(chartReadoutLine({ t: Date.parse("2026-10-20T21:00:00Z"), value: 4.123, unit: "m³/s",
                              kind: "reading", daily: false, median: 10.61 }, VAN))
      .toBe("2026-10-20 at 14:00 PDT · 4.12 m³/s · median 10.6");
  });
  it("marks a forecast step as a forecast, and leaves a missing median out", () => {
    expect(chartReadoutLine({ t: Date.parse("2026-10-22T07:00:00Z"), value: 13.9, unit: "m³/s",
                              kind: "forecast", daily: false, median: null }, VAN))
      .toBe("2026-10-22 at 00:00 PDT · 13.9 m³/s forecast");
  });
  it("says a daily mean's own calendar day, never the evening before", () => {
    expect(chartReadoutLine({ t: Date.parse("2026-03-05"), value: 0.335, unit: "m", kind: "reading",
                              daily: true }, VAN))
      .toBe("2026-03-05 · 0.34 m");
  });
  it("names the zoom controls and the gauge switcher in one table", () => {
    expect(CONDITIONS_WORDS.zoom).toEqual({ in: "Zoom in", out: "Zoom out", fit: "Whole chart", group: "Chart zoom" });
    expect(CONDITIONS_WORDS.chartGauge).toBe("CHART THIS GAUGE");
    expect(CONDITIONS_WORDS.zoomHint.touch)
      .toBe("Pinch to zoom, drag to move along, tap to read a value. Double-tap for the whole chart.");
    expect(CONDITIONS_WORDS.zoomHint.pointer)
      .toBe("Scroll to zoom, drag to move along, point to read a value. Double-click for the whole chart.");
  });
});
