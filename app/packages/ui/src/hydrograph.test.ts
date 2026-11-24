import { describe, expect, it } from "vitest";
import type { Band } from "@app/core";
import { buildHydrograph, logTicks, minSpan, niceTicks, yearScale, DEFAULT_BOX } from "./hydrograph";

const BAND: Band = [18.8, 23.0, 30.6, 39.0, 56.6];   // 08MH001, 30 Aug, real
const flat = <T,>(n: number, v: T): T[] => Array.from({ length: n }, () => v);

describe("ticks", () => {
  it("uses numbers a person reads", () => {
    for (const t of niceTicks(0, 100, 4)) expect(String(t)).toMatch(/^\d+(\.\d)?$/);
  });
  it("never leaves an axis with two gridlines on it", () => {
    expect(niceTicks(8.3, 59.5, 4).length).toBeGreaterThanOrEqual(4);
  });
  it("log ticks are 1, 2, 5 per decade", () => {
    expect(logTicks(10, 1000)).toEqual([10, 20, 50, 100, 200, 500, 1000]);
  });
});

// (T1043) CAPILANO AT CANYON'S 72 H LEVEL, 2026-11-21..23 (EC realtime, real): 0.585 to 0.6 m with no band. The axis
// stepped 0.005 m and wrote "0.59 0.59 0.6 0.6". MUTATION: drop `[lo, hi] = minSpan(lo, hi)` from `buildHydrograph` ->
// the Capilano labels repeat. One rule for every station and either quantity (`minSpan`), no station named in the code.
describe("(T1043) no two gridlines carry the same label", () => {
  const CAPILANO = [0.592, 0.592, 0.591, 0.585, 0.585, 0.593, 0.592, 0.6, 0.592, 0.585, 0.595, 0.592];
  const labels = (values: number[]) => buildHydrograph({ values, bands: values.map(() => null), xLabels: [] })
    .yTicks.map((t) => t.label);
  it("Capilano's barely-moving level: distinct labels, round values, the line inside them", () => {
    const got = labels(CAPILANO);
    expect(got).toEqual(["0.58", "0.59", "0.6", "0.61"]);
  });
  it("any size: a lake at 340 m moving 3 cm, a flow of 1.8, one at 1,000, and one crossing 10", () => {
    for (const values of [[340.21, 340.23, 340.24], [1.8, 1.8, 1.81], [1001, 1004, 1009], [9.98, 9.99, 9.995]]) {
      const got = labels(values);
      expect(new Set(got).size, `${values} -> ${got}`).toBe(got.length);
      expect(got.length, `${values} -> ${got}`).toBeGreaterThanOrEqual(3);
    }
  });
  it("leaves an axis that already reads apart alone, and never goes below 0", () => {
    expect(minSpan(8.3, 59.5)).toEqual([8.3, 59.5]);
    expect(minSpan(0.001, 0.003)[0]).toBe(0);
    const [lo, hi] = minSpan(0.585, 0.6);
    expect(lo <= 0.585 && hi >= 0.6).toBe(true);
  });
});

// (T1043, owner 2026-11-23: "the scale of the chart should use the year data if not enough histroical data exists")
// MUTATIONS: drop the `if (byYear)` widening -> the Capilano case scales to its own wiggle ("reading", top < 1);
// let `byYear` ignore a band (drop `!banded &&`) -> the banded chart's scale moves and reads "year".
describe("(T1043) a chart with no band is scaled to its year", () => {
  const CAPILANO = [0.592, 0.592, 0.591, 0.585, 0.585, 0.593, 0.592, 0.6, 0.592, 0.585, 0.595, 0.592];
  const YEAR = { lo: 0.55, hi: 2.21 };      // 2026's daily levels at Canyon run ~0.55 to ~2.2 m
  const top = (h: ReturnType<typeof buildHydrograph>) => Math.max(...h.yTicks.map((t) => t.value));
  it("no band: the year's low and high set the axis (with the usual padding), and it says so", () => {
    const h = buildHydrograph({ values: CAPILANO, bands: CAPILANO.map(() => null), xLabels: [], scaleTo: YEAR });
    expect(h.scale).toBe("year");
    expect(top(h)).toBeGreaterThanOrEqual(2);
    expect(Math.min(...h.yTicks.map((t) => t.value))).toBeLessThanOrEqual(0.6);
  });
  it("no year either: the drawn values and the minimum span, said as `reading`", () => {
    for (const scaleTo of [undefined, null]) {
      const h = buildHydrograph({ values: CAPILANO, bands: CAPILANO.map(() => null), xLabels: [], scaleTo });
      expect([h.scale, h.yTicks.map((t) => t.label)]).toEqual(["reading", ["0.58", "0.59", "0.6", "0.61"]]);
    }
  });
  it("a band on screen keeps today's scale: the year is not read", () => {
    const values = flat(72, 15.7);
    const plain = buildHydrograph({ values, bands: flat(72, BAND), xLabels: [] });
    const given = buildHydrograph({ values, bands: flat(72, BAND), xLabels: [], scaleTo: { lo: 1, hi: 900 } });
    expect([given.scale, given.yTicks]).toEqual(["band", plain.yTicks]);
  });
  it("a log axis (a year of flow) is its own scale", () => {
    const h = buildHydrograph({ values: [10, 100, 1000], bands: [null, null, null], xLabels: [], log: true,
                                scaleTo: { lo: 1, hi: 5000 } });
    expect(h.scale).toBe("log");
  });
  it("the year's range is the 5th to 95th percentile of this year's and the earlier years' values; nothing is null", () => {
    expect(yearScale({ values: [0.7, null, 1.9], priorYears: [{ values: [0.55, 2.21, null] }] })).toEqual({ lo: 0.55, hi: 2.21 });
    // (review of #77) one storm day does not set the axis: 98 days near 0.6 m, one at 2.96, one at 0.52
    const days = [...Array.from({ length: 98 }, (_, i) => 0.58 + (i % 5) / 100), 2.96, 0.52];
    expect(yearScale({ values: days })).toEqual({ lo: 0.58, hi: 0.62 });
    expect(yearScale({ values: [null, Number.NaN], priorYears: [] })).toBeNull();
    expect(yearScale(null)).toBeNull();
  });
});

describe("the y range always includes the normal band", () => {
  it("a reading far below the 10th percentile is still shown as far below", () => {
    // 15.7 against a normal range of 23-39. Scaled to the observations alone the drought
    // fills the frame and looks ordinary; the band has to be in view or the chart lies.
    const h = buildHydrograph({
      values: flat(72, 15.7), bands: flat(72, BAND), xLabels: ["3d", "2d", "1d", "now"],
    });
    // The p90 of the normal band must land inside the plot, not off the top of it.
    const plotTop = h.box.padTop;
    const plotBottom = h.box.height - h.box.padBottom;
    const p90 = h.envelopes[0]!.d;
    const ys = p90.replace(/^M/, "").split(/[LZ]/).filter(Boolean)
      .map((pt) => Number(pt.split(",")[1]));
    expect(Math.min(...ys)).toBeGreaterThanOrEqual(plotTop - 0.5);
    expect(Math.max(...ys)).toBeLessThanOrEqual(plotBottom + 0.5);
    // and today sits below the middle of the frame, because it is lower than normal
    expect(h.now!.y).toBeGreaterThan((plotTop + plotBottom) / 2);
  });
});

describe("paths", () => {
  it("draws the observed line and both envelopes", () => {
    const h = buildHydrograph({
      values: flat(24, 20), bands: flat(24, BAND), xLabels: ["a", "b"],
    });
    expect(h.line.startsWith("M")).toBe(true);
    expect(h.envelopes).toHaveLength(2);
    for (const e of h.envelopes) expect(e.d.endsWith("Z")).toBe(true);
    expect(h.median.startsWith("M")).toBe(true);
  });

  it("a gap in the record breaks the line rather than inventing one", () => {
    // A dashed guess across missing hours reads exactly like measured water.
    const h = buildHydrograph({
      values: [10, 11, null, null, 13, 14], bands: flat(6, BAND), xLabels: ["a", "b"],
    });
    expect(h.line.split("M").length - 1).toBe(2);
  });

  it("survives a series with no band at all", () => {
    const h = buildHydrograph({ values: flat(10, 5), bands: flat(10, null), xLabels: ["a"] });
    expect(h.envelopes).toHaveLength(0);
    expect(h.line).not.toBe("");
  });

  it("survives a single reading", () => {
    const h = buildHydrograph({ values: [7], bands: [BAND], xLabels: ["now"] });
    expect(h.now).not.toBeNull();
    expect(Number.isFinite(h.now!.x)).toBe(true);
  });
});

describe("log scale, for a whole year", () => {
  it("spreads two orders of magnitude instead of flattening the summer", () => {
    const rising = [20, 50, 200, 800, 300, 60, 25];
    const lin = buildHydrograph({ values: rising, bands: flat(7, null), xLabels: ["Jan"], log: false });
    const lg = buildHydrograph({ values: rising, bands: flat(7, null), xLabels: ["Jan"], log: true });
    const spread = (h: typeof lin) => {
      const ys = h.line.replace(/^M/, "").split("L").map((p) => Number(p.split(",")[1]));
      return Math.max(...ys) - Math.min(...ys);
    };
    // On a linear axis the low-flow months are squashed into the bottom sliver.
    const linLow = lin.line.replace(/^M/, "").split("L").map((p) => Number(p.split(",")[1]));
    const lgLow = lg.line.replace(/^M/, "").split("L").map((p) => Number(p.split(",")[1]));
    expect(Math.abs(linLow[0]! - linLow[6]!)).toBeLessThan(Math.abs(lgLow[0]! - lgLow[6]!));
    expect(spread(lg)).toBeGreaterThan(0);
  });
});

describe("everything the component needs is a number or a string", () => {
  it("returns no elements, so both renderers draw the same chart", () => {
    const h = buildHydrograph({ values: flat(5, 1), bands: flat(5, BAND), xLabels: ["a"] });
    for (const v of Object.values(h)) {
      expect(["string", "number", "object", "boolean"]).toContain(typeof v);
    }
    expect(h.box).toEqual(DEFAULT_BOX);
  });
});

/**
 * The forecast is the one thing on this chart that has not happened yet.
 *
 * Everything else here is a measurement or a percentile of measurements. A model run drawn
 * with the same geometry as an observation is the failure mode that matters — so the
 * arithmetic keeps them structurally apart, and these hold it there.
 */
describe("the forecast", () => {
  // Three hourly observations, then a run continuing past them on the same clock.
  const at = ["2026-09-01T00:00:00Z", "2026-09-01T01:00:00Z", "2026-09-01T02:00:00Z"];
  const base = {
    values: [10, 11, 12] as (number | null)[],
    bands: [null, null, null],
    xLabels: ["a", "b", "c"],
    at,
  };
  const run = (n: number, mid: (i: number) => number,
               bounds = true) => ({
    at: Array.from({ length: n }, (_, i) =>
      new Date(Date.UTC(2026, 8, 1, 3 + i)).toISOString()),
    mid: Array.from({ length: n }, (_, i) => mid(i)),
    lo: bounds ? Array.from({ length: n }, (_, i) => mid(i) - 2) : [],
    hi: bounds ? Array.from({ length: n }, (_, i) => mid(i) + 2) : [],
  });

  it("is absent when no model is running", () => {
    expect(buildHydrograph(base).forecast).toBeNull();
  });

  it("draws every step the model published, not two points", () => {
    // THE BUG THIS EXISTS FOR. The summary layer carries one number per station, so the
    // ribbon was a straight line to the frame edge and the band a triangle. A 24-step run
    // has to produce a path with 24 vertices in it.
    const h = buildHydrograph({ ...base, forecast: run(24, (i) => 12 + i) });
    const vertices = (h.forecast!.line.match(/L/g) ?? []).length;
    expect(vertices).toBeGreaterThanOrEqual(24);
  });

  it("starts where the observations stop, never before", () => {
    const h = buildHydrograph({ ...base, forecast: run(10, () => 12) });
    // The boundary is the last OBSERVATION, so there is no gap and no overlap.
    expect(h.forecast!.at).toBeGreaterThan(h.box.padLeft);
    expect(h.forecast!.at).toBeLessThan(h.box.width - h.box.padRight);
  });

  it("drops the model's own hindcast", () => {
    // Both models publish steps for days already measured. Drawn, that is two lines
    // claiming yesterday — and only one of them is a measurement.
    const withPast = {
      at: ["2026-08-31T00:00:00Z", "2026-09-01T01:00:00Z", "2026-09-01T06:00:00Z"],
      mid: [99, 98, 20], lo: [90, 90, 18], hi: [110, 110, 22],
    };
    const h = buildHydrograph({ ...base, forecast: withPast });
    // Only the one step past the last observation survives, so the 99 never scales the axis.
    expect(Math.max(...h.yTicks.map((t) => t.value))).toBeLessThan(90);
  });

  it("keeps its own extremes inside the frame", () => {
    // A flood outlook that runs off the top is the one case where the chart most needs to
    // be readable. The y scale has to include the forecast, not just the record.
    const h = buildHydrograph({ ...base, forecast: run(6, () => 900) });
    expect(Math.max(...h.yTicks.map((t) => t.value))).toBeGreaterThan(100);
  });

  it("gives a longer outlook more of the width", () => {
    const short = buildHydrograph({ ...base, forecast: run(4, () => 10) });
    const long = buildHydrograph({ ...base, forecast: run(60, () => 10) });
    expect(long.forecast!.at).toBeLessThan(short.forecast!.at);
  });

  it("draws a line but no band when the model published no bounds", () => {
    const h = buildHydrograph({ ...base, forecast: run(8, () => 10, false) });
    expect(h.forecast!.line).not.toBe("");
    expect(h.forecast!.band).toBe("");
  });
});

describe("marking today", () => {
  it("uses the reading it was given, not the last cell of the array", () => {
    // The seasonal chart is a year of envelope with almost every observation cell null.
    // Reading the marker out of the series there would put today on New Year's Eve.
    const h = buildHydrograph({
      values: Array.from({ length: 365 }, () => null),
      bands: Array.from({ length: 365 }, () => [1, 2, 3, 4, 5] as const),
      xLabels: ["Jan", "Apr", "Jul", "Oct"],
      nowIndex: 100,
      nowValue: 2.5,
    });
    expect(h.now).not.toBeNull();
    // Just over a quarter of the way along the year, not at the right-hand edge.
    expect(h.now!.x).toBeLessThan(h.box.width / 2);
  });
});

describe("(T377) other models drawn at once", () => {
  const at = ["2026-09-01T00:00:00Z", "2026-09-01T01:00:00Z", "2026-09-01T02:00:00Z"];
  const base = { values: [10, 11, 12] as (number | null)[], bands: [null, null, null], xLabels: ["a", "b", "c"], at };
  const steps = (n: number, mid: (i: number) => number, from = 3) => ({
    at: Array.from({ length: n }, (_, i) => new Date(Date.UTC(2026, 8, 1, from + i)).toISOString()),
    mid: Array.from({ length: n }, (_, i) => mid(i)),
  });
  const run = (n: number, mid: (i: number) => number) => ({ ...steps(n, mid), lo: [], hi: [] });

  it("each is a trace of its own from the last observation, every step, in the order given", () => {
    const h = buildHydrograph({ ...base, forecast: run(10, () => 12),
                                alsoForecasts: [{ key: "GEM", ...steps(10, () => 14) }, { key: "ELF", ...steps(5, () => 9) }] });
    expect(h.alsoForecasts.map((a) => a.key)).toEqual(["GEM", "ELF"]);
    expect((h.alsoForecasts[0]!.line.match(/L/g) ?? []).length).toBe(10);
    // from the same point the chosen model's ribbon starts at
    expect(h.alsoForecasts[0]!.line.split("L")[0]).toBe(h.forecast!.line.split("L")[0]);
  });

  it("their extremes are inside the frame, and the longest run sets the frame's end", () => {
    const one = buildHydrograph({ ...base, forecast: run(4, () => 12) });
    const two = buildHydrograph({ ...base, forecast: run(4, () => 12), alsoForecasts: [{ key: "ELF", ...steps(40, () => 900) }] });
    expect(Math.max(...two.yTicks.map((t) => t.value))).toBeGreaterThan(100);
    expect(two.forecast!.at).toBeLessThan(one.forecast!.at);
    expect(two.full!.t1).toBe(Date.parse(steps(40, () => 0).at[39]!));
  });

  it("a hindcast-only or empty run is no trace; none given is none drawn", () => {
    const past = { key: "OLD", at: ["2026-08-31T00:00:00Z"], mid: [5] };
    expect(buildHydrograph({ ...base, forecast: run(4, () => 12), alsoForecasts: [past, { key: "E", at: [], mid: [] }] }).alsoForecasts)
      .toEqual([]);
    expect(buildHydrograph(base).alsoForecasts).toEqual([]);
  });
});
