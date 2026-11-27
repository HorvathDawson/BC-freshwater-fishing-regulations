/**
 * The flow chart as a view (owner 2026-10-21): built at the measured width, zoomed and panned by measurements only,
 * read out in core's words. The gestures that feed it are drawn in @app/ui-native; this pins what they cause.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, renderHook, waitFor } from "@testing-library/react";
import { chartReadoutLine, monthAbbr } from "@app/core";
import type { WaterSource, Series, StationId } from "@app/data";
import { chartBox, chartHeight, chartTicks, tickCount, useChartView } from "./chart";
import { peekAnswer, seriesKey } from "./answers";

const H = 3_600_000;
const T0 = Date.parse("2026-10-18T00:00:00Z");
const N = 73;
const SERIES: Series = {
  step: "1h", from: "feed", parameter: "discharge",
  at: Array.from({ length: N }, (_, i) => new Date(T0 + i * H).toISOString()),
  values: Array.from({ length: N }, (_, i) => 4 + i / 100),
  band: [[2, 4, 6, 8, 10]],
  now: { index: N - 1, value: 4.72 },
  forecast: null, forecasts: [], priorYears: [],
};
const source = { gaugeSeries: async () => ({ fetchedAt: 0, value: SERIES }) } as unknown as WaterSource;
const ST = "08MH001" as StationId;

describe("the chart's size", () => {
  it("is built for the width it is drawn at, between 260 and 420 high (T367, T920)", () => {
    expect(chartBox(1000)).toEqual({ width: 1000, height: 420, padLeft: 4, padRight: 4, padTop: 34, padBottom: 24 });
    expect(chartBox(358)).toEqual({ width: 358, height: 420, padLeft: 4, padRight: 4, padTop: 34, padBottom: 24 });
    // (T920 review) a 360 dp phone's card inside (302) is drawn 1:1, never forced to 320 and scaled down
    expect(chartBox(302).width).toBe(302);
    expect(chartBox(100).width).toBe(200);
    expect(chartBox(0).width).toBe(320);   // not measured yet
    // (T920) about square on a phone, capped on a wide column
    expect([chartHeight(200), chartHeight(332), chartHeight(390), chartHeight(900)]).toEqual([260, 398, 420, 420]);
  });
});

describe("the axis names real times", () => {
  it("puts a year chart's months on their own first days, at most six", () => {
    const view = { t0: Date.UTC(2026, 0, 1), t1: Date.UTC(2026, 10, 20) };
    expect(chartTicks(view, "year").map((k) => [new Date(k.t).toISOString().slice(0, 10), k.label])).toEqual([
      ["2026-01-01", "Jan"], ["2026-03-01", "Mar"], ["2026-05-01", "May"], ["2026-07-01", "Jul"],
      ["2026-09-01", "Sep"], ["2026-11-01", "Nov"],
    ]);
  });
  it("names the days of a zoomed year window, as calendar days", () => {
    const view = { t0: Date.UTC(2026, 6, 1), t1: Date.UTC(2026, 6, 21) };
    expect(chartTicks(view, "year").map((k) => k.label)).toEqual(["1 Jul", "6 Jul", "11 Jul", "16 Jul", "21 Jul"]);
  });
  it("names hours on a 72-hour window, in the reader's clock", () => {
    const ticks = chartTicks({ t0: T0, t1: T0 + 72 * H }, "72h");
    const d = new Date(T0);
    expect(ticks[0]!.label).toBe(`${d.getDate()} ${monthAbbr(d.getMonth())} ${String(d.getHours()).padStart(2, "0")}h`);
    expect(ticks).toHaveLength(5);
  });
  it("a phone-width chart names three times, not five (\"20 Oct 06h 20 Oct 21h\" ran together at 390 px)", () => {
    expect(tickCount(358)).toBe(3);
    expect(tickCount(600)).toBe(5);
    const view = { t0: Date.UTC(2026, 6, 1), t1: Date.UTC(2026, 6, 21) };
    expect(chartTicks(view, "year", 3).map((k) => k.label)).toEqual(["1 Jul", "11 Jul", "21 Jul"]);
    // A year of months thins to a quarter apart on a phone (month names are short: one more fits).
    const year = { t0: Date.UTC(2026, 0, 1), t1: Date.UTC(2026, 11, 31) };
    expect(chartTicks(year, "year", 3).map((k) => k.label)).toEqual(["Jan", "Apr", "Jul", "Oct"]);
  });
});

describe("(T1205) the chart's Today mark is the last point only while it is current by the clock", () => {
  afterEach(() => { vi.useRealTimers(); });
  const markAt = async (nowMs: number) => {
    vi.useFakeTimers({ toFake: ["Date"], shouldAdvanceTime: true }); vi.setSystemTime(nowMs);
    const { result } = renderHook(() => useChartView(source, ST, "72h", undefined, undefined, 600));
    await waitFor(() => expect(result.current.state).toBe("ready"));
    const v = result.current.state === "ready" ? result.current.value! : null!;
    return [v.shape.todayX !== null, v.shape.now !== null];
  };
  const LAST = T0 + (N - 1) * H;
  it("present for a last point an hour old", async () => { expect(await markAt(LAST + H)).toEqual([true, true]); });
  it("absent for a last point nine hours old (the line and its dates stay)", async () => { expect(await markAt(LAST + 9 * H)).toEqual([false, false]); });
});

describe("useChartView", () => {
  const draw = (width = 600) => renderHook(({ w }) => useChartView(source, ST, "72h", undefined, undefined, w),
                                           { initialProps: { w: width } });
  const ready = async (r: ReturnType<typeof draw>["result"]) => {
    await waitFor(() => expect(r.current.state).toBe("ready"));
    return () => (r.current.state === "ready" ? r.current.value! : null)!;
  };

  it("opens on the whole record, at the measured width", async () => {
    const { result, rerender } = draw(600);
    const v = await ready(result);
    expect([v().zoomed, v().shape.box.width, v().readout]).toEqual([false, 600, null]);
    rerender({ w: 900 });
    expect(v().shape.box.width).toBe(900);
  });

  it("zooms in and out about the middle, and the whole chart clears it", async () => {
    const { result } = draw();
    const v = await ready(result);
    act(() => v().zoomIn());
    expect(v().shape.view).toEqual({ t0: T0 + 18 * H, t1: T0 + 54 * H });
    expect(v().zoomed).toBe(true);
    act(() => v().zoomOut());
    expect([v().shape.view, v().zoomed]).toEqual([{ t0: T0, t1: T0 + 72 * H }, false]);
    act(() => { v().zoomIn(); v().zoomIn(); });          // two presses in one frame COMPOUND
    expect(v().shape.view).toEqual({ t0: T0 + 27 * H, t1: T0 + 45 * H });
    act(() => v().fit());
    expect(v().zoomed).toBe(false);
  });

  it("pans by the finger's pixels: dragging right brings earlier hours in", async () => {
    const { result } = draw(584);                          // plot = 576 px wide (T920: 4 px either side)
    const v = await ready(result);
    act(() => v().zoomIn());                               // 18 h → 54 h: 36 h over 576 px, 16 px an hour
    act(() => v().panBy(80));                              // five hours earlier
    expect(v().shape.view).toEqual({ t0: T0 + 13 * H, t1: T0 + 49 * H });
    act(() => v().panBy(-10_000));                         // stops at the end of the record
    expect(v().shape.view).toEqual({ t0: T0 + 36 * H, t1: T0 + 72 * H });
  });

  it("zooms where the pinch or wheel is, not in the middle", async () => {
    const { result } = draw(590);
    const v = await ready(result);
    act(() => v().zoomAt(4, 4));                           // the plot's left edge
    expect(v().shape.view).toEqual({ t0: T0, t1: T0 + 18 * H });
  });

  it("says whether a zoom would do anything, so the page keeps a wheel the chart cannot use", async () => {
    const { result } = draw(590);
    const v = await ready(result);
    expect(v().zooms(0.5)).toBe(false);                   // out, at the whole chart
    expect(v().zooms(2)).toBe(true);
    act(() => v().zoomAt(300, 1000));                     // to the narrowest window
    expect(v().zooms(2)).toBe(false);
    expect(v().zooms(0.5)).toBe(true);
  });

  it("reads the value under the finger in core's words, and stops reading", async () => {
    const { result } = draw(584);                          // 72 h over 576 px: 8 px an hour
    const v = await ready(result);
    act(() => v().probeAt(4 + 8 * 10));                    // ten hours in
    expect(v().readout?.line).toBe(chartReadoutLine({ t: T0 + 10 * H, value: 4.1, unit: "m³/s", quantity: "discharge", kind: "reading",
                                                      daily: false, median: 6 }));
    expect(v().readout?.x).toBeCloseTo(84, 6);
    act(() => v().probeAt(null));
    expect(v().readout).toBeNull();
  });

  it("(T830) reads a level series to two decimals", async () => {
    const lv = { gaugeSeries: async () => ({ fetchedAt: 0, value: { ...SERIES, parameter: "level" as const,
                 values: Array.from({ length: N }, () => 1) } }) } as unknown as WaterSource;
    const { result } = renderHook(() => useChartView(lv, "08GA031" as StationId, "72h", "level", undefined, 590));
    const v = await ready(result);
    act(() => v().probeAt(40 + 7.5 * 10));
    expect(v().readout?.line).toMatch(/ · 1\.00 m · median 6\.00$/);
  });
});

describe("(T256) the chart before its envelope", () => {
  it("draws the feed's observations while the series (climate) is on its way, and never keeps them as the series", async () => {
    const { clearAnswers, peekAnswer, seriesKey } = await import("./answers");
    clearAnswers();
    let land!: (v: unknown) => void;
    const slow = { gaugeSeries: () => new Promise((r) => { land = r; }) } as unknown as WaterSource;
    const obs = { fetchedAt: 0, from: "feed", parameter: "discharge" as const, at: SERIES.at, discharge: SERIES.values,
                  level: SERIES.values.map(() => null), daily: [], forecasts: {}, priorYears: {} };
    const draft = { feed: { observations: async () => obs } };
    const { result } = renderHook(() => useChartView(slow, ST, "72h", undefined, undefined, 600, true, draft));
    await waitFor(() => expect(result.current.state === "ready" && result.current.value?.draft).toBe(true));
    const v = result.current.state === "ready" ? result.current.value! : null;
    expect([v!.pending, v!.shape.box.width]).toEqual([false, 600]);
    // the answer store holds the draft under its own key: the series is still unanswered
    expect(peekAnswer(slow, seriesKey(ST, "72h", undefined, undefined))).toBeNull();
    await act(async () => { land({ fetchedAt: 0, value: SERIES }); });
    await waitFor(() => expect(result.current.state === "ready" && result.current.value?.draft).toBe(false));
    expect(peekAnswer<{ value: Series }>(slow, seriesKey(ST, "72h", undefined, undefined))?.value.value.band)
      .toEqual(SERIES.band);
  });

  it("without a feed it waits for the series, as before", async () => {
    const { clearAnswers } = await import("./answers");
    clearAnswers();
    const slow = { gaugeSeries: () => new Promise(() => {}) } as unknown as WaterSource;
    const { result } = renderHook(() => useChartView(slow, ST, "72h", undefined, undefined, 600));
    await new Promise((r) => setTimeout(r, 20));
    expect(result.current.state).toBe("loading");
  });
});

// (T1043) A BANDLESS 72 H CHART IS SCALED TO ITS YEAR: the release has no Year series for such a station (no envelope), so
// the year is read from the feed's own record (`draftSeriesOf` over its observations: this year's daily values and the
// earlier years). No feed, or a feed whose read fails (REPORTED once by `useAsync`, T368): the chart's own values, said as
// scale "reading". A banded chart never asks. MUTATIONS: pass no `scaleTo` to `buildHydrograph` -> the first fails;
// ask regardless of the band (`bandless` true) -> the banded case asks the feed and fails.
describe("(T1043) a bandless 72 h chart is scaled to its year", () => {
  const bare = { ...SERIES, parameter: "level" as const, band: [], values: Array.from({ length: N }, (_, i) => 0.59 + (i % 2) / 100) };
  const obs = { fetchedAt: 0, from: "feed", parameter: "discharge" as const, at: bare.at, discharge: bare.values.map(() => 1.8),
                level: bare.values, daily: [["2026-01-04", 1.9648, 105.4], ["2026-06-01", 0.71, 9.1], ["2026-10-17", 0.6, 2.6]],
                forecasts: {}, priorYears: {} };
  const src = { gaugeSeries: async () => ({ fetchedAt: 0, value: bare }) } as unknown as WaterSource;
  const YKEY = `yearscale:${seriesKey("08GA031" as StationId, "year", "level", undefined)}`;
  const feedOf = (fail = false) => ({ feed: { observations: async () => {
    if (fail) throw new Error("station file would not read");
    return obs;
  } } });
  const draw = (source: WaterSource, draft: ReturnType<typeof feedOf> | null) =>
    renderHook(() => useChartView(source, "08GA031" as StationId, "72h", "level", undefined, 590, true, draft as never));
  const scaleOf = (r: ReturnType<typeof draw>["result"]) => (r.current.state === "ready" ? r.current.value?.shape.scale : null);
  // (T1213) WAITS FOR A CONDITION, NEVER A DURATION. The hook's reads are promises (microtasks) with no timer behind them, so
  // each `act` round flushes one more stage (render -> effect -> ask -> answer). The bound counts rounds, not milliseconds:
  // a starved worker takes longer over the same rounds and still gets the same answer (`waitFor`'s 1 s clock did not).
  const until = async (what: string, cond: () => boolean) => {
    for (let i = 0; i < 200 && !cond(); i++) await act(async () => {});
    if (!cond()) throw new Error(`never reached: ${what}`);
  };
  const flush = async (rounds = 20) => { for (let i = 0; i < rounds; i++) await act(async () => {}); };

  it("reads the year's levels from the feed: the axis reaches this year's high", async () => {
    const { clearAnswers } = await import("./answers");
    clearAnswers();
    const { result } = draw(src, feedOf());
    await until("the year scale", () => scaleOf(result) === "year");
    expect(peekAnswer(src, YKEY)).not.toBeNull();
    const v = result.current.state === "ready" ? result.current.value! : null;
    expect(Math.max(...v!.shape.yTicks.map((t) => t.value))).toBeGreaterThanOrEqual(1.9);
  });
  it("no feed to read: its own values, said as `reading`", async () => {
    const { clearAnswers } = await import("./answers");
    clearAnswers();
    const { result } = draw(src, null);
    await until("the reading scale", () => scaleOf(result) === "reading");
  });
  it("a feed that fails is reported, and the chart scales to its reading", async () => {
    const { clearAnswers } = await import("./answers");
    const { setFailureSink } = await import("@app/core");
    clearAnswers();
    const seen: string[] = [];
    const undo = setFailureSink((e) => seen.push((e as Error).message));
    try {
      const { result } = draw(src, feedOf(true));
      // (the draft read while the series loads, T256, reports the same failure too)
      await until("the reading scale", () => scaleOf(result) === "reading");
      await flush();
      expect(peekAnswer(src, YKEY)).toBeNull();
      expect(seen.length).toBeGreaterThanOrEqual(1);
      expect(new Set(seen)).toEqual(new Set(["station file would not read"]));
    } finally { undo(); }
  });
  // (review of #77) A SWITCH BETWEEN TWO BANDLESS STATIONS never draws the new one on the old one's year, and a first view
  // is never drawn at the reading scale before its year. MUTATIONS: take the answer whatever its key (drop
  // `yearAsk.value?.key === yKey`) -> a B frame on A's year; drop the `yearPending` hold -> a frame at "reading".
  it("a switch draws each station only on its own year, and never at the reading scale first", async () => {
    const { clearAnswers } = await import("./answers");
    clearAnswers();
    const at = bare.at;
    const seriesFor = (st: string) => ({ ...bare, values: at.map(() => (st === "A" ? 0.59 : 5)),
                                         now: { index: N - 1, value: st === "A" ? 0.59 : 5 } });
    const two = { gaugeSeries: async (st: string) => ({ fetchedAt: 0, value: seriesFor(st) }) } as unknown as WaterSource;
    const yearOf = (st: string) => ({ ...obs, level: seriesFor(st).values,
      daily: st === "A" ? [["2026-01-04", 2.0, 1], ["2026-06-01", 0.5, 1]] : [["2026-01-04", 9.5, 1], ["2026-06-01", 4.0, 1]] });
    // (T1213) each station's year lands when the test lets it (held promises: the hook reads a station's feed more than
    // once, the draft and the year), not after a timer
    const held = new Map<string, (() => void)[]>();
    const release = (st: string) => { for (const go of held.get(st) ?? []) go(); };
    const draft = { feed: { observations: (st: string) => new Promise((done) => {
      held.set(st, [...(held.get(st) ?? []), () => done(yearOf(st))]);
    }) } };
    const frames: { st: string; scale: string; top: number }[] = [];
    const { rerender } = renderHook(({ st }) => {
      const r = useChartView(two, st as StationId, "72h", "level", undefined, 590, true, draft as never);
      if (r.state === "ready" && r.value && !r.value.pending)
        frames.push({ st, scale: r.value.shape.scale, top: Math.max(...r.value.shape.yTicks.map((t) => t.value)) });
      return r;
    }, { initialProps: { st: "A" } });
    await until("A asks its year", () => held.has("A"));
    await flush();
    expect(frames).toEqual([]);                            // nothing drawn before A's year is in
    await until("an A frame", () => { release("A"); return frames.some((f) => f.st === "A"); });   // (releases every read A has asked so far)
    rerender({ st: "B" });
    await until("B asks its year", () => held.has("B"));
    await flush();                                         // B's year still held: any B frame now is on A's year
    await until("a B frame", () => { release("B"); return frames.some((f) => f.st === "B"); });
    await flush();
    // A's year runs 0.5 to 2 m, B's 4 to 9.5 m: an A frame tops out under 2.5, a B frame at 8 or more
    expect(frames.filter((f) => f.scale !== "year" || (f.st === "A" ? f.top > 2.5 : f.top < 8))).toEqual([]);
  });
  // (T1047) A RECORD ON ANOTHER ZERO: `lineOnly` draws the line alone (no band, no earlier years, no year asked).
  // MUTATION: drop the band strip -> envelopes drawn, scale "band".
  it("lineOnly: a banded series drawn as its line alone, at its reading scale, never asking the year", async () => {
    const { clearAnswers } = await import("./answers");
    clearAnswers();
    const banded = { gaugeSeries: async () => ({ fetchedAt: 0, value: { ...SERIES, parameter: "level" as const } }) } as unknown as WaterSource;
    const { result } = renderHook(() => useChartView(banded, "08HD006" as StationId, "72h", "level", undefined, 590, true,
                                                    feedOf() as never, true, [], [], true));
    await until("the banded chart is ready", () => result.current.state === "ready");
    const v = result.current.state === "ready" ? result.current.value! : null;
    expect([v!.shape.envelopes.length, v!.shape.scale, v!.shape.priorYears.length]).toEqual([0, "reading", 0]);
    expect(peekAnswer(banded, `yearscale:${seriesKey("08HD006" as StationId, "year", "level", undefined)}`)).toBeNull();
  });
  it("a banded chart never asks for the year, and keeps its scale", async () => {
    const { clearAnswers } = await import("./answers");
    clearAnswers();
    const banded = { gaugeSeries: async () => ({ fetchedAt: 0, value: { ...SERIES, parameter: "level" as const } }) } as unknown as WaterSource;
    const { result } = draw(banded, feedOf());
    await until("the band scale", () => scaleOf(result) === "band");
    await flush();
    expect(peekAnswer(banded, YKEY)).toBeNull();           // the year's key was never asked
  });
});
