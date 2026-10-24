/**
 * The flow chart as a view (owner 2026-10-21): built at the measured width, zoomed and panned by measurements only,
 * read out in core's words. The gestures that feed it are drawn in @app/ui-native; this pins what they cause.
 */
import { describe, expect, it } from "vitest";
import { act, renderHook, waitFor } from "@testing-library/react";
import { chartReadoutLine, monthAbbr } from "@app/core";
import type { WaterSource, Series, StationId } from "@app/data";
import { chartBox, chartHeight, chartTicks, tickCount, useChartView } from "./chart";

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
  it("is built for the width it is drawn at, and never below the old box", () => {
    expect(chartBox(1000)).toEqual({ width: 1000, height: 340, padLeft: 40, padRight: 10, padTop: 12, padBottom: 24 });
    expect(chartBox(358)).toEqual({ width: 358, height: 190, padLeft: 40, padRight: 10, padTop: 12, padBottom: 24 });
    expect(chartBox(100).width).toBe(320);
    expect([chartHeight(390), chartHeight(500), chartHeight(900)]).toEqual([195, 250, 340]);
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
    const { result } = draw(590);                          // plot = 540 px wide
    const v = await ready(result);
    act(() => v().zoomIn());                               // 18 h → 54 h: 36 h over 540 px, 15 px an hour
    act(() => v().panBy(75));                              // five hours earlier
    expect(v().shape.view).toEqual({ t0: T0 + 13 * H, t1: T0 + 49 * H });
    act(() => v().panBy(-10_000));                         // stops at the end of the record
    expect(v().shape.view).toEqual({ t0: T0 + 36 * H, t1: T0 + 72 * H });
  });

  it("zooms where the pinch or wheel is, not in the middle", async () => {
    const { result } = draw(590);
    const v = await ready(result);
    act(() => v().zoomAt(40, 4));                          // the plot's left edge
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
    const { result } = draw(590);
    const v = await ready(result);
    act(() => v().probeAt(40 + 7.5 * 10));                 // ten hours in
    expect(v().readout?.line).toBe(chartReadoutLine({ t: T0 + 10 * H, value: 4.1, unit: "m³/s", kind: "reading",
                                                      daily: false, median: 6 }));
    expect(v().readout?.x).toBeCloseTo(115, 6);
    act(() => v().probeAt(null));
    expect(v().readout).toBeNull();
  });
});
