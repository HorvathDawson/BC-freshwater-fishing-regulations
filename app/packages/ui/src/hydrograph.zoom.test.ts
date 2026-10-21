/**
 * The chart's window (zoom) and read-out, as arithmetic (owner 2026-10-21: "zoom and look at data better").
 * The gestures only MEASURE; what a window draws and which sample a finger reads is decided here.
 */
import { describe, expect, it } from "vitest";
import { buildHydrograph } from "./hydrograph";

const H = 3_600_000;
const T0 = Date.parse("2026-10-18T00:00:00Z");
const N = 73;                                      // 72 hours, hourly
const at = Array.from({ length: N }, (_, i) => new Date(T0 + i * H).toISOString());
// a low first day, a spike on day two, low again
const values = Array.from({ length: N }, (_, i) => (i >= 30 && i <= 34 ? 100 : 5 + (i % 3)));
const bands = Array.from({ length: N }, () => [2, 4, 6, 8, 10] as const);
const BOX = { width: 600, height: 200, padLeft: 40, padRight: 10, padTop: 12, padBottom: 24 };
const base = { values, bands, at, xLabels: ["a", "b"], box: BOX, nowIndex: N - 1 };
const ticks = (v: { t0: number; t1: number }) =>
  [0, 0.5, 1].map((f) => ({ t: v.t0 + f * (v.t1 - v0(v)), label: `t${f}` }));
const v0 = (v: { t0: number }) => v.t0;

describe("the window", () => {
  it("is the whole record when none is asked for, and the plot rectangle is the box less its pads", () => {
    const h = buildHydrograph(base);
    expect(h.full).toEqual({ t0: T0, t1: T0 + 72 * H });
    expect(h.view).toEqual(h.full);
    expect(h.plot).toEqual({ x: 40, y: 12, width: 550, height: 164 });
  });

  it("lays the window across the full plot width", () => {
    const h = buildHydrograph({ ...base, domain: { t0: T0, t1: T0 + 24 * H } });
    // the first sample sits at the left edge, the 24th hour at the right edge
    expect(h.line.startsWith("M40.0,")).toBe(true);
    expect(h.line).toContain("590.0,");
  });

  it("scales the y axis to what is on screen: the spike off the window no longer sets it", () => {
    const whole = buildHydrograph(base);
    const early = buildHydrograph({ ...base, domain: { t0: T0, t1: T0 + 24 * H } });
    expect(Math.max(...whole.yTicks.map((t) => t.value))).toBeGreaterThanOrEqual(100);
    expect(Math.max(...early.yTicks.map((t) => t.value))).toBeLessThan(20);
    // MUTATION PIN: a window over the spike keeps it in scale.
    const spike = buildHydrograph({ ...base, domain: { t0: T0 + 24 * H, t1: T0 + 48 * H } });
    expect(Math.max(...spike.yTicks.map((t) => t.value))).toBeGreaterThanOrEqual(100);
  });

  it("does not draw today's rule or marker when today is off the window", () => {
    const early = buildHydrograph({ ...base, domain: { t0: T0, t1: T0 + 24 * H } });
    expect([early.todayX, early.now]).toEqual([null, null]);
    expect(buildHydrograph(base).todayX).toBeCloseTo(590, 6);
  });

  it("names the window's own times on the axis when given a tick rule", () => {
    const h = buildHydrograph({ ...base, domain: { t0: T0, t1: T0 + 24 * H }, ticks });
    expect(h.xTicks.map((t) => [t.label, t.at])).toEqual([["t0", 40], ["t0.5", 315], ["t1", 590]]);
    // The end labels hang INWARD: centred on the frame's edge, "24 Oct" was cut to "24 Oc" (owner screenshots).
    expect(h.xTicks.map((t) => t.anchor)).toEqual(["start", "middle", "end"]);
  });

  it("keeps a window inside the record", () => {
    const h = buildHydrograph({ ...base, domain: { t0: T0 - 100 * H, t1: T0 - 90 * H } });
    expect(h.view).toEqual({ t0: T0, t1: T0 + 10 * H });
  });
});

describe("the read-out", () => {
  it("reads the nearest measured sample, with its median, placed where it is drawn", () => {
    const h = buildHydrograph({ ...base, probe: T0 + 31 * H + 10 * 60_000 });
    expect(h.probe).toEqual({ x: expect.any(Number), y: expect.any(Number), t: T0 + 31 * H, value: 100,
                              kind: "reading", median: 6 });
    expect(h.probe!.x).toBeCloseTo(40 + (31 / 72) * 550, 6);
  });

  it("reads a forecast step past the record as a forecast", () => {
    const fc = { at: [new Date(T0 + 80 * H).toISOString()], mid: [12], lo: [10], hi: [14] };
    const h = buildHydrograph({ ...base, forecast: fc, probe: T0 + 79 * H });
    expect(h.probe && [h.probe.kind, h.probe.value, h.probe.t, h.probe.median])
      .toEqual(["forecast", 12, T0 + 80 * H, null]);
  });

  it("inside a forecast's span, reads its nearest published step however far apart the steps are", () => {
    // A 5-day run publishes a handful of steps: a pointer between two of them read NOTHING (the 1/20 gap rule), so
    // most of the forecast was unreadable. The line is drawn between the steps; the read-out snaps to one.
    const fc = { at: [T0 + 80 * H, T0 + 120 * H].map((t) => new Date(t).toISOString()),
                 mid: [12, 30], lo: [10, 25], hi: [14, 35] };
    const read = (t: number) => buildHydrograph({ ...base, forecast: fc, probe: t }).probe;
    expect([read(T0 + 95 * H)?.value, read(T0 + 105 * H)?.value]).toEqual([12, 30]);
    // MUTATION PIN: past the last step it is the gap rule again.
    expect(read(T0 + 200 * H)).toBeNull();
  });

  it("reads nothing over a gap wider than a twentieth of the window", () => {
    const gappy = values.map((v, i) => (i >= 20 && i <= 50 ? null : v));
    expect(buildHydrograph({ ...base, values: gappy, probe: T0 + 35 * H }).probe).toBeNull();
    // MUTATION PIN: near the gap's edge it reads the edge.
    expect(buildHydrograph({ ...base, values: gappy, probe: T0 + 21 * H }).probe?.t).toBe(T0 + 19 * H);
  });

  it("reads nothing without an instant, or without stamps", () => {
    expect(buildHydrograph(base).probe).toBeNull();
    expect(buildHydrograph({ values, bands, xLabels: ["a"], probe: T0 }).probe).toBeNull();
  });
});
