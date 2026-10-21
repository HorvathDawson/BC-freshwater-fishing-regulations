import { describe, expect, it } from "vitest";
import { CHART_ZOOM, clampDomain, fitDomain, isZoomed, minSpan, nearestIndex, panDomain, timeAt,
         wheelFactor, zoomChanges, zoomDomain } from "./chartZoom";

const H = 3_600_000;
const FULL = { t0: 0, t1: 96 * H };     // a 72 h record plus a day of forecast

describe("chart zoom: the window of time on screen", () => {
  it("pins its limits (a change here is a decision, not a refactor)", () => {
    expect(CHART_ZOOM).toEqual({ minFraction: 1 / 24, minSpanMs: 3 * H, step: 2, wheelPer100: 1.25,
                                 fitSlack: 0.001 });
    expect(minSpan(FULL)).toBe(4 * H);                       // 96 h / 24
    expect(minSpan({ t0: 0, t1: 24 * H })).toBe(3 * H);      // never under three hours …
    expect(minSpan({ t0: 0, t1: 2 * H })).toBe(2 * H);       // … unless the record is shorter
  });

  it("zooms about the focus: the instant under the finger stays under it", () => {
    const d = zoomDomain(fitDomain(FULL), FULL, 0.25, 2);
    expect(d).toEqual({ t0: 12 * H, t1: 60 * H });
    // the time at 25% across is 24 h before and after
    expect(timeAt(fitDomain(FULL), 0.25)).toBe(24 * H);
    expect(timeAt(d, 0.25)).toBe(24 * H);
  });

  it("zooming out past the whole record is the whole record; in, stops at the narrowest window", () => {
    expect(zoomDomain({ t0: 10 * H, t1: 20 * H }, FULL, 0.5, 0.01)).toEqual(FULL);
    const tight = zoomDomain(fitDomain(FULL), FULL, 0.5, 1000);
    expect(tight.t1 - tight.t0).toBe(4 * H);
    expect(tight).toEqual({ t0: 46 * H, t1: 50 * H });
  });

  it("a zoom at the edge stays inside the record", () => {
    expect(zoomDomain(fitDomain(FULL), FULL, 0, 4)).toEqual({ t0: 0, t1: 24 * H });
    expect(zoomDomain(fitDomain(FULL), FULL, 1, 4)).toEqual({ t0: 72 * H, t1: 96 * H });
  });

  it("pans by a fraction of the window and stops at either end, never shrinking", () => {
    const d = { t0: 24 * H, t1: 48 * H };
    expect(panDomain(d, FULL, 0.5)).toEqual({ t0: 36 * H, t1: 60 * H });
    expect(panDomain(d, FULL, -5)).toEqual({ t0: 0, t1: 24 * H });
    expect(panDomain(d, FULL, 50)).toEqual({ t0: 72 * H, t1: 96 * H });
    expect(panDomain(fitDomain(FULL), FULL, 0.3)).toEqual(FULL);
  });

  it("says zoomed only when it is", () => {
    expect(isZoomed(fitDomain(FULL), FULL)).toBe(false);
    expect(isZoomed({ t0: 0, t1: 96 * H - 1000 }, FULL)).toBe(false);   // a rounding error is not a zoom
    expect(isZoomed({ t0: 0, t1: 90 * H }, FULL)).toBe(true);
  });

  it("clamps a window that is wider than the record to the record", () => {
    expect(clampDomain({ t0: -10 * H, t1: 200 * H }, FULL)).toEqual(FULL);
  });

  it("turns a wheel notch into a factor: away zooms in, toward zooms out, nothing is nothing", () => {
    expect(wheelFactor(-100)).toBe(1.25);
    expect(wheelFactor(100)).toBe(0.8);
    expect(wheelFactor(0)).toBe(1);
    expect(wheelFactor(Number.NaN)).toBe(1);
  });

  it("ignores a nonsense factor rather than collapsing the window", () => {
    expect(zoomDomain(fitDomain(FULL), FULL, 0.5, 0)).toEqual(FULL);
    expect(zoomDomain(fitDomain(FULL), FULL, 0.5, Number.NaN)).toEqual(FULL);
  });
});

describe("nearestIndex: the sample a read-out names", () => {
  const times = [0, 10, 20, 30, 40];
  it("is the closest sample", () => {
    expect(nearestIndex(times, 14)).toBe(1);
    expect(nearestIndex(times, 16)).toBe(2);
    expect(nearestIndex(times, -100)).toBe(0);
    expect(nearestIndex(times, 1000)).toBe(4);
  });
  it("skips a gap in the record rather than reading it out", () => {
    expect(nearestIndex(times, 21, (i) => i !== 2)).toBe(3);
  });
  it("names nothing when the closest real sample is too far away, or there is none", () => {
    expect(nearestIndex(times, 21, (i) => i !== 2, 5)).toBe(-1);
    expect(nearestIndex([], 5)).toBe(-1);
  });

  it("says whether a zoom would change the window, so a wheel that does nothing leaves the page to scroll", () => {
    const whole = fitDomain(FULL);
    expect(zoomChanges(whole, FULL, wheelFactor(400))).toBe(false);   // out, at the whole chart
    expect(zoomChanges(whole, FULL, wheelFactor(-100))).toBe(true);   // in, from the whole chart
    const tight = zoomDomain(whole, FULL, 0.5, 1000);
    expect(zoomChanges(tight, FULL, wheelFactor(-100))).toBe(false);  // in, at the narrowest
    expect(zoomChanges(tight, FULL, wheelFactor(100))).toBe(true);    // out, from the narrowest
    expect(zoomChanges(whole, FULL, 1)).toBe(false);
    expect(zoomChanges(whole, FULL, Number.NaN)).toBe(false);
    expect(zoomChanges(whole, FULL, 0)).toBe(false);
  });
});
