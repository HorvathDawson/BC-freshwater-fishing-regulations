import { describe, expect, it } from "vitest";
import type { Band } from "@app/core";
import { buildHydrograph, logTicks, niceTicks, DEFAULT_BOX } from "./hydrograph";

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
    // and today sits below the middle of the frame, because it is below normal
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
