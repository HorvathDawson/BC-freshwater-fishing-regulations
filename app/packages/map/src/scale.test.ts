/**
 * N2 review: the native scale bar. The binding's own is top-left only on Android and in the device's units (imperial on
 * an en-US phone), drawn under the status bar — so the phone draws the web's: metric, bottom-right, MapLibre GL JS's
 * `ScaleControl` arithmetic (round to 1/2/3/5 × 10ⁿ, bar width in proportion), `maxWidth` 92 as `Map.web.tsx` asks.
 */
import { describe, expect, it } from "vitest";
import { metricScale } from "./scale";

describe("metricScale", () => {
  it("rounds down to 1, 2, 3 or 5 × 10ⁿ metres and sizes the bar in proportion", () => {
    // equator, z0: 40 075 016.686 m / 512 px ≈ 78 271.5 m/px; 92 px ≈ 7 201 km → 5 000 km over 63.9 px
    const z0 = metricScale(0, 0, 92);
    expect(z0.label).toBe("5000 km");
    expect(z0.width).toBeCloseTo(63.88, 1);
  });

  it("shrinks with latitude (cos φ) and halves per zoom", () => {
    // 49°N, z9: 78 271.5 × cos49° / 512 ≈ 100.3 m/px; 92 px ≈ 9.23 km → 5 km over 49.8 px
    const a = metricScale(49, 9, 92);
    expect(a.label).toBe("5 km");
    expect(a.width).toBeCloseTo(49.8, 0);
    // z14: ≈ 3.13 m/px; 92 px ≈ 288 m → 200 m
    expect(metricScale(49, 14, 92).label).toBe("200 m");
  });

  it("never exceeds the width it is given", () => {
    for (let z = 4; z <= 14; z += 0.25)
      for (const lat of [48.3, 54, 59.9]) {
        const s = metricScale(lat, z, 92);
        expect(s.width).toBeLessThanOrEqual(92);
        expect(s.width).toBeGreaterThan(92 / 5 - 1e-9);
      }
  });
});
