/** (T935) the year bars' stripes as shapes: the old patterns' geometry exactly, cut to the run, nothing outside it. */
import { describe, expect, it } from "vitest";
import { CLOSED_STRIPES, FLIES_STRIPES, UPRIGHT_STRIPES, stripes, type StripeSet } from "./stripes";

const polys = (x0: number, w: number, h: number, set: StripeSet) =>
  stripes(x0, w, h, set).map((s) => s.split(" ").map((p) => p.split(",").map(Number) as [number, number]));

/** even-odd point-in-polygon */
const inside = (poly: [number, number][], x: number, y: number) => {
  let c = false;
  for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
    const [xi, yi] = poly[i]!, [xj, yj] = poly[j]!;
    if ((yi > y) !== (yj > y) && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) c = !c;
  }
  return c;
};
const mod = (n: number, m: number) => ((n % m) + m) % m;

describe("(T935) stripes", () => {
  for (const [name, set] of [["closed", CLOSED_STRIPES], ["flies", FLIES_STRIPES], ["upright", UPRIGHT_STRIPES]] as const) {
    it(`${name}: a point is striped exactly where the old pattern's band was`, () => {
      const ps = polys(37.3, 120, 10, set);
      for (let x = 37.5; x < 157.3; x += 0.71) {
        for (let y = 0.25; y < 10; y += 0.83) {
          const v = mod(set.a * x + set.b * y, set.period);
          if (Math.min(v, set.band - v, set.period - v) < 0.05) continue;   // on an edge: either is right
          expect(ps.some((p) => inside(p, x, y)), `${name} ${x},${y}`).toBe(v < set.band);
        }
      }
    });
  }

  it("every point lies in the run's box; an empty box draws nothing", () => {
    for (const p of polys(10, 33, 10, CLOSED_STRIPES).flat()) expect(p[0] >= 10 && p[0] <= 43 && p[1] >= 0 && p[1] <= 10).toBe(true);
    expect(stripes(5, 0, 10, CLOSED_STRIPES)).toEqual([]);
  });

  it("the closed hatch covers half the run (6 px of every 12)", () => {
    const area = (poly: [number, number][]) =>
      Math.abs(poly.reduce((s, p, i) => { const q = poly[(i + 1) % poly.length]!; return s + p[0] * q[1] - q[0] * p[1]; }, 0)) / 2;
    const total = polys(0, 240, 10, CLOSED_STRIPES).reduce((s, p) => s + area(p), 0);
    expect(total).toBeCloseTo(1200, 0);
  });
});
