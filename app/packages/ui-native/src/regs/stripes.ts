/**
 * (T935) THE YEAR BARS' STRIPES AS SHAPES, not as an SVG `<Pattern>`. The closed hatch was a `<Pattern>` with
 * `patternTransform="rotate(135)"`; react-native-svg 15 on iOS hands that transform to the pattern's own content as well
 * (the JS `Pattern` passes the matrix twice: `matrix` and `patternTransform`) and draws the tile's content rotated out of
 * its cell, so every closed day, and the "✕ Closed" key, was a blank track on an iPhone (owner 2026-11-20, Fraser at
 * Mission). Android and the web drew it. Here each run's stripes are polygons cut to the run's box, computed once, so the
 * three platforms draw the same thing with no pattern at all.
 *
 * A stripe set is the points where `(a·x + b·y) mod period` lies in `[0, band)`, in the bar's own coordinates (so a run's
 * stripes line up with its neighbours', as the user-space pattern did):
 *   closed   a −1, b 1, period 12, band 6       — the old 6√2 tile turned 135°: "\" stripes 6 px wide every 12 px of x
 *   flies    a 1,  b 1, period 4√2, band 2√2    — the old 4 px tile turned 45°: "/" stripes
 *   release, fly-only  a 1, b 0, period 5, band 3  — upright stripes 3 px wide every 5 px (never transformed)
 */
export interface StripeSet { readonly a: number; readonly b: number; readonly period: number; readonly band: number }

export const CLOSED_STRIPES: StripeSet = { a: -1, b: 1, period: 12, band: 6 };
export const FLIES_STRIPES: StripeSet = { a: 1, b: 1, period: 4 * Math.SQRT2, band: 2 * Math.SQRT2 };
export const UPRIGHT_STRIPES: StripeSet = { a: 1, b: 0, period: 5, band: 3 };

type Pt = readonly [number, number];

/** The polygon `poly` cut to the side of the line `a·x + b·y = c` where the value is ≥ c (`ge`) or ≤ c. */
function cut(poly: readonly Pt[], a: number, b: number, c: number, ge: boolean): Pt[] {
  const v = (p: Pt) => a * p[0] + b * p[1];
  const inside = (p: Pt) => (ge ? v(p) >= c : v(p) <= c);
  const out: Pt[] = [];
  poly.forEach((p, i) => {
    const q = poly[(i + 1) % poly.length]!;
    const pin = inside(p), qin = inside(q);
    if (pin) out.push(p);
    if (pin !== qin) {
      const t = (c - v(p)) / (v(q) - v(p));
      out.push([p[0] + t * (q[0] - p[0]), p[1] + t * (q[1] - p[1])]);
    }
  });
  return out;
}

const area = (poly: readonly Pt[]) =>
  Math.abs(poly.reduce((s, p, i) => { const q = poly[(i + 1) % poly.length]!; return s + p[0] * q[1] - q[0] * p[1]; }, 0)) / 2;

const r2 = (n: number) => Math.round(n * 100) / 100;

/** The stripes of `set` inside the box `x0 … x0 + w` × `0 … h`, as SVG `points` strings (none for an empty box). */
export function stripes(x0: number, w: number, h: number, set: StripeSet): string[] {
  if (!(w > 0) || !(h > 0)) return [];
  const box: Pt[] = [[x0, 0], [x0 + w, 0], [x0 + w, h], [x0, h]];
  const vs = box.map((p) => set.a * p[0] + set.b * p[1]);
  const lo = Math.min(...vs), hi = Math.max(...vs);
  const out: string[] = [];
  for (let n = Math.floor(lo / set.period); n * set.period < hi; n++) {
    const from = n * set.period, to = from + set.band;
    const piece = cut(cut(box, set.a, set.b, from, true), set.a, set.b, to, false);
    if (piece.length >= 3 && area(piece) > 1e-6) out.push(piece.map((p) => `${r2(p[0])},${r2(p[1])}`).join(" "));
  }
  return out;
}
