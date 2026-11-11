/**
 * (T564) ROUND-FIGURE TICKS, once: the app's hydrograph (`@app/ui` hydrograph.ts) and the public gauge page's chart
 * (tools/seo gaugeClient.ts, which writes this function into its script from this source) pick their gridlines here.
 */

/** Ticks a person reads: 1, 2, 5 x 10^n. Linear. */
export function niceTicks(lo: number, hi: number, count: number): number[] {
  const span = hi - lo;
  if (span <= 0 || !Number.isFinite(span)) return [lo];
  let step = 10 ** Math.floor(Math.log10(span / count));
  // A little overshoot beats a nearly empty axis: without it a span of 51 asking for 4
  // ticks jumps from 5 to 2, and two gridlines is not a scale a person can read against.
  for (const m of [1, 2, 2.5, 5, 10]) {
    if (span / (step * m) <= count * 1.3) { step *= m; break; }
  }
  const out: number[] = [];
  for (let v = Math.ceil(lo / step) * step; v <= hi + 1e-9; v += step) {
    out.push(Number(v.toFixed(6)));
  }
  return out;
}
