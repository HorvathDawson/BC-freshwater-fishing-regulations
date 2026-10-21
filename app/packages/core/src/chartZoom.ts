/**
 * THE FLOW CHART'S ZOOM, DECIDED (owner 2026-10-21: "the gauge map should have the ability to zoom and look at data
 * better too" — the chart as well as the map). Pure arithmetic on a TIME WINDOW, so the touch, mouse-wheel and button
 * bindings cannot disagree about what a zoom is, and every limit is pinned in a test rather than spelled in a handler.
 *
 *   domain     the window of time on screen, [t0, t1] in epoch ms, always inside the chart's whole extent (`full`)
 *   zoom       scale the window about a focus (a 0–1 fraction across the plot): the time under the finger stays put
 *   pan        slide the window by a fraction of its own width, stopping at either end of the record
 *   nearest    the sample closest to a time, for the read-out under a finger or a pointer
 *
 * Nothing here knows about pixels beyond a fraction, and nothing knows what the series is.
 */

export interface Domain { t0: number; t1: number }

export const CHART_ZOOM = {
  /** The narrowest window, as a fraction of the whole extent: closer than this a 72 h chart is a few samples. */
  minFraction: 1 / 24,
  /** And never narrower than this many ms (a year chart may zoom to two weeks; a 72 h one to three hours). */
  minSpanMs: 3 * 3_600_000,
  /** One press of + or −, one double-tap … */
  step: 2,
  /** … and one notch of a mouse wheel (deltaY of 100 px). */
  wheelPer100: 1.25,
  /** A window this close to the whole extent IS the whole extent: "zoomed" is not shown for a rounding error. */
  fitSlack: 0.001,
} as const;

const clamp = (v: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, v));

/** The whole extent, as a window. */
export function fitDomain(full: Domain): Domain {
  return { t0: full.t0, t1: full.t1 };
}

/** The narrowest window this extent allows. */
export function minSpan(full: Domain): number {
  const whole = Math.max(1, full.t1 - full.t0);
  return Math.min(whole, Math.max(whole * CHART_ZOOM.minFraction, CHART_ZOOM.minSpanMs));
}

/** Slide a window (keeping its width) until it sits inside the extent; a window wider than the extent IS the extent. */
export function clampDomain(d: Domain, full: Domain): Domain {
  const whole = full.t1 - full.t0;
  const span = Math.min(Math.max(d.t1 - d.t0, minSpan(full)), whole);
  if (span >= whole) return fitDomain(full);
  const t0 = clamp(d.t0, full.t0, full.t1 - span);
  return { t0, t1: t0 + span };
}

/** Is the window narrower than the whole extent (beyond rounding)? */
export function isZoomed(d: Domain, full: Domain): boolean {
  const whole = full.t1 - full.t0;
  return whole > 0 && (d.t1 - d.t0) < whole * (1 - CHART_ZOOM.fitSlack);
}

/**
 * Scale the window by `factor` (> 1 zooms IN) about `focus`, a 0–1 fraction across the plot. The instant under the
 * focus stays under it, so a pinch or a wheel zooms into what the reader is pointing at, not into the middle.
 */
export function zoomDomain(d: Domain, full: Domain, focus: number, factor: number): Domain {
  if (!(factor > 0) || !Number.isFinite(factor)) return clampDomain(d, full);
  const f = clamp(Number.isFinite(focus) ? focus : 0.5, 0, 1);
  const span = d.t1 - d.t0;
  const at = d.t0 + f * span;
  const whole = full.t1 - full.t0;
  const next = clamp(span / factor, minSpan(full), whole);
  return clampDomain({ t0: at - f * next, t1: at - f * next + next }, full);
}

/**
 * Would zooming by `factor` change the window? False at the whole chart for a zoom OUT and at the narrowest window
 * for a zoom IN — the wheel then belongs to the page, which scrolls on past the chart (review 2026-10-21: a desktop
 * reader's scroll stuck over a chart that had nothing left to do with it).
 */
export function zoomChanges(d: Domain, full: Domain, factor: number): boolean {
  if (!(factor > 0) || !Number.isFinite(factor) || factor === 1) return false;
  const cur = clampDomain(d, full);
  const next = zoomDomain(cur, full, 0.5, factor);
  const whole = Math.max(1, full.t1 - full.t0);
  return Math.abs((next.t1 - next.t0) - (cur.t1 - cur.t0)) > whole * CHART_ZOOM.fitSlack;
}

/** Slide the window by `frac` of its own width (positive = later in time), stopping at the ends of the record. */
export function panDomain(d: Domain, full: Domain, frac: number): Domain {
  const shift = (d.t1 - d.t0) * (Number.isFinite(frac) ? frac : 0);
  return clampDomain({ t0: d.t0 + shift, t1: d.t1 + shift }, full);
}

/** A mouse wheel's deltaY (px; negative = away from the reader = zoom in) as a zoom factor. */
export function wheelFactor(deltaY: number): number {
  if (!Number.isFinite(deltaY) || deltaY === 0) return 1;
  return CHART_ZOOM.wheelPer100 ** (-deltaY / 100);
}

/** The time at a 0–1 fraction across the window. */
export function timeAt(d: Domain, frac: number): number {
  return d.t0 + clamp(frac, 0, 1) * (d.t1 - d.t0);
}

/**
 * The index of the sample nearest `t` among those that HAVE a value (`has(i)`), or -1. A gap in the record is never
 * read out as a number: the nearest real sample is, and only within `maxGapMs` of the finger (default: no limit).
 */
export function nearestIndex(times: readonly number[], t: number, has: (i: number) => boolean = () => true,
                             maxGapMs = Infinity): number {
  let best = -1;
  let gap = Infinity;
  for (let i = 0; i < times.length; i++) {
    const ti = times[i]!;
    if (!Number.isFinite(ti) || !has(i)) continue;
    const g = Math.abs(ti - t);
    if (g < gap) { gap = g; best = i; }
  }
  return gap <= maxGapMs ? best : -1;
}
