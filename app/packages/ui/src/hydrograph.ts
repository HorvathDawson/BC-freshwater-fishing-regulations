/**
 * The hydrograph, as arithmetic.
 *
 * Every number a chart needs — scales, ticks, path strings — is computed here, and the
 * component only draws the shapes it is handed. That is AGENTS.md rule 25 doing real work:
 * the phone renders these paths with react-native-svg, desktop renders the SAME paths with
 * DOM SVG, and there is no second chart to drift.
 *
 * It is also why there is no charting library. v1 needed chart.js plus a date adapter plus
 * a zoom plugin for one chart; the hydrograph is the only chart in this app and it is an
 * unusual one — a percentile envelope, a log axis, two year traces, forecast ribbons.
 * Generic chart APIs fight all four.
 */
import type { Band } from "@app/core";

export interface Box {
  width: number;
  height: number;
  padLeft: number;
  padRight: number;
  padTop: number;
  padBottom: number;
}

export const DEFAULT_BOX: Box = {
  width: 320, height: 190, padLeft: 40, padRight: 10, padTop: 12, padBottom: 24,
};

export interface Tick {
  value: number;
  at: number;
  label: string;
}

export interface Envelope {
  /** Filled band between two percentile traces, as an SVG path. */
  d: string;
  from: number;
  to: number;
}

/**
 * The forecast, as shapes — drawn to the RIGHT of the observations on the same axis.
 *
 * A separate field rather than more points on `line`, because a prediction and a
 * measurement must never be one stroke. The band is the model's own min-to-max spread; the
 * line is the number the model is asked for (highest for a flood model, lowest for a
 * low-flow one), and `at` is where the observations stop and the model starts.
 */
export interface ForecastShape {
  /** The model's published bounds, as a filled ribbon. Empty when it published none. */
  band: string;
  /** The forecast trace itself — every step, never two points. */
  line: string;
  /** x of the boundary between what happened and what is expected. */
  at: number;
  /** The last forecast point, for a label at the end of the ribbon. */
  end: { x: number; y: number } | null;
}

export interface Hydrograph {
  box: Box;
  /** Observed values, in whatever quantity the series is about. */
  line: string;
  /** Nested percentile bands, widest first, so they can be drawn in order. */
  envelopes: readonly Envelope[];
  /** The median trace. */
  median: string;
  yTicks: readonly Tick[];
  xTicks: readonly Tick[];
  /** Where "now" sits, for the marker and the vertical rule. */
  now: { x: number; y: number } | null;
  /** The model run past today, or null outside every model's season. */
  forecast: ForecastShape | null;
  /** True when the y scale is logarithmic — a whole year of flow spans two orders. */
  log: boolean;
}

const fmt = (v: number): string =>
  v >= 1000 ? `${Math.round(v / 100) / 10}k`
    : v >= 100 ? String(Math.round(v))
      : v >= 10 ? String(Math.round(v * 10) / 10)
        : String(Math.round(v * 100) / 100);

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

/** Log ticks: every 1, 2, 5 within the decades the data spans. */
export function logTicks(lo: number, hi: number): number[] {
  const out: number[] = [];
  for (let e = Math.floor(Math.log10(lo)); e <= Math.ceil(Math.log10(hi)); e++) {
    for (const m of [1, 2, 5]) {
      const v = m * 10 ** e;
      if (v >= lo && v <= hi) out.push(v);
    }
  }
  return out;
}

export interface BuildInput {
  /** Observed values, evenly spaced. */
  values: readonly (number | null)[];
  /** Percentile bands aligned to `values`, or one band repeated for a short span. */
  bands: readonly (Band | null)[];
  /** Labels for the x axis, evenly spaced across the series. */
  xLabels: readonly string[];
  /** Index of "now" within `values`; -1 for none. */
  nowIndex?: number;
  /**
   * The reading to MARK, when it is not one of `values`.
   *
   * The seasonal chart has no observations at all — it is a year of envelope with today on
   * it — so the dot cannot be read out of the series. Supplying it explicitly is the
   * difference between marking today and marking the last element of an array of nulls.
   */
  nowValue?: number | null;
  /**
   * The model run, as a SERIES on its own time axis.
   *
   * `at` here are the same kind of stamps as the observations', so the two are laid out
   * against one clock rather than against each other's index — which is what lets an
   * hourly ten-day CLEVER run and a daily thirty-day ELF run share the frame with a
   * half-hourly observation series without either being stretched.
   *
   * Steps at or before the last observation are DROPPED, not drawn: both models publish
   * their own hindcast, and a model's opinion about yesterday laid over the measurement of
   * yesterday is two lines claiming the same thing.
   */
  forecast?: { at: readonly string[];
               mid: readonly (number | null)[];
               lo: readonly (number | null)[];
               hi: readonly (number | null)[] } | null;
  /** Observation stamps, for laying the forecast on the same clock. */
  at?: readonly string[];
  log?: boolean;
  box?: Box;
}

/**
 * Build every path and tick for one hydrograph.
 *
 * The y range always includes the normal band, not just the observations — a reading far
 * below the 10th percentile is the whole story, and a chart scaled only to the observed
 * values hides it by filling the frame.
 */
export function buildHydrograph(input: BuildInput): Hydrograph {
  const box = input.box ?? DEFAULT_BOX;
  const { width, height, padLeft, padRight, padTop, padBottom } = box;
  const innerW = width - padLeft - padRight;
  const innerH = height - padTop - padBottom;
  const vals = input.values;
  const bands = input.bands;
  const log = input.log ?? false;

  // THE TWO SERIES SHARE ONE CLOCK. Observations run from the first stamp to the last;
  // the forecast continues past it. Everything below positions on time, not on index.
  const stamps = (input.at ?? []).map((t) => Date.parse(t));
  const t0 = stamps.length && Number.isFinite(stamps[0]!) ? stamps[0]! : 0;
  const t1 = stamps.length && Number.isFinite(stamps[stamps.length - 1]!)
    ? stamps[stamps.length - 1]! : t0 + 1;

  // A model's own hindcast is dropped: it overlaps the record, and two lines claiming
  // yesterday is one more than there is evidence for.
  const rawFc = input.forecast ?? null;
  const ahead = rawFc
    ? rawFc.at.map((t, i) => ({ t: Date.parse(t), mid: rawFc.mid[i] ?? null,
                                lo: rawFc.lo[i] ?? null, hi: rawFc.hi[i] ?? null }))
        .filter((p) => Number.isFinite(p.t) && p.t > t1 && p.mid !== null)
    : [];
  const fc = ahead.length ? ahead : null;
  const tEnd = fc ? fc[fc.length - 1]!.t : t1;

  let lo = Infinity;
  let hi = -Infinity;
  for (const v of vals) if (v !== null && Number.isFinite(v)) { lo = Math.min(lo, v); hi = Math.max(hi, v); }
  // THE FORECAST IS INSIDE THE SCALE. A flood outlook that runs off the top of the frame is
  // the one case where the chart most needs to be readable, and clipping it would show a
  // line leaving the picture with no indication of where it was going.
  for (const p of fc ?? [])
    for (const v of [p.lo, p.mid, p.hi])
      if (v !== null && Number.isFinite(v)) { lo = Math.min(lo, v); hi = Math.max(hi, v); }
  if (input.nowValue !== null && input.nowValue !== undefined && Number.isFinite(input.nowValue)) {
    lo = Math.min(lo, input.nowValue); hi = Math.max(hi, input.nowValue);
  }
  for (const b of bands) {
    if (!b) continue;
    lo = Math.min(lo, b[0]);
    hi = Math.max(hi, b[4]);
  }
  if (!Number.isFinite(lo) || !Number.isFinite(hi)) { lo = 0; hi = 1; }
  if (hi === lo) hi = lo + 1;
  if (log) {
    lo = Math.max(lo * 0.85, hi / 3000);
  } else {
    const pad = (hi - lo) * 0.18;
    lo = Math.max(0, lo - pad);
    hi += pad * 0.4;
  }

  const L0 = log ? Math.log10(Math.max(lo, 1e-6)) : lo;
  const L1 = log ? Math.log10(hi) : hi;
  const y = (v: number): number =>
    padTop + innerH - (((log ? Math.log10(Math.max(v, 10 ** L0)) : v) - L0) / (L1 - L0)) * innerH;
  // TIME IS THE X AXIS, so an hourly forecast and a daily record land where they belong.
  // Falls back to index when the caller passed no stamps, which keeps every existing chart
  // laid out exactly as it was.
  const span = Math.max(1, tEnd - t0);
  const xAt = (t: number): number => padLeft + ((t - t0) / span) * innerW;
  const obsW = tEnd > t1 ? xAt(t1) - padLeft : innerW;
  const x = (i: number): number =>
    stamps.length === vals.length && vals.length > 1
      ? xAt(stamps[i] ?? t0)
      : padLeft + (vals.length > 1 ? (i / (vals.length - 1)) * obsW : obsW / 2);

  const pt = (i: number, v: number): string => `${x(i).toFixed(1)},${y(v).toFixed(1)}`;

  // Observed. A null is a gap in the record, so the line breaks rather than inventing one.
  const segments: string[] = [];
  let run: string[] = [];
  vals.forEach((v, i) => {
    if (v === null || !Number.isFinite(v)) {
      if (run.length > 1) segments.push(`M${run.join("L")}`);
      run = [];
    } else run.push(pt(i, v));
  });
  if (run.length > 1) segments.push(`M${run.join("L")}`);

  const trace = (pick: (b: Band) => number): string[] =>
    bands.map((b, i) => (b ? pt(i, pick(b)) : "")).filter(Boolean);

  const envelope = (a: (b: Band) => number, z: (b: Band) => number): Envelope | null => {
    const top = trace(a);
    const bottom = trace(z);
    if (top.length < 2 || bottom.length < 2) return null;
    return { d: `M${top.join("L")}L${[...bottom].reverse().join("L")}Z`, from: 0, to: 1 };
  };

  const envelopes = [
    envelope((b) => b[4], (b) => b[0]),   // 10th - 90th
    envelope((b) => b[3], (b) => b[1]),   // the middle half
  ].filter((e): e is Envelope => e !== null);

  const medianPts = trace((b) => b[2]);
  const yValues = log ? logTicks(10 ** L0, 10 ** L1).slice(0, 7) : niceTicks(lo, hi, 4);

  const nowIndex = input.nowIndex ?? vals.length - 1;
  const nowValue = input.nowValue ?? (nowIndex >= 0 ? vals[nowIndex] ?? null : null);

  /**
   * The forecast, drawn as the model published it.
   *
   * It used to be one point: the summary layer carries a single value per station, so the
   * "ribbon" was a straight line from today to a dot at the frame edge and the band a
   * TRIANGLE. This is the model's own output — 240 hourly steps for a CLEVER run, 30 daily
   * ones for ELF — laid on the same clock as the record, so the shape on screen is the
   * shape the Centre published.
   *
   * The ribbon starts at the last OBSERVATION rather than at the first forecast step, so
   * there is no gap between what happened and what is expected.
   */
  const forecast: ForecastShape | null = fc ? (() => {
    const x0 = xAt(t1);
    const y0 = nowValue !== null ? y(nowValue) : y(fc[0]!.mid!);
    const line = [`${x0.toFixed(1)},${y0.toFixed(1)}`,
                  ...fc.map((p) => `${xAt(p.t).toFixed(1)},${y(p.mid!).toFixed(1)}`)];
    const withBounds = fc.filter((p) => p.lo !== null && p.hi !== null);
    const top = withBounds.map((p) => `${xAt(p.t).toFixed(1)},${y(p.hi!).toFixed(1)}`);
    const bottom = withBounds.map((p) => `${xAt(p.t).toFixed(1)},${y(p.lo!).toFixed(1)}`);
    const lastPoint = fc[fc.length - 1]!;
    return {
      band: top.length > 1
        ? `M${x0.toFixed(1)},${y0.toFixed(1)}L${top.join("L")}` +
          `L${[...bottom].reverse().join("L")}Z`
        : "",
      line: `M${line.join("L")}`,
      at: x0,
      end: { x: xAt(lastPoint.t), y: y(lastPoint.mid!) },
    };
  })() : null;

  return {
    box,
    line: segments.join(""),
    envelopes,
    median: medianPts.length > 1 ? `M${medianPts.join("L")}` : "",
    yTicks: yValues.map((v) => ({ value: v, at: y(v), label: fmt(v) })),
    // Spread across the WHOLE frame when a forecast extends it, so the last label is not
    // stranded halfway with a third of the chart unlabelled to its right.
    xTicks: input.xLabels.map((label, i) => ({
      value: i,
      at: padLeft + (input.xLabels.length > 1
        ? (i / (input.xLabels.length - 1)) * innerW : 0),
      label,
    })),
    now: nowValue !== null ? { x: x(nowIndex), y: y(nowValue) } : null,
    forecast,
    log,
  };
}
