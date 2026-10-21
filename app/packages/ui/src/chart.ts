/**
 * THE FLOW CHART AS A VIEW: its shape at the size it is drawn, the window of time on screen, and the value under the
 * reader's finger (owner 2026-10-21: "the plot doesnt scale to fill page", "zoom and look at data better").
 *
 * The chart was built once, at a fixed 320 × 190 box, and stretched — so on a desktop column it was a postage stamp
 * with 9-px labels in the middle of a white field. Now the component MEASURES its width and this builds the paths for
 * exactly that box, so text stays text-sized at every width.
 *
 * Every decision is core's (`chartZoom.ts`: what a zoom, a pan and a nearest sample are; `conditions.ts`: the words)
 * or `buildHydrograph`'s; this holds the state and hands the component actions that only take measurements.
 */
import { useMemo, useRef, useState } from "react";
import { CHART_ZOOM, CONDITIONS_WORDS, chartReadoutLine, fitDomain, isZoomed, monthAbbr, panDomain, timeAt,
         zoomDomain, type ChartSpan, type Domain } from "@app/core";
import type { Parameter, RegsSource, Series, StationId } from "@app/data";
import { useAsync, type Async } from "./async";
import { buildHydrograph, DEFAULT_BOX, type Box, type Hydrograph } from "./hydrograph";

/** How tall the chart is for a width: a phone keeps the old proportion; a wide column does not become a banner. */
export function chartHeight(width: number): number {
  return Math.round(Math.min(340, Math.max(190, width * 0.5)));
}

/** The chart's box at a measured width (never narrower than the old fixed one). */
export function chartBox(width: number): Box {
  const w = Math.max(DEFAULT_BOX.width, Math.round(width));
  return { ...DEFAULT_BOX, width: w, height: chartHeight(w) };
}

/** How many times the x axis names: five on a column, three on a phone, where "20 Oct 06h" five times runs together. */
export function tickCount(width: number): number {
  return width < 480 ? 3 : 5;
}

const pad2 = (n: number) => String(n).padStart(2, "0");

/**
 * The axis ticks for a window, as REAL TIMES. The year chart said "Jan Mar May Jul Sep Nov" evenly across a frame that
 * ends a month past today — so "Nov" sat over August. Months now sit on their own first days; a zoomed window names
 * its days; a short window its hours. A year chart's days are calendar days (UTC-stamped), said as such; the 72-hour
 * chart's hours are the reader's clock (P16).
 */
export function chartTicks(view: Domain, span: ChartSpan, n = 5): { t: number; label: string }[] {
  const ms = view.t1 - view.t0;
  const DAY = 86_400_000;
  if (span === "year" && ms > 60 * DAY) {
    const first = new Date(view.t0);
    let y = first.getUTCFullYear(), m = first.getUTCMonth() + (first.getUTCDate() > 1 ? 1 : 0);
    const months: number[] = [];
    for (;;) {
      const t = Date.UTC(y, m, 1);
      if (t > view.t1) break;
      months.push(t);
      m += 1;
    }
    const step = Math.max(1, Math.ceil(months.length / (n + 1)));
    return months.filter((_, i) => i % step === 0)
      .map((t) => ({ t, label: monthAbbr(new Date(t).getUTCMonth()) }));
  }
  return Array.from({ length: n }, (_, i) => i / Math.max(1, n - 1)).map((f) => {
    const t = view.t0 + ms * f;
    const d = new Date(t);
    if (span === "year") return { t, label: `${d.getUTCDate()} ${monthAbbr(d.getUTCMonth())}` };
    const day = `${d.getDate()} ${monthAbbr(d.getMonth())}`;
    return { t, label: ms <= 72 * 3_600_000 ? `${day} ${pad2(d.getHours())}h` : day };
  });
}

export interface ChartView {
  shape: Hydrograph;
  /** The window is narrower than the whole record — "Whole chart" has something to do. */
  zoomed: boolean;
  /** The value under the finger, placed and said; null when nothing is being read. */
  readout: { x: number; y: number; line: string; kind: "reading" | "forecast" } | null;
  /** + and − : one step about the middle. */
  zoomIn: () => void;
  zoomOut: () => void;
  /** Back to the whole record (double-tap, double-click, "Whole chart"). Also clears the read-out. */
  fit: () => void;
  /** A pinch or a wheel: scale by `factor` (> 1 is in) about `x` px across the chart. */
  zoomAt: (x: number, factor: number) => void;
  /** A drag: the finger moved `dx` px (right = earlier in time comes into view). */
  panBy: (dx: number) => void;
  /** Read the value at `x` px across the chart; null stops reading. */
  probeAt: (x: number | null) => void;
}

interface State { key: string; domain: Domain | null; probe: number | null }

/** The extent `buildHydrograph` will lay out — the record's first stamp to its forecast's last step. */
function extentOf(s: Series | null): Domain | null {
  if (!s || s.at.length < 2) return null;
  const t0 = Date.parse(s.at[0]!);
  const t1 = Date.parse(s.at[s.at.length - 1]!);
  const ahead = (s.forecast?.series?.at ?? []).map((t) => Date.parse(t)).filter((t) => Number.isFinite(t) && t > t1);
  const end = ahead.length ? Math.max(...ahead) : t1;
  return Number.isFinite(t0) && Number.isFinite(end) ? { t0, t1: Math.max(end, t0 + 1) } : null;
}

/**
 * The chart for one station, span, quantity and model, at `width` px. The window and the read-out reset when any of
 * those change: a zoom into last Tuesday on one gauge means nothing on the next.
 */
export function useChartView(source: RegsSource, station: StationId | null, span: ChartSpan,
                             parameter: Parameter | undefined, model: string | undefined,
                             width: number): Async<ChartView | null> {
  const key = `series:${station}:${span}:${parameter ?? "auto"}:${model ?? "auto"}`;
  const series = useAsync(
    () => (station ? source.gaugeSeries(station, span, parameter, model) : Promise.resolve(null)),
    key, station !== null);
  const [raw, setState] = useState<State>({ key, domain: null, probe: null });
  const st: State = raw.key === key ? raw : { key, domain: null, probe: null };
  const s = series.state === "ready" ? series.value?.value ?? null : null;
  const box = chartBox(width);
  const full = useMemo(() => extentOf(s), [s]);

  // The actions read the LATEST window, not the one they were made with: a pinch delivers many updates between two
  // renders, and each must compound on the last.
  const live = useRef({ full, box, key });
  live.current = { full, box, key };
  const update = (f: (d: Domain, full: Domain) => Partial<State>) =>
    setState((prev) => {
      const { full: fu, key: k } = live.current;
      if (!fu) return prev;
      const cur = prev.key === k ? prev : { key: k, domain: null, probe: null };
      return { ...cur, ...f(cur.domain ?? fitDomain(fu), fu) };
    });
  const frac = (x: number) => (x - live.current.box.padLeft)
    / Math.max(1, live.current.box.width - live.current.box.padLeft - live.current.box.padRight);

  const actions = useMemo(() => ({
    zoomIn: () => update((d, fu) => ({ domain: zoomDomain(d, fu, 0.5, CHART_ZOOM.step) })),
    zoomOut: () => update((d, fu) => ({ domain: zoomDomain(d, fu, 0.5, 1 / CHART_ZOOM.step) })),
    fit: () => update(() => ({ domain: null, probe: null })),
    zoomAt: (x: number, factor: number) => update((d, fu) => ({ domain: zoomDomain(d, fu, frac(x), factor) })),
    panBy: (dx: number) => update((d, fu) => ({ domain: panDomain(d, fu, -frac(dx + live.current.box.padLeft)) })),
    probeAt: (x: number | null) => update((d) => ({ probe: x === null ? null : timeAt(d, frac(x)) })),
  }), []);

  return useMemo<Async<ChartView | null>>(() => {
    if (series.state !== "ready") return series as Async<ChartView | null>;
    if (!s) return { state: "ready", value: null, error: null };
    const n = s.values.length;
    const bands = s.band.length === n ? s.band : Array.from({ length: n }, () => s.band[0] ?? null);
    const shape = buildHydrograph({
      values: s.values, bands, at: s.at, xLabels: [],
      nowIndex: s.now?.index ?? -1, nowValue: s.now?.value ?? null, priorYears: s.priorYears,
      forecast: s.forecast?.series
        ? { at: s.forecast.series.at, mid: s.forecast.series.mid, lo: s.forecast.series.lo, hi: s.forecast.series.hi }
        : null,
      log: span === "year" && s.parameter === "discharge",
      box, domain: st.domain, probe: st.probe,
      ticks: (v) => chartTicks(v, span, tickCount(box.width)),
    });
    const p = shape.probe;
    const readout = p ? {
      x: p.x, y: p.y, kind: p.kind,
      line: chartReadoutLine({ t: p.t, value: p.value, unit: CONDITIONS_WORDS.unit[s.parameter], kind: p.kind,
                               daily: span === "year", median: p.median }),
    } : null;
    return { state: "ready", error: null,
             value: { shape, readout, zoomed: !!(shape.view && shape.full && isZoomed(shape.view, shape.full)),
                      ...actions } };
  }, [series, s, span, box.width, box.height, st.domain, st.probe, actions]);
}
