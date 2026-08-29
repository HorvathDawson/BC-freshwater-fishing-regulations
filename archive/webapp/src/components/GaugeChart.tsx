/**
 * GaugeChart — hydrograph for a single hydrometric gauge station.
 *
 * Ported from the hydro POC viewer (pipeline/recurring/hydro/index.html), but
 * consuming the shipped R2 JSON artifacts (cron/hydro/{recent,history,climatology}
 * /<id>.json) instead of the local serve.py /api endpoints. Two modes:
 *   • Recent   — observed 30-min/hourly line + per-model forecast hatch ribbons +
 *                subtle percentile context bands + a dashed "now" line.
 *   • Seasonal — day-of-year percentile envelope (Min–Max → 10–90 → 25–75 →
 *                median) + this-year & last-year daily traces + a "today" line.
 *
 * ECCC + BC River Forecast Centre attribution is rendered by the parent tab
 * (getGaugeAttribution) — legally required wherever this data is shown.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import {
  Chart,
  LineController,
  LineElement,
  PointElement,
  LinearScale,
  LogarithmicScale,
  TimeScale,
  Filler,
  Legend,
  Tooltip,
  Decimation,
} from 'chart.js';
import 'chartjs-adapter-date-fns';
import zoomPlugin from 'chartjs-plugin-zoom';
import { waterbodyDataService, type GaugeStation } from '../services/waterbodyDataService';

Chart.register(
  LineController, LineElement, PointElement, LinearScale, LogarithmicScale,
  TimeScale, Filler, Legend, Tooltip, Decimation, zoomPlugin,
);

const DAY = 86400000;
const YEAR0 = Date.UTC(2000, 0, 1);
const ENVELOPE_RGB = '37,99,235';
const CONTEXT_RGB = '100,116,139';
const MODEL_COLORS: Record<string, string> = { CLEVER: '#7c3aed', COFFEE: '#0891b2', ELF: '#dc2626' };
const PCTL_COLS = ['p0', 'p10', 'p25', 'p50', 'p75', 'p90', 'p100'] as const;

type Param = 'discharge' | 'level';
type Mode = 'recent' | 'seasonal';

// ── artifact shapes (mirror export_hydro build_recent/history/climatology) ──
type Pair = [string, number | null];
interface RecentDoc {
  discharge?: { q30m_3d?: Pair[]; q1h_3to14d?: Pair[] };
  level?: { q30m_3d?: Pair[]; q1h_3to14d?: Pair[] };
  forecast?: Record<string, [string, number | null, number | null, number | null][]>;
}
interface HistoryDoc { discharge_daily?: Pair[]; level_daily?: Pair[] }
interface ClimParam {
  unit?: string;
  record?: { start_year?: number; end_year?: number; n_years?: number; window_days?: number };
  p0?: (number | null)[]; p10?: (number | null)[]; p25?: (number | null)[]; p50?: (number | null)[];
  p75?: (number | null)[]; p90?: (number | null)[]; p100?: (number | null)[];
}
type ClimDoc = { discharge?: ClimParam | null; level?: ClimParam | null };
type Band = { doy: number } & Record<(typeof PCTL_COLS)[number], number | null>;

const hexToRgb = (hex: string): string => {
  const n = parseInt(hex.replace('#', ''), 16);
  return `${(n >> 16) & 255},${(n >> 8) & 255},${n & 255}`;
};

function hatchPattern(rgb: string, alpha = 0.55): CanvasPattern | string {
  const c = document.createElement('canvas');
  c.width = c.height = 7;
  const x = c.getContext('2d');
  if (!x) return `rgba(${rgb},${alpha})`;
  x.strokeStyle = `rgba(${rgb},${alpha})`;
  x.lineWidth = 1.1;
  x.beginPath(); x.moveTo(0, 7); x.lineTo(7, 0);
  x.moveTo(-1, 1); x.lineTo(1, -1); x.moveTo(6, 8); x.lineTo(8, 6);
  x.stroke();
  return x.createPattern(c, 'repeat') || `rgba(${rgb},${alpha})`;
}

/** Dense 366-length percentile arrays → [{doy, p0..p100}] bands (drops empty doys). */
function climToBands(cp: ClimParam | null | undefined): Band[] {
  if (!cp) return [];
  const bands: Band[] = [];
  for (let i = 0; i < 366; i++) {
    const b = { doy: i + 1 } as Band;
    let any = false;
    for (const col of PCTL_COLS) {
      const v = cp[col]?.[i] ?? null;
      b[col] = v;
      if (v != null) any = true;
    }
    if (any) bands.push(b);
  }
  return bands;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function envelopeDatasets(bands: Band[], xOf: (doy: number) => number | null, opts: { full: boolean; rgb?: string }): any[] {
  const rgb = opts.rgb ?? ENVELOPE_RGB;
  const pts = (col: (typeof PCTL_COLS)[number]) =>
    bands.map((b) => ({ x: xOf(b.doy), y: b[col] }))
      .filter((p) => p.x != null && p.y != null)
      .sort((a, b) => (a.x as number) - (b.x as number));
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const ribbon = (col: (typeof PCTL_COLS)[number], lower: any, alpha: number, label: string) => ({
    label, data: pts(col), borderColor: 'rgba(0,0,0,0)', borderWidth: 0,
    pointRadius: 0, backgroundColor: `rgba(${rgb},${alpha})`, fill: lower, tension: 0.3, spanGaps: true,
    // _band marks percentile envelope ribbons so the legend draws a filled box
    // swatch for them (vs. a line swatch for actual line series).
    _band: true,
  });
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const ds: any[] = [];
  if (opts.full) {
    ds.push(ribbon('p0', false, 0, '_p0'));
    ds.push(ribbon('p100', '-1', 0.06, 'Min–Max'));
  }
  ds.push(ribbon('p10', false, 0, '_p10'));
  ds.push(ribbon('p90', '-1', 0.12, '10th–90th pct'));
  ds.push(ribbon('p25', false, 0, '_p25'));
  ds.push(ribbon('p75', '-1', 0.22, '25th–75th pct'));
  ds.push({
    label: 'Median (history)', data: pts('p50'), borderColor: `rgba(${rgb},0.9)`,
    borderWidth: 1.5, borderDash: [5, 3], pointRadius: 0, fill: false, tension: 0.3, spanGaps: true,
  });
  return ds;
}

const dateToDoyX = (d: Date) => Date.UTC(2000, d.getUTCMonth(), d.getUTCDate());
const doyOf = (d: Date) => Math.round((dateToDoyX(d) - YEAR0) / DAY) + 1;
const doyToX = (doy: number) => YEAR0 + (doy - 1) * DAY;

// Vertical dashed marker (now / today).
function vLine(id: string, xValue: () => number, label: string) {
  return {
    id,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    afterDatasetsDraw(c: any) {
      const x = c.scales.x.getPixelForValue(xValue());
      const { top, bottom, left, right } = c.chartArea;
      if (x < left || x > right) return;
      const ctx = c.ctx;
      ctx.save();
      ctx.beginPath(); ctx.setLineDash([5, 4]); ctx.lineWidth = 1.5; ctx.strokeStyle = '#64748b';
      ctx.moveTo(x, top); ctx.lineTo(x, bottom); ctx.stroke();
      ctx.setLineDash([]); ctx.fillStyle = '#64748b';
      ctx.font = '11px ui-sans-serif, system-ui, sans-serif';
      ctx.textAlign = 'center'; ctx.fillText(label, x, top + 10);
      ctx.restore();
    },
  };
}

export default function GaugeChart({ station }: { station: GaugeStation }) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const chartRef = useRef<Chart | null>(null);
  const readoutRef = useRef<HTMLDivElement | null>(null);
  const [param, setParam] = useState<Param>('discharge');
  const [mode, setMode] = useState<Mode>('recent');
  const [status, setStatus] = useState<'loading' | 'ready' | 'empty'>('loading');
  const [note, setNote] = useState<string>('');
  // Which parameters this station actually reports — only these get a toggle.
  const [availableParams, setAvailableParams] = useState<Param[]>(['discharge', 'level']);
  // Transient "Hold Ctrl to zoom" hint, shown when a user wheels over the chart
  // without the modifier (so plain scroll keeps scrolling the page).
  const [showZoomHint, setShowZoomHint] = useState(false);
  const hintTimer = useRef<number | null>(null);

  const yTitle = param === 'discharge' ? 'Discharge (m³/s)' : 'Water level (m)';
  const canSeasonal = station.has_climatology !== false;

  // Determine which parameters this station actually reports (discharge/level),
  // so we only offer a toggle for params that have data. Computed from the recent
  // doc (cached) + the station's latest value. The Recent/Seasonal *view* toggle
  // is intentionally NOT gated — seasonal always shows so the user can discover it
  // and see the "needs ≥10 yr" message when a station has no climatology.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const recent = await waterbodyDataService.getStationRecent<RecentDoc>(station.id);
      if (cancelled) return;
      const has = (p: Param) => {
        const s = recent?.[p];
        const n = (s?.q30m_3d?.length || 0) + (s?.q1h_3to14d?.length || 0);
        return n > 0 || station.latest?.[p]?.value != null;
      };
      const avail = (['discharge', 'level'] as Param[]).filter(has);
      const next = avail.length ? avail : (['discharge'] as Param[]);
      setAvailableParams(next);
      setParam(p => (next.includes(p) ? p : next[0]));
    })();
    return () => { cancelled = true; };
  }, [station.id, station.latest]);

  const key = useMemo(() => `${station.id}:${param}:${mode}`, [station.id, param, mode]);

  useEffect(() => {
    let cancelled = false;
    setStatus('loading');
    setNote('');

    (async () => {
      if (mode === 'seasonal') {
        const [clim, hist] = await Promise.all([
          waterbodyDataService.getStationClimatology<ClimDoc>(station.id),
          waterbodyDataService.getStationHistory<HistoryDoc>(station.id),
        ]);
        if (cancelled) return;
        const bands = climToBands(clim?.[param]);
        const dailyKey = param === 'discharge' ? 'discharge_daily' : 'level_daily';
        const hasDaily = (hist?.[dailyKey]?.length || 0) > 0;
        // Show the seasonal view whenever we have EITHER the percentile envelope
        // (≥10 yr climatology) OR at least a daily record to trace this year's
        // line — short-record stations still get a bare annual hydrograph.
        if (!bands.length && !hasDaily) {
          setStatus('empty');
          setNote('No seasonal data for this station yet.');
          destroyChart();
          return;
        }
        renderSeasonal(bands, clim?.[param], hist);
      } else {
        const [recent, clim] = await Promise.all([
          waterbodyDataService.getStationRecent<RecentDoc>(station.id),
          canSeasonal ? waterbodyDataService.getStationClimatology<ClimDoc>(station.id) : Promise.resolve(null),
        ]);
        if (cancelled) return;
        renderRecent(recent, clim?.[param]);
      }
    })();

    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);

  useEffect(() => () => {
    destroyChart();
    if (hintTimer.current) window.clearTimeout(hintTimer.current);
  }, []);

  // Plain wheel over the chart → flash the "Hold Ctrl to zoom" hint (the zoom
  // plugin ignores non-Ctrl wheels, so the page scrolls normally underneath).
  function onWheelCapture(e: React.WheelEvent) {
    if (e.ctrlKey || e.metaKey) { setShowZoomHint(false); return; }
    setShowZoomHint(true);
    if (hintTimer.current) window.clearTimeout(hintTimer.current);
    hintTimer.current = window.setTimeout(() => setShowZoomHint(false), 1100);
  }

  function destroyChart() {
    chartRef.current?.destroy();
    chartRef.current = null;
  }

  function renderRecent(recent: RecentDoc | null, cp: ClimParam | null | undefined) {
    const series = recent?.[param];
    const observed = [...(series?.q30m_3d || []), ...(series?.q1h_3to14d || [])]
      .filter((p) => p[1] != null)
      .map((p) => ({ x: new Date(p[0].replace(' ', 'T')).getTime(), y: p[1] as number }))
      .sort((a, b) => a.x - b.x);

    if (!observed.length && !(recent?.forecast && Object.keys(recent.forecast).length)) {
      setStatus('empty');
      setNote('No recent readings for this station.');
      destroyChart();
      return;
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const datasets: any[] = [];

    // Percentile context bands mapped from doy onto real dates behind the line.
    const bands = climToBands(cp);
    if (bands.length) {
      const doyToRealX: Record<number, number> = {};
      const now = Date.now();
      for (let off = -18; off <= 12; off++) {
        const d = new Date(now + off * DAY);
        doyToRealX[doyOf(d)] = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), 12);
      }
      datasets.push(...envelopeDatasets(bands, (doy) => doyToRealX[doy] ?? null, { full: false, rgb: CONTEXT_RGB }));
    }

    datasets.push({
      label: 'Observed', data: observed, borderColor: '#0f172a',
      backgroundColor: 'rgba(15,23,42,0.06)', borderWidth: 1.75, pointRadius: 0,
    });

    // Forecast ribbons (discharge only, matching the POC).
    const FORECAST_CAP_DAYS = 12;
    const forecastMaxX = Date.now() + FORECAST_CAP_DAYS * DAY;
    const last = observed[observed.length - 1];
    if (last && param === 'discharge' && recent?.forecast) {
      for (const [model, rows] of Object.entries(recent.forecast)) {
        const color = MODEL_COLORS[model] || '#dc2626';
        // COFFEE (fall-flood outlook) defaults OFF — it's a specialist model most
        // users don't want by default; the rest default ON. Toggle via the legend.
        const hidden = model === 'COFFEE';
        const line = (idx: 1 | 2 | 3) => {
          const pts = rows
            .filter((r) => r[idx] != null)
            .map((r) => ({ x: new Date(r[0]).getTime(), y: r[idx] as number }))
            .filter((p) => p.x <= forecastMaxX)
            .sort((a, b) => a.x - b.x);
          if (!pts.length) return pts;
          return pts[0].x > last.x ? [{ x: last.x, y: last.y }, ...pts] : pts;
        };
        // _model/_modelColor tag the 3 datasets so the legend collapses them into
        // one clickable item that toggles the whole model (see buildChart legend).
        const tag = { _model: model, _modelColor: color, hidden };
        datasets.push({ label: `_${model}_min`, data: line(1), borderColor: color + '70', borderWidth: 1, pointRadius: 0, fill: false, ...tag });
        datasets.push({ label: `${model} range`, data: line(3), borderColor: color + '70', borderWidth: 1, pointRadius: 0, backgroundColor: hatchPattern(hexToRgb(color)), fill: '-1', ...tag });
        datasets.push({ label: `${model} forecast`, data: line(2), borderColor: color, borderWidth: 2.25, pointRadius: 0, fill: false, ...tag });
      }
    }

    buildChart(datasets, {
      xMin: Date.now() - 15 * DAY,
      xMax: Date.now() + 10 * DAY,
      panMin: Date.now() - 20 * DAY,
      panMax: Date.now() + 15 * DAY,
      logY: false,
      plugin: vLine('nowLine', () => Date.now(), 'now'),
      tooltipFormat: 'yyyy-MM-dd HH:mm',
    });
    setStatus('ready');
    setNote(observed.length ? `${observed.length.toLocaleString()} points${bands.length ? ` · percentile context from ${cp?.record?.n_years ?? '?'}-yr record` : ''}` : '');
  }

  function renderSeasonal(bands: Band[], cp: ClimParam | null | undefined, hist: HistoryDoc | null) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const datasets: any[] = envelopeDatasets(bands, (doy) => doyToX(doy), { full: true });

    const daily = hist?.[param === 'discharge' ? 'discharge_daily' : 'level_daily'] || [];
    const byYear: Record<number, { x: number; y: number }[]> = {};
    for (const [ts, v] of daily) {
      if (v == null) continue;
      const d = new Date(ts.replace(' ', 'T'));
      (byYear[d.getUTCFullYear()] ||= []).push({ x: dateToDoyX(d), y: v });
    }
    const years = Object.keys(byYear).map(Number).sort((a, b) => b - a);
    const curYear = new Date().getUTCFullYear();
    const priorYear = years.find((y) => y < curYear);
    if (priorYear != null) {
      datasets.push({ label: `${priorYear}`, data: byYear[priorYear].sort((a, b) => a.x - b.x), borderColor: '#94a3b8', borderWidth: 1.25, pointRadius: 0, fill: false, tension: 0, spanGaps: true });
    }
    if (byYear[curYear]) {
      datasets.push({ label: `${curYear} (this year)`, data: byYear[curYear].sort((a, b) => a.x - b.x), borderColor: '#ea580c', borderWidth: 2.25, pointRadius: 0, fill: false, tension: 0, spanGaps: true });
    }

    const positive = bands.length > 0 && param === 'discharge' && bands.every((b) => b.p0 == null || (b.p0 as number) > 0);
    const todayX = dateToDoyX(new Date());
    const win = 45 * DAY;
    buildChart(datasets, {
      xMin: todayX - win, xMax: todayX + win,
      panMin: doyToX(1), panMax: doyToX(366),
      logY: positive,
      plugin: vLine('todayLine', () => todayX, 'today'),
      tooltipFormat: 'MMM d',
      timeUnit: 'month',
    });
    setStatus('ready');
    const rec = cp?.record;
    if (rec) {
      setNote(`record ${rec.start_year}–${rec.end_year} (${rec.n_years} yr)${byYear[curYear] ? '' : ' · no data this year yet'}`);
    } else {
      // No ≥10 yr climatology — percentile bands absent; we're showing the raw
      // annual trace only.
      setNote(`no percentile bands (needs ≥10 yr)${byYear[curYear] ? '' : ' · no data this year yet'}`);
    }
  }

  const READOUT_HINT = 'Hover the chart to read values at a point.';

  function clearReadout() {
    const el = readoutRef.current;
    if (!el) return;
    el.classList.add('empty');
    el.textContent = READOUT_HINT;
  }

  const fmtVal = (v: number) =>
    Math.abs(v) >= 100 ? Math.round(v).toLocaleString() : Math.abs(v) >= 10 ? v.toFixed(1) : v.toFixed(2);

  // Chart.js `external` tooltip handler — writes the hovered point(s) into the
  // off-plot readout strip instead of a floating box that hides the data.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  function renderReadout(context: any) {
    const el = readoutRef.current;
    if (!el) return;
    const tt = context.tooltip;
    if (!tt || tt.opacity === 0) { clearReadout(); return; }
    el.classList.remove('empty');
    const parts: string[] = [];
    const title = (tt.title || []).join(' ');
    if (title) parts.push(`<span class="gauge-readout-title">${title}</span>`);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (tt.dataPoints || []).forEach((dp: any) => {
      const label: string = dp.dataset?.label || '';
      if (label.startsWith('_')) return;
      const y = dp.parsed?.y;
      if (y == null) return;
      const border = dp.dataset?.borderColor;
      const color = border && border !== 'rgba(0,0,0,0)' ? border : dp.dataset?.backgroundColor || '#64748b';
      parts.push(
        `<span class="gauge-readout-item"><span class="gauge-readout-swatch" style="background:${color}"></span>` +
        `${label} <span class="gauge-readout-val">${fmtVal(y)}</span></span>`,
      );
    });
    el.innerHTML = parts.length > 1 ? parts.join('') : (title ? `${parts[0]}<span class="gauge-readout-item">no value here</span>` : READOUT_HINT);
    if (parts.length <= 1 && !title) clearReadout();
  }

  function buildChart(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    datasets: any[],
    o: { xMin: number; xMax: number; panMin: number; panMax: number; logY: boolean; plugin: unknown; tooltipFormat: string; timeUnit?: string },
  ) {
    destroyChart();
    const canvas = canvasRef.current;
    if (!canvas) return;
    chartRef.current = new Chart(canvas, {
      type: 'line',
      data: { datasets },
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      plugins: [o.plugin as any],
      options: {
        responsive: true,
        maintainAspectRatio: false,
        parsing: false,
        normalized: true,
        animation: false,
        spanGaps: true,
        elements: { line: { tension: 0 }, point: { radius: 0, hitRadius: 5, hoverRadius: 4 } },
        interaction: { mode: 'nearest', axis: 'x', intersect: false },
        scales: {
          x: {
            type: 'time',
            min: o.xMin,
            max: o.xMax,
            // Seasonal maps day-of-year onto a 2000 leap-year calendar, so show
            // month names only — never the (meaningless) year 2000 on the axis.
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            time: { unit: o.timeUnit as any, tooltipFormat: o.tooltipFormat, displayFormats: { month: 'MMM' } },
            ticks: { maxRotation: 0 },
          },
          y: { type: o.logY ? 'logarithmic' : 'linear', title: { display: true, text: yTitle } },
        },
        plugins: {
          /* eslint-disable @typescript-eslint/no-explicit-any -- chart.js legend callbacks are untyped */
          legend: {
            display: true,
            labels: {
              // usePointStyle lets each legend item pick its own swatch shape via
              // pointStyle: percentile bands get a filled 'rect' box, line series
              // (Observed, Median, year traces, forecasts) get a 'line'.
              usePointStyle: true,
              // Build the legend from scratch so each forecast MODEL is a single
              // item (fill = band colour, border = line colour) instead of three,
              // and non-applicable series (empty data or "_"-prefixed) are dropped.
              generateLabels: (chart: any) => {
                const items: any[] = [];
                const seenModel = new Set<string>();
                chart.data.datasets.forEach((d: any, i: number) => {
                  const label: string = d.label || '';
                  if (label.startsWith('_')) return;
                  if (!Array.isArray(d.data) || d.data.length === 0) return;
                  const model: string | undefined = d._model;
                  if (model) {
                    if (seenModel.has(model)) return;
                    seenModel.add(model);
                    const color = d._modelColor || d.borderColor;
                    const anchor = chart.data.datasets.findIndex(
                      (x: any) => x._model === model && String(x.label || '').endsWith('forecast'),
                    );
                    const idx = anchor >= 0 ? anchor : i;
                    items.push({
                      text: `${model} forecast`,
                      // Forecasts are line series → line swatch in the model colour.
                      pointStyle: 'line',
                      fillStyle: color,
                      strokeStyle: color,
                      lineWidth: 2,
                      hidden: !chart.isDatasetVisible(idx),
                      datasetIndex: idx,
                      _model: model,
                    });
                  } else {
                    // Percentile bands → filled box; everything else → line swatch.
                    const isBand = d._band === true;
                    items.push({
                      text: label,
                      pointStyle: isBand ? 'rect' : 'line',
                      fillStyle: d.backgroundColor,
                      strokeStyle: isBand ? d.backgroundColor : d.borderColor,
                      lineWidth: isBand ? 0 : (d.borderWidth || 1.5),
                      lineDash: d.borderDash || [],
                      hidden: !chart.isDatasetVisible(i),
                      datasetIndex: i,
                    });
                  }
                });
                return items;
              },
            },
            // Click a forecast-model item to toggle ALL its datasets at once.
            onClick: (_e: any, item: any, legend: any) => {
              const chart = legend.chart;
              const model = item._model as string | undefined;
              if (model) {
                const vis = chart.isDatasetVisible(item.datasetIndex);
                chart.data.datasets.forEach((x: any, i: number) => {
                  if (x._model === model) chart.setDatasetVisibility(i, !vis);
                });
              } else {
                const i = item.datasetIndex;
                chart.setDatasetVisibility(i, !chart.isDatasetVisible(i));
              }
              chart.update();
            },
          },
          /* eslint-enable @typescript-eslint/no-explicit-any */
          decimation: { enabled: true, algorithm: 'lttb' },
          tooltip: {
            // Disable the floating overlay — it covered the very point you're
            // inspecting. Values render off-plot in the readout strip below.
            enabled: false,
            filter: (item) => !(item.dataset.label || '').startsWith('_'),
            external: renderReadout,
          },
          zoom: {
            // Desktop: require Ctrl so plain wheel scrolls the page, not the chart
            // (Ctrl+wheel zooms, Ctrl+drag box-zooms). Plain drag pans; pinch zooms
            // on touch (no modifier). See the onWheel hint in the JSX below.
            zoom: {
              wheel: { enabled: true, modifierKey: 'ctrl' },
              pinch: { enabled: true },
              drag: { enabled: true, modifierKey: 'ctrl' },
              mode: 'x',
            },
            pan: { enabled: true, mode: 'x' },
            limits: { x: { min: o.panMin, max: o.panMax } },
          },
        },
      },
    });
  }

  return (
    <div className="gauge-chart">
      <div className="gauge-chart-controls">
        {/* Parameter toggle — only params this station reports. Hidden entirely
            when the station has just one (nothing to toggle). */}
        {availableParams.length > 1 && (
          <div className="gauge-seg" role="tablist" aria-label="Parameter">
            {availableParams.includes('discharge') && (
              <button role="tab" aria-selected={param === 'discharge'} className={param === 'discharge' ? 'active' : ''} onClick={() => setParam('discharge')}>Discharge</button>
            )}
            {availableParams.includes('level') && (
              <button role="tab" aria-selected={param === 'level'} className={param === 'level' ? 'active' : ''} onClick={() => setParam('level')}>Level</button>
            )}
          </div>
        )}
        {/* View toggle — always both, even without climatology (seasonal then
            shows the "needs ≥10 yr" message so the user knows it exists). */}
        <div className="gauge-seg" role="tablist" aria-label="View">
          <button role="tab" aria-selected={mode === 'recent'} className={mode === 'recent' ? 'active' : ''} onClick={() => setMode('recent')}>Recent</button>
          <button role="tab" aria-selected={mode === 'seasonal'} className={mode === 'seasonal' ? 'active' : ''} onClick={() => setMode('seasonal')}>Seasonal</button>
        </div>
      </div>

      {/* Canvas is ALWAYS mounted + visible so Chart.js measures a real size
          (hiding it during load builds the chart at 0×0 → blank/mis-scaled).
          Loading/empty states are overlays on top. */}
      <div className="gauge-chart-canvas" onWheel={onWheelCapture}>
        <canvas ref={canvasRef} />
        {status === 'loading' && <div className="gauge-chart-msg">Loading…</div>}
        {status === 'empty' && <div className="gauge-chart-msg">{note || 'No data for this view.'}</div>}
        {showZoomHint && status === 'ready' && (
          <div className="gauge-zoom-hint">Hold <kbd>Ctrl</kbd> + scroll to zoom · drag to pan</div>
        )}
      </div>

      {status === 'ready' && (
        <div ref={readoutRef} className="gauge-readout empty" aria-live="polite">{READOUT_HINT}</div>
      )}

      {status === 'ready' && note && <div className="gauge-chart-note">{note}</div>}

      <div className="gauge-explainer">
        {mode === 'recent' ? (
          <p>
            <strong>Recent</strong> shows the last two weeks of measured flow (near-black
            line) plus model forecasts ahead of the <em>now</em> line. The grey bands behind
            it are the historical range for these dates.
          </p>
        ) : (
          <p>
            <strong>Seasonal</strong> compares this year (bold) against the full historical
            record for each day of the year. Recent zooms into the last two weeks + forecast;
            Seasonal shows the whole year of typical conditions.
          </p>
        )}
        <p>
          <strong>Percentile bands</strong> show how today compares to history: the median
          (middle line) is the typical value; the <em>25–75%</em> band is the normal range
          (half of all years fall inside it); <em>10–90%</em> is wider; and Min–Max spans
          every year on record. Below the bands is unusually low, above is unusually high.
        </p>
      </div>
    </div>
  );
}
