/**
 * T487 — THE GAUGE PAGE'S LIVE HALF, in the reader's browser (no worker compute: the page itself is a static file).
 *
 * It reads two things and draws them:
 *   - the gauge's facts, `<data>/feeds/cards/<station>.json`: what the feeds worker drew the share card from in its gauge
 *     run (app/workers/feeds/src/gauges/card/card.ts `cardFacts`) — the reading, its standing words, its time, and the runs
 *     the app offers (for the credit line). The words were chosen there (core); this file lays them out and says the
 *     reading's age line the app's way, at the reader's own time, from the instants given.
 *     (T1039) THE CHART IS NOT DRAWN HERE: it is the app's own, drawn by gaugeChart.tsx (its own deferred script) into
 *     #gauge-chart from the live feed file and the station's climatology.
 *   - the weather at the gauge, from Open-Meteo (the one outside source the app uses for weather), at the URL the build
 *     wrote (rounded to ~1 km, data/spots/weather.ts `weatherPlace`).
 * A file that cannot be read is SHOWN as such where its section is AND reported (the app's crash endpoint, crash/2,
 * route "conditions") — unless it is the reader's own line (offline): shown, not reported (core failure.ts rule).
 *
 * ONE IMPORT, from core: the build (gauges.ts `clientScript`) transpiles this file to one plain script and writes the
 * core functions it names (and the ones they call) into it from their own source, so the page cannot round, stamp or age
 * a value differently from the app (T622; `gauges.test.ts` holds the names and the output to core's). Everything else it
 * needs (the weather URL, attributions, the sky words) arrives in the page's config.
 */
import { BC_TIME_LABEL, bandOfWord, bcDayWords, daysNear, FLOW_LEGEND, NORMAL_BAND, bcIsoDayWords, bcStampWords, freshness,
         observedTrend, readingAgeBrief, gaugeSummaryLine, standingNowLine, bandWord } from "@app/core";

export interface GaugeConfig {
  readonly station: string;
  /** the facts file, absolute */
  readonly facts: string;
  /** the Open-Meteo request, or null (a gauge with no position) */
  readonly weather: string | null;
  readonly weatherAttribution: string;
  /** WMO weather code -> the app's sky word (data/spots/words.ts `skyWord`) */
  readonly sky: Readonly<Record<string, string>>;
  /** the crash endpoint (null: reports off) and what a report names */
  readonly crash: string | null;
  readonly env: string;
  readonly release: string;
  /** (T1039) the live gauge feed's base (`<data>/feeds/live`, the app's `httpFeed` base): the chart's observations and runs */
  readonly live: string;
  /** (T1039) the station's climatology from the release (`gauge_clim`: a quantity, its clim/3 bands in base64) */
  readonly clim: readonly { readonly parameter: "discharge" | "level"; readonly bands: string }[];
  /** (T1039) the app's faces as `@font-face` rules (the web export's own files), or null: the build had no export */
  readonly fonts: string | null;
  /** (T1039) the app's contact (core `STORE_CONTACT`): the "Get the app" block's Join the test emails it */
  readonly contact: string | null;
}

export interface Run { model: string; issuedAt: string; issued: string; dueBy: number; points: [number, number][]; attribution: string; horizonDays?: number }

export interface Facts {
  format: string; station: string; name: string; drawnAt: string;
  parameter: "discharge" | "level" | null; unit: string | null; reading: number | null; value: string | null; standing: string | null;
  at: string | null; atText: string | null; staleAfterMs: number;
  /** when the feed wrote our copy (ISO), or null: the age the reading's line is judged by (the app's rule) */
  fetchedAt: string | null;
  /** core's words for a forecast run past its line ("likely out of date") */
  runOld: string;
  /** the station's expected change, in core's short words ("rises to 4× (Wed)"), or null */
  change: string | null;
  week: [number, number][];
  /** the climatology's normal range a day over the chart's window: [epoch ms, p10, p25, p50, p75, p90]; null: none for this gauge */
  normal?: [number, number, number, number, number, number][] | null;
  forecast: Run | null;
  /** (T908) every model with a current run, the station's own order (the page's forecast options); absent in a file the worker wrote before T908 */
  forecasts?: Run[];
  /** (T908) the reading's rank 0-1, what the standing was worded from; absent: no marker on the colour bar */
  percentile?: number | null;
  forecastOld: { model: string; issued: string } | null;
}

/** (T1039) what the page says where the chart goes when it could not be drawn (gaugeChart.tsx says the same) */
export const CHART_FAILED = "The chart could not be drawn. Try again later.";

const esc = (s: string): string => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

/** the reading block: the value, its standing, its time; a stale reading said in so many words */
export function readingHtml(f: Facts, now: number): string {
  if (f.value === null) {
    return `<p class="reading">No current reading</p>` +
      `<p class="warn">${f.atText ? `This gauge last reported ${esc(f.atText)}.` : "The live feed has no reading for this gauge."} ` +
      "It may be down, seasonal, or late reporting.</p>";
  }
  // (T622 G7, G8, G22, G24) the reading's time and how old OUR copy is, judged by core's `freshness` (the age of the feed's copy,
  // not of the reading). (T908, owner 2026-11-19) three rows: the headline with the figure, the colour bar, one small line
  const copy = freshness(f.fetchedAt ? Date.parse(f.fetchedAt) : null, now, f.staleAfterMs);
  const summary = gaugeSummaryLine(rankOf(f), f.change, readingAgeBrief(copy, f.at ? Date.parse(f.at) : null, now));
  return `<div class="headrow">${headlineHtml(f, now)}<p class="reading" aria-label="${f.parameter === "level" ? "water level" : "flow"}"><b>${esc(f.value)}</b></p></div>` +
    colourBarHtml(f) + `<p class="${copy.state === "live" ? "asof" : "warn"} meta">${esc(summary)}</p>`;
}

const DAY = 86_400_000;

/** core's rank half of the facts' standing line ("lower than 9 days in 10 for the date"), or null */
function rankOf(f: Facts): string | null {
  if (!f.standing) return null;
  const lead = f.standing.split(" · ")[0]!;
  return bandOfWord(lead) === null ? null : f.standing.slice(lead.length + 3) || null;
}

/**
 * (T908, owner 2026-11-19) THE HEADLINE: "Above normal for Nov 19 · rising", in the colour the map gives that standing (the Flow vs.
 * past years ramp; css `.b-<band>`). The band word is the one core said in the facts' standing line (`bandOfWord` reads it back to its
 * band); the day and the trend word are core's (`standingNowLine`, `observedTrend`); the trend is the measured line over the last
 * day. A standing that is none of the six bands is shown as core said it; no standing, nothing.
 */
export function headlineHtml(f: Facts, now: number): string {
  if (!f.standing) return "<span></span>";
  const lead = f.standing.split(" · ")[0]!, band = bandOfWord(lead);
  if (band === null) return `<p class="standing nowline">${esc(f.standing)}</p>`;
  const last = f.week[f.week.length - 1];
  let before: [number, number] | undefined;
  if (last) for (const p of f.week) if (p[0] <= last[0] - DAY && (!before || p[0] > before[0])) before = p;
  const trend = last && before ? observedTrend(before[1], last[1]) : null;
  return `<p class="standing nowline b-${band}">${esc(standingNowLine(bandWord(band), bcDayWords(now), trend))}</p>`;
}

/**
 * (T908, owner 2026-11-19: "the same color bar ... like we have in condition tab") THE CONDITIONS TAB'S "Flow vs. past years" BAR,
 * with this gauge's reading marked on it: the map's ramp (css `--flow-ramp`, built from the style's own stops), Low and High at the
 * ends, the bracket over the middle half marked Normal (core's `NORMAL_BAND`), and the reading at its percentile. No percentile in
 * the facts (a file from before T908, or no record to rank by): no bar.
 */
export function colourBarHtml(f: Facts): string {
  const p = f.percentile;
  if (typeof p !== "number" || !Number.isFinite(p) || p < 0 || p > 1) return "";
  const title = FLOW_LEGEND.title[f.parameter === "level" ? "level" : "discharge"];
  const pct = (x: number) => `${Math.round(x * 1000) / 10}%`;
  return `<div class="ramp" role="img" aria-label="${esc(`${FLOW_LEGEND.scale(title, FLOW_LEGEND.low, FLOW_LEGEND.high, FLOW_LEGEND.mid)}. This gauge: ${daysNear(p)}`)}">` +
    `<div class="rampbar"><span>${esc(FLOW_LEGEND.low)}</span><div class="bar"><i class="mark" style="left:${pct(p)}"></i></div><span>${esc(FLOW_LEGEND.high)}</span></div>` +
    `<div class="rampnormal"><div style="left:${pct(NORMAL_BAND[0])};width:${pct(NORMAL_BAND[1] - NORMAL_BAND[0])}">${esc(FLOW_LEGEND.mid)}</div></div></div>`;
}

/** the credit under the chart: the readings', and the Province's for the forecast runs the facts name (the app's chart draws them) */
export function creditHtml(f: Pick<Facts, "forecast" | "forecasts">): string {
  const run = f.forecasts?.[0] ?? f.forecast;
  return `<p class="small">Readings: Water Survey of Canada real-time data (Environment and Climate Change Canada), provisional.` +
    `${run ? ` ${esc(run.attribution)}` : ""}</p>`;
}

interface Meteo {
  current?: { time?: string; temperature_2m?: number; precipitation?: number; weather_code?: number; wind_speed_10m?: number };
  daily?: { time?: string[]; weather_code?: number[]; temperature_2m_max?: number[]; temperature_2m_min?: number[];
            precipitation_sum?: number[]; wind_speed_10m_max?: number[] };
}

/** the weather now and the coming days; throws on an answer with neither (shown + reported by the caller) */
export function weatherHtml(m: Meteo, sky: Readonly<Record<string, string>>, attribution: string, fetchedAt: number): string {
  const d = m.daily, c = m.current;
  if (!c && !d?.time?.length) throw new Error("weather: Open-Meteo answered with no current or daily rows");
  const n = (v: number | undefined, unit: string, digits = 0) => (typeof v === "number" && Number.isFinite(v) ? `${v.toFixed(digits)}${unit}` : "—");
  const word = (code: number | undefined) => (typeof code === "number" ? sky[String(code)] ?? "—" : "—");
  const head = c ? `Weather at the gauge: ${n(c.temperature_2m, "°C")} ${word(c.weather_code)}` : "Weather at the gauge";
  const now = c ? `<p class="small">Wind ${n(c.wind_speed_10m, " km/h")} · precipitation ${n(c.precipitation, " mm", 1)}${c.time ? ` · as of ${esc(c.time.replace("T", " "))} ${BC_TIME_LABEL}` : ""}</p>` : "";
  const rows = (d?.time ?? []).map((day, i) => {
    return `<tr><th scope="row">${esc(bcIsoDayWords(day))}</th><td>${esc(word(d!.weather_code?.[i]))}</td><td>${n(d!.temperature_2m_max?.[i], "°")} / ${n(d!.temperature_2m_min?.[i], "°")}</td>` +
      `<td>${n(d!.precipitation_sum?.[i], " mm", 1)}</td><td>${n(d!.wind_speed_10m_max?.[i], " km/h")}</td></tr>`;
  }).join("");
  return `<details class="wxfold"><summary>${esc(head)}</summary>` + now + (rows ? `<table class="wx"><thead><tr><th>Day</th><th>Sky</th><th>High / low</th><th>Rain</th><th>Wind</th></tr></thead><tbody>${rows}</tbody></table>` : "") +
    `<p class="small">${esc(attribution)}, fetched ${esc(bcStampWords(fetchedAt))}.</p></details>`;
}

/** the reader's own line (offline, dropped) or a failure marked the environment's (`environment: true`, a permission the
 *  reader refused): shown, never reported — core failure.ts `isEnvironment`, the same words */
export function isEnvironment(e: unknown): boolean {
  const x = e as { environment?: unknown; name?: unknown; message?: unknown } | null;
  if (!x || typeof x !== "object") return false;
  if (x.environment === true) return true;
  if (x.name === "AbortError") return true;
  return x.name === "TypeError" && typeof x.message === "string" &&
    /^(?:Network request failed|Failed to fetch|Load failed|NetworkError when attempting to fetch resource\.?)$/.test(x.message);
}

/** a crash/2 report (the feeds worker's `POST /crash`, workers/feeds/src/crashes.ts validateCrash) */
export function crashReport(e: unknown, cfg: Pick<GaugeConfig, "env" | "release"> & { app?: string; route?: string }, at: Date, install: string): Record<string, unknown> {
  const err = e instanceof Error ? e : new Error(String(e));
  return {
    format: "crash/2", created: at.toISOString(), kind: "js", fatal: false, route: cfg.route ?? "conditions",
    app: cfg.app ?? "gauge-page", env: cfg.env, platform: "web", release: cfg.release.slice(0, 100),
    name: (err.name || "Error").slice(0, 60), message: err.message.slice(0, 200), stack: (err.stack ?? "").slice(0, 2000),
    background: false, install, water: null, view: (cfg.route ?? "conditions") === "conditions" ? "conditions" : null, part: null, whole: false, sheet: null,
  };
}

/**
 * THE PAGE'S ONE REPORTER: a failure the page caught is posted to the crash endpoint (crash/2), once per call, unless it is
 * the environment's (shown, not reported) or reports are off (`crash` null). The gauge page's live half, its chart
 * (gaugeChart.tsx) and a region hub's find script (gauges.ts `findScript` writes it in) report through it.
 */
export function pageReporter(cfg: Pick<GaugeConfig, "crash" | "env" | "release"> & { app?: string; route?: string }, fetchImpl: typeof fetch, now: () => number): (e: unknown) => void {
  const install = Array.from({ length: 16 }, () => "0123456789abcdef"[Math.floor(Math.random() * 16)]).join("");
  return (e: unknown) => {
    if (!cfg.crash || isEnvironment(e)) return;
    void fetchImpl(cfg.crash, { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(crashReport(e, cfg, new Date(now()), install)) }).catch(() => { /* the report itself is best effort */ });
  };
}

async function readJson(fetchImpl: typeof fetch, url: string): Promise<unknown> {
  const r = await fetchImpl(url, { cache: "no-cache" });
  if (!r.ok) throw new Error(`${url}: HTTP ${r.status}`);
  return await r.json();
}

/** the page's live half: fills #gauge-now, #gauge-credit and #gauge-weather (the chart is gaugeChart.tsx's); resolves when both reads have settled */
export async function gaugePage(doc: Document, now: () => number, fetchImpl: typeof fetch): Promise<void> {
  const cfgEl = doc.getElementById("gauge-config");
  if (!cfgEl?.textContent) throw new Error("gauge page: no #gauge-config");
  const cfg = JSON.parse(cfgEl.textContent) as GaugeConfig;
  const report = pageReporter(cfg, fetchImpl, now);
  const put = (id: string, html: string) => { const el = doc.getElementById(id); if (el) el.innerHTML = html; };
  const failed = (id: string, what: string, e: unknown) => {
    put(id, `<p class="warn">${esc(what)} could not be read${isEnvironment(e) ? " (no connection)" : ""}. Try again later.</p>`);
    report(e);
  };
  const facts = readJson(fetchImpl, cfg.facts).then((raw) => {
    const f = raw as Facts;
    if (f.format !== "gauge-card/1") throw new Error(`${cfg.facts}: format ${String(f.format)}, not gauge-card/1`);
    put("gauge-now", readingHtml(f, now()));
    put("gauge-credit", creditHtml(f));
  }).catch((e) => failed("gauge-now", "The latest reading", e));
  const weather = cfg.weather === null
    ? Promise.resolve(put("gauge-weather", `<p class="small">This gauge has no position on file, so no weather is shown.</p>`))
    : readJson(fetchImpl, cfg.weather).then((m) => put("gauge-weather", weatherHtml(m as Meteo, cfg.sky, cfg.weatherAttribution, now())))
        .catch((e) => failed("gauge-weather", "The weather forecast", e));
  // (T1039 review, W46) THE CHART'S OWN SCRIPT (gaugeChart.tsx) marks #gauge-chart the moment it runs. Once the page has
  // loaded (every deferred script run, or failed), a box it never marked means that script did not load or did not parse:
  // said where the chart goes, and reported
  const chartRan = () => {
    const box = doc.getElementById("gauge-chart");
    if (!box || box.hasAttribute("data-mounted")) return;
    box.innerHTML = `<p class="warn">${esc(CHART_FAILED)}</p>`;
    report(new Error("gauge page: the chart's script did not run (not loaded, or did not parse)"));
  };
  // a document with no window (no load to wait for) is judged now
  const win = doc.defaultView;
  if (doc.readyState === "complete" || !win) chartRan(); else win.addEventListener("load", chartRan, { once: true });
  await Promise.all([facts, weather]);
}
