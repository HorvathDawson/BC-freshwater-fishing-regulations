/**
 * T487 — THE GAUGE PAGE'S LIVE HALF, in the reader's browser (no worker compute: the page itself is a static file).
 *
 * It reads two things and draws them:
 *   - the gauge's facts, `<data>/feeds/cards/<station>.json`: what the feeds worker drew the share card from in its gauge
 *     run (app/workers/feeds/src/gauges/card/card.ts `cardFacts`) — the reading, its standing words, its time, the last
 *     7 days and the forecast run with its working-day line. The words were chosen there (core); this file lays them out
 *     and says the reading's age line the app's way, at the reader's own time, from the instants given.
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
import { BC_TIME_LABEL, bandOfWord, bcDayWords, daysNear, FLOW_LEGEND, FORECAST_MODEL_CONTROL, forecastDash, forecastDrawn, forecastModelOption, NORMAL_BAND, NO_MODEL_PICK, pickModel, realtimeOnly, REALTIME_ONLY, bcIsoDayWords, bcMidnight, bcStampWords, bcWeekdayAbbr, figure, freshness, layoutDayMarks, niceTicks,
         normalRangeOf, observedTrend, readingAgeBrief, gaugeSummaryLine, standingNowLine } from "@app/core";

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
  return `<p class="standing nowline b-${band}">${esc(standingNowLine(lead, bcDayWords(now), trend))}</p>`;
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

/**
 * (T908, owner 2026-11-19: "gauge page should have clever and coffee forecast options") THE MODELS A GAUGE HAS: every current run
 * (`forecasts`, the station's own order), or the one `forecast` of a facts file written before T908; the lead is the first.
 * `horizonDays` is what the options say ("CLEVER · 10d"): the days to the run's last point when the file does not name it.
 */
export function runsOf(f: Facts, now: number): Run[] {
  const runs = f.forecasts ?? (f.forecast ? [f.forecast] : []);
  return runs.map((r) => ({ ...r, horizonDays: r.horizonDays ?? Math.max(1, Math.round((r.points[r.points.length - 1]![0] - now) / DAY)) }));
}
/** the models drawn for a pick, the app's own rule (`pickModel`, `forecastDrawn`): the chosen one first, then the others on */
export function drawnRuns(runs: readonly Run[], pick: ModelPickLike): Run[] {
  if (!forecastDrawn(pick.on, runs)) return [];
  const lead = pick.model ?? runs[0]?.model;
  return [lead, ...pick.also].flatMap((m) => runs.filter((r) => r.model === m));
}
type ModelPickLike = { readonly model: string | null; readonly also: readonly string[]; readonly on: boolean | null };

/** the forecast lines under the chart, one per model drawn; a gauge with no run says so in one line */
export function forecastHtml(f: Facts, now: number, drawn: readonly Run[] = drawnRuns(runsOf(f, now), NO_MODEL_PICK)): string {
  const runs = runsOf(f, now);
  if (runs.length) {
    return drawn.map((r) => `<p class="small">${esc(r.model)} (dashed): issued ${esc(r.issued)}` +
      (now > r.dueBy ? ` — <b>${esc(f.runOld)}</b> (no newer run since)` : "") + `.</p>`).join("");
  }
  if (f.forecastOld) return `<p class="small">No current flow forecast: the latest ${esc(f.forecastOld.model)} run (issued ${esc(f.forecastOld.issued)}) is ${esc(f.runOld)}, so it is not drawn.</p>`;
  return `<p class="small">No flow forecast for this gauge.</p>`;
}

/** the forecast options, the app's own: one switch per model (its colour and dash) and "Real-time only" (`pickModel`, `realtimeOnly`) */
export function modelsHtml(runs: readonly Run[], drawn: readonly Run[], realtime: boolean): string {
  if (!runs.length) return "";
  const on = (m: string) => drawn.some((r) => r.model === m);
  const chip = (r: Run) => `<button type="button" class="chip m-${esc(r.model)}" data-model="${esc(r.model)}" aria-pressed="${on(r.model)}">` +
    `<svg width="26" height="8" viewBox="0 0 26 8" aria-hidden="true"><line x1="1" y1="4" x2="25" y2="4" stroke-dasharray="${forecastDash(r.model)}"/></svg> ` +
    `${esc(forecastModelOption({ model: r.model, horizonDays: r.horizonDays! }))}</button>`;
  return `<div class="models" role="group" aria-label="${esc(FORECAST_MODEL_CONTROL)}">${runs.map(chip).join("")}` +
    `<button type="button" class="chip" data-model="" aria-pressed="${realtime}">${esc(REALTIME_ONLY)}</button></div>`;
}

// `figure`, the B.C. day and stamp words and the age line are core's: clientScript() writes their source into the shipped
// script, so none is typed twice

/** the usual range on a day from the facts' normal rows (the row nearest `t`, within a day); null: none */
export function usualAt(f: Facts, t: number): { lo: number; hi: number } | null {
  let best: [number, number, number, number, number, number] | null = null;
  for (const r of f.normal ?? []) if (Math.abs(r[0] - t) <= DAY && (!best || Math.abs(r[0] - t) < Math.abs(best[0] - t))) best = r;
  return best ? normalRangeOf([best[1], best[2], best[3], best[4], best[5]]) : null;
}

/**
 * (T564) THE CHART IS THE MAIN THING ON THE PAGE: drawn at the width it is shown at (so a phone's 13 px text is 13 px, not a
 * 640-wide drawing shrunk to 60 %), tall on a phone, with round-figure ticks (core `niceTicks`), a mark for every day, the
 * usual range for the date behind the line (dark: the middle half of past years, light: 8 in 10) and TODAY as the strong mark
 * (core `layoutDayMarks`, T604). The last 7 days and the forecast's dashes; null when there is nothing to draw.
 */
export function chartSvg(f: Facts, now: number, width = 640, drawn: readonly Run[] = drawnRuns(runsOf(f, now), NO_MODEL_PICK)): string | null {
  const W = Math.round(Math.min(900, Math.max(280, width))), H = W < 520 ? Math.round(W * 0.6) : Math.round(W * 0.3);
  const L = 48, R = 12, T = 30, B = 30, FS = 13;
  const obs = f.week, fc = drawn.flatMap((r) => r.points);
  if (obs.length + fc.length < 2) return null;
  const t0 = now - 7 * DAY, t1 = Math.max(now, ...fc.map((p) => p[0]));
  const vals = [...obs, ...fc].map((p) => p[1]);
  let lo = Math.min(...vals), hi = Math.max(...vals);
  if (hi - lo < 1e-9) { lo -= 1; hi += 1; }
  const pad = (hi - lo) * 0.1; lo = Math.max(f.parameter === "discharge" ? 0 : -Infinity, lo - pad); hi += pad;
  const x = (t: number) => (L + ((t - t0) / (t1 - t0 || 1)) * (W - L - R));
  const y = (v: number) => (T + (1 - (v - lo) / (hi - lo)) * (H - T - B));
  const n = (v: number) => v.toFixed(1);
  const path = (ps: readonly [number, number][]) => ps.map((p, i) => `${i ? "L" : "M"}${n(x(p[0]))} ${n(y(p[1]))}`).join("");
  const last = obs[obs.length - 1];
  // the usual range, clipped to the plot: the part of the window the facts hold
  const rows = (f.normal ?? []).filter((r) => r[0] >= t0 - DAY && r[0] <= t1 + DAY);
  const band = (a: number, b: number) => rows.length < 2 ? "" :
    `<path d="${rows.map((r, i) => `${i ? "L" : "M"}${n(x(r[0]))} ${n(y(r[b]!))}`).join("")}${[...rows].reverse().map((r) => `L${n(x(r[0]))} ${n(y(r[a]!))}`).join("")}Z" class="${a === 1 ? "bandw" : "bandn"}"/>`;
  const ticks = niceTicks(lo, hi, H < 300 ? 3 : 4);
  // a mark for every B.C. day (a weekday), thinned so two words never meet
  const days: number[] = [];
  for (let d = bcMidnight(new Date(t0 + DAY).toISOString().slice(0, 10)); d <= t1; d += DAY) if (d >= t0) days.push(d);
  const every = Math.max(1, Math.ceil((days.length * 36) / (W - L - R)));
  const marks = layoutDayMarks({ plot: { x: L, width: W - L - R }, top: 14, rowHeight: FS + 3, fontSize: FS,
                                 marks: [{ x: x(now), text: "Today", kind: "today" }] })[0]!;
  const parts = [
    `<clipPath id="plot"><rect x="${L}" y="${T}" width="${W - L - R}" height="${H - T - B}"/></clipPath>`,
    ...ticks.map((v) => `<line x1="${L}" y1="${n(y(v))}" x2="${W - R}" y2="${n(y(v))}" class="grid"/><text x="${L - 6}" y="${n(y(v) + 4)}" class="tick" text-anchor="end">${figure(v)}</text>`),
    `<g clip-path="url(#plot)">${band(1, 5)}${band(2, 4)}</g>`,
    ...days.map((d, i) => `<line x1="${n(x(d))}" y1="${H - B}" x2="${n(x(d))}" y2="${H - B + 4}" class="axis"/>` +
      (i % every === 0 ? `<text x="${n(x(d))}" y="${H - 8}" class="tick" text-anchor="middle">${esc(bcWeekdayAbbr(d + 12 * 3600_000))}</text>` : "")),
    `<line x1="${L}" y1="${H - B}" x2="${W - R}" y2="${H - B}" class="axis"/>`,
    obs.length >= 2 ? `<path d="${path(obs)}L${n(x(last![0]))} ${H - B}L${n(x(obs[0]![0]))} ${H - B}Z" class="area"/>` : "",
    obs.length >= 2 ? `<path d="${path(obs)}" class="obs"/>` : "",
    ...drawn.map((r) => `<path d="${path(last ? [last, ...r.points] : r.points)}" class="fc m-${esc(r.model)}" stroke-dasharray="${forecastDash(r.model)}"/>`),
    `<line x1="${n(x(now))}" y1="${T - 6}" x2="${n(x(now))}" y2="${H - B}" class="now"/>`,
    last ? `<circle cx="${n(x(last[0]))}" cy="${n(y(last[1]))}" r="5" class="dot"/>` : "",
    marks.placed ? `<text x="${n(marks.tx)}" y="${n(marks.ty)}" class="today" text-anchor="${marks.anchor}">Today</text>` : "",
  ];
  const what = f.parameter === "level" ? "Water level" : "Flow";
  return `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="${esc(`${what} over the last 7 days${fc.length ? " and the forecast" : ""}, in ${f.unit ?? ""}`)}">${parts.join("")}</svg>`;
}

/** what the lines and shades of the chart are, as cards under it (a chart with no key is a guess) */
export function chartKeyHtml(f: Facts): string {
  const item = (cls: string, words: string) => `<li><svg width="26" height="12" viewBox="0 0 26 12" aria-hidden="true"><${cls}</svg> ${esc(words)}</li>`;
  const usual = (f.normal?.length ?? 0) >= 2;
  return `<details class="keyfold"><summary>What the lines and shading are</summary><ul class="key"><li class="k-head">${esc(f.parameter === "level" ? "Water level" : "Flow")}, ${esc(f.unit ?? "")}</li>` +
    item('line x1="0" y1="6" x2="26" y2="6" class="obs"/>', "measured") +
    (usual ? item('rect x="0" y="1" width="26" height="10" class="bandn"/>', "usual range for the date: half of past years") +
             item('rect x="0" y="1" width="26" height="10" class="bandw"/>', "8 years in 10") : "") + `</ul></details>`;
}

/** the forecast, a day at a time: the expected figure and the usual range for that day (the same rows the chart shades) */
export function forecastDaysHtml(f: Facts, now: number, drawn: readonly Run[] = drawnRuns(runsOf(f, now), NO_MODEL_PICK)): string {
  const lead = drawn[0];
  if (!lead || f.week.length + lead.points.length < 2) return "";
  if (now > lead.dueBy) return "";
  const seen = new Set<string>();
  const rows: string[] = [];
  for (const [t, v] of lead.points) {
    const label = bcDayWords(t, true);
    if (t < now - 12 * 3600_000 || seen.has(label)) continue;
    seen.add(label);
    const u = usualAt(f, t);
    rows.push(`<tr><th scope="row">${esc(label)}</th><td>${esc(figure(v))} ${esc(f.unit ?? "")}</td><td>${u ? `${esc(figure(u.lo))}–${esc(figure(u.hi))}` : "—"}</td></tr>`);
  }
  if (!rows.length) return "";
  return `<details class="fdfold"><summary>The forecast, day by day</summary><table class="wx fd"><thead><tr><th>Day</th><th>Expected</th><th>Usual for the date</th></tr></thead><tbody>${rows.join("")}</tbody></table></details>`;
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
export function crashReport(e: unknown, cfg: Pick<GaugeConfig, "env" | "release">, at: Date, install: string): Record<string, unknown> {
  const err = e instanceof Error ? e : new Error(String(e));
  return {
    format: "crash/2", created: at.toISOString(), kind: "js", fatal: false, route: "conditions",
    app: "gauge-page", env: cfg.env, platform: "web", release: cfg.release.slice(0, 100),
    name: (err.name || "Error").slice(0, 60), message: err.message.slice(0, 200), stack: (err.stack ?? "").slice(0, 2000),
    background: false, install, water: null, view: "conditions", part: null, whole: false, sheet: null,
  };
}

/**
 * THE PAGE'S ONE REPORTER: a failure the page caught is posted to the crash endpoint (crash/2), once per call, unless it is
 * the environment's (shown, not reported) or reports are off (`crash` null). The gauge page's live half and its Join the
 * test card (joinClient.ts, T911; gauges.ts `joinScript` writes this into that script too) both report through it.
 */
export function pageReporter(cfg: Pick<GaugeConfig, "crash" | "env" | "release">, fetchImpl: typeof fetch, now: () => number): (e: unknown) => void {
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

/** the page's live half: fills #gauge-now, #gauge-chart and #gauge-weather; resolves when both reads have settled */
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
    const t = now();
    put("gauge-now", readingHtml(f, t));
    const box = doc.getElementById("gauge-chart");
    // (T908) the forecast options: the app's own pick (`pickModel`, `realtimeOnly`), redrawn on a tap
    const runs = runsOf(f, t);
    let pick: ModelPickLike & { readonly model: string | null; readonly also: readonly string[]; readonly on: boolean | null } = NO_MODEL_PICK;
    const render = () => {
      const tn = now(), drawn = drawnRuns(runs, pick);
      const svg = chartSvg(f, tn, box?.clientWidth || 640, drawn);
      put("gauge-chart", `<h2>Last 7 days and the forecast</h2>` + modelsHtml(runs, drawn, drawn.length === 0) +
        (svg ? svg + chartKeyHtml(f) : `<p class="small">No readings in the last 7 days.</p>`) + forecastHtml(f, tn, drawn) + forecastDaysHtml(f, tn, drawn) +
        `<p class="small">Readings: Water Survey of Canada real-time data (Environment and Climate Change Canada), provisional.` +
        `${runs[0] ? ` ${esc(runs[0].attribution)}` : ""}</p>`);
    };
    render();
    box?.addEventListener("click", (ev) => {
      const b = (ev.target as Element | null)?.closest?.("button[data-model]");
      if (!b) return;
      const m = b.getAttribute("data-model") ?? "";
      const drawing = drawnRuns(runs, pick).length > 0, lead = pick.model ?? runs[0]?.model ?? null;
      pick = m === "" ? realtimeOnly(pick) : pickModel(pick, m, drawing ? lead : null, drawing);
      render();
    });
    // the chart is drawn at the width it is shown at: a turned phone draws it again
    let drawnAt = box?.clientWidth ?? 0;
    doc.defaultView?.addEventListener("resize", () => {
      const w = box?.clientWidth ?? 0;
      if (!box || Math.abs(w - drawnAt) < 24) return;
      drawnAt = w;
      const first = box.querySelector("svg:not([aria-hidden])");
      if (first) first.outerHTML = chartSvg(f, now(), w, drawnRuns(runs, pick)) ?? "";
    });
  }).catch((e) => { failed("gauge-now", "The latest reading", e); put("gauge-chart", ""); });
  const weather = cfg.weather === null
    ? Promise.resolve(put("gauge-weather", `<p class="small">This gauge has no position on file, so no weather is shown.</p>`))
    : readJson(fetchImpl, cfg.weather).then((m) => put("gauge-weather", weatherHtml(m as Meteo, cfg.sky, cfg.weatherAttribution, now())))
        .catch((e) => failed("gauge-weather", "The weather forecast", e));
  await Promise.all([facts, weather]);
}
