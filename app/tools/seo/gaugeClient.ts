/**
 * T487 — THE GAUGE PAGE'S LIVE HALF, in the reader's browser (no worker compute: the page itself is a static file).
 *
 * It reads two things and draws them:
 *   - the gauge's facts, `<data>/feeds/cards/<station>.json`: what the feeds worker drew the share card from in its gauge
 *     run (app/workers/feeds/src/gauges/card/card.ts `cardFacts`) — the reading, its standing words, its time, the last
 *     7 days and the forecast run with its working-day line. Every word was chosen there (core); this file only lays it
 *     out and judges staleness at the reader's own time from the instants given.
 *   - the weather at the gauge, from Open-Meteo (the one outside source the app uses for weather), at the URL the build
 *     wrote (rounded to ~1 km, data/spots/weather.ts `weatherPlace`).
 * A file that cannot be read is SHOWN as such where its section is AND reported (the app's crash endpoint, crash/2,
 * route "conditions") — unless it is the reader's own line (offline): shown, not reported (core failure.ts rule).
 *
 * NO IMPORTS: the build (gauges.ts `clientScript`) transpiles this file to one plain script; everything it needs from
 * the app's packages (words, the weather URL, attributions) arrives in the page's config.
 */

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

export interface Facts {
  format: string; station: string; name: string; drawnAt: string;
  parameter: "discharge" | "level" | null; unit: string | null; reading: number | null; value: string | null; standing: string | null;
  at: string | null; atText: string | null; staleAfterMs: number;
  week: [number, number][];
  forecast: { model: string; issuedAt: string; issued: string; dueBy: number; points: [number, number][]; attribution: string } | null;
  forecastOld: { model: string; issued: string } | null;
}

const esc = (s: string): string => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

/** "21 hours ago" — coarse, as a reader says it */
export function ago(ms: number): string {
  const m = Math.max(0, Math.round(ms / 60_000));
  if (m < 2) return "just now";
  if (m < 90) return `${m} minutes ago`;
  const h = Math.round(m / 60);
  if (h < 36) return `${h} hours ago`;
  return `${Math.round(h / 24)} days ago`;
}

/** the reading block: the value, its standing, its time; a stale reading said in so many words */
export function readingHtml(f: Facts, now: number): string {
  if (f.value === null) {
    return `<p class="reading">No current reading</p>` +
      `<p class="warn">${f.atText ? `This gauge last reported ${esc(f.atText)}.` : "The live feed has no reading for this gauge."} ` +
      "It may be down, seasonal, or late reporting.</p>";
  }
  const at = f.at ? Date.parse(f.at) : NaN;
  const stale = !Number.isFinite(at) || now - at > f.staleAfterMs;
  const what = f.parameter === "level" ? "water level" : "flow";
  return `<p class="reading"><b>${esc(f.value)}</b> ${what}</p>` +
    (f.standing ? `<p class="standing">${esc(f.standing)}</p>` : "") +
    `<p class="asof">Measured ${esc(f.atText ?? "at an unknown time")}${Number.isFinite(at) ? ` (${ago(now - at)})` : ""}</p>` +
    (stale ? `<p class="warn">Out of date: no new reading for ${Number.isFinite(at) ? ago(now - at).replace(" ago", "") : "an unknown time"}. ` +
             "The gauge may be down or late reporting; the reading above is its last.</p>" : "");
}

/** the forecast line under the chart */
export function forecastHtml(f: Facts, now: number): string {
  if (f.forecast) {
    const old = now > f.forecast.dueBy;
    return `<p class="small">Dashed: the ${esc(f.forecast.model)} flow forecast, issued ${esc(f.forecast.issued)}` +
      (old ? ` — <b>out of date</b> (no newer run since)` : "") + `.</p>`;
  }
  if (f.forecastOld) return `<p class="small">No current flow forecast: the latest ${esc(f.forecastOld.model)} run (issued ${esc(f.forecastOld.issued)}) is out of date, so it is not drawn.</p>`;
  return `<p class="small">No flow forecast for this gauge.</p>`;
}

/** three figures, no false precision — core conditions.ts `chartFigure` (a test holds the two equal) */
export function figure(v: number): string {
  const a = Math.abs(v);
  if (a >= 100) return String(Math.round(v));
  if (a >= 10) return String(Math.round(v * 10) / 10);
  return String(Math.round(v * 100) / 100);
}

const DAY = 86_400_000;
const dayLabel = (t: number) => new Date(t).toLocaleDateString("en-CA", { month: "short", day: "numeric", timeZone: "America/Vancouver" });

/** the last 7 days (and the forecast's dashes) as an SVG; null when there is nothing to draw */
export function chartSvg(f: Facts, now: number): string | null {
  const W = 640, H = 220, L = 52, R = 12, T = 12, B = 30;
  const obs = f.week, fc = f.forecast?.points ?? [];
  if (obs.length + fc.length < 2) return null;
  const t0 = now - 7 * DAY, t1 = Math.max(now, fc.length ? fc[fc.length - 1]![0] : now);
  const vals = [...obs, ...fc].map((p) => p[1]);
  let lo = Math.min(...vals), hi = Math.max(...vals);
  if (hi - lo < 1e-9) { lo -= 1; hi += 1; }
  const pad = (hi - lo) * 0.1; lo = Math.max(f.parameter === "discharge" ? 0 : -Infinity, lo - pad); hi += pad;
  const x = (t: number) => (L + ((t - t0) / (t1 - t0 || 1)) * (W - L - R)).toFixed(1);
  const y = (v: number) => (T + (1 - (v - lo) / (hi - lo)) * (H - T - B)).toFixed(1);
  const path = (ps: readonly [number, number][]) => ps.map((p, i) => `${i ? "L" : "M"}${x(p[0])} ${y(p[1])}`).join("");
  const last = obs[obs.length - 1];
  const parts = [
    `<line x1="${L}" y1="${H - B}" x2="${W - R}" y2="${H - B}" class="axis"/>`,
    `<text x="${L - 6}" y="${y(hi)}" class="tick" text-anchor="end" dominant-baseline="hanging">${figure(hi)}</text>`,
    `<text x="${L - 6}" y="${y(lo)}" class="tick" text-anchor="end">${figure(lo)}</text>`,
    `<text x="${L - 6}" y="${T}" class="tick" text-anchor="end" dy="1.4em">${esc(f.unit ?? "")}</text>`,
    obs.length >= 2 ? `<path d="${path(obs)}L${x(last![0])} ${H - B}L${x(obs[0]![0])} ${H - B}Z" class="area"/>` : "",
    obs.length >= 2 ? `<path d="${path(obs)}" class="obs"/>` : "",
    fc.length ? `<path d="${path(last ? [last, ...fc] : fc)}" class="fc"/>` : "",
    last ? `<circle cx="${x(last[0])}" cy="${y(last[1])}" r="4" class="dot"/>` : "",
    fc.length ? `<line x1="${x(now)}" y1="${T}" x2="${x(now)}" y2="${H - B}" class="now"/>` : "",
    `<text x="${L}" y="${H - 8}" class="tick">${esc(dayLabel(t0))}</text>`,
    `<text x="${x(now)}" y="${H - 8}" class="tick" text-anchor="${fc.length ? "middle" : "end"}">now</text>`,
    fc.length ? `<text x="${W - R}" y="${H - 8}" class="tick" text-anchor="end">${esc(dayLabel(t1))}</text>` : "",
  ];
  return `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(`${f.parameter === "level" ? "Water level" : "Flow"} over the last 7 days${fc.length ? " and the forecast" : ""}`)}">${parts.join("")}</svg>`;
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
  const now = c ? `<p class="reading"><b>${n(c.temperature_2m, "°C")}</b> ${esc(word(c.weather_code))}</p>` +
    `<p class="small">Wind ${n(c.wind_speed_10m, " km/h")} · precipitation ${n(c.precipitation, " mm", 1)}${c.time ? ` · as of ${esc(c.time.replace("T", " "))} Pacific` : ""}</p>` : "";
  const rows = (d?.time ?? []).map((day, i) => {
    const wd = new Date(`${day}T12:00:00-08:00`).toLocaleDateString("en-CA", { weekday: "short", month: "short", day: "numeric", timeZone: "America/Vancouver" });
    return `<tr><th scope="row">${esc(wd)}</th><td>${esc(word(d!.weather_code?.[i]))}</td><td>${n(d!.temperature_2m_max?.[i], "°")} / ${n(d!.temperature_2m_min?.[i], "°")}</td>` +
      `<td>${n(d!.precipitation_sum?.[i], " mm", 1)}</td><td>${n(d!.wind_speed_10m_max?.[i], " km/h")}</td></tr>`;
  }).join("");
  return now + (rows ? `<table class="wx"><thead><tr><th>Day</th><th>Sky</th><th>High / low</th><th>Rain</th><th>Wind</th></tr></thead><tbody>${rows}</tbody></table>` : "") +
    `<p class="small">${esc(attribution)}, fetched ${esc(new Date(fetchedAt).toLocaleTimeString("en-CA", { hour: "numeric", minute: "2-digit", timeZone: "America/Vancouver", timeZoneName: "short" }))}.</p>`;
}

/** the reader's own line (offline, dropped): shown, never reported — core failure.ts `isEnvironment`, the same words */
export function isEnvironment(e: unknown): boolean {
  const x = e as { name?: unknown; message?: unknown } | null;
  if (!x || typeof x !== "object") return false;
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
  const install = Array.from({ length: 16 }, () => "0123456789abcdef"[Math.floor(Math.random() * 16)]).join("");
  const report = (e: unknown) => {
    if (!cfg.crash || isEnvironment(e)) return;
    void fetchImpl(cfg.crash, { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(crashReport(e, cfg, new Date(now()), install)) }).catch(() => { /* the report itself is best effort */ });
  };
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
    const svg = chartSvg(f, t);
    put("gauge-chart", (svg ?? `<p class="small">No readings in the last 7 days.</p>`) + forecastHtml(f, t) +
      `<p class="small">Readings: Water Survey of Canada real-time data (Environment and Climate Change Canada), provisional. ` +
      `Read by our feed ${esc(ago(t - Date.parse(f.drawnAt)))}.${f.forecast ? ` ${esc(f.forecast.attribution)}` : ""}</p>`);
  }).catch((e) => { failed("gauge-now", "The latest reading", e); put("gauge-chart", ""); });
  const weather = cfg.weather === null
    ? Promise.resolve(put("gauge-weather", `<p class="small">This gauge has no position on file, so no weather is shown.</p>`))
    : readJson(fetchImpl, cfg.weather).then((m) => put("gauge-weather", weatherHtml(m as Meteo, cfg.sky, cfg.weatherAttribution, now())))
        .catch((e) => failed("gauge-weather", "The weather forecast", e));
  await Promise.all([facts, weather]);
}
