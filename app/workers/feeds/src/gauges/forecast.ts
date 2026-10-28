// T225 port of pipeline/gauges/feed/forecast.py (the spec): the BC River Forecast Centre's model runs — the summary
// layer (three ArcGIS queries) and the per-station series CSVs fetched only for runs whose issue time moved.
import type { Fetch } from "../io";
import { stale } from "./change";
import { pool, pyFloat, pyRound, pyStr, pySum, splitlines, urlencode } from "./py";
import { instant, isoSeconds, zoned } from "./time";

export type Run = Record<string, any>;
export type Runs = Record<string, Run>;
export type Summary = Record<string, Runs>;

const ARCGIS = "https://services6.arcgis.com/ubm4tcTYICKBpist/arcgis/rest/services";
const UA = "BC-freshwater-fishing-regulations/1.0 (+https://canifishthis.ca)";

export const ATTRIBUTION =
  "Forecast data provided by the BC River Forecast Centre, " +
  "Province of British Columbia, and used under the Province's " +
  "copyright terms (https://www2.gov.bc.ca/gov/content/home/copyright). " +
  "Users should use the information on this website with caution and " +
  "at their own risk.";

interface ModelCfg {
  service: string;
  horizon_days: number;
  rep: "min" | "ave" | "max";
  obs: string;
  issued: string;
  fmin?: string;
  fave?: string;
  fmax?: string;
  summary_from_series?: boolean;
  csv: string;
  shape: "clever" | "elf";
  step: "1h" | "1d";
}

export const MODELS: Record<string, ModelCfg> = {
  CLEVER: {
    service: "CLM_MapHub_forecast", horizon_days: 10, rep: "max",
    obs: "Latest_Reading", issued: "Issued_at",
    fmax: "Forecast_maximum_in_5_days",
    summary_from_series: true,
    csv: "https://bcrfc.env.gov.bc.ca/freshet/clever/{s}.CSV",
    shape: "clever", step: "1h",
  },
  COFFEE: {
    service: "coffee_MapHub_forecast", horizon_days: 5, rep: "max",
    obs: "Latest_Reading", issued: "Issued_at",
    fmin: "Forecast_minimum_in_5_days",
    fave: "Forecast_average_in_5_days",
    fmax: "Forecast_maximum_in_5_days",
    csv: "https://bcrfc.env.gov.bc.ca/fallfloods/coffee/COFFEE_{s}.CSV",
    shape: "clever", step: "1d",
  },
  ELF: {
    service: "MapHub_ELF_Forecast", horizon_days: 30, rep: "min",
    obs: "Qobs_m3_s_", issued: "Issued_at",
    fmin: "Qfor_MIN_30_Days_m3_s_",
    fave: "Qfor_AVE_30_Days_m3_s_",
    fmax: "Qfor_MAX_30_Days_m3_s_",
    csv: "https://bcrfc.env.gov.bc.ca/lowflow/elf/csv/{s}_elf_forecast.csv",
    shape: "elf", step: "1d",
  },
};

export const CSV_DISCLAIMER = "USERS OF THIS DATA MUST ACCEPT ALL RESPONSIBILITY FOR THE USE AND INTERPRETATION.";

/** forecast._num: the first number in the cell. */
export function num(v: unknown): number | null {
  if (v === null || v === undefined) return null;
  const m = /-?\d+(?:\.\d+)?/.exec(pyStr(v));
  return m ? Number(m[0]) : null;
}

/** forecast._issued */
export function issued(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  if (typeof v === "number") {
    const ms = Math.floor(v / 1000) * 1000; // fromtimestamp(v/1000).isoformat(timespec="seconds")
    return isoSeconds({ ms, off: 0 });
  }
  const text = String(v).trim();
  if (!text) return null;
  let m = /(\d{1,2}):(\d{2})\s*([AP]M).*?(\d{4}-\d{2}-\d{2})/i.exec(text);
  if (m) {
    const hour = (Number(m[1]) % 12) + (m[3]!.toUpperCase() === "PM" ? 12 : 0);
    return `${m[4]}T${String(hour).padStart(2, "0")}:${m[2]}:00`;
  }
  m = /\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2})?)?/.exec(text);
  return m ? m[0].replace(/ /g, "T") : text;
}

async function query(fetch: Fetch, service: string): Promise<Array<Record<string, unknown>>> {
  const url = `${ARCGIS}/${service}/FeatureServer/0/query`;
  const out: Array<Record<string, unknown>> = [];
  let offset = 0;
  const page = 1000;
  for (;;) {
    const q = urlencode([
      ["where", "1=1"], ["outFields", "*"], ["returnGeometry", "false"],
      ["f", "json"], ["resultOffset", offset], ["resultRecordCount", page],
    ]);
    const r = await fetch(`${url}?${q}`, { headers: { "User-Agent": UA }, signal: timeout(30_000) });
    if (!r.ok) throw new Error(`HTTP Error ${r.status}`);
    const data = JSON.parse(await r.text());
    if (data === null || typeof data !== "object" || Array.isArray(data)) throw new Error("not an object");
    const feats: any[] = data.features ?? [];
    for (const f of feats) out.push(f.attributes === undefined ? {} : f.attributes);
    if (feats.length < page || !data.exceededTransferLimit) return out;
    offset += page;
  }
}

export function timeout(ms: number): AbortSignal | undefined {
  return typeof AbortSignal !== "undefined" && "timeout" in AbortSignal ? AbortSignal.timeout(ms) : undefined;
}

/** forecast.fetch: `{station: {model: run}}`, every model running. A model that fails is skipped. */
export async function fetchSummary(fetch: Fetch, log: (s: string) => void, models?: string[]): Promise<Summary> {
  const out: Summary = {};
  for (const name of models ?? Object.keys(MODELS)) {
    const cfg = MODELS[name]!;
    let rows: Array<Record<string, unknown>>;
    try {
      rows = await query(fetch, cfg.service);
    } catch (e) {
      log(`  ${name}: unavailable (${String(e)})`);
      continue;
    }
    let kept = 0;
    for (const a of rows) {
      const station = pyStr(truthyOr(a.Station_ID, "")).trim();
      if (!station) continue;
      const lo = cfg.fmin ? num(a[cfg.fmin]) : null;
      const av = cfg.fave ? num(a[cfg.fave]) : null;
      const hi = cfg.fmax ? num(a[cfg.fmax]) : null;
      let rep = { min: lo, ave: av, max: hi }[cfg.rep];
      rep = rep !== null ? rep : hi !== null ? hi : av !== null ? av : lo;
      if (rep === null) continue;
      const row: Run = {
        model: name,
        issuedAt: issued(a[cfg.issued]),
        horizonDays: cfg.horizon_days,
        value: rep, extreme: cfg.rep,
        min: lo, ave: av, max: hi,
        observed: num(a[cfg.obs]),
        unit: "m3/s",
      };
      (out[station] ??= {})[name] = row;
      kept += 1;
    }
    log(`  ${name.padEnd(7)} ${String(kept).padStart(4)} station forecasts`);
  }
  return out;
}

function truthyOr(v: unknown, d: unknown): unknown {
  return v === null || v === undefined || v === "" || v === 0 || v === false ? d : v;
}

/** forecast._csv_rows */
function csvRowsOf(text: string): [string[], string[][]] {
  const lines = splitlines(text).filter((ln) => ln.trim());
  for (let i = 0; i < lines.length; i++) {
    const ln = lines[i]!;
    if (ln.toUpperCase().startsWith("DATE")) {
      const head = ln.split(",").map((c) => c.trim().toUpperCase());
      return [head, lines.slice(i + 1).map((r) => r.split(","))];
    }
  }
  return [[], []];
}

/** forecast.parse_series */
export function parseSeries(text: string, model: string): Record<string, any> | null {
  const cfg = MODELS[model];
  if (!cfg) return null;
  const [head, rows] = csvRowsOf(text);
  if (!head.length || !rows.length) return null;
  const ix = new Map<string, number>();
  head.forEach((name, i) => ix.set(name, i));
  const col = (row: string[], name: string): number | null => {
    const i = ix.get(name);
    if (i === undefined || i >= row.length) return null;
    const v = row[i]!;
    return v === "" ? null : pyFloat(v);
  };
  const out: Record<string, any[]> = {
    at: [], mid: [], lo: [], hi: [], level_mid: [], level_lo: [], level_hi: [], observed: [], level_observed: [],
  };
  for (const row of rows) {
    if (!row.length || !row[0]!.trim()) continue;
    const when = row[0]!.trim();
    if (cfg.shape === "clever") {
      const hi = ix.get("HOUR");
      const hour = hi !== undefined && row.length > hi ? row[hi]!.trim() : "00";
      out.at!.push(zoned(`${when}T${hour.padStart(2, "0")}:00:00`));
      out.mid!.push(col(row, "FORECAST_DISCHARGE"));
      out.lo!.push(col(row, "LOWER_BOUND"));
      out.hi!.push(col(row, "UPPER_BOUND"));
      out.observed!.push(null);
      out.level_mid!.push(null);
      out.level_lo!.push(null);
      out.level_hi!.push(null);
      out.level_observed!.push(null);
    } else {
      out.at!.push(when);
      out.mid!.push(col(row, "QFOR_AVE"));
      out.lo!.push(col(row, "QFOR_MIN"));
      out.hi!.push(col(row, "QFOR_MAX"));
      out.observed!.push(col(row, "QOBS"));
      out.level_mid!.push(col(row, "HFOR_AVE"));
      out.level_lo!.push(col(row, "HFOR_MIN"));
      out.level_hi!.push(col(row, "HFOR_MAX"));
      out.level_observed!.push(col(row, "HOBS"));
    }
  }
  if (!out.at!.length) return null;
  if (!out.mid!.concat(out.level_mid!).some((v) => v !== null)) return null;
  const res: Record<string, any> = out;
  res.step = cfg.step;
  res.disclaimer = CSV_DISCLAIMER;
  return res;
}

/** forecast.series: one station's CSV for one model, or null (404 = not in season). */
export async function fetchSeries(fetch: Fetch, station: string, model: string): Promise<Record<string, any> | null> {
  const cfg = MODELS[model];
  if (!cfg) return null;
  let text: string;
  try {
    const r = await fetch(cfg.csv.replace("{s}", station), { headers: { "User-Agent": UA }, signal: timeout(25_000) });
    if (!r.ok) return null;
    text = await r.text();
  } catch {
    return null;
  }
  return parseSeries(text, model);
}

/** forecast.ahead_of_issue */
export function aheadOfIssue(run: Run): number[] {
  const series = run.series ?? {};
  const iss = instant(run.issuedAt);
  const out: number[] = [];
  const ats: unknown[] = series.at || [];
  const mids: Array<number | null> = series.mid || [];
  for (let i = 0; i < Math.min(ats.length, mids.length); i++) {
    const v = mids[i];
    if (v === null || v === undefined) continue;
    const t = instant(ats[i]);
    if (iss !== null && (t === null || t.ms < iss.ms)) continue;
    out.push(v);
  }
  return out;
}

/** forecast.summary_from_series */
export function summaryFromSeries(run: Run): Run {
  const cfg = MODELS[String(run.model || "")];
  const vals = cfg?.summary_from_series ? aheadOfIssue(run) : [];
  if (!vals.length) return run;
  let lo = vals[0]!;
  let hi = vals[0]!;
  for (const v of vals) {
    if (v < lo) lo = v;
    if (v > hi) hi = v;
  }
  const av = pyRound(pySum(vals) / vals.length, 4);
  return { ...run, min: lo, ave: av, max: hi, value: { min: lo, ave: av, max: hi }[cfg!.rep] };
}

/** forecast.settle */
export function settle(run: Run): Run {
  const series = run.series;
  if (series && series.at && series.at.length) run = { ...run, series: { ...series, at: series.at.map((a: unknown) => zoned(String(a))) } };
  return summaryFromSeries(run);
}

export const SERIES_REQUEST_CAP = 1200;

/** forecast.plan_series: `[asked, carried, stale]`. Attaches carried series to `summary` in place. */
export function planSeries(summary: Summary, stations: Set<string> | null, keep: Record<string, Runs> | null, now: number | null): [Array<[string, string]>, number, number] {
  const want: Array<[string, string]> = [];
  let carried = 0;
  let old = 0;
  for (const st of Object.keys(summary).sort()) {
    if (stations !== null && !stations.has(st)) continue;
    const runs = summary[st]!;
    for (const model of Object.keys(runs).sort()) {
      const row = runs[model]!;
      const prev = (keep?.[st] || {})[model] || {};
      if (prev.series && (prev.issuedAt || "") === (row.issuedAt || "")) {
        row.series = prev.series;
        carried += 1;
      } else if (now !== null && stale(row, now)) old += 1;
      else want.push([st, model]);
    }
  }
  return [want.slice(0, SERIES_REQUEST_CAP), carried, old];
}

/** forecast.with_series */
export async function withSeries(fetch: Fetch, summary: Summary, stations: Set<string> | null, keep: Record<string, Runs> | null, now: number | null, log: (s: string) => void, workers = 8): Promise<Summary> {
  const [want, carried, old] = planSeries(summary, stations, keep, now);
  let total = 0;
  for (const r of Object.values(summary)) total += Object.keys(r).length;
  let n = 0;
  if (want.length) {
    const sorted = [...want].sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0));
    const got = await pool(sorted, workers, async ([st, model]) => [st, model, await fetchSeries(fetch, st, model)] as const);
    for (const [st, model, ser] of got) {
      if (ser) {
        summary[st]![model]!.series = ser;
        n += 1;
      }
    }
  }
  for (const runs of Object.values(summary)) {
    for (const model of Object.keys(runs)) {
      if (runs[model]!.series) runs[model] = settle(runs[model]!);
    }
  }
  if (!want.length) log(`  series   0 requests, 0 fetched (${carried} carried forward, ${old} stale skipped, ${total} runs)`);
  else log(`  series   ${want.length} requests, ${n} fetched, ${carried} carried forward, ${old} stale skipped (${total} runs)`);
  return summary;
}
