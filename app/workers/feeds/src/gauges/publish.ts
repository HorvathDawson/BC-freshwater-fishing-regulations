// T225 port of pipeline/gauges/feed/publish.py (the spec): `publish()` and `main()`'s orchestration as one Worker-safe
// job, `runGauges`. Network only through `deps.fetch`, storage only through two Stores (no node: imports):
//   feed  — the PUBLISHED prefix (feeds/live/): index.json, <station>.json, clim.json (input, written by the yearly
//           HYDAT tier in Python)
//   priv  — never served: runs/ (append-only, see skill.ts), temp_history.json, notified.json (`<out>.notified.json`),
//           roster.json (the realtime roster, when `deps.roster` is not passed)
import { contentHash, type Fetch, type Meter, type Store } from "../io";
import { sendDue } from "../notify";
import { combineDay, forecastDays, gaugeScale, modelName, stale, stationChange, type Rank } from "./change";
import { ATTRIBUTION, fetchSummary, timeout, withSeries, type Runs, type Summary } from "./forecast";
import { listedStations, rosterDrift, STATION_LIST, stationsToAsk, type RosterRow } from "./liveness";
import { dictRows, pool, pyFloat, pyRound, pyRoundInt, pySum, csvRows, truthy, urlencode } from "./py";
import { appendRuns, type DeletableStore } from "./skill";
import { addDays, centreWall, DAY_MS, dayMs, dayOf, HOUR, hours, instant, wallIso, yday } from "./time";

const BASE = "https://dd.weather.gc.ca/today/hydrometric/csv/BC";
const UA = "BC-FishRegs-Hydro/1.0";
export const WORKERS = 8;
export const ARCHIVE_WORKERS = 4;
const LEVEL = "Water Level / Niveau d'eau (m)";
const FLOW = "Discharge / Débit (cms)";
const WATEROFFICE = "https://wateroffice.ec.gc.ca/services/real_time_data/csv/inline";
const TEMP_PARAM = "5";
const TEMP_BATCH = 80;
const TEMP_TRIES = 4;
const TEMP_BACKOFF_S = 5.0;
const SENTINEL = 99_000.0;
export const PCTILES = [10, 25, 50, 75, 90]; // climatology.PCTILES
export const HORIZONS = [1, 3, 5, 10];
export const FORECAST_TOLERANCE_H = 18.0;
export const TEMP_SAMPLES_PER_PENTAD = 400;
export const ARCHIVE_DAYS = 29;
const DAILY = (st: string) => `${BASE}/daily/BC_${st}_daily_hydrometric.csv`;
const HYDAT_DIR = "https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/";
const HYDAT_UA = "BC-FishRegs-DataFetch/1.0";

export const HYDAT_ACTION = "scripts/update-hydat.sh";
export const ROSTER_PATH = "data/source/bc_hydrometric_stations.json";
export const ROSTER_ACTION =
  `rm ${ROSTER_PATH} && ` +
  "(cd data && PYTHONPATH=.. ../.venv/bin/python fetch_data.py --layers hydrometric_stations) && " +
  "scripts/heavy.sh build 40 -- .venv/bin/python -m pipeline build";

type Num = number | null;
type Row = [string, Num, Num];
interface Reading {
  station: string;
  at: string;
  level: Num;
  discharge: Num;
  recent: Row[];
}

const num = (v: string | null | undefined): Num => (v === null || v === undefined || v === "" ? null : pyFloat(v));

async function get(fetch: Fetch, url: string, ua = UA, ms = 30_000): Promise<string | null> {
  try {
    const r = await fetch(url, { headers: { "User-Agent": ua }, signal: timeout(ms) });
    if (!r.ok) return null;
    return await r.text();
  } catch {
    return null; // a station that did not answer is simply absent
  }
}

/** publish._parse_hourly */
export function parseHourly(station: string, text: string): Reading | null {
  const rows = dictRows(text).filter((r) => truthy(r.Date));
  if (!rows.length) return null;
  const series: Row[] = rows.map((r) => [r.Date!, num(r[LEVEL]), num(r[FLOW])]);
  // Python sorts the tuples; a duplicate stamp then compares the readings (and raises on None vs a number — that
  // crashes Python's whole run; here a null sorts first instead, see the parity report)
  series.sort((a, b) => cmpStr(a[0], b[0]) || cmpNum(a[1], b[1]) || cmpNum(a[2], b[2]));
  let last: Row | null = null;
  for (let i = series.length - 1; i >= 0; i--) {
    const s = series[i]!;
    if (s[1] !== null || s[2] !== null) {
      last = s;
      break;
    }
  }
  if (last === null) return null;
  return {
    station, at: last[0], level: last[1], discharge: last[2],
    recent: series.filter((_, i) => i % 6 === 0).map(([t, lv, q]) => [t, lv, q]),
  };
}

const cmpStr = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);
const cmpNum = (a: Num, b: Num) => (a === b ? 0 : a === null ? -1 : b === null ? 1 : a < b ? -1 : a > b ? 1 : 0);

/** publish.roster_action */
export function rosterAction(roster: string): string {
  if (roster === ROSTER_PATH) return ROSTER_ACTION;
  const i = ROSTER_ACTION.lastIndexOf(" && ");
  return `${ROSTER_ACTION.slice(0, i)} && cp ${ROSTER_PATH} ${roster} && ${ROSTER_ACTION.slice(i + 4)}`;
}

/** publish.newest_forecast_issue */
export function newestForecastIssue(forecast: Summary | null): string | null {
  let best: [number, string] | null = null;
  for (const runs of Object.values(forecast || {})) {
    for (const run of Object.values(runs || {})) {
      const s = (run || {}).issuedAt;
      const t = typeof s === "string" ? instant(s) : null;
      if (t !== null && (best === null || t.ms > best[0])) best = [t.ms, s];
    }
  }
  return best ? best[1] : null;
}

export interface Notice {
  kind: string;
  since: string;
  current: string;
  latest: string;
  action: string;
}

/** publish.maintenance_notices */
export function maintenanceNotices(o: { release: string | null; latest: string | null; drift: { joined: string[]; left: string[] } | null; previous: unknown; at: string; roster?: string }): Notice[] {
  const roster = o.roster ?? ROSTER_PATH;
  const prev = new Map<unknown, Record<string, any>>();
  if (Array.isArray(o.previous)) for (const n of o.previous) if (n && typeof n === "object" && !Array.isArray(n)) prev.set((n as any).kind ?? null, n);
  const notice = (kind: string, current: string, newest: string, action: string): Notice => {
    const p = prev.get(kind);
    const since = p && p.latest === newest && truthy(p.since) ? p.since : o.at;
    return { kind, since, current, latest: newest, action };
  };
  const out: Notice[] = [];
  if (o.release) {
    if (o.latest) {
      if (o.latest !== o.release) out.push(notice("hydat", o.release, o.latest, HYDAT_ACTION));
    } else {
      const p = prev.get("hydat");
      if (p && truthy(p.latest) && p.latest !== o.release) out.push(notice("hydat", o.release, p.latest, HYDAT_ACTION));
    }
  }
  if (o.drift !== null) {
    if (o.drift.joined.length || o.drift.left.length) {
      const said = [...o.drift.joined.map((s) => `+${s}`), ...o.drift.left.map((s) => `-${s}`)].join(" ");
      out.push(notice("roster", roster, said, rosterAction(roster)));
    }
  } else {
    const p = prev.get("roster");
    if (p && truthy(p.latest)) out.push(notice("roster", roster, p.latest, rosterAction(roster)));
  }
  return out.sort((a, b) => cmpStr(a.kind, b.kind));
}

/** publish.shown_parameter */
export function shownParameter(envelope: Record<string, unknown>, discharge: Num, level: Num): string {
  if ("discharge" in envelope && discharge !== null) return "discharge";
  if ("level" in envelope && level !== null) return "level";
  return discharge === null && level !== null ? "level" : "discharge";
}

/** publish.percentile_of */
export function percentileOf(value: Num | undefined, band: Array<number | null> | null | undefined): Num {
  if (value === null || value === undefined || !band || band.length < 5 || band.some((b) => b === null)) return null;
  const ps = PCTILES.map((p) => p / 100);
  const vs = band as number[];
  if (value <= vs[0]!) return 0.05;
  if (value >= vs[vs.length - 1]!) return 0.95;
  for (let i = 0; i < vs.length - 1; i++) {
    const lo = vs[i]!;
    const hi = vs[i + 1]!;
    if (lo <= value && value <= hi) {
      const span = hi - lo;
      const f = span === 0 ? 0.0 : (value - lo) / span;
      return ps[i]! + f * (ps[i + 1]! - ps[i]!);
    }
  }
  return null;
}

/** publish.band_for: `when` epoch ms (its UTC day of year). */
export function bandFor(bands: Record<string, Array<number | null>> | null | undefined, when: number): Array<number | null> | null {
  if (!bands || !Object.keys(bands).length) return null;
  const pos = (yday(when) - 1) / 5.0 - 0.5;
  const lo = Math.floor(pos);
  const frac = pos - lo;
  const a = bands[String(((lo % 73) + 73) % 73)];
  const b = bands[String((((lo + 1) % 73) + 73) % 73)];
  if (a === undefined || a === null || b === undefined || b === null) return truthy(a) ? a! : (b ?? null);
  if (a.length !== b.length) return a;
  return a.map((x, i) => {
    const y = b[i]!;
    return x === null || y === null ? null : x + (y - x) * frac;
  });
}

export function pentadOf(when: number): number {
  return Math.min(72, Math.floor((yday(when) - 1) / 5));
}

/** publish.forecast_at */
export function forecastAt(series: Record<string, any> | null | undefined, when: number, quantity: string): Num {
  if (!series || !Object.keys(series).length) return null;
  const key = quantity === "level" ? "level_mid" : "mid";
  const vals: Num[] = series[key] || [];
  const ats: unknown[] = series.at || [];
  const times = ats.map((a) => instant(a)).filter((t) => t !== null).map((t) => t!.ms).sort((a, b) => a - b);
  const gaps: number[] = [];
  for (let i = 1; i < times.length; i++) {
    const g = hours(times[i]!, times[i - 1]!);
    if (g > 0) gaps.push(g);
  }
  gaps.sort((a, b) => a - b);
  const tol = gaps.length ? Math.min(FORECAST_TOLERANCE_H, gaps[Math.floor(gaps.length / 2)]! / 2.0) : FORECAST_TOLERANCE_H;
  const day = dayOf(centreWall(when));
  let best: [number, number] | null = null;
  const n = Math.min(ats.length, vals.length);
  for (let i = 0; i < n; i++) {
    const at = ats[i];
    const v = vals[i];
    if (v === null || v === undefined) continue;
    const named = stepDayOf(series, at);
    if (named !== null) {
      if (named === day) return v;
      continue;
    }
    const t = instant(at);
    if (t === null) continue;
    const off = Math.abs(hours(t.ms, when));
    if (off <= tol && (best === null || off < best[0])) best = [off, v];
  }
  return best === null ? null : best[1];
}

function stepDayOf(series: Record<string, any>, at: unknown): string | null {
  const day = typeof at === "string" && /^\d{4}-\d{2}-\d{2}$/.test(at.trim());
  if (!(day || series.step === "1d")) return null;
  const t = instant(at);
  return t === null ? null : dayOf(centreWall(t.ms));
}

/** publish.forecast_percentiles */
export function forecastPercentiles(runs: Runs | null | undefined, envelopes: Record<string, any> | null | undefined, now: number, observed: Record<string, Num> | null): Record<string, any> {
  if (!runs || !Object.keys(runs).length || !envelopes || !Object.keys(envelopes).length) return {};
  const out: Record<string, any> = {};
  for (const days of HORIZONS) {
    const when = now + days * DAY_MS;
    const got: Record<string, number> = {};
    const by: Record<string, string> = {};
    for (const q of ["discharge", "level"]) {
      const values: Record<string, Num> = {};
      for (const model of Object.keys(runs).sort()) {
        const run = runs[model];
        const series = (run || {}).series;
        if (!series || !Object.keys(series).length || stale(run, now)) continue;
        const s = gaugeScale(model, run!, now, (observed || {})[q] ?? null, q);
        const v = forecastAt(series, when, q);
        values[model] = v === null || s === null ? null : v * s;
      }
      const pick = combineDay(values);
      const p = pick !== null ? percentileOf(pick[1], bandFor(envelopes[q], when)) : null;
      if (pick !== null && p !== null) {
        by[q] = modelName(pick[0]);
        got[q] = p;
      }
    }
    if (Object.keys(got).length) {
      const model = by.discharge || by.level!;
      out[String(days)] = { ...got, model, ...((by.level ?? model) !== model ? { levelModel: by.level } : {}) };
    }
  }
  return out;
}

/** publish.fetch_temperatures: station -> [stamp, degrees]; insertion order as Python's dict. */
async function fetchTemperatures(fetch: Fetch, stations: string[], now: number, sleep: (ms: number) => Promise<void>, hoursBack = 24): Promise<Map<string, [string, number]>> {
  const fmt = (ms: number) => wallIso(ms).replace("T", " ");
  const start = fmt(now - hoursBack * HOUR);
  const end = fmt(now);
  const out = new Map<string, [string, number]>();
  for (let i = 0; i < stations.length; i += TEMP_BATCH) {
    const batch = stations.slice(i, i + TEMP_BATCH);
    const q = urlencode([
      ...batch.map((s) => ["stations[]", s] as [string, string]),
      ["parameters[]", TEMP_PARAM], ["start_date", start], ["end_date", end],
    ]);
    let text: string | null = null;
    for (let k = 0; k < TEMP_TRIES; k++) {
      if (k) await sleep(TEMP_BACKOFF_S * 2 ** (k - 1) * 1000);
      text = await get(fetch, `${WATEROFFICE}?${q}`);
      if (text) break;
    }
    if (!text) continue;
    for (const row of csvRows(text)) {
      if (row.length < 4 || row[2]!.trim() !== TEMP_PARAM) continue;
      const v = num(row[3]);
      if (v === null || v >= SENTINEL) continue;
      const stn = row[0]!.trim();
      const when = row[1]!.trim();
      if (stn) out.set(stn, [when, v]);
    }
  }
  return out;
}

const TEMP_BANDS: Array<[number, string]> = [[18.0, "cool"], [20.0, "warm"], [Infinity, "critical"]];
export function temperatureBand(c: number): string {
  for (const [ceiling, name] of TEMP_BANDS) if (c < ceiling) return name;
  return "critical";
}

/** publish.temperature_envelope */
export function temperatureEnvelope(hist: Record<string, Record<string, number[]>>, minObs = 10): Record<string, Record<string, number[]>> {
  const out: Record<string, Record<string, number[]>> = {};
  for (const [station, buckets] of Object.entries(hist)) {
    for (const [pentad, vals] of Object.entries(buckets)) {
      if (vals.length < minObs) continue;
      const xs = [...vals].sort((a, b) => a - b);
      const q = (p: number) => xs[Math.min(xs.length - 1, Math.max(0, pyRoundInt(p * (xs.length - 1))))]!;
      (out[station] ??= {})[pentad] = [q(0.1), q(0.25), q(0.5), q(0.75), q(0.9)];
    }
  }
  return out;
}

function recordStart(today: string): string {
  const y = Number(today.slice(0, 4));
  const m = Number(today.slice(5, 7));
  return m === 1 ? `${String(y - 1).padStart(4, "0")}-12-01` : `${today.slice(0, 4)}-01-01`;
}

/** publish.daily_gaps: [inside, beyond] */
export function dailyGaps(rows: unknown[][], today: string): [string[], string[]] {
  const have = new Set<string>();
  for (const r of rows || []) if (r.length >= 3 && (r[1] !== null || r[2] !== null)) have.add(String(r[0]));
  const edge = addDays(today, -ARCHIVE_DAYS);
  const inside: string[] = [];
  const beyond: string[] = [];
  let d = recordStart(today);
  while (d < today) {
    if (!have.has(d)) (d >= edge ? inside : beyond).push(d);
    d = addDays(d, 1);
  }
  return [inside, beyond];
}

export function dayRuns(days: string[]): string[][] {
  const out: string[][] = [];
  for (const s of [...days].sort()) {
    const last = out[out.length - 1];
    if (last && addDays(last[1]!, 1) === s) last[1] = s;
    else out.push([s, s]);
  }
  return out;
}

/** publish._local_day: the reading's own day, else the fallback (now's UTC day). */
function localDay(at: string, fallback: string): string {
  const s = String(at).slice(0, 10);
  return dayMs(s) !== null ? s : fallback;
}

type Means = Map<string, [Num, Num]>;
function meansOf(rows: Iterable<[string, Num, Num]>): Means {
  const acc = new Map<string, [number[], number[]]>();
  for (const [date, lv, q] of rows) {
    const day = (date || "").slice(0, 10);
    if (!day) continue;
    let cell = acc.get(day);
    if (!cell) acc.set(day, (cell = [[], []]));
    if (lv !== null) cell[0].push(lv);
    if (q !== null) cell[1].push(q);
  }
  const out: Means = new Map();
  for (const [d, [a, b]] of acc) {
    out.set(d, [a.length ? pyRound(pySum(a) / a.length, 4) : null, b.length ? pyRound(pySum(b) / b.length, 4) : null]);
  }
  return out;
}

/** publish.daily_means over an ECCC CSV */
export function dailyMeans(text: string): Means {
  return meansOf(dictRows(text).map((r) => [r.Date || "", num(r[LEVEL]), num(r[FLOW])] as [string, Num, Num]));
}

/** publish.merge_daily */
export function mergeDaily(previous: unknown[][], fresh: Means, year: number, month = 12): unknown[][] {
  const keep = [String(year), ...(month === 1 ? [`${year - 1}-12`] : [])];
  const starts = (s: string) => keep.some((k) => s.startsWith(k));
  const have = new Map<string, unknown[]>();
  for (const row of previous || []) if (starts(String(row[0]))) have.set(String(row[0]), row);
  for (const [day, [lv, q]] of fresh) {
    if (!starts(day)) continue;
    have.set(day, [day, lv, q]);
  }
  return [...have.keys()].sort().map((d) => have.get(d)!);
}

export interface GaugeDeps {
  fetch: Fetch;
  /** the published prefix (feeds/live/): index.json, <station>.json, clim.json, temp_history.json */
  feed: Store;
  /** never served: runs/<YYYY-MM>.jsonl, notified.json, roster.json */
  priv: DeletableStore;
  now: Date;
  /** bc_hydrometric_stations.json rows (only `station` and `realtime` are read); else priv `roster.json` */
  roster?: RosterRow[] | null;
  /** the roster file as notices name it (default `data/source/bc_hydrometric_stations.json`) */
  rosterName?: string;
  notifyUrl?: string | null;
  limit?: number;
  noReleaseCheck?: boolean;
  noForecast?: boolean;
  log?: (s: string) => void;
  sleep?: (ms: number) => Promise<void>;
  /** how often the HYDAT listing is asked (change-aware; default a day) */
  hydatCheckMs?: number;
  /** skip parse/compute/writes for stations whose CSV is not modified (default true; the parity test passes false) */
  changeAware?: boolean;
  meter?: Meter;
}

export interface GaugeSummary {
  asked: number;
  answered: number;
  release: string | null;
  stale: boolean;
  latest: string | null;
  maintenance: Notice[];
  withPercentile: number;
  archive: { asked: number; filled: number };
  runArchive: Record<string, any>;
  gapsBeyondWindow: Record<string, string[][]>;
  listSource: "eccc" | "roster";
  forecastRuns: number;
  notify: { sent: string[]; failed: string[]; off: boolean };
  /** change-aware: no new forecast issuance, the last forecast output was reused */
  forecastReused?: boolean;
  /** asked stations with no usable answer this run (fetch failed / unparseable) */
  failed?: string[];
  /** of those, how many kept their last good index entry, marked `stale` */
  carried?: number;
  /** forecast models whose summary query failed (their last good runs were carried) */
  forecastFailed?: string[];
  /** no station answered: index.json was NOT rewritten (the last good one stands) */
  indexKept?: boolean;
}

const JSON_CT = { contentType: "application/json" };
/** a failed station's last entry is carried while its observation is younger than this (then it drops out) */
export const CARRY_MAX_MS = 3 * DAY_MS;
/** private (T225 follow-up; Python: in the published feed dir) */
export const TEMP_HISTORY = "temp_history.json";

/** `python -m pipeline.gauges.feed.publish` as one call: list, HYDAT check, forecasts, publish, archive, notify. */
export async function runGauges(deps: GaugeDeps): Promise<GaugeSummary> {
  const log = deps.log ?? (() => {});
  const sleep = deps.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const { fetch, feed, priv } = deps;
  const now = deps.now.getTime();
  const rosterName = deps.rosterName ?? ROSTER_PATH;

  let roster = deps.roster ?? null;
  if (!roster) {
    try {
      roster = JSON.parse((await priv.get("roster.json")) ?? "null");
    } catch {
      roster = null;
    }
  }
  const listed = listedStations(await get(fetch, STATION_LIST));
  let [ids, source] = stationsToAsk(roster ?? [], listed);
  const drift = roster ? rosterDrift(roster, listed) : null;
  if (!roster) log("  ⚠️  no roster (priv roster.json): no drift check this run");
  if (source === "roster") log(`  ⚠️  ECCC real-time station list unavailable; asking the roster's ${ids.length} realtime stations`);
  if (drift && (drift.joined.length || drift.left.length))
    log(`  ⚠️  ROSTER DRIFT: ECCC now lists ${drift.joined.length} station(s) the roster calls silent (${drift.joined.join(", ")}) and no longer lists ${drift.left.length} it calls live (${drift.left.join(", ")}).`);
  if (deps.limit) ids = ids.slice(0, deps.limit);

  let clim: Record<string, any> | null = null;
  const climText = await feed.get("clim.json");
  if (climText) {
    clim = JSON.parse(climText);
    // KEEP ONLY THE ASKED STATIONS' envelopes and recent years (438 of 1,958: ~21 MB parsed -> ~5 MB); `stats` is not
    // read by the feed at all (P20). The Worker has 128 MB.
    if (clim && typeof clim === "object" && !Array.isArray(clim)) {
      const slim = (o: unknown) => {
        if (!o || typeof o !== "object") return o;
        const out: Record<string, unknown> = {};
        for (const st of ids) if (st in (o as object)) out[st] = (o as Record<string, unknown>)[st];
        return out;
      };
      clim = { ...clim, stations: slim(clim.stations), recentYears: slim(clim.recentYears) };
      delete clim!.stats;
    }
  }

  let latest: string | null = null;
  // owner 2026-10-28: the HYDAT listing is asked once a DAY (change-aware), not every run — the last answer stands between
  let hydatCheck: { latest: string | null; at: number } | null = null;
  if ((deps.changeAware ?? true) && !deps.noReleaseCheck) {
    try {
      hydatCheck = JSON.parse((await priv.get(HYDAT_CHECK)) ?? "null");
    } catch {
      hydatCheck = null;
    }
  }
  if (hydatCheck && now - hydatCheck.at < (deps.hydatCheckMs ?? DAY_MS)) latest = hydatCheck.latest;
  else if (!deps.noReleaseCheck) {
    const html = await get(fetch, HYDAT_DIR, HYDAT_UA, 60_000);
    if (html) {
      const found = [...new Set([...html.matchAll(/Hydat_sqlite3_(\d{8})\.zip/g)].map((m) => m[1]!))].sort();
      latest = found.length ? found[found.length - 1]! : null;
      if (deps.changeAware ?? true) await priv.put(HYDAT_CHECK, JSON.stringify({ latest, at: now }));
    }
  }

  // EVERY STATION FILE IS READ ONCE (Python reads the forecast stations' twice) AND KEPT SLIM: only what the run reads
  // back — `daily` (the record it grows) and the forecast runs that still carry a series (what `withSeries` may carry
  // forward). The whole parsed files are ~48 MB for the province; the Worker has 128 MB.
  const aware = deps.changeAware ?? true;
  // FORECAST REUSE (owner 2026-10-28): the BC River Forecast Centre issues once a day (COFFEE ~09:00 PT, CLEVER in
  // batches 09:30–11:00 PT, ELF ~09:10 PT, not every day). A run whose three layers' `dataLastEditDate` are what the last
  // forecast run saw, on the same local day, reuses that run's forecast output (index ahead/aheadDays/change, the station
  // files' forecasts + change) and only updates the observations. Decided BEFORE the station files are read.
  let reuse = false;
  let sig: Record<string, number | null> | null = null;
  let fstate: ForecastState | null = null;
  if (aware && !deps.noForecast) {
    try {
      fstate = JSON.parse((await priv.get(FORECAST_STATE)) ?? "null");
    } catch {
      fstate = null;
    }
    sig = await forecastSignature(fetch);
    const today = dayOf(centreWall(now));
    reuse = !!(fstate && fstate.rev === GAUGE_REV && fstate.day === today && sig && sameSig(sig, fstate.sig));
  }
  const files = new Map<string, Promise<Prev | null>>();
  const stationFile = (st: string): Promise<Prev | null> => {
    let p = files.get(st);
    if (!p) {
      p = feed.get(`${st}.json`).then((t) => {
        if (t === null) return null;
        let f: any;
        try {
          f = JSON.parse(t);
        } catch {
          return { ok: false }; // unreadable: Python skips it (keep) / starts from nothing (prior)
        }
        if (!f || typeof f !== "object" || Array.isArray(f)) return { ok: false };
        if (reuse) return { ok: true, daily: f.daily, forecastsText: JSON.stringify(f.forecasts ?? {}), change: f.change };
        const keep: Runs = {};
        for (const [m, run] of Object.entries((f.forecasts || {}) as Runs)) if (truthy(run?.series)) keep[m] = run;
        return { ok: true, daily: f.daily, keep };
      });
      files.set(st, p);
    }
    return p;
  };

  let fcast: Summary = {};
  let forecastFailed: string[] = [];
  if (reuse) {
    fcast = fstate!.summary;
    log(`  forecast: no new issuance since ${fstate!.at} (layer edit dates unchanged) — last forecast output reused`);
  } else if (!deps.noForecast) {
    try {
      const failedModels: string[] = [];
      fcast = await fetchSummary(fetch, log, undefined, failedModels);
      if (failedModels.length) {
        forecastFailed = failedModels;
        // a model whose summary failed keeps its LAST GOOD runs (the last forecast state) instead of vanishing
        if (aware && fstate?.summary) {
          for (const [st, runs] of Object.entries(fstate.summary))
            for (const m of failedModels) if (runs[m]) ((fcast[st] ??= {})[m] = runs[m]!);
          log(`  forecast: ${failedModels.join(", ")} unavailable — their last good runs carried`);
        }
      }
      const have: Record<string, Runs> = {};
      const sts = Object.keys(fcast);
      const prevs = await pool(sts, WORKERS, (st) => stationFile(st));
      sts.forEach((st, i) => {
        const f = prevs[i];
        if (f && f.ok) have[st] = f.keep!;
      });
      fcast = await withSeries(fetch, fcast, new Set(ids), have, now, log);
      for (const f of prevs) if (f) delete f.keep; // carried series now live in `fcast`; the rest is garbage
      if (aware && sig && Object.keys(fcast).length && !forecastFailed.length) // a partial answer is not saved for reuse
        await priv.put(FORECAST_STATE, JSON.stringify({ rev: GAUGE_REV, sig, day: dayOf(centreWall(now)), at: wallIso(now) + "Z", summary: stripSeries(fcast) }));
    } catch (e) {
      log(`  forecast unavailable: ${String(e)}`);
    }
  }

  const summary = await publish({ fetch, feed, priv, ids, clim, forecast: fcast, latest, now, listed, drift, roster: rosterName, log, sleep, stationFile, aware, reuse, meter: deps.meter });
  const runs = Object.values(fcast).reduce((n, v) => n + Object.keys(v).length, 0);
  log(`  ✅ ${summary.answered}/${summary.asked} answered, ${summary.withPercentile} with a percentile; ${runs} model runs across ${Object.keys(fcast).length} stations`);
  for (const n of summary.maintenance) if (n.kind === "hydat") log(`  ⚠️  A NEWER HYDAT IS OUT: ${n.latest} (envelope is on ${n.current}, noticed ${n.since}). Refresh with: ${n.action}`);
  const notify = await sendDue(summary.maintenance as any, { url: deps.notifyUrl, fetch, state: priv, stateKey: "notified.json", log });
  return { ...summary, listSource: source, forecastRuns: runs, notify, forecastReused: reuse, ...(forecastFailed.length ? { forecastFailed } : {}) };
}

// ── forecast reuse ───────────────────────────────────────────────────────────────────────────────────────────────────
export const FORECAST_STATE = "forecast.state.json";
export const HYDAT_CHECK = "hydat.check.json";
interface ForecastState {
  rev: string;
  sig: Record<string, number | null>;
  /** the Centre-local day the output was computed on (a new day recomputes: aheadDays are dated) */
  day: string;
  at: string;
  summary: Summary;
}
const LAYERS: Record<string, string> = { CLEVER: "CLM_MapHub_forecast", COFFEE: "coffee_MapHub_forecast", ELF: "MapHub_ELF_Forecast" };
/** each forecast layer's `editingInfo.dataLastEditDate` (one small request per model); null when any is unreadable */
export async function forecastSignature(fetch: Fetch): Promise<Record<string, number | null> | null> {
  const out: Record<string, number | null> = {};
  for (const [m, svc] of Object.entries(LAYERS)) {
    const t = await get(fetch, `https://services6.arcgis.com/ubm4tcTYICKBpist/arcgis/rest/services/${svc}/FeatureServer/0?f=json`);
    if (!t) return null;
    try {
      const v = JSON.parse(t)?.editingInfo?.dataLastEditDate;
      if (typeof v !== "number") return null;
      out[m] = v;
    } catch {
      return null;
    }
  }
  return out;
}
const sameSig = (a: Record<string, number | null>, b: Record<string, number | null> | undefined) =>
  !!b && Object.keys(a).length === Object.keys(b).length && Object.keys(a).every((k) => a[k] === b[k]);
function stripSeries(f: Summary): Summary {
  const out: Summary = {};
  for (const [st, runs] of Object.entries(f)) {
    out[st] = {};
    for (const [m, run] of Object.entries(runs)) {
      const { series: _s, ...rest } = run;
      out[st]![m] = rest;
    }
  }
  return out;
}
/** a station's forecast runs as issued: what its stored output was computed from */
const fsigOf = (runs: Runs | undefined) => JSON.stringify(Object.keys(runs || {}).sort().map((m) => [m, runs![m]!.issuedAt ?? null]));

interface PublishArgs {
  fetch: Fetch;
  feed: Store;
  priv: DeletableStore;
  ids: string[];
  clim: Record<string, any> | null;
  forecast: Summary;
  latest: string | null;
  now: number;
  listed: Set<string> | null;
  drift: { joined: string[]; left: string[] } | null;
  roster: string;
  log: (s: string) => void;
  sleep: (ms: number) => Promise<void>;
  stationFile: (st: string) => Promise<Prev | null>;
  aware: boolean;
  reuse?: boolean;
  meter?: Meter;
}

// ── change-aware runs (owner 2026-10-28) ───────────────────────────────────────────────────────────────────────────
/** Bump when a code change alters a station's output: every station is recomputed once after a deploy. */
export const GAUGE_REV = "2026-10-28.1";
export const STATION_CACHE = "stations.cache.json";
interface Base {
  percentile: Num;
  parameter: string;
  per: Record<string, unknown>;
  observedAt: string;
  forecast: boolean;
  ahead?: unknown;
  aheadDays?: unknown;
  change?: unknown;
}
interface Cached {
  /** the station's forecast runs (model, issuedAt) its forecast fields were computed from */
  fsig?: string;
  /** the station's non-CSV inputs this run (day, pentad, clim release, forecast runs): its CSV is asked conditionally
   *  only when these are the same as last time */
  ctx: string;
  etag: string | null;
  lm: string | null;
  reading: { at: string; level: Num; discharge: Num };
  base: Base;
  newest: string | null;
  beyond?: string[][];
}
interface StationCache {
  rev: string;
  st: Record<string, Cached>;
}

/** The index entry, in Python's key order; the temperature parts are this run's (the CSV skip never skips them). */
function composeEntry(b: Base, tC: Num, tAt: string | null, band: string | null, pt: Num): Record<string, any> {
  return {
    percentile: b.percentile,
    parameter: b.parameter,
    ...b.per,
    ...(tC !== null ? { temperatureBand: band } : {}),
    ...(tC !== null && pt !== null ? { temperature: pt } : {}),
    observedAt: b.observedAt,
    ...(tC !== null ? { temperatureC: tC, temperatureAt: tAt } : {}),
    forecast: b.forecast,
    ...(b.ahead !== undefined ? { ahead: b.ahead } : {}),
    ...(b.aheadDays !== undefined ? { aheadDays: b.aheadDays } : {}),
    ...(b.change !== undefined ? { change: b.change } : {}),
  };
}

/** GET with validators: `same` when the server answers 304 Not Modified. */
async function getCond(fetch: Fetch, url: string, etag: string | null, lm: string | null): Promise<{ text: string | null; same: boolean; etag: string | null; lm: string | null }> {
  try {
    const headers: Record<string, string> = { "User-Agent": UA };
    if (etag) headers["If-None-Match"] = etag;
    if (lm) headers["If-Modified-Since"] = lm;
    const r = await fetch(url, { headers, signal: timeout(30_000) });
    if (r.status === 304) return { text: null, same: true, etag, lm };
    if (!r.ok) return { text: null, same: false, etag: null, lm: null };
    return { text: await r.text(), same: false, etag: r.headers.get("etag"), lm: r.headers.get("last-modified") };
  } catch {
    return { text: null, same: false, etag: null, lm: null };
  }
}

/** A previous station file, slimmed (see `stationFile`). */
interface Prev {
  ok: boolean;
  daily?: unknown;
  keep?: Runs;
  /** forecast reuse: the file's `forecasts` block as written, and its `change` */
  forecastsText?: string;
  change?: unknown;
}

/** publish.publish */
async function publish(a: PublishArgs): Promise<Omit<GaugeSummary, "listSource" | "forecastRuns" | "notify">> {
  const { fetch, feed, priv, clim, now, log } = a;
  const forecast = a.forecast || {};
  let before: unknown = null;
  let prevStations: Record<string, any> = {};
  try {
    const t = await feed.get("index.json");
    const j = t === null ? null : JSON.parse(t);
    before = j?.maintenance ?? null;
    if (j && j.stations && typeof j.stations === "object") prevStations = j.stations;
  } catch {
    before = null;
  }
  const envelope: Record<string, any> = (clim || {}).stations ?? {};
  const release: string | null = (clim || {}).release ?? null;
  const pent = pentadOf(now);
  const nowDay = dayOf(now);
  const fetchedAt = wallIso(now) + "Z";

  // CHANGE-AWARE: a station whose CSV is "not modified" and whose other inputs are the same as last run keeps its last
  // answers — no parse, no daily merge, no station-file read or write. A station with forecast runs is always
  // recomputed (its `ahead` values are read at this minute).
  let cache: StationCache | null = null;
  if (a.aware) {
    try {
      cache = JSON.parse((await priv.get(STATION_CACHE)) ?? "null");
    } catch {
      cache = null;
    }
    if (!cache || cache.rev !== GAUGE_REV) cache = { rev: GAUGE_REV, st: {} };
  }
  const nextCache: StationCache = { rev: GAUGE_REV, st: {} };
  const ctxOf = (st: string) =>
    contentHash(JSON.stringify([nowDay, pent, release, (envelope[st] ? "e" : ""), Object.keys(forecast[st] || {}).length ? (a.reuse ? fsigOf(forecast[st]) : now) : null]));
  const unchanged = new Set<string>();
  const validators = new Map<string, { etag: string | null; lm: string | null }>();
  const got = (
    await pool([...a.ids].sort(), WORKERS, async (st) => {
      const url = `${BASE}/hourly/BC_${st}_hourly_hydrometric.csv`;
      if (!a.aware) {
        const text = await get(fetch, url);
        return text ? parseHourly(st, text) : null;
      }
      const c = cache!.st[st];
      const cond = c && c.ctx === ctxOf(st);
      const r = await getCond(fetch, url, cond ? c.etag : null, cond ? c.lm : null);
      if (r.same && c) {
        unchanged.add(st);
        return { station: st, ...c.reading, recent: [] } as Reading;
      }
      validators.set(st, { etag: r.etag, lm: r.lm });
      return r.text ? parseHourly(st, r.text) : null;
    })
  ).filter((r): r is Reading => r !== null);
  if (a.meter) a.meter.sourcesUnchanged += unchanged.size;
  if (a.aware) log(`  unchanged ${unchanged.size} of ${got.length} station CSVs (304): parse, merge and station-file write skipped`);
  const fresh = got.filter((r) => !unchanged.has(r.station));

  const priorList = await pool(fresh, WORKERS, async (r) => {
    const f = await a.stationFile(r.station);
    return { daily: f && f.ok && truthy(f.daily) ? (f.daily as unknown[][]) : [] };
  });
  const prior = new Map(fresh.map((r, i) => [r.station, priorList[i]!]));

  const seenBy = new Map<string, Means>();
  for (const r of fresh) seenBy.set(r.station, meansOf(r.recent));
  const todayOf = new Map(fresh.map((r) => [r.station, localDay(r.at, nowDay)]));
  const ym = (d: string) => [Number(d.slice(0, 4)), Number(d.slice(5, 7))] as const;

  const gappy = fresh
    .map((r) => r.station)
    .filter((st) => {
      const [y, m] = ym(todayOf.get(st)!);
      const have = mergeDaily(prior.get(st)!.daily, seenBy.get(st)!, y, m);
      return dailyGaps(have, todayOf.get(st)!)[0].length > 0;
    });
  const backfill = new Map<string, Means>();
  let filled = 0;
  if (gappy.length) {
    const res = await pool([...gappy].sort(), ARCHIVE_WORKERS, async (st) => {
      const text = await get(fetch, DAILY(st));
      return [st, text ? dailyMeans(text) : new Map()] as const;
    });
    for (const [st, m] of res) backfill.set(st, m);
    filled = [...backfill.values()].filter((v) => v.size).length;
    log(`  daily    refilled ${filled} of ${gappy.length} station records with a gap from ECCC's 30-day archive`);
  }
  const beyondWindow: Record<string, string[][]> = {};

  const temps = await fetchTemperatures(fetch, got.map((r) => r.station), now, a.sleep);
  // accumulate_temperatures
  // T225 follow-up: temp_history.json is STATE (the samples the envelope is built from; the app never reads it), so it
  // lives in the PRIVATE store now. Python publishes it in the feed dir; a legacy public copy is read once, then removed.
  let hist: Record<string, Record<string, number[]>> = {};
  let legacyPublic = false;
  try {
    let t = await priv.get(TEMP_HISTORY);
    if (t === null) {
      t = await feed.get(TEMP_HISTORY);
      legacyPublic = t !== null;
    }
    hist = t === null ? {} : JSON.parse(t);
  } catch {
    hist = {};
  }
  if (!hist || typeof hist !== "object" || Array.isArray(hist)) hist = {};
  const pkey = String(pent);
  for (const [station, [, c]] of temps) {
    const buckets = (hist[station] ??= {});
    const vals = (buckets[pkey] ??= []);
    vals.push(pyRound(c, 2));
    if (vals.length > TEMP_SAMPLES_PER_PENTAD) vals.splice(0, vals.length - TEMP_SAMPLES_PER_PENTAD);
  }
  await priv.put(TEMP_HISTORY, JSON.stringify(hist), JSON_CT);
  if (legacyPublic && (feed as DeletableStore).delete) await (feed as DeletableStore).delete!(TEMP_HISTORY);
  const tEnv = temperatureEnvelope(hist);
  log(`  water temperature: ${temps.size} of ${got.length} stations  (${Object.keys(tEnv).length} now have enough history for an envelope)`);

  const index: Record<string, any> = {};
  let newest: string | null = null;
  // station files are PUT as they are made, 8 at a time, never buffered (17 MB for the province)
  const inflight = new Set<Promise<void>>();
  const put = async (k: string, body: string) => {
    while (inflight.size >= WORKERS) await Promise.race(inflight);
    const p: Promise<void> = feed.put(k, body, JSON_CT).finally(() => inflight.delete(p));
    inflight.add(p);
  };
  for (const r of got) {
    const st = r.station;
    const t0 = temps.get(st);
    const tC0 = t0 ? t0[1] : null;
    const tempParts = (): [Num, string | null, string | null, Num] =>
      tC0 === null ? [null, null, null, null] : [tC0, t0![0], temperatureBand(tC0), percentileOf(tC0, (tEnv[st] || {})[String(pent)])];
    if (unchanged.has(st)) {
      const c = cache!.st[st]!;
      index[st] = composeEntry(c.base, ...tempParts());
      nextCache.st[st] = c;
      if (c.beyond) beyondWindow[st] = c.beyond;
      if (c.newest && (!newest || c.newest > newest)) newest = c.newest;
      continue;
    }
    const have: Record<string, any> = envelope[st] || {};
    const param = shownParameter(have, r.discharge, r.level);
    const band = bandFor(have[param], now);
    const observed = param === "level" ? r.level : r.discharge;
    const t = temps.get(st);
    const tAt = t ? t[0] : null;
    const tC = t ? t[1] : null;
    const flowEnv = have.discharge;
    const rankDay: Rank | null = truthy(flowEnv) ? (v, d) => percentileOf(v, bandFor(flowEnv, dayMs(d)! + 12 * HOUR)) : null;
    const runs = forecast[st];
    const cprev = a.reuse ? cache?.st[st] : undefined;
    const prevFile = a.reuse ? await a.stationFile(st) : null;
    const reuseF = !!(cprev && cprev.fsig === fsigOf(runs) && prevFile?.ok && prevFile.forecastsText !== undefined);
    let changed: ReturnType<typeof stationChange>;
    if (reuseF) changed = [{}, null, null] as unknown as ReturnType<typeof stationChange>;
    else if (a.reuse && prevFile?.ok && prevFile.forecastsText !== undefined) {
      // reuse run, but this station has no stored output (new, or its cache entry is gone): its runs WITH series are
      // in its last station file — compute from those
      const fromFile = JSON.parse(prevFile.forecastsText) as Runs;
      const withSer: Runs = {};
      for (const [m, run] of Object.entries(runs || {})) withSer[m] = fromFile[m]?.issuedAt === run.issuedAt && fromFile[m]?.series ? { ...run, series: fromFile[m]!.series } : run;
      forecast[st] = withSer;
      changed = stationChange(withSer, now, r.discharge, rankDay);
    } else changed = stationChange(runs, now, r.discharge, rankDay);
    const runsNow = forecast[st];
    const per: Record<string, any> = {};
    for (const q of ["discharge", "level"]) {
      const v = q === "level" ? r.level : r.discharge;
      const p = percentileOf(v, bandFor(have[q], now));
      if (p !== null) per[q] = p;
    }
    if (tC !== null) {
      per.temperatureBand = temperatureBand(tC);
      const pt = percentileOf(tC, (tEnv[st] || {})[String(pent)]);
      if (pt !== null) per.temperature = pt;
    }
    const ahead = reuseF ? ((cprev!.base.ahead as Record<string, unknown>) ?? {}) : forecastPercentiles(runsNow, have, now, { discharge: r.discharge, level: r.level });
    const days = reuseF ? (cprev!.base.aheadDays ?? null) : forecastDays(runsNow, now, r.discharge, rankDay);
    const idxChange = reuseF ? (cprev!.base.change ?? null) : changed[2];
    index[st] = {
      percentile: percentileOf(observed, band),
      parameter: param,
      ...per,
      observedAt: r.at,
      ...(tC !== null ? { temperatureC: tC, temperatureAt: tAt } : {}),
      forecast: reuseF ? cprev!.base.forecast : Object.values(runsNow || {}).some((run) => truthy(run.series)),
      ...(Object.keys(ahead).length ? { ahead } : {}),
      ...(days ? { aheadDays: days } : {}),
      ...(idxChange ? { change: idxChange } : {}),
    };
    const seen: Means = new Map(seenBy.get(st)!);
    for (const [k, v] of backfill.get(st) || []) seen.set(k, v);
    const day = todayOf.get(st)!;
    const [y, m] = ym(day);
    const daily = mergeDaily(prior.get(st)!.daily, seen, y, m);
    const old = dailyGaps(daily, day)[1];
    if (old.length) beyondWindow[st] = dayRuns(old);

    const forecasts: Record<string, any> = {};
    for (const mdl of Object.keys(runsNow || {}).sort()) {
      const run = runsNow![mdl]!;
      forecasts[mdl] = mdl in changed[0] ? { ...run, change: changed[0][mdl] } : run;
    }
    const stChange = reuseF ? prevFile!.change : changed[1];
    const FC = "\u0000forecasts\u0000";
    let text = JSON.stringify({
      fetchedAt,
      station: st,
      now: { discharge: r.discharge, level: r.level, at: r.at, percentile: index[st].percentile, parameter: param },
      recent: r.recent,
      daily,
      priorYears: ((clim || {}).recentYears ?? {})[st] ?? null,
      forecasts: reuseF ? FC : forecasts,
      ...(stChange ? { change: stChange } : {}),
    });
    if (reuseF) text = text.replace(JSON.stringify(FC), () => prevFile!.forecastsText!); // the stored block, verbatim
    await put(`${st}.json`, text);
    if (prevFile) delete prevFile.forecastsText;
    prior.delete(st);
    seenBy.delete(st);
    backfill.delete(st);
    const stamps = [newest, ...(reuseF ? [cprev!.newest] : Object.values(changed[0]).map((c: any) => c.issuedAt))].filter((x) => truthy(x)) as string[];
    newest = stamps.length ? stamps.reduce((a2, b2) => (b2 > a2 ? b2 : a2)) : newest;
    if (a.aware) {
      const own = reuseF ? (cprev!.newest ? [cprev!.newest] : []) : (Object.values(changed[0]).map((c: any) => c.issuedAt).filter((x) => truthy(x)) as string[]).sort();
      const { temperatureBand: _b, temperature: _t, temperatureC: _c, temperatureAt: _a, percentile, parameter, observedAt, forecast: fc, ahead: ah, aheadDays: ad, change: ch, ...perQ } = index[st];
      const base: Base = { percentile, parameter, per: perQ, observedAt, forecast: fc };
      if (ah !== undefined) base.ahead = ah;
      if (ad !== undefined) base.aheadDays = ad;
      if (ch !== undefined) base.change = ch;
      const v = validators.get(st) ?? { etag: null, lm: null };
      nextCache.st[st] = {
        ctx: ctxOf(st), fsig: fsigOf(runs), etag: v.etag, lm: v.lm, reading: { at: r.at, level: r.level, discharge: r.discharge }, base,
        newest: own.length ? own[own.length - 1]! : null, ...(beyondWindow[st] ? { beyond: beyondWindow[st] } : {}),
      };
    }
  }
  await Promise.all(inflight);
  // ROBUSTNESS (owner 2026-10-28, change-aware runs only; the Python drops them): a station asked but not answered this
  // run keeps its last good index entry, marked `stale` (+ `staleSince`), for up to CARRY_MAX_MS of observation age;
  // its station file is untouched and its cache entry kept (next run asks with the same validators).
  const answered = new Set(got.map((r) => r.station));
  const failed = [...a.ids].filter((st) => !answered.has(st)).sort();
  let carried = 0;
  if (a.aware) {
    for (const st of failed) {
      const p = prevStations[st];
      const obs = p && typeof p.observedAt === "string" ? Date.parse(/(Z|[+-]\d\d:?\d\d)$/i.test(p.observedAt) ? p.observedAt : p.observedAt + "Z") : NaN;
      if (!p || !(now - obs < CARRY_MAX_MS)) continue;
      index[st] = { ...p, stale: true, staleSince: p.staleSince ?? fetchedAt };
      if (cache?.st[st]) nextCache.st[st] = cache.st[st]!;
      carried++;
    }
    if (failed.length) log(`  ⚠️  ${failed.length} station(s) not answered; ${carried} kept their last good entry (stale)`);
  }
  const indexKept = a.aware && got.length === 0;
  if (a.aware && !indexKept) await priv.put(STATION_CACHE, JSON.stringify(nextCache));

  let archived: Record<string, any>;
  try {
    archived = await appendRuns(priv, forecast, now, new Map(got.map((r) => [r.station, r.discharge])));
  } catch (e) {
    archived = { added: 0, error: String(e) };
  }
  log(`  runs     ${archived.added ?? 0} new run(s) archived for scoring${archived.error ? ` (${archived.error})` : ""}`);

  const staleRelease = Boolean(a.latest && release && a.latest !== release);
  const maintenance = maintenanceNotices({ release, latest: a.latest, drift: a.drift, previous: before, at: fetchedAt, roster: a.roster });
  if (!indexKept) await feed.put("index.json", JSON.stringify({
    fetchedAt,
    // T225 follow-up: written ONCE. Python's dict literal names it twice and the second (T52: the newest issue among the
    // runs with a `change` block) silently replaced the first; the app's Forecasts line (T68) means the newest run
    // feed-wide, which is what this is.
    forecastIssuedAt: newestForecastIssue(forecast),
    hydat: { release, latest: a.latest, stale: staleRelease },
    maintenance,
    ...(newest ? { forecastAttribution: ATTRIBUTION } : {}),
    stations: index,
    ...(a.listed !== null ? { listed: [...a.listed].sort() } : {}),
  }), JSON_CT);

  return {
    asked: a.ids.length, answered: got.length, release, stale: staleRelease, latest: a.latest, maintenance,
    withPercentile: Object.values(index).filter((v) => v.percentile !== null).length,
    archive: { asked: gappy.length, filled },
    runArchive: archived,
    gapsBeyondWindow: beyondWindow,
    ...(a.aware ? { failed, carried, indexKept } : {}),
  };
}
