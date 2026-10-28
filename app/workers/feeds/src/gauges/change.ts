// T225 port of pipeline/gauges/feed/change.py (the spec): the expected change (T52/T144), the one combination of a
// forecast day (T177) and every ranked forecast day (T171). Clocks: `now` is epoch ms; a Centre wall clock is ms "as
// if UTC" (time.ts); a day is "YYYY-MM-DD".
import { erf, pyRound, pySum } from "./py";
import { addDays, centreWall, dayOf, HOUR, hours, instant } from "./time";
import type { Run, Runs } from "./forecast";

export const CHANGE_DAYS = 10;
export const RISE_RATIO = 2.0;
export const FALL_RATIO = 0.5;
export const BIG_POINTS = 0.25;
export const SPIKE_BACK = 0.5;
export const STALE_HOURS = 36.0;
export const FLOOD_MODELS = ["CLEVER", "COFFEE"] as const;
export const FLOOD_WEIGHTS: Record<string, number> = { CLEVER: 1.0, COFFEE: 1.0 };
export const LOW_WATER_MODEL = "ELF";
export const CHANGE_SCALE = 2.0;
export const CODE_FLOOR = 0.005;
export const PEAK_MODELS = ["COFFEE"];
export const AHEAD_DAYS = CHANGE_DAYS;
export const DAY_COVER = 0.75;

/** `rank(flow, day)`: that day's percentile for a flow, or null. */
export type Rank = (v: number, day: string) => number | null;
type Day = [string, number, number];

/** forecast.step_day (lives here to keep forecast.ts free of a cycle): the Centre day a DAILY step answers for. */
export function stepDay(series: Record<string, any> | null | undefined, at: unknown): string | null {
  const day = typeof at === "string" && /^\d{4}-\d{2}-\d{2}$/.test(at.trim());
  if (!(day || (series || {}).step === "1d")) return null;
  const t = instant(at);
  return t === null ? null : dayOf(centreWall(t.ms));
}

export function coded(ratio: number): number {
  const z = Math.log2(Math.max(ratio, 1e-9)) / CHANGE_SCALE;
  const p = 0.5 * (1.0 + erf(z / Math.sqrt(2.0)));
  return Math.min(1.0 - CODE_FLOOR, Math.max(CODE_FLOOR, p));
}

/** change._local: a stamp as a naive Centre wall clock (ms). */
function local(at: unknown): number | null {
  const t = instant(at);
  return t === null ? null : centreWall(t.ms);
}

export function issuedAgeH(issuedAt: unknown, now: number): number | null {
  const t = issuedAt ? local(issuedAt) : null;
  if (t === null) return null;
  return hours(centreWall(now), t);
}

export function stale(run: Run | null | undefined, now: number): boolean {
  const age = issuedAgeH((run || {}).issuedAt, now);
  return age !== null && age > STALE_HOURS;
}

function zip<A, B>(a: A[] | undefined | null, b: B[] | undefined | null): Array<[A, B]> {
  const x = a || [];
  const y = b || [];
  const n = Math.min(x.length, y.length);
  const out: Array<[A, B]> = [];
  for (let i = 0; i < n; i++) out.push([x[i]!, y[i]!]);
  return out;
}

/** change.daily_extremes: `start` a Centre wall clock, `last` a day. */
export function dailyExtremes(series: Record<string, any>, start: number, last: string): Day[] {
  const days = new Map<string, number[]>();
  const startDay = dayOf(start);
  for (const [at, v] of zip<unknown, number | null>(series.at, series.mid)) {
    if (v === null || v === undefined) continue;
    const t = local(at);
    if (t === null || dayOf(t) > last) continue;
    if (t < start && !(dayOf(t) === startDay && stepDay(series, at) !== null)) continue;
    const d = dayOf(t);
    let a = days.get(d);
    if (!a) days.set(d, (a = []));
    a.push(v);
  }
  return [...days.entries()].sort((a, b) => (a[0] < b[0] ? -1 : 1)).map(([d, vs]) => [d, minOf(vs), maxOf(vs)]);
}

function minOf(vs: number[]): number {
  let m = vs[0]!;
  for (const v of vs) if (v < m) m = v;
  return m;
}
function maxOf(vs: number[]): number {
  let m = vs[0]!;
  for (const v of vs) if (v > m) m = v;
  return m;
}

/** change.base_of: `nowLocal` a Centre wall clock. */
export function baseOf(series: Record<string, any>, nowLocal: number, key = "mid"): number | null {
  let best: [number, number] | null = null;
  const today = dayOf(nowLocal);
  for (const [at, v] of zip<unknown, number | null>(series.at, series[key])) {
    if (v === null || v === undefined) continue;
    const t = local(at);
    if (t === null) continue;
    if (stepDay(series, at) !== null) {
      if (dayOf(t) === today) return v;
      continue;
    }
    const off = Math.abs(hours(t, nowLocal));
    if (off <= 3.0 && (best === null || off < best[0])) best = [off, v];
  }
  return best === null ? null : best[1];
}

function back(days: Day[], after: number, rise: boolean, base: number, extreme: number, until: string | null = null): string | null {
  if (rise ? extreme <= base : extreme >= base) return null;
  const line = base + SPIKE_BACK * (extreme - base);
  for (const [d, lo, hi] of days.slice(after + 1)) {
    if (until !== null && d > until) return null;
    if (rise ? hi <= line : lo >= line) return d;
  }
  return null;
}

function measure(days: Day[], base: number, scale: number | null, rank: Rank | null, p0: number | null, stops: string[] = []): Record<string, any> {
  let ip = 0;
  for (let i = 1; i < days.length; i++) if (days[i]![2] > days[ip]![2]) ip = i;
  let it = 0;
  for (let i = 1; i < days.length; i++) if (days[i]![1] < days[it]![1]) it = i;

  const end = (i: number, rise: boolean): Record<string, any> => {
    const [d, lo, hi] = days[i]!;
    const v = rise ? hi : lo;
    const p = rank !== null && scale !== null ? rank(v * scale, d) : null;
    let until: string | null = null;
    for (const s of stops) if (s >= d && (until === null || s < until)) until = s;
    const bk = back(days, i, rise, base, v, until);
    const moved = rise ? v > base : v < base;
    return {
      day: d, ratio: pyRound(v / base, 4), value: pyRound(v, 4),
      ...(p !== null ? { percentile: pyRound(p, 4) } : {}),
      ...(p !== null && p0 !== null ? { points: pyRound(p - p0, 4) + 0.0 } : {}),
      back: bk,
      ...(until !== null && bk === null && moved ? { until } : {}),
    };
  };
  const peak = end(ip, true);
  const trough = end(it, false);
  const bigRise = peak.ratio >= RISE_RATIO || (peak.points ?? 0.0) >= BIG_POINTS;
  const bigFall = trough.ratio <= FALL_RATIO || (trough.points ?? 0.0) <= -BIG_POINTS;
  const dir = bigRise && bigFall ? "both" : bigRise ? "rise" : bigFall ? "fall" : "steady";
  return {
    base: pyRound(base, 4), days: days.map(([d, lo, hi]) => [d, pyRound(lo, 4), pyRound(hi, 4)]),
    peak, trough, dir,
  };
}

function todayRank(now: number, observed: number | null, rank: Rank | null): number | null {
  if (rank === null || !observed || observed <= 0) return null;
  return rank(observed, dayOf(centreWall(now)));
}

export function runDays(run: Run, now: number): [number, Day[]] | null {
  const series = run.series || {};
  if (!Object.keys(series).length || stale(run, now)) return null;
  const nowLocal = centreWall(now);
  const base = baseOf(series, nowLocal);
  if (base === null || base <= 0) return null;
  const start = Math.floor(nowLocal / HOUR) * HOUR; // replace(minute=0, second=0, microsecond=0)
  const days = dailyExtremes(series, start, addDays(dayOf(nowLocal), CHANGE_DAYS));
  return days.length ? [base, days] : null;
}

function ofRun(run: Run, got: [number, Day[]], observed: number | null, rank: Rank | null, p0: number | null): Record<string, any> {
  const [base, days] = got;
  const scale = observed && observed > 0 ? observed / base : null;
  return { model: run.model ?? null, issuedAt: run.issuedAt ?? null, ...measure(days, base, scale, rank, p0) };
}

export function changeSource(runs: Map<string, [number, Day[]]>): { models: string[]; lowWater: boolean; days: Day[]; reach: Record<string, string> } | null {
  const flood = FLOOD_MODELS.filter((m) => runs.has(m)) as string[];
  const use = flood.length ? flood : runs.has(LOW_WATER_MODEL) ? [LOW_WATER_MODEL] : [];
  if (!use.length) return null;
  const byDay = new Map<string, Map<string, [number, number]>>();
  for (const m of use) {
    const [base, days] = runs.get(m)!;
    for (const [d, lo, hi] of days) {
      let e = byDay.get(d);
      if (!e) byDay.set(d, (e = new Map()));
      e.set(m, [lo / base, hi / base]);
    }
  }
  const joined = (d: string, k: 0 | 1): number => {
    const vals: Record<string, number> = {};
    for (const [m, v] of byDay.get(d)!) vals[m] = v[k];
    return combineDay(vals)![1];
  };
  const reach: Record<string, string> = {};
  for (const m of use) {
    const ds = runs.get(m)![1];
    reach[m] = ds[ds.length - 1]![0];
  }
  return {
    models: use, lowWater: !flood.length,
    days: [...byDay.keys()].sort().map((d) => [d, joined(d, 0), joined(d, 1)] as Day),
    reach,
  };
}

/** change.combine_day */
export function combineDay(values: Record<string, number | null | undefined>): [string[], number, boolean] | null {
  const flood: Array<[string, number]> = [];
  for (const m of FLOOD_MODELS) {
    const v = values[m];
    if (v !== null && v !== undefined) flood.push([m, v]);
  }
  if (flood.length === 1) return [[flood[0]![0]], flood[0]![1], false];
  if (flood.length) {
    const w = pySum(flood.map(([m]) => FLOOD_WEIGHTS[m]!));
    const v = Math.exp(pySum(flood.map(([m, x]) => FLOOD_WEIGHTS[m]! * Math.log(Math.max(x, 1e-9)))) / w);
    return [flood.map(([m]) => m), v, false];
  }
  const low = values[LOW_WATER_MODEL];
  if (low !== null && low !== undefined) return [[LOW_WATER_MODEL], low, true];
  return null;
}

export function modelName(models: string[]): string {
  return models.join("+");
}

export function gaugeScale(model: string, run: Run, now: number, observed: number | null | undefined, quantity = "discharge"): number | null {
  if (!PEAK_MODELS.includes(model)) return 1.0;
  const key = quantity === "level" ? "level_mid" : "mid";
  const base = baseOf(run.series || {}, centreWall(now), key);
  if (!observed || observed <= 0 || base === null || base <= 0) return null;
  return observed / base;
}

/** change.day_means: `{Centre day: mean}`; insertion order as Python's (hourly days first-seen, then daily steps). */
export function dayMeans(series: Record<string, any>, key = "mid"): Map<string, number> {
  const acc = new Map<string, number[]>();
  const times: number[] = [];
  const daily = new Map<string, number>();
  for (const [at, v] of zip<unknown, number | null>(series.at, series[key])) {
    const t = local(at);
    if (t !== null) times.push(t);
    if (v === null || v === undefined || t === null) continue;
    const named = stepDay(series, at);
    if (named !== null) {
      daily.set(named, v);
      continue;
    }
    const d = dayOf(t);
    let a = acc.get(d);
    if (!a) acc.set(d, (a = []));
    a.push(v);
  }
  const st = [...times].sort((a, b) => a - b);
  const gaps: number[] = [];
  for (let i = 1; i < st.length; i++) {
    const g = hours(st[i]!, st[i - 1]!);
    if (g > 0) gaps.push(g);
  }
  gaps.sort((a, b) => a - b);
  const perDay = gaps.length ? 24.0 / gaps[Math.floor(gaps.length / 2)]! : 24.0;
  const out = new Map<string, number>();
  for (const [d, vs] of acc) if (vs.length >= DAY_COVER * perDay) out.set(d, pySum(vs) / vs.length);
  for (const [d, v] of daily) out.set(d, v);
  return out;
}

export function forecastDays(runs: Runs | null | undefined, now: number, observed: number | null, rank: Rank | null): Record<string, any> | null {
  if (!runs || !Object.keys(runs).length || rank === null) return null;
  const first = addDays(dayOf(centreWall(now)), 1);
  const means = new Map<string, [Map<string, number>, number | null]>();
  for (const model of Object.keys(runs).sort()) {
    const run = runs[model];
    const series = (run || {}).series;
    if (!series || !Object.keys(series).length || stale(run, now)) continue;
    means.set(model, [dayMeans(series), gaugeScale(model, run!, now, observed)]);
  }
  const ps: Array<number | null> = [];
  const ms: Array<string | null> = [];
  const low: number[] = [];
  for (let k = 0; k < AHEAD_DAYS; k++) {
    const d = addDays(first, k);
    const vals: Record<string, number | null> = {};
    for (const [m, [v, s]] of means) vals[m] = v.has(d) && s !== null ? v.get(d)! * s : null;
    const got = combineDay(vals);
    const p = got ? rank(got[1], d) : null;
    ps.push(p !== null ? pyRound(p, 4) : null);
    ms.push(got && p !== null ? modelName(got[0]) : null);
    if (got && p !== null && got[2]) low.push(k);
  }
  if (ps.every((p) => p === null)) return null;
  return { from: first, discharge: ps, model: ms, ...(low.length ? { lowWater: low } : {}) };
}

function extreme(c: Record<string, any>): Record<string, any> {
  if (c.dir === "rise") return c.peak;
  if (c.dir === "fall") return c.trough;
  const up = c.peak.ratio;
  const down = c.trough.ratio;
  if (Math.abs(Math.log(Math.max(up, 1e-9))) >= Math.abs(Math.log(Math.max(down, 1e-9)))) return c.peak;
  return c.trough;
}

/** change.station_change: `[per model, source, summary]`. */
export function stationChange(runs: Runs | null | undefined, now: number, observed: number | null, rank: Rank | null): [Record<string, any>, Record<string, any> | null, Record<string, any> | null] {
  const raw = new Map<string, [number, Day[]]>();
  const per: Record<string, any> = {};
  const p0 = todayRank(now, observed, rank);
  for (const model of Object.keys(runs || {}).sort()) {
    const run = runs![model]!;
    const got = runDays(run, now);
    if (got !== null) {
      raw.set(model, got);
      per[model] = ofRun({ ...run, model: run.model || model }, got, observed, rank, p0);
    }
  }
  const chosen = changeSource(raw);
  if (chosen === null) return [per, null, null];
  const lastDay = chosen.days[chosen.days.length - 1]![0];
  const stops = [...new Set(Object.values(chosen.reach).filter((d) => d < lastDay))].sort();
  const src: Record<string, any> = {
    models: chosen.models, lowWater: chosen.lowWater, reach: chosen.reach,
    ...measure(chosen.days, 1.0, observed && observed > 0 ? observed : null, rank, p0, stops),
  };
  const e = extreme(src);
  const srcDays = src.days as Array<[string, number, number]>;
  const summary = {
    discharge: pyRound(coded(e.ratio), 4), models: src.models, dir: src.dir,
    ratio: e.ratio, day: e.day,
    shape: e.back ? "spike" : e.until || e.day === srcDays[srcDays.length - 1]![0] ? "open" : "step",
    ...(e.back ? { back: e.back } : {}),
    ...(src.lowWater ? { lowWater: true } : {}),
  };
  return [per, src, summary];
}
