// T178 port of the SCORING half of pipeline/gauges/feed/skill.py (`band`, `cases`, `_stats`, `score`, `verdict`,
// `feed_observed`, `clim_ranks`): the worker scores CLEVER vs COFFEE on the run archive skill.ts appends, once a month,
// and REPORTS the verdict (status + the outbox). It never applies it: FLOOD_WEIGHTS (Python's change.py, generated into
// src/gauges/policy.generated.ts) move only in a reviewed change (owner 2026-11-04) — a weight that moved on its own would change every
// forecast without anyone seeing why.
//
// THE WINDOW (the one difference from the CLI, which scores the whole archive): ONE archive month, the one that just
// closed — `runs/<YYYY-MM>.jsonl` plus any of that month's day files / parts still in runs/state.json, concatenated in
// key order (= Python's month file). Bounded on purpose (CPU, owner 2026-11-04: the worker uses ~9 % of the plan): a
// month holds ~22 weekday issue days, past the verdict's MIN_ISSUE_DAYS, and the station files' `daily` record (this
// calendar year, plus last December through January) still holds that month's observations on the 1st.
import type { Store } from "../io";
import { AHEAD_DAYS, combineDay, FLOOD_MODELS, LOW_WATER_MODEL, type Rank } from "./change";
import { bandFor, percentileOf } from "./publish";
import { pyFloatRepr, pyRound, pySum, splitlines, truthy } from "./py";
import type { RunsState } from "./skill";
import { RUNS_STATE } from "./skill";
import { addDays, centreWall, dayMs, dayOf, HOUR, instant } from "./time";

// (T557 item 2) skill.py's constants, generated (pipeline.tools.emit.gauge_policy), never typed here.
import { HIGH, LEADS, LOW, MIN_ISSUE_DAYS, MIN_PAIRS } from "./policy.generated";
export { HIGH, LEADS, LOW, MIN_ISSUE_DAYS, MIN_PAIRS };

export type Rec = Record<string, any>;
export interface Case {
  station: string;
  issued: string;
  lead: number;
  day: string;
  obs: number;
  by: Record<string, number>;
}

/** skill.band */
export function band(p: number): "low" | "normal" | "high" {
  return p < LOW ? "low" : p >= HIGH ? "high" : "normal";
}

function issueDay(r: Rec): string | null {
  const t = instant(r.issuedAt);
  return t === null ? null : dayOf(centreWall(t.ms));
}

const pyStrOf = (v: unknown) => (v === null || v === undefined ? "None" : String(v));

/** skill.cases: one per station, issue day and lead, each model's LAST run of that issue day, days over (`< before`)
 * with an observed mean. */
export function cases(records: Rec[], observed: Record<string, Record<string, number>>, before: string): Case[] {
  const last = new Map<string, { st: string; d0: string; m: string; r: Rec }>();
  for (const r of records) {
    const d0 = issueDay(r);
    if (d0 === null || r.scale === null || r.scale === undefined) continue;
    const k = `${r.station}\u0000${d0}\u0000${r.model}`;
    const had = last.get(k);
    if (!had || pyStrOf(r.issuedAt) > pyStrOf(had.r.issuedAt)) last.set(k, { st: String(r.station), d0, m: String(r.model), r });
  }
  const grouped = new Map<string, { st: string; d0: string; runs: Map<string, Rec> }>();
  for (const { st, d0, m, r } of last.values()) {
    const g = `${st}\u0000${d0}`;
    let e = grouped.get(g);
    if (!e) grouped.set(g, (e = { st, d0, runs: new Map() }));
    e.runs.set(m, r);
  }
  const keys = [...grouped.values()].sort((a, b) => (a.st < b.st ? -1 : a.st > b.st ? 1 : a.d0 < b.d0 ? -1 : a.d0 > b.d0 ? 1 : 0));
  const out: Case[] = [];
  for (const { st, d0, runs } of keys) {
    for (let lead = 1; lead <= AHEAD_DAYS; lead++) {
      const day = addDays(d0, lead);
      const obs = (observed[st] || {})[day];
      if (day >= before || obs === null || obs === undefined || obs <= 0) continue;
      const by: Record<string, number> = {};
      for (const [m, r] of runs) {
        const v = (r.days || {})[day];
        if (v !== undefined && v !== null && v > 0) by[m] = v * r.scale;
      }
      if (Object.keys(by).length) out.push({ station: st, issued: d0, lead, day, obs, by });
    }
  }
  return out;
}

type Pair = [number, number, number | null, number | null];

/** skill._stats: `pairs` of (forecast, observed, forecast percentile, observed percentile). */
export function stats(pairs: Pair[]): Record<string, number> {
  if (!pairs.length) return { n: 0 };
  const e = pairs.map(([f, o]) => Math.log2(f / o));
  const ranked = pairs.filter(([, , pf, po]) => pf !== null && po !== null).map(([, , pf, po]) => [pf!, po!] as [number, number]);
  return {
    n: pairs.length,
    bias: pyRound(pySum(e) / e.length, 3),
    mae: pyRound(pySum(e.map(Math.abs)) / e.length, 3),
    ...(ranked.length
      ? {
          pmae: pyRound(pySum(ranked.map(([a, b]) => Math.abs(a - b))) / ranked.length, 3),
          hit: pyRound(ranked.filter(([a, b]) => band(a) === band(b)).length / ranked.length, 3),
          nr: ranked.length,
        }
      : {}),
  };
}

/** skill.score: per lead day, each model on every case it answered (`own`), and on the cases both flood models
 * answered (`paired`) CLEVER, COFFEE, their combination and the head-to-head difference. */
export function score(found: Case[], rankFor: (st: string) => Rank | null, leads: readonly number[] = LEADS): Record<string, any> {
  const table: Record<string, any> = {};
  const [c0, c1] = FLOOD_MODELS;
  for (const lead of leads) {
    const at = found.filter((c) => c.lead === lead);
    if (!at.length) continue;
    const row = (c: Case, f: number): Pair => {
      const rank = rankFor(c.station);
      return [f, c.obs, rank ? rank(f, c.day) : null, rank ? rank(c.obs, c.day) : null];
    };
    const own: Record<string, any> = {};
    for (const m of [...FLOOD_MODELS, LOW_WATER_MODEL]) own[m] = stats(at.filter((c) => m in c.by).map((c) => row(c, c.by[m]!)));
    const both = at.filter((c) => FLOOD_MODELS.every((m) => m in c.by));
    const paired: Record<string, any> = {};
    for (const m of FLOOD_MODELS) paired[m] = stats(both.map((c) => row(c, c.by[m]!)));
    paired.combined = stats(both.map((c) => row(c, combineDay(c.by)![1])));
    if (both.some((c) => LOW_WATER_MODEL in c.by))
      paired[LOW_WATER_MODEL] = stats(both.filter((c) => LOW_WATER_MODEL in c.by).map((c) => row(c, c.by[LOW_WATER_MODEL]!)));
    const diffs = both.map((c) => Math.abs(Math.log2(c.by[c0]! / c.obs)) - Math.abs(Math.log2(c.by[c1]! / c.obs)));
    if (diffs.length >= 2) {
      const mean = pySum(diffs) / diffs.length;
      const sd = Math.sqrt(pySum(diffs.map((x) => (x - mean) ** 2)) / (diffs.length - 1));
      paired.difference = { n: diffs.length, mean: pyRound(mean, 3), se: pyRound(sd / Math.sqrt(diffs.length), 3) };
    }
    table[String(lead)] = {
      own, paired,
      stations: new Set(at.map((c) => c.station)).size,
      pairedStations: new Set(both.map((c) => c.station)).size,
      issueDays: new Set(at.map((c) => c.issued)).size,
    };
  }
  return table;
}

/** skill.verdict (T177's rule) for one lead day's row: too few → equal; within 2 SE → equal; else inverse error. */
export function verdict(row: Record<string, any>): { weights: Record<string, number>; why: string } {
  const [c, f] = FLOOD_MODELS;
  const equal = { [c]: 1.0, [f]: 1.0 };
  const d = (row.paired || {}).difference;
  const n: number = (d || {}).n ?? 0;
  const days: number = row.issueDays ?? 0;
  if (n < MIN_PAIRS || days < MIN_ISSUE_DAYS)
    return { weights: equal, why: `too few: ${n} paired cases over ${days} issue day(s) (needs ${MIN_PAIRS} over ${MIN_ISSUE_DAYS})` };
  if (Math.abs(d.mean) <= 2 * d.se) return { weights: equal, why: `neither better: difference ${pyFloatRepr(d.mean)} within 2 x ${pyFloatRepr(d.se)}` };
  const inv: Record<string, number> = { [c]: 1.0 / Math.max(row.paired[c].mae, 1e-6), [f]: 1.0 / Math.max(row.paired[f].mae, 1e-6) };
  const top = Math.max(...Object.values(inv));
  const weights: Record<string, number> = {};
  for (const [m, v] of Object.entries(inv)) weights[m] = pyRound(v / top, 2);
  return { weights, why: `${d.mean < 0 ? c : f} better: difference ${pyFloatRepr(d.mean)}, se ${pyFloatRepr(d.se)}` };
}

/** skill.feed_observed for the stations asked: `{station: {day: discharge daily mean}}` from `<station>.json` `daily`. */
export async function feedObserved(feed: Store, stations: Iterable<string>): Promise<Record<string, Record<string, number>>> {
  const out: Record<string, Record<string, number>> = {};
  for (const st of [...new Set(stations)].sort()) {
    let rows: unknown;
    try {
      rows = JSON.parse((await feed.get(`${st}.json`)) ?? "null")?.daily;
    } catch {
      continue;
    }
    const got: Record<string, number> = {};
    for (const r of Array.isArray(rows) ? rows : []) if (Array.isArray(r) && r.length >= 3 && r[2] !== null && r[2] !== undefined) got[String(r[0])] = Number(r[2]);
    if (Object.keys(got).length) out[st] = got;
  }
  return out;
}

/** skill.clim_ranks: a station's day-ranking against its discharge envelope (the publisher's rankDay). */
export function climRanks(clim: Record<string, any> | null): (st: string) => Rank | null {
  const stations = (clim || {}).stations || {};
  return (st) => {
    const env = (stations[st] || {}).discharge;
    if (!truthy(env)) return null;
    return (v, d) => percentileOf(v, bandFor(env, dayMs(d)! + 12 * HOUR));
  };
}

/** One archive month's records, as Python's `read_runs` would read the concatenated month file: `runs/<month>.jsonl`,
 * then that month's day files and parts still listed in runs/state.json, in key order. A torn line is skipped. */
export async function readMonth(priv: Store, month: string): Promise<{ records: Rec[]; objects: number }> {
  let state: Partial<RunsState> = {};
  try {
    state = JSON.parse((await priv.get(RUNS_STATE)) ?? "{}") ?? {};
  } catch {
    state = {};
  }
  const inMonth = (k: string) => k.startsWith(`runs/${month}/`);
  const keys = [`runs/${month}.jsonl`, ...[...(state.days ?? []), ...(state.parts ?? [])].filter(inMonth)].sort();
  const records: Rec[] = [];
  let objects = 0;
  for (const k of [...new Set(keys)]) {
    const text = await priv.get(k);
    if (text === null) continue;
    objects++;
    for (const line of splitlines(text)) {
      try {
        records.push(JSON.parse(line));
      } catch {
        /* torn */
      }
    }
  }
  return { records, objects };
}

/** The month before `month` ("YYYY-MM"). */
export function prevMonth(month: string): string {
  const y = Number(month.slice(0, 4));
  const m = Number(month.slice(5, 7));
  return m === 1 ? `${y - 1}-12` : `${y}-${String(m - 1).padStart(2, "0")}`;
}

export interface SkillReport {
  at: string;
  /** the archive month scored */
  month: string;
  /** cases are days strictly before this Centre day */
  before: string;
  runs: number;
  objects: number;
  stations: number;
  cases: number;
  table: Record<string, any>;
  verdict: Record<string, { weights: Record<string, number>; why: string }>;
  /** the weights the feed ships now (change.FLOOD_WEIGHTS) and whether any lead's verdict differs from them */
  current: Record<string, number>;
  differs: boolean;
}

/** score + verdict over records (what the CLI prints, plus the counts). */
export function scoreRecords(records: Rec[], observed: Record<string, Record<string, number>>, clim: Record<string, any> | null, before: string) {
  const found = cases(records, observed, before);
  const table = score(found, climRanks(clim));
  const v: Record<string, { weights: Record<string, number>; why: string }> = {};
  for (const [lead, row] of Object.entries(table)) v[lead] = verdict(row);
  return { runs: records.length, cases: found.length, table, verdict: v };
}

/** THE MONTHLY JOB: score the archive month before `now`'s Centre month. Reads one month of runs (private), the scored
 * stations' files and clim.json (published); writes nothing — the caller records and reports. */
export async function runSkill(a: { feed: Store; priv: Store; now: Date; month?: string; weights: Record<string, number> }): Promise<SkillReport> {
  const today = dayOf(centreWall(a.now.getTime()));
  const month = a.month ?? prevMonth(today.slice(0, 7));
  const { records, objects } = await readMonth(a.priv, month);
  const scored = new Set(records.filter((r) => r.scale !== null && r.scale !== undefined && r.station).map((r) => String(r.station)));
  const observed = await feedObserved(a.feed, scored);
  let clim: Record<string, any> | null = null;
  try {
    const c = JSON.parse((await a.feed.get("clim.json")) ?? "null");
    // keep only the scored stations' envelopes (the parse is the cost; the rest is dropped at once)
    if (c && typeof c === "object") clim = { stations: Object.fromEntries([...scored].filter((s) => c.stations?.[s]).map((s) => [s, { discharge: c.stations[s].discharge }])) };
  } catch {
    clim = null;
  }
  const r = scoreRecords(records, observed, clim, today);
  const differs = Object.values(r.verdict).some((v) => Object.entries(v.weights).some(([m, w]) => w !== a.weights[m]));
  return { at: a.now.toISOString(), month, before: today, runs: r.runs, objects, stations: scored.size, cases: r.cases, table: r.table, verdict: r.verdict, current: { ...a.weights }, differs };
}

/** The report in plain words (the email / webhook text). */
export function skillText(s: SkillReport): string {
  const lines = [`Forecast model scores for ${s.month}: ${s.runs} archived runs, ${s.cases} scored cases at ${s.stations} stations.`, ""];
  for (const [lead, v] of Object.entries(s.verdict)) {
    const d = s.table[lead]?.paired?.difference;
    lines.push(`Lead ${lead} d: ${v.why} → CLEVER ${v.weights.CLEVER}, COFFEE ${v.weights.COFFEE}${d ? ` (n ${d.n})` : ""}`);
  }
  lines.push("", `The feed ships CLEVER ${s.current.CLEVER}, COFFEE ${s.current.COFFEE}.`);
  lines.push(
    s.differs
      ? "A lead's verdict differs from the shipped weights. Nothing changes by itself: to apply, edit FLOOD_WEIGHTS in pipeline/gauges/feed/change.py and run python -m pipeline.tools.emit.gauge_policy (T178, T557), with this table in decisions.md, then deploy the worker."
      : "Every verdict matches the shipped weights: nothing to do.",
  );
  lines.push("", "The full table: GET /status.json (skill).");
  return lines.join("\n");
}
