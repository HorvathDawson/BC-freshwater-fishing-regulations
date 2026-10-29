// T225 port of the part of pipeline/gauges/feed/skill.py the publisher calls: `append_runs`, the forecast-run archive
// kept for scoring the models (T177). PRIVATE store, never the published prefix: `runs/<YYYY-MM>.jsonl` (Python's
// `<feed dir>.runs/<YYYY-MM>.jsonl`). The scoring CLI stays Python and reads a copy of these files.
//
// DIFFERENCES FROM THE PYTHON, both about closed months (none touch a current run's lines): a Store holds text, so a
// closed month is NOT gzipped (Python: `<YYYY-MM>.jsonl.gz`); and retention deletes the month that just fell out of
// the ARCHIVE_MONTHS window by name — only when the store can delete (`delete`, R2 can) — instead of listing.
import type { Store } from "../io";
import { AHEAD_DAYS, dayMeans, gaugeScale, stale } from "./change";
import type { Run, Summary } from "./forecast";
import { dumpsSorted, pyRound, splitlines } from "./py";
import { addDays, centreWall, dayOf, wallIso } from "./time";

export const ARCHIVE_MONTHS = 14;

export interface DeletableStore extends Store {
  delete?(key: string): Promise<void>;
}

export function runRecord(station: string, model: string, run: Run, now: number, observed: number | null | undefined): Record<string, any> | null {
  const series = (run || {}).series;
  if (!series || !Object.keys(series).length || stale(run, now)) return null;
  const today = dayOf(centreWall(now));
  const last = addDays(today, AHEAD_DAYS);
  const days: Record<string, number> = {};
  for (const [d, v] of [...dayMeans(series)].sort((a, b) => (a[0] < b[0] ? -1 : 1))) {
    if (today <= d && d <= last) days[d] = pyRound(v, 4);
  }
  if (!Object.keys(days).length) return null;
  const s = gaugeScale(model, run, now, observed ?? null);
  return {
    station, model, issuedAt: run.issuedAt ?? null,
    at: wallIso(now) + "Z",
    observed: observed ?? null, scale: s === null ? null : pyRound(s, 6), days,
  };
}

function monthOf(wall: number): string {
  return dayOf(wall).slice(0, 7);
}

function shiftMonth(month: string, n: number): string {
  let y = Number(month.slice(0, 4));
  let m = Number(month.slice(5, 7)) - 1 + n;
  y += Math.floor(m / 12);
  m = ((m % 12) + 12) % 12;
  return `${String(y).padStart(4, "0")}-${String(m + 1).padStart(2, "0")}`;
}

/** The archive's small private state: the last issue times archived per station+model (the dedupe), the per-run part
 * objects of the open day, and the compacted day objects of the open month. */
export interface RunsState {
  v: 1;
  last: Record<string, string[]>;
  parts: string[];
  days: string[];
}
export const RUNS_STATE = "runs/state.json";
const LAST_KEEP = 8;

/** skill.append_runs, APPEND-ONLY (T225 follow-up). Python (and this port until 2026-10-28) read the whole month file
 * and rewrote it every run (~8 MB late in a month). Now each run writes ONE small object, never rewritten:
 *   runs/<YYYY-MM>/<YYYY-MM-DD>/<HHMMSS>.jsonl   this run's new lines (exactly the lines Python appended)
 * and a compaction step keeps the object count down:
 *   a closed day's parts   → runs/<YYYY-MM>/<YYYY-MM-DD>.jsonl   (parts deleted)
 *   a closed month's days  → runs/<YYYY-MM>.jsonl                (days deleted; the same file Python keeps — a legacy
 *                                                                 month file written before the switch is its head)
 * The dedupe reads only runs/state.json (the last LAST_KEEP issue times per station+model), seeded once from the
 * legacy month files. Concatenating month file + day files + parts in key order gives Python's month file byte for
 * byte (the parity test does exactly that). Retention: the month ARCHIVE_MONTHS back is deleted, as before. */
export async function appendRuns(priv: DeletableStore, forecast: Summary | null, now: number, observed: Map<string, number | null>): Promise<Record<string, any>> {
  const local = centreWall(now);
  const month = monthOf(local);
  const today = dayOf(local);
  let state: RunsState | null = null;
  try {
    const t = await priv.get(RUNS_STATE);
    if (t !== null) state = JSON.parse(t);
  } catch {
    state = null;
  }
  if (!state || state.v !== 1) {
    // first run on this layout: seed the dedupe from the legacy month files (read once, never again)
    state = { v: 1, last: {}, parts: [], days: [] };
    for (const m of [shiftMonth(month, -1), month])
      for (const line of splitlines((await priv.get(`runs/${m}.jsonl`)) ?? "")) {
        try {
          remember(state, JSON.parse(line));
        } catch {
          /* a torn line is skipped */
        }
      }
  }
  const seen = (r: Record<string, any>) => (state!.last[`${r.station}\u0000${r.model}`] ?? []).includes(String(r.issuedAt));
  const lines: string[] = [];
  for (const st of Object.keys(forecast || {}).sort()) {
    const runs = forecast![st] || {};
    for (const model of Object.keys(runs).sort()) {
      const rec = runRecord(st, model, runs[model]!, now, observed.get(st));
      if (rec === null || seen(rec)) continue;
      remember(state, rec);
      lines.push(dumpsSorted(rec));
    }
  }
  let file: string | null = null;
  let bytes = 0;
  if (lines.length) {
    const body = lines.join("\n") + "\n";
    file = `runs/${month}/${today}/${wallIso(now).slice(11, 19).replace(/:/g, "")}.jsonl`;
    await priv.put(file, body, { contentType: "application/x-ndjson" }); // the part FIRST: a lost state write repeats lines, never loses them
    if (!state.parts.includes(file)) state.parts.push(file);
    bytes = new TextEncoder().encode(body).length;
  }
  const compacted = await compact(priv, state, today, month);
  await priv.put(RUNS_STATE, JSON.stringify(state));
  if (priv.delete) await priv.delete(`runs/${shiftMonth(month, -ARCHIVE_MONTHS)}.jsonl`);
  return { added: lines.length, file, bytes, ...compacted };
}

function remember(state: RunsState, r: Record<string, any>): void {
  const k = `${r.station}\u0000${r.model}`;
  const l = (state.last[k] ??= []);
  const v = String(r.issuedAt);
  if (!l.includes(v)) l.push(v);
  if (l.length > LAST_KEEP) l.splice(0, l.length - LAST_KEEP);
}

/** Fold closed days' parts into day objects and closed months' days into the month file. */
export async function compact(priv: DeletableStore, state: RunsState, today: string, month: string): Promise<{ compactedDays: number; compactedMonths: number }> {
  let compactedDays = 0;
  let compactedMonths = 0;
  const dayOfPart = (k: string) => k.split("/")[2]!;
  const closedDays = [...new Set(state.parts.map(dayOfPart))].filter((d) => d < today).sort();
  for (const d of closedDays) {
    const parts = state.parts.filter((k) => dayOfPart(k) === d).sort();
    const dayKey = `runs/${d.slice(0, 7)}/${d}.jsonl`;
    let body = (await priv.get(dayKey)) ?? "";
    for (const k of parts) body += (await priv.get(k)) ?? "";
    await priv.put(dayKey, body, { contentType: "application/x-ndjson" });
    if (priv.delete) for (const k of parts) await priv.delete(k);
    state.parts = state.parts.filter((k) => !parts.includes(k));
    if (!state.days.includes(dayKey)) state.days.push(dayKey);
    compactedDays++;
  }
  const monthOfDay = (k: string) => k.split("/")[1]!;
  const closedMonths = [...new Set(state.days.map(monthOfDay))].filter((m) => m < month).sort();
  for (const m of closedMonths) {
    const days = state.days.filter((k) => monthOfDay(k) === m).sort();
    const monthKey = `runs/${m}.jsonl`;
    let body = (await priv.get(monthKey)) ?? "";
    for (const k of days) body += (await priv.get(k)) ?? "";
    await priv.put(monthKey, body, { contentType: "application/x-ndjson" });
    if (priv.delete) for (const k of days) await priv.delete(k);
    state.days = state.days.filter((k) => !days.includes(k));
    compactedMonths++;
  }
  return { compactedDays, compactedMonths };
}
