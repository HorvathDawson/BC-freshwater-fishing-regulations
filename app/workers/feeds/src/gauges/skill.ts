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

const key = (r: Record<string, any>): string => `${String(r.station)}\u0000${String(r.model)}\u0000${String(r.issuedAt)}`;

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

/** skill.append_runs over a private Store. Returns `{added, file, bytes}`. */
export async function appendRuns(priv: DeletableStore, forecast: Summary | null, now: number, observed: Map<string, number | null>): Promise<Record<string, any>> {
  const local = centreWall(now);
  const month = monthOf(local);
  const prev = shiftMonth(month, -1);
  const seen = new Set<string>();
  const texts: Record<string, string | null> = {};
  for (const m of [prev, month]) {
    const text = await priv.get(`runs/${m}.jsonl`);
    texts[m] = text;
    for (const line of splitlines(text ?? "")) {
      try {
        seen.add(key(JSON.parse(line)));
      } catch {
        /* a torn line is skipped */
      }
    }
  }
  const lines: string[] = [];
  for (const st of Object.keys(forecast || {}).sort()) {
    const runs = forecast![st] || {};
    for (const model of Object.keys(runs).sort()) {
      const rec = runRecord(st, model, runs[model]!, now, observed.get(st));
      if (rec === null || seen.has(key(rec))) continue;
      seen.add(key(rec));
      lines.push(dumpsSorted(rec));
    }
  }
  const path = `runs/${month}.jsonl`;
  let body = texts[month] ?? null;
  if (lines.length) {
    body = (body ?? "") + lines.join("\n") + "\n";
    await priv.put(path, body, { contentType: "application/x-ndjson" });
  }
  if (priv.delete) await priv.delete(`runs/${shiftMonth(month, -ARCHIVE_MONTHS)}.jsonl`);
  return { added: lines.length, file: path, bytes: body === null ? 0 : new TextEncoder().encode(body).length };
}
