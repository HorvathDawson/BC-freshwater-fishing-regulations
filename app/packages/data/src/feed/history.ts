/**
 * (T315) THE GAUGES' LAST WEEK, KEPT ON THE PHONE.
 *
 * owner 2026-10-31: "in offline mode we didn't get the gauge feed data … the phone didn't store the gauge data for the
 * last week … had no measured line". The feed (`./http.ts`) kept its last good copy of a file for the SESSION only; a
 * phone opened offline had nothing, and the Conditions chart drew no measured line.
 *
 * Two things are kept, in one small file of the app's own (never inside a release):
 *   rows   EVERY station's observations (`[time, level, discharge]`, the feed's own rows), merged from each station
 *          file the app reads and from the feed-wide `obs.json` the publisher writes each run (the run's new rows for
 *          every station: a few kB gzipped), so a station never opened still has its week. Older than `KEEP_MS`: dropped.
 *   files  the last whole station file for the stations actually viewed (its forecast, daily means, reading), the
 *          `FILES` most recently used; the rest are dropped.
 * Offline, `file(station)` answers with the kept file whose `recent` is the kept rows (or a file made of rows alone)
 * and whose `fetchedAt` is the newest copy's, so every age line says how old it is (P02).
 *
 * Bounded: 438 stations × 8 days × 48 rows ≈ 170k rows, stored as minutes + numbers (≈ 2–3 MB of text at most), plus
 * the kept files (≈ 40 kB each).
 */
import type { StationFile } from "./http";

export type ObsRow = readonly [string, number | null, number | null];

/** What the publisher writes beside the index each run (`obs.json`): every station's newest rows. */
export interface ObsFile { fetchedAt: string; stations: Record<string, readonly ObsRow[]> }

export interface HistoryText { read(): string | null; write(text: string): void }

/** How long a row is kept: a week, and a day of slack so "the last 7 days" is always whole. */
export const KEEP_MS = 8 * 86_400_000;
/** How many whole station files are kept (the most recently viewed). */
export const FILES = 24;

interface StationRows {
  /** the zone suffix the feed writes its times in ("-08:00"), so a row comes back as it came in */
  z: string;
  /** epoch minutes, ascending */
  m: number[];
  l: (number | null)[];
  q: (number | null)[];
}
interface Disk {
  v: 1;
  /** per station, the newest copy's `fetchedAt` (epoch ms) */
  at: Record<string, number>;
  rows: Record<string, StationRows>;
  files: Record<string, { used: number; file: StationFile }>;
}

export interface GaugeHistory {
  /** A station file read from the network: its rows join the week, the file is kept (the most recent `FILES`). */
  noteFile(station: string, file: StationFile): void;
  /** The feed-wide newest rows (`obs.json`): every station's week grows. */
  noteObs(obs: ObsFile): void;
  /** What offline answers for a station: the kept file with the week's rows, or a file of rows alone; null: nothing. */
  file(station: string): StationFile | null;
  /** Write anything waiting now (tests; the app writes a moment after the last change). */
  flush(): void;
}

const zoneOf = (iso: string): string => /([+-]\d\d:\d\d|Z)$/.exec(iso)?.[1] ?? "Z";
const pad = (n: number) => String(n).padStart(2, "0");
/** An epoch minute written back in the zone the feed used ("2026-10-30T13:30:00-08:00"). */
function isoIn(min: number, z: string): string {
  const sign = z === "Z" ? 0 : z[0] === "-" ? -1 : 1;
  const off = z === "Z" ? 0 : sign * (Number(z.slice(1, 3)) * 60 + Number(z.slice(4, 6)));
  const d = new Date((min + off) * 60_000);
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}T${pad(d.getUTCHours())}:` +
         `${pad(d.getUTCMinutes())}:00${z}`;
}

function empty(): Disk { return { v: 1, at: {}, rows: {}, files: {} }; }

function parse(text: string | null): Disk {
  if (!text) return empty();
  try {
    const d = JSON.parse(text) as Partial<Disk>;
    if (d?.v !== 1 || typeof d.rows !== "object" || typeof d.files !== "object" || typeof d.at !== "object") return empty();
    return d as Disk;
  } catch {
    return empty();     // a cache: unreadable reads as nothing kept
  }
}

export function gaugeHistory(store: HistoryText | null, o: { now?: () => number; writeAfterMs?: number } = {}): GaugeHistory {
  const now = o.now ?? Date.now;
  let disk: Disk | null = null;
  const d = (): Disk => (disk ??= parse(safeRead(store)));
  let timer: ReturnType<typeof setTimeout> | null = null;
  const save = () => {
    if (timer) { clearTimeout(timer); timer = null; }
    if (!store || !disk) return;
    try { store.write(JSON.stringify(disk)); } catch { /* a cache: a failed write is the next one's to make */ }
  };
  const later = () => {
    if (timer) clearTimeout(timer);
    timer = setTimeout(save, o.writeAfterMs ?? 2_000);
  };

  function merge(station: string, rows: readonly ObsRow[] | null | undefined, fetchedAt: number): boolean {
    const cut = Math.floor((now() - KEEP_MS) / 60_000);
    const had = d().rows[station];
    const by = new Map<number, [number | null, number | null]>();
    if (had) had.m.forEach((m, i) => { if (m >= cut) by.set(m, [had.l[i] ?? null, had.q[i] ?? null]); });
    let z = had?.z ?? null;
    for (const r of rows ?? []) {
      const t = Date.parse(r[0]);
      if (!Number.isFinite(t) || (r[1] === null && r[2] === null)) continue;
      const m = Math.round(t / 60_000);
      if (m < cut) continue;
      z ??= zoneOf(r[0]);
      by.set(m, [r[1], r[2]]);
    }
    if (Number.isFinite(fetchedAt)) d().at[station] = Math.max(d().at[station] ?? 0, fetchedAt);
    if (!by.size) { delete d().rows[station]; return !!had; }
    const m = [...by.keys()].sort((a, b) => a - b);
    d().rows[station] = { z: z ?? "Z", m, l: m.map((k) => by.get(k)![0]), q: m.map((k) => by.get(k)![1]) };
    return true;
  }

  function rowsOf(station: string): [string, number | null, number | null][] {
    const r = d().rows[station];
    if (!r) return [];
    const cut = Math.floor((now() - KEEP_MS) / 60_000);
    return r.m.flatMap((m, i): [string, number | null, number | null][] => (m >= cut ? [[isoIn(m, r.z), r.l[i] ?? null, r.q[i] ?? null]] : []));
  }

  return {
    noteFile(station, file) {
      const at = Date.parse(file.fetchedAt);
      merge(station, file.recent, at);
      const files = d().files;
      files[station] = { used: now(), file: { ...file, recent: [] } };      // its rows are kept once, above
      const keys = Object.keys(files);
      if (keys.length > FILES) {
        keys.sort((a, b) => files[a]!.used - files[b]!.used);
        for (const k of keys.slice(0, keys.length - FILES)) delete files[k];
      }
      later();
    },
    noteObs(obs) {
      const at = Date.parse(obs.fetchedAt);
      let changed = false;
      for (const [station, rows] of Object.entries(obs.stations ?? {})) changed = merge(station, rows, at) || changed;
      // a station not in this run's file keeps its rows until they age out; prune those here too
      for (const station of Object.keys(d().rows)) if (!(station in (obs.stations ?? {}))) changed = merge(station, [], NaN) || changed;
      if (changed) later();
    },
    file(station) {
      const kept = d().files[station]?.file ?? null;
      const recent = rowsOf(station);
      if (!kept && !recent.length) return null;
      const at = d().at[station];
      const fetchedAt = at ? new Date(at).toISOString() : kept?.fetchedAt ?? new Date(0).toISOString();
      const last = recent[recent.length - 1];
      const keptNow = kept?.now ?? null;
      // a row newer than the kept reading is the reading now (its standing is the kept file's to say, so none)
      const newer = last && (!keptNow?.at || Date.parse(last[0]) > Date.parse(keptNow.at));
      const nowRow = newer ? { discharge: last[2], level: last[1], at: last[0], percentile: null,
                               ...(keptNow?.parameter ? { parameter: keptNow.parameter } : {}) }
        : keptNow ?? { discharge: null, level: null, at: null, percentile: null };
      const base = kept ?? { station, daily: [], forecasts: null, priorYears: null };
      return { ...base, fetchedAt, station, now: nowRow, recent, daily: withDays(base.daily ?? [], recent), kept: true };
    },
    flush: save,
  };
}

/**
 * The kept daily means, and after the last of them each WHOLE day the kept rows cover (the mean of its rows, in the
 * feed's own day): the year chart of a station never opened, or opened days ago, still reaches the last full day. The
 * last day the rows reach is not over in them, so it is never made a mean.
 */
export function withDays(daily: readonly (readonly [string, number | null, number | null, ...unknown[]])[],
                         recent: readonly ObsRow[]): [string, number | null, number | null][] {
  const out = daily.map((d) => [d[0], d[1], d[2]] as [string, number | null, number | null]);
  if (!recent.length) return out;
  const lastKept = out.length ? out[out.length - 1]![0] : "";
  const lastRowDay = recent[recent.length - 1]![0].slice(0, 10);
  const sums = new Map<string, { l: number; nl: number; q: number; nq: number }>();
  for (const [t, l, q] of recent) {
    const day = t.slice(0, 10);
    if (day <= lastKept || day >= lastRowDay) continue;
    const a = sums.get(day) ?? { l: 0, nl: 0, q: 0, nq: 0 };
    if (l !== null) { a.l += l; a.nl++; }
    if (q !== null) { a.q += q; a.nq++; }
    sums.set(day, a);
  }
  const r3 = (v: number) => Math.round(v * 1000) / 1000;
  for (const day of [...sums.keys()].sort()) {
    const a = sums.get(day)!;
    out.push([day, a.nl ? r3(a.l / a.nl) : null, a.nq ? r3(a.q / a.nq) : null]);
  }
  return out;
}

function safeRead(store: HistoryText | null): string | null {
  try { return store?.read() ?? null; } catch { return null; }
}
