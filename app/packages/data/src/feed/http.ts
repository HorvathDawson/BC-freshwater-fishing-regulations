/**
 * The live gauge feed, over HTTP.
 *
 * ONE IMPLEMENTATION, TWO ORIGINS. In dev the base URL is the local tile server serving
 * files that `python -m pipeline.hydro.publish` wrote; in production it is the R2 bucket the
 * cron syncs that same directory to. The bytes are identical, so a bug cannot hide on one
 * side — and nothing in the app ever talks to ECCC directly, which is what stops a browser
 * full of users hammering the source we scrape.
 *
 * ADDRESSED BY STATION ID AND NOTHING ELSE. `{base}/index.json` and `{base}/{station}.json`
 * — no query strings, no matching, no context. That is what makes the feed cacheable at the
 * edge and what lets the publisher stay ignorant of the bundle.
 *
 * FAILURE IS AN ANSWER HERE. Every method resolves to `null` rather than throwing: offline
 * is the ordinary case for this app, and a rejected promise on a fishing trip turns into an
 * error screen where "we could not check" is the truth. `live()` returning `null` is
 * distinct from returning an empty set — unknown, versus known to be nobody.
 */
// `standing` comes from core rather than being restated here. A second copy of the
// thresholds is a second answer to "is this river low", and the feed and the sheet
// would eventually disagree about the same reading (AGENTS rule 23).
import { standing } from "@app/core";
import type { Aged, Forecast, Parameter, Reading, StationId } from "../index";

/** The forecast exactly as the publisher writes it — both quantities, unpicked. */
interface RawForecast extends Omit<Forecast, "series" | "disclaimer"> {
  series?: {
    step: "1h" | "1d";
    at: string[];
    mid: (number | null)[]; lo: (number | null)[]; hi: (number | null)[];
    level_mid: (number | null)[];
    level_lo: (number | null)[];
    level_hi: (number | null)[];
    disclaimer?: string;
  } | null;
}

/** How long a fetched file is reused. The publisher runs every 30 minutes. */
const TTL_MS = 5 * 60_000;

interface IndexFile {
  fetchedAt: string;
  /** Which HYDAT release every percentile in this file was measured against. */
  hydat?: { release: string | null; latest: string | null; stale: boolean };
  stations: Record<string, {
    percentile: number | null;
    observedAt: string | null;
    forecast: (number | null)[] | null;
  }>;
}

/** How long a station's record runs — the weight behind "below normal for the date". */
export interface GaugeRecord {
  fromYear: number;
  toYear: number;
  years: number;
}

interface StationFile {
  fetchedAt: string;
  station: string;
  now: { discharge: number | null; level: number | null; at: string | null;
         percentile: number | null; parameter?: Parameter };
  /** `[timestamp, level, discharge]`, thinned to 30-minute steps by the publisher. */
  recent?: [string, number | null, number | null][];
  /** Every BC River Forecast Centre run for this station, keyed by model name. */
  forecasts?: Record<string, RawForecast> | null;
  /** `[day, level, discharge]` daily means for the current year, grown by the feed. */
  daily?: [string, number | null, number | null][];
  /** `{parameter: {year: [366 daily values]}}` from the same HYDAT release as the envelope. */
  priorYears?: Record<string, Record<string, (number | null)[]>> | null;
}

/**
 * The observations, with no envelope on them.
 *
 * THE FEED CANNOT BUILD A `Series` AND MUST NOT PRETEND TO. Half of one — the percentile
 * envelope — lives in the bundle, because it is derived from a HYDAT release and changes
 * once a year, not once every thirty minutes. Splitting them this way is what lets the
 * feed stay a set of static files addressed by station id and nothing else. The source
 * joins the two halves; see `gaugeSeries` in `bundle/source.ts`.
 */
export interface Observations {
  fetchedAt: number;
  from: string;
  /** Which quantity the station itself leads with, when the caller did not pick one. */
  parameter: Parameter;
  /** ISO timestamps, one per sample — the source needs them to align the envelope. */
  at: readonly string[];
  discharge: readonly (number | null)[];
  level: readonly (number | null)[];
  /** This calendar year's daily means: `[day, level, discharge]`, oldest first. */
  daily: readonly [string, number | null, number | null][];
  /**
   * Every run, with BOTH quantities still in each.
   *
   * Picked apart by the source, not here, for the same reason `discharge` and `level` both
   * come back: the feed does not know which chart is being drawn, and ELF publishes a level
   * forecast beside its discharge one. Choosing here would throw away the other half.
   */
  forecasts: Record<string, RawForecast>;
  /** `{parameter: {year: [366 values]}}` — the recent complete years, unpicked. */
  priorYears: Record<string, Record<string, (number | null)[]>>;
}

/**
 * The forecast in ONE quantity, ready for a chart.
 *
 * Kept out of the feed and applied by the source, because the answer depends on which
 * chart is being drawn. CLEVER publishes discharge only; ELF publishes both. A level chart
 * asking CLEVER gets null, which is correct — its outlook is in m3/s.
 */
export function forecastFor(raw: RawForecast | null | undefined,
                            parameter: Parameter): Forecast | null {
  if (!raw) return null;
  const s = raw.series ?? null;
  const level = parameter === "level";
  const mid = level ? s?.level_mid : s?.mid;
  const lo = level ? s?.level_lo : s?.lo;
  const hi = level ? s?.level_hi : s?.hi;
  const has = (mid ?? []).some((v) => v !== null && Number.isFinite(v));
  // The HEADLINE number is discharge in every model, so a level chart keeps the ribbon and
  // drops the summary rather than printing "8.2 m" over a run measured in m3/s.
  return {
    ...raw,
    value: level ? Number.NaN : raw.value,
    min: level ? null : raw.min, ave: level ? null : raw.ave, max: level ? null : raw.max,
    unit: level ? "m" : raw.unit,
    series: s && has
      ? { step: s.step, at: s.at, mid: mid!, lo: lo ?? [], hi: hi ?? [] }
      : null,
    disclaimer: s?.disclaimer ?? null,
  };
}

export interface GaugeFeed {
  now(station: StationId): Promise<Aged<Reading> | null>;
  /** Raw recent observations. The envelope is the bundle's half — see `Observations`. */
  observations(station: StationId): Promise<Observations | null>;
  live(): Promise<ReadonlySet<string> | null>;
  /** The whole index, for colouring the map. Null when it could not be fetched. */
  index(): Promise<IndexFile | null>;
  /**
   * How long this station's record is.
   *
   * Shown NEXT TO a percentile, never instead of it. "4th percentile" backed by 97 years
   * and by 11 are different claims, and only one of them is worth acting on — a reader who
   * cannot see which is being made will assume the stronger one.
   */
  record(station: StationId): Promise<GaugeRecord | null>;
}

export function httpFeed(base: string, fetchImpl: typeof fetch = fetch): GaugeFeed {
  const trimmed = base.replace(/\/+$/, "");
  // One in-flight request per URL, and a short-lived result cache. Without the first, a
  // screen that asks three components for the same station opens three connections.
  const inflight = new Map<string, Promise<unknown>>();
  const cache = new Map<string, { at: number; value: unknown }>();

  async function load<T>(path: string): Promise<T | null> {
    const url = `${trimmed}/${path}`;
    const hit = cache.get(url);
    if (hit && Date.now() - hit.at < TTL_MS) return hit.value as T;

    let p = inflight.get(url) as Promise<T | null> | undefined;
    if (!p) {
      p = (async () => {
        try {
          const res = await fetchImpl(url);
          if (!res.ok) return null;          // 404 = this station is not transmitting
          const value = (await res.json()) as T;
          cache.set(url, { at: Date.now(), value });
          return value;
        } catch {
          return null;                        // offline — an answer, not an exception
        } finally {
          inflight.delete(url);
        }
      })();
      inflight.set(url, p);
    }
    return p;
  }

  const stamp = (s: string | undefined) => (s ? Date.parse(s) : Date.now());

  return {
    index: () => load<IndexFile>("index.json"),

    async record(station) {
      const f = await load<{ stations: Record<string, {
        from_year: number; to_year: number; years: number }> }>("stats.json");
      const r = f?.stations?.[station];
      return r ? { fromYear: r.from_year, toYear: r.to_year, years: r.years } : null;
    },

    async live() {
      const idx = await load<IndexFile>("index.json");
      // null, not an empty set. "We could not check" and "no gauge in BC is reporting" are
      // different claims, and only one of them has ever been true.
      return idx ? new Set(Object.keys(idx.stations)) : null;
    },

    async now(station) {
      const f = await load<StationFile>(`${station}.json`);
      if (!f?.now || (f.now.discharge === null && f.now.level === null)) return null;
      return {
        fetchedAt: stamp(f.fetchedAt),
        value: {
          discharge: f.now.discharge, level: f.now.level,
          at: f.now.at ?? "", percentile: f.now.percentile,
          standing: standing(f.now.percentile),
        },
      };
    },

    async observations(station) {
      const f = await load<StationFile>(`${station}.json`);
      const rows = f?.recent ?? [];
      if (!rows.length) return null;
      return {
        fetchedAt: stamp(f?.fetchedAt),
        from: rows[0]![0],
        // The publisher already decided which quantity this station's percentile is about,
        // against the envelope it actually has. Re-deciding here would be a second opinion
        // on a question that has one right answer per station.
        parameter: f?.now?.parameter
          ?? (f?.now?.discharge !== null && f?.now?.discharge !== undefined
                ? "discharge" : "level"),
        at: rows.map((r) => r[0]),
        level: rows.map((r) => r[1]),
        discharge: rows.map((r) => r[2]),
        daily: f?.daily ?? [],
        forecasts: f?.forecasts ?? {},
        priorYears: f?.priorYears ?? {},
      };
    },
  };
}
