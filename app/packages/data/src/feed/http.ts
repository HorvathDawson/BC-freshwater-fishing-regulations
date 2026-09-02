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
import type { Aged, Reading, Series, StationId } from "../index";

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
         percentile: number | null };
  recent?: [string, number | null, number | null][];
  forecast?: (number | null)[] | null;
}

export interface GaugeFeed {
  now(station: StationId): Promise<Aged<Reading> | null>;
  series(station: StationId, span: "72h" | "year"): Promise<Aged<Series> | null>;
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

    async series(station, span) {
      const f = await load<StationFile>(`${station}.json`);
      const rows = f?.recent ?? [];
      if (!rows.length) return null;
      // Only the fine recent series is published today; a "year" request has no source yet
      // and says so rather than returning the 72 h series relabelled.
      if (span === "year") return null;
      return {
        fetchedAt: stamp(f?.fetchedAt),
        value: {
          step: "1h",
          from: rows[0]![0],
          discharge: rows.map((r) => r[2]),
          band: rows.map(() => null),
        },
      };
    },
  };
}
