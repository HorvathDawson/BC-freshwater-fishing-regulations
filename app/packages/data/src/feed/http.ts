/**
 * The live gauge feed, over HTTP.
 *
 * ONE IMPLEMENTATION, TWO ORIGINS. In dev the base URL is the local tile server serving
 * files that `python -m pipeline.gauges.feed.publish` wrote; in production it is the R2 bucket the
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
  /**
   * The station ids ECCC listed as real-time on the publishing run (`publish.py`). A panel member absent from it is
   * "not a real-time gauge" rather than "not reporting". Absent on an older feed: every station reads as listed.
   */
  listed?: string[];
  stations: Record<string, {
    percentile: number | null;
    observedAt: string | null;
    forecast: (number | null)[] | null;
    /**
     * BOTH PERCENTILES, each against its own envelope — `percentile` above is only the
     * station's own default. A regulated river can sit at its normal STAGE while its
     * discharge is in the bottom tenth, because the dam is holding the pond and letting
     * nothing through, so one number per station makes the two impossible to offer
     * honestly. Either may be absent: 361 stations publish a discharge percentile and 419
     * a level, and they are not nested sets.
     */
    discharge?: number | null;
    level?: number | null;
    /** Which quantity `percentile` above is about — the station's own default. */
    parameter?: Parameter;
    /**
     * DEGREES, NOT A RANKING — and that asymmetry against every other value in this file
     * is deliberate. There is no historical water-temperature record anywhere to rank
     * against: HYDAT carries level, flow and sediment and nothing else. And a ranking
     * would be the wrong shape anyway. "Unusually warm for early September" is a fact
     * about the weather; "20 degrees" is a fact about whether a released fish survives,
     * and it is the second that closes rivers in this province.
     *
     * `temperatureBand` is a policy reading of the number, not a measurement — see
     * `_TEMP_BANDS` in the publisher, which says plainly that it is a placeholder until
     * the real per-river thresholds are curated beside the regulations.
     */
    temperatureC?: number | null;
    temperatureAt?: string | null;
    temperatureBand?: "cool" | "warm" | "critical" | null;
  }>;
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
  /**
   * `[day, level, discharge]` daily means for the current year, grown by the feed. Days older than ECCC's 30-day
   * archive, filled by `pipeline.gauges.feed.year_backfill`, carry a fourth element, "final" (HYDAT / approved) or
   * "provisional"; nothing reads it yet, and a missing day is absent (the chart breaks its line there).
   */
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
  /** When our copy was fetched; null when the file does not say (P02: unknown age, never "now"). */
  fetchedAt: number | null;
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
  /** Whether the feed actually carries this station — see the note on the implementation. */
  published(station: string): Promise<boolean>;
  /**
   * When the station last reported (ISO instant, or a bare "YYYY-MM-DD" when only a daily mean survives), from its own
   * file — read EVEN WHEN THE INDEX HAS DROPPED IT, because a dropped station is exactly the quiet one whose last report
   * the reader asked for (owner 2026-10-21). Null ONLY when the file was read and holds nothing — the station has no
   * data at all. Undefined when the file could not be read (404, offline): that is "not known", never "nothing", and
   * the silent line must not say "has never reported" from it (review 2026-10-22).
   */
  lastReport(station: StationId): Promise<string | null | undefined>;
  /**
   * (Owner 2026-10-22, the sync icon) Forget what was fetched, so the next ask goes to the network rather than the
   * short-lived cache. Optional: a feed with no cache (a fixture) has nothing to forget.
   */
  refresh?(): void;
}

type Row = readonly [string, number | null, number | null];

/**
 * THE LAST REPORT IN A STATION FILE: its reading's own time (`now.at`), else the last recent row that holds a value,
 * else the last daily mean's day. Pure, so the order is pinned in a test rather than spelled in a fetch.
 */
export function lastReportAt(f: { now?: { at?: string | null } | null; recent?: readonly Row[] | null;
                                  daily?: readonly Row[] | null } | null | undefined): string | null {
  if (!f) return null;
  if (f.now?.at && Number.isFinite(Date.parse(f.now.at))) return f.now.at;
  const has = (r: Row) => r[1] != null || r[2] != null;
  const rec = [...(f.recent ?? [])].reverse().find(has);
  if (rec) return rec[0];
  const day = [...(f.daily ?? [])].reverse().find(has);
  return day ? day[0] : null;
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

  // P02: a file with no fetchedAt, or a malformed one, has an UNKNOWN age (null), never "now".
  const stamp = (s: string | undefined): number | null => {
    const t = s ? Date.parse(s) : NaN;
    return Number.isFinite(t) ? t : null;
  };

  return {
    index: () => load<IndexFile>("index.json"),

    // in-flight requests are left to finish: the next ask after them is a fresh fetch
    refresh() { cache.clear(); },

    async lastReport(station) {
      const f = await load<StationFile>(`${station}.json`);
      return f ? lastReportAt(f) : undefined;
    },

    async live() {
      const idx = await load<IndexFile>("index.json");
      // null, not an empty set. "We could not check" and "no gauge in BC is reporting" are
      // different claims, and only one of them has ever been true.
      return idx ? new Set(Object.keys(idx.stations)) : null;
    },

    /**
     * Is there a file to ask for at all?
     *
     * `index.json` lists the stations the publisher actually wrote, and it is already
     * fetched and cached for `live()`. Asking without checking meant every station the
     * BUNDLE knows about but the FEED never published — a station retired between the two
     * builds, or one that never cleared the publisher's record threshold — produced a 404
     * per view. Handled (a 404 is "not transmitting", which is a true answer), but a
     * request whose only possible outcome is a miss should not be sent.
     *
     * A MISSING INDEX IS NOT AN EMPTY ONE. If the index itself failed to load we fall
     * through and try the station file, because refusing every station on the strength of
     * one failed request would turn a hiccup into an outage.
     */
    async published(station: string): Promise<boolean> {
      const idx = await load<IndexFile>("index.json");
      return idx ? station in idx.stations : true;
    },

    async now(station) {
      if (!(await this.published(station))) return null;
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
      if (!(await this.published(station))) return null;
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
