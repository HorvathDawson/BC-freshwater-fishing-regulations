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
import { gaugeFeedRefusal, reportFailure, shownParameter, standing, type GaugeFeedFile, type SourceChange, now as clockNow }
  from "@app/core";
import type { Aged, Forecast, Parameter, Reading, StationId } from "../index";
import type { AheadDays, StationChange } from "../panel";
import { fetchJsonResilient } from "../net/fetch";
import type { GaugeHistory, ObsFile } from "./history";

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

/** How long a fetched file is reused. The publisher runs every 30 minutes. THE ONE FEED LIFETIME (T125 item 6): the
 *  answers cache, the sync poll, the map's feed poll and the reading poll are this, never a second `5 * 60_000`. */
export const FEED_TTL_MS = 5 * 60_000;

interface IndexFile {
  /** (T557) `gauge-feed/1` (core GAUGE_FEED_FORMAT); a read refuses any other or none. A kept copy from before may lack it. */
  format?: string;
  fetchedAt: string;
  /** Which HYDAT release every percentile in this file was measured against. */
  hydat?: { release: string | null; latest: string | null; stale: boolean };
  /**
   * The OWNER's due upstream tasks (publish.py `maintenance_notices`, owner 2026-10-22 late): `hydat` when a newer HYDAT
   * is listed, `roster` when ECCC's real-time list drifts from the roster. `since` is the first run that saw it;
   * `action` the owner's command. Empty: nothing due. Absent: an index from before notices. The app reads the hydat
   * notice for the Data sources page (`hydatNotice`, core staleness.ts) and nothing else.
   */
  maintenance?: { kind: string; since: string; current: string; latest: string; action: string }[];
  /**
   * (T68) The newest BC River Forecast Centre run's `issuedAt` across every station, as the station files carry it
   * (publish.py `newest_forecast_issue`): the More tab's Forecasts line. null: the run held no forecast (out of
   * season); absent: an index from before the field.
   */
  forecastIssuedAt?: string | null;
  /**
   * The station ids ECCC listed as real-time on the publishing run (`publish.py`). A panel member absent from it is
   * "not a real-time gauge" rather than "not reporting". Absent on an older feed: every station reads as listed.
   */
  listed?: string[];
  stations: Record<string, {
    percentile: number | null;
    observedAt: string | null;
    /**
     * (T550) the station was not answered this run: its last good entry is carried (`staleSince`: the first run that
     * carried it) while its reading is younger than the worker's carry limit. The publisher writes an entry whose
     * reading is past core's age line unranked (`readingCurrent`), so a carried rank is a current reading's.
     */
    stale?: boolean;
    staleSince?: string;
    /**
     * WHETHER any forecast run is held for the station (publish.py: `any(run.get("series") …)`), the series itself
     * being in the station's own file. A boolean — this was typed as an array the publisher has never written; nothing
     * read it until T157 (`aheadWhy` in ../panel: a fresh run with no day ranked is a station with no record).
     */
    forecast?: boolean;
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
    /** Forecast percentiles keyed by days ahead (publish.py `forecast_percentiles`); read by `reading` in ../panel. */
    ahead?: Record<string, { discharge?: number; level?: number; model?: string; levelModel?: string }>;
    /** The expected change over the next ten days (T52), flow only: `discharge` is the coded ratio, `ratio` the ratio. */
    change?: StationChange;
    /** Every forecast day's discharge percentile (T171, publish `change.forecast_days`); read by spots' `flowAhead`. */
    aheadDays?: AheadDays;
  }>;
}

export interface StationFile {
  /** (T557) `gauge-feed/1` (core GAUGE_FEED_FORMAT); a read refuses any other or none. A kept copy from before may lack it. */
  format?: string;
  fetchedAt: string;
  /** (T315) This answer is the phone's kept copy (`./history`), not a read of the network. Never in a published file. */
  kept?: true;
  station: string;
  now: { discharge: number | null; level: number | null; at: string | null;
         percentile: number | null; parameter?: Parameter };
  /** `[timestamp, level, discharge]`, thinned to 30-minute steps by the publisher. */
  recent?: [string, number | null, number | null][];
  /** Every BC River Forecast Centre run for this station, keyed by model name. */
  forecasts?: Record<string, RawForecast> | null;
  /** The station's expected change (T144): which models make it is the publisher's decision (`change_source`). */
  change?: SourceChange | null;
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
 * joins the two halves; see `gaugeSeries` in `release/source.ts`.
 */
export interface Observations {
  /** When our copy was fetched; null when the file does not say (P02: unknown age, never "now"). */
  fetchedAt: number | null;
  /** (T315) From the phone's kept week (offline), not the network: the chart says "as of" `fetchedAt`. */
  kept?: true;
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
  /** The station's expected change as the feed wrote it (T144), or null. */
  change?: SourceChange | null;
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
  /**
   * (T231) The update service fetched a changed `index.json` whole: take it as the index (the station files it may have
   * moved are forgotten, so they are read again on the next ask). Throws when it is not an index: nothing is changed.
   */
  offer?(index: unknown): void;
  /**
   * (T315) Read the publisher's feed-wide newest rows (`obs.json`) into the phone's kept week: every station's, not
   * only the ones viewed. A feed with no history (or offline, or a publisher that does not write it yet) does nothing.
   */
  syncObservations?(): Promise<void>;
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

/** (T254) A feed file is small (a station's ≈ 50 kB, the index ≈ 130 kB on the wire): 45 s for the whole of it. */
const FEED_LIMITS = { firstByteMs: 45_000, stallMs: 45_000, totalMs: 45_000 };

/**
 * (T315, T234) THE PUBLISHER'S FEED-WIDE NEWEST ROWS (`obs.json`, every station's latest `[time, level, discharge]`),
 * read the one way: the phone's kept week takes it (`syncObservations`) and so does the background alert check
 * (`notify/check.ts`). Throws: the network (offline is the environment's, `NetError`), an HTTP status, or a file that
 * is not the feed's.
 */
export async function fetchObs(base: string, fetchImpl: typeof fetch = fetch,
                               limits: typeof FEED_LIMITS = FEED_LIMITS): Promise<ObsFile> {
  const url = `${base.replace(/\/+$/, "")}/obs.json`;
  const got = await fetchJsonResilient<ObsFile>(url, { fetchImpl, attempts: 1, limits, cache: "no-store" });
  if (!got.ok) throw new Error(`${url}: HTTP ${got.status}`);
  const why = gaugeFeedRefusal(got.value, "obs");    // (T557)
  if (why) throw new Error(`${url}: ${why}`);
  return got.value;
}

export function httpFeed(base: string, fetchImpl: typeof fetch = fetch,
                         opts: { indexHeld?: boolean; limits?: { firstByteMs: number; stallMs: number; totalMs: number };
                                 /** (T315) the phone's kept week: fed by every station file read, answering offline */
                                 history?: GaugeHistory;
                                 /** (T315) the device's online word: offline, no request is made — the kept copy answers */
                                 online?: () => boolean } = {}): GaugeFeed {
  const trimmed = base.replace(/\/+$/, "");
  /** (T231) the update service owns the index: once read it is held until the service offers a newer one (`offer`) */
  const held = (url: string) => !!opts.indexHeld && url === `${trimmed}/index.json`;
  // One in-flight request per URL, and a short-lived result cache. Without the first, a
  // screen that asks three components for the same station opens three connections.
  const inflight = new Map<string, Promise<unknown>>();
  const cache = new Map<string, { at: number; value: unknown }>();
  /** (T254) the last good copy of each file this session read: what a dead line answers with (a refresh keeps it) */
  const good = new Map<string, unknown>();

  /** (T557) which gauge feed file a path is, for its format check */
  const kindOf = (path: string): GaugeFeedFile => (path === "index.json" ? "index" : path === "obs.json" ? "obs" : "station");
  /** (T315) a station file's name → its station; the index and obs files are not one */
  const stationOf = (path: string): string | null =>
    path === "index.json" || path === "obs.json" || !path.endsWith(".json") ? null : path.slice(0, -5);
  /** (T315) the session's last good copy, else (a station file) the phone's kept week */
  const fallback = <T>(path: string, url: string): T | null => {
    const g = good.get(url) as T | undefined;
    if (g !== undefined) return g;
    if (!opts.history) return null;
    // (T315) the index too: the panels' standings offline, as of the kept index's own `fetchedAt`
    if (path === "index.json") return (opts.history.index() as T | null) ?? null;
    const st = stationOf(path);
    return st ? (opts.history.file(st) as T | null) : null;
  };

  async function load<T>(path: string): Promise<T | null> {
    const url = `${trimmed}/${path}`;
    const hit = cache.get(url);
    if (hit && (held(url) || clockNow() - hit.at < FEED_TTL_MS)) return hit.value as T;
    // (T315) offline: no request (it would only wait out its timeouts); what this session or the phone holds answers
    if (opts.online && !opts.online()) return fallback<T>(path, url);

    let p = inflight.get(url) as Promise<T | null> | undefined;
    if (!p) {
      p = (async () => {
        try {
          // (T254) on a bad line a dead request is cut and tried once more (never longer than FEED_LIMITS)
          // (T265) the held index is read the way the update service reads it (`no-store`: the origin's word), so a read made
          // while the service's check is in the air joins that request (`shareFetch`) instead of sending a second one
          const got = await fetchJsonResilient<T>(url, { fetchImpl, attempts: 2, limits: opts.limits ?? FEED_LIMITS,
                                                          ...(held(url) ? { cache: "no-store" as const } : {}) });
          if (!got.ok) return null;          // 404 = this station is not transmitting
          // (T557) a file in another format, or not the shape asked for, is refused and REPORTED, never cached or kept:
          // what this session or the phone already holds answers instead, with its own age
          const why = gaugeFeedRefusal(got.value, kindOf(path));
          if (why) {
            reportFailure(new Error(`${url}: ${why}`));
            return fallback<T>(path, url);
          }
          cache.set(url, { at: clockNow(), value: got.value });
          good.set(url, got.value);
          const st = stationOf(path);
          try {
            if (st && opts.history) opts.history.noteFile(st, got.value as StationFile);
            else if (path === "index.json") opts.history?.noteIndex(got.value);
          } catch { /* a cache */ }
          return got.value;
        } catch (e) {
          reportFailure(e);   // (T368) the line down is not reported; anything else is (the copy below still answers, aged)
          // (T254) offline or a dead line — an answer, not an exception: the last good copy if this session has one
          // (its own fetchedAt says how old it is, P02), else nothing
          // (T315) and across launches, the phone's kept week of a station
          return fallback<T>(path, url);
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

    async syncObservations() {
      if (!opts.history || (opts.online && !opts.online())) return;
      try {
        opts.history.noteObs(await fetchObs(trimmed, fetchImpl, opts.limits ?? FEED_LIMITS));
      } catch { /* offline or not published yet: the next feed update asks again */ }
    },

    offer(index) {
      const i = index as Partial<IndexFile> | null;
      const why = gaugeFeedRefusal(i, "index");      // (T557) the same check a read makes
      if (why) throw new Error(why);
      cache.clear();
      cache.set(`${trimmed}/index.json`, { at: clockNow(), value: i });
      try { opts.history?.noteIndex(i); } catch { /* a cache */ }
    },

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
      // `published`'s test, on the one index read the rank below takes too
      const idx = await load<IndexFile>("index.json");
      if (idx && !(station in idx.stations)) return null;
      const f = await load<StationFile>(`${station}.json`);
      if (!f?.now || (f.now.discharge === null && f.now.level === null)) return null;
      // (T585 F8) ONE RANK FOR ONE READING. The rows, dots, map and estimate rank a station from the INDEX (held by the
      // update service); this file is read on a 5-minute TTL and can be another publisher run's. So the rank is the
      // index row's, and only for the reading that row names (same `at`): a file a run ahead or behind gives its figure
      // with NO rank, and `rankedAt` names the reading the index ranked, for the card to say both times. No index
      // read (offline, failed): the file's own rank, as before.
      const row = idx?.stations[station];
      const same = !row || row.observedAt === f.now.at;
      const percentile = row ? (same ? row.percentile : null) : f.now.percentile;
      return {
        fetchedAt: stamp(f.fetchedAt),
        ...(f.kept ? { kept: true as const } : {}),
        value: {
          discharge: f.now.discharge, level: f.now.level,
          at: f.now.at ?? "", percentile,
          standing: standing(percentile),
          parameter: f.now.parameter ?? null,
          ...(same ? {} : { rankedAt: row!.observedAt ?? null }),
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
        ...(f?.kept ? { kept: true as const } : {}),
        from: rows[0]![0],
        // The publisher already decided which quantity this station's percentile is about,
        // against the envelope it actually has. Re-deciding here would be a second opinion
        // on a question that has one right answer per station.
        parameter: shownParameter(f?.now ?? {}),
        at: rows.map((r) => r[0]),
        level: rows.map((r) => r[1]),
        discharge: rows.map((r) => r[2]),
        daily: f?.daily ?? [],
        forecasts: f?.forecasts ?? {},
        priorYears: f?.priorYears ?? {},
        change: f?.change ?? null,
      };
    },
  };
}
