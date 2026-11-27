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
import { FeedNeedsNewerApp, readingCurrentNow, readingCurrentUntil, gaugeFeedRefusal, runOffered, isFeedTooNew, parseSemver, reportFailure, reportOnce, shownParameter, standing,
         type FeedGate, type GaugeFeedFile, type SourceChange, now as clockNow } from "@app/core";
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
  /** (T676 item 2) the oldest app that reads this format; the feed gate refuses the index for an older app */
  minAppVersion?: string;
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
     * (T830) WHAT THE READING CARRIES, ranked or not (publish.py `live_quantities`): a level with no band (Capilano at
     * Canyon) is listed, a level on another datum (T550) is not. Absent on a feed from before it: nothing is known.
     */
    reports?: Parameter[];
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

/**
 * (T1205) THE INDEX AS OF NOW: a station whose reading is not current by the clock (`readingCurrentNow`) keeps its
 * `observedAt` (its last report is still said) and loses what only a current reading carries: the percentiles, which
 * quantities it reports, the temperature. The worker wrote those while the reading was young; an index held, kept or
 * never refreshed would otherwise show them as live for ever. Forecast fields are other sources' and stay. Returns the
 * same object when nothing changes.
 */
export function ageIndex(idx: IndexFile | null, nowMs: number): IndexFile | null {
  if (!idx) return idx;
  const kept = aged.get(idx);
  if (kept && nowMs >= kept.from && nowMs <= kept.until) return kept.out;   // nothing has crossed the line since
  let out: IndexFile["stations"] | null = null;
  let until = Infinity;
  for (const [id, s] of Object.entries(idx.stations)) {
    const last = readingCurrentUntil(s.observedAt);
    if (last === null || nowMs <= last) { if (last !== null) until = Math.min(until, last); continue; }
    // already stripped (the publisher's own age gate wrote it so): nothing to take away
    if ((s.percentile ?? null) === null && s.discharge === undefined && s.level === undefined && s.reports === undefined
        && s.temperatureC === undefined && s.temperatureAt === undefined && s.temperatureBand === undefined) continue;
    out ??= { ...idx.stations };
    const { discharge: _d, level: _l, reports: _r, temperatureC: _t, temperatureAt: _ta, temperatureBand: _tb, ...rest } = s;
    out[id] = { ...rest, percentile: null };
  }
  const res = out ? { ...idx, stations: out } : idx;
  aged.set(idx, { out: res, from: nowMs, until });   // the same answer while no station crosses the line (the clock only runs forward)
  return res;
}
/** per raw index: its aged copy and the span of instants it holds for (so a poll gets the same object, and a later tick re-judges) */
const aged = new WeakMap<object, { out: IndexFile; from: number; until: number }>();

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
  const has = runOffered(s, parameter);
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
 * (`notify/check.ts`). Throws: the network (offline is the environment's, `NetError`), an HTTP status, a file that
 * is not the feed's, or the gate's `FeedNeedsNewerApp` (a file stamped for a newer app: a known state).
 */
export async function fetchObs(base: string, fetchImpl: typeof fetch = fetch,
                               limits: typeof FEED_LIMITS = FEED_LIMITS, held?: ObsHeld, gate?: FeedGate | null): Promise<ObsFile> {
  const url = `${base.replace(/\/+$/, "")}/obs.json`;
  const k = held ? readHeldObs(held, url) : { kept: null, refused: null };
  const kept = k.kept;
  // (T676 L2) a file this app refused as newer, remembered by its validator: asked about by THAT validator, so an
  // unchanged refused file costs a 304 and no body (it was downloaded again on every run). The gate is asked first
  // whether the remembered need still refuses (an app updated since reads the file whole); asking holds the need, which
  // a readable answer below clears
  let refused = k.refused;
  if (refused && !(gate && stillRefused(gate, refused.need, url))) refused = null;
  // (T741) a conditional GET when a copy and its validator are kept: an unchanged file costs a 304 and no body. Sent
  // STRONG, as the update service's (`update/check.ts`): the edge gzips and weakens the R2 ETag, the data worker
  // compares strong validators against the R2 object, so a 304 is the origin's proof that the object has not moved.
  const validator = refused?.etag ?? kept?.etag ?? null;
  const headers = validator ? { "If-None-Match": validator.replace(/^W\//, "") } : undefined;
  const got = await fetchJsonResilient<ObsFile>(url, { fetchImpl, attempts: 1, limits, cache: "no-store", ...(headers ? { headers } : {}) });
  if (!got.ok && got.status === 304 && refused) throw new FeedNeedsNewerApp("gauges", refused.need);
  if (!got.ok && got.status === 304 && kept) return kept.value;
  if (!got.ok) throw new Error(`${url}: HTTP ${got.status}`);
  // (T676 item 2) a file stamped for a newer app throws the gate's refusal (a known state: the caller keeps what it has
  // and reports nothing). Only the file's OWN stamp excuses it: every gauge file is stamped, so a file in another format
  // without one, or a broken file in this app's own format, is a real failure, reported (T557)
  try {
    gate?.checkFile(got.value, "gauges", url);
  } catch (e) {
    if (isFeedTooNew(e) && held) {
      // (T676 L2) remembered beside the kept copy, by its validator; with none there is nothing to ask with (said)
      if (got.etag) held.write(JSON.stringify({ url, ...(kept ? { etag: kept.etag, value: kept.value } : {}),
                                                refused: { etag: got.etag, need: e.need } }));
      else console.info(`[obs] ${url}: refused as newer than this app, with no ETag; the next check reads it whole again`);
    }
    throw e;
  }
  const why = gaugeFeedRefusal(got.value, "obs");    // (T557)
  if (why) throw new Error(`${url}: ${why}`);
  if (held) {
    if (got.etag) held.write(JSON.stringify({ url, etag: got.etag, value: got.value }));
    else {
      // no validator from the origin: nothing to ask with next time, so every run is a whole read (said, not hidden)
      held.write("");
      console.info(`[obs] ${url}: no ETag in the answer; the next check reads the whole file again`);
    }
  }
  return got.value;
}

/** (T676 L2) Does the gate still refuse a file it once refused for `need`? (asking holds the need under `file`) */
function stillRefused(gate: FeedGate, need: string, file: string): boolean {
  try { gate.checkFile({ minAppVersion: need }, "gauges", file); return false; } catch (e) { if (isFeedTooNew(e)) return true; throw e; }
}

/** (T741) Where the background check keeps the last `obs.json` it read and its ETag (a cache: losing it costs one whole
 *  read), and (T676 L2) the validator and need of the last one it refused as newer than this app. */
export interface ObsHeld { read(): string | null; write(text: string): void }

/** A small in-memory `ObsHeld` (the foreground's own: the session's conditional GETs and refusal). */
const memoryObsHeld = (): ObsHeld => {
  let t: string | null = null;
  return { read: () => t, write: (x) => { t = x; } };
};

interface HeldObs {
  /** the kept copy for `url` (none, another url, or unreadable: a whole read follows, and a bad copy is said) */
  kept: { etag: string; value: ObsFile } | null;
  /** (T676 L2) the last answer refused as newer than this app: its validator and the version it asked for */
  refused: { etag: string; need: string } | null;
}

function readHeldObs(held: ObsHeld, url: string): HeldObs {
  const none: HeldObs = { kept: null, refused: null };
  const text = held.read();
  if (!text) return none;
  let k: { url?: unknown; etag?: unknown; value?: unknown; refused?: unknown };
  try { k = JSON.parse(text) as typeof k; } catch (e) {
    reportFailure(new Error(`kept obs.json is not JSON (${e instanceof Error ? e.message : String(e)}); reading it whole`));
    return none;
  }
  if (!k || typeof k !== "object" || k.url !== url) return none;
  const r = k.refused as { etag?: unknown; need?: unknown } | undefined;
  // a cache: a refusal not in its shape is dropped (one whole read), never trusted
  const refused = r && typeof r === "object" && typeof r.etag === "string" && r.etag && typeof r.need === "string" && parseSemver(r.need)
    ? { etag: r.etag, need: r.need } : null;
  if (typeof k.etag !== "string" || !k.etag) return { kept: null, refused };
  if (gaugeFeedRefusal(k.value, "obs")) {
    reportFailure(new Error(`kept obs.json is not the feed's (${gaugeFeedRefusal(k.value, "obs")}); reading it whole`));
    return { kept: null, refused };
  }
  return { kept: { etag: k.etag, value: k.value as ObsFile }, refused };
}

export function httpFeed(base: string, fetchImpl: typeof fetch = fetch,
                         opts: { indexHeld?: boolean; limits?: { firstByteMs: number; stallMs: number; totalMs: number };
                                 /** (T315) the phone's kept week: fed by every station file read, answering offline */
                                 history?: GaugeHistory;
                                 /** (T315) the device's online word: offline, no request is made — the kept copy answers */
                                 online?: () => boolean;
                                 /** (T676 item 2) refuses an index newer than this app: what is held answers */
                                 gate?: FeedGate } = {}): GaugeFeed {
  const trimmed = base.replace(/\/+$/, "");
  /** (T231) the update service owns the index: once read it is held until the service offers a newer one (`offer`) */
  const held = (url: string) => !!opts.indexHeld && url === `${trimmed}/index.json`;
  // One in-flight request per URL, and a short-lived result cache. Without the first, a
  // screen that asks three components for the same station opens three connections.
  const inflight = new Map<string, Promise<unknown>>();
  const cache = new Map<string, { at: number; value: unknown }>();
  /** (T254) the last good copy of each file this session read: what a dead line answers with (a refresh keeps it) */
  const good = new Map<string, unknown>();
  /** (T676 L2) the foreground's own kept obs.json and its validator (and a refusal's): the session's conditional GETs */
  const obsHeld = memoryObsHeld();

  /** (T557) which gauge feed file a path is, for its format check */
  const kindOf = (path: string): GaugeFeedFile => (path === "index.json" ? "index" : path === "obs.json" ? "obs" : "station");
  /** (T315) a station file's name → its station; the index and obs files are not one */
  const stationOf = (path: string): string | null =>
    path === "index.json" || path === "obs.json" || !path.endsWith(".json") ? null : path.slice(0, -5);
  /** (T315) the session's last good copy, else (a station file) the phone's kept week */
  const fallback = async <T>(path: string, url: string): Promise<T | null> => {
    const g = good.get(url) as T | undefined;
    if (g !== undefined) return g;
    if (!opts.history) return null;
    // (T633 item 6) a week kept where it is read later (the web's) is waited for: never "nothing kept" a moment early
    await opts.history.ready();
    // (T315) the index too: the panels' standings offline, as of the kept index's own `fetchedAt`
    if (path === "index.json") return (opts.history.index() as T | null) ?? null;
    const st = stationOf(path);
    return st ? (opts.history.file(st) as T | null) : null;
  };

  /**
   * (T676 L2) EACH FILE REFUSED AS NEWER THAN THIS APP, remembered: its validator, the version it asked for, and when it
   * was last asked. Within FEED_TTL_MS it is not asked again at all (what this session or the phone holds answers);
   * after, it is asked by ITS validator, so an unchanged refused file costs a 304 and no body. It was downloaded again
   * on every read before. `refresh` (the sync icon) asks again at once, still by the validator.
   */
  const refusedFiles = new Map<string, { etag: string | null; need: string; at: number }>();
  /** the gate's check for `path` (the index speaks for the feed; any other file holds its own need, by url) */
  const gateCheck = (path: string, url: string, doc: unknown) => {
    if (path === "index.json") opts.gate!.check(doc, "gauges");
    else opts.gate!.checkFile(doc, "gauges", url);
  };

  async function load<T>(path: string): Promise<T | null> {
    const url = `${trimmed}/${path}`;
    const hit = cache.get(url);
    if (hit && (held(url) || clockNow() - hit.at < FEED_TTL_MS)) return hit.value as T;
    // (T315) offline: no request (it would only wait out its timeouts); what this session or the phone holds answers
    if (opts.online && !opts.online()) return fallback<T>(path, url);
    // (T676 L2) refused a moment ago: nothing is asked; the gate still holds its need
    const rem = opts.gate ? refusedFiles.get(url) : undefined;
    if (rem && clockNow() - rem.at < FEED_TTL_MS) return fallback<T>(path, url);

    let p = inflight.get(url) as Promise<T | null> | undefined;
    if (!p) {
      p = (async () => {
        try {
          // (T254) on a bad line a dead request is cut and tried once more (never longer than FEED_LIMITS)
          // (T265) the held index is read the way the update service reads it (`no-store`: the origin's word), so a read made
          // while the service's check is in the air joins that request (`shareFetch`) instead of sending a second one
          // (T676 L2) a refused file is asked about by its validator (STRONG, as `fetchObs`)
          const got = await fetchJsonResilient<T>(url, { fetchImpl, attempts: 2, limits: opts.limits ?? FEED_LIMITS,
                                                          ...(held(url) ? { cache: "no-store" as const } : {}),
                                                          ...(rem?.etag ? { headers: { "If-None-Match": rem.etag.replace(/^W\//, "") } } : {}) });
          if (!got.ok && got.status === 304 && rem) {
            // (T676 L2) the same file this app is too old for: still refused, its need held again; nothing downloaded
            rem.at = clockNow();
            try { gateCheck(path, url, { minAppVersion: rem.need }); } catch (e) { if (!isFeedTooNew(e)) throw e; }
            return fallback<T>(path, url);
          }
          if (!got.ok) {
            // 404 = this station is not transmitting (a hold its refused file set is gone with it)
            refusedFiles.delete(url);
            if (opts.gate && got.status === 404 && path !== "index.json") opts.gate.clearFile("gauges", url);
            return null;
          }
          // (T676 item 2) an index for a newer app: not a failure (no report), what this session or the phone holds
          // answers, and the gate holds the need for the update prompt. A station file or obs.json for a newer app: the
          // same (the gate holds the file's need until that file reads; T676 L3)
          if (opts.gate) {
            try {
              gateCheck(path, url, got.value);
            } catch (e) {
              if (isFeedTooNew(e)) {
                refusedFiles.set(url, { etag: got.etag, need: e.need, at: clockNow() });
                return fallback<T>(path, url);
              }
              throw e;
            }
            refusedFiles.delete(url);
          }
          // (T557) a file in another format, or not the shape asked for, is refused and REPORTED, never cached or kept:
          // what this session or the phone already holds answers instead, with its own age. (T676 item 2) Only a file's own
          // `minAppVersion` (the gate, above) excuses it; anything else this app cannot read is reported
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
    index: async () => ageIndex(await load<IndexFile>("index.json"), clockNow()),

    // in-flight requests are left to finish: the next ask after them is a fresh fetch
    refresh() {
      cache.clear();
      for (const r of refusedFiles.values()) r.at = -Infinity;   // (T676 L2) asked again now, by the validator
    },

    async syncObservations() {
      if (!opts.history || (opts.online && !opts.online())) return;
      try {
        // (T676 L2) the session's own kept copy: an unchanged obs.json (a refused one included) costs a 304
        opts.history.noteObs(await fetchObs(trimmed, fetchImpl, opts.limits ?? FEED_LIMITS, obsHeld, opts.gate));
      } catch (e) {
        // (T676 L4) a file stamped for a newer app is a known state: the gate holds its need (the update prompt and More's
        // gauges row say it), nothing reported. Offline is the environment's (never reported). Anything else (an HTTP
        // status, a file that is not the feed's) is a defect: REPORTED, once per kind a run (`reportOnce`): the sync runs
        // on every feed update, and the same broken file must not fill the crash inbox. Nothing on screen claims the
        // rows are new: the kept week answers with its own age ("as of")
        if (isFeedTooNew(e)) return;
        reportOnce(`obs sync: ${e instanceof Error ? e.message : String(e)}`, e);
      }
    },

    offer(index) {
      const i = index as Partial<IndexFile> | null;
      opts.gate?.check(i, "gauges");                 // (T676 item 2) throws for a newer app: the service says so
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
      const idx = ageIndex(await load<IndexFile>("index.json"), clockNow());   // (T1205) as of the clock, like `index()`
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
      // (T1205) no index row to follow: the file's own rank, only while its reading is current by the clock
      const percentile = row ? (same ? row.percentile : null) : readingCurrentNow(f.now.at, clockNow()) ? f.now.percentile : null;
      return {
        fetchedAt: stamp(f.fetchedAt),
        ...(f.kept ? { kept: true as const } : {}),
        value: {
          discharge: f.now.discharge, level: f.now.level,
          at: f.now.at ?? "", percentile,
          standing: standing(percentile),
          parameter: f.now.parameter ?? null,
          ...(same ? {} : { rankedAt: row!.observedAt ?? null }),
          // (T830) the quantities the publisher says this station reports live (the index row's; none without an index)
          ...(row?.reports ? { reports: row.reports } : {}),
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
