/**
 * HOW LONG EACH FEED STAYS FRESH — THE ONE TABLE (owner 2026-10-22 late: "gauge data is much faster turnover than other
 * feeds"). Every staleness the app judges reads its line here and nowhere else: the More tab's Data block and the Data
 * sources & dates page (`sourceFreshness`, menu/freshness.ts), the top bar's sync dot (`syncVM`, more.ts), a gauge reading's age
 * line (`freshness` / `STALE_AFTER_MS`, freshness.ts) and a forecast run's line (`forecastRunLine`, conditions.ts).
 *
 * Each line is set from the feed's REAL publish cadence (handoff/PLAN/app-integration/decisions.md,
 * "CLAUDE 2026-10-22 (owner delegated) — staleness per feed"):
 *
 *   by "age"      past `ms` the feed is LIKELY out of date (a cadence is an expectation, not a promise)
 *   by "release"  ships with the data release: stale only when a newer release is known, or (the regulations) their
 *                 last day in force has passed, or (the map tiles) the tiles and the data are not one pair — never by
 *                 age, so a release is never called stale just for being months old
 *   by "notice"   the gauge feed's publisher SAYS whether a newer upstream release is out (its index's `maintenance`
 *                 notice, owner 2026-10-22 late): due, stale; checked and none due, fresh — never by age; only when
 *                 no word reaches the app (an index from before notices, a run that could not check) does `ms` decide
 *   by "dated"   (T700) base geography and maps, which change rarely and carry no signal that a newer edition exists:
 *                 the row says its date and NOTHING about age — age alone is never "likely out of date" for it. Only a real
 *                 signal may ever mark one stale (none exists today: the feed's notices cover HYDAT only)
 *   by "weekdays" (T347) issued on working days only: current until the next working day's issue plus a grace
 *                 (`forecastDueBy`), so Friday's run holds the weekend and a holiday Monday
 *   by "live"     fetched on demand: it has no age to hold
 */
import { CHANGE_POLICY, FORECAST_HOLIDAYS, READING_MAX_AGE_HOURS } from "./gauge-policy.generated";

export type StaleRule =
  | { readonly by: "age"; readonly ms: number }
  | { readonly by: "notice"; readonly ms: number }
  | { readonly by: "weekdays" }
  | { readonly by: "release" }
  | { readonly by: "dated" }
  | { readonly by: "live" };

const MIN = 60_000;
const HOUR = 60 * MIN;
const DAY = 24 * HOUR;
/**
 * (T549 items 7, 10) The stale line of a feed the worker checks ONCE A DAY off season (DFO salmon, the in-season
 * notices; wrangler.toml crons): the 24 h cadence plus 6 h. The worker's scheduled test holds it over the slowest cron.
 */
const DAILY_FEED_STALE_MS = 30 * HOUR;

export const STALE_AFTER = {
  /**
   * The gauge feed's index, by its own `fetchedAt`. pipeline/gauges/feed/publish.py runs every 30 minutes over ECCC's
   * hourly real-time CSVs: 2 h is four missed runs (two missed ECCC hours). Also a reading's own line (P02).
   */
  feed: { by: "age", ms: 2 * HOUR },
  /**
   * (T550) ONE GAUGE'S OWN READING, by its `observedAt` against the time the feed was written (the index's `fetchedAt`).
   * ECCC publishes hourly, about an hour behind; a station that stops answering is carried by the worker for days
   * (`stale`). Older than this, the reading is not today's: it is not ranked, counted, coloured or drawn as a standing
   * (`readingCurrent`, freshness.ts) — the gauge says when it last reported instead. The gauge alerts' gate is the same
   * line (`ALERT_STALE_MS`, pinned equal in freshness.test.ts). The hours are the publisher's (`READING_MAX_AGE_HOURS`).
   */
  reading: { by: "age", ms: READING_MAX_AGE_HOURS * HOUR },
  /**
   * THIS DEVICE's last reach of the gauge feed — the sync dot. The publisher's own 30-minute tick: older than one tick,
   * a tap can fetch something newer. (Open, the app re-checks every 5 minutes, so this is reached in the background or
   * after a failed check.)
   */
  feedCheck: { by: "age", ms: 30 * MIN },
  /**
   * A BC River Forecast Centre run, by its `issuedAt`. The models are issued once (ELF, COFFEE) or at most twice
   * (CLEVER) a day (pipeline/gauges/feed/forecast.py), on WORKING DAYS only (T347, owner 2026-11-02): current until the
   * next working day's issue plus half a day's slack (`forecastDueBy`) — 36 h on a weekday, Friday's to Monday evening.
   */
  forecast: { by: "weekdays" },
  /**
   * (T526, owner 2026-11-09: "older then a day … older just means it wasnt checked because it doesnt change that
   * frequently but we need to check it often") DFO's salmon feed, by WHEN THE FEEDS WORKER LAST CHECKED DFO's pages — the
   * feed's `at` (written by every run, T549 items 7 and 10) — never by when DFO's content last changed. The worker
   * checks at least once a day (every 6 h Jun–Oct), so the line is `DAILY_FEED_STALE_MS`: one day's check plus margin
   * for a late run and a held copy. At exactly 24 h it flickered "likely out of date" every day at the daily write.
   * Not checked for longer: likely out of date. No checked time: unknown, and reported.
   */
  dfo: { by: "age", ms: DAILY_FEED_STALE_MS },
  /**
   * (T700, owner 2026-11-13: "Fresh water atlas rarely changes it isn't stale") The Freshwater Atlas is base geography
   * that rarely changes; its age says nothing about whether it is right. Its row gives its date, never a verdict.
   */
  fwa: { by: "dated" },
  /** (T700) The basemap's roads and towns (OpenStreetMap via Protomaps), rebuilt by us: its date, never a verdict by age. */
  basemap: { by: "dated" },
  /**
   * HYDAT: the feed checks HYDAT's listing every run and says a newer release is out (`hydatNotice`). The age line is
   * the fallback only: HYDAT is published quarterly and a release takes it yearly, so 18 months is a missed year.
   */
  hydat: { by: "notice", ms: 548 * DAY },
  /** The regulations: the release's edition, in force through `valid_until`. */
  synopsis: { by: "release" },
  /** The map's tiles: built with the release, proven one pair with its data (P11). */
  tiles: { by: "release" },
  /**
   * (T548) B.C. Fish and Wildlife's in-season changes (the Province's page, read by the feeds worker since T225 and by
   * the app since T230), by the feed's own `fetchedAt` — written by every run that read the page, changed or not
   * (app/workers/feeds/src/inseason.ts), as DFO's is (T526, T549 item 7). Not rewritten for longer than
   * `DAILY_FEED_STALE_MS`: the worker is blocked or failing, and the notices shown are likely out of date. No time: unknown.
   */
  inseason: { by: "age", ms: DAILY_FEED_STALE_MS },
  /** Weather: fetched with each saved spot. */
  weather: { by: "live" },
  /** Satellite imagery: fetched while online. */
  imagery: { by: "live" },
} as const satisfies Record<string, StaleRule>;

export type StaleKey = keyof typeof STALE_AFTER;
/** A feed with an age line: age-ruled, or notice-ruled (its line is the fallback when no notice word arrives). */
export type AgeKey = { [K in StaleKey]: (typeof STALE_AFTER)[K] extends { ms: number } ? K : never }[StaleKey];

/**
 * An age-ruled feed's verdict: within its line fresh (the line itself included), past it stale, no age unknown.
 * The one comparison every age-ruled staleness makes (and a notice-ruled one when no notice word reached the app).
 */
export function byCadence(k: AgeKey, ageMs: number | null): "fresh" | "stale" | "unknown" {
  if (ageMs === null || !Number.isFinite(ageMs)) return "unknown";
  return ageMs > STALE_AFTER[k].ms ? "stale" : "fresh";
}

/** (T347) The Forecast Centre's calendar: a fixed UTC−8 (pipeline `CENTRE_UTC_OFFSET_H`), as its stamps are read. */
const CENTRE_OFFSET_MS = -8 * HOUR;
const HOLIDAYS = new Set(FORECAST_HOLIDAYS);
const HOLIDAY_YEARS = [FORECAST_HOLIDAYS[0]!.slice(0, 4), FORECAST_HOLIDAYS[FORECAST_HOLIDAYS.length - 1]!.slice(0, 4)] as const;

/**
 * (T347) WHEN A FORECAST RUN STOPS BEING CURRENT — the pipeline's `change.due_by`: its issue instant plus the whole days
 * to the next working day on the Centre's calendar (no weekend, no `FORECAST_HOLIDAYS`) plus
 * `CHANGE_POLICY.staleGraceHours`. A run past the emitted holiday years is an error, never a guess.
 */
export function forecastDueBy(issuedMs: number): number {
  const day0 = Math.floor((issuedMs + CENTRE_OFFSET_MS) / DAY) * DAY;
  let days = 0, d = day0;
  do {
    d += DAY; days++;
    const iso = new Date(d).toISOString().slice(0, 10), wd = new Date(d).getUTCDay();
    if (iso.slice(0, 4) < HOLIDAY_YEARS[0] || iso.slice(0, 4) > HOLIDAY_YEARS[1])
      throw new Error(`forecast staleness: ${iso} is outside the holiday years ${HOLIDAY_YEARS.join("–")}`);
    if (wd !== 0 && wd !== 6 && !HOLIDAYS.has(iso)) break;
  } while (true);
  return issuedMs + days * DAY + CHANGE_POLICY.staleGraceHours * HOUR;
}

/** (T347) A forecast run's verdict at `now` by the working-day schedule; no issue time, unknown. */
export function byWeekdays(issuedMs: number | null, now: number): "fresh" | "stale" | "unknown" {
  if (issuedMs === null || !Number.isFinite(issuedMs)) return "unknown";
  return now > forecastDueBy(issuedMs) ? "stale" : "fresh";
}

/**
 * WHAT THE GAUGE FEED SAYS ABOUT HYDAT (owner 2026-10-22 late: "the feed should check for hydat releases ... similar
 * to bundle changes where the app checks when in service for updates"). The publisher writes the owner's due tasks
 * into its index as `maintenance` notices (pipeline/gauges/feed/publish.py `maintenance_notices`); the app re-reads
 * the index while in service (the sync state), so a notice reaches the reader with no app change.
 *
 *   "due"                a `hydat` notice: a newer HYDAT is listed than the one the flows are measured against
 *   { current: <day> }   the index carries notices, none for HYDAT, and this run checked (its `hydat` block names both
 *                        the release in use and the newest listed): the FEED is clear as of HYDAT <day> (`latest`, an
 *                        ISO day). Not yet the APP: its bundle (gauge_clim, gauge_stats, section_panel) moves to a new
 *                        HYDAT only with the next data release, so the HYDAT line is current only when the release's
 *                        `sources.hydat.date` is that same day (menu/freshness.ts `sourceFreshness`)
 *                        `feed`: (T633 item 5) the HYDAT the feed's own ranks are measured against (`hydat.release`);
 *                        when it is not the release's, the dot's rank and the chart's bands come from two envelopes
 *                        and the HYDAT line says so (menu/freshness.ts)
 *   null                 no word: no index, an index from before notices, or a run that could not check — the age
 *                        line decides
 *
 * Structural, so core reads the index without importing @app/data's types.
 */
export type HydatNotice = "due" | { readonly current: string; readonly feed?: string };

export interface MaintenanceIndex {
  readonly hydat?: { readonly release: string | null; readonly latest: string | null; readonly stale?: boolean } | null;
  readonly maintenance?: readonly { readonly kind: string }[] | null;
}

/** The feed's HYDAT release word (YYYYMMDD, HYDAT's own) as an ISO day, the manifest's form; null when it is no day. */
function hydatDay(release: string | null | undefined): string | null {
  const m = /^(\d{4})-?(\d{2})-?(\d{2})$/.exec(release ?? "");
  return m ? `${m[1]}-${m[2]}-${m[3]}` : null;
}

export function hydatNotice(index: MaintenanceIndex | null | undefined): HydatNotice | null {
  if (!index || !Array.isArray(index.maintenance)) return null;
  if (index.maintenance.some((n) => n?.kind === "hydat")) return "due";
  const latest = hydatDay(index.hydat?.latest);
  const feed = hydatDay(index.hydat?.release);
  return index.hydat?.release && latest ? { current: latest, ...(feed ? { feed } : {}) } : null;
}
