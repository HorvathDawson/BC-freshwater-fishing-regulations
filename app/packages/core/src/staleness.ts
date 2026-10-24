/**
 * HOW LONG EACH FEED STAYS FRESH — THE ONE TABLE (owner 2026-10-22 late: "gauge data is much faster turnover than other
 * feeds"). Every staleness the app judges reads its line here and nowhere else: the More tab's Data block and the Data
 * sources & dates page (`sourceFreshness`, menu.ts), the top bar's sync dot (`syncVM`, more.ts), a gauge reading's age
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
 *   by "notRead"  the app does not read it: unknown, the reader's to check
 *   by "live"     fetched on demand: it has no age to hold
 */
export type StaleRule =
  | { readonly by: "age"; readonly ms: number }
  | { readonly by: "notice"; readonly ms: number }
  | { readonly by: "release" }
  | { readonly by: "notRead" }
  | { readonly by: "live" };

const MIN = 60_000;
const HOUR = 60 * MIN;
const DAY = 24 * HOUR;

export const STALE_AFTER = {
  /**
   * The gauge feed's index, by its own `fetchedAt`. pipeline/gauges/feed/publish.py runs every 30 minutes over ECCC's
   * hourly real-time CSVs: 2 h is four missed runs (two missed ECCC hours). Also a reading's own line (P02).
   */
  feed: { by: "age", ms: 2 * HOUR },
  /**
   * THIS DEVICE's last reach of the gauge feed — the sync dot. The publisher's own 30-minute tick: older than one tick,
   * a tap can fetch something newer. (Open, the app re-checks every 5 minutes, so this is reached in the background or
   * after a failed check.)
   */
  feedCheck: { by: "age", ms: 30 * MIN },
  /**
   * A BC River Forecast Centre run, by its `issuedAt`. The models are issued once (ELF, COFFEE) or at most twice
   * (CLEVER) a day (pipeline/gauges/feed/forecast.py): a day plus half a day's slack for a late morning run. Past
   * 36 h a daily run has been missed.
   */
  forecast: { by: "age", ms: 36 * HOUR },
  /** DFO's salmon notices turn over in season (~half the rules a year): a scrape older than 120 days has likely missed one. */
  dfo: { by: "age", ms: 120 * DAY },
  /** The Freshwater Atlas changes slowly; a copy two years old has likely missed a revision. */
  fwa: { by: "age", ms: 730 * DAY },
  /** The basemap's roads and towns (OpenStreetMap via Protomaps), rebuilt yearly. */
  basemap: { by: "age", ms: 365 * DAY },
  /**
   * HYDAT: the feed checks HYDAT's listing every run and says a newer release is out (`hydatNotice`). The age line is
   * the fallback only: HYDAT is published quarterly and a release takes it yearly, so 18 months is a missed year.
   */
  hydat: { by: "notice", ms: 548 * DAY },
  /** The regulations: the release's edition, in force through `valid_until`. */
  synopsis: { by: "release" },
  /** The map's tiles: built with the release, proven one pair with its data (P11). */
  tiles: { by: "release" },
  /** The Province's in-season changes: not read by the app. */
  inseason: { by: "notRead" },
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
 *                        `sources.hydat.date` is that same day (menu.ts `sourceFreshness`)
 *   null                 no word: no index, an index from before notices, or a run that could not check — the age
 *                        line decides
 *
 * Structural, so core reads the index without importing @app/data's types.
 */
export type HydatNotice = "due" | { readonly current: string };

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
  return index.hydat?.release && latest ? { current: latest } : null;
}
