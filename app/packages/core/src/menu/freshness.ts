/** How fresh each source is (the More tab's list), the regulations' lapse rule, and the forecasts' "when". */
import { agoWords } from "../freshness";
import { localDate } from "../day";
import { monthAbbr } from "../flow";
import { byCadence, STALE_AFTER, type AgeKey, type StaleRule } from "../staleness";
import type { SourceDateKey, SourcesInput } from "./sources";

/** "2026-07-15" -> "Jul 15, 2026"; anything that is not an ISO day -> null (never a guess). */
export function isoDayWords(day: string | null | undefined): string | null {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(day ?? "");
  if (!m) return null;
  const mo = Number(m[2]), d = Number(m[3]);
  if (mo < 1 || mo > 12 || d < 1 || d > 31) return null;
  return `${monthAbbr(mo - 1)} ${d}, ${m[1]}`;
}

/**
 * HOW FRESH A SOURCE IS (owner 2026-10-22: "when each feed was last updated … and if any are likely out of date"):
 *   fresh    inside its expected cadence
 *   stale    older than its expected cadence: LIKELY out of date (a cadence is an expectation, not a promise)
 *   unknown  no date recorded, or a source the app does not read — never guessed fresh
 *   live     fetched on demand (weather with a spot, imagery while online): it has no age to hold
 */
export type SourceFreshness = "fresh" | "stale" | "unknown" | "live";
export interface FreshnessVM { readonly kind: SourceFreshness; readonly label: string }

export const FRESHNESS_WORDS = {
  fresh: "Up to date",
  stale: "Likely out of date",
  unknown: "Unknown",
  live: "Live",
  /** the in-season changes: the app does not read them, so their freshness is the reader's to check */
  notRead: "Not read",
  /** the regulations past their last day: not "likely" — the edition has lapsed (`regsLapsed`) */
  lapsed: "Out of date",
  /**
   * HYDAT when the feed says a newer release is listed (`hydatNotice` "due"): not "likely" (the publisher saw it) and
   * not "Out of date" (the records in use are still right; the new release adds a year) — what is known, no more
   */
  newerRelease: "Newer release out",
} as const;

/**
 * THE ONE LAPSE RULE (owner 2026-10-22 late: the release stamps `valid_until`, the synopsis period's end): the
 * regulations are in force THROUGH their last day on the READER'S calendar (a B.C. day, like every day the app reads —
 * `day.ts`), and out of date from the next local day. No stamp, or one that is not an ISO day (today's live release
 * states none): never lapsed — the app does not say so on a guess.
 */
export function regsLapsed(validUntil: string | null | undefined, now: number): boolean {
  return isoDayWords(validUntil) !== null && localDate(now) > validUntil!;
}

/** The sentence a lapsed edition's Regulations page leads with; null while in force or with no stamp. */
export const REGS_LAPSED_NOTE = (day: string) =>
  `Out of date: these regulations were valid to ${day}. Check the current Synopsis and the Province's notices before ` +
  "you fish.";

export function regsLapsedNote(validUntil: string | null | undefined, now: number): string | null {
  return regsLapsed(validUntil, now) ? REGS_LAPSED_NOTE(isoDayWords(validUntil)!) : null;
}

/** The age in ms of an ISO day at `now` (from its start, UTC), or null when it is not one. */
const dayAgeMs = (day: string | null | undefined, now: number): number | null =>
  isoDayWords(day) === null ? null : now - Date.parse(`${day}T00:00:00Z`);
const badge = (kind: SourceFreshness, label: string = FRESHNESS_WORDS[kind]): FreshnessVM => ({ kind, label });

/**
 * An age-ruled source's age at `now`: the feed by its index's `fetchedAt`, the forecasts by the newest run's
 * `forecastIssuedAt`, the rest by the release's date for them.
 */
function ageMs(k: AgeKey, s: SourcesInput): number | null {
  if (k === "feed" || k === "forecast") {
    const at = k === "feed" ? s.feedFetchedAt : s.forecastIssuedAt;
    const t = typeof at === "string" ? Date.parse(at) : NaN;
    return Number.isFinite(t) ? s.now - t : null;
  }
  if (k === "dfo" || k === "fwa" || k === "basemap" || k === "hydat") return dayAgeMs(s.sources?.[k]?.date, s.now);
  return null;
}

/**
 * A release-ruled source: likely out of date once a newer release is known (whatever else holds), else —
 *   synopsis  in force through its last day (`valid_until`): fresh through that day, stale after; no last day, unknown
 *   tiles     the proven pair: paired fresh, a mixed pair stale, not established unknown
 */
function byRelease(k: SourceDateKey, s: SourcesInput): FreshnessVM {
  if ((s.newRelease ?? null) !== null) return badge("stale");
  if (k === "synopsis") {
    if (isoDayWords(s.validUntil) === null) return badge("unknown");
    // in force THROUGH its last day (the reader's calendar); after it, out of date — said without "likely"
    return regsLapsed(s.validUntil, s.now) ? badge("stale", FRESHNESS_WORDS.lapsed) : badge("fresh");
  }
  return badge(s.tilesPaired === true ? "fresh" : s.tilesPaired === false ? "stale" : "unknown");
}

/**
 * ONE SOURCE'S FRESHNESS, by its rule in THE ONE TABLE (`STALE_AFTER`, staleness.ts):
 *   age       the feed (its index's own `fetchedAt`), the forecasts (the index's newest run, `forecastIssuedAt`; none
 *             running: the feed's own freshness) and dfo / fwa / basemap / hydat (the release's date for each)
 *             against the table's line; never checked or not recorded, unknown
 *   notice    hydat: the feed's notice — due "Newer release out"; clear, up to date whatever the age ONLY when the
 *             release's `sources.hydat.date` is the feed's newest HYDAT, "Newer release out" when the release is
 *             older (the app's data has not caught up); no word or no release date, the age line as above
 *   release   synopsis / tiles (`byRelease`)
 *   notRead   the in-season changes: unknown ("Not read")
 *   live      weather / imagery
 */
export function sourceFreshness(k: SourceDateKey, s: SourcesInput): FreshnessVM {
  const rule: StaleRule = STALE_AFTER[k];
  switch (rule.by) {
    case "age":
      // (T68) the feed says it holds no forecast run (out of season): known, and as fresh as the check that said so
      if (k === "forecast" && s.forecastIssuedAt === null) return sourceFreshness("feed", s);
      return badge(byCadence(k as AgeKey, ageMs(k as AgeKey, s)));
    case "notice": {
      const n = s.hydatNotice ?? null;
      if (n === "due") return badge("stale", FRESHNESS_WORDS.newerRelease);
      // the feed is clear; the APP is current only when its release carries that same HYDAT (ISO days compare as
      // days), and an older one has not caught up yet. No release date (or a newer one): the age line
      const own = isoDayWords(s.sources?.hydat?.date) === null ? null : s.sources!.hydat!.date!;
      if (n !== null && own !== null) {
        if (own === n.current) return badge("fresh");
        if (own < n.current) return badge("stale", FRESHNESS_WORDS.newerRelease);
      }
      return badge(byCadence(k as AgeKey, ageMs(k as AgeKey, s)));
    }
    case "release": return byRelease(k, s);
    case "notRead": return badge("unknown", FRESHNESS_WORDS.notRead);
    case "live": return badge("live");
  }
}

/**
 * (T68) THE FORECASTS' "WHEN", one rule for the Data block and the Details page (each in its own words): the newest
 * run's age; none running; the feed reached but silent on forecasts (an index from before the field); not reached.
 */
export function forecastWhen(s: SourcesInput, w: { issued: (ago: string) => string; none: string; notSaid: string;
                                                   notChecked: string }): string {
  const at = s.forecastIssuedAt;
  if (typeof at === "string" && Number.isFinite(Date.parse(at))) return w.issued(agoWords(at, s.now));
  if (at === null) return w.none;
  return s.feedFetchedAt !== null && Number.isFinite(Date.parse(s.feedFetchedAt)) ? w.notSaid : w.notChecked;
}
