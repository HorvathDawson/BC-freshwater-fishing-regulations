/**
 * HOW OLD A VALUE IS, said once (P02, T61 2.15).
 *
 * `Freshness` encodes a rule the plan calls a correctness requirement, not a nicety: a stale answer must never be
 * rendered as a live one (13-build-plan §5). `agoWords` is the app's ONE way of saying an age — the gauge reading's
 * line, the menu's Data sources page, the Layers sheet's feed age (ui-native format.ts `ago`), "Like your visit?" and
 * the sync line all read it. There were two, with different wording ("3 h ago" under a reading, "3h ago" in the menu).
 */
import { localStamp } from "./day";
import { STALE_AFTER } from "./staleness";

export type Freshness =
  | { state: "live"; ageMs: number }
  | { state: "stale"; ageMs: number }
  | { state: "unknown" };

/** A feed reading older than this is stale (P02): the gauge feed's line in THE ONE staleness table (staleness.ts) —
 *  the publisher writes every 30 minutes, so two hours is four missed runs. */
export const STALE_AFTER_MS = STALE_AFTER.feed.ms;

/** Classify a cached value's age. `unknown` is NOT the same as "nothing to report". */
export function freshness(
  fetchedAt: number | null,
  now: number,
  staleAfterMs: number = STALE_AFTER_MS,
): Freshness {
  if (fetchedAt === null || !Number.isFinite(fetchedAt)) return { state: "unknown" };
  const ageMs = Math.max(0, now - fetchedAt);
  return ageMs > staleAfterMs ? { state: "stale", ageMs } : { state: "live", ageMs };
}

/** The words of an age ("Checked 7h ago"). */
export const AGO_WORDS = {
  now: "just now",
  minutes: (n: number) => `${n} min ago`,
  hours: (n: number) => `${n}h ago`,
  days: (n: number) => `${n}d ago`,
  unknown: "unknown age",
} as const;

/** An age in milliseconds, in words: minutes under an hour (the feed is published every 30), hours under two days, then
 *  days, because "72h ago" is arithmetic the reader should not have to do. Not a finite age: "unknown age". */
function ageWords(ms: number): string {
  if (!Number.isFinite(ms)) return AGO_WORDS.unknown;
  const min = Math.floor(Math.max(0, ms) / 60_000);
  if (min < 1) return AGO_WORDS.now;
  if (min < 60) return AGO_WORDS.minutes(min);
  const h = Math.round(min / 60);
  if (h < 48) return AGO_WORDS.hours(h);
  return AGO_WORDS.days(Math.round(h / 24));
}

/** "7h ago" — how old a value stamped at `iso` (an ISO instant) is at `now`. A stamp that is not a date: "unknown age". */
export function agoWords(iso: string, now: number): string {
  return ageWords(now - Date.parse(iso));
}

/**
 * The line under a gauge reading (P02): when it was observed, and how old our copy is. An unknown age says so; it
 * is never shown as live. `observedAt` is the reading's own ISO time, left out when missing or malformed.
 */
export function readingAgeLine(f: Freshness, observedAt: string | null, zone?: string): string {
  const parts = ["This gauge's own reading"];
  const t = observedAt ? Date.parse(observedAt) : NaN;
  // LOCAL AND LABELLED (P16): the reader's clock, with its zone named — see `localStamp`.
  if (Number.isFinite(t)) parts.push(`observed ${localStamp(t, zone)}`);
  parts.push(f.state === "unknown" ? "age unknown"
    : f.state === "live" ? `checked ${ageWords(f.ageMs)}`
    : `last checked ${ageWords(f.ageMs)}, may be out of date`);
  return parts.join(" · ");
}
