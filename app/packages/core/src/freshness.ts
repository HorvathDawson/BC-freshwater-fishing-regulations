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

/** (T550) A gauge reading older than this, at the time its feed was written, is not today's reading (`STALE_AFTER.reading`). */
export const READING_MAX_AGE_MS = STALE_AFTER.reading.ms;

/**
 * (T550) IS THIS GAUGE READING TODAY'S — THE ONE AGE RULE for a station's own reading: its `observedAt` no older than
 * `READING_MAX_AGE_MS` before `ref`, the instant its feed was written (the index's `fetchedAt`, epoch ms). Every rank,
 * count, weight and map colour of today asks this (@app/data `reading`, `donorState`; the worker's share card).
 * A reading the rule cannot judge — no stamp, a naive stamp, one that is not a time, or no feed time — is NOT refused: the
 * worker writes both on every entry, and the app's index reader refuses an index without `fetchedAt` (T557 format check).
 */
export function readingCurrent(observedAt: string | null | undefined, ref: number | null | undefined): boolean {
  if (!observedAt || ref === null || ref === undefined || !Number.isFinite(ref)) return true;
  // only a ZONED stamp is judged (ECCC writes every one with its offset): a naive one would be read on the device's
  // clock here and the Centre's in Python — the publisher's `reading_current` leaves it unjudged too
  if (!/(Z|[+-]\d\d:?\d\d)$/i.test(observedAt.trim())) return true;
  const t = Date.parse(observedAt);
  if (!Number.isFinite(t)) return true;
  return ref - t <= READING_MAX_AGE_MS;
}

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
 *  days, because "72h ago" is arithmetic the reader should not have to do. Not a finite age: "unknown age".
 *  Exported so the public gauge page's script is written from it (`tools/seo/gauges.ts` `clientScript`). */
export function ageWords(ms: number): string {
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

/** What a reading's age line says of a copy that is past its time (T622: the card and the public page say it too). */
export const MAY_BE_OLD = "may be out of date";
/** The line's lead. */
export const READING_LEAD = "This gauge's own reading";

/**
 * The pieces of a gauge reading's age line after its lead (P02; T622: ONE line for the app, the share card and the public
 * gauge page): "observed <stamp>" when the reading's time is known, then how old our copy is. `observed` is the reading's
 * time already said (`localStamp`); null leaves it out. An unknown age says so; it is never shown as live.
 */
export function readingAgeParts(f: Freshness, observed: string | null): string[] {
  const parts: string[] = [];
  if (observed !== null) parts.push(`observed ${observed}`);
  parts.push(f.state === "unknown" ? "age unknown"
    : f.state === "live" ? `checked ${ageWords(f.ageMs)}`
    : `last checked ${ageWords(f.ageMs)}, ${MAY_BE_OLD}`);
  return parts;
}

/** (T585 F14) The line's lead when the reading is the phone's KEPT copy (offline) — the chart's "Offline — … saved on this phone", for the figure. */
const READING_LEAD_KEPT = "Offline — reading saved on this phone";

/** The whole line from a stamp already said: "This gauge's own reading · observed … · checked 7h ago". */
export function readingAgeWords(f: Freshness, observed: string | null): string {
  return [READING_LEAD, ...readingAgeParts(f, observed)].join(" · ");
}

/**
 * The line under a gauge reading (P02): when it was observed, and how old our copy is. An unknown age says so; it
 * is never shown as live. `observedAt` is the reading's own ISO time, left out when missing or malformed.
 */
export function readingAgeLine(f: Freshness, observedAt: string | null, zone?: string, kept = false): string {
  const t = observedAt ? Date.parse(observedAt) : NaN;
  // LOCAL AND LABELLED (P16): the reader's clock, with its zone named — see `localStamp`.
  const observed = Number.isFinite(t) ? localStamp(t, zone) : null;
  // (T585 F14) the phone's kept copy, offline: said as that (the public page's script takes `readingAgeWords`, which a
  // page never answers from a kept copy, so the kept lead lives here)
  return kept ? [READING_LEAD_KEPT, ...readingAgeParts(f, observed)].join(" · ") : readingAgeWords(f, observed);
}
