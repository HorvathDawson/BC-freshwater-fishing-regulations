/**
 * How the app writes a quantity. One home, because two screens had already disagreed.
 *
 * The Layers sheet rendered "60,648 reaches" through a local helper while the search header
 * rendered "19699 named waters" with a bare interpolation — the same kind of figure, on two
 * screens, formatted two ways. That is rule 23 one layer below where it usually gets
 * applied: not a second vocabulary for an outcome, a second convention for a number.
 *
 * These take `undefined` and return `undefined` on purpose. A count the app does not have
 * yet must render as NOTHING, never as "0" — "we have not asked" and "we asked and there
 * are none" are different claims, and only one of them is safe to show.
 */

/** `count(60648, "reaches")` -> "60,648 reaches". Undefined in, undefined out. */
export function count(n: number | null | undefined, word: string): string | undefined {
  return n === null || n === undefined ? undefined : `${n.toLocaleString()} ${word}`;
}

/** `plural(1, "stretch", "stretches")` -> "1 stretch". */
export function plural(n: number, one: string, many: string): string {
  return `${n.toLocaleString()} ${n === 1 ? one : many}`;
}

/**
 * "7h ago" — how old a live value is.
 *
 * A live value with no age is a live value you cannot trust: the app showed a gauge feed as
 * current for three days because the timestamp beside it was a hardcoded string rather than
 * the feed's own. Days past 48h, because "72h ago" is arithmetic the reader should not have
 * to do.
 */
export function ago(iso: string, now: number = Date.now()): string {
  const h = Math.round((now - Date.parse(iso)) / 3.6e6);
  if (!Number.isFinite(h)) return "unknown age";
  if (h < 1) return "just now";
  if (h < 48) return `${h}h ago`;
  return `${Math.round(h / 24)}d ago`;
}
