/**
 * (T557 items 3-4, T601 item 3) THE GAUGE FEED'S FORMAT — the one boundary that had none. Every release file, the DFO feed
 * (`dfo-feed/3`), in-season (`inseason-feed/1`, `inseason-links/3`) and run timing carry a format their reader refuses
 * on; the gauge feed's index, station files and obs.json were taken as anything JSON with a `stations` object.
 *
 * The feeds worker (the one publisher) writes `format` first in every file and imports this constant; the Python spec
 * writes the same (`FEED_FORMAT` in pipeline/gauges/feed/publish.py, held by scripts/check-format-pairs.py). The app
 * reads a file through `gaugeFeedRefusal`: another format, or a file that is not the shape asked for, is refused and
 * reported, and what the app already holds answers instead (@app/data feed/http.ts).
 *
 * A FILE WITH NO `format` is refused too (T557 / T633 item 2, 2026-11-11): the transition is over. Staging's worker writes
 * the tag on all 429 station files, index.json and obs.json (read 2026-11-11 22:35Z); production serves no gauge feed yet
 * (its data domain answers 404) and its first deploy is this code. A phone's kept copy from before the tag still answers
 * offline (`history`, never re-read through this check).
 */
export const GAUGE_FEED_FORMAT = "gauge-feed/1";

export type GaugeFeedFile = "index" | "station" | "obs";

/** Why this app cannot read `doc` as a gauge feed `what`, in words; null when it can. */
export function gaugeFeedRefusal(doc: unknown, what: GaugeFeedFile): string | null {
  if (!doc || typeof doc !== "object" || Array.isArray(doc)) return `the gauge feed's ${what} file is not a JSON object`;
  const d = doc as Record<string, unknown>;
  if (d.format === undefined) return `the gauge feed's ${what} file names no format; this app reads ${GAUGE_FEED_FORMAT}`;
  if (d.format !== GAUGE_FEED_FORMAT)
    return `the gauge feed's ${what} file is ${JSON.stringify(d.format)}; this app reads ${GAUGE_FEED_FORMAT}`;
  if (what === "station") {
    if (typeof d.station !== "string") return "the gauge feed's station file names no station";
    return null;
  }
  if (!d.stations || typeof d.stations !== "object" || Array.isArray(d.stations))
    return `the gauge feed's ${what} file has no stations`;
  // the index's own time is what a reading's age is judged against (`readingCurrent`): an index without one is refused
  if (what === "index" && (typeof d.fetchedAt !== "string" || !Number.isFinite(Date.parse(d.fetchedAt))))
    return "the gauge feed's index has no fetchedAt";
  return null;
}
