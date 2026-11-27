/**
 * (owner 2026-10-29) THE OPEN PAGE FOLLOWS THE FEEDS. When the update service takes a changed feed (gauges, DFO salmon,
 * in-season: `UpdateService.onFeedChange`, told here by `useSync`), every view built from that feed asks again IN PLACE:
 * the Conditions reading, chart and forecast, the map's gauge dots and standings, the salmon colouring and card. No
 * reload, no loading frame: each hook puts the feed's version in its `useAsync` key with its old key as `sameAs` (a
 * refresh — the last answer stays drawn until the new one is in), so scroll, selection and a chart's zoom are kept.
 *
 * A release is NOT a feed: it keeps its safe-moment apply (the Shell's `onSafeToApply`, the "Updated to …" toast).
 *
 * Module state, on purpose: the feeds are one per app, and a hook deep in a panel must not need a prop threaded to it.
 */
import { useSyncExternalStore } from "react";
import { FEED_TTL_MS } from "@app/data";
import { clearAnswers } from "./answers";
import { onAppActive } from "./today";

export type LiveFeedId = "gauges" | "dfo" | "inseason";

let versions: Readonly<Record<LiveFeedId, number>> = { gauges: 0, dfo: 0, inseason: 0 };
const listeners = new Set<() => void>();

/** A feed moved: the answers built from it are forgotten (a gauge's reading and series), and its readers ask again. */
export function feedMoved(id: LiveFeedId): void {
  versions = { ...versions, [id]: versions[id] + 1 };
  // the answer store holds readings and series for half a poll: a moved gauge feed must not be answered from it
  if (id === "gauges") clearAnswers();
  for (const l of [...listeners]) l();
}

/**
 * (T1205) THE JUDGEMENT OF A READING'S AGE RE-RUNS AS TIME PASSES, with no fetch: a gauge reading is current by the clock
 * (`readingCurrentNow`), so a feed that stopped, or a phone offline with the app open, must see its readings go old. While
 * anything watches the gauges, one beat per feed lifetime (the poll the colours already use) and the app's return to the
 * front count as a move of the gauge version: every reader keyed on it asks again, and `httpFeed().index()` judges at the
 * clock. Not a feed change, so the answer store is left alone.
 */
let judged = 0;
let beat: ReturnType<typeof setInterval> | undefined;
let unactive: (() => void) | undefined;
function rejudge(): void { judged += 1; for (const l of [...listeners]) l(); }

const subscribe = (on: () => void) => {
  listeners.add(on);
  if (listeners.size === 1) { beat = setInterval(rejudge, FEED_TTL_MS); unactive = onAppActive(rejudge); }
  return () => {
    listeners.delete(on);
    if (!listeners.size) { clearInterval(beat); beat = undefined; unactive?.(); unactive = undefined; }
  };
};

/** The feed's version: a new number each time the update service takes a changed copy of it. */
export function useFeedVersion(id: LiveFeedId): number {
  const read = () => versions[id] + (id === "gauges" ? judged : 0);
  return useSyncExternalStore(subscribe, read, read);
}

/** Test seam: start from version 0 everywhere. */
export function resetFeedVersions(): void {
  versions = { gauges: 0, dfo: 0, inseason: 0 };
}
