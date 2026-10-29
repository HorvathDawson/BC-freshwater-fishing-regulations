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
import { clearAnswers } from "./answers";

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

const subscribe = (on: () => void) => { listeners.add(on); return () => { listeners.delete(on); }; };

/** The feed's version: a new number each time the update service takes a changed copy of it. */
export function useFeedVersion(id: LiveFeedId): number {
  const read = () => versions[id];
  return useSyncExternalStore(subscribe, read, read);
}

/** Test seam: start from version 0 everywhere. */
export function resetFeedVersions(): void {
  versions = { gauges: 0, dfo: 0, inseason: 0 };
}
