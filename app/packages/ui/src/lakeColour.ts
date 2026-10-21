/**
 * HOW THE LAKE MODES' VALUES REACH THE MAP (owner 2026-10-21: stocked lakes and the depth colouring go through the one
 * colouring pathway, and update from data alone). The release's `lakes` file (`RegsSource.lakeFacts`) says, per lake
 * section, whether it was surveyed and when it was last stocked; the web map's colouring engine labels each tile's lakes
 * with them as the tile arrives (`MapProps.lakes`: the `survey` and `stocked` channels) — the same per-tile path as the
 * day's status and the Conditions standings, so a new release recolours the lakes with no tile change.
 *
 * Handed over always (both tabs, every lake mode), not only while Stocked or Depth is chosen: the labels are a few
 * thousand lakes, and switching the mode is then one paint change — no relabel, no clear of the other channels.
 * Only on a PROVEN PAIR (a handle means a section only with its own tiles), and only to a map that labels each tile
 * (`coloursEachTile`); the native map has no per-tile event yet (N2) and keeps its lakes plain.
 */
import { useMemo } from "react";
import { dayNumber, type LakeFacts } from "@app/core";
import type { RegsSource } from "@app/data";
import { useAsync } from "./async";
import { sourceKey } from "./conditionsColour";

/** What the map is handed (`MapProps.lakes`): the release's facts, and today as a day number (the stocked bands). */
export interface LakeFeedVM {
  readonly facts: LakeFacts;
  readonly day: number;
}

export function useLakeColour(i: { source: RegsSource; perTile: boolean; paired: boolean; today: Date }):
    LakeFeedVM | null {
  const on = i.perTile && i.paired;
  const facts = useAsync(() => i.source.lakeFacts(), `lakefacts:${sourceKey(i.source)}`, on);
  const f = on && facts.state === "ready" ? facts.value : null;
  const day = dayNumber(i.today);
  return useMemo(() => (f ? { facts: f, day } : null), [f, day]);
}
