/**
 * (T377 item 1) THE FORECAST-DAY LIST OF ONE WATERSHED GROUP, headless: the group a Conditions view is about (`basin.ts`
 * — browse: the one most on the map, `col.viewBasin`; a water: its own, `useWaterBasin`; NEVER one for the other), and that
 * group's standing on each day the horizon chips offer (`basinStandings` in @app/data, the field's own arithmetic, asked
 * for that ONE group — eleven small votes, not the province). Core words it (`basinDaysVM`); a tap on a day is the
 * horizon chips' own pick (`useHorizonPick`), handed in by the caller — one path.
 *
 * States, all shown: loading; failed (the group, the classes or the feed could not be read — reported where it was
 * caught, `useAsync` / `useViewBasin`); none (the place is in no group: a fact, said); ready.
 *
 * (T417) NO FLICKER BETWEEN GROUPS: when the view moves to another group, the last group's days STAY on screen — under
 * the last group's own name, never the new one's — until the new group's days are in, then swap in one render. Only a
 * first ask (nothing shown yet) or another quantity (the old days would answer a different question) draws `loading`.
 */
import { useMemo, useRef } from "react";
import { basinDaysVM, bcNow, BASIN_DAYS_WORDS, DAY_HORIZONS, environmentError, type BasinDaysVM, type DayHorizon,
         type Horizon } from "@app/core";
import { basinStandings, type Basin, type BasinMember, type Parameter, type WaterSource } from "@app/data";
import { useAsync, type Async } from "./async";
import { sourceKey } from "./conditionsColour";
import type { PanelFeed } from "./panel";

/** Which view asks: the browse map (what it shows) or a water's Conditions view (its own place). Only the words differ. */
export type BasinDaysFor = "browse" | "water";

export type BasinDaysState =
  | { readonly state: "loading" }
  | { readonly state: "failed"; readonly error: Error }
  | { readonly state: "none"; readonly line: string }
  | { readonly state: "ready"; readonly basin: Basin; readonly vm: BasinDaysVM };

/** The browse map's group (`col.viewBasin` / `viewBasinError`) as the one shape a view's group is asked in. */
export function viewBasinAsk(group: Basin | null | undefined, error: Error | null): Async<Basin | null> {
  if (error) return { state: "failed", value: null, error };
  if (group === undefined) return { state: "loading", value: null, error: null };
  return { state: "ready", value: group, error: null };
}

/**
 * ONE GROUP'S STANDING PER DAY: `basinStandings` over a map holding that group alone, at each day horizon. A day the
 * group has no vote for (nothing reporting, no forecast that far, no gauge at all) is null.
 */
export function basinDayStandings(members: readonly BasinMember[], idx: Parameters<typeof basinStandings>[1],
                                  quantity: Parameter): Map<DayHorizon, number | null> {
  const one = new Map([["g", members]]);
  return new Map(DAY_HORIZONS.map((d) => [d, basinStandings(one, idx, quantity, d).get("g") ?? null] as const));
}

export function useBasinDays(i: {
  source: WaterSource; feed: PanelFeed | undefined;
  /** The group asked: the map's (`viewBasinAsk`) on browse, `useWaterBasin` in a water's view. */
  basin: Async<Basin | null>;
  for: BasinDaysFor;
  quantity: Parameter;
  /** The horizon shown: its day is lit. */
  horizon: Horizon;
  /** Off (another tab, no proven pair): nothing is asked. */
  enabled: boolean;
  /** The sync epoch: a refreshed feed re-asks. */
  epoch?: number;
  /** The day the chips count from (default: B.C.'s now). */
  now?: Date;
}): BasinDaysState {
  const { source, feed, basin, quantity, horizon, enabled } = i;
  const group = basin.state === "ready" ? basin.value : null;
  const id = group?.id ?? "";
  const asks = enabled && group !== null;
  const days = useAsync(async () => {
    if (!feed) throw environmentError(new Error("basins: no gauge feed to read the days from"));
    const [classes, idx] = await Promise.all([source.standingClasses(), feed.index()]);
    return { id, quantity, standings: basinDayStandings(classes.basins.get(id) ?? [], idx, quantity) };
  }, `basin-days:${sourceKey(source)}:${id}:${quantity}:${i.epoch ?? 0}:${feed ? 1 : 0}`, asks,
     // the same group and quantity again (a feed refresh): the last days stay up until the new ones are in
     `basin-days:${sourceKey(source)}:${id}:${quantity}`);
  const now = i.now;
  // (T417) the last group whose days were shown, and for which quantity: held on screen while the next group's load
  const shown = useRef<{ src: number; basin: Basin; quantity: Parameter; standings: Map<DayHorizon, number | null> } | null>(null);
  return useMemo((): BasinDaysState => {
    if (basin.state === "failed") return { state: "failed", error: basin.error };
    if (group === null && basin.state === "ready") {
      shown.current = null;
      return { state: "none", line: BASIN_DAYS_WORDS.noGroup[i.for] };
    }
    if (days.state === "failed" && group !== null && basin.state === "ready") return { state: "failed", error: days.error };
    // the days of THIS group: shown, and remembered as what is on screen
    // (the answer is tagged with its group AND quantity: the render right after a new question still holds the last one)
    if (group !== null && days.state === "ready" && days.value.id === group.id && days.value.quantity === quantity) {
      shown.current = { src: sourceKey(source), basin: group, quantity, standings: days.value.standings };
      return { state: "ready", basin: group,
               vm: basinDaysVM(group.name, days.value.standings, horizon, quantity, now ?? bcNow()) };
    }
    // the next group (or the group itself) still loading: the last group's days stay up, as that group's — never
    // shown as the new one's — unless they answer another quantity, or come from another release
    const held = shown.current;
    if (held && held.quantity === quantity && held.src === sourceKey(source))
      return { state: "ready", basin: held.basin,
               vm: basinDaysVM(held.basin.name, held.standings, horizon, quantity, now ?? bcNow()) };
    return { state: "loading" };
  }, [source, basin, group, days, horizon, quantity, now, i.for]);
}
