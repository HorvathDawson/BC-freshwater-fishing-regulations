/**
 * (T377 item 1) THE FORECAST-DAY LIST OF ONE WATERSHED GROUP, headless: the group a Conditions view is about (`basin.ts`
 * — browse: the map centre's, `col.centreBasin`; a water: its own, `useWaterBasin`; NEVER one for the other), and that
 * group's standing on each day the horizon chips offer (`basinStandings` in @app/data, the field's own arithmetic, asked
 * for that ONE group — eleven small votes, not the province). Core words it (`basinDaysVM`); a tap on a day is the
 * horizon chips' own pick (`useHorizonPick`), handed in by the caller — one path.
 *
 * States, all shown: loading; failed (the group, the classes or the feed could not be read — reported where it was
 * caught, `useAsync` / `useCentreBasin`); none (the place is in no group: a fact, said); ready.
 */
import { useMemo } from "react";
import { basinDaysVM, bcNow, BASIN_DAYS_WORDS, DAY_HORIZONS, environmentError, type BasinDaysVM, type DayHorizon,
         type Horizon } from "@app/core";
import { basinStandings, type Basin, type BasinMember, type Parameter, type WaterSource } from "@app/data";
import { useAsync, type Async } from "./async";
import { sourceKey } from "./conditionsColour";
import type { PanelFeed } from "./panel";

/** Which view asks: the browse map (its centre) or a water's Conditions view (its own place). Only the words differ. */
export type BasinDaysFor = "browse" | "water";

export type BasinDaysState =
  | { readonly state: "loading" }
  | { readonly state: "failed"; readonly error: Error }
  | { readonly state: "none"; readonly line: string }
  | { readonly state: "ready"; readonly basin: Basin; readonly vm: BasinDaysVM };

/** The browse map's centre group (`col.centreBasin` / `centreBasinError`) as the one shape a view's group is asked in. */
export function centreBasinAsk(centre: Basin | null | undefined, error: Error | null): Async<Basin | null> {
  if (error) return { state: "failed", value: null, error };
  if (centre === undefined) return { state: "loading", value: null, error: null };
  return { state: "ready", value: centre, error: null };
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
  /** The group asked: the centre's (`centreBasinAsk`) on browse, `useWaterBasin` in a water's view. */
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
    return { id, standings: basinDayStandings(classes.basins.get(id) ?? [], idx, quantity) };
  }, `basin-days:${sourceKey(source)}:${id}:${quantity}:${i.epoch ?? 0}:${feed ? 1 : 0}`, asks,
     // the same group and quantity again (a feed refresh): the last days stay up until the new ones are in
     `basin-days:${sourceKey(source)}:${id}:${quantity}`);
  const now = i.now;
  return useMemo((): BasinDaysState => {
    if (basin.state === "failed") return { state: "failed", error: basin.error };
    if (basin.state === "loading") return { state: "loading" };
    if (group === null) return { state: "none", line: BASIN_DAYS_WORDS.noGroup[i.for] };
    if (days.state === "failed") return { state: "failed", error: days.error };
    // the days of THIS group only: an answer still held for the group asked before is never shown as this one's
    if (days.state !== "ready" || days.value.id !== group.id) return { state: "loading" };
    return { state: "ready", basin: group,
             vm: basinDaysVM(group.name, days.value.standings, horizon, quantity, now ?? bcNow()) };
  }, [basin, group, days, horizon, quantity, now, i.for]);
}
