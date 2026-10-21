/**
 * MAP MODES (P12 c): which layer the map colours by what, decided here and drawn by the Shell.
 *
 * About seventy-five lines of `Shell.tsx` chose these inline — the quantity, the percentile asked of the panel, the
 * field, every layer's colour mode, the active layer groups. A component draws a view model (AGENTS 25), so the
 * decision is `mapModes`, pure and tested whole, and `useMapModes` memoises it for the Shell.
 *
 * The reasons each rule reads as it does are kept beside the rule below; they were the Shell's comments.
 */
import { useMemo } from "react";
import type { Horizon, Parameter } from "@app/data";
import type { GaugeQuantity } from "./gaugePoints";
import type { BrowseTab } from "./route";

/**
 * What the Conditions map is coloured by. ONE quantity plus the temperature field — see `Quantity` in @app/data for
 * why "both" is gone: it put stage and discharge percentiles on the same ramp, and they disagree by more than 25
 * points at 84 stations.
 */
export type FlowParam = Parameter | "temperature";

/** A layer's colouring, as the map's `modes` prop takes it. */
export type MapModes = {
  stream: string; lake: string; lake_flowing: string; wetland_flowing: string; gauges: string; basin: string;
};

export interface MapModeInput {
  tab: BrowseTab;
  flowParam: FlowParam;
  horizon: Horizon;
  /** Below the handover zoom (the field draws, nothing is tappable). */
  zoomedOut: boolean;
  /** The data and the tiles are a PROVEN pair (`usePairing` ok === true). Anything less — a mixed pair, or one not
   *  yet established — colours nothing (P11). */
  paired: boolean;
  /** The day's status index is in and matches this atlas. */
  statusOk: boolean;
  /** The Layers sheet's lake choice: its colour mode, and a layer group it switches on. */
  lake: { mode: string; group?: string } | undefined;
  /** The Layers sheet's group toggles. */
  groups: Readonly<Record<string, boolean>>;
}

export interface MapModesVM {
  onConditions: boolean;
  /** Which gauge roster the dots show. */
  quantity: GaugeQuantity;
  /** The percentile the panel is asked for — a real one even under temperature, where the rivers stay plain. */
  standingParam: Parameter;
  /** Temperature has no forecast (the models publish flow and stage), so it stays on today. */
  standingHorizon: Horizon;
  /** The zoomed-out field: on Conditions, not under temperature, below the handover. */
  fieldOn: boolean;
  /** The forecast horizon picker is offered (Conditions, not temperature). */
  horizonsShown: boolean;
  modes: MapModes;
  activeGroups: Record<string, boolean>;
}

export function mapModes(i: MapModeInput): MapModesVM {
  const onConditions = i.tab === "conditions";
  const temperature = i.flowParam === "temperature";
  const quantity: GaugeQuantity = temperature ? "temperature" : i.flowParam === "level" ? "level" : "flow";
  const standingParam: Parameter = temperature ? "discharge" : (i.flowParam as Parameter);
  const lakeMode = i.lake?.mode ?? "plain";
  /*
   * COLOURED FOR FLOW AND FOR LEVEL; PLAIN ONLY UNDER TEMPERATURE. A level question is answered from level donors or
   * not at all (see `Quantity` in @app/data), so a reach with nothing to say arrives with no standing and draws as
   * unmeasured. Temperature stays plain: nothing carries a temperature between waters, so colouring a river from a
   * gauge 40 km away would invent a reading. On the Map tab: the day's status once the index has loaded and matched.
   * A STREAM'S POLYGON IS A STREAM — `lake_flowing` / `wetland_flowing` wear the rivers' colouring in every view.
   */
  const river = !i.paired ? "plain"
    : onConditions ? (temperature ? "plain" : "standing")
    : i.statusOk ? "status" : "plain";
  const modes: MapModes = {
    stream: river,
    // Lakes answer the same question as the rivers on Conditions (`lake_gauge`, a station IN the lake). On the Map tab
    // a lake left "plain" in Layers wears the day's status; Stocked and Depth are questions the reader asked instead.
    lake: !i.paired ? "plain"
      : onConditions ? (temperature ? "plain" : "standing")
      : lakeMode === "plain" && i.statusOk ? "status" : lakeMode,
    lake_flowing: river,
    wetland_flowing: river,
    gauges: temperature ? "temperature" : "standing",
    // THE FIELD must be named or it is never painted. `standing` only where the rivers are — it is the zoomed-out
    // form of the same answer; under temperature the models publish no field.
    basin: !i.paired ? "plain" : onConditions && !temperature ? "standing" : "plain",
  };
  const activeGroups = { ...i.groups };
  if (i.lake?.group) activeGroups[i.lake.group] = true;
  return {
    onConditions, quantity, standingParam,
    standingHorizon: temperature ? 0 : i.horizon,
    fieldOn: onConditions && !temperature && i.zoomedOut,
    horizonsShown: onConditions && !temperature,
    modes, activeGroups,
  };
}

/** `mapModes`, memoised on its inputs so the map's effects see a stable `modes` while nothing changed. */
export function useMapModes(i: MapModeInput): MapModesVM {
  const { tab, flowParam, horizon, zoomedOut, paired, statusOk, lake, groups } = i;
  const lakeMode = lake?.mode, lakeGroup = lake?.group;
  return useMemo(
    () => mapModes({ tab, flowParam, horizon, zoomedOut, paired, statusOk,
                     lake: lakeMode === undefined ? undefined : { mode: lakeMode, group: lakeGroup }, groups }),
    [tab, flowParam, horizon, zoomedOut, paired, statusOk, lakeMode, lakeGroup, groups]);
}
