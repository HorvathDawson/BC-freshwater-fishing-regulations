/**
 * MAP MODES (P12 c): which layer the map colours by what, decided here and drawn by the Shell.
 *
 * About seventy-five lines of `Shell.tsx` chose these inline — the quantity, the percentile asked of the panel, the
 * field, every layer's colour mode, the active layer groups. A component draws a view model (AGENTS 25), so the
 * decision is `mapModes`, pure and tested whole, and `useMapModes` memoises it for the Shell.
 *
 * The reasons each rule reads as it does are kept beside the rule below; they were the Shell's comments.
 */
import { useMemo, useRef } from "react";
import { basemapInUse, SATELLITE_OFFLINE, type Basemap, type StatusIndex, type WaterColour } from "@app/core";
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
  /**
   * How the Map tab colours the water — ONE choice for streams, lakes and wetlands together (owner 2026-10-21 late:
   * "it is dangerous to have the divergent layers for features"). The layer registry's `effective.water`
   * (`@app/core` layersVM): an unavailable choice has already fallen back to "regulations".
   */
  water: WaterColour;
  /** The style groups the layer registry sets (`effective.groups`): switches, and what a water colouring turns on. */
  groups: Readonly<Record<string, boolean>>;
  /** The Layers sheet's ground: the READER'S CHOICE, kept whatever the connection does. */
  basemap: Basemap;
  /**
   * Can the satellite imagery be reached (`useImageryReach`)? Satellite is ONLINE-ONLY (owner 2026-10-21): while it
   * cannot be, the map draws its own ground and the sheet says why. REQUIRED, so no map is handed the choice raw.
   */
  imageryUp: boolean;
  /**
   * (T211) The Map tab's regulations are painted from DFO's salmon (the Standard / Salmon toggle on Salmon, and the
   * release's salmon handed to the map). Only under the Regulations water colouring; absent = Standard.
   */
  salmon?: boolean;
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
  /**
   * The map's own horizon control is offered: on Conditions, not under temperature (which
   * has no forecast). Restored by the owner (2026-10-20) after P16 removed it; it shares the
   * panel header's state (`useHorizon`), so the two never disagree.
   */
  horizonsShown: boolean;
  /**
   * Whether the map is asked what is on screen (`MapProps.onVisible`: the `idle` -> query -> React round trip). Only
   * where something reads the answer: the Conditions legend's marks. NOT the Map tab, where the colouring engine labels
   * each tile itself and a per-pan re-render of the Shell would be work for nothing (the one-pathway audit, 2026-10-21).
   */
  asksVisible: boolean;
  modes: MapModes;
  activeGroups: Record<string, boolean>;
  /** The ground ACTUALLY drawn — what every map is handed. Not the choice: satellite offline draws the map. */
  basemap: Basemap;
  /** Why the ground drawn is not the one chosen (the Layers sheet's line), else null. */
  basemapNote: string | null;
}

/**
 * IS THE CATCHMENT FIELD THE ANSWER IN THIS VIEW (below the handover)? On Conditions, under Flow or Level — never under
 * temperature, where the models publish no field and the rivers stay drawn (plain) at every zoom. The ONE predicate the
 * field's drawing (`fieldOn`) and the map's taps (`tapOpensWater`) both read.
 */
export const fieldAnswers = (tab: BrowseTab, flowParam: FlowParam): boolean =>
  tab === "conditions" && flowParam !== "temperature";

/**
 * DOES A TAP AT THIS ZOOM OPEN A WATER? Not where the field is drawn: below the handover (`FIELD_HANDOVER_Z`, @app/map)
 * the Conditions map is catchments, and a reach sheet for a shape the size of a valley answers a question nobody asked.
 * At and above it the rivers are back and they are what you tap (T130: the handover is a zoom, `zoom < handover` is
 * the field, exactly as the style's exclusive maxzoom draws it).
 */
export function tapOpensWater(i: { tab: BrowseTab; flowParam: FlowParam; zoom: number; handover: number }): boolean {
  return !(fieldAnswers(i.tab, i.flowParam) && i.zoom < i.handover);
}

export function mapModes(i: MapModeInput): MapModesVM {
  const onConditions = i.tab === "conditions";
  const temperature = i.flowParam === "temperature";
  const quantity: GaugeQuantity = temperature ? "temperature" : i.flowParam === "level" ? "level" : "flow";
  const standingParam: Parameter = temperature ? "discharge" : (i.flowParam as Parameter);
  /*
   * A LAKE MEASURE (Stocked, Depth) COLOURS THE LAKES BY IT AND EVERY OTHER WATER NEUTRAL: stocking and surveys are
   * recorded for lakes only, and a river still wearing the day's regulations beside lakes wearing a stocking ramp is
   * two answers on one map (owner 2026-10-21 late). The legend says the neutral ones are not covered.
   */
  const lakeMeasure = i.water === "stocked" ? "stocked" : i.water === "depth" ? "surveyed" : null;
  /*
   * COLOURED FOR FLOW AND FOR LEVEL; PLAIN ONLY UNDER TEMPERATURE. A level question is answered from level donors or
   * not at all (see `Quantity` in @app/data), so a reach with nothing to say arrives with no standing and draws as
   * unmeasured. Temperature stays plain: nothing carries a temperature between waters, so colouring a river from a
   * gauge 40 km away would invent a reading. On the Map tab: the day's status once the index has loaded and matched —
   * through the `profile` mode (colour-latency AP): the map labels each section with its profile as its tile arrives
   * and carries the day in the style, so a new tile is coloured on its first frame instead of after `idle`.
   * A STREAM'S POLYGON IS A STREAM — `lake_flowing` / `wetland_flowing` wear the rivers' colouring in every view.
   */
  const statusMode = "profile";
  /*
   * (T211) SALMON: DFO's salmon for the day, through its own channel (`salmon`, labelled per tile like the status).
   * Only on the Map tab under the Regulations colouring: a toggle is ONLY a mode change here, everything it paints
   * from is already handed over.
   */
  const salmonOn = !!i.salmon && i.paired && !onConditions && !lakeMeasure;
  const salmonMode = "salmon";
  // (T213) bait ban / fly only are no mode: marks on the Standard map's status mode (@app/map `gearPaint`)
  const regsMode = salmonOn ? salmonMode : i.statusOk ? statusMode : "plain";
  const river = !i.paired ? "plain"
    : onConditions ? (temperature ? "plain" : "standing")
    : lakeMeasure ? "plain"
    : regsMode;
  const modes: MapModes = {
    stream: river,
    // Lakes answer the same question as the rivers, on both tabs (`lake_gauge` on Conditions, a station IN the lake);
    // under a lake measure the lakes wear it and the rivers go neutral
    lake: !i.paired ? "plain"
      : onConditions ? (temperature ? "plain" : "standing")
      : lakeMeasure ?? regsMode,
    lake_flowing: river,
    wetland_flowing: river,
    gauges: temperature ? "temperature" : "standing",
    // THE FIELD must be named or it is never painted. `standing` only where the rivers are — it is the zoomed-out
    // form of the same answer; under temperature the models publish no field.
    basin: !i.paired ? "plain" : onConditions && !temperature ? "standing" : "plain",
  };
  const activeGroups = { ...i.groups };
  return {
    onConditions, quantity, standingParam,
    standingHorizon: temperature ? 0 : i.horizon,
    fieldOn: fieldAnswers(i.tab, i.flowParam) && i.zoomedOut,
    horizonsShown: onConditions && !temperature,
    asksVisible: onConditions,
    modes, activeGroups,
    // THE GROUND IS NOT A COLOURING: it moves no mode and no group (one colouring pathway), only what is under them
    basemap: basemapInUse(i.basemap, i.imageryUp),
    basemapNote: i.basemap === "satellite" && !i.imageryUp ? SATELLITE_OFFLINE : null,
  };
}

/**
 * `mapModes`, memoised on its inputs — and its LOOK (`modes`, `activeGroups`) kept as the same objects while their
 * content is the same. A horizon change moves the standings, never a layer's mode; handed a new `modes` the map re-ran
 * its whole look pass (139 `setPaintProperty` per horizon click, measured 2026-10-21). Content-compared rather than
 * memoised on a hand-picked subset of inputs, so a later rule that makes a mode depend on anything stays correct.
 */
export function useMapModes(i: MapModeInput): MapModesVM {
  const { tab, flowParam, horizon, zoomedOut, paired, statusOk, water, groups, basemap, imageryUp, salmon } = i;
  const vm = useMemo(
    () => mapModes({ tab, flowParam, horizon, zoomedOut, paired, statusOk, water, groups, basemap, imageryUp,
                     salmon }),
    [tab, flowParam, horizon, zoomedOut, paired, statusOk, water, groups, basemap, imageryUp, salmon]);
  const modes = useSameContent(vm.modes);
  const activeGroups = useSameContent(vm.activeGroups);
  return useMemo(() => ({ ...vm, modes, activeGroups }), [vm, modes, activeGroups]);
}

/** The value last returned while its content (a flat record of strings or booleans) is unchanged; else the new one. */
function useSameContent<T extends Readonly<Record<string, unknown>>>(v: T): T {
  const kept = useRef<{ key: string; v: T } | null>(null);
  const key = JSON.stringify(Object.entries(v).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)));
  if (kept.current?.key !== key) kept.current = { key, v };
  return kept.current.v;
}

/**
 * HOW THE DAY'S STATUS REACHES THE MAP: the index and the day, on BOTH tabs (a tab switch is not a withdrawal, which
 * would clear every label); the map labels each tile itself — both renderers (T74). Only on a proven pair with an
 * index that matched (`statusOk`); otherwise nothing.
 */
export function statusFeed(i: { statusOk: boolean; index: StatusIndex | null; day: Date }):
    { index: StatusIndex; day: Date } | null {
  return i.statusOk && i.index ? { index: i.index, day: i.day } : null;
}
