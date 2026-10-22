/**
 * THE ROUTE (P13): one value says what the app is showing, and one pure reducer moves it. It replaces the Shell's
 * implicit priority chain (open spot → adding → item → search → conditions → map, with the tab bar clearing four
 * separate states), so "back" is a function a test can call.
 *
 *   browse   the three tabs (Map · Search · Conditions), the bottom bar shown. The spots are the maps' rail's (owner
 *            2026-10-21): a spot opens over the map it was picked on, and adding one starts at a point when the
 *            reader gave one (a long press, the map's middle, their location).
 *   water    one water latched (APP-PREP §0): a STACK of frames, each a water at a part and a view (regs · map ·
 *            conditions). The bottom bar is hidden. `from` is the browse state underneath, kept whole, so leaving
 *            restores the tab (the Conditions map or the Map tab) and the open spot exactly. The camera is not here (it would
 *            re-render on every pan): the Shell keeps it in a ref and restores it on the last back.
 *
 * THE VIEWS ARE ONE STRIP (owner 2026-10-21): Regulations · Conditions · Map (`@app/core` `WATER_VIEWS`), moved between by
 * the strip and the sideways swipe (`viewSwipeAction`). A frame keeps the point the reader tapped (`tapSid`, `at`), so
 * every view of a water opened by a tap is about that point — the Conditions view is that reach, "you are here" and all.
 *
 * ONE WATER SCREEN, WHICHEVER MAP YOU TAPPED (owner rule 2026-10-21): a tap on the Conditions map and a tap on the Map
 * tab open the SAME frame — same water, same part, same section, same point — differing only in `view`. There is no
 * second Conditions screen in browse mode (the old `cond` drill is gone): the Conditions view of the water screen IS
 * the reach panel. Every water is opened by `openWater` (openWater.ts), the one place a tap's item is resolved.
 *
 * AN UNNAMED REACH (no registry item: most gauged reaches) has no regulations page of its own, so its frame has
 * `item: null` and offers the Conditions view only (`frameViews`). `openWater` puts Conditions in front for it from
 * either map, so both maps open the same frame; the reducer refuses any other view for it.
 *
 * ONE BACK RULE: the views are lateral, not history — in water mode back leaves the water on screen whatever view is in
 * front: to the water the reader flowed in from, else to the browse state underneath, whole (the tab, the open spot,
 * the map camera the Shell keeps). In browse mode back closes what is open on the tab (a spot being added, a spot),
 * returns a water search's eye SHOWED on the map to search, then returns to the Map tab (the start tab: Android's
 * "fixed start destination" — back from Search, Conditions or Spots goes to the map, as it does in Google's own apps
 * and on the web's history), and on the Map tab itself stays: depth 0, where Android's back leaves the app.
 */
import { useReducer } from "react";
import { WATER_VIEWS, type WaterView } from "@app/core";
import type { ItemId, SectionId } from "@app/data";

export type BrowseTab = "map" | "search" | "conditions";
export type View = WaterView;
export type SpotId = string;
/** Where on a reach a Conditions tap landed: the "you are here" end of its route map. */
export interface LatLon { lat: number; lon: number }

/**
 * One water on screen. `item` null: an unnamed reach (then `tapSid` is set and `view` is "conditions"). `at`: where on
 * `tapSid` the tap landed (the Conditions view's "you are here"), null when not tapped there.
 */
export interface WaterFrame {
  item: ItemId | null; part: number | null; tapSid: SectionId | null; at: LatLon | null; view: View;
}
/** A spot being added: at the point the reader gave (a long press, the map's middle, their location), or none yet. */
export interface AddingSpot { at: LatLon | null }
export interface BrowseRoute {
  mode: "browse"; tab: BrowseTab;
  spot: SpotId | null; adding: AddingSpot | null;
  /**
   * A water SHOWN on the main map by search's eye — framed and lit there, the bar still up. Back returns to search
   * (which keeps its query and selection: the Shell holds them). Any tab change forgets it.
   */
  shown: ItemId | null;
}
export interface WaterRoute { mode: "water"; stack: readonly WaterFrame[]; from: BrowseRoute }
export type Route = BrowseRoute | WaterRoute;

export type RouteAction =
  | { type: "tab"; tab: BrowseTab }
  /**
   * Built ONLY by `openWater` (openWater.ts). `at`: where on `tapSid` the tap landed — kept only with a `tapSid`.
   * `view`: the view in front ("regs" when absent). `item` null: an unnamed reach, opened on Conditions only.
   */
  | { type: "openWater"; item: ItemId | null; tapSid?: SectionId | null; part?: number | null; at?: LatLon | null;
      view?: View }
  /** Search's eye: show this water on the main map, and come back to search on back. */
  | { type: "show"; item: ItemId }
  | { type: "view"; view: View }
  /**
   * The frame's part (the water map's step, "Show regulations for this part"). `item`: a split lake's OTHER closure
   * area — a sibling of the same water — which becomes the frame's water; the tap it was opened by was on another water,
   * so the frame forgets it.
   */
  | { type: "part"; part: number; item?: ItemId }
  | { type: "flowInto"; item: ItemId; part: number }
  | { type: "back" }
  | { type: "openSpot"; id: SpotId }
  /** Start adding a spot, at a point when one was given (a long press, the map's middle, the reader's location). */
  | { type: "addSpot"; at?: LatLon | null };

/** The water stack holds at most this many frames (APP-PREP §0 rule 4): the oldest goes first. */
export const STACK_CAP = 8;

export const HOME: BrowseRoute = { mode: "browse", tab: "map", spot: null, adding: null, shown: null };

/** The views a frame offers: all three for a water; an unnamed reach has no regulations or water map, only Conditions. */
export const REACH_ONLY: readonly WaterView[] = ["conditions"];
export function frameViews(f: Pick<WaterFrame, "item">): readonly WaterView[] {
  return f.item === null ? REACH_ONLY : WATER_VIEWS;
}

const top = (r: WaterRoute): WaterFrame => r.stack[r.stack.length - 1]!;
const withTop = (r: WaterRoute, f: Partial<WaterFrame>): WaterRoute =>
  ({ ...r, stack: [...r.stack.slice(0, -1), { ...top(r), ...f }] });
const push = (r: WaterRoute, f: WaterFrame): WaterRoute => ({ ...r, stack: [...r.stack, f].slice(-STACK_CAP) });

/** THE ONE FRAME BUILDER (an open and a flow alike); null for a frame that cannot be (an unnamed reach off Conditions). */
const opened = (a: Extract<RouteAction, { type: "openWater" }>): WaterFrame | null => {
  const tapSid = a.tapSid ?? null;
  const f: WaterFrame = { item: a.item, part: a.part ?? null, tapSid, at: tapSid !== null ? a.at ?? null : null,
                          view: a.view ?? "regs" };
  if (f.item === null && tapSid === null) return null;
  return frameViews(f).includes(f.view) ? f : null;
};

export function reduce(r: Route, a: RouteAction): Route {
  if (r.mode === "browse") {
    switch (a.type) {
      case "tab": return { mode: "browse", tab: a.tab, spot: null, adding: null, shown: null };
      case "show": return { mode: "browse", tab: "map", spot: null, adding: null, shown: a.item };
      case "openWater": {
        const f = opened(a);
        return f === null ? r : { mode: "water", from: r, stack: [f] };
      }
      // a spot opens over the map it was picked on (the rail's menu); back returns to that map
      case "openSpot": return { ...r, spot: a.id, adding: null };
      case "addSpot": return { ...r, adding: { at: a.at ?? null }, spot: null };
      case "back":
        if (r.adding !== null) return { ...r, adding: null };
        if (r.spot !== null) return { ...r, spot: null };
        if (r.shown !== null) return { ...r, tab: "search", shown: null };
        if (r.tab !== HOME.tab) return HOME;
        return r;
      default: return r;            // view / part / flowInto mean nothing without a water
    }
  }
  switch (a.type) {
    case "view": return frameViews(top(r)).includes(a.view) ? withTop(r, { view: a.view }) : r;
    case "part":
      if (top(r).item === null) return r;
      return a.item === undefined || a.item === top(r).item ? withTop(r, { part: a.part })
        : withTop(r, { item: a.item, part: a.part, tapSid: null, at: null });
    case "flowInto": {
      const f = opened({ type: "openWater", item: a.item, part: a.part });
      return f === null ? r : push(r, f);
    }
    case "openWater": {
      const f = opened(a);
      return f === null ? r : push(r, f);
    }
    case "back":                    // the views are lateral: back leaves the water, whatever view is in front
      if (r.stack.length > 1) return { ...r, stack: r.stack.slice(0, -1) };
      return r.from;
    default: return r;              // a tab or a spot is impossible: there is no bar
  }
}

/**
 * The water on screen, or null while browsing (T01): its item, the part it was opened at and the section the reader
 * TAPPED to open it (null from search, a list or a flow) — what the Regulations page opens on. A new water is a new
 * frame, so a tap kept for one water never names the part of another.
 */
export function waterFrame(r: Route): WaterFrame | null {
  return r.mode === "water" ? top(r) : null;
}

/** The bottom bar: shown while browsing, hidden in water mode and while a spot is being added. */
export function bottomBarShown(r: Route): boolean {
  return r.mode === "browse" && r.adding === null;
}

export function useRoute(initial: Route = HOME): [Route, (a: RouteAction) => void] {
  return useReducer(reduce, initial);
}
