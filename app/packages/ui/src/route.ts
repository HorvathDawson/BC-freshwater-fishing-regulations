/**
 * THE ROUTE (P13): one value says what the app is showing, and one pure reducer moves it. It replaces the Shell's
 * implicit priority chain (open spot → adding → item → search → conditions → map, with the tab bar clearing four
 * separate states), so "back" is a function a test can call.
 *
 *   browse   the four tabs (Map · Search · Conditions · Spots), the bottom bar shown.
 *   water    one water latched (APP-PREP §0): a STACK of frames, each a water at a part and a view (regs · map ·
 *            conditions). The bottom bar is hidden. `from` is the browse state underneath, kept whole, so leaving
 *            restores the tab, the Conditions drill and the open spot exactly. The map camera is not here (it would
 *            re-render on every pan): the Shell keeps it in a ref and restores it on the last back.
 *
 * ONE BACK RULE: in water mode, a map or conditions view goes back to the regs; the regs of a water the reader flowed
 * into go back to the previous water; the regs of the first water leave water mode. In browse mode back closes what is
 * open on the tab (a spot being added, a spot, a Conditions section), and otherwise stays.
 */
import { useReducer } from "react";
import type { ItemId, SectionId } from "@app/data";

export type BrowseTab = "map" | "search" | "conditions" | "spots";
export type View = "regs" | "map" | "conditions";
export type SpotId = string;
/** Where on a reach a Conditions tap landed: the "you are here" end of its route map. */
export interface LatLon { lat: number; lon: number }

export interface WaterFrame { item: ItemId; part: number | null; tapSid: SectionId | null; view: View }
export interface BrowseRoute {
  mode: "browse"; tab: BrowseTab; cond: { section: SectionId; at: LatLon | null } | null;
  spot: SpotId | null; adding: boolean;
}
export interface WaterRoute { mode: "water"; stack: readonly WaterFrame[]; from: BrowseRoute }
export type Route = BrowseRoute | WaterRoute;

export type RouteAction =
  | { type: "tab"; tab: BrowseTab }
  | { type: "openWater"; item: ItemId; tapSid?: SectionId; part?: number }
  | { type: "view"; view: View }
  | { type: "part"; part: number }
  | { type: "flowInto"; item: ItemId; part: number }
  | { type: "back" }
  | { type: "openSpot"; id: SpotId }
  | { type: "addSpot" }
  | { type: "condSection"; section: SectionId; at?: LatLon | null };

/** The water stack holds at most this many frames (APP-PREP §0 rule 4): the oldest goes first. */
export const STACK_CAP = 8;

export const HOME: BrowseRoute = { mode: "browse", tab: "map", cond: null, spot: null, adding: false };

const top = (r: WaterRoute): WaterFrame => r.stack[r.stack.length - 1]!;
const withTop = (r: WaterRoute, f: Partial<WaterFrame>): WaterRoute =>
  ({ ...r, stack: [...r.stack.slice(0, -1), { ...top(r), ...f }] });
const push = (r: WaterRoute, f: WaterFrame): WaterRoute => ({ ...r, stack: [...r.stack, f].slice(-STACK_CAP) });

export function reduce(r: Route, a: RouteAction): Route {
  if (r.mode === "browse") {
    switch (a.type) {
      case "tab": return { mode: "browse", tab: a.tab, cond: null, spot: null, adding: false };
      case "openWater":
        return { mode: "water", from: r,
                 stack: [{ item: a.item, part: a.part ?? null, tapSid: a.tapSid ?? null, view: "regs" }] };
      case "openSpot": return { ...r, tab: "spots", spot: a.id, adding: false };
      case "addSpot": return { ...r, adding: true, spot: null };
      case "condSection": return { ...r, tab: "conditions", cond: { section: a.section, at: a.at ?? null } };
      case "back":
        if (r.adding) return { ...r, adding: false };
        if (r.spot !== null) return { ...r, spot: null };
        if (r.cond !== null) return { ...r, cond: null };
        return r;
      default: return r;            // view / part / flowInto mean nothing without a water
    }
  }
  switch (a.type) {
    case "view": return withTop(r, { view: a.view });
    case "part": return withTop(r, { part: a.part });
    case "flowInto": return push(r, { item: a.item, part: a.part, tapSid: null, view: "regs" });
    case "openWater": return push(r, { item: a.item, part: a.part ?? null, tapSid: a.tapSid ?? null, view: "regs" });
    case "back":
      if (top(r).view !== "regs") return withTop(r, { view: "regs" });
      if (r.stack.length > 1) return { ...r, stack: r.stack.slice(0, -1) };
      return r.from;
    default: return r;              // a tab, a spot or a Conditions drill is impossible: there is no bar
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
  return r.mode === "browse" && !r.adding;
}

export function useRoute(initial: Route = HOME): [Route, (a: RouteAction) => void] {
  return useReducer(reduce, initial);
}
