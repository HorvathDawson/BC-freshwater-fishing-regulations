/**
 * THE ROUTE (P13): one value says what the app is showing, and one pure reducer moves it. It replaces the Shell's
 * implicit priority chain (open spot → adding → item → search → conditions → map, with the tab bar clearing four
 * separate states), so "back" is a function a test can call.
 *
 *   browse   the four tabs (Map · Conditions · Spots · More, T48), the bottom bar shown. SEARCH is no tab: it opens over
 *            either map (`search`), from the top bar's field, and back closes it before anything else. A spot opens
 *            over the tab it was picked on (the Spots tab, or a map's long press), and adding one starts at a point when
 *            the reader gave one (a long press, a tap in pick mode, their location).
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
 * AN UNNAMED REACH (no registry item: most streams) is a frame with `item: null`, identified by its section. It HAS
 * regulations (T53, owner 2026-10-22: its region's base plus any override reaching it, which the pipeline answers per
 * section), so it offers Regulations and Conditions (`frameViews`); only the water Map is a named water's. From the Map
 * tab Regulations is in front, from the Conditions map Conditions — the same frame either way.
 *
 * ONE BACK RULE: the views are lateral, not history — in water mode back leaves the water on screen whatever view is in
 * front: to the water the reader flowed in from, else to the browse state underneath, whole (the tab, the open spot,
 * the map camera the Shell keeps). In browse mode back closes what is open on the tab (a spot being added, a spot,
 * search), then returns to the Map tab (the start tab: Android's "fixed start destination" — back from Conditions,
 * Spots or More goes to the map, as it does in Google's own apps and on the web's history), and on the Map tab itself
 * stays: depth 0, where Android's back leaves the app. ONE EXCEPTION (owner 2026-10-22): More is a detour, so back from
 * it returns first to the tab it was opened from (`prev`) — the ‹ at its top and the system back alike.
 *
 * BACK STEPS OUT ONE LAYER, IT NEVER UNDOES A MOVE (owner 2026-10-22 late): every move within one water — a view, a part,
 * a split lake's other closure area, the same water opened again — REPLACES the top frame; only a DIFFERENT water opened
 * from inside is a new frame, and one already held is gone back to rather than stacked again (`enter`). A river that
 * changes its name is ONE system (owner 2026-10-22 late): ▲ / ▼ into the water this one continues as / from (`cross`)
 * replaces the top frame too; "Flows into" from a tributary stays a new frame. `close` (the
 * header's ×) leaves every water at once. The browser's history and Android's back count these same frames
 * (`backDepth`), so all three inputs agree; `tools/back-layers.test.ts` holds the rule, mutation-pinned.
 */
import { useReducer } from "react";
import { WATER_VIEWS, type WaterView } from "@app/core";
import type { ItemId, SectionId } from "@app/data";

export type BrowseTab = "map" | "conditions" | "spots" | "more";
/** The tabs search opens over (T48): the two maps. Spots and More have no map behind them to search on. */
export const SEARCH_TABS: readonly BrowseTab[] = ["map", "conditions"];
export type View = WaterView;
export type SpotId = string;
/** Where on a reach a Conditions tap landed: the "you are here" end of its route map. */
export interface LatLon { lat: number; lon: number }

/**
 * One water on screen. `item` null: an unnamed reach (then `tapSid` is set and `view` is not "map"). `at`: where on
 * `tapSid` the tap landed (the Conditions view's "you are here"), null when not tapped there.
 */
export interface WaterFrame {
  item: ItemId | null; part: number | null; tapSid: SectionId | null; at: LatLon | null; view: View;
}
/** A spot being added: at the point the reader gave (a long press, a tap in pick mode, their location), or none yet. */
export interface AddingSpot { at: LatLon | null }
export interface BrowseRoute {
  mode: "browse"; tab: BrowseTab;
  spot: SpotId | null; adding: AddingSpot | null;
  /**
   * (T48) Search is open over this map: the top bar's field has focus or its results sheet is up. Back closes it first;
   * a tab change closes it. A water opened FROM A RESULT consumes it (owner 2026-10-22 late, `fromSearch`): back from the
   * water lands on the map, search closed. A water opened by a tap on the map beside it keeps it. Only on the maps
   * (`SEARCH_TABS`).
   */
  search: boolean;
  /**
   * (Owner 2026-10-22, "add a back to top to go to previous tab") On the More tab only: the tab it was opened from, when
   * that was not the Map tab. Back from More — its ‹ at the top, Android's back, the browser's — returns THERE (then
   * the Map tab, as from any tab). Absent: the Map tab.
   */
  prev?: BrowseTab;
}
export interface WaterRoute { mode: "water"; stack: readonly WaterFrame[]; from: BrowseRoute }
export type Route = BrowseRoute | WaterRoute;

export type RouteAction =
  | { type: "tab"; tab: BrowseTab }
  /**
   * Built ONLY by `openWater` (openWater.ts). `at`: where on `tapSid` the tap landed — kept only with a `tapSid`.
   * `view`: the view in front ("regs" when absent). `item` null: an unnamed reach (Regulations or Conditions; T53).
   */
  | { type: "openWater"; item: ItemId | null; tapSid?: SectionId | null; part?: number | null; at?: LatLon | null;
      view?: View;
      /**
       * (Owner 2026-10-22 late, "back from a searched water lands on the map") Opened from a search result: the search
       * is consumed — `from` is the map underneath with search CLOSED, so back and × land on the map, not the list.
       */
      fromSearch?: boolean }
  /** (T48) Open or close search over the map on screen. */
  | { type: "search"; open: boolean }
  | { type: "view"; view: View }
  /**
   * The frame's part (the water map's step, "Show regulations for this part"). `item`: a split lake's OTHER closure
   * area — a sibling of the same water — which becomes the frame's water; the tap it was opened by was on another water,
   * so the frame forgets it.
   */
  | { type: "part"; part: number; item?: ItemId }
  | { type: "flowInto"; item: ItemId; part: number }
  /**
   * (Owner 2026-10-22 late, "a river that changes its name is ONE system") ▲ / ▼ off the end of a water into the water
   * it CONTINUES AS / FROM (`water_continue`: the Vedder ↔ Chilliwack): that water at `part`, its Map in front, IN
   * PLACE of the top frame — so to and fro never stacks and one back leaves the whole system. Built ONLY by
   * `crossAction` (waterMap.ts). Nothing in browse mode.
   */
  | { type: "cross"; item: ItemId; part: number }
  | { type: "back" }
  /**
   * (Owner 2026-10-22 late) The water header's ×: leave EVERY water at once, for where the first was opened from
   * (`from`, whole). Nothing in browse mode.
   */
  | { type: "close" }
  | { type: "openSpot"; id: SpotId }
  /** Start adding a spot, at a point when one was given (a long press, a tap in pick mode, the reader's location). */
  | { type: "addSpot"; at?: LatLon | null };

/** The water stack holds at most this many frames (APP-PREP §0 rule 4): the oldest goes first. */
export const STACK_CAP = 8;

export const HOME: BrowseRoute = { mode: "browse", tab: "map", spot: null, adding: null, search: false };

/**
 * The views a frame offers: all three for a water. An unnamed reach (T53) has its Regulations (the store answers its
 * section: `RegsTarget` kind "section") and its Conditions, but no water map — the map frames a named water's parts.
 */
export const REACH_ONLY: readonly WaterView[] = ["regs", "conditions"];
export function frameViews(f: Pick<WaterFrame, "item">): readonly WaterView[] {
  return f.item === null ? REACH_ONLY : WATER_VIEWS;
}

const top = (r: WaterRoute): WaterFrame => r.stack[r.stack.length - 1]!;
const withTop = (r: WaterRoute, f: Partial<WaterFrame>): WaterRoute =>
  ({ ...r, stack: [...r.stack.slice(0, -1), { ...top(r), ...f }] });
const push = (r: WaterRoute, f: WaterFrame): WaterRoute => ({ ...r, stack: [...r.stack, f].slice(-STACK_CAP) });
/** A frame's water: its item, or for an unnamed reach (no item) the section it stands on. */
const sameWater = (a: WaterFrame, b: WaterFrame) =>
  a.item !== null || b.item !== null ? a.item === b.item : a.tapSid === b.tapSid;
/**
 * BACK STEPS OUT ONE LAYER (owner 2026-10-22 late): a water opened from inside water mode is ONE layer only when it is
 * a different water from every frame held. The water on screen opened again (a tap on its own section) replaces its
 * frame — a move within it; a water already under the top (A -> B -> A, ▲ then ▼ across a name change) is gone BACK
 * to, the frames above it dropped, so crossing back and forth never stacks and back from it is where A was opened from.
 */
const enter = (r: WaterRoute, f: WaterFrame): WaterRoute => {
  const i = r.stack.findIndex((s) => sameWater(s, f));
  return i < 0 ? push(r, f) : { ...r, stack: [...r.stack.slice(0, i), f] };
};

/** THE ONE FRAME BUILDER (an open and a flow alike); null for a frame that cannot be (an unnamed reach on Map, or none). */
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
      case "tab": {
        const next: BrowseRoute = { mode: "browse", tab: a.tab, spot: null, adding: null, search: false };
        // More remembers the tab it came from (kept when More is pressed again); the Map tab is the default, unsaid
        const prev = a.tab !== "more" ? undefined : r.tab === "more" ? r.prev : r.tab;
        return prev !== undefined && prev !== HOME.tab ? { ...next, prev } : next;
      }
      case "search":
        if (a.open === r.search || (a.open && !SEARCH_TABS.includes(r.tab))) return r;
        return { ...r, search: a.open };
      case "openWater": {
        const f = opened(a);
        return f === null ? r : { mode: "water", from: a.fromSearch ? { ...r, search: false } : r, stack: [f] };
      }
      // a spot opens over the map it was picked on (the rail's menu); back returns to that map
      case "openSpot": return { ...r, spot: a.id, adding: null, search: false };
      case "addSpot": return { ...r, adding: { at: a.at ?? null }, spot: null, search: false };
      case "back":
        if (r.adding !== null) return { ...r, adding: null };
        if (r.spot !== null) return { ...r, spot: null };
        if (r.search) return { ...r, search: false };
        if (r.tab !== HOME.tab) return r.prev !== undefined ? { ...HOME, tab: r.prev } : HOME;
        return r;
      default: return r;            // view / part / flowInto / cross mean nothing without a water
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
      return f === null ? r : enter(r, f);
    }
    case "openWater": {
      const f = opened(a);
      return f === null ? r : enter(r, f);
    }
    case "cross": {                 // one system: the top frame is replaced (a water already under it is gone back to)
      const f = opened({ type: "openWater", item: a.item, part: a.part, view: "map" });
      return f === null ? r : enter({ ...r, stack: r.stack.slice(0, -1) }, f);
    }
    case "back":                    // the views are lateral: back leaves the water, whatever view is in front
      if (r.stack.length > 1) return { ...r, stack: r.stack.slice(0, -1) };
      return r.from;
    case "close": return r.from;    // every water at once
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
