/**
 * GESTURES, AS ROUTE ACTIONS (G1). Every gesture ends as one of the route's own actions, so a swipe and a button can
 * never do two different things:
 *
 *   useBack       the ONE back function. The ‹ button, Android's system back (button and edge gesture, through
 *                 `BackHandler`), the browser's back button and a phone browser's own swipe-back (through the History
 *                 API) and the iOS left-edge swipe all call it, and it dispatches `{type: "back"}` (APP-PREP §0 rule 1)
 *                 — unless a sheet is open (`useBackLayer`), which it closes first.
 *   historySync   keeps the browser's history and the route in step: one history entry per back step the route holds,
 *                 so the browser's back pops exactly one frame and the ‹ button never leaves a stranded entry.
 *   useViewSwipe  a sideways swipe on a water → `{type: "view"}` on the neighbouring view (Regulations · Conditions ·
 *                 Map), among the views its frame offers.
 *   useEdgeBack / useSheetDrag   the thresholds a drag is measured against, for the components that only measure.
 *
 * No react-native here (layers.json): the platform glue — `BackHandler`, `window.history` — lives in the app shell and
 * reaches this file as a `SystemBack`, and the gesture recognisers are `@app/ui-native`'s.
 */
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { EDGE_BACK, classifySwipe, edgeBackCommits, edgeBackEnabled, edgeBackStarts, follow, sheetDismiss,
         stepView, viewSwipeStarts, type SwipeStep, type WaterView } from "@app/core";
import { frameViews, reduce, type Route, type RouteAction, type View } from "./route";

const BACK: RouteAction = { type: "back" };

/**
 * How many times back would do something from here: the route's own depth, counted by the reducer that pops it (so it
 * cannot disagree with it). 0 is the browse root, where a system back leaves the app.
 */
export function backDepth(r: Route): number {
  let n = 0;
  for (let at = r; n < 64; n++) {
    const next = reduce(at, BACK);
    if (next === at) break;
    at = next;
  }
  return n;
}

/**
 * A platform's back input, attached by the app shell (`apps/mobile/src/systemBack.native.ts` / `.web.ts`): given the
 * one back function, it starts listening and returns how to keep it informed and how to stop.
 */
export type SystemBack = (back: () => boolean) => SystemBackBinding;
export interface SystemBackBinding {
  /** The route's back depth changed (`backDepth`). The web pushes or drops history entries to match; native ignores it. */
  sync(depth: number): void;
  dispose(): void;
}

export interface Back {
  /**
   * Close the top back layer (an open sheet) or, with none open, pop one frame. False when there was nothing to do
   * (the browse root, no sheet): Android then leaves the app as usual.
   */
  back: () => boolean;
  /** The back steps held: the route's own (`backDepth`) plus one per open back layer. */
  depth: number;
  /** Where an open sheet registers (`BackLayerContext` / `useBackLayer`). */
  layers: BackLayers;
}

/**
 * BACK LAYERS: what back closes before it pops the route. A sheet (Layers) is not a route frame — it does not belong
 * in the route, a reload or a link — but back must close it first on every platform: Android's Modal did so through
 * `onRequestClose`, while the browser's back went under it (and at the browse root left the app with the sheet open).
 * An open sheet registers here; it counts as one back step (so on the web it holds one history entry, dropped again
 * when it closes any other way), and back closes the most recent one before anything else.
 */
export interface BackLayers {
  /** An open layer and how to close it; returns its removal (the layer closed some other way: Done, scrim, drag). */
  register(close: () => void): () => void;
  /** The one back (`Back.back`), for a layer's own platform back — Android's Modal `onRequestClose`. */
  back: () => boolean;
}

export const BackLayerContext = createContext<BackLayers | null>(null);

/**
 * A surface's registration while `open` — EVERY surface that back must close before it pops the route registers here:
 * a sheet (`<Sheet>` does it for you), a menu, a dialog, a confirm, an in-screen step (a spot being captured, a town's
 * list in search). See app/HANDOFF-ui.md "Back, everywhere" for the table and how a new surface joins it.
 *
 * `close` is what one back does to this surface. It need not close it: a back that only takes ONE STEP inside it (a
 * capture's step, an edit that asks before it drops anything) leaves it `open`, and the layer is then registered
 * again, on top — so the next back reaches it again rather than going under it. A surface that closes (`open` turns
 * false) is simply gone.
 *
 * Returns what the platform's own back for the surface calls (a Modal's `onRequestClose`): the one back where there is
 * one to register with, else the surface's own close.
 */
export function useBackLayer(open: boolean, close: () => void): () => void {
  const layers = useContext(BackLayerContext);
  const latest = useRef(close);
  latest.current = close;
  // bumped by every back that reached this layer: still open afterwards -> registered again (the effect re-runs)
  const [reached, setReached] = useState(0);
  useEffect(() => {
    if (!open || !layers) return;
    return layers.register(() => { latest.current(); setReached((n) => n + 1); });
  }, [open, layers, reached]);
  return useCallback(() => { if (layers) layers.back(); else latest.current(); }, [layers]);
}

/**
 * THE ONE BACK. `back()` reads the latest route through a ref, so two backs before a re-render (the browser going
 * back two entries at once) pop two frames rather than the same one twice; a layer leaves the list as it is closed,
 * for the same reason.
 */
export function useBack(route: Route, go: (a: RouteAction) => void, system?: SystemBack | null): Back {
  const latest = useRef(route);
  latest.current = route;
  const open = useRef<{ close: () => void }[]>([]);
  const [layerCount, setLayerCount] = useState(0);
  const back = useCallback(() => {
    const top = open.current.pop();
    if (top) {
      setLayerCount(open.current.length);
      top.close();
      return true;
    }
    const r = latest.current;
    const next = reduce(r, BACK);
    if (next === r) return false;
    latest.current = next;
    go(BACK);
    return true;
  }, [go]);
  const layers = useMemo<BackLayers>(() => ({
    back,
    register: (close) => {
      const layer = { close };
      open.current.push(layer);
      setLayerCount(open.current.length);
      return () => {
        const i = open.current.indexOf(layer);
        if (i < 0) return;                  // back closed it, and already took it off
        open.current.splice(i, 1);
        setLayerCount(open.current.length);
      };
    },
  }), [back]);
  const depth = backDepth(route) + layerCount;

  const binding = useRef<SystemBackBinding | null>(null);
  const depthNow = useRef(depth);
  depthNow.current = depth;
  useEffect(() => {
    if (!system) return;
    const b = system(back);
    binding.current = b;
    b.sync(depthNow.current);
    return () => { binding.current = null; b.dispose(); };
  }, [system, back]);
  useEffect(() => { binding.current?.sync(depth); }, [depth]);
  return { back, depth, layers };
}

/**
 * The few calls of the History API the sync needs, so it can be driven by a test (no DOM in this package). `index` is
 * the back depth stored on the current entry, null on an entry this app did not push (the page's own first one).
 */
export interface HistoryPort {
  index(): number | null;
  push(index: number): void;
  go(delta: number): void;
  listen(onPop: () => void): () => void;
}

/**
 * THE BROWSER'S HISTORY, IN STEP WITH THE ROUTE. Entry i above the page's own carries `index = i`, and there is one
 * entry per back step the route holds:
 *
 *   the route got deeper (a water opened, a view pushed)   push the missing entries
 *   the route got shallower (‹, a tab change)              `go(-n)` to drop them — and the popstate that follows lands
 *                                                          on the index we already hold, so it pops nothing (no double pop)
 *   popstate to a lower index (the browser's back)         the browser already dropped the entries: pop the route that
 *                                                          many times
 *   popstate to a higher index (the browser's forward)     refused — a popped frame is gone — by going back to where we are
 *
 * While a `go` is in flight nothing is pushed: a push before the traversal lands would be the entry it removes.
 */
export function historySync(port: HistoryPort, back: () => boolean): SystemBackBinding {
  let at = port.index() ?? 0;
  let inFlight = false;
  let want: number | null = null;

  const travel = (delta: number) => { inFlight = true; port.go(delta); };
  let popping = false;
  const sync = (depth: number) => {
    if (inFlight || popping) { want = depth; return; }
    want = null;
    if (depth > at) { for (let i = at + 1; i <= depth; i++) port.push(i); at = depth; }
    else if (depth < at) { const n = at - depth; at = depth; travel(-n); }
  };
  const stop = port.listen(() => {
    const idx = port.index() ?? 0;
    inFlight = false;
    // our own traversal lands on `at`, which was set when it was asked for: nothing below pops (no double pop)
    if (idx < at) {
      // the browser already dropped these entries: pop the route as many times, and only then hear its depth
      const n = at - idx;
      at = idx;
      popping = true;
      try { for (let i = 0; i < n; i++) if (!back()) break; } finally { popping = false; }
    } else if (idx > at) {
      travel(at - idx);
    }
    if (want !== null) sync(want);
  });
  return { sync, dispose: stop };
}

/**
 * The back steps the iOS left-edge swipe may take: `depth` (`useBack`'s) less the last step when it is a TAB RETURN
 * (another tab's root → the Map tab). That step is Android's convention (a fixed start destination) and the browser's
 * (a tab change is history); on iOS tabs are not history and a tab root has nothing to swipe back to.
 */
export function edgeBackDepth(r: Route, depth: number): number {
  let at = r;
  for (let n = 0; n < 64; n++) {
    const next = reduce(at, BACK);
    if (next === at) return depth;
    const tabReturn = at.mode === "browse" && !at.adding && at.spot === null && !at.search;
    if (tabReturn) return Math.max(0, depth - 1);
    at = next;
  }
  return depth;
}

/** The left-edge back swipe's measurements (iOS only — `edgeBackEnabled`). */
export interface EdgeBackDrag {
  /** This platform has the swipe at all (iOS). Fixed for the life of the app. */
  available: boolean;
  /** Available, and there is something to go back to. */
  enabled: boolean;
  zone: number;
  starts(x: number): boolean;
  /** Where the view sits for a finger `dx` px right of where it started: it follows, never left of home. */
  offset(dx: number): number;
  /** Released: goes back (and says so) past the commit distance or on a flick. */
  release(dx: number, vx: number): boolean;
}

/** `os` is the platform's name (`Platform.OS`); `depth` the route's back depth (`useBack`). */
export function useEdgeBack(back: () => boolean, os: string, depth: number): EdgeBackDrag {
  const available = edgeBackEnabled(os);
  const enabled = available && depth > 0;
  return useMemo(() => ({
    available, enabled, zone: EDGE_BACK.zone,
    starts: edgeBackStarts,
    offset: (dx: number) => Math.max(0, dx),
    release: (dx: number, vx: number) => edgeBackCommits(dx, vx) && back(),
  }), [back, available, enabled]);
}

/** A bottom sheet's downward drag. */
export interface SheetDrag {
  offset(dy: number): number;
  /** Released: closes (and says so) past a third of its height or on a downward flick; otherwise springs back. */
  release(dy: number, vy: number, height: number): boolean;
}

export function useSheetDrag(onClose: () => void): SheetDrag {
  return useMemo(() => ({
    offset: (dy: number) => Math.max(0, dy),
    release: (dy: number, vy: number, height: number) => {
      if (!sheetDismiss(dy, vy, height)) return false;
      onClose();
      return true;
    },
  }), [onClose]);
}

/** The view on screen, and the views its frame offers, or null where a swipe means nothing (browse). */
const viewHere = (route: Route): { at: View; within: readonly WaterView[] } | null => {
  if (route.mode !== "water") return null;
  const f = route.stack[route.stack.length - 1]!;
  return { at: f.view, within: frameViews(f) };
};

/**
 * The target of a finished sideways swipe, or null: the neighbouring view in the strip's order (`@app/core`
 * `WATER_VIEWS`), finger left = the view to the right; nothing past either end, and nothing in browse (a tap on either
 * map opens the water screen itself — there is no browse-mode reach to swipe from).
 */
export function viewSwipeAction(route: Route, step: SwipeStep): RouteAction | null {
  const here = viewHere(route);
  if (here === null) return null;
  const to = stepView(here.at, step, here.within);
  return to === null ? null : { type: "view", view: to };
}

export interface ViewSwipe {
  /** False where there is nothing to move between (browse, or a frame with one view). */
  enabled: boolean;
  starts(x: number, width: number): boolean;
  /** Where the view sits for a finger `dx` px along: with it toward a neighbour, held back toward an end. */
  offset(dx: number): number;
  /** Released: moves (and says which way) or does nothing. */
  release(dx: number, dy: number, vx: number): SwipeStep | null;
}

/** The view swipe's measurements. */
export function useViewSwipe(route: Route, go: (a: RouteAction) => void): ViewSwipe {
  const latest = useRef(route);
  latest.current = route;
  const here = viewHere(route);
  const at = here?.at ?? null;
  const within = here?.within ?? null;
  return useMemo(() => ({
    enabled: at !== null && within !== null && within.length > 1,
    starts: viewSwipeStarts,
    offset: (dx: number) =>
      follow(dx, at !== null && within !== null && stepView(at, dx < 0 ? "next" : "prev", within) !== null),
    release: (dx: number, dy: number, vx: number) => {
      const step = classifySwipe(dx, dy, vx);
      if (step === null) return null;
      const t = viewSwipeAction(latest.current, step);
      if (t === null) return null;
      go(t);
      return step;
    },
  }), [at, within, go]);
}
