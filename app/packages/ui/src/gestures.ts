/**
 * GESTURES, AS ROUTE ACTIONS (G1). Every gesture ends as one of the route's own actions, so a swipe and a button can
 * never do two different things:
 *
 *   useBack       the ONE back function. The ‹ button, Android's system back (button and edge gesture, through
 *                 `BackHandler`), the browser's back button and a phone browser's own swipe-back (through the History
 *                 API) and the iOS left-edge swipe all call it, and it dispatches `{type: "back"}` (APP-PREP §0 rule 1).
 *   historySync   keeps the browser's history and the route in step: one history entry per back step the route holds,
 *                 so the browser's back pops exactly one frame and the ‹ button never leaves a stranded entry.
 *   usePartSwipe  a sideways swipe on a water's view → `{type: "part"}` on the neighbouring part.
 *   useEdgeBack / useSheetDrag   the thresholds a drag is measured against, for the components that only measure.
 *
 * No react-native here (layers.json): the platform glue — `BackHandler`, `window.history` — lives in the app shell and
 * reaches this file as a `SystemBack`, and the gesture recognisers are `@app/ui-native`'s.
 */
import { useCallback, useEffect, useMemo, useRef } from "react";
import { EDGE_BACK, classifySwipe, edgeBackCommits, edgeBackEnabled, edgeBackStarts, follow, partSwipeStarts, sheetDismiss,
         stepPart, type SwipeStep } from "@app/core";
import { reduce, type Route, type RouteAction } from "./route";

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
  /** Pop one frame. False when there was nothing to pop (the browse root): Android then leaves the app as usual. */
  back: () => boolean;
  depth: number;
}

/**
 * THE ONE BACK. `back()` reads the latest route through a ref, so two backs before a re-render (the browser going
 * back two entries at once) pop two frames rather than the same one twice.
 */
export function useBack(route: Route, go: (a: RouteAction) => void, system?: SystemBack | null): Back {
  const latest = useRef(route);
  latest.current = route;
  const back = useCallback(() => {
    const r = latest.current;
    const next = reduce(r, BACK);
    if (next === r) return false;
    latest.current = next;
    go(BACK);
    return true;
  }, [go]);
  const depth = backDepth(route);

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
  return { back, depth };
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

/**
 * The part a sideways swipe steps to, as a route action, or null. `order` is the water's parts (`part` values) in step
 * order, mouth first — the water map's ▼ Downstream / ▲ Upstream order — so a finger moving left ("next") goes
 * upstream. Only on a water's regs, map or conditions view, only once a part is being read, never past an end.
 */
export function partSwipeAction(route: Route, order: readonly number[] | null, step: SwipeStep): RouteAction | null {
  const at = partPosition(route, order);
  if (at === null || !order) return null;
  const to = stepPart(at, order.length, step);
  return to === null ? null : { type: "part", part: order[to]! };
}

const partPosition = (route: Route, order: readonly number[] | null): number | null => {
  if (route.mode !== "water" || !order || order.length === 0) return null;
  const part = route.stack[route.stack.length - 1]!.part;
  if (part === null) return null;
  const i = order.indexOf(part);
  return i < 0 ? null : i;
};

export interface PartSwipe {
  /** False when there is nothing to step through (browse mode, no part yet, one part, parts not known). */
  enabled: boolean;
  starts(x: number, width: number): boolean;
  /** Where the view sits for a finger `dx` px along: with it toward a neighbour, held back toward an end. */
  offset(dx: number): number;
  /** Released: steps (and says which way) or does nothing. */
  release(dx: number, dy: number, vx: number): SwipeStep | null;
}

export function usePartSwipe(route: Route, go: (a: RouteAction) => void,
                             order: readonly number[] | null): PartSwipe {
  const latest = useRef(route);
  latest.current = route;
  const at = partPosition(route, order);
  const count = order?.length ?? 0;
  return useMemo(() => ({
    enabled: at !== null && count > 1,
    starts: partSwipeStarts,
    offset: (dx: number) => follow(dx, at !== null && stepPart(at, count, dx < 0 ? "next" : "prev") !== null),
    release: (dx: number, dy: number, vx: number) => {
      const step = classifySwipe(dx, dy, vx);
      if (step === null) return null;
      const a = partSwipeAction(latest.current, order, step);
      if (a === null) return null;
      go(a);
      return step;
    },
  }), [at, count, order, go]);
}
