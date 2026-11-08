/**
 * (T257, owner 2026-10-29) COACH MARKS: the walkthrough points at the real controls, in place. What is left here is the
 * registry and the geometry its card (`Walkthrough` GuideCard) uses; the modal coach tour and the one-step mode hints
 * that drew on it were removed (owner 2026-11-08).
 *
 * TARGETS. A control that the tour may point at registers its host view under a key (`useCoachTarget("days")`); the tour
 * measures it in the window when its step comes up (`measureInWindow`, react-native and react-native-web alike) and
 * measures its own layer the same way, so the spotlight lands on the control whatever insets sit between them. A key with
 * no registered or measurable view (Standard / Salmon is not offered; no sync button with nothing to sync) drops its
 * step: the tour never describes a control that is not on screen. Registering is free outside a tour (no provider: a no-op).
 */
import { createContext, useCallback, useContext, useMemo, useRef, type ReactNode } from "react";
import { View, type ViewStyle } from "react-native";
// (T466) the measuring and the scrolled page live in `coachScroll` (a Sheet provides the page; it cannot import this file)
import { CoachScrollContext, type CoachScroller, type Measurable, type Rect } from "./coachScroll";
export { CoachScrollContext, measure, useCoachScroller, type CoachScroller, type Measurable, type Rect } from "./coachScroll";



/**
 * The registry the tour reads: key → the host views drawn for that control. (T293) Several views may share a key (a rule
 * book link on every rule, a map under a water's map): `get` gives the first still mounted, and each view leaves alone.
 * (T456) Each view keeps the page it scrolls with (`scrollerOf`), so the walk can bring it into view.
 */
export interface CoachRegistry {
  set(key: string, node: Measurable | null, prev?: Measurable | null, scroller?: CoachScroller | null): void;
  get(key: string): Measurable | null;
  scrollerOf?(key: string): CoachScroller | null;
}

export const CoachContext = createContext<CoachRegistry | null>(null);

/**
 * (T456) The water the walkthrough is about (its item): search's row for it registers itself ("search:hit") and its open
 * arrow ("search:go"), so the steps that say "tap it in the list" ring the row they mean.
 */
export const CoachItemContext = createContext<string | null>(null);

export function useCoachRegistry(): CoachRegistry {
  return useMemo(() => {
    const nodes = new Map<string, Measurable[]>();
    const pages = new Map<Measurable, CoachScroller>();
    return {
      set: (k, n, prev, scroller) => {
        const l = (nodes.get(k) ?? []).filter((q) => q !== prev && q !== n);
        if (prev) pages.delete(prev);
        if (n) { l.push(n); if (scroller) pages.set(n, scroller); }
        if (l.length) nodes.set(k, l); else nodes.delete(k);
      },
      get: (k) => nodes.get(k)?.[0] ?? null,
      scrollerOf: (k) => { const n = nodes.get(k)?.[0]; return n ? pages.get(n) ?? null : null; },
    };
  }, []);
}

/** A ref for the view the tour may point at under `key`; undefined key or no provider: nothing registered. */
export function useCoachTarget(key: string | undefined): (node: unknown) => void {
  const reg = useContext(CoachContext);
  const page = useContext(CoachScrollContext);
  const held = useRef<Measurable | null>(null);
  return useCallback((node: unknown) => {
    if (!reg || !key) return;
    reg.set(key, node as Measurable | null, held.current, page);
    held.current = node as Measurable | null;
  }, [reg, key, page]);
}

/** Refs for several views at once (a row of tabs): `refs(key)` is the same callback for a key every render. */
export function useCoachRefs(): (key: string) => (node: unknown) => void {
  const reg = useContext(CoachContext);
  const page = useContext(CoachScrollContext);
  const cache = useRef(new Map<string, (node: unknown) => void>());
  return useCallback((key: string) => {
    let f = cache.current.get(key);
    if (!f) {
      let held: Measurable | null = null;
      f = (node: unknown) => { reg?.set(key, node as Measurable | null, held, page); held = node as Measurable | null; };
      cache.current.set(key, f);
    }
    return f;
  }, [reg, page]);
}

/** (T293) A box round a control the walkthrough points at, where the control itself takes no ref. */
export function CoachSpot({ k, children, style }: { k: string; children: ReactNode; style?: ViewStyle }) {
  const target = useCoachTarget(k);
  return <View ref={target} collapsable={false} style={style}>{children}</View>;
}

/** The union of several rects (the bell and the refresh button, side by side). */
export function unionRect(rs: readonly Rect[]): Rect | null {
  if (rs.length === 0) return null;
  const x = Math.min(...rs.map((r) => r.x)), y = Math.min(...rs.map((r) => r.y));
  const r = Math.max(...rs.map((q) => q.x + q.w)), b = Math.max(...rs.map((q) => q.y + q.h));
  return { x, y, w: r - x, h: b - y };
}

const RING = 2;

/** A spotlight inside the window, its ring included. */
export function insideWindow(h: Rect, win: { w: number; h: number }): Rect {
  const x = Math.max(RING, h.x), y = Math.max(RING, h.y);
  return { x, y, w: Math.min(win.w - RING, h.x + h.w) - x, h: Math.min(win.h - RING, h.y + h.h) - y };
}

