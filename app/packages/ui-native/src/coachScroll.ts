/**
 * (T466) THE WALKTHROUGH'S MEASURING AND SCROLLING, apart from the coach marks: a scroll view (a page, a sheet's body)
 * provides the page a ringed control scrolls with, and the walk measures a control in the window. Its own file so a
 * `Sheet` can provide it without importing `CoachMarks` (which imports the sheet's title face).
 */
import { createContext, useMemo, useRef } from "react";
import { Animated } from "react-native";
import { now, reportFailure } from "@app/core";
import { NATIVE_DRIVER } from "./motion";
import { windowRect } from "./windowRect";

export interface Rect { x: number; y: number; w: number; h: number }

export interface Measurable { measureInWindow?: (cb: (x: number, y: number, w: number, h: number) => void) => void }

/**
 * (T456) THE PAGE A TARGET SCROLLS WITH: a scroll view the walkthrough may move, so the control a step is about comes into
 * view on a phone as on the web (react-native has no scrollIntoView). `by(dy)` scrolls the page dy further down.
 */
export interface CoachScroller {
  by(dy: number): void;
  /** the page's own box in the window (null: not laid out) */
  view(): Promise<Rect | null>;
  /**
   * (T824) Told on each of the page's scroll events (the scroll view's own, ~60 a second while it moves); the answer stops
   * telling. (T842) The walk no longer measures on each one (a measure is a round trip: the highlight trailed the row); it
   * waits for the page to settle and checks the highlight is where `y` moved it.
   */
  onScroll?(f: () => void): () => void;
  /**
   * (T842, owner on 0.7.12: "the box has a weird delay tracking the box") THE PAGE'S SCROLL OFFSET AS AN ANIMATED VALUE,
   * driven on the UI thread: on a phone the scroll view's own native scroll event drives it (`Animated.attachNativeEvent`,
   * no JS on the way), on the web the scroll handler sets it. The highlight's transform is `offset when measured - y`, so
   * it moves in the same frame as the row under it.
   */
  readonly y?: Animated.Value;
  /** (T842) the offset the scroll handler last heard, and when (ms): a page still moving is not measured */
  offset?(): number;
  scrolledAt?(): number;
}
export const CoachScrollContext = createContext<CoachScroller | null>(null);

/**
 * A scroll view's side of it: pass `ref` and `onScroll` (beside its own) to the ScrollView and provide `scroller` round
 * its content (`CoachScrollContext`). The offset is followed from the scroll events, so `by` needs no measuring.
 *
 * (T842) `ref` also ties the page's native scroll event to `y` (a phone), so a highlight moves with the page on the UI
 * thread. A FlatList's ref is not the scroll view: its native scroll view (`getNativeScrollRef`) is what is tied and
 * measured, and it scrolls by `scrollToOffset`.
 */
export function useCoachScroller(): {
  scroller: CoachScroller; ref: (node: unknown) => void; track: (y: number) => void;
} {
  const node = useRef<ScrollHost | null>(null);
  const y = useRef(0), at = useRef(0);
  return useMemo(() => {
    const told = new Set<() => void>();
    const value = new Animated.Value(0);
    let tied: { detach(): void } | null = null;
    return {
      scroller: { by: (dy: number) => {
                    y.current = Math.max(0, y.current + dy);
                    const n = node.current;
                    if (n?.scrollToOffset) n.scrollToOffset({ offset: y.current, animated: true });
                    else n?.scrollTo?.({ y: y.current, animated: true });
                  },
                  view: () => measure(hostOf(node.current)),
                  onScroll: (f: () => void) => { told.add(f); return () => { told.delete(f); }; },
                  y: value, offset: () => y.current, scrolledAt: () => at.current },
      ref: (n: unknown) => {
        if (n === node.current) return;
        node.current = n as ScrollHost | null;
        tied?.detach(); tied = null;
        const host = hostOf(node.current);
        if (!NATIVE_DRIVER || !host) return;
        // the scroll view's own native event drives the value: the UI thread moves the highlight, no JS per frame
        try { tied = Animated.attachNativeEvent(host, "onScroll", [{ nativeEvent: { contentOffset: { y: value } } }], null); }
        catch (e) { reportFailure(e); }                 // (no silent fallbacks) the highlight then hides while it scrolls
      },
      track: (to: number) => {
        y.current = to; at.current = now();
        if (!NATIVE_DRIVER) value.setValue(to);
        for (const f of told) f();
      },
    };
  }, []);
}

/** What a page's ref may be: a ScrollView (scrollTo) or a FlatList (scrollToOffset, its scroll view inside). */
type ScrollHost = Measurable & {
  scrollTo?: (o: { y: number; animated?: boolean }) => void;
  scrollToOffset?: (o: { offset: number; animated?: boolean }) => void;
  getNativeScrollRef?: () => unknown;
};
/** The scroll view itself: a FlatList's inner one, else the node. */
const hostOf = (n: ScrollHost | null): Measurable | null =>
  (n?.getNativeScrollRef?.() as Measurable | null | undefined) ?? n;

/** (T674) The room kept between a revealed block and its page's edge, px. */
const REVEAL_MARGIN = 12;

/**
 * (T674) HOW FAR A PAGE SCROLLS (down: positive) so `r` sits wholly inside the page's `view` with `REVEAL_MARGIN` to
 * spare; 0 when it already does. A block taller than the view shows its top. Both boxes are in the window.
 */
function revealDy(r: Rect, view: Rect): number {
  const top = view.y + REVEAL_MARGIN, bottom = view.y + view.h - REVEAL_MARGIN;
  if (r.y < top || r.h > bottom - top) return r.y - top;
  if (r.y + r.h > bottom) return r.y + r.h - bottom;
  return 0;
}

/**
 * (T674) BRING A BLOCK INTO ITS PAGE'S VIEW (react-native has no scrollIntoView): measured in the window against the
 * page, the page scrolled by what is missing. Answers what happened; "unmeasured" is the block or the page not laid out.
 */
export async function bringIntoView(node: Measurable | null, page: CoachScroller): Promise<"moved" | "in-view" | "unmeasured"> {
  const [r, v] = await Promise.all([measure(node), page.view()]);
  if (!r || !v) return "unmeasured";
  const dy = revealDy(r, v);
  if (Math.abs(dy) < 1) return "in-view";
  page.by(dy);
  return "moved";
}

export function measure(node: Measurable | null): Promise<Rect | null> {
  // (T824) the browser's box at once (react-native-web's measureInWindow waits a task); a phone asks the native side
  const now = windowRect(node);
  if (now !== undefined) return Promise.resolve(now);
  return new Promise((res) => {
    if (!node?.measureInWindow) { res(null); return; }
    let done = false;
    const t = setTimeout(() => { if (!done) { done = true; res(null); } }, 500);
    try {
      node.measureInWindow((x, y, w, h) => {
        if (done) return;
        done = true; clearTimeout(t);
        res(w > 0 && h > 0 && Number.isFinite(x) && Number.isFinite(y) ? { x, y, w, h } : null);
      });
    } catch { done = true; clearTimeout(t); res(null); }
  });
}
