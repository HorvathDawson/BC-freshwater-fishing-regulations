/**
 * (T466) THE WALKTHROUGH'S MEASURING AND SCROLLING, apart from the coach marks: a scroll view (a page, a sheet's body)
 * provides the page a ringed control scrolls with, and the walk measures a control in the window. Its own file so a
 * `Sheet` can provide it without importing `CoachMarks` (which imports the sheet's title face).
 */
import { createContext, useMemo, useRef } from "react";

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
}
export const CoachScrollContext = createContext<CoachScroller | null>(null);

/**
 * A scroll view's side of it: pass `ref` and `onScroll` (beside its own) to the ScrollView and provide `scroller` round
 * its content (`CoachScrollContext`). The offset is followed from the scroll events, so `by` needs no measuring.
 */
export function useCoachScroller(): {
  scroller: CoachScroller; ref: (node: unknown) => void; track: (y: number) => void;
} {
  const node = useRef<(Measurable & { scrollTo?: (o: { y: number; animated?: boolean }) => void }) | null>(null);
  const y = useRef(0);
  return useMemo(() => ({
    scroller: { by: (dy: number) => { y.current = Math.max(0, y.current + dy); node.current?.scrollTo?.({ y: y.current, animated: true }); },
                view: () => measure(node.current) },
    ref: (n: unknown) => { node.current = n as typeof node.current; },
    track: (at: number) => { y.current = at; },
  }), []);
}

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
