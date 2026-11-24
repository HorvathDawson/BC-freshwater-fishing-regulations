/**
 * (T1049) A START LINK'S LANDING, one tick at a time (pure: `RegsScreen` runs it on a 100 ms timer, web and phone).
 *
 * A start (`?regs=…&tab=licence`) asks the page to show its tab's section at the top. The section's top is not known
 * once: on a cold start the page above it keeps growing for seconds (the 0.9.0 device pass: the salmon card, waiting on
 * its feed, landed ~200 dp above the sections after the old follow had stopped; on the web the Keep tab grew 201 px 35 ms
 * after the first layout), and the page below it too (a section near the end cannot reach the top until the page is long
 * enough). So the start FOLLOWS: it jumps again whenever the page is not where it should be (the target moved, or the
 * page could not take the jump and now can), and it is done only once the page has SETTLED: the target, the content
 * height and the scroll unchanged for START_STEADY_MS while nothing the page knows of is still coming (`loading`). A pause
 * in the loading is not settled while something is still coming.
 *
 * All of it is WALL TIME (`now`), never a count of ticks: on a busy JS thread a timer's ticks run back to back (the
 * device log: a jump and "held two ticks" in the same millisecond, before the jump's own scroll event).
 *
 * The follow never fights the reader: a drag, a tab press or a scroll that is not its own (`taken`, `isStartOwnScroll`)
 * ends it at once. A page that never settles is followed START_SETTLE_MAX_MS from when its section was first drawn (not
 * from the start: a slow phone draws the page long before it holds still), then the follow stops (`cap`: the caller says so).
 */

/** the follow's timer, ms */
export const START_TICK_MS = 100;
/** ms the page must hold still, nothing coming, before the start is done */
export const START_STEADY_MS = 300;
/** ms a start is followed at most once its section is drawn, then it stops and is said */
export const START_SETTLE_MAX_MS = 30_000;
/** ms a start waits for its tab's section to be drawn (a slow phone's first layout), then it is said and done */
export const START_WAIT_MS = 5000;

export interface StartFollow {
  /** when the start began (its first tick) */
  readonly begun: number | null;
  /** when the section was first drawn */
  readonly drawn: number | null;
  /** the y last asked for (null: none yet) */
  readonly at: number | null;
  /** the content height at the last tick */
  readonly ch: number | null;
  /** since when the page has held still (null: it moved at the last tick) */
  readonly still: number | null;
  /** since when the scroll has been off its landing with nothing else moving (null: on it) */
  readonly off: number | null;
}
export const START_FOLLOW_0: StartFollow = { begun: null, drawn: null, at: null, ch: null, still: null, off: null };

/** What the page shows at a tick. */
export interface StartSeen {
  /** now, ms (any clock that only goes forward) */
  readonly now: number;
  /** the y the start wants (the section's top + 2 + its offset); null: the section is not drawn */
  readonly target: number | null;
  /** the content height and the scroll view's height (0: not laid out yet: the scroll is not checked against a landing) */
  readonly contentH: number;
  readonly viewH: number;
  /** the page's scroll (its last scroll event) */
  readonly scrollY: number;
  /** something the page draws is still coming (the salmon card): not settled, however still */
  readonly loading: boolean;
  /** the reader took the page (a drag, a tab press, a scroll not the start's own) */
  readonly taken: boolean;
}

export type StartStep =
  | { readonly do: "wait" }
  | { readonly do: "jump"; readonly y: number }
  | { readonly do: "done"; readonly why: "held" | "taken" | "no-section" | "cap" };

/** Where a jump to `y` comes to rest on a page `contentH` tall in a view `viewH` tall (the scroll clamps to the end). */
export function startLanding(y: number, contentH: number, viewH: number): number {
  return Math.max(0, Math.min(y, contentH - viewH));
}

/** One tick of the follow: the next state and what to do. */
export function startTick(s0: StartFollow, seen: StartSeen): [StartFollow, StartStep] {
  const now = seen.now;
  const s: StartFollow = s0.begun == null ? { ...s0, begun: now } : s0;
  if (seen.taken) return [s, { do: "done", why: "taken" }];
  if (seen.target == null) return [s, now - s.begun! >= START_WAIT_MS ? { do: "done", why: "no-section" } : { do: "wait" }];
  const drawn = s.drawn ?? now;
  const y = seen.target;
  // the page is not where the start wants it: the target moved, or the scroll is off its landing (the page grew since a
  // jump it could not take whole, or a layout moved it). A jump's own scroll event can come late, so a scroll still off
  // after START_STEADY_MS of nothing else moving is jumped again too. A page not laid out has no landing to be off from.
  const off = seen.contentH > 0 && seen.viewH > 0 && Math.abs(seen.scrollY - startLanding(y, seen.contentH, seen.viewH)) > 1;
  const offSince = off ? s.off ?? now : null;
  const over = now - drawn >= START_SETTLE_MAX_MS;
  if (y !== s.at || (off && seen.contentH !== s.ch) || (offSince != null && now - offSince >= START_STEADY_MS)) {
    if (over) return [{ ...s, drawn }, { do: "done", why: "cap" }];
    return [{ ...s, drawn, at: y, ch: seen.contentH, still: null, off: null }, { do: "jump", y }];
  }
  const isStill = seen.contentH === s.ch && !seen.loading && !off;
  const still = isStill ? s.still ?? now : null;
  const next: StartFollow = { ...s, drawn, at: y, ch: seen.contentH, still, off: offSince };
  if (still != null && now - still >= START_STEADY_MS) return [next, { do: "done", why: "held" }];
  if (over) return [next, { do: "done", why: "cap" }];
  return [next, { do: "wait" }];
}
