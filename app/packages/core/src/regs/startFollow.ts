/**
 * (T1049) A START LINK'S LANDING, one tick at a time (pure: `RegsScreen` runs it on a 100 ms timer, web and phone).
 *
 * A start (`?regs=…&tab=licence`) asks the page to show its tab's section at the top. The section's top is not known
 * once: on a cold start the page above it keeps growing for seconds (the 0.9.0 device pass: the salmon card, waiting on
 * its feed, landed ~200 dp above the sections after the old follow had stopped; on the web the Keep tab grew 201 px 35 ms
 * after the first layout), and the page below it too (a section near the end cannot reach the top until the page is long
 * enough). So the start FOLLOWS: it jumps again whenever the page is not where it should be (the target moved, or the
 * page could not take the jump and now can), and it is done only once the page has SETTLED: the target, the content
 * height and the scroll unchanged for START_STEADY_MS while nothing the page knows of is still coming (`loading`: the
 * salmon card, the in-season notices). A pause in the loading is not settled while something is still coming.
 *
 * Its time is the FOLLOW'S OWN CLOCK: the wall time between ticks, each gap counted at most START_GAP_MAX_MS and not at
 * all while the app is not in front (`active`). On a busy JS thread a timer's ticks run back to back (the device log: a
 * jump and "held two ticks" in the same millisecond), so a hold needs real time; and a phone's JS that slept in the
 * background does not wake up to a follow that "ran" for minutes.
 *
 * Two bounds, both from when the section was first drawn (a slow phone draws the page long before it holds still):
 *  - START_SETTLE_MAX_MS with NOTHING coming and the page still moving (from the section drawn or the last part that
 *    came, whichever is later): a defect (`cap`, reported);
 *  - while something is still coming, the follow goes on past it, up to START_COMING_MAX_MS (the feed's own give-up on a
 *    bad line): then the start is dropped where the page is (`coming-cap`), and that is the line's, not a defect.
 * The follow never fights the reader: a drag, a tab press, a sheet opened or a scroll that is not its own (`taken`,
 * `isStartOwnScroll`) ends it at once.
 */

/** the follow's timer, ms */
export const START_TICK_MS = 100;
/** ms the page must hold still, nothing coming, before the start is done */
export const START_STEADY_MS = 300;
/** ms a start is followed at most once its section is drawn while nothing is coming, then it stops and is said */
export const START_SETTLE_MAX_MS = 30_000;
/**
 * ms a start is followed at most while a part of the page is still coming: past the feed's own give-up on a bad line —
 * `@app/data` net policy: 4 tries of (20 s to a first byte + a small file at 4 kB/s, 16 s) with 1, 2 and 4 s (± 20 %)
 * between them, ~153 s; pinned against the policy by `data/src/net/startFollow.policy.test.ts`.
 */
export const START_COMING_MAX_MS = 180_000;
/** ms a start waits for its tab's section to be drawn (a slow phone's first layout), then it is said and done */
export const START_WAIT_MS = 5000;
/** the most one gap between ticks counts on the follow's clock (a JS thread that slept, the app in the background) */
export const START_GAP_MAX_MS = 1000;

export interface StartFollow {
  /** the wall time of the last tick (null: none yet) */
  readonly last: number | null;
  /** the follow's own clock, ms (see the file's note) */
  readonly clock: number;
  /** when (follow clock) the section was first drawn */
  readonly drawn: number | null;
  /** the y last asked for (null: none yet) */
  readonly at: number | null;
  /** the content height at the last tick */
  readonly ch: number | null;
  /** since when (follow clock) the page has held still (null: it moved at the last tick) */
  readonly still: number | null;
  /** since when (follow clock) the scroll has been off its landing with nothing else moving (null: on it) */
  readonly off: number | null;
  /** when (follow clock) something was last still coming (null: never, since the section was drawn) */
  readonly came: number | null;
}
export const START_FOLLOW_0: StartFollow = { last: null, clock: 0, drawn: null, at: null, ch: null, still: null, off: null, came: null };

/** What the page shows at a tick. */
export interface StartSeen {
  /** now, wall ms (any clock that only goes forward) */
  readonly now: number;
  /** the app is in front (a phone's AppState "active", a visible web tab): time away is not the follow's */
  readonly active: boolean;
  /** the y the start wants (the section's top + 2 + its offset); null: the section is not drawn */
  readonly target: number | null;
  /** the content height and the scroll view's height (0: not laid out yet: the scroll is not checked against a landing) */
  readonly contentH: number;
  readonly viewH: number;
  /** the page's scroll (its last scroll event) */
  readonly scrollY: number;
  /** something the page draws is still coming (the salmon card, the in-season notices): not settled, however still */
  readonly loading: boolean;
  /** the reader took the page (a drag, a tab press, a sheet opened, a scroll not the start's own) */
  readonly taken: boolean;
}

export type StartEnd = "held" | "taken" | "no-section" | "cap" | "coming-cap";
export type StartStep =
  | { readonly do: "wait" }
  | { readonly do: "jump"; readonly y: number }
  | { readonly do: "done"; readonly why: StartEnd };

/** Where a jump to `y` comes to rest on a page `contentH` tall in a view `viewH` tall (the scroll clamps to the end). */
export function startLanding(y: number, contentH: number, viewH: number): number {
  return Math.max(0, Math.min(y, contentH - viewH));
}

/** One tick of the follow: the next state and what to do. */
export function startTick(s0: StartFollow, seen: StartSeen): [StartFollow, StartStep] {
  const gap = s0.last == null || !seen.active ? 0 : Math.min(Math.max(0, seen.now - s0.last), START_GAP_MAX_MS);
  const t = s0.clock + gap;
  const s: StartFollow = { ...s0, last: seen.now, clock: t };
  if (seen.taken) return [s, { do: "done", why: "taken" }];
  if (!seen.active) return [s, { do: "wait" }];
  if (seen.target == null) return [s, t >= START_WAIT_MS ? { do: "done", why: "no-section" } : { do: "wait" }];
  const drawn = s.drawn ?? t;
  const came = seen.loading ? t : s.came;
  const y = seen.target;
  // past the settle bound: a defect when nothing is coming (counted from the section drawn, or from the last part that
  // came, whichever is later: a card landing at 40 s still gets its jump and its hold); while something is still coming,
  // the follow goes on to the line's own bound
  const over: StartEnd | null = seen.loading ? (t - drawn >= START_COMING_MAX_MS ? "coming-cap" : null)
    : t - Math.max(drawn, came ?? drawn) >= START_SETTLE_MAX_MS ? "cap" : null;
  // the page is not where the start wants it: the target moved, or the scroll is off its landing (the page grew since a
  // jump it could not take whole, or a layout moved it). A jump's own scroll event can come late, so a scroll still off
  // after START_STEADY_MS of nothing else moving is jumped again too. A page not laid out has no landing to be off from.
  const off = seen.contentH > 0 && seen.viewH > 0 && Math.abs(seen.scrollY - startLanding(y, seen.contentH, seen.viewH)) > 1;
  const offSince = off ? s.off ?? t : null;
  if (y !== s.at || (off && seen.contentH !== s.ch) || (offSince != null && t - offSince >= START_STEADY_MS)) {
    if (over) return [{ ...s, drawn, came }, { do: "done", why: over }];
    return [{ ...s, drawn, came, at: y, ch: seen.contentH, still: null, off: null }, { do: "jump", y }];
  }
  const isStill = seen.contentH === s.ch && !seen.loading && !off;
  const still = isStill ? s.still ?? t : null;
  const next: StartFollow = { ...s, drawn, came, at: y, ch: seen.contentH, still, off: offSince };
  if (still != null && t - still >= START_STEADY_MS) return [next, { do: "done", why: "held" }];
  if (over) return [next, { do: "done", why: over }];
  return [next, { do: "wait" }];
}

/**
 * The sentence a bounded follow is said with (the screen reports it: `cap` as a defect, `coming-cap` as the line's), whole.
 * `seconds`: on the follow's clock since the section was drawn.
 */
export function startEndSentence(tab: string, why: "cap" | "coming-cap", seconds: number): string {
  return why === "cap"
    ? `start link: tab=${tab}: the page was still moving ${seconds} s after its section was drawn, with nothing left to come: the follow stopped where the page was`
    : `start link: tab=${tab}: a part of the page was still on its way ${seconds} s after its section was drawn: the start was dropped where the page was`;
}
