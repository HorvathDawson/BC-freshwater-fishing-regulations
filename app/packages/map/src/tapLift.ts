/**
 * (T716) A TAP'S LIFT ANSWERS ITS CLICK. On Android the map hears a tap twice: at the finger's lift (`onTapUp`, the
 * patched binding) and, once a double tap is ruled out, as a click (`onPress`, ~300 ms after the finger went down). The
 * lift has already asked what is under the finger — a spot, a boundary mark, a fish obstacle mark, the water — so the
 * click at the same point reads that answer instead of asking again (measured: the second set of queries waited on the
 * frame that lit the cue, 95-135 ms on some taps). Only the same tap: the same point (within `LIFT_SAME_DP`) and soon
 * after (`LIFT_MAX_MS`); anything else asks for itself, as before.
 */

/** What the lift found: the water under the finger (null: none), when nothing else (a spot, a mark) claimed the tap. */
export interface LiftAnswer<H> { readonly hit: H | null }

export interface Lift<H> {
  readonly x: number;
  readonly y: number;
  /** when the lift was heard (epoch ms) */
  readonly at: number;
  /** null: the lift did not decide (a spot, a mark or an obstacle under the finger, or a failed query) */
  readonly answer: Promise<LiftAnswer<H> | null>;
}

export const LIFT_SAME_DP = 2;
export const LIFT_MAX_MS = 2000;

/** The lift's answer for a click at (x, y) at `now`, or null when that click must ask for itself. */
export function liftFor<H>(l: Lift<H> | null, x: number, y: number, now: number): Promise<LiftAnswer<H> | null> | null {
  if (!l || now - l.at > LIFT_MAX_MS || now < l.at || Math.hypot(l.x - x, l.y - y) > LIFT_SAME_DP) return null;
  return l.answer;
}
