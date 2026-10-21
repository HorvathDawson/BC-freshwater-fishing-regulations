/**
 * A WATER'S VIEWS (P13 route, owner 2026-10-21): one water latched, three ways to look at it, in ONE order that the
 * header's view strip draws left to right and the sideways swipe walks — so a finger moving left always brings in the
 * view written to the right of the one on screen.
 *
 *   Regulations · Conditions · Map
 *
 * The order is the owner's ("a sideways swipe moves between a water's views: Regs <-> Conditions <-> Map",
 * G1-gestures.md 2026-10-21). APP-PREP §0 drew the header's two actions as "[Map] [Conditions here]"; with the views
 * now one strip the swipe's order wins, and the strip is drawn in it, so the two can never disagree.
 *
 * Words for them live here (no hand-written labels in a component, AGENTS rule 23).
 */
import type { SwipeStep } from "./gestures";

export const WATER_VIEWS = ["regs", "conditions", "map"] as const;
export type WaterView = (typeof WATER_VIEWS)[number];

/** Each view's name, as the strip prints it and a screen reader says it. */
export const WATER_VIEW_LABEL: { readonly [V in WaterView]: string } = {
  regs: "Regulations",
  conditions: "Conditions",
  map: "Map",
};

/** The strip's own name for a screen reader. */
export const WATER_VIEWS_LABEL = "Views of this water";

/** The one labelled way from a Conditions reach to its water's rules (and its screen-reader hint). */
export const TO_REGULATIONS = { label: "Regulations", hint: "The fishing regulations for this water" } as const;

/**
 * The view a swipe lands on, or null at either end (no wrap). "next" is the view to the right in `WATER_VIEWS`.
 * `within` narrows the views on offer (a Conditions reach opened from the Conditions tab offers Regulations and
 * Conditions only): the step is taken in that list, still in the one order.
 */
export function stepView(at: WaterView, dir: SwipeStep,
                         within: readonly WaterView[] = WATER_VIEWS): WaterView | null {
  const views = WATER_VIEWS.filter((v) => within.includes(v));
  const i = views.indexOf(at);
  if (i < 0) return null;
  const to = dir === "next" ? i + 1 : i - 1;
  return to >= 0 && to < views.length ? views[to]! : null;
}
