/**
 * The day's context every box above the card reads (T07): page v36's `settle` over a tap's `RegsInputs`.
 *
 * Owner rule 2026-10-21 (decisions.md): every answer here is the PIPELINE's — the ladder verdicts (ladder_frame /
 * ladder_verdict), the display frame's status and its decided `closing`. This file only decodes, selects (by segment,
 * fish, origin) and groups them for display; it never decides which rule governs, what is lifted or what closes.
 *
 * Ports (page_v36.js @ 7fb6809f, inventory names): settle (the `broad` ordering, as `broadOf`).
 */
import type { DisplayFrame, RuleIndex, RuleView } from "./inputs";

/**
 * page v36 `settle` broad: the frame's DECIDED closures (`closing [[rix, fish[]]]`, the pipeline's — tidal frames have
 * none), drawn the one naming the most fish first, then by the part's rank. Ordering only: the set is the frame's.
 * Array.prototype.sort is stable, as V8's is in the page.
 */
export function broadOf(frame: DisplayFrame | null, R: RuleIndex): readonly RuleView[] {
  const closing = frame && frame.status !== "tidal" ? frame.closing : [];
  return closing.map(([ix, fs]) => [R.get(ix), fs.length] as const)
    .sort((a, b) => b[1] - a[1] || a[0].rank - b[0].rank)
    .map(([r]) => r);
}
