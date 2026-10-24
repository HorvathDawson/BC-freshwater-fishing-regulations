/**
 * (T60) WORDS OF THE SPOT SCREEN that were typed in the component. Started with the one FIGURE it typed — the weather
 * window's column heads, an hour either side of the visit — so that no component types a number of its own
 * (`tools/data-vs-ui.test.ts`). The rest of SpotScreen's and SpotCapture's copy (its labels, questions and notes, listed
 * there as `KNOWN_COPY`) belongs here too and moves under ticket T60-copy.
 */
export const SPOT_WINDOW = {
  /** The column heads: the hour before, the visit, the hour after. */
  before: "−1 h", at: "visit", after: "+1 h",
} as const;
