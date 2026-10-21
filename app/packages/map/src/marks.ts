/**
 * A boundary mark's label (P15, `MapProps.marks`), kept inside the map: the prototype's `--dx` (`regs-v11.html`
 * `placeMarks`). The label is centred over its ring and at most 180 px wide, so it is moved sideways by however much
 * of a 100 px half-width would hang over either edge. Platform-free, so the rule is a test (`marks.test.ts`).
 */
export const MARK_LABEL_HALF = 100;

/** The sideways shift (px) for a label whose ring is at `x` on a map `width` px wide. */
export function labelShift(x: number, width: number, half: number = MARK_LABEL_HALF): number {
  return Math.min(0, width - half - x) + Math.max(0, half - x);
}
