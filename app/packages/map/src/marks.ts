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

/**
 * (T151) A mark label's face — NORMAL weight on both maps (owner 2026-10-23: "the bold in the section map view is also
 * weird"): a mark's words name a place; they are not an emphasis competing with the water's colours. One value, read
 * by the web map (`markElement`, a CSS font) and the phone's (`Map.native.tsx`, a loaded font family). The web names
 * the SAME loaded face: the app registers Archivo per weight (`Archivo_400Regular`, expo-google-fonts) and no family
 * called "Archivo", so the old `Archivo,sans-serif` drew every label in the browser's fallback sans (T151 review).
 */
export const MARK_LABEL_FONT = { web: "400 12px/1.25 Archivo_400Regular,sans-serif", native: "Archivo_400Regular" } as const;
