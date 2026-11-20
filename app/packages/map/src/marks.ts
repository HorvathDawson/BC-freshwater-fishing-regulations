/**
 * A boundary mark's label (P15, `MapProps.marks`), kept inside the map. The prototype's `--dx` (`regs-v11.html`
 * `placeMarks`) moved the label's POINT sideways by what would hang over an edge; as a map point that was wrong (T929,
 * a tester on 0.8.3: "the labels for the sections just move around with you as you pan"): a mark off screen had its
 * label pulled up to 100 px inside the edge at every settle, so the label followed the camera, and a shift measured in
 * pixels at one zoom was another distance at the next. Now the label's point IS the mark's (the cut point it names) and
 * only its ANCHOR changes: centred over the ring where there is room, hanging right of it near the left edge and left of
 * it near the right, so the box's corner always sits over its own ring and no leader is needed. Platform-free, so the
 * rule is a test (`marks.test.ts`).
 */
export const MARK_LABEL_HALF = 100;

/** How a label hangs from its point: the map's `text-anchor`. */
export type MarkLabelAnchor = "bottom" | "bottom-left" | "bottom-right";

/** The anchor for a label whose ring is at `x` on a map `width` px wide: centred unless a half-width would hang over. */
export function labelAnchor(x: number, width: number, half: number = MARK_LABEL_HALF): MarkLabelAnchor {
  if (x < half) return "bottom-left";
  if (x > width - half) return "bottom-right";
  return "bottom";
}

/**
 * (T151) A mark label's face — NORMAL weight on both maps (owner 2026-10-23: "the bold in the section map view is also
 * weird"): a mark's words name a place; they are not an emphasis competing with the water's colours. One value, read
 * by the web map (`markElement`, a CSS font) and the phone's (`Map.native.tsx`, a loaded font family). The web names
 * the SAME loaded face: the app registers Archivo per weight (`Archivo_400Regular`, expo-google-fonts) and no family
 * called "Archivo", so the old `Archivo,sans-serif` drew every label in the browser's fallback sans (T151 review).
 */
export const MARK_LABEL_FONT = { web: "400 12px/1.25 Archivo_400Regular,sans-serif", native: "Archivo_400Regular" } as const;
