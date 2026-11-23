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

/** How a label hangs from its point: the map's `text-anchor` — `bottom*` above its ring, `top*` below it (T997). */
export type MarkLabelAnchor = "bottom" | "bottom-left" | "bottom-right" | "top" | "top-left" | "top-right";

/**
 * The anchor for a label whose ring is at `x` on a map `width` px wide: centred unless a half-width would hang over.
 * A ring OFF the map hangs its label outward, off the map with it (T929 review: hung inward, up to 180 px of a label
 * stood on the map with no ring under it). `below` (T997, `hangBelow`): the label hangs under its ring, not over it.
 */
export function labelAnchor(x: number, width: number, half: number = MARK_LABEL_HALF, below = false): MarkLabelAnchor {
  const v = below ? "top" : "bottom";
  if (x < 0) return `${v}-right`;
  if (x > width) return `${v}-left`;
  if (x < half) return `${v}-left`;
  if (x > width - half) return `${v}-right`;
  return v;
}

/**
 * (T997 round 2, T465) THE TWO END LABELS NEVER COVER EACH OTHER, AT ANY ZOOM. A water's map labels only the lit part's
 * two ends (`@app/ui` waterMap); zoomed out (or a short part framed at the camera's closest) the two rings meet and two
 * boxes hung over them drew on top of each other (the Nicomekl's "Dyke gates" over "23 m below the Nicomekl floodgates
 * dam"). So the LOWEST labelled ring on the screen hangs its label BELOW it and every other hangs above: with two, one box
 * lies wholly above the higher ring and the other wholly below the lower one, so they can never meet — and neither is
 * ever collided away (the layer allows overlap: a label that vanished left nothing naming its end, #51 review). `ys`:
 * each labelled mark's screen y (down is +), null for a mark with no label or not measured; a tie goes to the later one.
 * True at the index that hangs below; one label (or none) hangs above, as it always did.
 */
export function hangBelow(ys: readonly (number | null)[]): boolean[] {
  let low = -1;
  ys.forEach((y, i) => { if (y !== null && (low < 0 || y >= ys[low]!)) low = i; });
  const labelled = ys.filter((y) => y !== null).length;
  return ys.map((_, i) => labelled > 1 && i === low);
}

/**
 * (T151) A mark label's face — NORMAL weight on both maps (owner 2026-10-23: "the bold in the section map view is also
 * weird"): a mark's words name a place; they are not an emphasis competing with the water's colours. One value, read
 * by the web map (`markElement`, a CSS font) and the phone's (`Map.native.tsx`, a loaded font family). The web names
 * the SAME loaded face: the app registers Archivo per weight (`Archivo_400Regular`, expo-google-fonts) and no family
 * called "Archivo", so the old `Archivo,sans-serif` drew every label in the browser's fallback sans (T151 review).
 */
export const MARK_LABEL_FONT = { web: "400 12px/1.25 Archivo_400Regular,sans-serif", native: "Archivo_400Regular" } as const;
