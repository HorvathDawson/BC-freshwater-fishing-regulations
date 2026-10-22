/**
 * T40 — THE HIT AREA of everything a reader can press on the water screen: at least 44 × 44 (the platform guideline; the
 * prototype's `r20c-style-tokens` floor was 32), reached with `hitSlop` where the drawn box is smaller, so nothing moves.
 *
 * `box` is the press's KNOWN size from its own style (a `minHeight: 44` row, a 20 px glyph button, a tab's label) — a lower
 * bound, never a measurement: jsdom has no layout, and a device's real box is at least the style's. `hitProps(box)` hands a
 * Pressable both the slop and a `data-hitslop` attribute (react-native-web renders `dataSet`) holding the box and the slop,
 * which is how the a11y render test (a11y.test.tsx) proves the area without a layout. Native ignores `dataSet`.
 */
export const MIN_HIT = 44;

export interface Insets { readonly top: number; readonly bottom: number; readonly left: number; readonly right: number }
export interface HitBox { readonly w: number; readonly h: number }

/** the hitSlop that grows a w×h box to at least 44×44, split evenly (the odd pixel above / left); zero when already big enough */
export function hitSlopFor(w: number, h: number): Insets {
  const dy = Math.max(0, Math.ceil(MIN_HIT - h)), dx = Math.max(0, Math.ceil(MIN_HIT - w));
  return { top: dy - Math.floor(dy / 2), bottom: Math.floor(dy / 2), left: dx - Math.floor(dx / 2), right: Math.floor(dx / 2) };
}

/** what `data-hitslop` holds: "w h top right bottom left" — the box, then the slop (CSS order) */
export function hitData(box: HitBox): string {
  const s = hitSlopFor(box.w, box.h);
  return `${box.w} ${box.h} ${s.top} ${s.right} ${s.bottom} ${s.left}`;
}

/** read back `data-hitslop`: the area the press reaches (the test's side of `hitData`) */
export function hitArea(data: string): { w: number; h: number } | null {
  const n = data.trim().split(/\s+/).map(Number);
  if (n.length !== 6 || n.some((x) => !Number.isFinite(x))) return null;
  const [w, h, top, right, bottom, left] = n as [number, number, number, number, number, number];
  return { w: w + left + right, h: h + top + bottom };
}

/**
 * The props a Pressable spreads to reach 44 × 44: `hitSlop` (native and web) and `dataSet.hitslop` (the test's record).
 * `dataSet` is a react-native-web prop that react-native's types do not list, so the bag is typed as an open object.
 */
export function hitProps(box: HitBox): { hitSlop: Insets } & Record<string, unknown> {
  return { hitSlop: hitSlopFor(box.w, box.h), dataSet: { hitslop: hitData(box) } };
}
