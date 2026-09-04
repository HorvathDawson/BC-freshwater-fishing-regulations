/**
 * The bits both runtime images need — the hatch pattern and the gauge label's pill.
 *
 * Two small files that each draw RGBA pixels and hand them to `map.addImage`, and both had
 * their own copy of the same hex parser and the same device scale. Nothing had drifted yet;
 * the point is that a colour parser is exactly the kind of thing that drifts silently,
 * because a wrong one still returns a colour.
 */

/**
 * Draw at twice the nominal size and let the renderer scale down.
 *
 * `addImage` takes a `pixelRatio`, so a 2x bitmap declared as 2x occupies the same space
 * and stays sharp on a retina screen. Anything less and the pill's rounded corner and the
 * hatch's stripe edge both alias visibly.
 */
export const SCALE = 2;

/**
 * `#rgb` or `#rrggbb` to a triple, with a caller-chosen fallback.
 *
 * THE FALLBACK IS A PARAMETER because the two callers want opposite things from a bad
 * value: a pill wants white, so unreadable text is obviously wrong; a hatch wants mid-grey,
 * so a closure that lost its colour still reads as marked rather than as a hole. Hard-coding
 * either into a shared helper would have quietly changed one of them.
 */
export function rgb(hex: string,
                    fallback: [number, number, number]): [number, number, number] {
  const h = hex.replace("#", "");
  const full = h.length === 3 ? h.split("").map((c) => c + c).join("") : h;
  if (full.length < 6) return fallback;
  return [parseInt(full.slice(0, 2), 16), parseInt(full.slice(2, 4), 16),
          parseInt(full.slice(4, 6), 16)];
}
