/**
 * The rounded-white-pill a gauge reading sits in, drawn at runtime.
 *
 * WHY IT IS NOT A HALO. The first version widened `text-halo-width` until the label had a
 * pale outline, which looks like a pill on an empty page and like a smudge over roads,
 * landcover and contour lines — exactly where a gauge label lands. The design draws a
 * filled card with a hairline border, and there is no way to get that from text paint.
 *
 * WHY IT IS NOT A SPRITE FILE. The sprite URL belongs to the Protomaps basemap; adding one
 * image would mean forking and hosting their whole sprite sheet, which is a build artifact
 * and a deploy step to change a border colour. `map.addImage` takes raw pixels, so the pill
 * is generated from the resolved theme at style time and re-added when the theme changes.
 *
 * WHY IT IS STRETCHABLE. `stretchX`/`stretchY` mark the middle of the image as the part
 * MapLibre may repeat; with `icon-text-fit: "both"` the corners stay round at any label
 * length. Without them a long reading gives an oval and a short one a circle.
 */

/** The nine-patch geometry: a 24x24 button with an 8 px corner radius, at 2x. */
const R = 8;
const SIZE = 24;
const SCALE = 2;

export interface PillImage {
  width: number;
  height: number;
  data: Uint8Array;
  pixelRatio: number;
  stretchX: [number, number][];
  stretchY: [number, number][];
  content: [number, number, number, number];
}

/** `#rrggbb` (or `#rgb`) to bytes. Anything else is treated as opaque white. */
function rgb(hex: string): [number, number, number] {
  const h = hex.replace("#", "");
  const full = h.length === 3 ? h.split("").map((c) => c + c).join("") : h;
  if (full.length < 6) return [255, 255, 255];
  return [parseInt(full.slice(0, 2), 16), parseInt(full.slice(2, 4), 16),
          parseInt(full.slice(4, 6), 16)];
}

/**
 * The pill as RGBA pixels: a filled rounded rectangle with a one-pixel border.
 *
 * Drawn by hand rather than through a canvas because this runs where there may not be one
 * — the native renderer and the test environment both lack a DOM — and because a rounded
 * rectangle is four corner arcs and some straight edges, which is less code than the
 * canvas plumbing would be.
 */
export function pillImage(fill: string, border: string): PillImage {
  const w = SIZE * SCALE;
  const h = SIZE * SCALE;
  const r = R * SCALE;
  const [fr, fg, fb] = rgb(fill);
  const [br, bg, bb] = rgb(border);
  const data = new Uint8Array(w * h * 4);

  for (let y = 0; y < h; y++) {
    for (let x = 0; x < w; x++) {
      // Distance from the nearest edge, measured against the corner arc when inside one.
      const cx = x < r ? r : x > w - 1 - r ? w - 1 - r : x;
      const cy = y < r ? r : y > h - 1 - r ? h - 1 - r : y;
      const d = Math.hypot(x - cx, y - cy);
      const edge = Math.min(x, y, w - 1 - x, h - 1 - y);
      // Anti-aliased: coverage falls off over one pixel at the outer boundary, so the
      // corners are not stepped at the sizes these are drawn at.
      const inside = d <= r ? 1 : Math.max(0, 1 - (d - r));
      if (inside <= 0) continue;
      // The border is the outermost 1.5 device pixels, whether straight edge or arc.
      const onBorder = (d > r - 1.5 * SCALE && d > 0.1) || edge < 1.5 * SCALE;
      const i = (y * w + x) * 4;
      data[i] = onBorder ? br : fr;
      data[i + 1] = onBorder ? bg : fg;
      data[i + 2] = onBorder ? bb : fb;
      data[i + 3] = Math.round(255 * inside);
    }
  }

  // Only the middle repeats, so the corners survive `icon-text-fit`. `content` is the box
  // the text is fitted into, inset past the border.
  const stretch: [number, number][] = [[r, w - r]];
  return {
    width: w, height: h, data, pixelRatio: SCALE,
    stretchX: stretch,
    stretchY: [[r, h - r]],
    content: [3 * SCALE, 3 * SCALE, w - 3 * SCALE, h - 3 * SCALE],
  };
}
