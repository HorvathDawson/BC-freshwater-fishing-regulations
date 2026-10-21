/**
 * "YOU ARE HERE", as pixels — a map pin, so it cannot be mistaken for a gauge.
 *
 * OWNER 2026-10-21: "the icon for where you are should be different than gauges". Both were
 * MapLibre's default teardrop marker in two colours, so the only thing telling the reader
 * which end of the route was theirs was a hue — and the donor tones are hues too. Now the
 * SHAPE carries it: every gauge is a round dot, and the spot is a pin (a round head over a
 * point, with a hollow centre) standing on the place, in the accent — the app's colour for
 * "you chose this" (`color.ui.accent`), which the donor tones are all chosen far from.
 *
 * Generated rather than shipped, on the same rails as the gauge pill (`pill.ts`): a style
 * cannot declare a generated image, and forking the basemap sprite for one glyph would tie
 * our mark to Protomaps' sprite releases. Repainted on a theme change like the pill.
 */
import { rgb, SCALE } from "./raster";

export interface MarkImage { width: number; height: number; data: Uint8Array; pixelRatio: number }

/** The pin's size in CSS pixels: a 22 px head over an 8 px point. */
export const HERE_W = 22;
export const HERE_H = 30;

/**
 * The pin: `fill` is the accent, `edge` is the paper it stands out from (outline and the
 * hollow centre). Anti-aliased over one device pixel.
 */
export function hereImage(fill: string, edge: string): MarkImage {
  const w = HERE_W * SCALE, h = HERE_H * SCALE;
  const [fr, fg, fb] = rgb(fill, [0, 0, 0]);
  const [er, eg, eb] = rgb(edge, [255, 255, 255]);
  const data = new Uint8Array(w * h * 4);
  const r = (w / 2) - 1;                 // head radius, one device pixel inside the edge
  const cx = w / 2, cy = r + 1;
  const tip = h - 1;
  const rim = 2 * SCALE;                 // the paper outline
  const hole = r * 0.36;                 // the hollow centre

  /** Signed distance to the pin's silhouette: <0 inside. A circle unioned with a cone. */
  const sd = (x: number, y: number): number => {
    const head = Math.hypot(x - cx, y - cy) - r;
    // The cone: from the tangent points of the head down to the tip.
    const t = (y - cy) / (tip - cy);
    const half = t >= 0 && t <= 1 ? r * 0.82 * (1 - t) : -Infinity;
    const cone = t >= 0 && t <= 1 ? Math.abs(x - cx) - half : Infinity;
    return Math.min(head, cone);
  };

  for (let y = 0; y < h; y++)
    for (let x = 0; x < w; x++) {
      const d = sd(x + 0.5, y + 0.5);
      const cover = Math.max(0, Math.min(1, 0.5 - d));
      if (cover <= 0) continue;
      const centre = Math.hypot(x + 0.5 - cx, y + 0.5 - cy);
      const paper = d > -rim || centre < hole;
      const i = (y * w + x) * 4;
      data[i] = paper ? er : fr;
      data[i + 1] = paper ? eg : fg;
      data[i + 2] = paper ? eb : fb;
      data[i + 3] = Math.round(255 * cover);
    }
  return { width: w, height: h, data, pixelRatio: SCALE };
}
