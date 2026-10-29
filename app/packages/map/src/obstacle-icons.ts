/**
 * THE FISH OBSTACLE ICONS, as pixels (T173): `@app/core` `OBSTACLE_ICON_SHAPES` (one drawing per type, on a 16 grid)
 * rasterised in the theme's ink over a paper halo, for `addImage` on the web and the binding's `Images` on a phone
 * (`images.ts`). Drawn at 3x and declared 3x: a 16 px mark is exact on a 3x phone and only scaled down on 1x / 2x.
 * A signed distance per shape, one device pixel of anti-aliasing; straight alpha, like the hatches.
 */
import { OBSTACLE_ICON_GRID, OBSTACLE_ICON_HALO, OBSTACLE_ICON_SHAPES, type IconShape, type ObstacleIconKind, type Pt } from "@app/core";
import { rgb } from "./raster";
import type { MarkImage } from "./here-mark";

export const OBSTACLE_ICON_SCALE = 3;
export const OBSTACLE_IMAGE_PREFIX = "obstacle-";
export const obstacleImageId = (kind: ObstacleIconKind): string => `${OBSTACLE_IMAGE_PREFIX}${kind}`;

const segment = (px: number, py: number, [ax, ay]: Pt, [bx, by]: Pt): number => {
  const dx = bx - ax, dy = by - ay, l2 = dx * dx + dy * dy;
  const t = l2 === 0 ? 0 : Math.max(0, Math.min(1, ((px - ax) * dx + (py - ay) * dy) / l2));
  return Math.hypot(px - (ax + t * dx), py - (ay + t * dy));
};

/** Signed distance (grid units, < 0 inside) from a point to one shape. */
function distance(s: IconShape, x: number, y: number): number {
  if ("stroke" in s) {
    let d = Infinity;
    for (let i = 1; i < s.stroke.length; i++) d = Math.min(d, segment(x, y, s.stroke[i - 1]!, s.stroke[i]!));
    return d - s.w / 2;
  }
  if ("fill" in s) {
    let d = Infinity, inside = false;
    for (let i = 0, j = s.fill.length - 1; i < s.fill.length; j = i++) {
      const [xi, yi] = s.fill[i]!, [xj, yj] = s.fill[j]!;
      d = Math.min(d, segment(x, y, s.fill[j]!, s.fill[i]!));
      if ((yi > y) !== (yj > y) && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) inside = !inside;
    }
    return inside ? -d : d;
  }
  if ("ring" in s) {
    const [cx, cy, rx, ry] = s.ring;
    return Math.abs((Math.hypot((x - cx) / rx, (y - cy) / ry) - 1) * Math.min(rx, ry)) - s.w / 2;
  }
  return Math.hypot(x - s.disc[0], y - s.disc[1]) - s.disc[2];
}

/** One icon: `ink` over a `halo` of `OBSTACLE_ICON_HALO` grid units. */
export function obstacleIconImage(kind: ObstacleIconKind, ink: string, halo: string): MarkImage {
  const px = OBSTACLE_ICON_GRID * OBSTACLE_ICON_SCALE, S = OBSTACLE_ICON_SCALE;
  const [ir, ig, ib] = rgb(ink, [0, 0, 0]);
  const [hr, hg, hb] = rgb(halo, [255, 255, 255]);
  const data = new Uint8Array(px * px * 4);
  const cover = (d: number) => Math.max(0, Math.min(1, 0.5 - d * S));
  for (let j = 0; j < px; j++)
    for (let i = 0; i < px; i++) {
      const x = (i + 0.5) / S, y = (j + 0.5) / S;
      let d = Infinity;
      for (const s of OBSTACLE_ICON_SHAPES[kind]) d = Math.min(d, distance(s, x, y));
      const a = cover(d), h = cover(d - OBSTACLE_ICON_HALO);
      const alpha = a + h * (1 - a);
      if (alpha <= 0) continue;
      const k = (j * px + i) * 4;
      data[k] = Math.round((ir * a + hr * h * (1 - a)) / alpha);
      data[k + 1] = Math.round((ig * a + hg * h * (1 - a)) / alpha);
      data[k + 2] = Math.round((ib * a + hb * h * (1 - a)) / alpha);
      data[k + 3] = Math.round(255 * alpha);
    }
  return { width: px, height: px, data, pixelRatio: S };
}
