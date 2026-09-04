/**
 * The diagonal hatch a wetland is filled with, drawn at runtime.
 *
 * WHY A HATCH AT ALL. A wetland is the largest layer by area in the province and it is
 * CONTEXT, not an answer — it says "this is marsh", never "you may fish here". A flat
 * green wash at any opacity that makes it visible also makes it read like the lakes it
 * surrounds. Texture is the difference: v1 hatched it
 * (`archive/webapp/src/components/Map.tsx`, `createWetlandPattern`) and a hatched polygon
 * is legible at 30% where a flat one needs 60% and buries every river inside it.
 *
 * WHY IT IS NOT A SPRITE, and why not canvas. Same two reasons as `pill.ts`: the sprite
 * URL belongs to the Protomaps basemap, so shipping one image would mean forking their
 * sheet; and `document.createElement("canvas")` is a browser API this package may not
 * reach for, because the native adapter renders the same style. Raw RGBA is the one
 * currency `addImage` takes on both.
 *
 * The tile is 16 px at 2x and repeats seamlessly: a stripe leaving the right edge at 45
 * degrees re-enters at the left one row down, which is true for any spacing that divides
 * the tile size. `SPACING` does — change it to another divisor of 16 or the seam shows.
 *
 * TWO WEAVES OUT OF ONE FUNCTION, chosen by the alphas the caller passes.
 *
 *   a WASH   ground 0.45, stripe 0.70, darken 0.35 — a tinted area with texture in it.
 *            Wetland: "this is marsh", context you read past.
 *   a WARNING ground 0.00, stripe 0.55, darken 0.00, and WIDE — bold diagonal stripes on
 *            nothing, in the colour given. v1 drew national parks and closed land this way,
 *            and the reason is not decoration: on a paper chart, hatching means KEEP OUT. A
 *            solid tint of the same red would read as "an area", and the whole point is
 *            that it is an area you may not fish in.
 *
 * `spacing` and `weight` are what separate the two. A 1.2 px line every 4 px is a texture
 * you read past — right for marsh, and at a glance on a closure it looked like paper grain.
 * A 3.5 px stripe every 8 px is a MARK: you see it before you have decided to look at it,
 * which is the whole job of a closure on a map.
 */

/** Tile edge in CSS pixels. `spacing` must divide it, or the diagonal seams. */
const SIZE = 16;
const SCALE = 2;

export interface HatchImage {
  width: number;
  height: number;
  data: Uint8Array;
  pixelRatio: number;
}

/** `#rrggbb` (or `#rgb`) to bytes. Anything else is treated as mid grey. */
function rgb(hex: string): [number, number, number] {
  const h = hex.replace("#", "");
  const full = h.length === 3 ? h.split("").map((c) => c + c).join("") : h;
  if (full.length < 6) return [128, 128, 128];
  return [parseInt(full.slice(0, 2), 16), parseInt(full.slice(2, 4), 16),
          parseInt(full.slice(4, 6), 16)];
}

/** Toward black by `k`, so the stripe is the same hue as the ground it sits on. */
function darken([r, g, b]: [number, number, number], k: number): [number, number, number] {
  return [Math.round(r * (1 - k)), Math.round(g * (1 - k)), Math.round(b * (1 - k))];
}

/**
 * A repeating 45-degree hatch in `colour`: a translucent ground with darker stripes.
 *
 * Antialiased along the stripe by measuring the distance from each pixel centre to the
 * nearest stripe line rather than by drawing runs. A hard-edged 45-degree line on a 32 px
 * tile is a staircase, and MapLibre tiles the image at whatever the device ratio is, so
 * the staircase is what you would actually see.
 */
export function hatchImage(colour: string, groundAlpha = 0.45, stripeAlpha = 0.7,
                           darkenBy = 0.35, spacing = 4, weight = 1.2,
                           cross = false): HatchImage {
  const px = SIZE * SCALE;
  // A pitch that does not divide the tile puts a seam down every repeat, and the repeat is
  // every 16 px — so the seam becomes a grid over the whole polygon.
  const pitch = Math.max(1, Math.round(SIZE / Math.max(1, Math.round(SIZE / spacing)))) * SCALE;
  const half = (weight / 2) * SCALE;           // half the stripe width, in device px
  const ground = rgb(colour);
  const stripe = darken(ground, darkenBy);
  const data = new Uint8Array(px * px * 4);

  for (let y = 0; y < px; y++) {
    for (let x = 0; x < px; x++) {
      // Stripes run down-right, so `x - y` is constant along one. The distance to the
      // nearest stripe centre is that residue folded into [0, pitch/2] and scaled by
      // sqrt(2)/2 — the perpendicular distance from an axis-aligned one.
      const band = (u: number) => {
        let d = u % pitch;
        if (d < 0) d += pitch;
        if (d > pitch / 2) d = pitch - d;
        // Antialias over one device pixel at the stripe's edge, whatever its width.
        return Math.max(0, Math.min(1, (half - d * Math.SQRT1_2) + 0.5));
      };
      // BOTH DIAGONALS, for the doubly-restricted areas. v1 drew ecological reserves and
      // closed land as a cross and national parks as a single diagonal, and the reason is
      // in its own comment: the PATTERN carries the severity as well as the colour, so a
      // reader who cannot separate crimson from amber still sees two different marks.
      const on = cross ? Math.max(band(x - y), band(x + y)) : band(x - y);

      const i = (y * px + x) * 4;
      const a = groundAlpha + (stripeAlpha - groundAlpha) * on;
      for (let c = 0; c < 3; c++) {
        // STRAIGHT ALPHA, NOT PREMULTIPLIED. `addImage` takes an ImageData-shaped buffer and
        // ImageData is non-premultiplied — `pill.ts` writes full-strength colour beside its
        // own alpha and renders correctly, which is the proof. Premultiplying here scaled
        // every channel by 0.55 before MapLibre scaled it again, so the closure hatch drew
        // as a muddy grey instead of crimson: the colour was being dimmed twice.
        const g = ground[c] ?? 0;
        data[i + c] = Math.round(g + ((stripe[c] ?? 0) - g) * on);
      }
      data[i + 3] = Math.round(a * 255);
    }
  }
  return { width: px, height: px, data, pixelRatio: SCALE };
}
