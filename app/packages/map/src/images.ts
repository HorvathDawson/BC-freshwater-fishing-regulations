/**
 * THE MAP'S GENERATED IMAGES — the gauge label's pill, the five hatches and the route overlay's "you are here" pin — in
 * one theme's colours. ONE list for both renderers (T74): the web adds them with `map.addImage` (`web/images.ts`), the
 * phone hands them to the binding's `Images` as PNG `data:` URIs (`nativeImages`), so the two draw the same marks.
 */
import { colour } from "./chrome";
import { hatchImage } from "./hatch";
import { hereImage } from "./here-mark";
import { pillImage } from "./pill";
import { pngDataUri } from "./png";
import { STYLE_META } from "./style";
import { HERE_IMAGE } from "./trace-overlay";

export const PILL_IMAGE = "gauge-pill";

/** One generated image: RGBA pixels at `pixelRatio`, and a stretchable image's zones (the pill). */
export interface GeneratedImage {
  id: string;
  width: number;
  height: number;
  data: Uint8Array;
  pixelRatio: number;
  stretchX?: [number, number][];
  stretchY?: [number, number][];
  content?: [number, number, number, number];
}

/** Is `id` one of the images `generatedImages` makes (what `styleimagemissing` re-adds)? */
export const isGeneratedImage = (id: string): boolean =>
  id === PILL_IMAGE || id === HERE_IMAGE || id.endsWith("-hatch");

export function generatedImages(theme: string): GeneratedImage[] {
  // The pill's border read `color.line`, which was never a token, so every theme drew
  // the light theme's hairline from a literal fallback. It is the app's hairline now.
  const out: GeneratedImage[] = [{ id: PILL_IMAGE, ...pillImage(colour(theme, "color.outside"), colour(theme, "color.ui.line")) }];
  /*
   * AND THE HATCHES, on exactly the same rails and for the same reasons. A patterned
   * layer whose image is missing draws NOTHING — not a fallback colour, nothing — so
   * a wetland would silently disappear rather than look wrong, which is the harder
   * bug to notice.
   */
  for (const [layerId, w] of Object.entries(STYLE_META.patterns ?? {}))
    out.push({ id: `${layerId}-hatch`,
               ...hatchImage(colour(theme, w.token), w.ground, w.stripe, w.darken, w.spacing, w.weight, w.cross) });
  // AND THE "YOU ARE HERE" PIN of the route overlay — the accent on this theme's paper.
  out.push({ id: HERE_IMAGE, ...hereImage(colour(theme, "color.ui.accent"), colour(theme, "color.outside")) });
  return out;
}

/**
 * A generated image's name in a PHONE's style: per theme. The binding's `Images` adds a name once and never replaces
 * it, so a theme change (a style swap) must ask for new names — `nativeStyle` renames the references to match.
 */
export const nativeImageName = (id: string, theme: string): string => `${id}.${theme}`;

/** What the binding's `Images` takes for one image: a source with its scale, and a stretchable image's zones. */
export interface NativeImage {
  source: { uri: string; scale: number };
  stretchX?: [number, number][];
  stretchY?: [number, number][];
  content?: [number, number, number, number];
}

/** The generated images as the phone's `Images` takes them, under their per-theme names. */
export function nativeImages(theme: string): Record<string, NativeImage> {
  const out: Record<string, NativeImage> = {};
  for (const g of generatedImages(theme)) {
    const img: NativeImage = { source: { uri: pngDataUri(g.width, g.height, g.data), scale: g.pixelRatio } };
    if (g.stretchX) img.stretchX = g.stretchX;
    if (g.stretchY) img.stretchY = g.stretchY;
    if (g.content) img.content = g.content;
    out[nativeImageName(g.id, theme)] = img;
  }
  return out;
}

/** `v` with every reference to a generated image (a string anywhere in a paint/layout value) renamed for `theme`. */
export function themedImageRefs(v: unknown, theme: string): unknown {
  if (typeof v === "string") return isGeneratedImage(v) ? nativeImageName(v, theme) : v;
  if (Array.isArray(v)) return v.map((x) => themedImageRefs(x, theme));
  return v;
}
