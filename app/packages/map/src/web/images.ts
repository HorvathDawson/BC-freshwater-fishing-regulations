/**
 * THE MAP'S GENERATED IMAGES — the gauge label's pill, the hatches and the route overlay's "you are here" pin — added in
 * the theme's colours, and added again on a theme change or whenever a layer asks for one it does not have.
 */
import type * as maplibregl from "maplibre-gl";
import { colour } from "../chrome";
import { hatchImage } from "../hatch";
import { hereImage } from "../here-mark";
import { pillImage } from "../pill";
import { STYLE_META } from "../style";
import { HERE_IMAGE } from "../trace-overlay";

/** Is `id` one of the images `addImages` makes (what `styleimagemissing` re-adds)? */
export const isGeneratedImage = (id: string): boolean =>
  id === "gauge-pill" || id === HERE_IMAGE || id.endsWith("-hatch");

export function addImages(m: maplibregl.Map, theme: string): void {
  // The pill's border read `color.line`, which was never a token, so every theme drew
  // the light theme's hairline from a literal fallback. It is the app's hairline now.
  const img = pillImage(colour(theme, "color.outside"), colour(theme, "color.ui.line"));
  if (m.hasImage("gauge-pill")) m.removeImage("gauge-pill");
  m.addImage("gauge-pill", img as unknown as ImageData,
             { pixelRatio: img.pixelRatio, stretchX: img.stretchX,
               stretchY: img.stretchY, content: img.content });
  /*
   * AND THE HATCHES, on exactly the same rails and for the same reasons. A patterned
   * layer whose image is missing draws NOTHING — not a fallback colour, nothing — so
   * a wetland would silently disappear rather than look wrong, which is the harder
   * bug to notice. `styleimagemissing` covers whichever ordering we get.
   */
  for (const [layerId, w] of Object.entries(STYLE_META.patterns ?? {})) {
    const id = `${layerId}-hatch`;
    const img2 = hatchImage(colour(theme, w.token), w.ground, w.stripe, w.darken,
                            w.spacing, w.weight, w.cross);
    if (m.hasImage(id)) m.removeImage(id);
    m.addImage(id, img2 as unknown as ImageData, { pixelRatio: img2.pixelRatio });
  }
  // AND THE "YOU ARE HERE" PIN of the route overlay — the accent on this theme's paper.
  const here = hereImage(colour(theme, "color.ui.accent"), colour(theme, "color.outside"));
  if (m.hasImage(HERE_IMAGE)) m.removeImage(HERE_IMAGE);
  m.addImage(HERE_IMAGE, here as unknown as ImageData, { pixelRatio: here.pixelRatio });
}
