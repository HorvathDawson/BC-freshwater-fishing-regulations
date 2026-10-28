/**
 * THE MAP'S GENERATED IMAGES on the web: `generatedImages` (`../images.ts`, one list for both renderers) added with
 * `map.addImage` in the theme's colours, and added again on a theme change or whenever a layer asks for one it does
 * not have (`styleimagemissing`).
 */
import type * as maplibregl from "maplibre-gl";
import { generatedImages } from "../images";

export { isGeneratedImage } from "../images";

export function addImages(m: maplibregl.Map, theme: string): void {
  for (const g of generatedImages(theme)) {
    if (m.hasImage(g.id)) m.removeImage(g.id);
    m.addImage(g.id, g as unknown as ImageData, {
      pixelRatio: g.pixelRatio,
      ...(g.stretchX ? { stretchX: g.stretchX, stretchY: g.stretchY, content: g.content } : {}),
    });
  }
}
