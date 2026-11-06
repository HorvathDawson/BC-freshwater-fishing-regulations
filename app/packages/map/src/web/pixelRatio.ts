/**
 * (T420) THE WEB MAP DRAWS AT MOST 2 PIXELS PER CSS PIXEL — a set rule, the same for every browser.
 *
 * The owner's lag was mobile Chrome on a Pixel 9 (devicePixelRatio ~2.6; desktop fine). MapLibre draws at the device's
 * ratio by default, so a phone canvas is ~1080×2260 device pixels, and every layer fills it. On a GPU that is slow at
 * fill (the proxy: SwiftShader, Pixel 9 viewport 412×915 @ 2.625, Fraser Valley pans) the frame time follows the pixel
 * count: z10 38.8 → 27.1 ms, z11 41.8 → 25.0, z12 34.0 → 23.4 at ratio 2.625 → 2; no single style layer, the streams
 * included, moved it beyond the noise. At 2 the lines stay sharp on a phone (lines and labels are vector); desktops
 * (ratio 1–2) are untouched.
 *
 * Followed when the ratio changes (a window dragged to another screen, a browser zoom): the map is re-told the capped
 * value, so a window that opened on a 1× screen is not left blurry on a 2× one.
 */
import type { Map as GLMap } from "maplibre-gl";
import { MAX_PIXEL_RATIO } from "../pixel-ratio";

/** The most device pixels per CSS pixel the web map draws. */
export const WEB_MAX_PIXEL_RATIO = MAX_PIXEL_RATIO;

/** The ratio the web map draws at for a device ratio. */
export function webPixelRatio(deviceRatio: number): number {
  return Math.min(deviceRatio > 0 ? deviceRatio : 1, WEB_MAX_PIXEL_RATIO);
}

/** Keep `map` at the capped ratio as the device ratio changes; returns the unsubscribe. */
export function followPixelRatio(map: Pick<GLMap, "setPixelRatio" | "getPixelRatio">): () => void {
  let mq: MediaQueryList | null = null;
  const watch = () => {
    mq?.removeEventListener("change", onChange);
    mq = window.matchMedia(`(resolution: ${window.devicePixelRatio}dppx)`);
    mq.addEventListener("change", onChange);
  };
  const onChange = () => {
    const r = webPixelRatio(window.devicePixelRatio);
    if (map.getPixelRatio() !== r) map.setPixelRatio(r);
    watch();
  };
  watch();
  return () => mq?.removeEventListener("change", onChange);
}
