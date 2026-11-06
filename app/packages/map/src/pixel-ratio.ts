/**
 * (T420 web, T448 native) THE MOST DEVICE PIXELS THE MAP FILLS PER CSS PIXEL / DP — one rule for both maps.
 *
 * A Pixel 9 is 2.625; at 2 the map fills 42 % fewer pixels and lines and labels stay sharp (they are vector). The web
 * gives it to MapLibre GL JS as its pixel ratio (`web/pixelRatio.ts`); the native map gives it to the patched binding's
 * `maxPixelRatio` (Android: a smaller drawing surface the compositor scales up, so the map's scale, gestures and taps
 * are unchanged; iOS ignores it).
 */
export const MAX_PIXEL_RATIO = 2;
