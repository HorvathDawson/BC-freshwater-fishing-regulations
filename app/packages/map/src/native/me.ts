/**
 * (T303) THE READER'S OWN LOCATION on the phone's map: the blue dot and its accuracy circle, drawn from `MapProps.me.fix`
 * as the web's `useMe` draws its DOM marker — one GeoJSON point and two circle layers, coloured by the theme's tokens.
 *
 * Not MapLibre's `UserLocation`: on the emulator (maplibre-react-native 11.5.0) it drew no puck and `Camera
 * trackUserLocation` never moved the map, though expo-location had the fix. Our own fix is what the button already holds,
 * so the dot is where the app thinks you are, on every platform, and following is the camera going to each new fix.
 */
import { colour } from "../chrome";

export const ME_SOURCE = "me";
/** the dot's fill radius and its ring: 18 px across, as the web's */
const DOT_RADIUS = 6;
const RING_WIDTH = 3;
/** an accuracy circle wider than this many px is not drawn bigger (the web's cap) */
const MAX_ACCURACY_PX = 4000;

/** Metres per pixel at a latitude and zoom (512 px tiles). */
const metresPerPixel = (lat: number, zoom: number): number => 78271.51696 * Math.cos((lat * Math.PI) / 180) / 2 ** zoom;

type Fix = { lat: number; lon: number; accuracy?: number };

/** The source's data: the fix as one point, or nothing (the dot is off). */
export function meData(fix: Fix | null): string {
  if (!fix) return '{"type":"FeatureCollection","features":[]}';
  return JSON.stringify({ type: "FeatureCollection", features: [
    { type: "Feature", properties: {}, geometry: { type: "Point", coordinates: [fix.lon, fix.lat] } }] });
}

/**
 * The accuracy circle's radius in px by zoom: metres over metres-per-pixel at each whole zoom (doubling between them, so
 * the exponential-2 interpolation is exact), capped. A zoom expression must be the top-level interpolate, so the cap
 * is in the stops, not a `min` around them.
 */
export function accuracyRadius(fix: Fix): unknown[] | number {
  if (!fix.accuracy || !(fix.accuracy > 0)) return 0;
  const stops: number[] = [];
  for (let z = 0; z <= 22; z++) stops.push(z, Math.min(fix.accuracy / metresPerPixel(fix.lat, z), MAX_ACCURACY_PX));
  return ["interpolate", ["exponential", 2], ["zoom"], ...stops];
}

/** The two layers' paint, accuracy first (under the dot), from the theme's `color.ui.me` / `me-ring`. */
export function mePaint(theme: string, fix: Fix | null) {
  const blue = colour(theme, "color.ui.me"), ring = colour(theme, "color.ui.me-ring");
  return {
    accuracy: {
      "circle-radius": fix ? accuracyRadius(fix) : 0,
      "circle-color": blue,
      "circle-opacity": 0.13,
      "circle-stroke-color": blue,
      "circle-stroke-opacity": 0.4,
      "circle-stroke-width": 1,
      "circle-pitch-alignment": "map",
    },
    dot: {
      "circle-radius": DOT_RADIUS,
      "circle-color": blue,
      "circle-stroke-color": ring,
      "circle-stroke-width": RING_WIDTH,
    },
  } as const;
}
