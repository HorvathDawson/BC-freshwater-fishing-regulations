/**
 * Spots — the user's own records.
 *
 * The store is a platform pair behind a relative import, so a package `exports` target
 * cannot defeat platform resolution (which is exactly what happened with the bundle
 * driver: naming the file directly handed the web build the native one).
 */
export type { Spot, SpotStore, SpotReading, SpotWeather, WeatherSample,
  WeatherWindow } from "./model";
export { spotLabel, isUntitled } from "./model";
export { captureSpot, noWeather, type CaptureInput, type WeatherSource } from "./capture";
export { openSpots } from "./store";
export { openMeteo, OPEN_METEO_ATTRIBUTION } from "./weather";
export { needsRefresh, refreshReason, refreshSpot } from "./refresh";
