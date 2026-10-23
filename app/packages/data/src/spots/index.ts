/**
 * Spots — the user's own records.
 *
 * The store is a platform pair behind a relative import, so a package `exports` target
 * cannot defeat platform resolution (which is exactly what happened with the bundle
 * driver: naming the file directly handed the web build the native one).
 */
export type { Spot, SpotStore, SpotReading, SpotWeather, WeatherSample,
  WeatherWindow, DailyWeather, DailyGauge, DayPlace, LeadUp } from "./model";
export { spotLabel, isUntitled } from "./model";
export { captureSpot, noWeather, type CaptureInput, type WeatherSource } from "./capture";
export { openSpots, openLayerStore, openLikeStore } from "./store";
export { openSpotsWith, SpotsCorrupt, type SpotsFs, type SpotsStore } from "./core";
export { serialised } from "./queue";
export { openMeteo, OPEN_METEO_ATTRIBUTION } from "./weather";
export { needsRefresh, refreshReason, refreshSpot } from "./refresh";
export { observedLine, spotHeadline, spotReadingFigure, spotRowLines, weatherTrend,
         LEAD_UP_WORDS, leadUpDayLabel, leadUpGaugeCells, leadUpGaugeMeta, leadUpGaugeSentence, leadUpGaugeTrend,
         leadUpGaugePending, leadUpQuantity, leadUpUnit, leadUpRainLine, leadUpWeatherCells, leadUpWeatherPending,
         leadUpWeatherSentence, skyWord, PHOTO_WORDS, photoFailed, removePhotoLabel, staleSpotsLine } from "./words";
export { DRAFT_WORDS } from "./words";
// S1: the week before a visit, and a spot's photographs (the platform pair behind `./photoStore`)
export { leadUpFor, leadUpMissing, leadUpPendingDays, weekGauge, weekWeather, type FeedIndex } from "./leadup";
export { isPhotoRef, nextPhotoRef, openPhotosWith, photoExt, photosDropped, PHOTO_MAX_EDGE,
         type PhotoFiles, type PhotoSource, type PhotoStore, type PickedPhoto } from "./photos";
export { openPhotos } from "./photoStore";
// T51: "Like your visit?" — the inputs to core's `likeDay`, and the last fetched forecast kept for offline
export { comingConditions, fetchLike, flowAhead, likeStation, NOTHING_FETCHED, readLike, visitConditions, visitHour,
         writeLike, type LikeFetched, type LikeIndex, type LikeSources, type LikeStore,
         type WeatherAhead } from "./likeVisit";
