/**
 * Spots — the user's own records.
 *
 * The store is a platform pair behind a relative import, so a package `exports` target
 * cannot defeat platform resolution (which is exactly what happened with the bundle
 * driver: naming the file directly handed the web build the native one).
 */
export type { Spot, SpotReading, SpotWeather, WeatherSample, DailyWeather, DailyGauge, LeadUp, Visit } from "./model";
// T294: a spot's visits, each rated; every visit reads as a spot (`visitSpot`) and is written back alone (`withVisit`)
export { addVisit, captureVisit, latestVisitId, onVisit, photoOwner, rateVisit, removeVisit, spotVisits,
         visitIdOf, visitSpot, withVisit, type VisitSpot } from "./visits";
export { spotLabel, isUntitled } from "./model";
export { captureSpot, type WeatherSource } from "./capture";
export { openSpots, openLayerStore, openLikeStore, openSalmonFeedStore, openGaugeHistoryStore, openInSeasonStore, openInSeasonSeenStore, openInSeasonInboxStore, openFirstRunStore, openDataModeStore, openWalkthroughStore, openLocationAskStore, openAppUpdateStore, openRecentSearchStore, openRunSpeciesStore,
         openUpdateStore, openUpdateLogStore, openReportOutboxStore, openCrashSettingStore, openCrashPendingStore, openCrashExitStore,
         openInstallIdStore, openNotifyStore } from "./store";
export { openSpotsWith, type SpotsFs } from "./core";
export { openMeteo } from "./weather";
export { needsRefresh, refreshReason, refreshSpot } from "./refresh";
export { observedLine, spotHeadline, spotReadingFigure, spotRowLines, weatherTrend, LEAD_UP_WORDS, leadUpDayLabel,
         leadUpGaugeCells, leadUpGaugeMeta, leadUpGaugeSentence, leadUpGaugeTrend, leadUpGaugePending, leadUpQuantity,
         leadUpUnit, leadUpRainLine, leadUpWeatherCells, leadUpWeatherPending, leadUpWeatherSentence, PHOTO_WORDS,
         photoFailed, photoLabel, removePhotoLabel, staleSpotsLine } from "./words";
export { DRAFT_WORDS } from "./words";
// S1: the week before a visit, and a spot's photographs (the platform pair behind `./photoStore`)
export { leadUpPendingDays } from "./leadup";
export { isPhotoRef, type PhotoSource, type PhotoStore, type PickedPhoto } from "./photos";
export { openPhotos } from "./photoStore";
// T51: "Like your visit?" — the inputs to core's `likeDay`, and the last fetched forecast kept for offline
export { comingConditions, fetchLike, NOTHING_FETCHED, readLike, visitConditions, visitHour, writeLike,
         type LikeFetched, type LikeSources, type LikeStore } from "./likeVisit";
// T170: the visit day's flow from the day's record — the one read (`flowOnDay`), HYDAT from our data host, the fill
export { dayFlowAskable, dayFlowStation, fillDayFlow, storedHydat, readingIsTheDays, visitDay, type DayFlowAnswer,
         type HydatDaily } from "./dayFlow";
// Q161: the trip day's gauge panel for a spot added for an earlier day
export { savedGauges } from "./dayFlow";
export { DAY_FLOW_WORDS, DAY_PANEL_WORDS, dayFlowLine } from "./words";

export type { SkyIcon } from "./words";
