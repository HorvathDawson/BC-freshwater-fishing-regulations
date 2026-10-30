/**
 * @app/core — domain logic. Pure TypeScript, no React, no platform.
 *
 * Flow, gauge trust, the zoom ladder and the trace. The Regulations page's logic is
 * `./regs` (re-exported through `./regulations`) — pure functions from a hydrated `RegsInputs` to a `RegsPage`. The
 * day's STATUS of a section or a water is (`./statusIndex`, read through `./status`). How old a value is, and how that
 * is said, is `./freshness`.
 *
 * A PURE BARREL: no logic lives here (T61 2.15).
 */

export * from "./flow";
export * from "./trace";
export * from "./ladder";
export * from "./trust";
export * from "./section";
export * from "./regulations";
export * from "./status";
export * from "./statusIndex";
export * from "./access";
export * from "./lakeFacts";
export * from "./salmon";
export * from "./salmonFeed";
export * from "./runTiming";
export * from "./inseason";
export * from "./glance";
export * from "./method";
export * from "./paint";
export * from "./sliced";
export * from "./search";
export * from "./searchGeometry";
// (T247) the in-memory search index: typos, management units, regions, towns
export * from "./searchIndex";
export * from "./recentSearches";
export * from "./near";
export * from "./day";
export * from "./conditions";
export * from "./gaugeSide";
export * from "./change";
export * from "./chartZoom";
export * from "./routeMap";
export * from "./temperature";
export * from "./install";
export * from "./pairing";
export * from "./menu";
export * from "./iconSet";
export * from "./more";
// (T238) More → Offline maps: each region's size, kept or not, save / remove, storage used
export * from "./offline";
// (T248) the first-run walkthrough and the Help page, generated from the legend tokens
export * from "./help";
// (T231) the data updates in words: the sources' lines, the sync icon's failure sheet, the toast
export * from "./updates";
// (T251) the app's version (one source, the release gate) and the Updates log's events and words
export * from "./appVersion";
export * from "./updateLog";
export * from "./imagery";
export * from "./staleness";
export * from "./freshness";
export * from "./likeVisit";
export * from "./spotVisits";
export * from "./gestures";
export * from "./waterViews";
export * from "./tabs";
export * from "./spotWords";
export * from "./uiWords";
export * from "./waterNav";
export * from "./mapRail";
export * from "./spotPins";
export * from "./layers";
export * from "./mapKey";
export * from "./obstacles";
export * from "./obstacleIcons";
export * from "./tidal";
// (T292) a tapped sewage lagoon / tailings pond / dump: what it is, and that it is not a fishing water
export * from "./notFishing";
export * from "./dayPicker";
// (T254) bad connections in words: the Data setting, the slow / offline banner, waiting and missing parts, the rule book offer
export * from "./net";
