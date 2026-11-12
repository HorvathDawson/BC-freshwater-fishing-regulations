/**
 * @app/data — ONE interface, one implementation per platform.
 *
 * The shapes are in types.ts and the interface, `WaterSource`, in source.ts. Every method exists because a screen asks
 * for it (the query census in "One Interface, Two Storages" §1), and every one is in the
 * conformance suite, which runs against EVERY implementation. That suite is what stops
 * the phone and the browser answering the same question two ways — the v1 failure where
 * `waterbodyDataService.ts` was 1,157 lines on web and 116 on mobile.
 *
 * One shape carries the design: bulk reads take an ARRAY, because the map
 * colours a viewport, not a feature — `panelsFor` and `lakeStationsFor` (who speaks: `speakerFor`, standing.ts).
 *
 * REGULATIONS ARE NOT READ HERE. There is no rule, rule-set or licensing query on this
 * interface: regulations are not integrated, and `@app/core`'s `regulations.ts` says where
 * they plug in and which bundle tables they will read.
 */
export * from "./types";
export type { WaterSource } from "./source";

/** The driver seam (`Db`: four methods), for the layers that hand one to `@app/data` (T03). */
export type { Db, Row } from "./sql/db";

export { FEED_TTL_MS, fetchObs, httpFeed, type GaugeFeed } from "./feed/http";
// (T315) the gauges' last week, kept on the phone for offline
export { gaugeHistory } from "./feed/history";
// (T256) the Conditions chart before its envelope: the observations and forecast, no normal range
export { draftSeriesOf } from "./sql/shared";
// (T211) the live DFO salmon feed, the last capture kept on the device
export { httpSalmonFeed, type SalmonFeedSource } from "./feed/salmon";
// (T230) the provincial in-season changes + their curated links, the last capture kept on the device
export { httpInSeason, type InSeasonData, type InSeasonSource } from "./feed/inseason";
// (T199) the run timing (its own feed, not the release), read lazily, the last capture kept on the device
export { httpRunTiming, type RunTimingSource } from "./feed/runtiming";
export { openRunTimingStore } from "./spots/runTimingStore";

// The panel arithmetic: bundle rows plus a feed reading -> one answer. Three callers,
// one of them not a screen, so it cannot live behind a hook — see ./panel.ts.
export * from "./panel";
// The Conditions map's standings, per class of section (the static half) and per feed (the changing half) — ./standing.ts.
export * from "./standing";
// (T376) the watershed groups by name and by place (the release's `basin_grid`)
export type { Basin } from "./basinGrid";
export { downstreamWater } from "./downstream";
// THE RELEASE (P10): verified fetches, the manifest's diff and pairing
export { fetchVerified, verify } from "./fetchVerified";
export { parseManifest, pair, type Held, type Manifest, type SynopsisAppend } from "./manifest";
export { sha256Hex } from "./sha256";
// (T231) THE UPDATE SERVICE: every feed and the release checked against the origin, fetched verified, applied safely
export * from "./update";
// (T254) BAD CONNECTIONS: the low-bandwidth policy, the meter, the order (gate), the resilient download, what is downloading
export * from "./net";
// (T286) the open probe: a map tap to its drawn regulations, timed (test bed only; a no-op elsewhere)
export { openProbe } from "./openProbe";
// (T555) what the reader keeps by item id (a spot's water, an alert's water) follows core.item_alias, rewritten once
export { followSavedItems } from "./savedIds";
// (T234) phone-only notifications: gauge alerts and new data ready, the settings kept on the phone, the background check
export * from "./notify";
