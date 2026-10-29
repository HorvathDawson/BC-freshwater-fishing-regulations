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

export { FEED_TTL_MS, httpFeed, type GaugeFeed } from "./feed/http";
// (T211) the live DFO salmon feed, the last capture kept on the device
export { httpSalmonFeed } from "./feed/salmon";
// (T230) the provincial in-season changes + their curated links, the last capture kept on the device
export { httpInSeason, type InSeasonData, type InSeasonSource } from "./feed/inseason";

// The panel arithmetic: bundle rows plus a feed reading -> one answer. Three callers,
// one of them not a screen, so it cannot live behind a hook — see ./panel.ts.
export * from "./panel";
// The Conditions map's standings, per class of section (the static half) and per feed (the changing half) — ./standing.ts.
export * from "./standing";
export { downstreamWater } from "./downstream";
// THE RELEASE (P10): verified fetches, the manifest's diff and pairing
export { fetchVerified, verify } from "./fetchVerified";
export { parseManifest, pair, type Held, type Manifest } from "./manifest";
export { sha256Hex } from "./sha256";
// (T231) THE UPDATE SERVICE: every feed and the release checked against the origin, fetched verified, applied safely
export * from "./update";
