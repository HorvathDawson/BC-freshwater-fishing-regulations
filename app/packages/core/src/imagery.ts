/**
 * SATELLITE IS ONLINE-ONLY (owner 2026-10-21, `handoff/PLAN/app-integration/decisions.md`).
 *
 * The satellite ground is Esri World Imagery, fetched live and never stored (its terms forbid exporting tiles for
 * offline use outside Esri's own software; see "Esri terms" below). So a reader who chose
 * Satellite and lost the connection must see the DRAWN map, told why, with the choice kept: when the imagery can be
 * reached again it comes back by itself.
 *
 * "Can be reached" is two facts: the device says it is online (the web's `navigator.onLine` and its online/offline
 * events; a phone's NetInfo — where its module is not linked the phone stays `online`), and the imagery itself has not
 * failed (a tile error on the web map, or the probe both platforms run). Either one down is down.
 *
 * ESRI TERMS (kept here when the 2026-10-21 research folder was cut; owner decision 2026-10-21 late):
 *  - Source: Esri's ArcGIS Online World_Imagery MapServer, 256 px tiles, maxzoom 19 (the z18 camera asks for z19), with
 *    `blankTile=false` so a place with no imagery answers 404 and MapLibre overzooms the parent (without it Esri serves
 *    its grey "Map data not yet available" tile). Below z12 it is 15 m Earthstar TerraColor; rural BC is 0.3-0.5 m to z17-18.
 *  - Licence: the Esri Master License Agreement and Product-Specific Terms (item 10df2279f9684e4a9f6a7f08febac2a9). The item
 *    "is not intended to be used to export tiles for offline", so nothing is cached or packaged (`no-store` probe,
 *    external source). Credit: "Powered by Esri" plus the service's copyrightText ("Source: Esri, Vantor, Earthstar
 *    Geographics, and the GIS User Community"); both copies are held equal by tools/imagery.test.ts.
 *  - The token-free `server.arcgisonline.com` URL is not, by Esri's terms, a licensed path for a non-Esri app (the licensed
 *    route is ArcGIS Location Platform). The owner chose it anyway, as the original app ran ("this is a non profit app").
 *    `EXPO_PUBLIC_ARCGIS_API_KEY` stays optional config (2M basemap tiles a month free, then $0.15 per 1,000); the keyed
 *    endpoint was never exercised, and `blankTile=false` on it is assumed, not measured.
 *  - Nothing sharper than EOX's 10 m Sentinel-2 mosaic is free and licensed for bundling; province-wide offline high-res is
 *    impossible whatever the licence (z16 over BC is ~150 GB).
 */

export type Basemap = "map" | "satellite";

export interface ImageryReach {
  /** The device's own word. A platform that cannot say keeps `true`. */
  online: boolean;
  /** A tile or the probe failed since the imagery was last reached. */
  failed: boolean;
}

export type ImageryEvent =
  | { type: "online" }
  | { type: "offline" }
  /** An imagery tile or the probe failed with an outage (`isImageryOutage`). */
  | { type: "failed" }
  /** The probe answered. */
  | { type: "reached" };

export const IMAGERY_START: ImageryReach = { online: true, failed: false };

/**
 * The reducer. Coming back ONLINE clears a failure too: the failure most likely was the connection, and if it was not,
 * the next tile error says so again. A probe that answers does NOT outrank the device saying it is offline.
 */
export function imageryReach(s: ImageryReach, e: ImageryEvent): ImageryReach {
  switch (e.type) {
    case "online": return { online: true, failed: false };
    case "offline": return { ...s, online: false };
    case "failed": return { ...s, failed: true };
    case "reached": return { ...s, failed: false };
  }
}

export const imageryUp = (s: ImageryReach): boolean => s.online && !s.failed;

/**
 * Is this tile failure an OUTAGE? The source asks Esri with `blankTile=false`, so a place with no imagery at that zoom
 * answers 404 — a hole the renderer fills from the parent tile, which is the imagery working. Everything else (no
 * status: the network; 401/403: a key; 5xx: the service) means the reader is not getting satellite.
 */
export const isImageryOutage = (status: number | undefined): boolean => status !== 404;

/** The ground actually drawn: the reader's choice, unless that is satellite and it cannot be reached. */
export const basemapInUse = (chosen: Basemap, up: boolean): Basemap =>
  chosen === "satellite" && up ? "satellite" : "map";

/** The Layers sheet's line while Satellite is chosen and cannot be shown. */
export const SATELLITE_OFFLINE =
  "Satellite needs a connection. Showing the map until you are back online; satellite returns by itself.";

/**
 * ESRI'S REQUIRED CREDIT while its imagery is on screen: "Powered by Esri" and the World Imagery service's
 * `copyrightText` (read 2026-10-21 from services.arcgisonline.com/.../World_Imagery/MapServer?f=json; Esri says to
 * take it from the service, and it changed from "Maxar" to "Vantor" in 2025). The map's own attribution carries the
 * same words from the style (`layers.source.json`, source `imagery`); `tools/imagery.test.ts` holds the two equal.
 */
export const IMAGERY_ATTRIBUTION =
  "Powered by Esri · Source: Esri, Vantor, Earthstar Geographics, and the GIS User Community";
export const IMAGERY_CREDIT = `Satellite imagery: ${IMAGERY_ATTRIBUTION}`;

/** The app's credit lines for the ground drawn: Esri's added only while satellite is. */
export const withImageryCredit = (lines: readonly string[], drawn: Basemap): readonly string[] =>
  drawn === "satellite" ? [...lines, IMAGERY_CREDIT] : lines;

/** The one z0 tile of an `{z}/{y}/{x}` template: the cheapest request that proves the imagery answers. */
export const imageryProbeUrl = (template: string): string =>
  template.replace("{z}", "0").replace("{y}", "0").replace("{x}", "0");
