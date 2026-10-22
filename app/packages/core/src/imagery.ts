/**
 * SATELLITE IS ONLINE-ONLY (owner 2026-10-21, `handoff/PLAN/app-integration/decisions.md`).
 *
 * The satellite ground is Esri World Imagery, fetched live and never stored (its terms forbid exporting tiles for
 * offline use outside Esri's own software: `handoff/PLAN/app-data/satellite/README.md`). So a reader who chose
 * Satellite and lost the connection must see the DRAWN map, told why, with the choice kept: when the imagery can be
 * reached again it comes back by itself.
 *
 * "Can be reached" is two facts: the device says it is online (the web's `navigator.onLine` and its online/offline
 * events; a phone's NetInfo — where its module is not linked the phone stays `online`), and the imagery itself has not
 * failed (a tile error on the web map, or the probe both platforms run). Either one down is down.
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
export const IMAGERY_CREDIT =
  "Satellite imagery: Powered by Esri · Source: Esri, Vantor, Earthstar Geographics, and the GIS User Community";

/** The app's credit lines for the ground drawn: Esri's added only while satellite is. */
export const withImageryCredit = (lines: readonly string[], drawn: Basemap): readonly string[] =>
  drawn === "satellite" ? [...lines, IMAGERY_CREDIT] : lines;

/** The one z0 tile of an `{z}/{y}/{x}` template: the cheapest request that proves the imagery answers. */
export const imageryProbeUrl = (template: string): string =>
  template.replace("{z}", "0").replace("{y}", "0").replace("{x}", "0");
