/**
 * THE ENDPOINTS (P10): every URL the app reads, from ONE base, and the only file a `localhost` may appear in
 * (`tools/no-localhost.test.ts`).
 *
 * `EXPO_PUBLIC_DATA_BASE` sets the base (an R2 domain in production; a LAN address such as http://192.168.1.20:39217 for
 * a device dev build, where "localhost" is the phone itself). Unset, it is the dev server on this machine (`pnpm tiles`).
 *
 * ONE SET, ONE BASE. The release (`release/manifest.json`, P08/P11) is the app's data; the tiles below are drawn while
 * the manifest names no tile set of its own, and the feed is the live gauges. They must be one vintage: tiles, release
 * and feed join on section and station ids, and a mixed set colours nothing.
 */
import { imageryTiles } from "@app/map";

const BASE = (process.env.EXPO_PUBLIC_DATA_BASE ?? "http://localhost:39217").replace(/\/$/, "");

/**
 * THE MENU'S BUILD FACTS (owner 2026-10-21). `CONTACT` is THE ONE contact value the menu's Contact and Report a problem
 * pages read: an email address (`name@domain`) or a web link (`https://…`). Unset, the menu says contact details are
 * coming and a report goes out through the share sheet only — no address is ever written into the code; the owner sets
 * `EXPO_PUBLIC_CONTACT` for a build. `APP_BUILD` names the build in About and in a report ("1.2.0 (34)"), set by the
 * build (`EXPO_PUBLIC_APP_BUILD`); unset, the menu says it is not stated.
 */
export const MENU_INFO = {
  appBuild: process.env.EXPO_PUBLIC_APP_BUILD?.trim() || null,
  contact: process.env.EXPO_PUBLIC_CONTACT?.trim() || null,
} as const;

/**
 * THE SATELLITE IMAGERY'S TILES (owner 2026-10-21: Esri World Imagery, online-only). KEYLESS BY DEFAULT — the owner's
 * decision (2026-10-21 late: "we used esri free without a key in original app lets do that again. this is a non profit
 * app"): unset, the template is Esri's own ArcGIS Online World_Imagery MapServer, as the original app used, and that is
 * what ships. `EXPO_PUBLIC_ARCGIS_API_KEY` is OPTIONAL config: an ArcGIS Location Platform key (restricted by referrer /
 * app in the ArcGIS dashboard) switches the tiles to Esri's keyed endpoint, counted against that account's free tier.
 * Esri's terms on the keyless path: handoff/PLAN/app-data/satellite/esri/README.md. Never a key in the code.
 */
export const IMAGERY_TILES = imageryTiles(process.env.EXPO_PUBLIC_ARCGIS_API_KEY?.trim() || null);

export const ENDPOINTS = {
  base: BASE,
  /** The release manifest (app-manifest/2); every release file's url is relative to `${base}/release/`. */
  manifest: `${BASE}/release/manifest.json`,
  release: `${BASE}/release/`,
  tiles: {
    atlas: `${BASE}/atlas.pmtiles`,
    basemap: `${BASE}/basemap.pmtiles`,
    outside: `${BASE}/bc_outside.geojson`,
  },
  /** The live gauge feed: real ECCC data from `python -m pipeline.gauges.feed.publish`, matching the release. */
  feed: `${BASE}/feeds/live`,
  /** (T211) The live DFO salmon feed (`python -m pipeline.regs.dfo_salmon.live`; R2 `feeds/dfo/`). */
  dfoFeed: `${BASE}/feeds/dfo`,
  /** Where sql.js finds its engine on the web: the same origin as the release files it opens. */
  sqlWasm: `${BASE}/`,
} as const;
