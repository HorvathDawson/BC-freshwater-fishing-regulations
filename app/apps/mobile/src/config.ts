/**
 * THE ENDPOINTS (P10): every URL the app reads, from ONE base, and the only file a `localhost` may appear in
 * (`tools/no-localhost.test.ts`).
 *
 * `EXPO_PUBLIC_DATA_BASE` sets the base (an R2 domain in production; a LAN address such as http://192.168.1.20:39217 for
 * a device dev build, where "localhost" is the phone itself). Unset, it is the dev server on this machine (`pnpm tiles`).
 *
 * TWO SETS, ONE BASE. The release (`release/manifest.json`, P08/P11) is what the app moves to; until the reader switches
 * (P11), the bundle, tiles and feed below are the development set the app opens today. They must be one vintage: tiles,
 * bundle and feed join on section and station ids, and a mixed set colours nothing.
 */
const BASE = (process.env.EXPO_PUBLIC_DATA_BASE ?? "http://localhost:39217").replace(/\/$/, "");

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
  /** The province bundle (the development set until P11). */
  bundle: `${BASE}/province.sqlite`,
  /** The live gauge feed: real ECCC data from `python -m pipeline.hydro.publish`, matching the province bundle. */
  feed: `${BASE}/feeds/live`,
  /** Where sql.js finds its engine on the web: the same origin as the bundle it opens. */
  sqlWasm: `${BASE}/`,
} as const;
