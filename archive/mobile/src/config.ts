/**
 * App configuration — R2 data domain + shard version.
 *
 * Mirrors the webapp's data endpoints (see DEPLOY.md). Values come from
 * `app.json` → `expo.extra` and can be overridden per build/profile.
 */
import Constants from 'expo-constants';

interface AppExtra {
  dataBaseUrl?: string;
  shardVersion?: string;
}

const extra = (Constants.expoConfig?.extra ?? {}) as AppExtra;

/** R2 data domain, e.g. https://data.canifishthis.ca (prod). */
export const DATA_BASE_URL = (extra.dataBaseUrl ?? 'https://data.canifishthis.ca').replace(/\/$/, '');

/** Versioned shard prefix used for PMTiles / mobile db, e.g. "v2". */
export const SHARD_VERSION = extra.shardVersion ?? 'v2';

/** PMTiles archive URL (same file the webapp range-reads). */
export const PMTILES_URL = `${DATA_BASE_URL}/freshwater_atlas.pmtiles`;

/** Remote fallbacks for on-demand assets when they are not bundled. */
export const bathymetryUrl = (pdf: string) => `${DATA_BASE_URL}/bathymetry/${pdf}`;
export const sourceImageUrl = (png: string) => `${DATA_BASE_URL}/source/${png}`;

/** Feedback endpoint (same Worker route the webapp posts to). */
export const FEEDBACK_URL = `${DATA_BASE_URL}/api/feedback`;
