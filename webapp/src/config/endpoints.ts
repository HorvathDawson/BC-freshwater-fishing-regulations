/**
 * Centralized endpoint/base-URL config.
 *
 * All of the app's remote bases derive from the single `VITE_TILE_BASE_URL`
 * env var (the R2 worker origin in production, empty/relative in dev where
 * Vite proxies `/data` and `/api`). Previously this fallback logic was
 * re-derived in four separate files with slightly divergent defaults —
 * consolidated here so there's one source of truth.
 *
 *   - DATA_BASE — static JSON + assets (tier0.json, admin_visibility.json,
 *     in_season.json, bathymetry PDFs, row images). Falls back to `/data`
 *     so a dev server with no env var still resolves local files.
 *   - API_BASE  — dynamic edge endpoints (`/api/resolve`, `/api/feedback`).
 *     Falls back to `''` (same-origin relative) in dev.
 *   - TILE_BASE — PMTiles source URL for MapLibre, prefixed with the
 *     `pmtiles://` protocol. Falls back to the local `pmtiles:///data`
 *     dev path when unset.
 */

const RAW_BASE = import.meta.env.VITE_TILE_BASE_URL;

/** Static JSON + asset base (tier0/admin/in-season/bathymetry/row images). */
export const DATA_BASE = RAW_BASE || '/data';

/** Dynamic edge API base (/api/resolve, /api/feedback). */
export const API_BASE = RAW_BASE || '';

/** PMTiles source URL for MapLibre (falls back to local `/data` dev path). */
export const TILE_BASE = RAW_BASE ? `pmtiles://${RAW_BASE}` : 'pmtiles:///data';
