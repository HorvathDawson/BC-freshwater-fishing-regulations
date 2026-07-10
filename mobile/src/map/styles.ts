/**
 * Map style (mobile) — MapLibre Native style for the fishing atlas.
 *
 * PARITY NOTE
 * -----------
 * The web app's layer specs live in `webapp/src/map/styles.ts` as plain JSON
 * builders (they only import a `LayerSpecification` *type* from maplibre-gl).
 * Those builders are framework-agnostic and should be copied here **verbatim**
 * during the full UI port — swap the single type import for the local alias
 * below and they render identically on MapLibre Native.
 *
 * This module currently provides:
 *   - The remote PMTiles source (reused straight from R2 — same tiles the web
 *     app serves), via the `pmtiles://` protocol registered in MapView.
 *   - A protomaps light basemap.
 *   - The core FWA overlay layers + the corrected per-reserve `aboriginal_lands`
 *     layer, so the map is functional while the remaining web layers are ported.
 */
import { PMTILES_URL } from '../config';

/** Structural alias for a MapLibre layer spec (matches the web type import). */
export type LayerSpec = Record<string, unknown>;

const FEATURE_COLORS = {
  streams: '#4A90E2',
  lakes: '#64B5F6',
  wetlands: '#81C784',
  manmade: '#9575CD',
  ungazetted: '#F5A623',
};

/** OSM-style tan/ochre for Indigenous lands (matches web ADMIN_COLORS). */
const ABORIGINAL_LANDS_COLOR = '#8B6508';

/**
 * Build the atlas overlay layers rendered on top of the basemap.
 * Ported subset of `createRegulationLayers()`; extend by copying the remaining
 * layer builders from the web `styles.ts`.
 */
export function createRegulationLayers(): LayerSpec[] {
  return [
    {
      id: 'wetlands-fill',
      type: 'fill',
      source: 'regulations',
      'source-layer': 'wetlands',
      paint: { 'fill-color': FEATURE_COLORS.wetlands, 'fill-opacity': 0.4 },
    },
    {
      id: 'lakes-fill',
      type: 'fill',
      source: 'regulations',
      'source-layer': 'lakes',
      paint: { 'fill-color': FEATURE_COLORS.lakes, 'fill-opacity': 0.4 },
    },
    {
      id: 'lakes-line',
      type: 'line',
      source: 'regulations',
      'source-layer': 'lakes',
      paint: { 'line-color': FEATURE_COLORS.streams, 'line-opacity': 0.8, 'line-width': 1 },
    },
    {
      id: 'manmade-fill',
      type: 'fill',
      source: 'regulations',
      'source-layer': 'manmade',
      paint: { 'fill-color': FEATURE_COLORS.manmade, 'fill-opacity': 0.35 },
    },
    {
      id: 'streams',
      type: 'line',
      source: 'regulations',
      'source-layer': 'streams',
      paint: {
        'line-color': FEATURE_COLORS.streams,
        'line-opacity': 0.8,
        'line-width': [
          'interpolate',
          ['linear'],
          ['zoom'],
          4,
          ['+', 0.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.1]],
          12,
          ['*', ['+', 0.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 2],
          16,
          ['*', ['+', 0.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 4],
        ],
      },
    },
    // ── Indigenous / Aboriginal Lands (per-reserve; OSM-style rendering) ──
    {
      id: 'admin_aboriginal_lands-fill',
      type: 'fill',
      source: 'regulations',
      'source-layer': 'aboriginal_lands',
      paint: { 'fill-color': ABORIGINAL_LANDS_COLOR, 'fill-opacity': 0.1 },
    },
    {
      id: 'admin_aboriginal_lands-line',
      type: 'line',
      source: 'regulations',
      'source-layer': 'aboriginal_lands',
      minzoom: 9,
      paint: {
        'line-color': ABORIGINAL_LANDS_COLOR,
        'line-width': ['interpolate', ['linear'], ['zoom'], 9, 2, 14, 4],
        'line-opacity': ['interpolate', ['linear'], ['zoom'], 9, 0.15, 14, 0.3],
      },
    },
    {
      id: 'admin_aboriginal_lands-label',
      type: 'symbol',
      source: 'regulations',
      'source-layer': 'aboriginal_lands',
      minzoom: 10,
      layout: {
        'symbol-placement': 'point',
        'text-field': ['coalesce', ['get', 'name'], ['get', 'name_en']],
        'text-font': ['Noto Sans Regular'],
        'text-size': ['interpolate', ['linear'], ['zoom'], 10, 10, 14, 13],
        'text-max-width': 8,
        'text-allow-overlap': false,
      },
      paint: {
        'text-color': '#5A430A',
        'text-halo-color': '#FFFFFF',
        'text-halo-width': 1.2,
      },
    },
  ];
}

/**
 * The complete MapLibre style object: protomaps basemap + remote PMTiles atlas
 * source + overlay layers. Passed to `<MapLibreGL.MapView styleJSON={...}/>`.
 */
export function buildMapStyle(): object {
  return {
    version: 8,
    glyphs: 'https://protomaps.github.io/basemaps-assets/fonts/{fontstack}/{range}.pbf',
    sources: {
      // Reuse the exact same PMTiles the web app serves from R2.
      regulations: {
        type: 'vector',
        url: `pmtiles://${PMTILES_URL}`,
      },
    },
    layers: [
      { id: 'background', type: 'background', paint: { 'background-color': '#eef2f6' } },
      ...createRegulationLayers(),
    ],
  };
}
