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

/** Red = no public access, amber = conditional/permit-based access — matches
 *  web ADMIN_COLORS.land_access_closed/land_access_restricted. No hatch
 *  overlay on mobile yet (no image/sprite registration path off a static
 *  styleJSON), so this data-driven color split is the only severity signal. */
const LAND_ACCESS_COLOR_EXPR = [
  'match', ['get', 'restriction_level'],
  'closed', '#DC2626',
  'restricted', '#CC7A00',
  '#DC2626',
];

/** Brown — forest service roads (matches web forest_service_roads paint). */
const FOREST_SERVICE_ROAD_COLOR = '#8B6D3F';

/** Per-poi_type colors for water access points + waterfalls.
 *  NOTE: web renders these as distinct-shape glyph icons (see
 *  `createBoatLaunchIcon` etc. in webapp/src/components/Map.tsx); mobile
 *  doesn't yet have an image/sprite registration path off a static
 *  `styleJSON`, so these render as colored circle markers for now — a
 *  documented parity gap, same as wetlands/hatch patterns below. */
const POI_COLORS: Record<string, string> = {
  boat_launch: '#0072B2',
  pier: '#4A90E2',
  fishing_platform: '#059669',
  waterfall: '#0D47A1',
};

/** Grows the POI circle markers with zoom instead of a fixed radius, so
 *  they read as more than a small dot once zoomed in close. */
const POI_CIRCLE_RADIUS_EXPR = [
  'interpolate', ['linear'], ['zoom'],
  13, 4,
  16, 6,
  19, 9,
] as unknown as number;

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
    // ── Forest Service Roads (BC backcountry road network, DataBC) ────
    // Dotted when RETIRED/deactivated, solid otherwise — same
    // 'match'-on-status expression as the web layer.
    // Hidden by default for now (OSM's own road coverage reads better for
    // this) — kept wired, not deleted, so it's a one-line flip to bring
    // back or offer as a toggle later.
    {
      id: 'forest_service_roads',
      type: 'line',
      source: 'regulations',
      'source-layer': 'forest_service_roads',
      minzoom: 9,
      layout: { visibility: 'none' },
      paint: {
        'line-color': FOREST_SERVICE_ROAD_COLOR,
        'line-width': ['interpolate', ['linear'], ['zoom'], 9, 0.6, 12, 1, 15, 1.8],
        'line-opacity': 0.75,
        'line-dasharray': ['match', ['get', 'status'], 'RETIRED', ['literal', [1, 2]], ['literal', [1, 0]]],
      },
    },
    {
      id: 'forest_service_roads-label',
      type: 'symbol',
      source: 'regulations',
      'source-layer': 'forest_service_roads',
      minzoom: 12,
      filter: ['!=', ['get', 'display_name'], ''],
      layout: {
        visibility: 'none',
        'symbol-placement': 'line',
        'text-field': ['get', 'display_name'],
        'text-font': ['Noto Sans Regular'],
        'text-size': ['interpolate', ['linear'], ['zoom'], 12, 9, 15, 11],
      },
      paint: {
        'text-color': FOREST_SERVICE_ROAD_COLOR,
        'text-halo-color': '#FFFFFF',
        'text-halo-width': 1.2,
      },
    },
    // ── Land Access (watersheds, private/restricted land, DND, etc.) ──
    // Solid fill + border only — no hatch-pattern overlay yet on mobile
    // (no layer on this platform has one; see POI_COLORS note above for
    // the same static-styleJSON limitation).
    {
      id: 'admin_land_access-fill',
      type: 'fill',
      source: 'regulations',
      'source-layer': 'land_access',
      paint: { 'fill-color': LAND_ACCESS_COLOR_EXPR, 'fill-opacity': 0.14 },
    },
    {
      id: 'admin_land_access-line',
      type: 'line',
      source: 'regulations',
      'source-layer': 'land_access',
      paint: { 'line-color': LAND_ACCESS_COLOR_EXPR, 'line-width': 1.5, 'line-opacity': 0.6 },
    },
    {
      id: 'admin_land_access-label',
      type: 'symbol',
      source: 'regulations',
      'source-layer': 'land_access',
      minzoom: 11,
      layout: {
        'symbol-placement': 'point',
        'text-field': ['get', 'name'],
        'text-font': ['Noto Sans Regular'],
        'text-size': ['interpolate', ['linear'], ['zoom'], 10, 10, 14, 13],
        'text-max-width': 8,
        'text-allow-overlap': false,
      },
      paint: {
        'text-color': LAND_ACCESS_COLOR_EXPR,
        'text-halo-color': '#FFFFFF',
        'text-halo-width': 1.2,
      },
    },
    // ── Water access points + waterfalls (zoom-gated, colored by poi_type) ──
    {
      id: 'water_access_points-circle',
      type: 'circle',
      source: 'regulations',
      'source-layer': 'water_access_points',
      minzoom: 13,
      // Marina removed entirely — not rendered even if older cached tiles
      // still carry poi_type='marina' rows.
      filter: ['!=', ['get', 'poi_type'], 'marina'],
      paint: {
        'circle-radius': POI_CIRCLE_RADIUS_EXPR,
        'circle-color': [
          'match', ['get', 'poi_type'],
          'boat_launch', POI_COLORS.boat_launch,
          'pier', POI_COLORS.pier,
          'fishing_platform', POI_COLORS.fishing_platform,
          POI_COLORS.pier,
        ],
        'circle-stroke-color': '#FFFFFF',
        'circle-stroke-width': 1.2,
      },
    },
    {
      id: 'waterfalls-circle',
      type: 'circle',
      source: 'regulations',
      'source-layer': 'waterfalls',
      minzoom: 13,
      paint: {
        'circle-radius': POI_CIRCLE_RADIUS_EXPR,
        'circle-color': POI_COLORS.waterfall,
        'circle-stroke-color': '#FFFFFF',
        'circle-stroke-width': 1.2,
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
      minzoom: 11,
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
