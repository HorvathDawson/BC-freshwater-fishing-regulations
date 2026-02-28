import type { LayerSpecification, ExpressionSpecification } from 'maplibre-gl';

// Feature base colors
const FEATURE_COLORS = {
    streams: '#4A90E2',    // Blue for streams
    lakes: '#64B5F6',      // Light blue for lakes
    wetlands: '#81C784',   // Green for wetlands
    manmade: '#9575CD',    // Purple for manmade waterbodies
};

/**
 * Per-type highlight colors for hover state.
 * Each is a saturated, darker variant of its base FEATURE_COLOR so the
 * highlight feels native to the feature type rather than generic.
 */
export const HIGHLIGHT_COLORS: Record<string, string> = {
    stream:   '#1A5296',  // Deep vivid blue  (streams base: #4A90E2)
    streams:  '#1A5296',
    lake:     '#0277BD',  // Dark lake blue   (lakes base:   #64B5F6)
    lakes:    '#0277BD',
    wetland:  '#43A047',  // Medium green — closer in hue to base #81C784
    wetlands: '#43A047',
    manmade:  '#6A1B9A',  // Deep purple       (manmade base: #9575CD)
};

/** Uniform color for the active-selection state (same for all types). */
export const SELECTION_COLOR = '#0072B2'; // Wong deep blue

/**
 * Returns a MapLibre `match` expression that picks a color from `colorMap`
 * based on the `_feature_type` property injected at highlight-write time,
 * falling back to `defaultColor` for unknown types.
 */
export const matchByFeatureType = (
    colorMap: Record<string, string>,
    defaultColor: string,
): ExpressionSpecification => [
    'match', ['get', '_feature_type'],
    'stream',   colorMap.stream   ?? defaultColor,
    'lake',     colorMap.lake     ?? defaultColor,
    'wetland',  colorMap.wetland  ?? defaultColor,
    'manmade',  colorMap.manmade  ?? defaultColor,
    defaultColor,
];

// Admin area fill colors — color-blind safe palette (Wong 2011)
// Orange (#E69F00) is used exclusively for NO FISHING zones so the signal
// is unambiguous even for deuteranopia / protanopia viewers.
const ADMIN_COLORS: Record<string, string> = {
    // ── NO FISHING zones (orange = universal danger signal) ──────────
    admin_parks_nat: '#E69F00',        // Wong orange — national parks
    ECOLOGICAL_RESERVE: '#E69F00',     // Wong orange — eco reserves
    // ── BC Parks sub-types ───────────────────────────────────────────
    PROVINCIAL_PARK: '#009E73',        // Wong bluish-green — still open
    PROTECTED_AREA: '#0072B2',         // Wong blue
    RECREATION_AREA: '#8B6914',        // Dark amber-gold (distinct from orange)
    admin_parks_bc_default: '#009E73', // Fallback: same as provincial park
    // ── Other admin types ────────────────────────────────────────────
    admin_wma: '#7B2D8B',             // Purple — wildlife mgmt areas
    admin_watersheds: '#006D77',       // Deep teal — watersheds
    admin_historic_sites: '#795548',   // Warm brown — heritage sites
};

// All admin source-layer names that could appear in the PMTiles
const _ADMIN_LAYERS = [
    'admin_parks_nat',
    'admin_parks_bc',
    'admin_wma',
    'admin_watersheds',
    'admin_historic_sites',
] as const;
void _ADMIN_LAYERS;

// Helper function to create regulation layers from new PMTiles structure
export const createRegulationLayers = (): LayerSpecification[] => {
    // FWA features first (bottom), admin overlays on top so borders/fills
    // are always visible above waterbody fills and lines.
    const fwaLayers: LayerSpecification[] = [];
    const adminLayers: LayerSpecification[] = [];

    // ── FWA FEATURE LAYERS (bottom of stack) ─────────────────────────

    // Wetlands — lowest of the FWA features
    fwaLayers.push({
        id: 'wetlands-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'wetlands',
        paint: {
            'fill-pattern': 'wetland-pattern',
            'fill-opacity': [
                'interpolate',
                ['linear'],
                ['zoom'],
                4, 0.3,
                10, 0.4,
                12, 0.5
            ]
        }
    });

    fwaLayers.push({
        id: 'wetlands-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'wetlands',
        paint: {
            'line-color': FEATURE_COLORS.wetlands,
            'line-width': [
                'interpolate',
                ['linear'],
                ['zoom'],
                4, 0.5,
                10, 0.75,
                12, 1.25
            ],
            'line-opacity': 0.6
        },
        layout: {
            'line-cap': 'round',
            'line-join': 'round'
        }
    });
    
    // Lakes - rendered above wetlands
    fwaLayers.push({
        id: 'lakes-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'lakes',
        paint: {
            'fill-color': FEATURE_COLORS.lakes,
            'fill-opacity': 0.4,
            'fill-antialias': true
        }
    });
    
    fwaLayers.push({
        id: 'lakes-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'lakes',
        paint: {
            'line-color': FEATURE_COLORS.lakes,
            'line-width': [
                'interpolate',
                ['linear'],
                ['zoom'],
                4, 0.8,
                10, 1,
                12, 1.5
            ],
            'line-opacity': 0.8
        },
        layout: {
            'line-cap': 'round',
            'line-join': 'round'
        }
    });
    
    // Manmade waterbodies
    fwaLayers.push({
        id: 'manmade-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'manmade',
        paint: {
            'fill-color': FEATURE_COLORS.manmade,
            'fill-opacity': 0.35,
            'fill-antialias': true
        }
    });
    
    fwaLayers.push({
        id: 'manmade-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'manmade',
        paint: {
            'line-color': FEATURE_COLORS.manmade,
            'line-width': [
                'interpolate',
                ['linear'],
                ['zoom'],
                4, 0.8,
                10, 1,
                12, 1.5
            ],
            'line-opacity': 0.7,
            'line-dasharray': [3, 2]  // Dashed line for manmade
        },
        layout: {
            'line-cap': 'round',
            'line-join': 'round'
        }
    });
    
    // Streams
    fwaLayers.push({
        id: 'streams',
        type: 'line',
        source: 'regulations',
        'source-layer': 'streams',
        paint: {
            'line-color': FEATURE_COLORS.streams,
            'line-width': [
                'interpolate',
                ['linear'],
                ['zoom'],
                // At low zoom: 0.5 + (order * 0.1) - minimal stream order impact when zoomed out
                4, ['+', 0.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.1]],
                // At medium zoom: 0.6 + (order * 0.15) - still small impact
                8, ['+', 0.6, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.15]],
                // At zoom 11: ramp up - original formula * 1.5
                11, ['*', ['+', 0.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 1.5],
                // At zoom 12: original formula * 2 (like original zoom 8)
                12, ['*', ['+', 0.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 2],
                // At zoom 14: original formula * 3 (like original zoom 12)
                14, ['*', ['+', 0.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 3],
                // At zoom 16+: even bigger
                16, ['*', ['+', 0.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 4]
            ],
            'line-opacity': 0.8
        },
        layout: {
            'line-cap': 'round',
            'line-join': 'round'
        }
    });
    
    // Regions (zone boundaries)
    fwaLayers.push({
        id: 'regions',
        type: 'line',
        source: 'regulations',
        'source-layer': 'regions',
        paint: {
            'line-color': ['get', 'stroke_color'],
            'line-width': [
                'interpolate',
                ['linear'],
                ['zoom'],
                4, 2,
                8, 2.5,
                12, 3
            ],
            'line-opacity': 0.8
        },
        layout: {
            'line-cap': 'round',
            'line-join': 'round'
        }
    });

    // ── ADMIN BOUNDARY LAYERS (rendered above FWA — fills + borders on top) ──────
    // Hatch pattern overlays for no-fishing zones are added dynamically in
    // Map.tsx on 'load' (after pattern images are registered) and appended
    // above these layers.

    // National parks — orange base tint + bold border
    adminLayers.push({
        id: 'admin_parks_nat-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'admin_parks_nat',
        paint: {
            'fill-color': ADMIN_COLORS.admin_parks_nat,
            'fill-opacity': 0.12,
        },
    });
    adminLayers.push({
        id: 'admin_parks_nat-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'admin_parks_nat',
        paint: {
            'line-color': ADMIN_COLORS.admin_parks_nat,
            'line-width': 2.5,
            'line-opacity': 0.75,
        },
    });

    // BC Parks — colour keyed by admin_type; eco reserves share the orange no-fishing signal.
    adminLayers.push({
        id: 'admin_parks_bc-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'admin_parks_bc',
        paint: {
            'fill-color': [
                'match',
                ['get', 'admin_type'],
                'PROVINCIAL_PARK',    ADMIN_COLORS.PROVINCIAL_PARK,
                'ECOLOGICAL_RESERVE', ADMIN_COLORS.ECOLOGICAL_RESERVE,
                'PROTECTED_AREA',     ADMIN_COLORS.PROTECTED_AREA,
                'RECREATION_AREA',    ADMIN_COLORS.RECREATION_AREA,
                ADMIN_COLORS.admin_parks_bc_default,
            ],
            'fill-opacity': 0.12,
        },
    });
    adminLayers.push({
        id: 'admin_parks_bc-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'admin_parks_bc',
        paint: {
            'line-color': [
                'match',
                ['get', 'admin_type'],
                'PROVINCIAL_PARK',    ADMIN_COLORS.PROVINCIAL_PARK,
                'ECOLOGICAL_RESERVE', ADMIN_COLORS.ECOLOGICAL_RESERVE,
                'PROTECTED_AREA',     ADMIN_COLORS.PROTECTED_AREA,
                'RECREATION_AREA',    ADMIN_COLORS.RECREATION_AREA,
                ADMIN_COLORS.admin_parks_bc_default,
            ],
            'line-width': [
                'match',
                ['get', 'admin_type'],
                'ECOLOGICAL_RESERVE', 2.5,
                1.5,
            ],
            'line-opacity': 0.65,
        },
    });

    // Wildlife Management Areas
    adminLayers.push({
        id: 'admin_wma-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'admin_wma',
        paint: { 'fill-color': ADMIN_COLORS.admin_wma, 'fill-opacity': 0.12 },
    });
    adminLayers.push({
        id: 'admin_wma-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'admin_wma',
        paint: { 'line-color': ADMIN_COLORS.admin_wma, 'line-width': 1.5, 'line-opacity': 0.5 },
    });

    // Watersheds
    adminLayers.push({
        id: 'admin_watersheds-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'admin_watersheds',
        paint: { 'fill-color': ADMIN_COLORS.admin_watersheds, 'fill-opacity': 0.1 },
    });
    adminLayers.push({
        id: 'admin_watersheds-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'admin_watersheds',
        paint: { 'line-color': ADMIN_COLORS.admin_watersheds, 'line-width': 1.5, 'line-opacity': 0.45 },
    });

    // Historic Sites
    adminLayers.push({
        id: 'admin_historic_sites-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'admin_historic_sites',
        paint: { 'fill-color': ADMIN_COLORS.admin_historic_sites, 'fill-opacity': 0.15 },
    });
    adminLayers.push({
        id: 'admin_historic_sites-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'admin_historic_sites',
        paint: { 'line-color': ADMIN_COLORS.admin_historic_sites, 'line-width': 1.5, 'line-opacity': 0.5 },
    });

    // FWA first, admin overlays on top
    return [...fwaLayers, ...adminLayers];
};

/**
 * Early-zoom layers for forest service roads, paths, and other minor roads.
 * The built-in protomaps `roads_other` layer only renders kind=other/path
 * starting at ~z14.  These supplemental layers make them visible from z11
 * so users can see FSRs while still at regulation-relevant zoom levels.
 * Labels appear from z12.
 */
export const createEarlyRoadLayers = (): LayerSpecification[] => [
    // ── Road lines (z11 – z14, then protomaps takes over) ────────────
    {
        id: 'roads_other_early',
        type: 'line',
        source: 'protomaps',
        'source-layer': 'roads',
        minzoom: 11,
        maxzoom: 15,           // protomaps roads_other handles z14+
        filter: [
            'all',
            ['!has', 'is_tunnel'],
            ['!has', 'is_bridge'],
            ['in', 'kind', 'other', 'path'],
            ['!=', 'kind_detail', 'pier'],
        ],
        paint: {
            'line-color': '#b0a899',
            'line-dasharray': [3, 1.5],
            'line-width': [
                'interpolate', ['exponential', 1.6], ['zoom'],
                11, 0.4,
                12, 0.8,
                14, 1.5,
            ],
            'line-opacity': [
                'interpolate', ['linear'], ['zoom'],
                11, 0.5,
                13, 0.7,
            ],
        },
    },
    // ── Road labels (z12+) ───────────────────────────────────────────
    {
        id: 'roads_other_early_label',
        type: 'symbol',
        source: 'protomaps',
        'source-layer': 'roads',
        minzoom: 12,
        maxzoom: 15,           // protomaps roads_labels_minor handles z15+
        filter: ['in', 'kind', 'other', 'path'],
        layout: {
            'symbol-placement': 'line',
            'text-field': ['get', 'name'],
            'text-font': ['Noto Sans Regular'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 12, 9, 14, 11],
            'text-max-angle': 30,
            'symbol-spacing': 250,
            'text-anchor': 'center',
            'text-allow-overlap': false,
            'text-padding': 6,
        },
        paint: {
            'text-color': '#6b6356',
            'text-halo-color': '#FFFFFF',
            'text-halo-width': 1.2,
            'text-opacity': [
                'interpolate', ['linear'], ['zoom'],
                12, 0.7,
                14, 1.0,
            ],
        },
    },
];

/**
 * Admin boundary label layers — rendered above basemap labels so parks,
 * eco reserves, WMAs, etc. are always legible.
 *
 * Layer-level minzoom is set to the regulation source floor (4) so that
 * **tippecanoe's per-feature `tippecanoe:minzoom`** (area-based) controls
 * when each individual label first appears.  Large polygons get low
 * minzooms and show labels early; small ones are excluded from low-zoom
 * tiles entirely, so their labels only appear when zoomed in.
 *
 * Text size still scales by zoom level for readability.
 */
export const createAdminLabelLayers = (): LayerSpecification[] => {
    const labelLayers: LayerSpecification[] = [];

    // ── National Parks (high-priority — bold + uppercase) ────────────
    labelLayers.push({
        id: 'admin_parks_nat-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'admin_parks_nat',
        minzoom: 4,
        layout: {
            'symbol-placement': 'point',
            'text-field': ['get', 'name'],
            'text-font': ['Noto Sans Bold'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 4, 9, 7, 12, 10, 15, 12, 16],
            'text-max-width': 8,
            'text-transform': 'uppercase',
            'text-letter-spacing': 0.05,
            'text-anchor': 'center',
            'text-allow-overlap': false,
            'text-ignore-placement': false,
            'text-padding': 4,
        },
        paint: {
            'text-color': '#B37700',
            'text-halo-color': '#FFFFFF',
            'text-halo-width': 1.5,
            'text-opacity': ['interpolate', ['linear'], ['zoom'], 4, 0.75, 7, 1],
        },
    });

    // ── BC Parks: Ecological Reserves (no-fishing — bold + uppercase) ──
    labelLayers.push({
        id: 'admin_parks_bc-eco-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'admin_parks_bc',
        filter: ['==', ['get', 'admin_type'], 'ECOLOGICAL_RESERVE'],
        minzoom: 4,
        layout: {
            'symbol-placement': 'point',
            'text-field': ['get', 'name'],
            'text-font': ['Noto Sans Bold'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 4, 8, 7, 10, 10, 13, 12, 14],
            'text-max-width': 8,
            'text-transform': 'uppercase',
            'text-letter-spacing': 0.05,
            'text-anchor': 'center',
            'text-allow-overlap': false,
            'text-padding': 4,
        },
        paint: {
            'text-color': '#B37700',
            'text-halo-color': '#FFFFFF',
            'text-halo-width': 1.5,
        },
    });

    // ── BC Parks: Provincial Parks ───────────────────────────────────
    labelLayers.push({
        id: 'admin_parks_bc-prov-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'admin_parks_bc',
        filter: ['==', ['get', 'admin_type'], 'PROVINCIAL_PARK'],
        minzoom: 4,
        layout: {
            'symbol-placement': 'point',
            'text-field': ['get', 'name'],
            'text-font': ['Noto Sans Medium'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 4, 8, 7, 10, 10, 12, 12, 14],
            'text-max-width': 8,
            'text-letter-spacing': 0.03,
            'text-anchor': 'center',
            'text-allow-overlap': false,
            'text-padding': 4,
        },
        paint: {
            'text-color': '#007A5E',
            'text-halo-color': '#FFFFFF',
            'text-halo-width': 1.5,
        },
    });

    // ── BC Parks: Protected Areas ────────────────────────────────────
    labelLayers.push({
        id: 'admin_parks_bc-prot-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'admin_parks_bc',
        filter: ['==', ['get', 'admin_type'], 'PROTECTED_AREA'],
        minzoom: 4,
        layout: {
            'symbol-placement': 'point',
            'text-field': ['get', 'name'],
            'text-font': ['Noto Sans Medium'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 4, 8, 7, 10, 10, 12, 12, 13],
            'text-max-width': 8,
            'text-anchor': 'center',
            'text-allow-overlap': false,
            'text-padding': 4,
        },
        paint: {
            'text-color': '#005A8C',
            'text-halo-color': '#FFFFFF',
            'text-halo-width': 1.5,
        },
    });

    // ── BC Parks: Recreation Areas ───────────────────────────────────
    labelLayers.push({
        id: 'admin_parks_bc-rec-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'admin_parks_bc',
        filter: ['==', ['get', 'admin_type'], 'RECREATION_AREA'],
        minzoom: 4,
        layout: {
            'symbol-placement': 'point',
            'text-field': ['get', 'name'],
            'text-font': ['Noto Sans Regular'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 4, 8, 7, 10, 10, 12, 12, 13],
            'text-max-width': 8,
            'text-anchor': 'center',
            'text-allow-overlap': false,
            'text-padding': 4,
        },
        paint: {
            'text-color': '#6B5200',
            'text-halo-color': '#FFFFFF',
            'text-halo-width': 1.5,
        },
    });

    // ── Wildlife Management Areas ────────────────────────────────────
    labelLayers.push({
        id: 'admin_wma-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'admin_wma',
        minzoom: 4,
        layout: {
            'symbol-placement': 'point',
            'text-field': ['get', 'name'],
            'text-font': ['Noto Sans Italic'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 4, 8, 7, 10, 10, 12, 12, 13],
            'text-max-width': 8,
            'text-anchor': 'center',
            'text-allow-overlap': false,
            'text-padding': 4,
        },
        paint: {
            'text-color': '#5E1D6B',
            'text-halo-color': '#FFFFFF',
            'text-halo-width': 1.5,
        },
    });

    // ── Watersheds ───────────────────────────────────────────────────
    labelLayers.push({
        id: 'admin_watersheds-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'admin_watersheds',
        minzoom: 4,
        layout: {
            'symbol-placement': 'point',
            'text-field': ['get', 'name'],
            'text-font': ['Noto Sans Italic'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 4, 8, 7, 10, 10, 12, 12, 13],
            'text-max-width': 8,
            'text-anchor': 'center',
            'text-allow-overlap': false,
            'text-padding': 4,
        },
        paint: {
            'text-color': '#004D57',
            'text-halo-color': '#FFFFFF',
            'text-halo-width': 1.5,
        },
    });

    // ── Historic Sites ───────────────────────────────────────────────
    labelLayers.push({
        id: 'admin_historic_sites-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'admin_historic_sites',
        minzoom: 4,
        layout: {
            'symbol-placement': 'point',
            'text-field': ['get', 'name'],
            'text-font': ['Noto Sans Italic'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 4, 8, 7, 10, 10, 11, 12, 12],
            'text-max-width': 8,
            'text-anchor': 'center',
            'text-allow-overlap': false,
            'text-padding': 4,
        },
        paint: {
            'text-color': '#5D4037',
            'text-halo-color': '#FFFFFF',
            'text-halo-width': 1.5,
        },
    });

    return labelLayers;
};
