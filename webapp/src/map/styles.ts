import type { LayerSpecification, FilterSpecification, Map as MapLibreMap } from 'maplibre-gl';
import type { LayerManifest } from '../services/waterbodyDataService';
import type { LayerOverrides } from '../utils/layerOverrides';

/**
 * Canonical map: layer_manifest.json key → the MapLibre style-layer ids it
 * drives, split by geometry role so manifest `style` knobs land on the right
 * paint properties (fill_* on fills, line_* on lines). This is the SINGLE
 * frontend mapping — the layer-menu toggle list is derived from it (fill+line),
 * and applyManifestStyle() reads it to push manifest appearance onto the map.
 *
 * A manifest key that drives a data-driven layer (colour via match/interpolate)
 * simply omits that colour in the manifest — applyManifestStyle only writes the
 * keys actually present, so listing the layer here is always safe.
 */
export const LAYER_STYLE_TARGETS: Record<string, { fill?: string[]; line?: string[]; label?: string[] }> = {
    streams: { line: ['streams'], label: ['streams-label'] },
    under_lake_streams: { line: ['under-lake-streams'], label: ['under-lake-streams-label'] },
    lakes: { fill: ['lakes-fill'], line: ['lakes-line'], label: ['lakes-label'] },
    wetlands: { fill: ['wetlands-fill'], line: ['wetlands-line'], label: ['wetlands-label'] },
    manmade: { fill: ['manmade-fill'], line: ['manmade-line'], label: ['manmade-label'] },
    tidal_boundary: { fill: ['tidal_boundary-fill'], line: ['tidal_boundary-line'] },
    regions: { line: ['regions'] },
    wmu_boundary: { line: ['management_units'] },
    parks_nat: { fill: ['admin_parks_nat-fill'], line: ['admin_parks_nat-line'] },
    // Border fade-in zoom applies to every eco/parks border variant.
    eco_reserves: { fill: ['eco_reserves-fill', 'admin_parks_bc-fill'], line: ['eco_reserves-line', 'admin_parks_bc-line', 'admin_parks_bc-eco-line'] },
    wma: { fill: ['admin_wma-fill'], line: ['admin_wma-line'] },
    historic_sites: { fill: ['admin_historic_sites-fill'], line: ['admin_historic_sites-line'] },
    watersheds: { fill: ['admin_watersheds-fill'], line: ['admin_watersheds-line'] },
    land_access: { fill: ['admin_land_access-fill'], line: ['admin_land_access-line'] },
    aboriginal_lands: { fill: ['admin_aboriginal_lands-fill'], line: ['admin_aboriginal_lands-line'] },
    land_parcels_private: { fill: ['admin_land_parcels_private-fill'], line: ['admin_land_parcels_private-line'] },
    forest_service_roads: { line: ['forest_service_roads'], label: ['forest_service_roads-label'] },
    // OSM trails/paths ("kind=other/path") — the basemap trail overlay (source:
    // 'protomaps'), surfaced here so it can be toggled like any other layer
    // instead of being permanently on. See createEarlyRoadLayers().
    osm_trails: { line: ['roads_other_early'], label: ['roads_other_early_label'] },
    bc_mask: { fill: ['bc-mask'] },
};

/** Flat list of every style-layer id a manifest key maps to (fill+line+label). */
export const styleLayerIds = (key: string): string[] => {
    const t = LAYER_STYLE_TARGETS[key];
    return t ? [...(t.fill || []), ...(t.line || []), ...(t.label || [])] : [];
};

/** Fill+line ids only (the layers that carry an adjustable opacity paint prop). */
export const opacityLayerIds = (key: string): string[] => {
    const t = LAYER_STYLE_TARGETS[key];
    return t ? [...(t.fill || []), ...(t.line || [])] : [];
};

/** Max zoom used when a manifest `border_minzoom` sets a layer's zoom range. */
const MAX_STYLE_ZOOM = 24;

/**
 * Push manifest `style` knobs onto the live map, on top of the base style-layer
 * definitions above. Only the flat keys present in each entry are written, so a
 * layer keeps its coded default (including data-driven expressions) for anything
 * the manifest omits. Safe to call once the style is loaded.
 *
 * `border_minzoom` maps to the line layer's zoom range (setLayerZoomRange),
 * reproducing the coded ADMIN_BORDER_MINZOOM fade-in from the manifest instead.
 */
export const applyManifestStyle = (map: MapLibreMap, manifest: LayerManifest): void => {
    for (const [key, cfg] of Object.entries(manifest)) {
        const style = cfg?.style;
        const targets = LAYER_STYLE_TARGETS[key];
        if (!style || !targets) continue;

        for (const id of targets.fill || []) {
            if (!map.getLayer(id)) continue;
            if (style.fill_color !== undefined) map.setPaintProperty(id, 'fill-color', style.fill_color);
            if (style.fill_opacity !== undefined) map.setPaintProperty(id, 'fill-opacity', style.fill_opacity);
        }
        for (const id of targets.line || []) {
            if (!map.getLayer(id)) continue;
            if (style.line_color !== undefined) map.setPaintProperty(id, 'line-color', style.line_color);
            if (style.line_opacity !== undefined) map.setPaintProperty(id, 'line-opacity', style.line_opacity);
            if (style.line_width !== undefined) map.setPaintProperty(id, 'line-width', style.line_width);
            if (style.line_dash !== undefined) map.setPaintProperty(id, 'line-dasharray', style.line_dash);
            if (style.border_minzoom !== undefined) map.setLayerZoomRange(id, style.border_minzoom, MAX_STYLE_ZOOM);
        }
    }
};

/**
 * Push user overrides (localStorage) onto the live style — the highest layer of
 * precedence, applied immediately AFTER applyManifestStyle so user edits win over
 * manifest defaults. Only properties present in each override are written; absent
 * ones keep whatever applyManifestStyle/code left in place.
 *
 * A color override on a data-driven-colour layer (e.g. eco_reserves, land_access,
 * regions) replaces its match/interpolate expression with the chosen flat colour.
 * That is intentional and reversible: clearing the override and re-running
 * applyManifestStyle (plus a fresh style spec) restores the coded default — see
 * the reset path in Map.tsx.
 *
 * `visible` is NOT applied here — layer visibility flows through the existing
 * layerToggles path in Map.tsx, seeded from the override.
 */
export const applyUserOverrides = (map: MapLibreMap, overrides: LayerOverrides): void => {
    for (const [key, ov] of Object.entries(overrides)) {
        const targets = LAYER_STYLE_TARGETS[key];
        if (!targets || !ov) continue;
        for (const id of targets.fill || []) {
            if (!map.getLayer(id)) continue;
            if (ov.fill_color !== undefined) map.setPaintProperty(id, 'fill-color', ov.fill_color);
            if (ov.fill_opacity !== undefined) map.setPaintProperty(id, 'fill-opacity', ov.fill_opacity);
        }
        for (const id of targets.line || []) {
            if (!map.getLayer(id)) continue;
            if (ov.line_color !== undefined) map.setPaintProperty(id, 'line-color', ov.line_color);
            if (ov.line_opacity !== undefined) map.setPaintProperty(id, 'line-opacity', ov.line_opacity);
        }
    }
};

// Feature base colors
const FEATURE_COLORS = {
    streams: '#4A90E2',    // Blue for streams
    lakes: '#64B5F6',      // Light blue for lakes
    wetlands: '#81C784',   // Green for wetlands
    manmade: '#9575CD',    // Purple for manmade waterbodies
    ungazetted: '#F5A623', // Amber for ungazetted waterbodies
};

/**
 * Per-type highlight colors for hover state.
 * Each is a saturated, darker variant of its base FEATURE_COLOR so the
 * highlight feels native to the feature type rather than generic.
 */
export const HIGHLIGHT_COLORS: Record<string, string> = {
    stream:   '#7C3AED',  // Vibrant purple — high contrast against blue water
    streams:  '#7C3AED',
    lake:     '#7C3AED',
    lakes:    '#7C3AED',
    wetland:  '#7C3AED',
    wetlands: '#7C3AED',
    manmade:  '#7C3AED',
    ungazetted: '#7C3AED',
};

/** Uniform color for the active-selection state (same for all types). */
export const SELECTION_COLOR = '#7C3AED'; // Vibrant purple — matches highlight

// Admin area fill colors — colorblind-safe palette
// Crimson (#C22E2E) = NO FISHING — universal "prohibited" (NP + Eco Reserves)
// Amber (#CC7A00) = PARTIAL RESTRICTION — "caution" (research forests, etc.)
// Green (#009E73) = OPEN — fishing allowed (provincial parks)
const ADMIN_COLORS: Record<string, string> = {
    // ── NO FISHING zones (crimson = prohibited) ─────────────────────
    admin_parks_nat: '#C22E2E',        // Crimson — national parks (federal closure)
    ECOLOGICAL_RESERVE: '#C22E2E',     // Crimson — eco reserves (provincial closure)
    // ── BC Parks sub-types ───────────────────────────────────────────
    PROVINCIAL_PARK: '#009E73',        // Wong bluish-green — still open
    PROTECTED_AREA: '#0072B2',         // Wong blue
    RECREATION_AREA: '#6B8E6B',        // Muted sage-green — no regs (subtle like provincial parks)
    admin_parks_bc_default: '#009E73', // Fallback: same as provincial park
    // ── Other admin types ────────────────────────────────────────────
    admin_wma: '#7B2D8B',             // Purple — wildlife mgmt areas
    admin_watersheds: '#006D77',       // Deep teal — watersheds
    admin_historic_sites: '#795548',   // Warm brown — heritage sites
    // ── Land access (watersheds, private/restricted land, DND, etc.) ────
    // Two-tier severity, not one flat color: red = no public access at all
    // (private/no/military); amber = conditional/permit-based access (e.g.
    // Malcolm Knapp Research Forest) — same "caution, not prohibited"
    // language the old osm_admin amber used before it merged into this layer.
    land_access_closed: '#DC2626',     // Red — no public access
    land_access_restricted: '#CC7A00', // Amber — conditional/permit-based access
    // ── Indigenous / Aboriginal lands ────────────────────────────────
    aboriginal_lands: '#8B6508',       // Dark goldenrod — OSM-style tan/ochre
    // ── Land ownership (toggled via the layer menu) ─────────────────────
    // Deliberately outside the red/amber restriction language — this is a
    // neutral "who owns this" informational layer, not a restriction.
    // Two distinct colors so Crown/Public and Private read as different
    // categories at a glance, not one undifferentiated blob.
    land_parcels_crown: '#546E7A',      // Slate blue-grey — Crown (Provincial/Agency/Federal/Untitled Provincial/First Nations/Mixed/Unclassified)
    land_parcels_public: '#4A6FA5',     // Denim blue — Public/Local Government, distinct from Crown's slate-grey
    land_parcels_private: '#8D6E63',    // Warm brown — Private, distinct from both crown-blue and the red/amber restriction colors
};

/** Data-driven land_access color: red for "closed", amber for "restricted".
 *  Shared between createRegulationLayers() (fill/line) and
 *  createAdminLabelLayers() (label text-color). */
const LAND_ACCESS_COLOR_EXPR = [
    'match', ['get', 'restriction_level'],
    'closed', ADMIN_COLORS.land_access_closed,
    'restricted', ADMIN_COLORS.land_access_restricted,
    ADMIN_COLORS.land_access_closed,
] as unknown as string;

// Admin overlay polygon borders (national/BC parks, eco reserves, WMAs,
// watersheds, historic sites, land access / Malcolm Knapp, land ownership,
// Indigenous lands). Below this zoom the polygons render as fill (bg) only
// — no border; the border fades in uniformly at this single zoom across
// every overlay so they all appear at the same time, at a deliberately low
// opacity so borders don't dominate once visible. This only changes HOW a
// polygon is styled when it's already visible — the per-layer logic that
// decides WHETHER a polygon shows at all (admin_visibility.json /
// layer-menu toggles) is untouched.
const ADMIN_BORDER_MINZOOM = 11;
const ADMIN_BORDER_OPACITY = 0.35;

// Land-ownership fills (Crown/Public/Private) have no hatch pattern to lean
// on — unlike land_access's closed/restricted overlays, which stay legible
// at a low 0.14 fill-opacity because the diagonal hatch carries the visual
// weight. Without that, 0.12 measured as essentially invisible against the
// green forest basemap in testing (data was rendering correctly — it just
// couldn't be seen). Bumped up, plus a thicker border, so ownership is
// actually perceptible once toggled on.
const LAND_OWNERSHIP_FILL_OPACITY = 0.22;
const LAND_OWNERSHIP_LINE_WIDTH = 1.5;

// Helper function to create regulation layers from new PMTiles structure
// Waterbody name labels (streams/lakes/wetlands/manmade/FSRs) are returned
// separately from everything else so Map.tsx can insert them into the
// style AFTER createAdminLabelLayers() (park/watershed/land-use/aboriginal
// names) — MapLibre's symbol collision priority is decided by array order,
// later layers win, so keeping them bundled together with the fills meant
// waterbody names were losing collisions to park/land-use labels, which
// get added later. See Map.tsx for how the two are combined.
export const createRegulationLayers = (): { layers: LayerSpecification[]; labels: LayerSpecification[] } => {
    // Render order (bottom → top):
    //   1. fwaLayers   — waterbody fills, lines, geometry
    //   2. adminLayers — admin polygon fills, borders, query layers
    //   3. fwaLabels   — waterbody name labels (above admin polygons)
    const fwaLayers: LayerSpecification[] = [];
    const fwaLabels: LayerSpecification[] = [];
    const adminLayers: LayerSpecification[] = [];
    // ── BC MASK (grey area outside zone polygons) ─────────────────────────
    // Renders first (bottom) so all BC content appears above it
    fwaLayers.push({
        id: 'bc-mask',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'bc_mask',
        paint: {
            'fill-color': '#374151', // Tailwind gray-700
            'fill-opacity': 0.4
        }
    });
    // ── FWA FEATURE LAYERS (bottom of stack) ─────────────────────────

    // Wetlands — lowest of the FWA features
    fwaLayers.push({
        id: 'wetlands-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'wetlands',
        paint: {
            // fill-pattern is set dynamically on map 'load' after the image is registered
            'fill-color': '#81C784',
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
    
    // Lake outline width based on area (sqrt scale, clamped)
    // Small lakes (~10,000 m²) get thin outlines, large lakes (~100M m²) get thicker
    // sqrt(10000) = 100, sqrt(100M) = 10000
    // We normalize and clamp to get line widths that mesh well with streams
    // MapLibre doesn't have clamp, so we use max(min(val, max), min)
    const lakeLineWidth = [
        'interpolate',
        ['linear'],
        ['zoom'],
        4, [
            'max',
            ['min', ['+', 0.5, ['*', 0.00005, ['sqrt', ['coalesce', ['get', 'area'], 10000]]]], 1.5],
            0.5
        ],
        8, [
            'max',
            ['min', ['+', 0.6, ['*', 0.00008, ['sqrt', ['coalesce', ['get', 'area'], 10000]]]], 2],
            0.6
        ],
        12, [
            'max',
            ['min', ['+', 0.8, ['*', 0.0002, ['sqrt', ['coalesce', ['get', 'area'], 10000]]]], 3],
            0.8
        ],
        16, [
            'max',
            ['min', ['+', 1, ['*', 0.0003, ['sqrt', ['coalesce', ['get', 'area'], 10000]]]], 4],
            1
        ]
    ];

    fwaLayers.push({
        id: 'lakes-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'lakes',
        paint: {
            'line-color': FEATURE_COLORS.streams,  // Dark blue outline (same as streams)
            'line-width': lakeLineWidth as any,
            'line-opacity': 0.8
        },
        layout: {
            'line-cap': 'round',
            'line-join': 'round'
        }
    });

    // Under-lake streams — subtle dashed construction lines visible through lake fills
    fwaLayers.push({
        id: 'under-lake-streams',
        type: 'line',
        source: 'regulations',
        'source-layer': 'under_lake_streams',
        minzoom: 10,
        paint: {
            'line-color': '#3A7BD5',
            'line-width': [
                'interpolate', ['linear'], ['zoom'],
                10, 0.5,
                13, 1,
                16, 1.5,
            ],
            'line-opacity': 0.35,
            'line-dasharray': [4, 3],
        },
        layout: {
            'line-cap': 'butt',
            'line-join': 'round',
        },
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

    // Forest Service Roads — BC backcountry road network (DataBC), solid
    // when active, dotted when retired/deactivated so users can tell a
    // gated/decommissioned road from a driveable one at a glance.
    // Hidden by default for now (OSM's own road coverage reads better for
    // this) — kept wired, not deleted, so it's a one-line flip to bring
    // back or offer as a toggle later.
    fwaLayers.push({
        id: 'forest_service_roads',
        type: 'line',
        source: 'regulations',
        'source-layer': 'forest_service_roads',
        minzoom: 9,
        paint: {
            'line-color': '#8B6D3F',
            'line-width': ['interpolate', ['linear'], ['zoom'], 9, 0.6, 12, 1, 15, 1.8],
            'line-opacity': 0.75,
            'line-dasharray': ['match', ['get', 'status'], 'RETIRED', ['literal', [1, 2]], ['literal', [1, 0]]],
        },
        layout: {
            visibility: 'none',
            'line-cap': 'round',
            'line-join': 'round',
        },
    });

    // Ungazetted waterbodies — rendered via GeoJSON source in Map.tsx
    // (populated from tier0.json search index after data load).

    // ── WATERBODY NAME LABELS ────────────────────────────────────────
    // (Moved below region/MU boundary lines so text is not obscured.)

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
                7, 3,
                9, 4,
                12, 4.5
            ],
            'line-opacity': 0.8
        },
        layout: {
            'line-cap': 'round',
            'line-join': 'round'
        }
    });

    // Management Units (individual WMU boundaries — dotted)
    fwaLayers.push({
        id: 'management_units',
        type: 'line',
        source: 'regulations',
        'source-layer': 'wmu_boundary',
        minzoom: 7,
        paint: {
            'line-color': '#555555',
            'line-width': [
                'interpolate',
                ['linear'],
                ['zoom'],
                7, 1.2,
                9, 1.5,
                12, 1.8
            ],
            'line-opacity': [
                'interpolate', ['linear'], ['zoom'],
                7, 0.55,
                8, 0.6,
                9, 0.6,
                11, 0.65,
            ],
            'line-dasharray': [2, 3]
        },
        layout: {
            'line-cap': 'round',
            'line-join': 'round'
        }
    });

    // ── WATERBODY NAME LABELS (above boundary lines for readability) ─
    // Replace OSM water labels with our own using display_name.
    // Skip unnamed waterbodies (display_name == '').

    // Stream labels — follow line geometry like OSM river labels
    fwaLabels.push({
        id: 'streams-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'streams',
        minzoom: 11,
        filter: ['!=', ['get', 'display_name'], ''],
        layout: {
            'symbol-placement': 'line',
            'text-field': ['get', 'display_name'],
            'text-font': ['Noto Sans Italic'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 11, 11, 14, 14],
            'text-letter-spacing': 0.12,
            'text-max-angle': 25,
            'symbol-spacing': 300,
            'text-allow-overlap': false,
            'text-padding': 6,
        },
        paint: {
            'text-color': '#0D47A1',
            'text-halo-color': '#FFFFFF',
            'text-halo-width': 2,
            'text-halo-blur': 0.5,
        },
    });

    // Forest Service Road labels — follow line geometry like stream labels
    fwaLabels.push({
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
            'text-letter-spacing': 0.08,
            'text-max-angle': 30,
            'symbol-spacing': 300,
            'text-allow-overlap': false,
            'text-padding': 6,
        },
        paint: {
            'text-color': '#8B6D3F',
            'text-halo-color': '#FFFFFF',
            'text-halo-width': 1.2,
        },
    });

    // Under-lake stream labels — subtle names following the dashed lines
    fwaLabels.push({
        id: 'under-lake-streams-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'under_lake_streams',
        minzoom: 12,
        filter: ['!=', ['get', 'display_name'], ''],
        layout: {
            'symbol-placement': 'line',
            'text-field': ['get', 'display_name'],
            'text-font': ['Noto Sans Italic'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 12, 9, 14, 11],
            'text-letter-spacing': 0.1,
            'text-max-angle': 25,
            'symbol-spacing': 400,
            'text-allow-overlap': false,
            'text-padding': 8,
        },
        paint: {
            'text-color': '#5C85B2',
            'text-halo-color': '#FFFFFF',
            'text-halo-width': 1.5,
            'text-halo-blur': 0.5,
            'text-opacity': 0.7,
        },
    });

    // Lake labels — large lakes visible earlier, small lakes appear when zoomed in
    // area thresholds (m²): >5 km² @ z8, >1 km² @ z9, >0.1 km² @ z10, all @ z11
    fwaLabels.push({
        id: 'lakes-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'lakes',
        minzoom: 8,
        filter: ['all',
            ['!=', ['get', 'display_name'], ''],
            ['any',
                ['all', ['>=', ['zoom'], 11]],
                ['all', ['>=', ['zoom'], 10], ['>=', ['get', 'area'], 100000]],
                ['all', ['>=', ['zoom'], 9],  ['>=', ['get', 'area'], 1000000]],
                ['all', ['>=', ['zoom'], 8],  ['>=', ['get', 'area'], 5000000]],
            ],
        ],
        layout: {
            'symbol-placement': 'point',
            // Lower sort key wins placement/collision priority — negative
            // area means bigger lakes are placed (and win collisions)
            // before smaller ones, so a cluster of small lakes can no
            // longer crowd out a big, expected lake's label.
            'symbol-sort-key': ['-', 0, ['get', 'area']],
            'text-field': ['get', 'display_name'],
            'text-font': ['Noto Sans Italic'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 8, 10, 14, 14],
            'text-letter-spacing': 0.1,
            'text-max-width': 9,
            'text-allow-overlap': false,
            'text-padding': 3,
        },
        paint: {
            'text-color': '#1565C0',
            'text-halo-color': '#FFFFFF',
            'text-halo-width': 1.2,
        },
    });

    // Wetland labels
    fwaLabels.push({
        id: 'wetlands-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'wetlands',
        minzoom: 11,
        filter: ['!=', ['get', 'display_name'], ''],
        layout: {
            'symbol-placement': 'point',
            'symbol-sort-key': ['-', 0, ['get', 'area']],
            'text-field': ['get', 'display_name'],
            'text-font': ['Noto Sans Italic'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 11, 10, 14, 12],
            'text-letter-spacing': 0.1,
            'text-max-width': 8,
            'text-allow-overlap': false,
            'text-padding': 4,
        },
        paint: {
            'text-color': '#2E7D32',
            'text-halo-color': '#FFFFFF',
            'text-halo-width': 1.2,
        },
    });

    // Manmade waterbody labels
    fwaLabels.push({
        id: 'manmade-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'manmade',
        minzoom: 10,
        filter: ['!=', ['get', 'display_name'], ''],
        layout: {
            'symbol-placement': 'point',
            'symbol-sort-key': ['-', 0, ['get', 'area']],
            'text-field': ['get', 'display_name'],
            'text-font': ['Noto Sans Italic'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 10, 10, 14, 13],
            'text-letter-spacing': 0.1,
            'text-max-width': 9,
            'text-allow-overlap': false,
            'text-padding': 4,
        },
        paint: {
            'text-color': '#6A1B9A',
            'text-halo-color': '#FFFFFF',
            'text-halo-width': 1.2,
        },
    });

    // ── TIDAL BOUNDARY (low-opacity grey overlay — DFO jurisdiction) ────
    adminLayers.push({
        id: 'tidal_boundary-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'tidal_boundary',
        paint: {
            'fill-color': '#94A3B8',  // Tailwind slate-400
            'fill-opacity': ['interpolate', ['linear'], ['zoom'], 6, 0.06, 10, 0.12],
        },
    });
    adminLayers.push({
        id: 'tidal_boundary-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'tidal_boundary',
        minzoom: 9,
        paint: {
            'line-color': '#6B7280',
            'line-width': 2,
            'line-opacity': ['interpolate', ['linear'], ['zoom'], 9, 0, 10, 0.5],
            'line-dasharray': [4, 3],
        },
    });
    adminLayers.push({
        id: 'tidal_boundary-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'tidal_boundary',
        minzoom: 8,
        layout: {
            'symbol-placement': 'point',
            'text-field': 'Tidal Waters',
            'text-font': ['Noto Sans Bold'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 8, 10, 12, 14],
            'text-allow-overlap': false,
            'text-padding': 6,
        },
        paint: {
            'text-color': '#4B5563',
            'text-halo-color': '#FFFFFF',
            'text-halo-width': 1.5,
        },
    });

    // ── ADMIN BOUNDARY LAYERS (rendered above FWA — fills + borders on top) ──────
    // Hatch pattern overlays for no-fishing zones are added dynamically in
    // Map.tsx on 'load' (after pattern images are registered) and appended
    // above these layers.

    // Invisible hit-test layer for the WMU polygon fill (the visible dotted
    // boundary is 'management_units', a *line* layer over 'wmu_boundary' —
    // lines aren't reliably hit-testable by an interior click). Used only by
    // Map.tsx's click handler to look up "which management unit was this
    // click inside" for the disambiguation menu header; never painted.
    adminLayers.push({
        id: 'mu-hit-test',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'wmu',
        paint: { 'fill-opacity': 0 },
    });

    // National parks — crimson tint + thick solid border (NO FISHING)
    adminLayers.push({
        id: 'admin_parks_nat-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'parks_nat',
        paint: {
            'fill-color': ADMIN_COLORS.admin_parks_nat,
            'fill-opacity': 0.10,
        },
    });
    adminLayers.push({
        id: 'admin_parks_nat-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'parks_nat',
        minzoom: ADMIN_BORDER_MINZOOM,
        paint: {
            'line-color': ADMIN_COLORS.admin_parks_nat,
            'line-width': 3.0,
            'line-opacity': ADMIN_BORDER_OPACITY,
        },
    });

    // Ecological reserves — crimson tint + border (NO FISHING)
    adminLayers.push({
        id: 'eco_reserves-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'eco_reserves',
        filter: ['==', ['get', 'admin_type'], 'ECOLOGICAL_RESERVE'],
        paint: {
            'fill-color': ADMIN_COLORS.ECOLOGICAL_RESERVE,
            'fill-opacity': 0.10,
        },
    });
    adminLayers.push({
        id: 'eco_reserves-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'eco_reserves',
        filter: ['==', ['get', 'admin_type'], 'ECOLOGICAL_RESERVE'],
        minzoom: ADMIN_BORDER_MINZOOM,
        paint: {
            'line-color': ADMIN_COLORS.ECOLOGICAL_RESERVE,
            'line-width': 2.5,
            'line-opacity': ADMIN_BORDER_OPACITY,
        },
    });

    // BC Parks — colour keyed by admin_type; eco reserves share crimson no-fishing signal.
    // Tile layer is 'eco_reserves' (parks_bc data loaded into eco_reserves in atlas).
    adminLayers.push({
        id: 'admin_parks_bc-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'eco_reserves',
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
            'fill-opacity': [
                'match',
                ['get', 'admin_type'],
                'ECOLOGICAL_RESERVE', 0.10,
                0.12,
            ],
        },
    });
    adminLayers.push({
        id: 'admin_parks_bc-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'eco_reserves',
        filter: ['!', ['in', ['get', 'admin_type'], ['literal', ['ECOLOGICAL_RESERVE', 'PROVINCIAL_PARK', 'RECREATION_AREA', 'PROTECTED_AREA']]]],
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
            'line-width': 1.5,
            'line-opacity': ADMIN_BORDER_OPACITY,
        },
        minzoom: ADMIN_BORDER_MINZOOM,
    });
    adminLayers.push({
        id: 'admin_parks_bc-eco-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'eco_reserves',
        filter: ['==', ['get', 'admin_type'], 'ECOLOGICAL_RESERVE'],
        minzoom: ADMIN_BORDER_MINZOOM,
        paint: {
            'line-color': ADMIN_COLORS.ECOLOGICAL_RESERVE,
            'line-width': 2.5,
            'line-opacity': ADMIN_BORDER_OPACITY,
        },
    });

    // Wildlife Management Areas (tiles filtered to regulated features only)
    adminLayers.push({
        id: 'admin_wma-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'wma',
        layout: { visibility: 'none' },
        paint: { 'fill-color': ADMIN_COLORS.admin_wma, 'fill-opacity': 0.12 },
    });
    adminLayers.push({
        id: 'admin_wma-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'wma',
        minzoom: ADMIN_BORDER_MINZOOM,
        layout: { visibility: 'none' },
        paint: { 'line-color': ADMIN_COLORS.admin_wma, 'line-width': 1.5, 'line-opacity': ADMIN_BORDER_OPACITY },
    });

    // Watersheds (tiles filtered to regulated features only)
    adminLayers.push({
        id: 'admin_watersheds-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'watersheds',
        layout: { visibility: 'none' },
        paint: { 'fill-color': ADMIN_COLORS.admin_watersheds, 'fill-opacity': 0.1 },
    });
    adminLayers.push({
        id: 'admin_watersheds-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'watersheds',
        minzoom: ADMIN_BORDER_MINZOOM,
        layout: { visibility: 'none' },
        paint: { 'line-color': ADMIN_COLORS.admin_watersheds, 'line-width': 1.5, 'line-opacity': ADMIN_BORDER_OPACITY },
    });

    // Historic Sites (tiles filtered to regulated features only)
    adminLayers.push({
        id: 'admin_historic_sites-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'historic_sites',
        layout: { visibility: 'none' },
        paint: { 'fill-color': ADMIN_COLORS.admin_historic_sites, 'fill-opacity': 0.15 },
    });
    adminLayers.push({
        id: 'admin_historic_sites-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'historic_sites',
        minzoom: ADMIN_BORDER_MINZOOM,
        layout: { visibility: 'none' },
        paint: { 'line-color': ADMIN_COLORS.admin_historic_sites, 'line-width': 1.5, 'line-opacity': ADMIN_BORDER_OPACITY },
    });

    // Land Access (watersheds, private/restricted land, DND land, research
    // forests, etc.) — fill/border color keyed on restriction_level (red =
    // closed, amber = restricted/conditional); the matching hatch overlay
    // layers (colorblind-friendly, pattern differs by severity too, not
    // just hue) are added separately in Map.tsx alongside the other
    // hatch-pattern overlays (parks_nat/eco_reserves).
    adminLayers.push({
        id: 'admin_land_access-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'land_access',
        layout: { visibility: 'none' },
        paint: { 'fill-color': LAND_ACCESS_COLOR_EXPR, 'fill-opacity': 0.14 },
    });
    adminLayers.push({
        id: 'admin_land_access-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'land_access',
        minzoom: ADMIN_BORDER_MINZOOM,
        layout: { visibility: 'none' },
        paint: { 'line-color': LAND_ACCESS_COLOR_EXPR, 'line-width': 1.5, 'line-opacity': ADMIN_BORDER_OPACITY },
    });

    // Land ownership — BC parcel-level, dissolved to one polygon per
    // OWNER_TYPE (Private/Crown Provincial/Agency/Federal/First Nations/
    // Local Government/Mixed/Unclassified/Untitled Provincial), all in a
    // single `land_parcels_crown` tile source-layer distinguished by the
    // `name` property. Split into three independently-colored style layers
    // via filters on that property — Crown (Provincial/Agency/Federal/
    // Untitled Provincial/First Nations/Mixed/Unclassified), Public (Local
    // Government — municipal/civic land, a genuinely different category
    // from provincial/federal Crown land), and Private each read as
    // distinct categories. Crown and Public share the single "Crown /
    // Public Land" layer-menu toggle (both flip together) but render in
    // different colors; Private has its own separate toggle. Toggled via
    // the layer menu, not the admin-visibility system (no regulation
    // drives visibility here), so all default hidden and the menu's
    // checkboxes flip visibility directly.
    const IS_PRIVATE_FILTER = ['==', ['get', 'name'], 'Private'] as unknown as FilterSpecification;
    const IS_LOCAL_GOV_FILTER = ['==', ['get', 'name'], 'Local Government'] as unknown as FilterSpecification;
    const IS_CROWN_FILTER = ['!', ['in', ['get', 'name'], ['literal', ['Private', 'Local Government']]]] as unknown as FilterSpecification;
    adminLayers.push({
        id: 'admin_land_parcels_crown-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'land_parcels_crown',
        filter: IS_CROWN_FILTER,
        layout: { visibility: 'none' },
        paint: { 'fill-color': ADMIN_COLORS.land_parcels_crown, 'fill-opacity': LAND_OWNERSHIP_FILL_OPACITY },
    });
    adminLayers.push({
        id: 'admin_land_parcels_crown-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'land_parcels_crown',
        filter: IS_CROWN_FILTER,
        minzoom: ADMIN_BORDER_MINZOOM,
        layout: { visibility: 'none' },
        paint: { 'line-color': ADMIN_COLORS.land_parcels_crown, 'line-width': LAND_OWNERSHIP_LINE_WIDTH, 'line-opacity': ADMIN_BORDER_OPACITY },
    });
    adminLayers.push({
        id: 'admin_land_parcels_public-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'land_parcels_crown',
        filter: IS_LOCAL_GOV_FILTER,
        layout: { visibility: 'none' },
        paint: { 'fill-color': ADMIN_COLORS.land_parcels_public, 'fill-opacity': LAND_OWNERSHIP_FILL_OPACITY },
    });
    adminLayers.push({
        id: 'admin_land_parcels_public-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'land_parcels_crown',
        filter: IS_LOCAL_GOV_FILTER,
        minzoom: ADMIN_BORDER_MINZOOM,
        layout: { visibility: 'none' },
        paint: { 'line-color': ADMIN_COLORS.land_parcels_public, 'line-width': LAND_OWNERSHIP_LINE_WIDTH, 'line-opacity': ADMIN_BORDER_OPACITY },
    });
    adminLayers.push({
        id: 'admin_land_parcels_private-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'land_parcels_crown',
        filter: IS_PRIVATE_FILTER,
        layout: { visibility: 'none' },
        paint: { 'fill-color': ADMIN_COLORS.land_parcels_private, 'fill-opacity': LAND_OWNERSHIP_FILL_OPACITY },
    });
    adminLayers.push({
        id: 'admin_land_parcels_private-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'land_parcels_crown',
        filter: IS_PRIVATE_FILTER,
        minzoom: ADMIN_BORDER_MINZOOM,
        layout: { visibility: 'none' },
        paint: { 'line-color': ADMIN_COLORS.land_parcels_private, 'line-width': LAND_OWNERSHIP_LINE_WIDTH, 'line-opacity': ADMIN_BORDER_OPACITY },
    });

    // Aboriginal / Indigenous Lands
    adminLayers.push({
        id: 'admin_aboriginal_lands-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'aboriginal_lands',
        layout: { visibility: 'none' },
        paint: { 'fill-color': ADMIN_COLORS.aboriginal_lands, 'fill-opacity': 0.10 },
    });
    adminLayers.push({
        id: 'admin_aboriginal_lands-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'aboriginal_lands',
        minzoom: ADMIN_BORDER_MINZOOM,
        layout: { visibility: 'none' },
        paint: {
            'line-color': ADMIN_COLORS.aboriginal_lands,
            'line-width': ['interpolate', ['linear'], ['zoom'], ADMIN_BORDER_MINZOOM, 2, 14, 4],
            'line-opacity': ADMIN_BORDER_OPACITY,
        },
    });

    // FWA geometry → admin polygons → FWA labels on top
    return { layers: [...fwaLayers, ...adminLayers], labels: fwaLabels };
};

/**
 * Trail/track/backcountry-road ("kind=other/path") line layer, z11 through
 * max zoom. Owns the FULL zoom range rather than handing off to the
 * built-in protomaps `roads_other` layer at z15 — that native layer's own
 * width curve is `interpolate(exponential 1.6, zoom, 14, 0, 20, 7)`, i.e.
 * literally 0px at z14 and still under 1px through z16, which is why
 * trails became "extremely hard to see" past z15 (the custom overlay
 * handed off right at the zoom where the native layer is at its most
 * invisible). The native `roads_other` layer is filtered out of the
 * basemap entirely (see BROWN_ROAD_THEME usage in Map.tsx) so there's no
 * double-render — this layer is now the only thing drawing that tier, at
 * every zoom.
 * Labels still hand off to protomaps' `roads_labels_minor` at z15 — that
 * one uses a fixed `text-size: 12` with no zoom-based width-style curve,
 * so it was never actually broken.
 */
export const createEarlyRoadLayers = (): LayerSpecification[] => [
    // ── Road lines (z11 → max zoom, no handoff) ──────────────────────
    {
        id: 'roads_other_early',
        type: 'line',
        source: 'protomaps',
        'source-layer': 'roads',
        minzoom: 11,
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
                17, 2.2,
                20, 3.2,
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
 * Labels use short type prefixes (NP, ER, PP, WMA, etc.) to keep the map
 * clean, with the full name appended at higher zoom levels.
 * Labels appear 1-2 zoom levels after the polygon fill becomes visible.
 * Font matches the default basemap (Noto Sans Regular), no halo/outline.
 */
export const createAdminLabelLayers = (): LayerSpecification[] => {
    const labelLayers: LayerSpecification[] = [];

    // ── National Parks ───────────────────────────────────────────────
    labelLayers.push({
        id: 'admin_parks_nat-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'parks_nat',
        minzoom: 6,
        layout: {
            'symbol-placement': 'point',
            'text-field': [
                'step', ['zoom'],
                ['concat', 'NP ', ['get', 'display_name']],
                10, ['get', 'display_name'],
            ],
            'text-font': ['Noto Sans Regular'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 6, 10, 10, 13, 12, 14],
            'text-max-width': 8,
            'text-anchor': 'center',
            'text-allow-overlap': false,
            'text-ignore-placement': false,
            'text-padding': ['interpolate', ['linear'], ['zoom'], 6, 50, 10, 12, 14, 4],
        },
        paint: {
            'text-color': '#7F1D1D',
            'text-halo-color': '#ffffff',
            'text-halo-width': 1.2,
        },
    });

    // ── Ecological Reserves (new tiles: eco_reserves layer) ──────────
    labelLayers.push({
        id: 'eco_reserves-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'eco_reserves',
        minzoom: 9,
        layout: {
            'symbol-placement': 'point',
            'text-field': ['get', 'display_name'],
            'text-font': ['Noto Sans Regular'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 9, 9, 11, 11, 13, 13],
            'text-max-width': 8,
            'text-anchor': 'center',
            'text-allow-overlap': false,
            'text-padding': ['interpolate', ['linear'], ['zoom'], 9, 50, 11, 12, 14, 4],
        },
        paint: {
            'text-color': '#7F1D1D',
            'text-halo-color': '#ffffff',
            'text-halo-width': 1.0,
        },
    });

    // ── BC Parks: Provincial Parks ───────────────────────────────────
    labelLayers.push({
        id: 'admin_parks_bc-prov-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'eco_reserves',
        filter: ['==', ['get', 'admin_type'], 'PROVINCIAL_PARK'],
        minzoom: 8,
        layout: {
            'symbol-placement': 'point',
            'text-field': ['get', 'name'],
            'text-font': ['Noto Sans Regular'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 6, 9, 10, 12, 12, 13],
            'text-max-width': 8,
            'text-anchor': 'center',
            'text-allow-overlap': false,
            'text-padding': ['interpolate', ['linear'], ['zoom'], 6, 50, 10, 12, 14, 4],
        },
        paint: {
            'text-color': '#007A5E',
            'text-halo-color': '#ffffff',
            'text-halo-width': 0.8,
        },
    });

    // ── BC Parks: Protected Areas ────────────────────────────────────
    labelLayers.push({
        id: 'admin_parks_bc-prot-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'eco_reserves',
        filter: ['==', ['get', 'admin_type'], 'PROTECTED_AREA'],
        minzoom: 6,
        layout: {
            'symbol-placement': 'point',
            'text-field': [
                'step', ['zoom'],
                ['concat', 'PA ', ['get', 'name']],
                10, ['get', 'name'],
            ],
            'text-font': ['Noto Sans Regular'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 6, 9, 10, 12, 12, 13],
            'text-max-width': 8,
            'text-anchor': 'center',
            'text-allow-overlap': false,
            'text-padding': ['interpolate', ['linear'], ['zoom'], 6, 50, 10, 12, 14, 4],
        },
        paint: {
            'text-color': '#005A8C',
            'text-halo-color': '#ffffff',
            'text-halo-width': 0.8,
        },
    });

    // ── BC Parks: Recreation Areas ───────────────────────────────────
    labelLayers.push({
        id: 'admin_parks_bc-rec-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'eco_reserves',
        filter: ['==', ['get', 'admin_type'], 'RECREATION_AREA'],
        minzoom: 7,
        layout: {
            'symbol-placement': 'point',
            'text-field': [
                'step', ['zoom'],
                ['concat', 'RA ', ['get', 'name']],
                10, ['get', 'name'],
            ],
            'text-font': ['Noto Sans Regular'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 7, 9, 10, 11, 12, 13],
            'text-max-width': 8,
            'text-anchor': 'center',
            'text-allow-overlap': false,
            'text-padding': ['interpolate', ['linear'], ['zoom'], 7, 50, 10, 12, 14, 4],
        },
        paint: {
            'text-color': '#6B5200',
            'text-halo-color': '#ffffff',
            'text-halo-width': 0.8,
        },
    });

    // ── Wildlife Management Areas ────────────────────────────────────
    labelLayers.push({
        id: 'admin_wma-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'wma',
        minzoom: 7,
        layout: {
            'symbol-placement': 'point',
            'text-field': [
                'step', ['zoom'],
                ['concat', 'WMA ', ['get', 'name']],
                10, ['get', 'name'],
            ],
            'text-font': ['Noto Sans Regular'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 7, 9, 10, 12, 12, 13],
            'text-max-width': 8,
            'text-anchor': 'center',
            'text-allow-overlap': false,
            'text-padding': ['interpolate', ['linear'], ['zoom'], 7, 50, 10, 12, 14, 4],
        },
        paint: {
            'text-color': '#5E1D6B',
            'text-halo-color': '#ffffff',
            'text-halo-width': 0.8,
        },
    });

    // ── Land Access (watersheds, private/restricted land, DND, etc.) ──
    // Names withheld until z11 — at lower zooms the polygon fill/hatch is
    // still visible, but the name label just adds clutter before the user
    // is zoomed in enough to actually be near it.
    labelLayers.push({
        id: 'admin_land_access-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'land_access',
        minzoom: 11,
        layout: {
            visibility: 'none',
            'symbol-placement': 'point',
            'text-field': ['get', 'name'],
            'text-font': ['Noto Sans Regular'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 7, 9, 10, 12, 12, 13],
            'text-max-width': 8,
            'text-anchor': 'center',
            'text-allow-overlap': false,
            'text-padding': ['interpolate', ['linear'], ['zoom'], 7, 50, 10, 12, 14, 4],
        },
        paint: {
            'text-color': LAND_ACCESS_COLOR_EXPR,
            'text-halo-color': '#ffffff',
            'text-halo-width': 0.8,
        },
    });

    // ── Watersheds ───────────────────────────────────────────────────
    labelLayers.push({
        id: 'admin_watersheds-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'watersheds',
        minzoom: 6,
        layout: {
            visibility: 'none',
            'symbol-placement': 'point',
            'text-field': ['get', 'name'],
            'text-font': ['Noto Sans Regular'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 6, 9, 10, 12, 12, 13],
            'text-max-width': 8,
            'text-anchor': 'center',
            'text-allow-overlap': false,
            'text-padding': ['interpolate', ['linear'], ['zoom'], 6, 50, 10, 12, 14, 4],
        },
        paint: {
            'text-color': '#004D57',
            'text-halo-color': '#ffffff',
            'text-halo-width': 0.8,
        },
    });

    // ── Historic Sites ───────────────────────────────────────────────
    labelLayers.push({
        id: 'admin_historic_sites-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'historic_sites',
        minzoom: 7,
        layout: {
            visibility: 'none',
            'symbol-placement': 'point',
            'text-field': ['get', 'name'],
            'text-font': ['Noto Sans Regular'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 7, 9, 10, 11, 12, 12],
            'text-max-width': 8,
            'text-anchor': 'center',
            'text-allow-overlap': false,
            'text-padding': ['interpolate', ['linear'], ['zoom'], 7, 50, 10, 12, 14, 4],
        },
        paint: {
            'text-color': '#5D4037',
            'text-halo-color': '#ffffff',
            'text-halo-width': 0.8,
        },
    });

    // ── Aboriginal / Indigenous Lands label ──────────────────────────
    // Same z11 hold-off as land_access-label above — the fill is visible
    // earlier, the name just waits until the user is zoomed in closer.
    labelLayers.push({
        id: 'admin_aboriginal_lands-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'aboriginal_lands',
        minzoom: 11,
        layout: {
            visibility: 'none',
            'symbol-placement': 'point',
            'text-field': ['get', 'name'],
            'text-font': ['Noto Sans Regular'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 7, 9, 10, 12, 12, 13],
            'text-max-width': 8,
            'text-anchor': 'center',
            'text-allow-overlap': false,
            'text-padding': ['interpolate', ['linear'], ['zoom'], 7, 50, 10, 12, 14, 4],
        },
        paint: {
            'text-color': '#8B6508',
            'text-halo-color': '#ffffff',
            'text-halo-width': 0.8,
        },
    });

    // ── Region / Management Unit labels (topmost — above all other labels) ──
    // Region number labels — zoomed way out: large centred region number (1–8)
    labelLayers.push({
        id: 'regions-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'regions_fill',
        minzoom: 4,
        maxzoom: 7,
        layout: {
            'symbol-placement': 'point',
            'text-field': ['get', 'zone'],
            'text-font': ['Noto Sans Bold'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 4, 14, 6, 18],
            'text-anchor': 'center',
            'text-justify': 'center',
            'text-allow-overlap': false,
            'text-ignore-placement': false,
            'text-padding': 4,
            'text-max-width': 6,
        },
        paint: {
            'text-color': '#333333',
            'text-opacity': ['interpolate', ['linear'], ['zoom'], 4, 0.6, 6, 0.9],
            'text-halo-color': '#ffffff',
            'text-halo-width': 2.5,
        },
    });

    // Management Unit labels — mid zoom: centred inside polygon fill
    labelLayers.push({
        id: 'management_units-label-low',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'wmu',
        minzoom: 7,
        maxzoom: 8,
        layout: {
            'symbol-placement': 'point',
            'text-field': ['get', 'admin_id'],
            'text-font': ['Noto Sans Bold'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 7, 12, 8, 14],
            'text-anchor': 'center',
            'text-justify': 'center',
            'text-allow-overlap': true,
            'text-ignore-placement': false,
            'text-padding': 2,
            'text-max-width': 6,
        },
        paint: {
            'text-color': '#333333',
            'text-opacity': ['interpolate', ['linear'], ['zoom'], 7, 0.6, 8, 0.8],
            'text-halo-color': '#ffffff',
            'text-halo-width': 2,
        },
    });

    // Management Unit labels — zoomed in: repeated along boundary lines
    labelLayers.push({
        id: 'management_units-label',
        type: 'symbol',
        source: 'regulations',
        'source-layer': 'wmu_boundary',
        minzoom: 8,
        layout: {
            'symbol-placement': 'line',
            'text-field': ['get', 'admin_id'],
            'text-font': ['Noto Sans Bold'],
            'text-size': ['interpolate', ['linear'], ['zoom'], 8, 11, 9, 13, 12, 15],
            'text-allow-overlap': false,
            'text-ignore-placement': false,
            'symbol-spacing': ['interpolate', ['linear'], ['zoom'], 8, 150, 10, 200, 13, 200],
            'text-max-angle': 15,
            'text-offset': [0, -0.6],
            'symbol-avoid-edges': true,
        },
        paint: {
            'text-color': '#444444',
            'text-opacity': ['interpolate', ['linear'], ['zoom'], 8, 0.5, 9, 0.7, 12, 0.85],
            'text-halo-color': '#ffffff',
            'text-halo-width': 1.6,
        },
    });

    return labelLayers;
};
