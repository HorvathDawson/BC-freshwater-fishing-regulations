import { useEffect, useRef, useState, useCallback } from 'react';
import maplibregl from 'maplibre-gl';
import { Protocol } from 'pmtiles';
import { layers, LIGHT } from '@protomaps/basemaps';
import 'maplibre-gl/dist/maplibre-gl.css';
import { createRegulationLayers, createAdminLabelLayers, createEarlyRoadLayers, HIGHLIGHT_COLORS, SELECTION_COLOR } from '../map/styles';
import bcBoundary from '../map/bcBoundary.json';
import { waterbodyDataService } from '../services/waterbodyDataService';
import type { Reach, RegulationData, ResolveResult } from '../services/waterbodyDataService';
import { 
    isMobileViewport,
    type FeatureInfo, 
    type FeatureOption, 
    type FeatureGeometry,
    type CollapseState 
} from '../utils/featureUtils';
import { parseUrlState, navigateToWaterbody, navigateToFeature, clearUrlState } from '../utils/urlState';
import InfoPanel from './InfoPanel';
import DisambiguationMenu from './DisambiguationMenu';
import SearchBar from './SearchBar';
import Disclaimer, { DisclaimerLink } from './Disclaimer';
import type { SearchableFeature, RegulationSegment } from './SearchBar';
import './Map.css';

// --- CONFIG & PROTOCOL ---
// PMTiles tile cache: keeps decoded tiles in memory to avoid redundant
// Range requests.  150 entries covers ~2 full screens of tiles at z10.
// Stashed on globalThis so the cache survives Vite HMR module re-evaluation.
const protocol: Protocol = (globalThis as any).__pmtilesProtocol ??= new Protocol({ metadata: true });
if (!(globalThis as any).__pmtilesProtocolAdded) {
    maplibregl.addProtocol('pmtiles', protocol.tile);
    (globalThis as any).__pmtilesProtocolAdded = true;
}

// Tile base URL: empty in dev (local /data/), R2 public URL in production
const TILE_BASE = import.meta.env.VITE_TILE_BASE_URL
    ? `pmtiles://${import.meta.env.VITE_TILE_BASE_URL}`
    : 'pmtiles:///data';

// BC bounding box with margin for map constraints
// Interior: approx -139.05 to -114.03 (lng), 48.30 to 60.00 (lat)
const BC_BOUNDS: [[number, number], [number, number]] = [
    [-148.0, 45.0], // SW with margin
    [-108.0, 63.5], // NE with margin
];

// ── Satellite imagery source ────────────────────────────────────────
// Abstracted so it's easy to swap providers.  To switch to BC Gov SPOT 15m
// WMS once access is granted, replace SATELLITE_CONFIG below.  See
// SATELLITE_SOURCES.md for details and tested alternatives.
const SATELLITE_CONFIG = {
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
    tileSize: 256,
    attribution: 'Powered by <a href="https://www.esri.com">Esri</a>',
    maxzoom: 18,
};
// Alternative: Sentinel-2 Cloudless (CC-BY 4.0, 10m resolution, global)
// const SATELLITE_CONFIG = {
//     url: 'https://tiles.maps.eox.at/wmts/1.0.0/s2cloudless-2021_3857/default/GoogleMapsCompatible/{z}/{y}/{x}.jpg',
//     tileSize: 256,
//     attribution: 'Imagery: <a href="https://s2maps.eu">Sentinel-2 Cloudless 2021</a> by EOX — CC-BY 4.0',
//     maxzoom: 15,
// };

// Lucide SVG icon strings for the satellite toggle button (rendered imperatively via IControl)
const LAYERS_SVG = '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m12.83 2.18a2 2 0 0 0-1.66 0L2.6 6.08a1 1 0 0 0 0 1.83l8.58 3.91a2 2 0 0 0 1.66 0l8.58-3.9a1 1 0 0 0 0-1.83Z"/><path d="m22.54 12.43-1.42-.65-8.29 3.78a2 2 0 0 1-1.66 0l-8.29-3.78-1.42.65a1 1 0 0 0 0 1.84l8.58 3.91a2 2 0 0 0 1.66 0l8.58-3.9a1 1 0 0 0 0-1.85Z"/><path d="m22.54 16.43-1.42-.65-8.29 3.78a2 2 0 0 1-1.66 0l-8.29-3.78-1.42.65a1 1 0 0 0 0 1.84l8.58 3.91a2 2 0 0 0 1.66 0l8.58-3.9a1 1 0 0 0 0-1.85Z"/></svg>';
const MAP_SVG = '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14.106 5.553a2 2 0 0 0 1.788 0l3.659-1.83A1 1 0 0 1 21 4.619v12.764a1 1 0 0 1-.553.894l-4.553 2.277a2 2 0 0 1-1.788 0l-4.212-2.106a2 2 0 0 0-1.788 0l-3.659 1.83A1 1 0 0 1 3 19.381V6.618a1 1 0 0 1 .553-.894l4.553-2.277a2 2 0 0 1 1.788 0z"/><path d="M15 5.764v15"/><path d="M9 3.236v15"/></svg>';
// Lucide "sun" icon for disclosure of overlay opacity slider
const OPACITY_SVG = '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2"/><path d="M12 20v2"/><path d="m4.93 4.93 1.41 1.41"/><path d="m17.66 17.66 1.41 1.41"/><path d="M2 12h2"/><path d="M20 12h2"/><path d="m6.34 17.66-1.41 1.41"/><path d="m19.07 4.93-1.41 1.41"/></svg>';

// Map layer type → paint property names that control opacity.
// Used to capture and multiply base opacities when the overlay slider is active.
const OPACITY_PAINT_PROPS: Record<string, string[]> = {
    fill: ['fill-opacity'], line: ['line-opacity'], circle: ['circle-opacity'],
};

/**
 * Multiply an opacity value (or zoom-based expression) by a scalar.
 * MapLibre forbids wrapping zoom expressions with ['*', factor, expr] —
 * zoom inputs must be top-level inside 'step' or 'interpolate'.
 * So we walk the stop outputs and scale each numeric value directly.
 */
function scaleOpacity(base: unknown, factor: number): unknown {
    if (typeof base === 'number') return base * factor;
    if (!Array.isArray(base) || base.length < 4) return base;
    const head = base[0];
    if (head === 'interpolate') {
        // ['interpolate', method, ['zoom'], z1, v1, z2, v2, ...]
        const result = [...base];
        for (let i = 4; i < result.length; i += 2) {
            if (typeof result[i] === 'number') result[i] = result[i] * factor;
        }
        return result;
    }
    if (head === 'step') {
        // ['step', ['zoom'], defaultValue, z1, v1, z2, v2, ...]
        const result = [...base];
        if (typeof result[2] === 'number') result[2] = result[2] * factor;
        for (let i = 4; i < result.length; i += 2) {
            if (typeof result[i] === 'number') result[i] = result[i] * factor;
        }
        return result;
    }
    return base;
}

/**
 * Convert a legacy Mapbox GL filter to expression syntax so it can be
 * combined with expression-only operators like `within`.
 * Legacy:    ["==", "kind", "locality"]
 * Expression: ["==", ["get", "kind"], "locality"]
 */
function legacyFilterToExpression(f: any): any {
    if (!Array.isArray(f) || f.length === 0) return f;
    const [op, ...args] = f;
    // If second element is already an array, assume expression syntax
    if (args.length > 0 && Array.isArray(args[0])) return f;
    switch (op) {
        case '==': case '!=': case '>': case '>=': case '<': case '<=':
            return [op, ['get', args[0]], args[1]];
        case 'in':
            return ['in', ['get', args[0]], ['literal', args.slice(1)]];
        case '!in':
            return ['!', ['in', ['get', args[0]], ['literal', args.slice(1)]]];
        case 'has':
            return ['has', args[0]];
        case '!has':
            return ['!', ['has', args[0]]];
        case 'all':
            return ['all', ...args.map(legacyFilterToExpression)];
        case 'any':
            return ['any', ...args.map(legacyFilterToExpression)];
        case 'none':
            return ['!', ['any', ...args.map(legacyFilterToExpression)]];
        default:
            return f;
    }
}

const INTERACTABLE_LAYERS = ['streams', 'lakes-fill', 'wetlands-fill', 'manmade-fill'];

// Filter-based highlight / selection layer IDs.
// These render directly from the 'regulations' PMTiles source — no geometry
// copying. MapLibre handles tile clipping and buffer overlap natively, so
// fills and strokes render without seam or double-opacity artifacts.
const HL_LAYER_IDS = [
    'hl-streams', 'hl-lakes-fill', 'hl-lakes-line',
    'hl-wetlands-fill', 'hl-wetlands-line', 'hl-manmade-fill', 'hl-manmade-line',
];
const SL_LAYER_IDS = [
    'sl-streams', 'sl-lakes-fill', 'sl-lakes-line',
    'sl-wetlands-fill', 'sl-wetlands-line', 'sl-manmade-fill', 'sl-manmade-line',
];
/** Matches nothing — used to hide highlight/selection layers when inactive. */
const FILTER_NONE: maplibregl.FilterSpecification = ['literal', false] as any;

// --- STYLE EXPRESSIONS ---
const STREAM_LINE_WIDTH = ['interpolate', ['linear'], ['zoom'], 4, ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.1]], 8, ['+', 2, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.15]], 11, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 1.5], 12, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 2], 14, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 3], 16, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 4]];

// --- UTILITY ---
const isValidBbox = (bbox: unknown): bbox is [number, number, number, number] => {
    if (!bbox || !Array.isArray(bbox) || bbox.length !== 4) return false;
    const [minx, miny, maxx, maxy] = bbox as number[];
    return minx >= -180 && minx <= 180 && maxx >= -180 && maxx <= 180 &&
           miny >= -90 && miny <= 90 && maxy >= -90 && maxy <= 90 &&
           minx <= maxx && miny <= maxy;
};

/** True when bbox has zero area (point feature or degenerate sliver). */
const isPointBbox = (bbox: [number, number, number, number]): boolean =>
    bbox[0] === bbox[2] && bbox[1] === bbox[3];

/**
 * Fly to a bounding box, handling point features (zero-size bounds)
 * by centering on the point at minzoom instead of using cameraForBounds.
 *
 * When the feature's minzoom forces a zoom higher than what would fit
 * the full bbox in the viewport, we center on the bbox midpoint instead
 * of using cameraForBounds' center (which was computed for a different
 * zoom level).  The bbox may overflow the viewport — that's intentional.
 */
const flyToBbox = (
    map: maplibregl.Map,
    bbox: [number, number, number, number],
    padding: maplibregl.PaddingOptions,
    minzoom: number,
    duration = 800,
) => {
    const bboxCenter: [number, number] = [(bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2];

    if (isPointBbox(bbox)) {
        map.flyTo({
            center: bboxCenter,
            zoom: Math.min(minzoom, 15),
            padding,
            duration,
        });
    } else {
        const bounds = new maplibregl.LngLatBounds([bbox[0], bbox[1]], [bbox[2], bbox[3]]);
        const camera = map.cameraForBounds(bounds, { padding });
        if (camera) {
            const fitsZoom = camera.zoom || 0;
            // If minzoom forces a tighter (more zoomed in) view than what
            // would fit the bbox, center on the bbox midpoint so the feature
            // is visually centered even though edges overflow.
            const useMinzoom = fitsZoom < minzoom;
            const finalZoom = Math.min(useMinzoom ? minzoom : fitsZoom, 15);
            const center = useMinzoom ? bboxCenter : camera.center;
            map.flyTo({ center, zoom: finalZoom, padding, duration });
        }
    }
};

const createCirclePolygon = (lngLat: { lng: number; lat: number }, zoom: number) => {
    const radiusInMeters = 15 * (40075016.686 * Math.abs(Math.cos(lngLat.lat * Math.PI / 180)) / (256 * Math.pow(2, zoom)));
    const steps = 64; const coords: number[][] = [];
    for (let i = 0; i < steps; i++) {
        const angle = (i / steps) * 2 * Math.PI;
        const dx = radiusInMeters * Math.cos(angle) / (111320 * Math.cos(lngLat.lat * Math.PI / 180));
        const dy = radiusInMeters * Math.sin(angle) / 110540;
        coords.push([lngLat.lng + dx, lngLat.lat + dy]);
    }
    coords.push(coords[0]);
    return { type: 'Polygon' as const, coordinates: [coords] };
};

const createWetlandPattern = () => {
    const size = 16;
    const canvas = document.createElement('canvas');
    canvas.width = size; canvas.height = size;
    const ctx = canvas.getContext('2d');
    if (!ctx) return null;
    // Light green background so wetlands read as green over any base tile
    ctx.fillStyle = 'rgba(129, 199, 132, 0.45)';
    ctx.fillRect(0, 0, size, size);
    // Darker green diagonal stripes for texture
    ctx.strokeStyle = 'rgba(56, 142, 60, 0.7)';
    ctx.lineWidth = 1.2;
    for (let i = -size; i < size * 2; i += 4) {
        ctx.beginPath(); ctx.moveTo(i, 0); ctx.lineTo(i + size, size); ctx.stroke();
    }
    return ctx.getImageData(0, 0, size, size);
};

/** Parse a #rrggbb hex string → [r, g, b]. */
const parseHex = (hex: string): [number, number, number] => {
    const h = hex.replace('#', '');
    return [parseInt(h.slice(0, 2), 16), parseInt(h.slice(2, 4), 16), parseInt(h.slice(4, 6), 16)];
};

/**
 * Single-direction 45° diagonal hatch (used for National Parks).
 * Crimson lines signal a restricted / no-fishing zone.
 */
const createDiagonalHatchPattern = (hexColor: string): ImageData | null => {
    const size = 20, spacing = 10;
    const canvas = document.createElement('canvas');
    canvas.width = size; canvas.height = size;
    const ctx = canvas.getContext('2d');
    if (!ctx) return null;
    const [r, g, b] = parseHex(hexColor);
    ctx.strokeStyle = `rgba(${r},${g},${b},0.55)`;
    ctx.lineWidth = 1.5;
    for (let i = -size; i < size * 2; i += spacing) {
        ctx.beginPath(); ctx.moveTo(i, 0); ctx.lineTo(i + size, size); ctx.stroke();
    }
    return ctx.getImageData(0, 0, size, size);
};

/**
 * Cross-hatch pattern (used for Ecological Reserves — doubly restricted).
 * Two diagonal directions create a diamond mesh.
 */
const createCrossHatchPattern = (hexColor: string): ImageData | null => {
    const size = 22, spacing = 11;
    const canvas = document.createElement('canvas');
    canvas.width = size; canvas.height = size;
    const ctx = canvas.getContext('2d');
    if (!ctx) return null;
    const [r, g, b] = parseHex(hexColor);
    ctx.strokeStyle = `rgba(${r},${g},${b},0.45)`;
    ctx.lineWidth = 1;
    for (let i = -size; i < size * 2; i += spacing) {
        ctx.beginPath(); ctx.moveTo(i, 0); ctx.lineTo(i + size, size); ctx.stroke();
    }
    for (let i = -size; i < size * 2; i += spacing) {
        ctx.beginPath(); ctx.moveTo(i + size, 0); ctx.lineTo(i, size); ctx.stroke();
    }
    return ctx.getImageData(0, 0, size, size);
};

/**
 * Horizontal line pattern (used for OSM Admin / research forests — partial restriction).
 * Wide-spaced horizontal bars signal "caution" rather than "prohibited."
 */
const createHorizontalLinePattern = (hexColor: string): ImageData | null => {
    const size = 18, spacing = 14;
    const canvas = document.createElement('canvas');
    canvas.width = size; canvas.height = size;
    const ctx = canvas.getContext('2d');
    if (!ctx) return null;
    const [r, g, b] = parseHex(hexColor);
    ctx.strokeStyle = `rgba(${r},${g},${b},0.40)`;
    ctx.lineWidth = 1.0;
    for (let y = spacing / 2; y < size; y += spacing) {
        ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(size, y); ctx.stroke();
    }
    return ctx.getImageData(0, 0, size, size);
};

/** Normalize plural backend types to singular frontend types */
const normalizeType = (type: string): 'stream' | 'lake' | 'wetland' | 'manmade' | 'ungazetted' => {
    if (type === 'streams' || type === 'stream') return 'stream';
    if (type === 'lakes' || type === 'lake') return 'lake';
    if (type === 'wetlands' || type === 'wetland') return 'wetland';
    if (type === 'ungazetted') return 'ungazetted';
    return 'manmade';
};

/** Resolve tile source-layer from normalized feature type */
const resolveSourceLayer = (type: string): string => {
    if (type === 'stream' || type === 'streams') return 'streams';
    if (type === 'ungazetted') return 'ungazetted';
    return 'lakes';
};

/** Build a FeatureOption directly from a Reach when the reach is not in the search index. */
const buildFeatureFromReach = (
    reach: Reach,
    reachId: string,
    regSets: string[],
    reachSegments: Record<string, string[]>,
    opts?: {
        geometry?: FeatureGeometry;
        source?: string;
        sourceLayer?: string;
        waterbodyKey?: string;
    },
): FeatureOption => {
    const regIds = regSets[reach.reg_set_index] || '';
    const type = normalizeType(reach.feature_type);
    return {
        type,
        id: reachId,
        geometry: opts?.geometry,
        source: opts?.source,
        sourceLayer: opts?.sourceLayer,
        bbox: reach.bbox || undefined,
        minzoom: reach.min_zoom,
        properties: {
            display_name: reach.display_name || '',
            frontend_group_id: reachId,
            group_id: reachId,
            waterbody_key: opts?.waterbodyKey || '',
            waterbody_group: '',
            regulation_ids: regIds,
            regulation_count: regIds ? regIds.split(',').length : 0,
            zones: '',
            region_name: (reach.regions || []).join(', '),
            name_variants: reach.name_variants || [],
            tributary_reg_ids: reach.tributary_reg_ids || [],
            _reachId: reachId,
            _fidList: reachSegments[reachId],
        },
    };
};

/**
 * Build a FeatureOption from V2 JSON data exclusively.
 *
 * This is the **single** way menu/panel data should be constructed.
 * Tiles only provide:
 *   - geometry (for map highlighting)
 *   - _adminZones (computed at click-time from admin boundary tiles)
 *
 * Everything else (names, regulation_ids, zones, etc.) comes from the
 * SearchableFeature + RegulationSegment looked up via reach_id.
 *
 * V2: frontend_group_id carries the reach_id. Tiles don't have fgid —
 * the /api/resolve endpoint maps tile fids/wbks to reach_ids.
 * _fidList stores the fid list for stream highlight filters.
 */
const buildFeatureFromJSON = (
    feature: SearchableFeature,
    segment: RegulationSegment | null,
    opts: {
        geometry?: FeatureGeometry;
        source?: string;
        sourceLayer?: string;
        /** Override reach_id (stored in frontend_group_id slot) */
        frontendGroupId?: string;
        /** fid list for stream reach highlighting */
        fidList?: string[];
        /** Click-time extras (e.g. _adminZones) — never overwrites JSON fields */
        extras?: Record<string, any>;
    } = {},
): FeatureOption => {
    const type = normalizeType(feature.type);
    const srcLayer = opts.sourceLayer || resolveSourceLayer(type);
    const segBbox = segment?.bbox as [number, number, number, number] | undefined;
    const reachId = opts.frontendGroupId || segment?.frontend_group_id || '';

    // Look up in-season changes for this reach (synchronous — data loaded at startup)
    const inSeasonChanges = reachId ? waterbodyDataService.getInSeasonChanges(reachId) : [];
    const inSeasonMeta = inSeasonChanges.length > 0 ? waterbodyDataService.getInSeasonMeta() : undefined;

    return {
        type,
        id: reachId || feature.id,
        geometry: opts.geometry,
        source: opts.source || 'regulations',
        sourceLayer: srcLayer,
        bbox: segBbox || feature.bbox as [number, number, number, number],
        minzoom: Number(segment?.min_zoom || feature.min_zoom || feature.properties?.minzoom || 4),
        properties: {
            // ── Names (segment-specific where available) ──
            display_name: segment?.display_name || feature.display_name || '',
            name_variants: segment?.name_variants || feature.name_variants || [],

            // ── IDs (V2: frontend_group_id = reach_id) ──
            frontend_group_id: reachId,
            group_id: reachId,
            waterbody_key: feature.properties?.waterbody_key || segment?.waterbody_group || '',
            waterbody_group: (feature.properties?.waterbody_group as string) || segment?.waterbody_group || '',

            // ── Regulation data ──
            regulation_ids: segment?.regulation_ids || feature.properties?.regulation_ids || '',
            regulation_count: feature.properties?.regulation_count || 0,
            tributary_reg_ids: segment?.tributary_reg_ids || feature.properties?.tributary_reg_ids || [],

            // ── Geography ──
            zones: feature.properties?.zones || '',
            region_name: feature.properties?.region_name || '',
            mgmt_units: feature.properties?.mgmt_units || '',
            fwa_watershed_code: feature.properties?.fwa_watershed_code || '',
            minzoom: feature.min_zoom || feature.properties?.minzoom || 4,
            total_length_km: feature.properties?.total_length_km || 0,
            length_km: segment?.length_km || 0,

            // ── V2 highlighting support ──
            // Stored on properties so buildFeatureFilter can generate the
            // correct filter expression without external lookups.
            _reachId: reachId,
            _fidList: opts.fidList || segment?.group_ids || undefined,

            // ── In-season changes (if any) ──
            ...(inSeasonChanges.length > 0 ? {
                _inSeasonChanges: inSeasonChanges,
                _inSeasonMeta: inSeasonMeta,
            } : {}),

            // ── Click-time extras (admin zones, etc.) ──
            ...(opts.extras || {}),
        },
    };
};

const buildFeatureFilter = (feature: FeatureInfo | FeatureOption): unknown[] | null => {
    const props = feature.properties || {};
    
    // V2: Stream reaches carry a _fidList — highlight all fids in the reach.
    // This replaces the v1 frontend_group_id filter because v2 tiles don't
    // have fgid; they only have fid (streams) or waterbody_key (polygons).
    const fidList = props._fidList as string[] | undefined;
    if (fidList && fidList.length > 0) {
        return ['in', ['get', 'fid'], ['literal', fidList]];
    }
    
    // V2: Polygon reaches — match by waterbody_key
    const wbk = props.waterbody_key;
    if (wbk) {
        return ['==', ['get', 'waterbody_key'], wbk];
    }
    
    return null;
};

/**
 * Set the filter on a group of highlight or selection layers.
 *
 * When `feature` is non-null the matching filter (by frontend_group_id,
 * group_id, etc.) is applied to every layer in the group. When null,
 * FILTER_NONE hides everything.  Because the layers render directly from
 * the PMTiles vector source, MapLibre handles tile clipping and buffer
 * overlap natively — no geometry copying, no dedup, no seam artifacts.
 */
const setGroupFilter = (map: maplibregl.Map, layerIds: string[], feature: FeatureInfo | FeatureOption | null) => {
    const filter: maplibregl.FilterSpecification = feature
        ? (buildFeatureFilter(feature) as maplibregl.FilterSpecification) || FILTER_NONE
        : FILTER_NONE;
    for (const id of layerIds) {
        if (map.getLayer(id)) map.setFilter(id, filter);
    }
};

/** Bottom padding for the partial (35 vh) mobile panel. */
const getMobileBottomPadding = () => Math.round(window.innerHeight * 0.35) + 20;

/**
 * Pick mobile padding and target panel state based on bbox shape.
 * Always opens in partial mode so the map stays visible above the panel.
 */
const getMobilePaddingForBounds = (_bounds: maplibregl.LngLatBounds) => {
    return {
        padding: { top: 60, bottom: getMobileBottomPadding(), left: 40, right: 40 },
        panelState: 'partial' as CollapseState,
    };
};

const MapComponent = () => {
    const mapContainerRef = useRef<HTMLDivElement>(null);
    const mapRef = useRef<maplibregl.Map | null>(null);
    const satBtnRef = useRef<HTMLButtonElement | null>(null);
    const toggleSatelliteRef = useRef<() => void>(() => {});
    const opacityCtrlRef = useRef<HTMLElement | null>(null);
    const toggleSliderRef = useRef<() => void>(() => {});
    const handleOverlayOpacityRef = useRef<(val: string) => void>(() => {});
    // Cache of each regulation layer's original paint opacity values,
    // captured on first satellite toggle so the slider can multiply them.
    const baseOpacitiesRef = useRef<Record<string, [string, any][]>>({});
    // Remember the user's last satellite-mode opacity so it persists across toggles.
    const lastSatelliteOpacityRef = useRef(0.4);
    // Cache original label paint props so we can swap to satellite-friendly colours and restore.
    const baseLabelPaintsRef = useRef<Record<string, Record<string, any>>>({});
    const isDisambigOpenRef = useRef<boolean>(false);
    const hoverTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    const searchPollRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    const selectedFeatureRef = useRef<FeatureInfo | null>(null);
    const mobilePanelStateRef = useRef<CollapseState>('expanded');
    const prevMobilePanelStateRef = useRef<CollapseState>('expanded');
    const urlRestoredRef = useRef<boolean>(false);
    const popstateInProgressRef = useRef<boolean>(false);
    // Lookup map: reach_id → { feature, segment }
    // Populated once tier0.json loads; used by the click handler
    // to enrich tile features with search-level data (name_variants, etc.)
    const searchLookupRef = useRef<Map<string, { feature: SearchableFeature; segment: RegulationSegment | null }>>(new Map());
    // Reverse index: waterbody_group → all SearchableFeature entries on that physical waterbody.
    // Built once at data-load time. Values are references into the same feature objects — no copies.
    const wbgIndexRef = useRef<Map<string, SearchableFeature[]>>(new Map());
    // Loaded tier0 regulation data — holds reaches, reachSegments, regulations, reg_sets.
    const regDataRef = useRef<RegulationData | null>(null);
    // Monotonic counter for click handler — discards stale async resolve results.
    const clickGenRef = useRef(0);
    // Last known cursor position (lngLat) for re-drawing cursor circle on zoom
    const cursorLngLatRef = useRef<{ lng: number; lat: number } | null>(null);
    
    const [selectedFeature, setSelectedFeature] = useState<FeatureInfo | null>(null);
    // Derived from wbgIndexRef whenever selectedFeature changes — never set manually.
    const [siblingFeatures, setSiblingFeatures] = useState<SearchableFeature[]>([]);
    const [disambigOptions, setDisambigOptions] = useState<FeatureOption[]>([]);
    const [disambigPosition, setDisambigPosition] = useState<{ x: number; y: number } | null>(null);
    const [clickLoadingPos, setClickLoadingPos] = useState<{ x: number; y: number } | null>(null);
    // Delayed spinner — avoid flash on fast resolves.
    // Only show spinner after SPINNER_DELAY ms; once visible, keep for at least SPINNER_MIN ms.
    const spinnerTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    const spinnerShownAtRef = useRef<number>(0);

    const [mobilePanelState, setMobilePanelState] = useState<CollapseState>('expanded');
    const [highlightedOption, setHighlightedOption] = useState<FeatureOption | null>(null);
    const [highlightedSearchResult, setHighlightedSearchResult] = useState<SearchableFeature | null>(null);
    const [searchableFeatures, setSearchableFeatures] = useState<SearchableFeature[]>([]);
    const [dataLoaded, setDataLoaded] = useState(false);
    const [mapReady, setMapReady] = useState(false);
    const [filtersApplied, setFiltersApplied] = useState(false);
    // Fetched once from data_version.json (no-store). null = not yet resolved.
    // Used as ?v= query param on PMTiles URLs to bust browser cache on deploys.
    const [dataVersion, setDataVersion] = useState<string | null>(null);
    const [disclaimerOpen, setDisclaimerOpen] = useState(false);
    const [isSatellite, setIsSatellite] = useState(false);
    const [overlayOpacity, setOverlayOpacity] = useState(1);
    const [sliderOpen, setSliderOpen] = useState(false);

    // Spinner delay constants (ms)
    const SPINNER_DELAY = 150;  // wait before showing
    const SPINNER_MIN   = 300;  // once visible, keep at least this long

    /** Schedule spinner at position after SPINNER_DELAY ms. */
    const showSpinnerDelayed = useCallback((pos: { x: number; y: number }) => {
        // Cancel any pending hide/show
        if (spinnerTimerRef.current) clearTimeout(spinnerTimerRef.current);
        spinnerTimerRef.current = setTimeout(() => {
            spinnerShownAtRef.current = Date.now();
            setClickLoadingPos(pos);
        }, SPINNER_DELAY);
    }, []);

    /** Hide spinner — immediately if not yet visible, or after SPINNER_MIN if showing. */
    const hideSpinner = useCallback(() => {
        // Cancel pending show
        if (spinnerTimerRef.current) {
            clearTimeout(spinnerTimerRef.current);
            spinnerTimerRef.current = null;
        }
        if (!spinnerShownAtRef.current) {
            // Never became visible — hide immediately
            setClickLoadingPos(null);
            return;
        }
        const elapsed = Date.now() - spinnerShownAtRef.current;
        const remaining = SPINNER_MIN - elapsed;
        spinnerShownAtRef.current = 0;
        if (remaining <= 0) {
            setClickLoadingPos(null);
        } else {
            spinnerTimerRef.current = setTimeout(() => {
                setClickLoadingPos(null);
                spinnerTimerRef.current = null;
            }, remaining);
        }
    }, []);

    // Fetch data version once on mount — resolves quickly (~50 bytes, no-store).
    // Sets dataVersion so the map init useEffect can use versioned PMTiles URLs.
    // In dev mode skip the fetch: files are always current and ?v= would
    // interfere with the pmtiles protocol's in-memory tile cache.
    useEffect(() => {
        if (import.meta.env.DEV) {
            setDataVersion('');
        } else {
            waterbodyDataService.getDataVersion().then(setDataVersion).catch(() => setDataVersion(''));
        }
    }, []);

    // Mirror state → refs so map event-handler closures always see the latest values.
    // (State setters are stable; refs are mutable — this is the canonical React pattern.)
    useEffect(() => { selectedFeatureRef.current = selectedFeature; }, [selectedFeature]);
    useEffect(() => { mobilePanelStateRef.current = mobilePanelState; }, [mobilePanelState]);

    // Track mobile panel state transitions (no auto-zoom — user can use
    // the "Zoom to Feature" button in InfoPanel to fly to the feature).
    useEffect(() => {
        prevMobilePanelStateRef.current = mobilePanelState;
    }, [mobilePanelState]);

    // On desktop, shrink the map viewport when the info panel opens so it
    // doesn't overlap.  The CSS transition on .map-canvas is 200ms, so we
    // resize the MapLibre canvas after that transition completes.
    // After resize, pan so the selected feature stays centered in the
    // now-smaller (or restored) visible area.
    useEffect(() => {
        const container = mapContainerRef.current;
        if (!container) return;
        const panelOpen = selectedFeature !== null;
        container.closest('.map-container')?.classList.toggle('panel-open', panelOpen);
        // Resize after the CSS width transition (200ms) finishes
        const timer = setTimeout(() => {
            const map = mapRef.current;
            if (!map) return;
            map.resize();
            // Re-center on feature bbox after the canvas resizes (desktop only).
            // Skip re-center if the map is mid-flight (search/URL restore fly-to)
            // — the flyTo animation already targets the correct position.
            if (panelOpen && selectedFeature?.bbox && !isMobileViewport() && !map.isMoving()) {
                const bbox = selectedFeature.bbox;
                if (isValidBbox(bbox)) {
                    const center: [number, number] = [(bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2];
                    map.easeTo({ center, duration: 200 });
                }
            }
        }, 220);
        return () => clearTimeout(timer);
    }, [selectedFeature]);

    const toggleSatellite = useCallback(() => {
        const map = mapRef.current;
        if (!map) return;
        const next = !isSatellite;
        if (next) {
            // Entering satellite — restore last satellite opacity (default 40%)
            const satOpacity = lastSatelliteOpacityRef.current;
            setOverlayOpacity(satOpacity);
            // Capture base opacities on first activation
            if (Object.keys(baseOpacitiesRef.current).length === 0) {
                const style = map.getStyle();
                for (const layer of (style?.layers || [])) {
                    if ((layer as any).source !== 'regulations') continue;
                    if (layer.id.startsWith('admin_') || layer.id === 'bc-mask') continue;
                    const props = OPACITY_PAINT_PROPS[layer.type];
                    if (!props) continue;
                    baseOpacitiesRef.current[layer.id] = props.map(p =>
                        [p, map.getPaintProperty(layer.id, p) ?? 1]
                    );
                }
            }
            // Immediately apply scaled opacity so there's no full-opacity flash
            for (const [id, entries] of Object.entries(baseOpacitiesRef.current)) {
                for (const [prop, base] of entries) {
                    map.setPaintProperty(id, prop, scaleOpacity(base, satOpacity) as any);
                }
            }
        } else {
            // Leaving satellite — remember current opacity, force 100%
            lastSatelliteOpacityRef.current = overlayOpacity;
            setOverlayOpacity(1);
            // Immediately restore original paint opacities on the map
            // so there's no frame where satellite-level opacity persists.
            for (const [id, entries] of Object.entries(baseOpacitiesRef.current)) {
                for (const [prop, base] of entries) {
                    map.setPaintProperty(id, prop, base);
                }
            }
        }
        setIsSatellite(next);
        setSliderOpen(next); // auto-expand slider when satellite turns on

        // Show/hide satellite raster
        map.setLayoutProperty('satellite-tiles', 'visibility', next ? 'visible' : 'none');

        // Toggle protomaps base geometry layers (hide when satellite is on).
        // Label layers stay visible on top of satellite for readability,
        // with colours swapped to white-on-dark for satellite imagery.
        const LABEL_PAINT_KEYS = ['text-color', 'text-halo-color', 'text-halo-width', 'text-halo-blur', 'icon-opacity'] as const;
        const style = map.getStyle();
        if (style?.layers) {
            for (const layer of style.layers) {
                // The protomaps `background` layer has type "background" with no
                // source, so it won't match the source check below — handle it
                // explicitly to avoid it covering the satellite raster.
                if (layer.type === 'background') {
                    map.setLayoutProperty(layer.id, 'visibility', next ? 'none' : 'visible');
                    continue;
                }
                const src = (layer as any).source;
                const layout = (layer as any).layout;
                const isLabel = layout?.['text-field'] || layout?.['symbol-placement'];

                // Non-label protomaps layers (and shield/icon layers): hide in satellite.
                // Shield layers (icon-image) are hidden entirely — road names are
                // already shown by the separate roads_labels_major text layer.
                const hasIcon = layout?.['icon-image'];
                if (src === 'protomaps' && (!isLabel || hasIcon)) {
                    map.setLayoutProperty(layer.id, 'visibility', next ? 'none' : 'visible');
                    continue;
                }

                // Label layers (protomaps + regulations): swap colours for satellite
                if (isLabel) {
                    if (next) {
                        // Cache originals on first encounter
                        if (!baseLabelPaintsRef.current[layer.id]) {
                            const cached: Record<string, any> = {};
                            for (const k of LABEL_PAINT_KEYS) {
                                cached[k] = map.getPaintProperty(layer.id, k);
                            }
                            baseLabelPaintsRef.current[layer.id] = cached;
                        }
                        // Satellite style: white text, dark halo (Google Maps style)
                        map.setPaintProperty(layer.id, 'text-color', '#ffffff');
                        map.setPaintProperty(layer.id, 'text-halo-color', 'rgba(0,0,0,0.75)');
                        map.setPaintProperty(layer.id, 'text-halo-width', 1.5);
                        map.setPaintProperty(layer.id, 'text-halo-blur', 0.5);
                        // Hide highway shield icons so their dark background doesn't show
                        if (layout?.['icon-image']) {
                            map.setPaintProperty(layer.id, 'icon-opacity', 0);
                        }
                    } else if (baseLabelPaintsRef.current[layer.id]) {
                        // Restore original label paint
                        const orig = baseLabelPaintsRef.current[layer.id];
                        for (const k of LABEL_PAINT_KEYS) {
                            if (orig[k] !== undefined) {
                                map.setPaintProperty(layer.id, k, orig[k]);
                            }
                        }
                    }
                }
            }
        }
    }, [isSatellite, overlayOpacity]);

    // Keep the imperative satellite button ref in sync with React state
    useEffect(() => { toggleSatelliteRef.current = toggleSatellite; }, [toggleSatellite]);
    useEffect(() => {
        const btn = satBtnRef.current;
        if (!btn) return;
        btn.innerHTML = isSatellite ? MAP_SVG : LAYERS_SVG;
        btn.title = isSatellite ? 'Switch to map view' : 'Switch to satellite view';
        btn.setAttribute('aria-label', btn.title);
    }, [isSatellite]);

    // Apply overlay opacity multiplier to all regulation-sourced layers.
    // Captures each layer's paint opacity on first satellite activation,
    // then multiplies by the slider value.  Restores originals on deactivation.
    useEffect(() => {
        const map = mapRef.current;
        if (!map || !map.isStyleLoaded()) return;

        if (isSatellite) {
            // Capture base opacities on first activation
            if (Object.keys(baseOpacitiesRef.current).length === 0) {
                const style = map.getStyle();
                for (const layer of (style?.layers || [])) {
                    if ((layer as any).source !== 'regulations') continue;
                    if (layer.id.startsWith('admin_') || layer.id === 'bc-mask') continue;
                    const props = OPACITY_PAINT_PROPS[layer.type];
                    if (!props) continue;
                    baseOpacitiesRef.current[layer.id] = props.map(p =>
                        [p, map.getPaintProperty(layer.id, p) ?? 1]
                    );
                }
            }
            // Apply multiplied opacity
            for (const [id, entries] of Object.entries(baseOpacitiesRef.current)) {
                for (const [prop, base] of entries) {
                    map.setPaintProperty(id, prop, scaleOpacity(base, overlayOpacity) as any);
                }
            }
        } else {
            // Restore original opacities
            for (const [id, entries] of Object.entries(baseOpacitiesRef.current)) {
                for (const [prop, base] of entries) {
                    map.setPaintProperty(id, prop, base);
                }
            }
        }
    }, [isSatellite, overlayOpacity]);

    // Wire imperative refs for opacity IControl
    useEffect(() => {
        toggleSliderRef.current = () => setSliderOpen(o => !o);
    }, []);
    useEffect(() => {
        handleOverlayOpacityRef.current = (val: string) => {
            const v = parseFloat(val);
            setOverlayOpacity(v);
            lastSatelliteOpacityRef.current = v;
        };
    }, []);

    // Sync opacity IControl visibility and slider state with React state
    useEffect(() => {
        const wrapper = opacityCtrlRef.current;
        if (!wrapper) return;
        wrapper.style.display = isSatellite ? '' : 'none';
    }, [isSatellite]);
    useEffect(() => {
        const wrapper = opacityCtrlRef.current;
        if (!wrapper) return;
        const popup = wrapper.querySelector('.overlay-opacity-popup') as HTMLElement | null;
        if (popup) popup.style.display = sliderOpen ? '' : 'none';
        const input = wrapper.querySelector('input') as HTMLInputElement | null;
        if (input) input.value = String(overlayOpacity);
        const pct = wrapper.querySelector('.overlay-pct');
        if (pct) pct.textContent = `${Math.round(overlayOpacity * 100)}%`;
    }, [sliderOpen, overlayOpacity]);

    const clearSelection = useCallback(() => {
        setSelectedFeature(null);
        setHighlightedOption(null);
        setHighlightedSearchResult(null);
        setDisambigOptions([]);
        setDisambigPosition(null);
        isDisambigOpenRef.current = false;
        setMobilePanelState('expanded');
        
        // Clear feature from URL
        clearUrlState();

        if (searchPollRef.current) {
            clearInterval(searchPollRef.current);
            searchPollRef.current = null;
        }

        const map = mapRef.current;
        if (!map) return;
        // Clear cursor circle GeoJSON
        const cursorSrc = map.getSource('cursor-circle') as maplibregl.GeoJSONSource;
        if (cursorSrc) cursorSrc.setData({ type: 'FeatureCollection', features: [] });
        // Clear ungazetted point marker
        const ugSrc = map.getSource('ungazetted-marker') as maplibregl.GeoJSONSource;
        if (ugSrc) ugSrc.setData({ type: 'FeatureCollection', features: [] });
        // Reset highlight & selection layer filters to hide them
        setGroupFilter(map, HL_LAYER_IDS, null);
        setGroupFilter(map, SL_LAYER_IDS, null);
    }, []);

    // V2 data loading — loads tier0.json, builds SearchableFeature[],
    // populates searchLookupRef (reach_id → {feature, segment}), wbgIndexRef,
    // and regDataRef for click/highlight resolution.
    useEffect(() => {
        let cancelled = false;
        (async () => {
            try {
                const data = await waterbodyDataService.load();
                if (cancelled) return;
                regDataRef.current = data;

                const lookup = new Map<string, { feature: SearchableFeature; segment: RegulationSegment | null }>();
                const wbgMap = new Map<string, SearchableFeature[]>();
                const features: SearchableFeature[] = [];

                for (const entry of data.searchIndex) {
                    // Build RegulationSegment[] from tier0's enriched segments
                    const segments: RegulationSegment[] = (entry.segments || []).map(seg => {
                        const regIds = data.reg_sets[seg.reg_set_index] || '';
                        return {
                            frontend_group_id: seg.rid,
                            group_id: seg.rid,
                            group_ids: data.reachSegments[seg.rid] || undefined,
                            regulation_ids: regIds,
                            display_name: seg.display_name,
                            name_variants: seg.name_variants || [],
                            length_km: seg.length_km || 0,
                            bbox: seg.bbox || undefined,
                            min_zoom: seg.min_zoom,
                            waterbody_group: seg.waterbody_group || entry.waterbody_group,
                            tributary_reg_ids: seg.tributary_reg_ids || [],
                        } as RegulationSegment;
                    });

                    const type = normalizeType(entry.feature_type);
                    const reachIds = (entry.segments || []).map(s => s.rid);
                    const sf: SearchableFeature = {
                        id: entry.waterbody_group || entry.display_name,
                        display_name: entry.display_name,
                        name_variants: entry.name_variants || [],
                        type,
                        properties: {
                            waterbody_group: entry.waterbody_group,
                            zones: (entry.zones || []).join(', '),
                            region_name: (entry.regions || []).join(', '),
                            mgmt_units: (entry.management_units || []).join(', '),
                            total_length_km: entry.total_length_km || 0,
                            minzoom: entry.min_zoom,
                            regulation_count: segments.reduce((n, s) => n + (s.regulation_ids ? s.regulation_ids.split(',').length : 0), 0),
                        },
                        bbox: entry.bbox || undefined,
                        min_zoom: entry.min_zoom,
                        regulation_segments: segments,
                        _frontend_group_ids: reachIds,
                    };

                    features.push(sf);

                    // Populate search lookup: reach_id → {feature, segment}
                    for (const seg of segments) {
                        lookup.set(seg.frontend_group_id, { feature: sf, segment: seg });
                    }

                    // Populate wbg index
                    if (entry.waterbody_group) {
                        const arr = wbgMap.get(entry.waterbody_group);
                        if (arr) arr.push(sf);
                        else wbgMap.set(entry.waterbody_group, [sf]);
                    }
                }

                searchLookupRef.current = lookup;
                wbgIndexRef.current = wbgMap;
                setSearchableFeatures(features);
                setDataLoaded(true);
                console.log(`✅ V2 data loaded: ${features.length} search entries, ${lookup.size} reach lookups`);
            } catch (err) {
                console.error('❌ Failed to load regulation data:', err);
                setDataLoaded(true); // Allow map to remain usable without data
            }
        })();
        return () => { cancelled = true; };
    }, []);

    // Admin layer filtering — for "regulated_only" layers, apply a MapLibre
    // filter so only features with regulated admin_ids are visible.
    // Runs once when both the map is ready and regulation data is loaded.
    const ADMIN_LAYER_STYLE_IDS: Record<string, string[]> = {
        wma: ['admin_wma-fill', 'admin_wma-line', 'admin_wma-label'],
        watersheds: ['admin_watersheds-fill', 'admin_watersheds-line', 'admin_watersheds-label'],
        historic_sites: ['admin_historic_sites-fill', 'admin_historic_sites-line', 'admin_historic_sites-label'],
        osm_admin: ['admin_osm_admin-fill', 'admin_osm_admin-line', 'admin_osm_admin-label'],
        aboriginal_lands: ['admin_aboriginal_lands-fill', 'admin_aboriginal_lands-line', 'admin_aboriginal_lands-label'],
    };
    useEffect(() => {
        if (!mapReady || !dataLoaded) return;
        const map = mapRef.current;
        const regData = regDataRef.current;

        if (map && regData) {
            const adminVis = regData.adminVisibility;
            for (const [tileLayer, styleIds] of Object.entries(ADMIN_LAYER_STYLE_IDS)) {
                const cfg = adminVis[tileLayer];
                if (!cfg || cfg.display === 'all') {
                    // Show all features — make layers visible with no filter
                    for (const layerId of styleIds) {
                        if (map.getLayer(layerId)) map.setLayoutProperty(layerId, 'visibility', 'visible');
                    }
                    continue;
                }
                if (cfg.display === 'regulated_only' && cfg.regulated_ids?.length) {
                    const filter: any = ['in', ['get', 'admin_id'], ['literal', cfg.regulated_ids]];
                    for (const layerId of styleIds) {
                        if (map.getLayer(layerId)) {
                            map.setFilter(layerId, filter);
                            map.setLayoutProperty(layerId, 'visibility', 'visible');
                        }
                    }
                } else {
                    // regulated_only but no IDs → keep hidden
                    const filter: any = ['literal', false];
                    for (const layerId of styleIds) {
                        if (map.getLayer(layerId)) map.setFilter(layerId, filter);
                    }
                }
            }
        }
        setFiltersApplied(true);
    }, [mapReady, dataLoaded]);

    // URL restoration — restore feature selection from /waterbody/<wbg>/ or ?f=<id> or ?s=<reach_id>.
    // V2: Uses wbgIndexRef and searchLookupRef (keyed by reach_id) to find features.
    useEffect(() => {
        if (!mapReady || !dataLoaded || urlRestoredRef.current) return;
        urlRestoredRef.current = true;

        const url = parseUrlState();
        const regData = regDataRef.current;
        if (!regData) return;

        // Determine target feature from URL state
        let targetFeature: SearchableFeature | undefined;
        let targetReachId: string | undefined;

        if (url.waterbodyGroup) {
            // Canonical path: /waterbody/<wbg>/
            // When a specific section is targeted (?s=), resolve via searchLookupRef
            // first — side channels may share the same wbg as the main river.
            if (url.activeFgid) {
                const lookup = searchLookupRef.current.get(url.activeFgid);
                if (lookup) {
                    targetFeature = lookup.feature;
                    targetReachId = url.activeFgid;
                }
            }
            if (!targetFeature) {
                const entries = wbgIndexRef.current.get(url.waterbodyGroup);
                targetFeature = entries?.[0];
            }
        } else if (url.featureId) {
            // Legacy ?f= param — try as a reach_id first
            const lookup = searchLookupRef.current.get(url.featureId);
            targetFeature = lookup?.feature;
            targetReachId = url.featureId;
        }

        // Helper: select feature, fly to bbox, set state.
        // Uses the parent feature's full bbox + lowest segment min_zoom so
        // long streams fit in the viewport instead of over-zooming.
        const selectAndFly = (selected: FeatureOption, parent?: SearchableFeature) => {
            const flyBbox = (parent?.bbox ?? selected.bbox) as [number, number, number, number] | undefined;
            const segments = parent?.regulation_segments || [];
            const minZoom = segments.length > 0
                ? Math.min(...segments.map(s => s.min_zoom ?? 4))
                : (selected.minzoom ?? 10);
            if (flyBbox && isValidBbox(flyBbox)) {
                flyToBox(flyBbox, minZoom);
            }
            setSelectedFeature(selected);
        };

        if (targetFeature) {
            // If ?s=<reach_id> is present, select that specific section
            if (url.activeFgid) {
                targetReachId = url.activeFgid;
            }

            // Default to first segment if no specific reach targeted
            const segments = targetFeature.regulation_segments || [];
            let seg: RegulationSegment | null = segments[0] || null;
            if (targetReachId) {
                const match = segments.find(s => s.frontend_group_id === targetReachId);
                if (match) seg = match;
            }

            const reachId = seg?.frontend_group_id || '';
            const fidList = reachId ? regData.reachSegments[reachId] : undefined;
            const selected = buildFeatureFromJSON(targetFeature, seg, { fidList });
            selectAndFly(selected, targetFeature);
            return;
        }

        // Fallback: reach not in search index (unnamed feature).
        // Resolve via API to get reach metadata and fid list.
        if (targetReachId) {
            (async () => {
                const result = await waterbodyDataService.resolveByReachId(targetReachId!);
                if (!result) return;

                const reach = regData.reaches[targetReachId!];
                if (!reach) return;

                const selected = buildFeatureFromReach(reach, targetReachId!, regData.reg_sets, regData.reachSegments);
                selectAndFly(selected);
            })();
        }
    }, [mapReady, dataLoaded]);

    // Handle browser back/forward navigation.
    // Re-parse URL state and restore the corresponding feature or clear selection.
    useEffect(() => {
        const handlePopState = () => {
            const regData = regDataRef.current;
            if (!regData) return;

            popstateInProgressRef.current = true;

            const url = parseUrlState();
            let targetFeature: SearchableFeature | undefined;
            let targetReachId: string | undefined;

            if (url.waterbodyGroup) {
                if (url.activeFgid) {
                    const lookup = searchLookupRef.current.get(url.activeFgid);
                    if (lookup) {
                        targetFeature = lookup.feature;
                        targetReachId = url.activeFgid;
                    }
                }
                if (!targetFeature) {
                    const entries = wbgIndexRef.current.get(url.waterbodyGroup);
                    targetFeature = entries?.[0];
                }
            } else if (url.featureId) {
                const lookup = searchLookupRef.current.get(url.featureId);
                targetFeature = lookup?.feature;
                targetReachId = url.activeFgid || url.featureId;
            }

            if (targetFeature) {
                const segments = targetFeature.regulation_segments || [];
                let seg = segments[0] || null;
                if (targetReachId) {
                    const match = segments.find(s => s.frontend_group_id === targetReachId);
                    if (match) seg = match;
                }
                const reachId = seg?.frontend_group_id || '';
                const fidList = reachId ? regData.reachSegments[reachId] : undefined;
                const selected = buildFeatureFromJSON(targetFeature, seg, { fidList });
                setSelectedFeature(selected);
            } else if (!url.waterbodyGroup && !url.featureId) {
                // Back to root — clear selection without pushing another history entry
                setSelectedFeature(null);
                setDisambigOptions([]);
                setDisambigPosition(null);
                isDisambigOpenRef.current = false;
            }
        };

        window.addEventListener('popstate', handlePopState);
        return () => window.removeEventListener('popstate', handlePopState);
    }, []);

    // Update URL when a feature is selected or deselected.
    // Named features write the canonical /waterbody/<wbg>/ path.
    // Deselection resets to the root path.
    useEffect(() => {
        // Skip URL update during initial restoration
        if (!urlRestoredRef.current) return;
        // Don't push new history entries during popstate handling
        if (popstateInProgressRef.current) {
            popstateInProgressRef.current = false;
            return;
        }

        const props = selectedFeature?.properties;

        if (!selectedFeature) {
            clearUrlState();
            return;
        }

        // Tidal waters are ocean/coastal features — excluded from freshwater canonical URLs.
        if (props?._tidal) return;

        // Always include the current section fgid in the URL
        const sectionFgid = typeof props?.frontend_group_id === 'string' ? props.frontend_group_id : undefined;

        // Named features with a waterbody_group get the canonical /waterbody/<wbg>/ path.
        // Everything else (unnamed streams, compact-only features) gets ?f=<fgid>.
        const wbg = typeof props?.waterbody_group === 'string' ? props.waterbody_group : undefined;
        if (wbg) {
            navigateToWaterbody(wbg, sectionFgid);
        } else {
            if (sectionFgid) {
                navigateToFeature(sectionFgid, sectionFgid);
            }
        }
    }, [selectedFeature]);

    // Update document.title and meta description when a feature is selected.
    // Helps search engines index named features via JS-rendered title changes.
    useEffect(() => {
        const BASE_TITLE = 'Can I Fish This? - BC Freshwater Fishing Regulations';
        const BASE_DESC = 'Interactive map of BC freshwater fishing regulations - search streams, lakes, and find fishing rules';
        const props = selectedFeature?.properties;
        const name = props?.display_name;
        const metaDesc = document.querySelector<HTMLMetaElement>('meta[name="description"]');

        if (name && !props?._tidal) {
            const typeLabel = selectedFeature?.type === 'stream' ? 'Stream'
                : selectedFeature?.type === 'lake' ? 'Lake'
                : selectedFeature?.type === 'wetland' ? 'Wetland'
                : 'Waterbody';
            document.title = `${name} Fishing Regulations | BC Freshwater`;
            if (metaDesc) metaDesc.content = `BC freshwater fishing regulations for ${name} (${typeLabel}). View catch limits, closures, gear restrictions, and seasons.`;
        } else {
            document.title = BASE_TITLE;
            if (metaDesc) metaDesc.content = BASE_DESC;
        }
    }, [selectedFeature]);

    useEffect(() => {
        // Wait for the data version to resolve before initializing the map.
        // This ensures PMTiles URLs include the correct ?v= cache-buster.
        if (dataVersion === null) return;
        if (mapRef.current) return; // already initialized
        if (!mapContainerRef.current) return;
        const vParam = dataVersion ? `?v=${encodeURIComponent(dataVersion)}` : '';
        const map = new maplibregl.Map({
            container: mapContainerRef.current,
            maxBounds: BC_BOUNDS,
            style: {
                version: 8,
                glyphs: 'https://cdn.protomaps.com/fonts/pbf/{fontstack}/{range}.pbf',
                sprite: 'https://protomaps.github.io/basemaps-assets/sprites/v4/light',
                sources: {
                    protomaps: { type: 'vector', url: `${TILE_BASE}/bc.pmtiles${vParam}`, attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> · <a href="https://protomaps.com">Protomaps</a>', maxzoom: 15 },
                    regulations: { type: 'vector', url: `${TILE_BASE}/freshwater_atlas.pmtiles${vParam}`, attribution: '<a href="https://www2.gov.bc.ca/gov/content/data/open-data/open-government-licence-bc">OGL-BC</a>', minzoom: 4, maxzoom: 12 },
                    satellite: { type: 'raster', tiles: [SATELLITE_CONFIG.url], tileSize: SATELLITE_CONFIG.tileSize, attribution: SATELLITE_CONFIG.attribution, maxzoom: SATELLITE_CONFIG.maxzoom }
                },
                // Base map (no labels) → regulation overlays → labels on top
                // The `layers()` call without `lang` returns geometry-only layers;
                // `labelsOnly + lang` returns road / place / water name labels
                // so they render above the regulation fills and remain readable.
                // We filter out OSM water labels since we display our own.
                layers: [
                    // Satellite raster sits at the very bottom, hidden by default
                    { id: 'satellite-tiles', type: 'raster', source: 'satellite', layout: { visibility: 'none' } },
                    ...layers('protomaps', LIGHT),
                    ...createEarlyRoadLayers(),
                    ...createRegulationLayers(),
                    ...layers('protomaps', LIGHT, { labelsOnly: true, lang: 'en' })
                        .filter(l => !['water_waterway_label', 'water_label_ocean', 'water_label_lakes'].includes(l.id))
                        .map(l => {
                            const withinBC: any = ['within', bcBoundary];
                            if (!('filter' in l) || !l.filter) return { ...l, filter: withinBC };
                            return { ...l, filter: ['all', legacyFilterToExpression(l.filter), withinBC] as any };
                        }),
                    ...createAdminLabelLayers(),
                ]
            },
            center: [-123.0, 49.25], zoom: 8, maxZoom: 18, minZoom: 4, hash: true,
            // Cancel in-flight tile requests for intermediate zoom levels during
            // zoom animations — those tiles are immediately obsolete and waste bandwidth.
            cancelPendingTileRequestsWhileZooming: true,
            // Smooth zoom: allow fractional levels and ease between them
            scrollZoom: true,
            fadeDuration: 100,
            attributionControl: { compact: false }
        });

        // Add compass navigation control
        const compassPosition = isMobileViewport() ? 'top-right' : 'bottom-left';
        map.addControl(
            new maplibregl.NavigationControl({ showCompass: true, showZoom: true, visualizePitch: true }),
            compassPosition
        );

        // Add satellite toggle as a native MapLibre control
        // Desktop: bottom-left (stacked above compass).
        // Mobile: top-right (next to compass).
        const satPosition = isMobileViewport() ? 'top-right' : 'bottom-left';
        const satControl: maplibregl.IControl = {
            onAdd() {
                const container = document.createElement('div');
                container.className = 'maplibregl-ctrl maplibregl-ctrl-group';
                const btn = document.createElement('button');
                btn.className = 'satellite-toggle';
                btn.title = 'Switch to satellite view';
                btn.setAttribute('aria-label', 'Switch to satellite view');
                btn.innerHTML = LAYERS_SVG;
                btn.addEventListener('click', () => toggleSatelliteRef.current());
                container.appendChild(btn);
                satBtnRef.current = btn;
                return container;
            },
            onRemove() { satBtnRef.current = null; }
        };
        map.addControl(satControl, satPosition);

        // Add overlay opacity toggle as a small IControl button (same position as satellite).
        // Shows a sun icon; on click, toggles a compact slider popup.
        const opacityControl: maplibregl.IControl = {
            onAdd() {
                const wrapper = document.createElement('div');
                wrapper.className = 'maplibregl-ctrl maplibregl-ctrl-group overlay-opacity-ctrl';
                wrapper.style.display = 'none'; // hidden until satellite activates
                const btn = document.createElement('button');
                btn.className = 'satellite-toggle overlay-opacity-btn';
                btn.title = 'Overlay opacity';
                btn.setAttribute('aria-label', 'Overlay opacity');
                btn.innerHTML = OPACITY_SVG;
                btn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    toggleSliderRef.current();
                });
                const popup = document.createElement('div');
                popup.className = 'overlay-opacity-popup';
                popup.style.display = 'none';
                popup.innerHTML = '<label class="overlay-opacity-label"><span class="overlay-pct"></span></label>'
                    + '<input type="range" min="0" max="1" step="0.05" value="1" />';
                const input = popup.querySelector('input')!;
                input.addEventListener('input', (ev) => {
                    handleOverlayOpacityRef.current((ev as any).target.value);
                });
                wrapper.appendChild(btn);
                wrapper.appendChild(popup);
                opacityCtrlRef.current = wrapper;
                return wrapper;
            },
            onRemove() { opacityCtrlRef.current = null; }
        };
        map.addControl(opacityControl, satPosition);

        map.on('load', () => {
            const pattern = createWetlandPattern();
            if (pattern) {
                map.addImage('wetland-pattern', pattern);
                map.setPaintProperty('wetlands-fill', 'fill-pattern', 'wetland-pattern');
            }

            // Hatch patterns for no-fishing zones (National Parks + Ecological Reserves)
            const hatchDiag = createDiagonalHatchPattern('#C22E2E');
            if (hatchDiag) map.addImage('hatch-diagonal', hatchDiag);
            const hatchCross = createCrossHatchPattern('#C22E2E');
            if (hatchCross) map.addImage('hatch-cross', hatchCross);
            // Horizontal lines for partial restriction zones (research forests, etc.)
            const hatchHoriz = createHorizontalLinePattern('#CC7A00');
            if (hatchHoriz) map.addImage('hatch-horizontal', hatchHoriz);

            // Hatch overlays — appended after all static layers so they sit
            // above admin fills/lines but below the dynamic highlight layers.
            map.addLayer({
                id: 'admin_parks_nat-hatch',
                type: 'fill',
                source: 'regulations',
                'source-layer': 'parks_nat',
                paint: { 'fill-pattern': 'hatch-diagonal', 'fill-opacity': 0.60 },
            } as any);

            map.addLayer({
                id: 'eco_reserves-hatch',
                type: 'fill',
                source: 'regulations',
                'source-layer': 'eco_reserves',
                filter: ['==', ['get', 'admin_type'], 'ECOLOGICAL_RESERVE'],
                paint: { 'fill-pattern': 'hatch-cross', 'fill-opacity': 0.50 },
            } as any);

            // ── HIGHLIGHT LAYERS (hover / disambiguation) ─────────────
            // Filter-based: render directly from the PMTiles vector source.
            // All start hidden (FILTER_NONE). On hover/disambig, setGroupFilter
            // switches the match expression so only the target feature renders.
            const hlLineWidth = ['interpolate', ['linear'], ['zoom'], 4, 3, 8, 6, 12, 8] as any;
            const hlLineLayout = { 'line-cap': 'round' as const, 'line-join': 'round' as const };

            // Streams
            map.addLayer({ id: 'hl-streams', type: 'line', source: 'regulations', 'source-layer': 'streams', filter: FILTER_NONE,
                paint: { 'line-color': HIGHLIGHT_COLORS.stream, 'line-width': hlLineWidth, 'line-opacity': 1.0 }, layout: hlLineLayout });
            // Lakes
            map.addLayer({ id: 'hl-lakes-fill', type: 'fill', source: 'regulations', 'source-layer': 'lakes', filter: FILTER_NONE,
                paint: { 'fill-color': HIGHLIGHT_COLORS.lake, 'fill-opacity': 0.4 } });
            map.addLayer({ id: 'hl-lakes-line', type: 'line', source: 'regulations', 'source-layer': 'lakes', filter: FILTER_NONE,
                paint: { 'line-color': HIGHLIGHT_COLORS.lake, 'line-width': hlLineWidth, 'line-opacity': 1.0 }, layout: hlLineLayout });
            // Wetlands
            map.addLayer({ id: 'hl-wetlands-fill', type: 'fill', source: 'regulations', 'source-layer': 'wetlands', filter: FILTER_NONE,
                paint: { 'fill-color': HIGHLIGHT_COLORS.wetland, 'fill-opacity': 0.4 } });
            map.addLayer({ id: 'hl-wetlands-line', type: 'line', source: 'regulations', 'source-layer': 'wetlands', filter: FILTER_NONE,
                paint: { 'line-color': HIGHLIGHT_COLORS.wetland, 'line-width': hlLineWidth, 'line-opacity': 1.0 }, layout: hlLineLayout });
            // Manmade
            map.addLayer({ id: 'hl-manmade-fill', type: 'fill', source: 'regulations', 'source-layer': 'manmade', filter: FILTER_NONE,
                paint: { 'fill-color': HIGHLIGHT_COLORS.manmade, 'fill-opacity': 0.4 } });
            map.addLayer({ id: 'hl-manmade-line', type: 'line', source: 'regulations', 'source-layer': 'manmade', filter: FILTER_NONE,
                paint: { 'line-color': HIGHLIGHT_COLORS.manmade, 'line-width': hlLineWidth, 'line-opacity': 1.0 }, layout: hlLineLayout });

            // ── SELECTION LAYERS (active selection — uniform deep blue) ───
            const slPolyLineWidth = ['interpolate', ['linear'], ['zoom'], 4, 2, 8, 3, 12, 4] as any;

            map.addLayer({ id: 'sl-streams', type: 'line', source: 'regulations', 'source-layer': 'streams', filter: FILTER_NONE,
                paint: { 'line-color': SELECTION_COLOR, 'line-width': STREAM_LINE_WIDTH as any, 'line-opacity': 1.0 }, layout: hlLineLayout });
            map.addLayer({ id: 'sl-lakes-fill', type: 'fill', source: 'regulations', 'source-layer': 'lakes', filter: FILTER_NONE,
                paint: { 'fill-color': SELECTION_COLOR, 'fill-opacity': 0.35 } });
            map.addLayer({ id: 'sl-lakes-line', type: 'line', source: 'regulations', 'source-layer': 'lakes', filter: FILTER_NONE,
                paint: { 'line-color': SELECTION_COLOR, 'line-width': slPolyLineWidth, 'line-opacity': 1.0 }, layout: hlLineLayout });
            map.addLayer({ id: 'sl-wetlands-fill', type: 'fill', source: 'regulations', 'source-layer': 'wetlands', filter: FILTER_NONE,
                paint: { 'fill-color': SELECTION_COLOR, 'fill-opacity': 0.35 } });
            map.addLayer({ id: 'sl-wetlands-line', type: 'line', source: 'regulations', 'source-layer': 'wetlands', filter: FILTER_NONE,
                paint: { 'line-color': SELECTION_COLOR, 'line-width': slPolyLineWidth, 'line-opacity': 1.0 }, layout: hlLineLayout });
            map.addLayer({ id: 'sl-manmade-fill', type: 'fill', source: 'regulations', 'source-layer': 'manmade', filter: FILTER_NONE,
                paint: { 'fill-color': SELECTION_COLOR, 'fill-opacity': 0.35 } });
            map.addLayer({ id: 'sl-manmade-line', type: 'line', source: 'regulations', 'source-layer': 'manmade', filter: FILTER_NONE,
                paint: { 'line-color': SELECTION_COLOR, 'line-width': slPolyLineWidth, 'line-opacity': 1.0 }, layout: hlLineLayout });
            
            map.addSource('cursor-circle', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
            map.addLayer({ id: 'cursor-circle-fill', type: 'fill', source: 'cursor-circle', paint: { 'fill-color': '#7C3AED', 'fill-opacity': 0.1 } });
            map.addLayer({ id: 'cursor-circle-line', type: 'line', source: 'cursor-circle', paint: { 'line-color': '#7C3AED', 'line-width': 1.5, 'line-opacity': 0.6 } });

            // ── UNGAZETTED POINT MARKER (GeoJSON — no tile source) ────
            map.addSource('ungazetted-marker', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
            map.addLayer({ id: 'ungazetted-marker-ring', type: 'circle', source: 'ungazetted-marker',
                paint: { 'circle-radius': 18, 'circle-color': 'transparent', 'circle-stroke-color': SELECTION_COLOR, 'circle-stroke-width': 2.5, 'circle-stroke-opacity': 0.7 } });
            map.addLayer({ id: 'ungazetted-marker-dot', type: 'circle', source: 'ungazetted-marker',
                paint: { 'circle-radius': 7, 'circle-color': SELECTION_COLOR, 'circle-opacity': 0.85 } });
            map.addLayer({ id: 'ungazetted-marker-label', type: 'symbol', source: 'ungazetted-marker',
                layout: { 'text-field': ['get', 'display_name'], 'text-font': ['Noto Sans Regular'], 'text-size': 13,
                    'text-offset': [0, 2.2], 'text-anchor': 'top', 'text-max-width': 12 },
                paint: { 'text-color': '#1a1a2e', 'text-halo-color': '#ffffff', 'text-halo-width': 1.5 } });
            
            // Signal that map is ready for URL restoration
            setMapReady(true);
        });

        // -------------------------------------------------------------
        // NEW FIX: Listen for user-initiated pans and close the menu on desktop
        // -------------------------------------------------------------
        map.on('movestart', (e) => {
            // originalEvent ensures the move was triggered by a user (drag, wheel, keyboard),
            // not a programmatic map.flyTo() or map.fitBounds()
            if (!e.originalEvent) return;
            const isMobile = isMobileViewport();
            // Disambig menu — close fully on any user pan
            if (isDisambigOpenRef.current) {
                clearSelection();
            }
            // Info panel: partially collapse on mobile when user pans away
            if (isMobile && selectedFeatureRef.current && mobilePanelStateRef.current === 'expanded') {
                setMobilePanelState('partial');
            }
        });

        map.on('mousemove', (e) => {
            if (!map.isStyleLoaded()) return;
            cursorLngLatRef.current = e.lngLat;
            const features = map.queryRenderedFeatures([[e.point.x - 10, e.point.y - 10], [e.point.x + 10, e.point.y + 10]], { layers: INTERACTABLE_LAYERS });
            map.getCanvas().style.cursor = features.length > 0 ? 'pointer' : '';
            (map.getSource('cursor-circle') as maplibregl.GeoJSONSource)?.setData({ type: 'FeatureCollection', features: [{ type: 'Feature', geometry: createCirclePolygon(e.lngLat, map.getZoom()), properties: {} }] });
        });

        // Re-draw cursor circle on zoom so it resizes without requiring mouse movement
        map.on('zoom', () => {
            if (!cursorLngLatRef.current) return;
            const src = map.getSource('cursor-circle') as maplibregl.GeoJSONSource;
            src?.setData({ type: 'FeatureCollection', features: [{ type: 'Feature', geometry: createCirclePolygon(cursorLngLatRef.current, map.getZoom()), properties: {} }] });
        });

        map.on('click', async (e) => {
            // V2 click handler: resolve tile fid/wbk → reach via /api/resolve.
            const features = map.queryRenderedFeatures(
                [[e.point.x - 15, e.point.y - 15], [e.point.x + 15, e.point.y + 15]],
                { layers: INTERACTABLE_LAYERS }
            );
            if (!features.length) {
                // Click on blank map area — close both the info panel and disambig menu.
                clearSelection();
                return;
            }

            // Close disambig if open
            if (isDisambigOpenRef.current) {
                clearSelection();
            }

            const regData = regDataRef.current;
            if (!regData) return; // data not loaded yet

            // Collect fids (streams) and wbks (polygons) from clicked tile features
            const inputs: Array<{ fid?: string; wbk?: string; tile: maplibregl.MapGeoJSONFeature }> = [];
            const fids: string[] = [];
            const wbks: string[] = [];

            for (const feat of features) {
                const props = feat.properties;
                const srcLayer = feat.sourceLayer;
                if (srcLayer === 'streams') {
                    const fid = String(props.fid ?? '');
                    if (fid) {
                        inputs.push({ fid, tile: feat });
                        if (!fids.includes(fid)) fids.push(fid);
                    }
                } else {
                    const wbk = String(props.waterbody_key ?? '');
                    if (wbk) {
                        inputs.push({ wbk, tile: feat });
                        if (!wbks.includes(wbk)) wbks.push(wbk);
                    }
                }
            }

            if (!fids.length && !wbks.length) {
                console.debug('[Map] clicked feature has no fid/wbk:', features[0].properties);
                return;
            }

            // Increment click generation to detect stale results
            const thisClick = ++clickGenRef.current;
            showSpinnerDelayed({ x: e.point.x, y: e.point.y });
            // Hide the map-layer cursor circle so it doesn't drift under the spinner
            (map.getSource('cursor-circle') as maplibregl.GeoJSONSource)?.setData({ type: 'FeatureCollection', features: [] });

            let resolved: ResolveResult[];
            try {
                // In dev, add a delay so the click spinner is visible for testing.
                if (import.meta.env.DEV) await new Promise(r => setTimeout(r, 1500));
                resolved = await waterbodyDataService.resolve(fids, wbks);
            } catch (err) {
                console.error('[Map] resolve failed:', err);
                hideSpinner();
                return;
            }

            hideSpinner();

            // Discard stale click (user clicked elsewhere while resolving)
            if (clickGenRef.current !== thisClick) return;

            // Map resolved results back to tile features via matched_fids/matched_wbks
            const seen = new Set<string>();
            const candidates: { reachId: string; feature: maplibregl.MapGeoJSONFeature }[] = [];

            for (const r of resolved) {
                if (seen.has(r.reach_id)) continue;
                seen.add(r.reach_id);

                // Find the tile feature that triggered this result
                const tf = inputs.find(inp =>
                    (inp.fid && (r.matched_fids || r.fids).includes(inp.fid)) ||
                    (inp.wbk && (r.matched_wbks || []).includes(inp.wbk))
                );
                if (tf) candidates.push({ reachId: r.reach_id, feature: tf.tile });
            }

            if (candidates.length === 0) {
                console.debug('[Map] resolve returned no matching reaches for:', fids, wbks);
                return;
            }

            // Build FeatureOptions from candidates
            const options: FeatureOption[] = candidates.map(({ reachId, feature: tileFeature }) => {
                const lookup = searchLookupRef.current.get(reachId);
                const sf = lookup?.feature;
                const seg = lookup?.segment;

                if (sf && seg) {
                    return buildFeatureFromJSON(sf, seg, {
                        geometry: (tileFeature.geometry || (tileFeature as any).toJSON?.().geometry) as FeatureGeometry,
                        source: tileFeature.source,
                        sourceLayer: tileFeature.sourceLayer,
                        fidList: regData.reachSegments[reachId],
                    });
                }

                // Reach not in search index (unnamed zone-only feature).
                // resolve() already populated regData.reaches[reachId].
                const reach = regData.reaches[reachId];
                if (!reach) return null;
                return buildFeatureFromReach(reach, reachId, regData.reg_sets, regData.reachSegments, {
                    geometry: (tileFeature.geometry || (tileFeature as any).toJSON?.().geometry) as FeatureGeometry,
                    source: tileFeature.source,
                    sourceLayer: tileFeature.sourceLayer,
                    waterbodyKey: tileFeature.properties.waterbody_key || '',
                });
            }).filter(Boolean) as FeatureOption[];

            if (options.length === 0) return;

            if (options.length === 1) {
                // Single reach — select directly
                clearSelection();
                setSelectedFeature(options[0]);
            } else {
                // Multiple overlapping reaches — show disambiguation menu
                clearSelection();
                setDisambigOptions(options);
                setDisambigPosition({ x: e.point.x, y: e.point.y });
                isDisambigOpenRef.current = true;
                if (isMobileViewport()) setMobilePanelState('partial');
            }
        });

        mapRef.current = map;
        return () => map.remove();
    }, [clearSelection, dataVersion]);

    // Handle highlighted state changes — filter-based, no event listeners needed.
    // MapLibre re-evaluates filters automatically as tiles load/zoom changes.
    useEffect(() => {
        const map = mapRef.current;
        if (!map) return;
        setGroupFilter(map, HL_LAYER_IDS, highlightedOption);
    }, [highlightedOption]);

    // Handle selected state changes — filter-based, no event listeners needed.
    useEffect(() => {
        const map = mapRef.current;
        if (!map) return;
        setGroupFilter(map, SL_LAYER_IDS, selectedFeature);

        // Sync ungazetted point marker with selection state
        const ugSrc = map.getSource('ungazetted-marker') as maplibregl.GeoJSONSource | undefined;
        if (ugSrc) {
            if (selectedFeature?.type === 'ungazetted' && selectedFeature.bbox && isValidBbox(selectedFeature.bbox)) {
                const [lng, lat] = [(selectedFeature.bbox[0] + selectedFeature.bbox[2]) / 2, (selectedFeature.bbox[1] + selectedFeature.bbox[3]) / 2];
                ugSrc.setData({ type: 'FeatureCollection', features: [
                    { type: 'Feature', geometry: { type: 'Point', coordinates: [lng, lat] },
                      properties: { display_name: selectedFeature.properties.display_name || '' } }
                ] });
            } else {
                ugSrc.setData({ type: 'FeatureCollection', features: [] });
            }
        }
    }, [selectedFeature]);

    // Derive sibling section tabs whenever the selection changes.
    // V2: A SearchableFeature has multiple regulation_segments (one per reach).
    // We synthesize one SearchableFeature per segment so InfoPanel can render
    // them as tabs. Each synthetic sibling is a shallow clone of the parent
    // with only the one segment it represents.
    useEffect(() => {
        if (!selectedFeature) {
            setSiblingFeatures([]);
            return;
        }

        // Resolve the parent SearchableFeature via the search lookup,
        // keyed by the selected segment's reach_id (stored as frontend_group_id).
        const reachId = selectedFeature.properties.frontend_group_id as string | undefined;
        const lookup = reachId ? searchLookupRef.current.get(reachId) : undefined;
        const parentFeature = lookup?.feature;
        const segments = parentFeature?.regulation_segments ?? [];

        if (segments.length <= 1) {
            // Single-segment waterbody — pass parent through as-is. No tab bar rendered.
            setSiblingFeatures(parentFeature ? [parentFeature] : []);
            return;
        }

        // Multi-segment: one synthetic sibling per segment so InfoPanel renders the tab bar.
        setSiblingFeatures(segments.map(seg => ({ ...parentFeature, regulation_segments: [seg] } as SearchableFeature)));
    }, [selectedFeature]);

    /** Fly to a bbox with correct zoom — single function used by search, URL restore, and zoom button. */
    const flyToBox = useCallback((bbox: [number, number, number, number], minZoom: number) => {
        const map = mapRef.current;
        if (!map) return;
        const isMobile = isMobileViewport();
        const bounds = new maplibregl.LngLatBounds([bbox[0], bbox[1]], [bbox[2], bbox[3]]);
        let padding: maplibregl.PaddingOptions;
        let panelState: CollapseState = 'expanded';
        if (isMobile) {
            ({ padding, panelState } = getMobilePaddingForBounds(bounds));
        } else {
            padding = { top: 80, bottom: 80, left: 80, right: 80 };
        }
        flyToBbox(map, bbox, padding, minZoom + 0.25);
        if (isMobile) setMobilePanelState(panelState);
    }, []);

    const handleSearchSelect = useCallback((feature: SearchableFeature) => {
        const map = mapRef.current;
        if (!map) return;

        // Clean up old state WITHOUT nulling selectedFeature (avoids panel
        // close/open cycle which causes canvas resize during the fly animation).
        setHighlightedOption(null);
        setHighlightedSearchResult(null);
        setDisambigOptions([]);
        setDisambigPosition(null);
        isDisambigOpenRef.current = false;
        if (searchPollRef.current) {
            clearInterval(searchPollRef.current);
            searchPollRef.current = null;
        }
        const cursorSrc = map.getSource('cursor-circle') as maplibregl.GeoJSONSource;
        if (cursorSrc) cursorSrc.setData({ type: 'FeatureCollection', features: [] });
        setGroupFilter(map, HL_LAYER_IDS, null);
        
        // Check if this waterbody has multiple regulation segments (reaches)
        const segments = feature.regulation_segments || [];
        const hasMultipleSegments = segments.length > 1;

        // Named waterbodies (with a wbg) use InfoPanel tabs for section switching.
        // DisambiguationMenu is only shown for the map-click path (wbg-less features),
        // not for the search path.
        const rawWbg = feature.properties?.waterbody_group;
        const hasWbg = typeof rawWbg === 'string' && rawWbg.length > 0;

        const regData = regDataRef.current;

        // Build the selected feature first so fly-to uses the same bbox
        // as the "Zoom to section" button.
        const seg = segments[0] || null;
        const reachId = seg?.frontend_group_id || '';
        const fidList = reachId && regData ? regData.reachSegments[reachId] : undefined;
        const selected = buildFeatureFromJSON(feature, seg, { fidList });

        // Fly to the entire feature bbox (not just one segment) so long
        // streams fit in the viewport.  Use the lowest min_zoom across all
        // segments — only clamp if EVERY segment would be invisible at the
        // bbox-fitting zoom.
        const flyBbox = feature.bbox as [number, number, number, number] | undefined
            ?? selected.bbox;
        const featureMinZoom = segments.length > 0
            ? Math.min(...segments.map(s => s.min_zoom ?? 4))
            : (feature.min_zoom ?? 4);
        if (flyBbox && isValidBbox(flyBbox)) {
            flyToBox(flyBbox, featureMinZoom);
        }

        if (hasMultipleSegments && !hasWbg) {
            // Build disambiguation options from JSON via the unified builder.
            const options: FeatureOption[] = segments.map(s => {
                const rid = s.frontend_group_id;
                const fids = regData?.reachSegments[rid];
                return buildFeatureFromJSON(feature, s, { fidList: fids });
            });

            const screenCenter = { x: window.innerWidth / 2, y: window.innerHeight / 3 };
            setDisambigOptions(options);
            setDisambigPosition(screenCenter);
            isDisambigOpenRef.current = true;
            setMobilePanelState('partial');
        } else {
            setSelectedFeature(selected);

            // Ungazetted features have no tile geometry — show GeoJSON marker instead of polling
            if (selected.type === 'ungazetted') {
                if (selected.bbox && isValidBbox(selected.bbox)) {
                    const [lng, lat] = [(selected.bbox[0] + selected.bbox[2]) / 2, (selected.bbox[1] + selected.bbox[3]) / 2];
                    const ugSrc = map.getSource('ungazetted-marker') as maplibregl.GeoJSONSource;
                    if (ugSrc) ugSrc.setData({ type: 'FeatureCollection', features: [
                        { type: 'Feature', geometry: { type: 'Point', coordinates: [lng, lat] },
                          properties: { display_name: selected.properties.display_name || '' } }
                    ] });
                }
            } else {
            // Poll until tiles load to get geometry for highlighting
            const srcLayer = selected.sourceLayer || resolveSourceLayer(selected.type);
            const filter = buildFeatureFilter(selected);
            let attempts = 0;
            searchPollRef.current = setInterval(() => {
                attempts++;
                let found: any[] = filter
                    ? map.querySourceFeatures('regulations', { sourceLayer: srcLayer, filter: filter as maplibregl.FilterSpecification })
                    : [];
                
                if (found.length === 0 && feature.bbox) {
                    const pt = map.project([(feature.bbox[0]+feature.bbox[2])/2, (feature.bbox[1]+feature.bbox[3])/2]);
                    const hits = map.queryRenderedFeatures(pt, { layers: INTERACTABLE_LAYERS });
                    if (hits.length > 0) found = hits;
                }
                
                if (found.length > 0 || attempts > 20) {
                    if (found.length > 0) {
                        const bestTile = found.reduce((best, tile) => {
                            const bestOrder = best.properties?.stream_order || 0;
                            const tileOrder = tile.properties?.stream_order || 0;
                            return tileOrder > bestOrder ? tile : best;
                        }, found[0]);
                        
                        setSelectedFeature(prev => prev ? { 
                            ...prev, 
                            geometry: (bestTile.geometry || bestTile.toJSON?.().geometry) as FeatureGeometry, 
                            id: bestTile.id,
                        } : null);
                    }
                    if (searchPollRef.current) clearInterval(searchPollRef.current);
                }
            }, 200);
            }
        }
    }, [clearSelection]);

    /**
     * Update map selection highlight to a different section WITHOUT flying or
     * changing selectedFeature.  Called by InfoPanel tab clicks.
     */
    const handleHighlightSection = useCallback((sf: SearchableFeature) => {
        const map = mapRef.current;
        if (!map) return;
        const seg = sf.regulation_segments?.[0] || null;
        const reachId = seg?.frontend_group_id || '';
        const regData = regDataRef.current;
        const fidList = reachId && regData ? regData.reachSegments[reachId] : undefined;
        const built = buildFeatureFromJSON(sf, seg, { fidList });
        setGroupFilter(map, SL_LAYER_IDS, built);
    }, []);

    /**
     * Fly to a specific section bbox. Called by the InfoPanel "Zoom to section" button.
     * Does not change selectedFeature or close the panel.
     */
    const handleFlyToSection = useCallback((bbox: [number, number, number, number], minZoom: number) => {
        flyToBox(bbox, minZoom);
    }, [flyToBox]);

    return (
        <div className="map-container">
            <div ref={mapContainerRef} className="map-canvas" />
            {!(dataLoaded && filtersApplied) && (
                <div className="loading-overlay" role="status" aria-live="polite">
                    <div className="loading-spinner" aria-hidden="true" />
                    <p className="loading-text">Loading waterbody data…</p>
                </div>
            )}
            <div className="map-menu-wrapper">
                <SearchBar 
                    features={searchableFeatures} 
                    onSelect={handleSearchSelect} 
                    highlightedResult={highlightedSearchResult} 
                    onHighlight={f => { 
                        setHighlightedSearchResult(f); 
                        setHighlightedOption(f as FeatureOption | null); 
                    }} 
                    onSearchActive={() => {
                        // Fully close InfoPanel and disambig when user starts searching
                        if (disambigOptions.length > 0) {
                            setDisambigOptions([]);
                            setDisambigPosition(null);
                            isDisambigOpenRef.current = false;
                        }
                        if (selectedFeature && isMobileViewport()) {
                            clearSelection();
                        }
                    }}
                    placeholder="Search waterbodies..." 
                />
            </div>
            <InfoPanel feature={selectedFeature} onClose={clearSelection} collapseState={mobilePanelState} onSetCollapseState={setMobilePanelState} siblingFeatures={siblingFeatures} onHighlightSection={handleHighlightSection} onFlyToSection={handleFlyToSection} />
            <DisclaimerLink onClick={() => setDisclaimerOpen(true)} />
            <Disclaimer isOpen={disclaimerOpen} onClose={() => setDisclaimerOpen(false)} />
            {disambigOptions.length > 0 && (
                <DisambiguationMenu 
                    options={disambigOptions} position={disambigPosition} highlightedOption={highlightedOption}

                    onHighlight={(option) => {
                        if (hoverTimeoutRef.current) clearTimeout(hoverTimeoutRef.current);
                        if (option) {
                            hoverTimeoutRef.current = setTimeout(() => {
                                setHighlightedOption(option);
                            }, 50);
                        } else {
                            setHighlightedOption(null);
                        }
                    }}
                    onSelect={f => { 
                        clearSelection(); 
                        setSelectedFeature(f); 
                        setMobilePanelState('partial'); 
                    }} onClose={clearSelection}
                />
            )}
            {clickLoadingPos && (
                <div
                    className="click-loading-spinner"
                    aria-label="Loading feature data"
                />
            )}
        </div>
    );
};

export default MapComponent;