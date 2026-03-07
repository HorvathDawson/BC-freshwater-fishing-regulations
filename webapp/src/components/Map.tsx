import { useEffect, useRef, useState, useCallback } from 'react';
import maplibregl from 'maplibre-gl';
import { Protocol } from 'pmtiles';
import { layers, LIGHT } from '@protomaps/basemaps';
import 'maplibre-gl/dist/maplibre-gl.css';
import { createRegulationLayers, createAdminLabelLayers, createEarlyRoadLayers, HIGHLIGHT_COLORS, SELECTION_COLOR } from '../map/styles';
import { waterbodyDataService } from '../services/waterbodyDataService';
import type { WaterbodyItem } from '../services/waterbodyDataService';
import { 
    isMobileViewport,
    type FeatureInfo, 
    type FeatureOption, 
    type FeatureGeometry,
    type CollapseState 
} from '../utils/featureUtils';
import { parseUrlState, updateUrlState, clearUrlState } from '../utils/urlState';
import InfoPanel from './InfoPanel';
import DisambiguationMenu from './DisambiguationMenu';
import SearchBar from './SearchBar';
import Disclaimer, { DisclaimerLink } from './Disclaimer';
import type { SearchableFeature, RegulationSegment } from './SearchBar';
import './Map.css';

// --- CONFIG & PROTOCOL ---
const protocol = new Protocol();
maplibregl.addProtocol('pmtiles', protocol.tile);

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

// ESRI World Imagery satellite raster tile URL (free, no API key)
const ESRI_SATELLITE_URL = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}';

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

const INTERACTABLE_LAYERS = ['streams', 'lakes-fill', 'wetlands-fill', 'manmade-fill', 'ungazetted-circle'];
const ADMIN_FILL_LAYERS = [
    'admin_parks_nat-fill', 'admin_parks_bc-fill', 'admin_wma-fill',
    'admin_watersheds-fill', 'admin_historic_sites-fill', 'admin_osm_admin_boundaries-fill',
];

// Filter-based highlight / selection layer IDs.
// These render directly from the 'regulations' PMTiles source — no geometry
// copying. MapLibre handles tile clipping and buffer overlap natively, so
// fills and strokes render without seam or double-opacity artifacts.
const HL_LAYER_IDS = [
    'hl-streams', 'hl-lakes-fill', 'hl-lakes-line',
    'hl-wetlands-fill', 'hl-wetlands-line', 'hl-manmade-fill', 'hl-manmade-line',
    'hl-ungazetted',
];
const SL_LAYER_IDS = [
    'sl-streams', 'sl-lakes-fill', 'sl-lakes-line',
    'sl-wetlands-fill', 'sl-wetlands-line', 'sl-manmade-fill', 'sl-manmade-line',
    'sl-ungazetted',
];
/** Matches nothing — used to hide highlight/selection layers when inactive. */
const FILTER_NONE: maplibregl.FilterSpecification = ['==', ['get', 'frontend_group_id'], ''];

// --- STYLE EXPRESSIONS ---
const STREAM_LINE_WIDTH = ['interpolate', ['linear'], ['zoom'], 4, ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.1]], 8, ['+', 2, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.15]], 11, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 1.5], 12, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 2], 14, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 3], 16, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 4]];

// --- TYPES ---
interface LayerVisibility {
    streams: boolean; lakes: boolean; wetlands: boolean; manmade: boolean; ungazetted: boolean; regions: boolean;
    management_units: boolean;
    admin_parks_nat: boolean; admin_parks_bc: boolean; admin_wma: boolean;
    admin_watersheds: boolean; admin_historic_sites: boolean; admin_osm_admin_boundaries: boolean;
}

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
 */
const flyToBbox = (
    map: maplibregl.Map,
    bbox: [number, number, number, number],
    padding: maplibregl.PaddingOptions,
    minzoom: number,
    duration = 800,
) => {
    if (isPointBbox(bbox)) {
        map.flyTo({
            center: [bbox[0], bbox[1]],
            zoom: Math.min(minzoom, 15),
            padding,
            duration,
        });
    } else {
        const bounds = new maplibregl.LngLatBounds([bbox[0], bbox[1]], [bbox[2], bbox[3]]);
        const camera = map.cameraForBounds(bounds, { padding });
        if (camera) {
            const finalZoom = Math.max(camera.zoom || 0, minzoom);
            map.flyTo({ ...camera, zoom: Math.min(finalZoom, 15), duration });
        }
    }
};

const extendBoundsWithGeometry = (bounds: maplibregl.LngLatBounds, geometry: FeatureGeometry | null | undefined) => {
    if (!geometry || !geometry.coordinates) return;
    const processCoords = (coords: number[] | number[][] | number[][][] | number[][][][]) => {
        if (Array.isArray(coords) && typeof coords[0] === 'number') bounds.extend(coords as [number, number]);
        else if (Array.isArray(coords)) (coords as any[]).forEach(processCoords);
    };
    if (geometry.type === 'Point') bounds.extend(geometry.coordinates as any);
    else if (geometry.type === 'LineString') (geometry.coordinates as any[]).forEach((coord: number[] | number[][]) => bounds.extend(coord as [number, number]));
    else processCoords(geometry.coordinates);
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
 * Subtle but visible — indicates a restricted / no-fishing zone.
 */
const createDiagonalHatchPattern = (hexColor: string): ImageData | null => {
    const size = 24, spacing = 12;
    const canvas = document.createElement('canvas');
    canvas.width = size; canvas.height = size;
    const ctx = canvas.getContext('2d');
    if (!ctx) return null;
    const [r, g, b] = parseHex(hexColor);
    ctx.strokeStyle = `rgba(${r},${g},${b},0.5)`;
    ctx.lineWidth = 1.2;
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
    const size = 20, spacing = 10;
    const canvas = document.createElement('canvas');
    canvas.width = size; canvas.height = size;
    const ctx = canvas.getContext('2d');
    if (!ctx) return null;
    const [r, g, b] = parseHex(hexColor);
    ctx.strokeStyle = `rgba(${r},${g},${b},0.5)`;
    ctx.lineWidth = 1;
    for (let i = -size; i < size * 2; i += spacing) {
        ctx.beginPath(); ctx.moveTo(i, 0); ctx.lineTo(i + size, size); ctx.stroke();
    }
    for (let i = -size; i < size * 2; i += spacing) {
        ctx.beginPath(); ctx.moveTo(i + size, 0); ctx.lineTo(i, size); ctx.stroke();
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

/**
 * Build a FeatureOption from JSON data exclusively.
 *
 * This is the **single** way menu/panel data should be constructed.
 * Tiles only provide:
 *   - geometry (for map highlighting)
 *   - _adminZones (computed at click-time from admin boundary tiles)
 *
 * Everything else (names, regulation_ids, zones, etc.) comes from the
 * SearchableFeature + RegulationSegment looked up via frontend_group_id.
 */
const buildFeatureFromJSON = (
    feature: SearchableFeature,
    segment: RegulationSegment | null,
    opts: {
        geometry?: FeatureGeometry;
        source?: string;
        sourceLayer?: string;
        /** Override frontend_group_id (e.g. from tile props when segment is null) */
        frontendGroupId?: string;
        /** Click-time extras (e.g. _adminZones) — never overwrites JSON fields */
        extras?: Record<string, any>;
    } = {},
): FeatureOption => {
    const type = normalizeType(feature.type);
    const srcLayer = opts.sourceLayer || resolveSourceLayer(type);
    const segBbox = segment?.bbox as [number, number, number, number] | undefined;
    const fgid = opts.frontendGroupId || segment?.frontend_group_id || '';

    return {
        type,
        id: fgid || segment?.group_id || feature.id,
        geometry: opts.geometry,
        source: opts.source || 'regulations',
        sourceLayer: srcLayer,
        bbox: segBbox || feature.bbox as [number, number, number, number],
        minzoom: Number(feature.min_zoom || feature.properties?.minzoom || 4),
        properties: {
            // ── Names (segment-specific where available) ──
            display_name: segment?.display_name || feature.display_name || '',
            gnis_name: feature.gnis_name || '',
            name_variants: (segment?.name_variants || feature.name_variants || []) as any,

            // ── IDs ──
            frontend_group_id: fgid,
            group_id: segment?.group_id || feature.properties?.group_id || '',
            waterbody_key: feature.properties?.waterbody_key || '',

            // ── Regulation data ──
            regulation_ids: segment?.regulation_ids || feature.properties?.regulation_ids || '',
            regulation_count: feature.properties?.regulation_count || 0,

            // ── Geography ──
            zones: feature.properties?.zones || '',
            region_name: feature.properties?.region_name || '',
            mgmt_units: feature.properties?.mgmt_units || '',
            fwa_watershed_code: feature.properties?.fwa_watershed_code || '',
            minzoom: feature.min_zoom || feature.properties?.minzoom || 4,
            total_length_km: feature.properties?.total_length_km || 0,
            length_km: segment?.length_km || 0,

            // ── Click-time extras (admin zones, etc.) ──
            ...(opts.extras || {}),
        },
    };
};

const buildFeatureFilter = (feature: FeatureInfo | FeatureOption): unknown[] | null => {
    const props = feature.properties || {};
    
    // Primary: Use frontend_group_id for consistent highlighting
    // This groups by watershed_code + gnis_name + regulation_ids
    const frontendGroupId = props.frontend_group_id;
    if (frontendGroupId) {
        return ['==', ['get', 'frontend_group_id'], frontendGroupId];
    }
    
    // Fallback for older data: use group_id
    const groupId = props.group_id;
    if (groupId) {
        return ['==', ['get', 'group_id'], groupId];
    }
    
    // Fallback for unnamed/unidentified features
    if (props.linear_feature_id) return ['==', ['get', 'linear_feature_id'], props.linear_feature_id];
    if (props.waterbody_key) return ['==', ['get', 'waterbody_key'], props.waterbody_key];
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
    // Lookup map: frontend_group_id → { feature, segment }
    // Populated once search_index.json loads; used by the click handler
    // to enrich tile features with search-level data (name_variants, etc.)
    const searchLookupRef = useRef<Map<string, { feature: SearchableFeature; segment: RegulationSegment | null }>>(new Map());
    // Deduped regulation-id strings, indexed by reg_set_index.
    const regSetsRef = useRef<string[]>([]);
    // frontend_group_id → reg_set_index for unnamed zone-only features.
    const compactRef = useRef<Record<string, number>>({});
    
    const [selectedFeature, setSelectedFeature] = useState<FeatureInfo | null>(null);
    const [disambigOptions, setDisambigOptions] = useState<FeatureOption[]>([]);
    const [disambigPosition, setDisambigPosition] = useState<{ x: number; y: number } | null>(null);

    const [mobilePanelState, setMobilePanelState] = useState<CollapseState>('expanded');
    const [highlightedOption, setHighlightedOption] = useState<FeatureOption | null>(null);
    const [highlightedSearchResult, setHighlightedSearchResult] = useState<SearchableFeature | null>(null);
    const [searchableFeatures, setSearchableFeatures] = useState<SearchableFeature[]>([]);
    const [mapReady, setMapReady] = useState(false);
    const [disclaimerOpen, setDisclaimerOpen] = useState(false);
    const [isSatellite, setIsSatellite] = useState(false);
    const [overlayOpacity, setOverlayOpacity] = useState(1);
    const [sliderOpen, setSliderOpen] = useState(false);
    const [_layerVisibility, _setLayerVisibility] = useState<LayerVisibility>({
        streams: true, lakes: true, wetlands: true, manmade: true, ungazetted: true, regions: true,
        management_units: true,
        admin_parks_nat: true, admin_parks_bc: true, admin_wma: true,
        admin_watersheds: true, admin_historic_sites: true, admin_osm_admin_boundaries: true,
    });
    void _layerVisibility; void _setLayerVisibility;

    // Mirror state → refs so map event-handler closures always see the latest values.
    // (State setters are stable; refs are mutable — this is the canonical React pattern.)
    useEffect(() => { selectedFeatureRef.current = selectedFeature; }, [selectedFeature]);
    useEffect(() => { mobilePanelStateRef.current = mobilePanelState; }, [mobilePanelState]);

    // When the mobile panel is re-expanded from partial while a
    // feature is selected, fly back to that feature so it's visible again.
    // Aspect-ratio picks padding: tall features use partial-size padding so
    // they frame well; squarish features use full expanded padding.
    // Panel state is never changed — the user chose to expand.
    useEffect(() => {
        const prev = prevMobilePanelStateRef.current;
        prevMobilePanelStateRef.current = mobilePanelState;

        if (
            mobilePanelState === 'expanded' &&
            prev !== 'expanded' &&
            isMobileViewport()
        ) {
            const map = mapRef.current;
            const feat = selectedFeatureRef.current;
            if (!map || !feat) return;

            const targetMin = (feat.minzoom || 10) + 0.25;
            if (feat.bbox) {
                const bounds = new maplibregl.LngLatBounds([feat.bbox[0], feat.bbox[1]], [feat.bbox[2], feat.bbox[3]]);
                const { padding } = getMobilePaddingForBounds(bounds);
                flyToBbox(map, feat.bbox, padding, targetMin, 600);
            } else if (feat.geometry) {
                const bounds = new maplibregl.LngLatBounds();
                extendBoundsWithGeometry(bounds, feat.geometry);
                if (!bounds.isEmpty()) {
                    const { padding } = getMobilePaddingForBounds(bounds);
                    const camera = map.cameraForBounds(bounds, { padding });
                    if (camera) {
                        const finalZoom = Math.max(camera.zoom || 0, targetMin);
                        map.flyTo({ ...camera, zoom: Math.min(finalZoom, 15), duration: 600 });
                    }
                }
            }
        }
    }, [mobilePanelState]);

    // On desktop, shrink the map viewport when the info panel opens so it
    // doesn't overlap.  The CSS transition on .map-canvas is 200ms, so we
    // resize the MapLibre canvas after that transition completes.
    useEffect(() => {
        const container = mapContainerRef.current;
        if (!container) return;
        const panelOpen = selectedFeature !== null;
        container.closest('.map-container')?.classList.toggle('panel-open', panelOpen);
        // Resize after the CSS width transition (200ms) finishes
        const timer = setTimeout(() => { mapRef.current?.resize(); }, 220);
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
        // Reset highlight & selection layer filters to hide them
        setGroupFilter(map, HL_LAYER_IDS, null);
        setGroupFilter(map, SL_LAYER_IDS, null);
    }, []);

    useEffect(() => {
        Promise.all([
            waterbodyDataService.getWaterbodies(),
            waterbodyDataService.getRegSets(),
            waterbodyDataService.getCompact(),
        ]).then(([waterbodies, regSets, compact]) => {
            // Stash reg_sets / compact for click-time resolution of unnamed features
            regSetsRef.current = regSets || [];
            compactRef.current = compact || {};

            // Use backend search index directly - it's already grouped by physical stream
            // Only named features are in the waterbodies array (unnamed are in compact dict)
            const features: SearchableFeature[] = (waterbodies || []).map((item: WaterbodyItem) => {
                const normalizedType = item.type === 'streams' ? 'stream' : item.type === 'lakes' ? 'lake' : item.type;
                const typeLabel = normalizedType === 'stream' ? 'Stream' : normalizedType === 'lake' ? 'Lake' : normalizedType === 'wetland' ? 'Wetland' : 'Waterbody';
                const displayName = item.display_name || item.gnis_name || `Unnamed ${typeLabel}`;
                
                return {
                    id: item.id,
                    display_name: displayName,
                    gnis_name: item.gnis_name,
                    name: displayName,
                    type: normalizedType,
                    name_variants: item.name_variants || [],
                    regulation_segments: item.regulation_segments || [],
                    _frontend_group_ids: item.frontend_group_ids || [],
                    properties: {
                        ...item.properties,
                        zones: item.zones || '',
                        mgmt_units: item.mgmt_units,
                        region_name: item.region_name || '',
                        regulation_ids: item.regulation_ids,
                        fwa_watershed_code: item.properties?.fwa_watershed_code || '',
                        minzoom: item.min_zoom || 4,
                        total_length_km: item.total_length_km || 0,
                    },
                    bbox: isValidBbox(item.bbox) ? item.bbox : undefined,
                } as SearchableFeature;
            });
            
            setSearchableFeatures(features);
            
            // Build lookup indexed by frontend_group_id → {feature, segment}
            // Uses top-level frontend_group_ids + per-segment frontend_group_id for segment resolution
            const lookup = new Map<string, { feature: SearchableFeature; segment: RegulationSegment | null }>();
            for (const feat of features) {
                const segments = feat.regulation_segments || [];
                
                // Index each segment by its frontend_group_id + group_id
                for (const seg of segments) {
                    if (seg.frontend_group_id) {
                        lookup.set(seg.frontend_group_id, { feature: feat, segment: seg });
                    }
                    if (seg.group_id) {
                        lookup.set(seg.group_id, { feature: feat, segment: seg });
                    }
                }

                // Also index top-level frontend_group_ids (for quick waterbody-level lookups)
                const topIds: string[] = (feat as any)._frontend_group_ids || [];
                for (const fgid of topIds) {
                    if (!lookup.has(fgid)) {
                        // Only set if not already indexed by a segment (segment takes precedence)
                        lookup.set(fgid, { feature: feat, segment: segments[0] || null });
                    }
                }

                // Fallback: index by feature ID, group_id, waterbody_key for lakes without segments
                if (segments.length === 0) {
                    if (feat.id) lookup.set(feat.id, { feature: feat, segment: null });
                    if (feat.properties?.group_id) lookup.set(String(feat.properties.group_id), { feature: feat, segment: null });
                    if (feat.properties?.waterbody_key) lookup.set(String(feat.properties.waterbody_key), { feature: feat, segment: null });
                }
            }
            searchLookupRef.current = lookup;
        }).catch(console.error);
    }, []);

    // Restore feature selection from URL on initial load
    useEffect(() => {
        if (urlRestoredRef.current) return;
        
        // Wait for both data and map to be ready
        if (searchLookupRef.current.size === 0) return;
        if (!mapReady) return;
        const map = mapRef.current;
        if (!map) return;
        
        // Mark as restored regardless of whether we find a feature
        urlRestoredRef.current = true;
        
        const urlState = parseUrlState();
        if (!urlState.featureId) {
            return;
        }
        
        const lookupResult = searchLookupRef.current.get(urlState.featureId);
        if (!lookupResult) {
            console.warn('URL feature not found in search index:', urlState.featureId);
            return;
        }
        
        const { feature, segment } = lookupResult;
        
        // Build feature info from JSON via the unified builder.
        // Pass the URL's featureId as frontendGroupId so the selection
        // filter matches tiles even when the segment's own fgid differs
        // (e.g. top-level fgid resolved to first segment).
        const featureInfo = buildFeatureFromJSON(feature, segment, {
            frontendGroupId: urlState.featureId,
        });
        const featureBbox = featureInfo.bbox;
        
        // Fly to the feature's bbox — panel starts in 'partial' mode on
        // mobile so that long features still frame nicely (~65 vh visible).
        if (featureBbox) {
            const isMobile = isMobileViewport();
            const bounds = new maplibregl.LngLatBounds([featureBbox[0], featureBbox[1]], [featureBbox[2], featureBbox[3]]);
            const { padding, panelState } = isMobile
                ? getMobilePaddingForBounds(bounds)
                : { padding: { top: 80, bottom: 80, left: 80, right: 350 }, panelState: 'expanded' as CollapseState };
            flyToBbox(map, featureBbox, padding, (featureInfo.minzoom || 10) + 0.25);
            if (isMobile) setMobilePanelState(panelState);
        }
        
        setSelectedFeature(featureInfo);
        
        // Poll for tile geometry to enable highlighting
        const srcLayer = featureInfo.sourceLayer || resolveSourceLayer(featureInfo.type);
        let attempts = 0;
        const pollInterval = setInterval(() => {
            attempts++;
            
            // Try frontend_group_id filter first
            let found: any[] = [];
            const fgid = featureInfo.properties.frontend_group_id;
            const gid = featureInfo.properties.group_id;
            const wbKey = featureInfo.properties.waterbody_key;
            
            if (fgid) {
                const filter = buildFeatureFilter({ properties: { frontend_group_id: fgid } } as unknown as FeatureInfo);
                found = map.querySourceFeatures('regulations', { sourceLayer: srcLayer, filter: filter as maplibregl.FilterSpecification });
            }
            
            // Fall back to group_id filter
            if (!found.length && gid) {
                const filter = buildFeatureFilter({ properties: { group_id: gid } } as unknown as FeatureInfo);
                found = map.querySourceFeatures('regulations', { sourceLayer: srcLayer, filter: filter as maplibregl.FilterSpecification });
            }
            
            // Fall back to waterbody_key for lakes
            if (!found.length && wbKey) {
                const filter = buildFeatureFilter({ properties: { waterbody_key: wbKey } } as unknown as FeatureInfo);
                found = map.querySourceFeatures('regulations', { sourceLayer: srcLayer, filter: filter as maplibregl.FilterSpecification });
            }
            
            if (found.length > 0 || attempts > 25) {
                clearInterval(pollInterval);
                if (found.length > 0) {
                    const bestTile = found.reduce((best, tile) => {
                        const bestOrder = best.properties?.stream_order || 0;
                        const tileOrder = tile.properties?.stream_order || 0;
                        return tileOrder > bestOrder ? tile : best;
                    }, found[0]);
                    
                    setSelectedFeature(prev => prev ? {
                        ...prev,
                        geometry: (bestTile.geometry || bestTile.toJSON?.().geometry) as FeatureGeometry,
                    } : null);
                }
            }
        }, 200);
    }, [searchableFeatures, mapReady]);

    // Update URL when feature is selected
    useEffect(() => {
        // Skip URL update during initial restoration
        if (!urlRestoredRef.current) return;
        
        // Use frontend_group_id preferring, fallback to group_id, then waterbody_key
        const props = selectedFeature?.properties;
        const featureId = props?.frontend_group_id || props?.group_id || 
                          (props?.waterbody_key ? String(props.waterbody_key) : '');
        
        // Don't persist unnamed zone-only features in URL — they can't be restored
        // (no JSON entry, no bbox for fly-to).
        const hasName = !!(props?.display_name || props?.gnis_name);
        if (selectedFeature && !hasName) {
            return;
        }
        
        if (selectedFeature && !featureId) {
            console.warn('Selected feature missing all IDs:', props);
        }
        updateUrlState({ featureId: String(featureId || '') });
    }, [selectedFeature]);

    useEffect(() => {
        if (!mapContainerRef.current) return;
        const map = new maplibregl.Map({
            container: mapContainerRef.current,
            maxBounds: BC_BOUNDS,
            style: {
                version: 8,
                glyphs: 'https://cdn.protomaps.com/fonts/pbf/{fontstack}/{range}.pbf',
                sprite: 'https://protomaps.github.io/basemaps-assets/sprites/v4/light',
                sources: {
                    protomaps: { type: 'vector', url: `${TILE_BASE}/bc.pmtiles`, attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> · <a href="https://protomaps.com">Protomaps</a>', maxzoom: 15 },
                    regulations: { type: 'vector', url: `${TILE_BASE}/regulations_merged.pmtiles`, attribution: '<a href="https://www2.gov.bc.ca/gov/content/data/open-data/open-government-licence-bc">OGL-BC</a>', minzoom: 4, maxzoom: 12 },
                    satellite: { type: 'raster', tiles: [ESRI_SATELLITE_URL], tileSize: 256, attribution: 'Powered by <a href="https://www.esri.com">Esri</a>', maxzoom: 18 }
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
                        .filter(l => !['water_waterway_label', 'water_label_ocean', 'water_label_lakes'].includes(l.id)),
                    ...createAdminLabelLayers(),
                ]
            },
            center: [-123.0, 49.25], zoom: 8, maxZoom: 15, minZoom: 4, hash: true,
            // Smooth zoom: allow fractional levels and ease between them
            scrollZoom: { around: 'center' },
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
            const hatchDiag = createDiagonalHatchPattern('#E69F00');
            if (hatchDiag) map.addImage('hatch-diagonal', hatchDiag);
            const hatchCross = createCrossHatchPattern('#E69F00');
            if (hatchCross) map.addImage('hatch-cross', hatchCross);

            // Hatch overlays — appended after all static layers so they sit
            // above admin fills/lines but below the dynamic highlight layers.
            map.addLayer({
                id: 'admin_parks_nat-hatch',
                type: 'fill',
                source: 'regulations',
                'source-layer': 'admin_parks_nat',
                paint: { 'fill-pattern': 'hatch-diagonal', 'fill-opacity': 0.75 },
            } as any);

            map.addLayer({
                id: 'admin_parks_bc-eco-hatch',
                type: 'fill',
                source: 'regulations',
                'source-layer': 'admin_parks_bc',
                filter: ['==', ['get', 'admin_type'], 'ECOLOGICAL_RESERVE'],
                paint: { 'fill-pattern': 'hatch-cross', 'fill-opacity': 0.4 },
            } as any);

            // ── HIGHLIGHT LAYERS (hover / disambiguation) ─────────────
            // Filter-based: render directly from the PMTiles vector source.
            // All start hidden (FILTER_NONE). On hover/disambig, setGroupFilter
            // switches the match expression so only the target feature renders.
            const hlLineWidth = ['interpolate', ['linear'], ['zoom'], 4, 3, 8, 6, 12, 8] as any;
            const hlLineLayout = { 'line-cap': 'round' as const, 'line-join': 'round' as const };
            const hlCirclePaint = (color: string) => ({
                'circle-color': color, 'circle-radius': ['interpolate', ['linear'], ['zoom'], 10, 8, 13, 12, 16, 16] as any,
                'circle-stroke-color': '#FFFFFF', 'circle-stroke-width': 2, 'circle-opacity': 0.9,
            });

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
            // Ungazetted
            map.addLayer({ id: 'hl-ungazetted', type: 'circle', source: 'regulations', 'source-layer': 'ungazetted', filter: FILTER_NONE,
                paint: hlCirclePaint(HIGHLIGHT_COLORS.ungazetted) as any });

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
            map.addLayer({ id: 'sl-ungazetted', type: 'circle', source: 'regulations', 'source-layer': 'ungazetted', filter: FILTER_NONE,
                paint: { 'circle-color': SELECTION_COLOR, 'circle-radius': ['interpolate', ['linear'], ['zoom'], 10, 8, 13, 12, 16, 16] as any,
                    'circle-stroke-color': '#FFFFFF', 'circle-stroke-width': 2.5, 'circle-opacity': 1.0 } as any });
            
            map.addSource('cursor-circle', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
            map.addLayer({ id: 'cursor-circle-fill', type: 'fill', source: 'cursor-circle', paint: { 'fill-color': '#7C3AED', 'fill-opacity': 0.1 } });
            map.addLayer({ id: 'cursor-circle-line', type: 'line', source: 'cursor-circle', paint: { 'line-color': '#7C3AED', 'line-width': 1.5, 'line-opacity': 0.6 } });
            
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
            const features = map.queryRenderedFeatures([[e.point.x - 10, e.point.y - 10], [e.point.x + 10, e.point.y + 10]], { layers: INTERACTABLE_LAYERS });
            map.getCanvas().style.cursor = features.length > 0 ? 'pointer' : '';
            if (isDisambigOpenRef.current) return;
            (map.getSource('cursor-circle') as maplibregl.GeoJSONSource)?.setData({ type: 'FeatureCollection', features: [{ type: 'Feature', geometry: createCirclePolygon(e.lngLat, map.getZoom()), properties: {} }] });
        });

        map.on('click', (e) => {
            const features = map.queryRenderedFeatures([[e.point.x - 15, e.point.y - 15], [e.point.x + 15, e.point.y + 15]], { layers: INTERACTABLE_LAYERS });
            if (!features.length) return clearSelection();

            // Query admin boundary layers at click point to resolve zone names
            const adminHits = map.queryRenderedFeatures(
                [[e.point.x - 1, e.point.y - 1], [e.point.x + 1, e.point.y + 1]],
                { layers: ADMIN_FILL_LAYERS }
            );
            const adminZonesByRegId: Record<string, string[]> = {};
            for (const af of adminHits) {
                const aProps = af.properties || {};
                const regIds = (aProps.regulation_ids || '').split(',').filter(Boolean);
                const zoneName = aProps.name || '';
                if (!zoneName) continue;
                for (const rid of regIds) {
                    if (!adminZonesByRegId[rid]) adminZonesByRegId[rid] = [];
                    if (!adminZonesByRegId[rid].includes(zoneName)) adminZonesByRegId[rid].push(zoneName);
                }
            }

            // Deduplicate by frontend_group_id — each unique ID is one selectable option.
            // Properties come exclusively from the JSON lookup; tiles only provide geometry.
            const seenIds = new Set<string>();
            const options: FeatureOption[] = [];
            
            for (const f of features) {
                const tileProps = f.properties || {};
                const frontendGroupId = tileProps.frontend_group_id || '';
                const groupId = tileProps.group_id || '';
                const waterbodyKey = tileProps.waterbody_key || '';
                const dedupeKey = frontendGroupId || groupId || waterbodyKey || `${tileProps.linear_feature_id || ''}`;
                
                if (!dedupeKey) continue;
                if (seenIds.has(dedupeKey)) continue;
                seenIds.add(dedupeKey);
                
                // JSON lookup — the single source of truth for all menu data
                const lookupResult = searchLookupRef.current.get(frontendGroupId) || 
                                    searchLookupRef.current.get(groupId) ||
                                    searchLookupRef.current.get(waterbodyKey);
                
                if (lookupResult) {
                    // ✅ JSON-first: build entirely from JSON, only take geometry from tile
                    const option = buildFeatureFromJSON(lookupResult.feature, lookupResult.segment, {
                        geometry: f.toJSON().geometry,
                        source: f.layer.source,
                        sourceLayer: (f.layer as any)['source-layer'],
                        frontendGroupId: frontendGroupId,
                        extras: { _adminZones: adminZonesByRegId as any },
                    });
                    options.push(option);
                } else {
                    // Unnamed / zone-only feature — not in named lookup.
                    // Resolve regulation data from the compact dict (fgid → ri → reg_sets).
                    const ri = compactRef.current[frontendGroupId] ??
                               compactRef.current[groupId] ??
                               compactRef.current[waterbodyKey];
                    const regIds = ri !== undefined ? (regSetsRef.current[ri] || '') : '';

                    if (!regIds) {
                        // No compact entry and no named entry — data integrity issue
                        console.warn(
                            `[Map] Tile feature not in JSON lookup or compact dict. ` +
                            `frontend_group_id="${frontendGroupId}", layer="${f.layer.id}".`
                        );
                        continue;
                    }

                    // Build a synthetic SearchableFeature from tile properties + compact regs
                    const srcLayer = (f.layer as any)['source-layer'] || '';
                    const typeLabel = srcLayer === 'streams' ? 'Stream' : srcLayer === 'lakes' ? 'Lake' : srcLayer === 'wetlands' ? 'Wetland' : 'Waterbody';
                    const tileDisplayName = String(tileProps.display_name || '');
                    const syntheticFeature: SearchableFeature = {
                        id: frontendGroupId || groupId || waterbodyKey,
                        display_name: tileDisplayName || `Unnamed ${typeLabel}`,
                        gnis_name: '',
                        name: tileDisplayName || `Unnamed ${typeLabel}`,
                        type: srcLayer,
                        name_variants: [],
                        regulation_segments: [],
                        _frontend_group_ids: frontendGroupId ? [frontendGroupId] : [],
                        properties: {
                            frontend_group_id: frontendGroupId,
                            group_id: groupId,
                            waterbody_key: waterbodyKey,
                            regulation_ids: regIds,
                            regulation_count: regIds.split(',').filter(Boolean).length,
                            zones: '',
                            mgmt_units: '',
                            region_name: '',
                            fwa_watershed_code: '',
                            minzoom: 10,
                            total_length_km: 0,
                        },
                    } as SearchableFeature;

                    const option = buildFeatureFromJSON(syntheticFeature, null, {
                        geometry: f.toJSON().geometry,
                        source: f.layer.source,
                        sourceLayer: srcLayer,
                        frontendGroupId: frontendGroupId,
                        extras: { _adminZones: adminZonesByRegId as any },
                    });
                    options.push(option);
                }
            }

            clearSelection();
            if (options.length === 1) {
                const selected = options[0];
                setSelectedFeature(selected);

                // Fly to feature; on mobile use aspect-ratio-aware padding.
                const isMobile = isMobileViewport();

                if (selected.bbox) {
                    const bounds = new maplibregl.LngLatBounds(
                        [selected.bbox[0], selected.bbox[1]],
                        [selected.bbox[2], selected.bbox[3]]
                    );
                    const { padding, panelState } = isMobile
                        ? getMobilePaddingForBounds(bounds)
                        : { padding: { top: 80, bottom: 80, left: 80, right: 350 }, panelState: 'expanded' as CollapseState };
                    flyToBbox(map, selected.bbox, padding, (selected.minzoom || 10) + 0.25);
                    if (isMobile) setMobilePanelState(panelState);
                } else if (isMobile) {
                    setMobilePanelState('partial');
                }
            } else if (options.length > 1) { 
                setDisambigOptions(options); 
                setDisambigPosition({ x: e.point.x, y: e.point.y }); 
                isDisambigOpenRef.current = true; 
            }
        });

        mapRef.current = map;
        return () => map.remove();
    }, [clearSelection]);

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
    }, [selectedFeature]);

    const handleSearchSelect = useCallback((feature: SearchableFeature) => {
        const map = mapRef.current;
        if (!map) return;
        
        clearSelection();
        const targetMinZoom = Number(feature.min_zoom || feature.properties?.minzoom || 10);
        
        // Check if this waterbody has multiple regulation segments
        const segments = feature.regulation_segments || [];
        const hasMultipleSegments = segments.length > 1;

        // Fly to the feature first — use aspect-ratio-aware padding on mobile
        if (feature.bbox) {
            const isMobile = isMobileViewport();
            const bounds = new maplibregl.LngLatBounds([feature.bbox[0], feature.bbox[1]], [feature.bbox[2], feature.bbox[3]]);
            const { padding, panelState } = isMobile
                ? getMobilePaddingForBounds(bounds)
                : { padding: { top: 80, bottom: 80, left: 80, right: 350 }, panelState: 'expanded' as CollapseState };
            flyToBbox(map, feature.bbox, padding, targetMinZoom + 0.25);
            // Store target panel state; applied after disambig check below
            if (isMobile) setMobilePanelState(panelState);
        }

        if (hasMultipleSegments) {
            // Build disambiguation options from JSON via the unified builder
            const options: FeatureOption[] = segments.map(seg =>
                buildFeatureFromJSON(feature, seg)
            );

            // Show disambiguation menu immediately
            const screenCenter = { x: window.innerWidth / 2, y: window.innerHeight / 3 };
            setDisambigOptions(options);
            setDisambigPosition(screenCenter);
            isDisambigOpenRef.current = true;
            setMobilePanelState('partial');
        } else {
            // Single segment or no segments — select directly via the unified builder
            const seg = segments[0] || null;
            const selected = buildFeatureFromJSON(feature, seg);
            
            setSelectedFeature(selected);
            // Panel state was already set by the aspect-ratio logic above

            // Poll until tiles load to get geometry for highlighting
            const srcLayer = selected.sourceLayer || resolveSourceLayer(selected.type);
            const frontendGroupId = selected.properties.frontend_group_id || '';
            let attempts = 0;
            searchPollRef.current = setInterval(() => {
                attempts++;
                const filter = buildFeatureFilter({ properties: { frontend_group_id: frontendGroupId } } as unknown as FeatureInfo);
                let found: any[] = map.querySourceFeatures('regulations', { sourceLayer: srcLayer, filter: filter as maplibregl.FilterSpecification });
                
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
    }, [clearSelection]);

    return (
        <div className="map-container">
            <div ref={mapContainerRef} className="map-canvas" />
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
            <InfoPanel feature={selectedFeature} onClose={clearSelection} collapseState={mobilePanelState} onSetCollapseState={setMobilePanelState} />
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
                                const map = mapRef.current; if (!map) return;
                                const isMobile = isMobileViewport();
                                const padding = isMobile
                                    ? { top: 60, bottom: getMobileBottomPadding(), left: 40, right: 40 }
                                    : { top: 80, bottom: 80, left: 80, right: 350 };
                                if (option.bbox) {
                                    flyToBbox(map, option.bbox, padding, (option.minzoom || 10) + 0.25, 400);
                                } else if (option.geometry) {
                                    const bounds = new maplibregl.LngLatBounds();
                                    extendBoundsWithGeometry(bounds, option.geometry);
                                    if (!bounds.isEmpty()) {
                                        const camera = map.cameraForBounds(bounds, { padding });
                                        if (camera) {
                                            const minZoom = (option.minzoom || 10) + 0.25;
                                            const finalZoom = Math.max(camera.zoom || 0, minZoom);
                                            map.flyTo({ ...camera, zoom: Math.min(finalZoom, 15), duration: 400 });
                                        }
                                    }
                                }
                            }, 50);
                        } else {
                            setHighlightedOption(null);
                        }
                    }}
                    onSelect={f => { 
                        clearSelection(); 
                        setSelectedFeature(f); 
                        setMobilePanelState('partial'); 
                        
                        // Fly to the selected feature's bbox with minimum zoom enforcement
                        const map = mapRef.current;
                        if (!map) return;
                        
                        if (f.bbox) {
                            const isMobile = isMobileViewport();
                            const padding = isMobile 
                                ? { top: 60, bottom: getMobileBottomPadding(), left: 40, right: 40 }
                                : { top: 80, bottom: 80, left: 80, right: 350 };
                            flyToBbox(map, f.bbox, padding, (f.minzoom || 10) + 0.25);
                        }
                    }} onClose={clearSelection}
                />
            )}
        </div>
    );
};

export default MapComponent;