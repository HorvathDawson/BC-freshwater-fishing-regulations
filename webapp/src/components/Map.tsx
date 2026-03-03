import { useEffect, useRef, useState, useCallback } from 'react';
import maplibregl from 'maplibre-gl';
import { Protocol } from 'pmtiles';
import { layers, LIGHT } from '@protomaps/basemaps';
import 'maplibre-gl/dist/maplibre-gl.css';
import { createRegulationLayers, createAdminLabelLayers, createEarlyRoadLayers, HIGHLIGHT_COLORS, SELECTION_COLOR, matchByFeatureType } from '../map/styles';
import { regulationsService } from '../services/regulationsService';
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

const INTERACTABLE_LAYERS = ['streams', 'lakes-fill', 'wetlands-fill', 'manmade-fill'];
const ADMIN_FILL_LAYERS = [
    'admin_parks_nat-fill', 'admin_parks_bc-fill', 'admin_wma-fill',
    'admin_watersheds-fill', 'admin_historic_sites-fill',
];

// --- STYLE EXPRESSIONS ---
const STREAM_LINE_WIDTH = ['interpolate', ['linear'], ['zoom'], 4, ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.1]], 8, ['+', 2, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.15]], 11, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 1.5], 12, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 2], 14, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 3], 16, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 4]];

// --- TYPES ---
interface LayerVisibility {
    streams: boolean; lakes: boolean; wetlands: boolean; manmade: boolean; regions: boolean;
    admin_parks_nat: boolean; admin_parks_bc: boolean; admin_wma: boolean;
    admin_watersheds: boolean; admin_historic_sites: boolean;
}

// --- UTILITY ---
const isValidBbox = (bbox: unknown): bbox is [number, number, number, number] => {
    if (!bbox || !Array.isArray(bbox) || bbox.length !== 4) return false;
    const [minx, miny, maxx, maxy] = bbox as number[];
    return minx >= -180 && minx <= 180 && maxx >= -180 && maxx <= 180 &&
           miny >= -90 && miny <= 90 && maxy >= -90 && maxy <= 90 &&
           minx < maxx && miny < maxy;
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

const getFeatureType = (layerId: string): 'stream' | 'lake' | 'wetland' | 'manmade' => {
    if (layerId.includes('streams')) return 'stream';
    if (layerId.includes('lakes')) return 'lake';
    if (layerId.includes('wetlands')) return 'wetland';
    return 'manmade';
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

const updateMapSource = (map: maplibregl.Map, sourceId: string, feature: FeatureInfo | FeatureOption | null) => {
    const source = map.getSource(sourceId) as maplibregl.GeoJSONSource;
    if (!source) return;
    
    if (!feature) {
        source.setData({ type: 'FeatureCollection', features: [] });
        return;
    }

    const filter = buildFeatureFilter(feature);
    const srcLayer = feature.sourceLayer || (feature.type === 'stream' ? 'streams' : 'lakes');
    
    let features: any[] = [];
    if (filter) features = map.querySourceFeatures('regulations', { sourceLayer: srcLayer, filter: filter as maplibregl.FilterSpecification });
    
    if (!features.length && feature.geometry) {
        features = [{ geometry: feature.geometry, properties: feature.properties }];
    }
    
    source.setData({ 
        type: 'FeatureCollection', 
        features: features.map(f => ({ 
            type: 'Feature', 
            geometry: f.geometry || f.toJSON?.().geometry, 
            // Embed _feature_type so highlight/selection layers can match per-type colors
            properties: { ...f.properties, _feature_type: feature.type } 
        })) as any 
    });
};

const MapComponent = () => {
    const mapContainerRef = useRef<HTMLDivElement>(null);
    const mapRef = useRef<maplibregl.Map | null>(null);
    const isDisambigOpenRef = useRef<boolean>(false);
    const hoverTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    const searchPollRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    const selectedFeatureRef = useRef<FeatureInfo | null>(null);
    const mobilePanelStateRef = useRef<CollapseState>('expanded');
    const urlRestoredRef = useRef<boolean>(false);
    // Lookup map: frontend_group_id → { feature, segment }
    // Populated once search_index.json loads; used by the click handler
    // to enrich tile features with search-level data (name_variants, etc.)
    const searchLookupRef = useRef<Map<string, { feature: SearchableFeature; segment: RegulationSegment | null }>>(new Map());
    
    const [selectedFeature, setSelectedFeature] = useState<FeatureInfo | null>(null);
    const [disambigOptions, setDisambigOptions] = useState<FeatureOption[]>([]);
    const [disambigPosition, setDisambigPosition] = useState<{ x: number; y: number } | null>(null);
    const [disambigCollapsed, setDisambigCollapsed] = useState(false);
    const [mobilePanelState, setMobilePanelState] = useState<CollapseState>('expanded');
    const [highlightedOption, setHighlightedOption] = useState<FeatureOption | null>(null);
    const [highlightedSearchResult, setHighlightedSearchResult] = useState<SearchableFeature | null>(null);
    const [searchableFeatures, setSearchableFeatures] = useState<SearchableFeature[]>([]);
    const [mapReady, setMapReady] = useState(false);
    const [disclaimerOpen, setDisclaimerOpen] = useState(false);
    const [_layerVisibility, _setLayerVisibility] = useState<LayerVisibility>({
        streams: true, lakes: true, wetlands: true, manmade: true, regions: true,
        admin_parks_nat: true, admin_parks_bc: true, admin_wma: true,
        admin_watersheds: true, admin_historic_sites: true,
    });
    void _layerVisibility; void _setLayerVisibility;

    // Mirror state → refs so map event-handler closures always see the latest values.
    // (State setters are stable; refs are mutable — this is the canonical React pattern.)
    useEffect(() => { selectedFeatureRef.current = selectedFeature; }, [selectedFeature]);
    useEffect(() => { mobilePanelStateRef.current = mobilePanelState; }, [mobilePanelState]);

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
        ['cursor-circle', 'highlight-source', 'selection-source'].forEach(id => {
            const src = map.getSource(id) as maplibregl.GeoJSONSource;
            if (src) src.setData({ type: 'FeatureCollection', features: [] });
        });
    }, []);

    useEffect(() => {
        waterbodyDataService.getWaterbodies().then(waterbodies => {
            // Use backend search index directly - it's already grouped by physical stream
            const features: SearchableFeature[] = (waterbodies || []).map((item: WaterbodyItem) => {
                const synopsisNames = regulationsService.filterOutProvincialNames(item.regulation_names || []);
                const displayName = (item.gnis_name && item.gnis_name.toLowerCase() !== 'unnamed') 
                    ? item.gnis_name 
                    : (synopsisNames[0] || 'Unnamed Waterbody');
                const normalizedType = item.type === 'streams' ? 'stream' : item.type === 'lakes' ? 'lake' : item.type;
                
                return {
                    id: item.id,
                    gnis_name: displayName,
                    name: displayName,
                    type: normalizedType,
                    regulation_names: synopsisNames,
                    name_variants: item.name_variants || [],
                    regulation_segments: item.regulation_segments || [],
                    properties: {
                        ...item.properties,
                        zones: item.zones || '',
                        mgmt_units: item.mgmt_units,
                        region_name: item.region_name || '',
                        regulation_ids: item.regulation_ids,
                        fwa_watershed_code: item.properties?.fwa_watershed_code || '',
                        minzoom: item.min_zoom || 4,
                        length_km: item.length_km || 0,
                    },
                    bbox: isValidBbox(item.bbox) ? item.bbox : undefined,
                } as SearchableFeature;
            });
            
            setSearchableFeatures(features);
            
            // Build lookup indexed by frontend_group_id/group_id/waterbody_key for tile-click enrichment
            // Streams have regulation_segments, lakes may have IDs at the top level
            const lookup = new Map<string, { feature: SearchableFeature; segment: RegulationSegment | null }>();
            for (const feat of features) {
                const segments = feat.regulation_segments || [];
                
                if (segments.length > 0) {
                    // Streams: index each segment
                    for (const seg of segments) {
                        if (seg.frontend_group_id) {
                            lookup.set(seg.frontend_group_id, { feature: feat, segment: seg });
                        }
                        if (seg.group_id) {
                            lookup.set(seg.group_id, { feature: feat, segment: seg });
                        }
                    }
                } else {
                    // Lakes/other: index by feature ID directly
                    if (feat.id) {
                        lookup.set(feat.id, { feature: feat, segment: null });
                    }
                    // Also check properties for various ID types
                    if (feat.properties?.frontend_group_id) {
                        lookup.set(String(feat.properties.frontend_group_id), { feature: feat, segment: null });
                    }
                    if (feat.properties?.group_id) {
                        lookup.set(String(feat.properties.group_id), { feature: feat, segment: null });
                    }
                    if (feat.properties?.waterbody_key) {
                        lookup.set(String(feat.properties.waterbody_key), { feature: feat, segment: null });
                    }
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
        const srcLayer = (feature.type === 'stream' || feature.type === 'streams') ? 'streams' : 'lakes';
        const normalizedType = (feature.type === 'streams' ? 'stream' : feature.type === 'lakes' ? 'lake' : feature.type) as 'stream' | 'lake' | 'wetland' | 'manmade';
        const synopsisNames = regulationsService.filterOutProvincialNames(feature.regulation_names || []);
        const displayName = feature.gnis_name || synopsisNames[0] || 'Unnamed Waterbody';
        const targetMinZoom = Number(feature.properties?.minzoom ?? 10);
        const featureBbox = (segment?.bbox || feature.bbox) as [number, number, number, number];
        
        // Resolve IDs - don't use feature.id as it may be a search-index composite
        // For streams: use segment IDs
        // For lakes: use waterbody_key from properties
        const resolvedFrontendGroupId = segment?.frontend_group_id || feature.properties?.frontend_group_id || '';
        const resolvedGroupId = segment?.group_id || feature.properties?.group_id || '';
        const resolvedWaterbodyKey = feature.properties?.waterbody_key || '';
        
        // Build feature info from URL state
        const featureInfo: FeatureInfo = {
            type: normalizedType,
            properties: {
                ...feature.properties,
                gnis_name: displayName,
                regulation_names: (segment?.regulation_names || synopsisNames) as any,
                name_variants: (segment?.name_variants || feature.name_variants || []) as any,
                regulation_ids: segment?.regulation_ids || feature.properties?.regulation_ids,
                // Only include IDs that actually exist
                ...(resolvedFrontendGroupId && { frontend_group_id: resolvedFrontendGroupId }),
                ...(resolvedGroupId && { group_id: resolvedGroupId }),
                waterbody_key: resolvedWaterbodyKey || feature.properties?.waterbody_key,
                fwa_watershed_code: feature.properties?.fwa_watershed_code || '',
            },
            source: 'regulations',
            sourceLayer: srcLayer,
            bbox: featureBbox,
            minzoom: targetMinZoom,
        };
        
        // Fly to the feature's bbox
        if (featureBbox) {
            const isMobile = isMobileViewport();
            const padding = isMobile ? { top: 60, bottom: 250, left: 40, right: 40 } : { top: 80, bottom: 80, left: 80, right: 350 };
            const bounds = new maplibregl.LngLatBounds([featureBbox[0], featureBbox[1]], [featureBbox[2], featureBbox[3]]);
            const camera = map.cameraForBounds(bounds, { padding });
            if (camera) {
                const finalZoom = Math.max(camera.zoom || 0, targetMinZoom);
                map.flyTo({ ...camera, zoom: Math.min(finalZoom, 12.5), duration: 800 });
            }
        }
        
        setSelectedFeature(featureInfo);
        setMobilePanelState('expanded');
        
        // Poll for tile geometry to enable highlighting
        // Try frontend_group_id, group_id, and waterbody_key since tiles may only have one
        let attempts = 0;
        const pollInterval = setInterval(() => {
            attempts++;
            
            // Try frontend_group_id filter first
            let found: any[] = [];
            if (resolvedFrontendGroupId) {
                const filter = buildFeatureFilter({ properties: { frontend_group_id: resolvedFrontendGroupId } } as unknown as FeatureInfo);
                found = map.querySourceFeatures('regulations', { sourceLayer: srcLayer, filter: filter as maplibregl.FilterSpecification });
            }
            
            // Fall back to group_id filter
            if (!found.length && resolvedGroupId) {
                const filter = buildFeatureFilter({ properties: { group_id: resolvedGroupId } } as unknown as FeatureInfo);
                found = map.querySourceFeatures('regulations', { sourceLayer: srcLayer, filter: filter as maplibregl.FilterSpecification });
            }
            
            // Fall back to waterbody_key for lakes
            if (!found.length && resolvedWaterbodyKey) {
                const filter = buildFeatureFilter({ properties: { waterbody_key: resolvedWaterbodyKey } } as unknown as FeatureInfo);
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
                    regulations: { type: 'vector', url: `${TILE_BASE}/regulations_merged.pmtiles`, attribution: '<a href="https://www2.gov.bc.ca/gov/content/data/open-data/open-government-licence-bc">OGL-BC</a>', minzoom: 4, maxzoom: 12 }
                },
                // Base map (no labels) → regulation overlays → labels on top
                // The `layers()` call without `lang` returns geometry-only layers;
                // `labelsOnly + lang` returns road / place / water name labels
                // so they render above the regulation fills and remain readable.
                layers: [
                    ...layers('protomaps', LIGHT),
                    ...createEarlyRoadLayers(),
                    ...createRegulationLayers(),
                    ...layers('protomaps', LIGHT, { labelsOnly: true, lang: 'en' }),
                    ...createAdminLabelLayers(),
                ]
            },
            center: [-123.0, 49.25], zoom: 8, maxZoom: 15, minZoom: 4, hash: true, attributionControl: { compact: true }
        });

        // Add compass navigation control
        const compassPosition = isMobileViewport() ? 'top-right' : 'bottom-left';
        map.addControl(
            new maplibregl.NavigationControl({ showCompass: true, showZoom: true, visualizePitch: true }),
            compassPosition
        );

        map.on('load', () => {
            const pattern = createWetlandPattern();
            if (pattern) map.addImage('wetland-pattern', pattern);

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

            map.addSource('highlight-source', { type: 'geojson', data: { type: 'FeatureCollection', features: [] }, tolerance: 0.1 });
            // Hover / disambiguation highlight — per-type saturated color, wider line
            map.addLayer({ id: 'highlight-line', type: 'line', source: 'highlight-source', paint: { 'line-color': matchByFeatureType(HIGHLIGHT_COLORS, SELECTION_COLOR) as any, 'line-width': ['interpolate', ['linear'], ['zoom'], 4, 3, 8, 6, 12, 8], 'line-opacity': 1.0 }, layout: { 'line-cap': 'round', 'line-join': 'round' } });
            map.addLayer({ id: 'highlight-fill', type: 'fill', source: 'highlight-source', paint: { 'fill-color': matchByFeatureType(HIGHLIGHT_COLORS, SELECTION_COLOR) as any, 'fill-opacity': 0.4 }, filter: ['==', '$type', 'Polygon'] });
            
            map.addSource('selection-source', { type: 'geojson', data: { type: 'FeatureCollection', features: [] }, tolerance: 0.1 });
            // Active selection — uniform deep blue (same weight signal regardless of type)
            map.addLayer({ id: 'selection-line', type: 'line', source: 'selection-source', paint: { 'line-color': SELECTION_COLOR, 'line-width': STREAM_LINE_WIDTH as any, 'line-opacity': 1.0 }, layout: { 'line-cap': 'round', 'line-join': 'round' } });
            map.addLayer({ id: 'selection-fill', type: 'fill', source: 'selection-source', paint: { 'fill-color': SELECTION_COLOR, 'fill-opacity': 0.35 }, filter: ['==', '$type', 'Polygon'] });
            
            map.addSource('cursor-circle', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
            map.addLayer({ id: 'cursor-circle-fill', type: 'fill', source: 'cursor-circle', paint: { 'fill-color': '#3b82f6', 'fill-opacity': 0.1 } });
            map.addLayer({ id: 'cursor-circle-line', type: 'line', source: 'cursor-circle', paint: { 'line-color': '#3b82f6', 'line-width': 1.5, 'line-opacity': 0.6 } });
            
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
            // Disambig menu
            if (isDisambigOpenRef.current) {
                if (!isMobile) clearSelection();
                else setDisambigCollapsed(true);
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

            // Deduplicate by frontend_group_id - each unique frontend_group_id is one selectable option
            const seenFrontendGroupIds = new Set<string>();
            const options: FeatureOption[] = [];
            
            for (const f of features) {
                const props = f.properties || {};
                const frontendGroupId = props.frontend_group_id || '';
                const groupId = props.group_id || '';
                const waterbodyKey = props.waterbody_key || '';
                const dedupeKey = frontendGroupId || groupId || waterbodyKey || `${props.linear_feature_id || ''}`;
                
                // Skip duplicates (but allow features without any ID on first pass)
                if (!dedupeKey) continue;
                if (seenFrontendGroupIds.has(dedupeKey)) continue;
                seenFrontendGroupIds.add(dedupeKey);
                
                // Look up segment data from search index (try multiple ID types)
                const lookupResult = searchLookupRef.current.get(frontendGroupId) || 
                                    searchLookupRef.current.get(groupId) ||
                                    searchLookupRef.current.get(waterbodyKey);
                const segment = lookupResult?.segment;
                const searchEntry = lookupResult?.feature;
                
                // Use segment's frontend_group_id if available, or tile's own IDs
                // Don't use search entry ID - it's a composite key that won't match tiles
                const resolvedFrontendGroupId = segment?.frontend_group_id || frontendGroupId;
                const resolvedGroupId = segment?.group_id || groupId;
                // Keep waterbody_key separate for lakes
                const resolvedWaterbodyKey = waterbodyKey || props.waterbody_key || '';
                
                const bounds = new maplibregl.LngLatBounds();
                extendBoundsWithGeometry(bounds, f.toJSON().geometry);
                const tileBbox = bounds.toArray().flat() as [number, number, number, number];
                
                // Use segment bbox (most precise), then search entry bbox, then tile bbox
                const featureBbox = (segment?.bbox || searchEntry?.bbox || tileBbox) as [number, number, number, number];
                const featureMinzoom = Number(searchEntry?.min_zoom || searchEntry?.properties?.minzoom || props.min_zoom || 4);
                const displayName = searchEntry?.gnis_name || props.gnis_name || props.lake_name || '';
                
                options.push({
                    type: getFeatureType(f.layer.id),
                    id: resolvedFrontendGroupId || resolvedGroupId || resolvedWaterbodyKey,
                    geometry: f.toJSON().geometry,
                    source: f.layer.source,
                    sourceLayer: (f.layer as any)['source-layer'],
                    bbox: featureBbox,
                    minzoom: featureMinzoom,
                    properties: {
                        ...props,
                        // Only set these if they have values (don't overwrite with empty strings)
                        ...(resolvedFrontendGroupId && { frontend_group_id: resolvedFrontendGroupId }),
                        ...(resolvedGroupId && { group_id: resolvedGroupId }),
                        waterbody_key: resolvedWaterbodyKey || props.waterbody_key,
                        gnis_name: displayName,
                        _adminZones: adminZonesByRegId as any,
                        // Use segment-specific name_variants if available
                        name_variants: (segment?.name_variants || searchEntry?.name_variants || []) as any,
                        regulation_names: (segment?.regulation_names || searchEntry?.regulation_names || []) as any,
                    },
                });
            }

            clearSelection();
            if (options.length === 1) setSelectedFeature(options[0]);
            else if (options.length > 1) { 
                setDisambigOptions(options); 
                setDisambigPosition({ x: e.point.x, y: e.point.y }); 
                isDisambigOpenRef.current = true; 
            }
        });

        mapRef.current = map;
        return () => map.remove();
    }, [clearSelection]);

    // Handle highlighted state changes & detail refreshing securely
    useEffect(() => {
        const map = mapRef.current;
        if (!map) return;

        const refreshHighlight = () => updateMapSource(map, 'highlight-source', highlightedOption);
        
        refreshHighlight();
        
        if (highlightedOption) {
            map.on('zoomend', refreshHighlight);
            map.on('idle', refreshHighlight);
            return () => {
                map.off('zoomend', refreshHighlight);
                map.off('idle', refreshHighlight);
            };
        }
    }, [highlightedOption]);

    // Handle selected state changes & detail refreshing securely
    useEffect(() => {
        const map = mapRef.current;
        if (!map || !selectedFeature) {
            if (map) updateMapSource(map, 'selection-source', null);
            return;
        }

        const refreshSelection = () => updateMapSource(map, 'selection-source', selectedFeature);
        
        refreshSelection();
        map.on('zoomend', refreshSelection);
        map.on('idle', refreshSelection);
        // Also refresh when new tiles load (ensures highlight appears after fly-to)
        map.on('sourcedata', refreshSelection);

        if (mobilePanelState === 'expanded') {
            const bounds = new maplibregl.LngLatBounds();
            if (selectedFeature.bbox) {
                bounds.extend([selectedFeature.bbox[0], selectedFeature.bbox[1]]);
                bounds.extend([selectedFeature.bbox[2], selectedFeature.bbox[3]]);
            } else if (selectedFeature.geometry) {
                extendBoundsWithGeometry(bounds, selectedFeature.geometry);
            }

            if (!bounds.isEmpty()) {
                const isMobile = isMobileViewport();
                const padding = isMobile ? { top: 60, bottom: 250, left: 40, right: 40 } : { top: 80, bottom: 80, left: 80, right: 350 };
                const targetMinZoom = selectedFeature.minzoom || 10;
                
                const camera = map.cameraForBounds(bounds, { padding });
                if (camera) {
                    const finalZoom = Math.max(camera.zoom || 0, targetMinZoom);
                    map.flyTo({ ...camera, zoom: Math.min(finalZoom, 12.5), duration: 800 });
                }
            }
        }

        return () => { 
            map.off('zoomend', refreshSelection);
            map.off('idle', refreshSelection);
            map.off('sourcedata', refreshSelection);
        };
    }, [selectedFeature, mobilePanelState]);

    const handleSearchSelect = useCallback((feature: SearchableFeature) => {
        const map = mapRef.current;
        if (!map) return;
        
        clearSelection();
        const srcLayer = (feature.type === 'stream' || feature.type === 'streams') ? 'streams' : 'lakes';
        const normalizedType = (feature.type === 'streams' ? 'stream' : feature.type === 'lakes' ? 'lake' : feature.type) as 'stream' | 'lake' | 'wetland' | 'manmade';
        const synopsisNames = regulationsService.filterOutProvincialNames(feature.regulation_names || []);
        const displayName = (feature.gnis_name && feature.gnis_name.toLowerCase() !== 'unnamed') ? feature.gnis_name : synopsisNames[0];
        const targetMinZoom = Number(feature.properties.minzoom ?? 10);
        const watershedCode = feature.properties.fwa_watershed_code || '';
        
        // Check if this stream has multiple regulation segments
        const segments = feature.regulation_segments || [];
        const hasMultipleSegments = segments.length > 1;

        // Fly to the feature first
        if (feature.bbox) {
            const isMobile = isMobileViewport();
            const padding = isMobile ? { top: 60, bottom: 250, left: 40, right: 40 } : { top: 80, bottom: 80, left: 80, right: 350 };
            const bounds = new maplibregl.LngLatBounds([feature.bbox[0], feature.bbox[1]], [feature.bbox[2], feature.bbox[3]]);
            
            const camera = map.cameraForBounds(bounds, { padding });
            if (camera) {
                const finalZoom = Math.max(camera.zoom || 0, targetMinZoom);
                map.flyTo({ ...camera, zoom: Math.min(finalZoom, 12.5), duration: 800 });
            }
        }

        if (hasMultipleSegments) {
            // Poll until tiles load, then build disambiguation options
            // Always use segment list from search data to ensure ALL segments appear
            const segmentFrontendIds = new Set(segments.map(s => s.frontend_group_id).filter(Boolean));
            
            let attempts = 0;
            searchPollRef.current = setInterval(() => {
                attempts++;
                
                // Query all features in this source layer to get tile geometries
                const allFeatures = map.querySourceFeatures('regulations', { sourceLayer: srcLayer });
                
                // Build a map of frontend_group_id -> tile feature for geometry augmentation
                const tileFeatureMap = new Map<string, any>();
                for (const f of allFeatures) {
                    const fgid = f.properties?.frontend_group_id;
                    if (fgid && segmentFrontendIds.has(fgid) && !tileFeatureMap.has(fgid)) {
                        tileFeatureMap.set(fgid, f);
                    }
                }
                
                // Wait until tiles for ALL expected segments are loaded, or time out.
                // Firing on the first tile found (size > 0) was too eager — segments
                // whose tiles hadn't rendered yet ended up with no geometry and couldn't
                // be highlighted or zoomed to correctly.
                if (tileFeatureMap.size >= segmentFrontendIds.size || attempts > 25) {
                    if (searchPollRef.current) clearInterval(searchPollRef.current);
                    
                    // Build options from ALL segments, augmenting with tile geometry when available
                    const options: FeatureOption[] = [];
                    
                    for (const seg of segments) {
                        const segRegNames = regulationsService.filterOutProvincialNames(seg.regulation_names || []);
                        const frontendGroupId = seg.frontend_group_id || seg.group_id;
                        
                        // Try to find tile geometry for this segment
                        const tileFeature = tileFeatureMap.get(seg.frontend_group_id);
                        
                        let optionBbox: [number, number, number, number];
                        let optionGeom: FeatureGeometry | undefined = undefined;
                        
                        if (tileFeature) {
                            // Use tile geometry if available
                            optionGeom = tileFeature.toJSON().geometry;
                            const geomBounds = new maplibregl.LngLatBounds();
                            extendBoundsWithGeometry(geomBounds, optionGeom);
                            optionBbox = geomBounds.toArray().flat() as [number, number, number, number];
                        } else {
                            // Fall back to segment bbox from search data
                            optionBbox = (seg.bbox || feature.bbox) as [number, number, number, number];
                        }
                        
                        options.push({
                            type: normalizedType,
                            id: frontendGroupId,
                            geometry: optionGeom,
                            source: 'regulations',
                            sourceLayer: srcLayer,
                            bbox: optionBbox,
                            minzoom: targetMinZoom,
                            properties: {
                                ...(tileFeature?.properties || feature.properties),
                                gnis_name: displayName,
                                frontend_group_id: seg.frontend_group_id,
                                regulation_ids: seg.regulation_ids,
                                regulation_names: segRegNames as any,
                                name_variants: (seg.name_variants || feature.name_variants || []) as any,
                                fwa_watershed_code: watershedCode,
                                length_km: seg.length_km,
                            },
                        });
                    }
                    
                    // Show disambiguation menu
                    const screenCenter = { x: window.innerWidth / 2, y: window.innerHeight / 3 };
                    setDisambigOptions(options);
                    setDisambigPosition(screenCenter);
                    isDisambigOpenRef.current = true;
                    setMobilePanelState('partial');
                }
            }, 200);
        } else {
            // Single segment or no segments - select directly
            const seg = segments[0];
            const segRegIds = seg?.regulation_ids || feature.properties.regulation_ids;
            const frontendGroupId = seg?.frontend_group_id || '';
            const segNameVariants = seg?.name_variants || feature.name_variants || [];
            
            setSelectedFeature({ 
                type: normalizedType, 
                properties: { 
                    ...feature.properties, 
                    gnis_name: displayName, 
                    regulation_names: synopsisNames as any,
                    name_variants: segNameVariants as any,
                    regulation_ids: segRegIds,
                    frontend_group_id: frontendGroupId,
                    fwa_watershed_code: watershedCode,
                }, 
                source: 'regulations', 
                sourceLayer: srcLayer,
                bbox: feature.bbox as [number, number, number, number],
                minzoom: targetMinZoom
            });
            setMobilePanelState('partial');

            // Poll until tiles load to get geometry for highlighting
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
                    placeholder="Search waterbodies..." 
                />
            </div>
            <InfoPanel feature={selectedFeature} onClose={clearSelection} collapseState={mobilePanelState} onSetCollapseState={setMobilePanelState} />
            <DisclaimerLink onClick={() => setDisclaimerOpen(true)} />
            <Disclaimer isOpen={disclaimerOpen} onClose={() => setDisclaimerOpen(false)} />
            {disambigOptions.length > 0 && (
                <DisambiguationMenu 
                    options={disambigOptions} position={disambigPosition} highlightedOption={highlightedOption}
                    isCollapsed={disambigCollapsed}
                    onSetCollapse={setDisambigCollapsed}
                    onHighlight={(option) => {
                        if (hoverTimeoutRef.current) clearTimeout(hoverTimeoutRef.current);
                        if (option) {
                            hoverTimeoutRef.current = setTimeout(() => {
                                setHighlightedOption(option);
                                const map = mapRef.current; if (!map) return;
                                const bounds = new maplibregl.LngLatBounds();
                                if (option.bbox) bounds.extend([[option.bbox[0], option.bbox[1]], [option.bbox[2], option.bbox[3]]]);
                                else extendBoundsWithGeometry(bounds, option.geometry);
                                if (!bounds.isEmpty()) {
                                    const isMobile = isMobileViewport();
                                    // On mobile leave bottom padding for the bottom sheet
                                    const padding = isMobile
                                        ? { top: 60, bottom: 280, left: 40, right: 40 }
                                        : { top: 80, bottom: 80, left: 80, right: 350 };
                                    map.fitBounds(bounds, { padding, maxZoom: 15, duration: 400 });
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
                                ? { top: 60, bottom: 280, left: 40, right: 40 }
                                : { top: 80, bottom: 80, left: 80, right: 350 };
                            const bounds = new maplibregl.LngLatBounds(
                                [f.bbox[0], f.bbox[1]], 
                                [f.bbox[2], f.bbox[3]]
                            );
                            const targetMinZoom = f.minzoom || 10;
                            const camera = map.cameraForBounds(bounds, { padding });
                            if (camera) {
                                const finalZoom = Math.max(camera.zoom || 0, targetMinZoom);
                                map.flyTo({ ...camera, zoom: Math.min(finalZoom, 12.5), duration: 800 });
                            }
                        }
                    }} onClose={clearSelection}
                />
            )}
        </div>
    );
};

export default MapComponent;