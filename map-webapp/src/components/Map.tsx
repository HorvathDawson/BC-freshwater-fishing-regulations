import { useEffect, useRef, useState, useCallback } from 'react';
import maplibregl from 'maplibre-gl';
import { Protocol } from 'pmtiles';
import { layers, LIGHT } from '@protomaps/basemaps'; 
import 'maplibre-gl/dist/maplibre-gl.css';
import { createRegulationLayers } from '../map/styles';
import InfoPanel from './InfoPanel';
import DisambiguationMenu from './DisambiguationMenu';
import SearchBar from './SearchBar';
import type { SearchableFeature } from './SearchBar';
import './Map.css';

// --- CONFIG & PROTOCOL ---
const protocol = new Protocol();
maplibregl.addProtocol('pmtiles', protocol.tile);

const INTERACTABLE_LAYERS = ['streams', 'lakes-fill', 'wetlands-fill', 'manmade-fill'];
const MAP_LAYERS = ['streams', 'lakes', 'wetlands', 'manmade', 'regions'] as const;

// --- STYLE EXPRESSIONS ---
const STREAM_LINE_WIDTH = ['interpolate', ['linear'], ['zoom'], 4, ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.1]], 8, ['+', 2, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.15]], 11, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 1.5], 12, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 2], 14, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 3], 16, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 4]];

// --- TYPES ---
interface LayerVisibility {
    streams: boolean; lakes: boolean; wetlands: boolean; manmade: boolean; regions: boolean;
}

interface FeatureInfo {
    type: 'stream' | 'lake' | 'wetland' | 'manmade';
    properties: Record<string, any>;
    geometry?: any; 
    id?: string | number;
    source?: string;
    sourceLayer?: string;
    idKey?: string;
    _segmentCount?: number;
}

interface FeatureOption extends FeatureInfo {
    id: string;
    _groupedSegments?: FeatureOption[];
    minzoom?: number;
}

type CollapseState = 'expanded' | 'partial' | 'collapsed';

// --- UTILITY ---
const isValidBbox = (bbox: any): boolean => {
    if (!bbox || !Array.isArray(bbox) || bbox.length !== 4) return false;
    const [minx, miny, maxx, maxy] = bbox;
    return minx >= -180 && minx <= 180 && maxx >= -180 && maxx <= 180 &&
           miny >= -90 && miny <= 90 && maxy >= -90 && maxy <= 90 &&
           minx < maxx && miny < maxy;
};

const extendBoundsWithGeometry = (bounds: maplibregl.LngLatBounds, geometry: any) => {
    if (!geometry || !geometry.coordinates) return;
    const processCoords = (coords: any) => {
        if (Array.isArray(coords) && typeof coords[0] === 'number') bounds.extend(coords as [number, number]);
        else if (Array.isArray(coords)) coords.forEach(processCoords);
    };
    if (geometry.type === 'Point') bounds.extend(geometry.coordinates);
    else if (geometry.type === 'LineString') geometry.coordinates.forEach((coord: any) => bounds.extend(coord));
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
    return { type: 'Polygon', coordinates: [coords] };
};

const createWetlandPattern = () => {
    const canvas = document.createElement('canvas');
    canvas.width = 16; canvas.height = 16;
    const ctx = canvas.getContext('2d');
    if (!ctx) return null;
    ctx.strokeStyle = '#000000'; ctx.lineWidth = 1;
    for (let i = -16; i < 32; i += 4) {
        ctx.beginPath(); ctx.moveTo(i, 0); ctx.lineTo(i + 16, 16); ctx.stroke();
    }
    return ctx.getImageData(0, 0, 16, 16);
};

const getFeatureType = (layerId: string): 'stream' | 'lake' | 'wetland' | 'manmade' => {
    if (layerId.includes('streams')) return 'stream';
    if (layerId.includes('lakes')) return 'lake';
    if (layerId.includes('wetlands')) return 'wetland';
    return 'manmade';
};

const buildFeatureFilter = (feature: any): any[] | null => {
    const props = feature.properties || {};
    const name = props.gnis_name || props.lake_name || feature.gnis_name || feature.name;
    const isUnnamed = !name || name.toLowerCase() === 'unnamed' || name === 'Unnamed Waterbody';

    if (isUnnamed) {
        if (props.linear_feature_id) return ['==', ['get', 'linear_feature_id'], props.linear_feature_id];
        if (props.waterbody_key) return ['==', ['get', 'waterbody_key'], props.waterbody_key];
        if (feature.id && typeof feature.id === 'number') return ['==', ['id'], feature.id];
        return null;
    }

    const regIds = props.regulation_ids || feature.regulation_ids;
    if (regIds) return ['all', ['==', ['get', 'gnis_name'], name], ['==', ['get', 'regulation_ids'], regIds]];
    return ['==', ['get', 'gnis_name'], name];
};

const MapComponent = () => {
    const mapContainerRef = useRef<HTMLDivElement>(null);
    const mapRef = useRef<maplibregl.Map | null>(null);
    const isDisambigOpenRef = useRef<boolean>(false);
    const hoverTimeoutRef = useRef<NodeJS.Timeout | null>(null);
    
    const [selectedFeature, setSelectedFeature] = useState<FeatureInfo | null>(null);
    const [disambigOptions, setDisambigOptions] = useState<FeatureOption[]>([]);
    const [disambigPosition, setDisambigPosition] = useState<{ x: number; y: number } | null>(null);
    const [mobilePanelState, setMobilePanelState] = useState<CollapseState>('expanded');
    const [highlightedOption, setHighlightedOption] = useState<FeatureOption | null>(null);
    const [highlightedSearchResult, setHighlightedSearchResult] = useState<SearchableFeature | null>(null);
    const [searchableFeatures, setSearchableFeatures] = useState<SearchableFeature[]>([]);
    const [layerVisibility, setLayerVisibility] = useState<LayerVisibility>({
        streams: true, lakes: true, wetlands: true, manmade: true, regions: true,
    });

    const clearSelection = useCallback(() => {
        setSelectedFeature(null);
        setDisambigOptions([]);
        setDisambigPosition(null);
        isDisambigOpenRef.current = false;
        setMobilePanelState('expanded'); 
        const map = mapRef.current;
        if (!map) return;
        ['cursor-circle', 'highlight-source', 'selection-source'].forEach(id => {
            const src = map.getSource(id) as maplibregl.GeoJSONSource;
            if (src) src.setData({ type: 'FeatureCollection', features: [] });
        });
    }, []);

    useEffect(() => {
        fetch('/data/search_index.json').then(res => res.json()).then(data => {
            const grouped: Record<string, SearchableFeature> = {};
            (data.waterbodies || []).forEach((item: any) => {
                const displayName = (item.gnis_name && item.gnis_name.toLowerCase() !== 'unnamed') ? item.gnis_name : (item.regulation_names?.[0] || 'Unnamed Waterbody');
                const feature: SearchableFeature = {
                    id: item.id, gnis_name: displayName, name: displayName, type: item.type,
                    regulation_names: item.regulation_names || [],
                    properties: { ...item.properties, zones: item.zones || '', mgmt_units: item.mgmt_units, regulation_ids: item.regulation_ids, minzoom: item.min_zoom || 4 },
                    bbox: isValidBbox(item.bbox) ? item.bbox : undefined
                };
                const groupKey = (feature.gnis_name && feature.gnis_name !== 'Unnamed Waterbody') ? `${feature.gnis_name}|${feature.properties.regulation_ids}` : `unnamed-${feature.id}`;
                grouped[groupKey] = feature;
            });
            setSearchableFeatures(Object.values(grouped));
        }).catch(console.error);
    }, []);

    useEffect(() => {
        if (!mapContainerRef.current) return;
        const map = new maplibregl.Map({
            container: mapContainerRef.current,
            style: {
                version: 8,
                glyphs: 'https://protomaps.github.io/basemaps-assets/fonts/{fontstack}/{range}.pbf',
                sources: {
                    protomaps: { type: 'vector', url: 'pmtiles:///data/bc.pmtiles', attribution: 'Protomaps', maxzoom: 15 },
                    regulations: { type: 'vector', url: 'pmtiles:///data/regulations_merged.pmtiles', attribution: 'FWA BC', minzoom: 4, maxzoom: 12 }
                },
                layers: [...layers('protomaps', LIGHT), ...createRegulationLayers()]
            },
            center: [-123.0, 49.25], zoom: 8, maxZoom: 12.5, minZoom: 4, hash: true, attributionControl: { compact: true }
        });

        map.on('load', () => {
            const pattern = createWetlandPattern();
            if (pattern) map.addImage('wetland-pattern', pattern);
            map.addSource('highlight-source', { type: 'geojson', data: { type: 'FeatureCollection', features: [] }, tolerance: 0.375 });
            map.addLayer({ id: 'highlight-line', type: 'line', source: 'highlight-source', paint: { 'line-color': '#FFD700', 'line-width': ['interpolate', ['linear'], ['zoom'], 4, 2, 8, 4, 12, 5], 'line-opacity': 1 }, layout: { 'line-cap': 'round', 'line-join': 'round' } });
            map.addLayer({ id: 'highlight-fill', type: 'fill', source: 'highlight-source', paint: { 'fill-color': '#FFD700', 'fill-opacity': 0.3 }, filter: ['==', '$type', 'Polygon'] });
            map.addSource('selection-source', { type: 'geojson', data: { type: 'FeatureCollection', features: [] }, tolerance: 0.375 });
            map.addLayer({ id: 'selection-line', type: 'line', source: 'selection-source', paint: { 'line-color': '#FF0000', 'line-width': STREAM_LINE_WIDTH, 'line-opacity': 0.9 }, layout: { 'line-cap': 'round', 'line-join': 'round' } });
            map.addLayer({ id: 'selection-fill', type: 'fill', source: 'selection-source', paint: { 'fill-color': '#FF0000', 'fill-opacity': 0.4 }, filter: ['==', '$type', 'Polygon'] });
            map.addSource('cursor-circle', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
            map.addLayer({ id: 'cursor-circle-fill', type: 'fill', source: 'cursor-circle', paint: { 'fill-color': '#3b82f6', 'fill-opacity': 0.1 } });
            map.addLayer({ id: 'cursor-circle-line', type: 'line', source: 'cursor-circle', paint: { 'line-color': '#3b82f6', 'line-width': 1.5, 'line-opacity': 0.6 } });
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
            const rawOptions: FeatureOption[] = features.map((f, i) => {
                const props = f.properties || {};
                let idKey = props.linear_feature_id ? 'linear_feature_id' : props.group_id ? 'group_id' : (props.waterbody_key ? 'waterbody_key' : 'id');
                return { type: getFeatureType(f.layer.id), properties: props, id: (f.id || props[idKey] || `f-${i}`).toString(), geometry: f.toJSON().geometry, source: f.layer.source, sourceLayer: f.layer['source-layer'], idKey };
            });

            const grouped: FeatureOption[] = [];
            const processedKeys = new Set<string>();
            rawOptions.forEach(opt => {
                const name = opt.properties.gnis_name || opt.properties.lake_name;
                const isUnnamed = !name || name.toLowerCase() === 'unnamed';
                const compositeKey = isUnnamed ? `unnamed-${opt.id}` : `${name}|${opt.properties.regulation_ids}`;
                if (!processedKeys.has(compositeKey)) {
                    processedKeys.add(compositeKey);
                    grouped.push(opt);
                }
            });

            clearSelection();
            if (grouped.length === 1) setSelectedFeature(grouped[0]);
            else { setDisambigOptions(grouped); setDisambigPosition({ x: e.point.x, y: e.point.y }); isDisambigOpenRef.current = true; }
        });

        mapRef.current = map;
        return () => map.remove();
    }, [clearSelection]);

    useEffect(() => {
        const map = mapRef.current;
        if (!map || !selectedFeature) return;
        const source = map.getSource('selection-source') as maplibregl.GeoJSONSource;
        if (!source) return;

        const filter = buildFeatureFilter(selectedFeature);
        const srcLayer = selectedFeature.sourceLayer || (selectedFeature.type === 'stream' ? 'streams' : 'lakes');
        let features: any[] = [];
        if (filter) features = map.querySourceFeatures('regulations', { sourceLayer: srcLayer, filter: filter as any });
        if (!features.length && selectedFeature.geometry) features = [{ geometry: selectedFeature.geometry, properties: selectedFeature.properties }];

        source.setData({ type: 'FeatureCollection', features: features.map(f => ({ type: 'Feature', geometry: f.geometry || f.toJSON?.().geometry, properties: f.properties })) as any });

        if (mobilePanelState !== 'collapsed') {
            const bounds = new maplibregl.LngLatBounds();
            features.forEach(f => extendBoundsWithGeometry(bounds, f.geometry || f.toJSON?.().geometry));
            if (!bounds.isEmpty()) {
                const isMobile = window.innerWidth <= 768;
                const padding = isMobile ? { top: 60, bottom: 250, left: 40, right: 40 } : { top: 80, bottom: 80, left: 80, right: 350 };
                const fMinZoom = (selectedFeature as FeatureOption).minzoom || selectedFeature.properties?.['tippecanoe:minzoom'] || 4;
                map.fitBounds(bounds, { padding, maxZoom: 12.5, duration: 800 });
                setTimeout(() => { if (map.getZoom() < fMinZoom) map.easeTo({ zoom: fMinZoom + 0.2, duration: 400 }); }, 850);
            }
        }
    }, [selectedFeature, mobilePanelState]);

    const updateHighlight = useCallback((option: FeatureOption | null) => {
        const map = mapRef.current;
        if (!map || !map.isStyleLoaded()) return;
        const source = map.getSource('highlight-source') as maplibregl.GeoJSONSource;
        if (!source || !option) return source?.setData({ type: 'FeatureCollection', features: [] });
        const srcLayer = option.sourceLayer || (option.type === 'stream' ? 'streams' : 'lakes');
        let features: any[] = [];
        const filter = buildFeatureFilter(option);
        if (filter) features = map.querySourceFeatures('regulations', { sourceLayer: srcLayer, filter: filter as any });
        if (!features.length && option.geometry) features = [{ geometry: option.geometry, properties: option.properties } as any];
        source.setData({ type: 'FeatureCollection', features: features.map(f => ({ type: 'Feature', geometry: f.geometry || f.toJSON?.().geometry, properties: f.properties })) as any });
    }, []);

    const handleSearchSelect = useCallback((feature: SearchableFeature) => {
        const map = mapRef.current;
        if (!map) return;
        clearSelection();
        const srcLayer = feature.type === 'stream' ? 'streams' : 'lakes';
        const displayName = (feature.gnis_name && feature.gnis_name.toLowerCase() !== 'unnamed') ? feature.gnis_name : feature.regulation_names?.[0];

        setSelectedFeature({ type: feature.type, properties: { ...feature.properties, gnis_name: displayName, regulation_names: feature.regulation_names }, source: 'regulations', sourceLayer: srcLayer });
        setMobilePanelState('partial');

        if (feature.bbox) {
            const isMobile = window.innerWidth <= 768;
            const padding = isMobile ? { top: 60, bottom: 250, left: 40, right: 40 } : { top: 80, bottom: 80, left: 80, right: 350 };
            map.fitBounds(new maplibregl.LngLatBounds([feature.bbox[0], feature.bbox[1]], [feature.bbox[2], feature.bbox[3]]), { padding, maxZoom: 12.5, duration: 800 });
        }

        let attempts = 0;
        const poll = setInterval(() => {
            attempts++;
            const filter = buildFeatureFilter(feature);
            let found = map.querySourceFeatures('regulations', { sourceLayer: srcLayer, filter: filter as any });
            if (found.length === 0 && feature.bbox) {
                const pt = map.project([(feature.bbox[0]+feature.bbox[2])/2, (feature.bbox[1]+feature.bbox[3])/2]);
                const hits = map.queryRenderedFeatures(pt, { layers: INTERACTABLE_LAYERS });
                if (hits.length > 0) found = hits as any[];
            }
            if (found.length > 0 || attempts > 20) {
                if (found.length > 0) setSelectedFeature({ type: feature.type, properties: found[0].properties || {}, geometry: found[0].geometry, source: 'regulations', sourceLayer: found[0].layer?.['source-layer'] || srcLayer, id: found[0].id });
                clearInterval(poll);
            }
        }, 200);
    }, [clearSelection]);

    return (
        <div className="map-container">
            <div ref={mapContainerRef} className="map-canvas" />
            <div className="map-menu-wrapper">
                <SearchBar features={searchableFeatures} onSelect={handleSearchSelect} highlightedResult={highlightedSearchResult} onHighlight={f => { setHighlightedSearchResult(f); updateHighlight(f as any); }} placeholder="Search waterbodies..." />
            </div>
            <InfoPanel feature={selectedFeature} onClose={clearSelection} collapseState={mobilePanelState} onSetCollapseState={setMobilePanelState} />
            {disambigOptions.length > 0 && (
                <DisambiguationMenu 
                    options={disambigOptions as any} position={disambigPosition} highlightedOption={highlightedOption as any}
                    onHighlight={(option) => {
                        if (hoverTimeoutRef.current) clearTimeout(hoverTimeoutRef.current);
                        if (option) {
                            hoverTimeoutRef.current = setTimeout(() => {
                                setHighlightedOption(option as any); updateHighlight(option as any);
                                const map = mapRef.current; if (!map) return;
                                const bounds = new maplibregl.LngLatBounds(); extendBoundsWithGeometry(bounds, option.geometry);
                                if (!bounds.isEmpty()) {
                                    const isMobile = window.innerWidth <= 768;
                                    const padding = isMobile ? { top: 80, bottom: 250, left: 80, right: 80 } : { top: 80, bottom: 80, left: 80, right: 350 };
                                    map.fitBounds(bounds, { padding, maxZoom: 12.5, duration: 400 });
                                }
                            }, 50);
                        } else { setHighlightedOption(null); updateHighlight(null); }
                    }}
                    onSelect={f => { clearSelection(); setSelectedFeature(f as any); setMobilePanelState('partial'); }} onClose={clearSelection} 
                />
            )}
        </div>
    );
};

export default MapComponent;