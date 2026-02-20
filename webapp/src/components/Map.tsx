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
    bbox?: [number, number, number, number];
    minzoom?: number;
}

interface FeatureOption extends FeatureInfo {
    id: string;
    _groupedSegments?: FeatureOption[];
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
    if (filter) features = map.querySourceFeatures('regulations', { sourceLayer: srcLayer, filter: filter as any });
    
    if (!features.length && feature.geometry) {
        features = [{ geometry: feature.geometry, properties: feature.properties }];
    }
    
    source.setData({ 
        type: 'FeatureCollection', 
        features: features.map(f => ({ 
            type: 'Feature', 
            geometry: f.geometry || f.toJSON?.().geometry, 
            properties: f.properties 
        })) as any 
    });
};

const MapComponent = () => {
    const mapContainerRef = useRef<HTMLDivElement>(null);
    const mapRef = useRef<maplibregl.Map | null>(null);
    const isDisambigOpenRef = useRef<boolean>(false);
    const hoverTimeoutRef = useRef<NodeJS.Timeout | null>(null);
    const searchPollRef = useRef<NodeJS.Timeout | null>(null);
    
    const [selectedFeature, setSelectedFeature] = useState<FeatureInfo | null>(null);
    const [disambigOptions, setDisambigOptions] = useState<FeatureOption[]>([]);
    const [disambigPosition, setDisambigPosition] = useState<{ x: number; y: number } | null>(null);
    const [disambigCollapsed, setDisambigCollapsed] = useState(false);
    const [mobilePanelState, setMobilePanelState] = useState<CollapseState>('expanded');
    const [highlightedOption, setHighlightedOption] = useState<FeatureOption | null>(null);
    const [highlightedSearchResult, setHighlightedSearchResult] = useState<SearchableFeature | null>(null);
    const [searchableFeatures, setSearchableFeatures] = useState<SearchableFeature[]>([]);
    const [layerVisibility, setLayerVisibility] = useState<LayerVisibility>({
        streams: true, lakes: true, wetlands: true, manmade: true, regions: true,
    });

    const clearSelection = useCallback(() => {
        setSelectedFeature(null);
        setHighlightedOption(null);
        setHighlightedSearchResult(null);
        setDisambigOptions([]);
        setDisambigPosition(null);
        isDisambigOpenRef.current = false;
        setMobilePanelState('expanded'); 

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
            
            map.addSource('highlight-source', { type: 'geojson', data: { type: 'FeatureCollection', features: [] }, tolerance: 0.1 });
            map.addLayer({ id: 'highlight-line', type: 'line', source: 'highlight-source', paint: { 'line-color': '#FFD700', 'line-width': ['interpolate', ['linear'], ['zoom'], 4, 2, 8, 4, 12, 5], 'line-opacity': 1 }, layout: { 'line-cap': 'round', 'line-join': 'round' } });
            map.addLayer({ id: 'highlight-fill', type: 'fill', source: 'highlight-source', paint: { 'fill-color': '#FFD700', 'fill-opacity': 0.3 }, filter: ['==', '$type', 'Polygon'] });
            
            map.addSource('selection-source', { type: 'geojson', data: { type: 'FeatureCollection', features: [] }, tolerance: 0.1 });
            map.addLayer({ id: 'selection-line', type: 'line', source: 'selection-source', paint: { 'line-color': '#FF0000', 'line-width': STREAM_LINE_WIDTH, 'line-opacity': 0.9 }, layout: { 'line-cap': 'round', 'line-join': 'round' } });
            map.addLayer({ id: 'selection-fill', type: 'fill', source: 'selection-source', paint: { 'fill-color': '#FF0000', 'fill-opacity': 0.4 }, filter: ['==', '$type', 'Polygon'] });
            
            map.addSource('cursor-circle', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
            map.addLayer({ id: 'cursor-circle-fill', type: 'fill', source: 'cursor-circle', paint: { 'fill-color': '#3b82f6', 'fill-opacity': 0.1 } });
            map.addLayer({ id: 'cursor-circle-line', type: 'line', source: 'cursor-circle', paint: { 'line-color': '#3b82f6', 'line-width': 1.5, 'line-opacity': 0.6 } });
        });

        // -------------------------------------------------------------
        // NEW FIX: Listen for user-initiated pans and close the menu on desktop
        // -------------------------------------------------------------
        map.on('movestart', (e) => {
            // originalEvent ensures the move was triggered by a user (drag, wheel, keyboard), 
            // not a programmatic map.flyTo() or map.fitBounds()
            if (e.originalEvent && window.innerWidth > 768 && isDisambigOpenRef.current) {
                clearSelection();
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
            
            const rawOptions: FeatureOption[] = features.map((f, i) => {
                const props = f.properties || {};
                let idKey = props.linear_feature_id ? 'linear_feature_id' : props.group_id ? 'group_id' : (props.waterbody_key ? 'waterbody_key' : 'id');
                
                const bounds = new maplibregl.LngLatBounds();
                extendBoundsWithGeometry(bounds, f.toJSON().geometry);
                const bbox = bounds.toArray().flat() as [number, number, number, number];

                return { 
                    type: getFeatureType(f.layer.id), 
                    properties: props, 
                    id: (f.id || props[idKey] || `f-${i}`).toString(), 
                    geometry: f.toJSON().geometry, 
                    source: f.layer.source, 
                    sourceLayer: f.layer['source-layer'], 
                    idKey,
                    bbox,
                    minzoom: props.min_zoom || 4
                };
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

        if (mobilePanelState !== 'collapsed') {
            const bounds = new maplibregl.LngLatBounds();
            if (selectedFeature.bbox) {
                bounds.extend([selectedFeature.bbox[0], selectedFeature.bbox[1]]);
                bounds.extend([selectedFeature.bbox[2], selectedFeature.bbox[3]]);
            } else if (selectedFeature.geometry) {
                extendBoundsWithGeometry(bounds, selectedFeature.geometry);
            }

            if (!bounds.isEmpty()) {
                const isMobile = window.innerWidth <= 768;
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
        };
    }, [selectedFeature, mobilePanelState]);

    const handleSearchSelect = useCallback((feature: SearchableFeature) => {
        const map = mapRef.current;
        if (!map) return;
        
        clearSelection();
        // Handle both singular and plural type names from search data
        const srcLayer = (feature.type === 'stream' || feature.type === 'streams') ? 'streams' : 'lakes';
        const normalizedType = (feature.type === 'streams' ? 'stream' : feature.type === 'lakes' ? 'lake' : feature.type) as 'stream' | 'lake' | 'wetland' | 'manmade';
        const displayName = (feature.gnis_name && feature.gnis_name.toLowerCase() !== 'unnamed') ? feature.gnis_name : feature.regulation_names?.[0];
        const targetMinZoom = feature.properties.minzoom || 10;
        
        setSelectedFeature({ 
            type: normalizedType, 
            properties: { ...feature.properties, gnis_name: displayName, regulation_names: feature.regulation_names }, 
            source: 'regulations', 
            sourceLayer: srcLayer,
            bbox: feature.bbox as [number, number, number, number],
            minzoom: targetMinZoom
        });
        
        setMobilePanelState('partial');

        if (feature.bbox) {
            const isMobile = window.innerWidth <= 768;
            const padding = isMobile ? { top: 60, bottom: 250, left: 40, right: 40 } : { top: 80, bottom: 80, left: 80, right: 350 };
            const bounds = new maplibregl.LngLatBounds([feature.bbox[0], feature.bbox[1]], [feature.bbox[2], feature.bbox[3]]);
            
            const camera = map.cameraForBounds(bounds, { padding });
            if (camera) {
                const finalZoom = Math.max(camera.zoom || 0, targetMinZoom);
                map.flyTo({ ...camera, zoom: Math.min(finalZoom, 12.5), duration: 800 });
            }
        }

        let attempts = 0;
        searchPollRef.current = setInterval(() => {
            attempts++;
            const filter = buildFeatureFilter(feature);
            let found = map.querySourceFeatures('regulations', { sourceLayer: srcLayer, filter: filter as any });
            
            if (found.length === 0 && feature.bbox) {
                const pt = map.project([(feature.bbox[0]+feature.bbox[2])/2, (feature.bbox[1]+feature.bbox[3])/2]);
                const hits = map.queryRenderedFeatures(pt, { layers: INTERACTABLE_LAYERS });
                if (hits.length > 0) found = hits as any[];
            }
            
            if (found.length > 0 || attempts > 20) {
                if (found.length > 0) {
                    setSelectedFeature(prev => prev ? { ...prev, geometry: found[0].geometry, id: found[0].id } : null);
                }
                if (searchPollRef.current) clearInterval(searchPollRef.current);
            }
        }, 200);
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
                        setHighlightedOption(f as any); 
                    }} 
                    placeholder="Search waterbodies..." 
                />
            </div>
            <InfoPanel feature={selectedFeature} onClose={clearSelection} collapseState={mobilePanelState} onSetCollapseState={setMobilePanelState} />
            {disambigOptions.length > 0 && (
                <DisambiguationMenu 
                    options={disambigOptions as any} position={disambigPosition} highlightedOption={highlightedOption as any}
                    isCollapsed={disambigCollapsed}
                    onSetCollapse={setDisambigCollapsed}
                    onHighlight={(option) => {
                        if (hoverTimeoutRef.current) clearTimeout(hoverTimeoutRef.current);
                        if (option) {
                            hoverTimeoutRef.current = setTimeout(() => {
                                setHighlightedOption(option as any);
                                const map = mapRef.current; if (!map) return;
                                const bounds = new maplibregl.LngLatBounds(); 
                                if (option.bbox) bounds.extend([[option.bbox[0], option.bbox[1]], [option.bbox[2], option.bbox[3]]]);
                                else extendBoundsWithGeometry(bounds, option.geometry);
                                
                                if (!bounds.isEmpty()) {
                                    const isMobile = window.innerWidth <= 768;
                                    const padding = isMobile ? { top: 80, bottom: 250, left: 80, right: 80 } : { top: 80, bottom: 80, left: 80, right: 350 };
                                    map.fitBounds(bounds, { padding, maxZoom: 12.5, duration: 400 });
                                }
                            }, 50);
                        } else { 
                            setHighlightedOption(null); 
                        }
                    }}
                    onSelect={f => { clearSelection(); setSelectedFeature(f as any); setMobilePanelState('partial'); }} onClose={clearSelection} 
                />
            )}
        </div>
    );
};

export default MapComponent;