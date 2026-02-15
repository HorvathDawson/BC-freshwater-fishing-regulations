import { useEffect, useRef, useState, useCallback } from 'react';
import maplibregl from 'maplibre-gl';
import { Protocol } from 'pmtiles';
import { layers, LIGHT } from '@protomaps/basemaps'; 
import { Layers, X } from 'lucide-react';
import 'maplibre-gl/dist/maplibre-gl.css';
import { createRegulationLayers } from '../map/styles';
import InfoPanel from './InfoPanel';
import DisambiguationMenu from './DisambiguationMenu';
import './Map.css';

// --- CONFIG & PROTOCOL ---
const protocol = new Protocol();
maplibregl.addProtocol('pmtiles', protocol.tile);

const INTERACTABLE_LAYERS = ['streams', 'lakes-fill', 'wetlands-fill', 'manmade-fill'];
const MAP_LAYERS = ['streams', 'lakes', 'wetlands', 'manmade', 'regions'] as const;

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

// --- PURE UTILITY FUNCTIONS ---
const calculateGeometrySize = (geometry: any): number => {
    if (!geometry || !geometry.coordinates) return 0;
    const calcLineLen = (coords: number[][]) => {
        let len = 0;
        for (let i = 0; i < coords.length - 1; i++) {
            const dx = coords[i + 1][0] - coords[i][0];
            const dy = coords[i + 1][1] - coords[i][1];
            len += Math.sqrt(dx * dx + dy * dy);
        }
        return len;
    };
    if (geometry.type === 'LineString') return calcLineLen(geometry.coordinates);
    if (geometry.type === 'MultiLineString') return geometry.coordinates.reduce((acc: number, line: number[][]) => acc + calcLineLen(line), 0);
    if (geometry.type === 'Polygon') return calcLineLen(geometry.coordinates[0]);
    if (geometry.type === 'MultiPolygon') return Math.max(...geometry.coordinates.map((poly: number[][][]) => calcLineLen(poly[0])));
    return 0;
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

// Generates standardized filters for querying or highlighting features
const buildFeatureFilter = (feature: FeatureOption): any[] | null => {
    const gnisName = feature.properties?.gnis_name || feature.properties?.lake_name;
    const regIds = feature.properties?.regulation_ids;
    
    if (gnisName && regIds) return ['all', ['==', ['get', 'gnis_name'], gnisName], ['==', ['get', 'regulation_ids'], regIds]];
    if (gnisName) return ['==', ['get', 'gnis_name'], gnisName];
    if (feature.properties?.group_id) return ['==', ['get', 'group_id'], feature.properties.group_id];
    if (feature.properties?.waterbody_key) return ['==', ['get', 'waterbody_key'], feature.properties.waterbody_key];
    
    if (feature.idKey && feature.id) {
        const idVal = feature.id.toString();
        const numId = parseInt(idVal);
        return ['any', ['==', ['get', feature.idKey], idVal], ['==', ['get', feature.idKey], isNaN(numId) ? -1 : numId]];
    }
    
    // Fallback for grouped segments with no common name
    if (feature._groupedSegments && feature._groupedSegments.length > 0) {
        const ids = feature._groupedSegments.map(s => s.properties?.group_id || s.properties?.waterbody_key).filter(Boolean);
        const idKey = feature._groupedSegments[0]?.idKey || 'group_id';
        if (ids.length > 0) return ['any', ...ids.map(id => ['==', ['get', idKey], id])];
    }
    
    return null;
};

// --- MAPLIBRE STYLE EXPRESSIONS ---
const STREAM_LINE_WIDTH = [
    'interpolate', ['linear'], ['zoom'],
    4, ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.1]],
    8, ['+', 2, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.15]],
    11, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 1.5],
    12, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 2],
    14, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 3],
    16, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 4]
];
const POLYGON_LINE_WIDTH = ['interpolate', ['linear'], ['zoom'], 4, 3, 8, 4, 10, 5, 12, 6];

// --- MAIN COMPONENT ---
const Map = () => {
    const mapContainerRef = useRef<HTMLDivElement>(null);
    const mapRef = useRef<maplibregl.Map | null>(null);
    const isDisambigOpenRef = useRef<boolean>(false);
    const highlightedOptionRef = useRef<FeatureOption | null>(null);
    const cachedBoundsRef = useRef<Record<string, maplibregl.LngLatBounds>>({});
    
    const [selectedFeature, setSelectedFeature] = useState<FeatureInfo | null>(null);
    const [disambigOptions, setDisambigOptions] = useState<FeatureOption[]>([]);
    const [disambigPosition, setDisambigPosition] = useState<{ x: number; y: number } | null>(null);
    const [mobilePanelState, setMobilePanelState] = useState<CollapseState>('expanded');
    const [highlightedOption, setHighlightedOption] = useState<FeatureOption | null>(null);
    const [isLayerMenuOpen, setIsLayerMenuOpen] = useState(() => window.innerWidth > 768);
    
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
        
        ['cursor-circle', 'highlight-source'].forEach(id => {
            const src = map.getSource(id) as maplibregl.GeoJSONSource;
            if (src) src.setData({ type: 'FeatureCollection', features: [] });
        });
        ['selection-highlight-fill', 'selection-highlight-line'].forEach(id => {
            if (map.getLayer(id)) map.removeLayer(id);
        });
    }, []);

    // Effect: Pre-calculate Bounds
    useEffect(() => {
        const newBounds: Record<string, maplibregl.LngLatBounds> = {};
        disambigOptions.forEach((option) => {
            const bounds = new maplibregl.LngLatBounds();
            extendBoundsWithGeometry(bounds, option.geometry);
            option._groupedSegments?.forEach(seg => extendBoundsWithGeometry(bounds, seg.geometry));
            if (!bounds.isEmpty()) newBounds[option.id] = bounds;
        });
        cachedBoundsRef.current = newBounds;
    }, [disambigOptions]);

    // Effect: Initialize Map
    useEffect(() => {
        if (!mapContainerRef.current) return;

        // Cleanup local storage attribution cache
        Object.keys(localStorage).filter(k => k.includes('maplibregl-attrib')).forEach(k => localStorage.removeItem(k));

        const map = new maplibregl.Map({
            container: mapContainerRef.current,
            style: {
                version: 8,
                glyphs: 'https://protomaps.github.io/basemaps-assets/fonts/{fontstack}/{range}.pbf',
                sources: {
                    protomaps: { type: 'vector', url: 'pmtiles:///data/bc.pmtiles', attribution: '<a href="https://protomaps.com">Protomaps</a> © <a href="https://openstreetmap.org">OpenStreetMap</a>', maxzoom: 15 },
                    regulations: { type: 'vector', url: 'pmtiles:///data/regulations_merged.pmtiles', attribution: 'FWA BC, Province of British Columbia', minzoom: 4, maxzoom: 12 }
                },
                layers: [...layers('protomaps', LIGHT), ...createRegulationLayers()]
            },
            center: [-123.0, 49.25], zoom: 8, maxZoom: 12, minZoom: 4, hash: true, attributionControl: { compact: true }
        });

        const pattern = createWetlandPattern();
        if (pattern) map.addImage('wetland-pattern', pattern);
        map.addControl(new maplibregl.NavigationControl(), 'top-right');
        map.addControl(new maplibregl.ScaleControl(), 'bottom-left');

        // Attribution compact hack
        const observer = new MutationObserver(() => {
            const el = document.querySelector('.maplibregl-ctrl-attrib');
            if (el?.classList.contains('maplibregl-compact-show') && !map.loaded()) el.classList.remove('maplibregl-compact-show');
        });
        const attribContainer = document.querySelector('.maplibregl-ctrl-bottom-right');
        if (attribContainer) observer.observe(attribContainer, { attributes: true, attributeFilter: ['class'], subtree: true });
        map.once('load', () => observer.disconnect());

        map.on('load', () => {
            map.addSource('highlight-source', { type: 'geojson', data: { type: 'FeatureCollection', features: [] }, tolerance: 0.375 });
            map.addLayer({ id: 'highlight-line', type: 'line', source: 'highlight-source', paint: { 'line-color': '#FFD700', 'line-width': ['interpolate', ['linear'], ['zoom'], 4, 2, 8, 4, 12, 5], 'line-opacity': 1, 'line-blur': ['interpolate', ['linear'], ['zoom'], 4, 1, 8, 0.5, 12, 0]} });
            map.addLayer({ id: 'highlight-fill', type: 'fill', source: 'highlight-source', paint: { 'fill-color': '#FFD700', 'fill-opacity': 0.3 }, filter: ['==', '$type', 'Polygon'] });
            
            map.addSource('cursor-circle', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
            map.addLayer({ id: 'cursor-circle-fill', type: 'fill', source: 'cursor-circle', paint: { 'fill-color': '#3b82f6', 'fill-opacity': 0.1 } });
            map.addLayer({ id: 'cursor-circle-line', type: 'line', source: 'cursor-circle', paint: { 'line-color': '#3b82f6', 'line-width': 1.5, 'line-opacity': 0.6 } });
        });

        map.on('movestart', (e) => { if (e.originalEvent) setMobilePanelState('collapsed'); });

        map.on('mousemove', (e) => {
            if (!map.isStyleLoaded()) return;
            const features = map.queryRenderedFeatures([[e.point.x - 10, e.point.y - 10], [e.point.x + 10, e.point.y + 10]], { layers: INTERACTABLE_LAYERS });
            map.getCanvas().style.cursor = features.length > 0 ? 'pointer' : '';

            if (isDisambigOpenRef.current) return;
            const src = map.getSource('cursor-circle') as maplibregl.GeoJSONSource;
            if (src) src.setData({ type: 'FeatureCollection', features: [{ type: 'Feature', geometry: createCirclePolygon(e.lngLat, map.getZoom()), properties: {} }] });
        });

        map.on('mouseleave', () => {
            map.getCanvas().style.cursor = '';
            if (!isDisambigOpenRef.current) (map.getSource('cursor-circle') as maplibregl.GeoJSONSource)?.setData({ type: 'FeatureCollection', features: [] });
        });

        const handleMapClick = (point: maplibregl.Point, lngLat: maplibregl.LngLat) => {
            if (!map.isStyleLoaded()) return;
            const features = map.queryRenderedFeatures([[point.x - 15, point.y - 15], [point.x + 15, point.y + 15]], { layers: INTERACTABLE_LAYERS });

            if (!features.length) return clearSelection();

            try {
                const options: FeatureOption[] = features.map((f, i) => {
                    const props = f.properties || {};
                    let idKey = 'group_id'; let idVal = props.group_id;
                    if (!idVal) {
                        idKey = props.linear_feature_id ? 'linear_feature_id' : props.waterbody_key ? 'waterbody_key' : 'id';
                        idVal = props[idKey] || f.id;
                    }
                    return {
                        type: getFeatureType(f.layer.id), properties: props, id: (idVal || `f-${i}`).toString(),
                        geometry: f.toJSON().geometry, source: f.layer.source, sourceLayer: f.layer['source-layer'],
                        idKey, minzoom: props['tippecanoe:minzoom']
                    };
                });

                const deduped = options.filter((opt, i, self) => i === self.findIndex(o => o.id === opt.id && o.idKey === opt.idKey));
                const expanded: FeatureOption[] = [];
                const processed = new Set<string>();

                deduped.forEach(opt => {
                    const filter = buildFeatureFilter(opt);
                    const compositeKey = `${opt.properties?.gnis_name || opt.properties?.lake_name}|${opt.properties?.regulation_ids}`;
                    
                    if (filter && compositeKey !== '|') {
                        if (!processed.has(compositeKey)) {
                            processed.add(compositeKey);
                            const matching = map.querySourceFeatures('regulations', { sourceLayer: opt.sourceLayer, filter });
                            const grouped = Object.values(matching.reduce((acc: any, f) => {
                                const gId = f.properties?.group_id || f.properties?.waterbody_key;
                                if (gId && !acc[gId]) {
                                    acc[gId] = { ...opt, properties: f.properties, id: gId.toString(), geometry: f.toJSON().geometry, idKey: f.properties?.group_id ? 'group_id' : 'waterbody_key' };
                                }
                                return acc;
                            }, {}));
                            
                            if (grouped.length) expanded.push({ ...grouped[0], _groupedSegments: grouped.length > 1 ? grouped : undefined, _segmentCount: grouped.length > 1 ? grouped.length : undefined });
                        }
                    } else expanded.push(opt);
                });

                expanded.sort((a, b) => {
                    if (a.type === 'manmade' && b.type !== 'manmade') return 1;
                    if (a.type !== 'manmade' && b.type === 'manmade') return -1;
                    if (a.type === 'lake' && b.type !== 'lake') return -1;
                    if (a.type !== 'lake' && b.type === 'lake') return 1;
                    const orderDiff = (b.properties.stream_order ?? -1) - (a.properties.stream_order ?? -1);
                    if (orderDiff !== 0) return orderDiff;
                    const hasNameA = !!(a.properties.gnis_name || a.properties.lake_name || a.properties.name);
                    const hasNameB = !!(b.properties.gnis_name || b.properties.lake_name || b.properties.name);
                    if (hasNameA && !hasNameB) return -1;
                    if (!hasNameA && hasNameB) return 1;
                    return calculateGeometrySize(b.geometry) - calculateGeometrySize(a.geometry);
                });

                clearSelection();
                if (expanded.length === 1) {
                    setSelectedFeature(expanded[0]);
                } else if (expanded.length > 1) {
                    (map.getSource('cursor-circle') as maplibregl.GeoJSONSource)?.setData({ type: 'FeatureCollection', features: [{ type: 'Feature', geometry: createCirclePolygon(lngLat, map.getZoom()), properties: {} }] });
                    setDisambigOptions(expanded);
                    setDisambigPosition({ x: point.x, y: point.y });
                    isDisambigOpenRef.current = true;
                }
            } catch (err) { clearSelection(); }
        };

        map.on('click', (e) => handleMapClick(e.point, e.lngLat));

        // Mobile Touch Logic
        let touchStart: { x: number, y: number, time: number } | null = null;
        const canvas = map.getCanvas();
        canvas.addEventListener('touchstart', (e) => {
            if (e.touches.length === 1) touchStart = { x: e.touches[0].clientX, y: e.touches[0].clientY, time: Date.now() };
            else touchStart = null;
        });
        canvas.addEventListener('touchend', (e) => {
            if (!touchStart || e.touches.length > 0) return;
            const touch = e.changedTouches[0];
            const dist = Math.sqrt(Math.pow(touch.clientX - touchStart.x, 2) + Math.pow(touch.clientY - touchStart.y, 2));
            if (dist < 10 && Date.now() - touchStart.time < 300) {
                const rect = canvas.getBoundingClientRect();
                const point = new maplibregl.Point(touch.clientX - rect.left, touch.clientY - rect.top);
                handleMapClick(point, map.unproject(point));
            }
            touchStart = null;
        });

        mapRef.current = map;
        return () => { map.remove(); mapRef.current = null; };
    }, [clearSelection]);

    // Effect: Selection Highlight & Zoom
    useEffect(() => {
        const map = mapRef.current;
        if (!map) return;
        ['selection-highlight-fill', 'selection-highlight-line'].forEach(id => { if (map.getLayer(id)) map.removeLayer(id); });

        if (!selectedFeature?.source || !selectedFeature?.sourceLayer) return;

        try {
            const filter = buildFeatureFilter(selectedFeature as FeatureOption);
            if (!filter) return;

            map.addLayer({
                id: 'selection-highlight-line', type: 'line', source: selectedFeature.source, 'source-layer': selectedFeature.sourceLayer,
                paint: { 'line-color': '#FF0000', 'line-width': selectedFeature.type === 'stream' ? STREAM_LINE_WIDTH : POLYGON_LINE_WIDTH, 'line-opacity': 0.9 }, filter
            });

            if (selectedFeature.type !== 'stream') {
                map.addLayer({ id: 'selection-highlight-fill', type: 'fill', source: selectedFeature.source, 'source-layer': selectedFeature.sourceLayer, paint: { 'fill-color': '#FF0000', 'fill-opacity': 0.4 }, filter });
            }

            if (mobilePanelState !== 'collapsed') {
                const bounds = new maplibregl.LngLatBounds();
                if (selectedFeature._groupedSegments) {
                    const allFeatures = map.querySourceFeatures('regulations', { sourceLayer: selectedFeature.sourceLayer, filter });
                    (allFeatures.length ? allFeatures : selectedFeature._groupedSegments).forEach(f => extendBoundsWithGeometry(bounds, f.geometry || f.toJSON?.().geometry));
                } else if (selectedFeature.geometry) {
                    extendBoundsWithGeometry(bounds, selectedFeature.geometry);
                }

                if (!bounds.isEmpty()) {
                    const isMobile = window.innerWidth <= 768;
                    const padding = isMobile ? { top: 80, bottom: window.innerHeight * 0.65, left: 40, right: 40 } : { top: 50, bottom: 50, left: 50, right: 400 };
                    const featureMinZoom = selectedFeature.properties?.['tippecanoe:minzoom'] || 4;
                    const targetZoom = map.cameraForBounds(bounds, { padding })?.zoom;

                    if (targetZoom && targetZoom < featureMinZoom) {
                        map.easeTo({ center: bounds.getCenter(), zoom: featureMinZoom, duration: 1000 });
                    } else {
                        map.fitBounds(bounds, { padding, maxZoom: 11, animate: true, duration: 1000 });
                    }
                }
            }
        } catch (e) { /* Silently handle */ }
    }, [selectedFeature, mobilePanelState]);

    // Effect: Layer Visibility
    useEffect(() => {
        const map = mapRef.current;
        if (!map || !map.isStyleLoaded()) return;
        
        MAP_LAYERS.forEach(layer => {
            const isVisible = layerVisibility[layer] ? 'visible' : 'none';
            if (layer === 'streams' || layer === 'regions') {
                if (map.getLayer(layer)) map.setLayoutProperty(layer, 'visibility', isVisible);
            } else {
                if (map.getLayer(`${layer}-fill`)) map.setLayoutProperty(`${layer}-fill`, 'visibility', isVisible);
                if (map.getLayer(`${layer}-line`)) map.setLayoutProperty(`${layer}-line`, 'visibility', isVisible);
            }
        });
    }, [layerVisibility]);

    // Helper: Update Highlight
    const updateHighlight = useCallback((option: FeatureOption | null) => {
        const map = mapRef.current;
        if (!map || !map.isStyleLoaded()) return;
        const source = map.getSource('highlight-source') as maplibregl.GeoJSONSource;
        if (!source) return;

        if (!option) return source.setData({ type: 'FeatureCollection', features: [] });

        const layerMap: Record<string, string> = { 'streams': 'streams', 'lakes': 'lakes-fill', 'wetlands': 'wetlands-fill', 'manmade': 'manmade-fill' };
        const layerName = layerMap[option.sourceLayer || ''];
        
        let features = [];
        if (layerName) {
            const filter = buildFeatureFilter(option);
            if (filter) features = map.queryRenderedFeatures({ layers: [layerName], filter });
        }
        if (!features.length && option.geometry) features = [{ geometry: option.geometry, properties: option.properties } as any];

        source.setData({ type: 'FeatureCollection', features: features.map(f => ({ type: 'Feature', geometry: f.geometry || f.toJSON?.().geometry, properties: f.properties })) as any });
    }, []);

    // Effect: Highlight Sync on Move/Zoom
    useEffect(() => {
        const map = mapRef.current;
        if (!map) return;
        let timeout: NodeJS.Timeout;
        const handler = () => {
            clearTimeout(timeout);
            timeout = setTimeout(() => highlightedOptionRef.current && updateHighlight(highlightedOptionRef.current), 100);
        };
        map.on('moveend', handler);
        map.on('zoomend', handler);
        return () => { clearTimeout(timeout); map.off('moveend', handler); map.off('zoomend', handler); };
    }, [updateHighlight]);

    return (
        <div className="map-container">
            <div ref={mapContainerRef} className="map-canvas" />
            
            <div className="map-menu-wrapper">
                <button onClick={() => setIsLayerMenuOpen(!isLayerMenuOpen)} className="map-toggle-btn" title="Toggle Layers">
                    {isLayerMenuOpen ? <X size={20} strokeWidth={2.5} /> : <Layers size={20} strokeWidth={2.5} />}
                </button>

                {isLayerMenuOpen && (
                    <div className="map-layer-menu">
                        <div className="map-layer-header">Layers</div>
                        {MAP_LAYERS.map((key) => (
                            <label key={key} className="map-layer-label">
                                <input 
                                    type="checkbox" checked={layerVisibility[key]} 
                                    onChange={() => setLayerVisibility(p => ({ ...p, [key]: !p[key] }))} 
                                    className="map-layer-checkbox" 
                                />
                                {key}
                            </label>
                        ))}
                    </div>
                )}
            </div>

            <InfoPanel feature={selectedFeature} onClose={clearSelection} collapseState={mobilePanelState} onSetCollapseState={setMobilePanelState} />

            {disambigOptions.length > 0 && (
                <DisambiguationMenu
                    options={disambigOptions} position={disambigPosition} highlightedOption={highlightedOption}
                    onSelect={(option) => {
                        clearSelection(); setSelectedFeature(option); setHighlightedOption(null); highlightedOptionRef.current = null;
                        if (window.innerWidth <= 768) setMobilePanelState('partial');
                    }}
                    onHighlight={(option) => {
                        setHighlightedOption(option); highlightedOptionRef.current = option;
                        updateHighlight(option);
                        if (option && cachedBoundsRef.current[option.id] && !cachedBoundsRef.current[option.id].isEmpty()) {
                            const isMobile = window.innerWidth <= 768;
                            const padding = isMobile ? { top: 80, bottom: window.innerHeight * 0.5 + 50, left: 80, right: 80 } : { top: 80, bottom: 80, left: 80, right: 350 };
                            const fZoom = option.minzoom || option.properties?.['tippecanoe:minzoom'] || 4;
                            const tZoom = mapRef.current?.cameraForBounds(cachedBoundsRef.current[option.id], { padding })?.zoom;
                            
                            if (tZoom && tZoom < fZoom) mapRef.current?.easeTo({ center: cachedBoundsRef.current[option.id].getCenter(), zoom: fZoom, duration: 400 });
                            else mapRef.current?.fitBounds(cachedBoundsRef.current[option.id], { padding, maxZoom: 12, duration: 400 });
                        }
                    }}
                    onClose={clearSelection} isCollapsed={mobilePanelState === 'collapsed'}
                    onSetCollapse={(col) => setMobilePanelState(col ? 'collapsed' : 'expanded')}
                />
            )}
        </div>
    );
};

export default Map;