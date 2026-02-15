import { useEffect, useRef, useState } from 'react';
import maplibregl from 'maplibre-gl';
import { Protocol } from 'pmtiles';
import { layers, LIGHT } from '@protomaps/basemaps'; 
import { Layers, X } from 'lucide-react';
import 'maplibre-gl/dist/maplibre-gl.css';
import { createRegulationLayers } from '../map/styles';
import InfoPanel from './InfoPanel';
import DisambiguationMenu from './DisambiguationMenu';

const protocol = new Protocol();
maplibregl.addProtocol('pmtiles', protocol.tile);

interface LayerVisibility {
    streams: boolean;
    lakes: boolean;
    wetlands: boolean;
    manmade: boolean;
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
    _segmentCount?: number;
    minzoom?: number;
}

type CollapseState = 'expanded' | 'partial' | 'collapsed';

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

const Map = () => {
    const mapContainerRef = useRef<HTMLDivElement>(null);
    const mapRef = useRef<maplibregl.Map | null>(null);
    const isDisambiguationOpenRef = useRef<boolean>(false);
    const highlightedOptionRef = useRef<FeatureOption | null>(null);
    const cachedBoundsRef = useRef<Record<string, maplibregl.LngLatBounds>>({});
    
    // STATES
    const [selectedFeature, setSelectedFeature] = useState<FeatureInfo | null>(null);
    const [disambiguationOptions, setDisambiguationOptions] = useState<FeatureOption[]>([]);
    const [disambiguationPosition, setDisambiguationPosition] = useState<{ x: number; y: number } | null>(null);
    const [mobilePanelState, setMobilePanelState] = useState<CollapseState>('expanded');
    const [highlightedOption, setHighlightedOption] = useState<FeatureOption | null>(null);

    // FIXED: Default Open on Desktop (>768px), Closed on Mobile
    const [isLayerMenuOpen, setIsLayerMenuOpen] = useState(() => window.innerWidth > 768);

    const [layerVisibility, setLayerVisibility] = useState<LayerVisibility>({
        streams: true,
        lakes: true,
        wetlands: true,
        manmade: true,
    });

    const clearSelection = () => {
        setSelectedFeature(null);
        setDisambiguationOptions([]);
        setDisambiguationPosition(null);
        isDisambiguationOpenRef.current = false;
        
        // Clear cursor circle and highlight when closing disambiguation menu
        if (mapRef.current) {
            const circleSource = mapRef.current.getSource('cursor-circle') as maplibregl.GeoJSONSource;
            if (circleSource) circleSource.setData({ type: 'FeatureCollection', features: [] });
            
            const highlightSource = mapRef.current.getSource('highlight-source') as maplibregl.GeoJSONSource;
            if (highlightSource) highlightSource.setData({ type: 'FeatureCollection', features: [] });
        }
        setMobilePanelState('expanded'); 
        
        if (mapRef.current) {
            ['selection-highlight-fill', 'selection-highlight-line'].forEach(id => {
                if (mapRef.current!.getLayer(id)) mapRef.current!.removeLayer(id);
            });
        }
    };

    // Pre-calculate bounds for all disambiguation options when they change
    useEffect(() => {
        if (disambiguationOptions.length === 0) {
            cachedBoundsRef.current = {};
            return;
        }

        const newBounds: Record<string, maplibregl.LngLatBounds> = {};

        disambiguationOptions.forEach((option) => {
            const bounds = new maplibregl.LngLatBounds();

            const processCoords = (coords: any) => {
                if (Array.isArray(coords) && typeof coords[0] === 'number') {
                    bounds.extend(coords as [number, number]);
                } else if (Array.isArray(coords)) {
                    coords.forEach(processCoords);
                }
            };

            const processGeometry = (geometry: any) => {
                if (!geometry || !geometry.coordinates) return;

                if (geometry.type === 'Point') {
                    bounds.extend(geometry.coordinates);
                } else if (geometry.type === 'LineString') {
                    geometry.coordinates.forEach((coord: any) => bounds.extend(coord));
                } else if (geometry.type === 'MultiLineString' || geometry.type === 'Polygon') {
                    processCoords(geometry.coordinates);
                } else if (geometry.type === 'MultiPolygon') {
                    processCoords(geometry.coordinates);
                }
            };

            // Process the main geometry
            if ((option as any).geometry) {
                processGeometry((option as any).geometry);
            }

            // Process grouped segments if available
            if ((option as any)._groupedSegments && (option as any)._groupedSegments.length > 0) {
                (option as any)._groupedSegments.forEach((segment: any) => {
                    if (segment.geometry) {
                        processGeometry(segment.geometry);
                    }
                });
            }

            if (!bounds.isEmpty()) {
                newBounds[option.id] = bounds;
            }
        });

        cachedBoundsRef.current = newBounds;
    }, [disambiguationOptions]);

    useEffect(() => {
        if (!mapContainerRef.current) return;

        // Clear any stored attribution state before creating the map
        for (let i = 0; i < localStorage.length; i++) {
            const key = localStorage.key(i);
            if (key && key.includes('maplibregl-attrib')) {
                localStorage.removeItem(key);
            }
        }

        const canvas = document.createElement('canvas');
        const size = 16;
        canvas.width = size;
        canvas.height = size;
        const ctx = canvas.getContext('2d');
        let wetlandPatternData: ImageData | null = null;
        if (ctx) {
            ctx.strokeStyle = '#000000';
            ctx.lineWidth = 1;
            for (let i = -size; i < size * 2; i += 4) {
                ctx.beginPath();
                ctx.moveTo(i, 0);
                ctx.lineTo(i + size, size);
                ctx.stroke();
            }
            wetlandPatternData = ctx.getImageData(0, 0, size, size);
        }

        const map = new maplibregl.Map({
            container: mapContainerRef.current,
            style: {
                version: 8,
                glyphs: 'https://protomaps.github.io/basemaps-assets/fonts/{fontstack}/{range}.pbf',
                sources: {
                    protomaps: {
                        type: 'vector',
                        url: 'pmtiles:///data/bc.pmtiles',
                        attribution: '<a href="https://protomaps.com">Protomaps</a> © <a href="https://openstreetmap.org">OpenStreetMap</a>',
                        maxzoom: 15
                    },
                    regulations: {
                        type: 'vector',
                        url: 'pmtiles:///data/regulations_merged.pmtiles',
                        attribution: 'FWA BC, Province of British Columbia',
                        minzoom: 4,
                        maxzoom: 12
                    }
                },
                layers: [
                    ...layers('protomaps', LIGHT),
                    ...createRegulationLayers()
                ]
            },
            center: [-123.0, 49.25],
            zoom: 8,
            maxZoom: 12,
            minZoom: 4,
            hash: true,
            attributionControl: { compact: true }
        });

        if (wetlandPatternData) map.addImage('wetland-pattern', wetlandPatternData);
        
        map.addControl(new maplibregl.NavigationControl(), 'top-right');
        map.addControl(new maplibregl.ScaleControl(), 'bottom-left');
        
        // Watch for attribution control and force it to start collapsed
        const observer = new MutationObserver(() => {
            const attribElement = document.querySelector('.maplibregl-ctrl-attrib');
            if (attribElement && attribElement.classList.contains('maplibregl-compact-show')) {
                // Only remove on initial load, not when user actively clicks
                if (!map.loaded()) {
                    attribElement.classList.remove('maplibregl-compact-show');
                }
            }
        });
        
        // Start observing
        const attribContainer = document.querySelector('.maplibregl-ctrl-bottom-right');
        if (attribContainer) {
            observer.observe(attribContainer, { 
                attributes: true, 
                attributeFilter: ['class'],
                subtree: true 
            });
        }
        
        // Stop observing after map loads
        map.once('load', () => {
            observer.disconnect();
        });

        map.on('load', () => {
            // Highlight source for disambiguation menu hover
            map.addSource('highlight-source', { type: 'geojson', data: { type: 'FeatureCollection', features: [] }, tolerance: 0.375 });
            map.addLayer({ 
                id: 'highlight-line', 
                type: 'line', 
                source: 'highlight-source', 
                paint: { 
                    'line-color': '#FFD700', 
                    'line-width': [
                        'interpolate',
                        ['linear'],
                        ['zoom'],
                        4, 2,
                        8, 4,
                        12, 5
                    ],
                    'line-opacity': 1,
                    'line-blur': [
                        'interpolate',
                        ['linear'],
                        ['zoom'],
                        4, 1,
                        8, 0.5,
                        12, 0
                    ]
                } 
            });
            map.addLayer({ 
                id: 'highlight-fill', 
                type: 'fill', 
                source: 'highlight-source', 
                paint: { 
                    'fill-color': '#FFD700', 
                    'fill-opacity': 0.3 
                }, 
                filter: ['==', '$type', 'Polygon'] 
            });
            
            // Cursor circle to show disambiguation radius
            map.addSource('cursor-circle', { 
                type: 'geojson', 
                data: { type: 'FeatureCollection', features: [] } 
            });
            map.addLayer({
                id: 'cursor-circle-fill',
                type: 'fill',
                source: 'cursor-circle',
                paint: {
                    'fill-color': '#3b82f6',
                    'fill-opacity': 0.1
                }
            });
            map.addLayer({
                id: 'cursor-circle-line',
                type: 'line',
                source: 'cursor-circle',
                paint: {
                    'line-color': '#3b82f6',
                    'line-width': 1.5,
                    'line-opacity': 0.6
                }
            });
        });

        map.on('movestart', (e) => {
            if (e.originalEvent) setMobilePanelState('collapsed');
        });

        const interactableLayers = [
            'streams',
            'lakes-fill',
            'wetlands-fill',
            'manmade-fill'
        ];

        map.on('mousemove', (e) => {
            // Don't query features until map is fully loaded
            if (!map.isStyleLoaded()) return;
            
            const buffer = 10;
            const bbox: [maplibregl.PointLike, maplibregl.PointLike] = [
                [e.point.x - buffer, e.point.y - buffer],
                [e.point.x + buffer, e.point.y + buffer]
            ];
            const features = map.queryRenderedFeatures(bbox, { layers: interactableLayers });
            map.getCanvas().style.cursor = features.length > 0 ? 'pointer' : '';

            // Show cursor circle (15px radius = disambiguation buffer)
            // Only update if disambiguation menu is not open
            if (isDisambiguationOpenRef.current) return;
            
            const circleSource = map.getSource('cursor-circle') as maplibregl.GeoJSONSource;
            if (circleSource) {
                const radiusInMeters = 15 * (40075016.686 * Math.abs(Math.cos(e.lngLat.lat * Math.PI / 180)) / (256 * Math.pow(2, map.getZoom())));
                const circle = {
                    type: 'Feature' as const,
                    geometry: {
                        type: 'Point' as const,
                        coordinates: [e.lngLat.lng, e.lngLat.lat]
                    },
                    properties: {}
                };
                
                // Create circle using turf-like logic
                const steps = 64;
                const coords: number[][] = [];
                for (let i = 0; i < steps; i++) {
                    const angle = (i / steps) * 2 * Math.PI;
                    const dx = radiusInMeters * Math.cos(angle) / (111320 * Math.cos(e.lngLat.lat * Math.PI / 180));
                    const dy = radiusInMeters * Math.sin(angle) / 110540;
                    coords.push([e.lngLat.lng + dx, e.lngLat.lat + dy]);
                }
                coords.push(coords[0]); // Close the polygon
                
                circleSource.setData({
                    type: 'FeatureCollection',
                    features: [{
                        type: 'Feature',
                        geometry: {
                            type: 'Polygon',
                            coordinates: [coords]
                        },
                        properties: {}
                    }]
                });
            }
        });

        map.on('mouseleave', () => {
            map.getCanvas().style.cursor = '';
            // Only clear cursor circle if disambiguation menu is not open
            if (!isDisambiguationOpenRef.current) {
                const circleSource = map.getSource('cursor-circle') as maplibregl.GeoJSONSource;
                if (circleSource) circleSource.setData({ type: 'FeatureCollection', features: [] });
            }
        });
        
        // Helper function to create cursor circle at a specific position
        const setCursorCircleAt = (lngLat: { lng: number; lat: number }) => {
            const circleSource = map.getSource('cursor-circle') as maplibregl.GeoJSONSource;
            if (!circleSource) return;
            
            const radiusInMeters = 15 * (40075016.686 * Math.abs(Math.cos(lngLat.lat * Math.PI / 180)) / (256 * Math.pow(2, map.getZoom())));
            
            // Create circle using turf-like logic
            const steps = 64;
            const coords: number[][] = [];
            for (let i = 0; i < steps; i++) {
                const angle = (i / steps) * 2 * Math.PI;
                const dx = radiusInMeters * Math.cos(angle) / (111320 * Math.cos(lngLat.lat * Math.PI / 180));
                const dy = radiusInMeters * Math.sin(angle) / 110540;
                coords.push([lngLat.lng + dx, lngLat.lat + dy]);
            }
            coords.push(coords[0]); // Close the polygon
            
            circleSource.setData({
                type: 'FeatureCollection',
                features: [{
                    type: 'Feature',
                    geometry: {
                        type: 'Polygon',
                        coordinates: [coords]
                    },
                    properties: {}
                }]
            });
        };

        // Shared handler for both click and touch events
        const handleFeatureSelection = (point: maplibregl.Point, lngLat: maplibregl.LngLat) => {
            // Don't query features until map is fully loaded
            if (!map.isStyleLoaded()) return;
            
            const buffer = 15;
            const bbox: [maplibregl.PointLike, maplibregl.PointLike] = [
                [point.x - buffer, point.y - buffer],
                [point.x + buffer, point.y + buffer]
            ];

            const features = map.queryRenderedFeatures(bbox, { layers: interactableLayers });

            if (features.length === 0) {
                clearSelection();
                return;
            }

            const getFeatureType = (layerId: string): 'stream' | 'lake' | 'wetland' | 'manmade' => {
                if (layerId.includes('streams')) return 'stream';
                if (layerId.includes('lakes')) return 'lake';
                if (layerId.includes('wetlands')) return 'wetland';
                return 'manmade';
            };

            // Helper to create a regulation signature for grouping
            const getRegulationKey = (props: any): string => {
                // Prefer regulation_ids field if available (most reliable)
                if (props?.regulation_ids) {
                    return props.regulation_ids;
                }
                
                // Fallback: create signature from individual properties
                try {
                    return JSON.stringify({
                        limit: props?.species_limit || null,
                        season: props?.season_dates || null,
                        gear: props?.gear_restriction || null,
                        stocked: props?.is_stocked || null,
                        classified: props?.is_classified_water || null
                    });
                } catch (e) {
                    return 'default';
                }
            };

            try {
                const options: FeatureOption[] = features.map((feature, index) => {
                    const plainGeometry = feature.toJSON().geometry; 
                    
                    const props = feature.properties || {};
                    let idKey = 'group_id';  // Default to group_id for merged features
                    let idVal = props.group_id;

                    // Fallback to individual IDs if not a merged group
                    if (!idVal) {
                        if (props.linear_feature_id) {
                            idKey = 'linear_feature_id';
                            idVal = props.linear_feature_id;
                        } else if (props.waterbody_key) {
                            idKey = 'waterbody_key';
                            idVal = props.waterbody_key;
                        } else {
                            idKey = 'id';
                            idVal = feature.id;
                        }
                    }

                    return {
                        type: getFeatureType(feature.layer.id),
                        properties: props,
                        id: (idVal || `feature-${index}`).toString(),
                        geometry: plainGeometry,
                        source: feature.layer.source,
                        sourceLayer: feature.layer['source-layer'],
                        idKey: idKey,
                        minzoom: props['tippecanoe:minzoom']
                    };
                });

                // First, deduplicate tile-split segments by group_id
                const deduplicatedByGroupId = options.filter((option, index, self) => 
                    index === self.findIndex(o => o.id === option.id && o.idKey === option.idKey)
                );

                // For features with names, expand to include ALL matching features from the entire map
                const expandedOptions: FeatureOption[] = [];
                const processedKeys = new Set<string>();
                
                deduplicatedByGroupId.forEach(option => {
                    const gnisName = option.properties?.gnis_name || option.properties?.lake_name;
                    const regulationIds = option.properties?.regulation_ids;
                    
                    if (gnisName && regulationIds) {
                        // Create a composite key to avoid processing same feature type multiple times
                        const compositeKey = `${gnisName}|${regulationIds}`;
                        
                        if (!processedKeys.has(compositeKey)) {
                            processedKeys.add(compositeKey);
                            
                            // Query ALL features on the map with this name + regulation combo
                            // Note: All features use gnis_name property regardless of type
                            const allMatchingFeatures = map.querySourceFeatures('regulations', {
                                sourceLayer: option.sourceLayer,
                                filter: [
                                    'all',
                                    ['==', ['get', 'gnis_name'], gnisName],
                                    ['==', ['get', 'regulation_ids'], regulationIds]
                                ]
                            });
                            
                            // Convert to FeatureOptions and deduplicate by group_id/waterbody_key
                            const matchingOptions: Record<string, FeatureOption> = {};
                            allMatchingFeatures.forEach(feature => {
                                const props = feature.properties || {};
                                const groupId = props.group_id || props.waterbody_key;
                                const idKey = props.group_id ? 'group_id' : 'waterbody_key';
                                
                                if (groupId && !matchingOptions[groupId]) {
                                    matchingOptions[groupId] = {
                                        type: option.type,
                                        properties: props,
                                        id: groupId.toString(),
                                        geometry: feature.toJSON().geometry,
                                        source: option.source,
                                        sourceLayer: option.sourceLayer,
                                        idKey: idKey,
                                        minzoom: props['tippecanoe:minzoom']
                                    };
                                }
                            });
                            
                            // Add all matching features as a single grouped option
                            const allSegments = Object.values(matchingOptions);
                            if (allSegments.length > 0) {
                                const representative = allSegments[0];
                                expandedOptions.push({
                                    ...representative,
                                    _groupedSegments: allSegments.length > 1 ? allSegments : undefined,
                                    _segmentCount: allSegments.length > 1 ? allSegments.length : undefined
                                });
                            }
                        }
                    } else {
                        // No GNIS name - just add as-is
                        expandedOptions.push(option);
                    }
                });

                // Sort the final options
                const uniqueOptions = expandedOptions;

                // --- SORTING LOGIC ---
                uniqueOptions.sort((a, b) => {
                    const isManmadeA = a.type === 'manmade';
                    const isManmadeB = b.type === 'manmade';
                    if (isManmadeA && !isManmadeB) return 1; 
                    if (!isManmadeA && isManmadeB) return -1;

                    const isLakeA = a.type === 'lake';
                    const isLakeB = b.type === 'lake';
                    if (isLakeA && !isLakeB) return -1;
                    if (!isLakeA && isLakeB) return 1;

                    const orderA = a.properties.stream_order !== undefined ? a.properties.stream_order : -1;
                    const orderB = b.properties.stream_order !== undefined ? b.properties.stream_order : -1;
                    if (orderA !== orderB) return orderB - orderA;

                    const nameA = a.properties.gnis_name || a.properties.lake_name || a.properties.name;
                    const nameB = b.properties.gnis_name || b.properties.lake_name || b.properties.name;
                    const hasNameA = !!nameA;
                    const hasNameB = !!nameB;
                    if (hasNameA && !hasNameB) return -1;
                    if (!hasNameA && hasNameB) return 1;

                    const sizeA = calculateGeometrySize(a.geometry);
                    const sizeB = calculateGeometrySize(b.geometry);
                    return sizeB - sizeA;
                });

                clearSelection();

                if (uniqueOptions.length === 1) {
                    setSelectedFeature(uniqueOptions[0]);
                } else if (uniqueOptions.length > 1) {
                    // Lock cursor circle at click position
                    setCursorCircleAt(lngLat);
                    setDisambiguationOptions(uniqueOptions);
                    setDisambiguationPosition({ x: point.x, y: point.y });
                    isDisambiguationOpenRef.current = true;
                }
            } catch (error) {
                // Fallback to clearing selection on error
                clearSelection();
            }
        };

        // Click event handler
        map.on('click', (e) => {
            handleFeatureSelection(e.point, e.lngLat);
        });

        // Touch event handler for mobile
        let touchStartPos: { x: number; y: number } | null = null;
        let touchStartTime = 0;
        
        const mapCanvas = map.getCanvas();
        
        const handleTouchStart = (e: TouchEvent) => {
            if (e.touches.length === 1) {
                const touch = e.touches[0];
                touchStartPos = { x: touch.clientX, y: touch.clientY };
                touchStartTime = Date.now();
            } else {
                touchStartPos = null;
            }
        };
        
        const handleTouchEnd = (e: TouchEvent) => {
            if (!touchStartPos || e.touches.length > 0) {
                touchStartPos = null;
                return;
            }
            
            const touch = e.changedTouches[0];
            const touchEndPos = { x: touch.clientX, y: touch.clientY };
            const timeDiff = Date.now() - touchStartTime;
            
            // Calculate distance moved
            const dx = touchEndPos.x - touchStartPos.x;
            const dy = touchEndPos.y - touchStartPos.y;
            const distance = Math.sqrt(dx * dx + dy * dy);
            
            // Only trigger if it was a quick tap with minimal movement
            if (distance < 10 && timeDiff < 300) {
                const rect = mapCanvas.getBoundingClientRect();
                const point = new maplibregl.Point(
                    touchEndPos.x - rect.left,
                    touchEndPos.y - rect.top
                );
                const lngLat = map.unproject(point);
                handleFeatureSelection(point, lngLat);
            }
            
            touchStartPos = null;
        };
        
        mapCanvas.addEventListener('touchstart', handleTouchStart);
        mapCanvas.addEventListener('touchend', handleTouchEnd);

        mapRef.current = map;
        return () => {
            mapCanvas.removeEventListener('touchstart', handleTouchStart);
            mapCanvas.removeEventListener('touchend', handleTouchEnd);
            map.remove();
            mapRef.current = null;
        };
    }, []);

    // --- SELECTION HIGHLIGHT & ZOOM ---
    useEffect(() => {
        if (!mapRef.current) return;
        const map = mapRef.current;

        ['selection-highlight-fill', 'selection-highlight-line'].forEach(id => {
            if (map.getLayer(id)) map.removeLayer(id);
        });

        if (selectedFeature && selectedFeature.source && selectedFeature.sourceLayer) {
            try {
                const isPolygon = selectedFeature.type === 'lake' || selectedFeature.type === 'wetland' || selectedFeature.type === 'manmade';
            
            // Use group_id if available (for merged features), otherwise use individual ID
            let filter;
            
            // Handle grouped segments (multiple braids with same name + regulations)
            // Query for ALL features with matching name + regulation_ids
            if (selectedFeature._groupedSegments && selectedFeature._groupedSegments.length > 0) {
                const gnisName = selectedFeature.properties?.gnis_name || selectedFeature.properties?.lake_name;
                const regulationIds = selectedFeature.properties?.regulation_ids;
                
                if (gnisName && regulationIds) {
                    // Match all features with same name AND same regulation_ids
                    filter = [
                        'all',
                        ['==', ['get', 'gnis_name'], gnisName],
                        ['==', ['get', 'regulation_ids'], regulationIds]
                    ];
                } else if (gnisName) {
                    // Fallback: just match by name if no regulation_ids
                    filter = ['==', ['get', 'gnis_name'], gnisName];
                } else {
                    // Last resort: use the IDs we collected (group_id or waterbody_key)
                    const ids: any[] = [];
                    const idKey = selectedFeature._groupedSegments[0]?.idKey || 'group_id';
                    
                    selectedFeature._groupedSegments.forEach(segment => {
                        const id = segment.properties?.group_id || segment.properties?.waterbody_key;
                        if (id) {
                            ids.push(id);
                        }
                    });
                    
                    if (ids.length > 0) {
                        filter = ['any', ...ids.map(id => ['==', ['get', idKey], id])];
                    } else {
                        return;
                    }
                }
            }
            // Single feature with group_id or waterbody_key
            else if (selectedFeature.properties?.group_id) {
                filter = ['==', ['get', 'group_id'], selectedFeature.properties.group_id];
            }
            else if (selectedFeature.properties?.waterbody_key) {
                filter = ['==', ['get', 'waterbody_key'], selectedFeature.properties.waterbody_key];
            }
            // Individual ID
            else if (selectedFeature.idKey) {
                const idVal = selectedFeature.id.toString();
                const numId = parseInt(idVal);
                filter = [
                    'any',
                    ['==', ['get', selectedFeature.idKey], idVal],
                    ['==', ['get', selectedFeature.idKey], isNaN(numId) ? -1 : numId]
                ];
            } else {
                return;
            }

            // For streams, match line width to stream_order like base layer
            // For polygons, use thicker outline
            const lineWidth = selectedFeature.type === 'stream' ? [
                'interpolate',
                ['linear'],
                ['zoom'],
                // Match the base streams layer formula with added thickness for visibility
                4, ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.1]],
                8, ['+', 2, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.15]],
                11, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 1.5],
                12, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 2],
                14, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 3],
                16, ['*', ['+', 1.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 4]
            ] : [
                'interpolate',
                ['linear'],
                ['zoom'],
                4, 3,
                8, 4,
                10, 5,
                12, 6
            ];
            
            // Add line layer first (below fill)
            map.addLayer({
                id: 'selection-highlight-line',
                type: 'line',
                source: selectedFeature.source,
                'source-layer': selectedFeature.sourceLayer,
                paint: { 
                    'line-color': '#FF0000', 
                    'line-width': lineWidth, 
                    'line-opacity': 0.9 
                },
                filter: filter
            });
            
            // Add fill layer on top for polygons
            if (isPolygon) {
                map.addLayer({
                    id: 'selection-highlight-fill',
                    type: 'fill',
                    source: selectedFeature.source,
                    'source-layer': selectedFeature.sourceLayer,
                    paint: { 
                        'fill-color': '#FF0000', 
                        'fill-opacity': 0.4 
                    },
                    filter: filter
                });
            }

            if (mobilePanelState !== 'collapsed') {
                const bounds = new maplibregl.LngLatBounds();
                const extend = (coord: any) => {
                    if (Array.isArray(coord) && typeof coord[0] === 'number') {
                        bounds.extend(coord as [number, number]);
                    } else if (Array.isArray(coord)) {
                        coord.forEach(extend);
                    }
                };
                
                // For grouped segments with name+regulation matching, query all matching features
                if (selectedFeature._groupedSegments && selectedFeature._groupedSegments.length > 0) {
                    const gnisName = selectedFeature.properties?.gnis_name || selectedFeature.properties?.lake_name;
                    const regulationIds = selectedFeature.properties?.regulation_ids;
                    
                    if (gnisName && regulationIds && selectedFeature.sourceLayer) {
                        // Query ALL features matching the same criteria
                        const allMatchingFeatures = map.querySourceFeatures('regulations', {
                            sourceLayer: selectedFeature.sourceLayer,
                            filter: [
                                'all',
                                ['==', ['get', 'gnis_name'], gnisName],
                                ['==', ['get', 'regulation_ids'], regulationIds]
                            ]
                        });
                        
                        allMatchingFeatures.forEach(feature => {
                            const geom = feature.toJSON().geometry;
                            if (geom?.coordinates) {
                                extend(geom.coordinates);
                            }
                        });
                    } else {
                        // Fallback: use clicked segments
                        selectedFeature._groupedSegments.forEach(segment => {
                            if (segment.geometry?.coordinates) {
                                extend(segment.geometry.coordinates);
                            }
                        });
                    }
                }
                // Single feature
                else if (selectedFeature.geometry) {
                    extend(selectedFeature.geometry.coordinates);
                }

                const isMobile = window.innerWidth <= 768;
                const padding = isMobile ? { 
                    top: 80, 
                    bottom: window.innerHeight * 0.65,
                    left: 40, 
                    right: 40 
                } : { 
                    top: 50, 
                    bottom: 50, 
                    left: 50, 
                    right: 400 
                };

                if (!bounds.isEmpty()) {
                    // Get the feature's minimum zoom level from tippecanoe metadata
                    const featureMinZoom = selectedFeature.properties?.['tippecanoe:minzoom'] || 4;
                    
                    // Calculate what zoom fitBounds would use
                    const camera = map.cameraForBounds(bounds, { padding });
                    const targetZoom = camera?.zoom;
                    
                    // If the feature would require zooming out below its minzoom (where it's not visible),
                    // pan to center at its minzoom instead of fitting the full bounds
                    if (targetZoom && targetZoom < featureMinZoom) {
                        const center = bounds.getCenter();
                        map.easeTo({
                            center: center,
                            zoom: featureMinZoom,
                            duration: 1000
                        });
                    } else {
                        map.fitBounds(bounds, {
                            padding,
                            maxZoom: 11,
                            animate: true,
                            duration: 1000
                        });
                    }
                }
            }
            } catch (error) {
                // Silently handle errors
            }
        }
    }, [selectedFeature, mobilePanelState]);

    // --- LAYER VISIBILITY ---
    useEffect(() => {
        if (!mapRef.current || !mapRef.current.isStyleLoaded()) return;
        const map = mapRef.current;
        
        // Streams
        if (map.getLayer('streams')) {
            map.setLayoutProperty('streams', 'visibility', layerVisibility.streams ? 'visible' : 'none');
        }
        
        // Lakes
        if (map.getLayer('lakes-fill')) {
            map.setLayoutProperty('lakes-fill', 'visibility', layerVisibility.lakes ? 'visible' : 'none');
        }
        if (map.getLayer('lakes-line')) {
            map.setLayoutProperty('lakes-line', 'visibility', layerVisibility.lakes ? 'visible' : 'none');
        }
        
        // Wetlands
        if (map.getLayer('wetlands-fill')) {
            map.setLayoutProperty('wetlands-fill', 'visibility', layerVisibility.wetlands ? 'visible' : 'none');
        }
        if (map.getLayer('wetlands-line')) {
            map.setLayoutProperty('wetlands-line', 'visibility', layerVisibility.wetlands ? 'visible' : 'none');
        }
        
        // Manmade
        if (map.getLayer('manmade-fill')) {
            map.setLayoutProperty('manmade-fill', 'visibility', layerVisibility.manmade ? 'visible' : 'none');
        }
        if (map.getLayer('manmade-line')) {
            map.setLayoutProperty('manmade-line', 'visibility', layerVisibility.manmade ? 'visible' : 'none');
        }
    }, [layerVisibility]);

    const toggleLayer = (layer: keyof LayerVisibility) => {
        setLayerVisibility(prev => ({ ...prev, [layer]: !prev[layer] }));
    };

    // Helper function to update highlight based on current map state
    const updateHighlight = (option: FeatureOption | null) => {
        if (!mapRef.current || !mapRef.current.isStyleLoaded()) return;
        const map = mapRef.current;
        const source = map.getSource('highlight-source') as maplibregl.GeoJSONSource;
        
        if (!source) return;
        
        if (option === null) {
            source.setData({ type: 'FeatureCollection', features: [] });
            return;
        }
        
        // Map source-layer to actual layer name for queryRenderedFeatures
        const getLayerName = (sourceLayer: string | undefined): string | undefined => {
            if (!sourceLayer) return undefined;
            if (sourceLayer === 'streams') return 'streams';
            if (sourceLayer === 'lakes') return 'lakes-fill';
            if (sourceLayer === 'wetlands') return 'wetlands-fill';
            if (sourceLayer === 'manmade') return 'manmade-fill';
            return undefined;
        };
        
        let featuresToHighlight: any[] = [];
        
        // For grouped segments, query currently visible features with same name + regulations
        if (option._groupedSegments && option._groupedSegments.length > 0) {
            const gnisName = option.properties?.gnis_name || option.properties?.lake_name;
            const regulationIds = option.properties?.regulation_ids;
            const layerName = getLayerName(option.sourceLayer);
            
            if (gnisName && regulationIds && layerName) {
                // Query rendered features (respects current zoom level/LOD)
                const allMatchingFeatures = map.queryRenderedFeatures({
                    layers: [layerName],
                    filter: [
                        'all',
                        ['==', ['get', 'gnis_name'], gnisName],
                        ['==', ['get', 'regulation_ids'], regulationIds]
                    ]
                });
                featuresToHighlight = allMatchingFeatures;
            } else if (gnisName && layerName) {
                const allMatchingFeatures = map.queryRenderedFeatures({
                    layers: [layerName],
                    filter: ['==', ['get', 'gnis_name'], gnisName]
                });
                featuresToHighlight = allMatchingFeatures;
            }
        }
        // Single feature with group_id or waterbody_key
        else if ((option.properties?.group_id || option.properties?.waterbody_key) && option.sourceLayer) {
            const id = option.properties.group_id || option.properties.waterbody_key;
            const idKey = option.properties.group_id ? 'group_id' : 'waterbody_key';
            const layerName = getLayerName(option.sourceLayer);
            
            if (layerName) {
                const allGroupFeatures = map.queryRenderedFeatures({
                    layers: [layerName],
                    filter: ['==', ['get', idKey], id]
                });
                featuresToHighlight = allGroupFeatures;
            }
        }
        // Fallback: use geometry from option
        else if (option.geometry) {
            featuresToHighlight = [{
                type: 'Feature',
                geometry: option.geometry,
                properties: option.properties
            }];
        }
        
        const highlightGeojson = featuresToHighlight.map(f => ({
            type: 'Feature',
            geometry: f.geometry || f.toJSON?.().geometry,
            properties: f.properties
        }));
        
        source.setData({ type: 'FeatureCollection', features: highlightGeojson as any });
    };

    // Update highlight when map moves/zooms (if something is highlighted)
    useEffect(() => {
        if (!mapRef.current) return;
        const map = mapRef.current;
        
        let updateTimeout: NodeJS.Timeout;
        const handleMapUpdate = () => {
            // Debounce to avoid excessive updates
            clearTimeout(updateTimeout);
            updateTimeout = setTimeout(() => {
                if (highlightedOptionRef.current) {
                    updateHighlight(highlightedOptionRef.current);
                }
            }, 100);
        };
        
        map.on('moveend', handleMapUpdate);
        map.on('zoomend', handleMapUpdate);
        
        return () => {
            clearTimeout(updateTimeout);
            map.off('moveend', handleMapUpdate);
            map.off('zoomend', handleMapUpdate);
        };
    }, []);

    return (
        <div style={{ position: 'relative', width: '100%', height: '100%', fontFamily: 'sans-serif' }}>
            <div ref={mapContainerRef} style={{ width: '100%', height: '100%' }} />
            
            <div style={{
                position: 'absolute',
                top: '12px',
                left: '12px',
                zIndex: 1,
                display: 'flex',
                flexDirection: 'column',
                gap: '8px'
            }}>
                <button
                    onClick={() => setIsLayerMenuOpen(!isLayerMenuOpen)}
                    style={{
                        width: '40px',
                        height: '40px',
                        backgroundColor: 'white',
                        border: '1px solid black',
                        boxShadow: '4px 4px 0 rgba(0,0,0,1)',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        cursor: 'pointer',
                        padding: 0
                    }}
                    title="Toggle Layers"
                >
                    {isLayerMenuOpen ? <X size={20} strokeWidth={2.5} /> : <Layers size={20} strokeWidth={2.5} />}
                </button>

                {isLayerMenuOpen && (
                    <div style={{
                        backgroundColor: 'white',
                        border: '1px solid black',
                        boxShadow: '4px 4px 0 rgba(0,0,0,1)',
                        padding: '16px',
                        minWidth: '160px'
                    }}>
                        <div style={{ fontWeight: '800', marginBottom: '12px', fontSize: '11px', textTransform: 'uppercase', borderBottom: '2px solid #eee', paddingBottom: '8px', letterSpacing: '0.1em' }}>
                            Layers
                        </div>
                        {['streams', 'lakes', 'wetlands', 'manmade'].map((key) => (
                            <label key={key} style={{ display: 'flex', alignItems: 'center', marginBottom: '8px', cursor: 'pointer', fontSize: '12px', fontWeight: '500', textTransform: 'uppercase' }}>
                                <input 
                                    type="checkbox" 
                                    checked={layerVisibility[key as keyof LayerVisibility]} 
                                    onChange={() => toggleLayer(key as keyof LayerVisibility)} 
                                    style={{ marginRight: '8px', cursor: 'pointer', accentColor: 'black' }} 
                                />
                                {key}
                            </label>
                        ))}
                    </div>
                )}
            </div>

            <InfoPanel 
                feature={selectedFeature} 
                onClose={() => clearSelection()}
                collapseState={mobilePanelState}
                onSetCollapseState={setMobilePanelState}
            />

            {disambiguationOptions.length > 0 && (
                <DisambiguationMenu
                    options={disambiguationOptions}
                    position={disambiguationPosition}
                    highlightedOption={highlightedOption}
                    onSelect={(option) => {
                        clearSelection();
                        setSelectedFeature(option);
                        setHighlightedOption(null);
                        highlightedOptionRef.current = null;
                        // On mobile, set panel to partial collapse so user can see the focused feature
                        if (window.innerWidth <= 768) {
                            setMobilePanelState('partial');
                        }
                    }}
                    onHighlight={(option) => {
                        setHighlightedOption(option);
                        highlightedOptionRef.current = option;
                        
                        if (!mapRef.current) return;
                        const map = mapRef.current;
                        
                        // Update highlight using helper
                        updateHighlight(option);
                        
                        // Fit bounds if option is not null and we have cached bounds
                        if (option) {
                            const bounds = cachedBoundsRef.current[option.id];
                            if (bounds && !bounds.isEmpty()) {
                                const isMobile = window.innerWidth <= 768;
                                // On mobile, menu takes up to 50vh, so add padding to keep feature visible above it
                                const mobileBottomPadding = isMobile ? window.innerHeight * 0.5 + 50 : 80;
                                
                                const padding = isMobile 
                                    ? { top: 80, bottom: mobileBottomPadding, left: 80, right: 80 }
                                    : { top: 80, bottom: 80, left: 80, right: 350 };
                                
                                // Get the feature's minimum zoom level from tippecanoe metadata
                                const featureMinZoom = option.minzoom || option.properties?.['tippecanoe:minzoom'] || 4;
                                
                                // Calculate what zoom fitBounds would use
                                const camera = map.cameraForBounds(bounds, { padding });
                                const targetZoom = camera?.zoom;
                                
                                // If the feature would require zooming out below its minzoom (where it's not visible),
                                // pan to center at its minzoom instead of fitting the full bounds
                                if (targetZoom && targetZoom < featureMinZoom) {
                                    const center = bounds.getCenter();
                                    map.easeTo({
                                        center: center,
                                        zoom: featureMinZoom,
                                        duration: 400
                                    });
                                } else {
                                    map.fitBounds(bounds, {
                                        padding,
                                        maxZoom: 12,
                                        duration: 400
                                    });
                                }
                            }
                        }
                    }}
                    onClose={() => {
                        clearSelection();
                        setHighlightedOption(null);
                        highlightedOptionRef.current = null;
                    }}
                    isCollapsed={mobilePanelState === 'collapsed'}
                    onSetCollapse={(collapsed) => setMobilePanelState(collapsed ? 'collapsed' : 'expanded')}
                />
            )}
        </div>
    );
};

export default Map;