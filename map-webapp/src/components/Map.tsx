import { useEffect, useRef, useState, useCallback } from 'react';
import maplibregl from 'maplibre-gl';
import { Protocol } from 'pmtiles';
import { layers, LIGHT } from '@protomaps/basemaps'; 
import 'maplibre-gl/dist/maplibre-gl.css';
import { createZoneLayers } from '../map/styles';
import InfoPanel from './InfoPanel';
import DisambiguationMenu from './DisambiguationMenu';

const protocol = new Protocol();
maplibregl.addProtocol('pmtiles', protocol.tile);

interface LayerVisibility {
    zones: boolean;
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
    // We keep these for re-querying logic
    source?: string;
    sourceLayer?: string;
    idKey?: string; 
}

interface FeatureOption extends FeatureInfo {
    id: string;
}

// Helper for sorting
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
    
    // STATES
    const [selectedFeature, setSelectedFeature] = useState<FeatureInfo | null>(null);
    const [disambiguationOptions, setDisambiguationOptions] = useState<FeatureOption[]>([]);
    const [disambiguationPosition, setDisambiguationPosition] = useState<{ x: number; y: number } | null>(null);
    const [isMobilePanelCollapsed, setIsMobilePanelCollapsed] = useState(false);

    // Track selected ID in a ref so the moveend listener can access it without closure stale state
    const selectedFeatureRef = useRef<FeatureInfo | null>(null);

    const [layerVisibility, setLayerVisibility] = useState<LayerVisibility>({
        zones: true,
        streams: true,
        lakes: true,
        wetlands: true,
        manmade: true,
    });

    const clearSelection = () => {
        setSelectedFeature(null);
        selectedFeatureRef.current = null; // Sync ref
        setDisambiguationOptions([]);
        setDisambiguationPosition(null);
        setIsMobilePanelCollapsed(false); 
        
        if (mapRef.current) {
            const source = mapRef.current.getSource('selected-source') as maplibregl.GeoJSONSource;
            if (source) source.setData({ type: 'FeatureCollection', features: [] });
        }
    };

    useEffect(() => {
        if (!mapContainerRef.current) return;

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
                    waterbodies: {
                        type: 'vector',
                        url: 'pmtiles:///data/waterbodies_bc.pmtiles',
                        attribution: 'FWA BC, Province of British Columbia',
                        minzoom: 4,
                        maxzoom: 14
                    }
                },
                layers: [
                    ...layers('protomaps', LIGHT),
                    ...createZoneLayers()
                ]
            },
            center: [-123.0, 49.25],
            zoom: 8,
            maxZoom: 12,
            minZoom: 4,
            hash: true,
        });

        if (wetlandPatternData) map.addImage('wetland-pattern', wetlandPatternData);
        map.addControl(new maplibregl.NavigationControl(), 'top-right');
        map.addControl(new maplibregl.ScaleControl(), 'bottom-left');

        map.on('load', () => {
            // Highlight Source (Hover)
            map.addSource('highlight-source', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
            map.addLayer({ id: 'highlight-line', type: 'line', source: 'highlight-source', paint: { 'line-color': '#00ffff', 'line-width': 3, 'line-opacity': 0.8 } });
            map.addLayer({ id: 'highlight-fill', type: 'fill', source: 'highlight-source', paint: { 'fill-color': '#00ffff', 'fill-opacity': 0.2 }, filter: ['==', '$type', 'Polygon'] });
            
            // Selection Source (Persistent) - Using GeoJSON for reliability
            map.addSource('selected-source', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
            map.addLayer({ id: 'selected-fill', type: 'fill', source: 'selected-source', paint: { 'fill-color': '#2563eb', 'fill-opacity': 0.4 }, filter: ['==', '$type', 'Polygon'] });
            map.addLayer({ id: 'selected-line', type: 'line', source: 'selected-source', paint: { 'line-color': '#2563eb', 'line-width': 5, 'line-opacity': 1 } });

            for (let zone = 1; zone <= 8; zone++) {
                const streamId = `zone-${zone}-streams`;
                const streamFilter = layerVisibility.lakes
                    ? ['all', ['==', ['get', 'layer'], `zone_${zone}_streams`], ['==', ['get', 'lake_name'], '']]
                    : ['==', ['get', 'layer'], `zone_${zone}_streams`];
                if (map.getLayer(streamId)) map.setFilter(streamId, streamFilter);
            }
        });

        // 1. Collapse Logic
        map.on('movestart', (e) => {
            if (e.originalEvent) {
                setIsMobilePanelCollapsed(true);
            }
        });

        // 2. Self-Healing Geometry Logic
        // When zoom/move ends, check if we can find a better version of the selected feature
        map.on('moveend', () => {
            const currentSelected = selectedFeatureRef.current;
            if (!currentSelected || !currentSelected.id) return;

            // Query viewport for features that match our selected ID
            const features = map.queryRenderedFeatures({
                layers: [
                    'zone-1-streams', 'zone-2-streams', 'zone-3-streams', 'zone-4-streams',
                    'zone-5-streams', 'zone-6-streams', 'zone-7-streams', 'zone-8-streams',
                    // Include lakes/wetlands if needed
                ].concat(Array.from({ length: 8 }, (_, i) => [
                    `zone-${i + 1}-lakes-fill`, `zone-${i + 1}-wetlands-fill`
                ]).flat())
            });

            // Find matching feature by ID
            const match = features.find(f => {
                const fId = (f.properties?.linear_feature_id || f.properties?.waterbody_key || f.id)?.toString();
                return fId === currentSelected.id;
            });

            if (match) {
                // Found a higher-res version in the current tile view!
                const source = map.getSource('selected-source') as maplibregl.GeoJSONSource;
                if (source) {
                    source.setData({
                        type: 'FeatureCollection',
                        features: [{
                            type: 'Feature',
                            geometry: match.toJSON().geometry, // Use the new crisp geometry
                            properties: match.properties
                        } as any]
                    });
                }
            }
        });

        const interactableLayers = Array.from({ length: 8 }, (_, i) => [
            `zone-${i + 1}-streams`,
            `zone-${i + 1}-lakes-fill`,
            `zone-${i + 1}-wetlands-fill`,
            `zone-${i + 1}-manmade-fill`
        ]).flat();

        map.on('mousemove', (e) => {
            const buffer = 10;
            const bbox: [maplibregl.PointLike, maplibregl.PointLike] = [
                [e.point.x - buffer, e.point.y - buffer],
                [e.point.x + buffer, e.point.y + buffer]
            ];
            const features = map.queryRenderedFeatures(bbox, { layers: interactableLayers });
            map.getCanvas().style.cursor = features.length > 0 ? 'pointer' : '';

            const source = map.getSource('highlight-source') as maplibregl.GeoJSONSource;
            if (source) {
                const uniqueFeatures = features.filter((v, i, a) => 
                    a.findIndex(t => (t.id === v.id || t.properties?.linear_feature_id === v.properties?.linear_feature_id)) === i
                ).map(f => ({
                    type: 'Feature', geometry: f.toJSON().geometry, properties: f.properties
                }));
                source.setData({ type: 'FeatureCollection', features: uniqueFeatures as any });
            }
        });

        map.on('mouseleave', () => {
            map.getCanvas().style.cursor = '';
            const source = map.getSource('highlight-source') as maplibregl.GeoJSONSource;
            if (source) source.setData({ type: 'FeatureCollection', features: [] });
        });

        map.on('click', (e) => {
            const buffer = 15;
            const bbox: [maplibregl.PointLike, maplibregl.PointLike] = [
                [e.point.x - buffer, e.point.y - buffer],
                [e.point.x + buffer, e.point.y + buffer]
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

            const options: FeatureOption[] = features.map((feature, index) => {
                const plainGeometry = feature.toJSON().geometry; 
                
                // Determine ID key
                const props = feature.properties || {};
                let idKey = 'linear_feature_id';
                let idVal = props.linear_feature_id;

                if (props.waterbody_key) {
                    idKey = 'waterbody_key';
                    idVal = props.waterbody_key;
                } else if (!idVal) {
                    idKey = 'id';
                    idVal = feature.id; 
                }

                return {
                    type: getFeatureType(feature.layer.id),
                    properties: props,
                    id: (idVal || `feature-${index}`).toString(),
                    geometry: plainGeometry,
                    source: feature.layer.source,
                    sourceLayer: feature.layer['source-layer'],
                    idKey: idKey
                };
            });

            const uniqueOptions = options.filter((option, index, self) => 
                index === self.findIndex(o => o.id === option.id)
            );

            uniqueOptions.sort((a, b) => {
                const isLakeA = a.type === 'lake';
                const isLakeB = b.type === 'lake';
                if (isLakeA && !isLakeB) return -1;
                if (!isLakeA && isLakeB) return 1;

                const orderA = a.properties.stream_order !== undefined ? a.properties.stream_order : -1;
                const orderB = b.properties.stream_order !== undefined ? b.properties.stream_order : -1;
                
                if (orderA !== orderB) return orderB - orderA;

                const sizeA = calculateGeometrySize(a.geometry);
                const sizeB = calculateGeometrySize(b.geometry);
                return sizeB - sizeA;
            });

            clearSelection();

            if (uniqueOptions.length === 1) {
                setSelectedFeature(uniqueOptions[0]);
                selectedFeatureRef.current = uniqueOptions[0]; // Sync ref
            } else {
                setDisambiguationOptions(uniqueOptions);
                setDisambiguationPosition({ x: e.point.x, y: e.point.y });
            }
        });

        mapRef.current = map;
        return () => {
            map.remove();
            mapRef.current = null;
        };
    }, []);

    // --- ZOOM & SELECTION UPDATE LOGIC ---
    useEffect(() => {
        if (!mapRef.current) return;
        const map = mapRef.current;
        const source = map.getSource('selected-source') as maplibregl.GeoJSONSource;
        
        // Sync ref just in case
        selectedFeatureRef.current = selectedFeature;

        if (source) {
            if (selectedFeature && selectedFeature.geometry) {
                // 1. Initial Render (Low Res Snapshot)
                source.setData({ 
                    type: 'FeatureCollection', 
                    features: [{ 
                        type: 'Feature', 
                        geometry: selectedFeature.geometry, 
                        properties: selectedFeature.properties 
                    } as any] 
                });

                if (!isMobilePanelCollapsed) {
                    const bounds = new maplibregl.LngLatBounds();
                    const extend = (coord: any) => {
                        if (Array.isArray(coord) && typeof coord[0] === 'number') {
                            bounds.extend(coord as [number, number]);
                        } else if (Array.isArray(coord)) {
                            coord.forEach(extend);
                        }
                    };
                    extend(selectedFeature.geometry.coordinates);

                    const isMobile = window.innerWidth <= 768;
                    let padding = {};

                    if (isMobile) {
                        padding = { 
                            top: 80, 
                            bottom: window.innerHeight * 0.65,
                            left: 40, 
                            right: 40 
                        };
                    } else {
                        padding = { 
                            top: 50, 
                            bottom: 50, 
                            left: 50, 
                            right: 400 
                        };
                    }

                    if (!bounds.isEmpty()) {
                        map.fitBounds(bounds, {
                            padding,
                            maxZoom: 11, // Cap at 11
                            animate: true,
                            duration: 1000
                        });
                    }
                }

            } else {
                source.setData({ type: 'FeatureCollection', features: [] });
            }
        }
    }, [selectedFeature, isMobilePanelCollapsed]);

    useEffect(() => {
        if (!mapRef.current || !mapRef.current.isStyleLoaded()) return;
        const map = mapRef.current;
        for (let zone = 1; zone <= 8; zone++) {
             const fill = `zone-${zone}-boundaries-fill`;
             const line = `zone-${zone}-boundaries-line`;
             if (map.getLayer(fill)) map.setLayoutProperty(fill, 'visibility', layerVisibility.zones ? 'visible' : 'none');
             if (map.getLayer(line)) map.setLayoutProperty(line, 'visibility', layerVisibility.zones ? 'visible' : 'none');

             const streamId = `zone-${zone}-streams`;
             if (map.getLayer(streamId)) {
                map.setLayoutProperty(streamId, 'visibility', layerVisibility.streams ? 'visible' : 'none');
                const streamFilter = layerVisibility.lakes
                    ? ['all', ['==', ['get', 'layer'], `zone_${zone}_streams`], ['==', ['get', 'lake_name'], '']]
                    : ['==', ['get', 'layer'], `zone_${zone}_streams`];
                map.setFilter(streamId, streamFilter);
            }
             const lakeId = `zone-${zone}-lakes-fill`;
             if (map.getLayer(lakeId)) map.setLayoutProperty(lakeId, 'visibility', layerVisibility.lakes ? 'visible' : 'none');
             
             const wetlandId = `zone-${zone}-wetlands-fill`;
             if (map.getLayer(wetlandId)) map.setLayoutProperty(wetlandId, 'visibility', layerVisibility.wetlands ? 'visible' : 'none');
             
             const manmadeId = `zone-${zone}-manmade-fill`;
             if (map.getLayer(manmadeId)) map.setLayoutProperty(manmadeId, 'visibility', layerVisibility.manmade ? 'visible' : 'none');
        }
    }, [layerVisibility]);

    const toggleLayer = (layer: keyof LayerVisibility) => {
        setLayerVisibility(prev => ({ ...prev, [layer]: !prev[layer] }));
    };

    return (
        <div style={{ position: 'relative', width: '100%', height: '100%', fontFamily: 'sans-serif' }}>
            <div ref={mapContainerRef} style={{ width: '100%', height: '100%' }} />
            
            <div style={{
                position: 'absolute',
                top: '12px',
                left: '12px',
                backgroundColor: 'white',
                border: '1px solid black',
                boxShadow: '4px 4px 0 rgba(0,0,0,1)',
                padding: '16px',
                zIndex: 1,
                minWidth: '160px'
            }}>
                <div style={{ fontWeight: '800', marginBottom: '12px', fontSize: '11px', textTransform: 'uppercase', borderBottom: '2px solid #eee', paddingBottom: '8px', letterSpacing: '0.1em' }}>
                    Layers
                </div>
                {['zones', 'streams', 'lakes', 'wetlands', 'manmade'].map((key) => (
                    <label key={key} style={{ display: 'flex', alignItems: 'center', marginBottom: '8px', cursor: 'pointer', fontSize: '12px', fontWeight: '500', textTransform: 'uppercase' }}>
                        <input type="checkbox" checked={layerVisibility[key as keyof LayerVisibility]} onChange={() => toggleLayer(key as keyof LayerVisibility)} style={{ marginRight: '8px', cursor: 'pointer', accentColor: 'black' }} />
                        {key}
                    </label>
                ))}
            </div>

            <InfoPanel 
                feature={selectedFeature} 
                onClose={() => clearSelection()}
                isCollapsed={isMobilePanelCollapsed}
                onSetCollapse={setIsMobilePanelCollapsed}
            />

            {disambiguationOptions.length > 0 && (
                <DisambiguationMenu
                    options={disambiguationOptions}
                    position={disambiguationPosition}
                    onSelect={(option) => {
                        clearSelection();
                        setSelectedFeature(option);
                        selectedFeatureRef.current = option;
                    }}
                    onClose={() => clearSelection()}
                    isCollapsed={isMobilePanelCollapsed}
                    onSetCollapse={setIsMobilePanelCollapsed}
                />
            )}
        </div>
    );
};

export default Map;