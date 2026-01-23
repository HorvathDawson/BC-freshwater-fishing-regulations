import { useEffect, useRef, useState } from 'react';
import maplibregl from 'maplibre-gl';
import { Protocol } from 'pmtiles';
import { layers, LIGHT } from '@protomaps/basemaps'; 
import 'maplibre-gl/dist/maplibre-gl.css';
import { createZoneLayers } from '../map/styles';
import LayerStatsPanel from './LayerStats';

// Register PMTiles protocol
const protocol = new Protocol();
maplibregl.addProtocol('pmtiles', protocol.tile);

interface LayerVisibility {
    zones: boolean;
    streams: boolean;
    lakes: boolean;
    wetlands: boolean;
    manmade: boolean;
}

const Map = () => {
    const mapContainerRef = useRef<HTMLDivElement>(null);
    const mapRef = useRef<maplibregl.Map | null>(null);
    const [hoveredWatershedCode, setHoveredWatershedCode] = useState<string | null>(null);
    const hoveredWatershedCodeRef = useRef<string | null>(null);
    const [layerVisibility, setLayerVisibility] = useState<LayerVisibility>({
        zones: true,
        streams: true,
        lakes: true,
        wetlands: true,
        manmade: true,
    });

    useEffect(() => {
        if (!mapContainerRef.current) return;

        // Create wetland pattern image before initializing map
        const canvas = document.createElement('canvas');
        const size = 16;
        canvas.width = size;
        canvas.height = size;
        const ctx = canvas.getContext('2d');
        let wetlandPatternData: ImageData | null = null;
        
        if (ctx) {
            // Draw diagonal lines pattern
            ctx.strokeStyle = '#000000';
            ctx.lineWidth = 1;
            
            // Draw diagonal lines from top-left to bottom-right
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
            // Add zone-specific waterbody layers
            ...createZoneLayers()
            ]
        },
        center: [-123.0, 49.25],
        zoom: 8,
        maxZoom: 12,
        minZoom: 4,
        hash: true,
        });

        // Add pattern image immediately on map creation
        if (wetlandPatternData) {
            map.addImage('wetland-pattern', wetlandPatternData);
        }

        map.addControl(new maplibregl.NavigationControl(), 'top-right');
        map.addControl(new maplibregl.ScaleControl(), 'bottom-left');

        // Debug: Basic logging only
        map.on('load', () => {
            console.log('Map loaded');
            
            // Apply initial stream filters based on lakes visibility
            for (let zone = 1; zone <= 8; zone++) {
                const streamId = `zone-${zone}-streams`;
                
                const streamFilter = layerVisibility.lakes
                    ? ['all', ['==', ['get', 'layer'], `zone_${zone}_streams`], ['==', ['get', 'lake_name'], '']]
                    : ['==', ['get', 'layer'], `zone_${zone}_streams`];
                
                if (map.getLayer(streamId)) {
                    map.setFilter(streamId, streamFilter);
                }
                // Don't update hover layer filters here - they have their own watershed code logic
            }
        });

        map.on('error', (e) => {
            console.error('Map error:', e);
        });

        // Add hover interaction for streams (only at zoom >= 8)
        const streamLayerIds = Array.from({ length: 8 }, (_, i) => `zone-${i + 1}-streams`);
        let throttleTimeout: number | null = null;
        
        map.on('mousemove', (e) => {
            // Throttle to every 50ms for performance
            if (throttleTimeout) return;
            
            throttleTimeout = window.setTimeout(() => {
                throttleTimeout = null;
            }, 50);

            const zoom = map.getZoom();
            if (zoom < 8) {
                // Clear any existing hover state if zoomed out
                if (hoveredWatershedCodeRef.current) {
                    hoveredWatershedCodeRef.current = null;
                    setHoveredWatershedCode(null);
                    map.getCanvas().style.cursor = '';
                }
                return;
            }

            // Add a small buffer around the cursor for easier hovering (fat thumb margin)
            const buffer = 8; // pixels
            const bbox: [maplibregl.PointLike, maplibregl.PointLike] = [
                [e.point.x - buffer, e.point.y - buffer],
                [e.point.x + buffer, e.point.y + buffer]
            ];

            const features = map.queryRenderedFeatures(bbox, {
                layers: streamLayerIds
            });

            if (features.length > 0) {
                // Try different property names for watershed code
                const props = features[0].properties;
                const watershedCode = props?.fwa_watershed_code || props?.watershed_code || props?.linear_feature_id;
                
                if (watershedCode && watershedCode !== hoveredWatershedCodeRef.current) {
                    hoveredWatershedCodeRef.current = watershedCode;
                    setHoveredWatershedCode(watershedCode);
                    map.getCanvas().style.cursor = 'pointer';
                }
            } else {
                // No stream under cursor - immediately clear hover
                if (hoveredWatershedCodeRef.current !== null) {
                    hoveredWatershedCodeRef.current = null;
                    setHoveredWatershedCode(null);
                    map.getCanvas().style.cursor = '';
                }
            }
        });

        map.on('mouseleave', () => {
            if (hoveredWatershedCodeRef.current) {
                hoveredWatershedCodeRef.current = null;
                setHoveredWatershedCode(null);
                map.getCanvas().style.cursor = '';
            }
        });

        mapRef.current = map;

        return () => {
        map.remove();
        mapRef.current = null;
        };
    }, []);

    // Update hover filters when hoveredWatershedCode changes
    useEffect(() => {
        if (!mapRef.current) return;
        
        const map = mapRef.current;
        
        // Update all zone stream hover layers
        for (let zone = 1; zone <= 8; zone++) {
            const layerId = `zone-${zone}-streams-hover`;
            const layer = map.getLayer(layerId);
            
            if (layer) {
                if (hoveredWatershedCode) {
                    // Combine watershed code filter with lake_name filter when lakes are visible
                    const baseFilter = [
                        'all',
                        ['==', ['get', 'layer'], `zone_${zone}_streams`],
                        ['any',
                            ['==', ['get', 'fwa_watershed_code'], hoveredWatershedCode],
                            ['==', ['get', 'watershed_code'], hoveredWatershedCode],
                            ['==', ['get', 'linear_feature_id'], hoveredWatershedCode]
                        ]
                    ];
                    
                    // Add lake_name filter if lakes are visible
                    const finalFilter = layerVisibility.lakes
                        ? ['all', ...baseFilter.slice(1), ['==', ['get', 'lake_name'], '']]
                        : baseFilter;
                    
                    map.setFilter(layerId, finalFilter);
                } else {
                    // Hide all hover features by using a filter that can never match
                    map.setFilter(layerId, ['==', ['id'], -1]);
                }
            }
        }
    }, [hoveredWatershedCode, layerVisibility.lakes]);

    // Update layer visibility when toggles change
    useEffect(() => {
        if (!mapRef.current) return;
        
        const map = mapRef.current;
        
        for (let zone = 1; zone <= 8; zone++) {
            // Zone boundaries
            const zoneFillId = `zone-${zone}-boundaries-fill`;
            const zoneLineId = `zone-${zone}-boundaries-line`;
            if (map.getLayer(zoneFillId)) {
                map.setLayoutProperty(zoneFillId, 'visibility', layerVisibility.zones ? 'visible' : 'none');
            }
            if (map.getLayer(zoneLineId)) {
                map.setLayoutProperty(zoneLineId, 'visibility', layerVisibility.zones ? 'visible' : 'none');
            }
            
            // Streams - filter out lake segments if lakes are visible
            const streamId = `zone-${zone}-streams`;
            const streamHoverId = `zone-${zone}-streams-hover`;
            if (map.getLayer(streamId)) {
                map.setLayoutProperty(streamId, 'visibility', layerVisibility.streams ? 'visible' : 'none');
                
                // Apply filter: when lakes are visible, hide stream segments inside lakes
                const streamFilter = layerVisibility.lakes
                    ? ['all', ['==', ['get', 'layer'], `zone_${zone}_streams`], ['==', ['get', 'lake_name'], '']]
                    : ['==', ['get', 'layer'], `zone_${zone}_streams`];
                map.setFilter(streamId, streamFilter);
            }
            if (map.getLayer(streamHoverId)) {
                map.setLayoutProperty(streamHoverId, 'visibility', layerVisibility.streams ? 'visible' : 'none');
                // Hover layer filter is managed by mousemove handler - don't override it here
            }
            
            // Lakes
            const lakeFillId = `zone-${zone}-lakes-fill`;
            const lakeLineId = `zone-${zone}-lakes-line`;
            if (map.getLayer(lakeFillId)) {
                map.setLayoutProperty(lakeFillId, 'visibility', layerVisibility.lakes ? 'visible' : 'none');
            }
            if (map.getLayer(lakeLineId)) {
                map.setLayoutProperty(lakeLineId, 'visibility', layerVisibility.lakes ? 'visible' : 'none');
            }
            
            // Wetlands
            const wetlandFillId = `zone-${zone}-wetlands-fill`;
            const wetlandLineId = `zone-${zone}-wetlands-line`;
            if (map.getLayer(wetlandFillId)) {
                map.setLayoutProperty(wetlandFillId, 'visibility', layerVisibility.wetlands ? 'visible' : 'none');
            }
            if (map.getLayer(wetlandLineId)) {
                map.setLayoutProperty(wetlandLineId, 'visibility', layerVisibility.wetlands ? 'visible' : 'none');
            }
            
            // Manmade
            const manmadeFillId = `zone-${zone}-manmade-fill`;
            const manmadeLineId = `zone-${zone}-manmade-line`;
            if (map.getLayer(manmadeFillId)) {
                map.setLayoutProperty(manmadeFillId, 'visibility', layerVisibility.manmade ? 'visible' : 'none');
            }
            if (map.getLayer(manmadeLineId)) {
                map.setLayoutProperty(manmadeLineId, 'visibility', layerVisibility.manmade ? 'visible' : 'none');
            }
        }
    }, [layerVisibility]);

    const toggleLayer = (layer: keyof LayerVisibility) => {
        setLayerVisibility(prev => ({
            ...prev,
            [layer]: !prev[layer]
        }));
    };

    return (
        <div style={{ position: 'relative', width: '100%', height: '100%' }}>
            <div 
                ref={mapContainerRef} 
                style={{ width: '100%', height: '100%' }} 
            />
            
            {/* Layer Statistics Panel */}
            <LayerStatsPanel />
            
            {/* Layer toggle controls */}
            <div style={{
                position: 'absolute',
                top: '10px',
                left: '10px',
                backgroundColor: 'white',
                borderRadius: '4px',
                padding: '12px',
                boxShadow: '0 2px 4px rgba(0,0,0,0.2)',
                zIndex: 1,
                minWidth: '150px'
            }}>
                <div style={{ 
                    fontWeight: 'bold', 
                    marginBottom: '8px',
                    fontSize: '14px',
                    borderBottom: '1px solid #ddd',
                    paddingBottom: '6px'
                }}>
                    Layers
                </div>
                
                {[
                    { key: 'zones' as const, label: 'Zone Boundaries' },
                    { key: 'streams' as const, label: 'Streams' },
                    { key: 'lakes' as const, label: 'Lakes' },
                    { key: 'wetlands' as const, label: 'Wetlands' },
                    { key: 'manmade' as const, label: 'Manmade' },
                ].map(({ key, label }) => (
                    <label 
                        key={key}
                        style={{ 
                            display: 'flex', 
                            alignItems: 'center',
                            marginBottom: '6px',
                            cursor: 'pointer',
                            fontSize: '13px'
                        }}
                    >
                        <input
                            type="checkbox"
                            checked={layerVisibility[key]}
                            onChange={() => toggleLayer(key)}
                            style={{ marginRight: '8px', cursor: 'pointer' }}
                        />
                        {label}
                    </label>
                ))}
            </div>
        </div>
    );
};

export default Map;