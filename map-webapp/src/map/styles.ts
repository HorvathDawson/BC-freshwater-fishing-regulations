import type { LayerSpecification } from 'maplibre-gl';

// Zone color scheme - vibrant colors with good contrast against basemap
export const ZONE_COLORS: Record<string, string> = {
    '1': '#E63946',  // Vibrant Red
    '2': '#06D6A0',  // Emerald Green
    '3': '#118AB2',  // Ocean Blue
    '4': '#FF6F00',  // Vivid Orange
    '5': '#8338EC',  // Bright Purple
    '6': '#FFB703',  // Golden Yellow
    '7': '#EF476F',  // Hot Pink
    '8': '#2A9D8F',  // Teal
};

// Helper function to create layers for each zone
export const createZoneLayers = (): LayerSpecification[] => {
    const zoneLayers: LayerSpecification[] = [];
    
    for (let zone = 1; zone <= 8; zone++) {
        const zoneStr = zone.toString();
        const color = ZONE_COLORS[zoneStr];
        
        // Zone boundaries (lowest layer) - always visible
        zoneLayers.push({
            id: `zone-${zone}-boundaries-fill`,
            type: 'fill',
            source: 'waterbodies',
            'source-layer': 'waterbodies',
            filter: ['==', ['get', 'layer'], `zone_${zone}_boundaries`],
            minzoom: 0,
            maxzoom: 24,
            paint: {
                'fill-color': color,
                'fill-opacity': 0.08,  // Reduced opacity for more subtle background
                'fill-antialias': true
            }
        });
        
        zoneLayers.push({
            id: `zone-${zone}-boundaries-line`,
            type: 'line',
            source: 'waterbodies',
            'source-layer': 'waterbodies',
            filter: ['==', ['get', 'layer'], `zone_${zone}_boundaries`],
            minzoom: 0,
            maxzoom: 24,
            paint: {
                'line-color': color,
                'line-width': [
                    'interpolate',
                    ['linear'],
                    ['zoom'],
                    4, 1.5,
                    10, 2.5,
                    14, 4
                ],
                'line-opacity': 0.7,
                'line-dasharray': [4, 3]  // Dotted line pattern for zone boundaries
            },
            layout: {
                'line-cap': 'butt',  // Square caps work better with dashed lines
                'line-join': 'miter'
            }
        });
        
        // Wetlands - always visible (rendered first, below lakes and manmade)
        // Using a diagonal line pattern fill
        zoneLayers.push({
            id: `zone-${zone}-wetlands-fill`,
            type: 'fill',
            source: 'waterbodies',
            'source-layer': 'waterbodies',
            filter: ['==', ['get', 'layer'], `zone_${zone}_wetlands`],
            minzoom: 0,
            maxzoom: 24,
            paint: {
                'fill-pattern': 'wetland-pattern',
                'fill-opacity': [
                    'interpolate',
                    ['linear'],
                    ['zoom'],
                    4, 0.3,
                    10, 0.4,
                    14, 0.5
                ]
            }
        });
        
        zoneLayers.push({
            id: `zone-${zone}-wetlands-line`,
            type: 'line',
            source: 'waterbodies',
            'source-layer': 'waterbodies',
            filter: ['==', ['get', 'layer'], `zone_${zone}_wetlands`],
            minzoom: 0,
            maxzoom: 24,
            paint: {
                'line-color': color,
                'line-width': [
                    'interpolate',
                    ['linear'],
                    ['zoom'],
                    4, 0.5,
                    10, 0.75,
                    14, 1.25
                ],
                'line-opacity': 0.6
            },
            layout: {
                'line-cap': 'round',
                'line-join': 'round'
            }
        });
        
        // Lakes - always visible (rendered above wetlands)
        zoneLayers.push({
            id: `zone-${zone}-lakes-fill`,
            type: 'fill',
            source: 'waterbodies',
            'source-layer': 'waterbodies',
            filter: ['==', ['get', 'layer'], `zone_${zone}_lakes`],
            minzoom: 0,
            maxzoom: 24,
            paint: {
                'fill-color': color,
                'fill-opacity': 0.4,
                'fill-antialias': true
            }
        });
        
        zoneLayers.push({
            id: `zone-${zone}-lakes-line`,
            type: 'line',
            source: 'waterbodies',
            'source-layer': 'waterbodies',
            filter: ['==', ['get', 'layer'], `zone_${zone}_lakes`],
            minzoom: 0,
            maxzoom: 24,
            paint: {
                'line-color': color,
                'line-width': [
                    'interpolate',
                    ['linear'],
                    ['zoom'],
                    4, 0.8,
                    10, 1,
                    14, 1.5
                ],
                'line-opacity': 0.8
            },
            layout: {
                'line-cap': 'round',
                'line-join': 'round'
            }
        });
        
        // Manmade waterbodies - always visible (rendered above wetlands)
        zoneLayers.push({
            id: `zone-${zone}-manmade-fill`,
            type: 'fill',
            source: 'waterbodies',
            'source-layer': 'waterbodies',
            filter: ['==', ['get', 'layer'], `zone_${zone}_manmade`],
            minzoom: 0,
            maxzoom: 24,
            paint: {
                'fill-color': color,
                'fill-opacity': 0.35,
                'fill-antialias': true
            }
        });
        
        zoneLayers.push({
            id: `zone-${zone}-manmade-line`,
            type: 'line',
            source: 'waterbodies',
            'source-layer': 'waterbodies',
            filter: ['==', ['get', 'layer'], `zone_${zone}_manmade`],
            minzoom: 0,
            maxzoom: 24,
            paint: {
                'line-color': color,
                'line-width': [
                    'interpolate',
                    ['linear'],
                    ['zoom'],
                    4, 0.8,
                    10, 1,
                    14, 1.5
                ],
                'line-opacity': 0.7,
                'line-dasharray': [3, 2]  // Dashed line for manmade
            },
            layout: {
                'line-cap': 'round',
                'line-join': 'round'
            }
        });
        
        // Streams - tippecanoe already filtered by stream_order via minzoom
        // Line width is proportional to stream_order (higher order = thicker)
        zoneLayers.push({
            id: `zone-${zone}-streams`,
            type: 'line',
            source: 'waterbodies',
            'source-layer': 'waterbodies',
            filter: ['==', ['get', 'layer'], `zone_${zone}_streams`],
            minzoom: 0,
            maxzoom: 24,
            paint: {
                'line-color': color,
                'line-width': [
                    'interpolate',
                    ['linear'],
                    ['zoom'],
                    4, [
                        'interpolate',
                        ['linear'],
                        ['coalesce', ['get', 'stream_order'], 1],
                        1, 0.6,    // Stream order 1: very thin
                        3, 0.8,    // Stream order 3: thin
                        5, 1.0,    // Stream order 5: medium-thin
                        7, 1.4,    // Stream order 7: medium
                        9, 1.8     // Stream order 9+: thicker
                    ],
                    8, [
                        'interpolate',
                        ['linear'],
                        ['coalesce', ['get', 'stream_order'], 1],
                        1, 0.8,
                        3, 1.2,
                        5, 1.6,
                        7, 2.2,
                        9, 2.8
                    ],
                    12, [
                        'interpolate',
                        ['linear'],
                        ['coalesce', ['get', 'stream_order'], 1],
                        1, 1.5,
                        3, 2.5,
                        5, 3.5,
                        7, 5.0,
                        9, 6.5
                    ],
                    16, [
                        'interpolate',
                        ['linear'],
                        ['coalesce', ['get', 'stream_order'], 1],
                        1, 2.5,
                        3, 4.0,
                        5, 6.0,
                        7, 8.5,
                        9, 11.0
                    ],
                    20, [
                        'interpolate',
                        ['linear'],
                        ['coalesce', ['get', 'stream_order'], 1],
                        1, 4.0,
                        3, 7.0,
                        5, 10.0,
                        7, 14.0,
                        9, 18.0
                    ]
                ],
                'line-opacity': 0.8
            },
            layout: {
                'line-cap': 'round',
                'line-join': 'round'
            }
        });
        
        // Stream hover/highlight layer (visible at zoom >= 8 when watershed code is hovered)
        // Much thicker and brighter to make hover obvious
        zoneLayers.push({
            id: `zone-${zone}-streams-hover`,
            type: 'line',
            source: 'waterbodies',
            'source-layer': 'waterbodies',
            filter: ['all',
                ['==', ['get', 'layer'], `zone_${zone}_streams`],
                ['==', ['get', 'fwa_watershed_code'], '__none__']  // Will be updated dynamically
            ],
            minzoom: 8,  // Show highlights at zoom 8 and above
            maxzoom: 24,
            paint: {
                'line-color': '#FFD700',  // Bright gold color for visibility
                'line-width': [
                    'interpolate',
                    ['linear'],
                    ['zoom'],
                    10, [
                        'interpolate',
                        ['linear'],
                        ['coalesce', ['get', 'stream_order'], 1],
                        1, 3.0,    // Much thicker than base
                        3, 4.5,
                        5, 6.0,
                        7, 8.0,
                        9, 10.0
                    ],
                    12, [
                        'interpolate',
                        ['linear'],
                        ['coalesce', ['get', 'stream_order'], 1],
                        1, 4.0,
                        3, 6.0,
                        5, 8.0,
                        7, 11.0,
                        9, 14.0
                    ],
                    16, [
                        'interpolate',
                        ['linear'],
                        ['coalesce', ['get', 'stream_order'], 1],
                        1, 6.0,
                        3, 9.0,
                        5, 12.0,
                        7, 16.0,
                        9, 20.0
                    ],
                    20, [
                        'interpolate',
                        ['linear'],
                        ['coalesce', ['get', 'stream_order'], 1],
                        1, 10.0,
                        3, 14.0,
                        5, 18.0,
                        7, 24.0,
                        9, 30.0
                    ]
                ],
                'line-opacity': 0.8,
                'line-blur': 2  // Glowing effect
            },
            layout: {
                'line-cap': 'round',
                'line-join': 'round'
            }
        });
    }
    
    return zoneLayers;
};
