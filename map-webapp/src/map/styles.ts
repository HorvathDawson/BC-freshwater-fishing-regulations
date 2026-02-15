import type { LayerSpecification } from 'maplibre-gl';

// Feature type colors
const FEATURE_COLORS = {
    streams: '#4A90E2',    // Blue for streams
    lakes: '#64B5F6',      // Light blue for lakes
    wetlands: '#81C784',   // Green for wetlands
    manmade: '#9575CD',    // Purple for manmade waterbodies
    hover: '#FFD700',      // Gold for hover highlight
};

// Helper function to create regulation layers from new PMTiles structure
export const createRegulationLayers = (): LayerSpecification[] => {
    const layers: LayerSpecification[] = [];
    
    // Wetlands - rendered first (lowest z-index)
    layers.push({
        id: 'wetlands-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'wetlands',
        paint: {
            'fill-pattern': 'wetland-pattern',
            'fill-opacity': [
                'interpolate',
                ['linear'],
                ['zoom'],
                4, 0.3,
                10, 0.4,
                12, 0.5
            ]
        }
    });
    
    layers.push({
        id: 'wetlands-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'wetlands',
        paint: {
            'line-color': FEATURE_COLORS.wetlands,
            'line-width': [
                'interpolate',
                ['linear'],
                ['zoom'],
                4, 0.5,
                10, 0.75,
                12, 1.25
            ],
            'line-opacity': 0.6
        },
        layout: {
            'line-cap': 'round',
            'line-join': 'round'
        }
    });
    
    // Lakes - rendered above wetlands
    layers.push({
        id: 'lakes-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'lakes',
        paint: {
            'fill-color': FEATURE_COLORS.lakes,
            'fill-opacity': 0.4,
            'fill-antialias': true
        }
    });
    
    layers.push({
        id: 'lakes-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'lakes',
        paint: {
            'line-color': FEATURE_COLORS.lakes,
            'line-width': [
                'interpolate',
                ['linear'],
                ['zoom'],
                4, 0.8,
                10, 1,
                12, 1.5
            ],
            'line-opacity': 0.8
        },
        layout: {
            'line-cap': 'round',
            'line-join': 'round'
        }
    });
    
    // Manmade waterbodies - rendered above wetlands
    layers.push({
        id: 'manmade-fill',
        type: 'fill',
        source: 'regulations',
        'source-layer': 'manmade',
        paint: {
            'fill-color': FEATURE_COLORS.manmade,
            'fill-opacity': 0.35,
            'fill-antialias': true
        }
    });
    
    layers.push({
        id: 'manmade-line',
        type: 'line',
        source: 'regulations',
        'source-layer': 'manmade',
        paint: {
            'line-color': FEATURE_COLORS.manmade,
            'line-width': [
                'interpolate',
                ['linear'],
                ['zoom'],
                4, 0.8,
                10, 1,
                12, 1.5
            ],
            'line-opacity': 0.7,
            'line-dasharray': [3, 2]  // Dashed line for manmade
        },
        layout: {
            'line-cap': 'round',
            'line-join': 'round'
        }
    });
    
    // Streams - tippecanoe handles minzoom via importance scoring (with side channel penalty)
    // Line width is proportional to stream_order (calculated from property)
    layers.push({
        id: 'streams',
        type: 'line',
        source: 'regulations',
        'source-layer': 'streams',
        paint: {
            'line-color': FEATURE_COLORS.streams,
            'line-width': [
                'interpolate',
                ['linear'],
                ['zoom'],
                // At low zoom: 0.5 + (order * 0.1) - minimal stream order impact when zoomed out
                4, ['+', 0.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.1]],
                // At medium zoom: 0.6 + (order * 0.15) - still small impact
                8, ['+', 0.6, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.15]],
                // At zoom 11: ramp up - original formula * 1.5
                11, ['*', ['+', 0.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 1.5],
                // At zoom 12: original formula * 2 (like original zoom 8)
                12, ['*', ['+', 0.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 2],
                // At zoom 14: original formula * 3 (like original zoom 12)
                14, ['*', ['+', 0.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 3],
                // At zoom 16+: even bigger
                16, ['*', ['+', 0.5, ['*', ['coalesce', ['get', 'stream_order'], 1], 0.5]], 4]
            ],
            'line-opacity': 0.8
        },
        layout: {
            'line-cap': 'round',
            'line-join': 'round'
        }
    });
    
    // Regions (zone boundaries) - rendered on top for visibility
    layers.push({
        id: 'regions',
        type: 'line',
        source: 'regulations',
        'source-layer': 'regions',
        paint: {
            'line-color': ['get', 'stroke_color'],
            'line-width': [
                'interpolate',
                ['linear'],
                ['zoom'],
                4, 2,
                8, 2.5,
                12, 3
            ],
            'line-opacity': 0.8
        },
        layout: {
            'line-cap': 'round',
            'line-join': 'round'
        }
    });
    
    return layers;
};
