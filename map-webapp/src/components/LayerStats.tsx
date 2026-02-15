import { useEffect, useState } from 'react';
import { PMTiles } from 'pmtiles';
import { VectorTile } from '@mapbox/vector-tile';
import Protobuf from 'pbf';

// Add keyframe animations
const style = document.createElement('style');
style.textContent = `
    @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
    }
    @keyframes shimmer {
        0% { transform: translateX(-100%); }
        100% { transform: translateX(100%); }
    }
    /* Custom Scrollbar */
    .layer-stats-scroll::-webkit-scrollbar {
        width: 8px;
    }
    .layer-stats-scroll::-webkit-scrollbar-track {
        background: #f1f5f9;
        border-radius: 4px;
    }
    .layer-stats-scroll::-webkit-scrollbar-thumb {
        background: #cbd5e1;
        border-radius: 4px;
    }
    .layer-stats-scroll::-webkit-scrollbar-thumb:hover {
        background: #94a3b8;
    }
`;
if (!document.head.querySelector('style[data-layer-stats]')) {
    style.setAttribute('data-layer-stats', 'true');
    document.head.appendChild(style);
}

interface ZoomStats {
    [zoom: string]: number;
}

interface LayerStats {
    [layerName: string]: ZoomStats;
}

interface WatershedSets {
    [layerName: string]: { [zoom: number]: Set<string> };
}

const LayerStatsPanel = () => {
    const [featureStats, setFeatureStats] = useState<LayerStats>({});
    const [watershedStats, setWatershedStats] = useState<LayerStats>({});
    const [watershedSets, setWatershedSets] = useState<WatershedSets>({});
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [progress, setProgress] = useState<string>('');
    const [isExpanded, setIsExpanded] = useState(false);
    const [selectedLayer, setSelectedLayer] = useState<string>('all');
    const [groupByType, setGroupByType] = useState<boolean>(true);

    const analyzePMTiles = async () => {
        try {
            setLoading(true);
            setError(null);
            setProgress('Initializing...');

            const pmtiles = new PMTiles('/data/waterbodies_bc.pmtiles');
            const header = await pmtiles.getHeader();
            
            setProgress('Loading tile metadata...');
            const metadata = await pmtiles.getMetadata();

            // Initialize counters
            const featureCounts: LayerStats = {};
            const watershedCounts: { [layerName: string]: { [zoom: number]: Set<string> } } = {};

            // Get all tiles from the archive
            setProgress('Analyzing tiles...');
            
            let tilesProcessed = 0;

            // Iterate through all zoom levels
            for (let z = header.minZoom; z <= header.maxZoom; z++) {
                setProgress(`Processing zoom level ${z}/${header.maxZoom}...`);
                
                // Get tile coordinates for this zoom level
                const tilesAtZoom = getTilesAtZoom(z, header);
                
                for (const { x, y, z: zoom } of tilesAtZoom) {
                    try {
                        const tileData = await pmtiles.getZxy(zoom, x, y);
                        
                        if (tileData?.data) {
                            // Parse the vector tile
                            const tile = new VectorTile(new Protobuf(tileData.data));
                            
                            // Process each source layer in the tile
                            for (const sourceLayerName of Object.keys(tile.layers)) {
                                const layer = tile.layers[sourceLayerName];
                                
                                // Count features in this layer, grouped by their 'layer' property
                                for (let i = 0; i < layer.length; i++) {
                                    const feature = layer.feature(i);
                                    const props = feature.properties;
                                    
                                    // Use the 'layer' property from the feature to categorize
                                    const featureLayerName = props.layer || sourceLayerName;
                                    
                                    if (!featureCounts[featureLayerName]) {
                                        featureCounts[featureLayerName] = {};
                                    }
                                    if (!watershedCounts[featureLayerName]) {
                                        watershedCounts[featureLayerName] = {};
                                    }
                                    
                                    // Initialize sets for this zoom if needed
                                    for (let trackZoom = z; trackZoom <= header.maxZoom; trackZoom++) {
                                        if (!watershedCounts[featureLayerName][trackZoom]) {
                                            watershedCounts[featureLayerName][trackZoom] = new Set<string>();
                                        }
                                    }
                                    
                                    // Get the minzoom for this feature
                                    const minzoomProp = props['tippecanoe:minzoom'];
                                    const minzoom = typeof minzoomProp === 'number' ? minzoomProp : z;
                                    
                                    // Count this feature for all zoom levels >= minzoom
                                    for (let countZoom = minzoom; countZoom <= header.maxZoom; countZoom++) {
                                        if (!featureCounts[featureLayerName][countZoom]) {
                                            featureCounts[featureLayerName][countZoom] = 0;
                                        }
                                        featureCounts[featureLayerName][countZoom]++;
                                        
                                        // Track unique watersheds for stream layers
                                        if (featureLayerName.toLowerCase().includes('stream')) {
                                            const watershedCode = props.watershed_code || props.fwa_watershed_code;
                                            if (watershedCode) {
                                                watershedCounts[featureLayerName][countZoom].add(String(watershedCode));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                        tilesProcessed++;
                        if (tilesProcessed % 10 === 0) {
                            setProgress(`Processed ${tilesProcessed} tiles...`);
                        }
                    } catch (err) {
                        console.warn(`Error processing tile ${z}/${x}/${y}:`, err);
                    }
                }
            }

            // Store the raw watershed data for grouping later
            // Convert watershed Sets to counts
            const watershedCountsConverted: LayerStats = {};
            for (const [layerName, zoomSets] of Object.entries(watershedCounts)) {
                watershedCountsConverted[layerName] = {};
                for (const [zoom, watershedSet] of Object.entries(zoomSets)) {
                    watershedCountsConverted[layerName][zoom] = watershedSet.size;
                }
            }

            setFeatureStats(featureCounts);
            setWatershedStats(watershedCountsConverted);
            setWatershedSets(watershedCounts); // Store the original Sets
            setProgress('');
            setLoading(false);
        } catch (err) {
            console.error('Error analyzing PMTiles:', err);
            setError(err instanceof Error ? err.message : 'Unknown error');
            setProgress('');
            setLoading(false);
        }
    };

    const getTilesAtZoom = (zoom: number, header: any): Array<{ x: number; y: number; z: number }> => {
        // Calculate tile bounds at this zoom level
        const tiles: Array<{ x: number; y: number; z: number }> = [];
        
        // Convert geographic bounds to tile coordinates
        const minTileX = Math.max(0, Math.floor((header.minLon + 180) / 360 * Math.pow(2, zoom)));
        const maxTileX = Math.min(Math.pow(2, zoom) - 1, Math.floor((header.maxLon + 180) / 360 * Math.pow(2, zoom)));
        
        const minTileY = Math.max(0, Math.floor((1 - Math.log(Math.tan(header.maxLat * Math.PI / 180) + 1 / Math.cos(header.maxLat * Math.PI / 180)) / Math.PI) / 2 * Math.pow(2, zoom)));
        const maxTileY = Math.min(Math.pow(2, zoom) - 1, Math.floor((1 - Math.log(Math.tan(header.minLat * Math.PI / 180) + 1 / Math.cos(header.minLat * Math.PI / 180)) / Math.PI) / 2 * Math.pow(2, zoom)));
        
        for (let x = minTileX; x <= maxTileX; x++) {
            for (let y = minTileY; y <= maxTileY; y++) {
                tiles.push({ x, y, z: zoom });
            }
        }
        
        return tiles;
    };

    useEffect(() => {
        // Don't auto-load on mount - wait for user to click reload
    }, []);

    const getLayerType = (layerName: string): string => {
        const lower = layerName.toLowerCase();
        if (lower.includes('stream')) return 'streams';
        if (lower.includes('lake')) return 'lakes';
        if (lower.includes('wetland')) return 'wetlands';
        if (lower.includes('manmade')) return 'manmade';
        if (lower.includes('boundaries') || lower.includes('boundary')) return 'boundaries';
        return 'other';
    };

    const groupStatsByType = (stats: LayerStats): LayerStats => {
        const grouped: LayerStats = {};
        
        for (const [layerName, zoomStats] of Object.entries(stats)) {
            const type = getLayerType(layerName);
            
            if (!grouped[type]) {
                grouped[type] = {};
            }
            
            // Merge zoom stats
            for (const [zoom, count] of Object.entries(zoomStats)) {
                if (!grouped[type][zoom]) {
                    grouped[type][zoom] = 0;
                }
                grouped[type][zoom] += Number(count);
            }
        }
        
        return grouped;
    };

    const groupWatershedStatsByType = (stats: WatershedSets): { [type: string]: { [zoom: number]: Set<string> } } => {
        const grouped: { [type: string]: { [zoom: number]: Set<string> } } = {};
        
        for (const [layerName, zoomSets] of Object.entries(stats)) {
            const type = getLayerType(layerName);
            
            if (!grouped[type]) {
                grouped[type] = {};
            }
            
            // Merge watershed sets
            for (const [zoom, watershedSet] of Object.entries(zoomSets)) {
                if (!grouped[type][Number(zoom)]) {
                    grouped[type][Number(zoom)] = new Set<string>();
                }
                watershedSet.forEach(code => grouped[type][Number(zoom)].add(code));
            }
        }
        
        return grouped;
    };

    const getDisplayStats = (): LayerStats => {
        if (groupByType) {
            return groupStatsByType(featureStats);
        }
        return featureStats;
    };

    const getDisplayWatershedStats = (): LayerStats => {
        if (!groupByType) {
            return watershedStats;
        }
        
        // Use the actual Sets for proper grouping
        const grouped = groupWatershedStatsByType(watershedSets);
        
        // Convert to counts
        const result: LayerStats = {};
        for (const [type, zoomSets] of Object.entries(grouped)) {
            result[type] = {};
            for (const [zoom, watershedSet] of Object.entries(zoomSets)) {
                result[type][zoom] = watershedSet.size;
            }
        }
        
        return result;
    };

    const renderZoomTable = (layerName: string, zoomStats: ZoomStats) => {
        const zooms = Object.keys(zoomStats).map(Number).sort((a, b) => a - b);
        if (zooms.length === 0) return null;
        
        const isStreamLayer = layerName.toLowerCase().includes('stream');
        const displayWatershedStats = getDisplayWatershedStats();
        
        const displayZooms = zooms;
        const totalCount = Object.values(zoomStats).reduce((sum, val) => sum + Number(val), 0);
        if (totalCount === 0) return null;

        // Create distribution bars
        const maxCount = Math.max(...Object.values(zoomStats).map(Number));
        
        const renderTable = (stats: ZoomStats, title: string) => {
            const total = Object.values(stats).reduce((sum, val) => sum + Number(val), 0);
            const max = Math.max(...Object.values(stats).map(Number));
            
            // Calculate cumulative counts
            let cumulative = 0;
            const cumulativeData = displayZooms.map(zoom => {
                cumulative += Number(stats[zoom]) || 0;
                return cumulative;
            });
            
            return (
                <div style={{ marginBottom: isStreamLayer ? '16px' : '0' }}>
                    {isStreamLayer && <div style={{ 
                        fontSize: '11px', 
                        color: '#4b5563', 
                        marginBottom: '8px',
                        fontWeight: '600',
                        textTransform: 'uppercase',
                        letterSpacing: '0.05em',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '6px'
                    }}>
                        <span style={{ display: 'inline-block', width: '4px', height: '4px', borderRadius: '50%', backgroundColor: '#6366f1' }}></span>
                        {title}
                    </div>}
                    <div style={{
                        backgroundColor: '#ffffff',
                        border: '1px solid #f1f5f9',
                        borderRadius: '8px',
                        overflow: 'hidden',
                        boxShadow: '0 1px 2px 0 rgba(0, 0, 0, 0.05)'
                    }}>
                        <div style={{ 
                            display: 'flex',
                            backgroundColor: '#f8fafc', 
                            padding: '8px 12px',
                            borderBottom: '1px solid #e2e8f0',
                            gap: '12px',
                            fontSize: '10px',
                            fontWeight: '700',
                            color: '#64748b',
                            textTransform: 'uppercase',
                            letterSpacing: '0.5px'
                        }}>
                            <div style={{ width: '28px', textAlign: 'right' }}>Zoom</div>
                            <div style={{ width: '85px', textAlign: 'right' }}>Count</div>
                            <div style={{ width: '45px', textAlign: 'right' }}>%</div>
                            <div style={{ width: '45px', textAlign: 'right' }}>Cum%</div>
                            <div style={{ flex: 1 }}>Distribution</div>
                        </div>
                        
                        <div style={{ maxHeight: '300px', overflowY: 'auto' }}>
                            {displayZooms.map((zoom, index) => {
                                const count = Number(stats[zoom]) || 0;
                                const percentage = total > 0 ? (count / total) * 100 : 0;
                                const cumulativePercentage = total > 0 ? (cumulativeData[index] / total) * 100 : 0;
                                const barPercentage = max > 0 ? (count / max) * 100 : 0;
                                
                                return (
                                    <div 
                                        key={zoom} 
                                        style={{ 
                                            display: 'flex',
                                            alignItems: 'center',
                                            gap: '12px',
                                            padding: '6px 12px',
                                            borderBottom: index !== displayZooms.length - 1 ? '1px solid #f8fafc' : 'none',
                                            backgroundColor: 'white',
                                            transition: 'background-color 0.15s ease'
                                        }}
                                        onMouseEnter={(e) => e.currentTarget.style.backgroundColor = '#f8fafc'}
                                        onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'white'}
                                    >
                                        <div style={{ 
                                            fontFamily: 'Menlo, Monaco, Consolas, monospace',
                                            fontSize: '11px',
                                            fontWeight: '600',
                                            color: '#6366f1',
                                            width: '28px',
                                            textAlign: 'right'
                                        }}>
                                            {zoom}
                                        </div>
                                        <div style={{ 
                                            fontFamily: 'Menlo, Monaco, Consolas, monospace',
                                            fontSize: '11px',
                                            color: '#1e293b',
                                            width: '85px',
                                            textAlign: 'right'
                                        }}>
                                            {count.toLocaleString()}
                                        </div>
                                        <div style={{ 
                                            fontFamily: 'Menlo, Monaco, Consolas, monospace',
                                            fontSize: '10px',
                                            color: '#64748b',
                                            width: '45px',
                                            textAlign: 'right'
                                        }}>
                                            {percentage.toFixed(1)}%
                                        </div>
                                        <div style={{ 
                                            fontFamily: 'Menlo, Monaco, Consolas, monospace',
                                            fontSize: '10px',
                                            color: '#94a3b8',
                                            width: '45px',
                                            textAlign: 'right'
                                        }}>
                                            {cumulativePercentage.toFixed(1)}%
                                        </div>
                                        <div style={{ 
                                            flex: 1,
                                            height: '6px',
                                            backgroundColor: '#f1f5f9',
                                            borderRadius: '3px',
                                            overflow: 'hidden',
                                            minWidth: '50px'
                                        }}>
                                            <div style={{ 
                                                width: `${barPercentage}%`,
                                                height: '100%',
                                                backgroundColor: '#818cf8',
                                                borderRadius: '3px',
                                                transition: 'width 0.5s cubic-bezier(0.4, 0, 0.2, 1)'
                                            }} />
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    </div>
                </div>
            );
        };
        
        return (
            <div style={{ 
                marginBottom: '16px',
                padding: '16px',
                backgroundColor: 'white',
                borderRadius: '12px',
                border: '1px solid #e2e8f0',
                boxShadow: '0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px -1px rgba(0, 0, 0, 0.1)'
            }}>
                <h3 style={{ 
                    fontSize: '15px', 
                    margin: '0 0 16px 0',
                    fontWeight: '700',
                    color: '#0f172a',
                    textTransform: 'capitalize',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '8px'
                }}>
                    <span style={{ fontSize: '18px' }}>
                        {layerName.includes('stream') ? '🌊' 
                        : layerName.includes('lake') ? '🏞️'
                        : layerName.includes('wetland') ? '🌿'
                        : layerName.includes('manmade') ? '🏗️'
                        : layerName.includes('boundaries') ? '📍'
                        : '💧'}
                    </span>
                    {layerName}
                </h3>
                
                {isStreamLayer && displayWatershedStats[layerName] ? (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                        {renderTable(displayWatershedStats[layerName], 'Unique Watershed Codes')}
                        {renderTable(zoomStats, 'Total Segments')}
                    </div>
                ) : (
                    renderTable(zoomStats, '')
                )}
            </div>
        );
    };

    if (!isExpanded) {
        return (
            <div style={{
                position: 'absolute',
                top: '20px',
                right: '50px',
                zIndex: 50,
            }}>
                <button
                    onClick={() => setIsExpanded(true)}
                    style={{
                        backgroundColor: '#4f46e5',
                        border: 'none',
                        borderRadius: '99px',
                        padding: '10px 20px',
                        cursor: 'pointer',
                        fontSize: '14px',
                        fontWeight: '600',
                        color: 'white',
                        boxShadow: '0 10px 15px -3px rgba(79, 70, 229, 0.4), 0 4px 6px -4px rgba(79, 70, 229, 0.2)',
                        transition: 'all 0.2s ease',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '8px'
                    }}
                    onMouseEnter={(e) => {
                        e.currentTarget.style.transform = 'translateY(-2px)';
                        e.currentTarget.style.boxShadow = '0 20px 25px -5px rgba(79, 70, 229, 0.4), 0 8px 10px -6px rgba(79, 70, 229, 0.2)';
                    }}
                    onMouseLeave={(e) => {
                        e.currentTarget.style.transform = 'translateY(0)';
                        e.currentTarget.style.boxShadow = '0 10px 15px -3px rgba(79, 70, 229, 0.4), 0 4px 6px -4px rgba(79, 70, 229, 0.2)';
                    }}
                >
                    <span style={{ fontSize: '16px' }}>📊</span>
                    <span>Layer Stats</span>
                </button>
            </div>
        );
    }

    return (
        <div 
            className="layer-stats-scroll"
            style={{
                position: 'absolute',
                top: '20px',
                right: '20px',
                bottom: '20px',
                width: '450px',
                backgroundColor: '#f8fafc',
                borderRadius: '16px',
                padding: '24px',
                zIndex: 50,
                overflowY: 'auto',
                border: '1px solid #cbd5e1',
                boxShadow: '0 25px 50px -12px rgba(0, 0, 0, 0.25)',
                fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif'
            }}
        >
            <div style={{ 
                display: 'flex', 
                justifyContent: 'space-between', 
                alignItems: 'center',
                marginBottom: '24px'
            }}>
                <div>
                    <h2 style={{ 
                        margin: 0, 
                        fontSize: '20px',
                        fontWeight: '800',
                        color: '#0f172a',
                        letterSpacing: '-0.025em'
                    }}>
                        Layer Statistics
                    </h2>
                    <p style={{ margin: '4px 0 0 0', fontSize: '13px', color: '#64748b' }}>
                        PMTiles Analysis Dashboard
                    </p>
                </div>
                
                <div style={{ display: 'flex', gap: '8px' }}>
                    <button
                        onClick={analyzePMTiles}
                        disabled={loading}
                        style={{
                            backgroundColor: loading ? '#e2e8f0' : '#eff6ff',
                            color: loading ? '#94a3b8' : '#3b82f6',
                            border: '1px solid',
                            borderColor: loading ? '#cbd5e1' : '#bfdbfe',
                            borderRadius: '8px',
                            padding: '8px 12px',
                            cursor: loading ? 'not-allowed' : 'pointer',
                            fontSize: '13px',
                            fontWeight: '600',
                            transition: 'all 0.2s ease',
                            display: 'flex',
                            alignItems: 'center',
                            gap: '6px'
                        }}
                    >
                        {loading ? 'Analyzing...' : '⟳ Reload'}
                    </button>
                    <button
                        onClick={() => setIsExpanded(false)}
                        style={{
                            backgroundColor: 'white',
                            border: '1px solid #e2e8f0',
                            cursor: 'pointer',
                            color: '#64748b',
                            width: '34px',
                            height: '34px',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            borderRadius: '8px',
                            transition: 'all 0.2s ease',
                            fontSize: '20px'
                        }}
                        onMouseEnter={(e) => {
                            e.currentTarget.style.backgroundColor = '#fef2f2';
                            e.currentTarget.style.borderColor = '#fecaca';
                            e.currentTarget.style.color = '#ef4444';
                        }}
                        onMouseLeave={(e) => {
                            e.currentTarget.style.backgroundColor = 'white';
                            e.currentTarget.style.borderColor = '#e2e8f0';
                            e.currentTarget.style.color = '#64748b';
                        }}
                    >
                        ×
                    </button>
                </div>
            </div>

            {!loading && !error && Object.keys(featureStats).length > 0 && (
                <div style={{ 
                    marginBottom: '20px',
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '12px'
                }}>
                    <label style={{ 
                        cursor: 'pointer',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '12px',
                        padding: '12px',
                        backgroundColor: 'white',
                        borderRadius: '10px',
                        border: '1px solid #e2e8f0',
                        boxShadow: '0 1px 2px 0 rgba(0, 0, 0, 0.05)',
                        transition: 'border-color 0.2s ease'
                    }}
                    onMouseEnter={(e) => e.currentTarget.style.borderColor = '#cbd5e1'}
                    onMouseLeave={(e) => e.currentTarget.style.borderColor = '#e2e8f0'}>
                        <div style={{ position: 'relative', display: 'flex', alignItems: 'center' }}>
                            <input
                                type="checkbox"
                                checked={groupByType}
                                onChange={(e) => {
                                    setGroupByType(e.target.checked);
                                    setSelectedLayer('all');
                                }}
                                style={{
                                    width: '18px',
                                    height: '18px',
                                    cursor: 'pointer',
                                    accentColor: '#4f46e5'
                                }}
                            />
                        </div>
                        <div>
                            <span style={{ display: 'block', fontSize: '13px', fontWeight: '600', color: '#1e293b' }}>Group by Type</span>
                            <span style={{ display: 'block', fontSize: '11px', color: '#64748b' }}>Aggregate streams, lakes, wetlands</span>
                        </div>
                    </label>
                    
                    <div style={{
                        position: 'relative'
                    }}>
                        <select
                            value={selectedLayer}
                            onChange={(e) => setSelectedLayer(e.target.value)}
                            style={{
                                width: '100%',
                                padding: '12px 16px',
                                fontSize: '13px',
                                borderRadius: '10px',
                                border: '1px solid #e2e8f0',
                                backgroundColor: 'white',
                                cursor: 'pointer',
                                fontWeight: '500',
                                color: '#1e293b',
                                outline: 'none',
                                appearance: 'none',
                                boxShadow: '0 1px 2px 0 rgba(0, 0, 0, 0.05)',
                            }}
                        >
                            <option value="all">🌐 Show All Layers</option>
                            {Object.keys(getDisplayStats()).sort().map(layerName => {
                                const emoji = layerName.includes('stream') ? '🌊' 
                                    : layerName.includes('lake') ? '🏞️'
                                    : layerName.includes('wetland') ? '🌿'
                                    : layerName.includes('manmade') ? '🏗️'
                                    : layerName.includes('boundaries') ? '📍'
                                    : '💧';
                                return (
                                    <option key={layerName} value={layerName}>
                                        {emoji} {layerName}
                                    </option>
                                );
                            })}
                        </select>
                        <div style={{
                            position: 'absolute',
                            right: '16px',
                            top: '50%',
                            transform: 'translateY(-50%)',
                            pointerEvents: 'none',
                            fontSize: '10px',
                            color: '#64748b'
                        }}>▼</div>
                    </div>
                </div>
            )}

            {progress && (
                <div style={{ 
                    marginBottom: '20px',
                    padding: '16px',
                    backgroundColor: '#eff6ff',
                    borderRadius: '12px',
                    fontSize: '13px',
                    color: '#1d4ed8',
                    fontWeight: '500',
                    border: '1px solid #bfdbfe',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '12px',
                    boxShadow: '0 1px 2px 0 rgba(0, 0, 0, 0.05)'
                }}>
                    <div style={{
                        width: '18px',
                        height: '18px',
                        border: '2px solid #3b82f6',
                        borderTopColor: 'transparent',
                        borderRadius: '50%',
                        animation: 'spin 0.8s linear infinite'
                    }} />
                    {progress}
                </div>
            )}

            {loading && !progress && (
                <div style={{ 
                    textAlign: 'center', 
                    padding: '40px',
                    color: '#64748b',
                    fontSize: '14px',
                    backgroundColor: 'white',
                    borderRadius: '12px',
                    border: '1px dashed #cbd5e1'
                }}>
                    Loading statistics...
                </div>
            )}

            {error && (
                <div style={{ 
                    color: '#b91c1c', 
                    padding: '16px',
                    backgroundColor: '#fef2f2',
                    borderRadius: '12px',
                    fontSize: '13px',
                    marginBottom: '20px',
                    fontWeight: '500',
                    border: '1px solid #fecaca',
                    display: 'flex',
                    alignItems: 'start',
                    gap: '12px'
                }}>
                    <span style={{ fontSize: '16px', marginTop: '-2px' }}>⚠️</span>
                    <span>{error}</span>
                </div>
            )}

            {!loading && !error && Object.keys(featureStats).length === 0 && (
                <div style={{ 
                    color: '#64748b', 
                    fontSize: '14px',
                    textAlign: 'center',
                    padding: '40px 20px',
                    backgroundColor: 'white',
                    borderRadius: '12px',
                    border: '1px dashed #cbd5e1',
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'center',
                    gap: '12px'
                }}>
                    <span style={{ fontSize: '32px' }}>📊</span>
                    <div>
                        <div style={{ fontWeight: '600', color: '#0f172a', marginBottom: '4px' }}>No Data Loaded</div>
                        Click "Reload" to analyze PMTiles data
                    </div>
                </div>
            )}

            {!loading && !error && Object.keys(featureStats).length > 0 && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                    {(() => {
                        const displayStats = getDisplayStats();
                        const displayWatershedStats = getDisplayWatershedStats();
                        
                        return selectedLayer === 'all' 
                            ? Object.entries(displayStats)
                                .sort(([a], [b]) => a.localeCompare(b))
                                .map(([layerName, zoomStats]) => (
                                    <div key={layerName}>
                                        {renderZoomTable(layerName, zoomStats)}
                                    </div>
                                ))
                            : renderZoomTable(selectedLayer, displayStats[selectedLayer]);
                    })()}
                </div>
            )}
        </div>
    );
};

export default LayerStatsPanel;