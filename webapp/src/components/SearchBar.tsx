import React, { useState, useRef, useEffect } from 'react';
import { Search, X, Eye } from 'lucide-react';
import { Icon } from '@iconify/react';
import Fuse from 'fuse.js';
import { regulationsService } from '../services/regulationsService';
import './SearchBar.css';

export interface SearchableFeature {
    id: string;
    gnis_name?: string;
    lake_name?: string;
    name?: string;
    regulation_names?: string[];  // Array of regulation names
    type: 'stream' | 'lake' | 'wetland' | 'manmade' | 'streams' | 'lakes' | 'wetlands';
    properties: Record<string, any>;
    geometry?: any;
    _segmentCount?: number;
    _groupedSegments?: any[];
    bbox?: [number, number, number, number];  // [minx, miny, maxx, maxy] for zooming
    min_zoom?: number;  // Minimum zoom level where feature is visible
}

interface SearchBarProps {
    features: SearchableFeature[];
    onSelect: (feature: SearchableFeature) => void;
    highlightedResult: SearchableFeature | null;
    onHighlight: (feature: SearchableFeature | null) => void;
    placeholder?: string;
}

const getIconForType = (type: 'stream' | 'lake' | 'wetland' | 'manmade' | 'streams' | 'lakes' | 'wetlands') => {
    const iconMap = {
        stream: 'game-icons:splashy-stream',
        streams: 'game-icons:splashy-stream',
        lake: 'game-icons:oasis',
        lakes: 'game-icons:oasis',
        wetland: 'game-icons:swamp',
        wetlands: 'game-icons:swamp',
        manmade: 'game-icons:dam'
    };
    return iconMap[type as keyof typeof iconMap] || iconMap.lake;
};

const getColorForType = (type: 'stream' | 'lake' | 'wetland' | 'manmade' | 'streams' | 'lakes' | 'wetlands') => {
    const colorMap = {
        stream: '#3b82f6',
        streams: '#3b82f6',
        lake: '#0ea5e9',
        lakes: '#0ea5e9',
        wetland: '#10b981',
        wetlands: '#10b981',
        manmade: '#a855f7'
    };
    return colorMap[type as keyof typeof colorMap] || colorMap.lake;
};

const SearchBar: React.FC<SearchBarProps> = ({ features, onSelect, highlightedResult, onHighlight, placeholder = "Search waterbodies..." }) => {
    const [query, setQuery] = useState('');
    const [results, setResults] = useState<SearchableFeature[]>([]);
    const [isOpen, setIsOpen] = useState(false);
    const [selectedIndex, setSelectedIndex] = useState(-1);
    const [isMobile, setIsMobile] = useState(window.innerWidth <= 768);
    const searchRef = useRef<HTMLDivElement>(null);
    const inputRef = useRef<HTMLInputElement>(null);

    const fuse = useRef<Fuse<SearchableFeature> | null>(null);

    // Initialize Fuse.js
    useEffect(() => {
        if (features.length === 0) return;

        fuse.current = new Fuse(features, {
            keys: [
                { name: 'gnis_name', weight: 2 },
                { name: 'lake_name', weight: 2 },
                { name: 'name', weight: 2 },
                { name: 'regulation_names', weight: 2 }  // Search across all regulation names in array
            ],
            threshold: 0.3, // Even stricter for exact word matches
            distance: 50, // Strongly prefer matches at the beginning
            minMatchCharLength: 1, // Allow single character matches
            ignoreLocation: false, // Prioritize matches at the beginning of strings
            useExtendedSearch: false,
            includeScore: true,
            shouldSort: true,
            findAllMatches: true, // Find all matches
        });
    }, [features]);

    // Handle search
    useEffect(() => {
        if (!query.trim() || query.trim().length < 1 || !fuse.current) {
            setResults([]);
            setIsOpen(false);
            setSelectedIndex(-1);
            return;
        }

        const searchResults = fuse.current.search(query, { limit: 50 });
        
        // Custom sort to boost exact prefix matches
        const queryLower = query.toLowerCase();
        const sortedResults = searchResults.sort((a, b) => {
            const aItem = a.item;
            const bItem = b.item;
            
            // Check if any name field starts with the query
            const aStartsWith = [
                aItem.gnis_name, 
                aItem.lake_name, 
                aItem.name, 
                ...(aItem.regulation_names || [])
            ].some(name => name?.toLowerCase().startsWith(queryLower));
            
            const bStartsWith = [
                bItem.gnis_name, 
                bItem.lake_name, 
                bItem.name, 
                ...(bItem.regulation_names || [])
            ].some(name => name?.toLowerCase().startsWith(queryLower));
            
            // Prioritize exact prefix matches
            if (aStartsWith && !bStartsWith) return -1;
            if (!aStartsWith && bStartsWith) return 1;
            
            // Otherwise use Fuse.js score
            return (a.score || 0) - (b.score || 0);
        });
        
        const items = sortedResults.map(result => result.item);
        setResults(items);
        setIsOpen(items.length > 0);
        setSelectedIndex(-1);
    }, [query]);

    // Handle mobile detection
    useEffect(() => {
        const handleResize = () => {
            setIsMobile(window.innerWidth <= 768);
        };
        window.addEventListener('resize', handleResize);
        return () => window.removeEventListener('resize', handleResize);
    }, []);

    // Close dropdown when clicking outside
    useEffect(() => {
        const handleClickOutside = (e: MouseEvent) => {
            if (searchRef.current && !searchRef.current.contains(e.target as Node)) {
                setIsOpen(false);
                setSelectedIndex(-1);
                onHighlight(null);
            }
        };

        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, [onHighlight]);

    // Keyboard navigation
    const handleKeyDown = (e: React.KeyboardEvent) => {
        if (!isOpen || results.length === 0) return;

        switch (e.key) {
            case 'ArrowDown':
                e.preventDefault();
                setSelectedIndex(prev => (prev < results.length - 1 ? prev + 1 : prev));
                break;
            case 'ArrowUp':
                e.preventDefault();
                setSelectedIndex(prev => (prev > 0 ? prev - 1 : -1));
                break;
            case 'Enter':
                e.preventDefault();
                if (selectedIndex >= 0 && selectedIndex < results.length) {
                    handleSelect(results[selectedIndex]);
                } else if (results.length > 0) {
                    handleSelect(results[0]);
                }
                break;
            case 'Escape':
                setIsOpen(false);
                setSelectedIndex(-1);
                inputRef.current?.blur();
                break;
        }
    };

    const handleSelect = (feature: SearchableFeature) => {
        onSelect(feature);
        onHighlight(null);
        setQuery('');
        setResults([]);
        setIsOpen(false);
        setSelectedIndex(-1);
        inputRef.current?.blur();
    };

    const getDisplayName = (feature: SearchableFeature): string => {
        const synopsisNames = regulationsService.filterOutProvincialNames(feature.regulation_names || []);
        return feature.gnis_name || feature.lake_name || feature.name || synopsisNames[0] || 'Unnamed';
    };

    const clearSearch = () => {
        setQuery('');
        setResults([]);
        setIsOpen(false);
        setSelectedIndex(-1);
        onHighlight(null);
    };

    return (
        <div className="search-bar-container" ref={searchRef}>
            <div className="search-input-wrapper">
                <Search size={16} className="search-icon" />
                <input
                    ref={inputRef}
                    type="text"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    onKeyDown={handleKeyDown}
                    onFocus={() => {
                        if (results.length > 0) setIsOpen(true);
                    }}
                    placeholder={placeholder}
                    className="search-input"
                />
                {query && (
                    <button onClick={clearSearch} className="search-clear-btn" aria-label="Clear search">
                        <X size={16} />
                    </button>
                )}
            </div>

            {isOpen && results.length > 0 && (
                <div 
                    className="search-results"
                    onMouseLeave={() => {
                        if (!isMobile) {
                            onHighlight(null);
                        }
                    }}
                >
                    {results.map((feature, idx) => {
                        const displayName = getDisplayName(feature);
                        const synopsisNames = regulationsService.filterOutProvincialNames(feature.regulation_names || []);
                        const hasRegNames = synopsisNames.length > 0;
                        const zones = feature.properties?.zones;
                        const regionName = feature.properties?.region_name;
                        const isHighlighted = highlightedResult?.id === feature.id;

                        // Build compact region display: "7A – Omineca" or "7A – Omineca +2"
                        let regionDisplay: string | null = null;
                        if (zones) {
                            const zList = zones.split(',');
                            const nList = regionName ? regionName.split(',') : [];
                            const first = zList[0]?.trim();
                            const firstName = nList[0]?.trim();
                            regionDisplay = first + (firstName ? ` – ${firstName}` : '');
                            if (zList.length > 1) regionDisplay += ` +${zList.length - 1}`;
                        }

                        return (
                            <div
                                key={feature.id}
                                className={`search-result-wrapper ${isHighlighted ? 'highlighted' : ''}`}
                                onMouseEnter={() => {
                                    // Only highlight on hover on desktop
                                    if (!isMobile) {
                                        onHighlight(feature);
                                        setSelectedIndex(idx);
                                    }
                                }}
                            >
                                <button
                                    className={`search-result-item ${idx === selectedIndex ? 'selected' : ''}`}
                                    onClick={() => {
                                        if (isMobile) {
                                            // On mobile: first tap highlights, second tap (via Eye button) selects
                                            if (!isHighlighted) {
                                                onHighlight(feature);
                                                setSelectedIndex(idx);
                                            }
                                        } else {
                                            // On desktop: directly select
                                            handleSelect(feature);
                                        }
                                    }}
                                >
                                    <div className={`icon-container ${feature.type}`} style={{ backgroundColor: getColorForType(feature.type) }}>
                                        <Icon icon={getIconForType(feature.type)} width={28} height={28} color="white" />
                                    </div>
                                    <div className="search-result-content">
                                        <div className="search-result-name">
                                            {displayName}
                                            {feature._segmentCount && feature._segmentCount > 1 && (
                                                <span className="segment-badge"> ({feature._segmentCount} segments)</span>
                                            )}
                                        </div>
                                        {hasRegNames && (
                                            <div className="search-result-subtitle">
                                                Listed as: {synopsisNames.join(' | ')}
                                            </div>
                                        )}
                                        <div className="search-result-meta">
                                            <span className="search-result-type">{feature.type}</span>
                                            {regionDisplay && (
                                                <>
                                                    <span className="search-result-separator">•</span>
                                                    <span className="search-result-zone">{regionDisplay}</span>
                                                </>
                                            )}
                                        </div>
                                    </div>
                                </button>
                                {isMobile && isHighlighted && (
                                    <button
                                        className="focus-button"
                                        onClick={() => handleSelect(feature)}
                                        aria-label="Focus on this feature"
                                    >
                                        <Eye size={16} />
                                    </button>
                                )}
                            </div>
                        );
                    })}
                </div>
            )}
        </div>
    );
};

export default SearchBar;
