import React, { useState, useRef, useEffect } from 'react';
import { Search, X, Eye } from 'lucide-react';
import { Icon } from '@iconify/react';
import Fuse from 'fuse.js';
import { 
    getIconForType, 
    getColorForType, 
    getUniqueAliases,
    getFeatureDisplayName,
    isMobileViewport 
} from '../utils/featureUtils';
import './SearchBar.css';

import type { NameVariant, FeatureGeometry } from '../utils/featureUtils';

export interface RegulationSegment {
    frontend_group_id: string;
    group_id: string;
    group_ids?: string[];  // All group_ids for this regulation set (when consolidated)
    regulation_ids: string;
    display_name?: string;
    name_variants: NameVariant[];  // Names with tributary flag
    length_km: number;
    bbox?: [number, number, number, number];  // Per-segment bbox for fly-to
    waterbody_group?: string;  // BLK for streams, waterbody_key for polygons — groups all segments of the same physical waterbody
}

export interface SearchableFeature {
    id: string;
    gnis_name?: string;
    display_name?: string;
    lake_name?: string;
    name?: string;
    name_variants?: NameVariant[];  // All searchable names with tributary flag
    type: 'stream' | 'lake' | 'wetland' | 'manmade' | 'ungazetted' | 'streams' | 'lakes' | 'wetlands';
    properties: Record<string, string | number | boolean | null | undefined>;
    geometry?: FeatureGeometry;
    bbox?: [number, number, number, number];  // [minx, miny, maxx, maxy] for zooming
    min_zoom?: number;  // Minimum zoom level where feature is visible
    regulation_segments?: RegulationSegment[];  // Different regulation sections of same physical stream
    /** All fgids for this waterbody entry — set at data-load time, used for URL restoration fallback. */
    _frontend_group_ids?: string[];
}

interface SearchBarProps {
    features: SearchableFeature[];
    onSelect: (feature: SearchableFeature) => void;
    highlightedResult: SearchableFeature | null;
    onHighlight: (feature: SearchableFeature | null) => void;
    onSearchActive?: () => void;
    placeholder?: string;
}

const SearchBar: React.FC<SearchBarProps> = ({ features, onSelect, highlightedResult, onHighlight, onSearchActive, placeholder = "Search waterbodies..." }) => {
    const [query, setQuery] = useState('');
    const [results, setResults] = useState<SearchableFeature[]>([]);
    const [isOpen, setIsOpen] = useState(false);
    const [selectedIndex, setSelectedIndex] = useState(-1);
    const [isMobile, setIsMobile] = useState(isMobileViewport());
    const searchRef = useRef<HTMLDivElement>(null);
    const inputRef = useRef<HTMLInputElement>(null);

    const fuse = useRef<Fuse<SearchableFeature> | null>(null);

    // Initialize Fuse.js
    useEffect(() => {
        if (features.length === 0) return;

        fuse.current = new Fuse(features, {
            keys: [
                { name: 'display_name', weight: 3 },
                { name: 'gnis_name', weight: 2 },
                { name: 'lake_name', weight: 2 },
                { name: 'name', weight: 2 },
                { name: 'name_variants.name', weight: 2 }  // Search in name field of name_variants objects
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
                aItem.display_name,
                aItem.gnis_name, 
                aItem.lake_name, 
                aItem.name, 
                ...(aItem.name_variants || []).map(nv => typeof nv === 'string' ? nv : nv.name)
            ].some(name => name?.toLowerCase().startsWith(queryLower));
            
            const bStartsWith = [
                bItem.display_name,
                bItem.gnis_name, 
                bItem.lake_name, 
                bItem.name, 
                ...(bItem.name_variants || []).map(nv => typeof nv === 'string' ? nv : nv.name)
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
        const handleResize = () => setIsMobile(isMobileViewport());
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

    const getDisplayName = (feature: SearchableFeature): string => 
        getFeatureDisplayName(feature);

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
                <Search size={16} className="search-icon" aria-hidden="true" />
                <input
                    ref={inputRef}
                    type="text"
                    value={query}
                    onChange={(e) => { setQuery(e.target.value); onSearchActive?.(); }}
                    onKeyDown={handleKeyDown}
                    onFocus={() => {
                        if (results.length > 0) setIsOpen(true);
                        onSearchActive?.();
                    }}
                    placeholder={placeholder}
                    className="search-input"
                    role="combobox"
                    aria-expanded={isOpen && results.length > 0}
                    aria-controls="search-results-listbox"
                    aria-activedescendant={selectedIndex >= 0 ? `search-option-${selectedIndex}` : undefined}
                    aria-autocomplete="list"
                    aria-label="Search waterbodies"
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
                    id="search-results-listbox"
                    role="listbox"
                    aria-label="Search results"
                    onMouseLeave={() => {
                        if (!isMobile) {
                            onHighlight(null);
                        }
                    }}
                >
                    {results.map((feature, idx) => {
                        const displayName = getDisplayName(feature);
                        const aliases = getUniqueAliases(feature.name_variants || [], displayName);
                        const hasAliases = aliases.length > 0;
                        const zones = feature.properties?.zones;
                        const regionName = feature.properties?.region_name;
                        const isHighlighted = highlightedResult?.id === feature.id;

                        // Build compact region display: "7A – Omineca, 7B – Prince George" or "7A, 7B +1"
                        let regionDisplay: string | null = null;
                        if (zones) {
                            const zList = zones ? String(zones).split(',').map((z: string) => z.trim()) : [];
                            const nList = regionName ? String(regionName).split(',').map((n: string) => n.trim()) : [];
                            
                            if (zList.length === 1) {
                                // Single region: show "7A – Omineca"
                                regionDisplay = zList[0] + (nList[0] ? ` – ${nList[0]}` : '');
                            } else if (zList.length === 2) {
                                // Two regions: show "7A – Omineca, 7B – Prince George"
                                const first = zList[0] + (nList[0] ? ` – ${nList[0]}` : '');
                                const second = zList[1] + (nList[1] ? ` – ${nList[1]}` : '');
                                regionDisplay = `${first}, ${second}`;
                            } else {
                                // 3+ regions: show "7A, 7B +N"
                                regionDisplay = `${zList[0]}, ${zList[1]} +${zList.length - 2}`;
                            }
                        }

                        return (
                            <div
                                key={feature.id}
                                id={`search-option-${idx}`}
                                className={`search-result-wrapper ${isHighlighted ? 'highlighted' : ''}`}
                                role="option"
                                aria-selected={idx === selectedIndex}
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
                                        </div>
                                        {hasAliases && (() => {
                                            const tributaryAliases = aliases.filter(a => a.from_tributary);
                                            const regularAliases = aliases.filter(a => !a.from_tributary);
                                            const formatList = (items: string[]): string => {
                                                if (items.length === 1) return items[0];
                                                if (items.length === 2) return `${items[0]} and ${items[1]}`;
                                                return `${items.slice(0, -1).join(', ')}, and ${items[items.length - 1]}`;
                                            };
                                            const parts: string[] = [];
                                            if (tributaryAliases.length > 0) {
                                                parts.push(`Tributary of ${formatList(tributaryAliases.map(a => a.name))}`);
                                            }
                                            regularAliases.forEach(a => parts.push(a.name));
                                            return (
                                                <div className="search-result-subtitle">
                                                    Also known as: {parts.join(' · ')}
                                                </div>
                                            );
                                        })()}
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
                                {isMobile && (
                                    <button
                                        className={`focus-button ${isHighlighted ? '' : 'hidden'}`}
                                        onClick={() => handleSelect(feature)}
                                        aria-label="Focus on this feature"
                                        tabIndex={isHighlighted ? 0 : -1}
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
