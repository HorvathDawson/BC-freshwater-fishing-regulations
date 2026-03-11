import React, { useRef, useEffect, useState, useMemo, useCallback } from 'react';
import { X, Calendar, MapPin, FileImage, RotateCcw, Share2, Check, ChevronDown, ChevronRight, ZoomIn } from 'lucide-react';
import { Icon } from '@iconify/react';
import type { Regulation } from '../services/regulationsService';
import { regulationsService } from '../services/regulationsService';
import { 
    getIconForType, 
    getColorForType, 
    getFeatureDisplayName,
    calculateSwipeState, 
    type CollapseState,
    type FeatureInfo,
    type NameVariant
} from '../utils/featureUtils';
import { getShareableUrl, getCanonicalUrl, copyToClipboard, setActiveSectionParam } from '../utils/urlState';
import { sectionLabel } from '../utils/sectionLabel';
import SourceImageViewer from './SourceImageViewer';
import type { SearchableFeature } from './SearchBar';
import './InfoPanel.css';

/** Human-readable labels for admin scope_location keys */
const SCOPE_LOCATION_LABELS: Record<string, string> = {
    parks_bc: 'BC Parks / Ecological Reserves',
    parks_nat: 'National Parks',
    wma: 'Wildlife Management Areas',
    watersheds: 'Watersheds',
    historic_sites: 'Historic Sites',
};

interface InfoPanelProps {
    feature: FeatureInfo | null;
    onClose: () => void;
    collapseState?: CollapseState;
    onSetCollapseState: (state: CollapseState) => void;
    /** All named search entries sharing the same physical waterbody as the selected feature.
     *  Sourced from the wbgIndex in Map.tsx — references into the same objects, no copies. */
    siblingFeatures?: SearchableFeature[];
    /** Switch to a sibling section WITHOUT flying to it. Used by tab bar clicks. */
    onSwitchSection?: (feature: SearchableFeature) => void;
    /** Fly to a section bbox at a minimum zoom. Used by the "Zoom to section" button.
     *  minZoom ensures the tile layer is visible at the destination zoom level. */
    onFlyToSection?: (bbox: [number, number, number, number], minZoom: number) => void;
};

/** Map restriction_type to CSS class for colored pills */
const getRestrictionClass = (type: string): string => {
    const normalized = type.toLowerCase().replace(/[_ ]/g, '-');
    const classMap: Record<string, string> = {
        'closed': 'reg-closed',
        'closure': 'reg-closed',
        'catch-and-release': 'reg-catch-and-release',
        'quota': 'reg-quota',
        'annual-quota': 'reg-quota',
        'possession-quota': 'reg-quota',
        'harvest': 'reg-quota',
        'gear-restriction': 'reg-gear',
        'bait-restriction': 'reg-gear',
        'vessel-restriction': 'reg-gear',
        'notice': 'reg-notice',
        'note': 'reg-notice',
        'advisory---indigenous-territory': 'reg-advisory-indigenous',
    };
    return classMap[normalized] || '';
};

/** Filter categories - groups similar restriction types */
const FILTER_CATEGORIES: Record<string, { label: string; types: string[] }> = {
    closures: { label: 'Closures', types: ['closed', 'closure'] },
    quotas: { label: 'Quotas', types: ['quota', 'annual quota', 'possession quota', 'harvest'] },
    gear: { label: 'Gear', types: ['gear restriction', 'bait restriction', 'vessel restriction'] },
    catchRelease: { label: 'Catch & Release', types: ['catch and release'] },
    notices: { label: 'Notices', types: ['notice', 'note', 'advisory - indigenous territory'] },
};

/** Get category key for a restriction type */
const getFilterCategory = (type: string): string | null => {
    const normalized = type.toLowerCase().replace(/_/g, ' ');
    for (const [key, { types }] of Object.entries(FILTER_CATEGORIES)) {
        if (types.includes(normalized)) return key;
    }
    return null;
};

const InfoPanel = ({ feature, onClose, collapseState = 'expanded', onSetCollapseState, siblingFeatures = [], onSwitchSection, onFlyToSection }: InfoPanelProps) => {
    const touchStartY = useRef<number>(0);
    const touchStartTime = useRef<number>(0);
    // Set to true by tab clicks BEFORE calling onSwitchSection so the feature-change
    // effect below knows to skip overwriting activeFgid (the tab click already set it).
    const tabSwitchRef = useRef(false);
    const [regulations, setRegulations] = useState<Regulation[]>([]);
    const [loadingRegs, setLoadingRegs] = useState(false);
    const [sourceImage, setSourceImage] = useState<{ src: string; name: string } | null>(null);
    const [activeFilter, setActiveFilter] = useState<string>('');
    const [copied, setCopied] = useState(false);
    const [expandedExclusions, setExpandedExclusions] = useState<Set<number>>(new Set());

    // Which section tab is active — local display state, not selection state.
    // Updated by: (a) external map-click on any segment of the same river,
    // (b) tab click, (c) arrow-key nav. Tab clicks set tabSwitchRef so the
    // feature-change effect below doesn't override the already-correct local state.
    const [activeFgid, setActiveFgid] = useState<string | undefined>(undefined);
    useEffect(() => {
        // If this feature change was caused by a tab click (tabSwitchRef = true),
        // the onClick handler already set activeFgid to the correct value — skip.
        // If it came from an external map click (same or different river), sync
        // activeFgid to whatever segment the map selected.
        if (tabSwitchRef.current) {
            tabSwitchRef.current = false;
            return;
        }
        setActiveFgid(feature?.properties.frontend_group_id as string | undefined);
    // Depends on the full feature object so map-clicks on different segments of
    // the SAME river (same wbg) still update the active tab correctly.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [feature]);

    // Sort sibling sections: southernmost first (bbox[1] asc), null-bbox last, then by fgid.
    // This order determines the A/B/C label assignment — must match the map badge layer.
    const sortedSiblings = useMemo(() => {
        if (siblingFeatures.length <= 1) return siblingFeatures;
        return [...siblingFeatures].sort((a, b) => {
            const ab = (a.regulation_segments?.[0]?.bbox ?? a.bbox) as [number, number, number, number] | undefined;
            const bb = (b.regulation_segments?.[0]?.bbox ?? b.bbox) as [number, number, number, number] | undefined;
            const aLat = Array.isArray(ab) && ab.length === 4 ? ab[1] : null;
            const bLat = Array.isArray(bb) && bb.length === 4 ? bb[1] : null;
            if (aLat !== null && bLat !== null) return aLat - bLat;
            if (aLat !== null) return -1;
            if (bLat !== null) return 1;
            const aFgid = a.regulation_segments?.[0]?.frontend_group_id ?? (a as any).id ?? '';
            const bFgid = b.regulation_segments?.[0]?.frontend_group_id ?? (b as any).id ?? '';
            return String(aFgid).localeCompare(String(bFgid));
        });
    }, [siblingFeatures]);

    // Handle share button click
    const handleShare = async (e: React.MouseEvent) => {
        e.stopPropagation();
        const props = feature?.properties;
        // Named waterbodies use the stable canonical /waterbody/<wbg>/ URL.
        // Unnamed/legacy features fall back to ?f=<featureId>.
        // typeof guard required: properties record can hold number|null|boolean.
        const wbg = typeof props?.waterbody_group === 'string' ? props.waterbody_group : undefined;
        const featureId = props?.frontend_group_id || props?.group_id ||
                          (props?.waterbody_key ? String(props.waterbody_key) : '');
        // Validate before generating URL — never generate then discard.
        if (!wbg && !featureId) {
            console.warn('Cannot share: feature missing waterbody_group and all IDs');
            return;
        }
        const url = wbg ? getCanonicalUrl(wbg, activeFgid) : getShareableUrl(String(featureId), activeFgid);
        const success = await copyToClipboard(url);
        if (success) {
            setCopied(true);
            setTimeout(() => setCopied(false), 2000);
        }
    };

    // Extract available filter categories from current regulations
    const availableCategories = useMemo(() => {
        const categories = new Set<string>();
        for (const reg of regulations) {
            if (reg.restriction_type) {
                const cat = getFilterCategory(reg.restriction_type);
                if (cat) categories.add(cat);
            }
        }
        return Array.from(categories);
    }, [regulations]);

    // Filter regulations based on active filter category
    const filteredRegulations = useMemo(() => {
        if (!activeFilter) return regulations;
        const category = FILTER_CATEGORIES[activeFilter];
        if (!category) return regulations;
        return regulations.filter(reg => {
            const t = (reg.restriction_type || '').toLowerCase().replace(/_/g, ' ');
            return category.types.includes(t);
        });
    }, [regulations, activeFilter]);

    const resetFilters = () => setActiveFilter('');

    // Fetch regulations when feature changes
    useEffect(() => {
        if (!feature?.properties.regulation_ids) {
            setRegulations([]);
            setActiveFilter(''); // Reset filters
            return;
        }

        setLoadingRegs(true);
        setActiveFilter(''); // Reset filters on new feature
        regulationsService
            .getRegulations(feature.properties.regulation_ids as string)
            .then(setRegulations)
            .catch(err => {
                console.error('Failed to load regulations:', err);
                setRegulations([]);
            })
            .finally(() => setLoadingRegs(false));
    }, [feature?.properties.regulation_ids]);

    const handleTouchStart = (e: React.TouchEvent) => {
        touchStartY.current = e.touches[0].clientY;
        touchStartTime.current = Date.now();
    };

    const handleTouchEnd = (e: React.TouchEvent) => {
        const result = calculateSwipeState(
            touchStartY.current,
            e.changedTouches[0].clientY,
            touchStartTime.current,
            Date.now(),
            collapseState
        );
        if (result.handled) {
            onSetCollapseState(result.newState);
        }
    };

    const renderContent = () => {
        if (!feature) return null;
        const props = feature.properties;

        // Tidal boundary: simple informational panel
        if (props._tidal) {
            return (
                <>
                    <div
                        className="panel-header"
                        onClick={() => onSetCollapseState(collapseState === 'expanded' ? 'partial' : 'expanded')}
                        onTouchStart={handleTouchStart}
                        onTouchEnd={handleTouchEnd}
                    >
                        <div className="mobile-handle-bar" />
                        <div className="header-row">
                            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                <div className="type-icon" style={{ backgroundColor: '#6B7280' }}>
                                    <Icon icon="mdi:waves" width={32} height={32} color="white" />
                                </div>
                                <span className="type-tag">TIDAL WATERS</span>
                            </div>
                            <button onClick={(e) => { e.stopPropagation(); onClose(); }} className="square-btn" aria-label="Close panel">
                                <X size={20} />
                            </button>
                        </div>
                        <div className="title-group">
                            <h1 className="title">Tidal Waters</h1>
                        </div>
                    </div>
                    <div className="panel-content">
                        <div className="data-section">
                            <p style={{ margin: '0 0 12px', lineHeight: 1.5 }}>
                                This area falls within tidal waters. Freshwater fishing regulations do not apply here.
                                Please refer to DFO tidal water regulations.
                            </p>
                            {props._tidal_url && (
                                <a
                                    href={props._tidal_url as string}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="reg-source-img-btn"
                                    style={{ display: 'inline-flex', alignItems: 'center', gap: '4px', fontSize: '14px' }}
                                >
                                    DFO Tidal Regulations &rarr;
                                </a>
                            )}
                        </div>
                    </div>
                </>
            );
        }

        // Build deduplicated aliases from name_variants
        const nameVariantsRaw: (NameVariant | string)[] = Array.isArray(props.name_variants) ? props.name_variants : [];
        const title = getFeatureDisplayName(props);
        const typeLabel = feature.type.toUpperCase();
        const seen = new Set<string>();
        seen.add((title as string).toLowerCase());
        const aliases: NameVariant[] = [];
        for (const nv of nameVariantsRaw) {
            // Handle both old string format and new NameVariant format
            const name = typeof nv === 'string' ? nv : nv.name;
            const fromTributary = typeof nv === 'string' ? false : nv.from_tributary;
            const lower = name.toLowerCase();
            if (!seen.has(lower)) {
                seen.add(lower);
                aliases.push({ name, from_tributary: fromTributary });
            }
        }
        const hasAliases = aliases.length > 0;

        return (
            <>
                <div 
                    className="panel-header" 
                    onClick={() => {
                        // Toggle between expanded and partial
                        onSetCollapseState(collapseState === 'expanded' ? 'partial' : 'expanded');
                    }}
                    onTouchStart={handleTouchStart}
                    onTouchEnd={handleTouchEnd}
                >
                    <div className="mobile-handle-bar" />
                    
                    <div className="header-row">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                            <div className="type-icon" style={{ backgroundColor: getColorForType(feature.type) }}>
                                <Icon icon={getIconForType(feature.type)} width={32} height={32} color="white" />
                            </div>
                            <span className="type-tag">{typeLabel}</span>
                        </div>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '4px', position: 'relative' }}>
                            <button 
                                onClick={handleShare} 
                                className="square-btn" 
                                title={copied ? "Link copied!" : "Copy link to share"}
                            >
                                {copied ? <Check size={20} /> : <Share2 size={20} />}
                            </button>
                            {copied && <span className="copy-toast">Link copied!</span>}
                            <button onClick={(e) => { e.stopPropagation(); onClose(); }} className="square-btn" aria-label="Close panel">
                                <X size={20} />
                            </button>
                        </div>
                    </div>
                    <div className="title-group">
                        <h1 className="title">{title}</h1>
                        {hasAliases && (() => {
                            const tributaryAliases = aliases.filter(a => a.from_tributary);
                            const regularAliases = aliases.filter(a => !a.from_tributary);

                            // Format a list with Oxford comma: "A", "A and B", "A, B, and C"
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
                                <div className="regulation-subtitle alias-list">
                                    Also known as:{' '}
                                    {parts.length === 1 ? (
                                        <span>{parts[0]}</span>
                                    ) : (
                                        <ul>
                                            {parts.map((part, idx) => (
                                                <li key={idx}>{part}</li>
                                            ))}
                                        </ul>
                                    )}
                                </div>
                            );
                        })()}
                    </div>
                </div>

                {/* Section tab bar — only rendered for multi-section waterbodies.
                    Sticky between the header and the scrolling content area.
                    Single-section waterbodies render without any additional chrome. */}
                {sortedSiblings.length > 1 && (() => {
                    const waterbodyName = typeof title === 'string' ? title : String(title);
                    return (
                        <div className="section-tab-bar-wrapper">
                            <div
                                className="section-tab-bar"
                                role="tablist"
                                aria-label={`Regulation sections for ${waterbodyName}`}
                            >
                            <span className="section-tab-bar__label" aria-hidden="true">
                                Sections
                            </span>
                            {sortedSiblings.map((sf, index) => {
                                const sfFgid = sf.regulation_segments?.[0]?.frontend_group_id ?? sf.id ?? '';
                                const isActive = activeFgid === sfFgid;
                                const label = sectionLabel(index);
                                return (
                                    <button
                                        key={sfFgid}
                                        role="tab"
                                        aria-selected={isActive}
                                        aria-controls="section-panel"
                                        id={`section-tab-${sfFgid}`}
                                        title={`Section ${label}`}
                                        aria-label={`Section ${label} of ${sortedSiblings.length}`}
                                        className={`section-tab${isActive ? ' active' : ''}`}
                                        onClick={() => {
                                            tabSwitchRef.current = true;
                                            setActiveFgid(sfFgid);
                                            setActiveSectionParam(sfFgid);
                                            onSwitchSection?.(sf);
                                        }}
                                        onKeyDown={(e) => {
                                            if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
                                                e.preventDefault();
                                                const dir = e.key === 'ArrowRight' ? 1 : -1;
                                                const nextIdx = (index + dir + sortedSiblings.length) % sortedSiblings.length;
                                                const next = sortedSiblings[nextIdx];
                                                const nextFgid = next.regulation_segments?.[0]?.frontend_group_id ?? next.id ?? '';
                                                tabSwitchRef.current = true;
                                                setActiveFgid(nextFgid);
                                                setActiveSectionParam(nextFgid);
                                                onSwitchSection?.(next);
                                                document.getElementById(`section-tab-${CSS.escape(nextFgid)}`)?.focus();
                                            }
                                        }}
                                    >
                                        {label}
                                    </button>
                                );
                            })}
                            </div>
                        </div>
                    );
                })()}

<div
                    className="panel-content"
                    role={sortedSiblings.length > 1 ? 'tabpanel' : undefined}
                    id={sortedSiblings.length > 1 ? 'section-panel' : undefined}
                    aria-labelledby={sortedSiblings.length > 1 ? `section-tab-${activeFgid}` : undefined}
                    tabIndex={sortedSiblings.length > 1 ? 0 : undefined}
                >
                    {/* Zoom to section button — shown when feature has a valid bbox
                        and there is more than one section (single-section panels have
                        no concept of "zoom to this section" in isolation). */}
                    {sortedSiblings.length > 1 && Array.isArray(feature.bbox) && feature.bbox.length === 4 && (() => {
                        const bbox = feature.bbox as [number, number, number, number];
                        // Use the segment's own minzoom so the tile layer is guaranteed visible.
                        // Fall back to the parent feature minzoom, then 10 as a safe default.
                        const minZoom = (feature.minzoom as number | undefined) ?? 10;
                        const activeLabel = sectionLabel(sortedSiblings.findIndex(sf =>
                            (sf.regulation_segments?.[0]?.frontend_group_id ?? sf.id) === activeFgid
                        ));
                        return (
                            <button
                                className="zoom-to-section-btn"
                                onClick={() => onFlyToSection?.(bbox, minZoom)}
                                aria-label={`Zoom to Section ${activeLabel}`}
                            >
                                <ZoomIn size={13} strokeWidth={2} />
                                <span>Zoom to Section {activeLabel}</span>
                            </button>
                        );
                    })()}
                    {/* REGULATIONS SECTION */}
                    <div className="data-section">
                        <div className="section-header-row">
                            <h3>REGULATIONS</h3>
                            
                            {/* Compact filter dropdown */}
                            {!loadingRegs && availableCategories.length > 1 && (
                                <div className="reg-filter-compact">
                                    <select 
                                        className="reg-filter-select"
                                        value={activeFilter}
                                        onChange={(e) => setActiveFilter(e.target.value)}
                                    >
                                        <option value="">All</option>
                                        {availableCategories.map(cat => (
                                            <option key={cat} value={cat}>
                                                {FILTER_CATEGORIES[cat]?.label || cat}
                                            </option>
                                        ))}
                                    </select>
                                    {activeFilter && (
                                        <button 
                                            className="reg-filter-reset-icon" 
                                            onClick={resetFilters} 
                                            title="Clear filter"
                                        >
                                            <RotateCcw size={12} />
                                        </button>
                                    )}
                                </div>
                            )}
                        </div>
                        
                        {loadingRegs && (
                            <div className="loading-regulations">
                                Loading regulations...
                            </div>
                        )}

                        {!loadingRegs && !props.regulation_ids && (
                            <div className="no-regulations">
                                No specific regulations (standard regional rules apply)
                            </div>
                        )}

                        {!loadingRegs && props.regulation_ids && regulations.length === 0 && (
                            <div className="regulation-error">
                                Failed to load regulation details
                            </div>
                        )}

                        {!loadingRegs && (() => {
                            // Show message if filters hide all results
                            if (activeFilter && filteredRegulations.length === 0) {
                                return (
                                    <div className="no-regulations">
                                        No regulations match selected filter
                                    </div>
                                );
                            }

                            // Admin zone map passed from Map click handler
                            // Maps regulation_id → list of admin zone names at click point
                            const adminZones = (props._adminZones || {}) as Record<string, string[]>;

                            // --- helpers for dates rendering ---
                            const formatDates = (dates: Regulation['dates']): string | null => {
                                if (!dates || dates === 'null') return null;
                                if (Array.isArray(dates)) {
                                    const valid = dates.filter(d => d && d !== 'null');
                                    return valid.length > 0 ? valid.join(', ') : null;
                                }
                                if (typeof dates === 'string' && dates.trim()) return dates;
                                if (typeof dates === 'object' && 'period' in dates && dates.period) return dates.period;
                                return null;
                            };

                            // Group regulations by source category + region
                            const groupedRegulations = filteredRegulations.reduce((groups, reg) => {
                                let groupKey: string;
                                let groupLabel: string;
                                let groupSubtitle: string = '';

                                if (reg.source === 'zone') {
                                    // Zone regulations: reg.region already includes "REGION X - Name".
                                    // Include zone_ids in key so different zones stay separate.
                                    const zoneKey = reg.zone_ids?.length ? reg.zone_ids.sort().join(',') : '';
                                    const regionName = reg.region || 'Zone Regulations';
                                    groupKey = `zone|${zoneKey}|${regionName}`;
                                    groupLabel = regionName;
                                    groupSubtitle = '';
                                } else if (reg.source === 'provincial') {
                                    // Provincial / admin-boundary regulations
                                    const isIndigenousAdvisory = (reg.restriction_type || '').toLowerCase().includes('indigenous territory');

                                    if (isIndigenousAdvisory) {
                                        // Combine all indigenous territory advisories into one group.
                                        // Each per-instance reg has a different regulation_id but
                                        // identical restriction text — collect territory names from
                                        // the admin zone lookup and list them all under one header.
                                        groupKey = 'prov|indigenous_territory_advisory';
                                        const zoneNames = adminZones[reg.regulation_id];
                                        const newNames = (zoneNames && zoneNames.length > 0) ? zoneNames : [];
                                        if (groups[groupKey]) {
                                            // Append new territory names to existing label
                                            const existing = new Set(groups[groupKey]._territoryNames || []);
                                            for (const n of newNames) existing.add(n);
                                            groups[groupKey]._territoryNames = [...existing];
                                            groups[groupKey].label = groups[groupKey]._territoryNames.join(', ') || 'Indigenous Territory';
                                            // Don't duplicate the regulation row — text is identical
                                            return groups;
                                        }
                                        groupLabel = newNames.join(', ') || 'Indigenous Territory';
                                    } else if (reg.scope_location) {
                                        const zoneNames = adminZones[reg.regulation_id];
                                        if (zoneNames && zoneNames.length > 0) {
                                            groupLabel = zoneNames.join(', ');
                                        } else {
                                            groupLabel = SCOPE_LOCATION_LABELS[reg.scope_location] || reg.scope_location;
                                        }
                                    } else {
                                        groupLabel = 'Provincial Regulations';
                                    }
                                    if (!isIndigenousAdvisory) {
                                        groupKey = `prov|${groupLabel}`;
                                    }
                                    groupSubtitle = '';
                                } else {
                                    // Synopsis regulations: group by iid (identity ID) when
                                    // available — this ensures entries that share a name+region
                                    // but differ in management_units stay separate (e.g. CRAWFORD CREEK).
                                    // Falls back to name+region for any edge case without iid.
                                    const wbName = reg.waterbody_name || 'Regulations';
                                    const regionName = reg.region || '';
                                    if (reg.iid) {
                                        groupKey = `syn|${reg.iid}`;
                                    } else {
                                        console.warn(`Synopsis regulation "${reg.regulation_id}" has no iid — falling back to name+region grouping`);
                                        groupKey = `syn|${regionName}|${wbName}`;
                                    }
                                    groupLabel = wbName;
                                    groupSubtitle = regionName;
                                }

                                if (!groups[groupKey]) {
                                    groups[groupKey] = {
                                        label: groupLabel,
                                        subtitle: groupSubtitle,
                                        source: reg.source || 'synopsis',
                                        isTributary: false,
                                        exclusions: null,
                                        regulations: [],
                                        _territoryNames: [],
                                    };
                                    // Seed territory names for indigenous advisory groups
                                    if (reg.source === 'provincial' && (reg.restriction_type || '').toLowerCase().includes('indigenous territory')) {
                                        const zoneNames = adminZones[reg.regulation_id];
                                        groups[groupKey]._territoryNames = zoneNames && zoneNames.length > 0 ? [...zoneNames] : [];
                                    }
                                }
                                // Capture exclusions once per group (identity-level data, same for all rules)
                                if (!groups[groupKey].exclusions && reg.exclusions && reg.exclusions.length > 0) {
                                    groups[groupKey].exclusions = reg.exclusions;
                                }
                                groups[groupKey].regulations.push(reg);
                                return groups;
                            }, {} as Record<string, { label: string; subtitle: string; source: string; isTributary: boolean; exclusions: Regulation['exclusions']; regulations: Regulation[]; _territoryNames?: string[] }>);

                            // Sort groups: provincial with a "closed" reg or indigenous
                            // territory advisory floats to the top so users immediately
                            // see closures (e.g. Ecological Reserves) and indigenous
                            // land notices.  Otherwise: synopsis → zone → provincial.
                            // Within synopsis, direct-match groups appear before tributary groups.
                            const hasHighPriorityProvReg = (g: { regulations: Regulation[] }) =>
                                g.regulations.some(r => {
                                    const t = (r.restriction_type || '').toLowerCase();
                                    return t === 'closed' || t === 'closure'
                                        || t.includes('indigenous territory');
                                });

                            // Build set of this feature's own names (non-tributary)
                            const ownNames = new Set<string>();
                            ownNames.add((title as string).toLowerCase());
                            for (const nv of nameVariantsRaw) {
                                const name = typeof nv === 'string' ? nv : nv.name;
                                const isTrib = typeof nv === 'string' ? false : nv.from_tributary;
                                if (!isTrib) {
                                    ownNames.add(name.toLowerCase());
                                }
                            }

                            // A synopsis group is tributary-inherited if its label
                            // (waterbody_name from the regulation) doesn't match any
                            // of the feature's own names.
                            const isTributaryGroup = (g: { label: string; source: string }) => {
                                if (g.source !== 'synopsis') return false;
                                return !ownNames.has(g.label.toLowerCase());
                            };

                            // Tag synopsis groups that are tributary-inherited
                            for (const g of Object.values(groupedRegulations)) {
                                g.isTributary = isTributaryGroup(g);
                            }

                            const sourceOrder: Record<string, number> = { synopsis: 1, zone: 2, provincial: 3 };
                            const sortedGroups = Object.values(groupedRegulations).sort((a, b) => {
                                const aOrder = (a.source === 'provincial' && hasHighPriorityProvReg(a)) ? 0 : (sourceOrder[a.source] ?? 9);
                                const bOrder = (b.source === 'provincial' && hasHighPriorityProvReg(b)) ? 0 : (sourceOrder[b.source] ?? 9);
                                if (aOrder !== bOrder) return aOrder - bOrder;
                                // Within same source tier, push tributary synopsis groups after direct ones
                                const aTrib = isTributaryGroup(a) ? 1 : 0;
                                const bTrib = isTributaryGroup(b) ? 1 : 0;
                                return aTrib - bTrib;
                            });

                            // Consistent sort within each group by restriction_type
                            const typeOrder: Record<string, number> = {
                                'closed': 0, 'closure': 1,
                                'catch and release': 2, 'catch_and_release': 2,
                                'bait restriction': 3, 'bait_restriction': 3,
                                'gear restriction': 4, 'gear_restriction': 4,
                                'quota': 5, 'annual quota': 6, 'annual_quota': 6,
                                'possession quota': 7, 'possession_quota': 7,
                                'harvest': 8, 'vessel_restriction': 9, 'vessel restriction': 9,
                                'notice': 10, 'note': 11,
                            };
                            for (const g of sortedGroups) {
                                g.regulations.sort((a, b) => {
                                    const aKey = (a.restriction_type || '').toLowerCase();
                                    const bKey = (b.restriction_type || '').toLowerCase();
                                    return (typeOrder[aKey] ?? 99) - (typeOrder[bKey] ?? 99);
                                });
                            }

                            return sortedGroups.map((group, groupIdx) => (
                                <div key={groupIdx} className="regulation-group">
                                    {/* Group Header */}
                                    <div className={`regulation-group-header ${group.source === 'zone' ? 'zone-header' : ''} ${group.source === 'provincial' ? 'provincial-header' : ''} ${group.isTributary ? 'tributary-header' : ''}`}>
                                        {group.source === 'zone' && <span className="header-badge zone-badge">Zone</span>}
                                        {group.source === 'provincial' && <span className="header-badge provincial-badge">Provincial</span>}
                                        {group.isTributary && <span className="header-badge tributary-badge">Tributary of</span>}
                                        {group.label}
                                        {group.subtitle && <div className="regulation-group-subtitle">{group.subtitle}</div>}

                                        {/* Exclusions toggle inside identity header */}
                                        {group.exclusions && group.exclusions.length > 0 && (
                                            <div className="exclusions-section">
                                                <button
                                                    className="exclusions-toggle"
                                                    onClick={() => setExpandedExclusions(prev => {
                                                        const next = new Set(prev);
                                                        if (next.has(groupIdx)) next.delete(groupIdx);
                                                        else next.add(groupIdx);
                                                        return next;
                                                    })}
                                                    aria-expanded={expandedExclusions.has(groupIdx)}
                                                    aria-label={`${expandedExclusions.has(groupIdx) ? 'Hide' : 'Show'} exceptions`}
                                                >
                                                    {expandedExclusions.has(groupIdx)
                                                        ? <ChevronDown size={12} strokeWidth={2} />
                                                        : <ChevronRight size={12} strokeWidth={2} />
                                                    }
                                                    Exceptions ({group.exclusions.length})
                                                </button>
                                                {expandedExclusions.has(groupIdx) && (
                                                    <ul className="exclusions-list">
                                                        {group.exclusions.map((exc, excIdx) => {
                                                            const detail = exc.direction
                                                                ? exc.direction.toLowerCase().replace(/_/g, ' ') + (exc.landmark_verbatim ? ` of ${exc.landmark_verbatim}` : '')
                                                                : null;
                                                            const tribs = exc.includes_tributaries;
                                                            return (
                                                                <li key={excIdx} className="exclusion-item">
                                                                    <span className="exclusion-name">{exc.lookup_name}</span>
                                                                    {(detail || tribs) && (
                                                                        <span className="exclusion-meta">
                                                                            {' — '}
                                                                            {detail}
                                                                            {detail && tribs && ' · '}
                                                                            {tribs && <span className="exclusion-trib-tag">incl. tribs</span>}
                                                                        </span>
                                                                    )}
                                                                </li>
                                                            );
                                                        })}
                                                    </ul>
                                                )}
                                            </div>
                                        )}
                                    </div>

                                    {/* Compact regulation rows */}
                                    {group.regulations.map((reg, idx) => {
                                        const dateStr = formatDates(reg.dates);
                                        // Feature type labels for zone/provincial regs
                                        const ftLabels = reg.feature_types && reg.feature_types.length > 0
                                            ? reg.feature_types.map(ft => ft.replace(/s$/, '').replace(/^manmade$/, 'reservoir'))
                                            : null;
                                        const hasMeta = !!(dateStr || reg.scope_location || ftLabels);
                                        return (
                                            <div key={idx} className="regulation-row">
                                                <div className="reg-row-main">
                                                    {reg.restriction_type && (
                                                        <span className={`reg-type-pill ${getRestrictionClass(reg.restriction_type)}`}>
                                                            {reg.restriction_type.replace(/_/g, ' ')}
                                                        </span>
                                                    )}
                                                    {reg.restriction_details && (
                                                        <span className="reg-detail-text">{reg.restriction_details}</span>
                                                    )}
                                                </div>
                                                {hasMeta && (
                                                    <div className="reg-row-meta">
                                                        {dateStr && (
                                                            <span className="reg-date"><Calendar size={11} strokeWidth={2} /> {dateStr}</span>
                                                        )}
                                                        {reg.scope_location && reg.source === 'provincial' && (
                                                            <span className="reg-scope-tag">{SCOPE_LOCATION_LABELS[reg.scope_location] || reg.scope_location}</span>
                                                        )}
                                                        {reg.scope_location && reg.source !== 'provincial' && (
                                                            <span className="reg-location-text"><MapPin size={10} strokeWidth={2} /> {reg.scope_location}</span>
                                                        )}
                                                        {ftLabels && (
                                                            <span className="reg-applies-to">{ftLabels.join(' · ')}</span>
                                                        )}
                                                    </div>
                                                )}
                                                {(reg.rule_text || reg.source_image) && (
                                                <div className="reg-row-actions">
                                                    {reg.rule_text && (
                                                        <details className="reg-text-expand">
                                                            <summary>Official text</summary>
                                                            <div className="reg-text-body">{reg.rule_text}</div>
                                                        </details>
                                                    )}
                                                    {reg.source_image && (
                                                        <button
                                                            className="reg-source-img-btn"
                                                            title="View source image from synopsis"
                                                            onClick={() => setSourceImage({ src: `/data/row_images/${reg.source_image}`, name: reg.waterbody_name || 'Source' })}
                                                        >
                                                            <FileImage size={12} strokeWidth={2} />
                                                            <span>Source</span>
                                                        </button>
                                                    )}
                                                </div>
                                                )}
                                            </div>
                                        );
                                    })}
                                </div>
                            ));
                        })()}
                    </div>

                    {/* LEGACY DATA SECTION (for backwards compatibility) */}
                    {(props.species_limit || props.season_dates || props.gear_restriction) && (
                        <div className="data-section">
                            <h3>LEGACY DATA (PLACEHOLDER)</h3>
                            <div className="data-row">
                                <span className="label">Limit</span>
                                <span className="value">{props.species_limit || "Regional Standard"}</span>
                            </div>
                            <div className="data-row">
                                <span className="label">Season</span>
                                <span className="value">{props.season_dates || "Open All Year"}</span>
                            </div>
                            <div className="data-row">
                                <span className="label">Gear</span>
                                <span className="value">{props.gear_restriction || "No Restrictions"}</span>
                            </div>
                        </div>
                    )}

                    {props.regulation_text_snippet && (
                        <div className="raw-text-block">
                            <div className="block-label">OFFICIAL TEXT</div>
                            <p>"{props.regulation_text_snippet}"</p>
                        </div>
                    )}
                    
                    <div className="data-section">
                        <h3>DETAILS</h3>
                        {(() => {
                            const zoneList = props.zones ? (props.zones as string).split(',') : [];
                            const nameList = props.region_name ? (props.region_name as string).split(',') : [];
                            // Pair zone IDs with names — both sorted independently,
                            // so positional pairing works only when lengths match.
                            const regionTags = zoneList.map((z: string, i: number) => ({
                                id: z.trim(),
                                name: nameList[i]?.trim() || null,
                            }));
                            const muList = props.mgmt_units ? (props.mgmt_units as string).split(',').map((s: string) => s.trim()) : [];
                            return (
                                <>
                                    <div className="region-tags">
                                        <span className="label">REGIONS</span>
                                        <div className="tags-row">
                                            {regionTags.length > 0 ? regionTags.map((r: {id: string; name: string | null}) => (
                                                <span key={r.id} className="region-tag">
                                                    {r.id}{r.name ? ` — ${r.name}` : ''}
                                                </span>
                                            )) : <span className="value">—</span>}
                                        </div>
                                    </div>
                                    {muList.length > 0 && (
                                        <details className="mu-details">
                                            <summary>MGMT UNITS ({muList.length})</summary>
                                            <div className="mu-tags-row">
                                                {muList.map((mu: string) => (
                                                    <span key={mu} className="mu-tag">{mu}</span>
                                                ))}
                                            </div>
                                        </details>
                                    )}
                                </>
                            );
                        })()}
                        {props.fwa_watershed_code && (
                            <div className="stat-box mt-2">
                                <span className="label">WATERSHED CODE</span>
                                <span className="value code">{props.fwa_watershed_code}</span>
                            </div>
                        )}
                    </div>
                </div>
            </>
        );
    };

    return (
        <>
            <aside className={`panel-desktop ${feature ? 'visible' : ''}`} aria-label="Feature details">
                {renderContent()}
            </aside>
            
            <aside className={`panel-mobile ${feature ? 'visible' : ''} ${collapseState === 'partial' ? 'partial' : ''}`} aria-label="Feature details">
                {renderContent()}
            </aside>

            {/* Source image viewer */}
            {sourceImage && (
                <SourceImageViewer
                    src={sourceImage.src}
                    name={sourceImage.name}
                    onClose={() => setSourceImage(null)}
                />
            )}
        </>
    );
};

export default InfoPanel;