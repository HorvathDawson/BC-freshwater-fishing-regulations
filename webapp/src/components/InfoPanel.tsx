import React, { useRef, useEffect, useState, useMemo } from 'react';
import { X, Calendar, MapPin, FileImage, RotateCcw, Share2, Check } from 'lucide-react';
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
import { getShareableUrl, copyToClipboard } from '../utils/urlState';
import SourceImageViewer from './SourceImageViewer';
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
    };
    return classMap[normalized] || '';
};

/** Filter categories - groups similar restriction types */
const FILTER_CATEGORIES: Record<string, { label: string; types: string[] }> = {
    closures: { label: 'Closures', types: ['closed', 'closure'] },
    quotas: { label: 'Quotas', types: ['quota', 'annual quota', 'possession quota', 'harvest'] },
    gear: { label: 'Gear', types: ['gear restriction', 'bait restriction', 'vessel restriction'] },
    catchRelease: { label: 'Catch & Release', types: ['catch and release'] },
    notices: { label: 'Notices', types: ['notice', 'note'] },
};

/** Get category key for a restriction type */
const getFilterCategory = (type: string): string | null => {
    const normalized = type.toLowerCase().replace(/_/g, ' ');
    for (const [key, { types }] of Object.entries(FILTER_CATEGORIES)) {
        if (types.includes(normalized)) return key;
    }
    return null;
};

const InfoPanel = ({ feature, onClose, collapseState = 'expanded', onSetCollapseState }: InfoPanelProps) => {
    const touchStartY = useRef<number>(0);
    const touchStartTime = useRef<number>(0);
    const [regulations, setRegulations] = useState<Regulation[]>([]);
    const [loadingRegs, setLoadingRegs] = useState(false);
    const [sourceImage, setSourceImage] = useState<{ src: string; name: string } | null>(null);
    const [activeFilter, setActiveFilter] = useState<string>('');
    const [copied, setCopied] = useState(false);

    // Handle share button click
    const handleShare = async (e: React.MouseEvent) => {
        e.stopPropagation();
        // Use frontend_group_id preferring, fallback to group_id, then waterbody_key
        const props = feature?.properties;
        const featureId = props?.frontend_group_id || props?.group_id ||
                          (props?.waterbody_key ? String(props.waterbody_key) : '');
        if (!featureId) {
            console.warn('Cannot share: feature missing all IDs');
            return;
        }
        const url = getShareableUrl(String(featureId));
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
        
        // Build deduplicated aliases from name_variants
        const nameVariantsRaw: (NameVariant | string)[] = Array.isArray(props.name_variants) ? props.name_variants : [];
        const title = getFeatureDisplayName(props, regulationsService.filterOutProvincialNames);
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
                        // Cycle through states
                        if (collapseState === 'expanded') onSetCollapseState('partial');
                        else if (collapseState === 'partial') onSetCollapseState('collapsed');
                        else onSetCollapseState('expanded');
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
                        <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                            <button 
                                onClick={handleShare} 
                                className="square-btn" 
                                title={copied ? "Link copied!" : "Copy link to share"}
                            >
                                {copied ? <Check size={20} /> : <Share2 size={20} />}
                            </button>
                            <button onClick={(e) => { e.stopPropagation(); onClose(); }} className="square-btn" aria-label="Close panel">
                                <X size={20} />
                            </button>
                        </div>
                    </div>
                    <div className="title-group">
                        <h1 className="title">{title}</h1>
                        {hasAliases && (
                            <div className="regulation-subtitle">
                                Also known as:
                                {aliases.length === 1 ? (
                                    <span> {aliases[0].from_tributary ? `Tributary of ${aliases[0].name}` : aliases[0].name}</span>
                                ) : (
                                    <ul style={{ margin: '0.25rem 0 0 1rem', padding: 0, listStyle: 'disc' }}>
                                        {aliases.map((alias: NameVariant, idx: number) => (
                                            <li key={idx}>{alias.from_tributary ? `Tributary of ${alias.name}` : alias.name}</li>
                                        ))}
                                    </ul>
                                )}
                            </div>
                        )}
                    </div>
                </div>

                <div className="panel-content">
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
                                let groupLabel: string;

                                if (reg.source === 'zone') {
                                    // Zone regulations: use the region field set by the backend
                                    groupLabel = reg.region || 'Zone Regulations';
                                } else if (reg.source === 'provincial') {
                                    // Provincial / admin-boundary regulations
                                    if (reg.scope_location) {
                                        const zoneNames = adminZones[reg.regulation_id];
                                        if (zoneNames && zoneNames.length > 0) {
                                            groupLabel = zoneNames.join(', ');
                                        } else {
                                            groupLabel = SCOPE_LOCATION_LABELS[reg.scope_location] || reg.scope_location;
                                        }
                                    } else {
                                        groupLabel = 'Provincial Regulations';
                                    }
                                } else {
                                    // Synopsis regulations: group by waterbody_name
                                    groupLabel = reg.waterbody_name || 'Regulations';
                                }

                                if (!groups[groupLabel]) {
                                    groups[groupLabel] = {
                                        label: groupLabel,
                                        source: reg.source || 'synopsis',
                                        regulations: []
                                    };
                                }
                                groups[groupLabel].regulations.push(reg);
                                return groups;
                            }, {} as Record<string, { label: string; source: string; regulations: Regulation[] }>);

                            // Sort groups: provincial with a "closed" reg floats to the
                            // top so users immediately see closures (e.g. Ecological
                            // Reserves).  Otherwise: synopsis → zone → provincial.
                            const hasClosedReg = (g: { regulations: Regulation[] }) =>
                                g.regulations.some(r => {
                                    const t = (r.restriction_type || '').toLowerCase();
                                    return t === 'closed' || t === 'closure';
                                });
                            const sourceOrder: Record<string, number> = { synopsis: 1, zone: 2, provincial: 3 };
                            const sortedGroups = Object.values(groupedRegulations).sort((a, b) => {
                                const aOrder = (a.source === 'provincial' && hasClosedReg(a)) ? 0 : (sourceOrder[a.source] ?? 9);
                                const bOrder = (b.source === 'provincial' && hasClosedReg(b)) ? 0 : (sourceOrder[b.source] ?? 9);
                                return aOrder - bOrder;
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
                                    <div className={`regulation-group-header ${group.source === 'zone' ? 'zone-header' : ''} ${group.source === 'provincial' ? 'provincial-header' : ''}`}>
                                        {group.source === 'zone' && <span className="header-badge zone-badge">Zone</span>}
                                        {group.source === 'provincial' && <span className="header-badge provincial-badge">Provincial</span>}
                                        {group.label}
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
            
            <aside className={`panel-mobile ${feature ? 'visible' : ''} ${collapseState === 'partial' ? 'partial' : ''} ${collapseState === 'collapsed' ? 'collapsed' : ''}`} aria-label="Feature details">
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