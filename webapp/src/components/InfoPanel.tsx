import React, { useRef, useEffect, useState } from 'react';
import { X, Calendar } from 'lucide-react';
import { Icon } from '@iconify/react';
import type { Regulation } from '../services/regulationsService';
import { regulationsService } from '../services/regulationsService';
import './InfoPanel.css';

interface FeatureInfo {
    type: 'stream' | 'lake' | 'wetland' | 'manmade';
    properties: Record<string, any>;
    _segmentCount?: number;
}

type CollapseState = 'expanded' | 'partial' | 'collapsed';

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

interface InfoPanelProps {
    feature: FeatureInfo | null;
    onClose: () => void;
    collapseState?: CollapseState;
    onSetCollapseState: (state: CollapseState) => void;
};

const InfoPanel = ({ feature, onClose, collapseState = 'expanded', onSetCollapseState }: InfoPanelProps) => {
    const touchStartY = useRef<number>(0);
    const [regulations, setRegulations] = useState<Regulation[]>([]);
    const [loadingRegs, setLoadingRegs] = useState(false);

    // Fetch regulations when feature changes
    useEffect(() => {
        if (!feature?.properties.regulation_ids) {
            setRegulations([]);
            return;
        }

        setLoadingRegs(true);
        regulationsService
            .getRegulations(feature.properties.regulation_ids)
            .then(setRegulations)
            .catch(err => {
                console.error('Failed to load regulations:', err);
                setRegulations([]);
            })
            .finally(() => setLoadingRegs(false));
    }, [feature?.properties.regulation_ids]);

    const handleTouchStart = (e: React.TouchEvent) => {
        touchStartY.current = e.touches[0].clientY;
    };

    const handleTouchEnd = (e: React.TouchEvent) => {
        const touchEndY = e.changedTouches[0].clientY;
        const diffY = touchEndY - touchStartY.current;
        const threshold = 50; 

        if (diffY > threshold) {
            // Swiping down
            if (collapseState === 'expanded') onSetCollapseState('partial');
            else if (collapseState === 'partial') onSetCollapseState('collapsed');
        } else if (diffY < -threshold) {
            // Swiping up
            if (collapseState === 'collapsed') onSetCollapseState('partial');
            else if (collapseState === 'partial') onSetCollapseState('expanded');
        }
    };

    const renderContent = () => {
        if (!feature) return null;
        const props = feature.properties;
        
        // Handle both regulation_names (array from search) and regulation_names (string from tiles)
        const regulationNames = Array.isArray(props.regulation_names) 
            ? props.regulation_names 
            : (props.regulation_names ? props.regulation_names.split(' | ').filter(Boolean) : []);
        
        const title = props.gnis_name || props.lake_name || props.name || regulationNames[0] || 'Unnamed Waterbody';
        const typeLabel = feature.type.toUpperCase();
        const hasRegNames = regulationNames.length > 0;

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
                        <button onClick={(e) => { e.stopPropagation(); onClose(); }} className="square-btn">
                            <X size={20} />
                        </button>
                    </div>
                    <div className="title-group">
                        <h1 className="title">{title}</h1>
                        {hasRegNames && (
                            <div className="regulation-subtitle">
                                Listed as:
                                {regulationNames.length === 1 ? (
                                    <span> {regulationNames[0]}</span>
                                ) : (
                                    <ul style={{ margin: '0.25rem 0 0 1rem', padding: 0, listStyle: 'disc' }}>
                                        {regulationNames.map((name: string, idx: number) => (
                                            <li key={idx}>{name}</li>
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
                        <h3>REGULATIONS</h3>
                        
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
                            // Group regulations by waterbody_name + region combination
                            const groupedRegulations = regulations.reduce((groups, reg) => {
                                const waterbodyName = reg.waterbody_name || 'Unknown Waterbody';
                                const region = reg.region || 'Unknown Region';
                                
                                // Create a composite key for waterbody + region combination
                                const groupKey = `${waterbodyName}|||${region}`;
                                
                                if (!groups[groupKey]) {
                                    groups[groupKey] = {
                                        waterbodyName,
                                        region,
                                        regulations: []
                                    };
                                }
                                groups[groupKey].regulations.push(reg);
                                return groups;
                            }, {} as Record<string, { waterbodyName: string; region: string; regulations: Regulation[] }>);

                            return Object.values(groupedRegulations).map((group, groupIdx) => (
                                <div key={groupIdx} className="regulation-group">
                                    {/* Waterbody Name + Region Header */}
                                    <div className="regulation-group-header">
                                        {group.waterbodyName}
                                        {group.region && (
                                            <span style={{ fontWeight: 'normal', fontSize: '0.9em', opacity: 0.9 }}>
                                                {' '}- {group.region}
                                            </span>
                                        )}
                                    </div>

                                    {/* Regulations for this waterbody + region combination */}
                                    {group.regulations.map((reg, idx) => (
                                        <div key={idx} className="regulation-card">
                                            {/* Restriction Type */}
                                            {reg.restriction_type && (
                                                <div className="regulation-type">
                                                    {reg.restriction_type.replace(/_/g, ' ')}
                                                </div>
                                            )}

                                            {/* Restriction Details */}
                                            {reg.restriction_details && (
                                                <div className="regulation-details">
                                                    {reg.restriction_details}
                                                </div>
                                            )}

                                            {/* Dates */}
                                            {Array.isArray(reg.dates) && reg.dates.length > 0 && (
                                                <div className="regulation-dates">
                                                    <Calendar size={14} strokeWidth={2} />
                                                    <span>{reg.dates.join(', ')}</span>
                                                </div>
                                            )}

                                            {/* Scope Location */}
                                            {reg.scope_location && (
                                                <div className="regulation-scope">
                                                    Applies to: {reg.scope_location}
                                                </div>
                                            )}

                                            {/* Full Rule Text (Expandable) */}
                                            {reg.rule_text && (
                                                <details className="regulation-text-toggle">
                                                    <summary>
                                                        View Official Text
                                                    </summary>
                                                    <div className="regulation-text-content">
                                                        {reg.rule_text}
                                                    </div>
                                                </details>
                                            )}
                                        </div>
                                    ))}
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
                        <div className="grid-2">
                            <div className="stat-box">
                                <span className="label">ZONE</span>
                                <span className="value">{props.zones || "-"}</span>
                            </div>
                            <div className="stat-box">
                                <span className="label">MGMT UNIT</span>
                                <span className="value">{props.mgmt_units || "-"}</span>
                            </div>
                        </div>
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
            <div className={`panel-desktop ${feature ? 'visible' : ''}`}>
                {renderContent()}
            </div>
            
            <div className={`panel-mobile ${feature ? 'visible' : ''} ${collapseState === 'partial' ? 'partial' : ''} ${collapseState === 'collapsed' ? 'collapsed' : ''}`}>
                {renderContent()}
            </div>
        </>
    );
};

export default InfoPanel;