import React, { useRef } from 'react';
import './InfoPanel.css';

interface FeatureInfo {
    type: 'stream' | 'lake' | 'wetland' | 'manmade';
    properties: Record<string, any>;
    _segmentCount?: number;
}

type CollapseState = 'expanded' | 'partial' | 'collapsed';

interface InfoPanelProps {
    feature: FeatureInfo | null;
    onClose: () => void;
    collapseState?: CollapseState;
    onSetCollapseState: (state: CollapseState) => void;
}

const Icons = {
    Close: () => (
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <line x1="18" y1="6" x2="6" y2="18"></line>
            <line x1="6" y1="6" x2="18" y2="18"></line>
        </svg>
    )
};

const InfoPanel = ({ feature, onClose, collapseState = 'expanded', onSetCollapseState }: InfoPanelProps) => {
    const touchStartY = useRef<number>(0);

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
        const title = props.gnis_name || props.lake_name || props.name || 'Unnamed Waterbody';
        const typeLabel = feature.type.toUpperCase();

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
                        <span className="type-tag">{typeLabel}</span>
                        <button onClick={(e) => { e.stopPropagation(); onClose(); }} className="square-btn">
                            <Icons.Close />
                        </button>
                    </div>
                    <h1>{title}</h1>
                    {feature._segmentCount && feature._segmentCount > 1 && (
                        <div style={{ fontSize: '0.85rem', color: '#64748b', marginTop: '0.25rem' }}>
                            {feature._segmentCount} segments merged
                        </div>
                    )}
                    
                    <div className="tag-row">
                        {props.waterbody_key && <span className="tag">ID: {props.waterbody_key}</span>}
                        {props.is_stocked && <span className="tag highlight">STOCKED</span>}
                        {props.is_classified_water && <span className="tag alert">CLASSIFIED</span>}
                    </div>
                </div>

                <div className="panel-content">
                    <div className="data-section">
                        <h3>REGULATIONS</h3>
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