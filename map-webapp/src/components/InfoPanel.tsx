import React, { useRef } from 'react';

interface FeatureInfo {
    type: 'stream' | 'lake' | 'wetland' | 'manmade';
    properties: Record<string, any>;
}

interface InfoPanelProps {
    feature: FeatureInfo | null;
    onClose: () => void;
    isCollapsed?: boolean;
    onSetCollapse: (collapsed: boolean) => void;
}

const Icons = {
    Close: () => (
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <line x1="18" y1="6" x2="6" y2="18"></line>
            <line x1="6" y1="6" x2="18" y2="18"></line>
        </svg>
    )
};

const InfoPanel = ({ feature, onClose, isCollapsed = false, onSetCollapse }: InfoPanelProps) => {
    const touchStartY = useRef<number>(0);

    const handleTouchStart = (e: React.TouchEvent) => {
        touchStartY.current = e.touches[0].clientY;
    };

    const handleTouchEnd = (e: React.TouchEvent) => {
        const touchEndY = e.changedTouches[0].clientY;
        const diffY = touchEndY - touchStartY.current;
        const threshold = 50; 

        if (diffY > threshold) onSetCollapse(true);
        else if (diffY < -threshold) onSetCollapse(false);
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
                    onClick={() => onSetCollapse(!isCollapsed)}
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
            
            <div className={`panel-mobile ${feature ? 'visible' : ''} ${isCollapsed ? 'collapsed' : ''}`}>
                {renderContent()}
            </div>

            <style>{`
                :root { --bg: #ffffff; --text: #1a1a1a; }
                
                .panel-desktop {
                    position: absolute; top: 0; right: 0; bottom: 0; width: 350px;
                    background: var(--bg); border-left: 1px solid #000;
                    transform: translateX(100%); transition: transform 0.2s cubic-bezier(0,0,0,1);
                    z-index: 1000; display: flex; flex-direction: column; box-sizing: border-box;
                }
                .panel-desktop.visible { transform: translateX(0); }
                
                .panel-mobile { display: none; }

                .panel-header { padding: 24px; border-bottom: 2px solid #000; background: #f8f8f8; cursor: pointer; touch-action: none; }
                .mobile-handle-bar { display: none; }
                .header-row { display: flex; justify-content: space-between; margin-bottom: 12px; pointer-events: none; }
                .header-row button { pointer-events: auto; }
                .type-tag { font-size: 11px; font-weight: 800; background: #000; color: #fff; padding: 4px 8px; text-transform: uppercase; }
                .square-btn { background: none; border: 1px solid transparent; cursor: pointer; padding: 4px; color: #000; }
                .square-btn:hover { background: #e5e5e5; border: 1px solid #000; }
                h1 { font-size: 20px; font-weight: 700; margin: 0 0 16px 0; line-height: 1.2; text-transform: uppercase; pointer-events: none; }
                .tag-row { display: flex; flex-wrap: wrap; gap: 8px; pointer-events: none; }
                .tag { font-size: 10px; font-weight: 600; padding: 4px 6px; border: 1px solid #ccc; background: #fff; text-transform: uppercase; }
                .tag.highlight { border-color: #2563eb; color: #2563eb; background: #eff6ff; }
                .tag.alert { border-color: #dc2626; color: #dc2626; background: #fef2f2; }
                .panel-content { padding: 24px; overflow-y: auto; flex: 1; }
                .data-section { margin-bottom: 32px; }
                h3 { font-size: 11px; font-weight: 800; letter-spacing: 0.1em; margin: 0 0 12px 0; color: #999; text-transform: uppercase; }
                .data-row { display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #eee; }
                .label { font-size: 12px; font-weight: 600; color: #666; text-transform: uppercase; }
                .value { font-size: 13px; font-weight: 500; text-align: right; }
                .raw-text-block { background: #f1f1f1; padding: 16px; margin-bottom: 32px; border: 1px solid #ddd; }
                .block-label { font-size: 9px; font-weight: 700; color: #666; margin-bottom: 8px; }
                .raw-text-block p { margin: 0; font-size: 12px; line-height: 1.5; font-family: monospace; }
                .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
                .mt-2 { margin-top: 12px; }
                .stat-box { border: 1px solid #e5e5e5; padding: 8px; display: flex; flex-direction: column; gap: 4px; }
                .value.code { font-family: monospace; }

                @media (max-width: 768px) {
                    .panel-desktop { display: none; }
                    
                    .panel-mobile {
                        display: flex;
                        flex-direction: column;
                        position: fixed;
                        bottom: 0; 
                        
                        /* FIX: Pin to edges, auto width */
                        left: 0; 
                        right: 0;
                        width: auto;
                        margin: 0;
                        
                        background: #fff;
                        border-top: 2px solid #000;
                        height: 70vh;
                        transform: translateY(100%);
                        transition: transform 0.3s cubic-bezier(0.2, 0.8, 0.2, 1);
                        z-index: 2000;
                        box-shadow: 0 -4px 20px rgba(0,0,0,0.1);
                        box-sizing: border-box;
                    }
                    
                    .panel-mobile.visible { transform: translateY(0); }
                    .panel-mobile:not(.visible) { display: none; }
                    .panel-mobile.collapsed { transform: translateY(calc(100% - 160px)); }

                    .mobile-handle-bar {
                        display: block; width: 40px; height: 4px;
                        background: #ccc; margin: 0 auto 16px auto;
                    }
                }
            `}</style>
        </>
    );
};

export default InfoPanel;