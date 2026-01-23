import React, { useRef, useLayoutEffect, useState } from 'react';

interface FeatureOption {
    type: 'stream' | 'lake' | 'wetland' | 'manmade';
    properties: Record<string, any>;
    id: string;
}

interface DisambiguationMenuProps {
    options: FeatureOption[];
    position: { x: number; y: number } | null;
    onSelect: (option: FeatureOption) => void;
    onClose: () => void;
    isCollapsed?: boolean;
    onSetCollapse: (collapsed: boolean) => void;
}

const DisambiguationMenu = ({ options, position, onSelect, onClose, isCollapsed = false, onSetCollapse }: DisambiguationMenuProps) => {
    const menuRef = useRef<HTMLDivElement>(null);
    const [menuStyle, setMenuStyle] = useState<React.CSSProperties>({
        visibility: 'hidden', // Hide initially to prevent jump
        top: 0,
        left: 0
    });

    const getLabel = (opt: FeatureOption) => opt.properties.gnis_name || opt.properties.lake_name || opt.properties.name || 'Unnamed';

    // Touch tracking for mobile swipe
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

    // Smart Positioning Logic
    useLayoutEffect(() => {
        if (!position || !menuRef.current) return;

        const menu = menuRef.current;
        const rect = menu.getBoundingClientRect();
        const viewportW = window.innerWidth;
        const viewportH = window.innerHeight;
        const offset = 12; // Gap from cursor

        // Default: Bottom-Right of cursor
        let left = position.x + offset;
        let top = position.y + offset;

        // Check Right Edge Collision
        if (left + rect.width > viewportW) {
            // Flip to Left of cursor
            left = position.x - rect.width - offset;
        }

        // Check Bottom Edge Collision
        if (top + rect.height > viewportH) {
            // Flip to Top of cursor
            top = position.y - rect.height - offset;
        }

        // Ensure it doesn't go off the top/left edges either (safety clamp)
        if (left < 0) left = offset;
        if (top < 0) top = offset;

        setMenuStyle({
            visibility: 'visible',
            top: `${top}px`,
            left: `${left}px`
        });

    }, [position, options]); // Re-calculate when position or options change

    if (options.length === 0) return null;

    return (
        <>
            <div 
                ref={menuRef}
                className={`disambig-menu ${isCollapsed ? 'collapsed' : ''}`}
                style={menuStyle}
            >
                <div 
                    className="menu-header" 
                    onClick={() => onSetCollapse(!isCollapsed)}
                    onTouchStart={handleTouchStart}
                    onTouchEnd={handleTouchEnd}
                >
                    <div className="mobile-handle" />
                    <span>MULTIPLE FEATURES ({options.length})</span>
                    <button onClick={(e) => { e.stopPropagation(); onClose(); }} className="close-x">×</button>
                </div>
                <div className="menu-list">
                    {options.map((option, idx) => (
                        <button key={idx} className="menu-item" onClick={() => onSelect(option)}>
                            <div className={`square-icon ${option.type}`} />
                            <div className="item-info">
                                <span className="name">{getLabel(option)}</span>
                                <span className="type">{option.type}</span>
                            </div>
                        </button>
                    ))}
                </div>
            </div>

            <style>{`
                /* Desktop and shared styles */
                .disambig-menu {
                    position: absolute; 
                    background: #fff; 
                    border: 1px solid #000;
                    box-shadow: 4px 4px 0 rgba(0,0,0,1); 
                    min-width: 260px;
                    z-index: 2001; 
                    font-family: -apple-system, BlinkMacSystemFont, sans-serif;
                    /* Removed hardcoded transforms here, handled by JS style prop */
                }
                
                .mobile-handle { display: none; }
                
                .menu-header {
                    background: #f0f0f0; 
                    padding: 8px 12px; 
                    border-bottom: 1px solid #000;
                    display: flex; 
                    justify-content: space-between; 
                    align-items: center;
                    font-size: 10px; 
                    font-weight: 800; 
                    letter-spacing: 0.1em; 
                    cursor: pointer;
                    touch-action: none;
                }
                
                .close-x { 
                    border: none; 
                    background: none; 
                    font-size: 16px; 
                    font-weight: bold; 
                    cursor: pointer; 
                    line-height: 1; 
                }
                
                .menu-list { 
                    overflow-y: auto; 
                    max-height: 300px; 
                    background: #fff; 
                }
                
                .menu-item {
                    width: 100%; 
                    display: flex; 
                    align-items: center; 
                    gap: 12px; 
                    padding: 12px;
                    border: none; 
                    background: #fff; 
                    border-bottom: 1px solid #eee;
                    text-align: left; 
                    cursor: pointer;
                }
                .menu-item:hover { background: #f9f9f9; }
                
                .square-icon { width: 12px; height: 12px; border: 1px solid #000; flex-shrink: 0; }
                .square-icon.stream { background: #3b82f6; }
                .square-icon.lake { background: #0ea5e9; }
                .square-icon.wetland { background: #10b981; }
                
                .item-info { display: flex; flex-direction: column; }
                .name { font-size: 13px; font-weight: 600; color: #000; }
                .type { font-size: 10px; text-transform: uppercase; color: #666; }

                /* Mobile Override */
                @media (max-width: 768px) {
                    .disambig-menu {
                        /* Force fixed layout, override JS calculated styles */
                        position: fixed !important; 
                        top: auto !important; 
                        left: 0 !important; 
                        right: 0 !important; 
                        bottom: 0 !important; 
                        width: 100% !important;
                        transform: translateY(0) !important;
                        visibility: visible !important; /* Ensure visible even if JS hook hasn't run perfect */
                        
                        border: none !important; 
                        border-top: 2px solid #000 !important;
                        box-shadow: 0 -4px 15px rgba(0,0,0,0.15) !important;
                        min-width: 0 !important;
                        
                        transition: transform 0.3s cubic-bezier(0.2, 0.8, 0.2, 1);
                        display: flex; 
                        flex-direction: column;
                        max-height: 50vh;
                    }
                    
                    .disambig-menu.collapsed { 
                        transform: translateY(calc(100% - 45px)) !important; 
                    }
                    
                    .mobile-handle {
                        display: block; 
                        width: 30px; 
                        height: 3px; 
                        background: #bbb;
                        border-radius: 2px; 
                        position: absolute; 
                        top: 4px; 
                        left: 50%; 
                        transform: translateX(-50%);
                    }
                    
                    .menu-header { 
                        padding-top: 12px; 
                        justify-content: center; 
                    }
                    
                    .close-x { 
                        position: absolute; 
                        right: 12px; 
                    }
                    
                    .menu-list { 
                        flex: 1; 
                        max-height: none; 
                    }
                    
                    .menu-item { padding: 16px; }
                }
            `}</style>
        </>
    );
};

export default DisambiguationMenu;