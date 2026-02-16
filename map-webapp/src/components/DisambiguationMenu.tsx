import React, { useRef, useLayoutEffect, useState } from 'react';
import { Eye } from 'lucide-react';
import './DisambiguationMenu.css';

interface FeatureOption {
    type: 'stream' | 'lake' | 'wetland' | 'manmade';
    properties: Record<string, any>;
    id: string;
    _segmentCount?: number;
}

interface DisambiguationMenuProps {
    options: FeatureOption[];
    position: { x: number; y: number } | null;
    highlightedOption: FeatureOption | null;
    onSelect: (option: FeatureOption) => void;
    onHighlight: (option: FeatureOption | null) => void;
    onClose: () => void;
    isCollapsed?: boolean;
    onSetCollapse: (collapsed: boolean) => void;
}

const DisambiguationMenu = ({ options, position, highlightedOption, onSelect, onHighlight, onClose, isCollapsed = false, onSetCollapse }: DisambiguationMenuProps) => {
    const menuRef = useRef<HTMLDivElement>(null);
    const [menuStyle, setMenuStyle] = useState<React.CSSProperties>({
        visibility: 'hidden',
        top: 0,
        left: 0
    });
    const [isMobile, setIsMobile] = useState(window.innerWidth <= 768);

    const getLabel = (opt: FeatureOption) => {
        const regNames = opt.properties.regulation_names;
        const regNamesDisplay = Array.isArray(regNames) ? regNames[0] : regNames;
        return opt.properties.gnis_name || opt.properties.lake_name || opt.properties.name || regNamesDisplay || 'Unnamed';
    };

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

    useLayoutEffect(() => {
        const handleResize = () => {
            setIsMobile(window.innerWidth <= 768);
        };
        
        window.addEventListener('resize', handleResize);
        
        if (isMobile) {
            setMenuStyle({});
            return () => {
                window.removeEventListener('resize', handleResize);
            };
        }

        if (!position || !menuRef.current) {
            return () => {
                window.removeEventListener('resize', handleResize);
            };
        }

        const menu = menuRef.current;
        const rect = menu.getBoundingClientRect();
        const viewportW = window.innerWidth;
        const viewportH = window.innerHeight;
        const offset = 16;

        // Anchor to right side of viewport to avoid covering map features
        const left = viewportW - rect.width - offset;
        
        // Vertically center on click position, but constrain to viewport
        let top = position.y - rect.height / 2;
        top = Math.max(offset, Math.min(top, viewportH - rect.height - offset));

        setMenuStyle({
            visibility: 'visible',
            top: `${top}px`,
            left: `${left}px`
        });

        return () => {
            window.removeEventListener('resize', handleResize);
        };

    }, [position, options]);

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
                    {options.map((option, idx) => {
                        const isHighlighted = highlightedOption?.id === option.id;
                        
                        return (
                            <div 
                                key={idx} 
                                className={`menu-item-wrapper ${isHighlighted ? 'highlighted' : ''}`}
                                onMouseEnter={() => {
                                    // Only highlight on hover on desktop
                                    if (!isMobile) {
                                        onHighlight(option);
                                    }
                                }}
                                onMouseLeave={() => {
                                    // Only clear highlight on leave on desktop
                                    if (!isMobile) {
                                        onHighlight(null);
                                    }
                                }}
                            >
                                <button 
                                    className="menu-item" 
                                    onClick={() => {
                                        if (isMobile) {
                                            // On mobile: first tap highlights, second tap (via button) focuses
                                            if (!isHighlighted) {
                                                onHighlight(option);
                                            }
                                        } else {
                                            // On desktop: directly select
                                            onSelect(option);
                                        }
                                    }}
                                >
                                    <div className={`square-icon ${option.type}`} />
                                    <div className="item-info">
                                        <span className="name">
                                            {getLabel(option)}
                                            {option._segmentCount && option._segmentCount > 1 && (
                                                <span className="segment-badge"> ({option._segmentCount} segments)</span>
                                            )}
                                        </span>
                                        {option.properties.regulation_name && option.properties.regulation_name.toUpperCase() !== getLabel(option).toUpperCase() && (
                                            <span className="regulation-subtitle">Listed as: {option.properties.regulation_name}</span>
                                        )}
                                        <span className="type">{option.type}</span>
                                    </div>
                                </button>
                                {isMobile && isHighlighted && (
                                    <button 
                                        className="focus-button"
                                        onClick={() => onSelect(option)}
                                        aria-label="Focus on this feature"
                                    >
                                        <Eye size={16} />
                                    </button>
                                )}
                            </div>
                        );
                    })}
                </div>
            </div>
        </>
    );
};

export default DisambiguationMenu;