import React, { useRef, useLayoutEffect, useState } from 'react';
import { Eye } from 'lucide-react';
import { Icon } from '@iconify/react';
import { regulationsService } from '../services/regulationsService';
import { 
    getIconForType, 
    getColorForType, 
    getFeatureDisplayName,
    isMobileViewport,
    type FeatureOption 
} from '../utils/featureUtils';
import './DisambiguationMenu.css';

interface DisambiguationMenuProps {
    options: FeatureOption[];
    position: { x: number; y: number } | null;
    highlightedOption: FeatureOption | null;
    onSelect: (option: FeatureOption) => void;
    onHighlight: (option: FeatureOption | null) => void;
    onClose: () => void;
}

const DisambiguationMenu = ({ options, position, highlightedOption, onSelect, onHighlight, onClose }: DisambiguationMenuProps) => {
    const menuRef = useRef<HTMLDivElement>(null);
    const [menuStyle, setMenuStyle] = useState<React.CSSProperties>({
        visibility: 'hidden',
        top: 0,
        left: 0
    });
    const [isMobile, setIsMobile] = useState(isMobileViewport());

    const getLabel = (opt: FeatureOption) => 
        getFeatureDisplayName(opt.properties, regulationsService.filterOutProvincialNames);

    useLayoutEffect(() => {
        const handleResize = () => setIsMobile(isMobileViewport());
        
        window.addEventListener('resize', handleResize);
        
        if (isMobile) {
            setMenuStyle({});
            return () => window.removeEventListener('resize', handleResize);
        }

        if (!position || !menuRef.current) {
            return () => window.removeEventListener('resize', handleResize);
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
                className="disambig-menu"
                style={menuStyle}
                role="region"
                aria-label="Feature disambiguation menu"
            >
                <div className="menu-header">
                    <span>MULTIPLE FEATURES ({options.length})</span>
                    <button onClick={(e) => { e.stopPropagation(); onClose(); }} className="close-x" aria-label="Close feature menu">×</button>
                </div>
                <div className="menu-list" role="listbox" aria-label="Overlapping features">
                    {options.map((option, idx) => {
                        const isHighlighted = highlightedOption?.id === option.id;
                        
                        return (
                            <div 
                                key={idx} 
                                className={`menu-item-wrapper ${isHighlighted ? 'highlighted' : ''}`}
                                role="option"
                                aria-selected={isHighlighted}
                                onMouseEnter={() => { if (!isMobile) onHighlight(option); }}
                                onMouseLeave={() => { if (!isMobile) onHighlight(null); }}
                            >
                                <button 
                                    className="menu-item" 
                                    onClick={() => {
                                        if (isMobile) {
                                            // First tap highlights, second tap (or focus button) selects
                                            if (!isHighlighted) onHighlight(option);
                                            else onSelect(option);
                                        } else {
                                            onSelect(option);
                                        }
                                    }}
                                >
                                    <div className={`icon-container ${option.type}`} style={{ backgroundColor: getColorForType(option.type) }}>
                                        <Icon icon={getIconForType(option.type)} width={28} height={28} color="white" />
                                    </div>
                                    <div className="item-info">
                                        <span className="name">
                                            {getLabel(option)}
                                            {option._segmentCount && option._segmentCount > 1 && (
                                                <span className="segment-badge"> ({option._segmentCount} segments)</span>
                                            )}
                                        </span>
                                        {(() => {
                                            const rawName = option.properties.regulation_name || '';
                                            const names = rawName ? String(rawName).split(' | ').filter(Boolean) : [];
                                            const filtered = regulationsService.filterOutProvincialNames(names).join(' | ');
                                            return filtered && filtered.toUpperCase() !== getLabel(option).toUpperCase() ? (
                                                <span className="regulation-subtitle">Listed as: {filtered}</span>
                                            ) : null;
                                        })()}
                                        <span className="type">{option.type}</span>
                                    </div>
                                </button>
                                {isMobile && (
                                    <button
                                        className={`focus-button ${isHighlighted ? '' : 'hidden'}`}
                                        onPointerDown={(e) => { e.stopPropagation(); onSelect(option); }}
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
            </div>
        </>
    );
};

export default DisambiguationMenu;