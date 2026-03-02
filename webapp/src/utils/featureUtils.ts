/**
 * Shared utilities for waterbody feature display and interaction.
 */

// ─────────────────────────────────────────────────────────────────────────────
// TYPES
// ─────────────────────────────────────────────────────────────────────────────

export type FeatureType = 'stream' | 'lake' | 'wetland' | 'manmade' | 'streams' | 'lakes' | 'wetlands';
export type CollapseState = 'expanded' | 'partial' | 'collapsed';

/** Core feature info for displaying a selected waterbody */
export interface FeatureInfo {
    type: 'stream' | 'lake' | 'wetland' | 'manmade';
    properties: Record<string, any>;
    geometry?: any;
    id?: string | number;
    source?: string;
    sourceLayer?: string;
    bbox?: [number, number, number, number];
    minzoom?: number;
    _segmentCount?: number;
}

/** Extended feature info for disambiguation menus */
export interface FeatureOption extends FeatureInfo {
    id: string;
}

// ─────────────────────────────────────────────────────────────────────────────
// MOBILE DETECTION
// ─────────────────────────────────────────────────────────────────────────────

const MOBILE_BREAKPOINT = 768;

/** Check if current viewport is mobile-sized */
export const isMobileViewport = (): boolean => window.innerWidth <= MOBILE_BREAKPOINT;

// ─────────────────────────────────────────────────────────────────────────────
// ICONS & COLORS
// ─────────────────────────────────────────────────────────────────────────────

/** Icon name from Iconify for each feature type */
export const getIconForType = (type: FeatureType): string => {
    const iconMap: Record<FeatureType, string> = {
        stream: 'game-icons:splashy-stream',
        streams: 'game-icons:splashy-stream',
        lake: 'game-icons:oasis',
        lakes: 'game-icons:oasis',
        wetland: 'game-icons:swamp',
        wetlands: 'game-icons:swamp',
        manmade: 'game-icons:dam'
    };
    return iconMap[type] || iconMap.lake;
};

/** Color for each feature type */
export const getColorForType = (type: FeatureType): string => {
    const colorMap: Record<FeatureType, string> = {
        stream: '#3b82f6',
        streams: '#3b82f6',
        lake: '#0ea5e9',
        lakes: '#0ea5e9',
        wetland: '#10b981',
        wetlands: '#10b981',
        manmade: '#a855f7'
    };
    return colorMap[type] || colorMap.lake;
};

// ─────────────────────────────────────────────────────────────────────────────
// DISPLAY NAMES
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Get display name for a feature, using gnis_name, lake_name, name, 
 * or first regulation name as fallback.
 */
export const getFeatureDisplayName = (
    props: Record<string, any>,
    filterProvincialNames?: (names: string[]) => string[]
): string => {
    if (props.gnis_name) return props.gnis_name;
    if (props.lake_name) return props.lake_name;
    if (props.name) return props.name;
    
    // Try regulation names
    const regNames = props.regulation_names;
    if (regNames) {
        const regNamesArr = Array.isArray(regNames) 
            ? regNames 
            : regNames.split(' | ').filter(Boolean);
        const filtered = filterProvincialNames 
            ? filterProvincialNames(regNamesArr)
            : regNamesArr;
        if (filtered.length > 0) return filtered[0];
    }
    
    return 'Unnamed';
};

/**
 * Name variant with tributary source flag.
 */
export interface NameVariant {
    name: string;
    from_tributary: boolean;
}

/**
 * Get unique aliases from name_variants that aren't the display name.
 * Returns array of {name, from_tributary} for rendering with optional prefix.
 */
export const getUniqueAliases = (
    nameVariants: NameVariant[] | string[],
    displayName: string
): NameVariant[] => {
    const seen = new Set<string>();
    const result: NameVariant[] = [];
    seen.add(displayName.toLowerCase());
    
    for (const nv of nameVariants) {
        // Handle both old string[] format and new NameVariant[] format
        const name = typeof nv === 'string' ? nv : nv.name;
        const fromTributary = typeof nv === 'string' ? false : nv.from_tributary;
        
        const lower = name.toLowerCase();
        if (!seen.has(lower)) {
            seen.add(lower);
            result.push({ name, from_tributary: fromTributary });
        }
    }
    return result;
};

// ─────────────────────────────────────────────────────────────────────────────
// SWIPE HANDLING
// ─────────────────────────────────────────────────────────────────────────────

export interface SwipeResult {
    newState: CollapseState;
    handled: boolean;
}

/**
 * Calculate new collapse state based on swipe gesture (Google Maps style).
 * Fast swipes skip intermediate states.
 */
export const calculateSwipeState = (
    startY: number,
    endY: number,
    startTime: number,
    endTime: number,
    currentState: CollapseState
): SwipeResult => {
    const diffY = endY - startY;
    const timeDiff = endTime - startTime;
    const velocity = Math.abs(diffY) / timeDiff; // px/ms

    // Fast swipe (velocity > 0.5 px/ms) allows jumping states
    const isFastSwipe = velocity > 0.5;
    // Medium swipe threshold
    const threshold = 50;

    // Not enough movement
    if (Math.abs(diffY) < 30) {
        return { newState: currentState, handled: false };
    }

    const isSwipeDown = diffY > 0;
    const isSwipeUp = diffY < 0;

    if (isSwipeDown && Math.abs(diffY) >= threshold) {
        // Swiping down (collapse)
        if (isFastSwipe && currentState === 'expanded') {
            // Fast swipe from expanded goes directly to collapsed
            return { newState: 'collapsed', handled: true };
        }
        // Normal step-down
        if (currentState === 'expanded') return { newState: 'partial', handled: true };
        if (currentState === 'partial') return { newState: 'collapsed', handled: true };
    }

    if (isSwipeUp && Math.abs(diffY) >= threshold) {
        // Swiping up (expand)
        if (isFastSwipe && currentState === 'collapsed') {
            // Fast swipe from collapsed goes directly to expanded
            return { newState: 'expanded', handled: true };
        }
        // Normal step-up
        if (currentState === 'collapsed') return { newState: 'partial', handled: true };
        if (currentState === 'partial') return { newState: 'expanded', handled: true };
    }

    return { newState: currentState, handled: false };
};
