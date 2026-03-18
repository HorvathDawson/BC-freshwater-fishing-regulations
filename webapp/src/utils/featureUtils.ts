/**
 * Shared utilities for waterbody feature display and interaction.
 */

// ─────────────────────────────────────────────────────────────────────────────
// TYPES
// ─────────────────────────────────────────────────────────────────────────────

export type FeatureType = 'stream' | 'lake' | 'wetland' | 'manmade' | 'ungazetted' | 'streams' | 'lakes' | 'wetlands';
export type CollapseState = 'expanded' | 'partial';

/** Simplified GeoJSON geometry for feature display purposes */
export interface FeatureGeometry {
    type: string;
    coordinates: number[] | number[][] | number[][][] | number[][][][];
}

/** Core feature info for displaying a selected waterbody */
export interface FeatureInfo {
    type: 'stream' | 'lake' | 'wetland' | 'manmade' | 'ungazetted';
    properties: Record<string, unknown>;
    geometry?: FeatureGeometry;
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
        manmade: 'game-icons:dam',
        ungazetted: 'game-icons:fishing-lure'
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
        manmade: '#a855f7',
        ungazetted: '#f59e0b'
    };
    return colorMap[type] || colorMap.lake;
};

// ─────────────────────────────────────────────────────────────────────────────
// DISPLAY NAMES
// ─────────────────────────────────────────────────────────────────────────────

/** Human-readable fallback label for features with no name. */
export const getUnnamedLabel = (featureType?: string): string => {
    switch (featureType) {
        case 'stream':
        case 'streams':
            return 'Unnamed Stream';
        case 'lake':
        case 'lakes':
            return 'Unnamed Lake';
        case 'wetland':
        case 'wetlands':
            return 'Unnamed Wetland';
        case 'manmade':
            return 'Unnamed Reservoir';
        case 'ungazetted':
            return 'Unnamed Waterbody';
        default:
            return 'Unnamed';
    }
};

/**
 * Get display name for a feature.
 *
 * Resolution order:
 *   1. display_name  (pre-computed in backend)
 *   2. First direct name_variant (source === "direct")
 *   3. "Unnamed Stream" / "Unnamed Lake" / etc. (type-aware)
 */
export const getFeatureDisplayName = (
    props: Record<string, any>,
    featureType?: string,
): string => {
    if (props.display_name) return props.display_name;
    return firstDirectVariantName(props.name_variants) ?? getUnnamedLabel(featureType);
};

/**
 * Name variant with source provenance.
 *   - "direct"    — regulation directly matched to this feature
 *   - "tributary"  — inherited via tributary BFS
 *   - "admin"      — inherited via admin polygon (park/reserve)
 */
export interface NameVariant {
    name: string;
    source: 'direct' | 'tributary' | 'admin';
}

/**
 * Return the first direct name from a name_variants array,
 * or `null` if none exists.  Useful as a fallback before showing "Unnamed".
 */
export const firstDirectVariantName = (
    nameVariants: NameVariant[] | undefined | null
): string | null => {
    if (!nameVariants || !Array.isArray(nameVariants)) return null;
    for (const nv of nameVariants) {
        if (nv.source === 'direct' && nv.name) return nv.name;
    }
    return null;
};

/**
 * Get unique aliases from name_variants that aren't the display name.
 * Returns array of NameVariant for rendering with source-based prefix.
 */
export const getUniqueAliases = (
    nameVariants: NameVariant[],
    displayName: string
): NameVariant[] => {
    const seen = new Set<string>();
    const result: NameVariant[] = [];
    seen.add(displayName.toLowerCase());
    
    for (const nv of nameVariants) {
        const lower = nv.name.toLowerCase();
        if (!seen.has(lower)) {
            seen.add(lower);
            result.push(nv);
        }
    }
    return result;
};

// ─────────────────────────────────────────────────────────────────────────────
// TEXT FORMATTING
// ─────────────────────────────────────────────────────────────────────────────

/** Format a list with Oxford comma: "A", "A and B", "A, B, and C" */
export const formatList = (items: string[]): string => {
    if (items.length === 1) return items[0];
    if (items.length === 2) return `${items[0]} and ${items[1]}`;
    return `${items.slice(0, -1).join(', ')}, and ${items[items.length - 1]}`;
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
    _startTime: number,
    _endTime: number,
    currentState: CollapseState
): SwipeResult => {
    const diffY = endY - startY;

    // Swipe threshold
    const threshold = 50;

    // Not enough movement
    if (Math.abs(diffY) < 30) {
        return { newState: currentState, handled: false };
    }

    const isSwipeDown = diffY > 0;
    const isSwipeUp = diffY < 0;

    if (isSwipeDown && Math.abs(diffY) >= threshold) {
        // Swiping down → partial
        if (currentState === 'expanded') return { newState: 'partial', handled: true };
    }

    if (isSwipeUp && Math.abs(diffY) >= threshold) {
        // Swiping up → expanded
        if (currentState === 'partial') return { newState: 'expanded', handled: true };
    }

    return { newState: currentState, handled: false };
};
