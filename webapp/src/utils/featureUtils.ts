/**
 * Shared utilities for waterbody feature display and interaction.
 */

// ─────────────────────────────────────────────────────────────────────────────
// TYPES
// ─────────────────────────────────────────────────────────────────────────────

export type FeatureType = 'stream' | 'lake' | 'wetland' | 'manmade' | 'ungazetted' | 'streams' | 'lakes' | 'wetlands' | AdminFeatureType;
export type CollapseState = 'expanded' | 'partial';

/**
 * Land-ownership/access and protected-area/admin-boundary types — clickable
 * on the map (see ADMIN_INTERACTABLE_LAYERS in Map.tsx) in addition to
 * waterbodies. Each carries its own color/icon so the color already signals
 * severity/category (e.g. land_access is split into closed/restricted
 * variants instead of taking a `restriction_level` param) rather than
 * requiring callers to branch on extra properties.
 */
export type AdminFeatureType =
    | 'park_national'
    | 'park_provincial'
    | 'eco_reserve'
    | 'protected_area'
    | 'recreation_area'
    | 'wma'
    | 'watershed'
    | 'historic_site'
    | 'land_access_closed'
    | 'land_access_restricted'
    | 'land_ownership_crown'
    | 'land_ownership_public'
    | 'land_ownership_private'
    | 'aboriginal_land';

const ADMIN_FEATURE_TYPE_SET = new Set<string>([
    'park_national', 'park_provincial', 'eco_reserve', 'protected_area', 'recreation_area',
    'wma', 'watershed', 'historic_site',
    'land_access_closed', 'land_access_restricted',
    'land_ownership_crown', 'land_ownership_public', 'land_ownership_private',
    'aboriginal_land',
]);

/** True for the land-ownership/access/protected-area types (not a waterbody). */
export const isAdminFeatureType = (type: string): type is AdminFeatureType =>
    ADMIN_FEATURE_TYPE_SET.has(type);

/** Category label + optional explanatory note, shown in the admin info panel. */
export const ADMIN_TYPE_INFO: Record<AdminFeatureType, { label: string; note?: string }> = {
    park_national: {
        label: 'National Park',
        note: 'Federally protected — fishing is not permitted within park boundaries.',
    },
    park_provincial: { label: 'Provincial Park' },
    eco_reserve: {
        label: 'Ecological Reserve',
        note: 'Provincially protected — fishing is not permitted within reserve boundaries.',
    },
    protected_area: { label: 'Protected Area' },
    recreation_area: { label: 'Recreation Area' },
    wma: {
        label: 'Wildlife Management Area',
        note: 'Additional wildlife-management regulations may apply — check current WMA notices.',
    },
    watershed: {
        label: 'Watershed',
        note: 'Community or domestic watershed — access may be restricted to protect drinking water.',
    },
    historic_site: { label: 'Historic Site' },
    land_access_closed: {
        label: 'Closed — No Public Access',
        note: 'This area is closed to public access (e.g. military land, protected watershed, or private property).',
    },
    land_access_restricted: {
        label: 'Restricted Access',
        note: 'Conditional or permit-based access — check with the managing authority before entering.',
    },
    land_ownership_crown: {
        label: 'Crown Land',
        note: 'Provincial or federal Crown land — generally open for recreational access unless otherwise posted.',
    },
    land_ownership_public: {
        label: 'Public / Local Government Land',
        note: 'Municipal or local-government owned land — generally open for recreational access unless otherwise posted.',
    },
    land_ownership_private: {
        label: 'Private Land',
        note: 'Privately owned — obtain permission from the landowner before entering.',
    },
    aboriginal_land: {
        label: 'Indigenous / Treaty Land',
        note: 'This area overlaps Indigenous lands or treaty territory — respect any posted access requirements and local First Nations bylaws.',
    },
};

/**
 * Severity ranking for admin/land types, used to sort the disambiguation
 * menu (land options always sort below waterbodies; among themselves, most
 * severe/restrictive first). Lower number = more severe = listed first.
 * "Severity" here means how much the designation limits what you can do:
 * no access at all, then no fishing, then advisories, then ownership/
 * informational categories that are mostly open by default.
 */
export const ADMIN_TYPE_SEVERITY: Record<AdminFeatureType, number> = {
    land_access_closed: 0,
    land_access_restricted: 1,
    park_national: 2,
    eco_reserve: 3,
    aboriginal_land: 4,
    land_ownership_private: 5,
    wma: 6,
    watershed: 7,
    protected_area: 8,
    land_ownership_public: 9,
    land_ownership_crown: 10,
    recreation_area: 11,
    historic_site: 12,
    park_provincial: 13,
};

/** Format a raw m² area (as stored on AdminRecord) as hectares or km², whichever reads better. */
export const formatArea = (m2: unknown): string | null => {
    const n = Number(m2);
    if (!Number.isFinite(n) || n <= 0) return null;
    const hectares = n / 10_000;
    if (hectares >= 1000) return `${(hectares / 100).toLocaleString(undefined, { maximumFractionDigits: 1 })} km²`;
    return `${hectares.toLocaleString(undefined, { maximumFractionDigits: hectares < 10 ? 1 : 0 })} ha`;
};

/** Simplified GeoJSON geometry for feature display purposes */
export interface FeatureGeometry {
    type: string;
    coordinates: number[] | number[][] | number[][][] | number[][][][];
}

/** Core feature info for displaying a selected waterbody */
export interface FeatureInfo {
    type: 'stream' | 'lake' | 'wetland' | 'manmade' | 'ungazetted' | AdminFeatureType;
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
        ungazetted: 'game-icons:fishing-lure',
        // Admin / land-ownership / protected-area types
        park_national: 'mdi:pine-tree',
        park_provincial: 'mdi:tree',
        eco_reserve: 'mdi:leaf',
        protected_area: 'mdi:shield-outline',
        recreation_area: 'mdi:hiking',
        wma: 'mdi:paw',
        watershed: 'mdi:water',
        historic_site: 'mdi:bank',
        land_access_closed: 'mdi:lock',
        land_access_restricted: 'mdi:alert-circle-outline',
        land_ownership_crown: 'mdi:domain',
        land_ownership_public: 'mdi:city-variant-outline',
        land_ownership_private: 'mdi:fence',
        aboriginal_land: 'mdi:map-marker-star-outline',
    };
    return iconMap[type] || iconMap.lake;
};

/** Color for each feature type. Admin/land colors mirror ADMIN_COLORS in map/styles.ts — keep in sync. */
export const getColorForType = (type: FeatureType): string => {
    const colorMap: Record<FeatureType, string> = {
        stream: '#3b82f6',
        streams: '#3b82f6',
        lake: '#0ea5e9',
        lakes: '#0ea5e9',
        wetland: '#10b981',
        wetlands: '#10b981',
        manmade: '#a855f7',
        ungazetted: '#f59e0b',
        // Admin / land-ownership / protected-area types
        park_national: '#C22E2E',
        park_provincial: '#009E73',
        eco_reserve: '#C22E2E',
        protected_area: '#0072B2',
        recreation_area: '#6B8E6B',
        wma: '#7B2D8B',
        watershed: '#006D77',
        historic_site: '#795548',
        land_access_closed: '#DC2626',
        land_access_restricted: '#CC7A00',
        land_ownership_crown: '#546E7A',
        land_ownership_public: '#4A6FA5',
        land_ownership_private: '#8D6E63',
        aboriginal_land: '#8B6508',
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
            if (featureType && isAdminFeatureType(featureType)) return ADMIN_TYPE_INFO[featureType].label;
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
 *   - "direct"     — regulation directly matched to this feature
 *   - "tributary"  — inherited via tributary BFS
 *   - "admin"      — inherited via admin polygon (park/reserve)
 *   - "bathymetry" — alternate name from a WSA bathymetric survey map
 *   - "stocking"   — alternate name from a FIDQ stocking record
 *   - "marker"     — alternate name from a gofishbc map marker
 */
export interface NameVariant {
    name: string;
    source: 'direct' | 'tributary' | 'admin' | 'alias' | 'stocking' | 'marker';
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
// TIME FORMATTING
// ─────────────────────────────────────────────────────────────────────────────

/** Format a dawn/dusk Date as a local time string (e.g. "5:12 AM"), or an em dash if null. */
export const formatDawnDuskTime = (d: Date | null | undefined): string =>
    d ? d.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' }) : '—';

// ─────────────────────────────────────────────────────────────────────────────
// TEXT FORMATTING
// ─────────────────────────────────────────────────────────────────────────────

/** Format a list with Oxford comma: "A", "A and B", "A, B, and C" */
export const formatList = (items: string[]): string => {
    if (items.length === 1) return items[0];
    if (items.length === 2) return `${items[0]} and ${items[1]}`;
    return `${items.slice(0, -1).join(', ')}, and ${items[items.length - 1]}`;
};

/**
 * Build formatted alias lines from a NameVariant array.
 * Returns { alsoKnownAs: string | null, inContext: string | null }
 * so both InfoPanel and DisambiguationMenu render identical text.
 */
export const buildAliasLines = (aliases: NameVariant[]): { alsoKnownAs: string | null; inContext: string | null } => {
    const tributaryAliases = aliases.filter(a => a.source === 'tributary');
    const adminAliases = aliases.filter(a => a.source === 'admin');
    // General alternate names: reg-match "direct" names plus the catch-all
    // "alias" bucket (feature display-name overrides, bathymetric-survey names,
    // and any future alternate-name source folded into `alias`).
    const regularAliases = aliases.filter(a => a.source === 'direct' || a.source === 'alias');

    const parts: string[] = [];
    if (tributaryAliases.length > 0) {
        parts.push(`Tributary of ${formatList(tributaryAliases.map(a => a.name))}`);
    }
    regularAliases.forEach(a => parts.push(a.name));

    return {
        alsoKnownAs: parts.length > 0 ? `Also known as: ${parts.join(' · ')}` : null,
        inContext: adminAliases.length > 0 ? `In ${formatList(adminAliases.map(a => a.name))}` : null,
    };
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
