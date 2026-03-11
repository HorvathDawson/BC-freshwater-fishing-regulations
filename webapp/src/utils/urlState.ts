/**
 * URL state management for sharing and deep-linking.
 * Syncs selected feature and search query with URL parameters.
 * Works alongside MapLibre's hash-based position tracking.
 *
 * Canonical URL for named waterbodies: /waterbody/<wbg>/
 *   wbg = fwa_watershed_code (streams) or waterbody_key (lakes/polygons)
 *   This is stable across annual regulation pipeline reruns.
 *
 * Legacy URL (?f=<fgid>) is still parsed for backwards compatibility
 * with old shared links but is never written by current code.
 */

// URL parameter names (legacy compat only — new URLs use path routing)
const PARAMS = {
    FEATURE: 'f',      // frontend_group_id (legacy — read-only for compat)
    SEARCH: 'q',       // search query text
} as const;

// Canonical path prefix for named waterbody pages
const WBG_PATH_PREFIX = '/waterbody/';

export interface UrlState {
    /** Legacy: fgid from ?f= param. Only populated for old shared links. */
    featureId?: string;
    /** Canonical: wbg slug from /waterbody/<wbg>/ path. */
    waterbodyGroup?: string;
    searchQuery?: string;
    /** Active section fgid from ?s=<fgid> param — written on tab switch, read on page load. */
    activeFgid?: string;
}

/** Extract waterbody_group slug from path /waterbody/<wbg>/ — undefined if not a wbg path. */
const parseWbgFromPath = (): string | undefined => {
    const path = window.location.pathname;
    if (!path.startsWith(WBG_PATH_PREFIX)) return undefined;
    const slug = decodeURIComponent(path.slice(WBG_PATH_PREFIX.length).replace(/\/$/, ''));
    return slug || undefined;
};

// Section param — encodes the active tab's fgid in the URL.
// Read-only in parseUrlState; written exclusively via setActiveSectionParam.
const SECTION_PARAM = 's';

/**
 * Parse current URL for state parameters.
 * Path-based wbg takes priority over legacy query params.
 * Always reads ?s=<fgid> regardless of path type.
 */
export const parseUrlState = (): UrlState => {
    const wbg = parseWbgFromPath();
    const params = new URLSearchParams(window.location.search);
    const activeFgid = params.get(SECTION_PARAM) || undefined;
    if (wbg) {
        return { waterbodyGroup: wbg, activeFgid };
    }
    return {
        featureId: params.get(PARAMS.FEATURE) || undefined,
        searchQuery: params.get(PARAMS.SEARCH) || undefined,
        activeFgid,
    };
};

/**
 * Write or clear the active section param (?s=<fgid>) without navigating.
 * Called by InfoPanel on every tab switch.
 * Preserves the existing pathname and hash (MapLibre position).
 */
export const setActiveSectionParam = (fgid: string | undefined): void => {
    const params = new URLSearchParams(window.location.search);
    if (fgid) {
        params.set(SECTION_PARAM, fgid);
    } else {
        params.delete(SECTION_PARAM);
    }
    const search = params.toString();
    window.history.replaceState(
        null,
        '',
        `${window.location.pathname}${search ? `?${search}` : ''}${window.location.hash}`,
    );
};

/**
 * Update URL query parameters without triggering navigation.
 * Always operates from root path (/) to avoid contaminating wbg paths.
 * Preserves the existing hash (map position).
 *
 * @deprecated No longer called by production code. Named waterbodies use
 * navigateToWaterbody(); deselection uses clearUrlState(). This function is
 * retained only for legacy compatibility — do not call it from a /waterbody/*
 * path as it will reset the URL to /.
 */
export const updateUrlState = (state: Partial<UrlState>): void => {
    const params = new URLSearchParams(window.location.search);
    
    if (state.featureId !== undefined) {
        if (state.featureId) {
            params.set(PARAMS.FEATURE, state.featureId);
        } else {
            params.delete(PARAMS.FEATURE);
        }
    }
    
    if (state.searchQuery !== undefined) {
        if (state.searchQuery) {
            params.set(PARAMS.SEARCH, state.searchQuery);
        } else {
            params.delete(PARAMS.SEARCH);
        }
    }
    
    const search = params.toString();
    const newUrl = search
        ? `/?${search}${window.location.hash}`
        : `/${window.location.hash}`;
    
    window.history.replaceState(null, '', newUrl);
};

/**
 * Strip trailing -000000 padding segments from FWA watershed codes for URL slugs.
 * FWA codes are fixed-length hierarchical codes where trailing zero groups are padding.
 *   900-105574-000000-...-000000  →  900-105574
 * Non-FWA keys (integer waterbody_key for lakes) are returned unchanged.
 */
export const collapseWbg = (wbg: string): string => wbg.replace(/(-000000)+$/, '');

/**
 * Navigate to the canonical URL for a named waterbody group.
 * Writes /waterbody/<wbg>/ as the path, preserving the map position hash
 * and the active section param (?s=<fgid>) if already present.
 * Automatically collapses trailing FWA padding from the slug.
 */
export const navigateToWaterbody = (wbg: string): void => {
    const existing = new URLSearchParams(window.location.search);
    const section = existing.get(SECTION_PARAM);
    const search = section ? `?${SECTION_PARAM}=${encodeURIComponent(section)}` : '';
    const newUrl = `${WBG_PATH_PREFIX}${encodeURIComponent(collapseWbg(wbg))}/${search}${window.location.hash}`;
    window.history.replaceState(null, '', newUrl);
};

/**
 * Navigate to the legacy ?f=<fgid> URL for unnamed/compact features.
 * Preserves the map position hash and active section param.
 * Used when an unnamed stream is selected so the address bar is shareable.
 */
export const navigateToFeature = (fgid: string): void => {
    const existing = new URLSearchParams(window.location.search);
    const section = existing.get(SECTION_PARAM);
    const params = new URLSearchParams();
    params.set(PARAMS.FEATURE, fgid);
    if (section) params.set(SECTION_PARAM, section);
    window.history.replaceState(null, '', `/?${params.toString()}${window.location.hash}`);
};

/**
 * Clear all URL state and return to the root path (/), preserving map position hash.
 * Always navigates to / — safe to call from any path including /waterbody/<wbg>/.
 */
export const clearUrlState = (): void => {
    window.history.replaceState(null, '', `/${window.location.hash}`);
};

/**
 * Generate the canonical shareable URL for a named waterbody group.
 * Uses the stable /waterbody/<wbg>/ path format, includes current map position hash.
 * Automatically collapses trailing FWA padding from the slug.
 */
export const getCanonicalUrl = (wbg: string, sectionFgid?: string): string => {
    const sectionParam = sectionFgid ? `?${SECTION_PARAM}=${encodeURIComponent(sectionFgid)}` : '';
    return `${window.location.origin}${WBG_PATH_PREFIX}${encodeURIComponent(collapseWbg(wbg))}/${sectionParam}${window.location.hash}`;
};

/**
 * Generate a shareable URL using a feature ID (for unnamed/legacy features).
 * Prefers getCanonicalUrl() for all named waterbodies.
 */
export const getShareableUrl = (featureId?: string, sectionFgid?: string): string => {
    const params = new URLSearchParams();
    if (featureId) {
        params.set(PARAMS.FEATURE, featureId);
    }
    if (sectionFgid) {
        params.set(SECTION_PARAM, sectionFgid);
    }
    const search = params.toString();
    const hash = window.location.hash;
    return search
        ? `${window.location.origin}/?${search}${hash}`
        : `${window.location.origin}/${hash}`;
};

/**
 * Copy text to clipboard with fallback for older browsers.
 */
export const copyToClipboard = async (text: string): Promise<boolean> => {
    try {
        if (navigator.clipboard && navigator.clipboard.writeText) {
            await navigator.clipboard.writeText(text);
            return true;
        }
        
        // Fallback for older browsers
        const textArea = document.createElement('textarea');
        textArea.value = text;
        textArea.style.position = 'fixed';
        textArea.style.left = '-9999px';
        document.body.appendChild(textArea);
        textArea.select();
        const success = document.execCommand('copy');
        document.body.removeChild(textArea);
        return success;
    } catch {
        return false;
    }
};
