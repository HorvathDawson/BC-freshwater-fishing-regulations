/**
 * URL state management for sharing and deep-linking.
 * Syncs selected feature and search query with URL parameters.
 * Works alongside MapLibre's hash-based position tracking.
 */

// URL parameter names
const PARAMS = {
    FEATURE: 'f',      // frontend_group_id of selected feature
    SEARCH: 'q',       // search query text
} as const;

export interface UrlState {
    featureId?: string;
    searchQuery?: string;
}

/**
 * Parse current URL for state parameters.
 * MapLibre uses the hash fragment for position, we use search params.
 */
export const parseUrlState = (): UrlState => {
    const params = new URLSearchParams(window.location.search);
    return {
        featureId: params.get(PARAMS.FEATURE) || undefined,
        searchQuery: params.get(PARAMS.SEARCH) || undefined,
    };
};

/**
 * Update URL with state parameters without triggering navigation.
 * Preserves the existing hash (map position).
 */
export const updateUrlState = (state: Partial<UrlState>): void => {
    const params = new URLSearchParams(window.location.search);
    
    // Update/remove parameters based on state
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
    
    // Build new URL preserving hash
    const search = params.toString();
    const newUrl = search 
        ? `${window.location.pathname}?${search}${window.location.hash}`
        : `${window.location.pathname}${window.location.hash}`;
    
    window.history.replaceState(null, '', newUrl);
};

/**
 * Clear all state parameters from URL.
 */
export const clearUrlState = (): void => {
    const newUrl = `${window.location.pathname}${window.location.hash}`;
    window.history.replaceState(null, '', newUrl);
};

/**
 * Generate a shareable URL for the current state.
 * Includes feature ID and map position.
 */
export const getShareableUrl = (featureId?: string): string => {
    const baseUrl = window.location.origin + window.location.pathname;
    const params = new URLSearchParams();
    
    if (featureId) {
        params.set(PARAMS.FEATURE, featureId);
    }
    
    const search = params.toString();
    const hash = window.location.hash;
    
    return search 
        ? `${baseUrl}?${search}${hash}`
        : `${baseUrl}${hash}`;
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
