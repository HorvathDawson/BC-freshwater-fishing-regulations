/**
 * Client-side per-layer appearance overrides (localStorage).
 *
 * Precedence (lowest → highest):
 *   code base style-layer default → manifest.style → user override.
 * Overrides are SPARSE — only user-edited properties are stored; an absent
 * property falls through to the manifest/coded default. Storing a value =
 * "set"; deleting the key = "reset to default".
 *
 * See webapp/LAYER_MENU_UI_SPEC.md §5 for the full contract.
 */

const STORAGE_KEY = 'bcfish.layerOverrides.v1';
const VERSION = 1;

/** One layer's sparse override — mirrors LayerManifestStyle's flat keys + visible. */
export interface LayerOverride {
    visible?: boolean;
    fill_opacity?: number;
    fill_color?: string;
    line_opacity?: number;
    line_color?: string;
}

/** Map of manifest layer key → its sparse override. */
export type LayerOverrides = Record<string, LayerOverride>;

interface StoredOverrides {
    version: number;
    layers: LayerOverrides;
}

/** Read + validate overrides from localStorage. Never throws — bad/absent
 *  storage yields an empty map so the map always renders defaults. */
export const loadOverrides = (): LayerOverrides => {
    try {
        const raw = localStorage.getItem(STORAGE_KEY);
        if (!raw) return {};
        const parsed = JSON.parse(raw) as StoredOverrides;
        if (!parsed || parsed.version !== VERSION || typeof parsed.layers !== 'object') {
            return {};
        }
        return parsed.layers || {};
    } catch {
        return {};
    }
};

/** Persist overrides, pruning empty per-layer objects. Never throws. */
export const saveOverrides = (overrides: LayerOverrides): void => {
    try {
        const pruned: LayerOverrides = {};
        for (const [key, ov] of Object.entries(overrides)) {
            if (ov && Object.keys(ov).length > 0) pruned[key] = ov;
        }
        localStorage.setItem(STORAGE_KEY, JSON.stringify({ version: VERSION, layers: pruned }));
    } catch {
        /* storage full / unavailable — overrides simply won't persist */
    }
};

/** Set one property on one layer's override (immutably) and return the new map. */
export const setOverrideProp = <K extends keyof LayerOverride>(
    overrides: LayerOverrides,
    key: string,
    prop: K,
    value: LayerOverride[K],
): LayerOverrides => {
    const next: LayerOverrides = { ...overrides, [key]: { ...overrides[key], [prop]: value } };
    return next;
};

/** Remove one property from a layer's override (reset that property to default). */
export const clearOverrideProp = (
    overrides: LayerOverrides,
    key: string,
    prop: keyof LayerOverride,
): LayerOverrides => {
    const layer = { ...overrides[key] };
    delete layer[prop];
    const next = { ...overrides };
    if (Object.keys(layer).length > 0) next[key] = layer;
    else delete next[key];
    return next;
};

/** Delete a whole layer's override (reset that layer to default). */
export const clearLayerOverride = (overrides: LayerOverrides, key: string): LayerOverrides => {
    if (!overrides[key]) return overrides;
    const next = { ...overrides };
    delete next[key];
    return next;
};

/** Clear every override (global reset-all). */
export const clearAllOverrides = (): LayerOverrides => ({});

/** True if the given layer currently has any user override. */
export const hasLayerOverride = (overrides: LayerOverrides, key: string): boolean =>
    !!overrides[key] && Object.keys(overrides[key]).length > 0;

/** True if any layer has an override (drives the global "Reset all" affordance). */
export const hasAnyOverride = (overrides: LayerOverrides): boolean =>
    Object.values(overrides).some((ov) => ov && Object.keys(ov).length > 0);
