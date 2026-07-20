/**
 * Shared feature-display helpers — ported from `webapp/src/utils/featureUtils.ts`.
 *
 * Pure functions are copied verbatim. The only adaptations for React Native:
 *   - `isMobileViewport()` uses `Dimensions` instead of `window.innerWidth`.
 *   - `getIconForType()` returns a `lucide-react-native` icon name (the web app
 *     uses Iconify game-icons; the closest lucide equivalents are mapped here).
 *   - Web-only swipe/DOM helpers are intentionally omitted (RN uses
 *     react-native-gesture-handler in the component layer instead).
 */
import { Dimensions } from 'react-native';

export type FeatureType =
  | 'stream'
  | 'lake'
  | 'wetland'
  | 'manmade'
  | 'ungazetted'
  | 'streams'
  | 'lakes'
  | 'wetlands';

/** Name variant with source provenance. */
export interface NameVariant {
  name: string;
  source: 'direct' | 'tributary' | 'admin' | 'bathymetry' | 'stocking' | 'marker';
}

// ── Mobile detection ─────────────────────────────────────────────────
const MOBILE_BREAKPOINT = 768;

/** True when the viewport is phone-sized (parity with web breakpoint). */
export const isMobileViewport = (): boolean =>
  Dimensions.get('window').width <= MOBILE_BREAKPOINT;

// ── Icons & colours ──────────────────────────────────────────────────

/** lucide-react-native icon name for each feature type. */
export const getIconForType = (type: FeatureType): string => {
  const iconMap: Record<FeatureType, string> = {
    stream: 'Waves',
    streams: 'Waves',
    lake: 'Droplets',
    lakes: 'Droplets',
    wetland: 'Sprout',
    wetlands: 'Sprout',
    manmade: 'Dam',
    ungazetted: 'Fish',
  };
  return iconMap[type] || iconMap.lake;
};

/** Accent colour for each feature type (must match webapp getColorForType). */
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
  };
  return colorMap[type] || colorMap.lake;
};

// ── Display names ────────────────────────────────────────────────────

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

/** Return the first direct name from a name_variants array, or null. */
export const firstDirectVariantName = (
  nameVariants: NameVariant[] | undefined | null,
): string | null => {
  if (!nameVariants || !Array.isArray(nameVariants)) return null;
  for (const nv of nameVariants) {
    if (nv.source === 'direct' && nv.name) return nv.name;
  }
  return null;
};

/**
 * Get display name for a feature.
 * Order: display_name → first direct variant → type-aware "Unnamed …".
 */
export const getFeatureDisplayName = (
  props: Record<string, unknown>,
  featureType?: string,
): string => {
  if (props.display_name) return String(props.display_name);
  return (
    firstDirectVariantName(props.name_variants as NameVariant[] | undefined) ??
    getUnnamedLabel(featureType)
  );
};

/** Unique aliases from name_variants that aren't the display name. */
export const getUniqueAliases = (
  nameVariants: NameVariant[],
  displayName: string,
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

// ── Text formatting ──────────────────────────────────────────────────

/** Format a list with Oxford comma: "A", "A and B", "A, B, and C". */
export const formatList = (items: string[]): string => {
  if (items.length === 1) return items[0];
  if (items.length === 2) return `${items[0]} and ${items[1]}`;
  return `${items.slice(0, -1).join(', ')}, and ${items[items.length - 1]}`;
};

/**
 * Build formatted alias lines from a NameVariant array so InfoPanel and
 * DisambiguationMenu render identical text.
 */
export const buildAliasLines = (
  aliases: NameVariant[],
): { alsoKnownAs: string | null; inContext: string | null } => {
  const tributaryAliases = aliases.filter((a) => a.source === 'tributary');
  const adminAliases = aliases.filter((a) => a.source === 'admin');
  const regularAliases = aliases.filter((a) => a.source === 'direct');
  const bathymetryAliases = aliases.filter((a) => a.source === 'bathymetry');

  const parts: string[] = [];
  if (tributaryAliases.length > 0) {
    parts.push(`Tributary of ${formatList(tributaryAliases.map((a) => a.name))}`);
  }
  regularAliases.forEach((a) => parts.push(a.name));
  bathymetryAliases.forEach((a) => parts.push(a.name));

  return {
    alsoKnownAs: parts.length > 0 ? `Also known as: ${parts.join(' · ')}` : null,
    inContext:
      adminAliases.length > 0
        ? `In ${formatList(adminAliases.map((a) => a.name))}`
        : null,
  };
};
