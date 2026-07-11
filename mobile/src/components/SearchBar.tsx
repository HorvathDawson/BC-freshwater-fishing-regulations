/**
 * SearchBar (mobile) — faithful port of `webapp/src/components/SearchBar.tsx`.
 *
 * Only the rendering layer (RN primitives instead of DOM/CSS) and the data
 * source change: results come from the on-device {@link searchService} (SQLite
 * FTS + fuse.js rerank) rather than the web app's in-memory Fuse index. The
 * search ranking and result UI are mirrored 1:1:
 *
 *   - Direct name matches rank above town-only matches.
 *   - Within name matches, exact prefix hits are boosted.
 *   - Within town-only matches, the nearest town ranks first.
 *   - Each row shows the display name (with a feature-type colour dot), an
 *     "Also known as:" alias line, an optional "X km from <town>" line, and a
 *     "<type> • <region>" meta line — exactly like the web dropdown.
 */
import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  ActivityIndicator,
  FlatList,
  Keyboard,
  Pressable,
  StyleSheet,
  Text,
  TextInput,
  View,
} from 'react-native';
import { Search, X } from 'lucide-react-native';

import { searchService, type SearchResult } from '../data/searchService';
import type { NearbyTown } from '../types/regulations';
import {
  buildAliasLines,
  getColorForType,
  getFeatureDisplayName,
  getUniqueAliases,
  type FeatureType,
  type NameVariant,
} from '../utils/featureUtils';
import { colors, radius, spacing, typography } from '../theme/tokens';

export interface SearchBarProps {
  onSelect: (result: SearchResult) => void;
  placeholder?: string;
}

/** How long to wait after the last keystroke before querying (async SQLite). */
const DEBOUNCE_MS = 200;
const RESULT_LIMIT = 50;

/** Normalise nearby_towns (which may be plain strings) to {name, km?}. */
const normalizeTowns = (towns?: (string | NearbyTown)[]): NearbyTown[] =>
  (towns ?? []).map((t) => (typeof t === 'string' ? { name: t } : { name: t.name, km: t.km }));

/** All searchable names for an entry (display + non-admin variants). */
const namesOf = (entry: SearchResult): string[] => [
  entry.display_name,
  ...(entry.name_variants ?? []).filter((nv) => nv.source !== 'admin').map((nv) => nv.name),
];

/** Build the compact region label: "7A – Omineca, 7B – Prince George" or "7A, 7B +N". */
const buildRegionDisplay = (zones: string[], regions: string[]): string | null => {
  const zList = zones ?? [];
  const nList = regions ?? [];
  if (zList.length === 0) return null;
  if (zList.length === 1) return zList[0] + (nList[0] ? ` – ${nList[0]}` : '');
  if (zList.length === 2) {
    const first = zList[0] + (nList[0] ? ` – ${nList[0]}` : '');
    const second = zList[1] + (nList[1] ? ` – ${nList[1]}` : '');
    return `${first}, ${second}`;
  }
  return `${zList[0]}, ${zList[1]} +${zList.length - 2}`;
};

export function SearchBar({
  onSelect,
  placeholder = 'Search waterbodies...',
}: SearchBarProps): React.JSX.Element {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<SearchResult[]>([]);
  /** For results that only surfaced via a nearby town: entry → that town. */
  const [townLabels, setTownLabels] = useState<Map<SearchResult, NearbyTown>>(() => new Map());
  const [isLoading, setIsLoading] = useState(false);

  // Guards against out-of-order async responses clobbering newer ones.
  const requestSeq = useRef(0);

  const runSearch = useCallback(async (raw: string) => {
    const trimmed = raw.trim();
    if (trimmed.length < 2) {
      setResults([]);
      setTownLabels(new Map());
      setIsLoading(false);
      return;
    }

    const seq = ++requestSeq.current;
    setIsLoading(true);

    let entries: SearchResult[] = [];
    try {
      entries = await searchService.search(trimmed, RESULT_LIMIT);
    } catch {
      entries = [];
    }
    // A newer keystroke superseded this request — drop the stale result.
    if (seq !== requestSeq.current) return;

    const queryLower = trimmed.toLowerCase();
    const tokens = queryLower.split(/\s+/).filter(Boolean);
    const matchesAnyToken = (hay: string) => tokens.some((t) => hay.includes(t));

    // A result is a direct "name match" when the query hit its display_name or
    // a (non-admin) name variant. Otherwise it only surfaced via a nearby town.
    const nameMatched = (entry: SearchResult): boolean =>
      namesOf(entry).some((n) => n && matchesAnyToken(n.toLowerCase()));

    // The nearest matched town (nearby_towns is ordered nearest-first, so the
    // first matching town is closest) — used to label and sort town-only rows.
    const townFor = (entry: SearchResult): NearbyTown | null => {
      for (const town of normalizeTowns(entry.nearby_towns)) {
        if (matchesAnyToken(town.name.toLowerCase())) return town;
      }
      return null;
    };

    const startsWithQuery = (entry: SearchResult): boolean =>
      namesOf(entry).some((n) => n?.toLowerCase().startsWith(queryLower));

    // Preserve the service's ranking as the final tiebreak.
    const orderOf = new Map<SearchResult, number>();
    entries.forEach((e, i) => orderOf.set(e, i));

    const sorted = [...entries].sort((a, b) => {
      const aName = nameMatched(a);
      const bName = nameMatched(b);
      // 1. Direct name matches always rank above town-only matches.
      if (aName !== bName) return aName ? -1 : 1;
      if (aName) {
        // 2. Within name matches, boost exact prefix hits.
        const aPre = startsWithQuery(a);
        const bPre = startsWithQuery(b);
        if (aPre !== bPre) return aPre ? -1 : 1;
      } else {
        // 2b. Within town-only matches, nearest town first.
        const aKm = townFor(a)?.km ?? Infinity;
        const bKm = townFor(b)?.km ?? Infinity;
        if (aKm !== bKm) return aKm - bKm;
      }
      // 3. Otherwise fall back to the service's ranking.
      return (orderOf.get(a) ?? 0) - (orderOf.get(b) ?? 0);
    });

    const labels = new Map<SearchResult, NearbyTown>();
    for (const entry of sorted) {
      if (nameMatched(entry)) continue;
      const town = townFor(entry);
      if (town) labels.set(entry, town);
    }

    setResults(sorted);
    setTownLabels(labels);
    setIsLoading(false);
  }, []);

  // Debounced search on query change (parity with the web's per-keystroke search).
  useEffect(() => {
    const handle = setTimeout(() => {
      void runSearch(query);
    }, DEBOUNCE_MS);
    return () => clearTimeout(handle);
  }, [query, runSearch]);

  const handleSelect = useCallback(
    (entry: SearchResult) => {
      Keyboard.dismiss();
      onSelect(entry);
      setQuery('');
      setResults([]);
      setTownLabels(new Map());
      requestSeq.current++;
    },
    [onSelect],
  );

  const clearSearch = useCallback(() => {
    setQuery('');
    setResults([]);
    setTownLabels(new Map());
    requestSeq.current++;
  }, []);

  const renderItem = useCallback(
    ({ item }: { item: SearchResult }) => {
      const type = item.feature_type as FeatureType;
      const displayName = getFeatureDisplayName(item as unknown as Record<string, unknown>, type);
      const variants = (item.name_variants ?? []) as NameVariant[];
      const aliases = getUniqueAliases(variants, displayName);
      const aliasLine = buildAliasLines(aliases).alsoKnownAs;

      const town = townLabels.get(item);
      const townLabel =
        town != null
          ? town.km != null && Number.isFinite(town.km)
            ? `${town.km} km from ${town.name}`
            : `Near ${town.name}`
          : null;

      const regionDisplay = buildRegionDisplay(item.zones, item.regions);

      return (
        <Pressable
          onPress={() => handleSelect(item)}
          accessibilityRole="button"
          accessibilityLabel={`Select ${displayName}`}
          style={({ pressed }) => [styles.resultRow, pressed && styles.resultRowPressed]}
        >
          <View style={[styles.typeDot, { backgroundColor: getColorForType(type) }]} />
          <View style={styles.resultContent}>
            <Text style={styles.resultName} numberOfLines={2}>
              {displayName}
            </Text>
            {aliasLine ? (
              <Text style={styles.resultSubtitle} numberOfLines={1}>
                {aliasLine}
              </Text>
            ) : null}
            {townLabel ? (
              <Text style={styles.resultNear} numberOfLines={1}>
                {townLabel}
              </Text>
            ) : null}
            <View style={styles.resultMeta}>
              <Text style={styles.resultType}>{type}</Text>
              {regionDisplay ? (
                <>
                  <Text style={styles.resultSeparator}>•</Text>
                  <Text style={styles.resultZone} numberOfLines={1}>
                    {regionDisplay}
                  </Text>
                </>
              ) : null}
            </View>
          </View>
        </Pressable>
      );
    },
    [handleSelect, townLabels],
  );

  const showResults = results.length > 0;

  return (
    <View style={styles.container}>
      <View style={styles.inputWrapper}>
        <Search size={16} color={colors.textMuted} style={styles.searchIcon} />
        <TextInput
          value={query}
          onChangeText={setQuery}
          placeholder={placeholder}
          placeholderTextColor={colors.textMuted}
          style={styles.input}
          autoCorrect={false}
          autoCapitalize="none"
          returnKeyType="search"
          accessibilityLabel="Search waterbodies"
        />
        {isLoading ? (
          <ActivityIndicator size="small" color={colors.textMuted} style={styles.trailing} />
        ) : query.length > 0 ? (
          <Pressable
            onPress={clearSearch}
            hitSlop={8}
            accessibilityRole="button"
            accessibilityLabel="Clear search"
            style={styles.trailing}
          >
            <X size={16} color={colors.textMuted} />
          </Pressable>
        ) : null}
      </View>

      {showResults ? (
        <View style={styles.results}>
          <FlatList
            data={results}
            keyExtractor={(item, index) => `${item.waterbody_group}-${index}`}
            renderItem={renderItem}
            keyboardShouldPersistTaps="handled"
            ItemSeparatorComponent={Separator}
            showsVerticalScrollIndicator
          />
        </View>
      ) : null}
    </View>
  );
}

const Separator = () => <View style={styles.separator} />;

const styles = StyleSheet.create({
  container: {
    width: '100%',
  },
  inputWrapper: {
    flexDirection: 'row',
    alignItems: 'center',
    height: 44,
    backgroundColor: colors.panelElevated,
    borderRadius: radius.md,
    borderWidth: 1,
    borderColor: colors.border,
    paddingHorizontal: spacing.md,
  },
  searchIcon: {
    marginRight: spacing.sm,
  },
  input: {
    flex: 1,
    color: colors.text,
    fontSize: typography.body,
    fontWeight: '500',
    paddingVertical: 0,
  },
  trailing: {
    marginLeft: spacing.sm,
    alignItems: 'center',
    justifyContent: 'center',
  },
  results: {
    marginTop: spacing.sm,
    maxHeight: 360,
    backgroundColor: colors.panel,
    borderRadius: radius.md,
    borderWidth: 1,
    borderColor: colors.border,
    overflow: 'hidden',
  },
  resultRow: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    gap: spacing.md,
    paddingVertical: spacing.md,
    paddingHorizontal: spacing.md,
  },
  resultRowPressed: {
    backgroundColor: colors.panelElevated,
  },
  typeDot: {
    width: 12,
    height: 12,
    borderRadius: radius.sm,
    marginTop: 4,
  },
  resultContent: {
    flex: 1,
    gap: 2,
  },
  resultName: {
    color: colors.text,
    fontSize: typography.small,
    fontWeight: '600',
    lineHeight: 18,
  },
  resultSubtitle: {
    color: colors.textMuted,
    fontSize: typography.caption,
    fontWeight: '500',
  },
  resultNear: {
    color: colors.accent,
    fontSize: typography.caption,
    fontWeight: '600',
  },
  resultMeta: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: spacing.xs,
    marginTop: 2,
  },
  resultType: {
    color: colors.textSubtle,
    fontSize: typography.caption,
    fontWeight: '600',
    letterSpacing: 0.5,
    textTransform: 'uppercase',
  },
  resultSeparator: {
    color: colors.textSubtle,
    fontSize: typography.caption,
  },
  resultZone: {
    flex: 1,
    color: colors.textMuted,
    fontSize: typography.caption,
    fontWeight: '600',
    textTransform: 'uppercase',
  },
  separator: {
    height: StyleSheet.hairlineWidth,
    backgroundColor: colors.border,
  },
});
