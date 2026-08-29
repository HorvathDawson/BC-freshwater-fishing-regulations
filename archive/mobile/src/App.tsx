/**
 * App shell (mobile) — root component, port of the web app's `App.tsx` + the
 * data-bootstrap that the web app performs on load.
 *
 * Lifecycle:
 *   1. Initialise the on-device SQLite services (downloads the DB once from R2
 *      with progress, then all queries are local).
 *   2. Render the MapLibre atlas.
 *   3. On a feature tap, resolve the reach → regulations via the ported
 *      services and show them in the InfoPanel.
 */
import React, { useCallback, useEffect, useState } from 'react';
import { ActivityIndicator, SafeAreaView, StyleSheet, Text, View } from 'react-native';
import { StatusBar } from 'expo-status-bar';
import { GestureHandlerRootView } from 'react-native-gesture-handler';

import { MapView, type TappedFeature } from './map/MapView';
import { InfoPanel, type InfoPanelData } from './components/InfoPanel';
import { waterbodyDataService } from './data/waterbodyDataService';
import { searchService } from './data/searchService';
import { regulationsService } from './data/regulationsService';
import { getFeatureDisplayName } from './utils/featureUtils';
import type { NameVariant } from './utils/featureUtils';
import { colors, spacing, typography } from './theme/tokens';
import { useDawnDusk } from './hooks/useDawnDusk';

type LoadState =
  | { phase: 'loading'; progress: number }
  | { phase: 'ready' }
  | { phase: 'error'; message: string };

export default function App(): React.JSX.Element {
  const [load, setLoad] = useState<LoadState>({ phase: 'loading', progress: 0 });
  const [panel, setPanel] = useState<InfoPanelData | null>(null);
  const { times: dawnDusk, updatePosition: updateDawnDuskPosition } = useDawnDusk();

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        await waterbodyDataService.init((fraction) => {
          if (!cancelled) setLoad({ phase: 'loading', progress: fraction });
        });
        await searchService.init();
        if (!cancelled) setLoad({ phase: 'ready' });
      } catch (err) {
        if (!cancelled) {
          setLoad({
            phase: 'error',
            message: err instanceof Error ? err.message : 'Failed to load data.',
          });
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const handleFeatureTap = useCallback(async (feature: TappedFeature) => {
    const props = feature.properties;
    const isReserve = feature.sourceLayer === 'aboriginal_lands';

    if (isReserve) {
      // Reserves have no regulations — just show the OSM-style name.
      setPanel({
        displayName: String(props.name || props.name_en || 'Indigenous Land'),
        featureType: 'lake',
        nameVariants: [],
        regulations: [],
      });
      return;
    }

    // Resolve the tapped tile feature → reach → regulations.
    const fid = props.fid != null ? String(props.fid) : null;
    const wbk = props.wbk != null ? String(props.wbk) : null;
    const reachId = fid
      ? await waterbodyDataService.reachIdForFid(fid)
      : wbk
        ? await waterbodyDataService.reachIdForWbk(wbk)
        : null;

    const reach = reachId ? await waterbodyDataService.getReach(reachId) : null;
    const regIds = reach ? waterbodyDataService.regIdsForSet(reach.reg_set_index) : [];
    const regulations = reach
      ? regulationsService.getRegulationsForReach(regIds, reach.tributary_reg_ids ?? [])
      : [];

    setPanel({
      displayName: getFeatureDisplayName(props, reach?.feature_type),
      featureType: reach?.feature_type ?? 'lake',
      nameVariants: (reach?.name_variants ?? []) as NameVariant[],
      regulations,
    });
  }, []);

  return (
    <GestureHandlerRootView style={styles.root}>
      <SafeAreaView style={styles.root}>
        <StatusBar style="light" />
        {load.phase === 'ready' ? (
          <>
            <MapView onFeatureTap={handleFeatureTap} onRegionChange={updateDawnDuskPosition} />
            <InfoPanel data={panel} onClose={() => setPanel(null)} dawnDusk={dawnDusk} />
          </>
        ) : (
          <View style={styles.center}>
            {load.phase === 'error' ? (
              <Text style={styles.error}>{load.message}</Text>
            ) : (
              <>
                <ActivityIndicator size="large" color={colors.accent} />
                <Text style={styles.loadingText}>
                  Preparing regulations data…
                  {load.progress > 0 ? ` ${Math.round(load.progress * 100)}%` : ''}
                </Text>
              </>
            )}
          </View>
        )}
      </SafeAreaView>
    </GestureHandlerRootView>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: colors.bg },
  center: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    padding: spacing.xl,
    gap: spacing.lg,
  },
  loadingText: { color: colors.textMuted, fontSize: typography.body, textAlign: 'center' },
  error: { color: colors.danger, fontSize: typography.body, textAlign: 'center' },
});
