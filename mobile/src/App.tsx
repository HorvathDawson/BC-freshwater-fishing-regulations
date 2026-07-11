/**
 * App shell (mobile) — root component and orchestrator. Mobile port of the web
 * app's `Map.tsx`: it owns all overlay state and wires the map, search,
 * disambiguation, info panel, depth-map PDF viewer, source-image viewer,
 * issue-report form, and first-run disclaimer together.
 *
 * Lifecycle:
 *   1. Initialise the on-device SQLite services (downloads the DB once from R2
 *      with progress, then all queries are local).
 *   2. Render the MapLibre atlas + overlays with a tablet/phone responsive
 *      layout (bottom sheet on phones, right-docked panel on tablets).
 *   3. On a feature tap resolve the reach → regulations; multiple hits open the
 *      disambiguation menu. Search flies the camera and opens the same panel.
 */
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  SafeAreaView,
  StyleSheet,
  useWindowDimensions,
  View,
} from 'react-native';
import { StatusBar } from 'expo-status-bar';
import { GestureHandlerRootView } from 'react-native-gesture-handler';

import { MapView, type MapViewHandle, type TappedFeature } from './map/MapView';
import { InfoPanel, type InfoPanelData } from './components/InfoPanel';
import { SearchBar } from './components/SearchBar';
import { DisambiguationMenu } from './components/DisambiguationMenu';
import { Disclaimer } from './components/Disclaimer';
import { PdfViewer } from './components/PdfViewer';
import { SourceImageViewer } from './components/SourceImageViewer';
import { IssueReport, type IssueReportContext } from './components/IssueReport';
import { DataGate } from './components/DataGate';
import { waterbodyDataService } from './data/waterbodyDataService';
import { databaseNeedsDownload } from './data/database';
import { searchService, type SearchResult } from './data/searchService';
import { regulationsService } from './data/regulationsService';
import { getFeatureDisplayName, type NameVariant } from './utils/featureUtils';
import {
  hasAcceptedDisclaimer,
  setDisclaimerAccepted as persistDisclaimerAccepted,
} from './utils/disclaimerStorage';
import { bathymetryUrl, sourceImageUrl, SHARD_VERSION } from './config';
import type { BathymetrySurvey } from './types/regulations';
import { colors, spacing } from './theme/tokens';

/** Width (dp) at/above which we switch to the tablet side-panel layout. */
const TABLET_BREAKPOINT = 768;

type LoadState =
  | { phase: 'checking' }
  | { phase: 'needs-download' }
  | { phase: 'downloading'; progress: number }
  | { phase: 'loading' }
  | { phase: 'ready' }
  | { phase: 'error'; message: string };

/** Rough feature-type from a vector-tile source-layer name (for the dot color). */
function typeFromSourceLayer(sourceLayer: string): string {
  if (sourceLayer.includes('stream')) return 'stream';
  if (sourceLayer.includes('lake')) return 'lake';
  if (sourceLayer.includes('wetland')) return 'wetland';
  if (sourceLayer.includes('manmade')) return 'manmade';
  return 'lake';
}

export default function App(): React.JSX.Element {
  const { width } = useWindowDimensions();
  const isTablet = width >= TABLET_BREAKPOINT;

  const mapRef = useRef<MapViewHandle>(null);

  const [load, setLoad] = useState<LoadState>({ phase: 'checking' });
  const [panel, setPanel] = useState<InfoPanelData | null>(null);
  const [disambig, setDisambig] = useState<TappedFeature[] | null>(null);
  const [pdfView, setPdfView] = useState<{ url: string; title: string } | null>(null);
  const [sourceImage, setSourceImage] = useState<{ url: string; label: string } | null>(null);
  const [issueVisible, setIssueVisible] = useState(false);
  const [issueContext, setIssueContext] = useState<IssueReportContext | null>(null);
  // null = not yet checked; false = must show; true = accepted.
  const [disclaimerAccepted, setDisclaimerAccepted] = useState<boolean | null>(null);

  // Open the DB (downloading if the user has opted in) and load services.
  const runInit = useCallback(async (isDownload: boolean) => {
    setLoad(isDownload ? { phase: 'downloading', progress: 0 } : { phase: 'loading' });
    try {
      await waterbodyDataService.init((fraction) => {
        setLoad({ phase: 'downloading', progress: fraction });
      });
      await searchService.init();
      setLoad({ phase: 'ready' });
    } catch (err) {
      setLoad({
        phase: 'error',
        message: err instanceof Error ? err.message : 'Failed to load data.',
      });
    }
  }, []);

  // Bootstrap: probe for offline data, then either gate on a download prompt or
  // initialise immediately when the DB is already present/bundled.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const needs = await databaseNeedsDownload();
        if (cancelled) return;
        if (needs) {
          setLoad({ phase: 'needs-download' });
        } else {
          await runInit(false);
        }
      } catch (err) {
        if (!cancelled) {
          setLoad({
            phase: 'error',
            message: err instanceof Error ? err.message : 'Failed to load data.',
          });
        }
      }
    })();
    void hasAcceptedDisclaimer().then((accepted) => {
      if (!cancelled) setDisclaimerAccepted(accepted);
    });
    return () => {
      cancelled = true;
    };
  }, [runInit]);

  /** Resolve a tapped tile feature → reach → regulations and open the panel. */
  const resolveFromProps = useCallback(async (feature: TappedFeature) => {
    const props = feature.properties;
    const isReserve = feature.sourceLayer === 'aboriginal_lands';

    if (isReserve) {
      // Reserves have no regulations — just show the OSM-style name.
      setPanel({
        displayName: String(props.name || props.name_en || 'Indigenous Land'),
        featureType: 'lake',
        nameVariants: [],
        regulations: [],
        bathymetry: [],
      });
      return;
    }

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
      bathymetry: reach?.bathymetry ?? [],
    });
  }, []);

  /** Map tap: one feature resolves directly, several open the disambiguation menu. */
  const handleFeatures = useCallback(
    (features: TappedFeature[]) => {
      if (features.length === 0) return;
      if (features.length === 1) {
        void resolveFromProps(features[0]);
        return;
      }
      setDisambig(features);
    },
    [resolveFromProps],
  );

  const disambigOptions = useMemo(
    () =>
      (disambig ?? []).map((f, index) => ({
        id: String(index),
        displayName: getFeatureDisplayName(f.properties),
        featureType: typeFromSourceLayer(f.sourceLayer),
      })),
    [disambig],
  );

  const handleDisambiguationSelect = useCallback(
    (option: { id: string }) => {
      const feature = disambig?.[Number(option.id)];
      setDisambig(null);
      if (feature) void resolveFromProps(feature);
    },
    [disambig, resolveFromProps],
  );

  /** Search result: fly the camera to the feature and open its panel. */
  const handleSearchSelect = useCallback(async (result: SearchResult) => {
    mapRef.current?.flyTo(result.bbox, result.min_zoom);

    const segment = result.segments?.[0];
    const fid = segment?.fids?.[0] ?? null;
    const reachId = fid ? await waterbodyDataService.reachIdForFid(String(fid)) : null;
    const reach = reachId ? await waterbodyDataService.getReach(reachId) : null;

    const regSetIndex = reach?.reg_set_index ?? segment?.reg_set_index;
    const regIds =
      regSetIndex != null ? waterbodyDataService.regIdsForSet(regSetIndex) : [];
    const tribIds = reach?.tributary_reg_ids ?? segment?.tributary_reg_ids ?? [];
    const regulations = regIds.length
      ? regulationsService.getRegulationsForReach(regIds, tribIds)
      : [];

    setPanel({
      displayName: result.display_name,
      featureType: result.feature_type,
      nameVariants: result.name_variants as NameVariant[],
      regulations,
      bathymetry: reach?.bathymetry ?? [],
    });
  }, []);

  const handleOpenPdf = useCallback((survey: BathymetrySurvey) => {
    setPdfView({ url: bathymetryUrl(survey.pdf), title: survey.title });
  }, []);

  const handleOpenSource = useCallback((png: string, label: string) => {
    setSourceImage({ url: sourceImageUrl(png), label });
  }, []);

  const handleReportIssue = useCallback(() => {
    setIssueContext({
      waterbodyName: panel?.displayName,
      waterbodyType: panel?.featureType,
      dataVersion: SHARD_VERSION,
    });
    setIssueVisible(true);
  }, [panel]);

  const handleAcceptDisclaimer = useCallback(() => {
    setDisclaimerAccepted(true);
    void persistDisclaimerAccepted();
  }, []);

  return (
    <GestureHandlerRootView style={styles.root}>
      <SafeAreaView style={styles.root}>
        <StatusBar style="light" />
        {load.phase === 'ready' ? (
          <>
            <MapView ref={mapRef} onFeatures={handleFeatures} />

            <View
              style={[styles.searchWrap, isTablet && styles.searchWrapTablet]}
              pointerEvents="box-none"
            >
              <SearchBar onSelect={handleSearchSelect} />
            </View>

            <InfoPanel
              data={panel}
              isTablet={isTablet}
              onClose={() => setPanel(null)}
              onOpenPdf={handleOpenPdf}
              onOpenSource={handleOpenSource}
              onReportIssue={handleReportIssue}
            />

            <DisambiguationMenu
              options={disambigOptions}
              onSelect={handleDisambiguationSelect}
              onClose={() => setDisambig(null)}
            />

            <PdfViewer
              visible={!!pdfView}
              url={pdfView?.url ?? null}
              title={pdfView?.title}
              onClose={() => setPdfView(null)}
            />

            <SourceImageViewer
              visible={!!sourceImage}
              imageUrl={sourceImage?.url ?? null}
              pageLabel={sourceImage?.label}
              onClose={() => setSourceImage(null)}
            />

            <IssueReport
              visible={issueVisible}
              context={issueContext}
              onClose={() => setIssueVisible(false)}
            />

            <Disclaimer
              visible={disclaimerAccepted === false}
              onAccept={handleAcceptDisclaimer}
            />
          </>
        ) : (
          <DataGate
            phase={load.phase}
            progress={load.phase === 'downloading' ? load.progress : 0}
            message={load.phase === 'error' ? load.message : undefined}
            onDownload={() => void runInit(true)}
          />
        )}
      </SafeAreaView>
    </GestureHandlerRootView>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: colors.bg },
  searchWrap: {
    position: 'absolute',
    top: spacing.md,
    left: spacing.md,
    right: spacing.md,
  },
  searchWrapTablet: {
    right: undefined,
    width: 420,
  },
});
