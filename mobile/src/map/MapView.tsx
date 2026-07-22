/**
 * MapView — MapLibre Native port of the web app's `Map.tsx` shell.
 *
 * Renders the fishing atlas from the same remote PMTiles the web app uses and
 * reports taps on waterbody/reserve features back up to the app shell so the
 * InfoPanel can resolve regulations via the on-device SQLite services.
 *
 * NOTE: `@maplibre/maplibre-react-native` requires a custom dev client (it is a
 * native module and does not run in Expo Go). See `mobile/README.md`.
 */
import React, { useCallback, useEffect, useRef } from 'react';
import { StyleSheet, View } from 'react-native';
import MapLibreGL from '@maplibre/maplibre-react-native';

import { buildMapStyle } from './styles';

// BC-wide starting camera (matches the web app's default view).
const INITIAL_CENTER: [number, number] = [-122.5, 52.5];
const INITIAL_ZOOM = 4.5;

/** Tile layers whose taps we care about (waterbodies + reserves). */
const QUERYABLE_LAYERS = [
  'streams',
  'lakes-fill',
  'manmade-fill',
  'wetlands-fill',
  'admin_aboriginal_lands-fill',
];

export interface TappedFeature {
  sourceLayer: string;
  properties: Record<string, unknown>;
}

interface MapViewProps {
  /** Called when the user taps a waterbody/reserve feature. */
  onFeatureTap?: (feature: TappedFeature) => void;
  /** Called when the map finishes panning/zooming, with the new center [lat, lng]. */
  onRegionChange?: (lat: number, lng: number) => void;
}

export function MapView({ onFeatureTap, onRegionChange }: MapViewProps): React.JSX.Element {
  const mapRef = useRef<MapLibreGL.MapView>(null);

  const handlePress = useCallback(
    async (e: GeoJSON.Feature) => {
      const map = mapRef.current;
      if (!map || !e.geometry || e.geometry.type !== 'Point') return;
      const [lng, lat] = e.geometry.coordinates as [number, number];

      const point = await map.getPointInView([lng, lat]);
      const results = await map.queryRenderedFeaturesAtPoint(
        point,
        undefined,
        QUERYABLE_LAYERS,
      );
      const hit = results?.features?.[0];
      if (hit) {
        onFeatureTap?.({
          sourceLayer: (hit as { sourceLayer?: string }).sourceLayer ?? '',
          properties: (hit.properties ?? {}) as Record<string, unknown>,
        });
      }
    },
    [onFeatureTap],
  );

  const handleRegionDidChange = useCallback(
    (feature: GeoJSON.Feature<GeoJSON.Point>) => {
      const [lng, lat] = feature.geometry.coordinates as [number, number];
      onRegionChange?.(lat, lng);
    },
    [onRegionChange],
  );

  // Seed the initial camera position (dawn/dusk needs a starting point even
  // before the user pans).
  useEffect(() => {
    onRegionChange?.(INITIAL_CENTER[1], INITIAL_CENTER[0]);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <View style={styles.container}>
      <MapLibreGL.MapView
        ref={mapRef}
        style={styles.map}
        styleJSON={JSON.stringify(buildMapStyle())}
        onPress={handlePress}
        onRegionDidChange={handleRegionDidChange}
        logoEnabled={false}
        attributionEnabled
      >
        <MapLibreGL.Camera
          defaultSettings={{ centerCoordinate: INITIAL_CENTER, zoomLevel: INITIAL_ZOOM }}
        />
      </MapLibreGL.MapView>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  map: { flex: 1 },
});
