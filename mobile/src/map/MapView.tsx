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
import React, {
  forwardRef,
  useCallback,
  useImperativeHandle,
  useRef,
} from 'react';
import { StyleSheet, View } from 'react-native';
import MapLibreGL, {
  type MapViewRef,
  type CameraRef,
} from '@maplibre/maplibre-react-native';

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

/** Imperative handle so the app shell can drive the camera (e.g. from search). */
export interface MapViewHandle {
  /**
   * Fly to a bbox `[minLng, minLat, maxLng, maxLat]`. Point (zero-area) bboxes
   * center at `minZoom` instead of fitting bounds. Mirrors the web `flyToBbox`.
   */
  flyTo: (bbox: [number, number, number, number] | null, minZoom?: number) => void;
}

interface MapViewProps {
  /**
   * Called with every waterbody/reserve feature under the tap. One feature →
   * resolve directly; several → the shell shows the disambiguation menu.
   */
  onFeatures?: (features: TappedFeature[]) => void;
}

/** Collapse overlapping query hits to unique features (by fid/wbk/name). */
function dedupeFeatures(features: GeoJSON.Feature[]): TappedFeature[] {
  const seen = new Set<string>();
  const out: TappedFeature[] = [];
  for (const f of features) {
    const properties = (f.properties ?? {}) as Record<string, unknown>;
    const sourceLayer = (f as { sourceLayer?: string }).sourceLayer ?? '';
    const key = String(
      properties.fid ?? properties.wbk ?? properties.name ?? `${sourceLayer}:${out.length}`,
    );
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({ sourceLayer, properties });
  }
  return out;
}

export const MapView = forwardRef<MapViewHandle, MapViewProps>(function MapView(
  { onFeatures },
  ref,
): React.JSX.Element {
  const mapRef = useRef<MapViewRef>(null);
  const cameraRef = useRef<CameraRef>(null);

  useImperativeHandle(
    ref,
    () => ({
      flyTo: (bbox, minZoom) => {
        const cam = cameraRef.current;
        if (!cam || !bbox) return;
        const isPoint = bbox[0] === bbox[2] && bbox[1] === bbox[3];
        if (isPoint) {
          cam.setCamera({
            centerCoordinate: [bbox[0], bbox[1]],
            zoomLevel: Math.min(minZoom ?? 12, 15),
            animationDuration: 800,
          });
        } else {
          // fitBounds(ne, sw, padding, duration)
          cam.fitBounds([bbox[2], bbox[3]], [bbox[0], bbox[1]], 48, 800);
        }
      },
    }),
    [],
  );

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
      const features = dedupeFeatures(results?.features ?? []);
      if (features.length > 0) onFeatures?.(features);
    },
    [onFeatures],
  );

  return (
    <View style={styles.container}>
      <MapLibreGL.MapView
        ref={mapRef}
        style={styles.map}
        mapStyle={JSON.stringify(buildMapStyle())}
        onPress={handlePress}
        logoEnabled={false}
        attributionEnabled
      >
        <MapLibreGL.Camera
          ref={cameraRef}
          defaultSettings={{ centerCoordinate: INITIAL_CENTER, zoomLevel: INITIAL_ZOOM }}
        />
      </MapLibreGL.MapView>
    </View>
  );
});

const styles = StyleSheet.create({
  container: { flex: 1 },
  map: { flex: 1 },
});
