/**
 * What an app may ask a map for. Identical on both platforms by construction — a prop that
 * only one renderer honours is how the two maps start diverging.
 */
import type { ViewStyle } from "react-native";
import type { TileEndpoints } from "./runtime-style";

export interface Camera {
  lon: number;
  lat: number;
  zoom: number;
}

export interface MapProps {
  at: TileEndpoints;
  /**
   * Any theme the style defines, not just light/dark. It used to be the pair, so the app
   * had to launder `cvd` into `light` before calling — which meant choosing the
   * colour-blind palette changed the chrome and left the MAP red/green, for the one person
   * who chose it because they cannot tell those apart. A control that silently does half
   * of what it says is worse than one that is greyed out.
   */
  theme: string;
  /** A preset that colours every layer at once. Applied first. */
  view: string;
  /**
   * Per-layer colouring, applied over the view.
   *
   * The design colours streams and lakes independently — Rules on the rivers while the
   * lakes show Stocked — because they answer different questions. A view is the preset;
   * this is what the Layers panel actually sets.
   */
  modes?: Record<string, string>;
  /** Which toggleable layer groups are on. Absent means "leave the style's default". */
  groups?: Record<string, boolean>;
  initial: Camera;
  /** Per-feature values the active colour mode reads, by layer then feature id. */
  data?: Record<string, Record<string, Record<string, unknown>>>;
  onPressFeature?: (layerId: string, featureId: string) => void;
  /**
   * Renderer failures. Not optional in spirit: an empty map and a map whose tiles failed
   * to load look identical, and this app's whole argument is that "we do not know" must
   * never be drawn as anything else.
   */
  onError?: (e: Error) => void;
  /**
   * A raw tapped coordinate, whether or not it hit a feature.
   *
   * Separate from `onPressFeature` because they answer different questions: picking a
   * WATER needs the feature under the finger, picking a POINT needs the place on the
   * ground. Step two of adding a spot is the latter — a gravel bar is not a feature.
   */
  onMapPoint?: (lat: number, lon: number) => void;
  /** Feature ids to draw as selected. Applied by feature-state, never by mutating paint. */
  highlight?: readonly string[];
  /** Gauge points as GeoJSON, drawn as a dot plus its reading. Replaced per feed tick. */
  gauges?: string;
  /**
   * The section ids currently rendered, so a caller can fetch exactly what is on screen.
   *
   * Debounced by the map — it fires after movement settles, not on every frame of a pan.
   */
  onVisible?: (sections: readonly string[]) => void;
  /**
   * A point to mark, e.g. the spot a person just tapped.
   *
   * They are choosing a place they intend to find again, so they have to be able to SEE
   * where the tap landed before it becomes a permanent record — a coordinate printed in a
   * banner is not the same as a dot on the water.
   */
  marker?: { lat: number; lon: number } | null;
  /**
   * Where the camera ended up, whenever the user stops moving it.
   *
   * The caller keeps this and hands it back as `initial` on the next mount, so opening a
   * water's sheet and coming back does not throw you across the province. Reported rather
   * than controlled: a controlled camera fights every pan, because each frame of a drag
   * would round-trip through React.
   */
  onMoved?: (at: Camera) => void;
  style?: ViewStyle;
}
