/**
 * @app/map — the ONE place that knows maplibre-gl and maplibre-react-native differ.
 *
 * The app never imports either directly. It renders <Map/> from here, and this
 * package picks the adapter. Both adapters consume the SAME generated style JSON,
 * so a layer change is one regenerated artifact rather than two edited component
 * trees — the main lever on "we will change how layers are shown".
 *
 * Keep this surface small: camera, tap, feature-state highlight. Everything the
 * style spec can express belongs in the style, not here.
 */
export { Map } from "./Map";
export type { MapProps, Camera, MapChrome } from "./map-props";
export { runtimeStyle, type TileEndpoints } from "./runtime-style";
export { baseAdapter, DEFAULT_VIEW, type MapAdapter, type MapHandle } from "./adapters/contract";
/** The catalogue a Layers menu is built from. Groups and views come from the generated
 *  style, never from a hand-written list in a component — that is how the two apps end up
 *  offering different layers. */
export { toggleableGroups, views, hiddenLayers, STYLE_META, resolveTheme, themeNames,
         waterStatusColour,
         type LayerGroup, type MapView, type Tokens } from "./style";
export { pillImage, type PillImage } from "./pill";
/** The one palette, read: a token's colour, its alpha, and MapLibre's controls from it. */
export { colour, amount, translucent, mapChrome } from "./chrome";
