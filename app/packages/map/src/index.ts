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
export type { MapProps, Camera, MapSpots } from "./map-props";
export { type TileEndpoints } from "./endpoints";
export { runtimeStyle } from "./runtime-style";
export { baseAdapter, type MapHandle } from "./adapters/contract";
/** The catalogue a Layers menu is built from. Groups and views come from the generated
 *  style, never from a hand-written list in a component — that is how the two apps end up
 *  offering different layers. */
//  (T282) the colours a legend reads without the map (salmon, gear, runs, temperature), the layer switches and the
//  imagery endpoints are @app/map/colours' alone
export { STYLE_META, FIELD_HANDOVER_Z, resolveTheme, waterStatusColour, tidalColours, notFishingColours } from "./style";
/** The one palette, read: a token's colour, its alpha, and MapLibre's controls from it. */
export { colour } from "./chrome";
/** The route overlay (owner 2026-10-21): routes copied out of the tiles, pins, and their words' slots. */
export { type TraceProp } from "./trace-overlay";
export { pressSpot } from "./spots";
export { MapLab } from "./MapLab";
export { LAB_ON } from "./lab";
export { GEOMETRY, mapChrome } from "./chrome";
