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
export {};
