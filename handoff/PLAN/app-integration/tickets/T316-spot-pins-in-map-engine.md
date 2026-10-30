# T316 — Spot (and parking) pins drawn by the map engine, not as overlays that lag on pan

Status: OPEN (owner 2026-10-31: "The spot icon doesn't render very well on the map when a user pans it lags behind the map?
Shouldn't it just be rendered in the actual map engine instead?? Like using geojson").

## Work
Replace the React-overlay spot pins (and T310 parking P marks, clusters) with a GeoJSON source + symbol/circle layers in
the map style (native MapLibre + web maplibre-gl), icon from the existing icon set as a map image, clustering via the
source's cluster option, taps via the layer's press handler (open the spot / zoom a cluster). No JS per camera tick.
Same look as today. Pan benchmark before/after (T273/T312 tooling).

Depends: T310, T312. Size: S–M. Owner: no.
