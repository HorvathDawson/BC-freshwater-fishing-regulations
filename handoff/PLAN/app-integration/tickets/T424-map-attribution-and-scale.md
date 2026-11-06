# T424 — Map attribution: better, the same on web and app, and no longer on top of the scale bar

Status: OPEN (owner 2026-11-05, 0.6.2 on the phone: "need ticket to improve attribution and the scale and attribution
overlap right now on map in app").

Today (`app/packages/map/src/Map.native.tsx`): MapLibre's own attribution button at `{bottom: 8 + insetBottom, right: 8}`
and our scale bar (`metricScale`) also at the bottom right, so they overlap. The map UI rule (memory map-ui-layout-rule,
T367): top = search + day; legend and its control in the bottom bar; square corners; cards, no loose text.

## Work
1. **The overlap:** one place for each, on every map that shows them (the main map, the water page's small map,
   Run timing, web). E.g. the scale bar bottom-left, attribution bottom-right, both above the bottom bar and the
   system inset; check with the legend open, a sheet up, landscape, and the web layout.
2. **Better attribution:** one credits line or sheet in our own style (square card, our type), not the binding's
   default "i". It names every source on screen, and only those: OpenStreetMap contributors + Protomaps (basemap),
   Province of B.C. FWA (streams and lakes), Esri (imagery, when on), and the layer sources when shown (run timing:
   Pacific Salmon Foundation + DFO, OGL – Canada; obstacles; bathymetry). It is one tap from the map, and the full
   list is in More → About (credits). Check each licence's wording requirement (OSM: "© OpenStreetMap contributors",
   visible on the map).
3. **Web and app the same** (owner 2026-11-05: "the attribution on web and app do not match. they are also formatted
   different"). Today each renderer draws its own control from the sources' `attribution` strings (`map/src/credits.ts`,
   the Esri string in `style.json`, `gaugeDots.ts` for ECCC) — the web's HTML control and the native "i" differ in
   what they list and how. One list from one source (`credits.ts`, by layer shown), drawn by our own component on both,
   so the wording, order and look match; a test pins web == native for the same layers on.
4. Screenshots before/after on the emulator (phone + web), and a test that the two never overlap at the narrowest
   width.
Owner question when started (default in brackets): the credits as a small text line on the map [a "©" chip that opens
a credits card].
Size: S–M. Owner: phone look.
