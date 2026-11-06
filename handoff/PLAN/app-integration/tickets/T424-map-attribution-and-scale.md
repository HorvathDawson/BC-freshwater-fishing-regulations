# T424 — Map attribution: better, the same on web and app, and no longer on top of the scale bar

Status: IN REVIEW — cfffecd69; proof pending: emulator/phone look (scale bottom-left, © chip bottom-right, card, with legend open / sheet / landscape; Esri badge on satellite). Web shots T424-shots/. Owner question taken at its default (a © chip that opens a credits card). Not done: bathymetry credit (no bathymetry layer is drawn); More -> About (credits) is the existing Sources and licences page, unchanged. (owner 2026-11-05, 0.6.2 on the phone: "need ticket to improve attribution and the scale and attribution
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

## The startup credit (2026-11-06, chat 3)
The default (a © chip, the OSM wording only in its card) did NOT meet the OSMF attribution guideline, checked
2026-11-06: the credit must show first and may then fold away (on a dismiss, on interaction, or after five seconds) as
long as an "(i)"/© or About still finds it. Now the first map of a session shows "© OpenStreetMap contributors" just
above the chip for 5 s (a tap folds it sooner), then folds to the chip; later maps in the session start folded. Above
the chip, so the bottom-left scale bar is never in reach. Tests: `MapCredits.render.test.tsx` ("shown first, then folds").
