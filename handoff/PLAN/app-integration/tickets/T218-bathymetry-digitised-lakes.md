# T218 — bathymetry: ship depth figures + contours for the already-digitised lakes

Status: OPEN. Not to be implemented yet (owner 2026-10-27).

Source: owner 2026-10-27 on T201's research (handoff/PLAN/bathymetry/SOURCES.md). The "Bathymetric Maps of Surveyed
Lakes" layer is in the BC Data Catalogue, so it is OGL-BC; no licence email needed.

## Work
- Fetch `WHSE_FISH.BATH_LAKE_BATHYMETRIC_SP` (WFS, EPSG:3005, 137 polygons). It holds 28 waterbodies under 26 names:
  Frog Lakes and South Gataga Lakes are two waterbodies each. Join on `LAKE_FWA_WATERBODY_KEY`.
- Depth figures per lake: `LAKE_MAX_DEPTH_M`, `LAKE_MEAN_DEPTH_M`, `LAKE_VOLUME_CUBM` and survey date/agency. Put them
  in the release's lake slot so they feed the prepared Depth mode (T201).
- Contours: the polygons are depth "slices" (`CONTOUR_DEPTH_M`; 0 = surface; 99999 = a no-data/island slice, so
  drop it). Draw the slice boundaries as lines in the tiles, with the T158 no-access-vs-contour rules. Size diff
  against the T64/T85 budget, though 137 polygons should be small.
- Attribution: OGL-BC line plus "contours generalised; not for navigation". Use the layer's own `KNOWN_LIMITATIONS`
  text.

## Acceptance
Depth mode coloured for the 28 waterbodies. Contours drawn on Fox, Pinchi and Weissener at z12+. Tile size delta
recorded here. One test pins a lake's max/mean depth from the bundle.

Depends: T201 (Depth mode slot), T158 (contour drawing rules). Size: S. Owner: no (licence settled 2026-10-27).
Default: ship all 28; no per-lake review.
