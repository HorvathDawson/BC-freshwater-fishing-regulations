# T299 — Water names on the map again; basemap place names by importance and BC only

Status: OPEN (owner 2026-10-30).

Source: owner: "make it so stream names show up on map still? Archived version had the display names show up on the vector
tile of streams and same for lakes etc. this is important to add back. Also the density of the OSM pmtile town names /
place names on the base tile feels off it puts a ton of names on the screen but should show most important first and hide
a lot until zoomed in a lot more. Also the names of places etc outside of BC (the BC polygon) should have been filtered out
of the base tile."

## Work
1. **Water names:** stream and lake labels drawn from our own atlas tiles (display name, incl. T224 accepted names), along
   the line for streams, centred for lakes; priority by size (stream order / lake area) so big waters label first and small
   ones only when zoomed in; collision-aware; theme tokens; match the archived app's behaviour (archive/webapp) as the
   reference. Tile attribute added in the tiles export if missing (tiles + bundle same vintage).
2. **Place-name density:** basemap `places` shown by importance (OSM `population_rank` / `pmap:min_zoom` / kind: city >
   town > village > hamlet > locality), far fewer at low zoom, more as you zoom in; style-side filter + symbol-sort-key.
3. **Outside BC:** drop `places` (and other labels: roads names, POIs if any) outside the BC polygon in the basemap ship stage
   (T280 two-stage build: add to the prune stage), so no Washington/Alberta/Alaska names. Basemap rebuild via heavy.sh.

## Acceptance
Screenshots at z7/z9/z11/z13 before/after (Lower Mainland + a border area); no labels outside BC; water names readable.

Depends: T280. Size: M. Owner: no.
