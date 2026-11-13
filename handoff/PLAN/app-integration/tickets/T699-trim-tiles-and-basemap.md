# T699 — Trim the map tiles and basemap

Status: POSTPONED (deferred, owner 2026-11-13, verbatim: "Then the last 2 items for after closed testing should be moved to deferred with stocked lakes and bathy map etc.." Moved to the deferred group with stocked lakes and the bathymetry map; earlier answer "After closed testing").

Source: T686 candidate 8 / group B question 4 (shrink shipped data): the atlas tiles and basemap are about 412 MB on disk
(186.9 MB pmtiles + 215 MB basemap dir at the 2026-11-12 survey). Owner's earlier answer: "After closed testing"; now deferred.

## Scope (when picked up)
Zoom ranges, attribute columns per layer, and simplification per zoom for the tiles and the basemap; measure each saving
on the live release first, against what the map loses (detail at the zooms anglers use, T202 basemap look).

## Rules
- No shipped change and no tile rebuild until the owner un-defers it. Tiles and bundle share a vintage; promote file by file.

Depends: T686 (survey), closed testing started. Size: M. Owner: yes (the pick). Default: tiles and basemap stay as built.
