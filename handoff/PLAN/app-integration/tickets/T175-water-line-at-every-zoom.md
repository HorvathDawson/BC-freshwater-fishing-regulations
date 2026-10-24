# T175 — A water's own line shows at every zoom on its Conditions (and water) map

Status: OPEN

Source: owner 2026-10-24: "the map in conditions tab of a stream sometimes doesn't show the stream because it is too
zoomed out it needs to copy the stream line then display at all zooms maybe?"

## Why
Small streams draw only from their tile minzoom (by stream order), so a water framed to fit a long or remote stretch can
open at a zoom where its own line is not in the tiles yet — the map shows the area but not the water.

## Work
1. The selected water's geometry stays FROM THE TILES (owner rule 2026-10-20: geometry from tiles only, no second
   geometry source): when the water is first drawn at a zoom that has its features, keep a copy of its line(s)
   (querySourceFeatures / the native equivalent, the same pick that frames it) and draw that copy as a highlight layer at
   every lower zoom; drop the copy when the water changes. Where no zoom has been seen yet, open the map no further out
   than the water's own minzoom (frame to fit, clamped to the minzoom), then capture.
2. The copy draws in the water's status/condition colour (the one colouring pathway), under labels and pins, never on
   another water.
3. Web + native; tests: a low-order creek framed at z8 still shows its line; the copy is cleared on change; shots of a
   small creek's Conditions map at the opening zoom, light/dark, phone/desktop.
Ships: app. Size: S–M.
