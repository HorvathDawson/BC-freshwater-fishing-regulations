# T131 — Spot pins on the map: look better, and tap to open the spot

Status: DONE — 0cc5ea09, review fix (selected face size, spotFace) (lane wf_58c33d34-a73-2; shots in handoff/PLAN/app-integration/T131-shots/; native drawn by
the same code but not run on a device: rides T50). Owner 2026-10-22: "make spots on the map a) look better and b)
clickable to open the spot."

## Built
- `@app/core` spotPins.ts: `SPOT_PIN` (44 px target, 30/36 px face, group under 40 px, names from z11), `spotMarkers`
  (greedy grouping in id order, splitZoom, names de-overlapped, the selected first), `spotPress`, words, `spotBack`.
- `@app/map` `MapProps.spots` + spots.ts `pressSpot`: web draws the marks in its own layer over the canvas (a tap never
  reaches the water); native as view annotations. Old accent `pins` for spots are gone (the town pin stays a pin).
- `@app/ui-native` SpotPin: the Spots icon (Icon table) on the card ground framed in ink, hard slab; selected = larger +
  inverted; group = count over a second face; name above in the boundary marks' label style.
- Tap → that spot (one back layer); back → the map where it was, that pin selected. On a water's map the spot opens
  OVER the water (`WaterRoute.spot`; back is the water's map). The spot's ‹ names where back goes.
- Tokens `color.ui.spot` / `color.ui.spot-ground` (ink / card); theme.test gates the mark dE >= 12 from every status,
  flow and temperature colour, the selection, the accent (you-are-here) and its ground.
- With Spots shown, Show no longer drops the accent marker on the spot: its own (selected) pin marks it.

## Owner questions (defaults built)
- Q-T131-1 Spot pin colour: neutral ink-on-card (selected inverted), the only choice clear of every status, flow and
  stocking colour in all three themes. A hue of its own instead? Default: neutral.
- Q-T131-2 Several spots on ONE point (no zoom parts them): the tap opens the Spots list (out of a water first, when on a
  water's map). Default: the list; alternative: fan them out.
- Q-T131-3 A spot opened from a water's map sits over the water (back returns to it) and its tab bar leaves the water for
  that tab. Default: as built.
- Q-T131-4 (review) The selected spot's name is dropped when a neighbouring face sits where it would go (seen on the
  desktop shot: "Vedder Crossing pool" named, the selected "Tamihi run" not). Place a blocked name below its face
  instead? Default: dropped, as for any name.
- Q-T131-5 (review) On the Conditions map a spot pin sits over a gauge's reading pill and hides part of it (the pin
  wins the tap). Default: as built; alternative: offset the pill or draw spots under gauge pills.

## Today
Saved spots are drawn as accent pins on every map since the Spots switch (Layers; useLayerChoice / spotPins in
@app/ui, Shell passes `pins`). They are plain markers and tapping one does nothing (or falls through to the water under
it). The web map's popup does not refresh a renamed spot until the pin moves.

## Want
- A designed pin: the spot's own mark (Icon table, Lucide), its name label at closer zooms, a clear selected state,
  colours from the theme tokens that stay distinct from water status colours and the you-are-here pin (palette gates).
  Clustering or de-overlap when several spots sit close.
- Tap a pin → open that spot (the same spot screen the Spots tab opens), as one back layer (back returns to the map
  where it was). The tap wins over the water under it.
- Same on web and native; the hit target is ≥ 44 px.

## Acceptance
Whole-value tests through the real Shell (pin list with names; tap → spot screen; back → map); palette gate entries;
light/dark shots at three zooms; check-platform clean.
