# T131 — Spot pins on the map: look better, and tap to open the spot

Status: OPEN — owner 2026-10-22: "make spots on the map a) look better and b) clickable to open the spot."

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
