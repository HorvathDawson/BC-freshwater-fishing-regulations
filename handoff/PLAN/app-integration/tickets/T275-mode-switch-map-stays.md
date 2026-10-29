# T275 — Switching Standard ↔ Salmon must not resize the map

Status: OPEN (owner 2026-10-29: "when moving from standard to salmon the map also resizes weird. it should only be the ui
block changing size and map staying the same").

## Work
The map view keeps a fixed size and camera across a mode switch; only the overlay UI block (legend/bar) changes height and
floats over the map. Find what ties the map's layout to the block (flex sibling, padding/inset change, safe-area recompute)
and decouple it — the block overlays; any camera padding is set without moving the camera. Phone (Android) + web.

## Acceptance
Before/after screen recordings or frame pairs: map pixels outside the block identical across Standard → Salmon → Runs;
no camera jump.

Depends: none. Size: S. Owner: no.
