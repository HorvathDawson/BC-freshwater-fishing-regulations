# T275 — Switching Standard ↔ Salmon must not resize the map

Status: DONE 2026-10-29 (owner 2026-10-29: "when moving from standard to salmon the map also resizes weird. it should only be the ui
block changing size and map staying the same").

## Work
The map view keeps a fixed size and camera across a mode switch; only the overlay UI block (legend/bar) changes height and
floats over the map. Find what ties the map's layout to the block (flex sibling, padding/inset change, safe-area recompute)
and decouple it — the block overlays; any camera padding is set without moving the camera. Phone (Android) + web.

## Acceptance
Before/after screen recordings or frame pairs: map pixels outside the block identical across Standard → Salmon → Runs;
no camera jump.

Depends: none. Size: S. Owner: no.

## Outcome
Cause: `Shell.tsx` drew the legend (`MapLegend` / `RunsLegend`) as a flex sibling under the map, so its height (Standard 79,
Salmon 137, Runs 259 px on a phone) changed the map's height on every mode switch. Fix: both now sit in a zero-height
`map-foot` row whose block is absolutely anchored at the bottom and hangs up over the map; the map is full height and fixed.
Verified on web (Playwright, phone 390x844 and desktop 1280x800, Standard -> Salmon -> Standard -> Runs): the map canvas rect is
identical in every frame (390x790 / 1280x746), only the foot changes. Frames: `T275-shots/`. Android: same component tree
(`Shell.tsx`), no emulator run. ui-native suite 1317 pass.
