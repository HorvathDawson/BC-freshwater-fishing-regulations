# T566 — Switching between the maps causes issues sometimes (owner, Pixel 9, build 24)

Status: OPEN.

Source: Owner 2026-11-09 (owner checks page, T512): "it is okay but not perfect. it is good to pan around, lags a tiny bit rarely but switching between the maps causes issues sometimes."

## Work
1. Ask the owner what "issues" is (blank map, stale layer, lag, crash) and between which maps (main map ↔ a water's Map tab, Regulations ↔ Conditions view).
2. Reproduce on the emulator (owner rule: simulate, not on his phone): a scenario that switches maps 20 times and records frame gaps and blank frames.
3. Fix what it shows. Related: T401 (edge pop-in, minor, later), T360 (memory).

## Acceptance
The emulator scenario runs clean; owner says fine on the next build.

Depends: —. Size: M. Owner: no.

## Owner answer 2026-11-11
It is the Trout / Salmon / Runs buttons on the main map: a blank or grey map, lag, and layers "stay wrong for half a second or so". Reproduce in the emulator (20 switches, record frames), then fix.
