# T452 — A fling's glide stutters as it goes (Pixel 9, 0.6.6)

Status: OPEN (owner 2026-11-06, 0.6.6 on Play internal, Pixel 9).

Owner: "the panning is a bit better, zoomed out and in are similar now; still feels a little stuttery — if you flick
the screen the way it moves stutters a bit as it goes."

## Work
1. Measure the glide's frame pacing on the NORMAL emulator (not only the stressed one): on a fast phone a stutter can be
   uneven camera steps rather than slow frames — per-frame camera deltas and timing (MapLibre's fling animator vs the
   frame callbacks), work on the UI/JS threads during the glide (the binding's per-frame events, tile events, renders).
2. Fix what the measurement names; before/after numbers on the same script.
Related: T312 (fling length: the binding's `tuneFling`), T420 (pan lag). Size: M. Owner: a phone feel check after.
