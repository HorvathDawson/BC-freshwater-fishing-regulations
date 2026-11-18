# T848 — The web map lags zooming out over Intata Reach (the Nechako Reservoir lakes)

Status: OPEN (owner 2026-11-18 ~13:15; target 0.8.1, the web site can ship it earlier).

Source: owner 2026-11-18, verbatim: "Here is a trace from zooming out in map on a high detail big lake and it lags out
bad … Be nice to fix this in 0.8.1 if it is still an issue in 0.8.0". In the dialog: where = "In Chrome (staging web)";
which lake = "Intata Reach area with the multiple lakes there".

## What the owner's trace shows (Pixel 9, Traceur, ~6.8 s)

The app was a cached background process (0 ms CPU). Chrome's renderer main thread ran ~100% of a core for ~3.5 s
(2.83 s CPU) with a DedicatedWorker adding 341 ms in bursts; 34 frames in 6.8 s, worst 44 ms. That is the MapLibre GL
JS web map working on the main thread. No URLs or Chrome categories, so no JS function names.

## Next

Reproduce on https://staging.canifishthis.ca: zoom out over Intata Reach, record a Chrome performance trace (CDP), and
name the main-thread work (symbol placement / collision, layout of big lake polygons and labels, style evaluation,
our own JS on camera change). Then fix and measure before/after.
