# T280 — Basemap in two stages: raw source → pruned/simplified ship copy (forest simplification for pan speed)

Status: OPEN (owner 2026-10-29: option A from T273, and "could we make it so the basemap we have a pruning stage? if it
doesnt look much different simplifying is always good … basemap in data that we get from pmtile directly then a translation
from that to the one we ship (with the other simplification we already did before). make sure it uses proper range requests").

Source: T273 measured pan hitches on new basemap tiles: 89 frames >50 ms on the drawn map vs 10 with the forest/green-space
layer removed (emulator, indicative).

## Work
1. **Source stage:** the raw Protomaps extract for the BC bbox, fetched straight from the upstream pmtiles with HTTP range
   requests (`pmtiles extract` / equivalent: header + directories + only the needed tile runs; resumable; verified), kept under
   data/ as the untouched source with its build date + upstream URL + hash. Re-fetch only on purpose.
2. **Ship stage:** one deterministic translation source → shipped basemap: today's trim (pipeline/deliver/tiles/basemap.py:
   bbox, zoom cap, roads rebuild, LANDUSE_DRAWN/WATER_LINES_KEPT, dropped layers) PLUS geometry simplification per zoom for
   polygons — landcover (forest), landuse/green space first; tolerance chosen by a visual gate (pixel diff of rendered tiles at
   several zooms over fixed BC extents, threshold in the ticket) and a size/vertex report.
3. Rebuild the basemap from the existing source (no re-fetch needed if the current raw extract is usable), re-run T273's pan
   benchmark (app/tools/testbed `run.mjs pan`), staging upload. Prod upload stays human (command in the Outcome).

## Acceptance
Two-stage build reproducible from the source file; render diff under threshold; pan benchmark frames >50 ms clearly down vs
T273's 89; basemap size before/after; tests pin the stages (basemap-filter tests still pass).

Depends: T273 (measurements). Size: M. Owner: no (prod upload is human).
