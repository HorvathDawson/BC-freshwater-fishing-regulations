# T317 — Audit: nothing on the map drawn as a lagging overlay; a once-over for similar bad patterns

Status: OPEN (owner 2026-10-31: "Make sure no other map data has the [overlay] drawing method … Do a once over on app to
check for bad stuff like that").

## Work
1. Find every map feature drawn as React views positioned over the map (MarkerView/PointAnnotation/absolute views fed by
   project()/onRegionIsChanging, DOM markers on web) — gauge dots, obstacles, labels, badges, highlight rings, my-location,
   in-season pins, run-timing marks, measure tools, etc. Each → map-engine GeoJSON source + layers (native + web), same look,
   taps via layer press. Spots/parking are T316 (another lane) — coordinate, don't duplicate.
2. Once-over for the same class of problem: JS work per camera tick or per frame, setState in map event handlers firing
   continuously, re-creating sources/styles on render, unthrottled listeners, big JSON stringify in hot paths, setInterval
   timers, images decoded on the JS thread, lists without virtualisation over hundreds of rows. Fix the clear ones; list
   the rest with file:line.
3. Before/after pan benchmark (T273/T312 tooling) on the emulator.

Depends: T316. Size: M. Owner: no.
