# T316 — Spot (and parking) pins drawn by the map engine, not as overlays that lag on pan

Status: IN REVIEW — built + tested (c9c18e5be); emulator pan before/after queued (owner 2026-10-31: "The spot icon doesn't render very well on the map when a user pans it lags behind the map?
Shouldn't it just be rendered in the actual map engine instead?? Like using geojson").

## Work
Replace the React-overlay spot pins (and T310 parking P marks, clusters) with a GeoJSON source + symbol/circle layers in
the map style (native MapLibre + web maplibre-gl), icon from the existing icon set as a map image, clustering via the
source's cluster option, taps via the layer's press handler (open the spot / zoom a cluster). No JS per camera tick.
Same look as today. Pan benchmark before/after (T273/T312 tooling).

Depends: T310, T312. Size: S–M. Owner: no.

## Done (c9c18e5be)
- `@app/map` `spot-marks.ts`: the spot / group / parking marks as ONE GeoJSON source (`t316-spots`) with three symbol
  layers over every map layer — the face (a generated RGBA image per size × selected, in the spot tokens: square mark,
  2 px ground frame; selected ringed in the mark after a 2 px gap; 12 / 18 px, ≤ `SPOT_PIN.max`), the count / P (ground
  colour, Noto Sans Medium — the glyph host has no Bold), and the name on a ground strip above (cut to the 120 px strip
  with "…"). Native `Map.native.tsx` mounts it as `GeoJSONSource` + `Layer` + `Images`; web `useSpots` adds it at runtime.
- Grouping stays `@app/core` `spotMarkers` at the zoom the camera SETTLED at (native region-did-change, web `zoomend`) —
  deliberately not the source's `cluster`: that groups by a different rule and loses the tested "zoom that parts it" /
  open-the-list behaviour and the name-collision pass. So no JS per camera tick; a pan moves the layers with the map.
- Taps: the map's own press → `spotUnder` (the 44 px target, platform-free) → `pressSpot`; web map clicks skip a spot's
  (`hitSpot`), as the native press always did. One named button per mark kept for screen readers (1 px, never moved).
- `MapSpots.face` → `MapSpots.colours` (the app's `palette.spot`); `@app/ui-native` `SpotPin` removed.
- Tests: `map/spot-marks.test.ts` (face pixels: frame, ring, gap; source props; names; layers), adapter contract (a tap in
  the target opens the spot, no water, no map point), `Spots.shell.test.tsx` mock updated. Map suite 291/291.
- Note (T317 lane): the web map's later hooks (`useMarks`, `usePins`, this) first see the map on a render AFTER it is
  made; the old `useSpots` caused that render incidentally (`one-engine.render.test.tsx` relies on it). Kept here
  (`useSpots` renders once on its first regroup); a map-ready state in `useMapInstance` would be the real fix.

## Left
- Pan benchmark: `PAN_SPOTS=6 node app/tools/testbed/run.mjs pan` (6 spots round Cultus Lake, pans from "Show on map")
  on the pre-T316 test-bed APK (14:02 build) vs. a fresh `--build`; numbers here when the batch lands.
