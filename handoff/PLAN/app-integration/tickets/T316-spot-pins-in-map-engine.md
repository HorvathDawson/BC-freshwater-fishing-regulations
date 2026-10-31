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

## Pan benchmark (emulator 2026-10-30, `PAN_SPOTS=6 node app/tools/testbed/run.mjs pan`: 6 spots round Cultus Lake, pans from "Show on map")
| build | round | map surface frames | >20 ms | p99 / max ms | UI (gfxinfo) frames | janky | slow draw |
|---|---|---|---|---|---|---|---|
| before (14:02, view annotations) | circling | 751 | 35 | 33.2 / 35.8 | 333 | 5.1 % | 17 |
| before | travelling | 793 | 28 | 33.1 / 35.0 | 329 | 4.9 % | 16 |
| after (T316 + T317 + T314/T315) | circling | 763 | 30 | 33.1 / 38.2 | 203 | 53.7 % | 107 |
| after | travelling | 797 | 36 | 33.1 / 36.9 | 204 | 56.4 % | 113 |
| 0.4.0 HEAD, PAN_SPOTS=0 (start view, 10-31) | circling | 998 | 136 | 49.8 / 198.6 | 197 | 55.8 % | 107 |
| 0.4.0 HEAD, PAN_SPOTS=0 | travelling | 1192 | 45 | 34.1 / 200.2 | 210 | 48.1 % | 101 |
| 0.4.0 HEAD, PAN_SPOTS=6 (Cultus, 10-31) | circling | 762 | 25 | 33.5 / 49.4 | 203 | 55.7 % | 112 |
| 0.4.0 HEAD, PAN_SPOTS=6 | travelling | 779 | 23 | 32.9 / 39.7 | 201 | 55.2 % | 110 |
**Jank answered (10-31): not the spots.** With no spots at all the UI thread shows the same ~50–56 % janky / ~105 slow
draws, and so did every pre-T316 pan with no spots (t280-before/after runs 10-29/30: 53–78 %, ~110 slow draws). The 5 %
"before" was the outlier: the view-annotation pins made the UI thread draw ~130 extra cheap frames per round, which
diluted the ratio. The map surface (what the reader sees move) is as good or better with the layers.
Read: the map's own surface is unchanged (it never waited on the pins); the app's UI thread draws ~40 % fewer frames
during a pan (no views moved per frame) — the lag the owner saw cannot happen to layers. BUT the UI frames that remain
are mostly flagged "slow issue draw commands" (5 % → 55 % janky). Not attributed: the after build also carries T317's
conversion and other lanes' work. Next: the same pan with PAN_SPOTS=0 on the after build splits the spots' share.

## Left
- Owner: look at the marks on a phone (same look as `SpotPin`; names in Noto Sans Medium, the glyph host has no bold).
- The jank question above (one more pan run, PAN_SPOTS=0 vs 6, same build).
