# T175 — A water's own line shows at every zoom on its Conditions (and water) map

Status: DONE — lane-T175, e8999343 (code + tests, with T176); this ticket update follows it. App only: no rebuild, no
deliver, no new data — the geometry is the tiles', the box the release's `waterBox` (already shipped).

Source: owner 2026-10-24: "the map in conditions tab of a stream sometimes doesn't show the stream because it is too
zoomed out it needs to copy the stream line then display at all zooms maybe?"

## Why
Small streams draw only from their tile minzoom (by stream order), so a water framed to fit a long or remote stretch can
open at a zoom where its own line is not in the tiles yet — the map shows the area but not the water.

## Work
1. The selected water's geometry stays FROM THE TILES (owner rule 2026-10-20: geometry from tiles only, no second
   geometry source): when the water is first drawn at a zoom that has its features, keep a copy of its line(s)
   (querySourceFeatures / the native equivalent, the same pick that frames it) and draw that copy as a highlight layer at
   every lower zoom; drop the copy when the water changes. Where no zoom has been seen yet, open the map no further out
   than the water's own minzoom (frame to fit, clamped to the minzoom), then capture.
2. The copy draws in the water's status/condition colour (the one colouring pathway), under labels and pins, never on
   another water.
3. Web + native; tests: a low-order creek framed at z8 still shows its line; the copy is cleared on change; shots of a
   small creek's Conditions map at the opening zoom, light/dark, phone/desktop.
Ships: app. Size: S–M.

## As built
- **The copy** — `app/packages/map/src/water-copy.ts` (platform-free). `WaterCopy` keeps, per section id, the pieces the
  map's own pick returned (tile clips merged into one Multi-geometry, the tile's `ord`/`name` kept so the stream's width
  expression reads the same), the FIRST time a zoom had it; a later pass replaces a section only when it saw more of it
  (half off-screen the first time). Only the water's ids, only a stream line or a lake polygon (a wetland is not copied).
  `reset(ids, atlas)` drops everything when the ids — or the archive they are ids in — change.
- **The one pick** — `MapRenderer.litFeatures(ids)` on both renderers (`adapters/web.ts`: `querySourceFeatures`;
  `adapters/native.ts`: the binding's `VectorSource.querySourceFeatures` with an id filter). `waterBox` (the search fit's
  refinement) is now its extent, so framing and copying are literally one pick; `parity.test.ts` still holds both.
- **What is copied** — `copyIds(highlight, water, waterLine.sections)`: every map copies what it lights (search's water,
  the water map's part + rest, a route map's chain); a map given `MapProps.waterLine` also holds that water's own sections
  (`useWaterLine` in @app/ui: the named water's `sections`, or the tapped section of an unnamed reach).
- **Drawn** — a runtime GeoJSON source `water-copy` with three layers placed DIRECTLY UNDER the water's own
  (`runtime-style.ts`): `water-copy-fill` under `lake`, `water-copy-casing` + `water-copy-line` under the selection casing
  and `stream`; so where the atlas draws the water it wins, and every label and pin is above. Each layer's `maxzoom` is
  the copy's CEILING (`WaterCopy.ceiling`): the deepest kept tile zoom, or the layer's own floor in this view (the
  Conditions field stands rivers down below z8), whichever is deeper — the copy stops where the atlas takes over (web:
  `setLayerZoomRange`; native: `nativeStyle({ copyCeiling })`, a style swap only when the ceiling moves).
- **One colouring pathway** — the adapter paints the copy in the SAME `paint` call as the stream and the lake
  (`copyLinePaint`: the stream's paint for its mode, width floored at `width.stream.highlight` — it is drawn as a
  highlight; `copyFillPaint`: the lake's fill; the casing is `casingPaint`, shown on `selected`). Every per-section
  feature-state the map writes for one of the water's sections is written to the copy too: `mirrorSink` wraps both
  renderers' state sink (pushed data, `selected`/`water`/`part`), and the web engine's label target mirrors its
  labels (`standing`, `p`, survey/stocked); a newly kept section is first given what its atlas feature holds
  (`getFeatureState`, both renderers). One write per state per section (the atlas gets one per per-section layer).
- **The clamp** (`probeZoom`, `zoomAround`) — on a `waterLine` map, each settle where NONE of the water's own sections
  is found or kept (a lit route down the big river does not count) steps in one whole zoom about `waterLine.at` (the tap,
  else the middle of the water's box, else the centre) — the anchor stays where it is on screen — until the tiles carry
  it, at most to z14 / 10 steps, never after the reader's own pan or zoom (web: `movestart` with an `originalEvent`;
  native: `userInteraction`). Then the copy is kept, so zooming back out still shows the water.
- **Framing (found on the way)** — a water opened WITHOUT a tap (search, list, link) framed only its gauges: Nursery
  Creek's one donor is 18 km away, so the route map opened at z12.5 on the gauge and the creek was not even in the frame
  (shot `before-nursery-desktop-light-opening.png`). `DonorViewOptions.place` = the release's `waterBox(item)` stands in
  for the place when there is no tap (camera and Fit both), no pin invented (`useWaterBox`; `WaterConditions` →
  `ConditionsPanel` → `DonorPanel`).
- **Wired**: route map (`DonorPanel` → `DonorMap` → `MiniMap` → `Map`, `waterLine = { sections, at }`), the water's Map
  view (`WaterMapView` → `MapScreen`, `waterLine = part + rest`), and every other map's highlight gets the copy (no clamp:
  search's own refine fit frames it).

## Tests (whole values; 13 mutations, each fails its test — `scratchpad/mutate.py`)
- `map/src/water-copy.test.ts` — kept collection whole (tile pieces merged, the river and the marsh not kept); first zoom
  kept / fuller pass replaces; reset on another water and on another atlas; ceilings; probe steps and every refusal;
  `zoomAround`; `mirrorSink` (one write across layers, never another water, removes only what it holds, after a style swap);
  the ticket's case "a low-order creek framed at z8" run end to end (z9→z12, kept, ceiling 12 so it draws at z8); the
  copy's paint IS the stream's/lake's/casing's (adapter, same call).
- `map/src/water-copy.render.test.tsx` — the real `Map.web.tsx` over a recording GL: framed at z8 it eases z9…z12 about
  the place read, keeps the creek, gives it the atlas's states, stops at z12; engine labels after capture reach the copy
  (never the river's); another water clears copy + states first; THE WATER decides (a lit route found at z8 does not stop
  it); the reader's pan stops it; a lit-only map never moves the camera.
- `ui/src/panel.place.test.ts` — no tap + the water's box frames gauge AND creek (Fit too, no pin); a tap ignores the box.
- Updated: `layer-order` (copy under the water lines, fill under the lake), `profile-style` (14 layers validated),
  `native-style` (the copy source reads feature-state), `adapter-contract` / `one-engine` fakes gain `loaded()`.
- Mutations M1–M7, M10–M13 (probe on any lit feature; take another water; reset keeps; mirror without dedupe; ceiling
  without floor; line not floored; copy over the stream; frame ignores the box; engine labels not mirrored; probe
  ignores the reader; new water does not clear the source) — all fail.
- `REGS_STORE_REQUIRED=1 pnpm -C app check` via `scripts/heavy.sh appcheck 30`: green, 8,156 passed / 24 skipped.

## Shots (own Expo port 8231, release data from :39217) — `handoff/PLAN/app-integration/T175-shots/`
Nursery Creek (gnis:24108, 4.6 km², magnitude-small: in the tiles from z13), opened from a link (no tap):
`after-nursery-desktop-light-opening.png` (opens at z13 with the creek on the map), `after-nursery-desktop-light-z10.png`
(zoomed out: the creek's copy still drawn, cased, off the Chilliwack), `after-nursery-desktop-dark-opening.png`,
`after-nursery-phone-light-opening.png`, `after-nursery-phone-dark-opening.png`, `after-nursery-phone-dark-z10.png`.
BEFORE (`before-nursery-desktop-light-opening.png`, `before-nursery-desktop-light-z10.png`) were taken in the SAME build
with the copy layers hidden and the camera put back to the pre-T175 opening (z12.5 on the gauge, measured in this session
before the framing change) — an emulation of the old behaviour, not a build of the old commit.

## Adversarial review
- **Drawn on another water?** Only ids in the copy set are kept (M2); the mirror writes only those ids (M4/test). The
  copy set on a route map includes the chain (the route down the Chilliwack) — that is what the map lights already.
- **Stale after switching water?** The copy is keyed on the ids + atlas; a new key removes all its states and empties the
  source before anything else (render test, M13). A route map is remounted per camera key anyway.
- **A lake?** Kept as a fill under the lake's own, lake paint. A wetland (or a marsh a stream flows through) is NOT copied
  (owner question 3). A lake divided into closure parts owns no section: its parts' ids are what is lit.
- **A long river** (most sections: 381, max in the bundle) — copied only as far as loaded tiles show; it is in the tiles
  from z4–8 anyway, so the probe never runs and the ceiling sits at the view floor.
- **No-access sections** wear the stream's paint, which hides a no-access section under T165's dots; the copy has no dots
  layer, so below the atlas zoom a no-access stretch of the copied water is a gap (owner question 4).
- **Copy under the atlas between its true minzoom and the ceiling**: the same line twice, the atlas on top; with the
  water-map "dim" cue the dimmed rest reads slightly less dim there. Accepted (ceiling, not per-feature).
- **Native parity — NOT RUN.** `Map.native.tsx` gets the same copy (GeoJSONSource ref + `setFeatureState`, `getFeatureState`
  seeding, ceiling in the style, probe on `onDidFinishRenderingMapFully`, reader by `userInteraction`) and typechecks,
  and `nativeStyle` is validated with the copy layers; it has not been run on a device or emulator (no Android run in this
  lane). The binding's `querySourceFeatures` id filter is the one `waterBox` already used.
- The web dev server's watcher did not see edits made inside `.claude/worktrees/…`; Expo was restarted with `--clear` to
  take each change (my own PID only).

## Owner questions (defaults built)
1. **Opening zoom when the water appears only deep (z13 for Nursery Creek)** — default: the map steps in about the place
   read until the creek is in the tiles, then keeps it; the far gauge may leave the frame (Fit brings both back, the
   creek still drawn). Alternative: keep the fit-to-gauges zoom and rely on the copy alone (needs a zoom that had it).
2. **No tap → frame the water's box with the gauges** (release `waterBox`, a box, not geometry). Default: yes. Alternative:
   frame the gauges only, as before.
3. **Wetlands not copied** (a marsh water, a marsh a stream flows through) — default: not copied. Alternative: copy them
   with the wetland's paint.
4. **No-access stretches in the copy** — default: hidden as on the atlas (no dots below the atlas zoom). Alternative: draw
   the copy's no-access stretch with the dots too.

## Left (merge 2026-10-24)
- The copy leaves no-access stretches as a gap below the tiles' zoom; draw them dotted grey like the tiles (T165).
