# T217 — iOS: the per-tile event (and stretchable Images) in the MapLibre binding patch

Status: DONE (2026-10-27) — commit in the README row.

Source: T74 (2026-10-27). The binding patch (`app/patches/@maplibre__maplibre-react-native@11.5.0.patch`, deps.md) is
Android only: `MLRNVectorSource.kt` reports each requested tile's section ids (`TileIds.kt`, the `MLRNTileIds` device
event), and `setFeatureStates` / `watchTiles` / `nativeTag` and the `Images` stretch zones exist only in Kotlin. With
T74 the pushed colouring is gone, so on iOS the map would draw every water PLAIN (no tile event, no labels) and the
gauge pill would not stretch. iOS has never been built here (T88).

## Work
- The same three additions in the iOS half of the patch (Objective-C/Swift under `ios/`): a tile-request hook on
  MapLibre iOS 6.31 (`MLNMapViewDelegate` tile action, or the observer the SDK exposes), the PMTiles + MVT id reader
  (port of `TileIds.kt`), emitting `MLRNTileIds` with the same payload; `setFeatureStates`, `watchTiles`, `nativeTag` on
  the vector source module; `stretchX` / `stretchY` / `content` on `Images` entries.
- Same JS side: nothing changes in `@app/map` (`native/useColouring.ts` is platform-free above the event).

## Acceptance
iOS simulator: pans with 0 grey frames by the T74 probe (`EXPO_PUBLIC_COLOUR_PROBE=1`, `T74PROBE` lines), lakes
coloured in Stocked / Depth, the five hatches and the pill drawn. Numbers and shots in this ticket.

Depends: T74. Size: M. Owner: no. (Built the simulator dev client ahead of T88; T88 keeps the device/edge-back walk.)

## Done (iOS half of the binding patch)
- `MLRNMapView.m`: `mapView:tileDidTriggerAction:…` (MapLibre iOS 6.31) forwards a RequestedFromCache/Network tile to
  the vector source of that id. `MLRNVectorSource.m`: `watchTiles:tag:emit:` + the per-tile read on a serial queue,
  `setFeatureStates:`; `MLRNPmTilesIds.{h,m}`: line-for-line port of `TileIds.kt` (gzip via NSData's raw-deflate
  inflate after the RFC 1952 header). `MLRNVectorSourceModule.mm`: `setFeatureStates` / `watchTiles`, the event through
  `callableJSModules` → `RCTDeviceEventEmitter` (`MLRNTileIds`, same payload). `MLRNImageLoader.m`: stretch zones →
  UIImage cap insets. No JS change.
- Three iOS-only findings, fixed natively: the atlas `VectorSource` has no `url` prop (it attaches to the style's
  source) → the archive comes from `MLNTileSource.configurationURL`; the first view's tiles are requested BEFORE JS can
  call `watchTiles` and iOS does not re-request them → the source keeps those (≤512) and the watch replays them (before
  this, the whole first view stayed plain: probe worst 1.0); a tab switch flushed states while the new map had not yet
  attached its source ("Source is not attached" toast) → states are kept and set on `addToMap`.
- Reader cross-checked on the Mac against the `pmtiles` JS reader + pbf: 70 tiles z4–z13 over BC, all 16 source-layers,
  identical id counts and sums.
- Deviation: iOS has one stretch box per axis and no separate `content`, so the pill's text box is its stretch box
  (a few px more padding than Android/web). Seen and acceptable in the shot.

## Simulator (iPhone 17, iOS 27.1 sim, `EXPO_PUBLIC_COLOUR_PROBE=1`, release b6b5000e171023c9)
- CocoaPods 1.17.0 installed (brew); `expo prebuild -p ios`; Debug build via scripts/heavy.sh. Pans driven by a scratch
  XCUITest (press-drag, 0.05 s hold, fast), 3 s between pans, at z10 and z11, final build:
  **26 pans, 21 sampled, 49 frames, 0 grey**. The cold start's own camera move (pan 1) is grey before the replay lands,
  as on Android.
- Map tab coloured by status (closed / own / base), the hatches (park, eco reserve, wetland, no-access dots) drawn;
  Conditions coloured by flow standing with the gauge pill (`p5th`) drawn; "At my location" → the pick-the-water
  sheet at the simulator's fix (49.15, -121.95), location pre-granted via `simctl privacy` (the system prompt itself
  is a phone check). Stocked / Depth not seen coloured: the release carries no lake facts (as T74).
- Shots: handoff/PLAN/app-integration/T217-shots/ (01 map, 02 conditions + pill, 03 map after pans, 04 At my location).
