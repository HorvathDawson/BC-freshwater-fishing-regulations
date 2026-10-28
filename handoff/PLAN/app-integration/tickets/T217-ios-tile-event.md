# T217 — iOS: the per-tile event (and stretchable Images) in the MapLibre binding patch

Status: OPEN.

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

Depends: T74, T88 (iOS build). Size: M. Owner: no. Default: Android-only until T88 builds iOS.
