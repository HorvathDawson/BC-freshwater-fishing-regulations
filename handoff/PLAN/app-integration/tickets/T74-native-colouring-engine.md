# T74 — Native colouring engine, plus the native map gaps (audit N4, APP-PREP N2-F1)

Status: IN PROGRESS (T74 lane; DONE with its commit).

Source: `map/colouring.ts` has `coloursEachTile = platform === "web"`; `ui/lakeColour.ts`: native "has no per-tile event
yet … keeps its lakes plain"; native still uses the pushed "status" mode (wave item 2.19). T50 refers to "the native
engine ticket" — this is it. N2-F1: hatches (`no_access` and the others) and the gauge pill are plain on native (RN has
no addImage). expo-location is not in apps/mobile, so "At my location" does not work on native.

## Work
- A per-tile source event through the MapLibre Native binding; `coloursEachTile` true on native; delete the pushed
  "status" mode and the documented split.
- Hatch/pill PNGs into the release map-assets sprite, registered through `Images`.
- Add expo-location (pinned; deps.md line).

## Acceptance
Emulator pans: 0 grey frames, numbers recorded (phone numbers come from T77). Lake modes coloured on native; the five
hatch layers draw; native "At my location" works.

Depends: T50 (emulator setup). Heavy (emulator), scripts/heavy.sh. Size: L. Owner: no.


## Owner 2026-10-27
Native gestures and navigation must MATCH the web app (see T50 note, T214).


## Done 2026-10-27
- **Per-tile event on native**: the MapLibre binding is patched (`app/patches/@maplibre__maplibre-react-native@11.5.0.patch`,
  deps.md; Android only — iOS is T217). The atlas `VectorSource` listens to MapLibre Native's `OnTileActionListener`;
  each requested tile's section ids are read off the UI thread from the same local `.pmtiles` (`TileIds.kt`) and sent
  as the device event `MLRNTileIds`; tiles requested before JS asks are replayed on `watchTiles`. `setFeatureStates`
  sends a batch per bridge call.
- **One engine**: `@app/map` `native/useColouring.ts` runs the web's `Colouring` on the phone (status, standing, lake
  modes, salmon) from that event; the style-borne values go through `nativeStyle`'s `values` (a style swap, which
  `Colouring.reset()` follows with a relabel). `coloursEachTile`, the pushed `status` / `salmon_status` modes, and every
  pushed producer in ui/ui-native/core (`statusData`, `pushedStandings`, `pushedRoute`, `perTile`, …) are deleted; the
  phone refuses engine keys in `data` as the web does (`withoutEngineKeys`).
- **Images**: the hatches, the gauge pill and the route pin come from one list (`images.ts`) for both maps; the phone
  gets them as PNG data URIs through `Images`, named per theme (the binding adds a name once); the pill keeps its
  stretch zones (patch).
- **expo-location 57.0.21** (deps.md): "At my location" asks from the reader's tap, foreground only.

## Emulator (n1_pixel, API 35, `EXPO_PUBLIC_COLOUR_PROBE=1`, release r-8ce19c1f2104)
- Pans: 18 one-viewport pans at z9 and z11 (adb swipes, 350–500 ms), the probe sampling 3–5 rendered frames per pan
  from the pan's start to 1.5 s after it settled: **13 pans sampled, 49 frames, 0 grey** (`T74PROBE … "grey":0`).
  5 pans got no sample (the async query was busy). The first view after a cold start is labelled after the first full
  render (the tile watch waits for it: before that the source's view cannot be resolved), so 1–2 sampled frames of
  that very first view are grey; pans are not.
- Map tab coloured by the day's status (own / base / closed), Conditions coloured by flow standing; the hatches draw
  (no_access, eco_reserve and park closures, wetland). Stocked / Depth are greyed on BOTH platforms: the release carries
  no lake facts yet ("turn on by themselves"), so lake modes could not be seen coloured; the engine's survey/stocked
  channels are installed on the phone now exactly as on the web.
- "At my location": offered with "Asks for your location first", the system prompt from the tap, "While using the
  app" → the drop sheet at the emulator's fix (49.15, -121.95).
- Not seen on the emulator: the gauge pill (no gauge label in the walked views; no image-missing error logged).
- Web re-checked after the change (Playwright, :39218): 70/70 rendered stream features carry an id and a profile
  label at Fraser Valley z11; a tap opens the Fraser's regulations; the hatches and the pill are registered.
- Shots: handoff/PLAN/app-integration/T74-shots/ (01 map, 02 z12 after pans, 03 layers sheet, 04 conditions,
  05–07 At my location, web-fv-z11-tap).
- Seen in passing (not T74): Spots → search "Vedder Crossing" said "Could not read what is near this town" on the
  emulator — for T50.
