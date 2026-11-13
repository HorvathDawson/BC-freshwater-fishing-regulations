# T688 — A wholly tidal water's Map view keeps the last camera and says "not available"

Status: DONE (92849a835)

Source: emulator run on build 29 (app 0.7.7), 2026-11-12. Evidence: `bcfish-session-backup-2026-11-12/apk-077/11-tidal-deas-slough.png`
(Regulations view: the tidal card, correct) and `12-tidal-deas-slough-map.png` / `.txt` (Map view: the camera still on the
Okanagan, the bar "The parts of this water are not available").

## Cause
After T685 Deas Slough is wholly tidal (0 to 8318 m). A tidal piece is never a listed part (T278), so `loadRawWaterGroup`
hands `waterGroup` no part, and `waterMapView` returns null for a group with no part. With no view model the Map view had
nothing to frame (`fit` null, so the map kept the camera of the water before) and printed `WATER_MAP_WORDS.unavailable`,
a failure that was not one. A frame that names the tidal piece (a tap, an address) went further and threw "part N is not
in its group" in `waterMapStartIx`.

## Work
- `loadTidalWater` (data): a water whose every piece is tidal, with its frame (`geoBounds` of the tidal pieces' `part_geo`
  boxes) and sections. `useTidalWaterMap` / `waterMapTidalView` (ui): asked only when the group lists no part.
  `useWaterMap` no longer throws on a part-less group.
- The Map view frames the water (`fit`) and the bar says "Tidal water", the water's name, the first paragraph of the tidal
  sheet (same words as the Regulations card) and "Show regulations for this water" (opens the tidal card). No steps, no parts.
- One load path: a search Open (no part) and a named tidal piece frame and say the same (tested).

## Owner wording (recommended default built in; change on a word from the owner)
- Bar title "Tidal water"; body = `TIDAL_SHEET_WORDS.body[0]`; button "Show regulations for this water".

## Done
- Test `packages/ui-native/src/water/WaterMapView.tidal.test.tsx` (fails before the fix: "not available", no fit).
- Left: emulator check on the next APK.

Depends: T685. Size: S.
