# T399 — The highlighted part's casing zigzags across the river (water map, phone)

Status: DONE 2026-11-04 in code (adfbe330e; owner, 0.5.0 Play internal: Coquitlam River, Part 1 of 3 — a pink casing and the
yellow line cross each other like two different lines; upstream of the part's end the river is fine).

**Cause:** the water copy (T175, `water-copy.ts`) keeps each section's line from the tiles of the zoom it was first
seen at, and was drawn below ONE ceiling for the whole copy — the deepest zoom any section was kept at
(`WaterCopy.ceiling`, applied as the copy layers' `maxzoom`). A main stem kept at z9 (z9's coarse tile line) was
therefore drawn over the atlas's own line up to the zoom some other section was first seen at, and past z13 (the
atlas's deepest tiles, overzoomed) as well: the copy's casing and line zigzag against the atlas line.

**Fix:** each copied feature carries its OWN ceiling (`copy_until` = its kept tile zoom, capped at `ATLAS_MAX_ZOOM`,
or the view's floor for its layer); the copy layers filter on it (`copyFilter`: `["<", ["zoom"], copy_until]`). On the
phone the ceiling is data now, not a style swap (`Map.native.tsx` no longer swaps the style when it changes; the
`copyCeiling` style option is gone). Tests: `water-copy.test.ts` (T399 cases).

**Left:** the owner's look on the phone (next APK). If MapLibre Native ever ignored `zoom` in the filter, the copy
would show at every zoom again — check Coquitlam part 1 at z12–15.

**Emulator 2026-11-05 (test bed r1105d, T345-04-pick-lowest.png):** Coquitlam part 1 — casing and line on one path, no
zigzag. The owner's phone check still stands.
