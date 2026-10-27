# T208 — show rules that hold on an undrawn part of a lake (e.g. François Lake outlet area, "map 1")

Status: PARTIAL

Source: owner 2026-10-26, reviewing François Lake: "Char — 2 per day … in part: the area at the outlet of François
Lake described on map 1" is held as an undrawn part (a note on the water, never colouring it).

## Problem
A lake divided into areas by a synopsis map is deferred (no lake splits yet — memory `lake-splits-deferred`), so the
rule binds the whole lake as an undrawn part. The app must still show the quota CLEARLY as applying only in that area,
not to the whole lake and not buried.

## Work (future, after the review work)
- App: an "in part of this lake" treatment — the quota stated with its area words up front ("Outlet area only (see
  map 1): char 2 a day"), visually distinct from whole-water rules, on the water page and the map sheet.
- Link to the synopsis map that defines the area (printed page → the PDF page/crop; `synopsis/pages` mapping), and
  later a drawn outline when lake splits land.
- List every rule of this shape (undrawn_part on a lake, "described on map N" / "area at the outlet/inlet") with its
  map reference so the treatment covers them all.
- Not in scope: making the lake split itself (deferred).

## Outcome (2026-10-27, app built; no deliver rebuild needed)
- The bundle already carries it: each undrawn-part rule's `not_yet_mapped.map` ({name, page = PRINTED page, cite}, pipeline
  `book_map`) is in the stored rule; `ExportRule.not_yet_mapped` is now typed in core. Nothing added to the deliver contract.
- `core/regs/caveats.ts areaOf`: an in-part item whose part is an outlet/inlet area or that the book draws on a map gets
  `area` = {tag "Outlet area only (see map 1)" (a mapped part without an outlet/inlet reads "Part of this lake only (see map A)";
  no map name: "(see the map)"), map link "See map 1 · p.51 ↗" through `statics.pdfPages` (printed -> PDF)}. `what` is unchanged
  (the golden's words); the tag is drawn before it. `ui-native KeepBlocks CaveatBlock`: such an item has a `restricted` bar on
  the left, the tag above its words and the map link under them - distinct from whole-water rules, which stay as they were.
- Covers the 91 `undrawn_part` rules: 19 outlet/inlet parts + the 6 naming a map (François r1, Shuswap A/B/C, ...). Tests:
  `caveats.test.ts` (François-shaped rule). The drawn outline stays with the lake splits (deferred).

## Outcome 2 (2026-10-27, never behind the fold)
- The in-part fold ("Some spots have their own rules") hid the area rule until opened. Core `inPartOf` now gives the block
  `areas` (its items' distinct area tags) and `areaNow` (an area rule in force on the day); `CaveatBlock` draws the areas on
  the folded head in `restricted` ("ⓘSome spots have their own rules / Outlet area only (see map 1)") and opens unfolded on a
  day the area rule holds (François Nov 21: "No fishing · Nov 15–May 31" with "See map 1 · p.51 ↗" shown at once). Blocks
  without an area are unchanged (golden parity holds). Tests: `caveats.test.ts`, `ui-native/regs/CaveatArea.test.tsx`.
  Shots: `T208-shots/` (web, :39218). The map sheet draws the same Keep tab. Still PARTIAL: the drawn outline waits on lake splits.
