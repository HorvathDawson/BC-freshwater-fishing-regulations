# T208 — show rules that hold on an undrawn part of a lake (e.g. François Lake outlet area, "map 1")

Status: DONE 2026-10-29 — display, map link and list built and in r-62e2f3a8c81e; the drawn outline is T281
(waits on lake splits, deferred by the owner).

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

## Outcome 3 (2026-10-29 overnight, the list + a wording fix)
- THE LIST (work item 3), measured from `data/generated/regs/ui-rules-export.json`: 83 rules carry `not_yet_mapped`; 30
  have an area the app tags (an outlet/inlet part, or a `not_yet_mapped.map`), and NO rule whose part or words cite a map
  is left without a tag (0 gaps). Mapped: François r1 (map 1, p.51); Shuswap r1/r2 (map A), r3 (map B), r4 (map C), r5,
  r5x, r11x (p.28, unnamed); Mahood r1–r4 (p.28); Canim r1 (p.42). Outlet/inlet only: Ruby r1, Leighton r1/r2, Loon r2,
  Mahood r5–r8, Mitchell r1, Buckinghorse r1, Helene r1, Klahowya r1, Silverthorne r1, Letain r1, Rainbow Lakes r1,
  Wolverine r1, Mabel r1.
- FIXED: an outlet/inlet rule with NO book map read "Outlet area only (see the map)" — a map that does not exist (17 of
  the 30). Now "Outlet area only" (core `WORDS.AREA_ONLY(lead, false)`); a book map without a name still says "(see the
  map)" with its page linked. Test: `caveats.test.ts` (Ruby / inlet / Mahood shapes).
- Still PARTIAL: the drawn outline waits on lake splits (deferred by the owner, memory `lake-splits-deferred`).

## Close-out (2026-10-29)
Every work item is built: the in-part treatment (tag up front, `restricted` bar, never behind the fold), the synopsis
map link, the list (30 tagged, 0 gaps). r-62e2f3a8c81e carries it: 83 rules with `not_yet_mapped`, 13 with a book map
(François r1: map 1, p.51). Tests: `caveats.test.ts` + `CaveatArea.test.tsx` 20/20. The one item left — a drawn outline
of the area — needs a lake split, which the owner deferred (memory `lake-splits-deferred`, Q142): moved to T281.
