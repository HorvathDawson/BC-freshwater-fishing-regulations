# T208 — show rules that hold on an undrawn part of a lake (e.g. François Lake outlet area, "map 1")

Status: OPEN

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
