# T532 — Checklist review for every extent (DFO places and the regulations' own extents), one interface

Status: OPEN

Owner 2026-11-09, on the round-4 mockup (shots: scratchpad t525-r4-*.png; copied to shots/T532-mockup-*.png): "definitely
better. need to make it play nice with entire list + future items when dfo adds new ones. also should have similar quality
(ideally the same interface) for the normal non dfo extents too because eventually we still need to manually go through all
of those items."

## The screen (from the mockup)
- Header: place name, source (DFO region / synopsis page), progress "12 of 287", prev/next, filters (unchecked / has ✗ / ?).
- Big map: the extent now in ONE colour, pending (saved, not built) dashed; numbered pins = rows.
- Rows = the CLAIMS: quoted source phrase → what our extent does (few words) → ✓ / ? / ✗. Hover/tap a row = its pin.
- One decision: "All correct" (enabled when every row is ✓ or ticked) / "Something's wrong" (pick rows + note). After a save:
  one status line. Nothing else visible by default (no ids, no older-wording lists, no legends of 6).

## Rows are GENERATED, never hand-written (scale: all DFO places, new DFO items, every regulation extent)
- From the extent (one model for both sources: `Extent` ops, items, splits/cuts, tributaries, within/outside areas,
  undrawn parts, rest/siblings): water(s) · from (cut) · to (cut / mouth / source) · each extra piece · tributaries ·
  each exception (outside areas/items, closures named in the words) · area membership.
- Each row is paired with the phrase in the source text that names it (cut labels/quotes, `extent_text`,
  `unresolved_locators`, DFO `source_text`); state: ✓ built and matches, ? saved/proposed but its cut is not built, ✗ the
  words say something the extent does not (an exception, a tributary call, a place with no cut).
- Words that no row covers are their own row ("words not checked: …"), so nothing hides.
- New DFO items (live queue) and changed wording arrive as unchecked places; a check is stale when its words or extent
  change (re-check, never silently kept).

## Both sources, one interface
- DFO places (T525 data path, marks, write-through, all R1–R7 guards kept).
- Regulation extents (synopsis rules, `pipeline.atlas.reach` bindings; the review app's existing extents tab): same row
  generator, same screen, same save (a mark), same status line.
- One shared module for rows (Python, with the reach builder: AGENTS 16 — the review app never re-implements a resolver).

## Done when
- Every DFO place and every regulation extent opens in the checklist; counts and filters work over the whole list.
- Tests: row generator over every curated place (no crash, every source phrase covered or listed as unchecked), a planted
  missing exception / tributary mismatch / pending cut each yields ✗ or ?, stale-on-change.
- Shots of the 3 mockup places + 2 regulation extents, owner look.
