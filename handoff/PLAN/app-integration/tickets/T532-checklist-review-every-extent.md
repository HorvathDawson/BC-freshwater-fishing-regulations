# T532 — Checklist review for every extent (DFO places and the regulations' own extents), one interface

Status: IN PROGRESS

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

## One water at a time (owner 2026-11-09)
"a water has all of the locations for it together … we want to do an entire river / waterbody at one time". The queue and
prev/next are grouped by water: every place on a water (its DFO places AND its regulation extents) before the next water;
progress per water ("Chilliwack River: 3 of 7") and overall; the water's map stays while stepping through its places.
Test: the queue order never leaves a water before all its places are listed.

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

## Owner 2026-11-09 (later): a whole water at one time
Review a whole river or waterbody at one time, never jump between waters: the queue and prev/next are grouped by water
(every place on one water before the next; in phase 3 a water's DFO places and regulation extents together), progress
per water ("Chilliwack River: 3 of 7") as well as overall, and the water's map stays loaded while stepping through its
places (rows and pins change, the map stays). Pinned by `test_the_queue_never_leaves_a_water_before_all_its_places`.

## Phase 1 — the row generator (2026-11-09)
- `pipeline/regs/review/checklist.py`: `rows(source, extents, …)` → numbered rows {kind, quote, ours, state, reason,
  anchor}; kinds water / where / from / to / also / area / tributaries / except / unchecked. Pure: the backend hands in
  a `Context` (cut labels, built cuts = the served atlas's, pending = in the cuts file or proposed by a mark but not
  built, water names, pins). `dfo_source` / `rule_source` (the row's name with its ✱ "Incl. Tribs", `extent_text` /
  `unresolved_locators`, else the verbatim's place phrases); `digest` (words + extent, pieces as a set) → stale.
- States: ✓ built and named by the words; ? saved / not built, or words no row checks; ✗ an exception not cut out, a
  tributary call against the words (or tributaries walked with no word for them), a named end with no cut, a carve-out
  the words never state, the whole water where the words name a part, a cut no word names.
- A check is a mark: `marks.Checklist` {digest, rows (n, kind, state, ticked, wrong)}; its one check is P; "all
  correct" needs every row ✓ or ticked, "wrong" names its rows. POST /api/marks refuses a checklist whose digest is
  not the place's now.
- Backend `curation-review/backend/checks.py` + GET /api/checks/{queue,progress,dfo/<id>,rule/<entry>/<rule>}: the
  queue by water (a water = its registry items; DFO places then regulation rules), filters keep a whole water.
- Counts (served atlas, 2026-11-09): DFO 332 places → 1,085 rows ✓ 709 · ? 134 · ✗ 242; regulations 3,354 rules →
  ✓ 10,018 · ? 178 · ✗ 503 (CI, cuts file as built: `test_review_checklist.py` prints them).
- Tests `pipeline/tests/test_review_checklist.py`: every curated DFO place and every rule with extents (no crash, every
  place phrase quoted by a row or listed as unchecked), planted mutations (exception dropped, tributaries flipped,
  pending / unknown cut, a named end with no cut), digest stale on words / extent, mark rules, queue by water.
