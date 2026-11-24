# T742 — Review app: no grouping, region ends in every picker, and faster

Status: DONE (review-app lane, 2026-11-14: 98a7a646b, 2a14a32fd, 52a89a87a, bd1489030, 7cff9cc72; unpushed; the owner restarts his copy to get it)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T742 | Review app: no grouping (one place at a time), region ends in every picker, and faster | DONE (review-app lane, 2026-11-14; unpushed; owner restarts his copy) | 98a7a646b, 2a14a32fd, 52a89a87a, bd1489030, 7cff9cc72 | owner 2026-11-14 while labelling the Fraser ("remove the grouping option", "region cuts are in all the app", "SO SO SO slow") |

Source: owner 2026-11-14, while labelling the Fraser's DFO places: "the fraser marks are all fucked up the groups are all
messed up. i think we should just remove the grouping option in the review app it makes it have a lot of glitches instead
one at a time is better I think. we should revise the review curation then also make sure the region cuts are in all the
app please." and "also the review app needs to be faster it is SO SO SO slow rn".

## A. What grouping did wrong (98a7a646b)

`handoff/PLAN/curation-review/fraser-group-marks-2026-11-14.md`: seven defects with the marks.jsonl lines. In short: a
group save carried "Mission -> Spuzzum" onto two wordings that end at Hope, and replaced the owner's own earlier
Mission -> Hope answer without saying so; the water list chained places by IDENTICAL EXTENT, so five wordings showed as
"the same place"; "not the same place" cleared the wording's real rewording link too (done twice on the Fraser's
"upstream of the CPR Bridge" pair); the save path and the list grouped differently; older wordings were hidden; one
take-apart was saved twice. 4 Fraser marks were written by a group save (lines 2547, 2548, 2554, 2555); 12 more
elsewhere (Nanaimo, Chilliwack-Vedder, Stave).

## B. One place at a time (2a14a32fd)

- Removed: the group save, save-for-all, the older-wordings ticks (`OlderWordings.tsx`), the same-place headings and
  their take-out / put-in, the "same extent as its twin" one-click, attach-as-variant (DfoPlaces is read only, each
  wording has its own "open" link).
- Server: a DFO mark with several `targets` or with `members` is refused (422, logged in refused-saves, nothing
  written), at the route and in `reuse.post_mark`; `dfo.marks_for_group`, `_groups`, `wording_check`, `fix_taking`,
  `_member_check` are gone. `checks.same_place` groups nothing.
- Every wording on a live water is its own queue row with its own state (`dfo.historic_only`: dormant on a live water).
- Old group-written marks stay exactly as written; the place shows "marked in a group" (chip in the water list, a line
  in the header), the filter "marked in a group" lists them, and progress counts them (`group_marked`).
- `variant_of` / `not_variant_of` marks already in the store are still read; nothing in the app sends a new one.

## C. Region ends in every picker (7cff9cc72)

A region line's cut id (`area:N`) has one atlas row per id, so the pickers drew the Fraser's Region 5 cut on Antler
Creek, labelled "Region 1 – Region 5 boundary"; area:5 and area:3 were both labelled "Region 3 – Region 5 boundary",
and nothing said which of area:5's two Fraser crossings was meant. `reuse.region_ends(item)` now gives each crossing,
in order, with the id that binds it (`area:5@down` / `area:5@up` when crossed twice on one line), "Region N boundary
· km K", and the stretch where two regions meet named at both its ends. The Fraser: Region 5 boundary km 418 (start of
where Regions 3 and 5 meet), Region 3 boundary km 453 (its end), Region 7A boundary km 674, Region 5 boundary km 704.
Include the shared stretch by picking its far end, leave it out by picking its near end. Checked: `area:5@down ->
area:3` resolves to the 2 shared sections.

| picker | before | after |
|---|---|---|
| DFO extent editor (inline + full), "a split on this map" | area ids, the point and label of another water | each crossing, plain name + km, its own point and id |
| Synopsis entry extent editor (`ExtentFixEditor` from EntryDetail) | `area:5`, `area:3`, `area:7A`, same atlas label for two | the same crossings list (a crossing no id binds is listed, not offered) |
| "any curated split" search (both editors) and the closed-area end search | no region cuts at all | the asker's water's crossings first |
| cut labels anywhere (state lines, extents in words, entry map) | another water's atlas label | "Region N boundary" (crossing named when it is one) |
| new-cut types (point, confluence, lake, park area boundary, border) | n/a: a region line is an existing cut | unchanged |

Not verified: a closed-area stretch saved with a region crossing as an end (the closures resolver was not exercised;
a refusal would show on the area).

## E. Speed (52a89a87a, bd1489030), from the profiling findings

Done: (1) the editor render loop (one `embedded` per draft; effects keyed on whether embedded); (2) the section -> item
map cached per build for `item_side_channels`; (3) a write-through outcome lays over the cached place list instead of
rebuilding all 3,686; (4) after a save the next place opens at once, the one before is said saved, and only the end of
an apply reloads, once; (5) the 811 KB entries list is read only on the entries screen; (7) a place's reach, saved
preview and proposal are fetched together; (8) DFO rows, the checklist places and the region membership warm before
"ready", the 60 s zone rule counts after.

Left for the owner: (6) StrictMode in dev doubles every effect fetch — running the built frontend (or no StrictMode)
halves server work; his call.

Left (bigger, next session): incremental marks-derived caches on append (post-save 3-5 s -> <0.3 s: dfo._rows,
proposals, applied_keys, marks health); smaller extent-preview / item geometry payloads (684 KB / 843 KB for the
Fraser); the write-through in its own process (it holds the GIL 2-29 s); splits.json parsed per split in dfo.detail
and a pyproj Transformer per lake.

## Tests

`node --test curation-review/frontend/tests/*.test.ts` (43), tsc clean; `test_curation_review_one_at_a_time.py` (new:
group saves refused, nothing written, logged; every wording its own place; group-written flag), `test_review_region_ends.py`
(new), `test_review_checklist.py` (no grouping; group filter; outcome overlay; cache staleness), `test_review_dfo.py`,
`test_review_gate.py` (late warm); the `test_curation_review_*`, `test_review_*`, `test_dfo_*` groups run through
scripts/heavy.sh.

2026-11-15: pipeline CI red on 67c9b32f1 / 189458c93, two stale tests: `test_marks_never_orphaned` still passed B's removed groups argument to `historic_only` (now 2 args; a dormant rewording is its own place), and C's region-line crossings made `search_splits` read the registry in the data-free cut-search test (now stubbed). The groups above ran in the data tier, so the build hid it; fast-fail now runs every review test in CI's tier when a review module changes.

## Owner 2026-11-15
- Owner 2026-11-15: "need to also fix all other tributary only items like on thre bulkley too. fix the proposals
  after adding ability for it". Added to the review-fix lane: every tributaries-only / except-tributary place in all
  regions listed; suggester proposes that form; a merge of only those rows into dfo-suggestions.json prepared (main
  chat applies with a backup); a list of already-marked places of this kind for him to re-mark.
