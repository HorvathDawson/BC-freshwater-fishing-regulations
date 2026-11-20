# T801 — Skeena Section 4: the "separate licence per section" line barely shows, the stamp sentence quotes the wrong printed line, zone (b) reads "the Zymoetz River confluence (1500 m upstream)"; the upper-end flag is cleared (owner: all the way up)

Status: IN PROGRESS — 9af7efbd7, 6d5651c2f, 8d7f17709, 8e3d5f051, d0df51fd0, ab64dd2f1; items 1-5(1) landed (5(1) ab64dd2f1, merged via 6720c8eb7); item 5(2) dropped by the owner 11-20; left (Claude): ships with the next data release. (Earlier note: IN REVIEW (items 1-4 landed: 1 9af7efbd7; 2 pipeline 6d5651c2f + app 8d7f17709 (batch 45fff5e63); 3 8e3d5f051; 4 d0df51fd0; item 5(1) built on lane/l2-parts ab64dd2f1: "Mainstem only" on every Skeena River 2 and Section 4 main-stem part; item 5(2) DROPPED by the owner 2026-11-20, not to be done. owner: OWNER-TODO "5(2) (invisible Skeena side channel)" (dropped 11-20); left (Claude): main chat review + merge; ships with the next data release).)

Source: owner 2026-11-16, question 35 and his two maps of the weekend zones: "Should run all the way up but also we
should handle the weekend zones properly too. Make sure licensing info is done properly there." The province's
regulation (Angling and Scientific Collection Regulation, Schedule A, item 33): "Skeena River upstream of 1.5 km
upstream of Zymoetz River (Skeena River 4)", Class II, July 1 to Dec 31, no upper end.

## Checked right (release r-46e4355a7277, every main-stem part, nine days)
Class II and the named section, the dates, the steelhead stamp, no Classified Waters Licence between 1.5 km above the
Kitsumkalum and 1.5 km above the Zymoetz, Section 4 to the source, both weekend zones on the right parts and days, a
resident's status unchanged on a Saturday, no side stream carrying either licence unit.

## To fix
1. Clear the stored row's `review_reason` "a curator must draw the upper bound from the page 54/55 maps": the owner
   has ruled it runs all the way up, main river only.
2. "(non-residents and non-resident aliens require separate licences describing which section they are fishing)"
   shows only inside a quotation in the Skeena River 2 box; the Section 4 box has none. One plain line in both boxes.
3. The stamp sentence in the Skeena River 2 box quotes the line that names the section, not the stamp line
   (`read.py` line 387: `x["verbatim"]` where `w["verbatim"]` is meant).
4. Zone (b) on weekends: "the Zymoetz River confluence (1500 m upstream)"; the book: "to 1.5 km upstream of Zymoetz
   River confluence". The words come from the stored cut point.
5. Cosmetic: (1) the "Mainstem only" tag shows on parts 11 and 14 to 18 only — BUILT 2026-11-20 (lane L2, below);
   (2) the Reserve side channel (part 4) carries the Skeena River 2 unit internally (closed all year, nothing shows) —
   **DROPPED by the owner 2026-11-20 (lane-plan batch, OWNER-TODO 01:56: "Make sure their tickets are
   removed/completed/no doing labelled"): not to be done.**
The weekday wording of the weekend closure is the app chat's ticket T800.

## Lane result (2026-11-16, branch `lane/book-rule-fixes-1116`, commit 186388445)
Item 1 done: the Section 4 designation's `review_reason` now records the owner's ruling and Schedule A item 33's words
("Skeena River upstream of 1.5 km upstream of Zymoetz River (Skeena River 4)"). Binding unchanged (already right).
Designation records forbid extra fields, so the note lives in `review_reason` (as Skeena River 2's provenance does);
the review app's tiers still count a designation with any review_reason as flagged. Items 2-5 not in this lane.

## Lane result, items 2-4 (lane/bugs-080, 2026-11-17)
- Item 4 FIXED d0df51fd0: a confluence offset reads "1.5 km upstream of the Zymoetz River confluence" (`place_names.point`;
  the 9 confluence-offset cuts: Zymoetz/Skeena, Morrison/Puntledge x2, Thorne and Kemess/Attichika x4, Halfway/Peace x2).
- Item 3 NOT changed: the sentence quotes the designation's row ON PURPOSE for the place (the Chilko's "upstream of
  Brittany Creek" is pinned by `test_the_export_says_where_and_when_the_stamp_is_not_required`); quoting the stamp line
  instead drops every waiver's place. For the Skeena River 2 the row is the whole two-waters sentence. Needs a choice:
  quote the stamp line beside the place, or trim the Skeena row to its (a) part.
- Item 2 needs the app: the class box body is composed in `app/packages/core/src/regs/licence.ts` from the record's
  fields; a "separate licence per section" line needs a field the box reads (the app chat).

2026-11-17 ~15:11, owner: see OWNER-TODO 15:11 (verbatim there).
- Item 2, pipeline half DONE 6d5651c2f (lane/bugs-080b): each Skeena classified designation (skeena_river_2,
  skeena_river_section_4) carries `non_resident_note: {says}` = "Non-residents and non-resident aliens require separate
  licences describing which section they are fishing." (book p.53), in the licensing words the app reads (`LicWords`,
  through the store's `LIC_KEEP`; no generated TS file changes). Left (app chat): the class box shows it.
- Item 3 DONE 8e3d5f051 (owner 2026-11-17 15:11, "Trim to this water's part (Recommended)"): the curated Skeena River 2
  designation quotes only its (a) part; its stamp sentence follows. Diff-proved: 9 waivers, only skeena_river_2's moved.
  Ships with the 0.8.0 reach + deliver build (the corpus digest moves).

## Built (app), item 2 (branch `app-fix/t801-t797`, 2026-11-17; not merged)
`core/src/regs/licence.ts`: `LicWords.non_resident_note?: { says } | null`; the class box says `says` as its own sentence
after the unit and stamp words ("Licence unit: … . <stamp text>. Non-residents and non-resident aliens require separate
licences describing which section they are fishing."); absent or null, no line. Reads the per-record licensing words
(store `LIC_KEEP`, lane/bugs-080b 6d5651c2f), so it shows once a release carries the field. Tests: `licence.test.ts`
(with, without, null, after the stamp text); mutation (the appended line removed) fails 2.

## Item 5(1) built (lane L2, branch `lane/l2-parts`, 2026-11-20); item 5(2) DROPPED by the owner
Commit ab64dd2f1. The place (`display.part.part_place`, the tag) named the "(mainstem only)" row only where its RULES
bind — the two weekend zones (parts 11 and 14-18). A row whose place is its printed SCOPE, and that speaks by a rule
somewhere on the water, now also speaks on every part whose MAIN STEM its licensing record covers (`_lic_entries`,
`reach` placement; a rule-set group only when every main-stem piece is covered — `_by_grp` keeps each member's
licensing set). Two limits, both measured: a region place ("In Region 6") stays read from the rules alone (else the
Stellako's Region 6 part would read "Where Region 6 and Region 7 meet" because the Region 7 row's designation covers
it); a part of side channels alone takes nothing from a designation (the Reserve side channel stays untagged — item
5(2), its Skeena River 2 unit, the owner DROPPED 2026-11-20 and is not done).
- Measured on the live export (19,766 waters): three Skeena rule-set groups gain "Mainstem only" (the Skeena River 2
  stretch Exchamsiks → Hell's Gate, Chimdemash → Cedarvale, and Cedarvale → Sedan + Shegunia → source); nothing else
  moves. Side deliver r-6db99b676653: binding diff empty, 0 of 1,957,010 sections' answers changed (only part wording).
- Test: `pipeline/tests/test_t801_mainstem_tag.py` (5 tests, mutations: a side channel given a main-stem run takes the
  tag; a row with no rule on the water places nothing; a region row moves no region; a group needs every main-stem
  piece covered).
- Picture: the water_map tool's Skeena panels 4-10, before / after: `T780-shots/skeena-tags-before.png`,
  `T780-shots/skeena-tags-after.png` (the app shows this place only in the long part label; the map tool shows it).
