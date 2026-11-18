# T801 — Skeena Section 4: the "separate licence per section" line barely shows, the stamp sentence quotes the wrong printed line, zone (b) reads "the Zymoetz River confluence (1500 m upstream)"; the upper-end flag is cleared (owner: all the way up)

Status: OPEN (pipeline: `pipeline/deliver/export/read.py` about line 387, the curated licensing record; data: `data/curated/regulations/synopsis/region-6.json`).

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
5. Cosmetic: the "Mainstem only" tag shows on parts 11 and 14 to 18 only; the Reserve side channel (part 4) carries
   the Skeena River 2 unit internally (closed all year, nothing shows).
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
