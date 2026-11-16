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
