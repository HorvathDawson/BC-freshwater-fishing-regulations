# T610 — A "same place" grouping writes why on the place (the Serpentine link had no note)

Status: IN REVIEW

Source: found 2026-11-10 while testing T607; coordinator: "ticket + fix … in your lane".

## Today
`pipeline/tests/test_dfo_salmon.py::test_a_curator_confirmed_grouping_records_why_it_crossed_an_op` fails on real data:
`2:serpentine-river:downstream-of-168th-street` is a confirmed grouping (`duplicate_confirmed`) with no note in its
binding. The owner grouped it from the checklist on 2026-11-10; the grouping apply (`proposals._variant_proposal`,
commit e85d31fee) wrote the link and no note.

## Done
- The apply now also writes the note (`proposals.grouping_note`): the owner's own words from his mark and the date,
  else "owner confirmed same place, <date>, mark <12 hex of the mark's line>". Written once: a place that has a note
  keeps it, so taking a wording out and adding it back leaves the file byte-equal (T587's test, updated).
  Tests: `test_review_write_through.py::test_a_same_place_save_writes_why_on_the_place`.
- The Serpentine link's note was landed by the app's own apply of the owner's mark (`python -m
  pipeline.regs.review.apply dfo`, its own "T185 apply" commit) — no hand edit of the places file or the mark.

## Left
`3:thompson-river:from-kamloops-lake-outlet-downstre` is also a confirmed grouping with no note. Its "same place" mark
(2026-11-09) is no longer the mark in force (a later problem report replaced it; the link stays until he takes it
out), so no mark's apply can write it, and the test above does not look at region 3. It gets its note the next time
he saves a "same place" on it — or the test's regions are widened and this one decided. Owner / coordinator call.

Depends: —. Size: S. Owner: no.
