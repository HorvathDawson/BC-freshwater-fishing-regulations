# T587 — Taking a wording out of its group and adding it back, through the real write-through path

Status: OPEN

Source: T532 rounds 5–6 (the checklist's grouping decisions), leftover a.

## Today
"Not the same place" (`not_variant_of`) and "same place as…" (`variant_of`) are saved as marks and tested at the mark
and proposal level (`test_review_proposals.py`), and clicked in a sandbox with write-through OFF. Nothing runs the pair
through the write-through that the owner's server uses (the background DFO apply, one commit per proposal).

## Work
A test in `test_review_write_through.py` (the `wt` fixture: temp git repo, write-through on): post a `not_variant_of`
mark on a grouped wording, wait for the apply, assert the places file no longer links it and the commit names it; post
the `variant_of` back, assert the link returns and the file is byte-equal to the start. Mutation pin: an apply that
skips the un-link must fail the test.

Depends: —. Size: S. Owner: no.
