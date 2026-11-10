# T591 — A test that the checklist's places cache goes stale when it must

Status: DONE — 4089f60cd, 2026-11-10 — `test_review_checklist.py::test_the_places_cache_goes_stale_on_each_thing_it_is_made_from_and_on_nothing_else` (the mutation — dropping a key part — was not run by hand)

Source: T532 round 8 (speed), leftover e.

## Today
`checks._places()` is memoised on the marks file's stamp, the DFO rows, the context, the entries directory's stamp and
the write-through's apply stamp. No test proves each of those five invalidates it (a missed one shows the owner a
place as not done after he saved it).

## Work
One test per key part: change it, assert the memo misses (and that an unrelated change does not). Mutation pin: drop
a part from the key and the test fails.

Depends: —. Size: S. Owner: no.
