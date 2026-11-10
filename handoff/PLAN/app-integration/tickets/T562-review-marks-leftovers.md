# T562 — Review marks, what T545 and T541 left

Status: PARTIAL — 0f6e7607c, aed6f6a25 (2026-11-10): items 4, 5 and 7 done; 6 waits on the owner; 1, 2, 3 open (what each needs is written below).

Source: the T545 and T541 lanes' reports, 2026-11-09.

## Work
1. "Extent changed since the mark" guard for entry/rule marks and the 89 older DFO marks: record what a mark replaced,
   hold when the file is neither the before nor the after, add a supported rename path (T545 item 5; needs mark-schema
   and save-path work in the backend). Re-asserting marks on apply stays the design.
2. Found names: write the auto-accepted candidate ids into `found_name_calls.json`, move `mine.py` under `pipeline/`,
   pin the candidates digest in the found-names header (T545 item 6 remainder).
3. An accepted name on a lake or registry item: resolve `wbks` / `gnis` targets in `add_mined_names`, or write the
   atlas's absent-names list to a file and gate on it (T545 item 7 remainder; today the accept is refused with a reason).
4. Review app shows the `apply --check` list at start-up, and a file-domain save shows its `dropped` rows and commit
   (both are in the API response and log only).
5. `proposals.side_dir` still cuts the id at 40 characters; split proposal ids can still clash (now refused, not
   prevented): give them the same hash suffix as T545 item 1.
6. `NOT_LANDED_OK` lives in code (`pipeline/regs/review/apply.py`); move to a curated file only if the owner wants to
   edit it.
7. Small: `reuse._anchors_now` repeats `pending.anchors_now`; `dfo_salmon/coverage.py` docstring names the old
   `handoff/PLAN/curation-review/dfo-coverage.md` path; `data/curated/review/README.md` says marks.jsonl is never edited
   by a tool while `commit_marks.py` supports a carry rewrite; retire the port-clash paragraph at the top of STATUS.md.

## Acceptance
Each item has a test with a mutation pin; `python -m pipeline.regs.review.apply --check` stays clean on the real marks.

Depends: —. Size: M. Owner: no.

## Done 2026-11-10 (the review-app lane)
- Item 5: `proposals.side_dir` uses `_cut_slug` (a hash suffix when the cut at 40 drops something); an id that fits
  keeps its directory. Test: `test_review_proposals.py::test_a_side_directory_never_merges_two_ids…`.
- Item 7: `reuse._anchors_now` memoises `pending.anchors_now` (one copy of the rule); `coverage.py`'s docstring names
  the real path (`data/generated/review/dfo-coverage.md`); `data/curated/review/README.md` says the one tool that
  rewrites marks.jsonl is a migration's carry through `commit_marks`. NOT done of item 7: the port-clash paragraph at
  the top of STATUS.md (not this lane's entry to retire).
- Also: `test_review_queue.py::test_no_second_copy_of_the_order…` was failing on a NAME clash, not a second copy —
  `pending.on_disk` (T541: the pending build read from disk) beside `fingerprint.Context.on_disk` (a context built from
  files). Both are live and do different things; the pending one is now `pending.pending_on_disk` (callers:
  `pipeline/release.py`, `pending.main`, `test_curation_review_vintage.py`). The test is green.

## Still open
1 (the changed-extent guard for entry / rule marks and the 89 older DFO marks: mark-schema and save-path work),
2 (found names: accepted ids in the calls file, `mine.py` under `pipeline/`, the digest pin), 3 (an accepted name on a
lake or registry item), 4 (the `apply --check` list and a file save's dropped rows shown in the app), 6
(`NOT_LANDED_OK` to a curated file, only if the owner wants to edit it).

## Second pass, 2026-11-10 (aed6f6a25)
- Item 4 DONE: `GET /api/review/not-landed` runs `apply.check` once per server process in the background (2.7 s,
  measured) and the app's banner lists every mark in force that is not in its curated file (a mark that only waits
  for a cut is listed apart, as information); a check that fails is said. A file-domain save's dropped rows and its
  commit are shown in the same banner stack (they were in the API reply and the log only). Tests:
  `test_curation_review_safety.py::test_the_landed_check_is_served_once…`, frontend `notLandedWords` /
  `domainSaveWords`.
- Item 6 NOT done by design: the ticket says "only if the owner wants to edit it" — an owner question, no code.
- Item 1 NOT done (in this lane's paths, but large): needs (a) `Mark.extent_before` filled for entry / rule marks at
  the save (`reuse.post_mark`, as `dfo.mark_for` does for DFO), (b) `proposals` holding a mark whose target's extent is
  neither its before nor its after, (c) a backfill decision for the 89 older DFO marks that carry no before (they
  cannot be reconstructed: either they stay re-asserted, as today, or the owner re-saves them), (d) a rename path.
  (c) is an owner decision.
- Item 2 NOT done: `mine.py` lives in `handoff/PLAN/curation-review/name-variants/` and is run by hand; moving it under
  `pipeline/` and pinning the candidates digest changes the found-names header that `pipeline/atlas/registry/mined.py`
  reads — outside the review-app lane's paths.
- Item 3 NOT done: `add_mined_names` is `pipeline/atlas/registry/mined.py` (the atlas lane); the accept is still
  refused with a reason, which is the safe state.
