# T562 — Review marks, what T545 and T541 left

Status: PARTIAL — 0f6e7607c, aed6f6a25 (2026-11-10): items 4, 5, 7 done; 6 closed (owner: not wanted); 1: older DFO marks audited and passed (owner), the guard for entry / rule marks open; 2, 3 open (outside the review lane).

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

## Owner 2026-11-10, and the audit of the older DFO marks (item 1)
- Item 6 CLOSED — owner 2026-11-10: not wanted; `NOT_LANDED_OK` stays in code.
- Item 1, the older DFO marks — owner: "i think they should just be passed? you could do a quick audit for obvious
  issues?" They stay as they are: no re-saving, treated as accepted.
- THE AUDIT (read only; no mark and no curated file changed). Scope: DFO marks in force that carry an extent and were
  made before a mark recorded the extent it replaced: 83 (the "89" was an older count; 114 DFO marks in force carry an
  extent, 31 of them already record their before). Checked per mark, through the review server on today's files: the
  place id still exists; its words are the words the mark was made on; the curated file holds what the mark says; its
  extent resolves and binds (every piece, outcome "bound"); no two wordings of one place carry different extents; and
  the length against any distance the words name.
  | Result | Count |
  |---|---|
  | OK | 79 (73 landed and binding; 6 landed and waiting only for a new cut to be built) |
  | Real finding | 1 |
  | Flagged by the rough word checks, looked at, nothing wrong | 3 |
  REAL: `1:little-qualicum-river:all-waters-unless-indicated-below` (verified 2026-11-03) — its extent is "the rest
  of the water" (`op: rest`) and the file holds exactly that, but it binds NOTHING today (0 sections, outcome
  "unresolved"). The other place that uses `rest` (Stamp River) binds. Not changed; it needs a look at why the Little
  Qualicum's rest is empty (its other places may already cover the whole river, or the rest is not resolving).
  LOOKED AT, FINE: Chapman Creek (4.67 km; the "100 m" and "550 m" in its words place the falls, they are not its
  length); Little Qualicum "all open portions" and Morice Lake "including tributaries" (whole water, which is what
  the words say). No place id was missing, no wording had changed, nothing was un-landed, no two wordings disagreed.
  The per-mark rows are in the session scratchpad (`audit-older-dfo-marks.json`), not in the repo.
- THE GUARD FOR NEW MARKS: a new DFO mark already records the extent it was made on and is refused when the file has
  moved (T525, `extent_before`). What is still missing is the same for ENTRY and RULE marks (the book's side): the
  save path (`reuse.post_mark`) and the proposal check — not small, not built; it stays here as the open part of
  item 1, with (c) answered: older marks pass.
