# T562 — Review marks, what T545 and T541 left

Status: OPEN.

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
