# T562 — Review marks, what T545 and T541 left

Status: PARTIAL — 0f6e7607c, aed6f6a25 (2026-11-10). 2026-11-22: items 1 (the entry/rule changed-extent guard), 2 and 3 (found names, lake names in `add_mined_names`) looked at and NOT started: 1 is mark-schema + save-path work, 2 and 3 are `pipeline/atlas/registry/mined.py` + `mine.py` (the atlas lane). Each is its own ticket-sized lane. left (Claude): items 1, 2, 3 (each its own lane).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T562 | Review marks, what T545 and T541 left: changed-extent guard for older marks, found-name freeze, lake names, check list in the app | PARTIAL — 4, 5, 7 done; 6 closed; items 1 (entry/rule guard), 2, 3 open: mark-schema / atlas-registry work, not leftovers; left (Claude): items 1, 2, 3 | 0f6e7607c, aed6f6a25 | T545 + T541 lane reports 2026-11-09 |

Source: the T545 and T541 lanes' reports, 2026-11-09.

Done (2026-11-10, 0f6e7607c, aed6f6a25): item 5 (`proposals.side_dir` uses `_cut_slug`), item 7 except the port-clash
paragraph at the top of STATUS.md (not this lane's entry to retire), item 4 (`GET /api/review/not-landed` + the app's
banner; a file-domain save's dropped rows and commit shown), `pending.on_disk` renamed `pending.pending_on_disk`. Item 6
CLOSED — owner 2026-11-10: not wanted; `NOT_LANDED_OK` stays in code.

## Work (left)
1. "Extent changed since the mark" guard for ENTRY and RULE marks (the book's side; new DFO marks already record
   `extent_before` and are refused when the file has moved, T525): (a) `Mark.extent_before` filled for entry / rule marks
   at the save (`reuse.post_mark`, as `dfo.mark_for` does for DFO), (b) `proposals` holding a mark whose target's extent is
   neither its before nor its after, (d) a supported rename path. Re-asserting marks on apply stays the design. (c) the
   older DFO marks — owner: "i think they should just be passed? you could do a quick audit for obvious issues?" They stay
   as they are: no re-saving, treated as accepted.
   Audit (read only, 83 older DFO marks with an extent and no before): 79 OK, 3 word-check flags fine, 1 REAL:
   `1:little-qualicum-river:all-waters-unless-indicated-below` (verified 2026-11-03) — its extent is "the rest of the
   water" (`op: rest`) and the file holds exactly that, but it binds NOTHING today (0 sections, outcome "unresolved"). The
   other place that uses `rest` (Stamp River) binds. Not changed; it needs a look at why the Little Qualicum's rest is
   empty (its other places may already cover the whole river, or the rest is not resolving).
2. Found names: write the auto-accepted candidate ids into `found_name_calls.json`, move `mine.py` (today in
   `docs/curation-review/name-variants/`, run by hand) under `pipeline/`, pin the candidates digest in the
   found-names header that `pipeline/atlas/registry/mined.py` reads (T545 item 6 remainder; the atlas lane).
3. An accepted name on a lake or registry item: resolve `wbks` / `gnis` targets in `add_mined_names`
   (`pipeline/atlas/registry/mined.py`), or write the atlas's absent-names list to a file and gate on it (T545 item 7
   remainder; today the accept is refused with a reason, the safe state).
Each of 1, 2, 3 is its own ticket-sized lane (1 = mark-schema + save path; 2, 3 = the atlas lane).

## Acceptance
Each item has a test with a mutation pin; `python -m pipeline.regs.review.apply --check` stays clean on the real marks.

Depends: —. Size: M. Owner: no.

State 2026-11-22 (stopped for credits): see Status above; code c77eeb601, ledger f087c8968; branch fix/T775-review-app-leftovers. fast-fail not confirmed (was still running).
