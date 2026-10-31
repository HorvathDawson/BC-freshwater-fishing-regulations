# T329 — Ponds (Q154): close the domain, no reader, no export

Status: DONE (62ff4d515) (2026-10-30) — was OPEN — DECIDED (owner 2026-10-30: "The pond marks can be not exported tho it just means lakes? They don't need
to be saved, build will never use").

A pond call only confirms the default (a lake), so nothing reads it. Close the `pond` domain on the closed-domains list
(T325, option a — marks.jsonl untouched), stop exporting `decided/ponds.json` and delete it, close the review tab.
Depends: T325. Size: S.

## Done (2026-10-30)
`python -m pipeline.regs.review.lock ponds` passed its checks: 80 of 80 sample ponds marked, none unsure, nothing
waiting. `closed.json` now lists `ponds` (scopes q154, final: none). `decided/ponds.json` is deleted, and the exporter
no longer writes it. The live review app answers 423 on /api/q154/*, and the Small ponds sub-tab reads "locked".
marks.jsonl is untouched (the 81 q154 lines stay as history). `test_review_export` now expects closed scopes not to
load.
