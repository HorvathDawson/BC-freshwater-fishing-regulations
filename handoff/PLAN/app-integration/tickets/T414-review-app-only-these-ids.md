# T414 — Review app: an "only these entries" filter (the yearly synopsis diff's review list)

Status: DROPPED — merged into T239 (2026-11-05 audit; the work continues there) — was: OPEN (found by T239, 2026-11-05).

`python -m pipeline.tools.synopsis_diff … --ids-out <file>` writes the entries a new book added or changed. The
runbook (`handoff/PLAN/runbooks/annual-synopsis.md`) has the owner review ONLY those. The review app needs a filter
that takes that list (a file path or `#ids=` param) and limits the entry queue to it, with progress over the list.
Size: S. Owner: no. Needed before the next book (not urgent).
