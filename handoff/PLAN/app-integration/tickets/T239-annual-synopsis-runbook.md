# T239 — annual synopsis update: runbook + dry run

Status: OPEN — not started.

Source: owner 2026-10-28 — approved proposal P5 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").

## Work
1. Write `handoff/PLAN/runbooks/annual-synopsis.md`: fetch the new PDF (printed-page offsets, SHA) → batch export →
   PARSE (HUMAN-RUN: the owner runs run_parse.sh; Claude never does) → ingest dry-run → diff vs last year by entry
   (added / removed / changed rows) → review queue of changed entries only (review app filter) → regulations
   `valid_until` → deliver → staging → prod.
2. A diff tool (`pipeline/tools/synopsis_diff.py`): entry-level and row-level diff between two ingested books.
3. Dry run on the current book (re-ingest the 2025–27 book as if new; the diff should be empty or explained).
## Acceptance
Runbook committed; the dry run's diff is empty/explained; every step names its command and who runs it.
Depends: none. Size: L.
Owner: yes — runs the parse (credits) and reviews changed entries. Default: dry run uses the current book.
