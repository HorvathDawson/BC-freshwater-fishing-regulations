# T402 — Two tests fail only in the full suites (2026-11-05 run)

Status: OPEN (found in the post-0.5.0 full run; both pass alone).

1. **pipeline/tests/test_curation_review_split_preview.py::test_every_proposal_is_shown_under_its_real_id…** — in the
   full pytest run `dfo.real_ids()` (curation-review/backend/dfo.py:901) returned `{}`; alone, 6/6 pass. Suspect: the
   `_REAL_IDS_MEMO` keyed on file stamps keeps an empty answer from an earlier test that pointed `reuse.MARKS_PATH` /
   the splits / overrides at a temp dir (or a second `dfo` module: pipeline/regs/review/dfo.py vs the backend's, both
   importable as `dfo`). Find the leaking test; reset the memo in a fixture.
2. **app/apps/mobile/src/back.test.tsx** "the Regulations page's day sheet closes first; then the water leaves for the
   map" — `expected false to be true` under the parallel vitest run; alone 27/27. Same shape as the known Walkthrough
   flake (short `waitFor`s under load). Lengthen the wait or await the sheet's own close signal.

Full-run counts: app 8457 passed, 13 failed (12 = the ticket-ledger guard, fixed the same session: DONE rows without hashes;
1 = item 2); pipeline 6268 passed, 1 failed (item 1).
