# T402 — Two tests fail only in the full suites (2026-11-05 run)

Status: DONE 2026-11-06 (chat 3; 93d274234 item 1, 5b5865c89 items 2–3).

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
3. **app/packages/ui-native/src/Walkthrough.test.tsx** (the T293 Layers step) — the known one from the handoff; failed
   again in the T398/T396/T395 lane's `pnpm -C app check` (11-05), passes alone.

## 2026-11-05
- **Item 1 fixed (93d274234):** six curation-review files set `CURATION_MARKS` (temp) before importing the backend and
  never dropped it after; `reuse.MARKS_PATH` is read at import, so the rest of the session (and `dfo`, which holds that
  `reuse`) read an empty temp marks file. split_preview now imports every backend module fresh. The six + the victim:
  50/50 together.
- Items 2–3 (back.test, Walkthrough) and the 7 load failures two parallel lanes saw (back, a11y, RegsWide, palette) did
  not recur in a quiet full `pnpm -C app check` (8564 passed): they fail only when the machine is loaded. Left open:
  lengthen their `waitFor`s if they show up in a quiet run.

## Items 2–3 (2026-11-06, 5b5865c89)
Both were waits that ran out under the full parallel check, not wrong behaviour (alone they pass). A `waitFor` ceiling
returns as soon as it holds, so raising it costs a quiet run nothing: back.test's `until` 10 → 30 s and the day-sheet
test its own 60 s (as its neighbours already had); Walkthrough's waits 2–2.5 → 10 s and its describe 30 s. If either
shows again under load, the next step is awaiting the sheet's own close signal instead of a ceiling.
