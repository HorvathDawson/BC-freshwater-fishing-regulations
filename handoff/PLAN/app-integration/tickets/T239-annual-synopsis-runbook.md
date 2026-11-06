# T239 — annual synopsis update: runbook + dry run

Status: DONE (2df8add7b, 4ac91de6f, 83c9995d7) 2026-11-05 — runbook, diff tool, carry/replay tool, dry run (diff explained), T414 (the review app's "only these entries" list), T415 (Heber extraction fixed). Owner choice left: Heber's whitespace (T415 part, last point).

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

## Owner 2026-10-29: scope (later, not this credits batch)
The book's changes may be small but its FORMATTING changes a lot each year, so step 1 is decoding the new PDF to text again
with updated extraction code. Then a tool that takes the newly decoded items with arbitrary additions / removals / changes
against last year's and lets the owner handle each change (review queue of differences only). Soon, not now.

## Done 2026-11-05
- Runbook: `handoff/PLAN/runbooks/annual-synopsis.md` — 12 steps, each with its command and who runs it (parse and prod:
  owner only).
- `pipeline/tools/synopsis_diff.py`: entry and row diff between two catalogue dirs, read as plain JSON. Entries pair by
  `entry_id`, then by matched registry items, then by printed text. Rules pair by `rule_id` and licensing by `id`, then
  by `verbatim`. Results: added / removed / renamed / changed (a word diff of the printed text, plus the fields that
  moved) / cosmetic (page moves only). Writes text, `--json`, and `--ids-out` (the review list: added + changed).
- `pipeline/tools/synopsis_book.py`: `carry` copies each unchanged row's entry (same id, same text) and every table into
  the new book, so curator work survives. `replay` answers batches with last year's entries, for a rehearsal with no
  credits.
- `batch_exporter --rows` (export a scratch extraction); `extract_synopsis --workers` (default 4).
- Extraction fixes: symbol order was hash-seeded (`set`), now first-seen; the p.4 sample table (PDF p.6, ALICE/ALOUETTE
  LAKE, region None) is now skipped with a loud stderr line. The 09-24 live file did not have it.
- Tests: `pipeline/tests/test_synopsis_diff.py` (10).

## Dry run 2026-11-05 (2025–27 book, no credits; scratch `data/generated/scratch/t239/`)
- PDF SHA-256 `08f58537261ffe51c513193013ba165754285719dec8d0332beaef1cbab8ecfa`.
- Re-extraction: 1,393 rows, the same as the live 09-24 extraction except Heber River's text (below). Symbol order
  differed on 22 rows, which is not a content change.
- Full export (`--force`): 1,393 rows, 47 batches, 25 no-registry rows.
- Rehearsal, as the runbook runs it: carry 1,390 unchanged rows + 122 tables; `--only-changed` exports 3; replay +
  ingest; diff vs `data/curated/.../catalogue`: **1,512 of 1,515 unchanged, 0 added, 0 changed, 3 removed**. All 3
  are explained. In each, curators fixed the entry text by hand, a fresh extraction does not reproduce it, so the
  replayed rules' quotes fail ingest's "verbatim is a substring" gate:
  - `r1:heber_river@1-9`: the current extraction garbles line 3 ("Folpye fnishing only … when"; the 09-24 file read
    "Fly fishing only … when open"). This is an extraction regression since 09-24.
  - `r2:chilliwack_vedder_rivers…@2-4`: the extraction keeps "hatch-\nery"; the curated text reads "hatchery".
  - `r2:coquihalla_river@2-17`: the extraction ends at "Nov"; the curated text has "Nov 1-Mar 31".
  A real run parses these 3 again; they would show as changed and go to review.
- Why rows are carried, not re-ingested. A full re-ingest of all 1,393 rows (also run) refused 17 and changed 31:
  - 14 refused: curator binds to OTHER waters' cut-points (Elk, Atnarko/Bella Coola, Upper Arrow, Nilkitkwa …).
    These are not on the row's own menu.
  - 20 changed: the curated `display_name` (e.g. Tepee Lakes, Vedder River).
  - 6 changed: the curated `matched`.
  - 4 changed: ingest rewrote gauge-alias split ids to their canonical names.
  So a re-ingest would undo curation. Unchanged rows must be copied, and only changed rows parsed (runbook steps 4–5).

## Follow-ups (to ticket)
1. The review app has no "only these entry ids" queue filter. The review list (`review_ids.txt`) has to be opened id by
   id. This is a `curation-review` change: the other lane's area.
2. The extraction regression on Heber River line 3 (character order). Fix it before the next book; check the
   Chilliwack hyphen and the Coquihalla truncation at the same time.


## Merged in (ticket audit 2026-11-05, owner)
This ticket now also carries the leftovers of: T414, T415 — read each file's open items and finish them here; each closes pointing here.

## T414 part DONE 2026-11-05 (4ac91de6f)
- The owner opens `http://127.0.0.1:5173/?ids=<file>` (`run.sh` unchanged; a search param, so the hash stays the open
  entry). `GET /api/review/ids?path=` (`backend/idlist.py`) reads the `--ids-out` file (repo-relative or absolute,
  inside the repo; blank lines and `#` comments skipped, repeats listed once).
- The queue lists only those ids, in the queue's own order. The bar above it says "N of M reviewed" (a mark in force,
  as the progress bar counts it) and how many are verified. It also says how many the other filters hide, and names
  every id that matches no entry.
- A file it cannot read is a 422 with the reason. The bar shows the reason and the queue shows nothing, never the
  whole queue.
- Runbook step 8 and step 10 use the URL.
- Tests: `pipeline/tests/test_curation_review_ids.py` (3), plus `test_curation_review_safety.py` (32, CI filter);
  frontend `tsc --noEmit` clean. Not yet seen in a browser.

## T415 part — done 2026-11-05 (83c9995d7)
- Cause: NOT an extractor change since 09-24. The extractor at every commit back to a6fcf21e1 (09-03) reads Heber
  (PDF p.18) as "Folpye fnishing … when"; the 09-24 file's "Fly fishing … when open" was a hand fix (9ba92c01b,
  "garbled verbatims restored"). The real cause: `extract_text_with_bold` starts a line only when a glyph is MORE than
  5 pt lower; Heber's last line "open" is set exactly 5.0 pt under "…confluence when", so it joined that line and the
  x sort interleaved the two.
- Fix (`extract_synopsis._stacked_under`): a glyph more than half its size lower that overlaps a glyph already on the
  line starts a new line. Heber now reads "…confluence when\nopen". Single-page runs vs the live 09-24 file: PDF
  pp.17, 18, 19, 21, 24, 26, 60 = 231 of 232 rows identical (only Heber moves); Chilliwack/Vedder and Coquihalla
  (p.24) unchanged. A raw-chars sweep of all 39 table pages found the rule firing only on Heber among table rows.
- Tests: `test_extract_line_grouping.py` (3, no PDF, every tier); `test_extract_synopsis.py::
  test_heber_river_tight_last_line_is_its_own_line` (slow, p.18 pinned with neighbours). Both fail without the fix.
- Left: the curated Heber text has a space where the extraction has a line break ("when open" vs "when\nopen"), so
  `synopsis_book carry` (exact compare) still sends Heber to the parse once. Ingest's `squash` gate passes it.
  Owner choice: accept one re-parse, or let carry compare whitespace-normalised.
  Chilliwack "hatch-\nery" and Coquihalla "Nov" are not this defect (still hand fixes; not touched).
