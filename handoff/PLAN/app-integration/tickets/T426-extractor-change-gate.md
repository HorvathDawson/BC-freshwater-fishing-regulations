# T426 — A changed synopsis extractor must still read the current book the same way

Status: DONE (aad8f7313) 2026-11-05 — gate, baseline, accepted diffs, fast tier, runbook step 2. The whole-book
`--check-against` is a heavy run, owed (below). Source: owner 2026-11-05: "what about if we need a new extractor for the synopsis? we will need to make sure the
extractor is producing the same output etc? then we can compare the 2? or what is the workflow".

## Why
The annual runbook (`runbooks/annual-synopsis.md` step 2) fixes the extractor for the NEW book's layout, then diffs the
new book against last year's entries. Nothing proves the changed extractor still reads the CURRENT book as before, so
an extractor regression shows up as a fake "the book changed" row. T415 is that, found by accident (Heber River: the
extractor drifted since the 09-24 run).

## Work
1. **Baseline:** the current book's rows as the accepted extractor reads them — `data/generated/regs/extraction/
   synopsis_raw_data.json` frozen with its PDF sha (the repo copy, 2025–27 `08f58537…`) and the extractor commit, in a
   committed manifest (rows' sha per row; the rows file itself can stay generated if it is large).
2. **`extract_synopsis --check-against <baseline>`:** re-extract the current book with the new code and compare row by
   row (by page + row box): zero differences, or each one listed with a reason in a committed `accepted-diffs` file.
   Heavy (a whole-book run): through `scripts/heavy.sh`. Exit non-zero on an unexplained row.
3. **Fast tier in pytest:** ~10 hard pages as small fixtures (Heber, multi-line regulations, the unnumbered centre
   pages, the sample table that must be skipped, a page per region) with their expected rows — seconds, every run;
   `needs_source` if it reads the PDF.
4. **Runbook:** step 2 becomes: change the extractor → fast tier green → `--check-against` on the current book clean →
   only then extract the new book → `synopsis_diff` shows only what the book changed. A new baseline is cut when the
   new book is accepted (step 9).

## Acceptance
An extractor change that alters any current-book row fails the gate naming the row; T415's Heber fix is the first
accepted change through it; the runbook says the order.

Links: T239 (runbook), T415 (Heber). Size: M. Owner: no (the owner runs nothing; it gates Claude's extractor edits).

## Outcome 2026-11-05 (aad8f7313)
- **Baseline = the live 09-24 extraction, Heber an accepted diff.** `pipeline/regs/extraction/baseline/2025-27.json`:
  the 1,393 rows (39 pages) the corpus was parsed from, PDF sha `08f58537…`, extractor `3fa4e5eda` (the file was
  written 09-24 11:24 from the working tree committed at 14:06), one sha per row (+ 8-hex per field) keyed by PDF page
  + row box. Cut from the live rows + `row_boxes.json` (same raw and PDF shas), no re-extraction. Its Heber row is
  the 09-24 hand fix no extractor produced; T415's reading ("…confluence when\nopen") is the first entry of
  `baseline/accepted_diffs.json` (key + was + now + reason). Symbols compare as a set (the 09-24 order was
  hash-seeded); everything else exactly, whitespace included.
- **Gate:** `extract_synopsis --check-against <manifest> [--pages …] [--report …]` (= `python -m
  pipeline.regs.extraction.baseline check`): re-extracts with no row images, finds the baseline's book by sha (after
  runbook step 1, last year's copy), exits 1 naming every changed (with fields) / gone / new row not accepted, and
  every stale accepted entry. `baseline cut` (step 9; resets the accepted list) and `baseline fixture` (fast-tier
  chars). `extract_rows`' row text moved to `row_from_cells`, the page rule to `keep_page` (shared by extraction
  and gate).
- **Fast tier:** `test_extract_fixtures.py` (no PDF, every tier, 1 s): 13 hard rows, one per region at least,
  replayed from their cell characters (`fixtures/extract_rows.json.gz`, 90 KB) — Heber, Chilliwack/Vedder,
  Coquihalla, Shuswap, Columbia, Kootenay, Atnarko, Dean, Babine, a Classified+tribs row, bold No Fishing + Stocked in
  7A/7B/R8; their shas are the baseline's or the accepted diff's; mutation (no `_stacked_under`) fails Heber only;
  the sample-table skip. `test_extract_baseline.py`: the comparison on synthetic rows (changed/gone/new/accepted/
  re-changed/stale/page scope), the committed files agree, and (`needs_source`, ~6 s) 14 hard pages (6 sample, 18,
  24, 34, 37, 43–46 centre, 50, 56, 65, 71, 75) vs the baseline: 331 rows, only Heber (accepted).
- **Runbook** step 2: 2a change → 2b fast tier → 2c `--check-against` clean → 2d extract the new book → 2e the diff
  shows only the book; step 9 cuts the new baseline + re-captures fixtures.
- Not run (heavy, owed): the whole-book check —
  `bash scripts/heavy.sh synopsis-check 50 -- .venv/bin/python -m pipeline.regs.extraction.extract_synopsis
  --check-against pipeline/regs/extraction/baseline/2025-27.json --report data/generated/scratch/t426/check.json`.
  Expected: 1,393 compared, 1 accepted (Heber), 0 unexplained. Evidence so far: 14 pages / 331 rows here, plus T415's
  pp.17, 19, 21, 26, 60 and its raw-chars sweep of all 39 table pages.
