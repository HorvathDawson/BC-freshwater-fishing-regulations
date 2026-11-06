# T426 — A changed synopsis extractor must still read the current book the same way

Status: OPEN — owner 2026-11-05: "what about if we need a new extractor for the synopsis? we will need to make sure the
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
