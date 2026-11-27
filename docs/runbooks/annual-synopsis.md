# Runbook: a new synopsis (the annual book update)

From a new Synopsis PDF to a staging release. The PDF is decoded again with the extractor fixed for the new layout, and
only the rows whose printed text CHANGED are parsed and reviewed; unchanged rows keep last year's entry as curated.
**Owner** = the human (credits, decisions, production); **Claude** = an agent may run it.

Set once in every shell (one work dir per book, never under `data/curated`):

```bash
cd ~/dev/personal/BC-freshwater-fishing-regulations && export PYTHONPATH="$PWD"
Y=2027-29                                            # the new book's years
W=data/generated/scratch/synopsis-$Y                 # the work dir
OLD=data/curated/regulations/synopsis                # last year's book (read only until step 9)
```

## 1. Fetch the PDF (Owner or Claude)

```bash
cp data/source/fishing_synopsis.pdf data/source/fishing_synopsis.2025-27.pdf     # keep last year's copy
curl -L -o data/source/fishing_synopsis.pdf \
  https://www2.gov.bc.ca/assets/gov/sports-recreation-arts-and-culture/outdoor-recreation/fishing-and-hunting/freshwater-fishing/fishing_synopsis.pdf
shasum -a 256 data/source/fishing_synopsis.pdf      # cite the repo copy's SHA: gov.bc.ca's bytes change for one edition
```

- Every `page` / `source_pages` is the PRINTED page; `pipeline.common.synopsis_pages.printed_to_pdf()` reads the
  footers: every water-table page of the new book must map (fix `_FOOTER_PAGE` if the footer wording changed).
- `valid_until` is read off the cover: `pytest pipeline/tests/test_synopsis_period.py`; fix the reader if it fails.

## 2. Decode the PDF to rows (Claude)

Each extractor change must still read the CURRENT book exactly as before, or it shows up later as a fake "the book
changed" row. In this order: **2a** change `pipeline/regs/extraction/extract_synopsis.py` for the new layout (`--page
<pdf page>` tries one page); **2b** the fast tier is green (`pytest -q pipeline/tests/test_extract_fixtures.py
pipeline/tests/test_extract_baseline.py`); **2c** `--check-against` on the current book is clean (heavy):

```bash
bash scripts/heavy.sh synopsis-check 50 -- .venv/bin/python -m pipeline.regs.extraction.extract_synopsis \
  --check-against pipeline/regs/extraction/baseline/2025-27.json --report $W/check.json 2>&1 | tee $W/check.log
```

It exits 1 naming each row neither identical nor in `baseline/accepted_diffs.json`. A row a fix moves ON PURPOSE goes
there (key, `was`, `now`, reason) in the same commit as the change; any other row means the change broke the current
book: back to 2a. **2d** Only then extract the new book (writes only under `$W`):

```bash
bash scripts/heavy.sh synopsis-extract 50 -- .venv/bin/python -m pipeline.regs.extraction.extract_synopsis \
  data/source/fishing_synopsis.pdf $W/extraction/synopsis_raw_data.json --workers 4 2>&1 | tee $W/extract.log
```

A page it cannot read fails the run by name (T1190). Check the rows are near last year's (1,393), the one `[struck]` line (a NEW
item is not in `extraction/known_strikes.json`: show the owner), 10 rows against `$W/extraction/row_images/`; a fix goes back to 2a.

## 3-4. Export every row, then carry the unchanged ones (Claude)

```bash
bash scripts/heavy.sh synopsis-export 50 -- .venv/bin/python -m pipeline.regs.parsing.batch_exporter \
  --rows $W/extraction/synopsis_raw_data.json --out-dir $W/all --force     # no credits; mints each entry_id
.venv/bin/python -m pipeline.tools.synopsis_book carry --batches $W/all/batches --catalogue $OLD --out $W/book
```

Writes `$W/book`: every entry the new book prints with the SAME id and words (other line breaks or spacing count as
the same and are listed as reflowed), plus every province/zone table. It prints **changed** and **new** (parsed in steps
5 and 6) and **gone** (the owner confirms each in step 8; a renamed row shows under gone AND new).

## 5. Export only the changed and new rows (Claude)

```bash
bash scripts/heavy.sh synopsis-export 50 -- .venv/bin/python -m pipeline.regs.parsing.batch_exporter \
  --rows $W/extraction/synopsis_raw_data.json --out-dir $W/parse --only-changed
```

The count must equal changed plus new from step 3-4. Do not use `run_parse.sh parse` here: its export is
`--skip-existing`, which skips a CHANGED row whose id survived.

## 6. PARSE (Owner only, spends credits)

Claude never runs it (AGENTS.md A1): hand the owner the line
`.venv/bin/python -m pipeline.regs.parsing.dispatch --work-dir $W/parse --model sonnet --concurrency 3`.

## 7. Ingest into the new book, dry run first (Claude)

```bash
.venv/bin/python -m pipeline.regs.parsing.ingest_catalogue --batch $W/parse/batches/batch_*.json \
  --response $W/parse/responses/batch_*.json --out $W/book --ledger $W/parse/ingested.json \
  --rows $W/extraction/synopsis_raw_data.json --dry-run
```

Then the same without `--dry-run`. It writes `$W/book` only and refuses batches whose `rows_digest` is another
extraction's. Every `FAIL` line is an entry left out (it then shows as REMOVED in step 8): re-dispatch that batch (owner:
`dispatch --work-dir $W/parse --only <batch no.> --force`) or hand-curate it.

## 8. Diff the books; the owner reviews the differences (Claude runs)

```bash
.venv/bin/python -m pipeline.tools.synopsis_diff $OLD $W/book --json $W/diff.json --ids-out $W/review_ids.txt | tee $W/diff.txt
.venv/bin/python -m pipeline.tools.synopsis_diff $OLD $W/book --scope table     # must be empty (tables carried)
```

Output: added, removed, renamed, changed (a word diff), cosmetic (not reviewed). `review_ids.txt` is added plus changed:
the owner's review list; `removed` rows are confirmed gone.

The province/zone tables are not parsed: the owner compares the new region pages and
`data/curated/regulations/reference/*.md` with last year's by eye; Claude edits the `z…` entries and reference files to
match. Then pin them to the new print: `.venv/bin/python -m pytest -q pipeline/tests/test_base_rules_in_print.py
pipeline/tests/test_catalogue_entries.py`. A figure the new book does not print is an owner question (`DISAGREE`),
never edited to pass. If the new layout moves the column gutters, fix `synopsis_print.GUTTERS`
(`python -m pipeline.common.synopsis_print 13` prints a page as read).

After step 10 the owner reviews in the app: `bash curation-review/run.sh`, then
`http://127.0.0.1:5173/?ids=<repo path of review_ids.txt>`.

## 9. Adopt the new book (Claude, after the owner says go)

```bash
cp -r $OLD /tmp/catalogue-before-$Y                                  # AGENTS.md A2: back up first
cp $W/book/region-*.json $OLD/ && git diff --stat -- $OLD             # only the diff's entries may move
cp $W/extraction/synopsis_raw_data.json data/generated/regs/extraction/
bash pipeline/regs/parsing/run_parse.sh seed-ledger                   # no credits
```

The seed moves the ingest ledger to `io.entry_digest`; seed here, not earlier (it declares every edit since the last
seed to be ingest's own write). `pipeline/tests/test_fee_table_in_print.py` pins every stored fee to the new p.5.

Cut the new baseline (accepted diffs reset) and re-capture the fast tier's hard rows (one per region at least):

```bash
bash scripts/heavy.sh row-boxes 50 -- .venv/bin/python -m pipeline.regs.extraction.row_boxes
.venv/bin/python -m pipeline.regs.extraction.baseline cut --book $Y --out pipeline/regs/extraction/baseline/$Y.json \
  --commit $(git rev-parse --short HEAD) --note "the step-2d extraction, adopted <date>"
git rm pipeline/regs/extraction/baseline/2025-27.json      # one book at a time
.venv/bin/python -m pipeline.regs.extraction.baseline fixture --out pipeline/tests/fixtures/extract_rows.json.gz \
  --rows page_0NN_row_0NN.png …                             # then fix the page lists in the two test files
```

`synopsis_diff /tmp/catalogue-before-$Y $OLD` must equal `$W/diff.txt`. Commit with `git commit -m "…" -- data/curated/regulations/synopsis`;
removed rows go only on the owner's confirmation.

## 10. Build and deliver (Claude)

`bash scripts/heavy.sh build 50 -- .venv/bin/python -m pipeline build` (reach, deliver); check `python -m pipeline build --status`.

## 11. Staging (Claude) and 12. Production (Owner only)

Staging and production are the release command: `docs/runbooks/release.md` (`python -m pipeline release
--staging`; production `CONFIRM_PROD=<r-id> python -m pipeline release --prod <r-id>`, never Claude).

## Rehearsal (no credits, Claude)

Steps 2 to 5, then `.venv/bin/python -m pipeline.tools.synopsis_book replay --batches $W/parse/batches --catalogue $OLD
--out $W/parse/responses` stands in for step 6; then steps 7 and 8. The diff must be empty, or every difference explained.
