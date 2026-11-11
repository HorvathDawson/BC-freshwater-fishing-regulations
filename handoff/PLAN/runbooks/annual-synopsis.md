# Runbook: a new synopsis (the annual book update)

T239. From a new BC Freshwater Fishing Regulations Synopsis PDF to a staging release. Every step names its command and
WHO runs it. **Owner** = the human (credits, decisions, prod). **Claude** = an agent may run it.

The idea (owner 2026-10-29): the book's wording changes little, its FORMATTING a lot. So the PDF is decoded to text again
with the extraction code fixed for the new layout, and only the rows whose printed text CHANGED are parsed and reviewed.
Unchanged rows keep last year's entry exactly as curated (display names, matched waters, binds to other waters' cuts).

Rehearsed on the 2025–27 book, 2026-11-05 (T239 "Dry run"): extraction → export → carry → ingest → diff, no credits.

Set once, in every shell (one work dir per book; never under `data/curated`):

```bash
cd ~/dev/personal/BC-freshwater-fishing-regulations && export PYTHONPATH="$PWD"
Y=2027-29                                            # the new book's years
W=data/generated/scratch/synopsis-$Y                 # the work dir
OLD=data/curated/regulations/synopsis               # last year's book (read only until step 9)
```

## 1. Fetch the PDF — Owner (or Claude)

```bash
cp data/source/fishing_synopsis.pdf data/source/fishing_synopsis.2025-27.pdf     # keep last year's copy
curl -L -o data/source/fishing_synopsis.pdf \
  https://www2.gov.bc.ca/assets/gov/sports-recreation-arts-and-culture/outdoor-recreation/fishing-and-hunting/freshwater-fishing/fishing_synopsis.pdf
shasum -a 256 data/source/fishing_synopsis.pdf      # record it in the ticket / STATUS
```

- The repo copy is THE source: gov.bc.ca's bytes change day to day for the same edition, so cite the repo copy's SHA.
  2025–27: `08f58537261ffe51c513193013ba165754285719dec8d0332beaef1cbab8ecfa`.
- Page numbers: every `page` / `source_pages` is the PRINTED page. 2025–27: printed = PDF − 2 up to printed 40, then
  4 unnumbered centre pages, then PDF − 6. Nothing is typed: `pipeline.common.synopsis_pages.printed_to_pdf()` reads
  the footers. Check it on the new book (Claude):
  `.venv/bin/python -c "from pipeline.common.synopsis_pages import printed_to_pdf as m; print(m())"` — every water-table
  page must map. If the footer wording changed, fix `_FOOTER_PAGE` first.
- The validity period (`valid_until`) is read off the cover (`synopsis_pages.synopsis_period`,
  `test_synopsis_period.py`). Run `pytest pipeline/tests/test_synopsis_period.py` (Claude); fix the reader if it fails.

## 2. Decode the PDF to rows — Claude

The new book's layout needs extractor changes; each change must still read the CURRENT book exactly as before
(T426), or a regression shows up later as a fake "the book changed" row (T415: Heber River). In this order, and
never out of it:

**2a. Change the extractor** (`pipeline/regs/extraction/extract_synopsis.py`) for the new layout. Try it on single
pages of the new book: `.venv/bin/python -m pipeline.regs.extraction.extract_synopsis --page <pdf page>`.

**2b. Fast tier green** (seconds; CI runs the first two with no PDF):

```bash
.venv/bin/python -m pytest -q pipeline/tests/test_extract_fixtures.py pipeline/tests/test_extract_line_grouping.py \
  pipeline/tests/test_extract_baseline.py
```

`test_extract_fixtures.py` replays 13 hard rows (one per region at least) from their characters
(`fixtures/extract_rows.json.gz`); `test_extract_baseline.py::test_hard_pages_read_as_the_baseline` (`needs_source`)
re-reads 14 hard pages of the current book — Heber, the sample table (skipped), the four unnumbered centre pages, a
page per region — against the baseline.

**2c. `--check-against` on the current book: clean** (the whole book; heavy, through heavy.sh):

```bash
bash scripts/heavy.sh synopsis-check 50 -- .venv/bin/python -m pipeline.regs.extraction.extract_synopsis \
  --check-against pipeline/regs/extraction/baseline/2025-27.json --report $W/check.json 2>&1 | tee $W/check.log
```

- No PDF argument: the gate finds the baseline's own book by sha — after step 1 that is last year's copy
  (`fishing_synopsis.2025-27.pdf`). It writes no rows file and no row images.
- It compares every row by PDF page + row box (`baseline/2025-27.json`: the PDF sha, the extractor commit, a sha per
  row) and exits 1 naming each row neither identical nor in `baseline/accepted_diffs.json` (changed + which fields,
  gone, new), and each accepted diff no longer seen (stale).
- A row the change moves ON PURPOSE (a fix): add it to `accepted_diffs.json` with its key, `was`, `now` (the gate
  prints all three) and a reason, in the same commit as the extractor change. An entry explains exactly that move;
  the row changing again fails again. T415's Heber fix is the first entry (`…confluence when\nopen`).
- Any other row: the change broke the current book — fix the extractor, back to 2a.
- Single pages while iterating (seconds): add `--pages 18 24`.

**2d. Only then extract the new book:**

```bash
bash scripts/heavy.sh synopsis-extract 50 -- .venv/bin/python -m pipeline.regs.extraction.extract_synopsis \
  data/source/fishing_synopsis.pdf $W/extraction/synopsis_raw_data.json --workers 4 2>&1 | tee $W/extract.log
```

- Writes only under `$W` (the PDF copy, row images, `synopsis_raw_data.json`). Never the live
  `data/generated/regs/extraction/` until step 9.
- Check, in this order; a fix here is an extractor change, so it goes back through 2a–2c:
  1. `grep -c 'Error on page' $W/extract.log` is 0; `[Skipped]` lines name only the sample table (2025–27: PDF p.6).
  2. Row count near last year's (2025–27: 1,393 rows on 39 pages), every row with a `region` and an MU.
  3. Spot-check 10 rows against the PDF crops (`$W/extraction/row_images/`), especially multi-line regulations,
     bold `**No Fishing**`, and the symbol column (`Incl. Tribs`, `Classified`, `Stocked`).
- Known extraction defects on the 2025–27 book (curators fixed the entry text by hand): the Chilliwack/Vedder row
  keeps a line-end hyphen ("hatch-\nery"), the Coquihalla row loses its last words ("Nov" for "Nov 1-Mar 31"). Heber
  River's garble is fixed (T415). A new book will have its own.

**2e.** The book diff (step 8, `synopsis_diff`) then shows only what the BOOK changed: the extractor's own changes
were proven on the current book in 2c.

## 3. Export every row (full) — Claude

```bash
bash scripts/heavy.sh synopsis-export 50 -- .venv/bin/python -m pipeline.regs.parsing.batch_exporter \
  --rows $W/extraction/synopsis_raw_data.json --out-dir $W/all --force
```

No credits. Matches every row against the registry and mints each row's `entry_id` (`r{region}:{name}@{MUs}`).
`manifest.json` lists unmatched rows (2025–27: 25 no-registry rows, all known).

## 4. Carry the unchanged rows — Claude

```bash
.venv/bin/python -m pipeline.tools.synopsis_book carry --batches $W/all/batches --catalogue $OLD --out $W/book
```

Writes `$W/book`: every entry whose row the new book prints with the SAME id and the SAME words, copied as is, plus every
province/zone table (`z…`, never parsed). Same words with other line breaks or spacing count as the same (owner
2026-11-05): carried, curated text kept, and listed as **reflowed** so a reflow is seen, not parsed. Prints the lists:
- **changed** (same id, new text) and **new** (an id last year did not have) → the parse, step 5–6;
- **gone** (last year's id not in the new book) → the owner confirms each in step 8 (a renamed row shows here AND
  under new; the diff pairs them by matched water).

## 5. Export only the changed + new rows — Claude

```bash
bash scripts/heavy.sh synopsis-export 50 -- .venv/bin/python -m pipeline.regs.parsing.batch_exporter \
  --rows $W/extraction/synopsis_raw_data.json --out-dir $W/parse --only-changed
less $W/parse/batches/batch_000.prompt.txt           # read one prompt before anything is spent
```

`--only-changed` drops every row whose `regs_verbatim` equals last year's. Its count must equal changed + new from step 4.
Do NOT use `run_parse.sh parse` here: its export is `--skip-existing`, which skips a CHANGED row whose id survived.

## 6. PARSE — **Owner only (spends credits)**

```bash
.venv/bin/python -m pipeline.regs.parsing.dispatch --work-dir $W/parse --model sonnet --concurrency 3
# optional second pass, findings to $W/parse/reviews/ (credits too):
.venv/bin/python -m pipeline.regs.parsing.dispatch --work-dir $W/parse --model sonnet --review --review-model haiku
```

Claude never runs either (AGENTS 1, CLAUDE.md), not even when asked to "run it" — hand the owner these lines.

## 7. Ingest into the new book (dry run first) — Claude

```bash
.venv/bin/python -m pipeline.regs.parsing.ingest_catalogue --batch $W/parse/batches/batch_*.json \
  --response $W/parse/responses/batch_*.json --out $W/book --ledger $W/parse/ingested.json \
  --rows $W/extraction/synopsis_raw_data.json --dry-run
.venv/bin/python -m pipeline.regs.parsing.ingest_catalogue --batch $W/parse/batches/batch_*.json \
  --response $W/parse/responses/batch_*.json --out $W/book --ledger $W/parse/ingested.json \
  --rows $W/extraction/synopsis_raw_data.json
```

Writes `$W/book` only. `--rows` is the extraction step 5 exported from: ingest refuses batches whose `rows_digest`
is another extraction's, and writes nothing (T553). Every `FAIL` line is an entry left out — a rejected entry, a row the
model never returned (`no response row`) or an index answered twice — it then shows as REMOVED in step 8 although step 4
listed it as changed. Fix it (re-dispatch that batch: owner, `dispatch --work-dir $W/parse --only <batch no.> --force`) or
note it for hand curation.

## 8. Diff the books; review the differences only — Claude runs, **Owner reviews**

```bash
.venv/bin/python -m pipeline.tools.synopsis_diff $OLD $W/book --json $W/diff.json --ids-out $W/review_ids.txt \
  | tee $W/diff.txt
.venv/bin/python -m pipeline.tools.synopsis_diff $OLD $W/book --scope table     # must be empty (tables carried)
```

- Entries pair by `entry_id`, then by matched registry items (a re-spelled row name), then by printed text; rules by
  `rule_id`, licensing by `id`, then by `verbatim`. Output: added / removed / renamed / changed (with a word diff of the
  printed text and the rule fields that moved) / cosmetic (page moves only — not reviewed).
- `review_ids.txt` = added + changed: the owner's review list. `removed` = rows to confirm gone.
- The province/zone tables are not parsed: the owner compares the new book's region pages (and
  `data/curated/regulations/reference/*.md`, the hand transcriptions) with last year's by eye; Claude edits the `z…`
  entries and reference files to match, each edit a reviewed change. Then pin them to the new print (T553; seconds,
  reads the new `data/source/fishing_synopsis.pdf`): `.venv/bin/python -m pytest -q
  pipeline/tests/test_base_rules_in_print.py pipeline/tests/test_catalogue_entries.py` — every number, date and
  "word number" pair of every `z…` entry on its `source_pages`, and of every reference file on its pages
  (`REFERENCE_PAGES`). A figure the new book does not print is an owner question (`DISAGREE`), never edited to pass.
  If the new layout moves the column gutters, fix `synopsis_print.GUTTERS` (`python -m pipeline.common.synopsis_print
  13` prints a page as read).
- Review in the app happens after step 10 builds the new book (the app reviews built entries only, AGENTS 65): start it
  as usual (`bash curation-review/run.sh`) and open `http://127.0.0.1:5173/?ids=$W/review_ids.txt` ($W spelled out,
  e.g. `?ids=data/generated/scratch/synopsis-2027-29/review_ids.txt`; any path under the repo). The queue lists only those entries, with "N of M reviewed"; an id that matches no entry is
  listed in the bar above the queue (a row the build dropped: check it against `removed`).

## 9. Adopt the new book — Claude, after the owner says go

```bash
cp -r $OLD /tmp/catalogue-before-$Y                                  # AGENTS 2: back up first
cp $W/book/region-*.json $OLD/ && git diff --stat -- $OLD             # only the diff's entries may move
cp $W/extraction/synopsis_raw_data.json data/generated/regs/extraction/
bash pipeline/regs/parsing/run_parse.sh seed-ledger                   # no credits
```

The seed is also what moves the ingest ledger to `io.entry_digest` (T628): until it, a ledger from before reads as
"never seen" and ingest keeps every entry on disk, saying so. Seed here and not earlier — it declares every edit made
since the last seed to be ingest's own write. The fee table: `pipeline/tests/test_fee_table_in_print.py` reads the new
book's p.5 as a table (`synopsis_print.fee_table`; it raises on a layout it does not know) and pins every stored fee
to its column.

Cut the new book's baseline for the extractor gate (T426; the accepted diffs reset to empty, since the new baseline
holds them), and re-capture the fast tier's rows from the new book (pick hard rows again — one per region at least):

```bash
bash scripts/heavy.sh row-boxes 50 -- .venv/bin/python -m pipeline.regs.extraction.row_boxes    # boxes of the adopted rows
.venv/bin/python -m pipeline.regs.extraction.baseline cut --book $Y --out pipeline/regs/extraction/baseline/$Y.json \
  --commit $(git rev-parse --short HEAD) --note "the step-2d extraction, adopted <date>"
git rm pipeline/regs/extraction/baseline/2025-27.json      # one book at a time (the latest <years>.json is current)
.venv/bin/python -m pipeline.regs.extraction.baseline fixture --out pipeline/tests/fixtures/extract_rows.json.gz \
  --rows page_0NN_row_0NN.png …                             # then fix the page lists in the two test files
```

Then re-run `synopsis_diff /tmp/catalogue-before-$Y $OLD` — it must equal `$W/diff.txt`. Commit with
`git commit -m "…" -- data/curated/regulations/synopsis`. Removed rows go only on the owner's confirmation.

## 10. Build and deliver — Claude

```bash
bash scripts/heavy.sh build 50 -- .venv/bin/python -m pipeline build      # reach → deliver (atlas unchanged)
```

Stages skip what is up to date; the corpus changed, so reach and deliver run. The release stamps the new
`valid_until` from the cover. Check `python -m pipeline build --status` and the card check (problems 0).
Then the owner reviews the `review_ids.txt` entries in the review app: `http://127.0.0.1:5173/?ids=$W/review_ids.txt`
(step 8).

## 11. Staging — Claude

```bash
deploy/staging/upload.sh                     # the release data/release/LATEST names; staging bucket only
```

## 12. Production — **Owner only**

`python -m pipeline.deliver.release.publish --release data/release/<r> --yes` (plan without `--yes` first). Never Claude.

## Rehearsal (no credits) — Claude

To prove the pipeline on a book already parsed: steps 2–5 as above, then stand in for step 6 with last year's entries
and run steps 7–8:

```bash
.venv/bin/python -m pipeline.tools.synopsis_book replay --batches $W/parse/batches --catalogue $OLD --out $W/parse/responses
```

The diff must be empty, or every difference explained (T239: 1,512 of 1,515 unchanged; the 3 are the extraction defects
in step 2).
