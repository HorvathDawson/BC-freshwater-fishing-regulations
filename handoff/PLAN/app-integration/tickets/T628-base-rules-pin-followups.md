# T628 — Base rules pinned to print: follow-ups from T553

Status: DONE — dd59a9f58, b185130de

Source: T553 report 2026-11-11.

## Work
1. A reader for the fee table on printed p.5, so the fee column assignment is checked against print (T553 did not verify it).
2. `--replace-entry` refuses an entry that carries an owner mark, and names the mark.
3. The ingest ledger keys on `io.entry_digest`. Needs a ledger re-seed: plan it with the next annual run, not before.
4. T539's dead-path guard also covers `handoff/PLAN/runbooks/`.

## Acceptance
- Changing one fee on p.5's transcription fails a test. A replace on a marked entry is refused. The guard fails on a dead runbook path.

Depends: T553. Size: S–M. No parser run.

## Built (2026-11-11)
1. `synopsis_print.fee_table()` reads printed p.5 as a table: each cell by the column heading it is printed under,
   group headings, ★ cells, "/day"; it raises on a layout it does not know. `pipeline/tests/test_fee_table_in_print.py`
   pins every fee record of `zp:licence_fees` to its printed row and column (label, three residencies, per day, the
   record's quote, the row in `regs_verbatim`). Measured: 16 rows, 40 prices, all equal to print — the column
   assignment T553 left unverified is right. Mutation: each of the 40 prices changed twice (by a cent; to another price
   the page prints) fails, as do a swapped column, a priced ★ cell, a lost row and a changed price in `regs_verbatim`.
2. `ingest_catalogue`: `--replace-entry` and `--replace-edited` leave an edited entry that carries an owner mark in
   force (`owner_marks`: an entry or rule mark, any verdict but cleared) as it is, print `REFUSED … carries an owner
   mark (<verdict> on <the entry | rule id>, <reviewer> <date>)` and exit 1. No flag goes past it. The marks store is
   read only for a replace; a missing store raises. Not exercised on live data.
3. The ledger holds `io.entry_digest` and names it (`{"digest": "io.entry_digest", "entries": …}`). The live ledger
   (2026-09-23, old digest) was not touched and not re-seeded: ingest reads it as "never seen" (every entry on disk
   KEPT) and prints why. The re-seed is annual runbook step 9 (noted there).
4. `check-tickets.mjs` `checkDocs` reads every `handoff/PLAN/runbooks/*.md`, command blocks included (`runbookPaths`);
   a path a runbook creates is on `RUNBOOK_MAKES` (one: last year's PDF copy), which fails when stale.

Tests: `test_fee_table_in_print.py` 5, `test_ingest_marked_and_ledger.py` 10, `test_ingest_catalogue.py` +
`test_parse_end_to_end.py` + `test_agent_parse_flow.py` + `test_base_rules_in_print.py` unchanged and green;
`app/tools/check-tickets.test.ts` 33.

Left as it is, by choice: `write()` takes the marks from its caller (`run` always passes them) rather than requiring
them, because `test_ingest_catalogue.py` calls it bare and that file was fenced during T667. An UNEDITED marked entry
(ledger digest equal) is still replaced by a plain re-parse, as before; the mark then reads stale in the review app.
