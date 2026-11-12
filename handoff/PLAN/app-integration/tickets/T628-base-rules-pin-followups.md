# T628 — Base rules pinned to print: follow-ups from T553

Status: DONE — dd59a9f58, b185130de, d0c00bc9f (item 5)

Source: T553 report 2026-11-11.

## Work
1. A reader for the fee table on printed p.5, so the fee column assignment is checked against print (T553 did not verify it).
2. `--replace-entry` refuses an entry that carries an owner mark, and names the mark.
3. The ingest ledger keys on `io.entry_digest`. Needs a ledger re-seed: plan it with the next annual run, not before.
4. T539's dead-path guard also covers `handoff/PLAN/runbooks/`.
5. (Reopened 2026-11-11, follow-up.) The gap the first report left: an UNEDITED marked entry is replaced by a plain
   re-parse, and `write()` takes the marks from its caller instead of requiring them.

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

## Item 5 (2026-11-11): a plain re-parse and a marked entry nobody edited — d0c00bc9f
**What happened before (measured on a temp tree, never on live data).** Ingest writes an entry, the owner marks it
verified, a second parse of the same row reads it another way, plain `write` (no flag, ledger digest equal):
- the stored entry WAS replaced; ingest printed only `wrote 1 entries -> region-3.json`, no word about the mark;
- the mark was not orphaned (the entry id comes from the batch, so the target survives) and did not follow: its status
  went from `verified` to `stale`, reason `rule changed` (`marks.status` on `fingerprint.entry_fingerprints`; `health`:
  0 orphans, 1 stale). A re-parse that only renumbers a rule is followed (`match_records`), by design.
- The stale state is shown in the review app only (the marks banner, the queue, the backend's start-up log). Nothing in
  ingest, the build or the release names it: release preflight `check_marks` asks whether marks have LANDED
  (`review.apply --check`), not whether one went stale.

**Decision: add the guard.** The staleness mechanism detects the change afterwards, in another tool; it does not stop
the reviewed text being overwritten, and ingest itself told nobody. It was also backwards: an entry the owner marked
AND edited was protected with no flag past it (item 2), while one the owner marked verified without needing an edit
(the parse was right) could be replaced by any repass. Live today: 37 of 1,515 entries carry an entry or rule mark in
force (35 verified, 2 wrong). No second staleness mechanism was added: the guard is the same `owner_marks` refusal.

**Built.** `ingest_catalogue.write`:
- an entry on disk that nobody edited, that carries an owner mark in force, and whose re-parse DIFFERS from the copy on
  disk is left as it is; `REFUSED re-parse of <id>: carries an owner mark (<verdict> on <the entry | rule id>, <reviewer>
  <date>) — not replaced; a reviewed entry changes by a reviewed edit, never by a re-parse`; a `NOT REPLACED` block
  lists them (not under KEPT: they were not edited, and `--replace-entry` does not apply); exit 1. A dry run says the
  same. No flag goes past it (`--replace-entry` / `--replace-edited` on such an entry refuse too).
- a re-parse EQUAL to the reviewed copy is no change: written as before, not refused, store not opened.
- `write()` now REQUIRES `marks_path` (no default, no `None`); it reads the store once, BEFORE any file is written, and
  only when the run would put other content over an entry on disk — so a first parse or a run that only keeps opens no
  store, and a missing store stops the run with every file and the ledger as they were. `run` resolves the default
  (`CURATED.review.marks`).
- The way to change a marked entry is unchanged: a reviewed edit to the curated file, or the owner clears the mark in
  the review app first (a cleared mark is not in force). `pipeline/regs/parsing/README.md` says so.

**Not affected.** The annual run ingests into its own book dir (`$W/book`), where a changed row has no copy on disk, so
nothing there is refused. The live ledger is still the legacy one (every entry KEPT until the annual re-seed), so the
plain path replaces nothing on the live corpus today; the guard matters from the re-seed on. Not exercised on live data.

Tests: `test_ingest_marked_and_ledger.py` 21 (was 10): plain refusal for a verified / wrong / rule mark, real and dry
run, bytes and ledger unchanged; replaced when no mark is in force (empty store, other entry, cleared); equal re-parse
not refused; `write` without `marks_path` is a TypeError and a missing store stops before the first file; the command's
exact REFUSED and NOT REPLACED lines; the before-state pinned (the mark holds on the reviewed copy, reads stale "rule
changed" on the re-parse, 0 orphans). Mutation: taking the plain-path check out fails 9 of them.
`test_ingest_catalogue.py` and `test_parse_end_to_end.py` call the real `write` with their tree's own empty marks store
(no assertion changed).
