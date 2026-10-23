# T135 — Curation review phase 4: risk-ordered queue, pre-checks, ticket export, commit marks

Status: DONE — 1218d094 (lane-T135). Owner approved PLAN.md 2026-10-23 (defaults Q34–Q41).
Takes effect on merge: no rebuild. The run-based factors read the served reach run as it is; a reach run (already
part of every `python -m pipeline build --no-tiles`) refreshes them, and the screen re-reads it by file stamp.

Source: handoff/PLAN/curation-review/PLAN.md §4 "Phase 4: queue, pre-checks and output" (and §5); split out of T97.

## Work
See PLAN.md §4 "Phase 4: queue, pre-checks and output" — built as written there.

**Owner 2026-10-23 — splits mid-review:** "Make sure we could add splits and rebuild then continue review with the new
splits." In fix mode the owner adds or moves a split → the tool prints the rebuild commands (atlas → binding diff →
promote → reach → deliver; it never runs them) → after they run, the tool picks up the new vintage without losing
anything: the new split appears in tier S0 to confirm, entries whose binding moved go stale with reason "binding moved",
every other mark stands, and review continues where it was.

## Acceptance
PLAN.md §4 "Phase 4: queue, pre-checks and output" "Accept when", run unfiltered (not through rtk).
- A split added after review began enters the S0 tier of the queue on the next load; its dependants re-queue only if stale.

Depends: T97, T134. Size: M (about 3 days). Owner: no (plan approved).

## As built
- **The run's facts, one reader** — `pipeline/regs/review/runfacts.py` `RunFacts`: per entry and rule, the ambiguous
  cuts (`rule_diagnostic` kind `ambiguous_cut`: candidates, used, `toward`; `curated` = a `cut_pick`), the unresolved
  rules (`rule_unresolved`) and the unplaced pieces (kind `unclassified`). A run without `report.json` (written last)
  is still being written and is REFUSED, never read as "no findings". The review app reads it by the files' stamps
  (`reuse.run_facts`; none → the factors are not claimed and `queue_facts` / progress `facts` say why).
- **The risk factors and THE ORDER** — `tiers.RISKS` = no match, unresolved, undrawn part, ambiguous cut, cut, several
  waters, flag (PLAN §3's "P1 starts with" order). `risks(entry, binds=, run=)`: `unresolved` and `ambiguous cut`
  from the run; `undrawn part` from the rule (new: a closure with an undrawn part is P1, PLAN's "52 closures with
  undrawn parts"); a zone entry is never "no match". `queue_rank` = (tier, most urgent risk, −severity-A pre-checks);
  `split_rank` = S0 first, L grade first (raises for a split not in S0). `reuse.queue(order="risk")` (the new default;
  "attention" retired, `422`) sorts by it, then the book.
- **One queue across all regions**: `GET /api/entries` with no region is the whole corpus; the screen's region
  selector defaults to "all regions" and the order to the review order; in that view the S0 split cards LEAD the list
  (`review.ts splitsInQueue`, under the same review filter) and `j` / `k` walk from the last card into P1 (the
  selection knows whether it is a card or an entry). Rows carry `risks` and `prechecks {A, B}`; the list is headed by
  tier. `GET /api/regions` is one pass over the one queue, grouped.
- **Statuses renamed** (`reuse.entry_status`, `STATUSES`): `flagged` → `parser-uncertain`, `unreviewed` → `plain` (not
  shown as a badge); `status=` anything else is `422`.
- **Pre-checks** — `pipeline/regs/review/precheck.py` (`CHECKS` the one table, `prechecks` the one function, CLI
  `python -m pipeline.regs.review.precheck [--list|--entry|--code|--severity|--json] [--reach DIR]`). Severity A
  (facts): `gate`, `binds_nothing`, `unresolved`, `unplaced_pieces`, `ambiguous_cut`, `split_to_verify`,
  `parser_uncertain`, `unresolved_locator`, `undrawn_part`; B (hints): `cut_picked`, `quote_not_in_row`,
  `unquoted_text` (> 15 characters no rule quotes). The review view lists them (the gate run for the open entry),
  with every ambiguous cut's candidates and the one taken (`Prechecks.tsx`).
  Measured on reach run 81e726a3e6fbad3b (full_prev; the live `full` was being rewritten): binds_nothing 13 on 13
  entries, unresolved 29 / 14, unplaced 2 / 2, ambiguous_cut 95 / 52, split_to_verify 164 / 89, parser_uncertain
  263 / 151, unresolved_locator 31 / 22, undrawn_part 135 / 87, unquoted_text 214 / 214, quote_not_in_row 0 (the gate
  now refuses one), gate not counted (the CLI does not run the review tool's gate; the screen does), cut_picked 0 (Skeena's pick is a licensing record's).
- **Tier counts with the run** (PLAN §3 in brackets): P1 226 (227), Z 132 (132), P2 261 (261), P3 118 (116), P4 728
  (730), sweep 60 (59); closures with undrawn parts 52 rules (52), with ambiguous cuts 32 rules (32). P1 by first
  factor: no match 2, undrawn part 58, ambiguous cut 31, cut 121, several waters 10, flag 4.
- **Second-pass agent findings**: `reuse.parse_review_findings` (`io.read_review_findings`, every severity) on the
  review view (`parse_findings`); none exist, so it is empty.
- **Ticket export** — `pipeline/regs/review/tickets.py`: `ticket_text` (mark fields, the reviewer's note quoted, the
  entry / split, what changed since, the work with `vintage.rebuild_commands`), `parse_ticket` (the ROUND TRIP back
  to the mark's fields), `export` (wrong + unsure marks IN FORCE only; superseded ones are not tickets). API
  `GET /api/review/tickets` and `/api/review/tickets.md` (reads); CLI `python -m pipeline.regs.review.tickets
  [--out DIR] [--verdict wrong|unsure]`. The screen's "output" panel links it.
- **Commit marks** — `pipeline/regs/review/commit_marks.py`: `git add` + `git commit --only -- <marks store>`; refuses a
  store that does not load or lies outside the repo; message `review: N marks`. A CLI the screen PRINTS (review mode
  keeps exactly one write route, `POST /api/marks`).
- **`cut_pick` editor (fix mode)** — `ExtentEditor` `CutPick`: per split of a cut extent: ruling E16 (unset) / lower /
  upper / one of the run's candidate measures (`RunCutsCtx`, from the review view) / a measure typed; a pick for a
  split no longer chosen is dropped.
- **Splits mid-review** (acceptance): a split appended to splits.json is in `GET /api/review/splits` (S0, unmarked)
  and the S0 progress on the next load, and every entry row (tier, mark) is as it was — nothing re-queues that is not
  stale (`test_a_split_added_mid_review_heads_the_queue_and_moves_no_entry`; staling on a move is T97 / T134's).
- AGENTS 67; README "The one queue, pre-checks and the output"; API.md.

## Checks
Unfiltered (`rtk proxy`, via `scripts/heavy.sh`, `-m "slow or not slow"`, `UI_EXPORT_REACHES` = full_prev because
the live `reaches/full` was mid-rewrite by another lane): the T134 suites + the new ones —
`test_curation_review_{api,writes,vintage,safety,screen,queue,browser}.py`, `test_review_{marks,vintage,screen,
reaches,queue}.py`, `test_no_silent_skips.py`: **319 passed** (the first run found 2 vintage tests reading the
lower-case "awaiting build" that `refusal_words` had capitalised; fixed — unmatched messages pass through as they
are — and re-run). `pytest -m smoke` 205 passed. Frontend `tsc -p tsconfig.json --noEmit` clean. The CLIs run
against the real store/run: precheck (counts above), tickets (0 tickets), commit_marks `--dry-run` (nothing to commit).
New tests: `test_review_queue.py` (run facts whole + a run without report.json refused; every risk factor in order;
the run moves an entry into a risk tier; the queue order whole incl. S0 splits; every pre-check on one entry whole;
the run's pre-checks + a curated pick; corpus measurements of pre-checks and of the tiers vs PLAN §3; ticket round
trip over awkward notes / rule / split / unsure marks; the ticket text whole; only wrong/unsure marks in force export;
commit touches only the store, staged + edited files untouched, a store that does not load refused; no second copy),
`test_curation_review_queue.py` (above, through the app with a fixture run).
Mutation pins (each named mutant fails the named test): `risks` ignored by the rank →
`test_the_queue_order_whole` (`test_the_queue_order_is_pinned_by_mutation`); the note quoted on its first line only →
`test_a_ticket_round_trips_every_mark_field` (`test_the_round_trip_is_pinned_by_mutation`).

## Owner questions (built with the default; say if not)
1. **PLAN's audit codes A1–A13 were never committed**, so the pre-checks are named for what they check (above), each
   measured; severity A = a fact read from the data, B = a heuristic. *Default:* these 12 codes; name any A-code from
   the audit that is missing and it is added.
2. **An undrawn part is a risk factor** (PLAN §3 lists P1 as starting with "closures with undrawn parts"). It moves no
   closure today (all 58 already had another factor) and 1 quota entry P4 → P3; the "partial" closure in the T134
   test is now P1. *Default:* built.
3. **The review order is the default** (all regions, S0 first); "book (page)" stays one click away. *Default:* built.
4. **The run-based factors read the served reach run's diagnostics**, not the bundle's `rule_ambiguous_cut` table (the
   bundle is the same run's copy once delivered). *Default:* the run, as PLAN §4 says.

## Left / follow-ups
- Licensing records' ambiguous cuts (`licensing_diagnostic`: Thompson, Dean) are not a pre-check or a risk factor.
- The queue computes the pre-checks for every row on every load (~0.1 s for the corpus); the save gate only for the
  open entry, so `gate` shows on the screen, not in the row counts.
- `parse_findings` is untested against a real `reviews/` dir (none exists; `io.read_review_findings` has its own tests).
- Not run: Tier 2 (`scripts/test-full.sh`), `pnpm -C app check` (no app changes).
