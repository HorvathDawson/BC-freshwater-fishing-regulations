# T135 — Curation review phase 4: risk-ordered queue, pre-checks, ticket export, commit marks

Status: DONE — 8d0f5d76, adversarial review fixes 9b8de2e3 (lane-T135). Owner approved PLAN.md 2026-10-23 (defaults Q34–Q41).
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
  and the S0 progress on the next load; no row moves tier or place; a MARKED entry on the split's own water goes
  stale, "binding moved" (`fingerprint.entry_binding_fp` holds every split on the entry's waters, T133), every other
  mark stands (`test_a_split_added_mid_review_heads_the_queue_and_moves_no_entry`, with marks since the adversarial
  review — see below).
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

## Adversarial review (2026-10-23, 9b8de2e3)
Read the diff against PLAN §4 phase 4, re-ran every measurement on the live run (b9be92114323aa22, the same counts as
81e726a3: P1 226 / Z 132 / P2 261 / P3 118 / P4 728 / sweep 60; P1 first factor 2 / 58 / 31 / 121 / 10 / 4; every
pre-check count identical), spot-checked rows on real waters (Ash River: three ambiguous cuts on r1 / r2 with the
candidates and the side taken; Campbell River; Noons Creek's 2 unplaced pieces), checked every run entry / rule id is
in the corpus (all are), that the queue's "binds" equals the app's (`covered_ids`), and the safety pin (review mode
still writes only `POST /api/marks`; the new routes are GETs). Found and fixed:
1. **Unplaced pieces lost.** The builder writes one `unclassified` diagnostic PER EXTENT; `RunFacts` kept one per rule
   (the last), so a rule with two cut extents under-reported. And a payload with `included: true` (placed) counted as
   unplaced. Now a tuple per rule, placed ones dropped; the pre-check counts the union of pieces.
   Test `test_every_extent_s_unplaced_pieces_are_kept_and_placed_ones_are_not` (mutants: overwrite per rule; keep
   included — both fail it).
2. **A cut decided by hand was still a risk.** `precheck` called a `cut_pick` cut a hint (B, `cut_picked`) while
   `tiers.risks` counted it as `ambiguous cut` (P1). One predicate now: `runfacts.is_curated_pick`; `risks` reads
   `RunFacts.open_ambiguous_of`. Test `test_a_cut_decided_by_hand_is_not_an_ambiguous_cut_risk` (mutant: `risks`
   reads `ambiguous_of` → fails). Moves nothing today (0 curated picks on rules).
3. **The pre-check CLI decided S0 its own way, without the marks.** Its docstring said it read the marks store; it
   did not, so a split whose mark went stale was S0 on the screen but not in the CLI, and the S0 set was computed in
   two places. Now `tiers.s0_split_ids` is the one set (reuse and the CLI); the CLI reads the split marks' status
   through `fingerprint.Context.on_disk` (which `reuse._review_context` now uses too) and `marks.split_states`.
   Both CLIs (precheck, tickets) also read the corpus raw, bypassing `io.read_entryfile`'s absorbed-id folding —
   a folded `matched` would read "binds nothing" there and not on the screen; now through it (0 differ today).
   Tests `test_s0_is_one_function_of_splits_json_and_the_marks`, the no-second-copy test (now also `s0_split_ids`,
   `is_curated_pick`, `statuses_now`, `on_disk`, and no `and split_needs_verify(` set outside tiers).
4. **The CLI's tickets never said what changed since.** The API passed each mark's status, the CLI none, while the
   docs said "the same text". `tickets.statuses_now` serves both. Test
   `test_a_ticket_says_what_changed_since_and_the_cli_says_the_same` (edits Frog Lake after its mark; mutant: the API
   passes no statuses → fails). Also: two marks whose targets slug to one file name are refused, never written over
   each other (`test_two_marks_never_share_a_ticket_file`; no collision in the corpus today).
5. **"Commit marks" said "nothing to commit" for a store that lost lines.** It decided by lines ADDED; a rewrite that
   only removes (a migration's carry) was left uncommitted with a false message. Now `store_changed` (git status of
   the store). Test `test_commit_marks_commits_a_store_that_lost_lines` (mutant: `if not n` → fails).
6. **The mid-review split test was vacuous.** It ran with an EMPTY marks store, so "no entry re-queues" could not
   fail. With marks on Adam River (on the split's water) and Ash River: Adam goes stale "binding moved", Ash stands,
   no row moves tier or place. "As built" above corrected; see owner question 5.
7. Docs: `curation-review/frontend/README.md` still listed `flagged` / `unreviewed` and "attention-first"; AGENTS 67
   names the new single functions.
Checks (unfiltered, `scripts/heavy.sh`, `-m "slow or not slow"`, live run): the T134/T135 suites +
`test_curation_review_browser.py` + `test_no_silent_skips.py`: **325 passed**; `test_review_queue.py` 25 passed after
the last fix; `tsc --noEmit` clean; the three CLIs against the real store (precheck counts as above, tickets 0 and 1
on a scratch store holding a real mark, commit_marks `--dry-run`).

### Owner questions (adversarial review; built with the default)
5. **A split added on a water stales every marked entry on that water**, even when no rule names the new split
   (`entry_binding_fp` holds "every authored split on its waters", T133). The acceptance read "entries whose binding
   moved go stale" — this is that, by T133's definition of an entry's binding. *Default:* as built (conservative:
   a new cut on the water may move a `rest` / tributary walk). Say if only entries whose rules NAME the split should go
   stale.
6. **A curated `cut_pick` is not a risk factor** (fix 2). *Default:* built; say if a hand pick should still put a
   closure in P1.

### Left / follow-ups (adversarial review)
- A reach run REWRITTEN IN PLACE keeps its old `report.json` until it finishes (`io.write_run` writes the tables
  first, `report.json` last, and removes nothing), so "no report.json = still being written" only catches a fresh
  directory; a read mid-rewrite can mix tables (the stamp key re-reads once it settles). Fix belongs to the reach
  writer (remove `report.json` first, or write to a temp dir and rename) — not this lane's code.
- In a single-region view the S0 cards still lead the list (splits are not filtered by region).
- `unplaced pieces` is a severity-A pre-check but not a risk factor (PLAN §3 does not list it): Noons Creek is P1 for
  its cut, not for its 2 unplaced pieces.
