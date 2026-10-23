# T125 — Latent duplicate checks: pins and small helpers (agree today)

Status: PARTIAL — 8a9dd913 (1), b47d770e (2), 697337aa (3), c0ca375f (4), 6e579b3a (5), fb6f70b5 (6), cdbd9b53 (7),
59ec2784 + 69150d8c + 90936600 + 373fe8ec + ade6cfe6 (8: matching 6/7, app 9/11, reach 9, answers 10/11), 4357b046 (9).
Lane lane-T125 (wf_144b677f-89c-3). Left in item 8: gauges 9, answers 12, reach 11 (see Left). Rebuild: reach rerun +
deliver (items 4, 5 ride the next reach rerun; 1-3, 8 ship with deliver); app check after the regs fixture re-cut.

## As built
1. `vocab.says_no_fishing(kind, take, may_target)` is the one closure test: `bundle.rules._is_species_closure`,
   `is_blanket_closure`, `closure_grade`, `labels._is_closure` + the label branch, the model's `closure_kind` and
   `suspended_while` checks and the export's "closed" example ask it. `verdicts.project.closes_here` is the one "speaks
   as a full closure, not partly lifted": `project.closes`/`closing` and `status_index.closes` ask it. `rows._closed_gate`
   is gone (it was `display.is_closure_gate`). Pins: `test_the_three_readers_ask_closure_grade` now covers every reader;
   `test_every_typed_closure_test_is_says_no_fishing`; `test_no_closure_test_is_spelled_twice` (AST over pipeline/,
   mutation-pinned on four old spellings).
2. `rules.keeps` (was `yields_to_release`; read.py's local `keeps` variables renamed `kept`). `display.record_kept` asks
   `keeps(y)` and its own while/caught exclusion from the fields; the origin is the rule's field, no dimension regex.
   Measured on the live bundle: the old test differed from `keeps` on 157 rules — 49 unlimited quotas and 108 size-only
   clauses (take None + lengths) that the old take-above-0 test called "no keeper"; origins agree on all 6 record rules.
   Pin: `test_4_a_keeper_is_what_rules_keeps_says` (whole values; `keeps → None` mutation; no `dimension` read).
3. `RegistryBoundary.answers_to()` = (id, *aliases without `split:`). The parse menu unions the aliases of every item
   sharing a boundary id (was `setdefault`); `extent._refs`/`_meta`, the bundle's place names read it. Measured on the
   live registry: 28 boundary ids differ in alias sets; 0 of 1,367 matched corpus entries' menus change. Pins in
   `test_parse_context` (fails on `setdefault`) and `test_registry`.
4. `build_reach` collects `resolve_extent(reasons=)` per extent; `classify._why(reasons)` maps the first explained code
   (`classify.UNBOUND_REASON`), `_cut_exists` is gone. The resolver now says the two sentences the run ships
   (`no_sections_for_items`, `cut_not_on_this_water`) and when a resolved extent selects nothing (`selects_nothing`,
   `between_selects_nothing`). Behaviour where the re-derivation guessed: `between_cuts_not_ordered` →
   `parallel_branches` (was `cuts_collapsed`); outside_*/no_splits/unsupported failures → `unknown` naming the code.
   Pins in `test_reach_builder` (each code → one reason, resolver→classify round trip, no-re-derivation mutation).
5. `classify.apply_limits(sections, kinds, kind_of, outside, tidal)` → (kept, steps), the global limits in one order;
   classify (kinds, border), `_without_tidal` (tidal) and the steelhead twins (all three — they took neither border nor
   tidal before) ask it. Pins in `test_reach_limits` (order, twin mutation, no reader applies a limit itself).
6. `emit_gauge_policy` also emits `PERCENTILE_KNOTS` (climatology `PCTILES`), `MIN_TOTAL_WEIGHT` (panel.py) and
   `FORECAST_DAYS` (publish `HORIZONS`); core's `flow.ts` KNOTS + standing breaks, `trust.ts` and `conditions.ts`
   HORIZONS read them; publish's `percentile_of` reads `PCTILES`. `FEED_TTL_MS` (data/feed/http.ts) is the one feed
   lifetime; ANSWER_TTL_MS, SYNC_POLL_MS, FEED_POLL_MS, READING_POLL_MS are it. No value changed. Pins:
   `app/tools/one-gauge-policy.test.ts` (incl. the map's standing ramp stops on the knots), `test_gauge_policy`.
7. `panel.py` `Contribution` / `answering` / `estimate`: a line-for-line mirror of trust.ts `answering` (lead shares,
   probit mean/spread, interpolated error + class, every refusal, the measured exemption, the reserve standing back);
   `combine` shares its arithmetic (`_mean_spread`); `lead_shares(own_share=)` for a script trying another floor. The
   four handoff scripts (live-panels/evaluate, own-gauge/evaluate + compare_own, side-channels/compare) import it.
   Note: their earlier recorded measurements were taken with the hand ports (row error, no lead share, no reserve).
   Pins: `one-formula.test.ts` "one combine, two languages" (24 panels; mutation-pinned: row error, leader's
   exemption), `test_panel` (no script rolls its own).
8. Done: matching 6 (`is_water` in the matcher, registry MU overlay, review app search), matching 7
   (`registry_kinds.area_key`, the resolver's three limits + place names), reach 9 (`outside.has_own_row`, both
   `rowed_waters` and `rules._own_rows`; 1,394 rowed waters before and after), answers 10
   (`read.steelhead_stamp_period`), answers 11 (`catalogue.held_days`: "no dates = the year" was a Jan 1–Dec 31 range
   in `rules._when_term` and `range(1, 367)` in licensing; `display._days` is another question — days IN FORCE, weekdays
   as part — and stays), app 9 (`WEEKDAY_NAMES`/`weekdayAbbr` beside `MONTHS`/`monthAbbr` in flow.ts; four copies
   gone), app 11 (`day.isIsoDay`; three regex copies gone). Pins: `TestNoBypass`, `test_a_row_of_its_own_is_one_
   predicate`, `test_the_stamp_period_is_one_answer`, `test_held_days_is_…`, `app/tools/one-day-table.test.ts`.
9. Not real. The reader is asked one day (and one moment) and a rule out of force takes no part, so `statement` never
   needs the season. Pinned whole: `test_two_quotas_with_different_seasons_compete_only_where_both_hold`.

## Measured
Baseline: the batch-5 rebuild (reaches/full digest b9be92114323aa22, release r-012fb43aa175; its bundle, verdicts,
status index, regs store and export/answers files snapshotted read-only before a later deliver replaced them).
- Reach (items 3, 4, 5, 8 — everything in the reach code), this lane's code in-process on the live atlas + corpus:
  `digest`, `licensing_digest`, steelhead/tidal/outside/straddle digests all equal; rule_unresolved 29 = 29 rows with
  identical reasons AND details; rule_diagnostic 1,001 = 1,001; licensing_diagnostic 186 = 186; licensing_placement
  108 = 108. No binding moved (so the twins held no border/tidal section, and the 22 `no_sections_for_items` rules keep
  their words).
- Deliver (`pipeline.deliver all` into the scratchpad from the live atlas + reach run, this lane's code incl. T109):
  bundle.sqlite all 63 tables identical; verdicts.sqlite 13 tables identical; status_index.bin byte-identical;
  ui-rules-answers.json byte-identical (so item 2's 157 newly-counted keepers change no paper-licence line);
  ui-rules-export.json byte-identical (T109's region is derivable, dropped on the wire); ui-rules-guide.json differs
  only in `field_dictionary.rule.provenance` (+region) and `field_dictionary.encoding`; card gate 0 problems (7,159
  cards, 16,004 groups). regs.sqlite: the `rule` table's 3,405 blobs differ only by the added `prov.region` (1: 496,
  2: 370, 3: 393, 4: 479, 5: 489, 6: 382, 7: 372, 7a: 29, 7b: 41, 8: 260, null: 94); `zdict` retrained, `meta`
  store_digest / export_subset_sha256 / card_check code_sha moved with it.
- `REGS_STORE_REQUIRED=1 pnpm -C app check`: every gate passes up to the tests; tests 7,680 pass, 284 fail, ALL in
  conformance/src/regs/{bait,gear,sheets,methods,harness}.parity — the committed regs fixture predates T109's
  `prov.region`, so its rank-3 rules tag "Region rule" ("Single barbless hook, in streams Region From the Region 4
  table…"). Fixed by the fixture re-cut after deliver (T109).

## Left / follow-ups
- Item 8, gauges 9: `promote.evidence` compares the station name only with the match's display name (`m.name`), while
  the matcher accepted any of the node's `name_tuples` (Arrow Reservoir vs Lower Arrow Lake), so such stations can never
  auto-promote. Fix needs the match record to carry the name it matched on (a `StationMatch` field → match.json
  regenerate) and an owner-run promote (`--accept-demotions` review, AGENTS 38) — not done here.
- Item 8, answers 12 (fish-list wording) and reach 11 (double computation): the audit's file/line references are not in
  the ticket and I could not identify the copies with confidence; need the audit's pointers.
- `pipeline/tests/test_entry_id.py::test_no_entry_id_is_parsed_outside_entry_id_py` fails on the base branch
  (pipeline/regs/review fingerprint/precheck/tiers inline prefix tests, T135/T142 lanes) — not this lane's.
- `test_dataflow_gates.py::test_gate_4_one_calendar_spec` fails on the base on `regs/edge.test.ts` (and did on
  dayPicker.ts / change.ts, which item 8 fixed).

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, tier 3 findings.
Each item agrees today or is low impact, so each needs only a pin or a small helper. Small related items, one ticket
(README rule). Do each as its own commit.

## Items
1. **Closure grade** (answers 9, cross 11): `take == 0 and may_target false` is re-inlined in `rules.py:462, 649`,
   `labels.py:463` and `export_ui_rules.py:2540`; `status_index.closes` and `verdicts/project.closes` are twins. 0
   differ today. → extend `test_the_three_readers_ask_closure_grade` to every reader; delete `rows._closed_gate`.
2. **Does this rule keep a fish?** (answers 8): `yields_to_release` against `display.record_kept`, which regex-reads
   `dimension`. → `rules.keeps()`.
3. **Ids a cut-point answers to** (matching 3): `parse_context.py:105-115` `setdefault` drops a second item's aliases
   for a shared boundary id; 28 boundary ids have differing alias sets. → `RegistryBoundary.answers_to()`.
4. **Why a rule did not bind** (reach 10): `classify._why` re-infers what `resolve_extent(reasons=)` already knows; all
   29 agree today. → `_why` reads the reasons.
5. **Global limits** (reach 7): applied piecemeal; the steelhead twins re-implement them. They agree today. →
   `apply_limits()`.
6. **Policy constants mirrored with no guard** (gauges 10, cross 12, app 6, app 7): `MIN_TOTAL_WEIGHT`, ranking knots,
   standing breaks, `HORIZONS`, map ramp stops; `FEED_TTL_MS` copied 4 times. → extend `emit_gauge_policy` or the
   `pyConst()` tests; one `FEED_TTL_MS`.
7. **Python `combine` is not a mirror of `trust.ts`** (gauges 8), and the handoff evaluation scripts each roll their
   own. This affects how owner decisions were measured, not what users see. → the scripts import one `combine` that a
   parity test pins to `trust.ts`.
8. **Smaller duplicates (low):** `promote.evidence` ignores aliases (14 stations can never auto-promote; gauges 9);
   `has_own_row` / `is_water_row` (reach 9); `registry_kinds` bypasses (matching 6); `area_key` (matching 7); the stamp
   period computed twice (answers 10); `when → days` ×4 (answers 11; fold into T104's module); fish-list wording
   (answers 12; partly intentional per ROUND15 1, keep the intentional split named); ISO-day validity (app 11);
   month/weekday tables (app 9); double computation (reach 11).
9. **Open question, not a unification:** `rules.statement` omits `when`, so the reader's step 6 could treat two quotas
   with different seasons as one statement. Check with a fixture; fix if real. (The audit dropped "one same-statement
   function": `statement`, `rows.restates` and `gear.vessel_says` answer different questions.)

## Acceptance
Each item: a pin (grep/AST or parity test) that fails on a re-inlined copy, and an empty answers / binding diff (items
3, 5, 8's `promote.evidence` may change behaviour: reviewed diff, each change named).

Rebuild: none for most; deliver for 1–3 and 8's answers items; reach + deliver for 4–5 (ride another reach rerun).
Depends: T104 (item 8's `when → days`). Size: M. Owner: no.
