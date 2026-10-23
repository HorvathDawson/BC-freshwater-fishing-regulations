# T125 — Latent duplicate checks: pins and small helpers (agree today)

Status: OPEN.

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
