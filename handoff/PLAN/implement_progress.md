# Implementation progress (IMPLEMENT-HANDOFF.md)

Restart here. Update this file after EVERY step: what is done, what is running, and the exact next action.
Logs from timing runs live in the session scratchpad, which a reboot wipes. The numbers that matter are copied here.

## Owner answers (2026-10-20)
- Heavy jobs: allowed now for the day-1 baseline (via heavy.sh, one at a time, ≤4 workers).
- Order: **P00 first**, then P05 + P03 (then P06, N1 scaffold).

## §7 First day
| step | state |
|---|---|
| 1 read docs | AGENTS.md + IMPLEMENT-HANDOFF read; APP-PREP §0–§1, MINIMAL-PACKAGING, integration README still to read before P05/P03 |
| 2 log / pause / go-ahead | HEAD 7a326133; UR7 committed; no `heavy-job.pause`; go-ahead given |
| 3 reader files | ALREADY DONE in a4b37eff (`app/design/regs-v11/port/reader/`) |
| 4 baseline | DONE, see Baseline counts |
| 5 golden re-sync | NOT NEEDED: golden.json meta ref 7fb6809f, v38, misses unchanged (4) |
| 6 first tickets | P00 DONE (a4ee3a9b) → P03 DONE → P05 (next) → P06 → N1 |

## P00: test tiers
- [x] measure: pytest fast (durations), pytest slow (collect count only), vitest per file, `pnpm check` per step, check.mjs total + per round, card gate
- [x] pytest `smoke` marker (registered in pytest.ini) on ~1 test per gate/invariant, no live-bundle scans
- [x] vitest smoke (not needed: whole suite 5 s) filter/project
- [x] check.mjs `ONLY=smoke` (NOTE: check.mjs is in the prototype lane, rule 12; P00 asks for it explicitly, so the edit is additive only)
- [x] `scripts/test-smoke.sh`, `scripts/test-full.sh`; one line in AGENTS.md
- [x] commit with a before/after time table

### Measurements (2026-10-20, laptop, through heavy.sh)
- pytest fast (default `-m "not slow"`): **3572 passed, 6 skipped, 264 deselected in 504.8 s (8:24)**, wall 528 s,
  serial (no xdist). 196 files. The top 6 files take 50 %: test_anchors 75 s, test_pfma_drainage 53 s,
  test_curation_review_api 43 s, test_export_ui_rules 37 s, test_steelhead_streams 33 s, test_competition 29 s.
  811 of the fast tests carry a `needs_*` marker. 11 files hold only `needs_*` tests.
- pytest slow: 264 tests (collect only, not run).
- `pnpm -C app check`, per step: boundaries 0.3, platform 0.3, style 0.4, colours 0.3, spinner 0.4, **fixture 8.4 (FAILS)**,
  gauge-policy 0.3, calendar 0.3, deps 0.3, typecheck 0.7 (incremental), test 4.7 s. Total ≈ 17 s.
  - vitest: **58 files, 642/642 passed**. The slowest are check-boundaries.test 2.9 s and agree.render 1.9 s; the rest are < 1 s.
- check.mjs full: **136/136 in 439 s** (7.3 min). By round: round20 94 s, core/unattributed 94 s, 20e 45 s, ur6 26 s,
  18 26 s, 20b 22 s, 20c 21 s, 20h 17 s, 19 16 s, ur7 16 s, ur1 14 s, 20g 13 s, ur5 13 s, 20f 7 s, int1 6 s, ur2 4 s, ur3 4 s.
- card gate (shipped answers): **0 problems**, 15,862 groups on 7,144 distinct cards, 87 s wall.

### P00 result: Tier 1 = 74 s (app check 13 s, pytest smoke 40 s / 185 tests, check.mjs smoke 21 s / 18 checks)
- pytest: repo-root `conftest.py` marks the ids in `pipeline/tests/smoke.txt` (the fastest no-needs test per file, 185 of
  196 files; 11 files have only needs_* tests). A whole-tree `-m smoke` run exits 4 naming any stale id (mutation-checked).
  The default and slow collections are unchanged (3578 / 264).
- vitest: no smoke split needed. The whole suite is 5 s, so Tier 1 runs all of it inside `pnpm check`.
- check.mjs `ONLY=smoke` = no-hand-facts + ur2, ur3, int1 (all 134 states rendered and scrolled), 20f. This is one
  additive line in a prototype-lane file, which P00 asked for.
- The rebuilt dev fixture was committed separately (P03's fixture item), because it blocked `pnpm check`.

## P03: CI gaps (DONE)
- f6332295: the rebuilt dev fixture. APP-PREP calls this regeneration "human-run", but `pnpm check`'s fixture step
  rebuilds it on every run anyway; committing it was the only change. Flagged to the owner.
- app-ci now runs `pnpm check` itself (this adds colours, spinner, fixture, calendar, gauge-policy and answers) with a
  `.venv` from requirements.txt. It also triggers on `schema.sql` / `indexes.sql`.
- pipeline-ci triggers on `app/packages/core/src/*.generated.*` (it was calendar only). `reference/calendar.generated.js`
  is under `pipeline/**`.
- `app/package.json` `answers` script, inside `check`.
- `app/packages/data/dev/fixture-schema.test.ts`: the fixture DDL equals schema.sql + indexes.sql. On the old fixture it
  FAILED (6 missing tables + section_ruleset changed); it passes now.
- Mutation: a hand edit to answers.generated.ts makes `pnpm -C app check` exit 1 ("stale"). Clean run: 59 files / 645 tests.
- NOT verified: a real CI run on a scratch branch (nothing pushed). Open: the fixture writes meta `shed_rule` where the
  bundle writes `trust_bands`; no app code reads either, so it is left for whoever next touches build-fixture.mjs.

## Baseline counts
- pytest fast: 3572 passed / 6 skipped (unfiltered; matches the UR7 report's 3462 plus the tests added since).
- `pnpm -C app check`: **RED at `fixture`.** `build-fixture.mjs` is deterministic (two rebuilds are byte-identical), but the
  committed `app/packages/data/dev/bundle.sqlite` is stale. The rebuild adds 6 empty tables (item_down, mu_set,
  region_place, section_straddle, trib_nearer, unnamed_part) and changes the `section_ruleset` schema. This is audit #5, so
  it **belongs to P03**: commit the rebuilt fixture there. The tree was restored to HEAD after measuring.
  Every step after `fixture` passes when run alone.

## Not verified
(fill in)
