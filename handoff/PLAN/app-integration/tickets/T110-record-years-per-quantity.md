# T110 — Record length per quantity: Level-mode weights use the discharge record

Status: DONE — 5a6c0576 (pipeline + app), 1905ba72 (adversarial review fixes). Ships with the next deliver (bundle schema + paint/4): see As built.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding gauges 4
(CONFIRMED on `bundle.sqlite`).

## Decision being unified
"How many years of record does this station have for this quantity?"

## Where it is decided today
- `pipeline/deliver/bundle/build.py:697` `years_by_station = max(years)` across quantities. It feeds
  `panel_member.years` and the TypeScript record factor (`RECORD_FULL_YEARS = 20`).
- `climatology.py` keeps its own minimum-record constant.

How they differ: 07ED001 has 59 years of discharge and 4 years of level, and `panel_member.years` = 59. 323 panel
stations have a level record at least 10 years shorter than discharge plus a live level percentile, so their Level-mode
weights are inflated.

## Work
- `record_years(station, quantity)`; `panel_member` carries years per quantity, and the app's record factor reads the
  one for the mode shown.
- One `MIN_RECORD_YEARS`, imported by `climatology.py` and the bundle (mirrored to TypeScript through the gauge-policy
  emitter, like the other policy constants).
- Pin: a grep test that nothing else computes record years.

## Acceptance
Proof is a reviewed behaviour change: Level-mode estimates over all panels, before and after; Flow mode unchanged.
07ED001's level weight uses 4 years (test).

Rebuild: deliver. Depends: none. Size: S. Owner: no.

## As built (5a6c0576)

**One function, one floor.**
- `pipeline/gauges/feed/record.py`: `record_years(stats, station, quantity)` over clim.json's `stats` (the rows
  `gauge_stats` is a copy of), `QUANTITIES`, and the one `MIN_RECORD_YEARS = 3`. Pure, so the feed half imports it.
- `climatology.py` lost its own `MIN_YEARS` and imports the floor; `panel.py` imports it (and `QUANTITIES`) instead of
  defining them; the bundle imports both. `emit_gauge_policy` mirrors `MIN_RECORD_YEARS`, `RecordQuantity` and
  `RECORD_QUANTITIES` into `gauge-policy.generated.{ts,json}`; core re-exports them from `trust.ts`.
- `weight_of` (Python) and `weightFactors` (TS) hold the floor: a record under 3 years has record factor 0. A no-op on
  real data (the climatology writes no envelope under 3 years, and `eligible` already gated on it); it makes "a quantity
  a donor has too short a record in weighs nothing" one rule on both sides. `one-formula.test.ts` now runs years 0 and 2.

**The record per quantity, end to end.**
- Bundle: `panel_member` and `basin_member` drop `years` for `discharge_years`, `level_years` (0 where none). The panels'
  donors carry `{quantity: record_years(...)}`; `build.py`'s `max(years) … GROUP BY station` is gone, and so is the basin
  roster's `{station: years}` dict that kept whichever `gauge_stats` row came last (the level one).
- `panel_for` gates, weighs and caps EACH quantity by its own record (`years_in`); a donor's overall weight — its place in
  the panel order, which decides the core — is the better of its quantities, identical to what the longest record gave,
  so member order does not move. `Member.years` is a tuple in `QUANTITIES` order.
- `paint/4`: two varints per member and basin row (discharge, level); `paint/3` is refused by both decoders. Live bundle
  re-encoded in memory: 951,644 → 953,530 B gzip (+1.9 KB; gate 1 MB). `pack.generated.ts` regenerated.
- App: `PanelMember.years` / `BasinMember.years` are `RecordYears` (`{ discharge, level }`), so a reader cannot use one
  as a number. `recordYears(record, quantity)` (core) is the one read: `answerFrom`, `contributionsOf` (the map's
  per-class standings) and `basinStandings` all pass the quantity shown. `DonorRow.years` is that quantity's record.

**Tests.** `pipeline/tests/test_record_years.py` (7) and `app/packages/data/src/recordYears.test.ts` (6). Mutation pins,
each run and each failing the named test:
- `recordYears` → `Math.max(discharge, level)`: "07ED001's level weight uses 4 years" (and 3 more).
- `answerFrom` reading `m.years.discharge`: "07ED001's level weight uses 4 years".
- `contributionsOf` passing the discharge record: "the map's Level standing weighs the level record".
- `basinStandings` passing the discharge record: "a basin's Level field weighs the level record, its Flow field…".
- `weightFactors` without the floor: "a record under the floor ranks nothing".
- `panel_for` weighing every quantity by the longest record: `test_a_short_level_record_loses_its_level_seat`.
- The no-second-copy pin: `test_nothing_else_computes_record_years` greps `pipeline/**.py` for a record length read out
  of clim stats / `gauge_stats` / a member row (`.get("years")`, `["years"]`, `max(years)`, `years … FROM gauge_stats|
  panel_member|basin_member`); only record.py (the reader), climatology.py (the producer's run summary) and the bundle's
  one copy into `gauge_stats` may. Both old forms (`max(years) FROM gauge_stats`, `SELECT station, years FROM
  gauge_stats`) match it. On the app side the type is the pin: `RecordYears` is not a number, so `tsc -b` rejects any
  reader that forgets the quantity (it found 17 test files' members).

**The behaviour change, measured** (read-only, the live bundle's panels and the feed index on disk, membership held
fixed; scratch script ports the app's `estimate` via `handoff/PLAN/app-data/live-panels/scripts/evaluate.py`):

| 246,705 panelled sections | Level, before → after | Flow |
|---|---|---|
| unchanged | 97,711 | 107,475 (all) |
| moves under 5 points | 7,629 | 0 |
| moves 5–25 points | 16 | 0 |
| gains an answer | 98 | 0 |
| loses its answer | 1,302 | 0 |
| silent before and after | 139,949 | 139,230 |

Level answers 105,454 sections after against 106,658 before (−1.1%). The losses are panels whose only level voice is a
distant tributary donor with a short level record: panel 1739 (Sukunka tributaries, 3–7 km²) is 07FB003 alone — 48 years
of discharge, 10 of level, a tributary 500x the reach — weighing 0.095 with the 48-year record and 0.047 with the
10-year one, under `MIN_TOTAL_WEIGHT` (0.05). Flow is unchanged: the 6 panel stations whose discharge record is shorter
than their level one change no answer on this feed. 386 panel stations change their level record factor.
07ED001's members now read (59, 4).

The basin field (zoomed out): Flow changes in 41 of 190 reporting groups, all under 5 points (largest ALBN 12.4 → 8.6);
Level is unchanged — the roster had been carrying the level record all along, so this is the Flow field's fix.

The per-quantity CAP can also reseat Level members (a donor with a short level record yields its level seat to a
longer-record level gauge), and Flow members in up to 77 panels (see Adversarial review). That needs the deliver to measure; it is bounded: 174 of 4,724 panels (829 sections) hold a
full eight level-reporting members, the only place the cap binds.

**Deliver needed: yes.** The bundle schema (`panel_member`, `basin_member`) and the paint format change, so the app
built from this commit refuses the current release's `paint/3` until `deliver` rebuilds the bundle and the release.
Check after: `sqlite3 bundle.sqlite "SELECT DISTINCT discharge_years, level_years FROM panel_member WHERE station='07ED001'"`
→ `59|4`; then the Level before/after above, re-run on the delivered panels to catch the cap reseating.

## Adversarial review (2026-10-23)

Read the diff and the ticket against the live bundle; mutated the code to check the pins. Three defects fixed, one claim
corrected.

- **The basin field had a second record factor, and the floor did not reach it.** `basinStanding` (core/trust.ts)
  computed `Math.min(1, Math.max(0, v.years / RECORD_FULL_YEARS))` itself, so the "under 3 years weighs 0 on both sides"
  rule held for the reach panels and not for the basin field. Fixed: one `recordFactor(years)` in trust.ts (ratio, cap
  and floor), used by `weightFactors` and `basinStanding`. On real data this changes nothing: a station with under 3
  years has no envelope, so it casts no basin vote. Pinned by "a basin vote under the floor weighs nothing"
  (data/src/recordYears.test.ts) and by `app/tools/record-factor.test.ts`, which greps every non-test source under
  app/packages for `/ RECORD_FULL_YEARS` and allows only trust.ts. Putting `basinStanding`'s own copy back fails both
  tests.
- **The Python grep pin missed a reduction of the new tuple.** `READS` caught `max(years)` and the SQL forms, but not
  `max(m.years)` on a `Member` (or `max(discharge_years, level_years)`), which is the easiest way to undo T110 now.
  Widened it to `max|min|sum(` over `years` / `discharge_years` / `level_years`, with or without an owner. It excludes
  `years[st]`, the climatology's own from/to years. Appending `max(m.years)` to build_panels.py fails
  `test_nothing_else_computes_record_years`.
- **Corrected claim: the Flow cap can reseat too, not only the Level cap.** Each quantity's seats are now ranked by
  that quantity's weight. A donor whose level record is longer than its discharge record ranks lower in Flow than
  before. Measured on the live bundle: 429 panels hold eight discharge-reporting members, and 77 of them (376
  sections) hold such a donor (9 stations have a longer level record than discharge). So "Flow unchanged" holds only
  with membership fixed, and the deliver's re-run has to cover Flow as well as Level. Level's bound reproduces as 171
  panels / 829 sections. The panel count here is distinct `section_panel` panels; the 174 above counts panels a
  different way. The section count agrees.
- Checked and sound: `measures` is `gauge_stats` parameters, which exist only at 3 years or more, so the new
  `wq[q] > 0` seat test excludes no donor the old measure test admitted. The overall order (max over quantities)
  equals the old longest-record weight because `weight_of` and `eligible` are monotone in years. 07ED001 is 59/4 and
  08LD001's basin row read 7 in Flow (bundle checked). The builder's mutations were spot-run: `recordYears` → max, and
  basin → discharge. The build.py comment "after gauge_stats because they read it" is still true: `measures` reads it.

Checks: `REGS_STORE_REQUIRED=1 pnpm -C app check` through heavy.sh passed (335 files, 8,029 tests). Full pytest
(unfiltered, heavy.sh): 3,456 passed. The 18 failures and 1,297 errors are data-dependent (no `data/generated` in the
worktree) or unrelated to T110. Those are `test_entry_id` (fingerprint.py, changed on dev since), `test_fixture_release`
(the manifest list lacks `access`, a stale list) and gate 4 (calendar spec in change.ts/dayPicker.ts). The T110 modules
(test_record_years, test_panel, test_basin_station, test_build_panels, test_release, test_live_panels, test_own_gauge)
are all green.

## Owner questions
- 1,302 sections lose their Level answer because a short level record now prices a distant tributary donor under
  `MIN_TOTAL_WEIGHT`. Built default: they go silent ("no baseline") — the same rule every other panel follows; the
  answer they had was weighted as a 48-year record it does not have. Alternative: none proposed (a separate Level floor
  would be a second constant).

## Left / follow-ups
- The archived measurement scripts under `handoff/PLAN/app-data/` (live-panels/evaluate.py, own-gauge/*, side-channels/*,
  measure/*) read `panel_member.years` and will not run against a T110 bundle; port them when next used.
- `app/packages/core/src/conditions.ts` words "only N years of record" under a typed `10` — a wording threshold, not the
  floor, but a typed constant all the same; name it if it is ever tuned.
