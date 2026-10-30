# T296 — A gauge reached through a lake counts for little when a closer gauge (no lake between) exists

Status: PARTIAL — code done 2026-10-30 (owner accepted the rule; map + saved spots added); rides the next bundle +
release (paint/5) + APK.

Source: owner: "the condition gauge through a lake we need to be careful of. if no gauge is downstream I agree that is fine
but if there are other gauges closer they should take almost full amount of the prediction because through a lake acts as
a natural dampening system for the measurement. so it becomes less important."

## Work
1. Where a water's conditions draw on more than one gauge (donor panels, conditions/forecast blends, the spot similarity's
   flow), a gauge whose path to the water passes through a lake is down-weighted heavily (lakes damp the signal) whenever
   another gauge with no lake on its path is available — that one takes almost all the weight. A through-lake gauge stays
   the full answer only when it is the only one.
2. Say it in the Conditions panel ("measured below Pitt Lake — the lake smooths changes, so read it loosely").
3. Gauge representativeness is owner policy (memory): record this rule in decisions.md; the build carries the path's lake
   crossing in section_gauge / the donor path so the app can apply it.

## Acceptance
Upper Pitt: closer non-lake gauge (if any) dominates; with only the through-lake gauge, it is used with the caution line.
Tests pin both cases.

Depends: T252. Size: S–M. Owner: policy recorded (this ticket).

## Built (2026-10-29)
- **Rule** (decisions.md "a gauge reached through a lake"): `lakeDamped` (core/trust.ts) ↔ `lake_damped` (panel.py),
  `THROUGH_LAKE_FACTOR` 0.05 generated into core (`gauge-policy.generated`). A through-lake vote is scaled and loses its
  lead only while a no-lake vote reports; all-through-lake panels are untouched. Applied in `answering`/`combineVotes`
  (both languages) and in `answerFrom`'s row shares.
- **Route**: `routesOf` marks `PanelRoute.throughLake` / `lakeName` / `lakeSide` (the first lake strictly between the
  spot's reach and the gauge's; `lakeOnPath`). A reach is a lake by `section_down.lake` (`HAS_DOWN_LAKE`/`LAKE_OF`),
  else by its water's kind.
- **Sheet**: `useConditionsView` walks the routes first (`useRoutes`), hands their lake stations to `usePanel`
  (`withThroughLake` → members' `throughLake`), refreshed in place (same `sameAs`, no loading frame). `usePanelRoutes`
  became `useRoutes` + `useJoinedRoutes`.
- **Words**: `throughLakeNote` / `THROUGH_LAKE_WORDS` (core) → `DonorVM.throughLake` → DonorPanel, beside the answer
  (`section-through-lake`), never folded.
- **Build**: `section_down.lake` (schema, contract, pack regenerated; `shed.lake_reaches`, written in bundle_tables).
- **Measured** (live bundle, named-lake approximation): 60,010 named panel sections with routes — 0 mixed, 673 all
  through-lake (Bowser 56, Lillooet 31, Toodoggone 29, Pitt 24, Shuswap 24 …) get the caution.

## Tests
`app/packages/data/src/throughLake.test.ts` (upper Pitt both cases: routes through Pitt Lake below it; the lake column
first, unnamed lake, a 0 overruling the kind; untouched weights + own gauge still leading; with Alvin 08MH017 above the
lake >90% and the arithmetic exact; the caution words); `app/tools/one-formula.test.ts` (4 lake panels, Python ↔ TS);
`pipeline/tests/test_gauge_shed.py::TestLakeReaches`.

## Map + saved spots (owner 2026-10-29: "the map should match the data in the click")
- ONE implementation: the build flags each member whose route crosses a lake — `panels.through_lake_panels`
  (`route_interior` mirrors `routesOf`, over the release's own `section_down` + `lake` rows; a lake reach with no row
  is none, as the app reads it) → `Member.lake` → `panel_member.lake` → paint/5 (one varint per member). Panels are
  re-interned on the flag (split only where a section passes a lake). The app reads it into `PanelMember.throughLake`
  + `Panel.lakeKnown`; the map's `panelStanding`, the sheet's `answerFrom` and a saved spot (`captureSpot`,
  `panelOnDay`, `panelOfAnswer`) all go through `contributionsOf` → core `estimate`/`answering` (`lakeDamped` inside)
  — the same function the one-formula test pins to Python. No weight is precomputed.
- On paint/5 the sheet ignores route-derived lakes (`withThroughLake` returns the panel when `lakeKnown`); on paint/4
  (older releases, readable) the map knows no lakes and the sheet uses its routes as before.
- Paint: core decoder reads 4 and 5 (`Paint.lakes`); an older APK refuses 5 by its version byte, so the release is
  published with `--min-app-version` = the new APK's version (older apps keep their data and ask to update).
- Tests: `throughLake.test.ts` "the map's colour is the sheet's answer" (upper Pitt: only through-lake, and with a
  no-lake gauge; release flags beat route lakes); `paint.test.ts` (v5 fixture, `lakes`); `test_gauge_shed.py::
  TestThroughLakePanels`; `test_release.py` paint round trip + regenerated app fixture.

## Left
- Build: `python -m pipeline.deliver bundle` (writes `section_down.lake`, `panel_member.lake`, prints "lakes: … panels
  carry a member across one") → release build → publish with `--min-app-version <new APK version>` → APK. Until the
  bundle is rebuilt, `test_release.py::test_paint_reproduces_the_status_index_and_the_flow_tables` errors (the paint
  encoder refuses a bundle without `panel_member.lake`) and `test_a_built_release_is_exactly_the_contract` fails on
  LATEST (no `section_down.lake`). Check the paint web gate in `release-budget.test.ts` on the new release (+1 byte
  per member, a few more panels).
