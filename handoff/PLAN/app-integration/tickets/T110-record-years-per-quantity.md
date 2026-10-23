# T110 — Record length per quantity: Level-mode weights use the discharge record

Status: OPEN.

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
