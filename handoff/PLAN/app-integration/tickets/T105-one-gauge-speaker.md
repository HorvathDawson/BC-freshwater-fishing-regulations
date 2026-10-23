# T105 — One gauge speaker: the map and the sheet ask different stations

Status: OPEN.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, findings gauges 1 and
12 (CONFIRMED on `bundle.sqlite`).

## Decision being unified
"Which gauge speaks for this section?"

## Where it is decided today
- Map: `app/packages/data/src/standing.ts:59` `sectionStanding`: the panel, then the lake station.
- Sheet: `app/packages/ui/src/panel.ts:726-736`: panel rows, then `matched?.station` from `section_gauge`, with no lake
  path.
- Pipeline: `section_gauge` comes from `pipeline/gauges/consume/shed.py` `trust_for`, which has no regulation or
  record gate. The panel's gate is `pipeline/atlas/gauges/panel.py:421` `eligible`.

How they differ: 54,401 sections have a `section_gauge` row and no `section_panel` row. The map leaves them uncoloured
while the sheet quotes a station for them. Gauged lakes show a colour on the map but no reading on the sheet.

## Work
- One `speakerFor(section)` in `@app/data` `standing.ts`, used by both the map standing and the sheet.
- Pipeline: derive `section_gauge` from panel member 0, or stop using it as a reading source (it stays a match record).
  Coordinate with T93 (the gauge ↔ section relationship policy) and do not change the drainage gate (memory
  `gauge-representativeness-is-policy`).
- Pin: an app test (one-* style) that `section_gauge` / `matched.station` is read for a reading only inside
  `speakerFor`.

## Acceptance
Proof is a reviewed behaviour change. Report the count of sections whose sheet station changes and the gauged lakes
that start showing a reading. Map and sheet agree on the speaker for a sample over every region (test).

Rebuild: app, plus deliver if `section_gauge` changes. Depends: T93 (policy, default as built). Size: M. Owner: no.
