# T123 — One donor state: the Conditions lines and the map label read a donor differently

Status: OPEN.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding gauges 6
(CONFIRMED in code; mostly masked today).

## Decision being unified
"What state is this donor gauge in: reads, unranked, silent, no forecast?"

## Where it is decided today
- `app/packages/core/src/conditions.ts:174-236`: `donorReads`, `donorUnranked`, `donorSilent`, `donorWeightLine`.
- `app/packages/core/src/routeMap.ts:31`: `donorMapLabel`.

How they differ: with a horizon, one says "no forecast" and the other says "quiet". It is mostly masked because
`donorSilent` hides those pins.

## Work
- `donorState(donor, horizon)` returning one tagged state; every label is a `switch` over it.
- Pin: an app grep test that no module outside the state function tests a donor's reading fields directly.

## Acceptance
For each state a fixture shows the same word in the Conditions line and the map label. Pure refactor otherwise (app
shots unchanged except the horizon case).

Rebuild: none (app). Depends: none. Size: S. Owner: no.
