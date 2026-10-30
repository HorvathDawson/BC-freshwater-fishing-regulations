# T296 — A gauge reached through a lake counts for little when a closer gauge (no lake between) exists

Status: OPEN (owner 2026-10-29, on T252's donor paths through lakes).

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
