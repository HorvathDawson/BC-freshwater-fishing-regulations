# T494 — Web map console error: `this.expression.interpolationFactor is not a function`

Status: DONE — HASH; staging redeployed 2026-11-08 (index-a72c2148): Map ⇄ Conditions ⇄ Runs, 0 console errors.

`TypeError: this.expression.interpolationFactor is not a function at Ca.setUniform` (maplibre-gl-js
ProgramConfiguration). That call is made by a COMPOSITE expression binder: a paint property built as zoom+data
dependent whose expression is now something else. Likely a paint property swapped at run time (`setPaintProperty`, e.g.
a mode switch or T470's `fadeByOrder` mid-zoom fade) from a zoom curve to a non-zoom form, or a `step` where the binder
expects `interpolate`. Not seen in a smoke before 0.6.9 on staging (the staging site ran walkthrough-3 until 11-07).

## Do
- Reproduce on the web build (Playwright, the smoke's phone profile): which layer + property; which mode switch.
- Fix at the cause (style source / the property's expression), never by catching the error (AGENTS 46). Check the
  native side needs nothing (34a).
- Fast-fail (AGENTS 20a): make the smoke's console-error WARN a FAIL for TypeErrors once fixed, or a style check that
  every run-time paint swap keeps its expression kind.


## Outcome (2026-11-08)
Reproduced on staging: entering Conditions or Runs from the Map tab threw in the `stream` layer, `line-opacity`. The T470
order fade made that opacity a zoom AND data curve on the Map tab only (a profile day to fade); other modes handed a
data-only expression, and MapLibre keeps a built tile's binder for the old kind. `paint.ts`: with nothing to fade the
stream keeps the same zoom curve, flat. `expression-kind.test.ts`: every layer's data-driven paint keeps one kind over
every mode, theme and day (fails on the old code). The smoke FAILS on a console TypeError (was a WARN). Native shares
`paintFor`, so it gets the same expression.