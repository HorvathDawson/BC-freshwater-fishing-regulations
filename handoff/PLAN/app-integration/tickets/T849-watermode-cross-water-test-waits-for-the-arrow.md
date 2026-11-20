# T849 — WaterMode's cross-water test waits for the ▲ before it taps (a CI flake)

Status: DONE — 36559a549. WaterMode's cross-water taps wait for a settled, enabled arrow; load proof 10/10 twice.

Closed by the 2026-11-20 audit: fix built on the branch and merged as 36559a549 (2026-11-18).

Source: app CI on d6cbad35d, attempt 2 (2026-11-18): "across waters: ▲ on the Vedder opens the Chilliwack's map at its
lowest part IN ITS PLACE" failed at 22.1 s, the first `until()` (20 s) after the "Upstream into Chilliwack River" tap.
Passed on attempt 1 and locally 9/9 on the same code. Under CI load the tap lands before the screen is ready, nothing
moves, and the wait can never succeed.

## Fix

Wait until the arrow is ready (the screen settled on the Vedder's top part, the button enabled) before each tap, or
re-tap inside the wait; prove it under load (several suites at once). Not an app fault; the app's own tap handling is
untouched.

## Done (branch worktree-agent-a2d88a582cedab642)
`WaterMode.test.tsx`, the cross-water test: every ▲/▼ tap goes through `tap(place, button)`, which waits until the bar
stands at the place the last step left (`where()`), is not `aria-busy` (a step's read in flight), and the arrow exists and
is not `aria-disabled`, then clicks once inside `act` (no re-tap, so no double move). The same-water "▲ twice" test is
not a cross-water tap and was left. Load proof: 10 runs of the file with 2 `vitest --project render packages/ui-native/src`
runs going: before 9/10 (flake reproduced), after 10/10 twice.
