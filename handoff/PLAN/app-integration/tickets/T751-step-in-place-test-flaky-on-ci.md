# T751 — App test "▲ on the Vedder opens the Chilliwack's map … IN ITS PLACE" is flaky on GitHub's runners

Status: OPEN (before the final build: a red-then-green test hides real reds)

Source: iOS chat, 2026-11-15. The app test "the water's Map view and the rest of the water screen agree … across waters:
▲ on the Vedder opens the Chilliwack's map at its lowest part IN ITS PLACE" went red then green on identical code twice
in one night: ios/integration 107207143 → 3212f4936, and ios/t739-push ca91889bb (run 43694796944: first attempt red,
re-run green). It asserts `expected false to be true`. It passes locally.

## Likely cause (not proven)
A real-timer wait too short for a slow runner. T731's `useKeptWhilePending` holds the previous view for `PANE_HOLD_MS`
while the next water loads; the test probably checks before the hold resolves on a loaded runner. The same class as the
TapCue test fixed in c3c5850ec (fake timers).

## Work
1. Reproduce: run the file with the CPU throttled or with many workers until it fails; name the wait that loses.
2. Put the test on fake time, or wait on the condition instead of a duration. No longer timeout as the fix.
3. Mutation-check that the test still fails when the step is NOT in place.
4. Look for siblings: other tests near `StepInPlace.shell.test.tsx` that wait a fixed time across a `PANE_HOLD_MS` hold.

## Acceptance
- 50 runs in a row green with the CPU throttled; the mutation still fails it.
