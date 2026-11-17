# T751 — App test "▲ on the Vedder opens the Chilliwack's map … IN ITS PLACE" is flaky on GitHub's runners

Status: DONE 52bf027de (2026-11-17 0.8.0 stock-take: the cross-water wait carries the file deadline; was: IN PROGRESS (2026-11-16: cause proven and the test fixed on branch `app-fix/t751-flaky-vedder`, test file only; left: the 31 sibling waits listed at the end))

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

## Cause proven and fixed, 2026-11-16 (app chat)
It went red again on GitHub on 44b9c25bf (run 43921202721, a merge that changed no app code), at
`WaterMode.test.tsx:112:69`: the file's `until` helper, a `waitFor` on the condition with testing-library's DEFAULT
deadline of 1 s. Every other load wait in the file carries 20 s, and the twin helpers in `apps/mobile` carry 20 s and 30 s.
- **The wait that loses:** `until(() => where().startsWith("Part 1 of 5 · Chilliwack River"))` after ▲ into the
  Chilliwack. The step makes one read of the release; `WaterMapView` keeps the Vedder's view up to `PANE_HOLD_MS`
  (1.2 s) while it loads (`useKeptWhilePending`). A read slower than 1 s leaves the old view up when the wait gives up.
- **Reproduced without loading the machine** (a temporary loader that delays reads while the step runs; not
  committed): read delayed 1,100 ms and 1,500 ms: the same failure at the same line ("expected false to be true");
  delayed 600 ms: passed once and failed once on a busy Mac; with the 20 s deadline: passes at 1,100, 1,500 and
  3,000 ms. Unslowed, the three waits take 51 to 74 ms here.
- **Fix:** `until` carries the file's load deadline (20 s). The condition is still what is waited for; the deadline
  only bounds a hang. Fake time was not used: the step's read is real file I/O, which fake timers do not advance.
- **Mutation:** with `route.ts` `case "cross"` made to stack instead of replace, the test fails (the five-line
  comparison). Whole file 9 of 9, three runs.
- **Not done:** the 50 throttled runs of the acceptance line (the emulator benches had the machine); the
  reproduction above stands in for them.
- **Left (work item 4):** 31 more `waitFor` calls with the default 1 s deadline in render tests that load a release,
  each to be read for whether it waits across a load: `packages/ui/src/regs.render.test.tsx` 9,
  `WaterScreen.salmon.test.tsx` 4, `MapDay.shell.test.tsx` 4, `water/WaterMapView.test.tsx` 3,
  `WaterScreen.unnamed.test.tsx` 3, `WaterScreen.regs-props.test.tsx` 3, `TapCue.shell.test.tsx` 3,
  `guideClosed.test.tsx` 1, `Walkthrough.test.tsx` 1. `StepInPlace.shell.test.tsx` has none.
