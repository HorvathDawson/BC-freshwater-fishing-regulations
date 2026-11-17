# T767 — Four small app faults from the 2026-11-16 audit (fetch lane, Flow / Level pick, a swallowed failure, salmon text)

Status: IN PROGRESS (built on branch `app-fix/t766-t767`, 2026-11-17; all four fixed with a test that fails before; Android benches owed before merge).

Source: `handoff/PLAN/audit-2026-11-16/code.md` and `guards.md` (the main chat's audit). Area: the app.

## The four
1. **Fetch-ahead lane does not give way (T737; speed only).** `app/packages/data/src/release/lane.ts` `promote`, the
   "started" branch (about lines 151-154): a file promoted out of the fetch-ahead lane never calls `yieldSoon()`, so
   the ahead-fetch that is running is not aborted. The audit reproduced it with `joinMs` 10: after 50 ms `core` was
   not aborted (`{running: 'core', readers: 1}`). Fix + a test with that timing.
2. **The Flow / Level pick carries to the next reach (T728).** `app/packages/ui-native/src/WaterViews.tsx` (about line
   73): the pick is component state not keyed by the water or the tapped section, so the next reach opens on the last
   one's pick. One load path (T369): the same reach must open the same whatever was open before. By reading; write
   the failing render test first.
3. **A failure is swallowed.** `app/packages/ui/src/prefetch.ts` (about line 220): a rejected `waterRegionsOf` is
   caught and dropped. No silent fallbacks (T333): report it with `reportFailure`.
4. **A salmon verdict's short text drops "hatchery marked only".** The feed's verdict text `t` for Fraser coho reads
   "2 per day (Nov 7–30)"; the condition is only in `tree.says`. Check that no screen shows `t` alone; if one does,
   show the condition with it. (The text itself is the worker's; a worker change waits for the worker merges.)

## Done when
Each of the four has a test that fails before its fix; app code changes go on a branch and merge with the Android
benches and a message to the main chat, as always.

## Built (2026-11-17, branch `app-fix/t766-t767`)
1. **Fetch-ahead lane** (`data/src/release/lane.ts` `promote`): the "started" branch now calls `yieldSoon()`, so the file
   ahead on the network is aborted after `joinMs` like for any reader download. Test (`lane.test.ts`, `joinMs` 10): after
   50 ms `core` is aborted and waits in the lane again (`{ running: null, waiting: ["core"], readers: 1, aborted: 1 }`).
   Mutation: drop the call -> `core` is never aborted.
2. **Flow / Level pick carried to the next reach** (`ui-native/src/WaterViews.tsx`): the pick is held with the water and
   section it was made on (`pick.at`), read only there, and dropped when the view moves to another reach — so the next reach,
   and the first one again, open as they open (T369). Failing render test first (`WaterConditions.levelOnly.test.tsx`,
   "a Flow / Level pick stays on its reach"): before, both read "Flow Level*". Mutations: a bare `useState` (fails on the next
   reach); no drop on a move (fails on the first reach again).
3. **Swallowed `waterRegionsOf` failure** (`ui/src/prefetch.ts` `useWaterRegionsAhead`): the rejection goes to
   `reportFailure` (an offline line is the environment's and not reported). Test in `prefetch.render.test.tsx` with a failure
   sink; mutation: the old `() => {}` -> nothing reported. `tools/failures.test.ts` FINE count for the file 4 -> 3.
4. **Salmon verdict text without its condition**: the one screen showing a verdict's `t` is the salmon card's "DFO says"
   lines (`core/src/salmon.ts` `salmonBlock` `notesOf`). With one reading the quota line shows the tree (with "hatchery-marked
   only") and `t` is not repeated — fine. But a page on two chains whose verdicts read alike and whose trees differ draws no
   quota line, and `t` stood alone. Now each open verdict there says its tree's words beside DFO's (test in `salmon.test.ts`,
   "(T767) verdicts alike in words, trees apart"). Found with it: the stretch join key (`salmonTimelineKey`) compared `t`
   and not the tree, so two pieces whose verdicts differ only in the tree's condition JOINED into one entry; the key now holds
   the tree's words (never node ids). Test in `waterNav.merge.test.ts`. Both mutation-checked. The worker's short text itself
   is left as it is (a worker change, waiting for the worker merges).
- Left: the Android benches at merge.

