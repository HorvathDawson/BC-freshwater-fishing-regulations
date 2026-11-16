# T767 — Four small app faults from the 2026-11-16 audit (fetch lane, Flow / Level pick, a swallowed failure, salmon text)

Status: OPEN (filed 2026-11-16 by the app chat from the general audit).

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
