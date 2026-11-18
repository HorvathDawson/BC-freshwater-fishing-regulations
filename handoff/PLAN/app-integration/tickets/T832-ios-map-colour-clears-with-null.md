# T832 — iOS: a river can keep an old colour after a horizon change or a feed refresh (null feature-state writes dropped)

Status: OPEN (app chat; needs a style change).

Source: found by the T826 agent 2026-11-17 (T826 part 5's cause, the same on the colouring path).

React Native's iOS bridge drops any key whose value is null, so a feature-state write `{key: null}` reaches MapLibre
as `{}` and the old value stays. T826 fixed the highlight by writing `false` (`sections.ts` `unsetValue`); the map's
colouring still clears with null. Fix: a style that reads a cleared colour from a non-null sentinel (e.g. -1 / false),
the colouring writes it, an adapter-contract test fails if a null write comes back; prove on the iOS simulator by a
horizon change (a river that loses its colour shows none).

## Done (branch worktree-agent-a1e41c47a10b4ccef, 2026-11-18, 0.8.1 lane A; not merged)

- The sentinel is `""` for every non-highlight key (`sections.ts` `CLEARED` / `unsetValue`; highlight keys stay `false`):
  every string paint already reads `""` as unset (`coalesce(…, "")`, a categorical `match`'s fallback). The continuous
  `standing` colour now reads anything but a number as `missing` (`style.ts` `colorExpression`: `typeof != number`), so
  `-1` (no baseline) keeps its meaning and the dash is not lit by `""`.
- Written on both platforms from one place: the engine (`colouring.ts` `label`: a `null` label is written as the
  sentinel), the phone's queue clear (`native/useColouring.ts` `StateQueue.clear`), the basins' clears
  (`adapters/contract.ts` `setChannel` standing / runs). No feature-state write in `packages/map` carries a null now.
- Test: `app/tools/ios-null-clears.test.ts` — the engine through `StateQueue` and an iOS bridge that drops null keys: a
  horizon change that leaves a river nothing to say shows `missing` (evaluated with MapLibre's evaluator), a run label
  cleared and the whole channel withdrawn likewise, and no write sent is a null (+ a mutation pin of the old null write).
- NOT done: the iOS simulator proof (no iOS builds in this lane) — owed after merge.
