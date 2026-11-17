# T832 — iOS: a river can keep an old colour after a horizon change or a feed refresh (null feature-state writes dropped)

Status: OPEN (app chat; needs a style change).

Source: found by the T826 agent 2026-11-17 (T826 part 5's cause, the same on the colouring path).

React Native's iOS bridge drops any key whose value is null, so a feature-state write `{key: null}` reaches MapLibre
as `{}` and the old value stays. T826 fixed the highlight by writing `false` (`sections.ts` `unsetValue`); the map's
colouring still clears with null. Fix: a style that reads a cleared colour from a non-null sentinel (e.g. -1 / false),
the colouring writes it, an adapter-contract test fails if a null write comes back; prove on the iOS simulator by a
horizon change (a river that loses its colour shows none).
