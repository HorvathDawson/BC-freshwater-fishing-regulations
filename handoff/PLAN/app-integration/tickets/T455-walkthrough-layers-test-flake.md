# T455 — Walkthrough "Layers" test skips the ground step under load (flaky)

Status: DONE 2026-11-06 — 2290b6e25

`pnpm check` on 56c18c597 (quiet machine, under `scripts/heavy.sh`): 1 failed / 6700 passed. The failure was
`packages/ui-native/src/Walkthrough.test.tsx` › "(owner 2026-10-29) Layers: open, switch the ground, fish obstacles
on — then Next puts them back": log `[layers, obstacles, restore]`, expected `[layers, ground, obstacles, restore]`.
The file alone: 20/20 pass.

`otherGround` never ran: the walk got from `ground` to `obstacles` without the ground step's Show me. So a second
advance ran: the layers step's 700 ms done-timer (`useGuide`, `setTimeout(next, 700)`) and another `next`, or a resume
from `guide-at:layers` that lands late. If the double advance can happen in the app, a reader can skip a step. This is
not only a test problem until shown otherwise.

## Work
1. Reproduce: run the file repeatedly under CPU load (e.g. `vitest run --repeat` with a busy loop beside it).
2. Find the second advance (log every `next` with its caller). Fix it at the source. Never by a longer wait in the
   test.
Size: S.

## Done (2290b6e25)
Cause: a real app race. A Pressable (RN and RN-web) takes a new `onPress` only in a passive effect after the commit.
The timer moved layers → ground; a tap landing before that effect ran the LAYERS card's handler, whose step was done,
so it called `next()` again: ground → obstacles, ground never shown. The same double advance could come from a tap in
the 700 ms timer's own tick. Fix: `next(from)` drops a move from a step already left (timer and card pass their step);
the card's buttons call through a ref set at commit (`useLayoutEffect`), so a tap acts on the step on screen. Test
unchanged. Repro (file run alone beside 18 busy loops): before 2/20 failed, after 0/40. `pnpm -C app check`: only
failure `no-silent-skips` on `tools/testbed/scenarios/photo-zoom.mjs` (13eb712cd, T454's, not this change).
