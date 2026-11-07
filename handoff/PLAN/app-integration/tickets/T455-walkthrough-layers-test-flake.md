# T455 — Walkthrough "Layers" test skips the ground step under load (flaky)

Status: OPEN (chat 5, 2026-11-06).

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
