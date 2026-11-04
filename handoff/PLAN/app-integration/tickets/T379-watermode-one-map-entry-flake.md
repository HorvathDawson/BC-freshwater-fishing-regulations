# T379 — WaterMode "ONE MAP ENTRY" test is timing-dependent

Status: DONE — 75a907e3b (2026-11-04, with T369).

`packages/ui-native/src/WaterMode.test.tsx` › ONE MAP ENTRY (T71): `regs-header` never appears — the page shows the
loading spinner after the Keep tab appeared. Measured 2026-11-04 in a clean worktree: it fails run ALONE on 0b8a47e7f
(the session's start), 777a4fc2b, 61f819c52 (once passed, then failed twice) and 09d2d845e (3/3), and passed inside the
full suite at 0b8a47e7f. T367 (i) saw it too. So it is not T342/T376; it is a race in the water page's first load on
the regs fixture release (one load path, T369, may be the real fix). Find the race (a load that resolves after the
test's wait, or a second load that resets the page to loading), fix the page or the test's wait — not a longer timeout.

## Done (75a907e3b)
The race: `useDelayedShow`'s 300 ms show timer could fire after the render that drew the page had committed but before
its effect clean-up ran (a loaded machine), putting the spinner back over the page for the 400 ms hold — the Keep tab
seen, then the page gone. The timer now checks the wait as of the last render (`busyNow`). The test also waits on
`regs-header` itself. Run alone 5/5 and 3× concurrently under load, all green.
