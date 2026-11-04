# T379 — WaterMode "ONE MAP ENTRY" test is timing-dependent

Status: OPEN (2026-11-04; before the 0.5.0 suite run, batch 7).

`packages/ui-native/src/WaterMode.test.tsx` › ONE MAP ENTRY (T71): `regs-header` never appears — the page shows the
loading spinner after the Keep tab appeared. Measured 2026-11-04 in a clean worktree: it fails run ALONE on 0b8a47e7f
(the session's start), 777a4fc2b, 61f819c52 (once passed, then failed twice) and 09d2d845e (3/3), and passed inside the
full suite at 0b8a47e7f. T367 (i) saw it too. So it is not T342/T376; it is a race in the water page's first load on
the regs fixture release (one load path, T369, may be the real fix). Find the race (a load that resolves after the
test's wait, or a second load that resets the page to loading), fix the page or the test's wait — not a longer timeout.
