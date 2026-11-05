# T397 — Runs tab: the fish chips cannot be scrolled

Status: DONE in code 2026-11-05 (owner report 2026-11-05, 0.5.0 build 8, Runs). Bug.

The row of fish (run) options doesn't pan sideways on the phone: only the first few are reachable. Make the row scroll
horizontally (or wrap), and check it isn't swallowed by the map's pan gesture underneath. Test-bed check on the emulator.

## Done in code 2026-11-05 — phone check with the next APK
Cause: the map foot (legend / Runs bar) hung from a ZERO-HEIGHT row (`map-foot`, Shell.tsx, T275) and drew above it.
Android hands a native touch only to a view inside its parent's bounds: taps still worked (React Native hit-tests in
JS) and the slider is a PanResponder (JS), but the species row is a native horizontal ScrollView and never got the drag.
Fix: the foot is an absolute block at the bottom of the map area (same place on screen, same paint order). Guard in
Runs.shell.test.tsx: no ancestor of the species row is zero-height (fails on the old layout). Any other native scroller
in the foot (the Conditions forecast-day list) had the same problem and is fixed with it.
