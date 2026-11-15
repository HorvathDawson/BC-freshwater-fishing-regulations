# T749 — Android: turn the code shrinker (R8) back on, with keep rules, behind a device proof

Status: OPEN (proposal from T741; needs an emulator run and then a phone before it can ship).

Source: T741 survey 2026-11-14 (owner, verbatim: "can you make sure to keep working on optimizations? make app faster
and lighter for both ios and android"). History: T250, where the first APK built with R8 died at start and R8 was
turned off.

## Why
The Android bundle's compiled Java/Kotlin code (dex) is 30.4 MB of the 38.8 MB AAB (0.7.10 build 32) because R8 is off.
R8 typically removes 50 to 70 % of dex; estimated 6 to 10 MB off the Play download. Estimated, not measured.

## Work
1. On a branch (`perf/r8`): R8 on for release builds with keep rules for what reflection reaches: WorkManager and Room,
   the patched MapLibre, pdfium, the background-download module, expo modules, Hermes/JNI entry points. Find the T250
   crash cause first (its stack is in T250) and pin it with a keep rule and a note.
2. Measure: AAB and per-device download size before and after (`bundletool` is not installed: install it or say the
   figure is an estimate).
3. Device proof on the emulator: `run.mjs install crash-bed bg-install regs-open tap-stalls memory`, all as on the
   build without R8; then a Play internal build on the owner's phone before it reaches closed testing.

## Acceptance
- The app starts and every bench above passes with R8 on; the size before and after is in this ticket.
- The owner's phone check on a Play internal build is recorded.

Risk: high (a missing keep rule crashes only on the path that needs it). Size: M. Owner: phone check.
