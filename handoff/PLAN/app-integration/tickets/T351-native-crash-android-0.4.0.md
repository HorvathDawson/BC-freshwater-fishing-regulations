# T351 — Native crash in the foreground on Android 37 (0.4.0): no stack, cause unknown

Status: OPEN (from T346, 2026-11-02).

Evidence (staging crash store, `private/crashes/`): two foreground native exits on 0.4.0 (build 7, 89ebd94), platform
android 37, route "other":
- 2026-11-01 03:14 UTC: "CRASH while foreground, 406 MB in use: crash" (hash cee2100c438699bb)
- 2026-11-02 18:16 UTC: "CRASH_NATIVE while foreground, 435 MB in use, status 11: crash" (3f2b57e6ee0b6d4a); status 11 = SIGSEGV.
Both likely the owner's phone. The reports carry no stack and no screen, so the cause is not readable from them. Memory
is high (400+ MB) so look at a memory/native-map crash first: reproduce with the test bed (long session, map + layers,
zoom around), then `adb logcat` / the tombstone. Also see T352 (reports should carry more).

Background kills (not crashes, no action): SIGNALED status 9 at 99 MB (0.2.0, 1), LOW_MEMORY at 169 MB (0.3.0 and 0.4.0,
2 in one hash). T352 asks that these be marked non-fatal-to-the-user.
Size: M.

- 2026-11-05: no foreground native crash from 0.5.0 yet (the store holds only a 0.4.0 LOW_MEMORY background kill since);
  stays OPEN waiting for a 0.5.0 report.
- 2026-11-06, simulation (owner rule): NOT reproduced. Test-bed APK of redesign/stream-sections @ bccf9f668 (Expo 58), n1_pixel
  (2 GB) both normal and stressed (2 efficiency cores, `TESTBED_EMU_ECORES=1 -cores 2`): ~25 pan sessions (16–160 swipes,
  z10–z13, flings, a cold page cache), the guided walk ×3, `memory` (boot, pans, zoom, Cultus Lake, the rule book twice,
  Conditions, Salmon, RUNNING_CRITICAL trim), the crash bed. No foreground native crash, no tombstone, nothing in the crash
  buffer but the crash bed's own (`out/runs/stress*/exits.txt`: only force-stops and the test's JS crash). Peak PSS on the
  stressed `memory` run 385 MB (Conditions; native 231), under the reports' 406/435 MB. What the test bed can now do for a
  real one: a native SIGSEGV is reported end to end as `CRASH_NATIVE … status 11` (T242 `crash-bed`), so the next phone crash
  arrives in the same shape — still no stack (Android gives none to the app; a tombstone needs the device). Stays OPEN.
  Test-bed drift seen on the way: `layers-satellite` / "Choose layers" flows no longer find the Layers button (memory's
  satellite + Conditions-layer steps and `PAN_GROUND=satellite` fail) — a flow to update, not an app crash.

- 2026-11-08 (gate 2, T495): one ~28 min emulator session on 8fa19a6 (satellite, 2×16-swipe pans, gestures, the 30-step walkthrough, memory, 32 tab switches): 6/6 pass, no crash, ANR or tombstone. Crash store: no foreground crash from any 0.6.x build (every report is the emulator or the web, T494). Stays OPEN until the owner phone session (T495 gate 2).

- 2026-11-09 (T360 round 7): emulator peak PSS 379 MB (Salmon, build-21-era lean APK) — under the reports' 406/435 MB;
  HEAD + allocator decay experiment peaked at 314 MB (not a clean A/B, see T360). No crash in either run. Stays OPEN for
  the owner's build-22 session (OWNER-TODO).
