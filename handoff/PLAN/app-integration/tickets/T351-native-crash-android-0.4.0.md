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
