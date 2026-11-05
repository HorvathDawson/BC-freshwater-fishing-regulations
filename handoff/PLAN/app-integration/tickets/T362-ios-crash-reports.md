# T362 — iOS crash reports

Status: DONE 2026-11-05 — 5b832c347, 59fc7bb24 (iOS module), 5918cfa45 (privacy page), 089c8a00f + bf8ca3820 (test bed); verified: ios-t313g crash scenario.

Android records why the app died (exit-info, T242/T351/T352); on iOS that module is a no-op, so an iOS crash
leaves nothing. Add an iOS path (MetricKit diagnostics or equivalent) that reports into the same crash-report
endpoint as T242. Size: S–M.

## Done (2026-11-05)
iOS keeps no ApplicationExitInfo for an app, so the app writes its own record at the moment it dies: `modules/exit-info`
gets an iOS half (`ios/ExitInfoModule.swift`, autolinked pod `ExitInfo`):
- an Expo app-delegate subscriber installs, before the app's own launch code, an async-signal-safe handler for SIGSEGV,
  SIGBUS, SIGILL, SIGFPE, SIGABRT, SIGTRAP (on its own alternate stack, so a stack overflow is caught too; the previous
  action is restored and the signal re-raised) and an uncaught-NSException handler (chained to the previous one);
- each writes one small file to Library/Caches/exit-info: time, signal, app state (FOREGROUND / VISIBLE / BACKGROUND, from
  the delegate), memory footprint (what iOS's memory limit reads) and, for an exception, its name + reason. No stack, no paths;
- `recent(since)` returns them in Android's ExitRecord shape (signal → CRASH_NATIVE with status = signal and the signal's
  name; exception → CRASH with its words), newest 8 kept. JS is unchanged: `reportPastExits` sends them to POST /crash under
  the same switch and scrubbing; an RCTFatal's CRASH is skipped when the kept JS fatal was already sent (as on Android).
- `build-ios-sim.sh --probes` builds tag their reports `test` (the scenario crashes on purpose).
- Privacy page: "iPhones send no crash reports yet" replaced by what the note holds (the effective date was not moved; bump it
  when an iOS build first ships).
Verified (ios-t313g): SIGSEGV to the app's process → the next start logs "sent 1 exit record(s); … (CRASH_NATIVE)" (the worker
answered 2xx); a start after a normal stop sends 0.
Not caught (said in the module): kills by the system (memory/jetsam, watchdog) and the user (SIGKILL) send no catchable
signal. MetricKit's diagnostics (a day late, with stacks) would add those; a follow-up only if real-phone reports show
unexplained gaps. An NSException path was not exercised on the simulator (no way to raise one from outside); the signal path was.
