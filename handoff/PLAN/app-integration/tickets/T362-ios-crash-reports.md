# T362 — iOS crash reports

Status: OPEN (owner 2026-11-03: Android 0.5.0 first, iOS next; from the ticket check's iOS gaps).

Android records why the app died (exit-info, T242/T351/T352); on iOS that module is a no-op, so an iOS crash
leaves nothing. Add an iOS path (MetricKit diagnostics or equivalent) that reports into the same crash-report
endpoint as T242. Size: S–M.
