# T1094 — Android: the system ended the app for low memory while it was in the foreground (0.8.5, staging)

Status: OPEN.

Source: staging feeds worker, crash kind `9588028ebf260364`, 1 report, 2026-11-24 22:20Z, app 0.8.5: "NativeExit
LOW_MEMORY while foreground, 118 MB in use: [UNDELIVERED BROADCAST] Can't deliver broadcast". Two seconds later the
same install sent `804e01a3d7d4fd97` (HTTP 404, T1005 / T1014), so it restarted straight away. Read by
`pipeline reports`, planning chat 2026-11-24 22:20 MST.

## Work
1. Read the exit record's detail (`GET /crashes`, read-only): device, Android version, what screen, the memory trend
   if the record holds one. 118 MB is small for a map app, so the phone itself was probably short of memory.
2. Say whether 0.9.0 changes the picture (the pinned MapLibre build is 20–32% cheaper, T420's line): watch for the
   same kind on 0.9.0 for two weeks before spending a lane on it.
3. If it recurs on 0.9.0: measure the app's memory on the low-memory emulator profile over the report's path and
   ticket the largest holder.

## Acceptance
The kind is cited here with a verdict (device-side, or a defect with its own fix ticket).

Severity: low (one report, old build). Release: 0.9.2 (step 1 and 2 only; step 3 only if it recurs). Model: Sonnet.
Size: S. Owner: no.
