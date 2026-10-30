# T314 — Spot data refresh doesn't work in 0.3.0

Status: OPEN (owner 2026-10-31, on the 0.3.0 APK: "the refresh for spot data isn't working").

## Work
Reproduce on the emulator (0.3.0 build, staging): a spot saved offline or with pending weather/flow, then online → refresh
(the spot page's refresh / the app's reconnect backfill, T294 refresh across visits, T307 fast-fail backfill). Find why it
doesn't fill (T307's fastFail answering "failed" permanently? online detection never flips? refresh only for visit 1?
paint v5 / release swap?), fix, and pin with a test (pending → online → filled). Check both spot-level and visit-level.

Depends: T294, T307. Size: S. Owner: no.
