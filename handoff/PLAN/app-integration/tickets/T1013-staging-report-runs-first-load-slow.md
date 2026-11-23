# T1013 — Staging report 2026-11-22: switching to Runs takes "a very long time" the first time

Status: OPEN — next release batch.

Source: `GET /reports` on the STAGING feeds worker, read 2026-11-22 18:15 (report `50e342ef056ab300`). Android 0.8.5
(build 43, 558c527), staging, release `r-741815c6090d`; screen "more", standard mode, zoom 6.9 at 49.522, -122.492.
Reason "bug". Verbatim: "When you switch to runs it takes a very long time to load the first time?"

## Work
1. Reproduce on the emulator from a fresh install and from a warm one: time from the switch to the first coloured
   salmon line; record what loads (DFO feed, salmon chains, tiles) and in which order.
2. Cut the first-switch wait (prefetch the salmon reads when the map is idle, or show progress); one load path for
   every way into Runs (`one-load-path-per-screen`). Before/after timings in this ticket.

Depends: none. Size: M. Owner: no.
