# T1013 — Staging report 2026-11-22: switching to Runs takes "a very long time" the first time

Status: IN PROGRESS — two cuts in (02d0a8a90); left (Claude): the emulator timings before / after (both heavy locks with an emulator were taken 2026-11-22 evening)

Source: `GET /reports` on the STAGING feeds worker, read 2026-11-22 18:15 (report `50e342ef056ab300`). Android 0.8.5
(build 43, 558c527), staging, release `r-741815c6090d`; screen "more", standard mode, zoom 6.9 at 49.522, -122.492.
Reason "bug". Verbatim: "When you switch to runs it takes a very long time to load the first time?"

## Work
1. Reproduce on the emulator from a fresh install and from a warm one: time from the switch to the first coloured
   salmon line; record what loads (DFO feed, salmon chains, tiles) and in which order.
2. Cut the first-switch wait (prefetch the salmon reads when the map is idle, or show progress); one load path for
   every way into Runs (`one-load-path-per-screen`). Before/after timings in this ticket.

Depends: none. Size: M. Owner: no.

## Lane L4, 2026-11-22
**What the first switch waits for** (`packages/data/src/feed/runtiming.ts` + `@app/ui` `useRunTiming`): nothing is
read until the Runs tab is opened (T199: "read the first time Run timing is chosen"). Then, in order: the last capture
read and parsed (a phone: a ~5.9 MB JSON file), the index (953 B), the two files (`runs` 1.4 MB + `paint` 4.4 MB
decoded, ~1.0 MB on the wire gzipped), each sha256-checked and JSON-parsed, `RunTimingFacts` built, and then the whole
capture `JSON.stringify`'d and written to disk BEFORE the answer came back; only then does the map colour the runs.
Measured on V8 (Node, vitest, staging runtiming, this Mac): cold 672 ms (fetches 257 + 174 + 352 ms), warm from the
capture 144 ms; JSON.parse of the capture 14 ms, parse + stringify 26 ms. Hermes on a phone is several times slower on
the hash and the parse, and a phone's line slower than this Mac's: that is the "very long time". NOT measured on the
emulator (see Status).

**Cut (02d0a8a90):**
1. A phone reads the feed AHEAD: `usePrewarmRuns` (`ui-native/src/shell/prewarm.native.ts`) asks the source's own
   `latest()` idle 4 s after the map is paired. One load path: it is the same held promise the tab asks for (held for the
   hour), so whichever asks first, the other waits for the same read. The web does nothing (it fetches nothing unasked).
2. The answer no longer waits for the capture write (written after), and the capture is read while the index is asked.

**Left:** emulator, fresh install and warm: tap Runs, time to the first coloured run line (testbed `mode-switch`'s
Runs leg on a cold start, before = origin/main, after = this branch). If painting (not the read) is most of it, that is
the next cut.
