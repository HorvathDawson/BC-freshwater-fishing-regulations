# T273 — Panning feels laggy on Android: preload and cache around the view

Status: OPEN (owner 2026-10-29: "when panning around the app it feels a little bit laggy. maybe we could have better
preloading in the app and caching?").

## Work
1. Profile a scripted pan on the testbed emulator (frame times, JS thread, SQLite queue depth, tile fetches): split the lag
   between map rendering, per-viewport regs/colour queries (T264 serial queue), and tile/network fetches.
2. Fix by cause: cache per-section answers in memory (LRU, keyed by release + day), batch viewport queries into one
   statement, keep work off the JS thread during gestures (debounce until the pan settles), prefetch tiles/answers one
   screen beyond the view and for neighbouring regions (builds on T253).
3. Pin with a repeatable pan benchmark (numbers in the ticket, before/after).

## Acceptance
Benchmark shows fewer dropped frames and no visible colour pop-in on a normal pan; no regression on web.

Depends: T253, T264, T272 (same emulator lane). Size: M. Owner: no.
