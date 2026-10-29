# T286 — Tapping a water opens its regulations instantly on phones (never needs a spinner)

Status: OPEN (owner 2026-10-29: "should make the regulation opening faster when a click happens on mobile too. it should
never be slow enough to need a loading icon").

## Work
1. Measure tap → pane drawn with content on the testbed emulator (and a low-end profile) for: a small stream, a big
   multi-part river (Fraser), a lake; split the time (hit test, SQLite reads through the T264 serial queue, decompress,
   hydrate/buildRegsPage, React render, first frame).
2. Fix by cause: show the pane shell + header from what the tap already knows in the same frame; read the regs rows in one
   batched statement; cache hydrated pages (LRU by release + day); precompute/prefetch for the waters on screen when the map
   settles (idle only); move heavy work off the render path; keep T264's race fix.
3. Budget: content on screen ≤ 150 ms on the emulator for every case above; a test/benchmark pins it.

## Acceptance
Before/after timings per case; no spinner shown on a normal tap; checks green.

Depends: T264, T273. Size: M. Owner: no.
